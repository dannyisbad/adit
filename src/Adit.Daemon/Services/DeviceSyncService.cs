using System.Globalization;
using System.Threading.Channels;
using Adit.Core.Models;
using Adit.Core.Services;
using Microsoft.Extensions.Hosting;

namespace Adit.Daemon.Services;

public sealed class DeviceSyncService : BackgroundService
{
    private static readonly TimeSpan AncsStartupTimeout = TimeSpan.FromSeconds(25);
    private static readonly TimeSpan RecoveryRequestThrottle = TimeSpan.FromSeconds(10);

    private readonly object recoveryGate = new();
    private readonly Dictionary<string, DateTimeOffset> lastRecoveryRequestUtcByTransport = new(StringComparer.OrdinalIgnoreCase);
    private readonly Channel<string> syncRequests = Channel.CreateUnbounded<string>(
        new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });
    private readonly SemaphoreSlim syncLock = new(1, 1);

    private readonly ILogger<DeviceSyncService> logger;
    private readonly DaemonOptions options;
    private readonly DeviceCatalog deviceCatalog;
    private readonly PbapContactsService contactsService;
    private readonly MapMessagingService mapMessagingService;
    private readonly MapRealtimeSession mapRealtimeSession;
    private readonly AncsSession ancsSession;
    private readonly ConversationSynthesizer conversationSynthesizer;
    private readonly SqliteCacheStore cacheStore;
    private readonly DeviceFusionCoordinator fusionCoordinator;
    private readonly RuntimeStateService runtimeState;
    private readonly DaemonEventHub eventHub;
    private readonly PhoneLinkEvictionPolicy evictionPolicy;
    private readonly LearnedThreadReranker learnedThreadReranker;
    private volatile string notificationsMode = NotificationMode.Auto;
    private volatile bool notificationsEnabled;

    public DeviceSyncService(
        ILogger<DeviceSyncService> logger,
        DaemonOptions options,
        DeviceCatalog deviceCatalog,
        PbapContactsService contactsService,
        MapMessagingService mapMessagingService,
        MapRealtimeSession mapRealtimeSession,
        AncsSession ancsSession,
        ConversationSynthesizer conversationSynthesizer,
        SqliteCacheStore cacheStore,
        DeviceFusionCoordinator fusionCoordinator,
        RuntimeStateService runtimeState,
        DaemonEventHub eventHub,
        PhoneLinkEvictionPolicy evictionPolicy,
        LearnedThreadReranker learnedThreadReranker)
    {
        this.logger = logger;
        this.options = options;
        this.deviceCatalog = deviceCatalog;
        this.contactsService = contactsService;
        this.mapMessagingService = mapMessagingService;
        this.mapRealtimeSession = mapRealtimeSession;
        this.ancsSession = ancsSession;
        this.conversationSynthesizer = conversationSynthesizer;
        this.cacheStore = cacheStore;
        this.fusionCoordinator = fusionCoordinator;
        this.runtimeState = runtimeState;
        this.eventHub = eventHub;
        this.evictionPolicy = evictionPolicy;
        this.learnedThreadReranker = learnedThreadReranker;

        mapRealtimeSession.EventReceived += OnMapRealtimeEvent;
        mapRealtimeSession.StateChanged += OnTransportStateChanged;
        ancsSession.NotificationReceived += OnNotificationReceived;
        ancsSession.NotificationRemoved += OnNotificationRemoved;
        ancsSession.StateChanged += OnTransportStateChanged;
    }

    public void RequestSync(string reason = "manual")
    {
        syncRequests.Writer.TryWrite(string.IsNullOrWhiteSpace(reason) ? "manual" : reason.Trim());
    }

    public bool AreNotificationsEnabled()
    {
        return notificationsEnabled;
    }

    public string GetNotificationsMode()
    {
        return notificationsMode;
    }

    public override void Dispose()
    {
        mapRealtimeSession.EventReceived -= OnMapRealtimeEvent;
        mapRealtimeSession.StateChanged -= OnTransportStateChanged;
        ancsSession.NotificationReceived -= OnNotificationReceived;
        ancsSession.NotificationRemoved -= OnNotificationRemoved;
        ancsSession.StateChanged -= OnTransportStateChanged;
        syncLock.Dispose();
        base.Dispose();
    }

    public async Task<NotificationsToggleResult> SetNotificationsEnabledAsync(
        bool enabled,
        CancellationToken cancellationToken)
    {
        var targetMode = enabled ? NotificationMode.On : NotificationMode.Off;
        var previouslyEnabled = notificationsEnabled;
        if (!enabled)
        {
            notificationsMode = NotificationMode.Off;
            notificationsEnabled = false;
            await cacheStore.SetNotificationsModeAsync(NotificationMode.Off, cancellationToken);
            var disabledSnapshot = runtimeState.SetNotificationsMode(NotificationMode.Off, false);
            await ancsSession.StopAsync();
            eventHub.Publish(
                "notifications.disabled",
                new
                {
                    runtime = disabledSnapshot
                });
            return new NotificationsToggleResult(
                Mode: NotificationMode.Off,
                Enabled: false,
                Ready: false,
                Status: "disabled",
                Reason: "Notifications are disabled.",
                Target: disabledSnapshot.Target,
                LeTarget: null,
                Runtime: disabledSnapshot);
        }

        var target = runtimeState.GetSnapshot().Target
            ?? await deviceCatalog.SelectClassicTargetAsync(null, options.DefaultNameContains);
        if (target is null)
        {
            var noDeviceSnapshot = runtimeState.SetNotificationsMode(notificationsMode, notificationsEnabled);
            return new NotificationsToggleResult(
                Mode: notificationsMode,
                Enabled: previouslyEnabled,
                Ready: false,
                Status: "no_device",
                Reason: "No paired classic iPhone target is available.",
                Target: null,
                LeTarget: null,
                Runtime: noDeviceSnapshot);
        }

        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(AncsStartupTimeout);
            var leTarget = await ResolveLeTargetForAncsAsync(target, timeout.Token);
            await ancsSession.StartAsync(leTarget, timeout.Token);

            var ready = ancsSession.CurrentPhase == DeviceSessionPhase.Connected;
            if (!ready)
            {
                return new NotificationsToggleResult(
                    Mode: notificationsMode,
                    Enabled: previouslyEnabled,
                    Ready: false,
                    Status: "not_ready",
                    Reason: "ANCS did not reach subscriptions_ready while adopting the existing Link to Windows pairing.",
                    Target: target,
                    LeTarget: leTarget,
                    Runtime: runtimeState.GetSnapshot());
            }

            notificationsMode = targetMode;
            notificationsEnabled = true;
            await cacheStore.SetNotificationsModeAsync(targetMode, cancellationToken);
            var enabledSnapshot = runtimeState.SetNotificationsMode(targetMode, true);
            eventHub.Publish(
                "notifications.enabled",
                new
                {
                    target,
                    leTarget,
                    runtime = enabledSnapshot
                });
            RequestSync("notifications_enabled");
            return new NotificationsToggleResult(
                Mode: targetMode,
                Enabled: true,
                Ready: true,
                Status: "enabled",
                Reason: null,
                Target: target,
                LeTarget: leTarget,
                Runtime: enabledSnapshot);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return new NotificationsToggleResult(
                Mode: notificationsMode,
                Enabled: previouslyEnabled,
                Ready: false,
                Status: "timeout",
                Reason: "ANCS adoption timed out before subscriptions became ready.",
                Target: target,
                LeTarget: null,
                Runtime: runtimeState.GetSnapshot());
        }
        catch (Exception exception)
        {
            logger.LogDebug(exception, "Notification enable failed.");
            return new NotificationsToggleResult(
                Mode: notificationsMode,
                Enabled: previouslyEnabled,
                Ready: false,
                Status: "failed",
                Reason: exception.Message,
                Target: target,
                LeTarget: null,
                Runtime: runtimeState.GetSnapshot());
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await cacheStore.InitializeAsync(stoppingToken);
        notificationsMode = await cacheStore.GetNotificationsModeAsync(stoppingToken);
        notificationsEnabled = notificationsMode != NotificationMode.Off;
        var initializedSnapshot = runtimeState.SetNotificationsMode(notificationsMode, notificationsEnabled);
        eventHub.Publish(
            "notifications.configuration",
            new
            {
                mode = notificationsMode,
                enabled = notificationsEnabled,
                runtime = initializedSnapshot
            });

        var nextReason = "startup";
        while (!stoppingToken.IsCancellationRequested)
        {
            await RunSyncAsync(nextReason, stoppingToken);

            var snapshot = runtimeState.GetSnapshot();
            var delay = snapshot.Phase == "error"
                ? TimeSpan.FromSeconds(options.ErrorBackoffSeconds)
                : TimeSpan.FromSeconds(options.SyncIntervalSeconds);

            try
            {
                var readTask = syncRequests.Reader.ReadAsync(stoppingToken).AsTask();
                var delayTask = Task.Delay(delay, stoppingToken);
                var completed = await Task.WhenAny(readTask, delayTask);
                nextReason = completed == readTask ? await readTask : "timer";
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }

        await StopRealtimeSessionsAsync();
    }

    private async Task RunSyncAsync(string reason, CancellationToken cancellationToken)
    {
        await syncLock.WaitAsync(cancellationToken);
        try
        {
            runtimeState.MarkSyncStarting(reason, autoEvictPhoneLink: false);
            eventHub.Publish("sync.started", new { reason, autoEvictPhoneLink = false });

            var target = await deviceCatalog.SelectClassicTargetAsync(null, options.DefaultNameContains);
            if (target is null)
            {
                await StopRealtimeSessionsAsync();
                var noDeviceSnapshot = runtimeState.MarkNoDevice(reason, autoEvictPhoneLink: false);
                eventHub.Publish("sync.no_device", noDeviceSnapshot);
                return;
            }

            var evictPhoneLinkForMap = await evictionPolicy.ShouldEvictForMapAsync(target, cancellationToken);
            runtimeState.MarkSyncStarting(reason, evictPhoneLinkForMap, target);
            await fusionCoordinator.RunAsync(
                target.Id,
                $"sync:{reason}",
                ct => RunSyncOnTargetAsync(reason, target, evictPhoneLinkForMap, ct),
                cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            logger.LogError(exception, "Adit background sync failed.");
            var autoEvictPhoneLink = runtimeState.GetSnapshot().AutoEvictPhoneLink;
            var errorSnapshot = ShouldMarkDegraded()
                ? runtimeState.MarkDegraded(
                    reason,
                    autoEvictPhoneLink,
                    exception,
                    mapRealtimeSession.CurrentTarget)
                : runtimeState.MarkError(
                    reason,
                    autoEvictPhoneLink,
                    exception,
                    mapRealtimeSession.CurrentTarget);
            eventHub.Publish(
                "sync.failed",
                new
                {
                    reason,
                    error = exception.ToString(),
                    runtime = errorSnapshot
                });
        }
        finally
        {
            syncLock.Release();
        }
    }

    private async Task RunSyncOnTargetAsync(
        string reason,
        BluetoothEndpointRecord target,
        bool evictPhoneLinkForMap,
        CancellationToken cancellationToken)
    {
        await EnsureRealtimeSessionsAsync(target, evictPhoneLinkForMap, cancellationToken);

        var cachedContacts = await cacheStore.GetContactsAsync(target.Id, null, cancellationToken);
        var cachedMessages = await cacheStore.GetStoredMessagesAsync(target.Id, null, null, null, cancellationToken);
        var currentState = runtimeState.GetSnapshot();
        var contactsAreStale = currentState.LastContactsRefreshUtc is null
            || DateTimeOffset.UtcNow - currentState.LastContactsRefreshUtc.Value >= TimeSpan.FromMinutes(options.ContactRefreshMinutes)
            || cachedContacts.Count == 0;
        var fallbacks = new List<string>();

        IReadOnlyList<ContactRecord> contacts = cachedContacts;
        DateTimeOffset? contactsRefreshUtc = currentState.LastContactsRefreshUtc;
        if (contactsAreStale)
        {
            try
            {
                contacts = await contactsService.PullContactsAsync(
                    target,
                    evictPhoneLink: evictPhoneLinkForMap,
                    cancellationToken);
                await cacheStore.ReplaceContactsAsync(target.Id, contacts, cancellationToken);
                contactsRefreshUtc = DateTimeOffset.UtcNow;
                eventHub.Publish(
                    "contacts.updated",
                    new
                    {
                        reason,
                        target,
                        count = contacts.Count
                    });
            }
            catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning(exception, "PBAP contact refresh failed for {TargetName}. Using cached contacts.", target.Name);
                fallbacks.Add("contacts_cache");
                eventHub.Publish(
                    "contacts.refresh_failed",
                    new
                    {
                        reason,
                        target,
                        error = exception.ToString(),
                        cachedCount = cachedContacts.Count
                    });
                contacts = cachedContacts;
            }
        }

        MessageSyncSnapshot? liveSnapshot = null;
        try
        {
            liveSnapshot = await PullSnapshotAsync(target, evictPhoneLinkForMap, cancellationToken);
            await cacheStore.AppendMapSnapshotObservationsAsync(
                target.Id,
                mapRealtimeSession.CurrentSessionId,
                liveSnapshot.Messages,
                cancellationToken);
        }
        catch (Exception exception) when (!cancellationToken.IsCancellationRequested && cachedMessages.Count > 0)
        {
            logger.LogWarning(exception, "MAP snapshot failed for {TargetName}. Using cached messages.", target.Name);
            fallbacks.Add("messages_cache");
            eventHub.Publish(
                "messages.snapshot_failed",
                new
                {
                    reason,
                    target,
                    error = exception.ToString(),
                    cachedCount = cachedMessages.Count
                });
        }

        if (liveSnapshot is null && cachedMessages.Count == 0)
        {
            throw new InvalidOperationException("MAP snapshot failed and no cached messages were available.");
        }

        var completedSendIntentMessages = await cacheStore.GetCompletedSendIntentMessagesAsync(
            target.Id,
            options.MessageCacheLimit,
            cancellationToken);
        var cachedSourceMessages = cachedMessages
            .Select(message => message.Message)
            .Where(IsSyncSourceMessage)
            .ToArray();
        var mergedMessages = liveSnapshot is null
            ? MergeMessages(
                cachedSourceMessages,
                completedSendIntentMessages,
                options.MessageCacheLimit)
            : MergeMessages(
                cachedSourceMessages,
                liveSnapshot.Messages.Concat(completedSendIntentMessages),
                options.MessageCacheLimit);
        var messageNotifications = await GetMessagesNotificationsAsync(target.Id, cancellationToken);
        var synthesis = conversationSynthesizer.Synthesize(mergedMessages, contacts, messageNotifications);
        synthesis = await learnedThreadReranker.TryRerankAsync(synthesis, messageNotifications, cancellationToken);

        await cacheStore.ReplaceMessageSnapshotAsync(target.Id, synthesis, cancellationToken);
        var notificationCount = await cacheStore.CountActiveNotificationsAsync(target.Id, cancellationToken);

        var readySnapshot = runtimeState.MarkReady(
            reason,
            evictPhoneLinkForMap,
            target,
            contacts.Count,
            synthesis.Messages.Count,
            synthesis.Conversations.Count,
            contactsRefreshUtc);
        readySnapshot = runtimeState.UpdateNotificationCount(notificationCount);

        eventHub.Publish(
                "sync.completed",
                new
                {
                    reason,
                    target,
                    folders = liveSnapshot?.Folders ?? Array.Empty<string>(),
                    contactCount = contacts.Count,
                    messageCount = synthesis.Messages.Count,
                    notificationCount,
                    conversationCount = synthesis.Conversations.Count,
                    fallbacks
                });
        eventHub.Publish("runtime.updated", readySnapshot);
    }

    private async Task<MessageSyncSnapshot> PullSnapshotAsync(
        BluetoothEndpointRecord target,
        bool evictPhoneLinkForMap,
        CancellationToken cancellationToken)
    {
        try
        {
            if (mapRealtimeSession.CurrentTarget is not null
                && string.Equals(mapRealtimeSession.CurrentTarget.Id, target.Id, StringComparison.OrdinalIgnoreCase))
            {
                return await mapRealtimeSession.PullSnapshotAsync(options.MessageFetchLimit, cancellationToken);
            }
        }
        catch (Exception exception)
        {
            logger.LogDebug(exception, "MAP realtime snapshot failed, falling back to one-shot MAP snapshot.");
        }

        return await mapMessagingService.PullSnapshotAsync(
            target,
            options.MessageFetchLimit,
            evictPhoneLinkForMap,
            cancellationToken);
    }

    private async Task EnsureRealtimeSessionsAsync(
        BluetoothEndpointRecord target,
        bool evictPhoneLinkForMap,
        CancellationToken cancellationToken)
    {
        try
        {
            await mapRealtimeSession.StartAsync(target, evictPhoneLinkForMap, cancellationToken);
        }
        catch (Exception exception)
        {
            logger.LogDebug(exception, "MAP realtime startup failed.");
            eventHub.Publish(
                "map.start_failed",
                new
                {
                    target,
                    error = exception.ToString()
                });
        }

        if (!notificationsEnabled)
        {
            if (ancsSession.CurrentTarget is not null || ancsSession.CurrentPhase != DeviceSessionPhase.Disconnected)
            {
                await ancsSession.StopAsync();
            }

            return;
        }

        try
        {
            var leTarget = await ResolveLeTargetForAncsAsync(target, cancellationToken);
            using var ancsTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            ancsTimeout.CancelAfter(AncsStartupTimeout);
            await ancsSession.StartAsync(leTarget, ancsTimeout.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            logger.LogDebug("ANCS startup timed out for {TargetName}.", target.Name);
            eventHub.Publish(
                "ancs.start_timeout",
                new
                {
                    target,
                    timeoutSeconds = AncsStartupTimeout.TotalSeconds
                });
        }
        catch (Exception exception)
        {
            logger.LogDebug(exception, "ANCS startup failed.");
            eventHub.Publish(
                "ancs.start_failed",
                new
                {
                    target,
                    error = exception.ToString()
                });
        }
    }

    private async Task StopRealtimeSessionsAsync()
    {
        try
        {
            await mapRealtimeSession.StopAsync();
        }
        catch (Exception exception)
        {
            logger.LogDebug(exception, "MAP realtime stop failed.");
        }

        try
        {
            await ancsSession.StopAsync();
        }
        catch (Exception exception)
        {
            logger.LogDebug(exception, "ANCS stop failed.");
        }
    }

    private void OnMapRealtimeEvent(MapRealtimeEventRecord realtimeEvent)
    {
        fusionCoordinator.Post(
            realtimeEvent.DeviceId,
            "map_realtime_event",
            ct => HandleMapRealtimeEventAsync(realtimeEvent, ct));
    }

    private void OnNotificationReceived(NotificationRecord notification)
    {
        var currentTarget = runtimeState.GetSnapshot().Target;
        if (currentTarget is null)
        {
            return;
        }

        fusionCoordinator.Post(
            currentTarget.Id,
            "ancs_notification_received",
            ct => HandleNotificationReceivedAsync(currentTarget, notification, ct));
    }

    private void OnNotificationRemoved(uint notificationUid)
    {
        var currentTarget = runtimeState.GetSnapshot().Target;
        if (currentTarget is null)
        {
            return;
        }

        fusionCoordinator.Post(
            currentTarget.Id,
            "ancs_notification_removed",
            ct => HandleNotificationRemovedAsync(currentTarget, notificationUid, ct));
    }

    private void OnTransportStateChanged(SessionStateChangedRecord state)
    {
        var snapshot = runtimeState.UpdateTransportState(state);
        eventHub.Publish("transport.state", state);
        eventHub.Publish("runtime.updated", snapshot);

        if ((state.Phase == DeviceSessionPhase.Disconnected || state.Phase == DeviceSessionPhase.Faulted)
            && ShouldRequestRecovery(state.Transport))
        {
            if (string.Equals(state.Transport, "ancs", StringComparison.OrdinalIgnoreCase))
            {
                if (!notificationsEnabled)
                {
                    return;
                }

                QueueBackgroundWork(RestartAncsAsync, "ancs_recovery");
                return;
            }

            RequestSync($"{state.Transport}_recovery");
        }
    }

    private async Task HandleMapRealtimeEventAsync(MapRealtimeEventRecord realtimeEvent, CancellationToken cancellationToken)
    {
        eventHub.Publish("map.event", realtimeEvent);

        var currentTarget = runtimeState.GetSnapshot().Target;
        if (currentTarget is null
            || !string.Equals(currentTarget.Id, realtimeEvent.DeviceId, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        await cacheStore.AppendMapRealtimeObservationAsync(
            currentTarget.Id,
            mapRealtimeSession.CurrentSessionId,
            realtimeEvent,
            cancellationToken);

        var contacts = await cacheStore.GetContactsAsync(currentTarget.Id, null, cancellationToken);
        var cachedMessages = await cacheStore.GetStoredMessagesAsync(
            currentTarget.Id,
            null,
            null,
            null,
            cancellationToken);
        var mergedMessages = ApplyRealtimeEvent(
            cachedMessages
                .Select(message => message.Message)
                .Where(IsSyncSourceMessage),
            realtimeEvent,
            options.MessageCacheLimit);
        var messageNotifications = await GetMessagesNotificationsAsync(currentTarget.Id, cancellationToken);
        var synthesis = conversationSynthesizer.Synthesize(mergedMessages, contacts, messageNotifications);
        synthesis = await learnedThreadReranker.TryRerankAsync(synthesis, messageNotifications, cancellationToken);

        await cacheStore.ReplaceMessageSnapshotAsync(currentTarget.Id, synthesis, cancellationToken);

        var snapshot = runtimeState.MarkReady(
            $"map:{realtimeEvent.EventType}",
            runtimeState.GetSnapshot().AutoEvictPhoneLink,
            currentTarget,
            contacts.Count,
            synthesis.Messages.Count,
            synthesis.Conversations.Count,
            runtimeState.GetSnapshot().LastContactsRefreshUtc);

        eventHub.Publish(
            "messages.updated",
            new
            {
                target = currentTarget,
                source = "map_realtime",
                realtimeEvent,
                messageCount = synthesis.Messages.Count,
                conversationCount = synthesis.Conversations.Count
            });
        eventHub.Publish("runtime.updated", snapshot);
    }

    private async Task HandleNotificationReceivedAsync(
        BluetoothEndpointRecord currentTarget,
        NotificationRecord notification,
        CancellationToken cancellationToken)
    {
        await cacheStore.AppendAncsObservationAsync(
            currentTarget.Id,
            ancsSession.CurrentSessionId,
            notification,
            cancellationToken);
        await cacheStore.UpsertNotificationAsync(currentTarget.Id, notification, cancellationToken);
        var notificationCount = await cacheStore.CountActiveNotificationsAsync(currentTarget.Id, cancellationToken);
        var snapshot = runtimeState.UpdateNotificationCount(notificationCount);
        eventHub.Publish("runtime.updated", snapshot);

        eventHub.Publish(
            "notification.received",
            new
            {
                target = currentTarget,
                notification
            });

        if (IsMessagesNotification(notification))
        {
            RequestSync("ancs_messages_notification");
        }
    }

    private async Task HandleNotificationRemovedAsync(
        BluetoothEndpointRecord currentTarget,
        uint notificationUid,
        CancellationToken cancellationToken)
    {
        await cacheStore.MarkAncsObservationRemovedAsync(
            currentTarget.Id,
            ancsSession.CurrentSessionId,
            notificationUid,
            cancellationToken);
        await cacheStore.MarkNotificationRemovedAsync(currentTarget.Id, notificationUid, cancellationToken);
        var notificationCount = await cacheStore.CountActiveNotificationsAsync(currentTarget.Id, cancellationToken);
        var snapshot = runtimeState.UpdateNotificationCount(notificationCount);
        eventHub.Publish("runtime.updated", snapshot);

        eventHub.Publish(
            "notification.removed",
            new
            {
                target = currentTarget,
                notificationUid
            });
    }

    private void QueueBackgroundWork(Func<Task> work, string operationName)
    {
        _ = Task.Run(
            async () =>
            {
                try
                {
                    await work();
                }
                catch (Exception exception)
                {
                    logger.LogDebug(exception, "Background operation failed: {OperationName}", operationName);
                }
            },
            CancellationToken.None);
    }

    private bool ShouldRequestRecovery(string transport)
    {
        lock (recoveryGate)
        {
            var now = DateTimeOffset.UtcNow;
            if (lastRecoveryRequestUtcByTransport.TryGetValue(transport, out var lastRequestedUtc)
                && now - lastRequestedUtc < RecoveryRequestThrottle)
            {
                return false;
            }

            lastRecoveryRequestUtcByTransport[transport] = now;
            return true;
        }
    }

    private bool ShouldMarkDegraded()
    {
        var snapshot = runtimeState.GetSnapshot();
        return snapshot.NotificationCount > 0
            || snapshot.MessageCount > 0
            || snapshot.ContactCount > 0
            || snapshot.MapSession?.Phase == DeviceSessionPhase.Connected
            || snapshot.AncsSession?.Phase == DeviceSessionPhase.Connected;
    }

    private static bool IsMessagesNotification(NotificationRecord notification)
    {
        return string.Equals(notification.AppIdentifier, "com.apple.MobileSMS", StringComparison.OrdinalIgnoreCase)
            || string.Equals(notification.AppIdentifier, "com.apple.MobileSMS.notification", StringComparison.OrdinalIgnoreCase);
    }

    private async Task<IReadOnlyList<StoredNotificationRecord>> GetMessagesNotificationsAsync(
        string deviceId,
        CancellationToken cancellationToken)
    {
        var primary = await cacheStore.GetNotificationsAsync(
            deviceId,
            false,
            "com.apple.MobileSMS",
            256,
            cancellationToken);
        var secondary = await cacheStore.GetNotificationsAsync(
            deviceId,
            false,
            "com.apple.MobileSMS.notification",
            256,
            cancellationToken);

        return primary
            .Concat(secondary)
            .GroupBy(
                notification => string.Join(
                    "|",
                    notification.Notification.AppIdentifier ?? string.Empty,
                    notification.Notification.NotificationUid.ToString(CultureInfo.InvariantCulture),
                    notification.Notification.ReceivedAtUtc.ToString("O", CultureInfo.InvariantCulture),
                    notification.Notification.Title ?? string.Empty,
                    notification.Notification.Message ?? string.Empty),
                StringComparer.Ordinal)
            .Select(group => group
                .OrderByDescending(notification => notification.UpdatedAtUtc)
                .First())
            .OrderByDescending(notification => notification.UpdatedAtUtc)
            .Take(256)
            .ToArray();
    }

    private async Task RestartAncsAsync()
    {
        var target = runtimeState.GetSnapshot().Target;
        if (target is null)
        {
            return;
        }

        try
        {
            using var timeout = new CancellationTokenSource(AncsStartupTimeout);
            var leTarget = await ResolveLeTargetForAncsAsync(target, timeout.Token, recovery: true);
            await ancsSession.StartAsync(leTarget, timeout.Token);
            eventHub.Publish(
                "ancs.restarted",
                new
                {
                    target,
                    leTarget
                });
        }
        catch (OperationCanceledException)
        {
            eventHub.Publish(
                "ancs.start_timeout",
                new
                {
                    target,
                    timeoutSeconds = AncsStartupTimeout.TotalSeconds
                });
        }
        catch (Exception exception)
        {
            logger.LogDebug(exception, "ANCS restart failed.");
            eventHub.Publish(
                "ancs.start_failed",
                new
                {
                    target,
                    error = exception.ToString()
                });
        }
    }

    private async Task<BluetoothLeDeviceRecord> ResolveLeTargetForAncsAsync(
        BluetoothEndpointRecord target,
        CancellationToken cancellationToken,
        bool recovery = false)
    {
        var leTarget = await deviceCatalog.SelectLeTargetAsync(null, target.ContainerId, target.Name);
        if (leTarget is not null && IsAuthoritativeTrustedLeTarget(leTarget))
        {
            await cacheStore.UpsertTrustedLeDeviceAsync(target, leTarget, cancellationToken);
            return leTarget;
        }

        var trustedLe = await cacheStore.GetTrustedLeDeviceAsync(target.Id, target.ContainerId, cancellationToken);
        BluetoothLeDeviceRecord? trustedFallback = null;
        if (trustedLe is { } trusted)
        {
            leTarget = await deviceCatalog.SelectLeTargetAsync(trusted.DeviceId, target.ContainerId, target.Name);
            if (leTarget is not null)
            {
                if (IsAuthoritativeTrustedLeTarget(leTarget))
                {
                    await cacheStore.UpsertTrustedLeDeviceAsync(target, leTarget, cancellationToken);
                }

                eventHub.Publish(
                    "ancs.target_trusted",
                    new
                    {
                        target,
                        leTarget,
                        detail = "Recovered LE target from the daemon's trusted LE mapping."
                    });
                return leTarget;
            }

            trustedFallback = new BluetoothLeDeviceRecord(
                trusted.DeviceId,
                string.IsNullOrWhiteSpace(target.Name) ? "(unnamed)" : target.Name,
                true,
                trusted.Address,
                null,
                null,
                target.ContainerId);
        }

        eventHub.Publish(
            "ancs.target_waiting",
            new
            {
                target,
                detail = recovery
                    ? "No paired LE endpoint matched during ANCS recovery. Waiting for Windows to materialize the paired LE endpoint."
                    : "No paired LE endpoint matched the classic endpoint. Waiting for Windows to materialize the paired LE endpoint."
            });

        leTarget = await deviceCatalog.WaitForPairedLeTargetAsync(
            target.ContainerId,
            target.Name,
            cancellationToken);
        if (leTarget is not null)
        {
            await cacheStore.UpsertTrustedLeDeviceAsync(target, leTarget, cancellationToken);
            eventHub.Publish(
                "ancs.target_recovered",
                new
                {
                    target,
                    leTarget
                });
            return leTarget;
        }

        leTarget = await deviceCatalog.WaitForConnectedLeTargetAsync(
            target.ContainerId,
            target.Name,
            cancellationToken);
        if (leTarget is not null)
        {
            eventHub.Publish(
                "ancs.target_connected_volatile",
                new
                {
                    target,
                    leTarget,
                    detail = recovery
                        ? "Recovered a live connected BLE device interface for ANCS without promoting it to trusted state."
                        : "Using a live connected BLE device interface for ANCS without promoting it to trusted state."
                });
            return leTarget;
        }

        var syntheticTarget = CreateSyntheticLeTarget(target);
        if (!string.IsNullOrWhiteSpace(syntheticTarget.Address))
        {
            eventHub.Publish(
                "ancs.target_synthetic",
                new
                {
                    target,
                    syntheticTarget,
                    detail = recovery
                        ? "No LE endpoint matched during ANCS recovery. Using raw-address fallback target."
                        : "No LE endpoint matched the classic endpoint. Using raw-address fallback target."
                });
            return syntheticTarget;
        }

        if (trustedFallback is not null)
        {
            eventHub.Publish(
                "ancs.target_trusted_fallback",
                new
                {
                    target,
                    leTarget = trustedFallback,
                    detail = "Raw-address fallback was unavailable. Falling back to the daemon's stored trusted LE device id."
                });
            return trustedFallback;
        }

        throw new InvalidOperationException("Unable to resolve any ANCS LE target.");
    }

    private static BluetoothLeDeviceRecord CreateSyntheticLeTarget(BluetoothEndpointRecord target)
    {
        var suffix = !string.IsNullOrWhiteSpace(target.ContainerId)
            ? target.ContainerId
            : target.Id;
        return new BluetoothLeDeviceRecord(
            $"raw:{suffix}",
            string.IsNullOrWhiteSpace(target.Name) ? "(unnamed)" : target.Name,
            false,
            target.BluetoothAddress ?? target.AepAddress,
            target.IsConnected,
            target.IsPresent,
            target.ContainerId);
    }

    private static bool IsAuthoritativeTrustedLeTarget(BluetoothLeDeviceRecord? target)
    {
        return target is not null && target.IsPaired;
    }

    private static IReadOnlyList<MessageRecord> ApplyRealtimeEvent(
        IEnumerable<MessageRecord> cachedMessages,
        MapRealtimeEventRecord realtimeEvent,
        int limit)
    {
        var merged = cachedMessages
            .ToDictionary(
                ConversationSynthesizer.ComputeMessageKey,
                message => message,
                StringComparer.OrdinalIgnoreCase);

        if (IsDeleteEvent(realtimeEvent.EventType) && !string.IsNullOrWhiteSpace(realtimeEvent.Handle))
        {
            merged.Remove(realtimeEvent.Handle);
        }

        foreach (var message in realtimeEvent.AffectedMessages)
        {
            UpsertMergedMessage(merged, message);
        }

        if (realtimeEvent.Message is not null)
        {
            UpsertMergedMessage(merged, realtimeEvent.Message);
        }

        return merged.Values
            .OrderByDescending(message => ParseSortUtc(message) ?? DateTimeOffset.MinValue)
            .ThenByDescending(message => message.Datetime ?? string.Empty, StringComparer.OrdinalIgnoreCase)
            .Take(Math.Max(1, limit))
            .ToArray();
    }

    private static IReadOnlyList<MessageRecord> MergeMessages(
        IEnumerable<MessageRecord> cachedMessages,
        IEnumerable<MessageRecord> liveMessages,
        int limit)
    {
        var merged = new Dictionary<string, MessageRecord>(StringComparer.OrdinalIgnoreCase);

        foreach (var message in cachedMessages.Concat(liveMessages))
        {
            UpsertMergedMessage(merged, message);
        }

        return merged.Values
            .OrderByDescending(message => ParseSortUtc(message) ?? DateTimeOffset.MinValue)
            .ThenByDescending(message => message.Datetime ?? string.Empty, StringComparer.OrdinalIgnoreCase)
            .Take(Math.Max(1, limit))
            .ToArray();
    }

    private static bool IsSyncSourceMessage(MessageRecord message)
    {
        return !string.Equals(message.Folder, "notification", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(message.Type, "ANCS", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(message.MessageType, "ANCS", StringComparison.OrdinalIgnoreCase);
    }

    private static void UpsertMergedMessage(
        IDictionary<string, MessageRecord> merged,
        MessageRecord incoming)
    {
        var key = ConversationSynthesizer.ComputeMessageKey(incoming);
        if (!merged.TryGetValue(key, out var existing))
        {
            merged[key] = incoming;
            return;
        }

        merged[key] = MergeMessageRecord(existing, incoming);
    }

    private static MessageRecord MergeMessageRecord(MessageRecord existing, MessageRecord incoming)
    {
        return new MessageRecord(
            string.IsNullOrWhiteSpace(incoming.Folder) ? existing.Folder : incoming.Folder,
            incoming.Handle ?? existing.Handle,
            incoming.Type ?? existing.Type,
            incoming.Subject ?? existing.Subject,
            incoming.Datetime ?? existing.Datetime,
            incoming.SenderName ?? existing.SenderName,
            incoming.SenderAddressing ?? existing.SenderAddressing,
            incoming.RecipientAddressing ?? existing.RecipientAddressing,
            incoming.Size ?? existing.Size,
            incoming.AttachmentSize ?? existing.AttachmentSize,
            incoming.Priority ?? existing.Priority,
            incoming.Read ?? existing.Read,
            incoming.Sent ?? existing.Sent,
            incoming.Protected ?? existing.Protected,
            !string.IsNullOrWhiteSpace(incoming.Body) ? incoming.Body : existing.Body,
            incoming.MessageType ?? existing.MessageType,
            incoming.Status ?? existing.Status,
            incoming.Originators.Count > 0 ? incoming.Originators : existing.Originators,
            incoming.Recipients.Count > 0 ? incoming.Recipients : existing.Recipients);
    }

    private static bool IsDeleteEvent(string eventType)
    {
        return eventType.Contains("delete", StringComparison.OrdinalIgnoreCase)
            || eventType.Contains("removed", StringComparison.OrdinalIgnoreCase);
    }

    private static DateTimeOffset? ParseSortUtc(MessageRecord message)
    {
        return message.Datetime is not null
            && DateTimeOffset.TryParseExact(
                message.Datetime,
                "yyyyMMdd'T'HHmmss",
                null,
                System.Globalization.DateTimeStyles.AssumeLocal,
                out var parsed)
            ? parsed.ToUniversalTime()
            : null;
    }

}
