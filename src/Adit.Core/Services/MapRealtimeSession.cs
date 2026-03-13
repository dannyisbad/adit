using Adit.Core.Models;
using Adit.Core.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Internal.Bluetooth.Map;
using Microsoft.Internal.Bluetooth.Map.BMessage;
using Microsoft.Internal.Bluetooth.Map.Model;
using Microsoft.Internal.Bluetooth.Map.Request;
using System.Runtime.InteropServices;

namespace Adit.Core.Services;

public sealed class MapRealtimeSession : IAsyncDisposable
{
    private static readonly TimeSpan SocketReuseBackoff = TimeSpan.FromSeconds(2);
    private readonly SemaphoreSlim lifecycleLock = new(1, 1);
    private readonly SemaphoreSlim operationLock = new(1, 1);
    private readonly ILogger<MapRealtimeSession> logger;
    private readonly ILoggerFactory loggerFactory;
    private readonly PhoneLinkProcessController processController;

    private Microsoft.Internal.Diagnostics.Context.ITraceContext? traceContext;
    private BluetoothEndpointRecord? target;
    private MapClient? client;
    private bool disposed;
    private bool evictPhoneLink;

    public MapRealtimeSession(
        ILogger<MapRealtimeSession> logger,
        ILoggerFactory loggerFactory,
        PhoneLinkProcessController processController)
    {
        this.logger = logger;
        this.loggerFactory = loggerFactory;
        this.processController = processController;
    }

    public event Action<MapRealtimeEventRecord>? EventReceived;

    public event Action<SessionStateChangedRecord>? StateChanged;

    public BluetoothEndpointRecord? CurrentTarget => target;

    public string? CurrentSessionId { get; private set; }

    public DeviceSessionPhase CurrentPhase { get; private set; } = DeviceSessionPhase.Disconnected;

    public async Task StartAsync(
        BluetoothEndpointRecord classicTarget,
        bool evictPhoneLink,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        await lifecycleLock.WaitAsync(cancellationToken);
        try
        {
            this.evictPhoneLink = evictPhoneLink;

            if (target is not null && !string.Equals(target.Id, classicTarget.Id, StringComparison.OrdinalIgnoreCase))
            {
                await StopCoreAsync(clearTarget: true);
            }

            target = classicTarget;
            if (client is not null
                && CurrentPhase != DeviceSessionPhase.Disconnected
                && CurrentPhase != DeviceSessionPhase.Faulted)
            {
                PublishState(DeviceSessionPhase.Connected, "already_open");
                return;
            }

            await OpenCoreAsync(cancellationToken);
        }
        finally
        {
            lifecycleLock.Release();
        }
    }

    public async Task StopAsync()
    {
        if (disposed)
        {
            return;
        }

        await lifecycleLock.WaitAsync();
        try
        {
            await StopCoreAsync(clearTarget: true);
        }
        finally
        {
            lifecycleLock.Release();
        }
    }

    public async Task<IReadOnlyList<string>> ListFoldersAsync(CancellationToken cancellationToken)
    {
        return await RunWithClientAsync(
            async (currentClient, currentTraceContext) =>
            {
                await MapClientInterop.NavigateToMessagesRootAsync(currentClient, currentTraceContext, cancellationToken);
                var result = await currentClient.GetFolderListingAsync(
                    new GetFolderListingRequestParameters
                    {
                        MaxListCount = 50,
                        ListStartOffset = 0
                    },
                    currentTraceContext,
                    cancellationToken);

                return result.Body?.Folder?
                    .Select(folder => folder.Name)
                    .Where(name => !string.IsNullOrWhiteSpace(name))
                    .Concat(MapClientInterop.DefaultFolders)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToArray() ?? MapClientInterop.DefaultFolders;
            },
            cancellationToken);
    }

    public async Task<MessageFolderListing> ListMessagesAsync(
        string folderName,
        int limit,
        CancellationToken cancellationToken)
    {
        return await RunWithClientAsync(
            async (currentClient, currentTraceContext) =>
            {
                await MapClientInterop.NavigateToMessagesRootAsync(currentClient, currentTraceContext, cancellationToken);

                var listingResult = await currentClient.GetMessagesListingAsync(
                    MapClientInterop.CreateMessagesListingRequest(folderName, limit),
                    currentTraceContext,
                    cancellationToken);

                var messages = listingResult.Body ?? [];
                var details = new List<MessageRecord>(messages.Count);
                foreach (var message in messages)
                {
                    BMessage? detailMessage = null;
                    if (!string.IsNullOrWhiteSpace(message.Handle))
                    {
                        var detail = await currentClient.GetMessageAsync(
                            MapClientInterop.CreateGetMessageRequest(message.Handle),
                            currentTraceContext,
                            cancellationToken);
                        detailMessage = detail.Body;
                    }

                    details.Add(MapClientInterop.ToMessageRecord(folderName, message, detailMessage));
                }

                var typeCounts = messages
                    .Select(message => message.Type ?? "(unknown)")
                    .GroupBy(type => type, StringComparer.OrdinalIgnoreCase)
                    .ToDictionary(group => group.Key, group => group.Count(), StringComparer.OrdinalIgnoreCase);

                return new MessageFolderListing(
                    folderName,
                    messages.Count,
                    listingResult.MessagesListingSize,
                    listingResult.NewMessage,
                    listingResult.MseTime,
                    typeCounts,
                    details);
            },
            cancellationToken);
    }

    public async Task<MessageSyncSnapshot> PullSnapshotAsync(int limit, CancellationToken cancellationToken)
    {
        return await RunWithClientAsync(
            async (currentClient, currentTraceContext) =>
            {
                await MapClientInterop.NavigateToMessagesRootAsync(currentClient, currentTraceContext, cancellationToken);
                var folderListing = await currentClient.GetFolderListingAsync(
                    new GetFolderListingRequestParameters
                    {
                        MaxListCount = 50,
                        ListStartOffset = 0
                    },
                    currentTraceContext,
                    cancellationToken);

                var folders = folderListing.Body?.Folder?
                    .Select(folder => folder.Name)
                    .Where(name => !string.IsNullOrWhiteSpace(name))
                    .Concat(MapClientInterop.DefaultFolders)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToArray() ?? MapClientInterop.DefaultFolders;

                var allMessages = new List<MessageRecord>();
                foreach (var folderName in folders)
                {
                    var listingResult = await currentClient.GetMessagesListingAsync(
                        MapClientInterop.CreateMessagesListingRequest(folderName, limit),
                        currentTraceContext,
                        cancellationToken);
                    var messages = listingResult.Body ?? [];

                    foreach (var message in messages)
                    {
                        BMessage? detailMessage = null;
                        if (!string.IsNullOrWhiteSpace(message.Handle))
                        {
                            var detail = await currentClient.GetMessageAsync(
                                MapClientInterop.CreateGetMessageRequest(message.Handle),
                                currentTraceContext,
                                cancellationToken);
                            detailMessage = detail.Body;
                        }

                        allMessages.Add(MapClientInterop.ToMessageRecord(folderName, message, detailMessage));
                    }
                }

                return new MessageSyncSnapshot(folders, allMessages);
            },
            cancellationToken);
    }

    public async Task<SendMessageResult> SendMessageAsync(
        string recipient,
        string body,
        CancellationToken cancellationToken)
    {
        return await RunWithClientAsync(
            async (currentClient, currentTraceContext) =>
            {
                var result = await currentClient.PushMessageAsync(
                    MapClientInterop.CreatePushMessageRequest(recipient, body),
                    currentTraceContext,
                    cancellationToken);

                return new SendMessageResult(
                    MapClientInterop.ReadBoolProperty(result, "IsSuccess"),
                    MapClientInterop.ReadObjectProperty(result, "ResponseCode")?.ToString(),
                    MapClientInterop.ReadObjectProperty(result, "MessageHandle")?.ToString()
                        ?? MapClientInterop.ReadObjectProperty(result, "Handle")?.ToString());
            },
            cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (disposed)
        {
            return;
        }

        await StopAsync();
        disposed = true;
        lifecycleLock.Dispose();
        operationLock.Dispose();
    }

    private async Task OpenCoreAsync(CancellationToken cancellationToken)
    {
        var currentTarget = target ?? throw new InvalidOperationException("MAP target is not selected.");

        if (evictPhoneLink)
        {
            processController.Evict();
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
        }

        PublishState(DeviceSessionPhase.Connecting, "opening_client");

        for (var attempt = 1; attempt <= 2; attempt++)
        {
            var manager = new MapClientManager(
                new MapSocketProvider(),
                new MapRfcommServiceProviderFactory(),
                new MapSocketListenerProvider(),
                new MapBluetoothDeviceProvider(),
                loggerFactory);

            try
            {
                traceContext = TraceContextFactory.Create();
                var openResult = await manager.OpenAsync(currentTarget.Id, traceContext, cancellationToken);
                if (!MapClientInterop.ReadBoolProperty(openResult, "IsSuccess"))
                {
                    throw new InvalidOperationException(
                        $"MAP open failed: {MapClientInterop.ReadObjectProperty(openResult, "ResponseCode")}");
                }

                client = MapClientInterop.GetPropertyValue<MapClient>(openResult, "MapClient")
                    ?? MapClientInterop.GetPropertyValue<MapClient>(openResult, "Result")
                    ?? throw new InvalidOperationException("MAP client was not returned.");
                CurrentSessionId = CreateSessionId("map");

                client.Closed += OnClientClosed;
                client.MnsConnected += OnMnsConnected;
                client.MnsClosed += OnMnsClosed;
                client.MnsSendEventReceived += OnMnsSendEventReceived;

                await InitializeRealtimeAsync(cancellationToken);
                PublishState(DeviceSessionPhase.Connected, "client_ready");
                return;
            }
            catch (Exception exception) when (attempt < 2 && ShouldRetryOpen(exception))
            {
                logger.LogDebug(exception, "Retrying MAP open after socket reuse failure.");
                await StopCoreAsync(clearTarget: false, publishState: false);
                PublishState(DeviceSessionPhase.Connecting, "open_retry", exception.Message);
                await Task.Delay(SocketReuseBackoff, cancellationToken);
            }
            catch (Exception exception)
            {
                PublishState(DeviceSessionPhase.Faulted, "open_failed", exception.Message);
                await StopCoreAsync(clearTarget: false, publishState: false);
                throw;
            }
        }
    }

    private async Task InitializeRealtimeAsync(CancellationToken cancellationToken)
    {
        await RunWithOpenClientAsync(
            async (currentClient, currentTraceContext) =>
            {
                var registerResult = await currentClient.SetNotificationRegistrationAsync(
                    new SetNotificationRegistrationRequestParameters
                    {
                        EnableNotifications = true
                    },
                    currentTraceContext,
                    cancellationToken);

                if (!MapClientInterop.ReadBoolProperty(registerResult, "IsSuccess"))
                {
                    logger.LogWarning(
                        "MAP notification registration returned non-success: {ResponseCode}",
                        MapClientInterop.ReadObjectProperty(registerResult, "ResponseCode"));
                }

                await MapClientInterop.NavigateToMessagesRootAsync(currentClient, currentTraceContext, cancellationToken);
                _ = await currentClient.UpdateInboxAsync(currentTraceContext, cancellationToken);
                return true;
            },
            cancellationToken);
    }

    private async Task<T> RunWithClientAsync<T>(
        Func<MapClient, Microsoft.Internal.Diagnostics.Context.ITraceContext, Task<T>> action,
        CancellationToken cancellationToken)
    {
        await EnsureConnectedAsync(cancellationToken);
        return await RunWithOpenClientAsync(action, cancellationToken);
    }

    private async Task<T> RunWithOpenClientAsync<T>(
        Func<MapClient, Microsoft.Internal.Diagnostics.Context.ITraceContext, Task<T>> action,
        CancellationToken cancellationToken)
    {
        await operationLock.WaitAsync(cancellationToken);
        try
        {
            var currentClient = client ?? throw new InvalidOperationException("MAP client is not connected.");
            var currentTraceContext = traceContext ?? throw new InvalidOperationException("MAP trace context is unavailable.");
            return await action(currentClient, currentTraceContext);
        }
        catch (Exception exception)
        {
            PublishState(DeviceSessionPhase.Faulted, "operation_failed", exception.Message);
            throw;
        }
        finally
        {
            operationLock.Release();
        }
    }

    private async Task EnsureConnectedAsync(CancellationToken cancellationToken)
    {
        if (client is not null)
        {
            return;
        }

        await lifecycleLock.WaitAsync(cancellationToken);
        try
        {
            if (client is null)
            {
                if (target is null)
                {
                    throw new InvalidOperationException("MAP target is not selected.");
                }

                await OpenCoreAsync(cancellationToken);
            }
        }
        finally
        {
            lifecycleLock.Release();
        }
    }

    private async Task StopCoreAsync(bool clearTarget, bool publishState = true, string detail = "stopped")
    {
        var currentClient = client;
        if (currentClient is not null)
        {
            currentClient.Closed -= OnClientClosed;
            currentClient.MnsConnected -= OnMnsConnected;
            currentClient.MnsClosed -= OnMnsClosed;
            currentClient.MnsSendEventReceived -= OnMnsSendEventReceived;
            currentClient.Dispose();
            client = null;
        }

        traceContext = null;
        CurrentSessionId = null;
        if (clearTarget)
        {
            target = null;
        }

        if (publishState)
        {
            PublishState(DeviceSessionPhase.Disconnected, clearTarget ? detail : "client_disposed");
        }

        await Task.CompletedTask;
    }

    private void OnClientClosed(object? sender, MapClientClosedEventArgs args)
    {
        _ = Task.Run(
            async () =>
            {
                try
                {
                    await lifecycleLock.WaitAsync();
                    try
                    {
                        await StopCoreAsync(clearTarget: false, publishState: true, detail: "client_closed");
                    }
                    finally
                    {
                        lifecycleLock.Release();
                    }
                }
                catch (Exception exception)
                {
                    logger.LogDebug(exception, "MAP client close cleanup failed.");
                }
            },
            CancellationToken.None);
    }

    private void OnMnsConnected(object? sender, MapMnsConnectionStateChangeEventArgs args)
    {
        PublishState(DeviceSessionPhase.Connected, "mns_connected");
    }

    private void OnMnsClosed(object? sender, MapMnsConnectionStateChangeEventArgs args)
    {
        PublishState(DeviceSessionPhase.Connecting, "mns_closed");
    }

    private void OnMnsSendEventReceived(object? sender, SendEventReceivedEventArgs args)
    {
        if (args.Event is null)
        {
            return;
        }

        _ = Task.Run(
            async () =>
            {
                try
                {
                    var realtimeEvent = await FollowUpMnsEventAsync(args.Event, CancellationToken.None);
                    EventReceived?.Invoke(realtimeEvent);
                }
                catch (Exception exception)
                {
                    logger.LogDebug(exception, "MAP realtime follow-up failed.");
                    PublishState(DeviceSessionPhase.Faulted, "mns_followup_failed", exception.Message);
                }
            },
            CancellationToken.None);
    }

    private async Task<MapRealtimeEventRecord> FollowUpMnsEventAsync(
        MapEventReport report,
        CancellationToken cancellationToken)
    {
        await Task.Delay(TimeSpan.FromMilliseconds(750), cancellationToken);

        var currentTarget = target ?? throw new InvalidOperationException("MAP target is not selected.");
        var affectedFolders = new[] { report.Folder, report.OldFolder }
            .Select(MapClientInterop.ExtractRelativeFolderName)
            .Where(folderName => !string.IsNullOrWhiteSpace(folderName))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray()!;

        return await RunWithClientAsync(
            async (currentClient, currentTraceContext) =>
            {
                await MapClientInterop.NavigateToMessagesRootAsync(currentClient, currentTraceContext, cancellationToken);

                BMessage? detailMessage = null;
                if (!string.IsNullOrWhiteSpace(report.Handle))
                {
                    var detailResult = await currentClient.GetMessageAsync(
                        MapClientInterop.CreateGetMessageRequest(report.Handle),
                        currentTraceContext,
                        cancellationToken);
                    detailMessage = detailResult.Body;
                }

                var affectedMessages = new List<MessageRecord>();
                MessageRecord? eventMessage = null;

                foreach (var folderName in affectedFolders)
                {
                    var listingResult = await currentClient.GetMessagesListingAsync(
                        MapClientInterop.CreateMessagesListingRequest(folderName!, 10),
                        currentTraceContext,
                        cancellationToken);

                    foreach (var entry in listingResult.Body ?? [])
                    {
                        var detail = !string.IsNullOrWhiteSpace(report.Handle)
                            && string.Equals(entry.Handle, report.Handle, StringComparison.OrdinalIgnoreCase)
                            ? detailMessage
                            : null;
                        var mapped = MapClientInterop.ToMessageRecord(folderName!, entry, detail);
                        affectedMessages.Add(mapped);

                        if (!string.IsNullOrWhiteSpace(report.Handle)
                            && string.Equals(mapped.Handle, report.Handle, StringComparison.OrdinalIgnoreCase))
                        {
                            eventMessage = mapped;
                        }
                    }
                }

                if (eventMessage is null
                    && detailMessage is not null
                    && !string.IsNullOrWhiteSpace(report.Handle))
                {
                    var fallbackFolder = MapClientInterop.ExtractRelativeFolderName(report.Folder)
                        ?? MapClientInterop.ExtractRelativeFolderName(report.OldFolder)
                        ?? "inbox";
                    eventMessage = MapClientInterop.ToMessageRecord(
                        fallbackFolder,
                        report.Handle,
                        report.MessageType.ToString(),
                        detailMessage);
                    affectedMessages.Add(eventMessage);
                }

                var dedupedMessages = affectedMessages
                    .GroupBy(message => ConversationSynthesizer.ComputeMessageKey(message), StringComparer.OrdinalIgnoreCase)
                    .Select(group => group.OrderByDescending(message => !string.IsNullOrWhiteSpace(message.Body)).First())
                    .ToArray();

                return new MapRealtimeEventRecord(
                    currentTarget.Id,
                    currentTarget.Name,
                    DateTimeOffset.UtcNow,
                    report.Type.ToString(),
                    report.Handle,
                    MapClientInterop.ExtractRelativeFolderName(report.Folder),
                    MapClientInterop.ExtractRelativeFolderName(report.OldFolder),
                    report.MessageType.ToString(),
                    eventMessage,
                    dedupedMessages);
            },
            cancellationToken);
    }

    private void PublishState(DeviceSessionPhase phase, string detail, string? error = null)
    {
        CurrentPhase = phase;
        StateChanged?.Invoke(
            new SessionStateChangedRecord(
                "map",
                phase,
                DateTimeOffset.UtcNow,
                detail,
                error));
    }

    private static bool ShouldRetryOpen(Exception exception)
    {
        return exception switch
        {
            COMException comException when comException.HResult == unchecked((int)0x80072740) => true,
            _ when exception.Message.Contains(
                "Only one usage of each socket address",
                StringComparison.OrdinalIgnoreCase) => true,
            _ when exception.InnerException is not null => ShouldRetryOpen(exception.InnerException),
            _ => false
        };
    }

    private static string CreateSessionId(string prefix)
    {
        return $"{prefix}_{Guid.NewGuid():N}";
    }
}
