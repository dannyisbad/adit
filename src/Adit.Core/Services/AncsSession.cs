using Adit.Core.Ancs;
using Adit.Core.Models;
using Microsoft.Extensions.Logging;
using Windows.Devices.Bluetooth;
using Windows.Devices.Bluetooth.GenericAttributeProfile;
using Windows.Foundation;
using Windows.Security.Cryptography;
using Windows.Storage.Streams;

namespace Adit.Core.Services;

public sealed class AncsSession : IAsyncDisposable
{
    private static readonly TimeSpan WinRtOperationTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan GattSessionActivationTimeout = TimeSpan.FromSeconds(15);

    private readonly object pendingGate = new();
    private readonly SemaphoreSlim lifecycleLock = new(1, 1);
    private readonly SemaphoreSlim refreshLock = new(1, 1);
    private readonly SemaphoreSlim notificationAttributesLock = new(1, 1);
    private readonly ILogger<AncsSession> logger;
    private readonly AppleBleAddressResolver bleAddressResolver;

    private DateTimeOffset lastRefreshRequestUtc = DateTimeOffset.MinValue;
    private BluetoothLEDevice? device;
    private GattSession? deviceSession;
    private BluetoothLeDeviceRecord? target;
    private GattCharacteristic? controlPoint;
    private GattCharacteristic? dataSource;
    private PendingNotificationAttributesRequest? pendingAttributesRequest;
    private GattCharacteristic? notificationSource;
    private GattDeviceService? service;
    private bool disposed;
    private bool notifySubscriptionsArmed;

    public AncsSession(
        ILogger<AncsSession> logger,
        AppleBleAddressResolver bleAddressResolver)
    {
        this.logger = logger;
        this.bleAddressResolver = bleAddressResolver;
    }

    public event Action<NotificationRecord>? NotificationReceived;

    public event Action<uint>? NotificationRemoved;

    public event Action<SessionStateChangedRecord>? StateChanged;

    public BluetoothLeDeviceRecord? CurrentTarget => target;

    public string? CurrentSessionId { get; private set; }

    public DeviceSessionPhase CurrentPhase { get; private set; } = DeviceSessionPhase.Disconnected;

    public async Task StartAsync(BluetoothLeDeviceRecord leTarget, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        await lifecycleLock.WaitAsync(cancellationToken);
        try
        {
            var targetChanged = target is null
                || !string.Equals(target.Id, leTarget.Id, StringComparison.OrdinalIgnoreCase);
            if (targetChanged && target is not null)
            {
                await StopCoreAsync();
            }

            target = leTarget;
            if (!targetChanged
                && notifySubscriptionsArmed
                && device is not null
                && deviceSession?.SessionStatus == GattSessionStatus.Active)
            {
                return;
            }

            if (device is null)
            {
                var openedById = false;
                if (CanOpenDeviceById(leTarget.Id))
                {
                    openedById = await OpenDeviceByIdAsync(leTarget.Id, cancellationToken);
                }

                if (!openedById)
                {
                    await TryFallbackToResolvedAddressAsync(leTarget, cancellationToken, force: true);
                }
            }

            if (device is not null)
            {
                var gattSessionReady = await EnsureGattSessionAsync(cancellationToken);
                if (gattSessionReady)
                {
                    await RefreshBindingsAsync(cancellationToken);
                }

                if (!gattSessionReady)
                {
                    await TryFallbackToResolvedAddressAsync(leTarget, cancellationToken, force: true);
                }
            }
        }
        finally
        {
            lifecycleLock.Release();
        }
    }

    private static bool CanOpenDeviceById(string deviceId)
    {
        return !string.IsNullOrWhiteSpace(deviceId)
            && !deviceId.StartsWith("raw:", StringComparison.OrdinalIgnoreCase);
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
            await StopCoreAsync();
        }
        finally
        {
            lifecycleLock.Release();
        }
    }

    public async Task<bool> PerformActionAsync(
        uint notificationUid,
        NotificationAction action,
        CancellationToken cancellationToken)
    {
        var currentControlPoint = controlPoint;
        if (currentControlPoint is null)
        {
            return false;
        }

        var request = AncsProtocol.BuildPerformNotificationActionCommand(notificationUid, action);
        currentControlPoint.ProtectionLevel = GattProtectionLevel.EncryptionAndAuthenticationRequired;
        var status = await currentControlPoint.WriteValueAsync(
            CryptographicBuffer.CreateFromByteArray(request),
            GattWriteOption.WriteWithResponse).AsTask(cancellationToken);

        return status == GattCommunicationStatus.Success;
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
        refreshLock.Dispose();
        notificationAttributesLock.Dispose();
    }

    private async Task StopCoreAsync()
    {
        DetachBindings();
        DisposeActiveTransport();

        CurrentSessionId = null;
        target = null;
        PublishState(DeviceSessionPhase.Disconnected, "stopped");
        await Task.CompletedTask;
    }

    private async Task<bool> OpenDeviceByIdAsync(string deviceId, CancellationToken cancellationToken)
    {
        PublishState(DeviceSessionPhase.Connecting, "opening_le_device");
        var openedDevice = await WaitAsync(BluetoothLEDevice.FromIdAsync(deviceId), WinRtOperationTimeout, cancellationToken);
        if (openedDevice is null)
        {
            PublishState(DeviceSessionPhase.Faulted, "device_open_failed", "BluetoothLEDevice.FromIdAsync returned null.");
            return false;
        }

        AttachDevice(openedDevice);
        return true;
    }

    private async Task TryFallbackToResolvedAddressAsync(
        BluetoothLeDeviceRecord leTarget,
        CancellationToken cancellationToken,
        bool force)
    {
        if (!force && (CurrentPhase == DeviceSessionPhase.Connected || device is not null))
        {
            return;
        }

        var candidates = await bleAddressResolver.ResolveCandidatesAsync(
            leTarget.Name,
            TimeSpan.FromSeconds(6),
            cancellationToken,
            5);
        if (candidates.Count == 0)
        {
            PublishState(DeviceSessionPhase.Faulted, "raw_candidates_missing");
            return;
        }

        foreach (var candidate in candidates)
        {
            if (await TryResolvedAddressCandidateAsync(leTarget, candidate, cancellationToken))
            {
                return;
            }
        }

        PublishState(DeviceSessionPhase.Faulted, "raw_candidates_exhausted");
    }

    private async Task<bool> TryResolvedAddressCandidateAsync(
        BluetoothLeDeviceRecord leTarget,
        AppleBleAddressCandidate candidate,
        CancellationToken cancellationToken)
    {
        DetachBindings();
        DisposeActiveTransport();

        PublishState(
            DeviceSessionPhase.Connecting,
            $"raw_address_candidate:{FormatBluetoothAddress(candidate.Address)}",
            $"score={candidate.Score}; count={candidate.Count}; connectable={candidate.IsConnectable}; names={string.Join(",", candidate.LocalNames)}");

        var openedDevice = await WaitAsync(
            BluetoothLEDevice.FromBluetoothAddressAsync(candidate.Address),
            WinRtOperationTimeout,
            cancellationToken);
        if (openedDevice is null)
        {
            PublishState(
                DeviceSessionPhase.Connecting,
                "raw_candidate_open_failed",
                FormatBluetoothAddress(candidate.Address));
            return false;
        }

        var promotedTarget = leTarget with
        {
            Id = string.IsNullOrWhiteSpace(openedDevice.DeviceId) ? leTarget.Id : openedDevice.DeviceId,
            Name = string.IsNullOrWhiteSpace(openedDevice.Name) ? leTarget.Name : openedDevice.Name,
            IsConnected = openedDevice.ConnectionStatus == BluetoothConnectionStatus.Connected
        };

        BluetoothLEDevice activeDevice = openedDevice;
        if (CanOpenDeviceById(promotedTarget.Id)
            && !string.Equals(promotedTarget.Id, leTarget.Id, StringComparison.OrdinalIgnoreCase))
        {
            PublishState(DeviceSessionPhase.Connecting, $"resolved_device_id:{promotedTarget.Id}");

            var reopenedDevice = await WaitAsync(
                BluetoothLEDevice.FromIdAsync(promotedTarget.Id),
                WinRtOperationTimeout,
                cancellationToken);
            if (reopenedDevice is not null)
            {
                openedDevice.Dispose();
                activeDevice = reopenedDevice;
            }
        }

        if (activeDevice.ConnectionStatus != BluetoothConnectionStatus.Connected)
        {
            PublishState(
                DeviceSessionPhase.Connecting,
                "raw_candidate_disconnected",
                promotedTarget.Id);
            activeDevice.Dispose();
            return false;
        }

        target = promotedTarget;
        AttachDevice(activeDevice);
        if (!await EnsureGattSessionAsync(cancellationToken))
        {
            DetachBindings();
            DisposeActiveTransport();
            return false;
        }

        await RefreshBindingsAsync(cancellationToken);
        if (notifySubscriptionsArmed)
        {
            return true;
        }

        DetachBindings();
        DisposeActiveTransport();
        return false;
    }

    private void AttachDevice(BluetoothLEDevice openedDevice)
    {
        device = openedDevice;
        device.ConnectionStatusChanged += OnConnectionStatusChanged;
        device.GattServicesChanged += OnGattServicesChanged;
    }

    private void DisposeActiveTransport()
    {
        if (device is not null)
        {
            device.ConnectionStatusChanged -= OnConnectionStatusChanged;
            device.GattServicesChanged -= OnGattServicesChanged;
            device.Dispose();
            device = null;
        }

        DisposeGattSession();
    }

    private async Task RefreshBindingsAsync(CancellationToken cancellationToken)
    {
        if (device is null)
        {
            return;
        }

        await refreshLock.WaitAsync(cancellationToken);
        try
        {
            DetachCharacteristics();
            PublishState(DeviceSessionPhase.Connecting, "refresh_bindings");

            if (deviceSession is null || deviceSession.SessionStatus != GattSessionStatus.Active)
            {
                notifySubscriptionsArmed = false;
                PublishState(
                    DeviceSessionPhase.Faulted,
                    "gatt_session_inactive",
                    deviceSession?.SessionStatus.ToString() ?? "null");
                return;
            }

            service = await ResolveServiceByUuidAsync(device, AncsUuids.Service);
            if (service is null)
            {
                notifySubscriptionsArmed = false;
                PublishState(DeviceSessionPhase.Faulted, "ancs_service_missing");
                return;
            }

            if (!await EnsureServiceOpenAsync(cancellationToken))
            {
                notifySubscriptionsArmed = false;
                return;
            }

            var dataSourceBinding = await ResolveAndSubscribeCharacteristicAsync(
                service,
                AncsUuids.DataSource,
                OnDataSourceChanged,
                cancellationToken);
            dataSource = dataSourceBinding.characteristic;

            controlPoint = await BindCharacteristicAsync(service, AncsUuids.ControlPoint);
            if (controlPoint is null)
            {
                notifySubscriptionsArmed = false;
                PublishState(DeviceSessionPhase.Faulted, "characteristics_missing");
                return;
            }

            controlPoint.ProtectionLevel = GattProtectionLevel.EncryptionAndAuthenticationRequired;

            var notificationBinding = await ResolveAndSubscribeCharacteristicAsync(
                service,
                AncsUuids.NotificationSource,
                OnNotificationSourceChanged,
                cancellationToken);
            notificationSource = notificationBinding.characteristic;

            notifySubscriptionsArmed =
                dataSourceBinding.result.Status == GattCommunicationStatus.Success &&
                notificationBinding.result.Status == GattCommunicationStatus.Success;

            if (notifySubscriptionsArmed)
            {
                CurrentSessionId ??= CreateSessionId("ancs");
                PublishState(DeviceSessionPhase.Connected, "subscriptions_ready");
            }
            else
            {
                PublishState(
                    DeviceSessionPhase.Faulted,
                    "subscription_failed",
                    $"data={DescribeSubscriptionAttempt(dataSourceBinding.result)}; notification={DescribeSubscriptionAttempt(notificationBinding.result)}");
            }
        }
        finally
        {
            refreshLock.Release();
        }
    }

    private async Task<bool> EnsureGattSessionAsync(CancellationToken cancellationToken)
    {
        if (device is null)
        {
            return false;
        }

        if (deviceSession is null)
        {
            deviceSession = await WaitAsync(
                GattSession.FromDeviceIdAsync(device.BluetoothDeviceId),
                WinRtOperationTimeout,
                cancellationToken);
            if (deviceSession is null)
            {
                PublishState(DeviceSessionPhase.Faulted, "gatt_session_open_failed");
                return false;
            }

            deviceSession.SessionStatusChanged += OnSessionStatusChanged;
        }

        deviceSession.MaintainConnection = true;

        if (deviceSession.SessionStatus == GattSessionStatus.Active)
        {
            return true;
        }

        using var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutSource.CancelAfter(GattSessionActivationTimeout);

        try
        {
            await WaitForSessionActivationAsync(deviceSession, timeoutSource.Token);
            return deviceSession.SessionStatus == GattSessionStatus.Active;
        }
        catch (OperationCanceledException) when (timeoutSource.IsCancellationRequested)
        {
            PublishState(
                DeviceSessionPhase.Faulted,
                "gatt_session_wait_timeout",
                deviceSession.SessionStatus.ToString());
        }

        return false;
    }

    private async Task<bool> EnsureServiceOpenAsync(CancellationToken cancellationToken)
    {
        if (service is null || device is null)
        {
            return false;
        }

        for (var attempt = 0; attempt < 2; attempt++)
        {
            var access = await WaitAsync(service.RequestAccessAsync(), WinRtOperationTimeout, cancellationToken);
            if (access != Windows.Devices.Enumeration.DeviceAccessStatus.Allowed)
            {
                PublishState(DeviceSessionPhase.Faulted, "service_access_denied", access.ToString());
                return false;
            }

            var openStatus = await WaitAsync(service.OpenAsync(GattSharingMode.SharedReadAndWrite), WinRtOperationTimeout, cancellationToken);
            if (openStatus is GattOpenStatus.Success or GattOpenStatus.AlreadyOpened)
            {
                return true;
            }

            if (attempt == 0)
            {
                service.Dispose();
                service = await ResolveServiceByUuidAsync(device, AncsUuids.Service);
                if (service is null)
                {
                    PublishState(DeviceSessionPhase.Faulted, "ancs_service_missing");
                    return false;
                }

                continue;
            }

            PublishState(DeviceSessionPhase.Faulted, "service_open_failed", openStatus.ToString());
        }

        return false;
    }

    private async Task<GattDeviceService?> ResolveServiceByUuidAsync(
        BluetoothLEDevice bluetoothDevice,
        Guid serviceUuid)
    {
        foreach (var cacheMode in new[] { BluetoothCacheMode.Cached, BluetoothCacheMode.Uncached })
        {
            var result = await WaitAsync(
                bluetoothDevice.GetGattServicesForUuidAsync(serviceUuid, cacheMode),
                WinRtOperationTimeout,
                CancellationToken.None);
            if (result.Status == GattCommunicationStatus.Success && result.Services.Count > 0)
            {
                return result.Services[0];
            }
        }

        return null;
    }

    private async Task<GattCharacteristic?> BindCharacteristicAsync(
        GattDeviceService gattService,
        Guid characteristicUuid)
    {
        foreach (var cacheMode in new[] { BluetoothCacheMode.Cached, BluetoothCacheMode.Uncached })
        {
            var result = await WaitAsync(
                gattService.GetCharacteristicsForUuidAsync(characteristicUuid, cacheMode),
                WinRtOperationTimeout,
                CancellationToken.None);
            if (result.Status == GattCommunicationStatus.Success && result.Characteristics.Count > 0)
            {
                return result.Characteristics[0];
            }
        }

        return null;
    }

    private async Task<(GattCharacteristic? characteristic, SubscriptionAttemptResult result)> ResolveAndSubscribeCharacteristicAsync(
        GattDeviceService gattService,
        Guid characteristicUuid,
        TypedEventHandler<GattCharacteristic, GattValueChangedEventArgs> handler,
        CancellationToken cancellationToken)
    {
        var result = new SubscriptionAttemptResult(GattCommunicationStatus.Unreachable, null, "uninitialized", null);

        for (var attempt = 0; attempt < 2; attempt++)
        {
            var characteristic = await BindCharacteristicAsync(gattService, characteristicUuid);
            if (characteristic is null)
            {
                return (null, new SubscriptionAttemptResult(GattCommunicationStatus.Unreachable, null, "bind", "characteristic_not_found"));
            }

            characteristic.ValueChanged -= handler;
            characteristic.ValueChanged += handler;

            result = await TrySubscribeAsync(characteristic, cancellationToken);
            if (result.Status == GattCommunicationStatus.Success)
            {
                return (characteristic, result);
            }

            characteristic.ValueChanged -= handler;
        }

        return (null, result);
    }

    private static async Task<SubscriptionAttemptResult> TrySubscribeAsync(
        GattCharacteristic characteristic,
        CancellationToken cancellationToken)
    {
        try
        {
            characteristic.ProtectionLevel = GattProtectionLevel.EncryptionAndAuthenticationRequired;
            var readResult = await WaitAsync(
                characteristic.ReadClientCharacteristicConfigurationDescriptorAsync(),
                WinRtOperationTimeout,
                cancellationToken);
            if (readResult.Status == GattCommunicationStatus.Success
                && !readResult.ProtocolError.HasValue
                && readResult.ClientCharacteristicConfigurationDescriptor.HasFlag(
                    GattClientCharacteristicConfigurationDescriptorValue.Notify))
            {
                return new SubscriptionAttemptResult(
                    GattCommunicationStatus.Success,
                    null,
                    "already_notify",
                    null);
            }

            var result = await WaitAsync(
                characteristic.WriteClientCharacteristicConfigurationDescriptorWithResultAsync(
                    GattClientCharacteristicConfigurationDescriptorValue.Notify),
                WinRtOperationTimeout,
                cancellationToken);
            return new SubscriptionAttemptResult(
                result.Status,
                result.ProtocolError,
                readResult.Status == GattCommunicationStatus.Success && !readResult.ProtocolError.HasValue
                    ? "write"
                    : "write_after_read_failure",
                readResult.Status != GattCommunicationStatus.Success || readResult.ProtocolError.HasValue
                    ? $"cccd_read={readResult.Status}{(readResult.ProtocolError.HasValue ? $"/0x{readResult.ProtocolError.Value:X2}" : string.Empty)}"
                    : null);
        }
        catch (Exception exception)
        {
            return new SubscriptionAttemptResult(
                GattCommunicationStatus.Unreachable,
                null,
                "exception",
                exception.Message);
        }
    }

    private static string DescribeSubscriptionAttempt(SubscriptionAttemptResult result)
    {
        var detail = result.Status.ToString();
        if (!string.IsNullOrWhiteSpace(result.Stage))
        {
            detail += $"@{result.Stage}";
        }

        if (result.ProtocolError.HasValue)
        {
            detail += $"/0x{result.ProtocolError.Value:X2}";
        }

        if (!string.IsNullOrWhiteSpace(result.Detail))
        {
            detail += $" ({result.Detail})";
        }

        return detail;
    }

    private void OnConnectionStatusChanged(BluetoothLEDevice sender, object args)
    {
        notifySubscriptionsArmed = false;
        if (sender.ConnectionStatus == BluetoothConnectionStatus.Connected)
        {
            PublishState(DeviceSessionPhase.Connecting, "connection_restored");
            _ = EnsureSessionAndRefreshOnBackgroundAsync("connection_restored");
            return;
        }

        PublishState(DeviceSessionPhase.Disconnected, "connection_lost");
        CurrentSessionId = null;
        _ = EnsureSessionAndRefreshOnBackgroundAsync("connection_lost");
    }

    private void OnGattServicesChanged(BluetoothLEDevice sender, object args)
    {
        if (!notifySubscriptionsArmed)
        {
            _ = EnsureSessionAndRefreshOnBackgroundAsync("gatt_services_changed");
        }
    }

    private void OnSessionStatusChanged(GattSession sender, GattSessionStatusChangedEventArgs args)
    {
        if (args.Status == GattSessionStatus.Active)
        {
            PublishState(DeviceSessionPhase.Connecting, "gatt_session_active");
            _ = EnsureSessionAndRefreshOnBackgroundAsync("gatt_session_active");
            return;
        }

        notifySubscriptionsArmed = false;
        DetachCharacteristics();
        CurrentSessionId = null;
        PublishState(DeviceSessionPhase.Disconnected, "gatt_session_closed", args.Error.ToString());
        _ = EnsureSessionAndRefreshOnBackgroundAsync("gatt_session_closed");
    }

    private void OnNotificationSourceChanged(
        GattCharacteristic sender,
        GattValueChangedEventArgs args)
    {
        try
        {
            var payload = ReadBytes(args.CharacteristicValue);
            var notification = AncsProtocol.ParseNotificationSource(payload);

            if (notification.EventKind == NotificationEventKind.Removed)
            {
                NotificationRemoved?.Invoke(notification.NotificationUid);
                return;
            }

            if (controlPoint is null)
            {
                return;
            }

            _ = RequestNotificationAttributesAsync(notification, args.Timestamp);
        }
        catch (Exception exception)
        {
            logger.LogDebug(exception, "ANCS notification source parse failed.");
        }
    }

    private void OnDataSourceChanged(GattCharacteristic sender, GattValueChangedEventArgs args)
    {
        try
        {
            var payload = ReadBytes(args.CharacteristicValue);
            PendingNotificationAttributesRequest? pending;
            lock (pendingGate)
            {
                pending = pendingAttributesRequest;
            }

            pending?.Append(payload);
        }
        catch (Exception exception)
        {
            logger.LogDebug(exception, "ANCS data source parse failed.");
        }
    }

    private async Task RequestNotificationAttributesAsync(
        ParsedNotificationEvent notification,
        DateTimeOffset receivedAtUtc)
    {
        var currentControlPoint = controlPoint;
        if (currentControlPoint is null)
        {
            return;
        }

        await notificationAttributesLock.WaitAsync();
        try
        {
            var pending = new PendingNotificationAttributesRequest(
                notification.NotificationUid,
                AncsProtocol.DefaultNotificationAttributes);

            lock (pendingGate)
            {
                pendingAttributesRequest?.Cancel();
                pendingAttributesRequest = pending;
            }

            var request = AncsProtocol.BuildGetNotificationAttributesCommand(
                notification.NotificationUid,
                pending.RequestedAttributes);

            currentControlPoint.ProtectionLevel = GattProtectionLevel.EncryptionAndAuthenticationRequired;
            var status = await currentControlPoint.WriteValueAsync(
                CryptographicBuffer.CreateFromByteArray(request),
                GattWriteOption.WriteWithResponse);
            if (status != GattCommunicationStatus.Success)
            {
                PublishState(DeviceSessionPhase.Faulted, "ancs_attribute_write_failed", status.ToString());
                return;
            }

            using var timeoutSource = new CancellationTokenSource(TimeSpan.FromSeconds(8));
            var response = await pending.Completion.WaitAsync(timeoutSource.Token);
            NotificationReceived?.Invoke(MapNotification(notification, response, receivedAtUtc));
        }
        catch (OperationCanceledException)
        {
            PublishState(DeviceSessionPhase.Faulted, "ancs_attribute_timeout");
        }
        catch (Exception exception)
        {
            PublishState(DeviceSessionPhase.Faulted, "ancs_attribute_failed", exception.Message);
        }
        finally
        {
            lock (pendingGate)
            {
                if (pendingAttributesRequest?.NotificationUid == notification.NotificationUid)
                {
                    pendingAttributesRequest = null;
                }
            }

            notificationAttributesLock.Release();
        }
    }

    private static NotificationRecord MapNotification(
        ParsedNotificationEvent notification,
        ParsedNotificationAttributesResponse response,
        DateTimeOffset receivedAtUtc)
    {
        response.Attributes.TryGetValue(AncsNotificationAttributeId.AppIdentifier, out var appIdentifier);
        response.Attributes.TryGetValue(AncsNotificationAttributeId.Title, out var title);
        response.Attributes.TryGetValue(AncsNotificationAttributeId.Subtitle, out var subtitle);
        response.Attributes.TryGetValue(AncsNotificationAttributeId.Message, out var message);
        response.Attributes.TryGetValue(AncsNotificationAttributeId.MessageSize, out var messageSize);
        response.Attributes.TryGetValue(AncsNotificationAttributeId.Date, out var date);
        response.Attributes.TryGetValue(AncsNotificationAttributeId.PositiveActionLabel, out var positiveActionLabel);
        response.Attributes.TryGetValue(AncsNotificationAttributeId.NegativeActionLabel, out var negativeActionLabel);

        return new NotificationRecord(
            notification.NotificationUid,
            notification.EventKind,
            notification.EventFlags,
            notification.Category,
            notification.CategoryCount,
            receivedAtUtc.ToUniversalTime(),
            appIdentifier,
            title,
            subtitle,
            message,
            messageSize,
            date,
            positiveActionLabel,
            negativeActionLabel,
            response.Attributes.ToDictionary(pair => pair.Key.ToString(), pair => pair.Value));
    }

    private async Task EnsureSessionAndRefreshOnBackgroundAsync(string reason)
    {
        try
        {
            if (DateTimeOffset.UtcNow - lastRefreshRequestUtc < TimeSpan.FromSeconds(1))
            {
                return;
            }

            lastRefreshRequestUtc = DateTimeOffset.UtcNow;
            await Task.Delay(TimeSpan.FromMilliseconds(500));

            await lifecycleLock.WaitAsync();
            try
            {
                if (disposed || target is null || device is null)
                {
                    return;
                }

                if (!await EnsureGattSessionAsync(CancellationToken.None))
                {
                    await TryFallbackToResolvedAddressAsync(target, CancellationToken.None, force: true);
                    return;
                }

                await RefreshBindingsAsync(CancellationToken.None);
            }
            finally
            {
                lifecycleLock.Release();
            }
        }
        catch (Exception exception)
        {
            PublishState(DeviceSessionPhase.Faulted, $"refresh_failed:{reason}", exception.Message);
        }
    }

    private void DetachBindings()
    {
        DetachCharacteristics();

        if (service is not null)
        {
            service.Dispose();
            service = null;
        }

        lock (pendingGate)
        {
            pendingAttributesRequest?.Cancel();
            pendingAttributesRequest = null;
        }
    }

    private void DisposeGattSession()
    {
        if (deviceSession is null)
        {
            return;
        }

        deviceSession.SessionStatusChanged -= OnSessionStatusChanged;
        deviceSession.Dispose();
        deviceSession = null;
    }

    private void DetachCharacteristics()
    {
        if (notificationSource is not null)
        {
            notificationSource.ValueChanged -= OnNotificationSourceChanged;
            notificationSource = null;
        }

        if (dataSource is not null)
        {
            dataSource.ValueChanged -= OnDataSourceChanged;
            dataSource = null;
        }

        controlPoint = null;
        notifySubscriptionsArmed = false;
    }

    private void PublishState(DeviceSessionPhase phase, string detail, string? error = null)
    {
        CurrentPhase = phase;
        StateChanged?.Invoke(
            new SessionStateChangedRecord(
                "ancs",
                phase,
                DateTimeOffset.UtcNow,
                detail,
                error));
    }

    private static byte[] ReadBytes(IBuffer buffer)
    {
        var bytes = new byte[buffer.Length];
        DataReader.FromBuffer(buffer).ReadBytes(bytes);
        return bytes;
    }

    private static string FormatBluetoothAddress(ulong address)
    {
        var hex = address.ToString("X12", System.Globalization.CultureInfo.InvariantCulture);
        return string.Join(
            ":",
            Enumerable.Range(0, 6)
                .Select(index => hex.Substring(index * 2, 2)));
    }

    private static async Task WaitForSessionActivationAsync(
        GattSession session,
        CancellationToken cancellationToken)
    {
        if (session.SessionStatus == GattSessionStatus.Active)
        {
            return;
        }

        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        void Handler(GattSession sender, GattSessionStatusChangedEventArgs args)
        {
            if (args.Status == GattSessionStatus.Active)
            {
                completion.TrySetResult();
            }
        }

        session.SessionStatusChanged += Handler;
        try
        {
            using var registration = cancellationToken.Register(() => completion.TrySetCanceled(cancellationToken));
            await completion.Task;
        }
        finally
        {
            session.SessionStatusChanged -= Handler;
        }
    }

    private static async Task<T> WaitAsync<T>(
        IAsyncOperation<T> operation,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        using var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutSource.CancelAfter(timeout);
        return await operation.AsTask(timeoutSource.Token);
    }

    private static async Task<T> WaitAsync<T, TProgress>(
        IAsyncOperationWithProgress<T, TProgress> operation,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        using var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutSource.CancelAfter(timeout);
        return await operation.AsTask(timeoutSource.Token);
    }

    private static string CreateSessionId(string prefix)
    {
        return $"{prefix}_{Guid.NewGuid():N}";
    }

    private sealed record SubscriptionAttemptResult(
        GattCommunicationStatus Status,
        byte? ProtocolError,
        string Stage,
        string? Detail);
}
