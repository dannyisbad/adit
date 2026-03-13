using Windows.Devices.Bluetooth;
using Windows.Devices.Bluetooth.GenericAttributeProfile;
using Windows.Devices.Enumeration;
using Windows.Security.Cryptography;
using Windows.Storage.Streams;

namespace Adit.Probe;

internal sealed class AncsProbe
{
    private readonly object pendingGate = new();
    private readonly SemaphoreSlim refreshLock = new(1, 1);
    private readonly SemaphoreSlim notificationAttributesLock = new(1, 1);

    private readonly ProbeLogger logger;
    private readonly ProbeOptions options;
    private readonly PairedDeviceRecord target;

    private readonly List<GattCharacteristic> passiveObservedCharacteristics = [];
    private readonly List<GattDeviceService> discoveredServices = [];
    private DateTimeOffset lastRefreshRequestUtc = DateTimeOffset.MinValue;
    private bool activeServicesExercised;
    private bool inventoryLogged;
    private bool notifySubscriptionsArmed;
    private BluetoothLEDevice? device;
    private GattSession? deviceSession;
    private GattCharacteristic? controlPoint;
    private GattCharacteristic? dataSource;
    private PendingNotificationAttributesRequest? pendingAttributesRequest;
    private GattCharacteristic? notificationSource;
    private GattDeviceService? service;

    public AncsProbe(PairedDeviceRecord target, ProbeOptions options, ProbeLogger logger)
    {
        this.target = target;
        this.options = options;
        this.logger = logger;
    }

    public async Task<int> RunAsync(CancellationToken cancellationToken)
    {
        try
        {
            device = await BluetoothLEDevice.FromIdAsync(target.Id);
            if (device is null)
            {
                logger.Log("probe.device_open_failed", new { target.Id, target.Name });
                return 1;
            }

            device.ConnectionStatusChanged += OnConnectionStatusChanged;
            device.GattServicesChanged += OnGattServicesChanged;

            logger.Log(
                "probe.device_opened",
                new
                {
                    device.Name,
                    bluetoothAddress = device.BluetoothAddress.ToString("X"),
                    connectionStatus = device.ConnectionStatus.ToString(),
                    access = await GetDeviceAccessSnapshotAsync(device)
                });

            await EnsureGattSessionAsync(cancellationToken);
            await RefreshBindingsAsync(cancellationToken);

            if (options.MapWatchSeconds > 0 && !activeServicesExercised)
            {
                activeServicesExercised = true;
                await new BleActiveExerciser(logger).ExerciseAsync(discoveredServices, cancellationToken);
            }

            if (device.ConnectionStatus != BluetoothConnectionStatus.Connected)
            {
                logger.Log("probe.waiting_for_connection", new { target.Name });
            }

            if (options.MapWatchSeconds > 0)
            {
                logger.Log(
                    "ancs.watch_started",
                    new
                    {
                        seconds = options.MapWatchSeconds
                    });

                await Task.Delay(TimeSpan.FromSeconds(options.MapWatchSeconds), cancellationToken);
                logger.Log("ancs.watch_completed", new { seconds = options.MapWatchSeconds });
                return 0;
            }

            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(250, cancellationToken);
            }

            return 0;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return 0;
        }
        catch (Exception exception)
        {
            logger.Log("probe.unhandled_exception", new { error = exception.ToString() });
            return 1;
        }
        finally
        {
            Cleanup();
        }
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
            DetachBindings();

            logger.Log(
                "ancs.refresh_start",
                new
                {
                    target.Name,
                    connectionStatus = device.ConnectionStatus.ToString(),
                    access = await GetDeviceAccessSnapshotAsync(device)
                });

            var servicesResult = await device.GetGattServicesAsync(BluetoothCacheMode.Uncached);
            var serviceInventoryAvailable = servicesResult.Status == GattCommunicationStatus.Success;

            logger.Log(
                "ancs.service_query",
                new
                {
                    status = servicesResult.Status.ToString(),
                    protocolError = servicesResult.ProtocolError,
                    serviceCount = servicesResult.Services.Count,
                    services = servicesResult.Services.Select(discoveredService => discoveredService.Uuid)
                });

            if (serviceInventoryAvailable)
            {
                discoveredServices.AddRange(servicesResult.Services);
            }

            var pairingService = discoveredServices.FirstOrDefault(
                discoveredService => discoveredService.Uuid == PairingUuids.Service);
            pairingService ??= await ResolveServiceByUuidAsync(
                device,
                PairingUuids.Service,
                "pairing_service");
            if (pairingService is not null)
            {
                await LogPairingServiceAsync(pairingService);
            }

            var passiveSubscriptions = serviceInventoryAvailable
                ? await SubscribeToGenericNotifyCharacteristicsAsync(
                    discoveredServices.Where(discoveredService => discoveredService.Uuid != AncsUuids.Service))
                : 0;
            notifySubscriptionsArmed = passiveSubscriptions > 0;

            service = discoveredServices.FirstOrDefault(
                discoveredService => discoveredService.Uuid == AncsUuids.Service);
            service ??= await ResolveServiceByUuidAsync(device, AncsUuids.Service, "ancs_service");

            if (service is null)
            {
                if (serviceInventoryAvailable && !inventoryLogged)
                {
                    await LogServiceInventoryAsync(servicesResult.Services);
                    inventoryLogged = true;
                }

                return;
            }

            service.Session.MaintainConnection = true;
            var serviceAccess = await service.RequestAccessAsync();
            var openStatus = serviceAccess == DeviceAccessStatus.Allowed
                ? await service.OpenAsync(GattSharingMode.SharedReadAndWrite)
                : GattOpenStatus.Unspecified;

            logger.Log(
                "ancs.service_access",
                new
                {
                    serviceUuid = service.Uuid,
                    requestStatus = serviceAccess.ToString(),
                    openStatus = openStatus.ToString()
                });

            notificationSource =
                await BindCharacteristicAsync(
                    service,
                    AncsUuids.NotificationSource,
                    "notification_source");
            dataSource =
                await BindCharacteristicAsync(service, AncsUuids.DataSource, "data_source");
            controlPoint =
                await BindCharacteristicAsync(service, AncsUuids.ControlPoint, "control_point");

            if (notificationSource is null || dataSource is null || controlPoint is null)
            {
                logger.Log(
                    "ancs.bindings_incomplete",
                    new
                    {
                        hasNotificationSource = notificationSource is not null,
                        hasDataSource = dataSource is not null,
                        hasControlPoint = controlPoint is not null,
                        passiveSubscriptions
                    });

                if (serviceInventoryAvailable && !inventoryLogged)
                {
                    await LogServiceInventoryAsync(servicesResult.Services);
                    inventoryLogged = true;
                }

                return;
            }

            notificationSource.ValueChanged += OnNotificationSourceChanged;
            dataSource.ValueChanged += OnDataSourceChanged;

            var notificationSubscription = await TrySubscribeAncsCharacteristicAsync(
                notificationSource,
                "notification_source");
            var dataSubscription = await TrySubscribeAncsCharacteristicAsync(
                dataSource,
                "data_source");

            logger.Log(
                "ancs.subscriptions_ready",
                new
                {
                    notificationStatus = notificationSubscription.Status.ToString(),
                    notificationError = notificationSubscription.Error,
                    dataStatus = dataSubscription.Status.ToString(),
                    dataError = dataSubscription.Error
                });

            notifySubscriptionsArmed =
                notificationSubscription.Status == GattCommunicationStatus.Success ||
                dataSubscription.Status == GattCommunicationStatus.Success;

            if (serviceInventoryAvailable && !inventoryLogged)
            {
                await LogServiceInventoryAsync(servicesResult.Services);
                inventoryLogged = true;
            }
        }
        finally
        {
            refreshLock.Release();
        }
    }

    private async Task EnsureGattSessionAsync(CancellationToken cancellationToken)
    {
        if (device is null || deviceSession is not null)
        {
            return;
        }

        deviceSession = await GattSession.FromDeviceIdAsync(device.BluetoothDeviceId);
        if (deviceSession is null)
        {
            logger.Log("gatt.session_open_failed", new { target.Name });
            return;
        }

        deviceSession.SessionStatusChanged += OnSessionStatusChanged;
        deviceSession.MaintainConnection = true;

        logger.Log(
            "gatt.session_opened",
            new
            {
                canMaintainConnection = deviceSession.CanMaintainConnection,
                sessionStatus = deviceSession.SessionStatus.ToString()
            });

        if (deviceSession.SessionStatus == GattSessionStatus.Active)
        {
            return;
        }

        using var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutSource.CancelAfter(TimeSpan.FromSeconds(8));

        try
        {
            await WaitForSessionActivationAsync(deviceSession, timeoutSource.Token);
        }
        catch (OperationCanceledException) when (timeoutSource.IsCancellationRequested)
        {
            logger.Log(
                "gatt.session_wait_timeout",
                new
                {
                    sessionStatus = deviceSession.SessionStatus.ToString(),
                    connectionStatus = device.ConnectionStatus.ToString()
                });
        }
    }

    private async Task<GattCharacteristic?> BindCharacteristicAsync(
        GattDeviceService gattService,
        Guid characteristicUuid,
        string name)
    {
        foreach (var cacheMode in new[] { BluetoothCacheMode.Uncached, BluetoothCacheMode.Cached })
        {
            var result = await gattService.GetCharacteristicsForUuidAsync(
                characteristicUuid,
                cacheMode);

            logger.Log(
                "ancs.characteristic_query",
                new
                {
                    name,
                    uuid = characteristicUuid,
                    cacheMode = cacheMode.ToString(),
                    status = result.Status.ToString(),
                    protocolError = result.ProtocolError,
                    characteristicCount = result.Characteristics.Count
                });

            if (result.Status == GattCommunicationStatus.Success &&
                result.Characteristics.Count > 0)
            {
                return result.Characteristics[0];
            }
        }

        return null;
    }

    private async Task<GattDeviceService?> ResolveServiceByUuidAsync(
        BluetoothLEDevice bluetoothDevice,
        Guid serviceUuid,
        string name)
    {
        foreach (var cacheMode in new[] { BluetoothCacheMode.Uncached, BluetoothCacheMode.Cached })
        {
            var result = await bluetoothDevice.GetGattServicesForUuidAsync(serviceUuid, cacheMode);
            logger.Log(
                "gatt.service_query_by_uuid",
                new
                {
                    name,
                    serviceUuid,
                    cacheMode = cacheMode.ToString(),
                    status = result.Status.ToString(),
                    protocolError = result.ProtocolError,
                    serviceCount = result.Services.Count
                });

            if (result.Status != GattCommunicationStatus.Success ||
                result.Services.Count == 0)
            {
                continue;
            }

            var resolvedService = result.Services[0];
            if (!discoveredServices.Contains(resolvedService))
            {
                discoveredServices.Add(resolvedService);
            }

            for (var index = 1; index < result.Services.Count; index++)
            {
                result.Services[index].Dispose();
            }

            return resolvedService;
        }

        return null;
    }

    private async Task<AncsSubscriptionAttemptResult> TrySubscribeAncsCharacteristicAsync(
        GattCharacteristic characteristic,
        string name)
    {
        try
        {
            var result =
                await characteristic.WriteClientCharacteristicConfigurationDescriptorWithResultAsync(
                    GattClientCharacteristicConfigurationDescriptorValue.Notify);

            logger.Log(
                "ancs.subscription_result",
                new
                {
                    name,
                    serviceUuid = characteristic.Service.Uuid,
                    characteristicUuid = characteristic.Uuid,
                    status = result.Status.ToString(),
                    protocolError = result.ProtocolError
                });

            return new AncsSubscriptionAttemptResult(result.Status, null);
        }
        catch (Exception exception)
        {
            logger.Log(
                "ancs.subscription_failed",
                new
                {
                    name,
                    serviceUuid = characteristic.Service.Uuid,
                    characteristicUuid = characteristic.Uuid,
                    error = exception.Message,
                    hresult = $"0x{exception.HResult:X8}"
                });
            return new AncsSubscriptionAttemptResult(
                GattCommunicationStatus.Unreachable,
                exception.Message);
        }
    }

    private async Task LogServiceInventoryAsync(IReadOnlyList<GattDeviceService> services)
    {
        var inventory = new List<object>(services.Count);

        foreach (var discoveredService in services)
        {
            var characteristicsResult = await discoveredService.GetCharacteristicsAsync(
                BluetoothCacheMode.Uncached);
            var characteristics = new List<object>(characteristicsResult.Characteristics.Count);

            foreach (var characteristic in characteristicsResult.Characteristics)
            {
                characteristics.Add(
                    new
                    {
                        uuid = characteristic.Uuid,
                        properties = characteristic.CharacteristicProperties.ToString(),
                        readResult = await TryReadCharacteristicAsync(characteristic)
                    });
            }

            inventory.Add(
                new
                {
                    serviceUuid = discoveredService.Uuid,
                    status = characteristicsResult.Status.ToString(),
                    characteristicCount = characteristicsResult.Characteristics.Count,
                    characteristics
                });
        }

        logger.Log("gatt.inventory", new { services = inventory });
    }

    private async Task LogPairingServiceAsync(GattDeviceService pairingService)
    {
        var serviceAccess = await pairingService.RequestAccessAsync();
        var protocolVersion = await QueryServiceCharacteristicAsync(
            pairingService,
            PairingUuids.ProtocolVersion,
            "protocol_version",
            GattProtectionLevel.Plain,
            readValue: true);
        var pairingInfo = await QueryServiceCharacteristicAsync(
            pairingService,
            PairingUuids.PairingInfo,
            "pairing_info",
            GattProtectionLevel.Plain,
            readValue: true);
        var deviceInfo = await QueryServiceCharacteristicAsync(
            pairingService,
            PairingUuids.DeviceInfo,
            "device_info",
            GattProtectionLevel.EncryptionAndAuthenticationRequired,
            readValue: true);
        var pairingResult = await QueryServiceCharacteristicAsync(
            pairingService,
            PairingUuids.PairingResult,
            "pairing_result",
            GattProtectionLevel.EncryptionAndAuthenticationRequired,
            readValue: false);

        logger.Log(
            "pairing.service_snapshot",
            new
            {
                serviceUuid = pairingService.Uuid,
                serviceAccess = serviceAccess.ToString(),
                protocolVersion,
                pairingInfo,
                deviceInfo,
                pairingResult
            });
    }

    private async Task<int> SubscribeToGenericNotifyCharacteristicsAsync(
        IEnumerable<GattDeviceService> services)
    {
        var successfulSubscriptions = 0;

        foreach (var discoveredService in services)
        {
            var characteristicsResult = await discoveredService.GetCharacteristicsAsync(
                BluetoothCacheMode.Uncached);

            if (characteristicsResult.Status != GattCommunicationStatus.Success)
            {
                continue;
            }

            foreach (var characteristic in characteristicsResult.Characteristics)
            {
                if (!CharacteristicCanNotify(characteristic.CharacteristicProperties))
                {
                    continue;
                }

                var cccdValue = characteristic.CharacteristicProperties.HasFlag(
                    GattCharacteristicProperties.Notify)
                    ? GattClientCharacteristicConfigurationDescriptorValue.Notify
                    : GattClientCharacteristicConfigurationDescriptorValue.Indicate;

                characteristic.ValueChanged += OnGenericCharacteristicChanged;
                passiveObservedCharacteristics.Add(characteristic);

                try
                {
                    characteristic.ProtectionLevel =
                        GattProtectionLevel.EncryptionAndAuthenticationRequired;
                    var statusResult =
                        await characteristic.WriteClientCharacteristicConfigurationDescriptorWithResultAsync(
                            cccdValue);

                    logger.Log(
                        "gatt.passive_subscription",
                        new
                        {
                            serviceUuid = discoveredService.Uuid,
                            characteristicUuid = characteristic.Uuid,
                            mode = cccdValue.ToString(),
                            status = statusResult.Status.ToString(),
                            protocolError = statusResult.ProtocolError
                        });

                    if (statusResult.Status == GattCommunicationStatus.Success)
                    {
                        successfulSubscriptions++;
                    }
                }
                catch (Exception exception)
                {
                    logger.Log(
                        "gatt.passive_subscription_failed",
                        new
                        {
                            serviceUuid = discoveredService.Uuid,
                            characteristicUuid = characteristic.Uuid,
                            mode = cccdValue.ToString(),
                            error = exception.Message
                        });
                }
            }
        }

        return successfulSubscriptions;
    }

    private async Task<object?> TryReadCharacteristicAsync(GattCharacteristic characteristic)
    {
        if (!characteristic.CharacteristicProperties.HasFlag(GattCharacteristicProperties.Read))
        {
            return null;
        }

        try
        {
            var result = await characteristic.ReadValueAsync(BluetoothCacheMode.Uncached);
            if (result.Status != GattCommunicationStatus.Success)
            {
                return new
                {
                    status = result.Status.ToString(),
                    protocolError = result.ProtocolError
                };
            }

            var bytes = ReadBytes(result.Value);
            return new
            {
                status = result.Status.ToString(),
                payloadLength = bytes.Length,
                payloadHex = Convert.ToHexString(bytes),
                payloadUtf8 = TryDecodeUtf8(bytes)
            };
        }
        catch (Exception exception)
        {
            return new { error = exception.Message };
        }
    }

    private async Task<object> QueryServiceCharacteristicAsync(
        GattDeviceService service,
        Guid characteristicUuid,
        string name,
        GattProtectionLevel protectionLevel,
        bool readValue)
    {
        try
        {
            var queryResult = await service.GetCharacteristicsForUuidAsync(
                characteristicUuid,
                BluetoothCacheMode.Uncached);

            if (queryResult.Status != GattCommunicationStatus.Success ||
                queryResult.Characteristics.Count == 0)
            {
                return new
                {
                    name,
                    uuid = characteristicUuid,
                    status = queryResult.Status.ToString(),
                    protocolError = queryResult.ProtocolError,
                    characteristicCount = queryResult.Characteristics.Count,
                    payloadLength = 0,
                    payloadHex = (string?)null,
                    payloadUtf8 = (string?)null
                };
            }

            var characteristic = queryResult.Characteristics[0];
            characteristic.ProtectionLevel = protectionLevel;

            if (!readValue || !characteristic.CharacteristicProperties.HasFlag(GattCharacteristicProperties.Read))
            {
                return new
                {
                    name,
                    uuid = characteristicUuid,
                    status = queryResult.Status.ToString(),
                    protocolError = queryResult.ProtocolError,
                    characteristicCount = queryResult.Characteristics.Count,
                    properties = characteristic.CharacteristicProperties.ToString(),
                    protectionLevel = protectionLevel.ToString(),
                    payloadLength = 0,
                    payloadHex = (string?)null,
                    payloadUtf8 = (string?)null
                };
            }

            var readResult = await characteristic.ReadValueAsync(BluetoothCacheMode.Uncached);
            var payload = readResult.Status == GattCommunicationStatus.Success
                ? ReadBytes(readResult.Value)
                : [];

            return new
            {
                name,
                uuid = characteristicUuid,
                status = queryResult.Status.ToString(),
                protocolError = queryResult.ProtocolError,
                characteristicCount = queryResult.Characteristics.Count,
                properties = characteristic.CharacteristicProperties.ToString(),
                protectionLevel = protectionLevel.ToString(),
                readStatus = readResult.Status.ToString(),
                readProtocolError = readResult.ProtocolError,
                payloadLength = payload.Length,
                payloadHex = payload.Length > 0 ? Convert.ToHexString(payload) : null,
                payloadUtf8 = TryDecodeUtf8(payload)
            };
        }
        catch (Exception exception)
        {
            return new
            {
                name,
                uuid = characteristicUuid,
                error = exception.Message,
                protectionLevel = protectionLevel.ToString(),
                payloadLength = 0,
                payloadHex = (string?)null,
                payloadUtf8 = (string?)null
            };
        }
    }

    private void OnConnectionStatusChanged(BluetoothLEDevice sender, object args)
    {
        logger.Log(
            "device.connection_status_changed",
            new
            {
                sender.Name,
                connectionStatus = sender.ConnectionStatus.ToString()
            });

        if (sender.ConnectionStatus == BluetoothConnectionStatus.Connected)
        {
            _ = RefreshBindingsOnBackgroundAsync("connection_restored");
        }
    }

    private void OnGattServicesChanged(BluetoothLEDevice sender, object args)
    {
        logger.Log("device.gatt_services_changed", new { sender.Name });

        if (notifySubscriptionsArmed)
        {
            logger.Log("ancs.refresh_skipped", new { reason = "gatt_services_changed", detail = "subscriptions_armed" });
            return;
        }

        _ = RefreshBindingsOnBackgroundAsync("gatt_services_changed");
    }

    private void OnSessionStatusChanged(GattSession sender, GattSessionStatusChangedEventArgs args)
    {
        logger.Log(
            "gatt.session_status_changed",
            new
            {
                status = args.Status.ToString(),
                error = args.Error.ToString()
            });

        if (args.Status == GattSessionStatus.Active)
        {
            _ = RefreshBindingsOnBackgroundAsync("gatt_session_active");
        }
    }

    private void OnNotificationSourceChanged(
        GattCharacteristic sender,
        GattValueChangedEventArgs args)
    {
        try
        {
            var payload = ReadBytes(args.CharacteristicValue);
            var notification = AncsProtocol.ParseNotificationSource(payload);

            logger.Log(
                "ancs.notification_source",
                new
                {
                    receivedAt = args.Timestamp.ToString("O"),
                    notification.EventId,
                    notification.EventFlags,
                    notification.CategoryId,
                    notification.CategoryCount,
                    notification.NotificationUid
                });

            if (!options.AncsIncludePreexisting &&
                notification.EventFlags.HasFlag(AncsEventFlags.PreExisting))
            {
                logger.Log(
                    "ancs.notification_skipped",
                    new
                    {
                        notification.NotificationUid,
                        reason = "preexisting",
                        notification.EventFlags,
                        notification.CategoryId
                    });
                return;
            }

            if (notification.EventId == AncsNotificationEventId.Removed || controlPoint is null)
            {
                return;
            }

            _ = RequestNotificationAttributesAsync(notification);
        }
        catch (Exception exception)
        {
            logger.Log("ancs.notification_source_error", new { error = exception.ToString() });
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

            if (pending is null)
            {
                logger.Log(
                    "ancs.data_source_unexpected",
                    new
                    {
                        receivedAt = args.Timestamp.ToString("O"),
                        payloadLength = payload.Length,
                        payloadHex = Convert.ToHexString(payload)
                    });
                return;
            }

            pending.Append(payload);
            logger.Log(
                "ancs.data_source_fragment",
                new
                {
                    receivedAt = args.Timestamp.ToString("O"),
                    pending.NotificationUid,
                    fragmentLength = payload.Length,
                    pending.BufferedBytes,
                    payloadHex = Convert.ToHexString(payload),
                    payloadUtf8 = TryDecodeUtf8(payload)
                });
        }
        catch (Exception exception)
        {
            logger.Log("ancs.data_source_error", new { error = exception.ToString() });
        }
    }

    private void OnGenericCharacteristicChanged(
        GattCharacteristic sender,
        GattValueChangedEventArgs args)
    {
        try
        {
            var payload = ReadBytes(args.CharacteristicValue);
            logger.Log(
                "gatt.passive_notification",
                new
                {
                    receivedAt = args.Timestamp.ToString("O"),
                    serviceUuid = sender.Service.Uuid,
                    characteristicUuid = sender.Uuid,
                    payloadLength = payload.Length,
                    payloadHex = Convert.ToHexString(payload),
                    payloadUtf8 = TryDecodeUtf8(payload)
                });
        }
        catch (Exception exception)
        {
            logger.Log("gatt.passive_notification_error", new { error = exception.ToString() });
        }
    }

    private async Task RequestNotificationAttributesAsync(AncsNotificationEvent notification)
    {
        var currentControlPoint = controlPoint;
        if (currentControlPoint is null)
        {
            logger.Log(
                "ancs.attributes_skipped",
                new { notification.NotificationUid, reason = "control_point_unavailable" });
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

            logger.Log(
                "ancs.attributes_requested",
                new
                {
                    notification.NotificationUid,
                    requestLength = request.Length,
                    requestHex = Convert.ToHexString(request)
                });

            currentControlPoint.ProtectionLevel = GattProtectionLevel.EncryptionAndAuthenticationRequired;
            var status = await currentControlPoint.WriteValueAsync(
                CryptographicBuffer.CreateFromByteArray(request),
                GattWriteOption.WriteWithResponse);

            logger.Log(
                "ancs.attributes_write_complete",
                new
                {
                    notification.NotificationUid,
                    status = status.ToString()
                });

            if (status != GattCommunicationStatus.Success)
            {
                return;
            }

            using var timeoutSource = new CancellationTokenSource(
                TimeSpan.FromSeconds(options.AttributeTimeoutSeconds));
            var response = await pending.Completion.WaitAsync(timeoutSource.Token);

            logger.Log(
                "ancs.attributes_received",
                new
                {
                    response.NotificationUid,
                    response.Attributes
                });

            await TryPerformNotificationActionAsync(
                notification,
                response,
                currentControlPoint);
        }
        catch (OperationCanceledException)
        {
            logger.Log(
                "ancs.attributes_timeout",
                new
                {
                    notification.NotificationUid,
                    timeoutSeconds = options.AttributeTimeoutSeconds
                });
        }
        catch (Exception exception)
        {
            logger.Log(
                "ancs.attributes_failed",
                new
                {
                    notification.NotificationUid,
                    error = exception.ToString()
                });
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

    private async Task TryPerformNotificationActionAsync(
        AncsNotificationEvent notification,
        AncsNotificationAttributesResponse response,
        GattCharacteristic currentControlPoint)
    {
        if (string.IsNullOrWhiteSpace(options.AncsAutoAction))
        {
            return;
        }

        var actionId = string.Equals(options.AncsAutoAction, "positive", StringComparison.OrdinalIgnoreCase)
            ? AncsActionId.Positive
            : AncsActionId.Negative;
        var requiredFlag = actionId == AncsActionId.Positive
            ? AncsEventFlags.PositiveAction
            : AncsEventFlags.NegativeAction;

        if (!notification.EventFlags.HasFlag(requiredFlag))
        {
            logger.Log(
                "ancs.action_skipped",
                new
                {
                    notification.NotificationUid,
                    action = actionId.ToString(),
                    reason = "flag_not_present",
                    notification.EventFlags
                });
            return;
        }

        if (!string.IsNullOrWhiteSpace(options.MatchText))
        {
            var matches = response.Attributes.Values.Any(
                value => value.Contains(options.MatchText, StringComparison.OrdinalIgnoreCase));
            if (!matches)
            {
                logger.Log(
                    "ancs.action_skipped",
                    new
                    {
                        notification.NotificationUid,
                        action = actionId.ToString(),
                        reason = "match_text_miss",
                        options.MatchText
                    });
                return;
            }
        }

        var labelAttribute = actionId == AncsActionId.Positive
            ? AncsNotificationAttributeId.PositiveActionLabel
            : AncsNotificationAttributeId.NegativeActionLabel;
        response.Attributes.TryGetValue(labelAttribute, out var actionLabel);

        var request = AncsProtocol.BuildPerformNotificationActionCommand(
            notification.NotificationUid,
            actionId);

        logger.Log(
            "ancs.action_requested",
            new
            {
                notification.NotificationUid,
                action = actionId.ToString(),
                actionLabel,
                requestHex = Convert.ToHexString(request)
            });

        currentControlPoint.ProtectionLevel = GattProtectionLevel.EncryptionAndAuthenticationRequired;
        var status = await currentControlPoint.WriteValueAsync(
            CryptographicBuffer.CreateFromByteArray(request),
            GattWriteOption.WriteWithResponse);

        logger.Log(
            "ancs.action_result",
            new
            {
                notification.NotificationUid,
                action = actionId.ToString(),
                actionLabel,
                status = status.ToString()
            });
    }

    private async Task RefreshBindingsOnBackgroundAsync(string reason)
    {
        try
        {
            if (DateTimeOffset.UtcNow - lastRefreshRequestUtc < TimeSpan.FromSeconds(1))
            {
                logger.Log("ancs.refresh_skipped", new { reason, detail = "debounced" });
                return;
            }

            lastRefreshRequestUtc = DateTimeOffset.UtcNow;
            logger.Log("ancs.refresh_requested", new { reason });
            await Task.Delay(TimeSpan.FromMilliseconds(500));
            await RefreshBindingsAsync(CancellationToken.None);
        }
        catch (Exception exception)
        {
            logger.Log("ancs.refresh_failed", new { reason, error = exception.ToString() });
        }
    }

    private void Cleanup()
    {
        DetachBindings();

        if (device is not null)
        {
            device.ConnectionStatusChanged -= OnConnectionStatusChanged;
            device.GattServicesChanged -= OnGattServicesChanged;
            device.Dispose();
            device = null;
        }

        if (deviceSession is not null)
        {
            deviceSession.SessionStatusChanged -= OnSessionStatusChanged;
            deviceSession.Dispose();
            deviceSession = null;
        }
    }

    private void DetachBindings()
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

        foreach (var characteristic in passiveObservedCharacteristics)
        {
            characteristic.ValueChanged -= OnGenericCharacteristicChanged;
        }

        passiveObservedCharacteristics.Clear();
        notifySubscriptionsArmed = false;

        foreach (var discoveredService in discoveredServices)
        {
            discoveredService.Dispose();
        }

        discoveredServices.Clear();
        service = null;

        lock (pendingGate)
        {
            pendingAttributesRequest?.Cancel();
            pendingAttributesRequest = null;
        }
    }

    private static byte[] ReadBytes(IBuffer buffer)
    {
        var bytes = new byte[buffer.Length];
        DataReader.FromBuffer(buffer).ReadBytes(bytes);
        return bytes;
    }

    private static async Task<object> GetDeviceAccessSnapshotAsync(BluetoothLEDevice bluetoothDevice)
    {
        var requestStatus = await bluetoothDevice.RequestAccessAsync();
        var accessInformation = DeviceAccessInformation.CreateFromId(bluetoothDevice.DeviceId);

        return new
        {
            currentStatus = accessInformation.CurrentStatus.ToString(),
            requestStatus = requestStatus.ToString()
        };
    }

    private static bool CharacteristicCanNotify(GattCharacteristicProperties properties)
    {
        return properties.HasFlag(GattCharacteristicProperties.Notify) ||
               properties.HasFlag(GattCharacteristicProperties.Indicate);
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
            else if (args.Status == GattSessionStatus.Closed)
            {
                completion.TrySetException(
                    new InvalidOperationException($"Gatt session closed: {args.Error}"));
            }
        }

        session.SessionStatusChanged += Handler;
        try
        {
            using var registration = cancellationToken.Register(
                () => completion.TrySetCanceled(cancellationToken));
            await completion.Task;
        }
        finally
        {
            session.SessionStatusChanged -= Handler;
        }
    }

    private static string? TryDecodeUtf8(byte[] bytes)
    {
        if (bytes.Length == 0)
        {
            return string.Empty;
        }

        try
        {
            var decoded = System.Text.Encoding.UTF8.GetString(bytes);
            return decoded.Any(character => char.IsControl(character) && !char.IsWhiteSpace(character))
                ? null
                : decoded;
        }
        catch
        {
            return null;
        }
    }

    private sealed record AncsSubscriptionAttemptResult(
        GattCommunicationStatus Status,
        string? Error);
}
