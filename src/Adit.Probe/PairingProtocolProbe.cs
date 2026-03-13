using Windows.Devices.Bluetooth;
using Windows.Devices.Bluetooth.GenericAttributeProfile;
using Windows.Devices.Enumeration;
using Windows.Security.Cryptography;

namespace Adit.Probe;

internal sealed class PairingProtocolProbe
{
    private static readonly IReadOnlyList<GattProtectionLevel> ReadProtectionLevels =
    [
        GattProtectionLevel.Plain,
        GattProtectionLevel.EncryptionAndAuthenticationRequired
    ];

    private static readonly IReadOnlyList<BluetoothCacheMode> CacheModes =
    [
        BluetoothCacheMode.Uncached,
        BluetoothCacheMode.Cached
    ];

    private readonly ProbeLogger logger;
    private readonly ProbeOptions options;
    private readonly PairedDeviceRecord target;

    public PairingProtocolProbe(PairedDeviceRecord target, ProbeOptions options, ProbeLogger logger)
    {
        this.target = target;
        this.options = options;
        this.logger = logger;
    }

    public async Task<int> RunAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var device = await BluetoothLEDevice.FromIdAsync(target.Id);
            if (device is null)
            {
                logger.Log("pairing_probe.device_open_failed", new { target.Id, target.Name });
                return 1;
            }

            var access = await GetDeviceAccessSnapshotAsync(device);
            logger.Log(
                "pairing_probe.device_opened",
                new
                {
                    targetName = target.Name,
                    target.Id,
                    deviceName = device.Name,
                    bluetoothAddress = device.BluetoothAddress.ToString("X"),
                    connectionStatus = device.ConnectionStatus.ToString(),
                    isPaired = device.DeviceInformation.Pairing.IsPaired,
                    access
                });

            using var session = await GattSession.FromDeviceIdAsync(device.BluetoothDeviceId);
            if (session is not null)
            {
                session.MaintainConnection = true;
                logger.Log(
                    "pairing_probe.session",
                    new
                    {
                        canMaintainConnection = session.CanMaintainConnection,
                        sessionStatus = session.SessionStatus.ToString()
                    });

                await PrimeGattAsync(device);
                await WaitForSessionActivationAsync(session, cancellationToken);
            }

            var pairingService = await ResolvePairingServiceAsync(device);
            if (pairingService is null)
            {
                logger.Log("pairing_probe.service_not_found", new { serviceUuid = PairingUuids.Service });
                return 1;
            }

            using (pairingService)
            {
                await ExerciseServiceAsync(device, pairingService, cancellationToken);
            }

            return 0;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return 0;
        }
        catch (Exception exception)
        {
            logger.Log("pairing_probe.unhandled_exception", new { error = exception.ToString() });
            return 1;
        }
    }

    private async Task<GattDeviceService?> ResolvePairingServiceAsync(BluetoothLEDevice device)
    {
        foreach (var cacheMode in CacheModes)
        {
            var allServicesResult = await device.GetGattServicesAsync(cacheMode);
            logger.Log(
                "pairing_probe.service_inventory",
                new
                {
                    cacheMode = cacheMode.ToString(),
                    status = allServicesResult.Status.ToString(),
                    protocolError = allServicesResult.ProtocolError,
                    serviceCount = allServicesResult.Services.Count,
                    pairingServicePresent = allServicesResult.Services.Any(
                        candidate => candidate.Uuid == PairingUuids.Service)
                });

            foreach (var unusedService in allServicesResult.Services)
            {
                unusedService.Dispose();
            }
        }

        GattDeviceService? selectedService = null;
        foreach (var cacheMode in CacheModes)
        {
            var serviceResult = await device.GetGattServicesForUuidAsync(PairingUuids.Service, cacheMode);
            logger.Log(
                "pairing_probe.service_query",
                new
                {
                    cacheMode = cacheMode.ToString(),
                    status = serviceResult.Status.ToString(),
                    protocolError = serviceResult.ProtocolError,
                    serviceCount = serviceResult.Services.Count
                });

            if (selectedService is null &&
                serviceResult.Status == GattCommunicationStatus.Success &&
                serviceResult.Services.Count > 0)
            {
                selectedService = serviceResult.Services[0];
            }
            else
            {
                foreach (var unusedService in serviceResult.Services)
                {
                    unusedService.Dispose();
                }
            }
        }

        return selectedService;
    }

    private async Task ExerciseServiceAsync(
        BluetoothLEDevice device,
        GattDeviceService pairingService,
        CancellationToken cancellationToken)
    {
        var serviceAccess = await pairingService.RequestAccessAsync();
        var openStatus = serviceAccess == DeviceAccessStatus.Allowed
            ? await pairingService.OpenAsync(GattSharingMode.SharedReadAndWrite)
            : GattOpenStatus.Unspecified;

        logger.Log(
            "pairing_probe.service_opened",
            new
            {
                serviceUuid = pairingService.Uuid,
                serviceAccess = serviceAccess.ToString(),
                openStatus = openStatus.ToString(),
                isPaired = device.DeviceInformation.Pairing.IsPaired
            });

        var protocolVersionCharacteristic = await GetCharacteristicAsync(
            pairingService,
            PairingUuids.ProtocolVersion,
            "protocol_version");
        var pairingInfoCharacteristic = await GetCharacteristicAsync(
            pairingService,
            PairingUuids.PairingInfo,
            "pairing_info");
        var pairingResultCharacteristic = await GetCharacteristicAsync(
            pairingService,
            PairingUuids.PairingResult,
            "pairing_result");
        var deviceInfoCharacteristic = await GetCharacteristicAsync(
            pairingService,
            PairingUuids.DeviceInfo,
            "device_info");

        string? protocolVersion = null;
        if (protocolVersionCharacteristic is not null)
        {
            foreach (var cacheMode in CacheModes.Reverse())
            {
                var protocolVersionRead = await TryReadCharacteristicAsync(
                    pairingService,
                    protocolVersionCharacteristic,
                    $"protocol_version.{cacheMode.ToString().ToLowerInvariant()}",
                    GattProtectionLevel.Plain,
                    cacheMode,
                    PairingFieldSet.Unknown);

                if (protocolVersionRead.Status == GattCommunicationStatus.Success &&
                    !string.IsNullOrWhiteSpace(protocolVersionRead.PayloadUtf8))
                {
                    protocolVersion = protocolVersionRead.PayloadUtf8;
                    break;
                }
            }
        }

        if (pairingResultCharacteristic is not null)
        {
            foreach (var preflightWrite in BuildPreflightWriteAttempts())
            {
                await TryWritePairingResultAsync(pairingService, pairingResultCharacteristic, preflightWrite);
            }
        }

        if (pairingInfoCharacteristic is not null)
        {
            foreach (var protectionLevel in ReadProtectionLevels)
            {
                foreach (var cacheMode in CacheModes)
                {
                    await TryReadCharacteristicAsync(
                        pairingService,
                        pairingInfoCharacteristic,
                        "pairing_info",
                        protectionLevel,
                        cacheMode,
                        PairingFieldSet.PairingInfo,
                        protocolVersion);
                }
            }
        }

        if (deviceInfoCharacteristic is not null)
        {
            foreach (var protectionLevel in ReadProtectionLevels)
            {
                foreach (var cacheMode in CacheModes)
                {
                    await TryReadCharacteristicAsync(
                        pairingService,
                        deviceInfoCharacteristic,
                        "device_info",
                        protectionLevel,
                        cacheMode,
                        PairingFieldSet.DeviceInfo,
                        protocolVersion);
                }
            }
        }

        if (pairingResultCharacteristic is null)
        {
            return;
        }

        var initialPairingInfoRead = pairingInfoCharacteristic is null
            ? null
            : await TryReadCharacteristicAsync(
                pairingService,
                pairingInfoCharacteristic,
                "pairing_info_baseline",
                GattProtectionLevel.Plain,
                BluetoothCacheMode.Uncached,
                PairingFieldSet.PairingInfo,
                protocolVersion);

        var baselinePairingId = initialPairingInfoRead?.PayloadBytes;
        var writeAttempts = BuildWriteAttempts(baselinePairingId);

        foreach (var writeAttempt in writeAttempts)
        {
            await TryWritePairingResultAsync(pairingService, pairingResultCharacteristic, writeAttempt);
            await Task.Delay(350, cancellationToken);

            if (pairingInfoCharacteristic is not null)
            {
                await TryReadCharacteristicAsync(
                    pairingService,
                    pairingInfoCharacteristic,
                    $"{writeAttempt.Name}.pairing_info_followup",
                    GattProtectionLevel.Plain,
                    BluetoothCacheMode.Uncached,
                    PairingFieldSet.PairingInfo,
                    protocolVersion);
            }

            if (deviceInfoCharacteristic is not null)
            {
                await TryReadCharacteristicAsync(
                    pairingService,
                    deviceInfoCharacteristic,
                    $"{writeAttempt.Name}.device_info_followup",
                    GattProtectionLevel.EncryptionAndAuthenticationRequired,
                    BluetoothCacheMode.Uncached,
                    PairingFieldSet.DeviceInfo,
                    protocolVersion);
            }
        }

        if (options.MapWatchSeconds > 0)
        {
            logger.Log("pairing_probe.watch_started", new { seconds = options.MapWatchSeconds });
            await Task.Delay(TimeSpan.FromSeconds(options.MapWatchSeconds), cancellationToken);
            logger.Log("pairing_probe.watch_completed", new { seconds = options.MapWatchSeconds });
        }
    }

    private static IReadOnlyList<PairingWriteAttempt> BuildWriteAttempts(byte[]? baselinePairingId)
    {
        var attempts = new List<PairingWriteAttempt>
        {
            new("empty", [])
        };

        attempts.AddRange(BuildPreflightWriteAttempts());

        if (baselinePairingId is { Length: > 0 })
        {
            attempts.Add(
                new(
                    "baseline_pairing_id_success",
                    PairingProtocol.BuildPairingResultPayload(
                        pairingId: baselinePairingId,
                        sessionId: Guid.NewGuid(),
                        resultStatus: 0)));
        }

        return attempts;
    }

    private static IReadOnlyList<PairingWriteAttempt> BuildPreflightWriteAttempts()
    {
        return
        [
            new(
                "status_only_success",
                PairingProtocol.BuildPairingResultPayload(
                    pairingId: null,
                    sessionId: null,
                    resultStatus: 0)),
            new(
                "random_success",
                PairingProtocol.BuildPairingResultPayload(
                    pairingId: PairingProtocol.CreateRandomPairingId(),
                    sessionId: Guid.NewGuid(),
                    resultStatus: 0))
        ];
    }

    private async Task<PairingCharacteristicReference?> GetCharacteristicAsync(
        GattDeviceService service,
        Guid characteristicUuid,
        string name)
    {
        foreach (var cacheMode in CacheModes)
        {
            var result = await service.GetCharacteristicsForUuidAsync(
                characteristicUuid,
                cacheMode);

            logger.Log(
                "pairing_probe.characteristic_query",
                new
                {
                    name,
                    uuid = characteristicUuid,
                    cacheMode = cacheMode.ToString(),
                    status = result.Status.ToString(),
                    protocolError = result.ProtocolError,
                    characteristicCount = result.Characteristics.Count
                });

            if (result.Status != GattCommunicationStatus.Success || result.Characteristics.Count == 0)
            {
                continue;
            }

            var characteristic = result.Characteristics[0];
            logger.Log(
                "pairing_probe.characteristic_bound",
                new
                {
                    name,
                    uuid = characteristic.Uuid,
                    cacheMode = cacheMode.ToString(),
                    properties = characteristic.CharacteristicProperties.ToString(),
                    attributeHandle = characteristic.AttributeHandle
                });

            return new PairingCharacteristicReference(
                name,
                characteristic.Uuid,
                characteristic.CharacteristicProperties.ToString(),
                characteristic.AttributeHandle,
                cacheMode);
        }

        return null;
    }

    private async Task<PairingReadAttemptResult> TryReadCharacteristicAsync(
        GattDeviceService service,
        PairingCharacteristicReference characteristicReference,
        string name,
        GattProtectionLevel protectionLevel,
        BluetoothCacheMode cacheMode,
        PairingFieldSet fieldSet,
        string? protocolVersion = null)
    {
        try
        {
            var characteristic = await ResolveCharacteristicForOperationAsync(
                service,
                characteristicReference,
                cacheMode,
                "read");
            if (characteristic is null)
            {
                return new PairingReadAttemptResult(
                    GattCommunicationStatus.Unreachable,
                    null,
                    null,
                    null);
            }

            characteristic.ProtectionLevel = protectionLevel;
            var readResult = await characteristic.ReadValueAsync(cacheMode);
            var payloadBytes = readResult.Status == GattCommunicationStatus.Success
                ? ReadBytes(readResult.Value)
                : null;

            logger.Log(
                "pairing_probe.characteristic_read",
                new
                {
                    name,
                    uuid = characteristicReference.Uuid,
                    protectionLevel = protectionLevel.ToString(),
                    cacheMode = cacheMode.ToString(),
                    status = readResult.Status.ToString(),
                    protocolError = readResult.ProtocolError,
                    boundProperties = characteristic.CharacteristicProperties.ToString(),
                    boundAttributeHandle = characteristic.AttributeHandle,
                    payloadLength = payloadBytes?.Length ?? 0,
                    payloadHex = payloadBytes is { Length: > 0 } ? Convert.ToHexString(payloadBytes) : null,
                    payloadUtf8 = payloadBytes is { Length: > 0 } ? TryDecodeUtf8(payloadBytes) : string.Empty,
                    parsed = payloadBytes is { Length: > 0 }
                        ? PairingProtocol.DescribeFields(payloadBytes, fieldSet, protocolVersion)
                        : null
                });

            return new PairingReadAttemptResult(
                readResult.Status,
                readResult.ProtocolError,
                payloadBytes,
                payloadBytes is { Length: > 0 } ? TryDecodeUtf8(payloadBytes) : string.Empty);
        }
        catch (Exception exception)
        {
            logger.Log(
                "pairing_probe.characteristic_read_failed",
                new
                {
                    name,
                    uuid = characteristicReference.Uuid,
                    protectionLevel = protectionLevel.ToString(),
                    cacheMode = cacheMode.ToString(),
                    error = exception.ToString()
                });

            return new PairingReadAttemptResult(
                GattCommunicationStatus.Unreachable,
                null,
                null,
                null);
        }
    }

    private async Task TryWritePairingResultAsync(
        GattDeviceService service,
        PairingCharacteristicReference characteristicReference,
        PairingWriteAttempt writeAttempt)
    {
        try
        {
            var characteristic = await ResolveCharacteristicForOperationAsync(
                service,
                characteristicReference,
                BluetoothCacheMode.Cached,
                "write");
            if (characteristic is null)
            {
                logger.Log(
                    "pairing_probe.pairing_result_write_skipped",
                    new
                    {
                        name = writeAttempt.Name,
                        uuid = characteristicReference.Uuid,
                        payloadHex = Convert.ToHexString(writeAttempt.Payload),
                        reason = "characteristic_unreachable"
                    });
                return;
            }

            characteristic.ProtectionLevel = GattProtectionLevel.EncryptionAndAuthenticationRequired;
            var buffer = CryptographicBuffer.CreateFromByteArray(writeAttempt.Payload);
            var writeResult = await characteristic.WriteValueWithResultAsync(buffer);

            logger.Log(
                "pairing_probe.pairing_result_write",
                new
                {
                    name = writeAttempt.Name,
                    uuid = characteristicReference.Uuid,
                    protectionLevel = characteristic.ProtectionLevel.ToString(),
                    boundProperties = characteristic.CharacteristicProperties.ToString(),
                    boundAttributeHandle = characteristic.AttributeHandle,
                    payloadHex = Convert.ToHexString(writeAttempt.Payload),
                    parsed = PairingProtocol.DescribeFields(
                        writeAttempt.Payload,
                        PairingFieldSet.PairingResult),
                    status = writeResult.Status.ToString(),
                    protocolError = writeResult.ProtocolError
                });
        }
        catch (Exception exception)
        {
            logger.Log(
                "pairing_probe.pairing_result_write_failed",
                new
                {
                    name = writeAttempt.Name,
                    uuid = characteristicReference.Uuid,
                    payloadHex = Convert.ToHexString(writeAttempt.Payload),
                    parsed = PairingProtocol.DescribeFields(
                        writeAttempt.Payload,
                        PairingFieldSet.PairingResult),
                    error = exception.ToString()
                });
        }
    }

    private async Task<GattCharacteristic?> ResolveCharacteristicForOperationAsync(
        GattDeviceService service,
        PairingCharacteristicReference characteristicReference,
        BluetoothCacheMode cacheMode,
        string operation)
    {
        var result = await service.GetCharacteristicsForUuidAsync(
            characteristicReference.Uuid,
            cacheMode);

        logger.Log(
            "pairing_probe.characteristic_rebind",
            new
            {
                name = characteristicReference.Name,
                uuid = characteristicReference.Uuid,
                operation,
                cacheMode = cacheMode.ToString(),
                status = result.Status.ToString(),
                protocolError = result.ProtocolError,
                characteristicCount = result.Characteristics.Count
            });

        return result.Status == GattCommunicationStatus.Success &&
               result.Characteristics.Count > 0
            ? result.Characteristics[0]
            : null;
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

    private async Task PrimeGattAsync(BluetoothLEDevice device)
    {
        foreach (var cacheMode in CacheModes)
        {
            var result = await device.GetGattServicesAsync(cacheMode);
            logger.Log(
                "pairing_probe.gatt_prime",
                new
                {
                    cacheMode = cacheMode.ToString(),
                    status = result.Status.ToString(),
                    protocolError = result.ProtocolError,
                    serviceCount = result.Services.Count
                });

            foreach (var service in result.Services)
            {
                service.Dispose();
            }
        }
    }

    private static byte[] ReadBytes(Windows.Storage.Streams.IBuffer buffer)
    {
        var bytes = new byte[buffer.Length];
        Windows.Storage.Streams.DataReader.FromBuffer(buffer).ReadBytes(bytes);
        return bytes;
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

    private async Task WaitForSessionActivationAsync(
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
            using var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutSource.CancelAfter(TimeSpan.FromSeconds(8));
            using var registration = timeoutSource.Token.Register(
                () => completion.TrySetCanceled(timeoutSource.Token));
            await completion.Task;
        }
        catch (Exception exception)
        {
            logger.Log(
                "pairing_probe.session_wait",
                new
                {
                    status = session.SessionStatus.ToString(),
                    error = exception.Message
                });
        }
        finally
        {
            session.SessionStatusChanged -= Handler;
        }
    }
}

internal sealed record PairingReadAttemptResult(
    GattCommunicationStatus Status,
    byte? ProtocolError,
    byte[]? PayloadBytes,
    string? PayloadUtf8);

internal sealed record PairingCharacteristicReference(
    string Name,
    Guid Uuid,
    string Properties,
    ushort AttributeHandle,
    BluetoothCacheMode BoundCacheMode);

internal sealed record PairingWriteAttempt(string Name, byte[] Payload);
