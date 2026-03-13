using Windows.Devices.Bluetooth;
using Windows.Devices.Bluetooth.GenericAttributeProfile;
using Windows.Security.Cryptography;
using Windows.Storage.Streams;

namespace Adit.Probe;

internal sealed class BleActiveExerciser
{
    private static readonly IReadOnlyList<byte[]> GenericWriteCorpus =
    [
        [0x00],
        [0x01],
        [0x00, 0x00],
        [0x01, 0x00],
        [0xFF],
        [0xFF, 0x00],
        [0x55],
        [0xAA],
        System.Text.Encoding.ASCII.GetBytes("?"),
        [0x10, 0x00],
        [0x02, 0x00]
    ];

    private readonly ProbeLogger logger;

    public BleActiveExerciser(ProbeLogger logger)
    {
        this.logger = logger;
    }

    public async Task ExerciseAsync(
        IReadOnlyList<GattDeviceService> discoveredServices,
        CancellationToken cancellationToken)
    {
        if (discoveredServices.Count == 0)
        {
            return;
        }

        await ExerciseAmsAsync(discoveredServices, cancellationToken);
        await ExerciseCustomServiceAsync(
            discoveredServices,
            BleExerciseUuids.CustomService1,
            BleExerciseUuids.CustomCharacteristic1,
            "custom_service_1",
            GenericWriteCorpus,
            cancellationToken);
        await ExerciseCustomServiceAsync(
            discoveredServices,
            BleExerciseUuids.CustomService2,
            BleExerciseUuids.CustomCharacteristic2,
            "custom_service_2",
            GenericWriteCorpus,
            cancellationToken);
    }

    private async Task ExerciseAmsAsync(
        IReadOnlyList<GattDeviceService> discoveredServices,
        CancellationToken cancellationToken)
    {
        var service = discoveredServices.FirstOrDefault(candidate => candidate.Uuid == AmsUuids.Service);
        if (service is null)
        {
            return;
        }

        logger.Log("ble.exercise.ams_begin", new { serviceUuid = service.Uuid });
        await LogServiceAccessAsync(service, "ams", cancellationToken);

        var remoteCommand = await ResolveCharacteristicAsync(
            service,
            AmsUuids.RemoteCommand,
            "ams.remote_command",
            GattProtectionLevel.EncryptionAndAuthenticationRequired,
            cancellationToken);
        var entityUpdate = await ResolveCharacteristicAsync(
            service,
            AmsUuids.EntityUpdate,
            "ams.entity_update",
            GattProtectionLevel.EncryptionAndAuthenticationRequired,
            cancellationToken);
        var entityAttribute = await ResolveCharacteristicAsync(
            service,
            AmsUuids.EntityAttribute,
            "ams.entity_attribute",
            GattProtectionLevel.EncryptionAndAuthenticationRequired,
            cancellationToken);

        if (remoteCommand is not null)
        {
            await TrySubscribeAsync(remoteCommand, "ams.remote_command", cancellationToken);
        }

        if (entityUpdate is not null)
        {
            await TrySubscribeAsync(entityUpdate, "ams.entity_update", cancellationToken);
        }

        if (entityUpdate is not null)
        {
            await WriteAsync(entityUpdate, "ams.entity_update.player_attributes", [0x00, 0x00, 0x01, 0x02], cancellationToken);
            await PauseAsync(cancellationToken);
            await WriteAsync(entityUpdate, "ams.entity_update.queue_attributes", [0x01, 0x00, 0x01, 0x02, 0x03], cancellationToken);
            await PauseAsync(cancellationToken);
            await WriteAsync(entityUpdate, "ams.entity_update.track_attributes", [0x02, 0x00, 0x01, 0x02, 0x03], cancellationToken);
            await PauseAsync(cancellationToken);
        }

        if (entityAttribute is not null)
        {
            await TryReadAsync(entityAttribute, "ams.entity_attribute.initial_uncached", BluetoothCacheMode.Uncached, cancellationToken);

            for (var trackAttribute = 0; trackAttribute <= 3; trackAttribute++)
            {
                await WriteAsync(
                    entityAttribute,
                    $"ams.entity_attribute.track.{trackAttribute}",
                    [(byte)0x02, (byte)trackAttribute],
                    cancellationToken);
                await PauseAsync(cancellationToken);
                await TryReadAsync(
                    entityAttribute,
                    $"ams.entity_attribute.track.{trackAttribute}.cached",
                    BluetoothCacheMode.Cached,
                    cancellationToken);
                await TryReadAsync(
                    entityAttribute,
                    $"ams.entity_attribute.track.{trackAttribute}.uncached",
                    BluetoothCacheMode.Uncached,
                    cancellationToken);
            }

            for (var playerAttribute = 0; playerAttribute <= 2; playerAttribute++)
            {
                await WriteAsync(
                    entityAttribute,
                    $"ams.entity_attribute.player.{playerAttribute}",
                    [(byte)0x00, (byte)playerAttribute],
                    cancellationToken);
                await PauseAsync(cancellationToken);
                await TryReadAsync(
                    entityAttribute,
                    $"ams.entity_attribute.player.{playerAttribute}.cached",
                    BluetoothCacheMode.Cached,
                    cancellationToken);
            }

            for (var queueAttribute = 0; queueAttribute <= 3; queueAttribute++)
            {
                await WriteAsync(
                    entityAttribute,
                    $"ams.entity_attribute.queue.{queueAttribute}",
                    [(byte)0x01, (byte)queueAttribute],
                    cancellationToken);
                await PauseAsync(cancellationToken);
                await TryReadAsync(
                    entityAttribute,
                    $"ams.entity_attribute.queue.{queueAttribute}.cached",
                    BluetoothCacheMode.Cached,
                    cancellationToken);
            }
        }
    }

    private async Task ExerciseCustomServiceAsync(
        IReadOnlyList<GattDeviceService> discoveredServices,
        Guid serviceUuid,
        Guid characteristicUuid,
        string label,
        IReadOnlyList<byte[]> writeCorpus,
        CancellationToken cancellationToken)
    {
        var service = discoveredServices.FirstOrDefault(candidate => candidate.Uuid == serviceUuid);
        if (service is null)
        {
            return;
        }

        logger.Log("ble.exercise.custom_begin", new { label, serviceUuid });
        await LogServiceAccessAsync(service, label, cancellationToken);

        var characteristic = await ResolveCharacteristicAsync(
            service,
            characteristicUuid,
            label,
            GattProtectionLevel.EncryptionAndAuthenticationRequired,
            cancellationToken);
        if (characteristic is null)
        {
            return;
        }

        await TrySubscribeAsync(characteristic, label, cancellationToken);

        foreach (var payload in writeCorpus)
        {
            await WriteAsync(characteristic, label, payload, cancellationToken);
            await PauseAsync(cancellationToken);
        }
    }

    private async Task LogServiceAccessAsync(
        GattDeviceService service,
        string label,
        CancellationToken cancellationToken)
    {
        try
        {
            var accessStatus = await service.RequestAccessAsync().AsTask(cancellationToken);
            var openStatus = accessStatus == Windows.Devices.Enumeration.DeviceAccessStatus.Allowed
                ? await service.OpenAsync(GattSharingMode.SharedReadAndWrite).AsTask(cancellationToken)
                : GattOpenStatus.Unspecified;

            logger.Log(
                "ble.exercise.service_access",
                new
                {
                    label,
                    serviceUuid = service.Uuid,
                    requestStatus = accessStatus.ToString(),
                    openStatus = openStatus.ToString()
                });
        }
        catch (Exception exception)
        {
            logger.Log(
                "ble.exercise.service_access_failed",
                new
                {
                    label,
                    serviceUuid = service.Uuid,
                    error = exception.Message,
                    hresult = $"0x{exception.HResult:X8}"
                });
        }
    }

    private async Task<GattCharacteristic?> ResolveCharacteristicAsync(
        GattDeviceService service,
        Guid characteristicUuid,
        string label,
        GattProtectionLevel protectionLevel,
        CancellationToken cancellationToken)
    {
        try
        {
            var protectedResult = await service.GetCharacteristicsForUuidAsync(
                    characteristicUuid,
                    BluetoothCacheMode.Uncached)
                .AsTask(cancellationToken);

            logger.Log(
                "ble.exercise.characteristic_query",
                new
                {
                    label,
                    serviceUuid = service.Uuid,
                    characteristicUuid,
                    attempt = "direct",
                    requestedProtectionLevel = protectionLevel.ToString(),
                    status = protectedResult.Status.ToString(),
                    protocolError = protectedResult.ProtocolError,
                    characteristicCount = protectedResult.Characteristics.Count
                });

            if (protectedResult.Status == GattCommunicationStatus.Success &&
                protectedResult.Characteristics.Count > 0)
            {
                var protectedCharacteristic = protectedResult.Characteristics[0];
                protectedCharacteristic.ProtectionLevel = protectionLevel;
                AttachNotificationLogger(protectedCharacteristic, label);
                return protectedCharacteristic;
            }
        }
        catch (Exception exception)
        {
            logger.Log(
                "ble.exercise.characteristic_query_failed",
                new
                {
                    label,
                    serviceUuid = service.Uuid,
                    characteristicUuid,
                    attempt = "direct",
                    error = exception.Message,
                    hresult = $"0x{exception.HResult:X8}"
                });
            return null;
        }

        return null;
    }

    private void AttachNotificationLogger(GattCharacteristic characteristic, string label)
    {
        characteristic.ValueChanged -= OnCharacteristicValueChanged;
        characteristic.ValueChanged += OnCharacteristicValueChanged;

        void OnCharacteristicValueChanged(GattCharacteristic sender, GattValueChangedEventArgs args)
        {
            try
            {
                var payload = ReadBytes(args.CharacteristicValue);
                logger.Log(
                    "ble.exercise.notification",
                    new
                    {
                        label,
                        receivedAt = args.Timestamp.ToString("O"),
                        serviceUuid = sender.Service.Uuid,
                        characteristicUuid = sender.Uuid,
                        payloadLength = payload.Length,
                        payloadHex = Convert.ToHexString(payload),
                        payloadUtf8 = TryDecodeUtf8(payload),
                        parsed = TryParseKnownPayload(sender.Uuid, payload)
                    });
            }
            catch (Exception exception)
            {
                logger.Log(
                    "ble.exercise.notification_failed",
                    new
                    {
                        label,
                        serviceUuid = characteristic.Service.Uuid,
                        characteristicUuid = characteristic.Uuid,
                        error = exception.Message,
                        hresult = $"0x{exception.HResult:X8}"
                    });
            }
        }
    }

    private async Task TrySubscribeAsync(
        GattCharacteristic characteristic,
        string label,
        CancellationToken cancellationToken)
    {
        if (!CharacteristicCanNotify(characteristic.CharacteristicProperties))
        {
            logger.Log(
                "ble.exercise.subscription_skipped",
                new
                {
                    label,
                    serviceUuid = characteristic.Service.Uuid,
                    characteristicUuid = characteristic.Uuid,
                    properties = characteristic.CharacteristicProperties.ToString()
                });
            return;
        }

        try
        {
            var readResult = await characteristic.ReadClientCharacteristicConfigurationDescriptorAsync()
                .AsTask(cancellationToken);
            logger.Log(
                "ble.exercise.cccd_read",
                new
                {
                    label,
                    serviceUuid = characteristic.Service.Uuid,
                    characteristicUuid = characteristic.Uuid,
                    status = readResult.Status.ToString(),
                    protocolError = readResult.ProtocolError,
                    cccd = readResult.ClientCharacteristicConfigurationDescriptor.ToString()
                });
        }
        catch (Exception exception)
        {
            logger.Log(
                "ble.exercise.cccd_read_failed",
                new
                {
                    label,
                    serviceUuid = characteristic.Service.Uuid,
                    characteristicUuid = characteristic.Uuid,
                    error = exception.Message,
                    hresult = $"0x{exception.HResult:X8}"
                });
        }

        var desiredValue = characteristic.CharacteristicProperties.HasFlag(GattCharacteristicProperties.Notify)
            ? GattClientCharacteristicConfigurationDescriptorValue.Notify
            : GattClientCharacteristicConfigurationDescriptorValue.Indicate;

        try
        {
            var writeResult = await characteristic
                .WriteClientCharacteristicConfigurationDescriptorWithResultAsync(desiredValue)
                .AsTask(cancellationToken);

            logger.Log(
                "ble.exercise.subscription_result",
                new
                {
                    label,
                    serviceUuid = characteristic.Service.Uuid,
                    characteristicUuid = characteristic.Uuid,
                    mode = desiredValue.ToString(),
                    status = writeResult.Status.ToString(),
                    protocolError = writeResult.ProtocolError
                });
        }
        catch (Exception exception)
        {
            logger.Log(
                "ble.exercise.subscription_failed",
                new
                {
                    label,
                    serviceUuid = characteristic.Service.Uuid,
                    characteristicUuid = characteristic.Uuid,
                    mode = desiredValue.ToString(),
                    error = exception.Message,
                    hresult = $"0x{exception.HResult:X8}"
                });
        }
    }

    private async Task WriteAsync(
        GattCharacteristic characteristic,
        string label,
        byte[] payload,
        CancellationToken cancellationToken)
    {
        if (!CharacteristicCanWrite(characteristic.CharacteristicProperties))
        {
            logger.Log(
                "ble.exercise.write_skipped",
                new
                {
                    label,
                    serviceUuid = characteristic.Service.Uuid,
                    characteristicUuid = characteristic.Uuid,
                    properties = characteristic.CharacteristicProperties.ToString(),
                    payloadHex = Convert.ToHexString(payload)
                });
            return;
        }

        try
        {
            var buffer = CryptographicBuffer.CreateFromByteArray(payload);
            var writeResult = await characteristic.WriteValueWithResultAsync(buffer).AsTask(cancellationToken);
            logger.Log(
                "ble.exercise.write_result",
                new
                {
                    label,
                    serviceUuid = characteristic.Service.Uuid,
                    characteristicUuid = characteristic.Uuid,
                    payloadLength = payload.Length,
                    payloadHex = Convert.ToHexString(payload),
                    status = writeResult.Status.ToString(),
                    protocolError = writeResult.ProtocolError
                });
        }
        catch (Exception exception)
        {
            logger.Log(
                "ble.exercise.write_failed",
                new
                {
                    label,
                    serviceUuid = characteristic.Service.Uuid,
                    characteristicUuid = characteristic.Uuid,
                    payloadLength = payload.Length,
                    payloadHex = Convert.ToHexString(payload),
                    error = exception.Message,
                    hresult = $"0x{exception.HResult:X8}"
                });
        }
    }

    private async Task TryReadAsync(
        GattCharacteristic characteristic,
        string label,
        BluetoothCacheMode cacheMode,
        CancellationToken cancellationToken)
    {
        if (!characteristic.CharacteristicProperties.HasFlag(GattCharacteristicProperties.Read))
        {
            logger.Log(
                "ble.exercise.read_skipped",
                new
                {
                    label,
                    serviceUuid = characteristic.Service.Uuid,
                    characteristicUuid = characteristic.Uuid,
                    properties = characteristic.CharacteristicProperties.ToString(),
                    cacheMode = cacheMode.ToString()
                });
            return;
        }

        try
        {
            var readResult = await characteristic.ReadValueAsync(cacheMode).AsTask(cancellationToken);
            var payload = readResult.Status == GattCommunicationStatus.Success
                ? ReadBytes(readResult.Value)
                : [];

            logger.Log(
                "ble.exercise.read_result",
                new
                {
                    label,
                    serviceUuid = characteristic.Service.Uuid,
                    characteristicUuid = characteristic.Uuid,
                    cacheMode = cacheMode.ToString(),
                    status = readResult.Status.ToString(),
                    protocolError = readResult.ProtocolError,
                    payloadLength = payload.Length,
                    payloadHex = payload.Length > 0 ? Convert.ToHexString(payload) : null,
                    payloadUtf8 = TryDecodeUtf8(payload)
                });
        }
        catch (Exception exception)
        {
            logger.Log(
                "ble.exercise.read_failed",
                new
                {
                    label,
                    serviceUuid = characteristic.Service.Uuid,
                    characteristicUuid = characteristic.Uuid,
                    cacheMode = cacheMode.ToString(),
                    error = exception.Message,
                    hresult = $"0x{exception.HResult:X8}"
                });
        }
    }

    private static object? TryParseKnownPayload(Guid characteristicUuid, byte[] payload)
    {
        if (characteristicUuid == AmsUuids.RemoteCommand)
        {
            return new
            {
                availableCommands = payload.Select(value => value.ToString()).ToArray()
            };
        }

        if (characteristicUuid == AmsUuids.EntityUpdate && payload.Length >= 3)
        {
            return new
            {
                entityId = payload[0],
                attributeId = payload[1],
                flags = payload[2],
                attributeUtf8 = TryDecodeUtf8(payload.AsSpan(3).ToArray())
            };
        }

        return null;
    }

    private static bool CharacteristicCanNotify(GattCharacteristicProperties properties)
    {
        return properties.HasFlag(GattCharacteristicProperties.Notify) ||
               properties.HasFlag(GattCharacteristicProperties.Indicate);
    }

    private static bool CharacteristicCanWrite(GattCharacteristicProperties properties)
    {
        return properties.HasFlag(GattCharacteristicProperties.Write) ||
               properties.HasFlag(GattCharacteristicProperties.WriteWithoutResponse);
    }

    private static async Task PauseAsync(CancellationToken cancellationToken)
    {
        await Task.Delay(350, cancellationToken);
    }

    private static byte[] ReadBytes(IBuffer buffer)
    {
        var bytes = new byte[buffer.Length];
        DataReader.FromBuffer(buffer).ReadBytes(bytes);
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
}
