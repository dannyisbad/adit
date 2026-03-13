using Windows.Devices.Bluetooth;
using Windows.Devices.Bluetooth.GenericAttributeProfile;
using Windows.Security.Cryptography;
using Windows.Storage.Streams;

namespace Adit.Probe;

internal sealed class RawBleAddressProbe
{
    private readonly ProbeLogger logger;
    private readonly ProbeOptions options;
    private readonly string rawAddress;

    public RawBleAddressProbe(string rawAddress, ProbeOptions options, ProbeLogger logger)
    {
        this.rawAddress = rawAddress;
        this.options = options;
        this.logger = logger;
    }

    public async Task<int> RunAsync(CancellationToken cancellationToken)
    {
        if (!TryParseBluetoothAddress(rawAddress, out var bluetoothAddress))
        {
            logger.Log("ble.address_probe_invalid_address", new { rawAddress });
            return 1;
        }

        try
        {
            using var device = await BluetoothLEDevice.FromBluetoothAddressAsync(bluetoothAddress);
            if (device is null)
            {
                logger.Log("ble.address_probe_open_failed", new { rawAddress });
                return 1;
            }

            logger.Log(
                "ble.address_probe_opened",
                new
                {
                    requestedAddress = FormatBluetoothAddress(bluetoothAddress),
                    device.DeviceId,
                    device.Name,
                    connectionStatus = device.ConnectionStatus.ToString(),
                    bluetoothAddress = FormatBluetoothAddress(device.BluetoothAddress),
                    access = await GetDeviceAccessSnapshotAsync(device)
                });

            using var session = await GattSession.FromDeviceIdAsync(device.BluetoothDeviceId);
            if (session is not null)
            {
                session.MaintainConnection = true;
                logger.Log(
                    "ble.address_probe_session",
                    new
                    {
                        requestedAddress = FormatBluetoothAddress(bluetoothAddress),
                        canMaintainConnection = session.CanMaintainConnection,
                        sessionStatus = session.SessionStatus.ToString()
                    });
            }

            var servicesResult = await device.GetGattServicesAsync(BluetoothCacheMode.Uncached);
            logger.Log(
                "ble.address_probe_service_query",
                new
                {
                    requestedAddress = FormatBluetoothAddress(bluetoothAddress),
                    status = servicesResult.Status.ToString(),
                    protocolError = servicesResult.ProtocolError,
                    serviceCount = servicesResult.Services.Count,
                    serviceUuids = servicesResult.Services.Select(service => service.Uuid).ToArray()
                });

            if (servicesResult.Status == GattCommunicationStatus.Success)
            {
                var hasAncs = servicesResult.Services.Any(service => service.Uuid == AncsUuids.Service);
                await LogInventoryAsync(bluetoothAddress, servicesResult.Services);

                if (hasAncs && options.MapWatchSeconds > 0)
                {
                    logger.Log(
                        "ble.address_probe_ancs_handoff",
                        new
                        {
                            requestedAddress = FormatBluetoothAddress(bluetoothAddress),
                            device.DeviceId,
                            device.Name,
                            watchSeconds = options.MapWatchSeconds
                        });

                    var syntheticTarget = new PairedDeviceRecord(
                        device.DeviceId,
                        string.IsNullOrWhiteSpace(device.Name)
                            ? $"Bluetooth {FormatBluetoothAddress(bluetoothAddress)}"
                            : device.Name,
                        false,
                        FormatBluetoothAddress(bluetoothAddress),
                        device.ConnectionStatus == BluetoothConnectionStatus.Connected);

                    var ancsProbe = new AncsProbe(syntheticTarget, options, logger);
                    return await ancsProbe.RunAsync(cancellationToken);
                }
            }

            return 0;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return 0;
        }
        catch (Exception exception)
        {
            logger.Log(
                "ble.address_probe_failed",
                new
                {
                    rawAddress,
                    error = exception.ToString()
                });
            return 1;
        }
    }

    private async Task LogInventoryAsync(
        ulong bluetoothAddress,
        IReadOnlyList<GattDeviceService> services)
    {
        var inventory = new List<object>(services.Count);

        foreach (var service in services)
        {
            using (service)
            {
                var characteristicsResult = await service.GetCharacteristicsAsync(BluetoothCacheMode.Uncached);
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
                        serviceUuid = service.Uuid,
                        status = characteristicsResult.Status.ToString(),
                        protocolError = characteristicsResult.ProtocolError,
                        characteristicCount = characteristicsResult.Characteristics.Count,
                        characteristics
                    });
            }
        }

        logger.Log(
            "ble.address_probe_inventory",
            new
            {
                requestedAddress = FormatBluetoothAddress(bluetoothAddress),
                services = inventory
            });
    }

    private static async Task<object?> TryReadCharacteristicAsync(GattCharacteristic characteristic)
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

    private static async Task<object> GetDeviceAccessSnapshotAsync(BluetoothLEDevice bluetoothDevice)
    {
        var requestStatus = await bluetoothDevice.RequestAccessAsync();
        var accessInformation = Windows.Devices.Enumeration.DeviceAccessInformation.CreateFromId(
            bluetoothDevice.DeviceId);

        return new
        {
            currentStatus = accessInformation.CurrentStatus.ToString(),
            requestStatus = requestStatus.ToString()
        };
    }

    private static bool TryParseBluetoothAddress(string value, out ulong address)
    {
        address = 0;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var hex = value.Replace(":", string.Empty, StringComparison.Ordinal)
            .Replace("-", string.Empty, StringComparison.Ordinal);
        return ulong.TryParse(
            hex,
            System.Globalization.NumberStyles.HexNumber,
            System.Globalization.CultureInfo.InvariantCulture,
            out address);
    }

    private static string FormatBluetoothAddress(ulong address)
    {
        var hex = address.ToString("X12", System.Globalization.CultureInfo.InvariantCulture);
        return string.Join(
            ":",
            Enumerable.Range(0, 6).Select(index => hex.Substring(index * 2, 2)));
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
