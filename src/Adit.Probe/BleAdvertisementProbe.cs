using Windows.Devices.Bluetooth.Advertisement;
using Windows.Storage.Streams;

namespace Adit.Probe;

internal sealed class BleAdvertisementProbe
{
    private const ushort AppleCompanyId = 0x004C;
    private readonly ProbeLogger logger;
    private readonly ProbeOptions options;
    private readonly PairedDeviceRecord target;
    private readonly object gate = new();
    private readonly Dictionary<ulong, AdvertisementAggregate> aggregates = [];
    private readonly HashSet<ulong> matchedAddresses = [];
    private int matchedAdvertisementCount;
    private int totalAdvertisementCount;

    public BleAdvertisementProbe(
        PairedDeviceRecord target,
        ProbeOptions options,
        ProbeLogger logger)
    {
        this.target = target;
        this.options = options;
        this.logger = logger;
    }

    public async Task<int> RunAsync(CancellationToken cancellationToken)
    {
        var watcher = new BluetoothLEAdvertisementWatcher
        {
            ScanningMode = BluetoothLEScanningMode.Active
        };

        watcher.Received += OnAdvertisementReceived;
        watcher.Stopped += OnWatcherStopped;

        logger.Log(
            "ble.adv_probe_started",
            new
            {
                target.Name,
                target.Address,
                target.IsConnected,
                scanningMode = watcher.ScanningMode.ToString(),
                watchSeconds = GetWatchSeconds()
            });

        try
        {
            watcher.Start();
            await Task.Delay(TimeSpan.FromSeconds(GetWatchSeconds()), cancellationToken);
            watcher.Stop();

            lock (gate)
            {
                logger.Log(
                    "ble.adv_probe_summary",
                    new
                    {
                        target.Name,
                        totalAdvertisementCount,
                        matchedAdvertisementCount,
                        matchedAddresses = matchedAddresses
                            .Select(FormatBluetoothAddress)
                            .ToArray(),
                        topAddresses = aggregates
                            .OrderByDescending(pair => pair.Value.Count)
                            .ThenBy(pair => pair.Key)
                            .Take(10)
                            .Select(
                                pair => new
                                {
                                    bluetoothAddress = FormatBluetoothAddress(pair.Key),
                                    count = pair.Value.Count,
                                    appleManufacturerCount = pair.Value.AppleManufacturerCount,
                                    lastSignalStrengthInDbm = pair.Value.LastSignalStrengthInDbm,
                                    localNames = pair.Value.LocalNames.OrderBy(name => name).ToArray(),
                                    manufacturerIds = pair.Value.ManufacturerIds
                                        .OrderBy(id => id)
                                        .Select(id => $"0x{id:X4}")
                                        .ToArray(),
                                    serviceUuids = pair.Value.ServiceUuids.OrderBy(uuid => uuid).ToArray()
                                })
                            .ToArray(),
                        appleAddresses = aggregates
                            .Where(pair => pair.Value.AppleManufacturerCount > 0)
                            .OrderByDescending(pair => pair.Value.AppleManufacturerCount)
                            .ThenByDescending(pair => pair.Value.Count)
                            .Take(10)
                            .Select(
                                pair => new
                                {
                                    bluetoothAddress = FormatBluetoothAddress(pair.Key),
                                    count = pair.Value.Count,
                                    appleManufacturerCount = pair.Value.AppleManufacturerCount,
                                    lastSignalStrengthInDbm = pair.Value.LastSignalStrengthInDbm,
                                    localNames = pair.Value.LocalNames.OrderBy(name => name).ToArray(),
                                    manufacturerIds = pair.Value.ManufacturerIds
                                        .OrderBy(id => id)
                                        .Select(id => $"0x{id:X4}")
                                        .ToArray(),
                                    serviceUuids = pair.Value.ServiceUuids.OrderBy(uuid => uuid).ToArray()
                                })
                            .ToArray()
                    });
            }

            return 0;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            watcher.Stop();
            return 0;
        }
        catch (Exception exception)
        {
            logger.Log("ble.adv_probe_failed", new { error = exception.ToString() });
            return 1;
        }
        finally
        {
            watcher.Received -= OnAdvertisementReceived;
            watcher.Stopped -= OnWatcherStopped;
        }
    }

    private void OnAdvertisementReceived(
        BluetoothLEAdvertisementWatcher sender,
        BluetoothLEAdvertisementReceivedEventArgs args)
    {
        try
        {
            lock (gate)
            {
                totalAdvertisementCount++;
            }

            var localName = args.Advertisement.LocalName;
            var targetAddressMatches = TryParseBluetoothAddress(target.Address, out var targetAddress)
                && targetAddress == args.BluetoothAddress;
            var nameMatches = !string.IsNullOrWhiteSpace(localName)
                && localName.Contains(target.Name, StringComparison.OrdinalIgnoreCase);
            var genericNameMatches = !string.IsNullOrWhiteSpace(localName)
                && target.Name.Contains("iphone", StringComparison.OrdinalIgnoreCase)
                && localName.Contains("iphone", StringComparison.OrdinalIgnoreCase);
            var serviceUuids = args.Advertisement.ServiceUuids
                .Select(uuid => uuid.ToString())
                .ToArray();
            var manufacturerData = args.Advertisement.ManufacturerData
                .Select(
                    item => new ManufacturerAdvertisement(
                        item.CompanyId,
                        Convert.ToHexString(ReadBytes(item.Data))))
                .ToArray();
            var hasAppleManufacturerData = manufacturerData.Any(item => item.CompanyId == AppleCompanyId);

            if (!targetAddressMatches && !nameMatches && !genericNameMatches)
            {
                lock (gate)
                {
                    UpdateAggregate(args.BluetoothAddress, localName, serviceUuids, manufacturerData, args.RawSignalStrengthInDBm);
                }

                if (!hasAppleManufacturerData && serviceUuids.Length == 0 && string.IsNullOrWhiteSpace(localName))
                {
                    return;
                }
            }
            else
            {
                lock (gate)
                {
                    matchedAdvertisementCount++;
                    matchedAddresses.Add(args.BluetoothAddress);
                    UpdateAggregate(args.BluetoothAddress, localName, serviceUuids, manufacturerData, args.RawSignalStrengthInDBm);
                }
            }

            logger.Log(
                "ble.adv_received",
                new
                {
                    receivedAt = args.Timestamp.ToString("O"),
                    bluetoothAddress = FormatBluetoothAddress(args.BluetoothAddress),
                    localName,
                    eventType = args.AdvertisementType.ToString(),
                    rawSignalStrengthInDbm = args.RawSignalStrengthInDBm,
                    isAnonymous = args.IsAnonymous,
                    isConnectable = args.IsConnectable,
                    isDirected = args.IsDirected,
                    isScannable = args.IsScannable,
                    serviceUuids,
                    manufacturerData,
                    dataSections = args.Advertisement.DataSections
                        .Select(
                            section => new
                            {
                                dataType = $"0x{section.DataType:X2}",
                                payloadHex = Convert.ToHexString(ReadBytes(section.Data)),
                                payloadUtf8 = TryDecodeUtf8(ReadBytes(section.Data))
                            })
                        .ToArray()
                });
        }
        catch (Exception exception)
        {
            logger.Log("ble.adv_received_error", new { error = exception.ToString() });
        }
    }

    private void UpdateAggregate(
        ulong bluetoothAddress,
        string? localName,
        IReadOnlyList<string> serviceUuids,
        IReadOnlyList<ManufacturerAdvertisement> manufacturerData,
        int signalStrengthInDbm)
    {
        if (!aggregates.TryGetValue(bluetoothAddress, out var aggregate))
        {
            aggregate = new AdvertisementAggregate();
            aggregates[bluetoothAddress] = aggregate;
        }

        aggregate.Count++;
        aggregate.LastSignalStrengthInDbm = signalStrengthInDbm;

        if (!string.IsNullOrWhiteSpace(localName))
        {
            aggregate.LocalNames.Add(localName);
        }

        foreach (var serviceUuid in serviceUuids)
        {
            aggregate.ServiceUuids.Add(serviceUuid);
        }

        foreach (var manufacturerEntry in manufacturerData)
        {
            aggregate.ManufacturerIds.Add(manufacturerEntry.CompanyId);
            if (manufacturerEntry.CompanyId == AppleCompanyId)
            {
                aggregate.AppleManufacturerCount++;
            }
        }
    }

    private void OnWatcherStopped(
        BluetoothLEAdvertisementWatcher sender,
        BluetoothLEAdvertisementWatcherStoppedEventArgs args)
    {
        logger.Log(
            "ble.adv_probe_stopped",
            new
            {
                error = args.Error.ToString()
            });
    }

    private int GetWatchSeconds()
    {
        return options.MapWatchSeconds > 0 ? options.MapWatchSeconds : 15;
    }

    private static bool TryParseBluetoothAddress(string? value, out ulong address)
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
            Enumerable.Range(0, 6)
                .Select(index => hex.Substring(index * 2, 2)));
    }

    private static byte[] ReadBytes(IBuffer buffer)
    {
        var bytes = new byte[buffer.Length];
        DataReader.FromBuffer(buffer).ReadBytes(bytes);
        return bytes;
    }

    private static string? TryDecodeUtf8(byte[] value)
    {
        try
        {
            var decoded = System.Text.Encoding.UTF8.GetString(value);
            return decoded.Any(character => char.IsControl(character) && !char.IsWhiteSpace(character))
                ? null
                : decoded;
        }
        catch
        {
            return null;
        }
    }

    private sealed class AdvertisementAggregate
    {
        public int AppleManufacturerCount { get; set; }

        public int Count { get; set; }

        public int LastSignalStrengthInDbm { get; set; }

        public HashSet<string> LocalNames { get; } = [];

        public HashSet<ushort> ManufacturerIds { get; } = [];

        public HashSet<string> ServiceUuids { get; } = [];
    }

    private sealed record ManufacturerAdvertisement(ushort CompanyId, string PayloadHex);
}
