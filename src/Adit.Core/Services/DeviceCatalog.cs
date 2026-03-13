using System.Collections;
using Adit.Core.Models;
using Windows.Devices.Bluetooth;
using Windows.Devices.Enumeration;

namespace Adit.Core.Services;

public sealed class DeviceCatalog
{
    private static readonly TimeSpan DefaultLeWatchTimeout = TimeSpan.FromSeconds(12);
    private static readonly TimeSpan DefaultConnectedLeWatchTimeout = TimeSpan.FromSeconds(10);
    private const string DeviceAddressProperty = "System.Devices.Aep.DeviceAddress";
    private const string BluetoothAddressProperty = "System.DeviceInterface.Bluetooth.DeviceAddress";
    private const string ContainerIdProperty = "System.Devices.Aep.ContainerId";
    private const string CategoryProperty = "System.Devices.Aep.Category";
    private const string IsConnectedProperty = "System.Devices.Aep.IsConnected";
    private const string IsPairedProperty = "System.Devices.Aep.IsPaired";
    private const string IsPresentProperty = "System.Devices.Aep.IsPresent";
    private static readonly string[] LeAdditionalProperties =
    [
        DeviceAddressProperty,
        BluetoothAddressProperty,
        IsConnectedProperty,
        IsPairedProperty,
        IsPresentProperty,
        ContainerIdProperty
    ];

    public async Task<IReadOnlyList<BluetoothLeDeviceRecord>> ListPairedBluetoothLeDevicesAsync()
    {
        return await ListBluetoothLeDeviceInterfacesAsync(BluetoothLEDevice.GetDeviceSelectorFromPairingState(true));
    }

    public async Task<IReadOnlyList<BluetoothLeDeviceRecord>> ListConnectedBluetoothLeDevicesAsync()
    {
        return await ListBluetoothLeDeviceInterfacesAsync(
            BluetoothLEDevice.GetDeviceSelectorFromConnectionStatus(BluetoothConnectionStatus.Connected));
    }

    public async Task<IReadOnlyList<BluetoothEndpointRecord>> ListPairedBluetoothEndpointsAsync()
    {
        var additionalProperties = new[]
        {
            DeviceAddressProperty,
            BluetoothAddressProperty,
            ContainerIdProperty,
            CategoryProperty,
            IsConnectedProperty,
            IsPairedProperty,
            IsPresentProperty
        };

        var classicDevices = await FindAssociationEndpointsAsync(
            paired: true,
            BluetoothPairingConventions.ClassicTransport,
            additionalProperties);
        var lowEnergyDevices = await FindAssociationEndpointsAsync(
            paired: true,
            BluetoothPairingConventions.LowEnergyTransport,
            additionalProperties);

        return classicDevices
            .Select(device => MapEndpoint(device, "classic"))
            .Concat(lowEnergyDevices.Select(device => MapEndpoint(device, "le")))
            .OrderBy(device => device.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(device => device.Transport, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public async Task<BluetoothEndpointRecord?> SelectClassicTargetAsync(string? deviceId, string? nameContains)
    {
        var endpoints = await ListPairedBluetoothEndpointsAsync();
        var classicEndpoints = endpoints
            .Where(endpoint => string.Equals(endpoint.Transport, "classic", StringComparison.OrdinalIgnoreCase))
            .ToArray();

        if (!string.IsNullOrWhiteSpace(deviceId))
        {
            return classicEndpoints.FirstOrDefault(
                endpoint => string.Equals(endpoint.Id, deviceId, StringComparison.OrdinalIgnoreCase));
        }

        var candidates = classicEndpoints
            .Where(
                endpoint => string.IsNullOrWhiteSpace(nameContains)
                    || endpoint.Name.Contains(nameContains, StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(endpoint => ScoreEndpoint(endpoint, nameContains))
            .ThenBy(endpoint => endpoint.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        return candidates.FirstOrDefault();
    }

    public async Task<BluetoothLeDeviceRecord?> SelectLeTargetAsync(
        string? deviceId,
        string? containerId,
        string? nameContains)
    {
        if (!string.IsNullOrWhiteSpace(deviceId))
        {
            var pairedMatch = (await ListPairedBluetoothLeDevicesAsync()).FirstOrDefault(
                device => string.Equals(device.Id, deviceId, StringComparison.OrdinalIgnoreCase));
            if (pairedMatch is not null)
            {
                return pairedMatch;
            }

            var connectedMatch = (await ListConnectedBluetoothLeDevicesAsync()).FirstOrDefault(
                device => string.Equals(device.Id, deviceId, StringComparison.OrdinalIgnoreCase));
            if (connectedMatch is not null)
            {
                return connectedMatch;
            }

            return (await ListBluetoothLeDevicesAsync(false)).FirstOrDefault(
                device => string.Equals(device.Id, deviceId, StringComparison.OrdinalIgnoreCase));
        }

        var pairedCandidates = (await ListPairedBluetoothLeDevicesAsync())
            .Where(
                device =>
                    (string.IsNullOrWhiteSpace(containerId)
                        || string.Equals(device.ContainerId, containerId, StringComparison.OrdinalIgnoreCase))
                    && (string.IsNullOrWhiteSpace(nameContains)
                        || device.Name.Contains(nameContains, StringComparison.OrdinalIgnoreCase)))
            .OrderByDescending(device => ScoreLeDevice(device, containerId, nameContains))
            .ThenBy(device => device.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (pairedCandidates.Length > 0)
        {
            return pairedCandidates[0];
        }

        var connectedCandidates = (await ListConnectedBluetoothLeDevicesAsync())
            .Where(device => MatchesConnectedLeTarget(device, containerId, nameContains))
            .OrderByDescending(device => ScoreConnectedLeDevice(device, containerId, nameContains))
            .ThenBy(device => device.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (connectedCandidates.Length > 0)
        {
            return connectedCandidates[0];
        }

        var visibleUnpairedCandidates = (await ListBluetoothLeDevicesAsync(false))
            .Where(
                device =>
                    (string.IsNullOrWhiteSpace(containerId)
                        || string.Equals(device.ContainerId, containerId, StringComparison.OrdinalIgnoreCase))
                    && (string.IsNullOrWhiteSpace(nameContains)
                        || device.Name.Contains(nameContains, StringComparison.OrdinalIgnoreCase)
                        || string.Equals(device.Name, "(unnamed)", StringComparison.OrdinalIgnoreCase)))
            .OrderByDescending(device => ScoreLeDevice(device, containerId, nameContains))
            .ThenBy(device => device.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        return visibleUnpairedCandidates.FirstOrDefault();
    }

    public async Task<BluetoothLeDeviceRecord?> WaitForPairedLeTargetAsync(
        string? containerId,
        string? nameContains,
        CancellationToken cancellationToken,
        TimeSpan? timeout = null)
    {
        var immediateMatch = (await ListPairedBluetoothLeDevicesAsync())
            .Where(device => MatchesLeTarget(device, containerId, nameContains))
            .OrderByDescending(device => ScoreLeDevice(device, containerId, nameContains))
            .ThenBy(device => device.Name, StringComparer.OrdinalIgnoreCase)
            .FirstOrDefault();
        if (immediateMatch is not null)
        {
            return immediateMatch;
        }

        return await WaitForPairedLeTargetViaWatcherAsync(
            containerId,
            nameContains,
            timeout ?? DefaultLeWatchTimeout,
            cancellationToken);
    }

    public async Task<BluetoothLeDeviceRecord?> WaitForConnectedLeTargetAsync(
        string? containerId,
        string? nameContains,
        CancellationToken cancellationToken,
        TimeSpan? timeout = null)
    {
        var immediateMatch = (await ListConnectedBluetoothLeDevicesAsync())
            .Where(device => MatchesConnectedLeTarget(device, containerId, nameContains))
            .OrderByDescending(device => ScoreConnectedLeDevice(device, containerId, nameContains))
            .ThenBy(device => device.Name, StringComparer.OrdinalIgnoreCase)
            .FirstOrDefault();
        if (immediateMatch is not null)
        {
            return immediateMatch;
        }

        return await WaitForLeTargetViaWatcherAsync(
            BluetoothLEDevice.GetDeviceSelectorFromConnectionStatus(BluetoothConnectionStatus.Connected),
            kind: null,
            containerId,
            nameContains,
            timeout ?? DefaultConnectedLeWatchTimeout,
            cancellationToken,
            MatchesConnectedLeTarget,
            ScoreConnectedLeDevice);
    }

    private static async Task<IReadOnlyList<BluetoothLeDeviceRecord>> ListBluetoothLeDevicesAsync(bool paired)
    {
        var devices = await FindAssociationEndpointsAsync(
            paired,
            BluetoothPairingConventions.LowEnergyTransport,
            LeAdditionalProperties);

        return devices
            .Select(MapLeDevice)
            .OrderBy(device => device.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static async Task<IReadOnlyList<BluetoothLeDeviceRecord>> ListBluetoothLeDeviceInterfacesAsync(string selector)
    {
        var devices = await DeviceInformation.FindAllAsync(selector, LeAdditionalProperties);

        return devices
            .Select(MapLeDevice)
            .OrderBy(device => device.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static async Task<DeviceInformationCollection> FindAssociationEndpointsAsync(
        bool paired,
        string? transport,
        IReadOnlyList<string> additionalProperties)
    {
        var selector = BluetoothPairingConventions.BuildAssociationEndpointSelector(paired, transport);
        return await DeviceInformation.FindAllAsync(
            selector,
            additionalProperties,
            DeviceInformationKind.AssociationEndpoint);
    }

    private static async Task<BluetoothLeDeviceRecord?> WaitForPairedLeTargetViaWatcherAsync(
        string? containerId,
        string? nameContains,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var selector = BluetoothPairingConventions.BuildAssociationEndpointSelector(
            paired: true,
            BluetoothPairingConventions.LowEnergyTransport);
        return await WaitForLeTargetViaWatcherAsync(
            selector,
            DeviceInformationKind.AssociationEndpoint,
            containerId,
            nameContains,
            timeout,
            cancellationToken,
            MatchesLeTarget,
            ScoreLeDevice);
    }

    private static async Task<BluetoothLeDeviceRecord?> WaitForLeTargetViaWatcherAsync(
        string selector,
        DeviceInformationKind? kind,
        string? containerId,
        string? nameContains,
        TimeSpan timeout,
        CancellationToken cancellationToken,
        Func<BluetoothLeDeviceRecord, string?, string?, bool> match,
        Func<BluetoothLeDeviceRecord, string?, string?, int> score)
    {
        var watcher = DeviceInformation.CreateWatcher(
            selector,
            LeAdditionalProperties,
            kind ?? DeviceInformationKind.DeviceInterface);
        var sync = new object();
        var trackedDevices = new Dictionary<string, DeviceInformation>(StringComparer.OrdinalIgnoreCase);
        var matchTcs = new TaskCompletionSource<BluetoothLeDeviceRecord?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var completionTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        void TryMatch(DeviceInformation device)
        {
            var mapped = MapLeDevice(device);
            if (match(mapped, containerId, nameContains))
            {
                matchTcs.TrySetResult(mapped);
            }
        }

        void OnAdded(DeviceWatcher _, DeviceInformation device)
        {
            lock (sync)
            {
                trackedDevices[device.Id] = device;
            }

            TryMatch(device);
        }

        void OnUpdated(DeviceWatcher _, DeviceInformationUpdate update)
        {
            DeviceInformation? device = null;
            lock (sync)
            {
                if (trackedDevices.TryGetValue(update.Id, out var tracked))
                {
                    tracked.Update(update);
                    device = tracked;
                }
            }

            if (device is not null)
            {
                TryMatch(device);
            }
        }

        void OnRemoved(DeviceWatcher _, DeviceInformationUpdate update)
        {
            lock (sync)
            {
                trackedDevices.Remove(update.Id);
            }
        }

        void OnEnumerationCompleted(DeviceWatcher _, object __)
        {
            List<DeviceInformation> snapshot;
            lock (sync)
            {
                snapshot = trackedDevices.Values.ToList();
            }

            var matchedDevice = snapshot
                .Select(MapLeDevice)
                .Where(device => match(device, containerId, nameContains))
                .OrderByDescending(device => score(device, containerId, nameContains))
                .ThenBy(device => device.Name, StringComparer.OrdinalIgnoreCase)
                .FirstOrDefault();
            if (matchedDevice is not null)
            {
                matchTcs.TrySetResult(matchedDevice);
            }

            completionTcs.TrySetResult();
        }

        void OnStopped(DeviceWatcher _, object __)
        {
            completionTcs.TrySetResult();
        }

        watcher.Added += OnAdded;
        watcher.Updated += OnUpdated;
        watcher.Removed += OnRemoved;
        watcher.EnumerationCompleted += OnEnumerationCompleted;
        watcher.Stopped += OnStopped;

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linkedCts.CancelAfter(timeout);
        using var registration = linkedCts.Token.Register(() => matchTcs.TrySetResult(null));

        try
        {
            watcher.Start();
            return await matchTcs.Task.ConfigureAwait(false);
        }
        finally
        {
            watcher.Added -= OnAdded;
            watcher.Updated -= OnUpdated;
            watcher.Removed -= OnRemoved;
            watcher.EnumerationCompleted -= OnEnumerationCompleted;
            watcher.Stopped -= OnStopped;

            if (watcher.Status is DeviceWatcherStatus.Started or DeviceWatcherStatus.EnumerationCompleted)
            {
                watcher.Stop();
            }

            try
            {
                await completionTcs.Task.WaitAsync(TimeSpan.FromSeconds(2), CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
                // Ignore watcher teardown lag on the Windows Bluetooth stack.
            }
        }
    }

    private static int ScoreEndpoint(BluetoothEndpointRecord endpoint, string? nameContains)
    {
        var score = 0;

        if (endpoint.IsConnected == true)
        {
            score += 10;
        }

        if (endpoint.IsPresent == true)
        {
            score += 5;
        }

        if (!string.IsNullOrWhiteSpace(nameContains))
        {
            if (string.Equals(endpoint.Name, nameContains, StringComparison.OrdinalIgnoreCase))
            {
                score += 25;
            }
            else if (endpoint.Name.Contains(nameContains, StringComparison.OrdinalIgnoreCase))
            {
                score += 12;
            }
        }
        else if (endpoint.Name.Contains("iphone", StringComparison.OrdinalIgnoreCase))
        {
            score += 8;
        }

        return score;
    }

    private static int ScoreLeDevice(
        BluetoothLeDeviceRecord device,
        string? containerId,
        string? nameContains)
    {
        var score = 0;

        if (device.IsPaired)
        {
            score += 20;
        }

        if (device.IsConnected == true)
        {
            score += 15;
        }

        if (device.IsPresent == true)
        {
            score += 10;
        }

        if (!string.IsNullOrWhiteSpace(containerId)
            && string.Equals(device.ContainerId, containerId, StringComparison.OrdinalIgnoreCase))
        {
            score += 30;
        }

        if (!string.IsNullOrWhiteSpace(nameContains))
        {
            if (string.Equals(device.Name, nameContains, StringComparison.OrdinalIgnoreCase))
            {
                score += 20;
            }
            else if (device.Name.Contains(nameContains, StringComparison.OrdinalIgnoreCase))
            {
                score += 10;
            }
        }
        else if (device.Name.Contains("iphone", StringComparison.OrdinalIgnoreCase))
        {
            score += 8;
        }

        if (string.Equals(device.Name, "(unnamed)", StringComparison.OrdinalIgnoreCase))
        {
            score -= 2;
        }

        return score;
    }

    private static int ScoreConnectedLeDevice(
        BluetoothLeDeviceRecord device,
        string? containerId,
        string? nameContains)
    {
        var score = ScoreLeDevice(device, containerId, nameContains);

        if (device.IsConnected == true)
        {
            score += 20;
        }

        if (!string.IsNullOrWhiteSpace(containerId)
            && !string.Equals(device.ContainerId, containerId, StringComparison.OrdinalIgnoreCase))
        {
            score -= 10;
        }

        if (NamesLookRelated(device.Name, nameContains))
        {
            score += 8;
        }

        return score;
    }

    private static bool MatchesLeTarget(
        BluetoothLeDeviceRecord device,
        string? containerId,
        string? nameContains)
    {
        var matchesContainer = string.IsNullOrWhiteSpace(containerId)
            || string.Equals(device.ContainerId, containerId, StringComparison.OrdinalIgnoreCase);
        var matchesName = string.IsNullOrWhiteSpace(nameContains)
            || device.Name.Contains(nameContains, StringComparison.OrdinalIgnoreCase)
            || string.Equals(device.Name, "(unnamed)", StringComparison.OrdinalIgnoreCase);

        return matchesContainer && matchesName;
    }

    private static bool MatchesConnectedLeTarget(
        BluetoothLeDeviceRecord device,
        string? containerId,
        string? nameContains)
    {
        if (MatchesLeTarget(device, containerId, nameContains))
        {
            return true;
        }

        return device.IsConnected == true
            && NamesLookRelated(device.Name, nameContains)
            && (string.IsNullOrWhiteSpace(containerId)
                || string.Equals(device.ContainerId, containerId, StringComparison.OrdinalIgnoreCase)
                || device.Name.Contains("iphone", StringComparison.OrdinalIgnoreCase));
    }

    private static bool NamesLookRelated(string deviceName, string? requestedName)
    {
        if (string.IsNullOrWhiteSpace(requestedName))
        {
            return deviceName.Contains("iphone", StringComparison.OrdinalIgnoreCase);
        }

        if (deviceName.Contains(requestedName, StringComparison.OrdinalIgnoreCase)
            || requestedName.Contains(deviceName, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return deviceName.Contains("iphone", StringComparison.OrdinalIgnoreCase)
            && requestedName.Contains("iphone", StringComparison.OrdinalIgnoreCase);
    }

    private static BluetoothLeDeviceRecord MapLeDevice(DeviceInformation device)
    {
        return new BluetoothLeDeviceRecord(
            device.Id,
            string.IsNullOrWhiteSpace(device.Name) ? "(unnamed)" : device.Name,
            GetPairedState(device),
            GetStringProperty(device, DeviceAddressProperty) ?? GetStringProperty(device, BluetoothAddressProperty),
            GetBoolProperty(device, IsConnectedProperty),
            GetBoolProperty(device, IsPresentProperty),
            GetStringProperty(device, ContainerIdProperty));
    }

    private static string? GetStringProperty(DeviceInformation device, string propertyName)
    {
        if (!device.Properties.TryGetValue(propertyName, out var value) || value is null)
        {
            return null;
        }

        return value switch
        {
            string text => text,
            IEnumerable enumerable when value is not string => string.Join(
                ", ",
                enumerable.Cast<object?>()
                    .Where(item => item is not null)
                    .Select(item => item!.ToString())
                    .Where(item => !string.IsNullOrWhiteSpace(item))),
            _ => value.ToString()
        };
    }

    private static bool? GetBoolProperty(DeviceInformation device, string propertyName)
    {
        if (!device.Properties.TryGetValue(propertyName, out var value) || value is null)
        {
            return null;
        }

        return value switch
        {
            bool boolValue => boolValue,
            _ when bool.TryParse(value.ToString(), out var parsed) => parsed,
            _ => null
        };
    }

    private static BluetoothEndpointRecord MapEndpoint(DeviceInformation device, string transport)
    {
        return new BluetoothEndpointRecord(
            transport,
            device.Id,
            string.IsNullOrWhiteSpace(device.Name) ? "(unnamed)" : device.Name,
            GetPairedState(device),
            GetStringProperty(device, DeviceAddressProperty),
            GetStringProperty(device, BluetoothAddressProperty),
            GetBoolProperty(device, IsConnectedProperty),
            GetBoolProperty(device, IsPresentProperty),
            GetStringProperty(device, ContainerIdProperty),
            GetStringProperty(device, CategoryProperty));
    }

    private static bool GetPairedState(DeviceInformation device)
    {
        return GetBoolProperty(device, IsPairedProperty) ?? device.Pairing.IsPaired;
    }
}
