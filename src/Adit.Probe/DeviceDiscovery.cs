using Windows.Devices.Bluetooth;
using Windows.Devices.Enumeration;

namespace Adit.Probe;

internal static class DeviceDiscovery
{
    private const string DeviceAddressProperty = "System.Devices.Aep.DeviceAddress";
    private const string BluetoothAddressProperty = "System.DeviceInterface.Bluetooth.DeviceAddress";
    private const string ContainerIdProperty = "System.Devices.Aep.ContainerId";
    private const string CategoryProperty = "System.Devices.Aep.Category";
    private const string IsConnectedProperty = "System.Devices.Aep.IsConnected";
    private const string IsPresentProperty = "System.Devices.Aep.IsPresent";

    public static async Task<IReadOnlyList<PairedDeviceRecord>> ListPairedBluetoothLeDevicesAsync()
    {
        var selector = BluetoothLEDevice.GetDeviceSelectorFromPairingState(true);
        var additionalProperties = new[] { DeviceAddressProperty, IsConnectedProperty };
        var devices = await DeviceInformation.FindAllAsync(selector, additionalProperties);

        return devices
            .Select(
                device => new PairedDeviceRecord(
                    device.Id,
                    string.IsNullOrWhiteSpace(device.Name) ? "(unnamed)" : device.Name,
                    device.Pairing.IsPaired,
                    GetStringProperty(device, DeviceAddressProperty),
                    GetBoolProperty(device, IsConnectedProperty)))
            .OrderBy(device => device.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public static async Task<IReadOnlyList<BluetoothEndpointRecord>> ListPairedBluetoothEndpointsAsync()
    {
        var additionalProperties = new[]
        {
            DeviceAddressProperty,
            BluetoothAddressProperty,
            ContainerIdProperty,
            CategoryProperty,
            IsConnectedProperty,
            IsPresentProperty
        };

        var classicDevices = await DeviceInformation.FindAllAsync(
            BluetoothDevice.GetDeviceSelectorFromPairingState(true),
            additionalProperties);
        var lowEnergyDevices = await DeviceInformation.FindAllAsync(
            BluetoothLEDevice.GetDeviceSelectorFromPairingState(true),
            additionalProperties);

        return classicDevices
            .Select(device => MapEndpoint(device, "classic"))
            .Concat(lowEnergyDevices.Select(device => MapEndpoint(device, "le")))
            .OrderBy(device => device.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(device => device.Transport, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public static PairedDeviceRecord? SelectTarget(
        IReadOnlyList<PairedDeviceRecord> devices,
        ProbeOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.DeviceId))
        {
            return devices.FirstOrDefault(
                device => string.Equals(
                    device.Id,
                    options.DeviceId,
                    StringComparison.OrdinalIgnoreCase));
        }

        if (!string.IsNullOrWhiteSpace(options.NameContains))
        {
            return devices.FirstOrDefault(
                device => device.Name.Contains(
                    options.NameContains,
                    StringComparison.OrdinalIgnoreCase));
        }

        var iphones = devices
            .Where(device => device.Name.Contains("iphone", StringComparison.OrdinalIgnoreCase))
            .ToArray();

        return iphones.Length == 1 ? iphones[0] : null;
    }

    public static BluetoothEndpointRecord? SelectClassicTarget(
        IReadOnlyList<BluetoothEndpointRecord> endpoints,
        ProbeOptions options)
    {
        var classicEndpoints = endpoints
            .Where(endpoint => string.Equals(endpoint.Transport, "classic", StringComparison.OrdinalIgnoreCase))
            .ToArray();

        if (!string.IsNullOrWhiteSpace(options.DeviceId))
        {
            return classicEndpoints.FirstOrDefault(
                endpoint => string.Equals(
                    endpoint.Id,
                    options.DeviceId,
                    StringComparison.OrdinalIgnoreCase));
        }

        if (!string.IsNullOrWhiteSpace(options.NameContains))
        {
            return classicEndpoints.FirstOrDefault(
                endpoint => endpoint.Name.Contains(
                    options.NameContains,
                    StringComparison.OrdinalIgnoreCase));
        }

        var iphones = classicEndpoints
            .Where(endpoint => endpoint.Name.Contains("iphone", StringComparison.OrdinalIgnoreCase))
            .ToArray();

        return iphones.Length == 1 ? iphones[0] : null;
    }

    public static void WriteDevices(IReadOnlyList<PairedDeviceRecord> devices)
    {
        if (devices.Count == 0)
        {
            Console.WriteLine("No paired BLE devices found.");
            return;
        }

        Console.WriteLine("Paired BLE devices:");
        foreach (var device in devices)
        {
            Console.WriteLine($"- Name: {device.Name}");
            Console.WriteLine($"  Connected: {device.IsConnected?.ToString() ?? "unknown"}");
            Console.WriteLine($"  Address: {device.Address ?? "unknown"}");
            Console.WriteLine($"  Id: {device.Id}");
        }
    }

    public static void WriteBluetoothEndpoints(IReadOnlyList<BluetoothEndpointRecord> endpoints)
    {
        if (endpoints.Count == 0)
        {
            Console.WriteLine("No paired Bluetooth endpoints found.");
            return;
        }

        Console.WriteLine("Paired Bluetooth endpoints:");
        foreach (var endpoint in endpoints)
        {
            Console.WriteLine($"- Name: {endpoint.Name}");
            Console.WriteLine($"  Transport: {endpoint.Transport}");
            Console.WriteLine($"  Paired: {endpoint.IsPaired}");
            Console.WriteLine($"  Connected: {endpoint.IsConnected?.ToString() ?? "unknown"}");
            Console.WriteLine($"  Present: {endpoint.IsPresent?.ToString() ?? "unknown"}");
            Console.WriteLine($"  AEP Address: {endpoint.AepAddress ?? "unknown"}");
            Console.WriteLine($"  BT Address: {endpoint.BluetoothAddress ?? "unknown"}");
            Console.WriteLine($"  Container: {endpoint.ContainerId ?? "unknown"}");
            Console.WriteLine($"  Category: {endpoint.Category ?? "unknown"}");
            Console.WriteLine($"  Id: {endpoint.Id}");
        }
    }

    private static string? GetStringProperty(DeviceInformation device, string propertyName)
    {
        if (!device.Properties.TryGetValue(propertyName, out var value) || value is null)
        {
            return null;
        }

        return value.ToString();
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
            device.Pairing.IsPaired,
            GetStringProperty(device, DeviceAddressProperty),
            GetStringProperty(device, BluetoothAddressProperty),
            GetBoolProperty(device, IsConnectedProperty),
            GetBoolProperty(device, IsPresentProperty),
            GetStringProperty(device, ContainerIdProperty),
            GetStringProperty(device, CategoryProperty));
    }
}

internal sealed record PairedDeviceRecord(
    string Id,
    string Name,
    bool IsPaired,
    string? Address,
    bool? IsConnected);

internal sealed record BluetoothEndpointRecord(
    string Transport,
    string Id,
    string Name,
    bool IsPaired,
    string? AepAddress,
    string? BluetoothAddress,
    bool? IsConnected,
    bool? IsPresent,
    string? ContainerId,
    string? Category);
