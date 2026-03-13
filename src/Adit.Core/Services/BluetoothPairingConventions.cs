using Windows.Devices.Bluetooth;
using Windows.Devices.Enumeration;

namespace Adit.Core.Services;

public static class BluetoothPairingConventions
{
    private const string ProtocolIdProperty = "System.Devices.Aep.ProtocolId";

    public static readonly Guid ClassicProtocolId = new("e0cbf06c-cd8b-4647-bb8a-263b43f0f974");
    public static readonly Guid LowEnergyProtocolId = new("bb7bb05e-5972-42b5-94fc-76eaa7084d49");

    public const string AnyTransport = "any";
    public const string ClassicTransport = "classic";
    public const string LowEnergyTransport = "le";
    public const string UnknownTransport = "unknown";

    public static string BuildAssociationEndpointSelector(bool paired, string? transport)
    {
        return NormalizeTransport(transport) switch
        {
            ClassicTransport => BluetoothDevice.GetDeviceSelectorFromPairingState(paired),
            LowEnergyTransport => BluetoothLEDevice.GetDeviceSelectorFromPairingState(paired),
            _ => $"({BluetoothDevice.GetDeviceSelectorFromPairingState(paired)}) OR ({BluetoothLEDevice.GetDeviceSelectorFromPairingState(paired)})"
        };
    }

    public static string NormalizeTransport(string? transport)
    {
        if (string.IsNullOrWhiteSpace(transport))
        {
            return AnyTransport;
        }

        return transport.Trim().ToLowerInvariant() switch
        {
            "classic" or "rfcomm" => ClassicTransport,
            "le" or "ble" or "lowenergy" => LowEnergyTransport,
            _ => AnyTransport
        };
    }

    public static string GetTransportName(DeviceInformation device)
    {
        var protocolId = GetStringProperty(device, ProtocolIdProperty);
        return GetTransportName(protocolId);
    }

    public static string GetTransportName(string? protocolId)
    {
        if (Guid.TryParse(protocolId, out var protocolGuid))
        {
            if (protocolGuid == ClassicProtocolId)
            {
                return ClassicTransport;
            }

            if (protocolGuid == LowEnergyProtocolId)
            {
                return LowEnergyTransport;
            }
        }

        return UnknownTransport;
    }

    internal static string? GetStringProperty(DeviceInformation device, string propertyName)
    {
        if (!device.Properties.TryGetValue(propertyName, out var value) || value is null)
        {
            return null;
        }

        return value.ToString();
    }
}
