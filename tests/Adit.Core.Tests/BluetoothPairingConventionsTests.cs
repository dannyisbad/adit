using Adit.Core.Services;
using Windows.Devices.Bluetooth;

namespace Adit.Core.Tests;

public sealed class BluetoothPairingConventionsTests
{
    [Fact]
    public void BuildAssociationEndpointSelector_ForUnpairedLe_MatchesWinRtSelector()
    {
        var selector = BluetoothPairingConventions.BuildAssociationEndpointSelector(
            paired: false,
            transport: "ble");

        Assert.Equal(BluetoothLEDevice.GetDeviceSelectorFromPairingState(false), selector);
    }

    [Fact]
    public void BuildAssociationEndpointSelector_ForPairedClassic_MatchesWinRtSelector()
    {
        var selector = BluetoothPairingConventions.BuildAssociationEndpointSelector(
            paired: true,
            transport: "classic");

        Assert.Equal(BluetoothDevice.GetDeviceSelectorFromPairingState(true), selector);
    }

    [Theory]
    [InlineData(null, BluetoothPairingConventions.AnyTransport)]
    [InlineData("", BluetoothPairingConventions.AnyTransport)]
    [InlineData("rfcomm", BluetoothPairingConventions.ClassicTransport)]
    [InlineData("BLE", BluetoothPairingConventions.LowEnergyTransport)]
    [InlineData("weird", BluetoothPairingConventions.AnyTransport)]
    public void NormalizeTransport_ReturnsExpectedValue(string? input, string expected)
    {
        var actual = BluetoothPairingConventions.NormalizeTransport(input);
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData("e0cbf06c-cd8b-4647-bb8a-263b43f0f974", BluetoothPairingConventions.ClassicTransport)]
    [InlineData("bb7bb05e-5972-42b5-94fc-76eaa7084d49", BluetoothPairingConventions.LowEnergyTransport)]
    [InlineData(null, BluetoothPairingConventions.UnknownTransport)]
    [InlineData("not-a-guid", BluetoothPairingConventions.UnknownTransport)]
    public void GetTransportName_MapsKnownProtocolIds(string? protocolId, string expected)
    {
        var actual = BluetoothPairingConventions.GetTransportName(protocolId);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void BluetoothPairingService_GuessTransport_RecognizesBluetoothLePrefix()
    {
        var method = typeof(BluetoothPairingService)
            .GetMethod("GuessTransport", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);

        var actual = method?.Invoke(
            null,
            ["BluetoothLE#BluetoothLE3a:d5:88:41:2c:f0-6e:1b:a4:73:cc:28", null]) as string;

        Assert.Equal(BluetoothPairingConventions.LowEnergyTransport, actual);
    }
}
