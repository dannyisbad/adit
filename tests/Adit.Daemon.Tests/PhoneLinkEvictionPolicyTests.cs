using Adit.Core.Models;
using Adit.Daemon.Services;

namespace Adit.Daemon.Tests;

public sealed class PhoneLinkEvictionPolicyTests
{
    private static readonly BluetoothEndpointRecord ClassicTarget = new(
        "classic",
        "classic-1",
        "Test iPhone",
        true,
        "11:22:33:44:55:66",
        "112233445566",
        true,
        true,
        "container-1",
        "Unknown");

    [Fact]
    public void CanAutoEvictForMap_ReturnsFalse_WhenAutoEvictionIsDisabled()
    {
        var pairedLeTarget = new BluetoothLeDeviceRecord(
            "BluetoothLE#BluetoothLE-classic-1",
            "Test iPhone",
            true,
            "aa:bb:cc:dd:ee:ff",
            true,
            true,
            ClassicTarget.ContainerId);

        var result = PhoneLinkEvictionPolicy.CanAutoEvictForMap(
            ClassicTarget,
            pairedLeTarget,
            autoEvictPhoneLinkEnabled: false);

        Assert.False(result);
    }

    [Fact]
    public void CanAutoEvictForMap_ReturnsFalse_WhenPairingIsIncomplete()
    {
        var unpairedLeTarget = new BluetoothLeDeviceRecord(
            "BluetoothLE#BluetoothLE-classic-1",
            "Test iPhone",
            false,
            "aa:bb:cc:dd:ee:ff",
            true,
            true,
            ClassicTarget.ContainerId);

        Assert.False(
            PhoneLinkEvictionPolicy.CanAutoEvictForMap(
                ClassicTarget,
                pairedLeTarget: null,
                autoEvictPhoneLinkEnabled: true));
        Assert.False(
            PhoneLinkEvictionPolicy.CanAutoEvictForMap(
                ClassicTarget,
                unpairedLeTarget,
                autoEvictPhoneLinkEnabled: true));
    }

    [Fact]
    public void CanAutoEvictForMap_ReturnsTrue_WhenClassicAndLeTargetsArePaired()
    {
        var pairedLeTarget = new BluetoothLeDeviceRecord(
            "BluetoothLE#BluetoothLE-classic-1",
            "Test iPhone",
            true,
            "aa:bb:cc:dd:ee:ff",
            true,
            true,
            ClassicTarget.ContainerId);

        var result = PhoneLinkEvictionPolicy.CanAutoEvictForMap(
            ClassicTarget,
            pairedLeTarget,
            autoEvictPhoneLinkEnabled: true);

        Assert.True(result);
    }
}
