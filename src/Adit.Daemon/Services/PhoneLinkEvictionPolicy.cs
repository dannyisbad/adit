using Adit.Core.Models;
using Adit.Core.Services;

namespace Adit.Daemon.Services;

public sealed class PhoneLinkEvictionPolicy
{
    private readonly DaemonOptions options;
    private readonly DeviceCatalog deviceCatalog;

    public PhoneLinkEvictionPolicy(DaemonOptions options, DeviceCatalog deviceCatalog)
    {
        this.options = options;
        this.deviceCatalog = deviceCatalog;
    }

    public bool ShouldEvictForContacts(bool? requestedEviction)
    {
        return requestedEviction ?? false;
    }

    public Task<bool> ShouldEvictForContactsAsync(
        BluetoothEndpointRecord target,
        CancellationToken cancellationToken)
    {
        return ShouldEvictForContactsAsync(target, requestedEviction: null, cancellationToken);
    }

    public async Task<bool> ShouldEvictForContactsAsync(
        BluetoothEndpointRecord target,
        bool? requestedEviction,
        CancellationToken cancellationToken)
    {
        if (requestedEviction.HasValue)
        {
            return requestedEviction.Value;
        }

        if (!options.AutoEvictPhoneLink)
        {
            return false;
        }

        var pairedLeTarget = await deviceCatalog.SelectLeTargetAsync(
            null,
            target.ContainerId,
            target.Name);
        return CanAutoEvictForMap(target, pairedLeTarget, options.AutoEvictPhoneLink);
    }

    public Task<bool> ShouldEvictForMapAsync(
        BluetoothEndpointRecord target,
        CancellationToken cancellationToken)
    {
        return ShouldEvictForMapAsync(target, requestedEviction: null, cancellationToken);
    }

    public async Task<bool> ShouldEvictForMapAsync(
        BluetoothEndpointRecord target,
        bool? requestedEviction,
        CancellationToken cancellationToken)
    {
        if (requestedEviction.HasValue)
        {
            return requestedEviction.Value;
        }

        if (!options.AutoEvictPhoneLink)
        {
            return false;
        }

        var pairedLeTarget = await deviceCatalog.SelectLeTargetAsync(
            null,
            target.ContainerId,
            target.Name);
        return CanAutoEvictForMap(target, pairedLeTarget, options.AutoEvictPhoneLink);
    }

    internal static bool CanAutoEvictForMap(
        BluetoothEndpointRecord target,
        BluetoothLeDeviceRecord? pairedLeTarget,
        bool autoEvictPhoneLinkEnabled)
    {
        return autoEvictPhoneLinkEnabled
            && target.IsPaired
            && pairedLeTarget?.IsPaired == true;
    }
}
