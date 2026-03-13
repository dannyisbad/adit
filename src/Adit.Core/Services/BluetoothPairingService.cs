using Adit.Core.Models;
using Microsoft.Extensions.Logging;
using Windows.Devices.Bluetooth;
using Windows.Devices.Enumeration;
using Windows.Foundation;

namespace Adit.Core.Services;

public sealed class BluetoothPairingService
{
    public const string DeviceAddressProperty = "System.Devices.Aep.DeviceAddress";
    public const string ContainerIdProperty = "System.Devices.Aep.ContainerId";
    public const string CategoryProperty = "System.Devices.Aep.Category";
    public const string IsConnectedProperty = "System.Devices.Aep.IsConnected";
    public const string IsPresentProperty = "System.Devices.Aep.IsPresent";
    public const string ProtocolIdProperty = "System.Devices.Aep.ProtocolId";

    private static readonly DevicePairingKinds SupportedPairingKinds =
        DevicePairingKinds.ConfirmOnly |
        DevicePairingKinds.DisplayPin |
        DevicePairingKinds.ConfirmPinMatch |
        DevicePairingKinds.ProvidePin;

    private static readonly string[] AdditionalProperties =
    [
        DeviceAddressProperty,
        ContainerIdProperty,
        CategoryProperty,
        IsConnectedProperty,
        IsPresentProperty,
        ProtocolIdProperty
    ];

    private readonly ILogger<BluetoothPairingService> logger;
    private readonly DeviceCatalog deviceCatalog;

    public BluetoothPairingService(
        ILogger<BluetoothPairingService> logger,
        DeviceCatalog deviceCatalog)
    {
        this.logger = logger;
        this.deviceCatalog = deviceCatalog;
    }

    public async Task<IReadOnlyList<BluetoothPairingCandidateRecord>> ListCandidatesAsync(
        bool paired,
        string? transport,
        string? nameContains,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var effectiveTransport = BluetoothPairingConventions.NormalizeTransport(transport);
        var candidates = new List<BluetoothPairingCandidateRecord>();

        if (effectiveTransport is BluetoothPairingConventions.AnyTransport or BluetoothPairingConventions.ClassicTransport)
        {
            candidates.AddRange(
                await LoadCandidatesAsync(
                    BluetoothPairingConventions.ClassicTransport,
                    paired,
                    cancellationToken));
        }

        if (effectiveTransport is BluetoothPairingConventions.AnyTransport or BluetoothPairingConventions.LowEnergyTransport)
        {
            candidates.AddRange(
                await LoadCandidatesAsync(
                    BluetoothPairingConventions.LowEnergyTransport,
                    paired,
                    cancellationToken));
        }

        return candidates
            .Where(
                candidate => string.IsNullOrWhiteSpace(nameContains)
                    || candidate.Name.Contains(nameContains.Trim(), StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(candidate => candidate.IsConnected == true)
            .ThenByDescending(candidate => candidate.IsPresent == true)
            .ThenByDescending(candidate => candidate.CanPair)
            .ThenBy(candidate => candidate.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(candidate => candidate.Transport, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public async Task<BluetoothPairingCandidateRecord?> ResolveCandidateAsync(
        string? deviceId,
        string? nameContains,
        bool paired,
        string? transport,
        CancellationToken cancellationToken)
    {
        if (!string.IsNullOrWhiteSpace(deviceId))
        {
            var device = await CreateDeviceAsync(deviceId, cancellationToken);
            if (device is null)
            {
                return null;
            }

            var candidate = MapCandidate(device, GuessTransport(deviceId, transport));
            var effectiveTransport = BluetoothPairingConventions.NormalizeTransport(transport);
            if (candidate.IsPaired != paired)
            {
                return null;
            }

            if (effectiveTransport != BluetoothPairingConventions.AnyTransport
                && !string.Equals(candidate.Transport, effectiveTransport, StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }

            if (!string.IsNullOrWhiteSpace(nameContains)
                && !candidate.Name.Contains(nameContains.Trim(), StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }

            return candidate;
        }

        var candidates = await ListCandidatesAsync(paired, transport, nameContains, cancellationToken);
        return candidates.FirstOrDefault(candidate => candidate.IsPaired == paired);
    }

    public async Task<BluetoothPairingAttemptRecord> PairAsync(
        string deviceId,
        string? pin,
        DevicePairingProtectionLevel protectionLevel,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(deviceId))
        {
            throw new ArgumentException("deviceId is required.", nameof(deviceId));
        }

        var device = await CreateDeviceAsync(deviceId, cancellationToken);
        if (device is null)
        {
            return new BluetoothPairingAttemptRecord(
                deviceId,
                "(unknown)",
                BluetoothPairingConventions.UnknownTransport,
                false,
                false,
                "NotFound",
                "DeviceInformation.CreateFromIdAsync returned null.",
                null,
                []);
        }

        if (device.Pairing.IsPaired)
        {
            return await BuildAttemptRecordAsync(
                device,
                true,
                DevicePairingResultStatus.AlreadyPaired.ToString(),
                null,
                null,
                true,
                cancellationToken);
        }

        var promptTracker = new PairingPromptTracker();
        DevicePairingResult? result = null;
        Exception? exception = null;

        try
        {
            result = await PairCoreAsync(device, pin, protectionLevel, promptTracker, cancellationToken);
            logger.LogInformation(
                "Bluetooth pairing completed for {DeviceName} ({DeviceId}) with status {Status}.",
                device.Name,
                device.Id,
                result.Status);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            exception = ex;
            logger.LogWarning(ex, "Bluetooth pairing failed for {DeviceName} ({DeviceId}).", device.Name, device.Id);
        }

        return await BuildAttemptRecordAsync(
            device,
            exception is null && result is not null && IsSuccess(result.Status),
            result?.Status.ToString() ?? "Failed",
            promptTracker.Snapshot,
            exception?.ToString(),
            result is not null && IsSuccess(result.Status) ? true : null,
            cancellationToken);
    }

    public async Task<BluetoothPairingAttemptRecord> UnpairAsync(
        string deviceId,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(deviceId))
        {
            throw new ArgumentException("deviceId is required.", nameof(deviceId));
        }

        var device = await CreateDeviceAsync(deviceId, cancellationToken);
        if (device is null)
        {
            return new BluetoothPairingAttemptRecord(
                deviceId,
                "(unknown)",
                BluetoothPairingConventions.UnknownTransport,
                false,
                false,
                "NotFound",
                "DeviceInformation.CreateFromIdAsync returned null.",
                null,
                []);
        }

        if (!device.Pairing.IsPaired)
        {
            return await BuildAttemptRecordAsync(
                device,
                true,
                "NotPaired",
                null,
                null,
                false,
                cancellationToken);
        }

        DeviceUnpairingResult? result = null;
        Exception? exception = null;

        try
        {
            result = await device.Pairing.UnpairAsync().AsTask(cancellationToken);
            logger.LogInformation(
                "Bluetooth unpair completed for {DeviceName} ({DeviceId}) with status {Status}.",
                device.Name,
                device.Id,
                result.Status);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            exception = ex;
            logger.LogWarning(ex, "Bluetooth unpair failed for {DeviceName} ({DeviceId}).", device.Name, device.Id);
        }

        return await BuildAttemptRecordAsync(
            device,
            exception is null && result is not null && result.Status == DeviceUnpairingResultStatus.Unpaired,
            result?.Status.ToString() ?? "Failed",
            null,
            exception?.ToString(),
            result is not null && result.Status == DeviceUnpairingResultStatus.Unpaired ? false : null,
            cancellationToken);
    }

    private async Task<IReadOnlyList<BluetoothPairingCandidateRecord>> LoadCandidatesAsync(
        string transport,
        bool paired,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var selector = BluetoothPairingConventions.BuildAssociationEndpointSelector(paired, transport);
        var devices = await DeviceInformation.FindAllAsync(
            selector,
            AdditionalProperties,
            DeviceInformationKind.AssociationEndpoint);
        return devices.Select(device => MapCandidate(device, transport)).ToArray();
    }

    private async Task<DeviceInformation?> CreateDeviceAsync(
        string deviceId,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return await DeviceInformation.CreateFromIdAsync(deviceId, AdditionalProperties);
    }

    private async Task<DevicePairingResult> PairCoreAsync(
        DeviceInformation device,
        string? providedPin,
        DevicePairingProtectionLevel protectionLevel,
        PairingPromptTracker promptTracker,
        CancellationToken cancellationToken)
    {
        var customPairing = device.Pairing.Custom;

        void Handler(DeviceInformationCustomPairing sender, DevicePairingRequestedEventArgs args)
        {
            var deferral = args.GetDeferral();
            _ = Task.Run(
                () => HandlePairingRequestAsync(args, deferral, promptTracker, providedPin),
                CancellationToken.None);
        }

        customPairing.PairingRequested += Handler;
        try
        {
            DevicePairingResult? lastResult = null;
            foreach (var level in ResolveProtectionLevels(protectionLevel))
            {
                lastResult = await customPairing.PairAsync(SupportedPairingKinds, level).AsTask(cancellationToken);
                if (!ShouldRetryWithWeakerProtection(lastResult.Status))
                {
                    return lastResult;
                }
            }

            return lastResult ?? throw new InvalidOperationException("No pairing result was returned.");
        }
        finally
        {
            customPairing.PairingRequested -= Handler;
        }
    }

    private static async Task HandlePairingRequestAsync(
        DevicePairingRequestedEventArgs args,
        Deferral deferral,
        PairingPromptTracker promptTracker,
        string? providedPin)
    {
        try
        {
            promptTracker.Snapshot = new BluetoothPairingPromptRecord(
                args.PairingKind.ToString(),
                string.IsNullOrWhiteSpace(args.Pin) ? null : args.Pin,
                !RequiresManualPin(args.PairingKind) || !string.IsNullOrWhiteSpace(providedPin),
                RequiresManualPin(args.PairingKind) && string.IsNullOrWhiteSpace(providedPin));

            switch (args.PairingKind)
            {
                case DevicePairingKinds.ConfirmOnly:
                    args.Accept();
                    break;
                case DevicePairingKinds.DisplayPin:
                case DevicePairingKinds.ConfirmPinMatch:
                    if (!string.IsNullOrWhiteSpace(args.Pin))
                    {
                        args.Accept(args.Pin);
                    }
                    else
                    {
                        args.Accept();
                    }
                    break;
                case DevicePairingKinds.ProvidePin:
                    if (!string.IsNullOrWhiteSpace(providedPin))
                    {
                        args.Accept(providedPin);
                    }
                    break;
            }
        }
        finally
        {
            await Task.Yield();
            deferral.Complete();
            deferral.Dispose();
        }
    }

    private async Task<BluetoothPairingAttemptRecord> BuildAttemptRecordAsync(
        DeviceInformation device,
        bool success,
        string status,
        BluetoothPairingPromptRecord? prompt,
        string? error,
        bool? forceIsPaired,
        CancellationToken cancellationToken)
    {
        var refreshedDevice = await CreateDeviceAsync(device.Id, cancellationToken) ?? device;
        var endpoints = await ReadRelatedPairedEndpointsAsync(refreshedDevice, cancellationToken);
        var isPaired = forceIsPaired
            ?? refreshedDevice.Pairing.IsPaired;

        return new BluetoothPairingAttemptRecord(
            refreshedDevice.Id,
            string.IsNullOrWhiteSpace(refreshedDevice.Name) ? "(unnamed)" : refreshedDevice.Name,
            GuessTransport(refreshedDevice.Id, null),
            success,
            isPaired,
            status,
            error,
            prompt,
            endpoints);
    }

    private async Task<IReadOnlyList<BluetoothEndpointRecord>> ReadRelatedPairedEndpointsAsync(
        DeviceInformation device,
        CancellationToken cancellationToken)
    {
        var endpoints = await deviceCatalog.ListPairedBluetoothEndpointsAsync();
        var containerId = BluetoothPairingConventions.GetStringProperty(device, ContainerIdProperty);

        return endpoints
            .Where(
                endpoint =>
                    string.Equals(endpoint.Id, device.Id, StringComparison.OrdinalIgnoreCase)
                    || (!string.IsNullOrWhiteSpace(containerId)
                        && string.Equals(endpoint.ContainerId, containerId, StringComparison.OrdinalIgnoreCase)))
            .OrderBy(endpoint => endpoint.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(endpoint => endpoint.Transport, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static IEnumerable<DevicePairingProtectionLevel> ResolveProtectionLevels(
        DevicePairingProtectionLevel protectionLevel)
    {
        yield return protectionLevel;

        if (protectionLevel == DevicePairingProtectionLevel.Default)
        {
            yield return DevicePairingProtectionLevel.EncryptionAndAuthentication;
            yield return DevicePairingProtectionLevel.Encryption;
        }
        else if (protectionLevel == DevicePairingProtectionLevel.EncryptionAndAuthentication)
        {
            yield return DevicePairingProtectionLevel.Encryption;
        }
        else if (protectionLevel == DevicePairingProtectionLevel.Encryption)
        {
            yield return DevicePairingProtectionLevel.None;
        }
    }

    private static BluetoothPairingCandidateRecord MapCandidate(DeviceInformation device, string transport)
    {
        return new BluetoothPairingCandidateRecord(
            device.Id,
            transport,
            string.IsNullOrWhiteSpace(device.Name) ? "(unnamed)" : device.Name,
            device.Pairing.IsPaired,
            device.Pairing.CanPair,
            GetBoolProperty(device, IsConnectedProperty),
            GetBoolProperty(device, IsPresentProperty),
            BluetoothPairingConventions.GetStringProperty(device, DeviceAddressProperty),
            BluetoothPairingConventions.GetStringProperty(device, ContainerIdProperty),
            BluetoothPairingConventions.GetStringProperty(device, CategoryProperty));
    }

    private static string GuessTransport(string deviceId, string? requestedTransport)
    {
        var normalized = BluetoothPairingConventions.NormalizeTransport(requestedTransport);
        if (normalized != BluetoothPairingConventions.AnyTransport)
        {
            return normalized;
        }

        return deviceId.Contains("BTHLE", StringComparison.OrdinalIgnoreCase)
            || deviceId.Contains("BluetoothLE#", StringComparison.OrdinalIgnoreCase)
            ? BluetoothPairingConventions.LowEnergyTransport
            : BluetoothPairingConventions.ClassicTransport;
    }

    private static bool IsSuccess(DevicePairingResultStatus status)
    {
        return status is DevicePairingResultStatus.Paired or DevicePairingResultStatus.AlreadyPaired;
    }

    private static bool RequiresManualPin(DevicePairingKinds pairingKind)
    {
        return pairingKind == DevicePairingKinds.ProvidePin;
    }

    private static bool ShouldRetryWithWeakerProtection(DevicePairingResultStatus status)
    {
        return status is DevicePairingResultStatus.ProtectionLevelCouldNotBeMet
            or DevicePairingResultStatus.AuthenticationNotAllowed;
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

    private sealed class PairingPromptTracker
    {
        public BluetoothPairingPromptRecord? Snapshot { get; set; }
    }
}
