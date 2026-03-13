namespace Adit.Core.Models;

public sealed record BluetoothLeDeviceRecord(
    string Id,
    string Name,
    bool IsPaired,
    string? Address,
    bool? IsConnected,
    bool? IsPresent,
    string? ContainerId);

public sealed record BluetoothEndpointRecord(
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

public sealed record BluetoothPairingCandidateRecord(
    string Id,
    string Transport,
    string Name,
    bool IsPaired,
    bool CanPair,
    bool? IsConnected,
    bool? IsPresent,
    string? Address,
    string? ContainerId,
    string? Category);

public sealed record BluetoothPairingPromptRecord(
    string PairingKind,
    string? Pin,
    bool AcceptedAutomatically,
    bool RequiresPinInput);

public sealed record BluetoothPairingAttemptRecord(
    string DeviceId,
    string Name,
    string Transport,
    bool Success,
    bool IsPaired,
    string Status,
    string? Error,
    BluetoothPairingPromptRecord? Prompt,
    IReadOnlyList<BluetoothEndpointRecord> PairedEndpoints);

public sealed record PackageIdentityRecord(
    bool HasPackageIdentity,
    string? Name,
    string? FullName,
    string? FamilyName,
    string? Publisher,
    string? Error,
    string? Message);
