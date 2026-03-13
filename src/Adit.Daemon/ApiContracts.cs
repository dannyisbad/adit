namespace Adit.Daemon;

public sealed record SendMessageRequest(
    string? Recipient,
    string Body,
    string? DeviceId,
    string? NameContains,
    string? ContactId,
    string? ContactName,
    string? PreferredNumber,
    string? ConversationId,
    bool AutoSyncAfterSend = true,
    bool EvictPhoneLink = false);

public sealed record ResolveMessageRequest(
    string? Recipient,
    string? Body,
    string? DeviceId,
    string? NameContains,
    string? ContactId,
    string? ContactName,
    string? PreferredNumber,
    string? ConversationId,
    bool EvictPhoneLink = false);

public sealed record PairDeviceRequest(
    string? DeviceId,
    string? NameContains,
    string? Transport,
    string? Pin,
    string? ProtectionLevel,
    bool AutoSyncAfterPair = true);

public sealed record UnpairDeviceRequest(
    string? DeviceId,
    string? NameContains,
    string? Transport,
    bool AutoSyncAfterUnpair = true);
