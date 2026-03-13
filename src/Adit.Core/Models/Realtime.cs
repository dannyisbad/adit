namespace Adit.Core.Models;

public enum DeviceSessionPhase
{
    Disconnected = 0,
    Connecting = 1,
    Connected = 2,
    Faulted = 3
}

public sealed record SessionStateChangedRecord(
    string Transport,
    DeviceSessionPhase Phase,
    DateTimeOffset TimestampUtc,
    string? Detail,
    string? Error);

public sealed record MapRealtimeEventRecord(
    string DeviceId,
    string DeviceName,
    DateTimeOffset TimestampUtc,
    string EventType,
    string? Handle,
    string? Folder,
    string? OldFolder,
    string? MessageType,
    MessageRecord? Message,
    IReadOnlyList<MessageRecord> AffectedMessages);
