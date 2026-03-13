namespace Adit.Core.Models;

public sealed record MapMessageChangeRecord(
    string EventType,
    string? Handle,
    string? Folder,
    string? OldFolder,
    string? MessageType,
    MessageRecord? Message,
    IReadOnlyList<MessageRecord> RelatedMessages);
