namespace Adit.Core.Models;

public sealed record MessageParticipantRecord(
    string Name,
    IReadOnlyList<string> Phones,
    IReadOnlyList<string> Emails);

public sealed record MessageRecord(
    string Folder,
    string? Handle,
    string? Type,
    string? Subject,
    string? Datetime,
    string? SenderName,
    string? SenderAddressing,
    string? RecipientAddressing,
    uint? Size,
    uint? AttachmentSize,
    string? Priority,
    bool? Read,
    bool? Sent,
    bool? Protected,
    string? Body,
    string? MessageType,
    string? Status,
    IReadOnlyList<MessageParticipantRecord> Originators,
    IReadOnlyList<MessageParticipantRecord> Recipients);

public sealed record MessageFolderListing(
    string Folder,
    int TotalCount,
    ushort? ListingSize,
    bool? NewMessage,
    string? ListingTime,
    IReadOnlyDictionary<string, int> TypeCounts,
    IReadOnlyList<MessageRecord> Items);

public sealed record MessageSyncSnapshot(
    IReadOnlyList<string> Folders,
    IReadOnlyList<MessageRecord> Messages);

public sealed record SendMessageResult(
    bool IsSuccess,
    string? ResponseCode,
    string? MessageHandle);
