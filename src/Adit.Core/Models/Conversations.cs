namespace Adit.Core.Models;

public sealed record ConversationParticipantRecord(
    string Key,
    string DisplayName,
    IReadOnlyList<string> Phones,
    IReadOnlyList<string> Emails,
    bool IsSelf);

public sealed record SynthesizedMessageRecord(
    string MessageKey,
    string ConversationId,
    string ConversationDisplayName,
    bool IsGroup,
    DateTimeOffset? SortTimestampUtc,
    IReadOnlyList<ConversationParticipantRecord> Participants,
    MessageRecord Message);

public sealed record ConversationSnapshot(
    string ConversationId,
    string DisplayName,
    bool IsGroup,
    DateTimeOffset? LastMessageUtc,
    int MessageCount,
    int UnreadCount,
    string? LastPreview,
    IReadOnlyList<ConversationParticipantRecord> Participants,
    IReadOnlyList<string> SourceFolders,
    string? LastSenderDisplayName = null);

public sealed record ConversationSynthesisResult(
    IReadOnlyList<string> SelfPhones,
    IReadOnlyList<SynthesizedMessageRecord> Messages,
    IReadOnlyList<ConversationSnapshot> Conversations);
