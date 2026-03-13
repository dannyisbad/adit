namespace Adit.Core.Models;

public enum NotificationEventKind
{
    Added = 0,
    Modified = 1,
    Removed = 2
}

[Flags]
public enum NotificationEventFlags
{
    None = 0,
    Silent = 1,
    Important = 2,
    PreExisting = 4,
    PositiveAction = 8,
    NegativeAction = 16
}

public enum NotificationCategory
{
    Other = 0,
    IncomingCall = 1,
    MissedCall = 2,
    VoiceMail = 3,
    Social = 4,
    Schedule = 5,
    Email = 6,
    News = 7,
    HealthAndFitness = 8,
    BusinessAndFinance = 9,
    Location = 10,
    Entertainment = 11
}

public enum NotificationAction
{
    Positive = 0,
    Negative = 1
}

public sealed record NotificationRecord(
    uint NotificationUid,
    NotificationEventKind EventKind,
    NotificationEventFlags EventFlags,
    NotificationCategory Category,
    byte CategoryCount,
    DateTimeOffset ReceivedAtUtc,
    string? AppIdentifier,
    string? Title,
    string? Subtitle,
    string? Message,
    string? MessageSize,
    string? Date,
    string? PositiveActionLabel,
    string? NegativeActionLabel,
    IReadOnlyDictionary<string, string> Attributes);

public sealed record StoredNotificationRecord(
    string DeviceId,
    bool IsActive,
    DateTimeOffset UpdatedAtUtc,
    DateTimeOffset? RemovedAtUtc,
    NotificationRecord Notification);
