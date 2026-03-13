namespace Adit.Core.Models;

public sealed record PhoneLinkProcessRecord(
    string Name,
    int Id,
    string? Path,
    DateTimeOffset? StartTimeUtc);
