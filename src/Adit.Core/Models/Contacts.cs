namespace Adit.Core.Models;

public sealed record ContactPhoneRecord(
    string Raw,
    string? Normalized,
    string Type);

public sealed record ContactRecord(
    string? UniqueIdentifier,
    string DisplayName,
    IReadOnlyList<ContactPhoneRecord> Phones,
    IReadOnlyList<string> Emails);
