using System.Diagnostics;
using Adit.Core.Models;

namespace Adit.Core.Services;

public sealed class PhoneLinkProcessCatalog
{
    private static readonly string[] KnownProcessNames =
    [
        "PhoneExperienceHost",
        "CrossDeviceResume"
    ];

    public IReadOnlyList<PhoneLinkProcessRecord> ListRunning()
    {
        return KnownProcessNames
            .SelectMany(SafeGetProcessesByName)
            .OrderBy(process => process.ProcessName, StringComparer.OrdinalIgnoreCase)
            .Select(
                process => new PhoneLinkProcessRecord(
                    process.ProcessName,
                    process.Id,
                    SafeRead(() => process.MainModule?.FileName),
                    SafeReadDateTimeOffset(() => process.StartTime)))
            .ToArray();
    }

    private static IEnumerable<Process> SafeGetProcessesByName(string processName)
    {
        try
        {
            return Process.GetProcessesByName(processName);
        }
        catch
        {
            return [];
        }
    }

    private static string? SafeRead(Func<string?> reader)
    {
        try
        {
            return reader();
        }
        catch
        {
            return null;
        }
    }

    private static DateTimeOffset? SafeReadDateTimeOffset(Func<DateTime> reader)
    {
        try
        {
            return new DateTimeOffset(reader().ToUniversalTime(), TimeSpan.Zero);
        }
        catch
        {
            return null;
        }
    }
}
