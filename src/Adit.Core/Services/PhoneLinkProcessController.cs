using System.Diagnostics;
using Adit.Core.Models;

namespace Adit.Core.Services;

public sealed class PhoneLinkProcessController
{
    private static readonly string[] KnownProcessNames =
    [
        "PhoneExperienceHost",
        "CrossDeviceResume"
    ];

    public IReadOnlyList<PhoneLinkProcessRecord> Evict()
    {
        var terminated = new List<PhoneLinkProcessRecord>();

        foreach (var processName in KnownProcessNames)
        {
            foreach (var process in Process.GetProcessesByName(processName))
            {
                try
                {
                    var record = new PhoneLinkProcessRecord(
                        process.ProcessName,
                        process.Id,
                        TryGetPath(process),
                        TryGetStartTime(process));
                    process.Kill(entireProcessTree: true);
                    process.WaitForExit(5000);
                    terminated.Add(record);
                }
                catch
                {
                }
            }
        }

        return terminated;
    }

    private static string? TryGetPath(Process process)
    {
        try
        {
            return process.MainModule?.FileName;
        }
        catch
        {
            return null;
        }
    }

    private static DateTimeOffset? TryGetStartTime(Process process)
    {
        try
        {
            return new DateTimeOffset(process.StartTime.ToUniversalTime(), TimeSpan.Zero);
        }
        catch
        {
            return null;
        }
    }
}
