using System.Diagnostics;

namespace Adit.Probe;

internal static class PhoneLinkEviction
{
    private static readonly string[] ProcessNames =
    [
        "PhoneExperienceHost",
        "CrossDeviceResume"
    ];

    public static void Evict(ProbeLogger logger)
    {
        var terminated = new List<object>();

        foreach (var processName in ProcessNames)
        {
            foreach (var process in Process.GetProcessesByName(processName))
            {
                try
                {
                    var id = process.Id;
                    var path = TryGetPath(process);
                    process.Kill(entireProcessTree: true);
                    process.WaitForExit(5000);
                    terminated.Add(new { processName, id, path });
                }
                catch (Exception exception)
                {
                    logger.Log(
                        "map.process_evict_failed",
                        new
                        {
                            processName,
                            error = exception.ToString()
                        });
                }
            }
        }

        logger.Log("map.processes_evicted", new { count = terminated.Count, terminated });
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
}
