using System.Diagnostics;
using System.Net.Http;
using System.Text.Json;
using Adit.Core.Services;
using Adit.Core.Utilities;
using Adit.Daemon.Services;

namespace Adit.Daemon;

internal static class DaemonCli
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true
    };

    public static async Task<string[]?> ResolveServerArgsOrHandleAsync(
        string[] args,
        DaemonOptions options,
        CancellationToken cancellationToken)
    {
        if (args.Length == 0)
        {
            return args;
        }

        var command = args[0].Trim();
        if (string.IsNullOrWhiteSpace(command))
        {
            return Array.Empty<string>();
        }

        if (command.StartsWith('-'))
        {
            return args;
        }

        var normalized = command.ToLowerInvariant();
        var tail = args.Skip(1).ToArray();

        switch (normalized)
        {
            case "serve":
            case "server":
            case "run":
                return tail;
            case "help":
            case "--help":
            case "-h":
                PrintHelp();
                return null;
            case "open":
                OpenDashboard(options.ListenUrl);
                return null;
            case "devices":
                await PrintLocalDevicesAsync(options, cancellationToken);
                return null;
            case "doctor":
                await PrintDoctorAsync(options, cancellationToken);
                return null;
            case "info":
                await PrintRemoteJsonAsync(options.ListenUrl, options.AuthToken, "/v1/info", HttpMethod.Get, cancellationToken);
                return null;
            case "status":
                await PrintRemoteJsonAsync(options.ListenUrl, options.AuthToken, "/v1/status", HttpMethod.Get, cancellationToken);
                return null;
            case "runtime":
                await PrintRemoteJsonAsync(options.ListenUrl, options.AuthToken, "/v1/runtime", HttpMethod.Get, cancellationToken);
                return null;
            case "capabilities":
                await PrintRemoteJsonAsync(options.ListenUrl, options.AuthToken, "/v1/capabilities", HttpMethod.Get, cancellationToken);
                return null;
            case "sync":
                await PrintRemoteJsonAsync(options.ListenUrl, options.AuthToken, "/v1/sync/now", HttpMethod.Post, cancellationToken);
                return null;
            case "notifications-check":
                await PrintRemoteJsonAsync(options.ListenUrl, options.AuthToken, "/v1/notifications/check", HttpMethod.Post, cancellationToken);
                return null;
            case "notifications-enable":
                await PrintRemoteJsonAsync(options.ListenUrl, options.AuthToken, "/v1/notifications/enable", HttpMethod.Post, cancellationToken);
                return null;
            case "notifications-disable":
                await PrintRemoteJsonAsync(options.ListenUrl, options.AuthToken, "/v1/notifications/disable", HttpMethod.Post, cancellationToken);
                return null;
            default:
                Console.Error.WriteLine($"Unknown adit daemon command: {command}");
                Console.Error.WriteLine();
                PrintHelp();
                Environment.ExitCode = 1;
                return null;
        }
    }

    private static void PrintHelp()
    {
        Console.WriteLine("Adit daemon CLI");
        Console.WriteLine();
        Console.WriteLine("Usage:");
        Console.WriteLine("  dotnet run --project src\\Adit.Daemon -- [command]");
        Console.WriteLine();
        Console.WriteLine("Commands:");
        Console.WriteLine("  serve                  Start the local daemon (default).");
        Console.WriteLine("  doctor                 Print daemon health if it is running; otherwise print a local environment check.");
        Console.WriteLine("  devices                Inspect paired classic and LE Bluetooth devices without starting the daemon.");
        Console.WriteLine("  info                   Query GET /v1/info from the running daemon.");
        Console.WriteLine("  status                 Query GET /v1/status from the running daemon.");
        Console.WriteLine("  runtime                Query GET /v1/runtime from the running daemon.");
        Console.WriteLine("  capabilities           Query GET /v1/capabilities from the running daemon.");
        Console.WriteLine("  sync                   Trigger POST /v1/sync/now on the running daemon.");
        Console.WriteLine("  notifications-check    Trigger POST /v1/notifications/check on the running daemon.");
        Console.WriteLine("  notifications-enable   Trigger POST /v1/notifications/enable on the running daemon.");
        Console.WriteLine("  notifications-disable  Trigger POST /v1/notifications/disable on the running daemon.");
        Console.WriteLine("  open                   Open the daemon UI or root endpoint in the default browser.");
        Console.WriteLine("  help                   Show this help.");
        Console.WriteLine();
        Console.WriteLine("If the first argument starts with '-', it is passed through to ASP.NET hosting and the daemon starts normally.");
    }

    private static async Task PrintDoctorAsync(DaemonOptions options, CancellationToken cancellationToken)
    {
        if (await TryPrintRemoteJsonAsync(options.ListenUrl, options.AuthToken, "/v1/doctor", HttpMethod.Get, cancellationToken))
        {
            return;
        }

        await PrintLocalDoctorAsync(options, cancellationToken);
    }

    private static async Task PrintLocalDevicesAsync(DaemonOptions options, CancellationToken cancellationToken)
    {
        var catalog = new DeviceCatalog();
        var classicEndpoints = await catalog.ListPairedBluetoothEndpointsAsync();
        var leDevices = await catalog.ListPairedBluetoothLeDevicesAsync();
        var defaultClassicTarget = await catalog.SelectClassicTargetAsync(null, options.DefaultNameContains);
        var defaultLeTarget = await catalog.SelectLeTargetAsync(
            null,
            defaultClassicTarget?.ContainerId,
            options.DefaultNameContains);

        PrintJson(
            new
            {
                source = "local_environment",
                listenUrl = options.ListenUrl,
                defaultNameContains = options.DefaultNameContains,
                defaultClassicTarget,
                defaultLeTarget,
                classicEndpointCount = classicEndpoints.Count,
                leDeviceCount = leDevices.Count,
                classicEndpoints,
                leDevices
            });
    }

    private static async Task PrintLocalDoctorAsync(DaemonOptions options, CancellationToken cancellationToken)
    {
        var catalog = new DeviceCatalog();
        var processCatalog = new PhoneLinkProcessCatalog();
        var classicEndpoints = await catalog.ListPairedBluetoothEndpointsAsync();
        var leDevices = await catalog.ListPairedBluetoothLeDevicesAsync();
        var defaultClassicTarget = await catalog.SelectClassicTargetAsync(null, options.DefaultNameContains);
        var defaultLeTarget = await catalog.SelectLeTargetAsync(
            null,
            defaultClassicTarget?.ContainerId,
            options.DefaultNameContains);

        var runtime = new RuntimeStateService().GetSnapshot();
        var nextSteps = new List<string>();
        if (defaultClassicTarget is null)
        {
            nextSteps.Add("Complete one-time Link to Windows pairing so Adit can discover the classic MAP/PBAP target.");
        }
        else
        {
            nextSteps.Add("Start the daemon with `serve` and let the first sync settle.");
        }

        if (leDevices.Count == 0)
        {
            nextSteps.Add("Keep the iPhone nearby and unlocked so Windows can materialize the paired LE endpoint for notifications.");
        }

        if (processCatalog.ListRunning().Count == 0)
        {
            nextSteps.Add("If MAP or PBAP is busy, launch Phone Link once and complete LTW setup before retrying Adit.");
        }

        PrintJson(
            new
            {
                source = "local_environment",
                daemonReachable = false,
                listenUrl = options.ListenUrl,
                packageIdentity = PackageIdentitySnapshot.Capture(),
                setup = CapabilitySnapshotBuilder.BuildSetup(runtime, options),
                options = new
                {
                    options.DefaultNameContains,
                    options.DatabasePath,
                    options.AutoEvictPhoneLink,
                    options.EnableAncsByDefault,
                    options.EnableExperimentalPairingApi,
                    options.SyncIntervalSeconds
                },
                phoneLinkProcesses = processCatalog.ListRunning(),
                defaultClassicTarget,
                defaultLeTarget,
                classicEndpointCount = classicEndpoints.Count,
                leDeviceCount = leDevices.Count,
                nextSteps
            });
    }

    private static void OpenDashboard(string baseUrl)
    {
        Process.Start(
            new ProcessStartInfo(baseUrl)
            {
                UseShellExecute = true
            });
    }

    private static async Task PrintRemoteJsonAsync(
        string baseUrl,
        string? authToken,
        string path,
        HttpMethod method,
        CancellationToken cancellationToken)
    {
        if (!await TryPrintRemoteJsonAsync(baseUrl, authToken, path, method, cancellationToken))
        {
            Console.Error.WriteLine($"Adit daemon is not reachable at {baseUrl}. Start it with `serve` first.");
            Environment.ExitCode = 1;
        }
    }

    private static async Task<bool> TryPrintRemoteJsonAsync(
        string baseUrl,
        string? authToken,
        string path,
        HttpMethod method,
        CancellationToken cancellationToken)
    {
        using var client = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(5)
        };

        try
        {
            using var request = new HttpRequestMessage(method, new Uri(new Uri(EnsureTrailingSlash(baseUrl)), path.TrimStart('/')));
            if (!string.IsNullOrWhiteSpace(authToken))
            {
                request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", authToken);
            }
            using var response = await client.SendAsync(request, cancellationToken);
            var raw = await response.Content.ReadAsStringAsync(cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                Console.Error.WriteLine(raw);
                Environment.ExitCode = 1;
                return true;
            }

            PrintJson(raw);
            return true;
        }
        catch (HttpRequestException)
        {
            return false;
        }
        catch (TaskCanceledException)
        {
            return false;
        }
    }

    private static string EnsureTrailingSlash(string value)
    {
        return value.EndsWith("/", StringComparison.Ordinal) ? value : $"{value}/";
    }

    private static void PrintJson(object payload)
    {
        Console.WriteLine(JsonSerializer.Serialize(payload, JsonOptions));
    }

    private static void PrintJson(string rawJson)
    {
        try
        {
            using var document = JsonDocument.Parse(rawJson);
            Console.WriteLine(JsonSerializer.Serialize(document.RootElement, JsonOptions));
        }
        catch (JsonException)
        {
            Console.WriteLine(rawJson);
        }
    }
}
