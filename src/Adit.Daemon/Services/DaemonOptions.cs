using System.Net;

namespace Adit.Daemon.Services;

public sealed class DaemonOptions
{
    private static readonly string RepoRoot = ResolveRepoRoot();
    private static readonly string DefaultListenUrl = "http://127.0.0.1:5037";

    public string ListenUrl { get; init; } = DefaultListenUrl;

    public Uri ListenUri => new(ListenUrl, UriKind.Absolute);

    public string? DefaultNameContains { get; init; } = "iPhone";

    public string DatabasePath { get; init; } = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "Adit",
        "adit.db");

    public bool EncryptDatabaseAtRest { get; init; } = true;

    public int SyncIntervalSeconds { get; init; } = 45;

    public int ErrorBackoffSeconds { get; init; } = 15;

    public int ContactRefreshMinutes { get; init; } = 20;

    public int MessageFetchLimit { get; init; } = 100;

    public int MessageCacheLimit { get; init; } = 1000;

    public bool AutoEvictPhoneLink { get; init; } = true;

    public bool EnableAncsByDefault { get; init; } = false;

    public bool EnableExperimentalPairingApi { get; init; } = false;

    public int EventBufferSize { get; init; } = 200;

    public bool DisableBackgroundSync { get; init; } = false;

    public bool EnableLearnedThreadChooser { get; init; } = false;

    public string ThreadChooserPythonPath { get; init; } = Path.Combine(
        RepoRoot,
        ".venv-threadmodel",
        "Scripts",
        "python.exe");

    public string ThreadChooserScriptPath { get; init; } = Path.Combine(
        RepoRoot,
        "training",
        "thread_scoring_sidecar.py");

    public string ThreadChooserCheckpointPath { get; init; } = Path.Combine(
        RepoRoot,
        "training",
        "models",
        "thread-chooser-fused-headline.pt");

    public string ThreadChooserModelName { get; init; } = "Qwen/Qwen3-0.6B-Base";

    public int ThreadChooserPort { get; init; } = 5048;

    public int ThreadChooserMaxCandidates { get; init; } = 6;

    public int ThreadChooserHistoryTurns { get; init; } = 8;

    public static DaemonOptions FromEnvironment()
    {
        var encryptDatabaseAtRest = ParseBool("ADIT_ENCRYPT_DB_AT_REST", true);

        return new DaemonOptions
        {
            ListenUrl = NormalizeListenUrl(Environment.GetEnvironmentVariable("ADIT_URL")),
            DefaultNameContains = Environment.GetEnvironmentVariable("ADIT_DEFAULT_NAME_CONTAINS") ?? "iPhone",
            DatabasePath = Environment.GetEnvironmentVariable("ADIT_DB_PATH")
                ?? Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                    "Adit",
                    "adit.db"),
            EncryptDatabaseAtRest = encryptDatabaseAtRest,
            SyncIntervalSeconds = ParseInt("ADIT_SYNC_INTERVAL_SECONDS", 45),
            ErrorBackoffSeconds = ParseInt("ADIT_ERROR_BACKOFF_SECONDS", 15),
            ContactRefreshMinutes = ParseInt("ADIT_CONTACT_REFRESH_MINUTES", 20),
            MessageFetchLimit = ParseInt("ADIT_MESSAGE_FETCH_LIMIT", 100),
            MessageCacheLimit = ParseInt("ADIT_MESSAGE_CACHE_LIMIT", 1000),
            AutoEvictPhoneLink = ParseBool("ADIT_AUTO_EVICT_PHONE_LINK", true),
            EnableAncsByDefault = ParseBool("ADIT_ENABLE_ANCS_BY_DEFAULT", false),
            EnableExperimentalPairingApi = ParseBool("ADIT_ENABLE_EXPERIMENTAL_PAIRING_API", false),
            EventBufferSize = ParseInt("ADIT_EVENT_BUFFER_SIZE", 200),
            DisableBackgroundSync = ParseBool("ADIT_DISABLE_BACKGROUND_SYNC", false),
            EnableLearnedThreadChooser = ParseBool("ADIT_ENABLE_LEARNED_THREAD_CHOOSER", false),
            ThreadChooserPythonPath = Environment.GetEnvironmentVariable("ADIT_THREAD_CHOOSER_PYTHON")
                ?? Path.Combine(
                    RepoRoot,
                    ".venv-threadmodel",
                    "Scripts",
                    "python.exe"),
            ThreadChooserScriptPath = Environment.GetEnvironmentVariable("ADIT_THREAD_CHOOSER_SCRIPT")
                ?? Path.Combine(
                    RepoRoot,
                    "training",
                    "thread_scoring_sidecar.py"),
            ThreadChooserCheckpointPath = Environment.GetEnvironmentVariable("ADIT_THREAD_CHOOSER_CHECKPOINT")
                ?? Path.Combine(
                    RepoRoot,
                    "training",
                    "models",
                    "thread-chooser-fused-headline.pt"),
            ThreadChooserModelName = Environment.GetEnvironmentVariable("ADIT_THREAD_CHOOSER_MODEL")
                ?? "Qwen/Qwen3-0.6B-Base",
            ThreadChooserPort = ParseInt("ADIT_THREAD_CHOOSER_PORT", 5048),
            ThreadChooserMaxCandidates = ParseInt("ADIT_THREAD_CHOOSER_MAX_CANDIDATES", 6),
            ThreadChooserHistoryTurns = ParseInt("ADIT_THREAD_CHOOSER_HISTORY_TURNS", 8)
        };
    }

    private static int ParseInt(string name, int fallback)
    {
        return int.TryParse(Environment.GetEnvironmentVariable(name), out var value) && value > 0
            ? value
            : fallback;
    }

    private static bool ParseBool(string name, bool fallback)
    {
        return bool.TryParse(Environment.GetEnvironmentVariable(name), out var value)
            ? value
            : fallback;
    }

    private static string NormalizeListenUrl(string? raw)
    {
        var candidate = string.IsNullOrWhiteSpace(raw)
            ? DefaultListenUrl
            : raw.Trim();
        if (!Uri.TryCreate(candidate, UriKind.Absolute, out var uri)
            || (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps))
        {
            throw new InvalidOperationException($"ADIT_URL must be an absolute http/https URL. Current value: '{candidate}'.");
        }

        if (!IsLoopbackHost(uri.Host))
        {
            throw new InvalidOperationException(
                $"ADIT_URL '{candidate}' is not loopback. The daemon only binds to localhost/loopback addresses.");
        }

        return uri.ToString().TrimEnd('/');
    }

    private static bool IsLoopbackHost(string host)
    {
        return string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase)
            || (IPAddress.TryParse(host, out var address) && IPAddress.IsLoopback(address));
    }

    private static string ResolveRepoRoot()
    {
        foreach (var candidate in new[] { Directory.GetCurrentDirectory(), AppContext.BaseDirectory })
        {
            var resolved = TryFindRepoRoot(candidate);
            if (!string.IsNullOrWhiteSpace(resolved))
            {
                return resolved;
            }
        }

        return Directory.GetCurrentDirectory();
    }

    private static string? TryFindRepoRoot(string? startPath)
    {
        if (string.IsNullOrWhiteSpace(startPath))
        {
            return null;
        }

        DirectoryInfo? current;
        try
        {
            current = new DirectoryInfo(Path.GetFullPath(startPath));
        }
        catch
        {
            return null;
        }

        if (!current.Exists)
        {
            current = current.Parent;
        }

        while (current is not null)
        {
            if (File.Exists(Path.Combine(current.FullName, "Adit.sln")))
            {
                return current.FullName;
            }

            var scriptPath = Path.Combine(current.FullName, "training", "thread_scoring_sidecar.py");
            var checkpointPath = Path.Combine(current.FullName, "training", "models", "thread-chooser-fused-headline.pt");
            if (File.Exists(scriptPath) && File.Exists(checkpointPath))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        return null;
    }
}
