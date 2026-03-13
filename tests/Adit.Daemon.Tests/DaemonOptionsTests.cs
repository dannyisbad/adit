using Adit.Daemon.Services;
using System.Net;

namespace Adit.Daemon.Tests;

[Collection(EnvironmentVariableCollection.Name)]
public sealed class DaemonOptionsTests
{
    [Fact]
    public void DefaultReleaseSafeOptions_PointAtCuratedTrainingAssets()
    {
        var repoRoot = FindRepoRoot();
        var originalDirectory = Directory.GetCurrentDirectory();

        try
        {
            Directory.SetCurrentDirectory(repoRoot);

            var options = new DaemonOptions();

            Assert.Equal("http://127.0.0.1:5037", options.ListenUrl);
            Assert.True(options.AutoEvictPhoneLink);
            Assert.True(options.EncryptDatabaseAtRest);
            Assert.False(options.EnableLearnedThreadChooser);
            Assert.Equal(
                Path.Combine(repoRoot, "training", "thread_scoring_sidecar.py"),
                options.ThreadChooserScriptPath);
            Assert.Equal(
                Path.Combine(repoRoot, "training", "models", "thread-chooser-fused-headline.pt"),
                options.ThreadChooserCheckpointPath);
            Assert.True(File.Exists(options.ThreadChooserScriptPath));
            Assert.True(File.Exists(options.ThreadChooserCheckpointPath));
        }
        finally
        {
            Directory.SetCurrentDirectory(originalDirectory);
        }
    }

    [Fact]
    public void FromEnvironment_RejectsRemoteBind_Always()
    {
        using var url = new EnvironmentVariableScope("ADIT_URL", "http://0.0.0.0:5037");
        using var allowRemoteBind = new EnvironmentVariableScope("ADIT_ALLOW_REMOTE_BIND", "true");

        var exception = Assert.Throws<InvalidOperationException>(() => DaemonOptions.FromEnvironment());

        Assert.Contains("only binds to localhost/loopback", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void FromEnvironment_AcceptsLoopbackListenUrl()
    {
        using var url = new EnvironmentVariableScope("ADIT_URL", "http://localhost:5037");

        var options = DaemonOptions.FromEnvironment();

        Assert.Equal("http://localhost:5037", options.ListenUrl);
        Assert.True(
            string.Equals(options.ListenUri.Host, "localhost", StringComparison.OrdinalIgnoreCase)
            || (IPAddress.TryParse(options.ListenUri.Host, out var address) && IPAddress.IsLoopback(address)));
    }

    private static string FindRepoRoot()
    {
        var current = new DirectoryInfo(AppContext.BaseDirectory);
        while (current is not null)
        {
            if (Directory.Exists(Path.Combine(current.FullName, ".git")))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        throw new InvalidOperationException("Could not locate repo root from test base directory.");
    }

    private sealed class EnvironmentVariableScope : IDisposable
    {
        private readonly string name;
        private readonly string? originalValue;

        public EnvironmentVariableScope(string name, string? value)
        {
            this.name = name;
            originalValue = Environment.GetEnvironmentVariable(name);
            Environment.SetEnvironmentVariable(name, value);
        }

        public void Dispose()
        {
            Environment.SetEnvironmentVariable(name, originalValue);
        }
    }
}
