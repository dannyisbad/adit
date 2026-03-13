using System.Net.WebSockets;
using System.Net;

namespace Adit.Daemon.Tests;

[Collection(EnvironmentVariableCollection.Name)]
public sealed class DaemonBindingTests
{
    [Fact]
    public async Task KestrelHost_BindsToLoopbackAddress()
    {
        using var url = new EnvironmentVariableScope("ADIT_URL", "http://127.0.0.1:0");
        using var factory = new DaemonApiTestFactory();
        factory.UseKestrel();

        using var client = factory.CreateClient();

        Assert.NotNull(client.BaseAddress);
        Assert.True(
            IsLoopbackHost(client.BaseAddress!.Host),
            $"Expected loopback host, got '{client.BaseAddress}'.");

        using var response = await client.GetAsync("/v1/info");
        response.EnsureSuccessStatusCode();
    }

    [Fact]
    public async Task KestrelHost_WithPortZero_AllowsSameOriginWebSocketUpgrade()
    {
        using var url = new EnvironmentVariableScope("ADIT_URL", "http://127.0.0.1:0");
        using var factory = new DaemonApiTestFactory();
        factory.UseKestrel();

        using var client = factory.CreateClient();
        Assert.NotNull(client.BaseAddress);

        using var socket = new ClientWebSocket();
        socket.Options.SetRequestHeader("Origin", BuildOrigin(client.BaseAddress!));

        await socket.ConnectAsync(BuildWebSocketUri(client.BaseAddress!), CancellationToken.None);

        Assert.Equal(WebSocketState.Open, socket.State);

        await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None);
    }

    private static Uri BuildWebSocketUri(Uri baseAddress)
    {
        var builder = new UriBuilder(baseAddress)
        {
            Scheme = string.Equals(baseAddress.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase)
                ? "wss"
                : "ws",
            Path = "/v1/ws",
            Query = string.Empty
        };
        return builder.Uri;
    }

    private static string BuildOrigin(Uri baseAddress)
    {
        var builder = new UriBuilder(baseAddress)
        {
            Path = string.Empty,
            Query = string.Empty
        };
        return builder.Uri.ToString().TrimEnd('/');
    }

    private static bool IsLoopbackHost(string host)
    {
        return string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase)
            || (IPAddress.TryParse(host, out var address) && IPAddress.IsLoopback(address));
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
