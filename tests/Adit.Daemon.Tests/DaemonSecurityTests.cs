using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Net.WebSockets;
using System.Text.Json;

namespace Adit.Daemon.Tests;

[Collection(EnvironmentVariableCollection.Name)]
public sealed class DaemonSecurityTests
{
    [Fact]
    public async Task AuthToken_RejectsUnauthenticatedApiRequests()
    {
        using var token = new EnvironmentVariableScope("ADIT_AUTH_TOKEN", "test-token");
        using var factory = new DaemonApiTestFactory();
        using var client = factory.CreateClient();

        using var response = await client.GetAsync("/v1/runtime");

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        Assert.Contains(response.Headers.WwwAuthenticate, value => value.Scheme == "Bearer");

        var payload = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("Bearer token required.", payload.GetProperty("error").GetString());
    }

    [Fact]
    public async Task AuthToken_AllowsBearerAuthenticatedApiRequests()
    {
        using var token = new EnvironmentVariableScope("ADIT_AUTH_TOKEN", "test-token");
        using var factory = new DaemonApiTestFactory();
        using var client = factory.CreateClient();
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", "test-token");

        using var response = await client.GetAsync("/v1/runtime");

        response.EnsureSuccessStatusCode();
        var payload = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("ready", payload.GetProperty("phase").GetString());
    }

    [Fact]
    public async Task AuthToken_RejectsTrustedBrowserReadsWithoutBearerToken()
    {
        using var token = new EnvironmentVariableScope("ADIT_AUTH_TOKEN", "test-token");
        using var factory = new DaemonApiTestFactory();
        using var client = factory.CreateClient();
        using var request = new HttpRequestMessage(HttpMethod.Get, "/v1/runtime");
        request.Headers.TryAddWithoutValidation("Sec-Fetch-Site", "same-origin");

        using var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        var payload = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("Bearer token required.", payload.GetProperty("error").GetString());
    }

    [Fact]
    public async Task AuthToken_RejectsUnauthenticatedRootDiscoveryWhenUiIsUnavailable()
    {
        using var token = new EnvironmentVariableScope("ADIT_AUTH_TOKEN", "test-token");
        using var factory = new DaemonApiTestFactory();
        using var client = factory.CreateClient();

        using var response = await client.GetAsync("/");

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        var payload = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("Bearer token required.", payload.GetProperty("error").GetString());
    }

    [Fact]
    public async Task BrowserPost_WithUntrustedOrigin_IsRejected()
    {
        using var factory = new DaemonApiTestFactory();
        using var client = factory.CreateClient();
        using var request = new HttpRequestMessage(HttpMethod.Post, "/v1/sync/now?reason=cross_site");
        request.Headers.TryAddWithoutValidation("Origin", "http://evil.example");

        using var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        var payload = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("Untrusted browser origin.", payload.GetProperty("error").GetString());
    }

    [Fact]
    public async Task BrowserPost_WithTrustedOrigin_IsAllowed()
    {
        using var url = new EnvironmentVariableScope("ADIT_URL", "http://localhost");
        using var token = new EnvironmentVariableScope("ADIT_AUTH_TOKEN", "test-token");
        using var factory = new DaemonApiTestFactory();
        using var client = factory.CreateClient();
        using var request = new HttpRequestMessage(HttpMethod.Post, "/v1/sync/now?reason=same_origin");
        request.Headers.TryAddWithoutValidation("Origin", "http://localhost");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", "test-token");

        using var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var payload = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("same_origin", payload.GetProperty("reason").GetString());
    }

    [Fact]
    public async Task BrowserPost_WithSameOriginFetchMetadata_IsAllowed()
    {
        using var token = new EnvironmentVariableScope("ADIT_AUTH_TOKEN", "test-token");
        using var factory = new DaemonApiTestFactory();
        using var client = factory.CreateClient();
        using var request = new HttpRequestMessage(HttpMethod.Post, "/v1/sync/now?reason=fetch_metadata");
        request.Headers.TryAddWithoutValidation("Sec-Fetch-Site", "same-origin");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", "test-token");

        using var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var payload = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("fetch_metadata", payload.GetProperty("reason").GetString());
    }

    [Fact]
    public async Task WebSocket_WithAccessTokenQuery_IsAllowedWhenAuthTokenIsConfigured()
    {
        using var token = new EnvironmentVariableScope("ADIT_AUTH_TOKEN", "test-token");
        using var factory = new DaemonApiTestFactory();

        using var socket = await factory.Server.CreateWebSocketClient()
            .ConnectAsync(new Uri("ws://localhost/v1/ws?access_token=test-token"), CancellationToken.None);

        Assert.Equal(WebSocketState.Open, socket.State);
    }

    [Fact]
    public async Task WebSocket_WithTrustedOrigin_IsRejectedWithoutAccessTokenWhenAuthTokenIsConfigured()
    {
        using var token = new EnvironmentVariableScope("ADIT_AUTH_TOKEN", "test-token");
        using var url = new EnvironmentVariableScope("ADIT_URL", "http://localhost");
        using var factory = new DaemonApiTestFactory();
        var client = factory.Server.CreateWebSocketClient();
        client.ConfigureRequest = request => request.Headers.Origin = "http://localhost";

        var exception = await Assert.ThrowsAsync<WebSocketException>(
            () => client.ConnectAsync(new Uri("ws://localhost/v1/ws"), CancellationToken.None));

        Assert.Contains("401", exception.Message);
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
