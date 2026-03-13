using System.Net;
using System.Net.WebSockets;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;

namespace Adit.Daemon.Tests;

public sealed class DaemonApiTests : IClassFixture<DaemonApiTestFactory>
{
    private readonly DaemonApiTestFactory factory;
    private readonly HttpClient client;

    public DaemonApiTests(DaemonApiTestFactory factory)
    {
        this.factory = factory;
        client = factory.CreateClient();
    }

    [Fact]
    public async Task GetInfo_ReturnsDiscoveryDocument()
    {
        var response = await client.GetAsync("/v1/info");

        response.EnsureSuccessStatusCode();
        var payload = await ReadJsonAsync(response);

        Assert.Equal("adit", payload.GetProperty("name").GetString());
        Assert.Equal("Adit.Daemon", payload.GetProperty("daemon").GetString());
        Assert.Equal("v1", payload.GetProperty("apiVersion").GetString());
        Assert.Contains(
            payload.GetProperty("endpoints").EnumerateArray().Select(item => item.GetString()),
            endpoint => endpoint == "GET /v1/doctor");
    }

    [Fact]
    public async Task GetRuntime_ReturnsSeededSnapshot()
    {
        var response = await client.GetAsync("/v1/runtime");

        response.EnsureSuccessStatusCode();
        var payload = await ReadJsonAsync(response);

        Assert.Equal("ready", payload.GetProperty("phase").GetString());
        Assert.Equal(1, payload.GetProperty("contactCount").GetInt32());
        Assert.Equal(1, payload.GetProperty("messageCount").GetInt32());
        Assert.Equal("Test iPhone", payload.GetProperty("target").GetProperty("name").GetString());
    }

    [Fact]
    public async Task GetCapabilities_ReturnsCoreAndNotificationsState()
    {
        var response = await client.GetAsync("/v1/capabilities");

        response.EnsureSuccessStatusCode();
        var payload = await ReadJsonAsync(response);

        Assert.Equal("ready", payload.GetProperty("capabilities").GetProperty("messaging").GetProperty("state").GetString());
        Assert.Equal("ready", payload.GetProperty("capabilities").GetProperty("contacts").GetProperty("state").GetString());
        Assert.Equal("ready", payload.GetProperty("capabilities").GetProperty("notifications").GetProperty("state").GetString());
        Assert.Equal("complete", payload.GetProperty("setup").GetProperty("state").GetString());
    }

    [Fact]
    public async Task GetSetupGuide_ReturnsStructuredStepsAndActions()
    {
        var response = await client.GetAsync("/v1/setup/guide");

        response.EnsureSuccessStatusCode();
        var payload = await ReadJsonAsync(response);

        Assert.Equal("ready", payload.GetProperty("overall").GetString());
        Assert.Equal("complete", payload.GetProperty("setup").GetProperty("state").GetString());
        Assert.Equal("bootstrap_pairing", payload.GetProperty("steps")[0].GetProperty("id").GetString());
        Assert.Equal("complete", payload.GetProperty("steps")[0].GetProperty("status").GetString());
        Assert.Contains(
            payload.GetProperty("actions").EnumerateArray().Select(item => item.GetProperty("id").GetString()),
            id => id == "trigger_sync");
        Assert.Contains(
            payload.GetProperty("integrations").EnumerateArray().Select(item => item.GetProperty("id").GetString()),
            id => id == "claude_code_project_mcp");
    }

    [Fact]
    public async Task CheckSetup_ReturnsCurrentGuideSnapshot()
    {
        var response = await client.PostAsync("/v1/setup/check", content: null);

        response.EnsureSuccessStatusCode();
        var payload = await ReadJsonAsync(response);

        Assert.Equal("ready", payload.GetProperty("doctor").GetProperty("overall").GetString());
        Assert.Equal("notifications", payload.GetProperty("steps")[2].GetProperty("id").GetString());
    }

    [Fact]
    public async Task GetAgentContext_ReturnsAgentWorkflowHints()
    {
        var response = await client.GetAsync("/v1/agent/context");

        response.EnsureSuccessStatusCode();
        var payload = await ReadJsonAsync(response);

        Assert.EndsWith("/v1/ws", payload.GetProperty("webSocketUrl").GetString());
        Assert.True(payload.GetProperty("agent").GetProperty("sendCapabilities").GetProperty("supportsConversationId").GetBoolean());
        Assert.Equal(
            "GET /v1/setup/guide",
            payload.GetProperty("agent").GetProperty("preferredReadOrder")[0].GetString());
        Assert.Equal("ready", payload.GetProperty("setupGuide").GetProperty("overall").GetString());
        Assert.Equal(
            ".mcp.json",
            payload.GetProperty("agent").GetProperty("integrationHints").GetProperty("projectFiles")[0].GetString());
    }

    [Fact]
    public async Task GetDoctor_ReturnsNextStepSummary()
    {
        var response = await client.GetAsync("/v1/doctor");

        response.EnsureSuccessStatusCode();
        var payload = await ReadJsonAsync(response);

        Assert.Equal("ready", payload.GetProperty("doctor").GetProperty("overall").GetString());
        Assert.Equal("Messaging, contacts, and notifications are ready.", payload.GetProperty("doctor").GetProperty("summary").GetString());
        Assert.Equal("No action required.", payload.GetProperty("doctor").GetProperty("nextSteps")[0].GetString());
    }

    [Fact]
    public async Task GetThreadChooserStatus_ReturnsDisabledSnapshotWhenFeatureIsOff()
    {
        var response = await client.GetAsync("/v1/thread-chooser/status");

        response.EnsureSuccessStatusCode();
        var payload = await ReadJsonAsync(response);

        Assert.False(payload.GetProperty("enabled").GetBoolean());
        Assert.Equal("disabled", payload.GetProperty("status").GetString());
        Assert.Equal("Qwen/Qwen3-0.6B-Base", payload.GetProperty("configuredModelName").GetString());
        Assert.Equal(5048, payload.GetProperty("port").GetInt32());
    }

    [Fact]
    public async Task NotificationsCheck_ReturnsBootstrapPayload()
    {
        var response = await client.PostAsync("/v1/notifications/check", content: null);

        response.EnsureSuccessStatusCode();
        var payload = await ReadJsonAsync(response);

        Assert.Equal("ready", payload.GetProperty("capability").GetProperty("state").GetString());
        Assert.Equal("bootstrapped", payload.GetProperty("notificationsSetup").GetProperty("state").GetString());
        Assert.Equal("link_to_windows_once", payload.GetProperty("notificationsSetup").GetProperty("recommendedFlow").GetString());
    }

    [Fact]
    public async Task SyncNow_AcceptsManualTrigger()
    {
        var response = await client.PostAsync("/v1/sync/now?reason=manual_test", content: null);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var payload = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.True(payload.GetProperty("accepted").GetBoolean());
        Assert.Equal("manual_test", payload.GetProperty("reason").GetString());
    }

    [Fact]
    public async Task GetRecentEvents_ReturnsSeededEvent()
    {
        var response = await client.GetAsync("/v1/events/recent?limit=1");

        response.EnsureSuccessStatusCode();
        var payload = await ReadJsonAsync(response);

        Assert.Equal(1, payload.GetProperty("count").GetInt32());
        Assert.Equal("test.seeded", payload.GetProperty("events")[0].GetProperty("type").GetString());
    }

    [Fact]
    public async Task WebSocketHello_UsesCamelCaseEventShape()
    {
        using var socket = await factory.Server.CreateWebSocketClient()
            .ConnectAsync(new Uri("ws://localhost/v1/ws"), CancellationToken.None);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var payload = await ReceiveTextAsync(socket, cts.Token);
        using var json = JsonDocument.Parse(payload);

        Assert.True(json.RootElement.TryGetProperty("type", out var type));
        Assert.Equal("hello", type.GetString());
        Assert.True(json.RootElement.TryGetProperty("payload", out var helloPayload));
        Assert.Equal("ready", helloPayload.GetProperty("runtime").GetProperty("phase").GetString());
        Assert.False(json.RootElement.TryGetProperty("Type", out _));
    }

    [Fact]
    public async Task SearchContacts_ReturnsSeededContact()
    {
        var response = await client.GetAsync("/v1/contacts/search?query=mom");

        response.EnsureSuccessStatusCode();
        var payload = await ReadJsonAsync(response);

        Assert.Equal(1, payload.GetProperty("count").GetInt32());
        Assert.Equal("Mom", payload.GetProperty("contacts")[0].GetProperty("displayName").GetString());
    }

    [Fact]
    public async Task ListNotifications_ReturnsSeededNotification()
    {
        var response = await client.GetAsync("/v1/notifications");

        response.EnsureSuccessStatusCode();
        var payload = await ReadJsonAsync(response);

        Assert.Equal(1, payload.GetProperty("count").GetInt32());
        Assert.Equal("Mom", payload.GetProperty("notifications")[0].GetProperty("notification").GetProperty("title").GetString());
        Assert.Equal("Need milk", payload.GetProperty("notifications")[0].GetProperty("notification").GetProperty("message").GetString());
    }

    [Fact]
    public async Task ListConversations_ReturnsSeededConversation()
    {
        var response = await client.GetAsync("/v1/conversations");

        response.EnsureSuccessStatusCode();
        var payload = await ReadJsonAsync(response);

        Assert.Equal(1, payload.GetProperty("count").GetInt32());
        Assert.StartsWith("th_", payload.GetProperty("conversations")[0].GetProperty("conversationId").GetString());
        Assert.Equal("Mom", payload.GetProperty("conversations")[0].GetProperty("displayName").GetString());
    }

    [Fact]
    public async Task GetCachedMessages_ReturnsSeededMessage()
    {
        var response = await client.GetAsync("/v1/cache/messages?conversationId=th_seeded_mom");

        response.EnsureSuccessStatusCode();
        var payload = await ReadJsonAsync(response);

        Assert.Equal(1, payload.GetProperty("count").GetInt32());
        Assert.Equal("Need milk", payload.GetProperty("messages")[0].GetProperty("message").GetProperty("body").GetString());
    }

    [Fact]
    public async Task GetConversationMessages_ReturnsSeededConversationPayload()
    {
        var response = await client.GetAsync("/v1/conversations/th_seeded_mom");

        response.EnsureSuccessStatusCode();
        var payload = await ReadJsonAsync(response);

        Assert.StartsWith("th_", payload.GetProperty("conversationId").GetString());
        Assert.Equal(1, payload.GetProperty("count").GetInt32());
        Assert.Equal("Need milk", payload.GetProperty("messages")[0].GetProperty("message").GetProperty("body").GetString());
    }

    [Fact]
    public async Task ResolveMessage_UsesConversationRecipientForAgentReplies()
    {
        var response = await client.PostAsJsonAsync(
            "/v1/messages/resolve",
            new
            {
                conversationId = "th_seeded_mom",
                body = "On it"
            });

        response.EnsureSuccessStatusCode();
        var payload = await ReadJsonAsync(response);

        Assert.True(payload.GetProperty("ready").GetBoolean());
        Assert.Equal("conversation", payload.GetProperty("resolutionSource").GetString());
        Assert.Equal("+12025550114", payload.GetProperty("recipient").GetString());
        Assert.StartsWith(
            "th_",
            payload.GetProperty("resolvedConversation").GetProperty("conversationId").GetString());
        Assert.Equal("Mom", payload.GetProperty("resolvedConversation").GetProperty("displayName").GetString());
    }

    [Fact]
    public async Task ResolveMessage_RejectsMissingRecipientInputs()
    {
        var response = await client.PostAsJsonAsync(
            "/v1/messages/resolve",
            new
            {
                body = "No target"
            });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var payload = await ReadJsonAsync(response);
        Assert.Equal(
            "recipient, conversationId, or a resolvable contact is required.",
            payload.GetProperty("error").GetString());
    }

    private static async Task<JsonElement> ReadJsonAsync(HttpResponseMessage response)
    {
        var payload = await response.Content.ReadFromJsonAsync<JsonElement>();
        return payload;
    }

    private static async Task<string> ReceiveTextAsync(WebSocket socket, CancellationToken cancellationToken)
    {
        var buffer = new byte[16 * 1024];
        var result = await socket.ReceiveAsync(buffer, cancellationToken);
        return Encoding.UTF8.GetString(buffer, 0, result.Count);
    }
}
