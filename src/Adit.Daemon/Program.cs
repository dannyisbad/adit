using System.Net.WebSockets;
using System.Net;
using System.Text;
using System.Text.Json;
using Adit.Core.Models;
using Adit.Core.Services;
using Adit.Core.Utilities;
using Adit.Daemon;
using Adit.Daemon.Services;
using Microsoft.Extensions.FileProviders;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Windows.Devices.Enumeration;
var eventJsonOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web);
var daemonOptions = DaemonOptions.FromEnvironment();
var builderArgs = await DaemonCli.ResolveServerArgsOrHandleAsync(args, daemonOptions, CancellationToken.None);
if (builderArgs is null)
{
    return;
}

var builder = WebApplication.CreateBuilder(builderArgs);

builder.WebHost.ConfigureKestrel(options => ConfigureLoopbackBinding(options, daemonOptions.ListenUri));
builder.Services.AddSingleton(daemonOptions);
builder.Services.AddSingleton<DeviceCatalog>();
builder.Services.AddSingleton<BluetoothPairingService>();
builder.Services.AddSingleton<AppleBleAddressResolver>();
builder.Services.AddSingleton<PhoneLinkProcessCatalog>();
builder.Services.AddSingleton<PhoneLinkProcessController>();
builder.Services.AddSingleton<PbapContactsService>();
builder.Services.AddSingleton<MapMessagingService>();
builder.Services.AddSingleton<MapRealtimeSession>();
builder.Services.AddSingleton<AncsSession>();
builder.Services.AddSingleton<ConversationSynthesizer>();
builder.Services.AddSingleton<SqliteCacheStore>();
builder.Services.AddSingleton<DeviceFusionCoordinator>();
builder.Services.AddSingleton<RuntimeStateService>();
builder.Services.AddSingleton<DaemonEventHub>();
builder.Services.AddSingleton<PhoneLinkEvictionPolicy>();
builder.Services.AddSingleton<LearnedThreadReranker>();
builder.Services.AddSingleton<DeviceSyncService>();
if (!daemonOptions.DisableBackgroundSync)
{
    builder.Services.AddHostedService(services => services.GetRequiredService<DeviceSyncService>());
}

var app = builder.Build();
app.UseWebSockets();

var uiPath = NormalizeHostedPath(Path.Combine(AppContext.BaseDirectory, "wwwroot"));
if (Directory.Exists(uiPath))
{
    app.UseStaticFiles(new StaticFileOptions
    {
        FileProvider = new PhysicalFileProvider(uiPath)
    });
}

var hasUi = Directory.Exists(uiPath);

var publicEndpoints = new List<string>
{
    "GET /v1/status",
    "GET /v1/runtime",
    "GET /v1/capabilities",
    "GET /v1/doctor",
    "GET /v1/thread-chooser/status",
    "GET /v1/setup/guide",
    "POST /v1/setup/check",
    "GET /v1/agent/context",
    "POST /v1/sync/now",
    "GET /v1/events",
    "GET /v1/ws",
    "GET /v1/events/recent",
    "GET /v1/devices",
    "POST /v1/notifications/check",
    "POST /v1/notifications/enable",
    "POST /v1/notifications/disable",
    "GET /v1/contacts",
    "GET /v1/contacts/search",
    "GET /v1/notifications",
    "POST /v1/notifications/{notificationUid}/actions/{action}",
    "GET /v1/messages/folders",
    "GET /v1/messages",
    "GET /v1/cache/messages",
    "GET /v1/conversations",
    "GET /v1/conversations/{conversationId}",
    "POST /v1/messages/resolve",
    "POST /v1/messages/send"
};

if (daemonOptions.EnableExperimentalPairingApi)
{
    publicEndpoints.Add("GET /v1/pairing/candidates");
    publicEndpoints.Add("POST /v1/pairing/pair");
    publicEndpoints.Add("POST /v1/pairing/unpair");
}

// When the UI is deployed, / serves the SPA via the fallback.
// Otherwise, serve the JSON discovery document at /.
if (!hasUi)
{
    app.MapGet(
        "/",
        (RuntimeStateService runtimeStateService) =>
        {
            var runtime = runtimeStateService.GetSnapshot();
            return Results.Ok(
                new
                {
                    name = "adit",
                    daemon = "Adit.Daemon",
                    apiVersion = "v1",
                    listen = daemonOptions.ListenUrl,
                    packageIdentity = PackageIdentitySnapshot.Capture(),
                    runtime,
                    setup = CapabilitySnapshotBuilder.BuildSetup(runtime, daemonOptions),
                    webSocketUrl = BuildWebSocketUrl(daemonOptions.ListenUrl),
                    setupGuide = "/v1/setup/guide",
                    endpoints = publicEndpoints
                });
        });
}

// Always available at /v1/info regardless of UI deployment.
app.MapGet(
    "/v1/info",
    (RuntimeStateService runtimeStateService) =>
    {
        var runtime = runtimeStateService.GetSnapshot();
        return Results.Ok(
            new
            {
                name = "adit",
                daemon = "Adit.Daemon",
                apiVersion = "v1",
                listen = daemonOptions.ListenUrl,
                packageIdentity = PackageIdentitySnapshot.Capture(),
                runtime,
                setup = CapabilitySnapshotBuilder.BuildSetup(runtime, daemonOptions),
                webSocketUrl = BuildWebSocketUrl(daemonOptions.ListenUrl),
                setupGuide = "/v1/setup/guide",
                endpoints = publicEndpoints
            });
    });

SetupGuideSnapshot BuildSetupGuide(RuntimeStateService runtimeStateService)
{
    var runtime = runtimeStateService.GetSnapshot();
    return CapabilitySnapshotBuilder.BuildGuide(runtime, daemonOptions);
}

app.MapGet("/v1/setup/guide", BuildSetupGuide);
app.MapPost("/v1/setup/check", BuildSetupGuide);

app.MapGet(
    "/v1/agent/context",
    (RuntimeStateService runtimeStateService) =>
    {
        var runtime = runtimeStateService.GetSnapshot();
        var setup = CapabilitySnapshotBuilder.BuildSetup(runtime, daemonOptions);
        var capabilities = CapabilitySnapshotBuilder.Build(runtime, daemonOptions);
        var doctor = CapabilitySnapshotBuilder.BuildDoctor(runtime, daemonOptions);
        var setupGuide = CapabilitySnapshotBuilder.BuildGuide(runtime, daemonOptions);
        return Results.Ok(
            new
            {
                name = "adit",
                daemon = "Adit.Daemon",
                apiVersion = "v1",
                listen = daemonOptions.ListenUrl,
                webSocketUrl = BuildWebSocketUrl(daemonOptions.ListenUrl),
                runtime,
                setup,
                capabilities,
                doctor,
                setupGuide,
                agent = new
                {
                    preferredReadOrder = new[]
                    {
                        "GET /v1/setup/guide",
                        "GET /v1/agent/context",
                        "GET /v1/conversations",
                        "GET /v1/conversations/{conversationId}"
                    },
                    sendWorkflow = new[]
                    {
                        "Read setupGuide before making setup or recovery suggestions.",
                        "Use conversationId for replies to known one-to-one threads when possible.",
                        "Call POST /v1/messages/resolve before sending if a model needs a no-side-effect target/recipient check.",
                        "Fall back to contactId/contactName or an explicit recipient when conversationId is unavailable."
                    },
                    sendCapabilities = new
                    {
                        supportsConversationId = true,
                        supportsDryRunResolution = true,
                        recipientPriority = new[]
                        {
                            "recipient",
                            "conversationId",
                            "contactId/contactName"
                        }
                    },
                    integrationHints = new
                    {
                        recommendedHost = "claude_code_project",
                        suggestedPrompt = "Set this up for Claude Code in this repo.",
                        projectFiles = new[]
                        {
                            ".mcp.json",
                            ".claude/agents/adit-operator.md"
                        }
                    }
                },
                endpoints = publicEndpoints
            });
    });

app.MapGet(
    "/v1/status",
    async (DeviceCatalog deviceCatalog, PhoneLinkProcessCatalog processCatalog, RuntimeStateService runtimeStateService) =>
    {
        var runtime = runtimeStateService.GetSnapshot();
        var leDevices = await deviceCatalog.ListPairedBluetoothLeDevicesAsync();
        var endpoints = await deviceCatalog.ListPairedBluetoothEndpointsAsync();
        return Results.Ok(
            new
            {
                packageIdentity = PackageIdentitySnapshot.Capture(),
                runtime,
                setup = CapabilitySnapshotBuilder.BuildSetup(runtime, daemonOptions),
                capabilities = CapabilitySnapshotBuilder.Build(runtime, daemonOptions),
                notificationsSetup = CapabilitySnapshotBuilder.BuildNotificationsBootstrap(runtime, daemonOptions),
                notificationsBootstrap = CapabilitySnapshotBuilder.BuildNotificationsBootstrap(runtime, daemonOptions),
                leDeviceCount = leDevices.Count,
                endpointCount = endpoints.Count,
                processes = processCatalog.ListRunning(),
                leDevices,
                endpoints
            });
    });

app.MapGet(
    "/v1/runtime",
    (RuntimeStateService runtimeStateService) => Results.Ok(runtimeStateService.GetSnapshot()));

app.MapGet(
    "/v1/capabilities",
    (RuntimeStateService runtimeStateService) =>
    {
        var runtime = runtimeStateService.GetSnapshot();
        return Results.Ok(
            new
            {
                runtime,
                setup = CapabilitySnapshotBuilder.BuildSetup(runtime, daemonOptions),
                capabilities = CapabilitySnapshotBuilder.Build(runtime, daemonOptions),
                notificationsSetup = CapabilitySnapshotBuilder.BuildNotificationsBootstrap(runtime, daemonOptions),
                notificationsBootstrap = CapabilitySnapshotBuilder.BuildNotificationsBootstrap(runtime, daemonOptions)
            });
    });

app.MapGet(
    "/v1/doctor",
    (RuntimeStateService runtimeStateService) =>
    {
        var runtime = runtimeStateService.GetSnapshot();
        return Results.Ok(
            new
            {
                runtime,
                setup = CapabilitySnapshotBuilder.BuildSetup(runtime, daemonOptions),
                doctor = CapabilitySnapshotBuilder.BuildDoctor(runtime, daemonOptions)
            });
    });

IResult BuildNotificationsCheck(RuntimeStateService runtimeStateService)
{
    var runtime = runtimeStateService.GetSnapshot();
    var notificationsSetup = CapabilitySnapshotBuilder.BuildNotificationsBootstrap(runtime, daemonOptions);
    return Results.Ok(
        new
        {
            runtime,
            capability = CapabilitySnapshotBuilder.Build(runtime, daemonOptions).Notifications,
            notificationsSetup,
            notificationsBootstrap = notificationsSetup
        });
}

async Task<IResult> EnableNotificationsAsync(
    DeviceSyncService syncService,
    CancellationToken cancellationToken)
{
    var result = await syncService.SetNotificationsEnabledAsync(true, cancellationToken);
    return result.Ready
        ? Results.Ok(result)
        : Results.Json(result, statusCode: StatusCodes.Status409Conflict);
}

async Task<IResult> DisableNotificationsAsync(
    DeviceSyncService syncService,
    CancellationToken cancellationToken)
{
    var result = await syncService.SetNotificationsEnabledAsync(false, cancellationToken);
    return Results.Ok(result);
}

app.MapPost("/v1/notifications/check", BuildNotificationsCheck);
app.MapPost("/v1/notifications/enable", EnableNotificationsAsync);
app.MapPost("/v1/notifications/disable", DisableNotificationsAsync);

// Compatibility aliases for older bootstrap naming.
app.MapPost("/v1/bootstrap/notifications/check", BuildNotificationsCheck);
app.MapPost("/v1/bootstrap/notifications/enable", EnableNotificationsAsync);
app.MapPost("/v1/bootstrap/notifications/disable", DisableNotificationsAsync);

app.MapPost(
    "/v1/sync/now",
    (string? reason, DeviceSyncService syncService) =>
    {
        syncService.RequestSync(string.IsNullOrWhiteSpace(reason) ? "manual" : reason.Trim());
        return Results.Accepted(
            value: new
            {
                accepted = true,
                reason = string.IsNullOrWhiteSpace(reason) ? "manual" : reason.Trim()
            });
    });

app.MapGet(
    "/v1/events/recent",
    (int? limit, DaemonEventHub eventHub) => Results.Ok(
        new
        {
            count = Math.Max(1, limit ?? 25),
            events = eventHub.GetRecent(limit ?? 25)
        }));

app.MapGet(
    "/v1/thread-chooser/status",
    async Task<IResult> (
        LearnedThreadReranker learnedThreadReranker,
        CancellationToken cancellationToken) =>
    {
        var status = await learnedThreadReranker.GetStatusAsync(cancellationToken);
        return Results.Ok(status);
    });

app.MapGet(
    "/v1/events",
    async (
        HttpContext context,
        DaemonEventHub eventHub,
        RuntimeStateService runtimeStateService,
        CancellationToken cancellationToken) =>
    {
        context.Response.Headers.CacheControl = "no-cache";
        context.Response.Headers.Connection = "keep-alive";
        context.Response.ContentType = "text/event-stream";

        await WriteSseAsync(
            context,
            new DaemonEventRecord(
                0,
                DateTimeOffset.UtcNow,
                "hello",
                new
                {
                    runtime = runtimeStateService.GetSnapshot()
                }),
            cancellationToken);

        await using var subscription = eventHub.Subscribe(cancellationToken);
        await foreach (var item in subscription.Reader.ReadAllAsync(cancellationToken))
        {
            await WriteSseAsync(context, item, cancellationToken);
        }
    });

app.MapGet(
    "/v1/ws",
    async (
        HttpContext context,
        DaemonEventHub eventHub,
        RuntimeStateService runtimeStateService,
        CancellationToken cancellationToken) =>
    {
        if (!context.WebSockets.IsWebSocketRequest)
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsJsonAsync(
                new
                {
                    error = "WebSocket upgrade required."
                },
                cancellationToken);
            return;
        }

        using var socket = await context.WebSockets.AcceptWebSocketAsync();
        await WriteWebSocketAsync(
            socket,
            new DaemonEventRecord(
                0,
                DateTimeOffset.UtcNow,
                "hello",
                new
                {
                    runtime = runtimeStateService.GetSnapshot()
                }),
            cancellationToken);

        await using var subscription = eventHub.Subscribe(cancellationToken);
        await foreach (var item in subscription.Reader.ReadAllAsync(cancellationToken))
        {
            if (socket.State != WebSocketState.Open)
            {
                break;
            }

            await WriteWebSocketAsync(socket, item, cancellationToken);
        }

        if (socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
        {
            await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", cancellationToken);
        }
    });

app.MapGet(
    "/v1/devices",
    async (DeviceCatalog deviceCatalog) =>
    {
        var leDevices = await deviceCatalog.ListPairedBluetoothLeDevicesAsync();
        var endpoints = await deviceCatalog.ListPairedBluetoothEndpointsAsync();
        return Results.Ok(new { leDevices, endpoints });
    });

if (daemonOptions.EnableExperimentalPairingApi)
{
    app.MapGet(
        "/v1/pairing/candidates",
        async Task<IResult> (
            bool? paired,
            string? transport,
            string? nameContains,
            BluetoothPairingService pairingService,
            CancellationToken cancellationToken) =>
        {
            var effectivePaired = paired ?? false;
            var candidates = await pairingService.ListCandidatesAsync(
                effectivePaired,
                transport,
                nameContains,
                cancellationToken);
            return Results.Ok(
                new
                {
                    paired = effectivePaired,
                    transport = BluetoothPairingConventions.NormalizeTransport(transport),
                    count = candidates.Count,
                    candidates
                });
        });

    app.MapPost(
        "/v1/pairing/pair",
        async Task<IResult> (
            PairDeviceRequest request,
            BluetoothPairingService pairingService,
            DeviceSyncService syncService,
            DaemonEventHub eventHub,
            CancellationToken cancellationToken) =>
        {
            var candidate = await ResolvePairingCandidateAsync(
                pairingService,
                request.DeviceId,
                request.NameContains,
                paired: false,
                request.Transport,
                cancellationToken);
            if (candidate is null)
            {
                return Results.NotFound(new { error = "No unpaired Bluetooth candidate matched the request." });
            }

            var protectionLevel = ParseProtectionLevel(request.ProtectionLevel);
            if (protectionLevel is null)
            {
                return Results.BadRequest(
                    new
                    {
                        error = "protectionLevel must be one of: default, none, encryption, encryption_and_authentication."
                    });
            }

            var result = await pairingService.PairAsync(candidate.Id, request.Pin, protectionLevel.Value, cancellationToken);
            eventHub.Publish(
                "pairing.completed",
                new
                {
                    candidate,
                    result
                });

            if (request.AutoSyncAfterPair && result.IsPaired)
            {
                syncService.RequestSync("pairing_completed");
            }

            return result.IsPaired
                ? Results.Ok(new { candidate, result })
                : Results.StatusCode(StatusCodes.Status409Conflict);
        });

    app.MapPost(
        "/v1/pairing/unpair",
        async Task<IResult> (
            UnpairDeviceRequest request,
            BluetoothPairingService pairingService,
            DeviceSyncService syncService,
            DaemonEventHub eventHub,
            CancellationToken cancellationToken) =>
        {
            var candidate = await ResolvePairingCandidateAsync(
                pairingService,
                request.DeviceId,
                request.NameContains,
                paired: true,
                request.Transport,
                cancellationToken);
            if (candidate is null)
            {
                return Results.NotFound(new { error = "No paired Bluetooth candidate matched the request." });
            }

            var result = await pairingService.UnpairAsync(candidate.Id, cancellationToken);
            eventHub.Publish(
                "pairing.unpaired",
                new
                {
                    candidate,
                    result
                });

            if (request.AutoSyncAfterUnpair)
            {
                syncService.RequestSync("pairing_unpaired");
            }

            return Results.Ok(new { candidate, result });
        });
}

app.MapGet(
    "/v1/contacts",
    async Task<IResult> (
        string? deviceId,
        string? nameContains,
        bool? evictPhoneLink,
        DeviceCatalog deviceCatalog,
        RuntimeStateService runtimeStateService,
        PhoneLinkEvictionPolicy evictionPolicy,
        PbapContactsService contactsService,
        CancellationToken cancellationToken) =>
    {
        var target = await ResolveClassicTargetAsync(deviceCatalog, runtimeStateService, daemonOptions, deviceId, nameContains);
        if (target is null)
        {
            return Results.NotFound(new { error = "No classic Bluetooth target matched the requested device." });
        }

        var contacts = await contactsService.PullContactsAsync(
            target,
            await evictionPolicy.ShouldEvictForContactsAsync(
                target,
                evictPhoneLink,
                cancellationToken),
            cancellationToken);
        return Results.Ok(new { target, count = contacts.Count, contacts });
    });

app.MapGet(
    "/v1/contacts/search",
    async Task<IResult> (
        string query,
        string? deviceId,
        string? nameContains,
        int? limit,
        DeviceCatalog deviceCatalog,
        RuntimeStateService runtimeStateService,
        SqliteCacheStore cacheStore,
        CancellationToken cancellationToken) =>
    {
        if (string.IsNullOrWhiteSpace(query))
        {
            return Results.BadRequest(new { error = "query is required." });
        }

        var target = await ResolveClassicTargetAsync(deviceCatalog, runtimeStateService, daemonOptions, deviceId, nameContains);
        if (target is null)
        {
            return Results.NotFound(new { error = "No cached or paired target matched the requested device." });
        }

        var contacts = await cacheStore.SearchContactsAsync(target.Id, query, limit is > 0 ? limit.Value : 20, cancellationToken);
        return Results.Ok(new { target, count = contacts.Count, contacts });
    });

app.MapGet(
    "/v1/notifications",
    async Task<IResult> (
        string? deviceId,
        string? nameContains,
        bool? activeOnly,
        string? appIdentifier,
        int? limit,
        DeviceCatalog deviceCatalog,
        RuntimeStateService runtimeStateService,
        SqliteCacheStore cacheStore,
        CancellationToken cancellationToken) =>
    {
        var target = await ResolveClassicTargetAsync(deviceCatalog, runtimeStateService, daemonOptions, deviceId, nameContains);
        if (target is null)
        {
            return Results.NotFound(new { error = "No cached target matched the requested device." });
        }

        var notifications = await cacheStore.GetNotificationsAsync(
            target.Id,
            activeOnly ?? true,
            appIdentifier,
            limit is > 0 ? limit.Value : 50,
            cancellationToken);
        return Results.Ok(new { target, count = notifications.Count, notifications });
    });

app.MapPost(
    "/v1/notifications/{notificationUid:long}/actions/{action}",
    async Task<IResult> (
        long notificationUid,
        string action,
        AncsSession ancsSession,
        CancellationToken cancellationToken) =>
    {
        if (notificationUid <= 0 || notificationUid > uint.MaxValue)
        {
            return Results.BadRequest(new { error = "notificationUid must be a positive 32-bit integer." });
        }

        var parsedAction = action.Trim().ToLowerInvariant() switch
        {
            "positive" => NotificationAction.Positive,
            "negative" => NotificationAction.Negative,
            _ => (NotificationAction?)null
        };

        if (parsedAction is null)
        {
            return Results.BadRequest(new { error = "action must be positive or negative." });
        }

        var accepted = await ancsSession.PerformActionAsync((uint)notificationUid, parsedAction.Value, cancellationToken);
        return accepted
            ? Results.Ok(
                new
                {
                    notificationUid,
                    action = parsedAction.Value.ToString().ToLowerInvariant(),
                    accepted = true
                })
            : Results.StatusCode(StatusCodes.Status409Conflict);
    });

app.MapGet(
    "/v1/messages/folders",
    async Task<IResult> (
        string? deviceId,
        string? nameContains,
        bool? evictPhoneLink,
        DeviceCatalog deviceCatalog,
        RuntimeStateService runtimeStateService,
        PhoneLinkEvictionPolicy evictionPolicy,
        MapRealtimeSession mapRealtimeSession,
        MapMessagingService messagingService,
        CancellationToken cancellationToken) =>
    {
        var target = await ResolveClassicTargetAsync(deviceCatalog, runtimeStateService, daemonOptions, deviceId, nameContains);
        if (target is null)
        {
            return Results.NotFound(new { error = "No classic Bluetooth target matched the requested device." });
        }

        var resolvedEvictPhoneLink = await evictionPolicy.ShouldEvictForMapAsync(
            target,
            evictPhoneLink,
            cancellationToken);
        var folders = await ListFoldersViaBestAvailableAsync(
            target,
            resolvedEvictPhoneLink,
            mapRealtimeSession,
            messagingService,
            cancellationToken);
        return Results.Ok(new { target, count = folders.Count, folders });
    });

app.MapGet(
    "/v1/messages",
    async Task<IResult> (
        string? deviceId,
        string? nameContains,
        string? folder,
        int? limit,
        bool? evictPhoneLink,
        DeviceCatalog deviceCatalog,
        RuntimeStateService runtimeStateService,
        PhoneLinkEvictionPolicy evictionPolicy,
        MapRealtimeSession mapRealtimeSession,
        MapMessagingService messagingService,
        CancellationToken cancellationToken) =>
    {
        var target = await ResolveClassicTargetAsync(deviceCatalog, runtimeStateService, daemonOptions, deviceId, nameContains);
        if (target is null)
        {
            return Results.NotFound(new { error = "No classic Bluetooth target matched the requested device." });
        }

        var resolvedFolder = string.IsNullOrWhiteSpace(folder) ? "inbox" : folder.Trim();
        var resolvedLimit = limit is > 0 ? limit.Value : 25;
        var resolvedEvictPhoneLink = await evictionPolicy.ShouldEvictForMapAsync(
            target,
            evictPhoneLink,
            cancellationToken);
        var listing = await ListMessagesViaBestAvailableAsync(
            target,
            resolvedFolder,
            resolvedLimit,
            resolvedEvictPhoneLink,
            mapRealtimeSession,
            messagingService,
            cancellationToken);
        return Results.Ok(new { target, listing });
    });

app.MapGet(
    "/v1/cache/messages",
    async Task<IResult> (
        string? deviceId,
        string? nameContains,
        string? folder,
        string? conversationId,
        int? limit,
        DeviceCatalog deviceCatalog,
        RuntimeStateService runtimeStateService,
        SqliteCacheStore cacheStore,
        CancellationToken cancellationToken) =>
    {
        var target = await ResolveClassicTargetAsync(deviceCatalog, runtimeStateService, daemonOptions, deviceId, nameContains);
        if (target is null)
        {
            return Results.NotFound(new { error = "No cached target matched the requested device." });
        }

        var resolvedConversationId = !string.IsNullOrWhiteSpace(conversationId)
            ? await cacheStore.ResolveConversationIdAsync(target.Id, conversationId, cancellationToken) ?? conversationId
            : null;
        var messages = await cacheStore.GetStoredMessagesAsync(
            target.Id,
            folder,
            resolvedConversationId,
            limit is > 0 ? limit.Value : 50,
            cancellationToken);
        return Results.Ok(new { target, conversationId = resolvedConversationId, count = messages.Count, messages });
    });

app.MapGet(
    "/v1/conversations",
    async Task<IResult> (
        string? deviceId,
        string? nameContains,
        int? limit,
        DeviceCatalog deviceCatalog,
        RuntimeStateService runtimeStateService,
        SqliteCacheStore cacheStore,
        CancellationToken cancellationToken) =>
    {
        var target = await ResolveClassicTargetAsync(deviceCatalog, runtimeStateService, daemonOptions, deviceId, nameContains);
        if (target is null)
        {
            return Results.NotFound(new { error = "No cached target matched the requested device." });
        }

        var conversations = await cacheStore.GetConversationsAsync(target.Id, limit is > 0 ? limit.Value : 50, cancellationToken);
        return Results.Ok(new { target, count = conversations.Count, conversations });
    });

app.MapGet(
    "/v1/conversations/{conversationId}",
    async Task<IResult> (
        string conversationId,
        string? deviceId,
        string? nameContains,
        int? limit,
        DeviceCatalog deviceCatalog,
        RuntimeStateService runtimeStateService,
        SqliteCacheStore cacheStore,
        CancellationToken cancellationToken) =>
    {
        var target = await ResolveClassicTargetAsync(deviceCatalog, runtimeStateService, daemonOptions, deviceId, nameContains);
        if (target is null)
        {
            return Results.NotFound(new { error = "No cached target matched the requested device." });
        }

        var resolvedConversationId = await cacheStore.ResolveConversationIdAsync(target.Id, conversationId, cancellationToken)
            ?? conversationId;
        var messages = await cacheStore.GetStoredMessagesAsync(
            target.Id,
            null,
            resolvedConversationId,
            limit is > 0 ? limit.Value : 100,
            cancellationToken);
        return Results.Ok(new { target, conversationId = resolvedConversationId, count = messages.Count, messages });
    });

app.MapPost(
    "/v1/messages/resolve",
    async Task<IResult> (
        ResolveMessageRequest request,
        DeviceCatalog deviceCatalog,
        RuntimeStateService runtimeStateService,
        SqliteCacheStore cacheStore,
        CancellationToken cancellationToken) =>
    {
        var (plan, error, statusCode) = await ResolveMessagePlanAsync(
            deviceCatalog,
            runtimeStateService,
            cacheStore,
            daemonOptions,
            request.DeviceId,
            request.NameContains,
            request.Recipient,
            request.ContactId,
            request.ContactName,
            request.PreferredNumber,
            request.ConversationId,
            cancellationToken);
        if (plan is null)
        {
            return Results.Json(new { error }, statusCode: statusCode);
        }

        var body = string.IsNullOrWhiteSpace(request.Body) ? null : request.Body;
        return Results.Ok(
            new
            {
                ready = !string.IsNullOrWhiteSpace(body),
                target = plan.Target,
                recipient = plan.Recipient,
                resolutionSource = plan.ResolutionSource,
                resolvedContact = plan.ResolvedContact?.Contact,
                resolvedConversation = plan.ResolvedConversation?.Conversation,
                resolvedParticipant = plan.ResolvedConversation?.Participant,
                bodyRequired = string.IsNullOrWhiteSpace(body),
                recommendedNextAction = string.IsNullOrWhiteSpace(body)
                    ? "Provide a body, then POST /v1/messages/send with the returned sendRequest."
                    : "POST /v1/messages/send with the returned sendRequest.",
                sendRequest = new
                {
                    recipient = plan.Recipient,
                    body = body ?? string.Empty,
                    deviceId = plan.Target.Id,
                    contactId = plan.ResolvedContact?.Contact.UniqueIdentifier,
                    contactName = plan.ResolvedContact?.Contact.DisplayName,
                    preferredNumber = request.PreferredNumber,
                    conversationId = plan.ResolvedConversation?.Conversation.ConversationId ?? request.ConversationId,
                    autoSyncAfterSend = true,
                    evictPhoneLink = request.EvictPhoneLink
                }
            });
    });

app.MapPost(
    "/v1/messages/send",
    async Task<IResult> (
        SendMessageRequest request,
        DeviceCatalog deviceCatalog,
        RuntimeStateService runtimeStateService,
        SqliteCacheStore cacheStore,
        DeviceFusionCoordinator fusionCoordinator,
        PhoneLinkEvictionPolicy evictionPolicy,
        MapRealtimeSession mapRealtimeSession,
        MapMessagingService messagingService,
        DeviceSyncService syncService,
        DaemonEventHub eventHub,
        CancellationToken cancellationToken) =>
    {
        if (string.IsNullOrWhiteSpace(request.Body))
        {
            return Results.BadRequest(new { error = "body is required." });
        }

        var (plan, error, statusCode) = await ResolveMessagePlanAsync(
            deviceCatalog,
            runtimeStateService,
            cacheStore,
            daemonOptions,
            request.DeviceId,
            request.NameContains,
            request.Recipient,
            request.ContactId,
            request.ContactName,
            request.PreferredNumber,
            request.ConversationId,
            cancellationToken);
        if (plan is null)
        {
            return Results.Json(new { error }, statusCode: statusCode);
        }

        var sendIntentId = await fusionCoordinator.RunAsync(
            plan.Target.Id,
            "send_intent_create",
            ct => cacheStore.CreateSendIntentAsync(
                plan.Target.Id,
                plan.Recipient,
                plan.ResolvedContact,
                request.Body,
                ct),
            cancellationToken);
        var result = await SendViaBestAvailableAsync(
            plan.Target,
            plan.Recipient,
            request.Body,
            request.EvictPhoneLink || await evictionPolicy.ShouldEvictForMapAsync(plan.Target, cancellationToken),
            mapRealtimeSession,
            messagingService,
            cancellationToken);
        await fusionCoordinator.RunAsync(
            plan.Target.Id,
            "send_intent_complete",
            ct => cacheStore.CompleteSendIntentAsync(plan.Target.Id, sendIntentId, result, ct),
            cancellationToken);

        eventHub.Publish(
            "message.sent",
            new
            {
                sendIntentId,
                target = plan.Target,
                recipient = plan.Recipient,
                resolutionSource = plan.ResolutionSource,
                resolvedContact = plan.ResolvedContact?.Contact.DisplayName,
                resolvedConversation = plan.ResolvedConversation?.Conversation.ConversationId,
                result
            });

        if (request.AutoSyncAfterSend)
        {
            syncService.RequestSync("post_send");
        }

        return Results.Ok(
            new
            {
                sendIntentId,
                target = plan.Target,
                recipient = plan.Recipient,
                resolutionSource = plan.ResolutionSource,
                resolvedContact = plan.ResolvedContact?.Contact,
                resolvedConversation = plan.ResolvedConversation?.Conversation,
                result
            });
    });

if (Directory.Exists(uiPath))
{
    app.MapFallback(async context =>
    {
        context.Response.ContentType = "text/html";
        await context.Response.SendFileAsync(Path.Combine(uiPath, "index.html"));
    });
}

app.Run();

static string BuildWebSocketUrl(string listenUrl)
{
    var url = new Uri(listenUrl);
    var builder = new UriBuilder(url)
    {
        Scheme = url.Scheme.Equals("https", StringComparison.OrdinalIgnoreCase) ? "wss" : "ws",
        Path = "/v1/ws",
        Query = string.Empty
    };
    return builder.Uri.ToString();
}

static void ConfigureLoopbackBinding(KestrelServerOptions options, Uri listenUri)
{
    if (listenUri.Port == 0)
    {
        options.Listen(
            ResolveLoopbackAddress(listenUri.Host),
            port: 0,
            listenOptions =>
            {
                if (listenUri.Scheme.Equals(Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
                {
                    listenOptions.UseHttps();
                }
            });
        return;
    }

    options.ListenLocalhost(
        listenUri.Port,
        listenOptions =>
        {
            if (listenUri.Scheme.Equals(Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
            {
                listenOptions.UseHttps();
            }
        });
}

static IPAddress ResolveLoopbackAddress(string host)
{
    return IPAddress.TryParse(host, out var address) && address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6
        ? IPAddress.IPv6Loopback
        : IPAddress.Loopback;
}

static string NormalizeHostedPath(string path)
{
    if (OperatingSystem.IsWindows() && path.StartsWith(@"\\?\", StringComparison.Ordinal))
    {
        return path[4..];
    }

    return path;
}

static async Task<BluetoothEndpointRecord?> ResolveClassicTargetAsync(
    DeviceCatalog deviceCatalog,
    RuntimeStateService runtimeStateService,
    DaemonOptions daemonOptions,
    string? deviceId,
    string? nameContains)
{
    if (!string.IsNullOrWhiteSpace(deviceId) || !string.IsNullOrWhiteSpace(nameContains))
    {
        return await deviceCatalog.SelectClassicTargetAsync(deviceId, nameContains);
    }

    var runtimeTarget = runtimeStateService.GetSnapshot().Target;
    if (runtimeTarget is not null)
    {
        return runtimeTarget;
    }

    return await deviceCatalog.SelectClassicTargetAsync(null, daemonOptions.DefaultNameContains);
}

static async Task<(ResolvedMessagePlan? Plan, string? Error, int StatusCode)> ResolveMessagePlanAsync(
    DeviceCatalog deviceCatalog,
    RuntimeStateService runtimeStateService,
    SqliteCacheStore cacheStore,
    DaemonOptions daemonOptions,
    string? deviceId,
    string? nameContains,
    string? recipient,
    string? contactId,
    string? contactName,
    string? preferredNumber,
    string? conversationId,
    CancellationToken cancellationToken)
{
    var target = await ResolveClassicTargetAsync(deviceCatalog, runtimeStateService, daemonOptions, deviceId, nameContains);
    if (target is null)
    {
        return (null, "No classic Bluetooth target matched the requested device.", StatusCodes.Status404NotFound);
    }

    if (!string.IsNullOrWhiteSpace(recipient))
    {
        return (
            new ResolvedMessagePlan(target, recipient.Trim(), "recipient", null, null),
            null,
            StatusCodes.Status200OK);
    }

    if (!string.IsNullOrWhiteSpace(conversationId))
    {
        var resolvedConversation = await cacheStore.ResolveConversationRecipientAsync(
            target.Id,
            conversationId,
            cancellationToken);
        if (resolvedConversation is null)
        {
            return (
                null,
                "conversationId did not resolve to a cached one-to-one thread with a phone recipient.",
                StatusCodes.Status400BadRequest);
        }

        return (
            new ResolvedMessagePlan(
                target,
                resolvedConversation.Recipient,
                "conversation",
                null,
                resolvedConversation),
            null,
            StatusCodes.Status200OK);
    }

    var resolvedContact = await cacheStore.ResolveRecipientAsync(
        target.Id,
        contactId,
        contactName,
        preferredNumber,
        cancellationToken);
    if (resolvedContact is null)
    {
        return (
            null,
            "recipient, conversationId, or a resolvable contact is required.",
            StatusCodes.Status400BadRequest);
    }

    return (
        new ResolvedMessagePlan(target, resolvedContact.Recipient, "contact", resolvedContact, null),
        null,
        StatusCodes.Status200OK);
}

static async Task<IReadOnlyList<string>> ListFoldersViaBestAvailableAsync(
    BluetoothEndpointRecord target,
    bool evictPhoneLink,
    MapRealtimeSession mapRealtimeSession,
    MapMessagingService messagingService,
    CancellationToken cancellationToken)
{
    if (CanUseLiveMap(target, mapRealtimeSession))
    {
        try
        {
            return await mapRealtimeSession.ListFoldersAsync(cancellationToken);
        }
        catch
        {
        }
    }

    return await messagingService.ListFoldersAsync(target, evictPhoneLink, cancellationToken);
}

static async Task<MessageFolderListing> ListMessagesViaBestAvailableAsync(
    BluetoothEndpointRecord target,
    string folder,
    int limit,
    bool evictPhoneLink,
    MapRealtimeSession mapRealtimeSession,
    MapMessagingService messagingService,
    CancellationToken cancellationToken)
{
    if (CanUseLiveMap(target, mapRealtimeSession))
    {
        try
        {
            return await mapRealtimeSession.ListMessagesAsync(folder, limit, cancellationToken);
        }
        catch
        {
        }
    }

    return await messagingService.ListMessagesAsync(target, folder, limit, evictPhoneLink, cancellationToken);
}

static async Task<SendMessageResult> SendViaBestAvailableAsync(
    BluetoothEndpointRecord target,
    string recipient,
    string body,
    bool evictPhoneLink,
    MapRealtimeSession mapRealtimeSession,
    MapMessagingService messagingService,
    CancellationToken cancellationToken)
{
    if (CanUseLiveMap(target, mapRealtimeSession))
    {
        try
        {
            return await mapRealtimeSession.SendMessageAsync(recipient, body, cancellationToken);
        }
        catch
        {
        }
    }

    return await messagingService.SendMessageAsync(target, recipient, body, evictPhoneLink, cancellationToken);
}

static bool CanUseLiveMap(BluetoothEndpointRecord target, MapRealtimeSession mapRealtimeSession)
{
    return mapRealtimeSession.CurrentTarget is not null
        && mapRealtimeSession.CurrentPhase == DeviceSessionPhase.Connected
        && string.Equals(mapRealtimeSession.CurrentTarget.Id, target.Id, StringComparison.OrdinalIgnoreCase);
}

static async Task<BluetoothPairingCandidateRecord?> ResolvePairingCandidateAsync(
    BluetoothPairingService pairingService,
    string? deviceId,
    string? nameContains,
    bool paired,
    string? transport,
    CancellationToken cancellationToken)
{
    if (string.IsNullOrWhiteSpace(deviceId) && string.IsNullOrWhiteSpace(nameContains))
    {
        return null;
    }

    return await pairingService.ResolveCandidateAsync(
        deviceId,
        nameContains,
        paired,
        transport,
        cancellationToken);
}

static DevicePairingProtectionLevel? ParseProtectionLevel(string? protectionLevel)
{
    return protectionLevel?.Trim().ToLowerInvariant() switch
    {
        null or "" or "default" => DevicePairingProtectionLevel.Default,
        "none" => DevicePairingProtectionLevel.None,
        "encryption" => DevicePairingProtectionLevel.Encryption,
        "encryptionandauthentication" or "encryption_and_authentication" or "auth" =>
            DevicePairingProtectionLevel.EncryptionAndAuthentication,
        _ => null
    };
}

async Task WriteSseAsync(
    HttpContext context,
    DaemonEventRecord item,
    CancellationToken cancellationToken)
{
    var payload = JsonSerializer.Serialize(item, eventJsonOptions);
    await context.Response.WriteAsync($"id: {item.Sequence}\n", cancellationToken);
    await context.Response.WriteAsync($"event: {item.Type}\n", cancellationToken);
    await context.Response.WriteAsync($"data: {payload}\n\n", cancellationToken);
    await context.Response.Body.FlushAsync(cancellationToken);
}

async Task WriteWebSocketAsync(
    WebSocket socket,
    DaemonEventRecord item,
    CancellationToken cancellationToken)
{
    var payload = JsonSerializer.SerializeToUtf8Bytes(item, eventJsonOptions);
    await socket.SendAsync(payload, WebSocketMessageType.Text, true, cancellationToken);
}

public partial class Program
{
}
