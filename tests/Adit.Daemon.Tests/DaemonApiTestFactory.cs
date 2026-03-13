using Adit.Core.Models;
using Adit.Daemon.Services;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Adit.Daemon.Tests;

public sealed class DaemonApiTestFactory : WebApplicationFactory<Program>
{
    private const string DeviceId = "classic-1";
    private readonly string? originalDisableBackgroundSync = Environment.GetEnvironmentVariable("ADIT_DISABLE_BACKGROUND_SYNC");
    private string? tempRoot;

    public DaemonApiTestFactory()
    {
        Environment.SetEnvironmentVariable("ADIT_DISABLE_BACKGROUND_SYNC", "true");
    }

    protected override void ConfigureWebHost(Microsoft.AspNetCore.Hosting.IWebHostBuilder builder)
    {
        builder.ConfigureServices(
            services =>
            {
                tempRoot ??= Path.Combine(Path.GetTempPath(), $"adit-api-tests-{Guid.NewGuid():N}");
                Directory.CreateDirectory(tempRoot);

                services.RemoveAll<DaemonOptions>();
                services.RemoveAll<SqliteCacheStore>();
                services.RemoveAll<DaemonEventHub>();
                services.RemoveAll<RuntimeStateService>();

                var options = new DaemonOptions
                {
                    DatabasePath = Path.Combine(tempRoot, "adit.db"),
                    DisableBackgroundSync = true,
                    EncryptDatabaseAtRest = false,
                    EnableLearnedThreadChooser = false
                };
                var store = new SqliteCacheStore(options);
                store.InitializeAsync(CancellationToken.None).GetAwaiter().GetResult();
                SeedStore(store);

                var runtime = new RuntimeStateService();
                runtime.MarkReady(
                    "test",
                    autoEvictPhoneLink: true,
                    target: new BluetoothEndpointRecord(
                        "classic",
                        "classic-1",
                        "Test iPhone",
                        true,
                        "2a:7c:4f:91:5e:b3",
                        "2a7c4f915eb3",
                        true,
                        true,
                        "container-1",
                        "Communication.Phone"),
                    contactCount: 1,
                    messageCount: 1,
                    conversationCount: 1,
                    lastContactsRefreshUtc: DateTimeOffset.Parse("2026-03-10T18:25:11Z"));
                runtime.SetNotificationsMode(NotificationMode.Auto, true);
                runtime.UpdateNotificationCount(1);
                runtime.UpdateTransportState(
                    new SessionStateChangedRecord(
                        "map",
                        DeviceSessionPhase.Connected,
                        DateTimeOffset.Parse("2026-03-10T18:44:54Z"),
                        "already_open",
                        null));
                runtime.UpdateTransportState(
                    new SessionStateChangedRecord(
                        "ancs",
                        DeviceSessionPhase.Connected,
                        DateTimeOffset.Parse("2026-03-10T18:42:57Z"),
                        "subscriptions_ready",
                        null));

                var eventHub = new DaemonEventHub(options);
                eventHub.Publish("test.seeded", new { source = "factory" });

                services.AddSingleton(options);
                services.AddSingleton(store);
                services.AddSingleton(runtime);
                services.AddSingleton(eventHub);
            });
    }

    protected override void Dispose(bool disposing)
    {
        Environment.SetEnvironmentVariable("ADIT_DISABLE_BACKGROUND_SYNC", originalDisableBackgroundSync);

        if (tempRoot is not null)
        {
            try
            {
                Directory.Delete(tempRoot, true);
            }
            catch
            {
            }
        }

        base.Dispose(disposing);
    }

    private static void SeedStore(SqliteCacheStore store)
    {
        ContactRecord[] contacts =
        [
            new(
                "contact-1",
                "Mom",
                [new ContactPhoneRecord("(202) 555-0114", "+12025550114", "Cell")],
                [])
        ];
        store.ReplaceContactsAsync(DeviceId, contacts, CancellationToken.None).GetAwaiter().GetResult();

        ConversationParticipantRecord[] participants =
        [
            new("phone:+12025550114", "Mom", ["+12025550114"], [], false)
        ];
        var conversation = new ConversationSnapshot(
            "th_seeded_mom",
            "Mom",
            false,
            DateTimeOffset.Parse("2026-03-10T18:25:11Z"),
            1,
            1,
            "Need milk",
            participants,
            ["inbox"]);
        var message = new MessageRecord(
            "inbox",
            "handle-seeded-1",
            "SMS_GSM",
            null,
            "20260310T182511",
            "Mom",
            "+12025550114",
            "+12025550100",
            0,
            0,
            null,
            false,
            false,
            false,
            "Need milk",
            "SMSGSM",
            "Unread",
            [new MessageParticipantRecord("Mom", ["+12025550114"], [])],
            [new MessageParticipantRecord("My Number", ["+12025550100"], [])]);
        var synthesized = new SynthesizedMessageRecord(
            "msg-seeded-1",
            conversation.ConversationId,
            conversation.DisplayName,
            conversation.IsGroup,
            conversation.LastMessageUtc,
            conversation.Participants,
            message);
        store.ReplaceMessageSnapshotAsync(
                DeviceId,
                new ConversationSynthesisResult(["+12025550100"], [synthesized], [conversation]),
                CancellationToken.None)
            .GetAwaiter()
            .GetResult();

        var notification = new NotificationRecord(
            41,
            NotificationEventKind.Added,
            NotificationEventFlags.PositiveAction,
            NotificationCategory.Social,
            1,
            DateTimeOffset.Parse("2026-03-10T18:25:13Z"),
            "com.apple.MobileSMS",
            "Mom",
            null,
            "Need milk",
            null,
            null,
            "Reply",
            null,
            new Dictionary<string, string>());
        store.UpsertNotificationAsync(DeviceId, notification, CancellationToken.None)
            .GetAwaiter()
            .GetResult();
    }
}
