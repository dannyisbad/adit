using Adit.Core.Models;
using Adit.Daemon.Services;

namespace Adit.Daemon.Tests;

public sealed class SqliteCacheStoreTests
{
    [Fact]
    public async Task TrustedLeDeviceMapping_RoundTripsByClassicDeviceOnly()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"adit-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            var store = new SqliteCacheStore(
                new DaemonOptions
                {
                    DatabasePath = Path.Combine(tempRoot, "adit.db"),
                    EncryptDatabaseAtRest = false
                });
            await store.InitializeAsync(CancellationToken.None);

            var classicTarget = new BluetoothEndpointRecord(
                "classic",
                "classic-1",
                "Riley's iPhone",
                true,
                "2a:7c:4f:91:5e:b3",
                "2a7c4f915eb3",
                true,
                true,
                "container-1",
                "Communication.Phone");
            var leTarget = new BluetoothLeDeviceRecord(
                "BluetoothLE#BluetoothLE3a:d5:88:41:2c:f0-6e:1b:a4:73:cc:28",
                "Riley's iPhone",
                true,
                "6e:1b:a4:73:cc:28",
                true,
                true,
                "container-1");

            await store.UpsertTrustedLeDeviceAsync(classicTarget, leTarget, CancellationToken.None);

            var byClassicDevice = await store.GetTrustedLeDeviceAsync(
                classicTarget.Id,
                classicTarget.ContainerId,
                CancellationToken.None);
            Assert.NotNull(byClassicDevice);
            Assert.Equal(leTarget.Id, byClassicDevice.Value.DeviceId);
            Assert.Equal(leTarget.Address, byClassicDevice.Value.Address);

            var byDifferentClassicDevice = await store.GetTrustedLeDeviceAsync(
                "classic-2",
                classicTarget.ContainerId,
                CancellationToken.None);
            Assert.Null(byDifferentClassicDevice);
        }
        finally
        {
            try
            {
                Directory.Delete(tempRoot, true);
            }
            catch
            {
            }
        }
    }

    [Fact]
    public async Task NotificationsMode_RoundTrips_WithLegacyFalseMigratingToAuto()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"adit-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            var store = new SqliteCacheStore(
                new DaemonOptions
                {
                    DatabasePath = Path.Combine(tempRoot, "adit.db"),
                    EncryptDatabaseAtRest = false
                });
            await store.InitializeAsync(CancellationToken.None);

            Assert.Equal(NotificationMode.Auto, await store.GetNotificationsModeAsync(CancellationToken.None));

            await store.SetNotificationsModeAsync(NotificationMode.On, CancellationToken.None);
            Assert.Equal(NotificationMode.On, await store.GetNotificationsModeAsync(CancellationToken.None));

            await store.SetNotificationsModeAsync(NotificationMode.Off, CancellationToken.None);
            Assert.Equal(NotificationMode.Off, await store.GetNotificationsModeAsync(CancellationToken.None));

            await using var connection = new Microsoft.Data.Sqlite.SqliteConnection(
                new Microsoft.Data.Sqlite.SqliteConnectionStringBuilder
                {
                    DataSource = Path.Combine(tempRoot, "adit.db"),
                    Mode = Microsoft.Data.Sqlite.SqliteOpenMode.ReadWriteCreate,
                    Cache = Microsoft.Data.Sqlite.SqliteCacheMode.Shared
                }.ToString());
            await connection.OpenAsync(CancellationToken.None);
            await using var command = connection.CreateCommand();
            command.CommandText =
                """
                DELETE FROM daemon_settings WHERE setting_key = 'notifications_mode';
                INSERT INTO daemon_settings (setting_key, setting_value, updated_utc)
                VALUES ('notifications_enabled', 'False', $updatedUtc)
                ON CONFLICT(setting_key) DO UPDATE SET
                    setting_value = excluded.setting_value,
                    updated_utc = excluded.updated_utc;
                """;
            command.Parameters.AddWithValue("$updatedUtc", DateTimeOffset.UtcNow.ToString("O"));
            await command.ExecuteNonQueryAsync(CancellationToken.None);

            Assert.Equal(NotificationMode.Auto, await store.GetNotificationsModeAsync(CancellationToken.None));
        }
        finally
        {
            try
            {
                Directory.Delete(tempRoot, true);
            }
            catch
            {
            }
        }
    }

    [Fact]
    public async Task InitializeAsync_DoesNotThrow_WhenWindowsFileEncryptionIsUnavailable()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"adit-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            var databasePath = Path.Combine(tempRoot, "adit.db");
            await File.WriteAllTextAsync(databasePath, string.Empty, CancellationToken.None);

            var store = new SqliteCacheStore(
                new DaemonOptions
                {
                    DatabasePath = databasePath,
                    EncryptDatabaseAtRest = true
                },
                logger: null,
                getFileAttributes: _ => FileAttributes.Normal,
                encryptFile: _ => throw new NotSupportedException("EFS unavailable"));

            await store.InitializeAsync(CancellationToken.None);

            Assert.True(File.Exists(databasePath));
        }
        finally
        {
            try
            {
                Directory.Delete(tempRoot, true);
            }
            catch
            {
            }
        }
    }

    [Fact]
    public async Task ReplaceMessageSnapshotAsync_DoesNotReuseThreadId_ForSingleMessageContinuityShift()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"adit-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            var store = new SqliteCacheStore(
                new DaemonOptions
                {
                    DatabasePath = Path.Combine(tempRoot, "adit.db"),
                    EncryptDatabaseAtRest = false
                });
            await store.InitializeAsync(CancellationToken.None);

            const string deviceId = "device-1";
            var message = new MessageRecord(
                "inbox",
                "handle-1",
                "SMS_GSM",
                null,
                "20260307T091500",
                "Alice",
                "+12025550114",
                "+12025550100",
                0,
                0,
                null,
                false,
                false,
                false,
                "group ping",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("Alice", ["+12025550114"], [])],
                [new MessageParticipantRecord("My Number", ["+12025550100"], [])]);

            var firstConversation = new ConversationSnapshot(
                "conv_phone_alice",
                "Alice",
                false,
                DateTimeOffset.Parse("2026-03-07T17:15:00Z"),
                1,
                1,
                "group ping",
                [new ConversationParticipantRecord("phone:+12025550114", "Alice", ["+12025550114"], [], false)],
                ["inbox"]);
            var firstMessage = new SynthesizedMessageRecord(
                "handle-1",
                firstConversation.ConversationId,
                firstConversation.DisplayName,
                firstConversation.IsGroup,
                firstConversation.LastMessageUtc,
                firstConversation.Participants,
                message);

            await store.ReplaceMessageSnapshotAsync(
                deviceId,
                new ConversationSynthesisResult([], [firstMessage], [firstConversation]),
                CancellationToken.None);

            var initialStoredConversation = Assert.Single(
                await store.GetConversationsAsync(deviceId, null, CancellationToken.None));
            var initialThreadId = initialStoredConversation.ConversationId;

            var secondConversation = firstConversation with
            {
                ConversationId = "conv_group_weekend_crew",
                DisplayName = "Weekend Crew",
                IsGroup = true
            };
            var secondMessage = firstMessage with
            {
                ConversationId = secondConversation.ConversationId,
                ConversationDisplayName = secondConversation.DisplayName,
                IsGroup = secondConversation.IsGroup
            };

            await store.ReplaceMessageSnapshotAsync(
                deviceId,
                new ConversationSynthesisResult([], [secondMessage], [secondConversation]),
                CancellationToken.None);

            var finalStoredConversation = Assert.Single(
                await store.GetConversationsAsync(deviceId, null, CancellationToken.None));
            Assert.NotEqual(initialThreadId, finalStoredConversation.ConversationId);
            Assert.Equal("Weekend Crew", finalStoredConversation.DisplayName);
            Assert.True(finalStoredConversation.IsGroup);
        }
        finally
        {
            try
            {
                Directory.Delete(tempRoot, true);
            }
            catch
            {
            }
        }
    }

    [Fact]
    public async Task ReplaceMessageSnapshotAsync_DoesNotReuseThreadId_WhenContinuityIsSplitAcrossThreads()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"adit-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            var store = new SqliteCacheStore(
                new DaemonOptions
                {
                    DatabasePath = Path.Combine(tempRoot, "adit.db"),
                    EncryptDatabaseAtRest = false
                });
            await store.InitializeAsync(CancellationToken.None);

            const string deviceId = "device-1";
            var aliceMessage = new MessageRecord(
                "inbox",
                "handle-a",
                "SMS_GSM",
                null,
                "20260307T091500",
                "Alice",
                "+12025550114",
                "+12025550100",
                0,
                0,
                null,
                false,
                false,
                false,
                "group ping",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("Alice", ["+12025550114"], [])],
                [new MessageParticipantRecord("My Number", ["+12025550100"], [])]);
            var bobMessage = new MessageRecord(
                "inbox",
                "handle-b",
                "SMS_GSM",
                null,
                "20260307T091501",
                "Bob",
                "+12025550115",
                "+12025550100",
                0,
                0,
                null,
                false,
                false,
                false,
                "group ping",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("Bob", ["+12025550115"], [])],
                [new MessageParticipantRecord("My Number", ["+12025550100"], [])]);

            var aliceConversation = new ConversationSnapshot(
                "conv_phone_alice",
                "Alice",
                false,
                DateTimeOffset.Parse("2026-03-07T17:15:00Z"),
                1,
                1,
                "group ping",
                [new ConversationParticipantRecord("phone:+12025550114", "Alice", ["+12025550114"], [], false)],
                ["inbox"]);
            var bobConversation = new ConversationSnapshot(
                "conv_phone_bob",
                "Bob",
                false,
                DateTimeOffset.Parse("2026-03-07T17:15:01Z"),
                1,
                1,
                "group ping",
                [new ConversationParticipantRecord("phone:+12025550115", "Bob", ["+12025550115"], [], false)],
                ["inbox"]);

            await store.ReplaceMessageSnapshotAsync(
                deviceId,
                new ConversationSynthesisResult(
                    [],
                    [
                        new SynthesizedMessageRecord(
                            "handle-a",
                            aliceConversation.ConversationId,
                            aliceConversation.DisplayName,
                            aliceConversation.IsGroup,
                            aliceConversation.LastMessageUtc,
                            aliceConversation.Participants,
                            aliceMessage),
                        new SynthesizedMessageRecord(
                            "handle-b",
                            bobConversation.ConversationId,
                            bobConversation.DisplayName,
                            bobConversation.IsGroup,
                            bobConversation.LastMessageUtc,
                            bobConversation.Participants,
                            bobMessage)
                    ],
                    [aliceConversation, bobConversation]),
                CancellationToken.None);

            var initialConversations = await store.GetConversationsAsync(deviceId, null, CancellationToken.None);
            Assert.Equal(2, initialConversations.Count);
            var priorThreadIds = initialConversations.Select(conversation => conversation.ConversationId).ToHashSet(StringComparer.OrdinalIgnoreCase);

            var mergedConversation = new ConversationSnapshot(
                "conv_group_weekend_crew",
                "Weekend Crew",
                true,
                DateTimeOffset.Parse("2026-03-07T17:15:01Z"),
                2,
                2,
                "group ping",
                [
                    new ConversationParticipantRecord("phone:+12025550114", "Alice", ["+12025550114"], [], false),
                    new ConversationParticipantRecord("phone:+12025550115", "Bob", ["+12025550115"], [], false)
                ],
                ["inbox"]);

            await store.ReplaceMessageSnapshotAsync(
                deviceId,
                new ConversationSynthesisResult(
                    [],
                    [
                        new SynthesizedMessageRecord(
                            "handle-a",
                            mergedConversation.ConversationId,
                            mergedConversation.DisplayName,
                            mergedConversation.IsGroup,
                            DateTimeOffset.Parse("2026-03-07T17:15:00Z"),
                            mergedConversation.Participants,
                            aliceMessage),
                        new SynthesizedMessageRecord(
                            "handle-b",
                            mergedConversation.ConversationId,
                            mergedConversation.DisplayName,
                            mergedConversation.IsGroup,
                            DateTimeOffset.Parse("2026-03-07T17:15:01Z"),
                            mergedConversation.Participants,
                            bobMessage)
                    ],
                    [mergedConversation]),
                CancellationToken.None);

            var finalConversation = Assert.Single(
                await store.GetConversationsAsync(deviceId, null, CancellationToken.None));
            Assert.DoesNotContain(finalConversation.ConversationId, priorThreadIds);
            Assert.Equal("Weekend Crew", finalConversation.DisplayName);
            Assert.True(finalConversation.IsGroup);
        }
        finally
        {
            try
            {
                Directory.Delete(tempRoot, true);
            }
            catch
            {
            }
        }
    }

    [Fact]
    public async Task ReplaceMessageSnapshotAsync_DoesNotCollapseDirectSplitBackIntoExistingGroupThread()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"adit-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            var store = new SqliteCacheStore(
                new DaemonOptions
                {
                    DatabasePath = Path.Combine(tempRoot, "adit.db"),
                    EncryptDatabaseAtRest = false
                });
            await store.InitializeAsync(CancellationToken.None);

            const string deviceId = "device-1";
            var groupParticipants = new[]
            {
                new ConversationParticipantRecord("phone:+12025550101", "mom", ["+12025550101"], [], false),
                new ConversationParticipantRecord("phone:+12025550104", "Aunt Beth", ["+12025550104"], [], false)
            };
            var directMomParticipants = new[]
            {
                new ConversationParticipantRecord("phone:+12025550101", "mom", ["+12025550101"], [], false)
            };

            var groupOrigin = new MessageRecord(
                "inbox",
                "handle-group-origin",
                "SMS_GSM",
                null,
                "20260308T191230",
                "Aunt Beth",
                "+12025550104",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "I tried making coffee with sparkling water earlier.",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("Aunt Beth", ["+12025550104"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]);
            var groupReaction = new MessageRecord(
                "inbox",
                "handle-group-reaction",
                "SMS_GSM",
                null,
                "20260308T191306",
                "Maren",
                "+12025550107",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "Laughed at “I tried making coffee with sparkling water earlier.”",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("Maren", ["+12025550107"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]);
            var momFu = new MessageRecord(
                "inbox",
                "handle-mom-fu",
                "SMS_GSM",
                null,
                "20260308T193640",
                "mom",
                "+12025550101",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "On it",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("mom", ["+12025550101"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]);
            var momFolder = new MessageRecord(
                "inbox",
                "handle-mom-meds",
                "SMS_GSM",
                null,
                "20260308T193648",
                "mom",
                "+12025550101",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "Folder",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("mom", ["+12025550101"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]);

            var familyConversation = new ConversationSnapshot(
                "conv_family_group_v1",
                "To you, mom, Aunt Beth",
                true,
                DateTimeOffset.Parse("2026-03-08T23:36:48Z"),
                4,
                4,
                "Folder",
                groupParticipants,
                ["inbox"]);

            await store.ReplaceMessageSnapshotAsync(
                deviceId,
                new ConversationSynthesisResult(
                    [],
                    [
                        new SynthesizedMessageRecord(
                            "handle-group-origin",
                            familyConversation.ConversationId,
                            familyConversation.DisplayName,
                            familyConversation.IsGroup,
                            DateTimeOffset.Parse("2026-03-08T23:12:30Z"),
                            groupParticipants,
                            groupOrigin),
                        new SynthesizedMessageRecord(
                            "handle-group-reaction",
                            familyConversation.ConversationId,
                            familyConversation.DisplayName,
                            familyConversation.IsGroup,
                            DateTimeOffset.Parse("2026-03-08T23:13:06Z"),
                            groupParticipants,
                            groupReaction),
                        new SynthesizedMessageRecord(
                            "handle-mom-fu",
                            familyConversation.ConversationId,
                            familyConversation.DisplayName,
                            familyConversation.IsGroup,
                            DateTimeOffset.Parse("2026-03-08T23:36:40Z"),
                            groupParticipants,
                            momFu),
                        new SynthesizedMessageRecord(
                            "handle-mom-meds",
                            familyConversation.ConversationId,
                            familyConversation.DisplayName,
                            familyConversation.IsGroup,
                            DateTimeOffset.Parse("2026-03-08T23:36:48Z"),
                            groupParticipants,
                            momFolder)
                    ],
                    [familyConversation]),
                CancellationToken.None);

            var priorConversation = Assert.Single(await store.GetConversationsAsync(deviceId, null, CancellationToken.None));
            var priorFamilyThreadId = priorConversation.ConversationId;

            var correctedFamilyConversation = familyConversation with
            {
                ConversationId = "conv_family_group_v2",
                MessageCount = 2,
                LastPreview = "I tried making coffee with sparkling water earlier."
            };
            var correctedDirectConversation = new ConversationSnapshot(
                "conv_direct_mom_v1",
                "mom",
                false,
                DateTimeOffset.Parse("2026-03-08T23:36:48Z"),
                2,
                2,
                "Folder",
                directMomParticipants,
                ["inbox"]);

            await store.ReplaceMessageSnapshotAsync(
                deviceId,
                new ConversationSynthesisResult(
                    [],
                    [
                        new SynthesizedMessageRecord(
                            "handle-group-origin",
                            correctedFamilyConversation.ConversationId,
                            correctedFamilyConversation.DisplayName,
                            correctedFamilyConversation.IsGroup,
                            DateTimeOffset.Parse("2026-03-08T23:12:30Z"),
                            correctedFamilyConversation.Participants,
                            groupOrigin),
                        new SynthesizedMessageRecord(
                            "handle-group-reaction",
                            correctedFamilyConversation.ConversationId,
                            correctedFamilyConversation.DisplayName,
                            correctedFamilyConversation.IsGroup,
                            DateTimeOffset.Parse("2026-03-08T23:13:06Z"),
                            correctedFamilyConversation.Participants,
                            groupReaction),
                        new SynthesizedMessageRecord(
                            "handle-mom-fu",
                            correctedDirectConversation.ConversationId,
                            correctedDirectConversation.DisplayName,
                            correctedDirectConversation.IsGroup,
                            DateTimeOffset.Parse("2026-03-08T23:36:40Z"),
                            correctedDirectConversation.Participants,
                            momFu),
                        new SynthesizedMessageRecord(
                            "handle-mom-meds",
                            correctedDirectConversation.ConversationId,
                            correctedDirectConversation.DisplayName,
                            correctedDirectConversation.IsGroup,
                            DateTimeOffset.Parse("2026-03-08T23:36:48Z"),
                            correctedDirectConversation.Participants,
                            momFolder)
                    ],
                    [correctedFamilyConversation, correctedDirectConversation]),
                CancellationToken.None);

            var finalConversations = await store.GetConversationsAsync(deviceId, null, CancellationToken.None);
            Assert.Equal(2, finalConversations.Count);

            var finalGroup = finalConversations.Single(conversation => conversation.IsGroup);
            var finalDirectMom = finalConversations.Single(conversation => !conversation.IsGroup && conversation.DisplayName == "mom");

            Assert.Equal(priorFamilyThreadId, finalGroup.ConversationId);
            Assert.NotEqual(priorFamilyThreadId, finalDirectMom.ConversationId);

            var storedMessages = await store.GetStoredMessagesAsync(deviceId, null, null, 50, CancellationToken.None);
            Assert.Contains(
                storedMessages.Where(message => message.ConversationId == finalDirectMom.ConversationId),
                message => string.Equals(message.Message.Handle, "handle-mom-fu", StringComparison.OrdinalIgnoreCase));
            Assert.Contains(
                storedMessages.Where(message => message.ConversationId == finalDirectMom.ConversationId),
                message => string.Equals(message.Message.Handle, "handle-mom-meds", StringComparison.OrdinalIgnoreCase));
        }
        finally
        {
            try
            {
                Directory.Delete(tempRoot, true);
            }
            catch
            {
            }
        }
    }

    [Fact]
    public async Task ReplaceMessageSnapshotAsync_DeduplicatesProjectedRowsByStableIds()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"adit-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            var store = new SqliteCacheStore(
                new DaemonOptions
                {
                    DatabasePath = Path.Combine(tempRoot, "adit.db"),
                    EncryptDatabaseAtRest = false
                });
            await store.InitializeAsync(CancellationToken.None);

            const string deviceId = "device-1";
            var message = new MessageRecord(
                "inbox",
                "handle-1",
                "SMS_GSM",
                null,
                "20260307T091500",
                "Alice",
                "+12025550114",
                "+12025550100",
                0,
                0,
                null,
                false,
                false,
                false,
                "same message",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("Alice", ["+12025550114"], [])],
                [new MessageParticipantRecord("My Number", ["+12025550100"], [])]);

            var baseConversation = new ConversationSnapshot(
                "conv_duplicate",
                "Alice",
                false,
                DateTimeOffset.Parse("2026-03-07T17:15:00Z"),
                1,
                1,
                "same message",
                [new ConversationParticipantRecord("phone:+12025550114", "Alice", ["+12025550114"], [], false)],
                ["inbox"]);

            await store.ReplaceMessageSnapshotAsync(
                deviceId,
                new ConversationSynthesisResult(
                    [],
                    [
                        new SynthesizedMessageRecord(
                            "msg_duplicate",
                            baseConversation.ConversationId,
                            baseConversation.DisplayName,
                            baseConversation.IsGroup,
                            baseConversation.LastMessageUtc,
                            baseConversation.Participants,
                            message),
                        new SynthesizedMessageRecord(
                            "msg_duplicate",
                            baseConversation.ConversationId,
                            "Alice A.",
                            baseConversation.IsGroup,
                            baseConversation.LastMessageUtc,
                            baseConversation.Participants,
                            message)
                    ],
                    [
                        baseConversation,
                        baseConversation with
                        {
                            DisplayName = "Alice A.",
                            LastPreview = "same message"
                        }
                    ]),
                CancellationToken.None);

            var conversations = await store.GetConversationsAsync(deviceId, null, CancellationToken.None);
            var messages = await store.GetStoredMessagesAsync(deviceId, null, null, 50, CancellationToken.None);

            var conversation = Assert.Single(conversations);
            var storedMessage = Assert.Single(messages);
            Assert.Equal(conversation.ConversationId, storedMessage.ConversationId);
            Assert.Equal("same message", conversation.LastPreview);
        }
        finally
        {
            try
            {
                Directory.Delete(tempRoot, true);
            }
            catch
            {
            }
        }
    }

    [Fact]
    public async Task GetCompletedSendIntentMessagesAsync_ReturnsSyntheticSentMessages()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"adit-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            var store = new SqliteCacheStore(
                new DaemonOptions
                {
                    DatabasePath = Path.Combine(tempRoot, "adit.db"),
                    EncryptDatabaseAtRest = false
                });
            await store.InitializeAsync(CancellationToken.None);

            const string deviceId = "device-1";
            var resolvedRecipient = new ResolvedRecipientRecord(
                new ContactRecord(
                    "1",
                    "Alice",
                    [new ContactPhoneRecord("(202) 555-0114", "+12025550114", "Cell")],
                    []),
                "+12025550114");

            var sendIntentId = await store.CreateSendIntentAsync(
                deviceId,
                "+12025550114",
                resolvedRecipient,
                "hello there",
                CancellationToken.None);
            await store.CompleteSendIntentAsync(
                deviceId,
                sendIntentId,
                new SendMessageResult(true, "Success", "handle-1"),
                CancellationToken.None);

            var messages = await store.GetCompletedSendIntentMessagesAsync(deviceId, 10, CancellationToken.None);

            var message = Assert.Single(messages);
            Assert.Equal("sent", message.Folder);
            Assert.True(message.Sent);
            Assert.Equal("handle-1", message.Handle);
            Assert.Equal("+12025550114", message.RecipientAddressing);
            Assert.Equal("hello there", message.Body);
            var recipient = Assert.Single(message.Recipients);
            Assert.Equal("Alice", recipient.Name);
            Assert.Contains("+12025550114", recipient.Phones);
        }
        finally
        {
            try
            {
                Directory.Delete(tempRoot, true);
            }
            catch
            {
            }
        }
    }

    [Fact]
    public async Task ReplaceMessageSnapshotAsync_PrefersGroupProjectionAndDropsSyntheticDescriptorParticipant()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), $"adit-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempRoot);

        try
        {
            var store = new SqliteCacheStore(
                new DaemonOptions
                {
                    DatabasePath = Path.Combine(tempRoot, "adit.db"),
                    EncryptDatabaseAtRest = false
                });
            await store.InitializeAsync(CancellationToken.None);

            const string deviceId = "device-1";
            var groupParticipants = new[]
            {
                new ConversationParticipantRecord("phone:+12025550107", "Maren", ["+12025550107"], [], false),
                new ConversationParticipantRecord("phone:+12025550101", "mom", ["+12025550101"], [], false),
                new ConversationParticipantRecord("phone:+12025550102", "dad", ["+12025550102"], [], false)
            };
            var descriptorParticipant = new ConversationParticipantRecord(
                "raw:to you, mom, dad",
                "To you, mom, dad",
                ["To you, mom, dad"],
                [],
                false);
            var latestNotification = new MessageRecord(
                "notification",
                null,
                "ANCS",
                "Loved an image",
                "20260309T020000",
                "To you, mom, dad",
                "name:To you, mom, dad",
                "+12025550100",
                0,
                0,
                null,
                false,
                false,
                false,
                "Loved an image",
                "ANCS",
                "Unread",
                [new MessageParticipantRecord("To you, mom, dad", [], [])],
                [new MessageParticipantRecord("My Number", ["+12025550100"], [])]);
            var olderGroupMessage = new MessageRecord(
                "inbox",
                "handle-group-1",
                "SMS_GSM",
                null,
                "20260309T015500",
                "Maren",
                "+12025550107",
                "+12025550100",
                0,
                0,
                null,
                false,
                false,
                false,
                "Loved an image",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("Maren", ["+12025550107"], [])],
                [new MessageParticipantRecord("My Number", ["+12025550100"], [])]);

            await store.ReplaceMessageSnapshotAsync(
                deviceId,
                new ConversationSynthesisResult(
                    [],
                    [
                        new SynthesizedMessageRecord(
                            "msg-notify",
                            "conv_group_shadow",
                            "To you, mom, dad",
                            false,
                            DateTimeOffset.Parse("2026-03-09T02:00:00Z"),
                            [descriptorParticipant],
                            latestNotification),
                        new SynthesizedMessageRecord(
                            "msg-group",
                            "conv_group_shadow",
                            "To you, mom, dad",
                            true,
                            DateTimeOffset.Parse("2026-03-09T01:55:00Z"),
                            groupParticipants,
                            olderGroupMessage)
                    ],
                    [
                        new ConversationSnapshot(
                            "conv_group_shadow",
                            "To you, mom, dad",
                            false,
                            DateTimeOffset.Parse("2026-03-09T02:00:00Z"),
                            1,
                            1,
                            "Loved an image",
                            [descriptorParticipant],
                            ["notification"]),
                        new ConversationSnapshot(
                            "conv_group_shadow",
                            "To you, mom, dad",
                            true,
                            DateTimeOffset.Parse("2026-03-09T01:55:00Z"),
                            1,
                            1,
                            "Loved an image",
                            groupParticipants,
                            ["inbox"])
                    ]),
                CancellationToken.None);

            var conversation = Assert.Single(await store.GetConversationsAsync(deviceId, null, CancellationToken.None));
            Assert.True(conversation.IsGroup);
            Assert.DoesNotContain(conversation.Participants, participant => participant.Key.StartsWith("raw:to you", StringComparison.OrdinalIgnoreCase));
            Assert.Contains(conversation.Participants, participant => participant.DisplayName == "Maren");
            Assert.Contains(conversation.Participants, participant => participant.DisplayName == "mom");
            Assert.Contains(conversation.Participants, participant => participant.DisplayName == "dad");
        }
        finally
        {
            try
            {
                Directory.Delete(tempRoot, true);
            }
            catch
            {
            }
        }
    }
}
