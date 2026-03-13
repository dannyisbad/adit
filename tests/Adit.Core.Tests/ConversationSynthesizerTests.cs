using Adit.Core.Models;
using Adit.Core.Services;

namespace Adit.Core.Tests;

public sealed class ConversationSynthesizerTests
{
    private readonly ConversationSynthesizer synthesizer = new();

    [Fact]
    public void Synthesize_CreatesOneToOneConversation_AndResolvesContactName()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new ContactRecord(
                "1",
                "Alice",
                [new ContactPhoneRecord("(202) 555-0114", "+12025550114", "Cell")],
                [])
        ];

        MessageRecord[] messages =
        [
            new MessageRecord(
                "inbox",
                "msg-1",
                "SMS_GSM",
                null,
                "20260307T085657",
                "Alice",
                "+12025550114",
                "+12025550100",
                0,
                0,
                null,
                false,
                false,
                false,
                "hey there",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("Alice", ["+12025550114"], [])],
                [new MessageParticipantRecord("My Number", ["+12025550100"], [])]),
            new MessageRecord(
                "sent",
                "msg-2",
                "SMS_GSM",
                null,
                "20260307T090000",
                "My Number",
                "+12025550100",
                "+12025550114",
                0,
                0,
                null,
                true,
                true,
                false,
                "reply",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("My Number", ["+12025550100"], [])],
                [new MessageParticipantRecord("Alice", ["+12025550114"], [])])
        ];

        var result = synthesizer.Synthesize(messages, contacts);

        var conversation = Assert.Single(result.Conversations);
        Assert.False(conversation.IsGroup);
        Assert.Equal("Alice", conversation.DisplayName);
        Assert.Equal(2, conversation.MessageCount);
        Assert.Equal(1, conversation.UnreadCount);
        Assert.Equal("reply", conversation.LastPreview);
        Assert.Contains("+12025550100", result.SelfPhones);
    }

    [Fact]
    public void Synthesize_InfersOutboundDirection_FromSelfSenderEvenWithoutSentFolder()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new ContactRecord(
                "1",
                "Alice",
                [new ContactPhoneRecord("(202) 555-0114", "+12025550114", "Cell")],
                [])
        ];

        var message = new MessageRecord(
            "notification",
            "msg-outbound-weird",
            "SMS_GSM",
            null,
            "20260307T090000",
            "My Number",
            "+12025550100",
            "+12025550114",
            0,
            0,
            null,
            true,
            null,
            false,
            "reply",
            "SMSGSM",
            "Read",
            [new MessageParticipantRecord("My Number", ["+12025550100"], [])],
            [new MessageParticipantRecord("Alice", ["+12025550114"], [])]);

        var result = synthesizer.Synthesize([message], contacts);

        var conversation = Assert.Single(result.Conversations);
        Assert.False(conversation.IsGroup);
        Assert.Equal("Alice", conversation.DisplayName);
        var participant = Assert.Single(conversation.Participants);
        Assert.False(participant.IsSelf);
        Assert.Equal("Alice", participant.DisplayName);
    }

    [Fact]
    public void Synthesize_CreatesGroupConversation_WhenMultipleExternalParticipantsExist()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new ContactRecord(
                "1",
                "Alice",
                [new ContactPhoneRecord("+12025550114", "+12025550114", "Cell")],
                []),
            new ContactRecord(
                "2",
                "Bob",
                [new ContactPhoneRecord("+12025550115", "+12025550115", "Cell")],
                [])
        ];

        var message = new MessageRecord(
            "inbox",
            "group-1",
            "SMS_GSM",
            null,
            "20260307T091500",
            "Alice",
            "+12025550114",
            null,
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
            [
                new MessageParticipantRecord("My Number", ["+12025550100"], []),
                new MessageParticipantRecord("Bob", ["+12025550115"], [])
            ]);

        var result = synthesizer.Synthesize(new[] { message }, contacts);

        var conversation = Assert.Single(result.Conversations);
        Assert.True(conversation.IsGroup);
        Assert.Equal(2, conversation.Participants.Count);
        Assert.Contains(conversation.Participants, participant => participant.DisplayName == "Alice");
        Assert.Contains(conversation.Participants, participant => participant.DisplayName == "Bob");
        Assert.Contains("Alice", conversation.DisplayName);
        Assert.Contains("Bob", conversation.DisplayName);
    }

    [Fact]
    public void Synthesize_TreatsOutboundEmailSenderAsSelf_AndKeepsOneToOneThread()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new ContactRecord(
                "1",
                "Alice",
                [new ContactPhoneRecord("+12025550114", "+12025550114", "Cell")],
                [])
        ];

        var message = new MessageRecord(
            "sent",
            "msg-email-self",
            "SMS_GSM",
            null,
            "20260307T100000",
            "Riley",
            "e:riley.mercer@example.com",
            "+12025550114",
            0,
            0,
            null,
            true,
            true,
            false,
            "from weird sender alias",
            "SMSGSM",
            "Read",
            [new MessageParticipantRecord("Riley", [], ["riley.mercer@example.com"])],
            [new MessageParticipantRecord("Alice", ["+12025550114"], [])]);

        var result = synthesizer.Synthesize([message], contacts);

        var conversation = Assert.Single(result.Conversations);
        Assert.False(conversation.IsGroup);
        Assert.Equal("Alice", conversation.DisplayName);
        var participant = Assert.Single(conversation.Participants);
        Assert.Equal("Alice", participant.DisplayName);
        Assert.False(participant.IsSelf);
    }

    [Fact]
    public void Synthesize_CollapsesSelfOnlyConversation_InsteadOfMakingFakeGroup()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                [])
        ];

        MessageRecord[] messages =
        [
            new MessageRecord(
                "sent",
                "self-sent",
                "SMS_GSM",
                null,
                "20260307T101000",
                "Riley",
                "e:riley.mercer@example.com",
                "+12025550100",
                0,
                0,
                null,
                true,
                true,
                false,
                "testing self",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("Riley", [], ["riley.mercer@example.com"])],
                [new MessageParticipantRecord("My Number", ["+12025550100"], [])]),
            new MessageRecord(
                "inbox",
                "self-inbox",
                "SMS_GSM",
                null,
                "20260307T101001",
                "My Number",
                "+12025550100",
                "+12025550100",
                0,
                0,
                null,
                false,
                false,
                false,
                "testing self",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("My Number", ["+12025550100"], [])],
                [new MessageParticipantRecord("My Number", ["+12025550100"], [])])
        ];

        var result = synthesizer.Synthesize(messages, contacts);

        var conversation = Assert.Single(result.Conversations);
        Assert.False(conversation.IsGroup);
        var participant = Assert.Single(conversation.Participants);
        Assert.True(participant.IsSelf);
        Assert.Equal("self", participant.Key);
    }

    [Fact]
    public void ComputeMessageKey_IgnoresFolderOnlyChanges_WhenHandleIsMissing()
    {
        var outbox = new MessageRecord(
            "outbox",
            null,
            "SMS_GSM",
            null,
            "20260307T102000",
            "My Number",
            "+12025550100",
            "+12025550114",
            0,
            0,
            null,
            false,
            true,
            false,
            "same body",
            "SMSGSM",
            null,
            [new MessageParticipantRecord("My Number", ["+12025550100"], [])],
            [new MessageParticipantRecord("Alice", ["+12025550114"], [])]);
        var sent = outbox with
        {
            Folder = "sent",
            Read = true
        };

        var outboxKey = ConversationSynthesizer.ComputeMessageKey(outbox);
        var sentKey = ConversationSynthesizer.ComputeMessageKey(sent);

        Assert.Equal(outboxKey, sentKey);
    }

    [Fact]
    public void Synthesize_UsesAncsGroupHint_ToPromoteSparseMessageIntoGroupThread()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new ContactRecord(
                "1",
                "Alice",
                [new ContactPhoneRecord("+12025550114", "+12025550114", "Cell")],
                [])
        ];

        var message = new MessageRecord(
            "inbox",
            "group-hint-1",
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

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-07T17:15:02Z"),
                null,
                new NotificationRecord(
                    41,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-07T17:15:02Z"),
                    "com.apple.MobileSMS",
                    "Weekend Crew",
                    "Alice",
                    "group ping",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize([message], contacts, notifications);

        Assert.Single(result.Messages);
        var conversation = Assert.Single(result.Conversations);
        Assert.True(conversation.IsGroup);
        Assert.Equal("Weekend Crew", conversation.DisplayName);
        var participant = Assert.Single(conversation.Participants);
        Assert.Equal("Alice", participant.DisplayName);
    }

    [Fact]
    public void Synthesize_UsesAncsOneToOneHint_ToReplaceRawNumberDisplayName()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                [])
        ];

        var message = new MessageRecord(
            "inbox",
            "hint-1",
            "SMS_GSM",
            null,
            "20260307T111500",
            null,
            "+15551234567",
            "+12025550100",
            0,
            0,
            null,
            false,
            false,
            false,
            "landing now",
            "SMSGSM",
            "Unread",
            [new MessageParticipantRecord("(unnamed)", ["+15551234567"], [])],
            [new MessageParticipantRecord("My Number", ["+12025550100"], [])]);

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-07T19:15:01Z"),
                null,
                new NotificationRecord(
                    42,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-07T19:15:01Z"),
                    "com.apple.MobileSMS.notification",
                    "Ben",
                    null,
                    "landing now",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize([message], contacts, notifications);

        Assert.Single(result.Messages);
        var conversation = Assert.Single(result.Conversations);
        Assert.False(conversation.IsGroup);
        Assert.Equal("Ben", conversation.DisplayName);
    }

    [Fact]
    public void Synthesize_CreatesShadowConversation_ForActiveOneToOneMessagesNotification()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new ContactRecord(
                "1",
                "Ben",
                [new ContactPhoneRecord("(555) 123-4567", "+15551234567", "Cell")],
                [])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-07T19:15:01Z"),
                null,
                new NotificationRecord(
                    43,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-07T19:15:01Z"),
                    "com.apple.MobileSMS",
                    "Ben",
                    null,
                    "landing now",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(Array.Empty<MessageRecord>(), contacts, notifications);

        var message = Assert.Single(result.Messages);
        Assert.Equal("Ben", message.ConversationDisplayName);
        Assert.False(message.IsGroup);
        Assert.Equal("notification", message.Message.Folder);
        Assert.Equal("ANCS", message.Message.Type);
        var conversation = Assert.Single(result.Conversations);
        Assert.Equal("Ben", conversation.DisplayName);
        Assert.False(conversation.IsGroup);
        Assert.Equal(1, conversation.MessageCount);
        Assert.Equal(1, conversation.UnreadCount);
        var participant = Assert.Single(conversation.Participants);
        Assert.Equal("Ben", participant.DisplayName);
        Assert.Contains("+15551234567", participant.Phones);
        Assert.Contains("notification", conversation.SourceFolders);
    }

    [Fact]
    public void Synthesize_DoesNotExplodeSingleNotificationContact_WithMultiplePhones_IntoFakeGroup()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new ContactRecord(
                "1",
                "Palm Contact",
                [
                    new ContactPhoneRecord("+12025559052", "+12025559052", "Cell"),
                    new ContactPhoneRecord("+13105559052", "+13105559052", "Cell")
                ],
                [])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-13T00:26:59Z"),
                null,
                new NotificationRecord(
                    430,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-13T00:26:59Z"),
                    "com.apple.MobileSMS.notification",
                    "Palm Contact",
                    null,
                    "got it - this is a test message from adit",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(Array.Empty<MessageRecord>(), contacts, notifications);

        var message = Assert.Single(result.Messages);
        Assert.False(message.IsGroup);
        Assert.Equal("Palm Contact", message.ConversationDisplayName);
        Assert.Equal("name:Palm Contact", message.Message.SenderAddressing);

        var conversation = Assert.Single(result.Conversations);
        Assert.False(conversation.IsGroup);
        var participant = Assert.Single(conversation.Participants);
        Assert.Equal("Palm Contact", participant.DisplayName);
        Assert.Empty(participant.Phones);
    }

    [Fact]
    public void Synthesize_CreatesShadowConversation_ForActiveGroupMessagesNotification()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new ContactRecord(
                "1",
                "Maya",
                [new ContactPhoneRecord("(415) 555-0202", "+14155550202", "Cell")],
                [])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-07T20:15:01Z"),
                null,
                new NotificationRecord(
                    44,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-07T20:15:01Z"),
                    "com.apple.MobileSMS.notification",
                    "Weekend Crew",
                    "Maya",
                    "7pm still works?",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(Array.Empty<MessageRecord>(), contacts, notifications);

        var message = Assert.Single(result.Messages);
        Assert.True(message.IsGroup);
        Assert.Equal("Weekend Crew", message.ConversationDisplayName);
        Assert.Equal("notification", message.Message.Folder);
        var conversation = Assert.Single(result.Conversations);
        Assert.True(conversation.IsGroup);
        Assert.Equal("Weekend Crew", conversation.DisplayName);
        Assert.Equal(1, conversation.MessageCount);
        Assert.Equal(1, conversation.UnreadCount);
        var participant = Assert.Single(conversation.Participants);
        Assert.Equal("Maya", participant.DisplayName);
        Assert.Contains("+14155550202", participant.Phones);
    }

    [Fact]
    public void Synthesize_CreatesShadowConversation_WhenTitleCarriesLargeGroupDescriptor()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new ContactRecord(
                "1",
                "mom",
                [new ContactPhoneRecord("(202) 555-0101", "+12025550101", "Cell")],
                [])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T03:27:44Z"),
                null,
                new NotificationRecord(
                    440,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T03:27:44Z"),
                    "com.apple.MobileSMS.notification",
                    "To you, mom & 17 others",
                    null,
                    "+1 (202) 555-0109 laughed at an attachment",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(Array.Empty<MessageRecord>(), contacts, notifications);

        var conversation = Assert.Single(result.Conversations);
        Assert.True(conversation.IsGroup);
        Assert.Equal("To you, mom & 17 others", conversation.DisplayName);
        Assert.Contains(conversation.Participants, participant => participant.DisplayName == "mom");
        Assert.DoesNotContain(
            conversation.Participants,
            participant => participant.DisplayName.Contains("To you", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(
            conversation.Participants,
            participant => participant.DisplayName.Contains("202", StringComparison.Ordinal));
    }

    [Fact]
    public void Synthesize_DropsTruncatedTailFragment_FromLargeGroupDescriptor()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new ContactRecord(
                "1",
                "Aunt Beth",
                [new ContactPhoneRecord("(202) 555-0104", "+12025550104", "Cell")],
                []),
            new ContactRecord(
                "2",
                "romy  navarro",
                [new ContactPhoneRecord("(202) 555-0111", "+12025550111", "Cell")],
                []),
            new ContactRecord(
                "3",
                "Cami",
                [new ContactPhoneRecord("(202) 555-0116", "+12025550116", "Cell")],
                []),
            new ContactRecord(
                "4",
                "dad",
                [new ContactPhoneRecord("(202) 555-0102", "+12025550102", "Cell")],
                []),
            new ContactRecord(
                "5",
                "ellis",
                [new ContactPhoneRecord("(202) 555-0103", "+12025550103", "Cell")],
                []),
            new ContactRecord(
                "6",
                "mom",
                [new ContactPhoneRecord("(202) 555-0101", "+12025550101", "Cell")],
                [])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T03:30:09Z"),
                null,
                new NotificationRecord(
                    441,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T03:30:09Z"),
                    "com.apple.MobileSMS.notification",
                    "Tessa Lane Mercer",
                    "To you, mom, dad, ellis, Aunt Beth, romy  navarro, Cami, Au…",
                    "laughed at an attachment",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(Array.Empty<MessageRecord>(), contacts, notifications);

        var conversation = Assert.Single(result.Conversations);
        Assert.True(conversation.IsGroup);
        Assert.DoesNotContain(conversation.Participants, participant => participant.DisplayName == "Au");
        Assert.Contains(conversation.Participants, participant => participant.DisplayName == "mom");
        Assert.Contains(conversation.Participants, participant => participant.DisplayName == "Aunt Beth");
        Assert.Contains(conversation.Participants, participant => participant.DisplayName == "Cami");
    }

    [Fact]
    public void Synthesize_UsesAncsPreview_WhenMatchedMapMessageHasNoBody()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new ContactRecord(
                "1",
                "Maya",
                [new ContactPhoneRecord("(415) 555-0202", "+14155550202", "Cell")],
                [])
        ];

        var message = new MessageRecord(
            "inbox",
            "hint-preview-1",
            "SMS_GSM",
            null,
            "20260307T121500",
            "Maya",
            "+14155550202",
            "+12025550100",
            0,
            0,
            null,
            false,
            false,
            false,
            null,
            "SMSGSM",
            "Unread",
            [new MessageParticipantRecord("Maya", ["+14155550202"], [])],
            [new MessageParticipantRecord("My Number", ["+12025550100"], [])]);

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-07T20:15:01Z"),
                null,
                new NotificationRecord(
                    45,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-07T20:15:01Z"),
                    "com.apple.MobileSMS",
                    "Maya",
                    null,
                    "7pm still works?",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize([message], contacts, notifications);

        var synthesizedMessage = Assert.Single(result.Messages);
        Assert.Equal("7pm still works?", synthesizedMessage.Message.Body);
        var conversation = Assert.Single(result.Conversations);
        Assert.Equal("7pm still works?", conversation.LastPreview);
    }

    [Fact]
    public void Synthesize_UsesSubtitleAsGroupTitle_WhenAncsSubtitleLooksLikeParticipantList()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new ContactRecord(
                "1",
                "Maren",
                [new ContactPhoneRecord("(202) 555-0107", "+12025550107", "Cell")],
                [])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T02:26:25Z"),
                null,
                new NotificationRecord(
                    46,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T02:26:25Z"),
                    "com.apple.MobileSMS.notification",
                    "Maren",
                    "To you, mom, dad, ellis",
                    "Loved an image",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(Array.Empty<MessageRecord>(), contacts, notifications);

        var message = Assert.Single(result.Messages);
        Assert.True(message.IsGroup);
        Assert.Equal("mom, dad, ellis", message.ConversationDisplayName);

        var conversation = Assert.Single(result.Conversations);
        Assert.True(conversation.IsGroup);
        Assert.Equal("mom, dad, ellis", conversation.DisplayName);
        Assert.Contains(conversation.Participants, participant => participant.DisplayName == "Maren");
        Assert.Contains(conversation.Participants, participant => participant.DisplayName == "mom");
        Assert.Contains(conversation.Participants, participant => participant.DisplayName == "dad");
        Assert.Contains(conversation.Participants, participant => participant.DisplayName == "ellis");
    }

    [Fact]
    public void Synthesize_MatchesMapMessageToGroupHint_WhenSubtitleCarriesThreadDescriptor()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new ContactRecord(
                "1",
                "Maren",
                [new ContactPhoneRecord("(202) 555-0107", "+12025550107", "Cell")],
                [])
        ];

        var message = new MessageRecord(
            "inbox",
            "group-reaction-1",
            "SMS_GSM",
            null,
            "20260308T182625",
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

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T02:26:25Z"),
                null,
                new NotificationRecord(
                    47,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T02:26:25Z"),
                    "com.apple.MobileSMS.notification",
                    "Maren",
                    "To you, mom, dad, ellis",
                    "Loved an image",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize([message], contacts, notifications);

        Assert.Single(result.Messages);
        var conversation = Assert.Single(result.Conversations);
        Assert.True(conversation.IsGroup);
        Assert.Equal("mom, dad, ellis", conversation.DisplayName);
        Assert.Equal("Loved an image", conversation.LastPreview);
    }

    [Fact]
    public void Synthesize_UsesCanonicalMemberSeed_ForGroupHintsAcrossDifferentAuthors()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new ContactRecord(
                "1",
                "Maren",
                [new ContactPhoneRecord("(202) 555-0107", "+12025550107", "Cell")],
                []),
            new ContactRecord(
                "2",
                "Aunt Beth",
                [new ContactPhoneRecord("(202) 555-0104", "+12025550104", "Cell")],
                [])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T02:26:25Z"),
                null,
                new NotificationRecord(
                    48,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T02:26:25Z"),
                    "com.apple.MobileSMS",
                    "Maren",
                    "To you, mom, dad, Aunt Beth",
                    "Loved an image",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>())),
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T02:27:25Z"),
                null,
                new NotificationRecord(
                    49,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T02:27:25Z"),
                    "com.apple.MobileSMS",
                    "Aunt Beth",
                    "To you, mom, dad, Maren",
                    "Loved another image",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(Array.Empty<MessageRecord>(), contacts, notifications);

        var conversation = Assert.Single(result.Conversations);
        Assert.True(conversation.IsGroup);
        Assert.Equal(2, conversation.MessageCount);
    }

    [Fact]
    public void Synthesize_MergesEquivalentDescriptorGroups_WhenVisibleMembersShift()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new ContactRecord(
                "1",
                "Maren",
                [new ContactPhoneRecord("(202) 555-0107", "+12025550107", "Cell")],
                []),
            new ContactRecord(
                "2",
                "Aunt Beth",
                [new ContactPhoneRecord("(202) 555-0104", "+12025550104", "Cell")],
                []),
            new ContactRecord(
                "3",
                "Tessa Lane Mercer",
                [new ContactPhoneRecord("(202) 555-0112", "+12025550112", "Cell")],
                []),
            new ContactRecord(
                "4",
                "mom",
                [new ContactPhoneRecord("(202) 555-0101", "+12025550101", "Cell")],
                [])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T02:26:25Z"),
                null,
                new NotificationRecord(
                    48,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T02:26:25Z"),
                    "com.apple.MobileSMS",
                    "Maren",
                    "To you, mom, dad, ellis, Aunt Beth",
                    "Loved an image",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>())),
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T02:27:25Z"),
                null,
                new NotificationRecord(
                    49,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T02:27:25Z"),
                    "com.apple.MobileSMS",
                    "Aunt Beth",
                    "To you, mom, dad, ellis, Tessa Lane Mercer",
                    "Loved another image",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(Array.Empty<MessageRecord>(), contacts, notifications);

        var conversation = Assert.Single(result.Conversations);
        Assert.True(conversation.IsGroup);
        Assert.Equal(2, conversation.MessageCount);
        Assert.Equal("mom, dad, ellis +1", conversation.DisplayName);
    }

    [Fact]
    public void Synthesize_ReassignsReactionMessageIntoMatchingGroupThread()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new ContactRecord(
                "1",
                "Aunt Beth",
                [new ContactPhoneRecord("(202) 555-0104", "+12025550104", "Cell")],
                []),
            new ContactRecord(
                "2",
                "Maren",
                [new ContactPhoneRecord("(202) 555-0107", "+12025550107", "Cell")],
                [])
        ];

        MessageRecord[] messages =
        [
            new MessageRecord(
                "inbox",
                "group-origin",
                "SMS_GSM",
                null,
                "20260308T191230",
                "2025550104",
                "2025550104",
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
                [new MessageParticipantRecord("2025550104", ["2025550104"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new MessageRecord(
                "inbox",
                "group-reaction-map-only",
                "SMS_GSM",
                null,
                "20260308T192625",
                "2025550110",
                "2025550110",
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
                [new MessageParticipantRecord("2025550110", ["2025550110"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T02:12:30Z"),
                null,
                new NotificationRecord(
                    18,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T02:12:30Z"),
                    "com.apple.MobileSMS",
                    "Aunt Beth",
                    "To you, mom, dad, ellis, Tessa Lane Mercer",
                    "I tried making coffee with sparkling water earlier.",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(messages, contacts, notifications);

        var conversation = Assert.Single(result.Conversations);
        Assert.True(conversation.IsGroup);
        Assert.Equal(2, conversation.MessageCount);
        Assert.Contains(result.Messages, message => message.Message.Handle == "group-reaction-map-only" && message.IsGroup);
    }

    [Fact]
    public void Synthesize_EnrichesGroupParticipants_FromAncsDescriptorMembers()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new ContactRecord(
                "1",
                "Maren",
                [new ContactPhoneRecord("(202) 555-0107", "+12025550107", "Cell")],
                []),
            new ContactRecord(
                "2",
                "Aunt Beth",
                [new ContactPhoneRecord("(202) 555-0104", "+12025550104", "Cell")],
                [])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T02:26:25Z"),
                null,
                new NotificationRecord(
                    50,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T02:26:25Z"),
                    "com.apple.MobileSMS",
                    "Maren",
                    "To you, Aunt Beth, dad",
                    "Loved an image",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(Array.Empty<MessageRecord>(), contacts, notifications);

        var conversation = Assert.Single(result.Conversations);
        Assert.True(conversation.IsGroup);
        Assert.Contains(conversation.Participants, participant => participant.DisplayName == "Maren");
        Assert.Contains(conversation.Participants, participant => participant.DisplayName == "Aunt Beth");
        Assert.Contains(conversation.Participants, participant => participant.DisplayName == "dad");
    }

    [Fact]
    public void Synthesize_UsesStableSeed_ForCountBasedGroupDescriptorsAcrossAuthors()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new ContactRecord(
                "1",
                "Maren",
                [new ContactPhoneRecord("(202) 555-0107", "+12025550107", "Cell")],
                []),
            new ContactRecord(
                "2",
                "Aunt Beth",
                [new ContactPhoneRecord("(202) 555-0104", "+12025550104", "Cell")],
                []),
            new ContactRecord(
                "3",
                "mom",
                [new ContactPhoneRecord("(202) 555-0101", "+12025550101", "Cell")],
                [])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T03:11:00Z"),
                null,
                new NotificationRecord(
                    51,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T03:11:00Z"),
                    "com.apple.MobileSMS",
                    "Maren",
                    "To you, mom & 17 others",
                    "I tried making coffee with sparkling water earlier.",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>())),
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T03:12:00Z"),
                null,
                new NotificationRecord(
                    52,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T03:12:00Z"),
                    "com.apple.MobileSMS",
                    "Aunt Beth",
                    "To you, mom & 17 others",
                    "Aw! I'll call them tonight.",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(Array.Empty<MessageRecord>(), contacts, notifications);

        var conversation = Assert.Single(result.Conversations);
        Assert.True(conversation.IsGroup);
        Assert.Equal("To you, mom & 17 others", conversation.DisplayName);
        Assert.Equal(2, conversation.MessageCount);
        Assert.Contains(conversation.Participants, participant => participant.DisplayName == "Maren");
        Assert.Contains(conversation.Participants, participant => participant.DisplayName == "Aunt Beth");
        Assert.Contains(conversation.Participants, participant => participant.DisplayName == "mom");
    }

    [Fact]
    public void Synthesize_MatchesLargeGroupReaction_WhenPreviewDoesNotMatchExactly()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new ContactRecord(
                "1",
                "mom",
                [new ContactPhoneRecord("(202) 555-0101", "+12025550101", "Cell")],
                [])
        ];

        var message = new MessageRecord(
            "inbox",
            "group-reaction-large-1",
            "SMS_GSM",
            null,
            "20260308T201100",
            "+1 (202) 555-0110",
            "+12025550110",
            "+12025550100",
            0,
            0,
            null,
            false,
            false,
            false,
            "Laughed at “I tried making coffee with sparkling water earlier. It tasted like a dare but I kept drinking it anyway...”",
            "SMSGSM",
            "Unread",
            [new MessageParticipantRecord("+1 (202) 555-0110", ["+12025550110"], [])],
            [new MessageParticipantRecord("My Number", ["+12025550100"], [])]);

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T03:11:00Z"),
                null,
                new NotificationRecord(
                    53,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T03:11:00Z"),
                    "com.apple.MobileSMS",
                    "+1 (202) 555-0110",
                    "To you, mom & 17 others",
                    "+1 (202) 555-0110 laughed at an attachment",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize([message], contacts, notifications);

        var conversation = Assert.Single(result.Conversations);
        Assert.True(conversation.IsGroup);
        Assert.Equal("To you, mom & 17 others", conversation.DisplayName);
        Assert.Contains(conversation.Participants, participant => participant.DisplayName == "mom");
        Assert.Contains(conversation.Participants, participant => participant.DisplayName.Contains("202", StringComparison.Ordinal));
    }

    [Fact]
    public void Synthesize_ReassignsSparseGroupContextMessages_IntoEstablishedLargeGroup()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new ContactRecord(
                "1",
                "Aunt Beth",
                [new ContactPhoneRecord("(202) 555-0104", "+12025550104", "Cell")],
                []),
            new ContactRecord(
                "2",
                "Aunt Jean",
                [new ContactPhoneRecord("(202) 555-0105", "+12025550105", "Cell")],
                []),
            new ContactRecord(
                "3",
                "mom",
                [new ContactPhoneRecord("(202) 555-0101", "+12025550101", "Cell")],
                []),
            new ContactRecord(
                "4",
                "dad",
                [new ContactPhoneRecord("(202) 555-0102", "+12025550102", "Cell")],
                []),
            new ContactRecord(
                "5",
                "ellis",
                [new ContactPhoneRecord("(202) 555-0103", "+12025550103", "Cell")],
                [])
        ];

        MessageRecord[] messages =
        [
            new MessageRecord(
                "inbox",
                "aunt-jean-groupish",
                "SMS_GSM",
                null,
                "20260308T183457",
                "Aunt Jean",
                "+12025550105",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "Aw! I'll call them tonight. 👍🥰",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("Aunt Jean", ["+12025550105"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T02:18:50Z"),
                null,
                new NotificationRecord(
                    530,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T02:18:50Z"),
                    "com.apple.MobileSMS",
                    "Aunt Jean",
                    "To you, mom, dad, ellis, Aunt Beth",
                    "Do you think Aunt Jean is coming today",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(messages, contacts, notifications);

        var conversation = Assert.Single(result.Conversations);
        Assert.True(conversation.IsGroup);
        Assert.Equal(2, conversation.MessageCount);
        Assert.Contains(result.Messages, message => message.Message.Handle == "aunt-jean-groupish" && message.IsGroup);
        Assert.Contains(conversation.Participants, participant => participant.DisplayName == "Aunt Jean");
        Assert.Contains(conversation.Participants, participant => participant.DisplayName == "Aunt Beth");
    }

    [Fact]
    public void Synthesize_DoesNotOvermergeDirectMessageIntoEstablishedLargeGroup()
    {
        ContactRecord[] contacts =
        [
            new ContactRecord(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new ContactRecord(
                "1",
                "mom",
                [new ContactPhoneRecord("(202) 555-0101", "+12025550101", "Cell")],
                []),
            new ContactRecord(
                "2",
                "Aunt Beth",
                [new ContactPhoneRecord("(202) 555-0104", "+12025550104", "Cell")],
                []),
            new ContactRecord(
                "3",
                "dad",
                [new ContactPhoneRecord("(202) 555-0102", "+12025550102", "Cell")],
                []),
            new ContactRecord(
                "4",
                "ellis",
                [new ContactPhoneRecord("(202) 555-0103", "+12025550103", "Cell")],
                [])
        ];

        MessageRecord[] messages =
        [
            new MessageRecord(
                "inbox",
                "mom-direct",
                "SMS_GSM",
                null,
                "20260308T184237",
                "mom",
                "+12025550101",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "Do u want me to bring it",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("mom", ["+12025550101"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T02:18:50Z"),
                null,
                new NotificationRecord(
                    531,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T02:18:50Z"),
                    "com.apple.MobileSMS",
                    "Aunt Beth",
                    "To you, mom, dad, ellis",
                    "I tried making coffee with sparkling water earlier.",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(messages, contacts, notifications);

        Assert.Equal(2, result.Conversations.Count);
        Assert.Contains(result.Conversations, conversation => conversation.IsGroup);
        Assert.Contains(result.Conversations, conversation => !conversation.IsGroup && conversation.DisplayName == "mom");
        Assert.Contains(result.Messages, message => message.Message.Handle == "mom-direct" && !message.IsGroup);
    }

    [Fact]
    public void Synthesize_CollapsesShadowDuplicate_WhenDurableGroupMessageMatchesNotification()
    {
        ContactRecord[] contacts =
        [
            new(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new(
                "1",
                "grandma",
                [new ContactPhoneRecord("(202) 555-0108", "+12025550108", "Cell")],
                []),
            new(
                "2",
                "mom",
                [new ContactPhoneRecord("(202) 555-0101", "+12025550101", "Cell")],
                []),
            new(
                "3",
                "dad",
                [new ContactPhoneRecord("(202) 555-0102", "+12025550102", "Cell")],
                []),
            new(
                "4",
                "ellis",
                [new ContactPhoneRecord("(202) 555-0103", "+12025550103", "Cell")],
                [])
        ];

        MessageRecord[] messages =
        [
            new(
                "inbox",
                "grandma-teehee",
                "SMS_GSM",
                null,
                "20260308T194507",
                "2025550108",
                "+12025550108",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "Teehee",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("2025550108", ["+12025550108"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T03:45:07Z"),
                null,
                new NotificationRecord(
                    900,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T03:45:07Z"),
                    "com.apple.MobileSMS",
                    "grandma",
                    "To you, mom, dad, ellis",
                    "Teehee",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(messages, contacts, notifications);

        var conversation = Assert.Single(result.Conversations);
        Assert.True(conversation.IsGroup);
        Assert.Single(result.Messages);
        var synthesized = result.Messages[0];
        Assert.Equal("grandma-teehee", synthesized.Message.Handle);
        Assert.Equal("grandma", synthesized.Message.SenderName);
        Assert.Equal("Teehee", synthesized.Message.Body);
        Assert.DoesNotContain(result.Messages, message => string.Equals(message.Message.Type, "ANCS", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Synthesize_CollapsesShadowDuplicate_WhenDurableMessageIsBlankButSenderAndTimestampMatch()
    {
        ContactRecord[] contacts =
        [
            new(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new(
                "1",
                "423-02",
                [new ContactPhoneRecord("42302", "+42302", "ShortCode")],
                [])
        ];

        MessageRecord[] messages =
        [
            new(
                "inbox",
                "shortcode-map",
                "SMS_GSM",
                null,
                "20260308T181237",
                "42302",
                "42302",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                string.Empty,
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("42302", ["42302"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T01:12:37Z"),
                null,
                new NotificationRecord(
                    901,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T01:12:37Z"),
                    "com.apple.MobileSMS.notification",
                    "423-02",
                    null,
                    "DEVILS FRAT X GEN Z ENT: 2 STAGES, CO2 CANNON GUN, 12FT & 6FT LEPRECHAUNS",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(messages, contacts, notifications);

        var message = Assert.Single(result.Messages);
        Assert.Equal("shortcode-map", message.Message.Handle);
        Assert.Equal("423-02", message.Message.SenderName);
        Assert.Equal(
            "DEVILS FRAT X GEN Z ENT: 2 STAGES, CO2 CANNON GUN, 12FT & 6FT LEPRECHAUNS",
            message.Message.Body);
        Assert.DoesNotContain(result.Messages, candidate => string.Equals(candidate.Message.Type, "ANCS", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Synthesize_DedupesDuplicateNotificationHints_ForAmbiguousMultiPhoneContact()
    {
        ContactRecord[] contacts =
        [
            new(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new(
                "1",
                "Palm Contact",
                [
                    new ContactPhoneRecord("+12025559052", "+12025559052", "Cell"),
                    new ContactPhoneRecord("+13105559052", "+13105559052", "Cell")
                ],
                [])
        ];

        MessageRecord[] messages =
        [
            new(
                "inbox",
                "palm-map",
                "SMS_GSM",
                null,
                "20260312T202659",
                "Palm Contact",
                "+12025559052",
                "",
                41,
                0,
                null,
                false,
                false,
                false,
                "got it - this is a test message from adit",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("+12025559052", ["+12025559052"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-13T00:42:17Z"),
                null,
                new NotificationRecord(
                    6,
                    NotificationEventKind.Added,
                    NotificationEventFlags.NegativeAction | NotificationEventFlags.Important,
                    NotificationCategory.Social,
                    4,
                    DateTimeOffset.Parse("2026-03-13T00:42:16Z"),
                    "com.apple.MobileSMS",
                    "Palm Contact",
                    "",
                    "got it - this is a test message from adit",
                    "41",
                    "20260312T202659",
                    "",
                    "Clear",
                    new Dictionary<string, string>())),
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-13T00:26:59Z"),
                null,
                new NotificationRecord(
                    225,
                    NotificationEventKind.Added,
                    NotificationEventFlags.NegativeAction,
                    NotificationCategory.Social,
                    36,
                    DateTimeOffset.Parse("2026-03-13T00:26:59Z"),
                    "com.apple.MobileSMS",
                    "Palm Contact",
                    "",
                    "got it - this is a test message from adit",
                    "41",
                    "20260312T202659",
                    "",
                    "Clear",
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(messages, contacts, notifications);

        var message = Assert.Single(result.Messages);
        Assert.Equal("palm-map", message.Message.Handle);
        Assert.Equal("Palm Contact", message.ConversationDisplayName);
        Assert.False(message.IsGroup);
        Assert.DoesNotContain(result.Messages, candidate => string.Equals(candidate.Message.Type, "ANCS", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Synthesize_UsesMostRecentNonBlankPreview_ForConversationSnapshot()
    {
        ContactRecord[] contacts =
        [
            new(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new(
                "1",
                "423-02",
                [new ContactPhoneRecord("42302", "+42302", "ShortCode")],
                [])
        ];

        MessageRecord[] messages =
        [
            new(
                "inbox",
                "shortcode-newer",
                "SMS_GSM",
                null,
                "20260308T181237",
                "42302",
                "42302",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                string.Empty,
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("42302", ["42302"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "shortcode-older",
                "SMS_GSM",
                null,
                "20260307T204217",
                "42302",
                "42302",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "DEVILS FRAT X GEN Z ENT: ST PATTYS LINK BELOW!",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("42302", ["42302"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])])
        ];

        var result = synthesizer.Synthesize(messages, contacts);

        var conversation = Assert.Single(result.Conversations);
        Assert.Equal("DEVILS FRAT X GEN Z ENT: ST PATTYS LINK BELOW!", conversation.LastPreview);
        Assert.Equal("423-02", conversation.LastSenderDisplayName);
    }

    [Fact]
    public void Synthesize_ReassignsGenericReactionMessage_UsingStoredGroupHint()
    {
        ContactRecord[] contacts =
        [
            new(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new(
                "1",
                "romy  navarro",
                [new ContactPhoneRecord("(202) 555-0111", "+12025550111", "Cell")],
                []),
            new(
                "2",
                "mom",
                [new ContactPhoneRecord("(202) 555-0101", "+12025550101", "Cell")],
                []),
            new(
                "3",
                "dad",
                [new ContactPhoneRecord("(202) 555-0102", "+12025550102", "Cell")],
                []),
            new(
                "4",
                "ellis",
                [new ContactPhoneRecord("(202) 555-0103", "+12025550103", "Cell")],
                []),
            new(
                "5",
                "Aunt Beth",
                [new ContactPhoneRecord("(202) 555-0104", "+12025550104", "Cell")],
                [])
        ];

        MessageRecord[] messages =
        [
            new(
                "inbox",
                "group-origin",
                "SMS_GSM",
                null,
                "20260308T181500",
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
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "romy-reacted",
                "SMS_GSM",
                null,
                "20260308T151737",
                "2025550111",
                "+12025550111",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "Loved an image",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("2025550111", ["+12025550111"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T01:15:10Z"),
                null,
                new NotificationRecord(
                    900,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T01:15:10Z"),
                    "com.apple.MobileSMS",
                    "Aunt Beth",
                    "To you, mom, dad, ellis, romy  navarro",
                    "I tried making coffee with sparkling water earlier.",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>())),
            new(
                "device-1",
                false,
                DateTimeOffset.Parse("2026-03-09T00:50:59Z"),
                DateTimeOffset.Parse("2026-03-09T00:50:59Z"),
                new NotificationRecord(
                    901,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T00:50:44Z"),
                    "com.apple.MobileSMS",
                    "romy  navarro",
                    "To you, mom, dad, ellis, Aunt Beth",
                    "romy  navarro reacted 😭 to “I tried making coffee with sparkling water earlier.”",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(messages, contacts, notifications);

        var conversation = Assert.Single(result.Conversations);
        Assert.True(conversation.IsGroup);
        Assert.Equal(2, conversation.MessageCount);
        Assert.Contains(conversation.Participants, participant => participant.DisplayName == "romy  navarro");
        Assert.Contains(result.Messages, message => message.Message.Handle == "romy-reacted" && message.IsGroup);
    }

    [Fact]
    public void Synthesize_KeepsDirectReactionInOneToOneThread_WhenReferenceMatchesCurrentThread()
    {
        ContactRecord[] contacts =
        [
            new(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new(
                "1",
                "mom",
                [new ContactPhoneRecord("(202) 555-0101", "+12025550101", "Cell")],
                []),
            new(
                "2",
                "Aunt Beth",
                [new ContactPhoneRecord("(202) 555-0104", "+12025550104", "Cell")],
                []),
            new(
                "3",
                "dad",
                [new ContactPhoneRecord("(202) 555-0102", "+12025550102", "Cell")],
                []),
            new(
                "4",
                "ellis",
                [new ContactPhoneRecord("(202) 555-0103", "+12025550103", "Cell")],
                []),
            new(
                "5",
                "Aunt Jean",
                [new ContactPhoneRecord("(202) 555-0105", "+12025550105", "Cell")],
                []),
            new(
                "6",
                "2025550109",
                [new ContactPhoneRecord("(202) 555-0109", "+12025550109", "Cell")],
                [])
        ];

        MessageRecord[] messages =
        [
            new(
                "sent",
                "mom-direct",
                "SMS_GSM",
                null,
                "20260308T164417",
                "Me",
                "name:Me",
                "+12025550101",
                0,
                0,
                null,
                true,
                true,
                false,
                "testmap smoke 1",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("Me", [], [])],
                [new MessageParticipantRecord("mom", ["+12025550101"], [])]),
            new(
                "inbox",
                "mom-reacted",
                "SMS_GSM",
                null,
                "20260308T184438",
                "12025550101",
                "+12025550101",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "Disliked “testmap smoke 1”",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("12025550101", ["+12025550101"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "group-origin",
                "SMS_GSM",
                null,
                "20260308T181230",
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
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "group-followup",
                "SMS_GSM",
                null,
                "20260308T183753",
                "2025550109",
                "+12025550109",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "Okay I'll tell them 😂, how did Nina do at practice.",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("2025550109", ["+12025550109"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "group-aunt-jean",
                "SMS_GSM",
                null,
                "20260308T183457",
                "2025550105",
                "+12025550105",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "Aw! I'll call them tonight. 👍🥰",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("2025550105", ["+12025550105"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T02:44:10Z"),
                null,
                new NotificationRecord(
                    910,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T02:44:10Z"),
                    "com.apple.MobileSMS",
                    "Aunt Beth",
                    "To you, mom, dad, ellis",
                    "I tried making coffee with sparkling water earlier.",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(messages, contacts, notifications);

        var directConversation = result.Conversations.Single(conversation => conversation.DisplayName == "mom");
        Assert.DoesNotContain(
            result.Messages.Where(message => message.ConversationId != directConversation.ConversationId),
            message => message.Message.Handle == "mom-reacted");
        Assert.Contains(
            result.Messages.Where(message => message.ConversationId == directConversation.ConversationId),
            message => message.Message.Handle == "mom-reacted");
    }

    [Fact]
    public void Synthesize_DoesNotUseUnrelatedReactionHint_ToStealDirectReaction()
    {
        ContactRecord[] contacts =
        [
            new(
                "0",
                "My Number",
                [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")],
                []),
            new(
                "1",
                "mom",
                [new ContactPhoneRecord("+12025550101", "+12025550101", "Cell")],
                [])
        ];

        MessageRecord[] messages =
        [
            new(
                "sent",
                "sent-direct",
                "SMS_GSM",
                null,
                "20260308T154417",
                "Me",
                "name:Me",
                "+12025550101",
                0,
                0,
                null,
                true,
                true,
                false,
                "testmap smoke 1",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("Me", [], [])],
                [new MessageParticipantRecord("mom", ["+12025550101"], [])]),
            new(
                "inbox",
                "mom-reacted",
                "SMS_GSM",
                null,
                "20260308T154438",
                "12025550101",
                "+12025550101",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "Disliked \"testmap smoke 1\"",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("12025550101", ["+12025550101"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-08T23:44:10Z"),
                null,
                new NotificationRecord(
                    911,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-08T23:44:10Z"),
                    "com.apple.MobileSMS",
                    "mom",
                    "To you, dad, ellis, Aunt Beth",
                    "Loved an image",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(messages, contacts, notifications);

        var conversation = result.Conversations.Single(conversation => !conversation.IsGroup && conversation.DisplayName == "mom");
        Assert.Contains(result.Messages, message => message.Message.Handle == "mom-reacted" && message.ConversationId == conversation.ConversationId);
    }

    [Fact]
    public void Synthesize_MergesSparseFamilyGroupMessages_WithoutPullingDirectMomThread()
    {
        ContactRecord[] contacts =
        [
            new("0", "My Number", [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")], []),
            new("1", "mom", [new ContactPhoneRecord("+12025550101", "+12025550101", "Cell")], []),
            new("2", "Aunt Beth", [new ContactPhoneRecord("+12025550104", "+12025550104", "Cell")], []),
            new("3", "Aunt Jean", [new ContactPhoneRecord("+12025550105", "+12025550105", "Cell")], []),
            new("4", "Nina J. Mercer Esq.", [new ContactPhoneRecord("+12025550106", "+12025550106", "Cell")], []),
            new("5", "2025550109", [new ContactPhoneRecord("+12025550109", "+12025550109", "Cell")], []),
            new("6", "Maren", [new ContactPhoneRecord("+12025550107", "+12025550107", "Cell")], []),
            new("7", "grandma", [new ContactPhoneRecord("+12025550108", "+12025550108", "Cell")], [])
        ];

        MessageRecord[] messages =
        [
            new(
                "sent",
                "mom-direct-origin",
                "SMS_GSM",
                null,
                "20260308T154417",
                "Me",
                "name:Me",
                "+12025550101",
                0,
                0,
                null,
                true,
                true,
                false,
                "testmap smoke 1",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("Me", [], [])],
                [new MessageParticipantRecord("mom", ["+12025550101"], [])]),
            new(
                "inbox",
                "mom-reacted",
                "SMS_GSM",
                null,
                "20260308T154438",
                "12025550101",
                "+12025550101",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "Disliked \"testmap smoke 1\"",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("12025550101", ["+12025550101"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "group-origin",
                "SMS_GSM",
                null,
                "20260308T181230",
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
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "group-mid",
                "SMS_GSM",
                null,
                "20260308T183457",
                "2025550105",
                "+12025550105",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "Aw! I'll call them tonight. 👍🥰",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("2025550105", ["+12025550105"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "group-followup",
                "SMS_GSM",
                null,
                "20260308T183753",
                "2025550109",
                "+12025550109",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "Okay I'll tell them 😂, how did Nina do at practice.",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("2025550109", ["+12025550109"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "group-reaction",
                "SMS_GSM",
                null,
                "20260308T184040",
                "2025550107",
                "+12025550107",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "Laughed at \"I tried making coffee with sparkling water earlier.\"",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("2025550107", ["+12025550107"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "group-blank",
                "SMS_GSM",
                null,
                "20260308T141850",
                "2025550109",
                "+12025550109",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("2025550109", ["+12025550109"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "group-nina-reaction",
                "SMS_GSM",
                null,
                "20260308T144248",
                "2025550106",
                "+12025550106",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "\u200A\u200B❤️\u200B to “\u200ADo you think Aunt Jean is coming today\u200A”",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("2025550106", ["+12025550106"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "mom-plain-direct",
                "SMS_GSM",
                null,
                "20260308T193648",
                "12025550101",
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
                [new MessageParticipantRecord("12025550101", ["+12025550101"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "grandma-teehee",
                "SMS_GSM",
                null,
                "20260308T204507",
                "2025550108",
                "+12025550108",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "Teehee",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("2025550108", ["+12025550108"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T02:12:10Z"),
                null,
                new NotificationRecord(
                    912,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T02:12:10Z"),
                    "com.apple.MobileSMS",
                    "Aunt Beth",
                    "To you, mom, dad, ellis, Aunt Jean, Nina J. Mercer Esq.",
                    "I tried making coffee with sparkling water earlier.",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(messages, contacts, notifications);

        var groupConversation = result.Conversations.Single(conversation => conversation.IsGroup);
        Assert.Contains(
            result.Messages.Where(message => message.ConversationId == groupConversation.ConversationId),
            message => message.Message.Handle == "group-blank");
        Assert.Contains(
            result.Messages.Where(message => message.ConversationId == groupConversation.ConversationId),
            message => message.Message.Handle == "group-nina-reaction");
        Assert.Contains(
            result.Messages.Where(message => message.ConversationId == groupConversation.ConversationId),
            message => message.Message.Handle == "grandma-teehee");

        var directConversation = result.Conversations.Single(conversation => !conversation.IsGroup && conversation.DisplayName == "mom");
        Assert.Contains(
            result.Messages.Where(message => message.ConversationId == directConversation.ConversationId),
            message => message.Message.Handle == "mom-reacted");
        Assert.Contains(
            result.Messages.Where(message => message.ConversationId == directConversation.ConversationId),
            message => message.Message.Handle == "mom-plain-direct");
    }

    [Fact]
    public void Synthesize_KeepsLowSignalMomMessagesOutOfEstablishedFamilyBurst()
    {
        ContactRecord[] contacts =
        [
            new("0", "My Number", [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")], []),
            new("1", "mom", [new ContactPhoneRecord("+12025550101", "+12025550101", "Cell")], []),
            new("2", "dad", [new ContactPhoneRecord("+12025550102", "+12025550102", "Cell")], []),
            new("3", "ellis", [new ContactPhoneRecord("+12025550103", "+12025550103", "Cell")], []),
            new("4", "Aunt Beth", [new ContactPhoneRecord("+12025550104", "+12025550104", "Cell")], []),
            new("5", "Aunt Jean", [new ContactPhoneRecord("+12025550105", "+12025550105", "Cell")], []),
            new("6", "Nina J. Mercer Esq.", [new ContactPhoneRecord("+12025550106", "+12025550106", "Cell")], []),
            new("7", "Maren", [new ContactPhoneRecord("+12025550107", "+12025550107", "Cell")], []),
            new("8", "grandma", [new ContactPhoneRecord("+12025550108", "+12025550108", "Cell")], []),
            new("9", "2025550109", [new ContactPhoneRecord("+12025550109", "+12025550109", "Cell")], []),
            new("10", "2025550110", [new ContactPhoneRecord("+12025550110", "+12025550110", "Cell")], []),
            new("11", "romy  navarro", [new ContactPhoneRecord("+12025550111", "+12025550111", "Cell")], []),
            new("12", "Tessa Lane Mercer", [new ContactPhoneRecord("+12025550112", "+12025550112", "Cell")], [])
        ];

        const string GroupJoke = "I tried making coffee with sparkling water earlier.\r\nIt tasted like a dare but I kept drinking it anyway.\r\n☕😅";
        const string PlaneJoke = "There was a tiny boat carrying a pilot, a doctor, a teacher, and a kid with only three life jackets, and the pilot jumped first until the kid pointed out he had grabbed the picnic bag instead of a jacket, so there were still plenty left.";

        MessageRecord[] messages =
        [
            new(
                "inbox",
                "mom-direct-early",
                "SMS_GSM",
                null,
                "20260308T152732",
                "12025550101",
                "+12025550101",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "Bring the folder please",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("12025550101", ["+12025550101"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "sent",
                "mom-direct-test",
                "SMS_GSM",
                null,
                "20260308T153614",
                "Me",
                "name:Me",
                "+12025550101",
                0,
                0,
                null,
                true,
                true,
                false,
                "test",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("Me", [], [])],
                [new MessageParticipantRecord("mom", ["+12025550101"], [])]),
            new(
                "inbox",
                "group-origin-joke",
                "SMS_GSM",
                null,
                "20260308T161230",
                "Aunt Beth",
                "+12025550104",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                GroupJoke,
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("Aunt Beth", ["+12025550104"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "group-maren-reaction",
                "SMS_GSM",
                null,
                "20260308T161306",
                "2025550107",
                "+12025550107",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                $"Laughed at “{GroupJoke}”",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("2025550107", ["+12025550107"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "group-347-reaction",
                "SMS_GSM",
                null,
                "20260308T162625",
                "2025550110",
                "+12025550110",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                $"Laughed at “{GroupJoke}”",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("2025550110", ["+12025550110"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "mom-group-fu",
                "SMS_GSM",
                null,
                "20260308T163640",
                "12025550101",
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
                [new MessageParticipantRecord("12025550101", ["+12025550101"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "mom-group-meds",
                "SMS_GSM",
                null,
                "20260308T163648",
                "12025550101",
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
                [new MessageParticipantRecord("12025550101", ["+12025550101"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "mom-direct-followup",
                "SMS_GSM",
                null,
                "20260308T164237",
                "12025550101",
                "+12025550101",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "Do u want me to bring it",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("12025550101", ["+12025550101"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "sent",
                "mom-direct-origin",
                "SMS_GSM",
                null,
                "20260308T164417",
                "Me",
                "name:Me",
                "+12025550101",
                0,
                0,
                null,
                true,
                true,
                false,
                "testmap smoke 1",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("Me", [], [])],
                [new MessageParticipantRecord("mom", ["+12025550101"], [])]),
            new(
                "inbox",
                "mom-reacted",
                "SMS_GSM",
                null,
                "20260308T164438",
                "12025550101",
                "+12025550101",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "Disliked “testmap smoke 1”",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("12025550101", ["+12025550101"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "group-romy-reaction",
                "SMS_GSM",
                null,
                "20260308T172459",
                "romy  navarro",
                "+12025550111",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                $"Reacted 😭 to “{GroupJoke}”",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("romy  navarro", ["+12025550111"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "group-347-okay",
                "SMS_GSM",
                null,
                "20260308T172743",
                "2025550109",
                "+12025550109",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "Okay that's a good one 👍",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("2025550109", ["+12025550109"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "group-tessa-reaction",
                "SMS_GSM",
                null,
                "20260308T173009",
                "Tessa Lane Mercer",
                "+12025550112",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                $"Laughed at “{GroupJoke}”",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("Tessa Lane Mercer", ["+12025550112"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "group-plane",
                "SMS_GSM",
                null,
                "20260308T174125",
                "2025550109",
                "+12025550109",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                PlaneJoke,
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("2025550109", ["+12025550109"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "group-plane-reaction",
                "SMS_GSM",
                null,
                "20260308T174240",
                "2025550107",
                "+12025550107",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                $"Laughed at “{PlaneJoke}”",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("2025550107", ["+12025550107"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "grandma-teehee",
                "SMS_GSM",
                null,
                "20260308T174507",
                "2025550108",
                "+12025550108",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "Teehee",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("2025550108", ["+12025550108"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T02:12:10Z"),
                null,
                new NotificationRecord(
                    912,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T02:12:10Z"),
                    "com.apple.MobileSMS",
                    "Aunt Beth",
                    "To you, mom, dad, ellis, Aunt Jean, Nina J. Mercer Esq.",
                    "I tried making coffee with sparkling water earlier.",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(messages, contacts, notifications);

        var directConversation = result.Conversations.Single(conversation => !conversation.IsGroup && conversation.DisplayName == "mom");
        Assert.Contains(
            result.Messages.Where(message => message.ConversationId == directConversation.ConversationId),
            message => message.Message.Handle == "mom-group-fu");
        Assert.Contains(
            result.Messages.Where(message => message.ConversationId == directConversation.ConversationId),
            message => message.Message.Handle == "mom-group-meds");
        Assert.Contains(
            result.Messages.Where(message => message.ConversationId == directConversation.ConversationId),
            message => message.Message.Handle == "mom-reacted");
    }

    [Fact]
    public void Synthesize_KeepsExactLiveMomRowsOutOfFamilyThread()
    {
        ContactRecord[] contacts =
        [
            new("0", "My Number", [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")], []),
            new("1", "mom", [new ContactPhoneRecord("+12025550101", "+12025550101", "Cell")], []),
            new("2", "Aunt Beth", [new ContactPhoneRecord("+12025550104", "+12025550104", "Cell")], []),
            new("3", "Maren", [new ContactPhoneRecord("+12025550107", "+12025550107", "Cell")], []),
            new("4", "2025550110", [new ContactPhoneRecord("+12025550110", "+12025550110", "Cell")], []),
            new("5", "2025550109", [new ContactPhoneRecord("+12025550109", "+12025550109", "Cell")], []),
            new("6", "romy  navarro", [new ContactPhoneRecord("+12025550111", "+12025550111", "Cell")], []),
            new("7", "Tessa Lane Mercer", [new ContactPhoneRecord("+12025550112", "+12025550112", "Cell")], []),
            new("8", "grandma", [new ContactPhoneRecord("+12025550108", "+12025550108", "Cell")], [])
        ];

        const string GroupJoke = "I tried making coffee with sparkling water earlier. \r\nIt tasted like a dare but I kept drinking it anyway.\r\n☕😅";
        const string PlaneJoke = "There was a tiny boat carrying a pilot, a doctor, a teacher, and a kid with only three life jackets, and the pilot jumped first until the kid pointed out he had grabbed the picnic bag instead of a jacket, so there were still plenty left.";

        MessageRecord[] messages =
        [
            new(
                "inbox",
                "ABD975DB22B64D3",
                "SMS_GSM",
                null,
                "20260308T152549",
                "mom",
                "12025550101",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("12025550101", ["12025550101"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "4536DCBEADD4478",
                "SMS_GSM",
                "",
                "20260308T191230",
                "Aunt Beth",
                "2025550104",
                "",
                145,
                0,
                null,
                null,
                null,
                null,
                GroupJoke,
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("2025550104", ["2025550104"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "9D19B7AE376A4AE",
                "SMS_GSM",
                "",
                "20260308T191306",
                "Maren",
                "2025550107",
                "",
                162,
                0,
                null,
                null,
                null,
                null,
                $"Laughed at “{GroupJoke}”",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("2025550107", ["2025550107"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "6824475B05CE40B",
                "SMS_GSM",
                "",
                "20260308T192625",
                "2025550110",
                "2025550110",
                "",
                162,
                0,
                null,
                null,
                null,
                null,
                $"Laughed at “{GroupJoke}”",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("2025550110", ["2025550110"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "19064B0D9F0E433",
                "SMS_GSM",
                "",
                "20260308T192732",
                "mom",
                "12025550101",
                "",
                21,
                0,
                null,
                null,
                null,
                null,
                "Bring the folder please ",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("12025550101", ["12025550101"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "sent",
                "6D69D11227E241E",
                "SMS_GSM",
                "test",
                "20260308T193614",
                "Me",
                "name:Me",
                "+12025550101",
                4,
                0,
                null,
                true,
                true,
                false,
                "test",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("Me", [], [])],
                [new MessageParticipantRecord("+12025550101", ["+12025550101"], [])]),
            new(
                "inbox",
                "DAA94C4E37834A0",
                "SMS_GSM",
                "",
                "20260308T193640",
                "mom",
                "12025550101",
                "",
                2,
                0,
                null,
                null,
                null,
                null,
                "On it",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("12025550101", ["12025550101"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "467C294E0E0540A",
                "SMS_GSM",
                "",
                "20260308T193648",
                "mom",
                "12025550101",
                "",
                4,
                0,
                null,
                null,
                null,
                null,
                "Folder",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("12025550101", ["12025550101"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "75AFE81EE104481",
                "SMS_GSM",
                "",
                "20260308T194237",
                "mom",
                "12025550101",
                "",
                26,
                0,
                null,
                null,
                null,
                null,
                "Do u want me to bring it",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("12025550101", ["12025550101"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "sent",
                "D6CEE803FB5E4F8",
                "SMS_GSM",
                "testmap smoke 1",
                "20260308T194417",
                "Me",
                "name:Me",
                "+12025550101",
                15,
                0,
                null,
                true,
                true,
                false,
                "testmap smoke 1",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("Me", [], [])],
                [new MessageParticipantRecord("+12025550101", ["+12025550101"], [])]),
            new(
                "inbox",
                "32D15D2411734EA",
                "SMS_GSM",
                "",
                "20260308T194438",
                "mom",
                "12025550101",
                "",
                30,
                0,
                null,
                null,
                null,
                null,
                "Disliked “testmap smoke 1”",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("12025550101", ["12025550101"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "B2F80653689148D",
                "SMS_GSM",
                "",
                "20260308T202459",
                "romy  navarro",
                "2025550111",
                "",
                167,
                0,
                null,
                null,
                null,
                null,
                $"Reacted 😭 to “{GroupJoke}”",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("2025550111", ["2025550111"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "36E7A28973AE495",
                "SMS_GSM",
                "",
                "20260308T202743",
                "2025550109",
                "2025550109",
                "",
                27,
                0,
                null,
                null,
                null,
                null,
                "Okay that's a good one 👍",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("2025550109", ["2025550109"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "A670A4FB220A469",
                "SMS_GSM",
                "",
                "20260308T203009",
                "Tessa Lane Mercer",
                "2025550112",
                "",
                162,
                0,
                null,
                null,
                null,
                null,
                $"Laughed at “{GroupJoke}”",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("2025550112", ["2025550112"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "6318DC0C93284A4",
                "SMS_GSM",
                PlaneJoke,
                "20260308T204125",
                "+1 (202) 555-0109",
                "2025550109",
                "",
                504,
                0,
                null,
                false,
                null,
                null,
                PlaneJoke,
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("2025550109", ["2025550109"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "0CF8B0FD119845A",
                "SMS_GSM",
                "",
                "20260308T204240",
                "Maren",
                "2025550107",
                "",
                521,
                0,
                null,
                null,
                null,
                null,
                $"Laughed at “{PlaneJoke}”",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("2025550107", ["2025550107"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])]),
            new(
                "inbox",
                "61D7BF0A6A60468",
                "SMS_GSM",
                "",
                "20260308T204507",
                "grandma",
                "2025550108",
                "",
                6,
                0,
                null,
                null,
                null,
                null,
                "Teehee",
                "SMSGSM",
                "Read",
                [new MessageParticipantRecord("2025550108", ["2025550108"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                true,
                DateTimeOffset.Parse("2026-03-09T00:41:27.6137016+00:00"),
                null,
                new NotificationRecord(
                    100,
                    NotificationEventKind.Added,
                    NotificationEventFlags.NegativeAction,
                    NotificationCategory.Social,
                    42,
                    DateTimeOffset.Parse("2026-03-09T00:41:27.6137016+00:00"),
                    "com.apple.MobileSMS",
                    "+1 (202) 555-0109",
                    "To you, mom, dad, ellis, Aunt Beth, Tessa Lane Mercer, romy  na…",
                    "There was a tiny boat carrying a pilot, a doctor, a teacher, and a kid with only three life jackets, and the pilot jumped first until the kid pointed out he had grabbed the picnic bag instead of a jacket, so there were still plenty left.",
                    "256",
                    "20260308T204125",
                    "",
                    "Clear",
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(messages, contacts, notifications);

        var directConversation = result.Conversations.Single(conversation => !conversation.IsGroup && conversation.DisplayName == "mom");
        Assert.Contains(result.Messages.Where(message => message.ConversationId == directConversation.ConversationId), message => message.Message.Handle == "DAA94C4E37834A0");
        Assert.Contains(result.Messages.Where(message => message.ConversationId == directConversation.ConversationId), message => message.Message.Handle == "467C294E0E0540A");
        Assert.Contains(result.Messages.Where(message => message.ConversationId == directConversation.ConversationId), message => message.Message.Handle == "32D15D2411734EA");
    }

    [Fact]
    public void Synthesize_PrefersMatchingNotificationTimestamp_WhenMapDatetimeDrifts()
    {
        ContactRecord[] contacts =
        [
            new("0", "My Number", [new ContactPhoneRecord("+12025550100", "+12025550100", "Cell")], []),
            new("1", "rowan", [new ContactPhoneRecord("+12025550113", "+12025550113", "Cell")], [])
        ];

        MessageRecord[] messages =
        [
            new(
                "inbox",
                "rowan-map",
                "SMS_GSM",
                null,
                "20260308T215934",
                "rowan",
                "+12025550113",
                "",
                0,
                0,
                null,
                false,
                false,
                false,
                "Been out all day u free around noon",
                "SMSGSM",
                "Unread",
                [new MessageParticipantRecord("rowan", ["+12025550113"], [])],
                [new MessageParticipantRecord("(unnamed)", [], [])])
        ];

        StoredNotificationRecord[] notifications =
        [
            new(
                "device-1",
                false,
                DateTimeOffset.Parse("2026-03-09T01:59:43Z"),
                DateTimeOffset.Parse("2026-03-09T01:59:43Z"),
                new NotificationRecord(
                    13,
                    NotificationEventKind.Added,
                    NotificationEventFlags.None,
                    NotificationCategory.Social,
                    1,
                    DateTimeOffset.Parse("2026-03-09T01:56:47Z"),
                    "com.apple.MobileSMS",
                    "rowan",
                    "",
                    "Been out all day u free around noon",
                    null,
                    null,
                    null,
                    null,
                    new Dictionary<string, string>()))
        ];

        var result = synthesizer.Synthesize(messages, contacts, notifications);
        var message = Assert.Single(result.Messages);
        Assert.Equal(DateTimeOffset.Parse("2026-03-09T01:56:47Z"), message.SortTimestampUtc);
    }
}
