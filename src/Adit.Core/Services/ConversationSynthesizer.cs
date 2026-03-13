using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using Adit.Core.Models;
using Adit.Core.Utilities;

namespace Adit.Core.Services;

public sealed class ConversationSynthesizer
{
    public ConversationSynthesisResult Synthesize(
        IEnumerable<MessageRecord> messages,
        IEnumerable<ContactRecord> contacts,
        IEnumerable<StoredNotificationRecord>? notifications = null)
    {
        var contactIndex = BuildContactIndex(contacts);
        var selfIdentity = BuildSelfIdentity(contacts);
        var notificationHints = BuildNotificationHints(notifications ?? Array.Empty<StoredNotificationRecord>());
        var synthesizedMapMessages = messages
            .Select(message => SynthesizeMessage(message, contactIndex, selfIdentity, notificationHints))
            .ToArray();
        var deferredDirectMessageKeys = synthesizedMapMessages
            .Where(item => item.DeferredDirectIdentity)
            .Select(item => item.Message.MessageKey)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        var matchedHintIds = synthesizedMapMessages
            .Where(item => item.MatchedHint is not null)
            .Select(item => item.MatchedHint!.NotificationUid)
            .ToHashSet();
        var synthesizedMessages = synthesizedMapMessages
            .Select(item => item.Message)
            .Concat(BuildShadowMessages(notificationHints, matchedHintIds, contactIndex, selfIdentity))
            .ToArray();
        synthesizedMessages = CoalesceConversationMessages(synthesizedMessages, notificationHints, deferredDirectMessageKeys)
            .OrderByDescending(message => message.SortTimestampUtc ?? DateTimeOffset.MinValue)
            .ThenByDescending(message => !IsNotificationBacked(message.Message))
            .ThenByDescending(message => message.Message.Datetime ?? string.Empty, StringComparer.OrdinalIgnoreCase)
            .ThenBy(message => message.MessageKey, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var conversations = synthesizedMessages
            .GroupBy(message => message.ConversationId, StringComparer.OrdinalIgnoreCase)
            .Select(group => BuildConversation(group.Key, group.ToArray(), selfIdentity))
            .OrderByDescending(conversation => conversation.LastMessageUtc ?? DateTimeOffset.MinValue)
            .ThenByDescending(conversation => conversation.MessageCount)
            .ToArray();

        return new ConversationSynthesisResult(selfIdentity.Phones, synthesizedMessages, conversations);
    }

    private static SynthesizedMapMessage SynthesizeMessage(
        MessageRecord message,
        ContactIndex contactIndex,
        SelfIdentity selfIdentity,
        IReadOnlyList<NotificationConversationHint> notificationHints)
    {
        var direction = InferDirection(message, selfIdentity);
        var participants = ExtractParticipants(message, contactIndex, selfIdentity, direction);
        var conversationParticipants = SelectConversationParticipants(participants, selfIdentity);
        var matchedHint = FindBestNotificationHint(message, conversationParticipants, direction, notificationHints);
        conversationParticipants = EnrichConversationParticipants(
            conversationParticipants,
            matchedHint,
            contactIndex,
            selfIdentity);
        var projectedMessage = ApplyNotificationHintProjection(message, matchedHint);
        projectedMessage = ApplyResolvedSenderIdentity(projectedMessage, conversationParticipants, selfIdentity);
        var participantKeys = conversationParticipants
            .Select(participant => participant.Key)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(key => key, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var fallbackIdentity = string.Join(
            "|",
            new[]
            {
                message.Handle,
                direction.ToString(),
                message.Datetime,
                NormalizeAddressForIdentity(message.SenderAddressing),
                NormalizeAddressForIdentity(message.RecipientAddressing),
                message.Subject,
                message.Body
            }.Where(value => !string.IsNullOrWhiteSpace(value)));

        var nonSelfParticipants = conversationParticipants
            .Where(participant => !participant.IsSelf)
            .ToArray();
        var isGroup = matchedHint?.IsGroupHint == true
            || nonSelfParticipants.Length > 1;

        var deferredDirectIdentity = false;
        string conversationSeed;
        if (conversationParticipants.Count == 1 && conversationParticipants[0].IsSelf)
        {
            conversationSeed = "self";
        }
        else if (matchedHint is { IsGroupHint: true, ThreadTitleNormalized.Length: > 0 })
        {
            conversationSeed = $"group:{matchedHint.GroupSeedNormalized ?? matchedHint.ThreadTitleNormalized}";
        }
        else if (!isGroup
                 && nonSelfParticipants.Length == 1
                 && ShouldDeferDirectConversationIdentity(projectedMessage, conversationParticipants, direction, matchedHint))
        {
            deferredDirectIdentity = true;
            conversationSeed = $"deferred-direct:{ComputeDeferredDirectConversationSeed(projectedMessage, nonSelfParticipants[0])}";
        }
        else
        {
            conversationSeed = participantKeys.Length > 0 ? string.Join("|", participantKeys) : fallbackIdentity;
        }

        if (string.IsNullOrWhiteSpace(conversationSeed))
        {
            conversationSeed = Guid.NewGuid().ToString("N");
        }
        var conversationId = $"conv_{Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(conversationSeed))).ToLowerInvariant()}";
        var displayName = BuildConversationDisplayName(
            conversationParticipants,
            projectedMessage,
            isGroup,
            selfIdentity,
            matchedHint);

        return new SynthesizedMapMessage(
            new SynthesizedMessageRecord(
                ComputeMessageKey(message),
                conversationId,
                displayName,
                isGroup,
                ChooseSortTimestamp(projectedMessage, matchedHint),
                conversationParticipants,
                projectedMessage),
            matchedHint,
            deferredDirectIdentity);
    }

    private static ConversationSnapshot BuildConversation(
        string conversationId,
        IReadOnlyList<SynthesizedMessageRecord> messages,
        SelfIdentity selfIdentity)
    {
        var orderedMessages = messages
            .OrderByDescending(message => message.SortTimestampUtc ?? DateTimeOffset.MinValue)
            .ThenByDescending(message => !IsNotificationBacked(message.Message))
            .ThenByDescending(message => message.Message.Datetime ?? string.Empty, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var latest = orderedMessages[0];
        var latestPreviewMessage = orderedMessages
            .FirstOrDefault(message => !string.IsNullOrWhiteSpace(BuildPreview(message.Message)))
            ?? latest;
        var unreadCount = orderedMessages.Count(item => IsUnread(item.Message));
        var mergedParticipants = MergeParticipants(
            Array.Empty<ConversationParticipantRecord>(),
            orderedMessages.SelectMany(message => message.Participants));
        var isGroup = orderedMessages.Any(message => message.IsGroup)
            || mergedParticipants.Count(participant => !participant.IsSelf) > 1;
        var sourceFolders = orderedMessages
            .Select(item => item.Message.Folder)
            .Where(folder => !string.IsNullOrWhiteSpace(folder))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(folder => folder, StringComparer.OrdinalIgnoreCase)
            .ToArray()!;

        return new ConversationSnapshot(
            conversationId,
            latest.ConversationDisplayName,
            isGroup,
            latest.SortTimestampUtc,
            orderedMessages.Length,
            unreadCount,
            BuildPreview(latestPreviewMessage.Message),
            mergedParticipants,
            sourceFolders,
            ResolveSenderDisplayName(latestPreviewMessage.Message, mergedParticipants, selfIdentity));
    }

    public static string ComputeMessageKey(MessageRecord message)
    {
        if (!string.IsNullOrWhiteSpace(message.Handle))
        {
            return message.Handle!;
        }

        var seed = string.Join(
            "|",
            new[]
            {
                message.Datetime,
                NormalizeAddressForIdentity(message.SenderAddressing),
                NormalizeAddressForIdentity(message.RecipientAddressing),
                message.Subject,
                message.Body,
                message.MessageType,
                message.Type
            });

        return $"anon_{Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(seed))).ToLowerInvariant()}";
    }

    private static IReadOnlyList<ConversationParticipantRecord> SelectConversationParticipants(
        IReadOnlyList<MutableParticipant> participants,
        SelfIdentity selfIdentity)
    {
        var counterparties = participants
            .Where(IsConversationCounterparty)
            .Select(ToConversationParticipant)
            .OrderBy(participant => participant.DisplayName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(participant => participant.Key, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (counterparties.Length > 0)
        {
            return counterparties;
        }

        var selfParticipants = participants.Where(participant => participant.IsSelf).ToArray();
        if (selfParticipants.Length == 0)
        {
            return participants
                .Select(ToConversationParticipant)
                .OrderBy(participant => participant.DisplayName, StringComparer.OrdinalIgnoreCase)
                .ThenBy(participant => participant.Key, StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }

        return [CollapseSelfParticipants(selfParticipants, selfIdentity)];
    }

    private static bool IsConversationCounterparty(MutableParticipant participant)
    {
        if (participant.IsSelf)
        {
            return false;
        }

        return participant.InConversationScope
            || (!participant.HasSenderSideRole && !participant.HasRecipientSideRole);
    }

    private static ConversationParticipantRecord CollapseSelfParticipants(
        IReadOnlyList<MutableParticipant> participants,
        SelfIdentity selfIdentity)
    {
        var phones = participants
            .SelectMany(participant => participant.Phones)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(phone => phone, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var emails = participants
            .SelectMany(participant => participant.Emails)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(email => email, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var displayName = !string.IsNullOrWhiteSpace(selfIdentity.DisplayName)
            ? selfIdentity.DisplayName
            : "Me";

        return new ConversationParticipantRecord(
            "self",
            displayName,
            phones,
            emails,
            true);
    }

    private static ConversationParticipantRecord ToConversationParticipant(MutableParticipant participant)
    {
        return new ConversationParticipantRecord(
            participant.Key,
            participant.DisplayName,
            participant.Phones.OrderBy(phone => phone, StringComparer.OrdinalIgnoreCase).ToArray(),
            participant.Emails.OrderBy(email => email, StringComparer.OrdinalIgnoreCase).ToArray(),
            participant.IsSelf);
    }

    // A sparse inbound row with one visible counterparty is still ambiguous between a direct thread
    // and a collapsed group row. Direct ANCS titles do not remove that ambiguity because they only
    // label the thread, not the sender.
    private static bool ShouldDeferDirectConversationIdentity(
        MessageRecord message,
        IReadOnlyList<ConversationParticipantRecord> participants,
        MessageDirection direction,
        NotificationConversationHint? hint)
    {
        if (direction != MessageDirection.Inbound)
        {
            return false;
        }

        if (participants.Count(participant => !participant.IsSelf) != 1)
        {
            return false;
        }

        if (HasExplicitSenderEvidence(message))
        {
            return false;
        }

        if (!RecipientsLackParticipantIdentity(message))
        {
            return false;
        }

        return hint is not { IsGroupHint: true };
    }

    private static string ComputeDeferredDirectConversationSeed(
        MessageRecord message,
        ConversationParticipantRecord participant)
    {
        return string.Join(
            "|",
            new[]
            {
                participant.Key,
                ComputeMessageKey(message)
            }.Where(value => !string.IsNullOrWhiteSpace(value)));
    }

    private static IReadOnlyList<MutableParticipant> ExtractParticipants(
        MessageRecord message,
        ContactIndex contactIndex,
        SelfIdentity selfIdentity,
        MessageDirection direction)
    {
        var participants = new Dictionary<string, MutableParticipant>(StringComparer.OrdinalIgnoreCase);

        foreach (var participant in message.Originators)
        {
            AddParticipant(
                participants,
                participant.Phones,
                participant.Emails,
                participant.Name,
                contactIndex,
                selfIdentity,
                ParticipantRole.Originator,
                direction);
        }

        foreach (var participant in message.Recipients)
        {
            AddParticipant(
                participants,
                participant.Phones,
                participant.Emails,
                participant.Name,
                contactIndex,
                selfIdentity,
                ParticipantRole.Recipient,
                direction);
        }

        AddAddressParticipant(
            participants,
            message.SenderAddressing,
            message.SenderName,
            contactIndex,
            selfIdentity,
            ParticipantRole.SenderAddressing,
            direction);
        AddAddressParticipant(
            participants,
            message.RecipientAddressing,
            null,
            contactIndex,
            selfIdentity,
            ParticipantRole.RecipientAddressing,
            direction);

        return participants.Values.ToArray();
    }

    private static void AddAddressParticipant(
        Dictionary<string, MutableParticipant> participants,
        string? address,
        string? displayName,
        ContactIndex contactIndex,
        SelfIdentity selfIdentity,
        ParticipantRole role,
        MessageDirection direction)
    {
        if (string.IsNullOrWhiteSpace(address))
        {
            return;
        }

        var trimmedAddress = address.Trim();
        if (trimmedAddress.StartsWith("name:", StringComparison.OrdinalIgnoreCase))
        {
            var rawName = StripAddressPrefix(trimmedAddress);
            var normalizedName = NormalizeComparableText(rawName);
            var key = !string.IsNullOrWhiteSpace(normalizedName)
                ? $"name:{normalizedName}"
                : $"name:{rawName.Trim().ToLowerInvariant()}";
            var participant = GetOrAddParticipant(
                participants,
                key,
                displayName ?? rawName,
                null,
                null,
                null,
                contactIndex);
            MarkParticipantRoles(participant, role, direction);
            if (!participant.IsSelf && NameLooksSelf(displayName ?? rawName, selfIdentity))
            {
                participant.IsSelf = true;
            }

            return;
        }

        if (TryExtractEmail(address, out var email))
        {
            AddParticipant(
                participants,
                Array.Empty<string?>(),
                [email],
                displayName,
                contactIndex,
                selfIdentity,
                role,
                direction);
            return;
        }

        AddParticipant(
            participants,
            [StripAddressPrefix(address)],
            Array.Empty<string?>(),
            displayName,
            contactIndex,
            selfIdentity,
            role,
            direction);
    }

    private static void AddParticipant(
        Dictionary<string, MutableParticipant> participants,
        IEnumerable<string?> phones,
        IEnumerable<string?> emails,
        string? displayName,
        ContactIndex contactIndex,
        SelfIdentity selfIdentity,
        ParticipantRole role,
        MessageDirection direction)
    {
        foreach (var phone in phones)
        {
            if (string.IsNullOrWhiteSpace(phone))
            {
                continue;
            }

            if (TryExtractEmail(phone, out var phoneEmail))
            {
                AddParticipant(
                    participants,
                    Array.Empty<string?>(),
                    [phoneEmail],
                    displayName,
                    contactIndex,
                    selfIdentity,
                    role,
                    direction);
                continue;
            }

            var cleanedPhone = StripAddressPrefix(phone);
            var normalized = PhoneNumberNormalizer.Normalize(cleanedPhone);
            var key = !string.IsNullOrWhiteSpace(normalized)
                ? $"phone:{normalized}"
                : $"raw:{cleanedPhone.Trim().ToLowerInvariant()}";
            var participant = GetOrAddParticipant(
                participants,
                key,
                displayName,
                cleanedPhone,
                normalized,
                null,
                contactIndex);
            participant.Phones.Add(!string.IsNullOrWhiteSpace(normalized) ? normalized! : cleanedPhone.Trim());
            MarkParticipantRoles(participant, role, direction);
            if (!participant.IsSelf && !string.IsNullOrWhiteSpace(normalized) && selfIdentity.PhoneSet.Contains(normalized!))
            {
                participant.IsSelf = true;
            }
        }

        foreach (var email in emails)
        {
            if (string.IsNullOrWhiteSpace(email))
            {
                continue;
            }

            var trimmed = StripAddressPrefix(email).Trim();
            var key = $"email:{trimmed.ToLowerInvariant()}";
            var participant = GetOrAddParticipant(
                participants,
                key,
                displayName,
                null,
                null,
                trimmed,
                contactIndex);
            participant.Emails.Add(trimmed);
            MarkParticipantRoles(participant, role, direction);
            if (!participant.IsSelf && selfIdentity.EmailSet.Contains(trimmed.ToLowerInvariant()))
            {
                participant.IsSelf = true;
            }
        }
    }

    private static MutableParticipant GetOrAddParticipant(
        Dictionary<string, MutableParticipant> participants,
        string key,
        string? displayName,
        string? rawPhone,
        string? normalizedPhone,
        string? email,
        ContactIndex contactIndex)
    {
        if (!participants.TryGetValue(key, out var participant))
        {
            participant = new MutableParticipant(
                key,
                ResolveDisplayName(displayName, normalizedPhone, email, rawPhone, contactIndex));
            participants[key] = participant;
            return participant;
        }

        if (string.IsNullOrWhiteSpace(participant.DisplayName)
            || participant.DisplayName == "(unknown)")
        {
            participant.DisplayName = ResolveDisplayName(displayName, normalizedPhone, email, rawPhone, contactIndex);
        }

        return participant;
    }

    private static void MarkParticipantRoles(
        MutableParticipant participant,
        ParticipantRole role,
        MessageDirection direction)
    {
        switch (role)
        {
            case ParticipantRole.Originator:
            case ParticipantRole.SenderAddressing:
                participant.HasSenderSideRole = true;
                if (direction == MessageDirection.Inbound)
                {
                    participant.InConversationScope = true;
                }
                else if (direction == MessageDirection.Outbound)
                {
                    participant.IsSelf = true;
                }
                break;
            case ParticipantRole.Recipient:
            case ParticipantRole.RecipientAddressing:
                participant.HasRecipientSideRole = true;
                if (direction == MessageDirection.Outbound || direction == MessageDirection.Inbound)
                {
                    participant.InConversationScope = true;
                }
                break;
        }
    }

    private static string ResolveDisplayName(
        string? candidateName,
        string? normalizedPhone,
        string? email,
        string? rawPhone,
        ContactIndex contactIndex)
    {
        if (!string.IsNullOrWhiteSpace(normalizedPhone)
            && contactIndex.NameByPhone.TryGetValue(normalizedPhone, out var contactName))
        {
            return contactName;
        }

        if (!string.IsNullOrWhiteSpace(email)
            && contactIndex.NameByEmail.TryGetValue(email.ToLowerInvariant(), out contactName))
        {
            return contactName;
        }

        if (!string.IsNullOrWhiteSpace(candidateName) && candidateName != "(unnamed)")
        {
            return candidateName.Trim();
        }

        if (!string.IsNullOrWhiteSpace(normalizedPhone))
        {
            return normalizedPhone;
        }

        if (!string.IsNullOrWhiteSpace(email))
        {
            return email;
        }

        if (!string.IsNullOrWhiteSpace(rawPhone))
        {
            return rawPhone.Trim();
        }

        return "(unknown)";
    }

    private static string BuildConversationDisplayName(
        IReadOnlyList<ConversationParticipantRecord> participants,
        MessageRecord message,
        bool isGroup,
        SelfIdentity selfIdentity,
        NotificationConversationHint? hint)
    {
        if (hint is { IsGroupHint: true } && !string.IsNullOrWhiteSpace(hint.ThreadTitle))
        {
            var descriptor = ParseGroupDescriptor(hint.ThreadTitle);
            if (hint.GroupParticipantNames.Count > 1
                && LooksLikeExplicitParticipantDescriptor(hint.ThreadTitle)
                && !descriptor.HasOthersCount
                && !descriptor.WasTruncated)
            {
                var descriptorDisplayName = BuildGroupDisplayName(
                    hint.GroupParticipantNames,
                    participants);
                if (!string.IsNullOrWhiteSpace(descriptorDisplayName))
                {
                    return descriptorDisplayName!;
                }
            }

            return hint.ThreadTitle;
        }

        if (participants.Count == 0)
        {
            if (hint is { ThreadTitle.Length: > 0 })
            {
                return hint.ThreadTitle;
            }

            return !string.IsNullOrWhiteSpace(message.SenderName)
                ? message.SenderName!
                : !string.IsNullOrWhiteSpace(message.SenderAddressing)
                    ? message.SenderAddressing!
                    : "(unknown conversation)";
        }

        if (participants.Count == 1 && participants[0].IsSelf)
        {
            return !string.IsNullOrWhiteSpace(selfIdentity.DisplayName)
                ? selfIdentity.DisplayName
                : "Me";
        }

        if (!isGroup)
        {
            if (hint is { IsGroupHint: false, ThreadTitle.Length: > 0 }
                && LooksLikeUnresolvedDisplayName(participants[0].DisplayName))
            {
                return hint.ThreadTitle;
            }

            return participants[0].DisplayName;
        }

        var names = participants
            .Select(participant => participant.DisplayName)
            .Where(name => !string.IsNullOrWhiteSpace(name))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (names.Length <= 3)
        {
            return string.Join(", ", names);
        }

        return $"{string.Join(", ", names.Take(3))} +{names.Length - 3}";
    }

    private static string? BuildPreview(MessageRecord message)
    {
        var body = string.IsNullOrWhiteSpace(message.Body) ? message.Subject : message.Body;
        return BuildPreview(body);
    }

    private static MessageRecord ApplyNotificationHintProjection(
        MessageRecord message,
        NotificationConversationHint? hint)
    {
        if (hint is null)
        {
            return message;
        }

        var senderName = message.SenderName;
        if (hint.IsGroupHint
            && LooksLikeUnresolvedDisplayName(senderName ?? string.Empty)
            && !string.IsNullOrWhiteSpace(hint.AuthorDisplayName))
        {
            senderName = hint.AuthorDisplayName;
        }

        if (string.IsNullOrWhiteSpace(hint.Preview)
            || (!string.IsNullOrWhiteSpace(message.Body) || !string.IsNullOrWhiteSpace(message.Subject)))
        {
            return senderName == message.SenderName
                ? message
                : message with { SenderName = senderName };
        }

        return message with
        {
            SenderName = senderName,
            Subject = string.IsNullOrWhiteSpace(message.Subject) ? hint.Preview : message.Subject,
            Body = string.IsNullOrWhiteSpace(message.Body) ? hint.Preview : message.Body
        };
    }

    private static MessageRecord ApplyResolvedSenderIdentity(
        MessageRecord message,
        IReadOnlyList<ConversationParticipantRecord> participants,
        SelfIdentity selfIdentity)
    {
        if (!LooksLikeUnresolvedDisplayName(message.SenderName ?? string.Empty))
        {
            return message;
        }

        var resolvedSender = ResolveSenderDisplayName(message, participants, selfIdentity);
        if (string.IsNullOrWhiteSpace(resolvedSender))
        {
            return message;
        }

        return message with { SenderName = resolvedSender };
    }

    private static DateTimeOffset? ChooseSortTimestamp(
        MessageRecord message,
        NotificationConversationHint? hint)
    {
        var messageTimestamp = ParseMapTimestamp(message.Datetime);
        if (hint is null)
        {
            return messageTimestamp;
        }

        var hintTimestamp = hint.ObservedUtc;
        if (!messageTimestamp.HasValue)
        {
            return hintTimestamp;
        }

        var preview = BuildPreview(message);
        var exactPreviewMatch =
            !string.IsNullOrWhiteSpace(preview)
            && PreviewsLikelyMatch(preview, hint.Preview)
            && PreviewsLikelyMatch(hint.Preview, preview);
        var previewMatchesHint =
            string.IsNullOrWhiteSpace(preview)
            || PreviewsLikelyMatch(preview, hint.Preview)
            || ReactionSemanticsLikelyMatch(preview, hint.Preview);
        if (!previewMatchesHint)
        {
            return messageTimestamp;
        }

        var delta = (messageTimestamp.Value - hintTimestamp).Duration();
        if (exactPreviewMatch && delta <= TimeSpan.FromMinutes(5))
        {
            return hintTimestamp;
        }

        if (delta <= TimeSpan.FromMinutes(10))
        {
            return messageTimestamp;
        }

        if (!HasStrongPreviewIdentity(preview) && delta > TimeSpan.FromMinutes(5))
        {
            return messageTimestamp;
        }

        if (LooksLikeWholeHourTimestampSkew(messageTimestamp.Value, hintTimestamp))
        {
            return hintTimestamp;
        }

        return messageTimestamp;
    }

    private static bool IsUnread(MessageRecord message)
    {
        if (string.Equals(message.Type, "ANCS", StringComparison.OrdinalIgnoreCase)
            || string.Equals(message.MessageType, "ANCS", StringComparison.OrdinalIgnoreCase))
        {
            return message.Read != true
                && !string.Equals(message.Status, "Read", StringComparison.OrdinalIgnoreCase);
        }

        return string.Equals(message.Folder, "inbox", StringComparison.OrdinalIgnoreCase)
            && (message.Read == false
                || string.Equals(message.Status, "Unread", StringComparison.OrdinalIgnoreCase));
    }

    private static string? BuildPreview(string? body)
    {
        if (string.IsNullOrWhiteSpace(body))
        {
            return null;
        }

        body = body.Trim();
        return body.Length <= 160 ? body : $"{body[..160]}...";
    }

    private static DateTimeOffset? ParseMapTimestamp(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        return DateTimeOffset.TryParseExact(
            value,
            "yyyyMMdd'T'HHmmss",
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeLocal,
            out var parsed)
            ? parsed.ToUniversalTime()
            : null;
    }

    private static bool HasStrongPreviewIdentity(string? preview)
    {
        var normalized = NormalizeComparableText(preview);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return false;
        }

        var tokens = normalized
            .Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (tokens.Length >= 3)
        {
            return true;
        }

        if (normalized.Length >= 18)
        {
            return true;
        }

        return tokens.Length >= 2
            && tokens.All(token => token.Length >= 4)
            && normalized.Length >= 10;
    }

    private static bool LooksLikeWholeHourTimestampSkew(
        DateTimeOffset left,
        DateTimeOffset right)
    {
        var deltaMinutes = Math.Abs((left - right).TotalMinutes);
        if (deltaMinutes < 55 || deltaMinutes > (12 * 60) + 5)
        {
            return false;
        }

        var roundedHours = Math.Round(deltaMinutes / 60d, MidpointRounding.AwayFromZero);
        if (roundedHours < 1 || roundedHours > 12)
        {
            return false;
        }

        return Math.Abs(deltaMinutes - (roundedHours * 60d)) <= 5d;
    }

    private static MessageDirection InferDirection(MessageRecord message, SelfIdentity selfIdentity)
    {
        if (message.Sent == true
            || string.Equals(message.Folder, "sent", StringComparison.OrdinalIgnoreCase)
            || string.Equals(message.Folder, "outbox", StringComparison.OrdinalIgnoreCase))
        {
            return MessageDirection.Outbound;
        }

        var senderIsSelf = AddressMatchesSelf(message.SenderAddressing, selfIdentity)
            || ParticipantCollectionHasSelf(message.Originators, selfIdentity)
            || NameLooksSelf(message.SenderName, selfIdentity);
        var recipientIsSelf = AddressMatchesSelf(message.RecipientAddressing, selfIdentity)
            || ParticipantCollectionHasSelf(message.Recipients, selfIdentity);

        if (senderIsSelf && !recipientIsSelf)
        {
            return MessageDirection.Outbound;
        }

        if (recipientIsSelf && !senderIsSelf)
        {
            return MessageDirection.Inbound;
        }

        if (string.Equals(message.Folder, "inbox", StringComparison.OrdinalIgnoreCase)
            || message.Sent == false)
        {
            return MessageDirection.Inbound;
        }

        return MessageDirection.Unknown;
    }

    private static bool AddressMatchesSelf(string? address, SelfIdentity selfIdentity)
    {
        if (string.IsNullOrWhiteSpace(address))
        {
            return false;
        }

        if (TryExtractEmail(address, out var email))
        {
            return selfIdentity.EmailSet.Contains(email);
        }

        var normalizedPhone = PhoneNumberNormalizer.Normalize(StripAddressPrefix(address));
        return !string.IsNullOrWhiteSpace(normalizedPhone)
            && selfIdentity.PhoneSet.Contains(normalizedPhone);
    }

    private static bool ParticipantCollectionHasSelf(
        IReadOnlyList<MessageParticipantRecord> participants,
        SelfIdentity selfIdentity)
    {
        foreach (var participant in participants)
        {
            if (participant.Phones.Any(phone =>
                !string.IsNullOrWhiteSpace(phone)
                && selfIdentity.PhoneSet.Contains(
                    PhoneNumberNormalizer.Normalize(StripAddressPrefix(phone)) ?? phone)))
            {
                return true;
            }

            if (participant.Emails.Any(email =>
                !string.IsNullOrWhiteSpace(email)
                && selfIdentity.EmailSet.Contains(StripAddressPrefix(email).Trim().ToLowerInvariant())))
            {
                return true;
            }

            if (NameLooksSelf(participant.Name, selfIdentity))
            {
                return true;
            }
        }

        return false;
    }

    private static bool NameLooksSelf(string? candidateName, SelfIdentity selfIdentity)
    {
        if (string.IsNullOrWhiteSpace(candidateName))
        {
            return false;
        }

        var normalizedCandidate = NormalizeComparableText(candidateName);
        if (string.IsNullOrWhiteSpace(normalizedCandidate))
        {
            return false;
        }

        var normalizedSelf = NormalizeComparableText(selfIdentity.DisplayName);
        if (!string.IsNullOrWhiteSpace(normalizedSelf)
            && string.Equals(normalizedCandidate, normalizedSelf, StringComparison.Ordinal))
        {
            return true;
        }

        return string.Equals(normalizedCandidate, "my number", StringComparison.Ordinal)
            || string.Equals(normalizedCandidate, "me", StringComparison.Ordinal);
    }

    private static bool IsSelfContact(ContactRecord contact)
    {
        return string.Equals(contact.DisplayName, "My Number", StringComparison.OrdinalIgnoreCase)
            || string.Equals(contact.UniqueIdentifier, "0", StringComparison.OrdinalIgnoreCase);
    }

    private static SelfIdentity BuildSelfIdentity(IEnumerable<ContactRecord> contacts)
    {
        var selfContacts = contacts.Where(IsSelfContact).ToArray();
        var phones = selfContacts
            .SelectMany(contact => contact.Phones)
            .Select(phone => phone.Normalized)
            .Where(phone => !string.IsNullOrWhiteSpace(phone))
            .Select(phone => phone!)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(phone => phone, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var emails = selfContacts
            .SelectMany(contact => contact.Emails)
            .Where(email => !string.IsNullOrWhiteSpace(email))
            .Select(email => email.Trim().ToLowerInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(email => email, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var displayName = selfContacts
            .Select(contact => contact.DisplayName)
            .FirstOrDefault(name => !string.IsNullOrWhiteSpace(name));

        return new SelfIdentity(
            phones,
            emails,
            displayName ?? "Me",
            phones.ToHashSet(StringComparer.OrdinalIgnoreCase),
            emails.ToHashSet(StringComparer.OrdinalIgnoreCase));
    }

    private static ContactIndex BuildContactIndex(IEnumerable<ContactRecord> contacts)
    {
        var byPhone = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var byEmail = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var byComparableName = new Dictionary<string, List<ContactRecord>>(StringComparer.OrdinalIgnoreCase);

        foreach (var contact in contacts)
        {
            foreach (var phone in contact.Phones)
            {
                if (!string.IsNullOrWhiteSpace(phone.Normalized))
                {
                    byPhone[phone.Normalized!] = contact.DisplayName;
                }
            }

            foreach (var email in contact.Emails)
            {
                if (!string.IsNullOrWhiteSpace(email))
                {
                    byEmail[email.Trim().ToLowerInvariant()] = contact.DisplayName;
                }
            }

            var normalizedName = NormalizeComparableText(contact.DisplayName);
            if (!string.IsNullOrWhiteSpace(normalizedName))
            {
                if (!byComparableName.TryGetValue(normalizedName, out var matches))
                {
                    matches = [];
                    byComparableName[normalizedName] = matches;
                }

                matches.Add(contact);
            }
        }

        return new ContactIndex(
            byPhone,
            byEmail,
            byComparableName.ToDictionary(
                pair => pair.Key,
                pair => (IReadOnlyList<ContactRecord>)pair.Value.ToArray(),
                StringComparer.OrdinalIgnoreCase));
    }

    private static IReadOnlyList<NotificationConversationHint> BuildNotificationHints(
        IEnumerable<StoredNotificationRecord> notifications)
    {
        return notifications
            .Where(IsMessagesNotification)
            .OrderByDescending(notification => notification.IsActive)
            .ThenByDescending(notification => notification.UpdatedAtUtc)
            .ThenByDescending(notification => notification.Notification.NotificationUid)
            .Select(notification => (Notification: notification, Hint: BuildNotificationHint(notification)))
            .Where(item => item.Hint is not null)
            .GroupBy(
                item => BuildNotificationHintDedupKey(item.Hint!),
                StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First().Hint!)
            .OrderByDescending(hint => hint.ObservedUtc)
            .ThenByDescending(hint => hint.NotificationUid)
            .ToArray();
    }

    private static string BuildNotificationHintDedupKey(NotificationConversationHint hint)
    {
        return string.Join(
            "|",
            hint.IsGroupHint ? "group" : "direct",
            hint.ThreadTitleNormalized,
            hint.AuthorDisplayNameNormalized ?? string.Empty,
            hint.GroupSeedNormalized ?? string.Empty,
            hint.PreviewNormalized,
            hint.ObservedUtc.ToString("O", CultureInfo.InvariantCulture));
    }

    private static NotificationConversationHint? BuildNotificationHint(StoredNotificationRecord notification)
    {
        var preview = BuildPreview(notification.Notification.Message);
        var previewNormalized = NormalizeComparableText(preview);
        if (string.IsNullOrWhiteSpace(previewNormalized))
        {
            return null;
        }

        var title = notification.Notification.Title?.Trim();
        var subtitle = notification.Notification.Subtitle?.Trim();
        if (string.IsNullOrWhiteSpace(title) && string.IsNullOrWhiteSpace(subtitle))
        {
            return null;
        }

        var previewActor = TryExtractNotificationPreviewActor(preview);
        var titleIsDescriptor = LooksLikeGroupDescriptor(title);
        var subtitleIsDescriptor = LooksLikeGroupDescriptor(subtitle);
        var isGroupHint = false;
        var threadTitle = title;
        string? authorDisplayName = title;
        IReadOnlyList<string> groupParticipantNames = Array.Empty<string>();

        if (subtitleIsDescriptor && !NamesLikelyMatch(title, subtitle))
        {
            isGroupHint = true;
            threadTitle = subtitle!;
            authorDisplayName = !LooksLikeGroupDescriptor(title) ? title : previewActor;
        }
        else if (titleIsDescriptor && !string.IsNullOrWhiteSpace(title))
        {
            isGroupHint = true;
            threadTitle = title;
            authorDisplayName = !string.IsNullOrWhiteSpace(subtitle) && !LooksLikeGroupDescriptor(subtitle)
                ? subtitle
                : previewActor;
        }
        else
        {
            threadTitle = title ?? subtitle!;
            authorDisplayName = title ?? subtitle;
        }

        if (isGroupHint)
        {
            groupParticipantNames = LooksLikeExplicitParticipantDescriptor(threadTitle)
                ? ExtractGroupDescriptorDisplayNames(threadTitle!)
                : Array.Empty<string>();
            if (LooksLikeGroupDescriptor(authorDisplayName))
            {
                authorDisplayName = previewActor;
            }
        }

        var threadTitleNormalized = NormalizeComparableText(threadTitle);
        if (string.IsNullOrWhiteSpace(threadTitleNormalized))
        {
            return null;
        }

        var authorDisplayNameNormalized = NormalizeComparableText(authorDisplayName);

        var observedUtc = ParseMapTimestamp(notification.Notification.Date) ?? notification.Notification.ReceivedAtUtc;

        return new NotificationConversationHint(
            notification.Notification.NotificationUid,
            notification.IsActive,
            observedUtc,
            threadTitle,
            threadTitleNormalized,
            authorDisplayName,
            authorDisplayNameNormalized,
            BuildGroupSeedNormalized(threadTitle, authorDisplayName, isGroupHint),
            groupParticipantNames,
            preview!,
            previewNormalized!,
            isGroupHint);
    }

    private static NotificationConversationHint? FindBestNotificationHint(
        MessageRecord message,
        IReadOnlyList<ConversationParticipantRecord> participants,
        MessageDirection direction,
        IReadOnlyList<NotificationConversationHint> notificationHints)
    {
        if (notificationHints.Count == 0 || direction == MessageDirection.Outbound)
        {
            return null;
        }

        var preview = BuildPreview(message);
        var previewNormalized = NormalizeComparableText(preview);
        var sortUtc = ParseMapTimestamp(message.Datetime);
        var bestHint = default(NotificationConversationHint);
        var bestScore = int.MinValue;

        foreach (var hint in notificationHints)
        {
            var score = ScoreNotificationHint(message, participants, direction, previewNormalized!, sortUtc, hint);
            if (score > bestScore)
            {
                bestScore = score;
                bestHint = hint;
            }
        }

        var threshold = string.IsNullOrWhiteSpace(previewNormalized) ? 8 : 10;
        return bestScore >= threshold ? bestHint : null;
    }

    private static IReadOnlyList<ConversationParticipantRecord> EnrichConversationParticipants(
        IReadOnlyList<ConversationParticipantRecord> participants,
        NotificationConversationHint? hint,
        ContactIndex contactIndex,
        SelfIdentity selfIdentity)
    {
        if (hint is not { IsGroupHint: true } || hint.GroupParticipantNames.Count == 0)
        {
            return participants;
        }

        var enriched = participants.ToDictionary(participant => participant.Key, StringComparer.OrdinalIgnoreCase);
        var comparableNames = new HashSet<string>(
            participants
                .Select(participant => NormalizeComparableText(participant.DisplayName))
                .Where(value => !string.IsNullOrWhiteSpace(value))
                .Select(value => value!),
            StringComparer.Ordinal);

        foreach (var rawName in hint.GroupParticipantNames)
        {
            var normalizedName = NormalizeComparableText(rawName);
            if (string.IsNullOrWhiteSpace(normalizedName)
                || comparableNames.Contains(normalizedName!)
                || IsSelfDescriptor(normalizedName!)
                || NameLooksSelf(rawName, selfIdentity))
            {
                continue;
            }

            var participant = CreateHintParticipant(rawName, contactIndex, selfIdentity);
            if (participant is null || enriched.ContainsKey(participant.Key))
            {
                continue;
            }

            enriched[participant.Key] = participant;
            comparableNames.Add(normalizedName!);
        }

        return enriched.Values
            .OrderBy(participant => participant.DisplayName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(participant => participant.Key, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static IReadOnlyList<SynthesizedMessageRecord> BuildShadowMessages(
        IReadOnlyList<NotificationConversationHint> notificationHints,
        IReadOnlySet<uint> matchedHintIds,
        ContactIndex contactIndex,
        SelfIdentity selfIdentity)
    {
        return notificationHints
            .Where(hint => hint.IsActive && !matchedHintIds.Contains(hint.NotificationUid))
            .Select(hint => SynthesizeShadowMessage(hint, contactIndex, selfIdentity))
            .Where(message => message is not null)
            .Select(message => message!)
            .ToArray();
    }

    private static SynthesizedMessageRecord? SynthesizeShadowMessage(
        NotificationConversationHint hint,
        ContactIndex contactIndex,
        SelfIdentity selfIdentity)
    {
        var message = BuildShadowMapMessage(hint, contactIndex, selfIdentity);
        return message is null
            ? null
            : SynthesizeMessage(message, contactIndex, selfIdentity, [hint]).Message;
    }

    private static MessageRecord? BuildShadowMapMessage(
        NotificationConversationHint hint,
        ContactIndex contactIndex,
        SelfIdentity selfIdentity)
    {
        NotificationIdentity? senderIdentity = null;
        if (hint.IsGroupHint)
        {
            if (!string.IsNullOrWhiteSpace(hint.AuthorDisplayName))
            {
                senderIdentity = ResolveNotificationIdentity(hint.AuthorDisplayName, contactIndex);
            }
        }
        else
        {
            senderIdentity = ResolveNotificationIdentity(hint.ThreadTitle, contactIndex);
        }

        if (!hint.IsGroupHint && senderIdentity is null)
        {
            return null;
        }

        var selfPhones = selfIdentity.Phones.Count > 0 ? selfIdentity.Phones : Array.Empty<string>();
        var selfEmails = selfIdentity.Emails.Count > 0 ? selfIdentity.Emails : Array.Empty<string>();
        var senderAddressing = senderIdentity?.PrimaryAddressing;
        var recipientAddressing = selfPhones.FirstOrDefault() ?? selfEmails.FirstOrDefault();
        var localTimestamp = hint.ObservedUtc.ToLocalTime().ToString("yyyyMMdd'T'HHmmss", CultureInfo.InvariantCulture);
        var preview = hint.Preview;
        var originators = senderIdentity is null
            ? Array.Empty<MessageParticipantRecord>()
            : [new MessageParticipantRecord(senderIdentity.DisplayName, senderIdentity.Phones, senderIdentity.Emails)];

        return new MessageRecord(
            "notification",
            null,
            "ANCS",
            preview,
            localTimestamp,
            senderIdentity?.DisplayName,
            senderAddressing,
            recipientAddressing,
            (uint?)preview.Length,
            0,
            null,
            false,
            false,
            false,
            preview,
            "ANCS",
            hint.IsActive ? "Unread" : "Read",
            originators,
            selfPhones.Count > 0 || selfEmails.Count > 0
                ? [new MessageParticipantRecord(selfIdentity.DisplayName, selfPhones, selfEmails)]
                : Array.Empty<MessageParticipantRecord>());
    }

    private static int ScoreNotificationHint(
        MessageRecord message,
        IReadOnlyList<ConversationParticipantRecord> participants,
        MessageDirection direction,
        string? previewNormalized,
        DateTimeOffset? sortUtc,
        NotificationConversationHint hint)
    {
        var score = 0;
        var allowsPreviewMismatch = false;
        var exactPreviewMatch =
            !string.IsNullOrWhiteSpace(previewNormalized)
            && !string.IsNullOrWhiteSpace(hint.PreviewNormalized)
            && string.Equals(previewNormalized, hint.PreviewNormalized, StringComparison.Ordinal);
        var partialPreviewMatch =
            !exactPreviewMatch
            && !string.IsNullOrWhiteSpace(previewNormalized)
            && !string.IsNullOrWhiteSpace(hint.PreviewNormalized)
            && (previewNormalized.Contains(hint.PreviewNormalized, StringComparison.Ordinal)
                || hint.PreviewNormalized.Contains(previewNormalized, StringComparison.Ordinal));
        var previewIsStrong = HasStrongPreviewIdentity(previewNormalized) || HasStrongPreviewIdentity(hint.Preview);
        var counterparties = participants
            .Where(participant => !participant.IsSelf)
            .ToArray();
        var canUseDirectHintAsIdentity =
            !hint.IsGroupHint
            && counterparties.Length == 1
            && LooksLikeUnresolvedDisplayName(counterparties[0].DisplayName)
            && !LooksLikeUnresolvedDisplayName(hint.ThreadTitle);

        if (exactPreviewMatch)
        {
            score += previewIsStrong ? 10 : 6;
        }
        else if (partialPreviewMatch)
        {
            score += previewIsStrong ? 6 : 3;
        }
        else if (!string.IsNullOrWhiteSpace(previewNormalized))
        {
            allowsPreviewMismatch = AllowsGroupHintWithoutPreviewMatch(message, participants, sortUtc, hint);
            if (!allowsPreviewMismatch)
            {
                return int.MinValue;
            }
        }

        var senderCandidates = BuildSenderCandidates(message, participants);
        var hasExplicitSenderEvidence = senderCandidates.Count > 0;
        var senderMatchesHintAuthor = senderCandidates.Any(candidate => NamesLikelyMatch(candidate, hint.AuthorDisplayName));
        var senderMatchesDirectThreadTitle =
            !hint.IsGroupHint
            && senderCandidates.Any(candidate => NamesLikelyMatch(candidate, hint.ThreadTitle));
        var senderSupportsHintIdentity = hint.IsGroupHint
            ? senderMatchesHintAuthor
            : senderMatchesDirectThreadTitle;
        var wholeHourSkew = sortUtc.HasValue && LooksLikeWholeHourTimestampSkew(sortUtc.Value, hint.ObservedUtc);
        var directHintIdentityRescue =
            canUseDirectHintAsIdentity
            && exactPreviewMatch
            && (wholeHourSkew
                || !sortUtc.HasValue
                || (sortUtc.Value - hint.ObservedUtc).Duration() <= TimeSpan.FromHours(3));

        if (sortUtc.HasValue)
        {
            var delta = (sortUtc.Value - hint.ObservedUtc).Duration();
            if (delta <= TimeSpan.FromMinutes(3))
            {
                score += 4;
            }
            else if (delta <= TimeSpan.FromMinutes(10))
            {
                score += 2;
            }
            else if ((senderSupportsHintIdentity || directHintIdentityRescue) && wholeHourSkew)
            {
                score += directHintIdentityRescue ? 4 : 1;
            }
            else if (delta > TimeSpan.FromMinutes(30) && !directHintIdentityRescue)
            {
                score -= previewIsStrong ? 4 : 6;
            }
        }

        if (hint.IsActive)
        {
            score += 1;
        }

        if (direction == MessageDirection.Inbound)
        {
            score += 1;
        }

        if (hint.IsGroupHint)
        {
            if (hasExplicitSenderEvidence)
            {
                if (!senderMatchesHintAuthor)
                {
                    return int.MinValue;
                }

                score += 4;
            }
        }
        else
        {
            if (counterparties.Length == 1 && NamesLikelyMatch(counterparties[0].DisplayName, hint.ThreadTitle))
            {
                score += 3;
            }

            if (senderMatchesDirectThreadTitle)
            {
                score += 2;
            }

            if (directHintIdentityRescue)
            {
                score += 4;
            }
        }

        if (string.IsNullOrWhiteSpace(previewNormalized)
            && score < 7)
        {
            return int.MinValue;
        }

        if (allowsPreviewMismatch)
        {
            score += 1;
        }

        return score;
    }

    private static bool AllowsGroupHintWithoutPreviewMatch(
        MessageRecord message,
        IReadOnlyList<ConversationParticipantRecord> participants,
        DateTimeOffset? sortUtc,
        NotificationConversationHint hint)
    {
        if (!hint.IsGroupHint || sortUtc is null)
        {
            return false;
        }

        var delta = (sortUtc.Value - hint.ObservedUtc).Duration();
        if (delta > TimeSpan.FromMinutes(5))
        {
            return false;
        }

        var senderCandidates = BuildSenderCandidates(message, participants);
        if (!senderCandidates.Any(candidate => NamesLikelyMatch(candidate, hint.AuthorDisplayName)))
        {
            return false;
        }

        var preview = BuildPreview(message);
        if (string.IsNullOrWhiteSpace(preview))
        {
            return participants.Count(participant => !participant.IsSelf) <= 1;
        }

        if (LooksLikeReactionOrAttachmentText(preview))
        {
            if (!LooksLikeReactionOrAttachmentText(hint.Preview))
            {
                return false;
            }

            var previewReference = TryExtractReactionReferencePreview(preview);
            var hintReference = TryExtractReactionReferencePreview(hint.Preview);
            if (!string.IsNullOrWhiteSpace(previewReference)
                && !string.IsNullOrWhiteSpace(hintReference))
            {
                return PreviewsLikelyMatch(previewReference, hintReference);
            }

            var previewSignature = BuildReactionSignature(preview);
            var hintSignature = BuildReactionSignature(hint.Preview);
            return !string.IsNullOrWhiteSpace(previewSignature)
                && string.Equals(previewSignature, hintSignature, StringComparison.Ordinal);
        }

        return LooksLikeGroupContextPreview(preview);
    }

    private static bool LooksLikeReactionOrAttachmentText(string? value)
    {
        var normalized = NormalizeComparableText(value);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return false;
        }

        return normalized.Contains("laughed at", StringComparison.Ordinal)
            || normalized.Contains("liked ", StringComparison.Ordinal)
            || normalized.Contains("loved ", StringComparison.Ordinal)
            || normalized.Contains("disliked ", StringComparison.Ordinal)
            || normalized.Contains("reacted ", StringComparison.Ordinal)
            || normalized.Contains("emphasized ", StringComparison.Ordinal)
            || normalized.Contains("questioned ", StringComparison.Ordinal)
            || LooksLikeSymbolReactionPreview(value)
            || normalized.Contains("attachment", StringComparison.Ordinal)
            || normalized.Contains("image", StringComparison.Ordinal)
            || normalized.Contains("photo", StringComparison.Ordinal);
    }

    private static string? TryExtractNotificationPreviewActor(string? preview)
    {
        if (string.IsNullOrWhiteSpace(preview))
        {
            return null;
        }

        var patterns = new[]
        {
            @"^(?<actor>.+?)\s+laughed\s+at\s+",
            @"^(?<actor>.+?)\s+loved\s+",
            @"^(?<actor>.+?)\s+liked\s+",
            @"^(?<actor>.+?)\s+disliked\s+",
            @"^(?<actor>.+?)\s+reacted\s+",
            @"^(?<actor>.+?)\s+emphasized\s+",
            @"^(?<actor>.+?)\s+questioned\s+"
        };

        foreach (var pattern in patterns)
        {
            var match = Regex.Match(preview, pattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
            if (!match.Success)
            {
                continue;
            }

            var actor = match.Groups["actor"].Value.Trim();
            if (!string.IsNullOrWhiteSpace(actor))
            {
                return actor;
            }
        }

        return null;
    }

    private static IReadOnlyList<string> BuildSenderCandidates(
        MessageRecord message,
        IReadOnlyList<ConversationParticipantRecord> participants)
    {
        var candidates = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        AddRawSenderCandidates(candidates, message);

        foreach (var participant in participants.Where(participant => !participant.IsSelf))
        {
            if (!ParticipantMatchesSenderEvidence(participant, message))
            {
                continue;
            }

            AddParticipantIdentityCandidates(candidates, participant);
        }

        return candidates.ToArray();
    }

    private static void AddRawSenderCandidates(
        ISet<string> candidates,
        MessageRecord message)
    {
        if (!string.IsNullOrWhiteSpace(message.SenderName)
            && !LooksLikeUnresolvedDisplayName(message.SenderName))
        {
            candidates.Add(message.SenderName.Trim());
        }

        if (!string.IsNullOrWhiteSpace(message.SenderAddressing))
        {
            candidates.Add(StripAddressPrefix(message.SenderAddressing).Trim());
        }

        foreach (var originator in message.Originators)
        {
            if (!string.IsNullOrWhiteSpace(originator.Name)
                && !string.Equals(originator.Name, "(unnamed)", StringComparison.OrdinalIgnoreCase))
            {
                candidates.Add(originator.Name.Trim());
            }

            foreach (var phone in originator.Phones)
            {
                if (!string.IsNullOrWhiteSpace(phone))
                {
                    candidates.Add(phone.Trim());
                }
            }

            foreach (var email in originator.Emails)
            {
                if (!string.IsNullOrWhiteSpace(email))
                {
                    candidates.Add(email.Trim());
                }
            }
        }
    }

    private static void AddParticipantIdentityCandidates(
        ISet<string> candidates,
        ConversationParticipantRecord participant)
    {
        if (!string.IsNullOrWhiteSpace(participant.DisplayName))
        {
            candidates.Add(participant.DisplayName.Trim());
        }

        foreach (var phone in participant.Phones)
        {
            if (!string.IsNullOrWhiteSpace(phone))
            {
                candidates.Add(phone.Trim());
            }
        }

        foreach (var email in participant.Emails)
        {
            if (!string.IsNullOrWhiteSpace(email))
            {
                candidates.Add(email.Trim());
            }
        }
    }

    private static bool ParticipantMatchesSenderEvidence(
        ConversationParticipantRecord participant,
        MessageRecord message)
    {
        if (!string.IsNullOrWhiteSpace(message.SenderName)
            && ParticipantMatchesCandidate(participant, message.SenderName.Trim()))
        {
            return true;
        }

        if (AddressMatchesParticipant(participant, message.SenderAddressing))
        {
            return true;
        }

        foreach (var originator in message.Originators)
        {
            if (ParticipantMatchesOriginator(participant, originator))
            {
                return true;
            }
        }

        return false;
    }

    private static bool ParticipantMatchesOriginator(
        ConversationParticipantRecord participant,
        MessageParticipantRecord originator)
    {
        if (!string.IsNullOrWhiteSpace(originator.Name)
            && ParticipantMatchesCandidate(participant, originator.Name.Trim()))
        {
            return true;
        }

        foreach (var phone in originator.Phones)
        {
            if (!string.IsNullOrWhiteSpace(phone)
                && ParticipantMatchesCandidate(participant, phone.Trim()))
            {
                return true;
            }
        }

        foreach (var email in originator.Emails)
        {
            if (!string.IsNullOrWhiteSpace(email)
                && ParticipantMatchesCandidate(participant, email.Trim()))
            {
                return true;
            }
        }

        return false;
    }

    private static bool AddressMatchesParticipant(
        ConversationParticipantRecord participant,
        string? address)
    {
        if (string.IsNullOrWhiteSpace(address))
        {
            return false;
        }

        return ParticipantMatchesCandidate(participant, StripAddressPrefix(address).Trim());
    }

    private static bool HasExplicitSenderEvidence(MessageRecord message)
    {
        return BuildSenderCandidates(message, Array.Empty<ConversationParticipantRecord>()).Count > 0;
    }

    private static IReadOnlyList<string> BuildDirectCounterpartyCandidates(
        IReadOnlyList<ConversationParticipantRecord> participants)
    {
        var counterparties = participants
            .Where(participant => !participant.IsSelf)
            .ToArray();
        if (counterparties.Length != 1)
        {
            return Array.Empty<string>();
        }

        var counterparty = counterparties[0];
        var candidates = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (!string.IsNullOrWhiteSpace(counterparty.DisplayName))
        {
            candidates.Add(counterparty.DisplayName.Trim());
        }

        foreach (var phone in counterparty.Phones)
        {
            if (!string.IsNullOrWhiteSpace(phone))
            {
                candidates.Add(phone.Trim());
            }
        }

        foreach (var email in counterparty.Emails)
        {
            if (!string.IsNullOrWhiteSpace(email))
            {
                candidates.Add(email.Trim());
            }
        }

        return candidates.ToArray();
    }

    private static string? ResolveSenderDisplayName(
        MessageRecord message,
        IReadOnlyList<ConversationParticipantRecord> participants,
        SelfIdentity selfIdentity)
    {
        var direction = InferDirection(message, selfIdentity);
        if (direction == MessageDirection.Outbound)
        {
            return !string.IsNullOrWhiteSpace(selfIdentity.DisplayName)
                ? selfIdentity.DisplayName
                : "Me";
        }

        var senderCandidates = BuildSenderCandidates(message, participants);
        foreach (var candidate in senderCandidates)
        {
            var participantMatch = participants
                .Where(participant => !participant.IsSelf)
                .FirstOrDefault(participant => ParticipantMatchesCandidate(participant, candidate));
            if (participantMatch is not null)
            {
                return participantMatch.DisplayName;
            }
        }

        var senderName = message.SenderName?.Trim();
        if (!string.IsNullOrWhiteSpace(senderName)
            && !LooksLikeUnresolvedDisplayName(senderName))
        {
            return senderName;
        }

        var fallback = message.Originators
            .Select(originator => originator.Name?.Trim())
            .FirstOrDefault(
                name =>
                    !string.IsNullOrWhiteSpace(name)
                    && !string.Equals(name, "(unnamed)", StringComparison.OrdinalIgnoreCase));
        return !string.IsNullOrWhiteSpace(fallback)
            ? fallback
            : null;
    }

    private static bool IsMessagesNotification(StoredNotificationRecord notification)
    {
        return string.Equals(notification.Notification.AppIdentifier, "com.apple.MobileSMS", StringComparison.OrdinalIgnoreCase)
            || string.Equals(notification.Notification.AppIdentifier, "com.apple.MobileSMS.notification", StringComparison.OrdinalIgnoreCase);
    }

    private static NotificationIdentity? ResolveNotificationIdentity(
        string? candidate,
        ContactIndex contactIndex)
    {
        if (string.IsNullOrWhiteSpace(candidate))
        {
            return null;
        }

        if (TryExtractEmail(candidate, out var email))
        {
            var displayName = contactIndex.NameByEmail.TryGetValue(email, out var emailName)
                ? emailName
                : email;
            return new NotificationIdentity(displayName, null, Array.Empty<string>(), [email], email);
        }

        var stripped = StripAddressPrefix(candidate);
        var normalizedPhone = PhoneNumberNormalizer.Normalize(stripped);
        if (!string.IsNullOrWhiteSpace(normalizedPhone))
        {
            var displayName = contactIndex.NameByPhone.TryGetValue(normalizedPhone!, out var phoneName)
                ? phoneName
                : stripped.Trim();
            return new NotificationIdentity(displayName, stripped.Trim(), [normalizedPhone!], Array.Empty<string>(), normalizedPhone!);
        }

        var normalizedName = NormalizeComparableText(candidate);
        if (!string.IsNullOrWhiteSpace(normalizedName)
            && contactIndex.ContactsByComparableName.TryGetValue(normalizedName!, out var contacts)
            && contacts.Count == 1)
        {
            return CreateNotificationIdentity(contacts[0]);
        }

        var fuzzyMatch = FindSingleFuzzyContactMatch(normalizedName, contactIndex);
        if (fuzzyMatch is not null)
        {
            return CreateNotificationIdentity(fuzzyMatch);
        }

        var display = candidate.Trim();
        return new NotificationIdentity(display, null, Array.Empty<string>(), Array.Empty<string>(), $"name:{display}");
    }

    private static ConversationParticipantRecord? CreateHintParticipant(
        string rawName,
        ContactIndex contactIndex,
        SelfIdentity selfIdentity)
    {
        var resolved = ResolveNotificationIdentity(rawName, contactIndex);
        if (resolved is not null)
        {
            var isSelf = resolved.Phones.Any(phone => selfIdentity.PhoneSet.Contains(phone))
                || resolved.Emails.Any(email => selfIdentity.EmailSet.Contains(email));
            if (isSelf)
            {
                return null;
            }

            var key = resolved.Phones.FirstOrDefault() is { Length: > 0 } phone
                ? $"phone:{phone}"
                : resolved.Emails.FirstOrDefault() is { Length: > 0 } email
                    ? $"email:{email}"
                    : $"name:{NormalizeComparableText(resolved.DisplayName) ?? resolved.DisplayName.ToLowerInvariant()}";

            return new ConversationParticipantRecord(
                key,
                resolved.DisplayName,
                resolved.Phones.OrderBy(value => value, StringComparer.OrdinalIgnoreCase).ToArray(),
                resolved.Emails.OrderBy(value => value, StringComparer.OrdinalIgnoreCase).ToArray(),
                false);
        }

        var normalizedName = NormalizeComparableText(rawName);
        if (string.IsNullOrWhiteSpace(normalizedName))
        {
            return null;
        }

        return new ConversationParticipantRecord(
            $"name:{normalizedName}",
            rawName.Trim(),
            Array.Empty<string>(),
            Array.Empty<string>(),
            false);
    }

    private static bool LooksLikeUnresolvedDisplayName(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return true;
        }

        if (value.StartsWith("+", StringComparison.Ordinal)
            || value.Contains('@', StringComparison.Ordinal)
            || value.StartsWith("phone:", StringComparison.OrdinalIgnoreCase)
            || value.StartsWith("email:", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "(unknown)", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return value.All(character => char.IsDigit(character) || character is '+' or '-' or ' ');
    }

    private static bool LooksLikeGroupDescriptor(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
        var normalized = NormalizeComparableText(trimmed);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return false;
        }

        if (normalized.StartsWith("to ", StringComparison.Ordinal)
            || normalized.Contains(", ", StringComparison.Ordinal)
            || normalized.Contains(" and ", StringComparison.Ordinal)
            || normalized.Contains(" everyone", StringComparison.Ordinal)
            || normalized.Contains(" group", StringComparison.Ordinal)
            || normalized.Contains(" family", StringComparison.Ordinal)
            || normalized.Contains(" crew", StringComparison.Ordinal)
            || normalized.Contains(" chat", StringComparison.Ordinal))
        {
            return true;
        }

        var commaCount = trimmed.Count(character => character == ',');
        return commaCount >= 2;
    }

    private static bool LooksLikeExplicitParticipantDescriptor(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
        var normalized = NormalizeComparableText(trimmed);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return false;
        }

        return normalized.StartsWith("to ", StringComparison.Ordinal)
            || trimmed.Contains(',', StringComparison.Ordinal)
            || trimmed.Contains('&', StringComparison.Ordinal)
            || normalized.Contains(" and ", StringComparison.Ordinal)
            || Regex.IsMatch(normalized, @"\b\d+\s+others?\b", RegexOptions.CultureInvariant);
    }

    private static string? BuildGroupSeedNormalized(
        string threadTitle,
        string? authorDisplayName,
        bool isGroupHint)
    {
        if (!isGroupHint)
        {
            return null;
        }

        var normalizedTitle = NormalizeComparableText(threadTitle);
        if (string.IsNullOrWhiteSpace(normalizedTitle))
        {
            return null;
        }

        if (!LooksLikeGroupDescriptor(threadTitle))
        {
            return $"title:{normalizedTitle}";
        }

        var descriptor = ParseGroupDescriptor(threadTitle);
        var visibleMembers = descriptor.NormalizedMembers
            .Take(2)
            .ToArray();
        if ((descriptor.HasOthersCount || descriptor.WasTruncated || descriptor.EstimatedExternalCount >= 4)
            && visibleMembers.Length >= 2)
        {
            return $"members:{string.Join("|", visibleMembers.OrderBy(member => member, StringComparer.Ordinal))}|large";
        }

        var members = new HashSet<string>(visibleMembers, StringComparer.Ordinal);
        var normalizedAuthor = NormalizeComparableText(authorDisplayName);
        if (!string.IsNullOrWhiteSpace(normalizedAuthor)
            && !IsSelfDescriptor(normalizedAuthor!))
        {
            members.Add(normalizedAuthor!);
        }

        var orderedMembers = members
            .Take(2)
            .OrderBy(member => member, StringComparer.Ordinal)
            .ToArray();
        if (orderedMembers.Length >= 2)
        {
            return $"members:{string.Join("|", orderedMembers)}";
        }

        return $"title:{normalizedTitle}";
    }

    private static IReadOnlyList<string> ExtractGroupDescriptorMembers(string descriptor)
    {
        return ParseGroupDescriptor(descriptor).NormalizedMembers;
    }

    private static IReadOnlyList<string> ExtractGroupDescriptorDisplayNames(string descriptor)
    {
        return ParseGroupDescriptor(descriptor).DisplayMembers;
    }

    private static GroupDescriptor ParseGroupDescriptor(string descriptor)
    {
        if (string.IsNullOrWhiteSpace(descriptor))
        {
            return new GroupDescriptor(Array.Empty<string>(), Array.Empty<string>(), false, false, 0);
        }

        var trimmed = descriptor.Trim();
        var wasTruncated = trimmed.Contains("...", StringComparison.Ordinal)
            || trimmed.Contains("\u2026", StringComparison.Ordinal)
            || trimmed.Contains("…", StringComparison.Ordinal);
        var normalized = trimmed
            .Replace("…", string.Empty, StringComparison.Ordinal)
            .Replace("...", string.Empty, StringComparison.Ordinal)
            .Replace("\u2026", string.Empty, StringComparison.Ordinal)
            .Trim();
        if (normalized.StartsWith("To ", StringComparison.OrdinalIgnoreCase))
        {
            normalized = normalized[3..].Trim();
        }

        var parts = normalized
            .Replace(" and ", ",", StringComparison.OrdinalIgnoreCase)
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .ToList();

        var hasOthersCount = false;
        var othersCount = 0;
        if (parts.Count > 0
            && TrySplitOthersSuffix(parts[^1], out var visibleMember, out othersCount))
        {
            hasOthersCount = true;
            if (string.IsNullOrWhiteSpace(visibleMember))
            {
                parts.RemoveAt(parts.Count - 1);
            }
            else
            {
                parts[^1] = visibleMember!;
            }
        }

        if (wasTruncated
            && !hasOthersCount
            && parts.Count > 0
            && ShouldDropTrailingTruncatedMember(parts[^1]))
        {
            parts.RemoveAt(parts.Count - 1);
        }

        var displayMembers = parts
            .Where(part => !string.IsNullOrWhiteSpace(part))
            .Select(part => part.Trim())
            .Where(part => !IsSelfDescriptor(NormalizeComparableText(part) ?? string.Empty))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var normalizedMembers = displayMembers
            .Select(NormalizeComparableText)
            .Where(part => !string.IsNullOrWhiteSpace(part))
            .Select(part => part!)
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        return new GroupDescriptor(
            displayMembers,
            normalizedMembers,
            hasOthersCount,
            wasTruncated,
            normalizedMembers.Length + othersCount);
    }

    private static bool TrySplitOthersSuffix(string value, out string? visibleMember, out int othersCount)
    {
        var match = Regex.Match(
            value,
            @"^(?<name>.*?)(?:\s*&\s*|\s+and\s+)?(?<count>\d+)\s+others?$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        if (!match.Success || !int.TryParse(match.Groups["count"].Value, out othersCount))
        {
            visibleMember = null;
            othersCount = 0;
            return false;
        }

        visibleMember = match.Groups["name"].Value.Trim().Trim(',', '&');
        return true;
    }

    private static bool ShouldDropTrailingTruncatedMember(string value)
    {
        var trimmed = value.Trim();
        if (string.IsNullOrWhiteSpace(trimmed))
        {
            return true;
        }

        return trimmed.Length <= 4
            && trimmed.All(character => char.IsLetter(character) || character == '.');
    }

    private static bool IsSelfDescriptor(string value)
    {
        return string.Equals(value, "you", StringComparison.Ordinal)
            || string.Equals(value, "me", StringComparison.Ordinal)
            || string.Equals(value, "yourself", StringComparison.Ordinal)
            || string.Equals(value, "myself", StringComparison.Ordinal);
    }

    private static bool NamesLikelyMatch(string? left, string? right)
    {
        var normalizedLeft = NormalizeComparableText(left);
        var normalizedRight = NormalizeComparableText(right);
        if (string.IsNullOrWhiteSpace(normalizedLeft) || string.IsNullOrWhiteSpace(normalizedRight))
        {
            return false;
        }

        return string.Equals(normalizedLeft, normalizedRight, StringComparison.Ordinal)
            || normalizedLeft.Contains(normalizedRight, StringComparison.Ordinal)
            || normalizedRight.Contains(normalizedLeft, StringComparison.Ordinal);
    }

    private static IReadOnlyList<SynthesizedMessageRecord> CoalesceConversationMessages(
        IReadOnlyList<SynthesizedMessageRecord> messages,
        IReadOnlyList<NotificationConversationHint> notificationHints,
        IReadOnlySet<string> deferredDirectMessageKeys)
    {
        IReadOnlyList<SynthesizedMessageRecord> current = messages.ToArray();

        for (var pass = 0; pass < 3; pass++)
        {
            IReadOnlyList<SynthesizedMessageRecord> next = ReassignReactionMessagesToGroupThreads(current);
            next = ReassignSparseMessagesToLargeGroupThreads(next);
            next = MergeEquivalentGroupThreads(next);
            next = ReassignMessagesUsingSupportingGroupHints(next, notificationHints);
            next = MergeEquivalentGroupThreads(next);
            next = FinalizeDeferredDirectMessagesToEstablishedDirectThreads(next, deferredDirectMessageKeys);
            next = CollapseNotificationShadowDuplicates(next).ToArray();

            if (CoalescedMessagesEquivalent(current, next))
            {
                return next;
            }

            current = next;
        }

        return current;
    }

    private static bool CoalescedMessagesEquivalent(
        IReadOnlyList<SynthesizedMessageRecord> left,
        IReadOnlyList<SynthesizedMessageRecord> right)
    {
        if (left.Count != right.Count)
        {
            return false;
        }

        var leftSignature = left
            .OrderBy(message => message.MessageKey, StringComparer.OrdinalIgnoreCase)
            .Select(message => $"{message.MessageKey}|{message.ConversationId}|{message.IsGroup}|{message.ConversationDisplayName}")
            .ToArray();
        var rightSignature = right
            .OrderBy(message => message.MessageKey, StringComparer.OrdinalIgnoreCase)
            .Select(message => $"{message.MessageKey}|{message.ConversationId}|{message.IsGroup}|{message.ConversationDisplayName}")
            .ToArray();

        return leftSignature.SequenceEqual(rightSignature, StringComparer.OrdinalIgnoreCase);
    }

    private static IReadOnlyList<SynthesizedMessageRecord> ReassignMessagesUsingSupportingGroupHints(
        IReadOnlyList<SynthesizedMessageRecord> messages,
        IReadOnlyList<NotificationConversationHint> notificationHints)
    {
        var allBuckets = BuildConversationBuckets(messages)
            .ToDictionary(bucket => bucket.ConversationId, StringComparer.OrdinalIgnoreCase);
        var groupHints = notificationHints
            .Where(hint => hint.IsGroupHint)
            .ToArray();
        if (groupHints.Length == 0)
        {
            return messages;
        }

        var groupBuckets = allBuckets.Values
            .Where(bucket => bucket.IsGroup)
            .Where(IsEstablishedHintBackedGroupBucket)
            .ToArray();
        if (groupBuckets.Length == 0)
        {
            return messages;
        }

        return messages
            .Select(
                message =>
                {
                    if (!IsEligibleSparseGroupCandidate(message))
                    {
                        return message;
                    }

                    var bestHint = default(NotificationConversationHint);
                    var bestHintScore = int.MinValue;
                    foreach (var hint in groupHints)
                    {
                        var score = ScoreSupportingGroupHint(message, hint);
                        if (score > bestHintScore)
                        {
                            bestHintScore = score;
                            bestHint = hint;
                        }
                    }

                    if (bestHint is null || bestHintScore < 8)
                    {
                        return message;
                    }

                    var bestBucket = default(ConversationBucket);
                    var bestBucketScore = int.MinValue;
                    foreach (var bucket in groupBuckets)
                    {
                        var score = ScoreHintBackedGroupBucketMatch(message, bestHint, bucket);
                        if (score > bestBucketScore)
                        {
                            bestBucketScore = score;
                            bestBucket = bucket;
                        }
                    }

                    if (bestBucket is null || bestBucketScore < 8)
                    {
                        return message;
                    }

                    allBuckets.TryGetValue(message.ConversationId, out var currentBucket);
                    var stayScore = ScoreExistingDirectBucketAffinity(message, currentBucket);
                    if (bestBucketScore < stayScore + 3)
                    {
                        return message;
                    }

                    return message with
                    {
                        ConversationId = bestBucket.ConversationId,
                        ConversationDisplayName = bestBucket.DisplayName,
                        IsGroup = true,
                        Participants = MergeParticipants(message.Participants, bestBucket.Participants)
                    };
                })
            .ToArray();
    }

    // Finalize deferred direct rows only when the remaining evidence is unambiguous: exactly one
    // established direct bucket matches the counterparty, and there is nearby durable continuity.
    private static IReadOnlyList<SynthesizedMessageRecord> FinalizeDeferredDirectMessagesToEstablishedDirectThreads(
        IReadOnlyList<SynthesizedMessageRecord> messages,
        IReadOnlySet<string> deferredDirectMessageKeys)
    {
        if (deferredDirectMessageKeys.Count == 0)
        {
            return messages;
        }

        var directBuckets = BuildConversationBuckets(messages)
            .Where(IsEstablishedDirectBucket)
            .ToArray();
        if (directBuckets.Length == 0)
        {
            return messages;
        }

        return messages
            .Select(
                message =>
                {
                    if (!deferredDirectMessageKeys.Contains(message.MessageKey)
                        || message.IsGroup
                        || message.Participants.Count(participant => !participant.IsSelf) != 1)
                    {
                        return message;
                    }

                    if (!TrySelectEstablishedDirectBucketForDeferredMessage(message, directBuckets, out var matchedBucket))
                    {
                        return message;
                    }

                    return message with
                    {
                        ConversationId = matchedBucket!.ConversationId,
                        ConversationDisplayName = matchedBucket.DisplayName,
                        Participants = MergeParticipants(message.Participants, matchedBucket.Participants)
                    };
                })
            .ToArray();
    }

    private static bool TrySelectEstablishedDirectBucketForDeferredMessage(
        SynthesizedMessageRecord message,
        IReadOnlyList<ConversationBucket> directBuckets,
        out ConversationBucket? matchedBucket)
    {
        matchedBucket = null;

        foreach (var bucket in directBuckets)
        {
            if (!CanFinalizeDeferredDirectMessageToBucket(message, bucket))
            {
                continue;
            }

            if (matchedBucket is not null)
            {
                matchedBucket = null;
                return false;
            }

            matchedBucket = bucket;
        }

        return matchedBucket is not null;
    }

    private static bool CanFinalizeDeferredDirectMessageToBucket(
        SynthesizedMessageRecord message,
        ConversationBucket bucket)
    {
        var counterpartyCandidates = BuildDirectCounterpartyCandidates(message.Participants);
        if (counterpartyCandidates.Count == 0
            || !counterpartyCandidates.Any(candidate => BucketHasMatchingParticipant(bucket, candidate)))
        {
            return false;
        }

        var preview = BuildPreview(message.Message);
        if (LooksLikeReactionOrAttachmentText(preview) || LooksLikeGroupContextPreview(preview))
        {
            return false;
        }

        var nearbyDurableMessages = bucket.Messages
            .Where(candidate => !IsNotificationBacked(candidate.Message))
            .Where(
                candidate =>
                {
                    var delta = CalculateMessageDelta(message.SortTimestampUtc, candidate.SortTimestampUtc);
                    return delta is not null && delta <= TimeSpan.FromHours(12);
                })
            .ToArray();
        if (nearbyDurableMessages.Length == 0)
        {
            return false;
        }

        return nearbyDurableMessages.Any(IsOutboundConversationMessage)
            || nearbyDurableMessages.Length >= 2;
    }

    private static IReadOnlyList<SynthesizedMessageRecord> ReassignReactionMessagesToGroupThreads(
        IReadOnlyList<SynthesizedMessageRecord> messages)
    {
        var allBuckets = BuildConversationBuckets(messages)
            .ToDictionary(bucket => bucket.ConversationId, StringComparer.OrdinalIgnoreCase);
        var groupBuckets = allBuckets.Values
            .Where(bucket => bucket.IsGroup)
            .ToArray();
        if (groupBuckets.Length == 0)
        {
            return messages;
        }

        return messages
            .Select(
                message =>
                {
                    if (message.IsGroup)
                    {
                        return message;
                    }

                    var referencePreview = TryExtractReactionReferencePreview(BuildPreview(message.Message));
                    if (string.IsNullOrWhiteSpace(referencePreview))
                    {
                        return message;
                    }

                    var bestBucket = default(ConversationBucket);
                    var bestScore = int.MinValue;
                    foreach (var bucket in groupBuckets)
                    {
                        var score = ScoreReactionBucketMatch(message, referencePreview!, bucket);
                        if (score > bestScore)
                        {
                            bestScore = score;
                            bestBucket = bucket;
                        }
                    }

                    if (bestBucket is null || bestScore < 10)
                    {
                        return message;
                    }

                    allBuckets.TryGetValue(message.ConversationId, out var currentBucket);
                    var stayScore = ScoreExistingDirectBucketAffinity(message, currentBucket);
                    var preview = BuildPreview(message.Message);
                    var requiresStrongerGroupEvidence =
                        currentBucket is { IsGroup: false }
                        && message.Participants.Count(participant => !participant.IsSelf) == 1
                        && LooksLikeLowSignalReplyPreview(preview)
                        && !LooksLikeReactionOrAttachmentText(preview)
                        && !LooksLikeGroupContextPreview(preview)
                        && stayScore >= 6;
                    var requiredMargin = requiresStrongerGroupEvidence ? 8 : 3;
                    if (bestScore < stayScore + requiredMargin)
                    {
                        return message;
                    }

                    return message with
                    {
                        ConversationId = bestBucket.ConversationId,
                        ConversationDisplayName = bestBucket.DisplayName,
                        IsGroup = true,
                        Participants = MergeParticipants(message.Participants, bestBucket.Participants)
                    };
                })
            .ToArray();
    }

    private static IReadOnlyList<SynthesizedMessageRecord> ReassignSparseMessagesToLargeGroupThreads(
        IReadOnlyList<SynthesizedMessageRecord> messages)
    {
        var allBuckets = BuildConversationBuckets(messages)
            .ToDictionary(bucket => bucket.ConversationId, StringComparer.OrdinalIgnoreCase);
        var groupBuckets = allBuckets.Values
            .Where(bucket => bucket.IsGroup)
            .Where(IsEstablishedLargeGroupBucket)
            .ToArray();
        if (groupBuckets.Length == 0)
        {
            return messages;
        }

        return messages
            .Select(
                message =>
                {
                    if (!IsEligibleSparseGroupCandidate(message))
                    {
                        return message;
                    }

                    var bestBucket = default(ConversationBucket);
                    var bestScore = int.MinValue;
                    foreach (var bucket in groupBuckets)
                    {
                        var score = ScoreSparseGroupBucketMatch(message, bucket);
                        if (score > bestScore)
                        {
                            bestScore = score;
                            bestBucket = bucket;
                        }
                    }

                    if (bestBucket is null || bestScore < 10)
                    {
                        return message;
                    }

                    allBuckets.TryGetValue(message.ConversationId, out var currentBucket);
                    var stayScore = ScoreExistingDirectBucketAffinity(message, currentBucket);
                    var preview = BuildPreview(message.Message);
                    var lowSignalReply = LooksLikeLowSignalReplyPreview(preview);
                    var reactionLike = LooksLikeReactionOrAttachmentText(preview);
                    var groupCue = LooksLikeGroupContextPreview(preview);
                    var previewMissing = string.IsNullOrWhiteSpace(NormalizeComparableText(preview));
                    var directBucket = currentBucket is { IsGroup: false }
                        && currentBucket.Participants.Count(participant => !participant.IsSelf) == 1;
                    var senderCandidates = BuildSenderCandidates(message.Message, message.Participants);
                    var senderMessageMatchCount = senderCandidates
                        .Select(candidate => CountMessagesFromSender(bestBucket, candidate))
                        .DefaultIfEmpty(0)
                        .Max();
                    var senderOnlyListedInBestBucket = senderMessageMatchCount == 0
                        && senderCandidates.Any(candidate => BucketHasMatchingParticipant(bestBucket, candidate));
                    var directLikeLowSignal = directBucket
                        && !groupCue
                        && (lowSignalReply || reactionLike || previewMissing);
                    if (directLikeLowSignal
                        && stayScore >= 6
                        && senderOnlyListedInBestBucket
                        && bestScore < stayScore + 8)
                    {
                        return message;
                    }

                    if (bestScore < stayScore + 3)
                    {
                        return message;
                    }

                    return message with
                    {
                        ConversationId = bestBucket.ConversationId,
                        ConversationDisplayName = bestBucket.DisplayName,
                        IsGroup = true,
                        Participants = MergeParticipants(message.Participants, bestBucket.Participants)
                    };
                })
            .ToArray();
    }

    private static IReadOnlyList<SynthesizedMessageRecord> MergeEquivalentGroupThreads(
        IReadOnlyList<SynthesizedMessageRecord> messages)
    {
        var buckets = BuildConversationBuckets(messages)
            .Where(bucket => bucket.IsGroup)
            .ToArray();
        if (buckets.Length < 2)
        {
            return messages;
        }

        var parentByConversationId = buckets.ToDictionary(
            bucket => bucket.ConversationId,
            bucket => bucket.ConversationId,
            StringComparer.OrdinalIgnoreCase);

        string FindRoot(string conversationId)
        {
            var current = conversationId;
            while (!string.Equals(parentByConversationId[current], current, StringComparison.OrdinalIgnoreCase))
            {
                current = parentByConversationId[current];
            }

            return current;
        }

        void Union(ConversationBucket left, ConversationBucket right)
        {
            var leftRoot = FindRoot(left.ConversationId);
            var rightRoot = FindRoot(right.ConversationId);
            if (string.Equals(leftRoot, rightRoot, StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            var leftBucket = buckets.First(bucket => string.Equals(bucket.ConversationId, leftRoot, StringComparison.OrdinalIgnoreCase));
            var rightBucket = buckets.First(bucket => string.Equals(bucket.ConversationId, rightRoot, StringComparison.OrdinalIgnoreCase));
            var preferred = SelectPreferredBucket(leftBucket, rightBucket);
            var other = string.Equals(preferred.ConversationId, leftRoot, StringComparison.OrdinalIgnoreCase)
                ? rightRoot
                : leftRoot;
            parentByConversationId[other] = preferred.ConversationId;
        }

        for (var index = 0; index < buckets.Length; index++)
        {
            for (var otherIndex = index + 1; otherIndex < buckets.Length; otherIndex++)
            {
                if (ScoreGroupBucketSimilarity(buckets[index], buckets[otherIndex]) >= 10)
                {
                    Union(buckets[index], buckets[otherIndex]);
                }
            }
        }

        var aliases = parentByConversationId
            .Where(pair => !string.Equals(pair.Key, pair.Value, StringComparison.OrdinalIgnoreCase))
            .ToDictionary(pair => pair.Key, pair => FindRoot(pair.Value), StringComparer.OrdinalIgnoreCase);
        if (aliases.Count == 0)
        {
            return messages;
        }

        var canonicalBuckets = BuildConversationBuckets(
            messages.Select(
                message =>
                {
                    var canonicalId = aliases.TryGetValue(message.ConversationId, out var root)
                        ? root
                        : message.ConversationId;
                    return canonicalId == message.ConversationId
                        ? message
                        : message with { ConversationId = canonicalId };
                }))
            .ToDictionary(bucket => bucket.ConversationId, StringComparer.OrdinalIgnoreCase);

        return messages
            .Select(
                message =>
                {
                    var canonicalId = aliases.TryGetValue(message.ConversationId, out var root)
                        ? root
                        : message.ConversationId;
                    if (!canonicalBuckets.TryGetValue(canonicalId, out var bucket))
                    {
                        return message;
                    }

                    return message with
                    {
                        ConversationId = canonicalId,
                        ConversationDisplayName = bucket.DisplayName,
                        IsGroup = bucket.IsGroup || message.IsGroup,
                        Participants = bucket.IsGroup
                            ? MergeParticipants(message.Participants, bucket.Participants)
                            : message.Participants
                    };
                })
            .ToArray();
    }

    private static IReadOnlyList<SynthesizedMessageRecord> CollapseNotificationShadowDuplicates(
        IReadOnlyList<SynthesizedMessageRecord> messages)
    {
        var collapsed = new List<SynthesizedMessageRecord>(messages.Count);

        foreach (var group in messages.GroupBy(message => message.ConversationId, StringComparer.OrdinalIgnoreCase))
        {
            var ordered = group
                .OrderByDescending(message => message.SortTimestampUtc ?? DateTimeOffset.MinValue)
                .ThenByDescending(message => IsNotificationBacked(message.Message))
                .ThenByDescending(message => message.Message.Datetime ?? string.Empty, StringComparer.OrdinalIgnoreCase)
                .ThenBy(message => message.MessageKey, StringComparer.OrdinalIgnoreCase)
                .ToList();
            var unmatchedDurables = ordered
                .Where(message => !IsNotificationBacked(message.Message))
                .ToList();
            var unmatchedShadows = new List<SynthesizedMessageRecord>();

            foreach (var current in ordered.Where(message => IsNotificationBacked(message.Message)))
            {
                var duplicate = unmatchedDurables
                    .Select(candidate => (Candidate: candidate, Score: ScoreShadowDuplicateMatch(current, candidate)))
                    .Where(match => match.Score >= 10)
                    .OrderByDescending(match => match.Score)
                    .ThenByDescending(match => match.Candidate.SortTimestampUtc ?? DateTimeOffset.MinValue)
                    .FirstOrDefault();

                if (duplicate.Candidate is null)
                {
                    unmatchedShadows.Add(current);
                    continue;
                }

                unmatchedDurables.Remove(duplicate.Candidate);
                collapsed.Add(MergeShadowIntoDurableMessage(current, duplicate.Candidate));
            }

            collapsed.AddRange(unmatchedDurables);
            collapsed.AddRange(unmatchedShadows);
        }

        return collapsed;
    }

    private static IReadOnlyList<ConversationBucket> BuildConversationBuckets(
        IEnumerable<SynthesizedMessageRecord> messages)
    {
        return messages
            .GroupBy(message => message.ConversationId, StringComparer.OrdinalIgnoreCase)
            .Select(
                group =>
                {
                    var orderedMessages = group
                        .OrderByDescending(message => message.SortTimestampUtc ?? DateTimeOffset.MinValue)
                        .ThenByDescending(message => !IsNotificationBacked(message.Message))
                        .ThenByDescending(message => message.Message.Datetime ?? string.Empty, StringComparer.OrdinalIgnoreCase)
                        .ToArray();
                    var mergedParticipants = orderedMessages
                        .SelectMany(message => message.Participants)
                        .ToArray();

                    return new ConversationBucket(
                        group.Key,
                        orderedMessages[0].ConversationDisplayName,
                        orderedMessages.Any(message => message.IsGroup),
                        MergeParticipants(Array.Empty<ConversationParticipantRecord>(), mergedParticipants),
                        orderedMessages,
                        orderedMessages[0].SortTimestampUtc);
                })
            .ToArray();
    }

    private static int ScoreReactionBucketMatch(
        SynthesizedMessageRecord message,
        string referencePreviewNormalized,
        ConversationBucket bucket)
    {
        var score = 0;
        var senderCandidates = BuildSenderCandidates(message.Message, message.Participants);
        if (bucket.Messages.Any(
                candidate =>
                    !LooksLikeReactionPreview(BuildPreview(candidate.Message))
                    && string.Equals(
                        NormalizeComparableText(BuildPreview(candidate.Message)),
                        referencePreviewNormalized,
                        StringComparison.Ordinal)))
        {
            score += 12;
        }
        else if (bucket.Messages.Any(
                     candidate =>
                         !LooksLikeReactionPreview(BuildPreview(candidate.Message))
                         && PreviewTokensOverlap(BuildPreview(candidate.Message), referencePreviewNormalized)))
        {
            score += 8;
        }
        else if (bucket.Messages.Any(
                     candidate =>
                         string.Equals(
                             NormalizeComparableText(BuildPreview(candidate.Message)),
                             referencePreviewNormalized,
                             StringComparison.Ordinal)))
        {
            score += 4;
        }
        else if (LooksLikeReactionOrAttachmentText(BuildPreview(message.Message))
                 && bucket.Messages.Any(candidate => LooksLikeReactionOrAttachmentText(BuildPreview(candidate.Message)))
                 && senderCandidates.Any(candidate => BucketHasMatchingParticipant(bucket, candidate)))
        {
            score += 10;
        }
        else
        {
            return int.MinValue;
        }

        if (senderCandidates.Any(candidate => BucketHasMatchingParticipant(bucket, candidate)))
        {
            score += 2;
        }

        if (message.SortTimestampUtc.HasValue && bucket.LastMessageUtc.HasValue)
        {
            var delta = (message.SortTimestampUtc.Value - bucket.LastMessageUtc.Value).Duration();
            if (delta <= TimeSpan.FromMinutes(30))
            {
                score += 2;
            }
            else if (delta <= TimeSpan.FromHours(2))
            {
                score += 1;
            }
        }

        return score;
    }

    private static int ScoreGroupBucketSimilarity(
        ConversationBucket left,
        ConversationBucket right)
    {
        var score = 0;
        var leftCore = BuildComparableGroupCore(left);
        var rightCore = BuildComparableGroupCore(right);
        if (!string.IsNullOrWhiteSpace(leftCore)
            && string.Equals(leftCore, rightCore, StringComparison.Ordinal))
        {
            score += 10;
        }

        var overlapCount = CountParticipantOverlap(left.Participants, right.Participants);
        if (overlapCount >= 3)
        {
            score += 5;
        }
        else if (overlapCount >= 2)
        {
            score += 3;
        }

        if (BucketsShareReactionReference(left, right))
        {
            score += 6;
        }

        return score;
    }

    private static int ScoreSparseGroupBucketMatch(
        SynthesizedMessageRecord message,
        ConversationBucket bucket)
    {
        var preview = BuildPreview(message.Message);
        var reactionLike = LooksLikeReactionOrAttachmentText(preview);
        var groupCue = LooksLikeGroupContextPreview(preview);
        var previewMissing = string.IsNullOrWhiteSpace(NormalizeComparableText(preview));
        var lowSignalReply = LooksLikeLowSignalReplyPreview(preview);
        var previewMentionsParticipant = PreviewMentionsBucketParticipant(preview, bucket);

        var senderCandidates = BuildSenderCandidates(message.Message, message.Participants);
        var bucketHasSender = senderCandidates.Any(candidate => BucketHasMatchingParticipant(bucket, candidate));
        var senderMessageMatchCount = senderCandidates
            .Select(candidate => CountMessagesFromSender(bucket, candidate))
            .DefaultIfEmpty(0)
            .Max();
        var nearbyOtherSenderCount = CountNearbyDistinctSenders(
            bucket,
            message.SortTimestampUtc,
            TimeSpan.FromHours(1),
            senderCandidates);
        var nearbyOtherSenderCountExtended = CountNearbyDistinctSenders(
            bucket,
            message.SortTimestampUtc,
            TimeSpan.FromHours(3),
            senderCandidates);
        var hasLocalGroupActivity = nearbyOtherSenderCount >= 2;
        var recentGroupBurst = nearbyOtherSenderCount >= 3;
        var nearbyBurstExtended = nearbyOtherSenderCountExtended >= 3;
        var establishedSenderMembership = senderMessageMatchCount >= 2;
        var strongSenderContinuity = senderMessageMatchCount >= 1 && hasLocalGroupActivity;

        if (!reactionLike
            && !groupCue
            && !previewMissing
            && !lowSignalReply
            && !strongSenderContinuity
            && !establishedSenderMembership
            && !recentGroupBurst)
        {
            return int.MinValue;
        }

        var allowedDelta = reactionLike || previewMissing
            ? TimeSpan.FromHours(6)
            : TimeSpan.FromHours(3);
        var closestDelta = bucket.Messages
            .Select(candidate => CalculateMessageDelta(message.SortTimestampUtc, candidate.SortTimestampUtc))
            .Where(delta => delta is not null)
            .Select(delta => delta!.Value)
            .DefaultIfEmpty(TimeSpan.MaxValue)
            .Min();
        if (closestDelta > allowedDelta)
        {
            return int.MinValue;
        }

        var sparseSenderContinuation = previewMissing
            && senderMessageMatchCount >= 1
            && closestDelta <= TimeSpan.FromHours(6);
        var lowSignalLargeGroupContext = lowSignalReply
            && nearbyBurstExtended
            && closestDelta <= TimeSpan.FromHours(3);

        if (!bucketHasSender
            && !strongSenderContinuity
            && !establishedSenderMembership
            && !(groupCue
                 && LooksLikeGroupDescriptor(bucket.DisplayName)
                 && closestDelta <= TimeSpan.FromHours(2))
            && !(reactionLike && previewMentionsParticipant)
            && !(reactionLike && hasLocalGroupActivity)
            && !(previewMissing && hasLocalGroupActivity)
            && !sparseSenderContinuation
            && !lowSignalLargeGroupContext
            && !(recentGroupBurst
                 && closestDelta <= TimeSpan.FromMinutes(20)
                 && bucket.Participants.Count(participant => !participant.IsSelf) >= 8))
        {
            return int.MinValue;
        }

        var score = 0;
        if (reactionLike)
        {
            score += 6;
        }

        if (reactionLike && previewMentionsParticipant)
        {
            score += 4;
        }

        if (groupCue)
        {
            score += 4;
        }

        if (previewMissing)
        {
            score += 2;
        }

        if (lowSignalReply)
        {
            score += 1;
        }

        if (bucketHasSender)
        {
            score += 4;
        }
        else if (groupCue && LooksLikeGroupDescriptor(bucket.DisplayName))
        {
            score += 2;
        }

        if (strongSenderContinuity)
        {
            score += Math.Min(4, senderMessageMatchCount * 2);
        }
        else if (establishedSenderMembership)
        {
            score += Math.Min(4, senderMessageMatchCount);
        }
        else if (bucketHasSender && senderMessageMatchCount >= 1)
        {
            score += 2;
        }
        else if (hasLocalGroupActivity)
        {
            score += 2;
        }

        if (lowSignalLargeGroupContext)
        {
            score += 8;
        }

        if (sparseSenderContinuation)
        {
            score += 3;
        }

        if (recentGroupBurst && closestDelta <= TimeSpan.FromMinutes(20))
        {
            score += 4;
        }

        if (closestDelta <= TimeSpan.FromMinutes(10))
        {
            score += 4;
        }
        else if (closestDelta <= TimeSpan.FromMinutes(30))
        {
            score += 2;
        }
        else if (closestDelta <= TimeSpan.FromHours(2))
        {
            score += 1;
        }
        else
        {
            score += 0;
        }

        if (bucket.Messages.Any(candidate => IsNotificationBacked(candidate.Message)))
        {
            score += 1;
        }

        if (bucket.Participants.Count(participant => !participant.IsSelf) >= 8)
        {
            score += 1;
        }

        if (previewMissing && senderMessageMatchCount >= 1)
        {
            score += 3;
        }

        if (previewMissing && senderMessageMatchCount >= 2)
        {
            score += 2;
        }

        if (nearbyOtherSenderCount >= 3)
        {
            score += 2;
        }

        return score;
    }

    private static int ScoreSupportingGroupHint(
        SynthesizedMessageRecord message,
        NotificationConversationHint hint)
    {
        var senderCandidates = BuildSenderCandidates(message.Message, message.Participants);
        var hasExplicitSenderEvidence = senderCandidates.Count > 0;
        var senderMatchesHint = senderCandidates.Any(candidate => NamesLikelyMatch(candidate, hint.AuthorDisplayName));
        if (hasExplicitSenderEvidence && !senderMatchesHint)
        {
            return int.MinValue;
        }

        var preview = BuildPreview(message.Message);
        var previewNormalized = NormalizeComparableText(preview);
        var reactionLike = LooksLikeReactionOrAttachmentText(preview);
        var groupCue = LooksLikeGroupContextPreview(preview);
        var hintReactionLike = LooksLikeReactionOrAttachmentText(hint.Preview);
        var previewIsStrong = HasStrongPreviewIdentity(preview) || HasStrongPreviewIdentity(hint.Preview);

        var score = senderMatchesHint ? 6 : 0;
        var hasContentCorrelation = false;

        if (!string.IsNullOrWhiteSpace(previewNormalized)
            && string.Equals(previewNormalized, hint.PreviewNormalized, StringComparison.Ordinal))
        {
            score += previewIsStrong ? 6 : 3;
            hasContentCorrelation = true;
        }
        else if (!string.IsNullOrWhiteSpace(previewNormalized)
                 && (previewNormalized.Contains(hint.PreviewNormalized, StringComparison.Ordinal)
                     || hint.PreviewNormalized.Contains(previewNormalized, StringComparison.Ordinal)))
        {
            score += previewIsStrong ? 4 : 2;
            hasContentCorrelation = true;
        }
        else if (ReactionSemanticsLikelyMatch(preview, hint.Preview))
        {
            score += 4;
            hasContentCorrelation = true;
        }
        else if (string.IsNullOrWhiteSpace(previewNormalized))
        {
            score += senderMatchesHint ? 2 : 0;
            hasContentCorrelation = senderMatchesHint;
        }
        else if (reactionLike && hintReactionLike)
        {
            score += senderMatchesHint ? 2 : 0;
            hasContentCorrelation = senderMatchesHint;
        }
        else if (groupCue
                 && senderMatchesHint
                 && (hint.GroupParticipantNames.Count > 0 || LooksLikeGroupDescriptor(hint.ThreadTitle)))
        {
            score += 3;
            hasContentCorrelation = true;
        }
        else if (groupCue && hintReactionLike && senderMatchesHint)
        {
            score += 2;
            hasContentCorrelation = true;
        }

        if (!hasContentCorrelation)
        {
            return int.MinValue;
        }

        var delta = CalculateMessageDelta(message.SortTimestampUtc, hint.ObservedUtc);
        if (delta <= TimeSpan.FromMinutes(10))
        {
            score += 3;
        }
        else if (delta <= TimeSpan.FromMinutes(30))
        {
            score += 2;
        }
        else if (delta <= TimeSpan.FromHours(3))
        {
            score += 1;
        }
        else if (!hint.IsActive)
        {
            score -= 1;
        }

        if (!hasExplicitSenderEvidence)
        {
            if (!previewIsStrong)
            {
                return int.MinValue;
            }

            if (delta > TimeSpan.FromMinutes(10))
            {
                return int.MinValue;
            }
        }

        if (hint.GroupParticipantNames.Count > 0 || LooksLikeGroupDescriptor(hint.ThreadTitle))
        {
            score += 1;
        }

        return score;
    }

    private static int ScoreHintBackedGroupBucketMatch(
        SynthesizedMessageRecord message,
        NotificationConversationHint hint,
        ConversationBucket bucket)
    {
        var score = 0;
        var bucketCore = BuildComparableGroupCore(bucket);
        if (!string.IsNullOrWhiteSpace(hint.GroupSeedNormalized)
            && string.Equals(bucketCore, hint.GroupSeedNormalized, StringComparison.Ordinal))
        {
            score += 8;
        }
        else
        {
            var overlap = hint.GroupParticipantNames.Count == 0
                ? 0
                : CountNameOverlap(bucket.Participants, hint.GroupParticipantNames);
            if (overlap >= 2)
            {
                score += 6;
            }
            else if (overlap >= 1)
            {
                score += 4;
            }
            else if (!string.IsNullOrWhiteSpace(hint.ThreadTitleNormalized)
                     && NamesLikelyMatch(bucket.DisplayName, hint.ThreadTitle))
            {
                score += 3;
            }
        }

        if (BucketHasMatchingParticipant(bucket, hint.AuthorDisplayName ?? string.Empty))
        {
            score += 2;
        }

        var delta = bucket.Messages
            .Select(candidate => CalculateMessageDelta(candidate.SortTimestampUtc, message.SortTimestampUtc))
            .Where(value => value is not null)
            .Select(value => value!.Value)
            .DefaultIfEmpty(TimeSpan.MaxValue)
            .Min();
        if (delta <= TimeSpan.FromMinutes(30))
        {
            score += 2;
        }
        else if (delta <= TimeSpan.FromHours(3))
        {
            score += 1;
        }

        return score;
    }

    private static int ScoreShadowDuplicateMatch(
        SynthesizedMessageRecord shadow,
        SynthesizedMessageRecord durable)
    {
        var delta = CalculateMessageDelta(shadow.SortTimestampUtc, durable.SortTimestampUtc);
        if (delta is null || delta > TimeSpan.FromMinutes(20))
        {
            return int.MinValue;
        }

        var shadowPreview = BuildPreview(shadow.Message);
        var durablePreview = BuildPreview(durable.Message);
        var previewMatch = PreviewsLikelyMatch(shadowPreview, durablePreview)
            || ReactionSemanticsLikelyMatch(shadowPreview, durablePreview);
        var sparseDurablePreview = string.IsNullOrWhiteSpace(NormalizeComparableText(durablePreview));
        var senderMatch = MessagesLikelyShareSender(shadow, durable);
        var participantOverlap = CountParticipantOverlap(shadow.Participants, durable.Participants);
        var sparseShadowMerge =
            !previewMatch
            && sparseDurablePreview
            && senderMatch
            && delta <= TimeSpan.FromMinutes(3)
            && participantOverlap >= 1;
        if (!previewMatch && !sparseShadowMerge)
        {
            return int.MinValue;
        }

        var score = previewMatch ? 8 : 6;
        if (delta <= TimeSpan.FromMinutes(2))
        {
            score += 4;
        }
        else if (delta <= TimeSpan.FromMinutes(10))
        {
            score += 2;
        }

        if (senderMatch)
        {
            score += 4;
        }

        if (participantOverlap >= 1)
        {
            score += 2;
        }

        if (sparseShadowMerge)
        {
            score += 3;
        }

        return score;
    }

    private static SynthesizedMessageRecord MergeShadowIntoDurableMessage(
        SynthesizedMessageRecord shadow,
        SynthesizedMessageRecord durable)
    {
        var mergedParticipants = MergeParticipants(durable.Participants, shadow.Participants);
        var shadowPreviewSource = SelectLongerMessageValue(shadow.Message.Body, shadow.Message.Subject);
        var durablePreviewSource = SelectLongerMessageValue(durable.Message.Body, durable.Message.Subject);
        var preferredPreview = ChooseBetterPreviewSource(durablePreviewSource, shadowPreviewSource);
        var senderName = ResolveMergedSenderName(durable, shadow, mergedParticipants);

        return durable with
        {
            ConversationDisplayName = durable.IsGroup
                ? durable.ConversationDisplayName
                : shadow.ConversationDisplayName.Length > durable.ConversationDisplayName.Length
                    ? shadow.ConversationDisplayName
                    : durable.ConversationDisplayName,
            IsGroup = durable.IsGroup || shadow.IsGroup,
            Participants = mergedParticipants,
            Message = durable.Message with
            {
                SenderName = senderName,
                Subject = preferredPreview,
                Body = preferredPreview,
                Read = durable.Message.Read ?? shadow.Message.Read,
                Status = string.IsNullOrWhiteSpace(durable.Message.Status) ? shadow.Message.Status : durable.Message.Status
            }
        };
    }

    private static string? ResolveMergedSenderName(
        SynthesizedMessageRecord durable,
        SynthesizedMessageRecord shadow,
        IReadOnlyList<ConversationParticipantRecord> mergedParticipants)
    {
        var mergedCandidates = BuildSenderCandidates(durable.Message, mergedParticipants)
            .Concat(BuildSenderCandidates(shadow.Message, mergedParticipants))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        foreach (var candidate in mergedCandidates)
        {
            var participantMatch = mergedParticipants
                .Where(participant => !participant.IsSelf)
                .FirstOrDefault(participant => ParticipantMatchesCandidate(participant, candidate));
            if (participantMatch is not null)
            {
                return participantMatch.DisplayName;
            }
        }

        return PreferResolvedSenderName(durable.Message.SenderName, shadow.Message.SenderName);
    }

    private static bool BucketsShareReactionReference(ConversationBucket left, ConversationBucket right)
    {
        foreach (var message in left.Messages)
        {
            var reference = TryExtractReactionReferencePreview(BuildPreview(message.Message));
            if (!string.IsNullOrWhiteSpace(reference)
                && right.Messages.Any(
                    candidate =>
                        !LooksLikeReactionPreview(BuildPreview(candidate.Message))
                        && PreviewTokensOverlap(BuildPreview(candidate.Message), reference!)))
            {
                return true;
            }
        }

        foreach (var message in right.Messages)
        {
            var reference = TryExtractReactionReferencePreview(BuildPreview(message.Message));
            if (!string.IsNullOrWhiteSpace(reference)
                && left.Messages.Any(
                    candidate =>
                        !LooksLikeReactionPreview(BuildPreview(candidate.Message))
                        && PreviewTokensOverlap(BuildPreview(candidate.Message), reference!)))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsEstablishedLargeGroupBucket(ConversationBucket bucket)
    {
        return bucket.Participants.Count(participant => !participant.IsSelf) >= 5
            && (LooksLikeGroupDescriptor(bucket.DisplayName)
                || bucket.Messages.Any(message => IsNotificationBacked(message.Message)));
    }

    private static bool IsEstablishedHintBackedGroupBucket(ConversationBucket bucket)
    {
        return bucket.Participants.Count(participant => !participant.IsSelf) >= 3
            && (LooksLikeGroupDescriptor(bucket.DisplayName)
                || bucket.Messages.Any(message => IsNotificationBacked(message.Message)));
    }

    private static bool IsEstablishedDirectBucket(ConversationBucket bucket)
    {
        return !bucket.IsGroup
            && bucket.Participants.Count(participant => !participant.IsSelf) == 1
            && bucket.Messages.Count >= 2
            && (bucket.Messages.Any(IsOutboundConversationMessage)
                || bucket.Messages.Count(message => !IsNotificationBacked(message.Message)) >= 2);
    }

    private static bool IsEligibleSparseGroupCandidate(SynthesizedMessageRecord message)
    {
        if (message.IsGroup)
        {
            return false;
        }

        if (!string.Equals(message.Message.Folder, "inbox", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (message.Participants.Count(participant => !participant.IsSelf) != 1)
        {
            return false;
        }

        return RecipientsLackParticipantIdentity(message.Message);
    }

    private static bool RecipientsLackParticipantIdentity(MessageRecord message)
    {
        return message.Recipients.All(
            recipient =>
                string.IsNullOrWhiteSpace(recipient.Name)
                || string.Equals(recipient.Name, "(unnamed)", StringComparison.OrdinalIgnoreCase)
                || recipient.Phones.Count == 0 && recipient.Emails.Count == 0);
    }

    private static bool IsNotificationBacked(MessageRecord message)
    {
        return string.Equals(message.Folder, "notification", StringComparison.OrdinalIgnoreCase)
            || string.Equals(message.Type, "ANCS", StringComparison.OrdinalIgnoreCase)
            || string.Equals(message.MessageType, "ANCS", StringComparison.OrdinalIgnoreCase);
    }

    private static ConversationBucket SelectPreferredBucket(
        ConversationBucket left,
        ConversationBucket right)
    {
        return new[] { left, right }
            .OrderByDescending(bucket => bucket.Messages.Count(message => !LooksLikeReactionPreview(BuildPreview(message.Message))))
            .ThenByDescending(bucket => bucket.Messages.Count)
            .ThenByDescending(bucket => bucket.Participants.Count)
            .ThenByDescending(bucket => bucket.LastMessageUtc ?? DateTimeOffset.MinValue)
            .ThenBy(bucket => bucket.ConversationId, StringComparer.OrdinalIgnoreCase)
            .First();
    }

    private static string? BuildComparableGroupCore(ConversationBucket bucket)
    {
        if (LooksLikeGroupDescriptor(bucket.DisplayName))
        {
            var descriptor = ParseGroupDescriptor(bucket.DisplayName);
            var descriptorMembers = descriptor.NormalizedMembers
                .Take(2)
                .ToArray();
            if ((descriptor.HasOthersCount || descriptor.WasTruncated || descriptor.EstimatedExternalCount >= 4)
                && descriptorMembers.Length >= 2)
            {
                return $"members:{string.Join("|", descriptorMembers.OrderBy(value => value, StringComparer.Ordinal))}|large";
            }

            if (descriptorMembers.Length >= 2)
            {
                return $"members:{string.Join("|", descriptorMembers.OrderBy(value => value, StringComparer.Ordinal))}";
            }
        }

        var normalizedTitle = NormalizeComparableText(bucket.DisplayName);
        if (!string.IsNullOrWhiteSpace(normalizedTitle)
            && !LooksLikeUnresolvedDisplayName(bucket.DisplayName))
        {
            return $"title:{normalizedTitle}";
        }

        var participantCore = bucket.Participants
            .Where(participant => !participant.IsSelf)
            .Select(NormalizeParticipantIdentity)
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .Select(value => value!)
            .Take(3)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(value => value, StringComparer.Ordinal)
            .ToArray();
        return participantCore.Length >= 2
            ? $"members:{string.Join("|", participantCore)}"
            : null;
    }

    private static int CountParticipantOverlap(
        IReadOnlyList<ConversationParticipantRecord> left,
        IReadOnlyList<ConversationParticipantRecord> right)
    {
        var rightParticipants = right.Where(participant => !participant.IsSelf).ToArray();
        return left
            .Where(participant => !participant.IsSelf)
            .Count(leftParticipant => rightParticipants.Any(rightParticipant => ParticipantsLikelyMatch(leftParticipant, rightParticipant)));
    }

    private static bool ParticipantMatchesCandidate(
        ConversationParticipantRecord participant,
        string candidate)
    {
        if (NamesLikelyMatch(candidate, participant.DisplayName))
        {
            return true;
        }

        var normalizedPhone = PhoneNumberNormalizer.Normalize(candidate);
        if (!string.IsNullOrWhiteSpace(normalizedPhone)
            && participant.Phones.Any(
                phone => string.Equals(phone, normalizedPhone, StringComparison.OrdinalIgnoreCase)))
        {
            return true;
        }

        if (TryExtractEmail(candidate, out var email)
            && participant.Emails.Any(
                value => string.Equals(value, email, StringComparison.OrdinalIgnoreCase)))
        {
            return true;
        }

        return false;
    }

    private static bool MessagesShareDirectCounterparty(
        string counterpartyCandidate,
        SynthesizedMessageRecord message)
    {
        return BuildDirectCounterpartyCandidates(message.Participants)
            .Any(candidate => IdentityCandidatesLikelyMatch(counterpartyCandidate, candidate));
    }

    private static bool BucketHasMatchingParticipant(
        ConversationBucket bucket,
        string candidate)
    {
        return bucket.Participants
            .Where(participant => !participant.IsSelf)
            .Any(participant => ParticipantMatchesCandidate(participant, candidate));
    }

    private static int CountMessagesFromSender(
        ConversationBucket bucket,
        string candidate)
    {
        return bucket.Messages.Count(
            message =>
                ProvidesSubstantiveSenderContinuity(message)
                && BuildSenderCandidates(message.Message, message.Participants)
                    .Any(senderCandidate => IdentityCandidatesLikelyMatch(candidate, senderCandidate)));
    }

    private static bool ProvidesSubstantiveSenderContinuity(
        SynthesizedMessageRecord message)
    {
        var preview = BuildPreview(message.Message);
        if (string.IsNullOrWhiteSpace(NormalizeComparableText(preview)))
        {
            return false;
        }

        return !LooksLikeReactionOrAttachmentText(preview);
    }

    private static int CountNearbyDistinctSenders(
        ConversationBucket bucket,
        DateTimeOffset? referenceUtc,
        TimeSpan window,
        IReadOnlyList<string> excludedCandidates)
    {
        if (!referenceUtc.HasValue)
        {
            return 0;
        }

        var excludedKeys = excludedCandidates
            .Select(NormalizeSenderKey)
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .Select(value => value!)
            .ToHashSet(StringComparer.Ordinal);

        return bucket.Messages
            .Where(
                message =>
                    message.SortTimestampUtc.HasValue
                    && (message.SortTimestampUtc.Value - referenceUtc.Value).Duration() <= window)
            .SelectMany(message => BuildSenderIdentityKeys(message.Message, message.Participants))
            .Where(value => !excludedKeys.Contains(value))
            .Distinct(StringComparer.Ordinal)
            .Count();
    }

    private static IReadOnlyList<string> BuildSenderIdentityKeys(
        MessageRecord message,
        IReadOnlyList<ConversationParticipantRecord> participants)
    {
        var keys = new HashSet<string>(StringComparer.Ordinal);

        foreach (var participant in participants.Where(participant => !participant.IsSelf))
        {
            if (!ParticipantMatchesSenderEvidence(participant, message))
            {
                continue;
            }

            var participantIdentity = NormalizeParticipantIdentity(participant);
            if (!string.IsNullOrWhiteSpace(participantIdentity))
            {
                keys.Add(participantIdentity!);
            }
        }

        if (keys.Count > 0)
        {
            return keys.ToArray();
        }

        foreach (var candidate in BuildSenderCandidates(message, participants))
        {
            var senderKey = NormalizeSenderKey(candidate);
            if (!string.IsNullOrWhiteSpace(senderKey))
            {
                keys.Add(senderKey!);
            }
        }

        return keys.ToArray();
    }

    private static int ScoreExistingDirectBucketAffinity(
        SynthesizedMessageRecord message,
        ConversationBucket? bucket)
    {
        if (bucket is null || bucket.IsGroup)
        {
            return 0;
        }

        var neighboringMessages = bucket.Messages
            .Where(candidate => !string.Equals(candidate.MessageKey, message.MessageKey, StringComparison.OrdinalIgnoreCase))
            .ToArray();
        if (neighboringMessages.Length == 0)
        {
            return 0;
        }

        var counterpartyCandidates = BuildDirectCounterpartyCandidates(message.Participants);
        var sameCounterpartyCount = neighboringMessages.Count(
            candidate => counterpartyCandidates.Any(counterparty => MessagesShareDirectCounterparty(counterparty, candidate)));
        var closestDelta = neighboringMessages
            .Select(candidate => CalculateMessageDelta(message.SortTimestampUtc, candidate.SortTimestampUtc))
            .Where(delta => delta is not null)
            .Select(delta => delta!.Value)
            .DefaultIfEmpty(TimeSpan.MaxValue)
            .Min();

        var score = 0;
        if (sameCounterpartyCount > 0)
        {
            score += Math.Min(6, sameCounterpartyCount * 2);
        }

        if (closestDelta <= TimeSpan.FromMinutes(10))
        {
            score += 4;
        }
        else if (closestDelta <= TimeSpan.FromMinutes(30))
        {
            score += 3;
        }
        else if (closestDelta <= TimeSpan.FromHours(2))
        {
            score += 2;
        }
        else if (closestDelta <= TimeSpan.FromHours(6))
        {
            score += 1;
        }

        if (bucket.Participants.Count(participant => !participant.IsSelf) == 1)
        {
            score += 2;
        }

        var preview = BuildPreview(message.Message);
        var previewMissing = string.IsNullOrWhiteSpace(NormalizeComparableText(preview));
        var plainDirectText = !previewMissing
            && !LooksLikeReactionOrAttachmentText(preview)
            && !LooksLikeGroupContextPreview(preview);
        if (plainDirectText)
        {
            if (sameCounterpartyCount >= 2)
            {
                score += 4;
            }

            if (neighboringMessages.Any(IsOutboundConversationMessage))
            {
                score += 4;
            }

            if (neighboringMessages.Length >= 3)
            {
                score += 2;
            }
        }

        var referencePreview = TryExtractReactionReferencePreview(BuildPreview(message.Message));
        if (!string.IsNullOrWhiteSpace(referencePreview)
            && neighboringMessages.Any(
                candidate =>
                    !LooksLikeReactionPreview(BuildPreview(candidate.Message))
                    && PreviewTokensOverlap(BuildPreview(candidate.Message), referencePreview)))
        {
            score += 8;
        }

        return score;
    }

    private static bool IsOutboundConversationMessage(SynthesizedMessageRecord message)
    {
        return message.Message.Sent == true
            || string.Equals(message.Message.Folder, "sent", StringComparison.OrdinalIgnoreCase)
            || string.Equals(message.Message.Folder, "outbox", StringComparison.OrdinalIgnoreCase);
    }

    private static bool MessagesShareSender(
        string senderCandidate,
        SynthesizedMessageRecord message)
    {
        return BuildSenderCandidates(message.Message, message.Participants)
            .Any(candidate => IdentityCandidatesLikelyMatch(senderCandidate, candidate));
    }

    private static string? NormalizeSenderKey(string? candidate)
    {
        if (string.IsNullOrWhiteSpace(candidate))
        {
            return null;
        }

        var normalizedPhone = PhoneNumberNormalizer.Normalize(candidate);
        if (!string.IsNullOrWhiteSpace(normalizedPhone))
        {
            return $"phone:{normalizedPhone}";
        }

        var normalizedName = NormalizeComparableText(candidate);
        return string.IsNullOrWhiteSpace(normalizedName)
            ? null
            : $"name:{normalizedName}";
    }

    private static bool IdentityCandidatesLikelyMatch(
        string left,
        string right)
    {
        if (NamesLikelyMatch(left, right))
        {
            return true;
        }

        var normalizedLeftPhone = PhoneNumberNormalizer.Normalize(left);
        var normalizedRightPhone = PhoneNumberNormalizer.Normalize(right);
        if (!string.IsNullOrWhiteSpace(normalizedLeftPhone)
            && string.Equals(normalizedLeftPhone, normalizedRightPhone, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return TryExtractEmail(left, out var leftEmail)
            && TryExtractEmail(right, out var rightEmail)
            && string.Equals(leftEmail, rightEmail, StringComparison.OrdinalIgnoreCase);
    }

    private static bool ParticipantsLikelyMatch(
        ConversationParticipantRecord left,
        ConversationParticipantRecord right)
    {
        if (string.Equals(left.Key, right.Key, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (left.Phones.Any(phone => right.Phones.Contains(phone, StringComparer.OrdinalIgnoreCase)))
        {
            return true;
        }

        if (left.Emails.Any(email => right.Emails.Contains(email, StringComparer.OrdinalIgnoreCase)))
        {
            return true;
        }

        return NamesLikelyMatch(left.DisplayName, right.DisplayName);
    }

    private static IReadOnlyList<ConversationParticipantRecord> MergeParticipants(
        IEnumerable<ConversationParticipantRecord> left,
        IEnumerable<ConversationParticipantRecord> right)
    {
        return left
            .Concat(right)
            .GroupBy(participant => participant.Key, StringComparer.OrdinalIgnoreCase)
            .Select(
                group =>
                {
                    var preferred = group
                        .OrderByDescending(participant => participant.Phones.Count + participant.Emails.Count)
                        .ThenByDescending(participant => participant.DisplayName.Length)
                        .First();
                    return preferred with
                    {
                        Phones = group
                            .SelectMany(participant => participant.Phones)
                            .Distinct(StringComparer.OrdinalIgnoreCase)
                            .OrderBy(phone => phone, StringComparer.OrdinalIgnoreCase)
                            .ToArray(),
                        Emails = group
                            .SelectMany(participant => participant.Emails)
                            .Distinct(StringComparer.OrdinalIgnoreCase)
                            .OrderBy(email => email, StringComparer.OrdinalIgnoreCase)
                            .ToArray()
                    };
                })
            .OrderBy(participant => participant.DisplayName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(participant => participant.Key, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static string? TryExtractReactionReferencePreview(string? preview)
    {
        if (!LooksLikeReactionPreview(preview) || string.IsNullOrWhiteSpace(preview))
        {
            return null;
        }

        var openIndex = preview.IndexOf('“');
        var closeIndex = preview.LastIndexOf('”');
        if (openIndex < 0 || closeIndex <= openIndex)
        {
            openIndex = preview.IndexOf('"');
            closeIndex = preview.LastIndexOf('"');
        }

        if (openIndex < 0 || closeIndex <= openIndex)
        {
            return null;
        }

        return NormalizeComparableText(preview[(openIndex + 1)..closeIndex]);
    }

    private static bool LooksLikeReactionPreview(string? preview)
    {
        var normalized = NormalizeComparableText(preview);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return false;
        }

        return normalized.Contains(" laughed at ", StringComparison.Ordinal)
            || normalized.StartsWith("laughed at ", StringComparison.Ordinal)
            || normalized.Contains(" loved ", StringComparison.Ordinal)
            || normalized.StartsWith("loved ", StringComparison.Ordinal)
            || normalized.Contains(" liked ", StringComparison.Ordinal)
            || normalized.StartsWith("liked ", StringComparison.Ordinal)
            || normalized.Contains(" disliked ", StringComparison.Ordinal)
            || normalized.StartsWith("disliked ", StringComparison.Ordinal)
            || normalized.Contains(" emphasized ", StringComparison.Ordinal)
            || normalized.StartsWith("emphasized ", StringComparison.Ordinal)
            || normalized.Contains(" questioned ", StringComparison.Ordinal)
            || normalized.StartsWith("questioned ", StringComparison.Ordinal)
            || normalized.Contains(" reacted ", StringComparison.Ordinal)
            || LooksLikeSymbolReactionPreview(preview);
    }

    private static bool LooksLikeGroupContextPreview(string? preview)
    {
        var normalized = NormalizeComparableText(preview);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return false;
        }

        return normalized.Contains("guys", StringComparison.Ordinal)
            || normalized.Contains("everyone", StringComparison.Ordinal)
            || normalized.Contains("everybody", StringComparison.Ordinal)
            || normalized.Contains("all of you", StringComparison.Ordinal)
            || normalized.Contains("yall", StringComparison.Ordinal)
            || normalized.Contains("y all", StringComparison.Ordinal)
            || normalized.Contains("them", StringComparison.Ordinal);
    }

    private static bool LooksLikeLowSignalReplyPreview(string? preview)
    {
        var normalized = NormalizeComparableText(preview);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return false;
        }

        if (LooksLikeReactionOrAttachmentText(preview)
            || LooksLikeGroupContextPreview(preview)
            || HasStrongPreviewIdentity(preview))
        {
            return false;
        }

        var tokens = normalized.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return tokens.Length <= 2 && normalized.Length <= 16;
    }

    private static bool PreviewTokensOverlap(string? preview, string referencePreviewNormalized)
    {
        var normalized = NormalizeComparableText(preview);
        return !string.IsNullOrWhiteSpace(normalized)
            && (normalized.Contains(referencePreviewNormalized, StringComparison.Ordinal)
                || referencePreviewNormalized.Contains(normalized, StringComparison.Ordinal));
    }

    private static bool PreviewsLikelyMatch(string? left, string? right)
    {
        var normalizedLeft = NormalizeComparableText(left);
        var normalizedRight = NormalizeComparableText(right);
        if (string.IsNullOrWhiteSpace(normalizedLeft) || string.IsNullOrWhiteSpace(normalizedRight))
        {
            return false;
        }

        return string.Equals(normalizedLeft, normalizedRight, StringComparison.Ordinal)
            || normalizedLeft.Contains(normalizedRight, StringComparison.Ordinal)
            || normalizedRight.Contains(normalizedLeft, StringComparison.Ordinal)
            || PreviewTokensOverlap(left, normalizedRight!);
    }

    private static bool ReactionSemanticsLikelyMatch(string? left, string? right)
    {
        var leftSignature = BuildReactionSignature(left);
        var rightSignature = BuildReactionSignature(right);
        if (string.IsNullOrWhiteSpace(leftSignature) || string.IsNullOrWhiteSpace(rightSignature))
        {
            return false;
        }

        return string.Equals(leftSignature, rightSignature, StringComparison.Ordinal)
            || leftSignature.StartsWith(rightSignature, StringComparison.Ordinal)
            || rightSignature.StartsWith(leftSignature, StringComparison.Ordinal);
    }

    private static string? BuildReactionSignature(string? preview)
    {
        var normalized = NormalizeComparableText(preview);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return null;
        }

        var action = normalized.Contains("laughed at", StringComparison.Ordinal)
            ? "laughed"
            : normalized.Contains("loved", StringComparison.Ordinal)
                ? "loved"
                : normalized.Contains("liked", StringComparison.Ordinal)
                    ? "liked"
                    : normalized.Contains("disliked", StringComparison.Ordinal)
                        ? "disliked"
                        : normalized.Contains("reacted", StringComparison.Ordinal)
                            ? "reacted"
                            : normalized.Contains("emphasized", StringComparison.Ordinal)
                                ? "emphasized"
                                : normalized.Contains("questioned", StringComparison.Ordinal)
                                    ? "questioned"
                                    : LooksLikeSymbolReactionPreview(preview)
                                        ? "reacted"
                                    : null;
        if (action is null)
        {
            return null;
        }

        var target = normalized.Contains("attachment", StringComparison.Ordinal)
            ? "attachment"
            : normalized.Contains("image", StringComparison.Ordinal) || normalized.Contains("photo", StringComparison.Ordinal)
                ? "image"
                : TryExtractReactionReferencePreview(preview) is { Length: > 0 }
                    ? "text"
                    : "generic";
        return $"{action}:{target}";
    }

    private static bool LooksLikeSymbolReactionPreview(string? preview)
    {
        if (string.IsNullOrWhiteSpace(preview))
        {
            return false;
        }

        var sanitized = preview
            .Replace("\u00A0", " ", StringComparison.Ordinal)
            .Replace("\u200B", string.Empty, StringComparison.Ordinal)
            .Replace("\u200C", string.Empty, StringComparison.Ordinal)
            .Replace("\u200D", string.Empty, StringComparison.Ordinal)
            .Replace("\u2009", " ", StringComparison.Ordinal)
            .Replace("\u200A", " ", StringComparison.Ordinal)
            .Replace("\u202F", " ", StringComparison.Ordinal)
            .Replace("\u2060", string.Empty, StringComparison.Ordinal)
            .Replace("\uFE0F", string.Empty, StringComparison.Ordinal)
            .Trim();
        if (string.IsNullOrWhiteSpace(sanitized))
        {
            return false;
        }

        return Regex.IsMatch(
            sanitized,
            "^\\s*[^\\p{L}\\p{Nd}]{1,16}\\s+to\\s+[\\\"“”]",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    }

    private static TimeSpan? CalculateMessageDelta(
        DateTimeOffset? left,
        DateTimeOffset? right)
    {
        if (!left.HasValue || !right.HasValue)
        {
            return null;
        }

        return (left.Value - right.Value).Duration();
    }

    private static int CountNameOverlap(
        IReadOnlyList<ConversationParticipantRecord> participants,
        IReadOnlyList<string> names)
    {
        var normalizedNames = names
            .Select(NormalizeComparableText)
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .Select(value => value!)
            .ToHashSet(StringComparer.Ordinal);

        return participants
            .Where(participant => !participant.IsSelf)
            .Select(participant => NormalizeComparableText(participant.DisplayName))
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .Select(value => value!)
            .Count(normalizedNames.Contains);
    }

    private static bool PreviewMentionsBucketParticipant(
        string? preview,
        ConversationBucket bucket)
    {
        var normalizedPreview = NormalizeComparableText(preview);
        if (string.IsNullOrWhiteSpace(normalizedPreview))
        {
            return false;
        }

        return bucket.Participants
            .Where(participant => !participant.IsSelf)
            .Select(participant => NormalizeComparableText(participant.DisplayName))
            .Where(name => !string.IsNullOrWhiteSpace(name) && name!.Length >= 4)
            .Select(name => name!)
            .Any(name => normalizedPreview.Contains(name, StringComparison.Ordinal));
    }

    private static bool MessagesLikelyShareSender(
        SynthesizedMessageRecord left,
        SynthesizedMessageRecord right)
    {
        var leftCandidates = BuildSenderCandidates(left.Message, left.Participants);
        var rightCandidates = BuildSenderCandidates(right.Message, right.Participants);

        foreach (var leftCandidate in leftCandidates)
        {
            if (rightCandidates.Any(rightCandidate => IdentityCandidatesLikelyMatch(leftCandidate, rightCandidate)))
            {
                return true;
            }
        }

        return false;
    }

    private static string? SelectLongerMessageValue(string? primary, string? secondary)
    {
        if (string.IsNullOrWhiteSpace(primary))
        {
            return secondary;
        }

        if (string.IsNullOrWhiteSpace(secondary))
        {
            return primary;
        }

        return primary.Length >= secondary.Length ? primary : secondary;
    }

    private static string? ChooseBetterPreviewSource(string? preferred, string? alternate)
    {
        if (string.IsNullOrWhiteSpace(preferred))
        {
            return alternate;
        }

        if (string.IsNullOrWhiteSpace(alternate))
        {
            return preferred;
        }

        if (alternate.Length > preferred.Length
            && (preferred.Contains(alternate, StringComparison.Ordinal)
                || alternate.Contains(preferred, StringComparison.Ordinal)))
        {
            return alternate;
        }

        return preferred;
    }

    private static string? PreferResolvedSenderName(string? primary, string? alternate)
    {
        var primaryResolved = !string.IsNullOrWhiteSpace(primary) && !LooksLikeUnresolvedDisplayName(primary);
        var alternateResolved = !string.IsNullOrWhiteSpace(alternate) && !LooksLikeUnresolvedDisplayName(alternate);

        if (!primaryResolved && alternateResolved)
        {
            return alternate;
        }

        if (primaryResolved)
        {
            return primary;
        }

        return !string.IsNullOrWhiteSpace(primary) ? primary : alternate;
    }

    private static string? NormalizeParticipantIdentity(ConversationParticipantRecord participant)
    {
        var phone = participant.Phones.FirstOrDefault();
        if (!string.IsNullOrWhiteSpace(phone))
        {
            return $"phone:{phone}";
        }

        var email = participant.Emails.FirstOrDefault();
        if (!string.IsNullOrWhiteSpace(email))
        {
            return $"email:{email.ToLowerInvariant()}";
        }

        return NormalizeComparableText(participant.DisplayName);
    }

    private static string? BuildGroupDisplayName(
        IReadOnlyList<string> groupParticipantNames,
        IReadOnlyList<ConversationParticipantRecord> participants)
    {
        var orderedNames = groupParticipantNames
            .Select(name => name.Trim())
            .Where(name => !string.IsNullOrWhiteSpace(name))
            .ToArray();
        if (orderedNames.Length == 0)
        {
            orderedNames = participants
                .Where(participant => !participant.IsSelf)
                .Select(participant => participant.DisplayName)
                .Where(name => !string.IsNullOrWhiteSpace(name))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }

        if (orderedNames.Length == 0)
        {
            return null;
        }

        if (orderedNames.Length <= 3)
        {
            return string.Join(", ", orderedNames);
        }

        return $"{string.Join(", ", orderedNames.Take(3))} +{orderedNames.Length - 3}";
    }

    private static NotificationIdentity CreateNotificationIdentity(ContactRecord contact)
    {
        var displayName = !string.IsNullOrWhiteSpace(contact.DisplayName)
            ? contact.DisplayName
            : "(unknown)";
        var phones = contact.Phones
            .Select(phone => phone.Normalized ?? PhoneNumberNormalizer.Normalize(phone.Raw) ?? phone.Raw)
            .Where(phone => !string.IsNullOrWhiteSpace(phone))
            .Select(phone => phone!)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(phone => phone, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var emails = contact.Emails
            .Where(emailValue => !string.IsNullOrWhiteSpace(emailValue))
            .Select(emailValue => emailValue.Trim().ToLowerInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(emailValue => emailValue, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (phones.Length > 1 || emails.Length > 1)
        {
            return new NotificationIdentity(
                displayName,
                null,
                Array.Empty<string>(),
                Array.Empty<string>(),
                $"name:{displayName}");
        }

        var primaryAddressing = phones.FirstOrDefault()
            ?? emails.FirstOrDefault()
            ?? $"name:{displayName}";

        return new NotificationIdentity(displayName, phones.FirstOrDefault(), phones, emails, primaryAddressing);
    }

    private static ContactRecord? FindSingleFuzzyContactMatch(
        string? normalizedName,
        ContactIndex contactIndex)
    {
        if (string.IsNullOrWhiteSpace(normalizedName) || normalizedName.Length < 4)
        {
            return null;
        }

        var matches = contactIndex.ContactsByComparableName
            .Where(
                pair =>
                    pair.Key.StartsWith(normalizedName, StringComparison.Ordinal)
                    || normalizedName.StartsWith(pair.Key, StringComparison.Ordinal))
            .SelectMany(pair => pair.Value)
            .GroupBy(
                contact => !string.IsNullOrWhiteSpace(contact.UniqueIdentifier)
                    ? contact.UniqueIdentifier!
                    : contact.DisplayName,
                StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .ToArray();
        return matches.Length == 1 ? matches[0] : null;
    }

    private static string? NormalizeComparableText(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var builder = new StringBuilder(value.Length);
        foreach (var character in value.Trim().ToLowerInvariant())
        {
            if (char.IsLetterOrDigit(character))
            {
                builder.Append(character);
            }
            else if (builder.Length > 0 && builder[^1] != ' ')
            {
                builder.Append(' ');
            }
        }

        return builder
            .ToString()
            .Trim();
    }

    private static string NormalizeAddressForIdentity(string? address)
    {
        if (string.IsNullOrWhiteSpace(address))
        {
            return string.Empty;
        }

        if (TryExtractEmail(address, out var email))
        {
            return $"email:{email.ToLowerInvariant()}";
        }

        var stripped = StripAddressPrefix(address);
        var normalizedPhone = PhoneNumberNormalizer.Normalize(stripped);
        if (!string.IsNullOrWhiteSpace(normalizedPhone))
        {
            return $"phone:{normalizedPhone}";
        }

        return stripped.Trim().ToLowerInvariant();
    }

    private static bool TryExtractEmail(string raw, out string email)
    {
        var stripped = StripAddressPrefix(raw);
        if (!stripped.Contains('@', StringComparison.Ordinal))
        {
            email = string.Empty;
            return false;
        }

        email = stripped.Trim().ToLowerInvariant();
        return email.Length > 0;
    }

    private static string StripAddressPrefix(string raw)
    {
        var trimmed = raw.Trim();
        foreach (var prefix in AddressPrefixes)
        {
            if (trimmed.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            {
                return trimmed[prefix.Length..].Trim();
            }
        }

        return trimmed;
    }

    private static readonly string[] AddressPrefixes =
    [
        "e:",
        "email:",
        "mailto:",
        "name:",
        "tel:",
        "sms:",
        "phone:"
    ];

    private sealed record ContactIndex(
        IReadOnlyDictionary<string, string> NameByPhone,
        IReadOnlyDictionary<string, string> NameByEmail,
        IReadOnlyDictionary<string, IReadOnlyList<ContactRecord>> ContactsByComparableName);

    private sealed record SelfIdentity(
        IReadOnlyList<string> Phones,
        IReadOnlyList<string> Emails,
        string DisplayName,
        IReadOnlySet<string> PhoneSet,
        IReadOnlySet<string> EmailSet);

    private sealed record NotificationConversationHint(
        uint NotificationUid,
        bool IsActive,
        DateTimeOffset ObservedUtc,
        string ThreadTitle,
        string ThreadTitleNormalized,
        string? AuthorDisplayName,
        string? AuthorDisplayNameNormalized,
        string? GroupSeedNormalized,
        IReadOnlyList<string> GroupParticipantNames,
        string Preview,
        string PreviewNormalized,
        bool IsGroupHint);

    private sealed record NotificationIdentity(
        string DisplayName,
        string? RawPhone,
        IReadOnlyList<string> Phones,
        IReadOnlyList<string> Emails,
        string PrimaryAddressing);

    private sealed record GroupDescriptor(
        IReadOnlyList<string> DisplayMembers,
        IReadOnlyList<string> NormalizedMembers,
        bool HasOthersCount,
        bool WasTruncated,
        int EstimatedExternalCount);

    private sealed record ConversationBucket(
        string ConversationId,
        string DisplayName,
        bool IsGroup,
        IReadOnlyList<ConversationParticipantRecord> Participants,
        IReadOnlyList<SynthesizedMessageRecord> Messages,
        DateTimeOffset? LastMessageUtc);

    private sealed record SynthesizedMapMessage(
        SynthesizedMessageRecord Message,
        NotificationConversationHint? MatchedHint,
        bool DeferredDirectIdentity);

    private enum MessageDirection
    {
        Unknown = 0,
        Inbound = 1,
        Outbound = 2
    }

    private enum ParticipantRole
    {
        Originator = 0,
        Recipient = 1,
        SenderAddressing = 2,
        RecipientAddressing = 3
    }

    private sealed class MutableParticipant
    {
        public MutableParticipant(string key, string displayName)
        {
            Key = key;
            DisplayName = displayName;
        }

        public string Key { get; }

        public string DisplayName { get; set; }

        public bool IsSelf { get; set; }

        public bool HasSenderSideRole { get; set; }

        public bool HasRecipientSideRole { get; set; }

        public bool InConversationScope { get; set; }

        public HashSet<string> Phones { get; } = new(StringComparer.OrdinalIgnoreCase);

        public HashSet<string> Emails { get; } = new(StringComparer.OrdinalIgnoreCase);
    }
}
