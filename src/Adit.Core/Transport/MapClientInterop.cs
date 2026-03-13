using System.Globalization;
using System.Reflection;
using Adit.Core.Models;
using Microsoft.Internal.Bluetooth.Map;
using Microsoft.Internal.Bluetooth.Map.BMessage;
using Microsoft.Internal.Bluetooth.Map.Model;
using Microsoft.Internal.Bluetooth.Map.Request;
using MixERP.Net.VCards;
using MixERP.Net.VCards.Models;
using MixERP.Net.VCards.Types;

namespace Adit.Core.Transport;

internal static class MapClientInterop
{
    internal static readonly string[] DefaultFolders = ["inbox", "sent", "outbox", "deleted"];

    public static async Task NavigateToMessagesRootAsync(
        MapClient client,
        Microsoft.Internal.Diagnostics.Context.ITraceContext traceContext,
        CancellationToken cancellationToken)
    {
        await client.SetFolderAsync(CreateSetFolderRequest(string.Empty, "Root"), traceContext, cancellationToken);
        await client.SetFolderAsync(CreateSetFolderRequest("telecom", "Down"), traceContext, cancellationToken);
        await client.SetFolderAsync(CreateSetFolderRequest("msg", "Down"), traceContext, cancellationToken);
    }

    public static GetMessagesListingRequestParameters CreateMessagesListingRequest(string folderName, int limit)
    {
        return new GetMessagesListingRequestParameters
        {
            Name = folderName,
            MaxListCount = checked((ushort)Math.Min(limit, ushort.MaxValue)),
            ListStartOffset = 0,
            SubjectLength = 256,
            ParameterMask = (ParameterMask)0x001FFFFFu
        };
    }

    public static SetFolderRequestParameters CreateSetFolderRequest(string folderName, string flagName)
    {
        var request = new SetFolderRequestParameters();
        SetEnumProperty(request, nameof(SetFolderRequestParameters.Flags), flagName);
        if (!string.IsNullOrEmpty(folderName))
        {
            request.Name = folderName;
        }

        return request;
    }

    public static GetMessageRequestParameters CreateGetMessageRequest(string handle)
    {
        var request = new GetMessageRequestParameters
        {
            Name = handle
        };

        SetEnumProperty(request, nameof(GetMessageRequestParameters.Charset), "Utf8");
        SetEnumProperty(request, nameof(GetMessageRequestParameters.Attachment), "On");
        return request;
    }

    public static PushMessageRequestParameters CreatePushMessageRequest(string recipient, string body)
    {
        var request = new PushMessageRequestParameters
        {
            Name = "outbox",
            Message = CreateBMessage(recipient, body)
        };

        SetEnumProperty(request, nameof(PushMessageRequestParameters.Charset), "Utf8");
        request.Transparent = MessageTransparentType.Off;
        request.Retry = MessageRetryType.On;
        return request;
    }

    public static BMessage CreateBMessage(string recipient, string body)
    {
        return new BMessage
        {
            Recipients =
            [
                new VCard
                {
                    Telephones =
                    [
                        new Telephone
                        {
                            Number = recipient,
                            Preference = -1,
                            Type = TelephoneType.Personal
                        }
                    ]
                }
            ],
            BodyContent = new BMessageBodyContent
            {
                Content = body
            },
            Charset = BMessageCharset.Utf8,
            MessageType = BMessageType.SMSGSM,
            Status = BMessageStatus.Read
        };
    }

    public static MessageRecord ToMessageRecord(
        string folderName,
        MessageListingEntry entry,
        BMessage? detailMessage)
    {
        return new MessageRecord(
            folderName,
            entry.Handle,
            entry.Type,
            entry.Subject,
            entry.Datetime,
            entry.SenderName,
            entry.SenderAddressing,
            entry.RecipientAddressing,
            TryToUInt32(entry.Size),
            TryToUInt32(entry.AttachmentSize),
            entry.Priority?.ToString(),
            TryToBoolean(entry.Read),
            TryToBoolean(entry.Sent),
            TryToBoolean(entry.Protected),
            Truncate(detailMessage?.BodyContent?.Content, 2048),
            detailMessage?.MessageType?.ToString(),
            detailMessage?.Status?.ToString(),
            SummarizeParticipants(detailMessage?.Originators),
            SummarizeParticipants(detailMessage?.Recipients));
    }

    public static MessageRecord ToMessageRecord(
        string folderName,
        string? handle,
        string? type,
        BMessage? detailMessage)
    {
        var firstOriginator = detailMessage?.Originators?.FirstOrDefault();
        var firstRecipient = detailMessage?.Recipients?.FirstOrDefault();
        return new MessageRecord(
            folderName,
            handle,
            type,
            null,
            null,
            firstOriginator is null ? null : ReadDisplayName(firstOriginator),
            ReadFirstAddress(firstOriginator),
            ReadFirstAddress(firstRecipient),
            null,
            null,
            null,
            null,
            null,
            null,
            Truncate(detailMessage?.BodyContent?.Content, 2048),
            detailMessage?.MessageType?.ToString(),
            detailMessage?.Status?.ToString(),
            SummarizeParticipants(detailMessage?.Originators),
            SummarizeParticipants(detailMessage?.Recipients));
    }

    public static IReadOnlyList<MessageParticipantRecord> SummarizeParticipants(IEnumerable<VCard>? cards)
    {
        if (cards is null)
        {
            return [];
        }

        return cards
            .Take(10)
            .Select(
                card => new MessageParticipantRecord(
                    ReadDisplayName(card),
                    card.Telephones?
                        .Where(telephone => !string.IsNullOrWhiteSpace(telephone.Number))
                        .Select(telephone => telephone.Number)
                        .ToArray() ?? [],
                    card.Emails?
                        .Where(email => !string.IsNullOrWhiteSpace(email.EmailAddress))
                        .Select(email => email.EmailAddress)
                        .ToArray() ?? []))
            .ToArray();
    }

    public static string ReadDisplayName(VCard card)
    {
        if (!string.IsNullOrWhiteSpace(card.FormattedName))
        {
            return card.FormattedName;
        }

        var parts = new[]
        {
            card.Prefix,
            card.FirstName,
            card.MiddleName,
            card.LastName,
            card.Suffix
        };

        var composite = string.Join(
            " ",
            parts.Where(part => !string.IsNullOrWhiteSpace(part)).Select(part => part!.Trim()));

        return string.IsNullOrWhiteSpace(composite)
            ? "(unnamed)"
            : composite;
    }

    public static string? ExtractRelativeFolderName(string? folderPath)
    {
        if (string.IsNullOrWhiteSpace(folderPath))
        {
            return null;
        }

        var normalized = folderPath.Replace('\\', '/').Trim();
        var lastSlash = normalized.LastIndexOf('/');
        return lastSlash >= 0 ? normalized[(lastSlash + 1)..] : normalized;
    }

    public static T? GetPropertyValue<T>(object? target, string propertyName)
        where T : class
    {
        return target?.GetType()
            .GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public)
            ?.GetValue(target) as T;
    }

    public static object? ReadObjectProperty(object? target, string propertyName)
    {
        return target?.GetType()
            .GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public)
            ?.GetValue(target);
    }

    public static bool ReadBoolProperty(object? target, string propertyName)
    {
        var value = ReadObjectProperty(target, propertyName);
        return value is bool boolValue && boolValue;
    }

    private static string? ReadFirstAddress(VCard? card)
    {
        return card?.Telephones?.Select(telephone => telephone.Number).FirstOrDefault(number => !string.IsNullOrWhiteSpace(number))
            ?? card?.Emails?.Select(email => email.EmailAddress).FirstOrDefault(address => !string.IsNullOrWhiteSpace(address));
    }

    private static uint? TryToUInt32(object? value)
    {
        return value switch
        {
            null => null,
            uint typed => typed,
            ushort typed => typed,
            int typed when typed >= 0 => (uint)typed,
            long typed when typed >= 0 && typed <= uint.MaxValue => (uint)typed,
            _ when uint.TryParse(Convert.ToString(value, CultureInfo.InvariantCulture), out var parsed) => parsed,
            _ => null
        };
    }

    private static bool? TryToBoolean(object? value)
    {
        return value switch
        {
            null => null,
            bool typed => typed,
            _ when bool.TryParse(Convert.ToString(value, CultureInfo.InvariantCulture), out var parsed) => parsed,
            _ => null
        };
    }

    private static string? Truncate(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length <= maxLength)
        {
            return value;
        }

        return $"{value[..maxLength]}...";
    }

    private static void SetEnumProperty(object target, string propertyName, string enumValueName)
    {
        var property = target.GetType().GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public);
        if (property is null)
        {
            throw new MissingMemberException(target.GetType().FullName, propertyName);
        }

        property.SetValue(target, Enum.Parse(property.PropertyType, enumValueName, ignoreCase: false));
    }
}
