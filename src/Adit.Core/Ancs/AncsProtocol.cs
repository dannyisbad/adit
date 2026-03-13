using System.Buffers.Binary;
using System.Text;
using Adit.Core.Models;

namespace Adit.Core.Ancs;

public static class AncsUuids
{
    public static readonly Guid Service = new("7905F431-B5CE-4E99-A40F-4B1E122D00D0");
    public static readonly Guid NotificationSource = new("9FBF120D-6301-42D9-8C58-25E699A21DBD");
    public static readonly Guid ControlPoint = new("69D1D8F3-45E1-49A8-9821-9BBDFDAAD9D9");
    public static readonly Guid DataSource = new("22EAC6E9-24D6-4BB5-BE44-B36ACE7C7BFB");
}

internal enum AncsCommandId : byte
{
    GetNotificationAttributes = 0,
    GetAppAttributes = 1,
    PerformNotificationAction = 2
}

internal enum AncsNotificationAttributeId : byte
{
    AppIdentifier = 0,
    Title = 1,
    Subtitle = 2,
    Message = 3,
    MessageSize = 4,
    Date = 5,
    PositiveActionLabel = 6,
    NegativeActionLabel = 7
}

internal sealed record RequestedNotificationAttribute(
    AncsNotificationAttributeId AttributeId,
    ushort? MaxLength = null);

internal sealed record ParsedNotificationEvent(
    NotificationEventKind EventKind,
    NotificationEventFlags EventFlags,
    NotificationCategory Category,
    byte CategoryCount,
    uint NotificationUid);

internal sealed record ParsedNotificationAttributesResponse(
    uint NotificationUid,
    IReadOnlyDictionary<AncsNotificationAttributeId, string> Attributes);

internal static class AncsProtocol
{
    public static readonly IReadOnlyList<RequestedNotificationAttribute> DefaultNotificationAttributes =
    [
        new(AncsNotificationAttributeId.AppIdentifier),
        new(AncsNotificationAttributeId.Title, 64),
        new(AncsNotificationAttributeId.Subtitle, 64),
        new(AncsNotificationAttributeId.Message, 256),
        new(AncsNotificationAttributeId.MessageSize),
        new(AncsNotificationAttributeId.Date),
        new(AncsNotificationAttributeId.PositiveActionLabel),
        new(AncsNotificationAttributeId.NegativeActionLabel)
    ];

    public static ParsedNotificationEvent ParseNotificationSource(ReadOnlySpan<byte> data)
    {
        if (data.Length != 8)
        {
            throw new InvalidDataException($"Notification Source payload must be 8 bytes, got {data.Length}.");
        }

        return new ParsedNotificationEvent(
            (NotificationEventKind)data[0],
            (NotificationEventFlags)data[1],
            (NotificationCategory)data[2],
            data[3],
            BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(4, 4)));
    }

    public static byte[] BuildGetNotificationAttributesCommand(
        uint notificationUid,
        IReadOnlyList<RequestedNotificationAttribute> requestedAttributes)
    {
        using var stream = new MemoryStream();
        stream.WriteByte((byte)AncsCommandId.GetNotificationAttributes);
        WriteUInt32LittleEndian(stream, notificationUid);

        foreach (var requestedAttribute in requestedAttributes)
        {
            stream.WriteByte((byte)requestedAttribute.AttributeId);

            if (requestedAttribute.MaxLength is ushort maxLength)
            {
                WriteUInt16LittleEndian(stream, maxLength);
            }
        }

        return stream.ToArray();
    }

    public static bool TryParseNotificationAttributesResponse(
        ReadOnlySpan<byte> data,
        IReadOnlyList<RequestedNotificationAttribute> requestedAttributes,
        out ParsedNotificationAttributesResponse? response)
    {
        return TryParseNotificationAttributesResponse(
            data,
            requestedAttributes,
            out response,
            out _,
            out _);
    }

    internal static bool TryParseNotificationAttributesResponse(
        ReadOnlySpan<byte> data,
        IReadOnlyList<RequestedNotificationAttribute> requestedAttributes,
        out ParsedNotificationAttributesResponse? response,
        out int consumedBytes,
        out bool hasCompleteAttributeSet)
    {
        response = null;
        consumedBytes = 0;
        hasCompleteAttributeSet = false;
        if (data.Length < 5)
        {
            return false;
        }

        var commandId = (AncsCommandId)data[0];
        if (commandId != AncsCommandId.GetNotificationAttributes)
        {
            throw new InvalidDataException($"Unexpected ANCS command id: {data[0]}.");
        }

        var notificationUid = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(1, 4));
        var attributes = new Dictionary<AncsNotificationAttributeId, string>(requestedAttributes.Count);
        var requestedIds = requestedAttributes
            .Select(requestedAttribute => requestedAttribute.AttributeId)
            .ToHashSet();
        var offset = 5;

        while (offset < data.Length)
        {
            if (offset + 3 > data.Length)
            {
                return false;
            }

            var attributeId = (AncsNotificationAttributeId)data[offset];
            var attributeLength = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(offset + 1, 2));
            offset += 3;

            if (offset + attributeLength > data.Length)
            {
                return false;
            }

            if (requestedIds.Contains(attributeId))
            {
                attributes[attributeId] = Encoding.UTF8.GetString(data.Slice(offset, attributeLength));
                if (requestedIds.Count > 0 && requestedIds.All(attributes.ContainsKey))
                {
                    consumedBytes = offset + attributeLength;
                    hasCompleteAttributeSet = true;
                    response = new ParsedNotificationAttributesResponse(notificationUid, attributes);
                    return true;
                }
            }

            offset += attributeLength;
        }

        response = new ParsedNotificationAttributesResponse(notificationUid, attributes);
        consumedBytes = offset;
        hasCompleteAttributeSet = requestedIds.Count > 0 && requestedIds.All(attributes.ContainsKey);
        return true;
    }

    public static byte[] BuildPerformNotificationActionCommand(
        uint notificationUid,
        NotificationAction action)
    {
        using var stream = new MemoryStream();
        stream.WriteByte((byte)AncsCommandId.PerformNotificationAction);
        WriteUInt32LittleEndian(stream, notificationUid);
        stream.WriteByte(action == NotificationAction.Positive ? (byte)0 : (byte)1);
        return stream.ToArray();
    }

    private static void WriteUInt16LittleEndian(Stream stream, ushort value)
    {
        Span<byte> buffer = stackalloc byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(buffer, value);
        stream.Write(buffer);
    }

    private static void WriteUInt32LittleEndian(Stream stream, uint value)
    {
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(buffer, value);
        stream.Write(buffer);
    }
}
