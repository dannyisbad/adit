using System.Buffers.Binary;
using Adit.Probe;

namespace Adit.Probe.Tests;

public sealed class AncsProtocolTests
{
    [Fact]
    public void ParseNotificationSource_DecodesExpectedFields()
    {
        var payload = new byte[]
        {
            (byte)AncsNotificationEventId.Added,
            (byte)(AncsEventFlags.Important | AncsEventFlags.PositiveAction),
            (byte)AncsCategoryId.Social,
            3,
            0x78,
            0x56,
            0x34,
            0x12
        };

        var notification = AncsProtocol.ParseNotificationSource(payload);

        Assert.Equal(AncsNotificationEventId.Added, notification.EventId);
        Assert.Equal(
            AncsEventFlags.Important | AncsEventFlags.PositiveAction,
            notification.EventFlags);
        Assert.Equal(AncsCategoryId.Social, notification.CategoryId);
        Assert.Equal((byte)3, notification.CategoryCount);
        Assert.Equal(0x12345678u, notification.NotificationUid);
    }

    [Fact]
    public void BuildGetNotificationAttributesCommand_UsesLittleEndianPayload()
    {
        var command = AncsProtocol.BuildGetNotificationAttributesCommand(
            0x12345678u,
            AncsProtocol.DefaultNotificationAttributes);

        Assert.Equal((byte)AncsCommandId.GetNotificationAttributes, command[0]);
        Assert.Equal(0x12345678u, BinaryPrimitives.ReadUInt32LittleEndian(command.AsSpan(1, 4)));
        Assert.Contains((byte)AncsNotificationAttributeId.Message, command);
        Assert.Contains((byte)AncsNotificationAttributeId.MessageSize, command);
        Assert.Contains((byte)AncsNotificationAttributeId.PositiveActionLabel, command);
        Assert.Contains((byte)AncsNotificationAttributeId.NegativeActionLabel, command);
    }

    [Fact]
    public void TryParseNotificationAttributesResponse_WaitsForCompletePayload()
    {
        var responseBytes = BuildAttributesResponseBytes(
            0x12345678u,
            new Dictionary<AncsNotificationAttributeId, string>
            {
                [AncsNotificationAttributeId.AppIdentifier] = "com.apple.MobileSMS",
                [AncsNotificationAttributeId.Title] = "Riley",
                [AncsNotificationAttributeId.Subtitle] = "",
                [AncsNotificationAttributeId.Message] = "hello world",
                [AncsNotificationAttributeId.Date] = "20260307T221500"
            });

        var partial = responseBytes[..^5];
        var complete = AncsProtocol.TryParseNotificationAttributesResponse(
            partial,
            AncsProtocol.DefaultNotificationAttributes,
            out var partialResponse);

        Assert.False(complete);
        Assert.Null(partialResponse);

        var parsed = AncsProtocol.TryParseNotificationAttributesResponse(
            responseBytes,
            AncsProtocol.DefaultNotificationAttributes,
            out var fullResponse);

        Assert.True(parsed);
        Assert.NotNull(fullResponse);
        Assert.Equal(0x12345678u, fullResponse!.NotificationUid);
        Assert.Equal("Riley", fullResponse.Attributes[AncsNotificationAttributeId.Title]);
        Assert.Equal("hello world", fullResponse.Attributes[AncsNotificationAttributeId.Message]);
    }

    [Fact]
    public void TryParseNotificationAttributesResponse_AcceptsOutOfOrderAttributes()
    {
        var responseBytes = BuildAttributesResponseBytes(
            0x12345678u,
            new Dictionary<AncsNotificationAttributeId, string>
            {
                [AncsNotificationAttributeId.AppIdentifier] = "net.superblock.pushover",
                [AncsNotificationAttributeId.Title] = "probe-live",
                [AncsNotificationAttributeId.Subtitle] = "",
                [AncsNotificationAttributeId.Message] = "ANCS open test",
                [AncsNotificationAttributeId.Date] = "20260307T220136"
            },
            [
                AncsNotificationAttributeId.Title,
                AncsNotificationAttributeId.AppIdentifier,
                AncsNotificationAttributeId.Subtitle,
                AncsNotificationAttributeId.Message,
                AncsNotificationAttributeId.Date
            ]);

        var parsed = AncsProtocol.TryParseNotificationAttributesResponse(
            responseBytes,
            AncsProtocol.DefaultNotificationAttributes,
            out var response);

        Assert.True(parsed);
        Assert.NotNull(response);
        Assert.Equal("net.superblock.pushover", response!.Attributes[AncsNotificationAttributeId.AppIdentifier]);
        Assert.Equal("probe-live", response.Attributes[AncsNotificationAttributeId.Title]);
    }

    private static byte[] BuildAttributesResponseBytes(
        uint notificationUid,
        IReadOnlyDictionary<AncsNotificationAttributeId, string> attributes,
        IReadOnlyList<AncsNotificationAttributeId>? attributeOrder = null)
    {
        using var stream = new MemoryStream();
        stream.WriteByte((byte)AncsCommandId.GetNotificationAttributes);

        Span<byte> uidBytes = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(uidBytes, notificationUid);
        stream.Write(uidBytes);

        var orderedAttributes = attributeOrder ?? AncsProtocol.DefaultNotificationAttributes
            .Select(requestedAttribute => requestedAttribute.AttributeId)
            .Where(attributes.ContainsKey)
            .ToArray();

        foreach (var attributeId in orderedAttributes)
        {
            var value = attributes[attributeId];
            var valueBytes = System.Text.Encoding.UTF8.GetBytes(value);
            var lengthBytes = new byte[2];

            stream.WriteByte((byte)attributeId);
            BinaryPrimitives.WriteUInt16LittleEndian(lengthBytes, (ushort)valueBytes.Length);
            stream.Write(lengthBytes);
            stream.Write(valueBytes);
        }

        return stream.ToArray();
    }
}
