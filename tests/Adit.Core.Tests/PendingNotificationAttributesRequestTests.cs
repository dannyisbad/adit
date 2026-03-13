using System.Buffers.Binary;
using System.Text;
using Adit.Core.Ancs;

namespace Adit.Core.Tests;

public sealed class PendingNotificationAttributesRequestTests
{
    [Fact]
    public async Task Append_IgnoresStaleResponseAndCompletesCurrentRequest()
    {
        var requestedAttributes = new RequestedNotificationAttribute[]
        {
            new(AncsNotificationAttributeId.AppIdentifier),
            new(AncsNotificationAttributeId.Title, 64)
        };

        var pending = new PendingNotificationAttributesRequest(
            notificationUid: 45,
            requestedAttributes,
            settleDelay: TimeSpan.FromMilliseconds(10));

        var staleResponse = BuildResponse(
            notificationUid: 44,
            (AncsNotificationAttributeId.AppIdentifier, "com.apple.MobileSMS"),
            (AncsNotificationAttributeId.Title, "stale"));
        var currentResponse = BuildResponse(
            notificationUid: 45,
            (AncsNotificationAttributeId.AppIdentifier, "com.apple.MobileSMS"),
            (AncsNotificationAttributeId.Title, "current"));

        pending.Append(staleResponse.Concat(currentResponse).ToArray());

        var response = await pending.Completion.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal<uint>(45, response.NotificationUid);
        Assert.Equal("current", response.Attributes[AncsNotificationAttributeId.Title]);
    }

    [Fact]
    public async Task Append_MergesFragmentsBeforeCompleting()
    {
        var requestedAttributes = new RequestedNotificationAttribute[]
        {
            new(AncsNotificationAttributeId.AppIdentifier),
            new(AncsNotificationAttributeId.Message, 128)
        };

        var pending = new PendingNotificationAttributesRequest(
            notificationUid: 77,
            requestedAttributes,
            settleDelay: TimeSpan.FromMilliseconds(10));

        var payload = BuildResponse(
            notificationUid: 77,
            (AncsNotificationAttributeId.AppIdentifier, "com.apple.MobileSMS"),
            (AncsNotificationAttributeId.Message, "hello from ancs"));

        pending.Append(payload[..8]);
        pending.Append(payload[8..]);

        var response = await pending.Completion.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal("hello from ancs", response.Attributes[AncsNotificationAttributeId.Message]);
    }

    private static byte[] BuildResponse(
        uint notificationUid,
        params (AncsNotificationAttributeId AttributeId, string Value)[] attributes)
    {
        using var stream = new MemoryStream();
        stream.WriteByte(0);

        Span<byte> uidBytes = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(uidBytes, notificationUid);
        stream.Write(uidBytes);
        Span<byte> lengthBytes = stackalloc byte[2];

        foreach (var (attributeId, value) in attributes)
        {
            var valueBytes = Encoding.UTF8.GetBytes(value);
            stream.WriteByte((byte)attributeId);

            BinaryPrimitives.WriteUInt16LittleEndian(lengthBytes, checked((ushort)valueBytes.Length));
            stream.Write(lengthBytes);
            stream.Write(valueBytes);
        }

        return stream.ToArray();
    }
}
