using System.Buffers.Binary;
using Adit.Probe;

namespace Adit.Probe.Tests;

public sealed class PendingNotificationAttributesRequestTests
{
    [Fact]
    public async Task Append_MergesSupplementalPacketsBeforeCompletion()
    {
        const uint notificationUid = 0x12345678;
        var pending = new PendingNotificationAttributesRequest(
            notificationUid,
            AncsProtocol.DefaultNotificationAttributes,
            TimeSpan.FromMilliseconds(50));

        pending.Append(
            BuildAttributesResponseBytes(
                notificationUid,
                new Dictionary<AncsNotificationAttributeId, string>
                {
                    [AncsNotificationAttributeId.AppIdentifier] = "com.apple.MobileSMS",
                    [AncsNotificationAttributeId.Title] = "Riley",
                    [AncsNotificationAttributeId.Subtitle] = "",
                    [AncsNotificationAttributeId.Message] = "you there?",
                    [AncsNotificationAttributeId.Date] = "20260307T223000"
                }));

        Assert.False(pending.Completion.IsCompleted);

        pending.Append(
            BuildAttributesResponseBytes(
                notificationUid,
                new Dictionary<AncsNotificationAttributeId, string>
                {
                    [AncsNotificationAttributeId.MessageSize] = "10",
                    [AncsNotificationAttributeId.PositiveActionLabel] = "Reply",
                    [AncsNotificationAttributeId.NegativeActionLabel] = "Dismiss"
                }));

        var response = await pending.Completion.WaitAsync(TimeSpan.FromSeconds(1));

        Assert.Equal("Riley", response.Attributes[AncsNotificationAttributeId.Title]);
        Assert.Equal("10", response.Attributes[AncsNotificationAttributeId.MessageSize]);
        Assert.Equal("Reply", response.Attributes[AncsNotificationAttributeId.PositiveActionLabel]);
        Assert.Equal("Dismiss", response.Attributes[AncsNotificationAttributeId.NegativeActionLabel]);
    }

    [Fact]
    public async Task Append_CompletesPartialResponseAfterSettleDelay()
    {
        const uint notificationUid = 0x23456789;
        var pending = new PendingNotificationAttributesRequest(
            notificationUid,
            AncsProtocol.DefaultNotificationAttributes,
            TimeSpan.FromMilliseconds(20));

        pending.Append(
            BuildAttributesResponseBytes(
                notificationUid,
                new Dictionary<AncsNotificationAttributeId, string>
                {
                    [AncsNotificationAttributeId.AppIdentifier] = "ph.telegra.Telegraph",
                    [AncsNotificationAttributeId.Title] = "eggsy",
                    [AncsNotificationAttributeId.Message] = "what are u cooking"
                }));

        var response = await pending.Completion.WaitAsync(TimeSpan.FromSeconds(1));

        Assert.Equal("eggsy", response.Attributes[AncsNotificationAttributeId.Title]);
        Assert.Equal("what are u cooking", response.Attributes[AncsNotificationAttributeId.Message]);
        Assert.DoesNotContain(AncsNotificationAttributeId.PositiveActionLabel, response.Attributes.Keys);
    }

    private static byte[] BuildAttributesResponseBytes(
        uint notificationUid,
        IReadOnlyDictionary<AncsNotificationAttributeId, string> attributes)
    {
        using var stream = new MemoryStream();
        stream.WriteByte((byte)AncsCommandId.GetNotificationAttributes);

        Span<byte> uidBytes = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(uidBytes, notificationUid);
        stream.Write(uidBytes);

        foreach (var attribute in attributes)
        {
            var valueBytes = System.Text.Encoding.UTF8.GetBytes(attribute.Value);
            var lengthBytes = new byte[2];

            stream.WriteByte((byte)attribute.Key);
            BinaryPrimitives.WriteUInt16LittleEndian(lengthBytes, (ushort)valueBytes.Length);
            stream.Write(lengthBytes);
            stream.Write(valueBytes);
        }

        return stream.ToArray();
    }
}
