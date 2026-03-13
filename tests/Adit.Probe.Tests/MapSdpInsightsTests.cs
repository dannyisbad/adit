using Adit.Probe.MapInterop;

namespace Adit.Probe.Tests;

public sealed class MapSdpInsightsTests
{
    [Fact]
    public void DecodeMasSdpRecord_DecodesVersionFeaturesAndMessageTypes()
    {
        var attributes = new Dictionary<uint, byte[]>
        {
            [0x0004] = Hex("35113503190100350519000308023503190008"),
            [0x0009] = Hex("35083506191134090104"),
            [0x0100] = Hex("250B4D4150204D41532D694F53"),
            [0x0200] = Hex("09100B"),
            [0x0315] = Hex("0800"),
            [0x0316] = Hex("0802"),
            [0x0317] = Hex("0A0006027F")
        };

        var insight = MapSdpInsights.DecodeMasSdpRecord(attributes);

        Assert.Equal("MAP MAS-iOS", insight.ServiceName);
        Assert.Equal((ushort)0x0104, insight.MapVersionRaw);
        Assert.Equal("1.4", insight.MapVersion);
        Assert.Equal((byte)2, insight.RfcommChannelNumber);
        Assert.Equal((ushort)0x100B, insight.GoepL2capPsm);
        Assert.Equal((byte)0, insight.MasInstanceId);
        Assert.Equal((ulong)0x02, insight.SupportedMessageTypesRaw);
        Assert.Equal(["SmsGsm"], insight.SupportedMessageTypes);
        Assert.Equal((ulong)0x0006027F, insight.SupportedFeaturesRaw);
        Assert.Contains("MessagesListingFormatVersion11", insight.SupportedFeatures);
        Assert.Contains("NotificationFiltering", insight.SupportedFeatures);
        Assert.Contains("UtcOffsetTimestampFormat", insight.SupportedFeatures);
        Assert.Equal(
            [
                "DeliveryStatus",
                "ConversationId",
                "ConversationName",
                "Direction",
                "AttachmentMime"
            ],
            insight.RichMessageListingPotentialFields);
    }

    [Fact]
    public void RichMessageListingParameterMaskValue_RequestsBitsPastLegacyWrapperEnum()
    {
        var mask = MapSdpInsights.RichMessageListingParameterMaskValue;

        Assert.Equal(0x001F_FFFF, mask);
        Assert.True((mask & (1 << 16)) != 0);
        Assert.True((mask & (1 << 20)) != 0);
    }

    [Fact]
    public void DescribeDataElement_RecursivelyDecodesNestedSequences()
    {
        var description = MapSdpInsights.DescribeDataElement(Hex("35083506191134090104"));

        Assert.False(description.Malformed);
        Assert.Equal(6, description.TypeDescriptor);
        Assert.NotNull(description.Children);
        Assert.Single(description.Children!);
        Assert.Equal(6, description.Children[0].TypeDescriptor);
        Assert.Equal(2, description.Children[0].Children!.Count);
        Assert.Equal((ulong)0x1134, description.Children[0].Children![0].Uuid);
        Assert.Equal((ulong)0x0104, description.Children[0].Children![1].UnsignedInteger);
    }

    private static byte[] Hex(string value)
    {
        var bytes = new byte[value.Length / 2];
        for (var index = 0; index < bytes.Length; index++)
        {
            bytes[index] = Convert.ToByte(value.Substring(index * 2, 2), 16);
        }

        return bytes;
    }
}
