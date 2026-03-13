using System.Buffers.Binary;
using Microsoft.Internal.Bluetooth.Map.Model;

namespace Adit.Probe.MapInterop;

internal static class MapSdpInsights
{
    internal const uint ServiceNameAttributeId = 0x0100;
    internal const uint BluetoothProfileDescriptorListAttributeId = 0x0009;
    internal const uint GoepL2capPsmAttributeId = 0x0200;
    internal const uint MasInstanceIdAttributeId = 0x0315;
    internal const uint SupportedMessageTypesAttributeId = 0x0316;
    internal const uint SupportedFeaturesAttributeId = 0x0317;
    internal const int RichMessageListingParameterMaskValue = 0x001F_FFFF;

    private static readonly (uint Bit, string Name)[] SupportedMessageTypeBits =
    [
        (1u << 0, "Email"),
        (1u << 1, "SmsGsm"),
        (1u << 2, "SmsCdma"),
        (1u << 3, "Mms"),
        (1u << 4, "InstantMessaging")
    ];

    private static readonly (uint Bit, string Name)[] SupportedFeatureBits =
    [
        (1u << 0, "NotificationRegistration"),
        (1u << 1, "Notification"),
        (1u << 2, "Browsing"),
        (1u << 3, "Uploading"),
        (1u << 4, "Delete"),
        (1u << 5, "InstanceInformation"),
        (1u << 6, "ExtendedEventReport11"),
        (1u << 7, "EventReportVersion12"),
        (1u << 8, "MessageFormatVersion11"),
        (1u << 9, "MessagesListingFormatVersion11"),
        (1u << 10, "PersistentMessageHandles"),
        (1u << 11, "DatabaseIdentifier"),
        (1u << 12, "FolderVersionCounter"),
        (1u << 13, "ConversationVersionCounters"),
        (1u << 14, "ParticipantPresenceChangeNotification"),
        (1u << 15, "ParticipantChatStateChangeNotification"),
        (1u << 16, "PbapContactCrossReference"),
        (1u << 17, "NotificationFiltering"),
        (1u << 18, "UtcOffsetTimestampFormat"),
        (1u << 19, "MapSupportedFeaturesInConnectRequest"),
        (1u << 20, "ConversationListing"),
        (1u << 21, "OwnerStatus"),
        (1u << 22, "MessageForwarding")
    ];

    private static readonly string[] ExtendedListingFields =
    [
        "DeliveryStatus",
        "ConversationId",
        "ConversationName",
        "Direction",
        "AttachmentMime"
    ];

    public static MapMasSdpInsight DecodeMasSdpRecord(IReadOnlyDictionary<uint, byte[]> attributes)
    {
        var serviceName = TryReadText(attributes, ServiceNameAttributeId);
        var mapVersionRaw = TryReadProfileVersion(attributes);
        var goepL2capPsm = TryReadUnsigned(attributes, GoepL2capPsmAttributeId);
        var masInstanceId = TryReadUnsigned(attributes, MasInstanceIdAttributeId);
        var supportedMessageTypes = TryReadUnsigned(attributes, SupportedMessageTypesAttributeId);
        var supportedFeatures = TryReadUnsigned(attributes, SupportedFeaturesAttributeId);

        return new MapMasSdpInsight(
            ServiceName: serviceName,
            MapVersionRaw: mapVersionRaw is null ? null : checked((ushort)mapVersionRaw.Value),
            MapVersion: FormatMapVersion(mapVersionRaw),
            RfcommChannelNumber: TryReadRfcommChannelNumber(attributes),
            GoepL2capPsm: goepL2capPsm is null ? null : checked((ushort)goepL2capPsm.Value),
            MasInstanceId: masInstanceId is null ? null : checked((byte)masInstanceId.Value),
            SupportedMessageTypesRaw: supportedMessageTypes,
            SupportedMessageTypes: DecodeBitField(
                supportedMessageTypes,
                SupportedMessageTypeBits,
                includeUnknownBitNames: false),
            SupportedFeaturesRaw: supportedFeatures,
            SupportedFeatures: DecodeBitField(
                supportedFeatures,
                SupportedFeatureBits,
                includeUnknownBitNames: true),
            RichMessageListingParameterMaskRaw: RichMessageListingParameterMaskValue,
            RichMessageListingPotentialFields: supportedFeatures.HasValue
                && (supportedFeatures.Value & (1u << 9)) != 0
                ? ExtendedListingFields
                : []);
    }

    public static ParameterMask CreateRichMessageListingParameterMask()
    {
        return unchecked((ParameterMask)RichMessageListingParameterMaskValue);
    }

    public static SdpDataElementDescription DescribeDataElement(byte[] raw)
    {
        return DescribeDataElement(raw, depth: 0);
    }

    private static SdpDataElementDescription DescribeDataElement(byte[] raw, int depth)
    {
        if (!TryParseDataElement(raw, 0, out var element, out var _))
        {
            return new SdpDataElementDescription(
                TypeDescriptor: null,
                SizeIndex: null,
                PayloadLength: raw.Length,
                PayloadHex: Convert.ToHexString(raw),
                UnsignedInteger: null,
                Uuid: null,
                Text: null,
                Children: null,
                Malformed: true);
        }

        IReadOnlyList<SdpDataElementDescription>? children = null;
        if (depth < 4 && (element.TypeDescriptor == 6 || element.TypeDescriptor == 7))
        {
            var parsedChildren = new List<SdpDataElementDescription>();
            var payload = element.Payload;
            var offset = 0;

            while (offset < payload.Length)
            {
                if (!TryParseDataElement(payload, offset, out var child, out var consumed))
                {
                    parsedChildren.Add(
                        new SdpDataElementDescription(
                            TypeDescriptor: null,
                            SizeIndex: null,
                            PayloadLength: payload.Length - offset,
                            PayloadHex: Convert.ToHexString(payload[offset..]),
                            UnsignedInteger: null,
                            Uuid: null,
                            Text: null,
                            Children: null,
                            Malformed: true));
                    break;
                }

                parsedChildren.Add(DescribeDataElement(payload[offset..(offset + consumed)], depth + 1));
                offset += consumed;
            }

            children = parsedChildren;
        }

        return new SdpDataElementDescription(
            TypeDescriptor: element.TypeDescriptor,
            SizeIndex: element.SizeIndex,
            PayloadLength: element.Payload.Length,
            PayloadHex: Convert.ToHexString(element.Payload),
            UnsignedInteger: element.TypeDescriptor == 1 ? ReadBigEndianInteger(element.Payload) : null,
            Uuid: element.TypeDescriptor == 3 ? ReadBigEndianInteger(element.Payload) : null,
            Text: element.TypeDescriptor == 4 ? TryDecodeUtf8(element.Payload) : null,
            Children: children,
            Malformed: false);
    }

    private static ushort? TryReadProfileVersion(IReadOnlyDictionary<uint, byte[]> attributes)
    {
        if (!attributes.TryGetValue(BluetoothProfileDescriptorListAttributeId, out var raw)
            || !TryParseDataElement(raw, 0, out var root, out var _)
            || root.TypeDescriptor != 6)
        {
            return null;
        }

        foreach (var protocolDescriptor in EnumerateChildren(root.Payload))
        {
            if (protocolDescriptor.TypeDescriptor != 6)
            {
                continue;
            }

            var descriptorChildren = EnumerateChildren(protocolDescriptor.Payload).ToArray();
            if (descriptorChildren.Length < 2)
            {
                continue;
            }

            var profileUuid = descriptorChildren[0].TypeDescriptor == 3
                ? ReadBigEndianInteger(descriptorChildren[0].Payload)
                : null;
            if (profileUuid != 0x1134)
            {
                continue;
            }

            var version = descriptorChildren[1].TypeDescriptor == 1
                ? ReadBigEndianInteger(descriptorChildren[1].Payload)
                : null;
            return version is null ? null : checked((ushort)version.Value);
        }

        return null;
    }

    private static byte? TryReadRfcommChannelNumber(IReadOnlyDictionary<uint, byte[]> attributes)
    {
        if (!attributes.TryGetValue(0x0004, out var raw)
            || !TryParseDataElement(raw, 0, out var root, out var _)
            || root.TypeDescriptor != 6)
        {
            return null;
        }

        foreach (var protocolDescriptor in EnumerateChildren(root.Payload))
        {
            if (protocolDescriptor.TypeDescriptor != 6)
            {
                continue;
            }

            var descriptorChildren = EnumerateChildren(protocolDescriptor.Payload).ToArray();
            if (descriptorChildren.Length < 2)
            {
                continue;
            }

            var protocolUuid = descriptorChildren[0].TypeDescriptor == 3
                ? ReadBigEndianInteger(descriptorChildren[0].Payload)
                : null;
            if (protocolUuid != 0x0003)
            {
                continue;
            }

            var channel = descriptorChildren[1].TypeDescriptor == 1
                ? ReadBigEndianInteger(descriptorChildren[1].Payload)
                : null;
            return channel is null ? null : checked((byte)channel.Value);
        }

        return null;
    }

    private static ulong? TryReadUnsigned(IReadOnlyDictionary<uint, byte[]> attributes, uint attributeId)
    {
        if (!attributes.TryGetValue(attributeId, out var raw)
            || !TryParseDataElement(raw, 0, out var element, out var _)
            || element.TypeDescriptor != 1)
        {
            return null;
        }

        return ReadBigEndianInteger(element.Payload);
    }

    private static string? TryReadText(IReadOnlyDictionary<uint, byte[]> attributes, uint attributeId)
    {
        if (!attributes.TryGetValue(attributeId, out var raw)
            || !TryParseDataElement(raw, 0, out var element, out var _)
            || element.TypeDescriptor != 4)
        {
            return null;
        }

        return TryDecodeUtf8(element.Payload);
    }

    private static string? FormatMapVersion(ulong? rawVersion)
    {
        if (rawVersion is null)
        {
            return null;
        }

        var value = checked((ushort)rawVersion.Value);
        var major = (value >> 8) & 0xFF;
        var minor = value & 0xFF;
        return $"{major}.{minor}";
    }

    private static string[] DecodeBitField(
        ulong? rawValue,
        IReadOnlyList<(uint Bit, string Name)> definitions,
        bool includeUnknownBitNames)
    {
        if (rawValue is null)
        {
            return [];
        }

        var remainingBits = checked((uint)rawValue.Value);
        var names = new List<string>();

        foreach (var definition in definitions)
        {
            if ((remainingBits & definition.Bit) == 0)
            {
                continue;
            }

            names.Add(definition.Name);
            remainingBits &= ~definition.Bit;
        }

        if (includeUnknownBitNames)
        {
            var bitIndex = 0;
            while (remainingBits != 0)
            {
                if ((remainingBits & 1u) != 0)
                {
                    names.Add($"UnknownBit{bitIndex}");
                }

                remainingBits >>= 1;
                bitIndex++;
            }
        }

        return names.ToArray();
    }

    private static IReadOnlyList<SdpParseResult> EnumerateChildren(byte[] payload)
    {
        var children = new List<SdpParseResult>();
        var offset = 0;
        while (offset < payload.Length)
        {
            if (!TryParseDataElement(payload, offset, out var child, out var consumed))
            {
                break;
            }

            children.Add(child);
            offset += consumed;
        }

        return children;
    }

    private static bool TryParseDataElement(
        byte[] raw,
        int offset,
        out SdpParseResult element,
        out int consumed)
    {
        element = default;
        consumed = 0;

        if (offset >= raw.Length)
        {
            return false;
        }

        var header = raw[offset];
        var typeDescriptor = header >> 3;
        var sizeIndex = header & 0x07;
        var payloadOffset = 1;
        int payloadLength;

        switch (sizeIndex)
        {
            case <= 4:
                payloadLength = 1 << sizeIndex;
                break;
            case 5 when raw.Length >= offset + 2:
                payloadLength = raw[offset + 1];
                payloadOffset = 2;
                break;
            case 6 when raw.Length >= offset + 3:
                payloadLength = BinaryPrimitives.ReadUInt16BigEndian(raw.AsSpan(offset + 1, 2));
                payloadOffset = 3;
                break;
            case 7 when raw.Length >= offset + 5:
                payloadLength = checked((int)BinaryPrimitives.ReadUInt32BigEndian(raw.AsSpan(offset + 1, 4)));
                payloadOffset = 5;
                break;
            default:
                return false;
        }

        consumed = payloadOffset + payloadLength;
        if (raw.Length < offset + consumed)
        {
            return false;
        }

        element = new SdpParseResult(
            TypeDescriptor: typeDescriptor,
            SizeIndex: sizeIndex,
            Payload: raw.AsSpan(offset + payloadOffset, payloadLength).ToArray());
        return true;
    }

    private static ulong? ReadBigEndianInteger(ReadOnlySpan<byte> payload)
    {
        if (payload.Length is 0 or > 8)
        {
            return null;
        }

        ulong value = 0;
        foreach (var current in payload)
        {
            value = (value << 8) | current;
        }

        return value;
    }

    private static string? TryDecodeUtf8(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length == 0)
        {
            return string.Empty;
        }

        try
        {
            var decoded = System.Text.Encoding.UTF8.GetString(bytes);
            return decoded.Any(character => char.IsControl(character) && !char.IsWhiteSpace(character))
                ? null
                : decoded;
        }
        catch
        {
            return null;
        }
    }

    private readonly record struct SdpParseResult(
        int TypeDescriptor,
        int SizeIndex,
        byte[] Payload);
}

internal sealed record MapMasSdpInsight(
    string? ServiceName,
    ushort? MapVersionRaw,
    string? MapVersion,
    byte? RfcommChannelNumber,
    ushort? GoepL2capPsm,
    byte? MasInstanceId,
    ulong? SupportedMessageTypesRaw,
    string[] SupportedMessageTypes,
    ulong? SupportedFeaturesRaw,
    string[] SupportedFeatures,
    int RichMessageListingParameterMaskRaw,
    string[] RichMessageListingPotentialFields);

internal sealed record SdpDataElementDescription(
    int? TypeDescriptor,
    int? SizeIndex,
    int PayloadLength,
    string PayloadHex,
    ulong? UnsignedInteger,
    ulong? Uuid,
    string? Text,
    IReadOnlyList<SdpDataElementDescription>? Children,
    bool Malformed);
