using System.Text;
using System.Xml.Linq;

namespace Adit.Probe.MapInterop;

internal sealed class ObexTrafficInspector
{
    private static readonly Dictionary<byte, string> HeaderNames = new()
    {
        [0x01] = "Name",
        [0x42] = "Type",
        [0x46] = "Target",
        [0x47] = "Http",
        [0x48] = "Body",
        [0x49] = "EndOfBody",
        [0x4A] = "Who",
        [0x4C] = "AppParams",
        [0xCB] = "ConnectionId"
    };

    private static readonly Dictionary<byte, string> AppParamNames = new()
    {
        [1] = "MaxListCount",
        [2] = "StartOffset",
        [3] = "FilterMessageType",
        [4] = "FilterPeriodBegin",
        [5] = "FilterPeriodEnd",
        [6] = "FilterReadStatus",
        [7] = "FilterRecipient",
        [8] = "FilterOriginator",
        [9] = "FilterPriority",
        [10] = "Attachment",
        [11] = "Transparent",
        [12] = "Retry",
        [13] = "NewMessage",
        [14] = "NotificationStatus",
        [15] = "MasInstanceId",
        [16] = "ParameterMask",
        [17] = "FolderListingSize",
        [18] = "MessageListingSize",
        [19] = "SubjectLength",
        [20] = "Charset",
        [21] = "FractionRequest",
        [22] = "FractionDeliver",
        [23] = "StatusIndicator",
        [24] = "StatusValue",
        [25] = "MseTime"
    };

    private static readonly string[] ParameterMaskNames =
    [
        "Subject",
        "Datetime",
        "SenderName",
        "SenderAddressing",
        "RecipientName",
        "RecipientAddressing",
        "Type",
        "Size",
        "ReceptionStatus",
        "Text",
        "AttachmentSize",
        "Priority",
        "Read",
        "Sent",
        "Protected",
        "ReplyToAddressing",
        "DeliveryStatus",
        "ConversationId",
        "ConversationName",
        "Direction",
        "AttachmentMime"
    ];

    private static readonly HashSet<string> WrapperListingAttributes =
    [
        "handle",
        "subject",
        "datetime",
        "sender_name",
        "sender_addressing",
        "recipient_addressing",
        "type",
        "size",
        "attachment_size",
        "priority",
        "read",
        "sent",
        "protected"
    ];

    private readonly object gate = new();
    private readonly ProbeLogger? probeLogger;
    private readonly string socketId;
    private readonly Dictionary<string, List<byte>> buffers = new(StringComparer.OrdinalIgnoreCase)
    {
        ["read"] = [],
        ["write"] = []
    };

    public ObexTrafficInspector(string socketId, ProbeLogger? probeLogger)
    {
        this.socketId = socketId;
        this.probeLogger = probeLogger;
    }

    public void Record(string direction, ReadOnlySpan<byte> payload)
    {
        if (probeLogger is null || payload.Length == 0)
        {
            return;
        }

        lock (gate)
        {
            if (!buffers.TryGetValue(direction, out var buffer))
            {
                buffer = [];
                buffers[direction] = buffer;
            }

            buffer.AddRange(payload.ToArray());

            while (TryExtractPacket(buffer, out var packet))
            {
                LogPacket(direction, packet);
            }
        }
    }

    private void LogPacket(string direction, byte[] packet)
    {
        var parsed = ParsePacket(packet);

        probeLogger?.Log(
            "bt.obex_packet",
            new
            {
                socketId,
                direction,
                parsed.CodeHex,
                parsed.CodeName,
                parsed.PacketLength,
                parsed.Headers,
                parsed.Body
            });
    }

    private static bool TryExtractPacket(List<byte> buffer, out byte[] packet)
    {
        packet = [];
        if (buffer.Count < 3)
        {
            return false;
        }

        var packetLength = (buffer[1] << 8) | buffer[2];
        if (packetLength < 3)
        {
            buffer.RemoveAt(0);
            return false;
        }

        if (buffer.Count < packetLength)
        {
            return false;
        }

        packet = buffer.Take(packetLength).ToArray();
        buffer.RemoveRange(0, packetLength);
        return true;
    }

    private static ParsedObexPacket ParsePacket(byte[] packet)
    {
        var code = packet[0];
        var headers = new List<object>();
        var bodyBytes = new List<byte>();
        var offset = 3;
        var operationFields = DescribeOperationFields(code, packet, ref offset);

        while (offset < packet.Length)
        {
            var headerId = packet[offset];
            var classBits = headerId & 0xC0;

            switch (classBits)
            {
                case 0x00:
                case 0x40:
                {
                    if (offset + 3 > packet.Length)
                    {
                        headers.Add(new
                        {
                            id = $"0x{headerId:X2}",
                            name = GetHeaderName(headerId),
                            malformed = true,
                            reason = "length_prefix_missing"
                        });
                        offset = packet.Length;
                        break;
                    }

                    var totalLength = (packet[offset + 1] << 8) | packet[offset + 2];
                    if (totalLength < 3 || offset + totalLength > packet.Length)
                    {
                        headers.Add(new
                        {
                            id = $"0x{headerId:X2}",
                            name = GetHeaderName(headerId),
                            malformed = true,
                            reason = "invalid_length",
                            totalLength
                        });
                        offset = packet.Length;
                        break;
                    }

                    var value = packet.AsSpan(offset + 3, totalLength - 3).ToArray();
                    headers.Add(ParseLengthPrefixedHeader(headerId, totalLength, value));
                    if (headerId is 0x48 or 0x49)
                    {
                        bodyBytes.AddRange(value);
                    }

                    offset += totalLength;
                    break;
                }
                case 0x80:
                {
                    if (offset + 2 > packet.Length)
                    {
                        headers.Add(new
                        {
                            id = $"0x{headerId:X2}",
                            name = GetHeaderName(headerId),
                            malformed = true,
                            reason = "uint8_truncated"
                        });
                        offset = packet.Length;
                        break;
                    }

                    var value = packet[offset + 1];
                    headers.Add(new
                    {
                        id = $"0x{headerId:X2}",
                        name = GetHeaderName(headerId),
                        kind = "uint8",
                        value
                    });
                    offset += 2;
                    break;
                }
                case 0xC0:
                {
                    if (offset + 5 > packet.Length)
                    {
                        headers.Add(new
                        {
                            id = $"0x{headerId:X2}",
                            name = GetHeaderName(headerId),
                            malformed = true,
                            reason = "uint32_truncated"
                        });
                        offset = packet.Length;
                        break;
                    }

                    var value = ReadUInt32BigEndian(packet.AsSpan(offset + 1, 4));
                    headers.Add(new
                    {
                        id = $"0x{headerId:X2}",
                        name = GetHeaderName(headerId),
                        kind = "uint32",
                        value
                    });
                    offset += 5;
                    break;
                }
            }
        }

        return new ParsedObexPacket(
            CodeHex: $"0x{code:X2}",
            CodeName: DescribeCode(code),
            PacketLength: packet.Length,
            OperationFields: operationFields,
            Headers: headers.ToArray(),
            Body: DescribeBody(bodyBytes.ToArray()));
    }

    private static object? DescribeOperationFields(byte code, byte[] packet, ref int offset)
    {
        if (code == 0x80 && packet.Length >= 7)
        {
            offset = 7;
            return new
            {
                version = packet[3],
                flags = packet[4],
                maxPacketLength = ReadUInt16BigEndian(packet.AsSpan(5, 2))
            };
        }

        if (code == 0x85 && packet.Length >= 5)
        {
            offset = 5;
            return new
            {
                flags = packet[3],
                constants = packet[4]
            };
        }

        if (code == 0xA0 && packet.Length >= 7 && packet[3] == 0x10 && packet[4] == 0x00)
        {
            offset = 7;
            return new
            {
                version = packet[3],
                flags = packet[4],
                maxPacketLength = ReadUInt16BigEndian(packet.AsSpan(5, 2))
            };
        }

        return null;
    }

    private static object ParseLengthPrefixedHeader(byte headerId, int totalLength, byte[] value)
    {
        if (headerId == 0x4C)
        {
            return new
            {
                id = $"0x{headerId:X2}",
                name = GetHeaderName(headerId),
                kind = "app_params",
                totalLength,
                parameters = ParseAppParams(value)
            };
        }

        if (headerId is 0x48 or 0x49)
        {
            return new
            {
                id = $"0x{headerId:X2}",
                name = GetHeaderName(headerId),
                kind = "body",
                totalLength,
                byteLength = value.Length,
                utf8Preview = Truncate(TryDecodeUtf8(value), 240)
            };
        }

        if ((headerId & 0xC0) == 0x00)
        {
            return new
            {
                id = $"0x{headerId:X2}",
                name = GetHeaderName(headerId),
                kind = "unicode",
                totalLength,
                text = DecodeUnicodeHeader(value)
            };
        }

        return new
        {
            id = $"0x{headerId:X2}",
            name = GetHeaderName(headerId),
            kind = "bytes",
            totalLength,
            payloadHex = Convert.ToHexString(value),
            payloadUtf8 = TryDecodeUtf8(value)
        };
    }

    private static object[] ParseAppParams(byte[] payload)
    {
        var parameters = new List<object>();
        var offset = 0;

        while (offset + 2 <= payload.Length)
        {
            var tag = payload[offset];
            var length = payload[offset + 1];
            offset += 2;

            if (offset + length > payload.Length)
            {
                parameters.Add(new
                {
                    tag = $"0x{tag:X2}",
                    name = AppParamNames.GetValueOrDefault(tag, "Unknown"),
                    malformed = true,
                    length
                });
                break;
            }

            var value = payload.AsSpan(offset, length).ToArray();
            parameters.Add(new
            {
                tag = $"0x{tag:X2}",
                name = AppParamNames.GetValueOrDefault(tag, "Unknown"),
                length,
                payloadHex = Convert.ToHexString(value),
                value = DescribeAppParamValue(tag, value)
            });
            offset += length;
        }

        return parameters.ToArray();
    }

    private static object DescribeAppParamValue(byte tag, byte[] value)
    {
        return tag switch
        {
            1 or 2 or 17 or 18 or 19 => new
            {
                unsignedInteger = ReadUInt16BigEndian(value),
                text = TryDecodeUtf8(value)
            },
            10 or 11 or 12 or 13 or 14 or 15 or 20 or 21 or 22 or 23 or 24 => new
            {
                byteValue = value.Length > 0 ? value[0] : (byte?)null,
                boolValue = value.Length > 0 && (value[0] is 0 or 1)
                    ? (bool?)(value[0] == 1)
                    : null
            },
            16 => new
            {
                unsignedInteger = ReadUInt32BigEndian(value),
                flags = DecodeParameterMask(ReadUInt32BigEndian(value))
            },
            25 => new
            {
                text = TryDecodeUtf8(value)
            },
            _ => new
            {
                unsignedInteger = value.Length switch
                {
                    1 => value[0],
                    2 => ReadUInt16BigEndian(value),
                    4 => ReadUInt32BigEndian(value),
                    _ => null
                },
                text = TryDecodeUtf8(value)
            }
        };
    }

    private static string[] DecodeParameterMask(uint? value)
    {
        if (value is null)
        {
            return [];
        }

        var flags = new List<string>();
        for (var bit = 0; bit < ParameterMaskNames.Length; bit++)
        {
            if ((value.Value & (1u << bit)) != 0)
            {
                flags.Add(ParameterMaskNames[bit]);
            }
        }

        return flags.ToArray();
    }

    private static object? DescribeBody(byte[] bodyBytes)
    {
        if (bodyBytes.Length == 0)
        {
            return null;
        }

        var utf8 = TryDecodeUtf8(bodyBytes);
        if (string.IsNullOrWhiteSpace(utf8))
        {
            return new
            {
                byteLength = bodyBytes.Length,
                payloadHexPreview = Convert.ToHexString(bodyBytes.AsSpan(0, Math.Min(bodyBytes.Length, 64)).ToArray())
            };
        }

        var trimmed = utf8.Trim();
        if (trimmed.StartsWith('<'))
        {
            return DescribeXmlBody(trimmed);
        }

        if (trimmed.StartsWith("BEGIN:BMSG", StringComparison.OrdinalIgnoreCase))
        {
            return DescribeBMessage(trimmed);
        }

        return new
        {
            byteLength = bodyBytes.Length,
            utf8Preview = Truncate(trimmed, 512)
        };
    }

    private static object DescribeXmlBody(string xml)
    {
        try
        {
            var document = XDocument.Parse(xml, LoadOptions.PreserveWhitespace);
            var root = document.Root;
            if (root is null)
            {
                return new
                {
                    kind = "xml",
                    empty = true
                };
            }

            if (string.Equals(root.Name.LocalName, "MAP-msg-listing", StringComparison.OrdinalIgnoreCase))
            {
                var messages = root.Elements()
                    .Where(element => string.Equals(element.Name.LocalName, "msg", StringComparison.OrdinalIgnoreCase))
                    .ToArray();
                var attributeNames = messages
                    .SelectMany(message => message.Attributes().Select(attribute => attribute.Name.LocalName))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
                    .ToArray();

                return new
                {
                    kind = "xml",
                    root = root.Name.LocalName,
                    messageCount = messages.Length,
                    attributeNames,
                    wrapperMissingAttributeNames = attributeNames
                        .Where(name => !WrapperListingAttributes.Contains(name))
                        .ToArray(),
                    items = messages
                        .Take(10)
                        .Select(
                            message => message.Attributes()
                                .ToDictionary(
                                    attribute => attribute.Name.LocalName,
                                    attribute => attribute.Value,
                                    StringComparer.OrdinalIgnoreCase))
                        .ToArray()
                };
            }

            if (string.Equals(root.Name.LocalName, "folder-listing", StringComparison.OrdinalIgnoreCase))
            {
                return new
                {
                    kind = "xml",
                    root = root.Name.LocalName,
                    items = root.Elements()
                        .Take(20)
                        .Select(
                            element => new
                            {
                                name = element.Name.LocalName,
                                attributes = element.Attributes()
                                    .ToDictionary(
                                        attribute => attribute.Name.LocalName,
                                        attribute => attribute.Value,
                                        StringComparer.OrdinalIgnoreCase)
                            })
                        .ToArray()
                };
            }

            return new
            {
                kind = "xml",
                root = root.Name.LocalName,
                utf8Preview = Truncate(xml, 512)
            };
        }
        catch (Exception exception)
        {
            return new
            {
                kind = "xml",
                parseFailed = true,
                error = exception.Message,
                utf8Preview = Truncate(xml, 512)
            };
        }
    }

    private static object DescribeBMessage(string text)
    {
        var lines = text.Split(["\r\n", "\n"], StringSplitOptions.None);
        var messageText = ExtractBetween(text, "BEGIN:MSG", "END:MSG");
        return new
        {
            kind = "bmessage",
            interestingLines = lines
                .Where(
                    line => !string.IsNullOrWhiteSpace(line)
                        && (line.StartsWith("VERSION:", StringComparison.OrdinalIgnoreCase)
                            || line.StartsWith("STATUS:", StringComparison.OrdinalIgnoreCase)
                            || line.StartsWith("TYPE:", StringComparison.OrdinalIgnoreCase)
                            || line.StartsWith("FOLDER:", StringComparison.OrdinalIgnoreCase)
                            || line.StartsWith("CHARSET:", StringComparison.OrdinalIgnoreCase)
                            || line.StartsWith("LANGUAGE:", StringComparison.OrdinalIgnoreCase)
                            || line.StartsWith("FN", StringComparison.OrdinalIgnoreCase)
                            || line.StartsWith("TEL", StringComparison.OrdinalIgnoreCase)
                            || line.StartsWith("EMAIL", StringComparison.OrdinalIgnoreCase)))
                .Take(40)
                .ToArray(),
            messageText = Truncate(messageText?.Trim(), 512)
        };
    }

    private static string? ExtractBetween(string input, string startMarker, string endMarker)
    {
        var startIndex = input.IndexOf(startMarker, StringComparison.OrdinalIgnoreCase);
        if (startIndex < 0)
        {
            return null;
        }

        startIndex += startMarker.Length;
        var afterStart = input.AsSpan(startIndex);
        if (afterStart.StartsWith("\r\n".AsSpan(), StringComparison.Ordinal))
        {
            startIndex += 2;
        }
        else if (afterStart.StartsWith("\n".AsSpan(), StringComparison.Ordinal))
        {
            startIndex += 1;
        }

        var endIndex = input.IndexOf(endMarker, startIndex, StringComparison.OrdinalIgnoreCase);
        return endIndex >= 0 ? input[startIndex..endIndex] : input[startIndex..];
    }

    private static string DescribeCode(byte code)
    {
        return code switch
        {
            0x80 => "Connect",
            0x81 => "Disconnect",
            0x82 => "PutFinal",
            0x83 => "GetFinal",
            0x85 => "SetPath",
            0x90 => "Continue",
            0xA0 => "Success",
            _ => $"Unknown(0x{code:X2})"
        };
    }

    private static string GetHeaderName(byte headerId)
    {
        return HeaderNames.GetValueOrDefault(headerId, $"Unknown(0x{headerId:X2})");
    }

    private static uint? ReadUInt32BigEndian(ReadOnlySpan<byte> value)
    {
        return value.Length == 4
            ? (uint)((value[0] << 24) | (value[1] << 16) | (value[2] << 8) | value[3])
            : null;
    }

    private static ushort? ReadUInt16BigEndian(ReadOnlySpan<byte> value)
    {
        return value.Length == 2
            ? (ushort)((value[0] << 8) | value[1])
            : null;
    }

    private static string? DecodeUnicodeHeader(byte[] value)
    {
        if (value.Length == 0)
        {
            return string.Empty;
        }

        try
        {
            var decoded = Encoding.BigEndianUnicode.GetString(value);
            return decoded.TrimEnd('\0');
        }
        catch
        {
            return null;
        }
    }

    private static string? TryDecodeUtf8(byte[] value)
    {
        try
        {
            var decoded = Encoding.UTF8.GetString(value).TrimEnd('\0');
            return decoded.Any(character => char.IsControl(character) && !char.IsWhiteSpace(character))
                ? null
                : decoded;
        }
        catch
        {
            return null;
        }
    }

    private static string? Truncate(string? value, int maxLength)
    {
        if (string.IsNullOrEmpty(value) || value.Length <= maxLength)
        {
            return value;
        }

        return $"{value[..maxLength]}...(truncated)";
    }

    private sealed record ParsedObexPacket(
        string CodeHex,
        string CodeName,
        int PacketLength,
        object? OperationFields,
        object[] Headers,
        object? Body);
}
