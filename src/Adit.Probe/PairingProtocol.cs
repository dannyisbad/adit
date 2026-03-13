using System.Buffers.Binary;
using System.Security.Cryptography;

namespace Adit.Probe;

internal static class PairingProtocol
{
    public static byte[] BuildPairingResultPayload(
        byte[]? pairingId,
        Guid? sessionId,
        byte? resultStatus)
    {
        using var stream = new MemoryStream();

        if (pairingId is not null)
        {
            WriteField(stream, (byte)PairingResultFieldId.PairingId, pairingId, littleEndianLength: true);
        }

        if (sessionId is not null)
        {
            WriteField(
                stream,
                (byte)PairingResultFieldId.SessionId,
                sessionId.Value.ToByteArray(),
                littleEndianLength: true);
        }

        if (resultStatus is not null)
        {
            WriteField(
                stream,
                (byte)PairingResultFieldId.ResultStatus,
                [resultStatus.Value],
                littleEndianLength: true);
        }

        return stream.ToArray();
    }

    public static byte[] CreateRandomPairingId(int length = 16)
    {
        var bytes = new byte[length];
        RandomNumberGenerator.Fill(bytes);
        return bytes;
    }

    public static IReadOnlyList<PairingProtocolField> ParseFields(
        byte[] payload,
        bool littleEndianLength = false)
    {
        var fields = new List<PairingProtocolField>();
        var offset = 0;

        while (offset < payload.Length)
        {
            if (payload.Length - offset < 3)
            {
                fields.Add(
                    new PairingProtocolField(
                        payload[offset],
                        0,
                        payload[offset..],
                        true,
                        "truncated_header"));
                break;
            }

            var fieldId = payload[offset];
            var fieldLength = littleEndianLength
                ? BinaryPrimitives.ReadUInt16LittleEndian(payload.AsSpan(offset + 1, 2))
                : BinaryPrimitives.ReadUInt16BigEndian(payload.AsSpan(offset + 1, 2));
            offset += 3;

            if (payload.Length - offset < fieldLength)
            {
                fields.Add(
                    new PairingProtocolField(
                        fieldId,
                        fieldLength,
                        payload[offset..],
                        true,
                        "truncated_value"));
                break;
            }

            var value = payload.AsSpan(offset, fieldLength).ToArray();
            fields.Add(new PairingProtocolField(fieldId, fieldLength, value, false, null));
            offset += fieldLength;
        }

        return fields;
    }

    public static object DescribeFields(byte[] payload, PairingFieldSet fieldSet, string? protocolVersion = null)
    {
        var fields = ParseFields(
            payload,
            littleEndianLength: fieldSet == PairingFieldSet.PairingResult);
        return new
        {
            payloadHex = Convert.ToHexString(payload),
            fieldCount = fields.Count,
            fields = fields.Select(field => DescribeField(field, fieldSet, protocolVersion))
        };
    }

    private static object DescribeField(
        PairingProtocolField field,
        PairingFieldSet fieldSet,
        string? protocolVersion)
    {
        var fieldName = fieldSet switch
        {
            PairingFieldSet.PairingInfo => field.Id == (byte)PairingInfoFieldId.PairingId ? "pairing_id" : "unknown",
            PairingFieldSet.DeviceInfo => field.Id switch
            {
                (byte)DeviceInfoFieldId.OsVersion => "os_version",
                (byte)DeviceInfoFieldId.Locale => "locale",
                (byte)DeviceInfoFieldId.CompanionApp => "companion_app",
                _ => "unknown"
            },
            PairingFieldSet.PairingResult => field.Id switch
            {
                (byte)PairingResultFieldId.PairingId => "pairing_id",
                (byte)PairingResultFieldId.SessionId => "session_id",
                (byte)PairingResultFieldId.ResultStatus => "result_status",
                _ => "unknown"
            },
            _ => "unknown"
        };

        return new
        {
            id = field.Id,
            name = fieldName,
            length = field.Length,
            malformed = field.Malformed,
            note = field.Note,
            payloadHex = Convert.ToHexString(field.Value),
            payloadUtf8 = TryDecodeUtf8(field.Value),
            decoded = DescribeFieldValue(field, fieldSet, protocolVersion)
        };
    }

    private static object? DescribeFieldValue(
        PairingProtocolField field,
        PairingFieldSet fieldSet,
        string? protocolVersion)
    {
        return fieldSet switch
        {
            PairingFieldSet.PairingInfo when field.Id == (byte)PairingInfoFieldId.PairingId => new
            {
                pairingIdHex = Convert.ToHexString(field.Value)
            },
            PairingFieldSet.DeviceInfo when field.Id == (byte)DeviceInfoFieldId.OsVersion &&
                                            field.Value.Length == 2 => new
            {
                osVersionRaw = BinaryPrimitives.ReadUInt16BigEndian(field.Value),
                osVersion = BinaryPrimitives.ReadUInt16BigEndian(field.Value).ToString()
            },
            PairingFieldSet.DeviceInfo when field.Id == (byte)DeviceInfoFieldId.Locale => new
            {
                locale = TryDecodeUtf8(field.Value)
            },
            PairingFieldSet.DeviceInfo when field.Id == (byte)DeviceInfoFieldId.CompanionApp &&
                                            field.Value.Length > 0 => new
            {
                companionAppRaw = field.Value[0],
                companionAppName = DescribeCompanionApp(field.Value[0], protocolVersion)
            },
            PairingFieldSet.PairingResult when field.Id == (byte)PairingResultFieldId.PairingId => new
            {
                pairingIdHex = Convert.ToHexString(field.Value)
            },
            PairingFieldSet.PairingResult when field.Id == (byte)PairingResultFieldId.SessionId &&
                                              field.Value.Length == 16 => new
            {
                sessionGuid = new Guid(field.Value).ToString()
            },
            PairingFieldSet.PairingResult when field.Id == (byte)PairingResultFieldId.ResultStatus &&
                                              field.Value.Length > 0 => new
            {
                resultStatus = field.Value[0]
            },
            _ => null
        };
    }

    private static string DescribeCompanionApp(byte value, string? protocolVersion)
    {
        return protocolVersion switch
        {
            null or "" or "1.0" => value == 0 ? "Legacy" : $"Unknown({value})",
            _ => value switch
            {
                0 => "Unknown",
                1 => "LinkToWindows",
                2 => "PhoneLink",
                _ => $"Unknown({value})"
            }
        };
    }

    private static void WriteField(
        Stream stream,
        byte fieldId,
        byte[] value,
        bool littleEndianLength)
    {
        Span<byte> header = stackalloc byte[3];
        header[0] = fieldId;
        if (littleEndianLength)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(header[1..], checked((ushort)value.Length));
        }
        else
        {
            BinaryPrimitives.WriteUInt16BigEndian(header[1..], checked((ushort)value.Length));
        }
        stream.Write(header);
        stream.Write(value);
    }

    private static string? TryDecodeUtf8(byte[] bytes)
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
}

internal enum PairingFieldSet
{
    Unknown,
    PairingInfo,
    DeviceInfo,
    PairingResult
}

internal enum PairingInfoFieldId : byte
{
    PairingId = 0
}

internal enum DeviceInfoFieldId : byte
{
    OsVersion = 0,
    Locale = 1,
    CompanionApp = 2
}

internal enum PairingResultFieldId : byte
{
    PairingId = 0,
    SessionId = 1,
    ResultStatus = 2
}

internal sealed record PairingProtocolField(
    byte Id,
    ushort Length,
    byte[] Value,
    bool Malformed,
    string? Note);
