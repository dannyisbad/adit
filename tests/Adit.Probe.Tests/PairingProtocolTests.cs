using Adit.Probe;

namespace Adit.Probe.Tests;

public sealed class PairingProtocolTests
{
    [Fact]
    public void BuildPairingResultPayload_EncodesLittleEndianFieldLengths()
    {
        var pairingId = Enumerable.Range(0, 4).Select(static value => (byte)value).ToArray();
        var sessionId = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");

        var payload = PairingProtocol.BuildPairingResultPayload(pairingId, sessionId, resultStatus: 0);
        var fields = PairingProtocol.ParseFields(payload, littleEndianLength: true);

        Assert.Equal(3, fields.Count);
        Assert.Equal((byte)PairingResultFieldId.PairingId, fields[0].Id);
        Assert.Equal<ushort>(4, fields[0].Length);
        Assert.Equal(pairingId, fields[0].Value);
        Assert.Equal((byte)PairingResultFieldId.SessionId, fields[1].Id);
        Assert.Equal<ushort>(16, fields[1].Length);
        Assert.Equal(sessionId.ToByteArray(), fields[1].Value);
        Assert.Equal((byte)PairingResultFieldId.ResultStatus, fields[2].Id);
        Assert.Equal<ushort>(1, fields[2].Length);
        Assert.Equal([0], fields[2].Value);
    }

    [Fact]
    public void ParseFields_FlagsTruncatedPayload()
    {
        var fields = PairingProtocol.ParseFields([0x00, 0x00, 0x04, 0xAA]);

        var field = Assert.Single(fields);
        Assert.True(field.Malformed);
        Assert.Equal("truncated_value", field.Note);
    }

    [Fact]
    public void ParseFields_SupportsLittleEndianLengths()
    {
        var fields = PairingProtocol.ParseFields([0x02, 0x01, 0x00, 0x00], littleEndianLength: true);

        var field = Assert.Single(fields);
        Assert.False(field.Malformed);
        Assert.Equal((byte)PairingResultFieldId.ResultStatus, field.Id);
        Assert.Equal<ushort>(1, field.Length);
        Assert.Equal([0x00], field.Value);
    }
}
