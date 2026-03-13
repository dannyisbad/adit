using Adit.Probe;

namespace Adit.Probe.Tests;

public sealed class ProbeOptionsTests
{
    [Fact]
    public void Parse_ReadsMapProbeTuningOptions()
    {
        var options = ProbeOptions.Parse(
        [
            "--map-probe",
            "--watch-seconds",
            "120",
            "--message-limit",
            "40"
        ]);

        Assert.True(options.MapProbe);
        Assert.Equal(120, options.MapWatchSeconds);
        Assert.Equal(40, options.MapMessageLimit);
    }

    [Fact]
    public void Parse_ReadsPairingProbeAndRfcommOptions()
    {
        var options = ProbeOptions.Parse(
        [
            "--pairing-probe",
            "--ancs-include-preexisting",
            "--rfcomm-service",
            "02030302-1d19-415f-86f2-22a2106a0a77",
            "--rfcomm-hex",
            "ff5a0000"
        ]);

        Assert.True(options.PairingProbe);
        Assert.True(options.AncsIncludePreexisting);
        Assert.Equal("02030302-1d19-415f-86f2-22a2106a0a77", options.RfcommServiceUuid);
        Assert.Equal(["FF5A0000"], options.RfcommHexPayloads);
    }

    [Fact]
    public void Parse_RejectsNegativeWatchSeconds()
    {
        var exception = Assert.Throws<ArgumentException>(
            () => ProbeOptions.Parse(["--watch-seconds", "-1"]));

        Assert.Contains("--watch-seconds", exception.Message, StringComparison.Ordinal);
    }
}
