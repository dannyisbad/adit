using Adit.Probe;

namespace Adit.Probe.Tests;

public sealed class PhoneNumberNormalizerTests
{
    [Theory]
    [InlineData("(202) 555-0100", "+12025550100")]
    [InlineData("1-212-555-0100", "+12125550100")]
    [InlineData("+44 20 7946 0958", "+442079460958")]
    [InlineData("42302", "+42302")]
    public void Normalize_ReturnsDialableCanonicalForm(string input, string expected)
    {
        Assert.Equal(expected, PhoneNumberNormalizer.Normalize(input));
    }

    [Fact]
    public void Normalize_ReturnsNullForEmptyInput()
    {
        Assert.Null(PhoneNumberNormalizer.Normalize("   "));
    }
}
