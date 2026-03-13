namespace Adit.Core.Utilities;

public static class PhoneNumberNormalizer
{
    public static string? Normalize(string? rawNumber, string defaultCountryCode = "1")
    {
        if (string.IsNullOrWhiteSpace(rawNumber))
        {
            return null;
        }

        var trimmed = rawNumber.Trim();
        var hasLeadingPlus = trimmed.StartsWith('+');
        var digits = new string(trimmed.Where(char.IsDigit).ToArray());
        if (digits.Length == 0)
        {
            return null;
        }

        if (hasLeadingPlus)
        {
            return $"+{digits}";
        }

        if (digits.Length == 11 && digits.StartsWith(defaultCountryCode, StringComparison.Ordinal))
        {
            return $"+{digits}";
        }

        if (digits.Length == 10 && !string.IsNullOrWhiteSpace(defaultCountryCode))
        {
            return $"+{defaultCountryCode}{digits}";
        }

        return $"+{digits}";
    }
}
