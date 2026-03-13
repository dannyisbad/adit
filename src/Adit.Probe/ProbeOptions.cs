namespace Adit.Probe;

internal sealed record ProbeOptions
{
    public bool ListOnly { get; init; }

    public bool ListAll { get; init; }

    public bool RfcommScan { get; init; }

    public bool MapProbe { get; init; }

    public bool PbapProbe { get; init; }

    public bool PairingProbe { get; init; }

    public bool AdvertisementProbe { get; init; }

    public bool EvictPhoneLink { get; init; }

    public string? NameContains { get; init; }

    public string? DeviceId { get; init; }

    public string? RawBleAddress { get; init; }

    public string? Recipient { get; init; }

    public string? MessageBody { get; init; }

    public string? MapHandle { get; init; }

    public string? MarkReadHandle { get; init; }

    public int MapWatchSeconds { get; init; }

    public int MapMessageLimit { get; init; } = 25;

    public string LogDirectory { get; init; } = Path.Combine(AppContext.BaseDirectory, "logs");

    public int AttributeTimeoutSeconds { get; init; } = 10;

    public string? AncsAutoAction { get; init; }

    public string? MatchText { get; init; }

    public bool AncsIncludePreexisting { get; init; }

    public string? RfcommServiceUuid { get; init; }

    public IReadOnlyList<string> RfcommHexPayloads { get; init; } = [];

    public static ProbeOptions Parse(string[] args)
    {
        var options = new ProbeOptions();

        for (var index = 0; index < args.Length; index++)
        {
            var argument = args[index];
            switch (argument)
            {
                case "--list":
                    options = options with { ListOnly = true };
                    break;
                case "--list-all":
                    options = options with { ListAll = true };
                    break;
                case "--rfcomm-scan":
                    options = options with { RfcommScan = true };
                    break;
                case "--map-probe":
                    options = options with { MapProbe = true };
                    break;
                case "--pbap-probe":
                    options = options with { PbapProbe = true };
                    break;
                case "--pairing-probe":
                    options = options with { PairingProbe = true };
                    break;
                case "--adv-probe":
                    options = options with { AdvertisementProbe = true };
                    break;
                case "--evict-phone-link":
                    options = options with { EvictPhoneLink = true };
                    break;
                case "--name":
                    options = options with { NameContains = ReadValue(args, ref index, argument) };
                    break;
                case "--id":
                    options = options with { DeviceId = ReadValue(args, ref index, argument) };
                    break;
                case "--ble-address-probe":
                    options = options with { RawBleAddress = ReadValue(args, ref index, argument) };
                    break;
                case "--recipient":
                    options = options with { Recipient = ReadValue(args, ref index, argument) };
                    break;
                case "--body":
                    options = options with { MessageBody = ReadValue(args, ref index, argument) };
                    break;
                case "--handle":
                    options = options with { MapHandle = ReadValue(args, ref index, argument) };
                    break;
                case "--mark-read-handle":
                    options = options with { MarkReadHandle = ReadValue(args, ref index, argument) };
                    break;
                case "--watch-seconds":
                    options = options with
                    {
                        MapWatchSeconds = ReadNonNegativeInt(
                            ReadValue(args, ref index, argument),
                            argument)
                    };
                    break;
                case "--message-limit":
                    options = options with
                    {
                        MapMessageLimit = ReadPositiveInt(
                            ReadValue(args, ref index, argument),
                            argument)
                    };
                    break;
                case "--log-dir":
                    options = options with { LogDirectory = ReadValue(args, ref index, argument) };
                    break;
                case "--attribute-timeout-seconds":
                    options = options with
                    {
                        AttributeTimeoutSeconds = ReadPositiveInt(
                            ReadValue(args, ref index, argument),
                            argument)
                    };
                    break;
                case "--ancs-auto-action":
                    options = options with
                    {
                        AncsAutoAction = ReadAutoAction(
                            ReadValue(args, ref index, argument),
                            argument)
                    };
                    break;
                case "--match-text":
                    options = options with { MatchText = ReadValue(args, ref index, argument) };
                    break;
                case "--ancs-include-preexisting":
                    options = options with { AncsIncludePreexisting = true };
                    break;
                case "--rfcomm-service":
                    options = options with
                    {
                        RfcommServiceUuid = ReadGuidString(
                            ReadValue(args, ref index, argument),
                            argument)
                    };
                    break;
                case "--rfcomm-hex":
                    options = options with
                    {
                        RfcommHexPayloads = [.. options.RfcommHexPayloads, ReadHexString(
                            ReadValue(args, ref index, argument),
                            argument)]
                    };
                    break;
                case "--help":
                case "-h":
                case "/?":
                    WriteUsage();
                    Environment.Exit(0);
                    break;
                default:
                    throw new ArgumentException($"Unknown argument: {argument}");
            }
        }

        return options;
    }

    public static void WriteUsage()
    {
        Console.WriteLine("Usage:");
        Console.WriteLine("  dotnet run --project src/Adit.Probe -- --list");
        Console.WriteLine("  dotnet run --project src/Adit.Probe -- --list-all");
        Console.WriteLine("  dotnet run --project src/Adit.Probe -- --rfcomm-scan --name \"Riley's iPhone\"");
        Console.WriteLine("  dotnet run --project src/Adit.Probe -- --rfcomm-scan --name \"Riley's iPhone\" --rfcomm-service 02030302-1d19-415f-86f2-22a2106a0a77 --rfcomm-hex FF5A0000");
        Console.WriteLine("  dotnet run --project src/Adit.Probe -- --ble-address-probe \"6E:1B:A4:73:CC:28\"");
        Console.WriteLine("  dotnet run --project src/Adit.Probe -- --map-probe --name \"Riley's iPhone\"");
        Console.WriteLine("  dotnet run --project src/Adit.Probe -- --pbap-probe --name \"Riley's iPhone\"");
        Console.WriteLine("  dotnet run --project src/Adit.Probe -- --pairing-probe --name \"Riley's iPhone\"");
        Console.WriteLine("  dotnet run --project src/Adit.Probe -- --adv-probe --name \"Riley's iPhone\" --watch-seconds 15");
        Console.WriteLine("  dotnet run --project src/Adit.Probe -- --map-probe --evict-phone-link --name \"Riley's iPhone\"");
        Console.WriteLine("  dotnet run --project src/Adit.Probe -- --map-probe --name \"Riley's iPhone\" --watch-seconds 120");
        Console.WriteLine("  dotnet run --project src/Adit.Probe -- --name \"Riley's iPhone\" --watch-seconds 20 --ancs-auto-action positive --match-text \"Messages\"");
        Console.WriteLine("  dotnet run --project src/Adit.Probe -- --name \"Riley's iPhone\" --watch-seconds 20 --ancs-include-preexisting");
        Console.WriteLine("  dotnet run --project src/Adit.Probe -- --map-probe --name \"Riley's iPhone\" --handle \"ABC123...\"");
        Console.WriteLine("  dotnet run --project src/Adit.Probe -- --map-probe --name \"Riley's iPhone\" --mark-read-handle \"ABC123...\"");
        Console.WriteLine("  dotnet run --project src/Adit.Probe -- --map-probe --name \"Riley's iPhone\" --recipient \"+15551234567\" --body \"hello\"");
        Console.WriteLine("  dotnet run --project src/Adit.Probe -- --name \"Riley's iPhone\"");
        Console.WriteLine("  dotnet run --project src/Adit.Probe -- --id \"BluetoothLE#...\"");
        Console.WriteLine();
        Console.WriteLine("Options:");
        Console.WriteLine("  --list                             List paired BLE devices and exit.");
        Console.WriteLine("  --list-all                         List paired classic + BLE endpoints and exit.");
        Console.WriteLine("  --rfcomm-scan                      Query classic Bluetooth RFCOMM services for the selected device.");
        Console.WriteLine("  --rfcomm-service <uuid>            Limit RFCOMM custom-socket probing to one service UUID.");
        Console.WriteLine("  --rfcomm-hex <hex>                 Raw hex payload to write on the custom RFCOMM socket. Can be repeated.");
        Console.WriteLine("  --ble-address-probe <address>     Open a BLE device directly from a live advertisement address.");
        Console.WriteLine("  --map-probe                        Open the classic MAP client and try listing inbox messages.");
        Console.WriteLine("  --pbap-probe                       Open PBAP and fetch contacts.");
        Console.WriteLine("  --pairing-probe                    Exercise the custom BLE pairing service on the selected device.");
        Console.WriteLine("  --adv-probe                        Capture BLE advertisements for the selected device.");
        Console.WriteLine("  --evict-phone-link                 Kill Phone Link holders before MAP takeover.");
        Console.WriteLine("  --name <substring>                 Pick the first paired BLE device whose name contains this text.");
        Console.WriteLine("  --id <device-id>                   Pick an exact device id.");
        Console.WriteLine("  --recipient <phone>                Recipient number for MAP send testing.");
        Console.WriteLine("  --body <text>                      Message text for MAP send testing.");
        Console.WriteLine("  --handle <map-handle>              Fetch a specific MAP message handle.");
        Console.WriteLine("  --mark-read-handle <map-handle>    Set a specific MAP message handle to read.");
        Console.WriteLine("  --watch-seconds <n>                Keep the selected probe open for live events before exiting.");
        Console.WriteLine("  --message-limit <n>                Max messages to request per MAP folder.");
        Console.WriteLine("  --log-dir <path>                   Directory for JSONL logs.");
        Console.WriteLine("  --attribute-timeout-seconds <n>    Timeout for ANCS attribute reads.");
        Console.WriteLine("  --ancs-auto-action <positive|negative>  Fire ANCS actions on matching notifications when available.");
        Console.WriteLine("  --match-text <substring>           Limit ANCS auto-action to notifications whose attrs contain this text.");
        Console.WriteLine("  --ancs-include-preexisting         Request attributes for the initial ANCS preexisting notification flood.");
    }

    private static string ReadValue(string[] args, ref int index, string optionName)
    {
        var nextIndex = index + 1;
        if (nextIndex >= args.Length)
        {
            throw new ArgumentException($"Missing value for {optionName}.");
        }

        index = nextIndex;
        return args[index];
    }

    private static int ReadPositiveInt(string rawValue, string optionName)
    {
        if (!int.TryParse(rawValue, out var value) || value <= 0)
        {
            throw new ArgumentException($"{optionName} must be a positive integer.");
        }

        return value;
    }

    private static int ReadNonNegativeInt(string rawValue, string optionName)
    {
        if (!int.TryParse(rawValue, out var value) || value < 0)
        {
            throw new ArgumentException($"{optionName} must be a non-negative integer.");
        }

        return value;
    }

    private static string ReadAutoAction(string rawValue, string optionName)
    {
        return rawValue.ToLowerInvariant() switch
        {
            "positive" or "negative" => rawValue.ToLowerInvariant(),
            _ => throw new ArgumentException($"{optionName} must be 'positive' or 'negative'.")
        };
    }

    private static string ReadGuidString(string rawValue, string optionName)
    {
        if (!Guid.TryParse(rawValue, out var parsed))
        {
            throw new ArgumentException($"{optionName} must be a GUID.");
        }

        return parsed.ToString();
    }

    private static string ReadHexString(string rawValue, string optionName)
    {
        var normalized = new string(rawValue.Where(character => !char.IsWhiteSpace(character)).ToArray());
        if (normalized.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
        {
            normalized = normalized[2..];
        }

        if (normalized.Length == 0 || normalized.Length % 2 != 0)
        {
            throw new ArgumentException($"{optionName} must contain an even number of hex digits.");
        }

        if (!normalized.All(Uri.IsHexDigit))
        {
            throw new ArgumentException($"{optionName} must be hex.");
        }

        return normalized.ToUpperInvariant();
    }
}
