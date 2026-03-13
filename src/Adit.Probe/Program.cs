using Adit.Probe;

internal static class Program
{
    [STAThread]
    private static async Task<int> Main(string[] args)
    {
        ProbeOptions options;

        try
        {
            options = ProbeOptions.Parse(args);
        }
        catch (ArgumentException exception)
        {
            Console.Error.WriteLine(exception.Message);
            ProbeOptions.WriteUsage();
            return 1;
        }

        Directory.CreateDirectory(options.LogDirectory);
        var logPath = Path.Combine(
            options.LogDirectory,
            $"ancs-{DateTimeOffset.Now:yyyyMMdd-HHmmss-fff}-{Environment.ProcessId}.jsonl");

        using var logger = new ProbeLogger(logPath);
        logger.Log(
            "probe.started",
            new
            {
                osVersion = Environment.OSVersion.VersionString,
                framework = Environment.Version.ToString(),
                packageIdentity = PackageIdentitySnapshot.Capture(),
                options
            });

        if (options.RfcommScan || options.MapProbe || options.PbapProbe)
        {
            IReadOnlyList<BluetoothEndpointRecord> endpoints;

            try
            {
                endpoints = await DeviceDiscovery.ListPairedBluetoothEndpointsAsync();
            }
            catch (Exception exception)
            {
                logger.Log(
                    "probe.discovery_failed",
                    new
                    {
                        error = exception.ToString(),
                        mode = options.MapProbe ? "map" : options.PbapProbe ? "pbap" : "rfcomm"
                    });
                return 1;
            }

            var classicTarget = DeviceDiscovery.SelectClassicTarget(endpoints, options);
            if (classicTarget is null)
            {
                DeviceDiscovery.WriteBluetoothEndpoints(endpoints);
                Console.Error.WriteLine(
                    "No classic Bluetooth target matched. Re-run with --name \"My iPhone\" or --id \"...\".");
                logger.Log(
                    "probe.no_target",
                    new
                    {
                        mode = options.MapProbe ? "map" : options.PbapProbe ? "pbap" : "rfcomm",
                        endpointCount = endpoints.Count,
                        options.DeviceId,
                        options.NameContains
                    });
                Console.WriteLine($"Log file: {logPath}");
                return 1;
            }

            logger.Log("probe.classic_target_selected", classicTarget);
            int classicExitCode;

            if (options.MapProbe)
            {
                using var classicCancellationSource = new CancellationTokenSource();
                Console.CancelKeyPress += (_, eventArgs) =>
                {
                    eventArgs.Cancel = true;
                    classicCancellationSource.Cancel();
                };

                var mapProbe = new MicrosoftMapProbe(classicTarget, options, logger);
                classicExitCode = await mapProbe.RunAsync(classicCancellationSource.Token);
            }
            else if (options.PbapProbe)
            {
                using var classicCancellationSource = new CancellationTokenSource();
                Console.CancelKeyPress += (_, eventArgs) =>
                {
                    eventArgs.Cancel = true;
                    classicCancellationSource.Cancel();
                };

                var pbapProbe = new MicrosoftPbapProbe(classicTarget, options, logger);
                classicExitCode = await pbapProbe.RunAsync(classicCancellationSource.Token);
            }
            else
            {
                var rfcommProbe = new ClassicRfcommProbe(classicTarget, options, logger);
                classicExitCode = await rfcommProbe.RunAsync();
            }

            logger.Log(
                "probe.stopped",
                new
                {
                    exitCode = classicExitCode,
                    mode = options.MapProbe ? "map" : options.PbapProbe ? "pbap" : "rfcomm"
                });
            Console.WriteLine($"Log file: {logPath}");
            return classicExitCode;
        }

        if (!string.IsNullOrWhiteSpace(options.RawBleAddress))
        {
            using var rawBleCancellationSource = new CancellationTokenSource();
            Console.CancelKeyPress += (_, eventArgs) =>
            {
                eventArgs.Cancel = true;
                rawBleCancellationSource.Cancel();
            };

            var rawBleExitCode = await new RawBleAddressProbe(options.RawBleAddress, options, logger)
                .RunAsync(rawBleCancellationSource.Token);
            logger.Log("probe.stopped", new { exitCode = rawBleExitCode, mode = "ble_address" });
            Console.WriteLine($"Log file: {logPath}");
            return rawBleExitCode;
        }

        IReadOnlyList<PairedDeviceRecord> devices;

        try
        {
            devices = await DeviceDiscovery.ListPairedBluetoothLeDevicesAsync();
        }
        catch (Exception exception)
        {
            logger.Log("probe.discovery_failed", new { error = exception.ToString() });
            return 1;
        }

        if (options.ListAll)
        {
            var endpoints = await DeviceDiscovery.ListPairedBluetoothEndpointsAsync();
            DeviceDiscovery.WriteBluetoothEndpoints(endpoints);
            logger.Log("probe.endpoints_listed", new { endpointCount = endpoints.Count });
            Console.WriteLine($"Log file: {logPath}");
            return 0;
        }

        if (options.ListOnly)
        {
            DeviceDiscovery.WriteDevices(devices);
            logger.Log("probe.devices_listed", new { deviceCount = devices.Count });
            Console.WriteLine($"Log file: {logPath}");
            return 0;
        }

        var target = DeviceDiscovery.SelectTarget(devices, options);
        if (target is null)
        {
            DeviceDiscovery.WriteDevices(devices);
            Console.Error.WriteLine(
                "No target device matched. Re-run with --name \"My iPhone\" or --id \"...\".");
            logger.Log(
                "probe.no_target",
                new
                {
                    deviceCount = devices.Count,
                    options.DeviceId,
                    options.NameContains
                });
            Console.WriteLine($"Log file: {logPath}");
            return 1;
        }

        logger.Log("probe.target_selected", target);

        using var cancellationSource = new CancellationTokenSource();
        Console.CancelKeyPress += (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            cancellationSource.Cancel();
        };

        var exitCode = options.AdvertisementProbe
            ? await new BleAdvertisementProbe(target, options, logger).RunAsync(cancellationSource.Token)
            : options.PairingProbe
                ? await new PairingProtocolProbe(target, options, logger).RunAsync(cancellationSource.Token)
                : await new AncsProbe(target, options, logger).RunAsync(cancellationSource.Token);

        logger.Log("probe.stopped", new { exitCode });
        Console.WriteLine($"Log file: {logPath}");
        return exitCode;
    }
}
