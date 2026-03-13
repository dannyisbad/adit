using Adit.Probe.MapInterop;
using System.Diagnostics;
using Windows.Devices.Bluetooth;
using Windows.Devices.Bluetooth.Rfcomm;
using Windows.Devices.Enumeration;
using Windows.Networking.Sockets;
using Windows.Storage.Streams;

namespace Adit.Probe;

internal sealed class ClassicRfcommProbe
{
    private static readonly TimeSpan CustomServiceConnectTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan CustomServiceReadTimeout = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan CustomServiceInterProbeDelay = TimeSpan.FromMilliseconds(750);
    private static readonly TimeSpan DefaultCustomServiceSessionDuration = TimeSpan.FromSeconds(10);
    private static readonly IReadOnlyList<ActiveSocketProbePayload> DefaultActiveSocketProbePayloads =
    [
        new("sync_ff55", [0xFF, 0x55]),
        new("sync_ff5a", [0xFF, 0x5A]),
        new("sync_ff5a_len0", [0xFF, 0x5A, 0x00, 0x00]),
        new("sync_ff5a_len2_zero", [0xFF, 0x5A, 0x00, 0x02, 0x00, 0x00]),
        new("null_byte", [0x00]),
        new("carriage_return", [0x0D]),
        new("at_cr", System.Text.Encoding.ASCII.GetBytes("AT\r"))
    ];
    private static readonly IReadOnlyList<KnownRfcommService> KnownServices =
    [
        new("handsfree_audio_gateway", new Guid("0000111F-0000-1000-8000-00805F9B34FB")),
        new("phonebook_access_pse", new Guid("0000112F-0000-1000-8000-00805F9B34FB")),
        new("message_access_mas", new Guid("00001132-0000-1000-8000-00805F9B34FB"))
    ];

    private readonly IReadOnlyList<ActiveSocketProbePayload> activeSocketProbePayloads;
    private readonly ProbeLogger logger;
    private readonly Guid? rfcommServiceUuid;
    private readonly BluetoothEndpointRecord target;
    private readonly int watchSeconds;

    public ClassicRfcommProbe(BluetoothEndpointRecord target, ProbeOptions options, ProbeLogger logger)
    {
        this.target = target;
        this.logger = logger;
        watchSeconds = options.MapWatchSeconds;
        rfcommServiceUuid = Guid.TryParse(options.RfcommServiceUuid, out var parsedServiceUuid)
            ? parsedServiceUuid
            : null;
        activeSocketProbePayloads = BuildActiveSocketProbePayloads(options);
    }

    public async Task<int> RunAsync()
    {
        try
        {
            using var device = await BluetoothDevice.FromIdAsync(target.Id);
            if (device is null)
            {
                logger.Log("rfcomm.device_open_failed", new { target.Id, target.Name });
                return 1;
            }

            logger.Log(
                "rfcomm.device_opened",
                new
                {
                    device.Name,
                    bluetoothAddress = device.BluetoothAddress.ToString("X"),
                    connectionStatus = device.ConnectionStatus.ToString(),
                    access = await GetDeviceAccessSnapshotAsync(device)
                });

            var servicesResult = await device.GetRfcommServicesAsync(BluetoothCacheMode.Uncached);
            logger.Log(
                "rfcomm.services_query",
                new
                {
                    error = servicesResult.Error.ToString(),
                    serviceCount = servicesResult.Services.Count,
                    serviceFilter = rfcommServiceUuid,
                    services = servicesResult.Services.Select(DescribeService)
                });

            foreach (var service in servicesResult.Services)
            {
                using (service)
                {
                    await LogSdpAsync(service);
                    if (ShouldProbeService(service))
                    {
                        await ProbeCustomServiceSocketAsync(service);
                    }
                }
            }

            foreach (var knownService in KnownServices)
            {
                var knownResult = await device.GetRfcommServicesForIdAsync(
                    RfcommServiceId.FromUuid(knownService.Uuid),
                    BluetoothCacheMode.Uncached);
                logger.Log(
                    "rfcomm.known_service_query",
                    new
                    {
                        knownService.Name,
                        uuid = knownService.Uuid,
                        error = knownResult.Error.ToString(),
                        serviceCount = knownResult.Services.Count,
                        services = knownResult.Services.Select(DescribeService)
                    });
            }

            return 0;
        }
        catch (Exception exception)
        {
            logger.Log("rfcomm.unhandled_exception", new { error = exception.ToString() });
            return 1;
        }
    }

    private static IReadOnlyList<ActiveSocketProbePayload> BuildActiveSocketProbePayloads(ProbeOptions options)
    {
        if (options.RfcommHexPayloads.Count == 0)
        {
            return DefaultActiveSocketProbePayloads;
        }

        var payloads = new List<ActiveSocketProbePayload>();
        foreach (var rawPayload in options.RfcommHexPayloads)
        {
            payloads.Add(new($"custom_{rawPayload}", Convert.FromHexString(rawPayload)));
        }

        payloads.AddRange(DefaultActiveSocketProbePayloads);
        return payloads;
    }

    private static object DescribeService(RfcommDeviceService service)
    {
        return new
        {
            serviceId = service.ServiceId.Uuid,
            connectionServiceName = service.ConnectionServiceName,
            protectionLevel = service.ProtectionLevel.ToString(),
            maxProtectionLevel = service.MaxProtectionLevel.ToString()
        };
    }

    private async Task LogSdpAsync(RfcommDeviceService service)
    {
        try
        {
            var rawAttributes = await service.GetSdpRawAttributesAsync();
            var attributes = rawAttributes
                .ToDictionary(pair => pair.Key, pair => BufferToBytes(pair.Value));

            logger.Log(
                "rfcomm.service_sdp",
                new
                {
                    serviceId = service.ServiceId.Uuid,
                    connectionServiceName = service.ConnectionServiceName,
                    protectionLevel = service.ProtectionLevel.ToString(),
                    maxProtectionLevel = service.MaxProtectionLevel.ToString(),
                    attributes = attributes
                        .OrderBy(pair => pair.Key)
                        .Select(
                            pair => new
                            {
                                id = $"0x{pair.Key:X4}",
                                key = pair.Key,
                                payloadHex = Convert.ToHexString(pair.Value),
                                payloadUtf8 = TryDecodeUtf8(pair.Value),
                                dataElement = MapSdpInsights.DescribeDataElement(pair.Value)
                            })
                        .ToArray()
                });
        }
        catch (Exception exception)
        {
            logger.Log(
                "rfcomm.service_sdp_failed",
                new
                {
                    serviceId = service.ServiceId.Uuid,
                    connectionServiceName = service.ConnectionServiceName,
                    error = exception.ToString()
                });
        }
    }

    private async Task ProbeCustomServiceSocketAsync(RfcommDeviceService service)
    {
        var attempts = new List<object>();

        foreach (var protectionLevel in GetSocketProtectionLevels(service))
        {
            try
            {
                using var socket = await OpenSocketAsync(service, protectionLevel);
                var sessionDuration = watchSeconds > 0
                    ? TimeSpan.FromSeconds(watchSeconds)
                    : DefaultCustomServiceSessionDuration;
                var sessionStartedAt = Stopwatch.StartNew();
                using var sessionCancellationSource = new CancellationTokenSource();
                var traceState = new SocketTraceState();

                logger.Log(
                    "rfcomm.custom_service_connected",
                    new
                    {
                        serviceId = service.ServiceId.Uuid,
                        connectionServiceName = service.ConnectionServiceName,
                        protectionLevel = protectionLevel.ToString(),
                        remoteAddress = socket.Information.RemoteAddress?.RawName,
                        sessionDurationSeconds = sessionDuration.TotalSeconds,
                        probePayloadCount = activeSocketProbePayloads.Count
                    });

                var readTask = TraceSocketReadsAsync(
                    service,
                    protectionLevel,
                    socket,
                    traceState,
                    sessionStartedAt,
                    sessionCancellationSource.Token);

                await Task.Delay(CustomServiceReadTimeout);
                logger.Log(
                    "rfcomm.custom_service_idle_probe",
                    new
                    {
                        serviceId = service.ServiceId.Uuid,
                        connectionServiceName = service.ConnectionServiceName,
                        protectionLevel = protectionLevel.ToString(),
                        bytesObserved = traceState.TotalBytesObserved,
                        chunkCount = traceState.ChunksObserved,
                        timedOut = traceState.TotalBytesObserved == 0
                    });

                var deadlineUtc = DateTimeOffset.UtcNow + sessionDuration;
                await RunActiveSocketProbesAsync(
                    service,
                    protectionLevel,
                    socket,
                    traceState,
                    sessionStartedAt,
                    deadlineUtc,
                    sessionCancellationSource.Token);

                var remaining = deadlineUtc - DateTimeOffset.UtcNow;
                if (remaining > TimeSpan.Zero)
                {
                    await Task.Delay(remaining, sessionCancellationSource.Token);
                }

                sessionCancellationSource.Cancel();
                await AwaitTraceShutdownAsync(readTask);

                logger.Log(
                    "rfcomm.custom_service_session_complete",
                    new
                    {
                        serviceId = service.ServiceId.Uuid,
                        connectionServiceName = service.ConnectionServiceName,
                        protectionLevel = protectionLevel.ToString(),
                        elapsedMs = sessionStartedAt.ElapsedMilliseconds,
                        bytesObserved = traceState.TotalBytesObserved,
                        chunkCount = traceState.ChunksObserved
                    });

                if (traceState.TotalBytesObserved > 0)
                {
                    return;
                }

                attempts.Add(
                    new
                    {
                        protectionLevel = protectionLevel.ToString(),
                        result = "connected_but_silent"
                    });
            }
            catch (Exception exception)
            {
                attempts.Add(
                    new
                    {
                        protectionLevel = protectionLevel.ToString(),
                        error = exception.ToString()
                    });
            }
        }

        logger.Log(
            "rfcomm.custom_service_probe_failed",
            new
            {
                serviceId = service.ServiceId.Uuid,
                connectionServiceName = service.ConnectionServiceName,
                attempts
            });
    }

    private async Task RunActiveSocketProbesAsync(
        RfcommDeviceService service,
        SocketProtectionLevel protectionLevel,
        StreamSocket socket,
        SocketTraceState traceState,
        Stopwatch sessionStartedAt,
        DateTimeOffset deadlineUtc,
        CancellationToken cancellationToken)
    {
        var outputStream = socket.OutputStream.AsStreamForWrite();

        foreach (var payloadProbe in activeSocketProbePayloads)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (DateTimeOffset.UtcNow >= deadlineUtc)
            {
                logger.Log(
                    "rfcomm.custom_service_active_probe_skipped_deadline",
                    new
                    {
                        serviceId = service.ServiceId.Uuid,
                        connectionServiceName = service.ConnectionServiceName,
                        protectionLevel = protectionLevel.ToString(),
                        probeName = payloadProbe.Name,
                        elapsedMs = sessionStartedAt.ElapsedMilliseconds
                    });
                return;
            }

            try
            {
                logger.Log(
                    "rfcomm.custom_service_active_probe_write",
                    new
                    {
                        serviceId = service.ServiceId.Uuid,
                        connectionServiceName = service.ConnectionServiceName,
                        protectionLevel = protectionLevel.ToString(),
                        probeName = payloadProbe.Name,
                        probePayloadHex = Convert.ToHexString(payloadProbe.Payload),
                        probePayloadUtf8 = TryDecodeUtf8(payloadProbe.Payload),
                        elapsedMs = sessionStartedAt.ElapsedMilliseconds,
                        bytesObservedBeforeWrite = traceState.TotalBytesObserved
                    });

                await outputStream.WriteAsync(
                    payloadProbe.Payload,
                    0,
                    payloadProbe.Payload.Length,
                    cancellationToken);
                await outputStream.FlushAsync(cancellationToken);
            }
            catch (Exception exception)
            {
                logger.Log(
                    "rfcomm.custom_service_active_probe_failed",
                    new
                    {
                        serviceId = service.ServiceId.Uuid,
                        connectionServiceName = service.ConnectionServiceName,
                        protectionLevel = protectionLevel.ToString(),
                        probeName = payloadProbe.Name,
                        probePayloadHex = Convert.ToHexString(payloadProbe.Payload),
                        elapsedMs = sessionStartedAt.ElapsedMilliseconds,
                        error = exception.ToString()
                    });
                return;
            }

            await Task.Delay(CustomServiceInterProbeDelay, cancellationToken);
        }
    }

    private async Task TraceSocketReadsAsync(
        RfcommDeviceService service,
        SocketProtectionLevel protectionLevel,
        StreamSocket socket,
        SocketTraceState traceState,
        Stopwatch sessionStartedAt,
        CancellationToken cancellationToken)
    {
        var stream = socket.InputStream.AsStreamForRead();
        var buffer = new byte[2048];

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                int bytesRead;

                try
                {
                    bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }

                if (bytesRead == 0)
                {
                    logger.Log(
                        "rfcomm.custom_service_watch_eof",
                        new
                        {
                            serviceId = service.ServiceId.Uuid,
                            connectionServiceName = service.ConnectionServiceName,
                            protectionLevel = protectionLevel.ToString(),
                            elapsedMs = sessionStartedAt.ElapsedMilliseconds,
                            bytesObserved = traceState.TotalBytesObserved,
                            chunkCount = traceState.ChunksObserved
                        });
                    return;
                }

                var payload = buffer[..bytesRead].ToArray();
                traceState.TotalBytesObserved += bytesRead;
                traceState.ChunksObserved++;

                logger.Log(
                    "rfcomm.custom_service_watch_chunk",
                    new
                    {
                        serviceId = service.ServiceId.Uuid,
                        connectionServiceName = service.ConnectionServiceName,
                        protectionLevel = protectionLevel.ToString(),
                        elapsedMs = sessionStartedAt.ElapsedMilliseconds,
                        chunkIndex = traceState.ChunksObserved,
                        bytesRead,
                        bytesObserved = traceState.TotalBytesObserved,
                        payloadHex = Convert.ToHexString(payload),
                        payloadUtf8 = TryDecodeUtf8(payload)
                    });
            }
        }
        catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
        {
            logger.Log(
                "rfcomm.custom_service_watch_error",
                new
                {
                    serviceId = service.ServiceId.Uuid,
                    connectionServiceName = service.ConnectionServiceName,
                    protectionLevel = protectionLevel.ToString(),
                    elapsedMs = sessionStartedAt.ElapsedMilliseconds,
                    bytesObserved = traceState.TotalBytesObserved,
                    chunkCount = traceState.ChunksObserved,
                    error = exception.ToString()
                });
        }
    }

    private static async Task AwaitTraceShutdownAsync(Task readTask)
    {
        try
        {
            await readTask.WaitAsync(TimeSpan.FromSeconds(2));
        }
        catch (OperationCanceledException)
        {
        }
        catch (TimeoutException)
        {
        }
    }

    private static async Task<object> GetDeviceAccessSnapshotAsync(BluetoothDevice bluetoothDevice)
    {
        var requestStatus = await bluetoothDevice.RequestAccessAsync();
        var accessInformation = DeviceAccessInformation.CreateFromId(bluetoothDevice.DeviceId);

        return new
        {
            currentStatus = accessInformation.CurrentStatus.ToString(),
            requestStatus = requestStatus.ToString()
        };
    }

    private static byte[] BufferToBytes(IBuffer buffer)
    {
        var bytes = new byte[buffer.Length];
        DataReader.FromBuffer(buffer).ReadBytes(bytes);
        return bytes;
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

    private static IEnumerable<SocketProtectionLevel> GetSocketProtectionLevels(
        RfcommDeviceService service)
    {
        yield return SocketProtectionLevel.BluetoothEncryptionWithAuthentication;
        yield return SocketProtectionLevel.BluetoothEncryptionAllowNullAuthentication;

        if (service.ProtectionLevel == SocketProtectionLevel.PlainSocket ||
            service.MaxProtectionLevel == SocketProtectionLevel.PlainSocket)
        {
            yield return SocketProtectionLevel.PlainSocket;
        }
    }

    private static bool IsCustomService(RfcommDeviceService service)
    {
        return KnownServices.All(knownService => knownService.Uuid != service.ServiceId.Uuid);
    }

    private bool MatchesServiceFilter(RfcommDeviceService service)
    {
        return rfcommServiceUuid is null || service.ServiceId.Uuid == rfcommServiceUuid;
    }

    private bool ShouldProbeService(RfcommDeviceService service)
    {
        return MatchesServiceFilter(service) && (rfcommServiceUuid is not null || IsCustomService(service));
    }

    private static async Task<StreamSocket> OpenSocketAsync(
        RfcommDeviceService service,
        SocketProtectionLevel protectionLevel)
    {
        var socket = new StreamSocket();
        var opened = false;

        try
        {
            await socket.ConnectAsync(
                    service.ConnectionHostName,
                    service.ConnectionServiceName,
                    protectionLevel)
                .AsTask()
                .WaitAsync(CustomServiceConnectTimeout);
            opened = true;
            return socket;
        }
        finally
        {
            if (!opened)
            {
                socket.Dispose();
            }
        }
    }
}

internal sealed record KnownRfcommService(string Name, Guid Uuid);

internal sealed record ActiveSocketProbePayload(string Name, byte[] Payload);

internal sealed class SocketTraceState
{
    public int ChunksObserved { get; set; }

    public int TotalBytesObserved { get; set; }
}
