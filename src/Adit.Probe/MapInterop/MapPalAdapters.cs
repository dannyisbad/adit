using Microsoft.Extensions.Logging;
using Microsoft.Internal.Bluetooth.Pal.Contracts;
using Microsoft.Internal.Diagnostics.Context;
using Windows.Devices.Bluetooth;
using Windows.Devices.Bluetooth.Rfcomm;
using Windows.Devices.Enumeration;
using Windows.Networking;
using Windows.Networking.Sockets;
using Windows.Storage.Streams;

namespace Adit.Probe.MapInterop;

internal sealed class MapSocketProvider : ISocketProvider
{
    private static readonly TimeSpan SocketConnectSupervisionTimeout = TimeSpan.FromSeconds(60);
    private readonly ILoggerFactory loggerFactory;
    private readonly ProbeLogger? probeLogger;

    public MapSocketProvider(ILoggerFactory loggerFactory, ProbeLogger? probeLogger = null)
    {
        this.loggerFactory = loggerFactory;
        this.probeLogger = probeLogger;
    }

    public async Task<ISocket> OpenAsync(
        string hostName,
        string serviceName,
        ITraceContext traceContext,
        CancellationToken cancellationToken = default)
    {
        var socket = new StreamSocket();
        var opened = false;

        try
        {
            await socket.ConnectAsync(
                    new HostName(hostName),
                    serviceName,
                    SocketProtectionLevel.BluetoothEncryptionWithAuthentication)
                .AsTask(cancellationToken)
                .WaitAsync(SocketConnectSupervisionTimeout, cancellationToken);
            opened = true;
            return new MapSocket(socket, loggerFactory, probeLogger);
        }
        finally
        {
            if (!opened)
            {
                socket.Dispose();
            }
        }
    }

    public async Task<ISocket> OpenAsync(
        string hostName,
        uint port,
        ITraceContext traceContext,
        CancellationToken cancellationToken = default)
    {
        var socket = new StreamSocket();
        var opened = false;

        try
        {
            await socket.ConnectAsync(
                    new HostName(hostName),
                    port.ToString(System.Globalization.CultureInfo.InvariantCulture),
                    SocketProtectionLevel.BluetoothEncryptionWithAuthentication)
                .AsTask(cancellationToken)
                .WaitAsync(SocketConnectSupervisionTimeout, cancellationToken);
            opened = true;
            return new MapSocket(socket, loggerFactory, probeLogger);
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

internal sealed class MapSocket : ISocket, IDisposable
{
    private readonly ILogger<MapSocket> logger;
    private readonly ProbeLogger? probeLogger;
    private readonly StreamSocket socket;
    private bool disposed;

    public MapSocket(StreamSocket socket, ILoggerFactory loggerFactory, ProbeLogger? probeLogger = null)
    {
        this.socket = socket;
        this.probeLogger = probeLogger;
        logger = loggerFactory.CreateLogger<MapSocket>();
        SocketId = Guid.NewGuid().ToString();
        var inspector = new ObexTrafficInspector(SocketId, probeLogger);
        InputStream = new LoggingStream(
            socket.InputStream.AsStreamForRead(),
            probeLogger,
            SocketId,
            "read",
            inspector);
        OutputStream = new LoggingStream(
            socket.OutputStream.AsStreamForWrite(),
            probeLogger,
            SocketId,
            "write",
            inspector);

        probeLogger?.Log(
            "bt.socket_opened",
            new
            {
                socketId = SocketId,
                remoteAddress = socket.Information.RemoteAddress.RawName
            });
    }

    public string SocketId { get; }

    public string RemoteAddress => socket.Information.RemoteAddress.RawName;

    public Stream InputStream { get; }

    public Stream OutputStream { get; }

    public void Dispose()
    {
        if (disposed)
        {
            return;
        }

        disposed = true;

        try
        {
            OutputStream.Dispose();
        }
        catch (Exception exception) when (IsKnownDisposeException(exception))
        {
        }

        try
        {
            InputStream.Dispose();
        }
        catch (Exception exception) when (IsKnownDisposeException(exception))
        {
        }

        socket.Dispose();
        logger.LogDebug("Closed MAP socket {SocketId}", SocketId);
        probeLogger?.Log("bt.socket_closed", new { socketId = SocketId });
    }

    private static bool IsKnownDisposeException(Exception exception)
    {
        return exception is IOException
            or ObjectDisposedException
            || SocketError.GetStatus(exception.HResult) != SocketErrorStatus.Unknown;
    }
}

internal sealed class MapSocketListenerProvider : ISocketListenerProvider
{
    private readonly ILoggerFactory loggerFactory;
    private readonly ProbeLogger? probeLogger;

    public MapSocketListenerProvider(ILoggerFactory loggerFactory, ProbeLogger? probeLogger = null)
    {
        this.loggerFactory = loggerFactory;
        this.probeLogger = probeLogger;
    }

    public async Task<ISocketListener> FromServiceNameAsync(
        string serviceName,
        ITraceContext traceContext,
        CancellationToken cancellationToken = default)
    {
        var listener = new StreamSocketListener();
        await listener.BindServiceNameAsync(
            serviceName,
            SocketProtectionLevel.BluetoothEncryptionWithAuthentication);
        return new MapSocketListener(listener, serviceName, loggerFactory, probeLogger);
    }
}

internal sealed class MapSocketListener : ISocketListener, IDisposable
{
    private readonly ILoggerFactory loggerFactory;
    private readonly ProbeLogger? probeLogger;
    private bool disposed;

    public MapSocketListener(
        StreamSocketListener socketListener,
        string serviceName,
        ILoggerFactory loggerFactory,
        ProbeLogger? probeLogger = null)
    {
        Value = socketListener;
        ServiceName = serviceName;
        this.loggerFactory = loggerFactory;
        this.probeLogger = probeLogger;
        Value.ConnectionReceived += OnConnectionReceived;
    }

    internal StreamSocketListener? Value { get; private set; }

    public string ServiceName { get; }

    public event EventHandler<SocketListenerConnectionReceivedEventArgs>? ConnectionReceived;

    public void Dispose()
    {
        if (disposed)
        {
            return;
        }

        disposed = true;

        if (Value is not null)
        {
            Value.ConnectionReceived -= OnConnectionReceived;
            Value.Dispose();
            Value = null;
        }
    }

    private void OnConnectionReceived(
        StreamSocketListener sender,
        StreamSocketListenerConnectionReceivedEventArgs args)
    {
        ConnectionReceived?.Invoke(
            sender,
            new SocketListenerConnectionReceivedEventArgs(
                new MapSocket(args.Socket, loggerFactory, probeLogger)));
    }
}

internal sealed class MapRfcommServiceProviderFactory : IRfcommServiceProviderFactory
{
    public async Task<IRfcommServiceProvider> CreateAsync(Guid uuid)
    {
        return new MapRfcommServiceProvider(
            await Windows.Devices.Bluetooth.Rfcomm.RfcommServiceProvider.CreateAsync(
                RfcommServiceId.FromUuid(uuid)));
    }

    public async Task<IRfcommServiceProvider> CreateAsync(ushort shortId)
    {
        return new MapRfcommServiceProvider(
            await Windows.Devices.Bluetooth.Rfcomm.RfcommServiceProvider.CreateAsync(
                RfcommServiceId.FromShortId(shortId)));
    }
}

internal sealed class MapRfcommServiceProvider : IRfcommServiceProvider
{
    private readonly Windows.Devices.Bluetooth.Rfcomm.RfcommServiceProvider provider;

    public MapRfcommServiceProvider(Windows.Devices.Bluetooth.Rfcomm.RfcommServiceProvider provider)
    {
        this.provider = provider;
        SdpRawAttributes = provider.SdpRawAttributes
            .ToDictionary(pair => pair.Key, pair => BufferToBytes(pair.Value));
    }

    public IDictionary<uint, byte[]> SdpRawAttributes { get; }

    public Guid ServiceId => provider.ServiceId.Uuid;

    public void StartAdvertising(ISocketListener socketListener, bool radioDiscoverable = false)
    {
        provider.SdpRawAttributes.Clear();

        foreach (var attribute in SdpRawAttributes)
        {
            var writer = new DataWriter();
            writer.WriteBytes(attribute.Value);
            provider.SdpRawAttributes.Add(attribute.Key, writer.DetachBuffer());
        }

        provider.StartAdvertising(((MapSocketListener)socketListener).Value!, radioDiscoverable);
    }

    public void StopAdvertising()
    {
        provider.StopAdvertising();
    }

    private static byte[] BufferToBytes(IBuffer buffer)
    {
        var bytes = new byte[buffer.Length];
        DataReader.FromBuffer(buffer).ReadBytes(bytes);
        return bytes;
    }
}

internal sealed class MapBluetoothDeviceProvider : IBluetoothDeviceProvider
{
    public async Task<IBluetoothDevice> FromIdAsync(
        string id,
        ITraceContext traceContext,
        CancellationToken cancellationToken = default)
    {
        var bluetoothDevice = await BluetoothDevice.FromIdAsync(id);
        return new MapBluetoothDevice(bluetoothDevice!);
    }
}

internal sealed class MapBluetoothDevice : IBluetoothDevice
{
    private readonly BluetoothDevice bluetoothDevice;

    public MapBluetoothDevice(BluetoothDevice bluetoothDevice)
    {
        this.bluetoothDevice = bluetoothDevice;
    }

    public string Id => bluetoothDevice.DeviceId;

    public async Task<List<IRfcommService>> GetRfcommServicesForIdAsync(
        Guid serviceId,
        ITraceContext traceContext,
        CancellationToken cancellationToken = default)
    {
        var result = await bluetoothDevice.GetRfcommServicesForIdAsync(
                RfcommServiceId.FromUuid(serviceId),
                BluetoothCacheMode.Uncached)
            .AsTask(cancellationToken);

        if (result.Error != BluetoothError.Success)
        {
            throw new InvalidOperationException(
                $"RFCOMM service query failed: {result.Error}");
        }

        return result.Services
            .Select(service => (IRfcommService)new MapRfcommService(service))
            .ToList();
    }
}

internal sealed class MapRfcommService : IRfcommService, IDisposable
{
    private readonly RfcommDeviceService service;

    public MapRfcommService(RfcommDeviceService service)
    {
        this.service = service;
        Device = new MapBluetoothDevice(service.Device);
    }

    public string HostName => service.ConnectionHostName.RawName;

    public string ServiceName => service.ConnectionServiceName;

    public IBluetoothDevice Device { get; }

    public string ServiceId => service.ServiceId.Uuid.ToString();

    public void Dispose()
    {
        service.Dispose();
    }

    public async Task<IDictionary<uint, byte[]>> GetSdpRawAttributesAsync(
        ITraceContext traceContext,
        CancellationToken cancellationToken = default)
    {
        var result = await service.GetSdpRawAttributesAsync();
        return result.ToDictionary(pair => pair.Key, pair => BufferToBytes(pair.Value));
    }

    public async Task<bool> RequestAccessAsync(
        ITraceContext traceContext,
        CancellationToken cancellationToken = default)
    {
        return await service.RequestAccessAsync() == DeviceAccessStatus.Allowed;
    }

    private static byte[] BufferToBytes(IBuffer buffer)
    {
        var bytes = new byte[buffer.Length];
        DataReader.FromBuffer(buffer).ReadBytes(bytes);
        return bytes;
    }
}

internal sealed class LoggingStream : Stream
{
    private readonly string direction;
    private readonly ObexTrafficInspector? inspector;
    private readonly ProbeLogger? probeLogger;
    private readonly string socketId;
    private readonly Stream inner;

    public LoggingStream(
        Stream inner,
        ProbeLogger? probeLogger,
        string socketId,
        string direction,
        ObexTrafficInspector? inspector = null)
    {
        this.inner = inner;
        this.probeLogger = probeLogger;
        this.socketId = socketId;
        this.direction = direction;
        this.inspector = inspector;
    }

    public override bool CanRead => inner.CanRead;

    public override bool CanSeek => inner.CanSeek;

    public override bool CanWrite => inner.CanWrite;

    public override long Length => inner.Length;

    public override long Position
    {
        get => inner.Position;
        set => inner.Position = value;
    }

    public override void Flush()
    {
        inner.Flush();
    }

    public override Task FlushAsync(CancellationToken cancellationToken)
    {
        return inner.FlushAsync(cancellationToken);
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        var bytesRead = inner.Read(buffer, offset, count);
        LogBytes(buffer.AsSpan(offset, bytesRead));
        return bytesRead;
    }

    public override int Read(Span<byte> buffer)
    {
        var bytesRead = inner.Read(buffer);
        LogBytes(buffer[..bytesRead]);
        return bytesRead;
    }

    public override async ValueTask<int> ReadAsync(
        Memory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        var bytesRead = await inner.ReadAsync(buffer, cancellationToken);
        LogBytes(buffer.Span[..bytesRead]);
        return bytesRead;
    }

    public override async Task<int> ReadAsync(
        byte[] buffer,
        int offset,
        int count,
        CancellationToken cancellationToken)
    {
        var bytesRead = await inner.ReadAsync(buffer, offset, count, cancellationToken);
        LogBytes(buffer.AsSpan(offset, bytesRead));
        return bytesRead;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        return inner.Seek(offset, origin);
    }

    public override void SetLength(long value)
    {
        inner.SetLength(value);
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        LogBytes(buffer.AsSpan(offset, count));
        inner.Write(buffer, offset, count);
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        LogBytes(buffer);
        inner.Write(buffer);
    }

    public override async ValueTask WriteAsync(
        ReadOnlyMemory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        LogBytes(buffer.Span);
        await inner.WriteAsync(buffer, cancellationToken);
    }

    public override async Task WriteAsync(
        byte[] buffer,
        int offset,
        int count,
        CancellationToken cancellationToken)
    {
        LogBytes(buffer.AsSpan(offset, count));
        await inner.WriteAsync(buffer, offset, count, cancellationToken);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            inner.Dispose();
        }

        base.Dispose(disposing);
    }

    private void LogBytes(ReadOnlySpan<byte> bytes)
    {
        if (probeLogger is null || bytes.Length == 0)
        {
            return;
        }

        var payload = bytes.ToArray();
        probeLogger.Log(
            "bt.socket_bytes",
            new
            {
                socketId,
                direction,
                length = payload.Length,
                payloadHex = Convert.ToHexString(payload),
                payloadUtf8 = TryDecodeUtf8(payload)
            });

        inspector?.Record(direction, payload);
    }

    private static string? TryDecodeUtf8(byte[] bytes)
    {
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
