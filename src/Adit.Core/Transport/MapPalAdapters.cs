using Microsoft.Internal.Bluetooth.Pal.Contracts;
using Microsoft.Internal.Diagnostics.Context;
using Windows.Devices.Bluetooth;
using Windows.Devices.Bluetooth.Rfcomm;
using Windows.Devices.Enumeration;
using Windows.Networking;
using Windows.Networking.Sockets;
using Windows.Storage.Streams;

namespace Adit.Core.Transport;

internal sealed class MapSocketProvider : ISocketProvider
{
    private static readonly TimeSpan SocketConnectSupervisionTimeout = TimeSpan.FromSeconds(60);

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
            return new MapSocket(socket);
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
            return new MapSocket(socket);
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
    private readonly StreamSocket socket;
    private bool disposed;

    public MapSocket(StreamSocket socket)
    {
        this.socket = socket;
        SocketId = Guid.NewGuid().ToString("N");
        InputStream = socket.InputStream.AsStreamForRead();
        OutputStream = socket.OutputStream.AsStreamForWrite();
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
        // The WinRT stream adapters may flush on dispose after the remote endpoint has already closed.
        // Disposing the underlying socket is enough to tear down the transport without surfacing false 500s.
        DisposeSilently(socket);
    }

    private static void DisposeSilently(IDisposable disposable)
    {
        try
        {
            disposable.Dispose();
        }
        catch (ObjectDisposedException)
        {
        }
        catch (IOException)
        {
        }
        catch (System.Runtime.InteropServices.COMException)
        {
        }
    }
}

internal sealed class MapSocketListenerProvider : ISocketListenerProvider
{
    public async Task<ISocketListener> FromServiceNameAsync(
        string serviceName,
        ITraceContext traceContext,
        CancellationToken cancellationToken = default)
    {
        var listener = new StreamSocketListener();
        await listener.BindServiceNameAsync(
            serviceName,
            SocketProtectionLevel.BluetoothEncryptionWithAuthentication);
        return new MapSocketListener(listener, serviceName);
    }
}

internal sealed class MapSocketListener : ISocketListener, IDisposable
{
    private bool disposed;

    public MapSocketListener(StreamSocketListener socketListener, string serviceName)
    {
        Value = socketListener;
        ServiceName = serviceName;
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
            new SocketListenerConnectionReceivedEventArgs(new MapSocket(args.Socket)));
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
            throw new InvalidOperationException($"RFCOMM service query failed: {result.Error}");
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
