using System.Collections.Concurrent;
using System.Threading.Channels;

namespace Adit.Daemon.Services;

public sealed class DaemonEventHub
{
    private readonly ConcurrentDictionary<Guid, Channel<DaemonEventRecord>> subscribers = new();
    private readonly ConcurrentQueue<DaemonEventRecord> recentEvents = new();
    private readonly int recentEventLimit;
    private long nextSequence;

    public DaemonEventHub(DaemonOptions options)
    {
        recentEventLimit = Math.Max(10, options.EventBufferSize);
    }

    public DaemonEventRecord Publish(string type, object? payload)
    {
        var record = new DaemonEventRecord(
            Interlocked.Increment(ref nextSequence),
            DateTimeOffset.UtcNow,
            type,
            payload);

        recentEvents.Enqueue(record);
        while (recentEvents.Count > recentEventLimit && recentEvents.TryDequeue(out _))
        {
        }

        foreach (var channel in subscribers.Values)
        {
            channel.Writer.TryWrite(record);
        }

        return record;
    }

    public IReadOnlyList<DaemonEventRecord> GetRecent(int limit = 50)
    {
        return recentEvents
            .Reverse()
            .Take(Math.Max(1, limit))
            .Reverse()
            .ToArray();
    }

    public DaemonEventSubscription Subscribe(CancellationToken cancellationToken)
    {
        var id = Guid.NewGuid();
        var channel = Channel.CreateUnbounded<DaemonEventRecord>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });
        subscribers[id] = channel;

        cancellationToken.Register(
            state =>
            {
                var hub = (DaemonEventHub)state!;
                hub.Unsubscribe(id);
            },
            this);

        return new DaemonEventSubscription(id, channel.Reader, this);
    }

    private void Unsubscribe(Guid id)
    {
        if (subscribers.TryRemove(id, out var channel))
        {
            channel.Writer.TryComplete();
        }
    }

    public sealed class DaemonEventSubscription : IAsyncDisposable
    {
        private readonly DaemonEventHub hub;
        private readonly Guid id;
        private int disposed;

        internal DaemonEventSubscription(Guid id, ChannelReader<DaemonEventRecord> reader, DaemonEventHub hub)
        {
            this.id = id;
            this.hub = hub;
            Reader = reader;
        }

        public ChannelReader<DaemonEventRecord> Reader { get; }

        public ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref disposed, 1) == 0)
            {
                hub.Unsubscribe(id);
            }

            return ValueTask.CompletedTask;
        }
    }
}
