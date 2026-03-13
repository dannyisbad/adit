using System.Collections.Concurrent;
using System.Threading.Channels;

namespace Adit.Daemon.Services;

public sealed class DeviceFusionCoordinator : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, DeviceQueue> queues = new(StringComparer.OrdinalIgnoreCase);
    private readonly ILogger<DeviceFusionCoordinator> logger;
    private readonly CancellationTokenSource shutdown = new();

    public DeviceFusionCoordinator(ILogger<DeviceFusionCoordinator> logger)
    {
        this.logger = logger;
    }

    public Task RunAsync(
        string deviceId,
        string operationName,
        Func<CancellationToken, Task> work,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(deviceId);
        ArgumentException.ThrowIfNullOrWhiteSpace(operationName);
        ArgumentNullException.ThrowIfNull(work);

        return RunAsync<object?>(
            deviceId,
            operationName,
            async ct =>
            {
                await work(ct);
                return null;
            },
            cancellationToken);
    }

    public Task<T> RunAsync<T>(
        string deviceId,
        string operationName,
        Func<CancellationToken, Task<T>> work,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(deviceId);
        ArgumentException.ThrowIfNullOrWhiteSpace(operationName);
        ArgumentNullException.ThrowIfNull(work);

        cancellationToken.ThrowIfCancellationRequested();

        var queue = queues.GetOrAdd(deviceId, static (key, state) => new DeviceQueue(key, state.logger, state.shutdown.Token), (logger, shutdown));
        return queue.EnqueueAsync(operationName, work, cancellationToken);
    }

    public void Post(string deviceId, string operationName, Func<CancellationToken, Task> work)
    {
        _ = RunAsync(deviceId, operationName, work, CancellationToken.None)
            .ContinueWith(
                task =>
                {
                    if (task.Exception is null)
                    {
                        return;
                    }

                    logger.LogDebug(
                        task.Exception.GetBaseException(),
                        "Queued fusion operation failed for {DeviceId}: {OperationName}",
                        deviceId,
                        operationName);
                },
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted,
                TaskScheduler.Default);
    }

    public async ValueTask DisposeAsync()
    {
        shutdown.Cancel();

        foreach (var queue in queues.Values)
        {
            queue.Complete();
        }

        var processors = queues.Values.Select(queue => queue.Completion).ToArray();
        if (processors.Length > 0)
        {
            await Task.WhenAll(processors);
        }

        shutdown.Dispose();
    }

    private sealed class DeviceQueue
    {
        private readonly string deviceId;
        private readonly ILogger logger;
        private readonly CancellationToken shutdownToken;
        private readonly Channel<QueuedOperation> operations = Channel.CreateUnbounded<QueuedOperation>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });

        public DeviceQueue(string deviceId, ILogger logger, CancellationToken shutdownToken)
        {
            this.deviceId = deviceId;
            this.logger = logger;
            this.shutdownToken = shutdownToken;
            Completion = Task.Run(ProcessAsync, CancellationToken.None);
        }

        public Task Completion { get; }

        public Task<T> EnqueueAsync<T>(
            string operationName,
            Func<CancellationToken, Task<T>> work,
            CancellationToken cancellationToken)
        {
            var completionSource = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
            var operation = new QueuedOperation(
                operationName,
                async ct => await work(ct),
                completionSource);

            if (!operations.Writer.TryWrite(operation))
            {
                throw new InvalidOperationException($"Could not queue fusion operation '{operationName}' for '{deviceId}'.");
            }

            return AwaitResultAsync<T>(completionSource.Task, cancellationToken);
        }

        public void Complete()
        {
            operations.Writer.TryComplete();
        }

        private async Task ProcessAsync()
        {
            try
            {
                await foreach (var operation in operations.Reader.ReadAllAsync(shutdownToken))
                {
                    try
                    {
                        var result = await operation.Work(shutdownToken);
                        operation.CompletionSource.TrySetResult(result);
                    }
                    catch (OperationCanceledException exception) when (shutdownToken.IsCancellationRequested)
                    {
                        operation.CompletionSource.TrySetException(exception);
                        break;
                    }
                    catch (Exception exception)
                    {
                        logger.LogDebug(
                            exception,
                            "Fusion operation failed for {DeviceId}: {OperationName}",
                            deviceId,
                            operation.OperationName);
                        operation.CompletionSource.TrySetException(exception);
                    }
                }
            }
            catch (OperationCanceledException) when (shutdownToken.IsCancellationRequested)
            {
            }
            finally
            {
                while (operations.Reader.TryRead(out var operation))
                {
                    operation.CompletionSource.TrySetCanceled(shutdownToken);
                }
            }
        }

        private static async Task<T> AwaitResultAsync<T>(Task<object?> resultTask, CancellationToken cancellationToken)
        {
            if (!cancellationToken.CanBeCanceled)
            {
                return CastResult<T>(await resultTask);
            }

            var cancellationTask = Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            var completed = await Task.WhenAny(resultTask, cancellationTask);
            if (!ReferenceEquals(completed, resultTask))
            {
                throw new OperationCanceledException(cancellationToken);
            }

            return CastResult<T>(await resultTask);
        }

        private static T CastResult<T>(object? value)
        {
            if (value is null)
            {
                return default!;
            }

            return (T)value;
        }
    }

    private sealed record QueuedOperation(
        string OperationName,
        Func<CancellationToken, Task<object?>> Work,
        TaskCompletionSource<object?> CompletionSource);
}
