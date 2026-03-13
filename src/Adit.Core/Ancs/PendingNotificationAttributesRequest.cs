using System.Runtime.InteropServices;

namespace Adit.Core.Ancs;

internal sealed class PendingNotificationAttributesRequest
{
    private static readonly TimeSpan DefaultSettleDelay = TimeSpan.FromMilliseconds(350);

    private readonly object gate = new();
    private readonly List<byte> buffer = new();
    private readonly Dictionary<AncsNotificationAttributeId, string> mergedAttributes = [];
    private readonly TaskCompletionSource<ParsedNotificationAttributesResponse> completionSource =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TimeSpan settleDelay;
    private CancellationTokenSource? settleDelaySource;

    public PendingNotificationAttributesRequest(
        uint notificationUid,
        IReadOnlyList<RequestedNotificationAttribute> requestedAttributes,
        TimeSpan? settleDelay = null)
    {
        NotificationUid = notificationUid;
        RequestedAttributes = requestedAttributes;
        this.settleDelay = settleDelay ?? DefaultSettleDelay;
    }

    public uint NotificationUid { get; }

    public IReadOnlyList<RequestedNotificationAttribute> RequestedAttributes { get; }

    public Task<ParsedNotificationAttributesResponse> Completion => completionSource.Task;

    public int BufferedBytes => buffer.Count;

    public void Append(ReadOnlySpan<byte> fragment)
    {
        lock (gate)
        {
            buffer.AddRange(fragment.ToArray());
            if (completionSource.Task.IsCompleted || buffer.Count == 0)
            {
                return;
            }

            while (buffer.Count > 0)
            {
                if (!AncsProtocol.TryParseNotificationAttributesResponse(
                        CollectionsMarshal.AsSpan(buffer),
                        RequestedAttributes,
                        out var response,
                        out var consumedBytes,
                        out var hasCompleteAttributeSet))
                {
                    return;
                }

                if (consumedBytes <= 0 || consumedBytes > buffer.Count)
                {
                    return;
                }

                buffer.RemoveRange(0, consumedBytes);
                if (response!.NotificationUid != NotificationUid)
                {
                    continue;
                }

                foreach (var attribute in response.Attributes)
                {
                    mergedAttributes[attribute.Key] = attribute.Value;
                }

                if (hasCompleteAttributeSet || HasCompleteAttributeSetNoLock())
                {
                    CompleteNoLock();
                    return;
                }
            }

            if (mergedAttributes.Count > 0)
            {
                ArmSettleDelayNoLock();
            }
        }
    }

    public void Fail(Exception exception)
    {
        lock (gate)
        {
            CleanupSettleDelayNoLock();
            completionSource.TrySetException(exception);
        }
    }

    public void Cancel()
    {
        lock (gate)
        {
            CleanupSettleDelayNoLock();
            completionSource.TrySetCanceled();
        }
    }

    private bool HasCompleteAttributeSetNoLock()
    {
        return RequestedAttributes
            .Select(requestedAttribute => requestedAttribute.AttributeId)
            .Distinct()
            .All(mergedAttributes.ContainsKey);
    }

    private void ArmSettleDelayNoLock()
    {
        CleanupSettleDelayNoLock();
        settleDelaySource = new CancellationTokenSource();
        var token = settleDelaySource.Token;

        _ = Task.Run(
            async () =>
            {
                try
                {
                    await Task.Delay(settleDelay, token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                lock (gate)
                {
                    if (completionSource.Task.IsCompleted || buffer.Count > 0)
                    {
                        return;
                    }

                    CompleteNoLock();
                }
            },
            CancellationToken.None);
    }

    private void CompleteNoLock()
    {
        CleanupSettleDelayNoLock();
        completionSource.TrySetResult(
            new ParsedNotificationAttributesResponse(
                NotificationUid,
                new Dictionary<AncsNotificationAttributeId, string>(mergedAttributes)));
    }

    private void CleanupSettleDelayNoLock()
    {
        settleDelaySource?.Cancel();
        settleDelaySource?.Dispose();
        settleDelaySource = null;
    }
}
