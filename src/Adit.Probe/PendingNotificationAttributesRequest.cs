namespace Adit.Probe;

internal sealed class PendingNotificationAttributesRequest
{
    private static readonly TimeSpan DefaultSettleDelay = TimeSpan.FromMilliseconds(350);

    private readonly object gate = new();
    private readonly List<byte> buffer = new();
    private readonly Dictionary<AncsNotificationAttributeId, string> mergedAttributes = [];
    private readonly TaskCompletionSource<AncsNotificationAttributesResponse> completionSource =
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

    public Task<AncsNotificationAttributesResponse> Completion => completionSource.Task;

    public int BufferedBytes => buffer.Count;

    public void Append(ReadOnlySpan<byte> fragment)
    {
        lock (gate)
        {
            buffer.AddRange(fragment.ToArray());
            if (completionSource.Task.IsCompleted)
            {
                return;
            }

            if (!AncsProtocol.TryParseNotificationAttributesResponse(
                    buffer.ToArray(),
                    RequestedAttributes,
                    out var response))
            {
                return;
            }

            if (response!.NotificationUid != NotificationUid)
            {
                CleanupSettleDelay_NoLock();
                completionSource.TrySetException(
                    new InvalidDataException(
                        $"ANCS response uid {response.NotificationUid} did not match pending uid {NotificationUid}."));
                return;
            }

            foreach (var attribute in response.Attributes)
            {
                mergedAttributes[attribute.Key] = attribute.Value;
            }

            buffer.Clear();
            if (HasCompleteAttributeSet_NoLock())
            {
                Complete_NoLock();
                return;
            }

            ArmSettleDelay_NoLock();
        }
    }

    public void Fail(Exception exception)
    {
        lock (gate)
        {
            CleanupSettleDelay_NoLock();
            completionSource.TrySetException(exception);
        }
    }

    public void Cancel()
    {
        lock (gate)
        {
            CleanupSettleDelay_NoLock();
            completionSource.TrySetCanceled();
        }
    }

    private bool HasCompleteAttributeSet_NoLock()
    {
        return RequestedAttributes
            .Select(requestedAttribute => requestedAttribute.AttributeId)
            .Distinct()
            .All(mergedAttributes.ContainsKey);
    }

    private void ArmSettleDelay_NoLock()
    {
        CleanupSettleDelay_NoLock();
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

                    Complete_NoLock();
                }
            },
            CancellationToken.None);
    }

    private void Complete_NoLock()
    {
        CleanupSettleDelay_NoLock();
        completionSource.TrySetResult(
            new AncsNotificationAttributesResponse(
                NotificationUid,
                new Dictionary<AncsNotificationAttributeId, string>(mergedAttributes)));
    }

    private void CleanupSettleDelay_NoLock()
    {
        settleDelaySource?.Cancel();
        settleDelaySource?.Dispose();
        settleDelaySource = null;
    }
}
