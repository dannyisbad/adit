using Adit.Core.Models;
using Adit.Core.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Internal.Bluetooth.Map;
using Microsoft.Internal.Bluetooth.Map.BMessage;
using Microsoft.Internal.Bluetooth.Map.Request;

namespace Adit.Core.Services;

public sealed class MapMessagingService
{
    private readonly ILoggerFactory loggerFactory;
    private readonly PhoneLinkProcessController processController;

    public MapMessagingService(ILoggerFactory loggerFactory, PhoneLinkProcessController processController)
    {
        this.loggerFactory = loggerFactory;
        this.processController = processController;
    }

    public async Task<IReadOnlyList<string>> ListFoldersAsync(
        BluetoothEndpointRecord target,
        bool evictPhoneLink,
        CancellationToken cancellationToken)
    {
        return await WithClientAsync(
            target,
            evictPhoneLink,
            async (client, traceContext) =>
            {
                await NavigateToMessagesRootAsync(client, traceContext, cancellationToken);
                var result = await client.GetFolderListingAsync(
                    new GetFolderListingRequestParameters
                    {
                        MaxListCount = 50,
                        ListStartOffset = 0
                    },
                    traceContext,
                    cancellationToken);

                return result.Body?.Folder?
                    .Select(folder => folder.Name)
                    .Where(name => !string.IsNullOrWhiteSpace(name))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToArray() ?? [];
            },
            cancellationToken);
    }

    public async Task<MessageFolderListing> ListMessagesAsync(
        BluetoothEndpointRecord target,
        string folderName,
        int limit,
        bool evictPhoneLink,
        CancellationToken cancellationToken)
    {
        return await WithClientAsync(
            target,
            evictPhoneLink,
            async (client, traceContext) =>
            {
                await NavigateToMessagesRootAsync(client, traceContext, cancellationToken);

                var listingResult = await client.GetMessagesListingAsync(
                    MapClientInterop.CreateMessagesListingRequest(folderName, limit),
                    traceContext,
                    cancellationToken);

                var messages = listingResult.Body ?? [];
                var details = new List<MessageRecord>(messages.Count);
                foreach (var message in messages)
                {
                    BMessage? detailMessage = null;
                    if (!string.IsNullOrWhiteSpace(message.Handle))
                    {
                        var detail = await client.GetMessageAsync(
                            MapClientInterop.CreateGetMessageRequest(message.Handle),
                            traceContext,
                            cancellationToken);
                        detailMessage = detail.Body;
                    }

                    details.Add(MapClientInterop.ToMessageRecord(folderName, message, detailMessage));
                }

                var typeCounts = messages
                    .Select(message => message.Type ?? "(unknown)")
                    .GroupBy(type => type, StringComparer.OrdinalIgnoreCase)
                    .ToDictionary(group => group.Key, group => group.Count(), StringComparer.OrdinalIgnoreCase);

                return new MessageFolderListing(
                    folderName,
                    messages.Count,
                    listingResult.MessagesListingSize,
                    listingResult.NewMessage,
                    listingResult.MseTime,
                    typeCounts,
                    details);
            },
            cancellationToken);
    }

    public async Task<MessageSyncSnapshot> PullSnapshotAsync(
        BluetoothEndpointRecord target,
        int limit,
        bool evictPhoneLink,
        CancellationToken cancellationToken)
    {
        return await WithClientAsync(
            target,
            evictPhoneLink,
            async (client, traceContext) =>
            {
                await NavigateToMessagesRootAsync(client, traceContext, cancellationToken);
                var folderListing = await client.GetFolderListingAsync(
                    new GetFolderListingRequestParameters
                    {
                        MaxListCount = 50,
                        ListStartOffset = 0
                    },
                    traceContext,
                    cancellationToken);

                var folders = folderListing.Body?.Folder?
                    .Select(folder => folder.Name)
                    .Where(name => !string.IsNullOrWhiteSpace(name))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .Concat(["inbox", "sent", "outbox", "deleted"])
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToArray() ?? ["inbox", "sent", "outbox", "deleted"];

                var allMessages = new List<MessageRecord>();
                foreach (var folderName in folders)
                {
                    var listingResult = await client.GetMessagesListingAsync(
                        MapClientInterop.CreateMessagesListingRequest(folderName, limit),
                        traceContext,
                        cancellationToken);
                    var messages = listingResult.Body ?? [];

                    foreach (var message in messages)
                    {
                        BMessage? detailMessage = null;
                        if (!string.IsNullOrWhiteSpace(message.Handle))
                        {
                            var detail = await client.GetMessageAsync(
                                MapClientInterop.CreateGetMessageRequest(message.Handle),
                                traceContext,
                                cancellationToken);
                            detailMessage = detail.Body;
                        }

                        allMessages.Add(MapClientInterop.ToMessageRecord(folderName, message, detailMessage));
                    }
                }

                return new MessageSyncSnapshot(folders, allMessages);
            },
            cancellationToken);
    }

    public async Task<SendMessageResult> SendMessageAsync(
        BluetoothEndpointRecord target,
        string recipient,
        string body,
        bool evictPhoneLink,
        CancellationToken cancellationToken)
    {
        return await WithClientAsync(
            target,
            evictPhoneLink,
            async (client, traceContext) =>
            {
                var result = await client.PushMessageAsync(
                    MapClientInterop.CreatePushMessageRequest(recipient, body),
                    traceContext,
                    cancellationToken);

                return new SendMessageResult(
                    MapClientInterop.ReadBoolProperty(result, "IsSuccess"),
                    MapClientInterop.ReadObjectProperty(result, "ResponseCode")?.ToString(),
                    MapClientInterop.ReadObjectProperty(result, "MessageHandle")?.ToString()
                        ?? MapClientInterop.ReadObjectProperty(result, "Handle")?.ToString());
            },
            cancellationToken);
    }

    private async Task<T> WithClientAsync<T>(
        BluetoothEndpointRecord target,
        bool evictPhoneLink,
        Func<MapClient, Microsoft.Internal.Diagnostics.Context.ITraceContext, Task<T>> action,
        CancellationToken cancellationToken)
    {
        if (evictPhoneLink)
        {
            processController.Evict();
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
        }

        var manager = new MapClientManager(
            new MapSocketProvider(),
            new MapRfcommServiceProviderFactory(),
            new MapSocketListenerProvider(),
            new MapBluetoothDeviceProvider(),
            loggerFactory);

        object? openResult = null;
        MapClient? client = null;

        try
        {
            var traceContext = TraceContextFactory.Create();
            openResult = await manager.OpenAsync(target.Id, traceContext, cancellationToken);
            if (!MapClientInterop.ReadBoolProperty(openResult, "IsSuccess"))
            {
                throw new InvalidOperationException("MAP open failed.");
            }

            client = MapClientInterop.GetPropertyValue<MapClient>(openResult, "MapClient")
                ?? MapClientInterop.GetPropertyValue<MapClient>(openResult, "Result");
            if (client is null)
            {
                throw new InvalidOperationException("MAP client was not returned.");
            }

            return await action(client, traceContext);
        }
        finally
        {
            client?.Dispose();
        }
    }

    private static async Task NavigateToMessagesRootAsync(
        MapClient client,
        Microsoft.Internal.Diagnostics.Context.ITraceContext traceContext,
        CancellationToken cancellationToken)
    {
        await MapClientInterop.NavigateToMessagesRootAsync(client, traceContext, cancellationToken);
    }
}
