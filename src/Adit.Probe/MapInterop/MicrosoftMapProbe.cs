using System.Collections;
using System.Collections.Concurrent;
using System.Globalization;
using System.Reflection;
using Adit.Probe.MapInterop;
using Microsoft.Extensions.Logging;
using Microsoft.Internal.Bluetooth.Map;
using Microsoft.Internal.Bluetooth.Map.BMessage;
using Microsoft.Internal.Bluetooth.Map.Model;
using Microsoft.Internal.Bluetooth.Map.Request;
using Microsoft.Internal.Diagnostics.Context;
using MixERP.Net.VCards;
using MixERP.Net.VCards.Models;
using MixERP.Net.VCards.Types;
using Windows.Devices.Bluetooth;
using Windows.Devices.Bluetooth.Rfcomm;
using Windows.Storage.Streams;

namespace Adit.Probe;

internal sealed class MicrosoftMapProbe
{
    private const int DefaultBodyFetchLimit = 3;
    private const int DefaultFolderLimit = 50;
    private const BindingFlags AllInstanceProperties = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
    private static readonly Guid MapMasServiceUuid = new("00001132-0000-1000-8000-00805F9B34FB");
    private static readonly string[] MasSupportedFeatureNames =
    [
        "NotificationRegistration",
        "Notification",
        "Browsing",
        "Uploading",
        "Delete",
        "InstanceInformation",
        "ExtendedEventReport11",
        "EventReportVersion12",
        "MessageFormatVersion11",
        "MessagesListingFormatVersion11",
        "PersistentMessageHandles",
        "DatabaseIdentifier",
        "FolderVersionCounter",
        "ConversationVersionCounter",
        "ParticipantPresenceChangeNotification",
        "ParticipantChatStateChangeNotification",
        "PbapContactCrossReference",
        "NotificationFiltering",
        "UtcOffsetTimestampFormat",
        "MapSupportedFeaturesInConnectRequest",
        "ConversationListing",
        "OwnerStatus",
        "MessageForwarding"
    ];

    private static readonly string[] FallbackMessageFolders =
    [
        "inbox",
        "outbox",
        "sent",
        "draft",
        "deleted"
    ];

    private readonly ProbeLogger logger;
    private readonly ProbeOptions options;
    private readonly BluetoothEndpointRecord target;
    private readonly SemaphoreSlim mnsFollowUpLock = new(1, 1);
    private readonly ConcurrentBag<Task> pendingMnsFollowUps = [];
    private MapClient? liveClient;

    public MicrosoftMapProbe(
        BluetoothEndpointRecord target,
        ProbeOptions options,
        ProbeLogger logger)
    {
        this.target = target;
        this.options = options;
        this.logger = logger;
    }

    public async Task<int> RunAsync(CancellationToken cancellationToken)
    {
        if (options.EvictPhoneLink)
        {
            PhoneLinkEviction.Evict(logger);
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
        }

        using var loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Debug));
        var manager = new MapClientManager(
            new MapSocketProvider(loggerFactory, logger),
            new MapRfcommServiceProviderFactory(),
            new MapSocketListenerProvider(loggerFactory, logger),
            new MapBluetoothDeviceProvider(),
            loggerFactory);

        object? openResult = null;
        MapClient? mapClient = null;

        try
        {
            await LogMasSdpAsync(cancellationToken);

            openResult = await manager.OpenAsync(target.Id, CreateTraceContext(), cancellationToken);
            logger.Log("map.open_result", SnapshotObject(openResult) ?? new { });

            if (!ReadBoolProperty(openResult, "IsSuccess"))
            {
                return 1;
            }

            mapClient = GetPropertyValue<MapClient>(openResult, "MapClient")
                ?? GetPropertyValue<MapClient>(openResult, "Result");
            if (mapClient is null)
            {
                logger.Log("map.client_missing", new { target.Id, target.Name });
                return 1;
            }

            liveClient = mapClient;

            mapClient.Closed += OnClientClosed;
            mapClient.MnsConnected += OnMnsConnected;
            mapClient.MnsClosed += OnMnsClosed;
            mapClient.MnsSendEventReceived += OnMnsSendEventReceived;

            var registerResult = await mapClient.SetNotificationRegistrationAsync(
                new SetNotificationRegistrationRequestParameters
                {
                    EnableNotifications = true
                },
                CreateTraceContext(),
                cancellationToken);
            logger.Log("map.notification_registration", SnapshotObject(registerResult) ?? new { });

            await SetFolderAsync(mapClient, string.Empty, "Root", "map.set_folder_root", cancellationToken);
            await LogFolderListingAsync(mapClient, "root", cancellationToken);

            await SetFolderAsync(mapClient, "telecom", "Down", "map.set_folder_telecom", cancellationToken);
            await LogFolderListingAsync(mapClient, "telecom", cancellationToken);

            await SetFolderAsync(mapClient, "msg", "Down", "map.set_folder_msg", cancellationToken);
            var messageFolders = await LogFolderListingAsync(mapClient, "telecom/msg", cancellationToken);

            var updateInboxResult = await mapClient.UpdateInboxAsync(CreateTraceContext(), cancellationToken);
            logger.Log("map.update_inbox", SnapshotObject(updateInboxResult) ?? new { });

            foreach (var folderName in GetOrderedMessageFolders(messageFolders))
            {
                await LogFolderAsync(mapClient, folderName, cancellationToken);
            }

            if (!string.IsNullOrWhiteSpace(options.Recipient)
                && !string.IsNullOrWhiteSpace(options.MessageBody))
            {
                var pushResult = await mapClient.PushMessageAsync(
                    CreatePushMessageRequest(options.Recipient, options.MessageBody),
                    CreateTraceContext(),
                    cancellationToken);
                logger.Log("map.push_message", SnapshotObject(pushResult) ?? new { });
            }

            if (options.MapWatchSeconds > 0)
            {
                logger.Log(
                    "map.watch_started",
                    new
                    {
                        seconds = options.MapWatchSeconds,
                        reason = "live_mns_events"
                    });

                await Task.Delay(TimeSpan.FromSeconds(options.MapWatchSeconds), cancellationToken);

                logger.Log("map.watch_completed", new { seconds = options.MapWatchSeconds });
            }

            await FlushPendingMnsFollowUpsAsync();

            return 0;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            logger.Log("map.cancelled", new { target.Id, target.Name });
            return 0;
        }
        catch (Exception exception)
        {
            logger.Log(
                "map.unhandled_exception",
                new
                {
                    error = exception.ToString(),
                    openResult = SnapshotObject(openResult)
                });
            return 1;
        }
        finally
        {
            if (mapClient is not null)
            {
                liveClient = null;
                mapClient.Closed -= OnClientClosed;
                mapClient.MnsConnected -= OnMnsConnected;
                mapClient.MnsClosed -= OnMnsClosed;
                mapClient.MnsSendEventReceived -= OnMnsSendEventReceived;
                mapClient.Dispose();
            }
        }
    }

    private async Task SetFolderAsync(
        MapClient client,
        string folderName,
        string flagName,
        string logKind,
        CancellationToken cancellationToken)
    {
        var result = await client.SetFolderAsync(
            CreateSetFolderRequest(folderName, flagName),
            CreateTraceContext(),
            cancellationToken);
        logger.Log(logKind, SnapshotObject(result) ?? new { });
    }

    private async Task LogMasSdpAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var device = await BluetoothDevice.FromIdAsync(target.Id);
            if (device is null)
            {
                logger.Log("map.mas_sdp_device_open_failed", new { target.Id, target.Name });
                return;
            }

            var result = await device.GetRfcommServicesForIdAsync(
                RfcommServiceId.FromUuid(MapMasServiceUuid),
                BluetoothCacheMode.Uncached);
            logger.Log(
                "map.mas_sdp_service_query",
                new
                {
                    error = result.Error.ToString(),
                    serviceCount = result.Services.Count
                });

            foreach (var service in result.Services)
            {
                using (service)
                {
                    var rawAttributes = await service.GetSdpRawAttributesAsync();
                    var attributes = rawAttributes
                        .ToDictionary(pair => pair.Key, pair => BufferToBytes(pair.Value));

                    logger.Log(
                        "map.mas_sdp_raw",
                        new
                        {
                            serviceId = service.ServiceId.Uuid,
                            serviceName = service.ConnectionServiceName,
                            hostName = service.ConnectionHostName?.RawName,
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

                    logger.Log("map.mas_sdp_decoded", MapSdpInsights.DecodeMasSdpRecord(attributes));
                    logger.Log("map.mas_sdp_parsed", ParseMasSdpRecord(attributes));
                }
            }
        }
        catch (Exception exception)
        {
            logger.Log("map.mas_sdp_failed", new { error = exception.ToString() });
        }
    }

    private async Task<string[]> LogFolderListingAsync(
        MapClient client,
        string folderPath,
        CancellationToken cancellationToken)
    {
        var result = await client.GetFolderListingAsync(
            new GetFolderListingRequestParameters
            {
                MaxListCount = DefaultFolderLimit,
                ListStartOffset = 0
            },
            CreateTraceContext(),
            cancellationToken);

        logger.Log($"map.{SanitizeKey(folderPath)}.folder_listing", SnapshotObject(result) ?? new { });

        var folders = result.Body?.Folder ?? [];
        var files = result.Body?.File ?? [];

        logger.Log(
            $"map.{SanitizeKey(folderPath)}.folder_listing_entries",
            new
            {
                folderCount = folders.Length,
                folders = folders
                    .Select(folder => new
                    {
                        folder.Name,
                        folder.Size,
                        folder.Type,
                        folder.Modified,
                        folder.UserPermission
                    })
                    .ToArray(),
                fileCount = files.Length,
                files = files
                    .Select(file => new
                    {
                        file.Name,
                        file.Size,
                        file.Type,
                        file.Modified,
                        file.UserPemission
                    })
                    .ToArray()
            });

        return folders
            .Select(folder => folder.Name)
            .Where(name => !string.IsNullOrWhiteSpace(name))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray()!;
    }

    private async Task LogFolderAsync(
        MapClient client,
        string folderName,
        CancellationToken cancellationToken)
    {
        var listingResult = await client.GetMessagesListingAsync(
            CreateMessagesListingRequest(folderName),
            CreateTraceContext(),
            cancellationToken);
        logger.Log(
            $"map.{SanitizeKey(folderName)}.messages_listing",
            SnapshotObject(listingResult) ?? new { });

        var messages = listingResult.Body ?? [];

        logger.Log(
            $"map.{SanitizeKey(folderName)}.messages_listing_summary",
            BuildListingSummary(
                folderName,
                messages,
                listingResult.MessagesListingSize,
                listingResult.NewMessage,
                listingResult.MseTime));

        foreach (var message in messages.Take(Math.Min(DefaultBodyFetchLimit, messages.Count)))
        {
            if (string.IsNullOrWhiteSpace(message.Handle))
            {
                continue;
            }

            var messageResult = await client.GetMessageAsync(
                CreateGetMessageRequest(message.Handle),
                CreateTraceContext(),
                cancellationToken);

            logger.Log(
                $"map.{SanitizeKey(folderName)}.message_detail",
                new
                {
                    folder = folderName,
                    handle = message.Handle,
                    subject = message.Subject,
                    listingType = message.Type,
                    detail = SummarizeBMessage(messageResult.Body),
                    response = new
                    {
                        messageResult.IsSuccess,
                        responseCode = messageResult.ResponseCode.ToString()
                    }
                });
        }
    }

    private IEnumerable<string> GetOrderedMessageFolders(IEnumerable<string> discoveredFolders)
    {
        return discoveredFolders
            .Concat(FallbackMessageFolders)
            .Where(folder => !string.IsNullOrWhiteSpace(folder))
            .Select(folder => folder.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase);
    }

    private void OnClientClosed(object? sender, MapClientClosedEventArgs args)
    {
        logger.Log("map.client_closed", SnapshotObject(args) ?? new { });
    }

    private void OnMnsConnected(object? sender, MapMnsConnectionStateChangeEventArgs args)
    {
        logger.Log("map.mns_connected", SnapshotObject(args) ?? new { });
    }

    private void OnMnsClosed(object? sender, MapMnsConnectionStateChangeEventArgs args)
    {
        logger.Log("map.mns_closed", SnapshotObject(args) ?? new { });
    }

    private void OnMnsSendEventReceived(object? sender, SendEventReceivedEventArgs args)
    {
        logger.Log(
            "map.mns_event",
            new
            {
                report = args.Event is null
                    ? null
                    : new
                    {
                        type = args.Event.Type.ToString(),
                        args.Event.Handle,
                        args.Event.Folder,
                        args.Event.OldFolder,
                        messageType = args.Event.MessageType.ToString()
                    },
                raw = SnapshotObject(args) ?? new { }
            });

        if (args.Event is not null)
        {
            pendingMnsFollowUps.Add(FollowUpMnsEventAsync(args.Event));
        }
    }

    private async Task FollowUpMnsEventAsync(MapEventReport report)
    {
        try
        {
            await mnsFollowUpLock.WaitAsync();

            // Allow the phone to commit folder transitions before we chase the handle.
            await Task.Delay(TimeSpan.FromMilliseconds(750));

            var client = liveClient;
            if (client is null)
            {
                logger.Log(
                    "map.mns_event_followup_skipped",
                    new
                    {
                        reason = "client_unavailable",
                        type = report.Type.ToString(),
                        report.Handle
                    });
                return;
            }

            if (!string.IsNullOrWhiteSpace(report.Handle))
            {
                var messageResult = await client.GetMessageAsync(
                    CreateGetMessageRequest(report.Handle),
                    CreateTraceContext(),
                    CancellationToken.None);
                logger.Log(
                    "map.mns_event_message_detail",
                    new
                    {
                        eventType = report.Type.ToString(),
                        handle = report.Handle,
                        folder = report.Folder,
                        oldFolder = report.OldFolder,
                        messageType = report.MessageType.ToString(),
                        detail = SummarizeBMessage(messageResult.Body),
                        response = new
                        {
                            messageResult.IsSuccess,
                            responseCode = messageResult.ResponseCode.ToString()
                        }
                    });
            }

            foreach (var folderName in new[] { report.Folder, report.OldFolder }
                         .Select(ExtractRelativeFolderName)
                         .Where(folderName => !string.IsNullOrWhiteSpace(folderName))
                         .Distinct(StringComparer.OrdinalIgnoreCase))
            {
                var listingResult = await client.GetMessagesListingAsync(
                    CreateMessagesListingRequest(folderName!),
                    CreateTraceContext(),
                    CancellationToken.None);
                logger.Log(
                    $"map.mns_event.{SanitizeKey(folderName!)}.messages_listing_summary",
                    BuildListingSummary(
                        folderName!,
                        listingResult.Body ?? [],
                        listingResult.MessagesListingSize,
                        listingResult.NewMessage,
                        listingResult.MseTime));
            }
        }
        catch (Exception exception)
        {
            logger.Log(
                "map.mns_event_followup_failed",
                new
                {
                    report = new
                    {
                        type = report.Type.ToString(),
                        report.Handle,
                        report.Folder,
                        report.OldFolder,
                        messageType = report.MessageType.ToString()
                    },
                    error = exception.ToString()
                });
        }
        finally
        {
            mnsFollowUpLock.Release();
        }
    }

    private async Task FlushPendingMnsFollowUpsAsync()
    {
        if (pendingMnsFollowUps.IsEmpty)
        {
            return;
        }

        try
        {
            await Task.WhenAll(pendingMnsFollowUps.ToArray());
        }
        catch
        {
            // Per-event failures are logged at the source.
        }
    }

    private GetMessagesListingRequestParameters CreateMessagesListingRequest(string folderName)
    {
        return new GetMessagesListingRequestParameters
        {
            Name = folderName,
            MaxListCount = checked((ushort)Math.Min(options.MapMessageLimit, ushort.MaxValue)),
            ListStartOffset = 0,
            SubjectLength = 256,
            ParameterMask = CreateAllParameterMask()
        };
    }

    private static ParameterMask CreateAllParameterMask()
    {
        // The Windows enum stops at ReplyToAddressing, but the wire format is a 32-bit mask.
        // MAP 1.1/1.4 add delivery/conversation/direction MIME fields above bit 15.
        return MapSdpInsights.CreateRichMessageListingParameterMask();
    }

    private static SetFolderRequestParameters CreateSetFolderRequest(string folderName, string flagName)
    {
        var request = new SetFolderRequestParameters();
        SetEnumProperty(request, nameof(SetFolderRequestParameters.Flags), flagName);
        if (!string.IsNullOrEmpty(folderName))
        {
            request.Name = folderName;
        }

        return request;
    }

    private static GetMessageRequestParameters CreateGetMessageRequest(string handle)
    {
        var request = new GetMessageRequestParameters
        {
            Name = handle
        };

        SetEnumProperty(request, nameof(GetMessageRequestParameters.Charset), "Utf8");
        SetEnumProperty(request, nameof(GetMessageRequestParameters.Attachment), "On");
        return request;
    }

    private static PushMessageRequestParameters CreatePushMessageRequest(string recipient, string body)
    {
        var request = new PushMessageRequestParameters
        {
            Name = "outbox",
            Message = CreateBMessage(recipient, body)
        };

        SetEnumProperty(request, nameof(PushMessageRequestParameters.Charset), "Utf8");
        request.Transparent = MessageTransparentType.Off;
        request.Retry = MessageRetryType.On;
        return request;
    }

    private static BMessage CreateBMessage(string recipient, string body)
    {
        return new BMessage
        {
            Recipients =
            [
                new VCard
                {
                    Telephones =
                    [
                        new Telephone
                        {
                            Number = recipient,
                            Preference = -1,
                            Type = TelephoneType.Personal
                        }
                    ]
                }
            ],
            BodyContent = new BMessageBodyContent
            {
                Content = body
            },
            Charset = BMessageCharset.Utf8,
            MessageType = BMessageType.SMSGSM,
            Status = BMessageStatus.Read
        };
    }

    private static object SummarizeBMessage(BMessage? message)
    {
        if (message is null)
        {
            return new { missing = true };
        }

        return new
        {
            folder = message.Folder,
            messageType = message.MessageType?.ToString(),
            status = message.Status?.ToString(),
            encoding = message.Encoding?.ToString(),
            charset = message.Charset?.ToString(),
            language = message.Language?.ToString(),
            version = message.Version,
            bodyLength = message.BodyLength,
            body = Truncate(message.BodyContent?.Content, 512),
            originators = SummarizeVCards(message.Originators),
            recipients = SummarizeVCards(message.Recipients)
        };
    }

    private static object[] SummarizeVCards(IEnumerable<VCard>? cards)
    {
        if (cards is null)
        {
            return [];
        }

        return cards
            .Take(10)
            .Select(
                card => new
                {
                    name = ReadDisplayName(card),
                    phones = card.Telephones?
                        .Where(telephone => !string.IsNullOrWhiteSpace(telephone.Number))
                        .Select(
                            telephone => new
                            {
                                telephone.Number,
                                type = telephone.Type.ToString()
                            })
                        .ToArray() ?? [],
                    emails = card.Emails?
                        .Where(email => !string.IsNullOrWhiteSpace(email.EmailAddress))
                        .Select(email => email.EmailAddress)
                        .ToArray() ?? []
                })
            .ToArray();
    }

    private static string ReadDisplayName(VCard card)
    {
        if (!string.IsNullOrWhiteSpace(card.FormattedName))
        {
            return card.FormattedName;
        }

        var parts = new[]
        {
            card.Prefix,
            card.FirstName,
            card.MiddleName,
            card.LastName,
            card.Suffix
        };

        var composite = string.Join(
            " ",
            parts.Where(part => !string.IsNullOrWhiteSpace(part)).Select(part => part!.Trim()));

        return string.IsNullOrWhiteSpace(composite)
            ? "(unnamed)"
            : composite;
    }

    private static string? Truncate(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length <= maxLength)
        {
            return value;
        }

        return $"{value[..maxLength]}...";
    }

    private static void SetEnumProperty(object target, string propertyName, string enumValueName)
    {
        var property = target.GetType().GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public);
        if (property is null)
        {
            throw new MissingMemberException(target.GetType().FullName, propertyName);
        }

        property.SetValue(target, Enum.Parse(property.PropertyType, enumValueName, ignoreCase: false));
    }

    private static ITraceContext CreateTraceContext()
    {
        return new TraceContext(
            Guid.NewGuid().ToString("N"),
            string.Empty,
            Guid.NewGuid().ToString("N"),
            traceFlags: 0,
            new Dictionary<string, string>());
    }

    private static T? GetPropertyValue<T>(object? target, string propertyName)
        where T : class
    {
        return target?.GetType()
            .GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public)
            ?.GetValue(target) as T;
    }

    private static bool ReadBoolProperty(object? target, string propertyName)
    {
        var value = target?.GetType()
            .GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public)
            ?.GetValue(target);
        return value is bool boolValue && boolValue;
    }

    private static string SanitizeKey(string value)
    {
        return value
            .Replace('/', '_')
            .Replace('\\', '_')
            .Replace(' ', '_')
            .ToLowerInvariant();
    }

    private static object ParseMasSdpRecord(IDictionary<uint, byte[]> attributes)
    {
        try
        {
            var assembly = typeof(MapClient).Assembly;
            var recordType = assembly.GetType(
                "Microsoft.Internal.Bluetooth.Map.Protocol.MapMasSdpRecord",
                throwOnError: true)!;
            var record = Activator.CreateInstance(
                recordType,
                BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public,
                binder: null,
                args: [attributes],
                culture: null)!;

            var mapVersion = ReadPropertyValue<ushort>(recordType, record, "MapVersion");
            var supportedFeatureMask = TryReadSdpUnsignedInteger(attributes, 0x0317);
            return new
            {
                mapVersionRaw = mapVersion,
                mapVersion = FormatMapVersion(mapVersion),
                instanceId = ReadPropertyValue<uint>(recordType, record, "InstanceId"),
                serviceName = ReadPropertyValue<string>(recordType, record, "ServiceName"),
                port = ReadPropertyValue<uint>(recordType, record, "Port"),
                supportedMessageTypes = ((IEnumerable?)recordType.GetProperty("SupportedMessageTypes", AllInstanceProperties)
                        ?.GetValue(record))
                    ?.Cast<object>()
                    .Select(value => value.ToString())
                    .ToArray(),
                supportedFeaturesRaw = supportedFeatureMask,
                supportedFeatures = DecodeMasSupportedFeatures(supportedFeatureMask)
            };
        }
        catch (Exception exception)
        {
            return new
            {
                parseFailed = true,
                error = exception.ToString()
            };
        }
    }

    private static string? FormatMapVersion(ushort? rawVersion)
    {
        if (rawVersion is null)
        {
            return null;
        }

        var major = (rawVersion.Value >> 8) & 0xFF;
        var minor = rawVersion.Value & 0xFF;
        return $"{major}.{minor}";
    }

    private static T? ReadPropertyValue<T>(Type recordType, object record, string propertyName)
    {
        var property = recordType.GetProperty(propertyName, AllInstanceProperties);
        if (property is null)
        {
            return default;
        }

        var value = property.GetValue(record);
        if (value is T typed)
        {
            return typed;
        }

        if (value is null)
        {
            return default;
        }

        return (T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture);
    }

    private static ulong? TryReadSdpUnsignedInteger(IDictionary<uint, byte[]> attributes, uint attributeId)
    {
        return attributes.TryGetValue(attributeId, out var raw)
            ? ReadSdpUnsignedInteger(raw)
            : null;
    }

    private static ulong? ReadSdpUnsignedInteger(byte[] raw)
    {
        if (raw.Length < 2)
        {
            return null;
        }

        var header = raw[0];
        var typeDescriptor = header >> 3;
        var sizeIndex = header & 0x07;
        if (typeDescriptor != 1)
        {
            return null;
        }

        int lengthOffset;
        int payloadLength;

        switch (sizeIndex)
        {
            case <= 4:
                payloadLength = 1 << sizeIndex;
                lengthOffset = 1;
                break;
            case 5 when raw.Length >= 2:
                payloadLength = raw[1];
                lengthOffset = 2;
                break;
            case 6 when raw.Length >= 3:
                payloadLength = (raw[1] << 8) | raw[2];
                lengthOffset = 3;
                break;
            case 7 when raw.Length >= 5:
                payloadLength = (raw[1] << 24) | (raw[2] << 16) | (raw[3] << 8) | raw[4];
                lengthOffset = 5;
                break;
            default:
                return null;
        }

        return lengthOffset + payloadLength <= raw.Length
            ? ReadBigEndianInteger(raw.AsSpan(lengthOffset, payloadLength).ToArray())
            : null;
    }

    private static string[] DecodeMasSupportedFeatures(ulong? mask)
    {
        if (mask is null)
        {
            return [];
        }

        var features = new List<string>();
        for (var bit = 0; bit < MasSupportedFeatureNames.Length; bit++)
        {
            if ((mask.Value & (1UL << bit)) != 0)
            {
                features.Add(MasSupportedFeatureNames[bit]);
            }
        }

        return features.ToArray();
    }

    private static ulong? ReadBigEndianInteger(byte[] payload)
    {
        if (payload.Length is 0 or > 8)
        {
            return null;
        }

        ulong value = 0;
        foreach (var current in payload)
        {
            value = (value << 8) | current;
        }

        return value;
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

    private static string? ExtractRelativeFolderName(string? folderPath)
    {
        if (string.IsNullOrWhiteSpace(folderPath))
        {
            return null;
        }

        var normalized = folderPath.Replace('\\', '/').Trim();
        var lastSlash = normalized.LastIndexOf('/');
        return lastSlash >= 0 ? normalized[(lastSlash + 1)..] : normalized;
    }

    private static object BuildListingSummary(
        string folderName,
        IReadOnlyList<MessageListingEntry> messages,
        ushort? listingSize,
        bool? newMessage,
        string? listingTime)
    {
        var summary = messages
            .Take(Math.Min(messages.Count, 10))
            .Select(
                message => new
                {
                    message.Handle,
                    message.Type,
                    message.Subject,
                    message.Datetime,
                    message.SenderName,
                    message.SenderAddressing,
                    message.RecipientAddressing,
                    message.Size,
                    message.AttachmentSize,
                    message.Priority,
                    message.Read,
                    message.Sent,
                    message.Protected
                })
            .ToArray();
        var typeCounts = messages
            .Select(message => message.Type ?? "(unknown)")
            .GroupBy(type => type, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => group.Count(), StringComparer.OrdinalIgnoreCase);

        return new
        {
            folder = folderName,
            totalCount = messages.Count,
            listingSize,
            newMessage,
            listingTime,
            typeCounts,
            items = summary
        };
    }

    private static object? SnapshotObject(object? value, int depth = 0)
    {
        if (value is null)
        {
            return null;
        }

        if (depth >= 2)
        {
            return new
            {
                type = value.GetType().FullName,
                text = value.ToString()
            };
        }

        if (value is string || value.GetType().IsPrimitive || value is Guid || value is Enum)
        {
            return value;
        }

        if (value is IEnumerable enumerable && value is not IDictionary)
        {
            return enumerable.Cast<object?>()
                .Take(10)
                .Select(item => SnapshotObject(item, depth + 1))
                .ToArray();
        }

        var properties = value.GetType()
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(property => property.CanRead && property.GetIndexParameters().Length == 0)
            .Take(20)
            .ToDictionary(
                property => property.Name,
                property =>
                {
                    try
                    {
                        return SnapshotObject(property.GetValue(value), depth + 1);
                    }
                    catch (Exception exception)
                    {
                        return new
                        {
                            error = exception.GetType().Name,
                            exception.Message
                        };
                    }
                });

        return new
        {
            type = value.GetType().FullName,
            properties
        };
    }
}
