using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Adit.Core.Models;
using Adit.Core.Utilities;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

namespace Adit.Daemon.Services;

public sealed class SqliteCacheStore
{
    private const string ProjectionConversationAliasKind = "projection_conversation";
    private const string ProjectionMessageAliasKind = "projection_message";
    private const string MapHandleAliasKind = "map_handle";
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly string connectionString;
    private readonly string databasePath;
    private readonly bool encryptDatabaseAtRest;
    private readonly Func<string, FileAttributes> getFileAttributes;
    private readonly Action<string> encryptFile;
    private readonly ILogger<SqliteCacheStore>? logger;
    private bool databaseProtectionUnavailable;

    public SqliteCacheStore(DaemonOptions options)
        : this(options, logger: null)
    {
    }

    public SqliteCacheStore(DaemonOptions options, ILogger<SqliteCacheStore>? logger)
        : this(options, logger, File.GetAttributes, File.Encrypt)
    {
    }

    internal SqliteCacheStore(
        DaemonOptions options,
        ILogger<SqliteCacheStore>? logger,
        Func<string, FileAttributes> getFileAttributes,
        Action<string> encryptFile)
    {
        var fullPath = Path.GetFullPath(options.DatabasePath);
        Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
        databasePath = fullPath;
        encryptDatabaseAtRest = options.EncryptDatabaseAtRest;
        this.logger = logger;
        this.getFileAttributes = getFileAttributes;
        this.encryptFile = encryptFile;
        connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = fullPath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Shared
        }.ToString();

        if (File.Exists(databasePath))
        {
            EnsureDatabaseProtection();
        }
    }

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            CREATE TABLE IF NOT EXISTS contacts (
                device_id TEXT NOT NULL,
                contact_key TEXT NOT NULL,
                unique_identifier TEXT NULL,
                display_name TEXT NOT NULL,
                search_name TEXT NOT NULL,
                phones_json TEXT NOT NULL,
                emails_json TEXT NOT NULL,
                json TEXT NOT NULL,
                updated_utc TEXT NOT NULL,
                PRIMARY KEY (device_id, contact_key)
            );

            CREATE INDEX IF NOT EXISTS idx_contacts_search
            ON contacts(device_id, search_name);

            CREATE TABLE IF NOT EXISTS trusted_le_devices (
                classic_device_id TEXT NOT NULL,
                container_id TEXT NULL,
                classic_name TEXT NOT NULL,
                le_device_id TEXT NOT NULL,
                le_address TEXT NULL,
                updated_utc TEXT NOT NULL,
                PRIMARY KEY (classic_device_id)
            );

            CREATE INDEX IF NOT EXISTS idx_trusted_le_devices_container
            ON trusted_le_devices(container_id, updated_utc DESC);

            CREATE TABLE IF NOT EXISTS map_observations (
                device_id TEXT NOT NULL,
                observation_id TEXT NOT NULL,
                session_id TEXT NULL,
                observed_utc TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                event_type TEXT NULL,
                folder TEXT NULL,
                old_folder TEXT NULL,
                handle TEXT NULL,
                sort_utc TEXT NULL,
                sender_addressing TEXT NULL,
                recipient_addressing TEXT NULL,
                body_hash TEXT NULL,
                preview TEXT NULL,
                raw_json TEXT NOT NULL,
                PRIMARY KEY (device_id, observation_id)
            );

            CREATE INDEX IF NOT EXISTS idx_map_observations_handle
            ON map_observations(device_id, handle, observed_utc DESC);

            CREATE INDEX IF NOT EXISTS idx_map_observations_body
            ON map_observations(device_id, body_hash, observed_utc DESC);

            CREATE TABLE IF NOT EXISTS ancs_observations (
                device_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                notification_uid INTEGER NOT NULL,
                event_kind TEXT NOT NULL,
                event_flags INTEGER NOT NULL,
                category TEXT NOT NULL,
                received_utc TEXT NOT NULL,
                updated_utc TEXT NOT NULL,
                removed_utc TEXT NULL,
                active INTEGER NOT NULL,
                app_identifier TEXT NULL,
                title TEXT NULL,
                subtitle TEXT NULL,
                message TEXT NULL,
                message_hash TEXT NULL,
                raw_json TEXT NOT NULL,
                PRIMARY KEY (device_id, session_id, notification_uid)
            );

            CREATE INDEX IF NOT EXISTS idx_ancs_observations_active
            ON ancs_observations(device_id, active, updated_utc DESC);

            CREATE TABLE IF NOT EXISTS send_intents (
                device_id TEXT NOT NULL,
                send_intent_id TEXT NOT NULL,
                created_utc TEXT NOT NULL,
                recipient_raw TEXT NOT NULL,
                recipient_point_key TEXT NULL,
                recipient_display_name TEXT NULL,
                contact_unique_identifier TEXT NULL,
                body TEXT NOT NULL,
                body_hash TEXT NOT NULL,
                preview TEXT NOT NULL,
                result_success INTEGER NULL,
                result_code TEXT NULL,
                message_handle TEXT NULL,
                completed_utc TEXT NULL,
                json TEXT NOT NULL,
                PRIMARY KEY (device_id, send_intent_id)
            );

            CREATE INDEX IF NOT EXISTS idx_send_intents_handle
            ON send_intents(device_id, message_handle, created_utc DESC);

            CREATE TABLE IF NOT EXISTS thread_entities (
                device_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                revision INTEGER NOT NULL,
                state TEXT NOT NULL,
                display_name TEXT NOT NULL,
                is_group INTEGER NOT NULL,
                last_message_utc TEXT NULL,
                last_message_ticks INTEGER NOT NULL,
                unread_count INTEGER NOT NULL,
                message_count INTEGER NOT NULL,
                source_folders_json TEXT NOT NULL,
                json TEXT NOT NULL,
                updated_utc TEXT NOT NULL,
                PRIMARY KEY (device_id, thread_id)
            );

            CREATE INDEX IF NOT EXISTS idx_thread_entities_last
            ON thread_entities(device_id, last_message_ticks DESC);

            CREATE TABLE IF NOT EXISTS thread_aliases (
                device_id TEXT NOT NULL,
                alias_kind TEXT NOT NULL,
                alias_key TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                created_utc TEXT NOT NULL,
                PRIMARY KEY (device_id, alias_kind, alias_key)
            );

            CREATE INDEX IF NOT EXISTS idx_thread_aliases_thread
            ON thread_aliases(device_id, thread_id);

            CREATE TABLE IF NOT EXISTS message_entities (
                device_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                handle TEXT NULL,
                sort_utc TEXT NULL,
                sort_ticks INTEGER NOT NULL,
                folder TEXT NOT NULL,
                visibility_state TEXT NOT NULL,
                assignment_state TEXT NOT NULL,
                json TEXT NOT NULL,
                updated_utc TEXT NOT NULL,
                PRIMARY KEY (device_id, message_id)
            );

            CREATE INDEX IF NOT EXISTS idx_message_entities_thread
            ON message_entities(device_id, thread_id, sort_ticks DESC);

            CREATE INDEX IF NOT EXISTS idx_message_entities_handle
            ON message_entities(device_id, handle, sort_ticks DESC);

            CREATE TABLE IF NOT EXISTS message_aliases (
                device_id TEXT NOT NULL,
                alias_kind TEXT NOT NULL,
                alias_key TEXT NOT NULL,
                message_id TEXT NOT NULL,
                created_utc TEXT NOT NULL,
                PRIMARY KEY (device_id, alias_kind, alias_key)
            );

            CREATE INDEX IF NOT EXISTS idx_message_aliases_message
            ON message_aliases(device_id, message_id);

            CREATE TABLE IF NOT EXISTS messages (
                device_id TEXT NOT NULL,
                message_key TEXT NOT NULL,
                conversation_id TEXT NOT NULL,
                folder TEXT NOT NULL,
                sort_utc TEXT NULL,
                sort_ticks INTEGER NOT NULL,
                handle TEXT NULL,
                conversation_display_name TEXT NOT NULL,
                is_group INTEGER NOT NULL,
                preview TEXT NULL,
                json TEXT NOT NULL,
                updated_utc TEXT NOT NULL,
                PRIMARY KEY (device_id, message_key)
            );

            CREATE INDEX IF NOT EXISTS idx_messages_conversation
            ON messages(device_id, conversation_id, sort_ticks DESC);

            CREATE INDEX IF NOT EXISTS idx_messages_folder
            ON messages(device_id, folder, sort_ticks DESC);

            CREATE TABLE IF NOT EXISTS conversations (
                device_id TEXT NOT NULL,
                conversation_id TEXT NOT NULL,
                display_name TEXT NOT NULL,
                is_group INTEGER NOT NULL,
                last_message_utc TEXT NULL,
                last_message_ticks INTEGER NOT NULL,
                unread_count INTEGER NOT NULL,
                message_count INTEGER NOT NULL,
                last_preview TEXT NULL,
                participants_json TEXT NOT NULL,
                source_folders_json TEXT NOT NULL,
                json TEXT NOT NULL,
                updated_utc TEXT NOT NULL,
                PRIMARY KEY (device_id, conversation_id)
            );

            CREATE INDEX IF NOT EXISTS idx_conversations_last
            ON conversations(device_id, last_message_ticks DESC);

            CREATE TABLE IF NOT EXISTS notifications (
                device_id TEXT NOT NULL,
                notification_uid INTEGER NOT NULL,
                active INTEGER NOT NULL,
                received_utc TEXT NOT NULL,
                updated_utc TEXT NOT NULL,
                removed_utc TEXT NULL,
                app_identifier TEXT NULL,
                category TEXT NOT NULL,
                title TEXT NULL,
                subtitle TEXT NULL,
                message TEXT NULL,
                positive_action_label TEXT NULL,
                negative_action_label TEXT NULL,
                json TEXT NOT NULL,
                PRIMARY KEY (device_id, notification_uid)
            );

            CREATE INDEX IF NOT EXISTS idx_notifications_active
            ON notifications(device_id, active, updated_utc DESC);

            CREATE TABLE IF NOT EXISTS daemon_settings (
                setting_key TEXT NOT NULL,
                setting_value TEXT NOT NULL,
                updated_utc TEXT NOT NULL,
                PRIMARY KEY (setting_key)
            );
            """;
        await command.ExecuteNonQueryAsync(cancellationToken);
        EnsureDatabaseProtection();
    }

    public async Task UpsertTrustedLeDeviceAsync(
        BluetoothEndpointRecord classicTarget,
        BluetoothLeDeviceRecord leTarget,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await ExecuteNonQueryAsync(
            connection,
            null,
            """
            INSERT INTO trusted_le_devices (
                classic_device_id,
                container_id,
                classic_name,
                le_device_id,
                le_address,
                updated_utc
            )
            VALUES (
                $classicDeviceId,
                $containerId,
                $classicName,
                $leDeviceId,
                $leAddress,
                $updatedUtc
            )
            ON CONFLICT(classic_device_id) DO UPDATE SET
                container_id = excluded.container_id,
                classic_name = excluded.classic_name,
                le_device_id = excluded.le_device_id,
                le_address = excluded.le_address,
                updated_utc = excluded.updated_utc;
            """,
            [
                ("$classicDeviceId", classicTarget.Id),
                ("$containerId", (object?)classicTarget.ContainerId ?? DBNull.Value),
                ("$classicName", classicTarget.Name),
                ("$leDeviceId", leTarget.Id),
                ("$leAddress", (object?)leTarget.Address ?? DBNull.Value),
                ("$updatedUtc", DateTimeOffset.UtcNow.ToString("O"))
            ],
            cancellationToken);
    }

    public async Task<(string DeviceId, string? Address)?> GetTrustedLeDeviceAsync(
        string classicDeviceId,
        string? containerId,
        CancellationToken cancellationToken)
    {
        _ = containerId;
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT le_device_id, le_address
            FROM trusted_le_devices
            WHERE classic_device_id = $classicDeviceId
            ORDER BY updated_utc DESC
            LIMIT 1;
            """;
        command.Parameters.AddWithValue("$classicDeviceId", classicDeviceId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        var deviceId = reader.GetString(0);
        var address = reader.IsDBNull(1) ? null : reader.GetString(1);
        return (deviceId, address);
    }

    public async Task<string> GetNotificationsModeAsync(
        CancellationToken cancellationToken)
    {
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT setting_value
            FROM daemon_settings
            WHERE setting_key = 'notifications_mode'
            LIMIT 1;
            """;

        var value = await command.ExecuteScalarAsync(cancellationToken);
        if (value is not null && value is not DBNull)
        {
            return NotificationMode.Normalize(value.ToString());
        }

        command.Parameters.Clear();
        command.CommandText =
            """
            SELECT setting_value
            FROM daemon_settings
            WHERE setting_key = 'notifications_enabled'
            LIMIT 1;
            """;

        value = await command.ExecuteScalarAsync(cancellationToken);
        if (value is null || value is DBNull)
        {
            return NotificationMode.Auto;
        }

        // Legacy migration: the old default-off gate should not become a permanent opt-out.
        return bool.TryParse(value.ToString(), out var parsed) && parsed
            ? NotificationMode.On
            : NotificationMode.Auto;
    }

    public async Task SetNotificationsModeAsync(
        string mode,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await ExecuteNonQueryAsync(
            connection,
            null,
            """
            INSERT INTO daemon_settings (
                setting_key,
                setting_value,
                updated_utc
            )
            VALUES (
                'notifications_mode',
                $settingValue,
                $updatedUtc
            )
            ON CONFLICT(setting_key) DO UPDATE SET
                setting_value = excluded.setting_value,
                updated_utc = excluded.updated_utc;
            """,
            [
                ("$settingValue", NotificationMode.Normalize(mode)),
                ("$updatedUtc", DateTimeOffset.UtcNow.ToString("O"))
            ],
            cancellationToken);
    }

    public async Task ReplaceContactsAsync(
        string deviceId,
        IReadOnlyList<ContactRecord> contacts,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await ExecuteNonQueryAsync(
            connection,
            transaction,
            "DELETE FROM contacts WHERE device_id = $deviceId;",
            [("$deviceId", deviceId)],
            cancellationToken);

        foreach (var contact in contacts)
        {
            await ExecuteNonQueryAsync(
                connection,
                transaction,
                """
                INSERT INTO contacts (
                    device_id,
                    contact_key,
                    unique_identifier,
                    display_name,
                    search_name,
                    phones_json,
                    emails_json,
                    json,
                    updated_utc
                )
                VALUES (
                    $deviceId,
                    $contactKey,
                    $uniqueIdentifier,
                    $displayName,
                    $searchName,
                    $phonesJson,
                    $emailsJson,
                    $json,
                    $updatedUtc
                );
                """,
                [
                    ("$deviceId", deviceId),
                    ("$contactKey", BuildContactKey(contact)),
                    ("$uniqueIdentifier", (object?)contact.UniqueIdentifier ?? DBNull.Value),
                    ("$displayName", contact.DisplayName),
                    ("$searchName", contact.DisplayName.ToLowerInvariant()),
                    ("$phonesJson", JsonSerializer.Serialize(contact.Phones, SerializerOptions)),
                    ("$emailsJson", JsonSerializer.Serialize(contact.Emails, SerializerOptions)),
                    ("$json", JsonSerializer.Serialize(contact, SerializerOptions)),
                    ("$updatedUtc", DateTimeOffset.UtcNow.ToString("O"))
                ],
                cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    public async Task AppendMapSnapshotObservationsAsync(
        string deviceId,
        string? sessionId,
        IReadOnlyList<MessageRecord> messages,
        CancellationToken cancellationToken)
    {
        if (messages.Count == 0)
        {
            return;
        }

        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var observedUtc = DateTimeOffset.UtcNow;
        for (var index = 0; index < messages.Count; index++)
        {
            var message = messages[index];
            var observationId = $"mapobs_{observedUtc.UtcTicks:x}_{index:x4}";
            await InsertMapObservationAsync(
                connection,
                transaction,
                deviceId,
                observationId,
                sessionId,
                observedUtc,
                "snapshot",
                null,
                message.Folder,
                null,
                message.Handle,
                message,
                message,
                cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    public async Task AppendMapRealtimeObservationAsync(
        string deviceId,
        string? sessionId,
        MapRealtimeEventRecord realtimeEvent,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var observationSeed = CreateStableId("mapobs");
        await InsertMapObservationAsync(
            connection,
            transaction,
            deviceId,
            $"{observationSeed}_event",
            sessionId,
            realtimeEvent.TimestampUtc,
            "realtime_event",
            realtimeEvent.EventType,
            realtimeEvent.Folder,
            realtimeEvent.OldFolder,
            realtimeEvent.Handle,
            realtimeEvent.Message,
            realtimeEvent,
            cancellationToken);

        if (realtimeEvent.Message is not null)
        {
            await InsertMapObservationAsync(
                connection,
                transaction,
                deviceId,
                $"{observationSeed}_message",
                sessionId,
                realtimeEvent.TimestampUtc,
                "realtime_message",
                realtimeEvent.EventType,
                realtimeEvent.Message.Folder,
                realtimeEvent.OldFolder,
                realtimeEvent.Message.Handle ?? realtimeEvent.Handle,
                realtimeEvent.Message,
                realtimeEvent.Message,
                cancellationToken);
        }

        for (var index = 0; index < realtimeEvent.AffectedMessages.Count; index++)
        {
            var relatedMessage = realtimeEvent.AffectedMessages[index];
            await InsertMapObservationAsync(
                connection,
                transaction,
                deviceId,
                $"{observationSeed}_related_{index:x2}",
                sessionId,
                realtimeEvent.TimestampUtc,
                "realtime_related",
                realtimeEvent.EventType,
                relatedMessage.Folder,
                realtimeEvent.OldFolder,
                relatedMessage.Handle,
                relatedMessage,
                relatedMessage,
                cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    public async Task AppendAncsObservationAsync(
        string deviceId,
        string? sessionId,
        NotificationRecord notification,
        CancellationToken cancellationToken)
    {
        var effectiveSessionId = string.IsNullOrWhiteSpace(sessionId) ? "ancs_unknown" : sessionId.Trim();
        var rawJson = JsonSerializer.Serialize(notification, SerializerOptions);
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await ExecuteNonQueryAsync(
            connection,
            null,
            """
            INSERT INTO ancs_observations (
                device_id,
                session_id,
                notification_uid,
                event_kind,
                event_flags,
                category,
                received_utc,
                updated_utc,
                removed_utc,
                active,
                app_identifier,
                title,
                subtitle,
                message,
                message_hash,
                raw_json
            )
            VALUES (
                $deviceId,
                $sessionId,
                $notificationUid,
                $eventKind,
                $eventFlags,
                $category,
                $receivedUtc,
                $updatedUtc,
                NULL,
                1,
                $appIdentifier,
                $title,
                $subtitle,
                $message,
                $messageHash,
                $rawJson
            )
            ON CONFLICT(device_id, session_id, notification_uid) DO UPDATE SET
                event_kind = excluded.event_kind,
                event_flags = excluded.event_flags,
                category = excluded.category,
                received_utc = excluded.received_utc,
                updated_utc = excluded.updated_utc,
                removed_utc = NULL,
                active = 1,
                app_identifier = excluded.app_identifier,
                title = excluded.title,
                subtitle = excluded.subtitle,
                message = excluded.message,
                message_hash = excluded.message_hash,
                raw_json = excluded.raw_json;
            """,
            [
                ("$deviceId", deviceId),
                ("$sessionId", effectiveSessionId),
                ("$notificationUid", (long)notification.NotificationUid),
                ("$eventKind", notification.EventKind.ToString()),
                ("$eventFlags", (long)notification.EventFlags),
                ("$category", notification.Category.ToString()),
                ("$receivedUtc", notification.ReceivedAtUtc.ToString("O")),
                ("$updatedUtc", DateTimeOffset.UtcNow.ToString("O")),
                ("$appIdentifier", (object?)notification.AppIdentifier ?? DBNull.Value),
                ("$title", (object?)notification.Title ?? DBNull.Value),
                ("$subtitle", (object?)notification.Subtitle ?? DBNull.Value),
                ("$message", (object?)notification.Message ?? DBNull.Value),
                ("$messageHash", (object?)ComputeHash(notification.Message) ?? DBNull.Value),
                ("$rawJson", rawJson)
            ],
            cancellationToken);
    }

    public async Task MarkAncsObservationRemovedAsync(
        string deviceId,
        string? sessionId,
        uint notificationUid,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await ExecuteNonQueryAsync(
            connection,
            null,
            """
            UPDATE ancs_observations
            SET active = 0,
                updated_utc = $updatedUtc,
                removed_utc = $removedUtc
            WHERE device_id = $deviceId
              AND notification_uid = $notificationUid
              AND ($sessionId IS NULL OR session_id = $sessionId);
            """,
            [
                ("$deviceId", deviceId),
                ("$sessionId", string.IsNullOrWhiteSpace(sessionId) ? DBNull.Value : sessionId),
                ("$notificationUid", (long)notificationUid),
                ("$updatedUtc", DateTimeOffset.UtcNow.ToString("O")),
                ("$removedUtc", DateTimeOffset.UtcNow.ToString("O"))
            ],
            cancellationToken);
    }

    public async Task<string> CreateSendIntentAsync(
        string deviceId,
        string recipient,
        ResolvedRecipientRecord? resolvedRecipient,
        string body,
        CancellationToken cancellationToken)
    {
        var sendIntentId = CreateStableId("send");
        var createdUtc = DateTimeOffset.UtcNow;
        var recipientPointKey = BuildRecipientPointKey(recipient);
        var preview = BuildPreview(body) ?? string.Empty;
        var payload = new
        {
            sendIntentId,
            deviceId,
            createdUtc,
            recipient,
            recipientPointKey,
            resolvedContact = resolvedRecipient?.Contact,
            body,
            bodyHash = ComputeHash(body),
            preview
        };

        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await ExecuteNonQueryAsync(
            connection,
            null,
            """
            INSERT INTO send_intents (
                device_id,
                send_intent_id,
                created_utc,
                recipient_raw,
                recipient_point_key,
                recipient_display_name,
                contact_unique_identifier,
                body,
                body_hash,
                preview,
                result_success,
                result_code,
                message_handle,
                completed_utc,
                json
            )
            VALUES (
                $deviceId,
                $sendIntentId,
                $createdUtc,
                $recipientRaw,
                $recipientPointKey,
                $recipientDisplayName,
                $contactUniqueIdentifier,
                $body,
                $bodyHash,
                $preview,
                NULL,
                NULL,
                NULL,
                NULL,
                $json
            );
            """,
            [
                ("$deviceId", deviceId),
                ("$sendIntentId", sendIntentId),
                ("$createdUtc", createdUtc.ToString("O")),
                ("$recipientRaw", recipient),
                ("$recipientPointKey", (object?)recipientPointKey ?? DBNull.Value),
                ("$recipientDisplayName", (object?)resolvedRecipient?.Contact.DisplayName ?? DBNull.Value),
                ("$contactUniqueIdentifier", (object?)resolvedRecipient?.Contact.UniqueIdentifier ?? DBNull.Value),
                ("$body", body),
                ("$bodyHash", ComputeHash(body)),
                ("$preview", preview),
                ("$json", JsonSerializer.Serialize(payload, SerializerOptions))
            ],
            cancellationToken);

        return sendIntentId;
    }

    public async Task CompleteSendIntentAsync(
        string deviceId,
        string sendIntentId,
        SendMessageResult result,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await ExecuteNonQueryAsync(
            connection,
            null,
            """
            UPDATE send_intents
            SET result_success = $resultSuccess,
                result_code = $resultCode,
                message_handle = $messageHandle,
                completed_utc = $completedUtc
            WHERE device_id = $deviceId
              AND send_intent_id = $sendIntentId;
            """,
            [
                ("$deviceId", deviceId),
                ("$sendIntentId", sendIntentId),
                ("$resultSuccess", result.IsSuccess ? 1 : 0),
                ("$resultCode", (object?)result.ResponseCode ?? DBNull.Value),
                ("$messageHandle", (object?)result.MessageHandle ?? DBNull.Value),
                ("$completedUtc", DateTimeOffset.UtcNow.ToString("O"))
            ],
            cancellationToken);
    }

    public async Task<string?> ResolveConversationIdAsync(
        string deviceId,
        string conversationId,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT thread_id
            FROM thread_aliases
            WHERE device_id = $deviceId
              AND alias_kind = $aliasKind
              AND alias_key = $aliasKey
            LIMIT 1;
            """;
        command.Parameters.AddWithValue("$deviceId", deviceId);
        command.Parameters.AddWithValue("$aliasKind", ProjectionConversationAliasKind);
        command.Parameters.AddWithValue("$aliasKey", conversationId);

        var scalar = await command.ExecuteScalarAsync(cancellationToken);
        return scalar?.ToString();
    }

    public async Task ReplaceMessageSnapshotAsync(
        string deviceId,
        ConversationSynthesisResult synthesis,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var pendingMessagesByProjectionKey = new Dictionary<string, PendingStableMessage>(StringComparer.OrdinalIgnoreCase);
        foreach (var message in synthesis.Messages)
        {
            var messageId = await ResolveOrCreateMessageIdAsync(
                connection,
                transaction,
                deviceId,
                message.MessageKey,
                message.Message.Handle,
                cancellationToken);
            var existingThreadId = await GetExistingThreadIdForMessageAsync(
                connection,
                transaction,
                deviceId,
                messageId,
                cancellationToken);
            pendingMessagesByProjectionKey[message.MessageKey] = new PendingStableMessage(
                message,
                messageId,
                existingThreadId);
        }

        var continuitySeedConversations = synthesis.Conversations
            .GroupBy(conversation => conversation.ConversationId, StringComparer.OrdinalIgnoreCase)
            .Select(
                group => group
                    .OrderByDescending(conversation => conversation.IsGroup)
                    .ThenByDescending(conversation => conversation.MessageCount)
                    .ThenByDescending(conversation => conversation.LastMessageUtc ?? DateTimeOffset.MinValue)
                    .First())
            .ToArray();

        var pendingMessagesByConversationId = continuitySeedConversations
            .ToDictionary(
                conversation => conversation.ConversationId,
                conversation => pendingMessagesByProjectionKey.Values
                    .Where(
                        pending => string.Equals(
                            pending.Projection.ConversationId,
                            conversation.ConversationId,
                            StringComparison.OrdinalIgnoreCase))
                    .ToArray(),
                StringComparer.OrdinalIgnoreCase);
        var continuityOwners = await DetermineContinuityOwnersAsync(
            connection,
            transaction,
            deviceId,
            continuitySeedConversations,
            pendingMessagesByConversationId,
            cancellationToken);

        var stableConversationMap = new Dictionary<string, ConversationSnapshot>(StringComparer.OrdinalIgnoreCase);
        foreach (var conversation in synthesis.Conversations)
        {
            var pendingConversationMessages = pendingMessagesByConversationId[conversation.ConversationId];
            continuityOwners.TryGetValue(conversation.ConversationId, out var continuityThreadId);
            var threadId = await ResolveOrCreateThreadIdAsync(
                connection,
                transaction,
                deviceId,
                conversation.ConversationId,
                pendingConversationMessages,
                continuityThreadId,
                cancellationToken);
            var stableConversation = conversation with
            {
                ConversationId = threadId
            };

            var existingThread = await GetThreadProjectionAsync(
                connection,
                transaction,
                deviceId,
                threadId,
                cancellationToken);
            var revision = existingThread is null
                ? 1
                : AreEquivalent(existingThread.Value.Projection, stableConversation)
                    ? existingThread.Value.Revision
                    : existingThread.Value.Revision + 1;

            stableConversationMap[conversation.ConversationId] = stableConversation;

            await UpsertThreadAliasAsync(
                connection,
                transaction,
                deviceId,
                ProjectionConversationAliasKind,
                conversation.ConversationId,
                threadId,
                cancellationToken);
            await UpsertThreadEntityAsync(
                connection,
                transaction,
                deviceId,
                threadId,
                revision,
                stableConversation,
                cancellationToken);
        }

        var stableMessages = new List<SynthesizedMessageRecord>(synthesis.Messages.Count);
        foreach (var message in synthesis.Messages)
        {
            if (!stableConversationMap.TryGetValue(message.ConversationId, out var stableConversation))
            {
                continue;
            }

            if (!pendingMessagesByProjectionKey.TryGetValue(message.MessageKey, out var pendingMessage))
            {
                continue;
            }

            var messageId = pendingMessage.MessageId;
            var stableMessage = message with
            {
                MessageKey = messageId,
                ConversationId = stableConversation.ConversationId,
                ConversationDisplayName = stableConversation.DisplayName
            };

            stableMessages.Add(stableMessage);

            await UpsertMessageAliasAsync(
                connection,
                transaction,
                deviceId,
                ProjectionMessageAliasKind,
                message.MessageKey,
                messageId,
                cancellationToken);
            if (!string.IsNullOrWhiteSpace(message.Message.Handle))
            {
                await UpsertMessageAliasAsync(
                    connection,
                    transaction,
                    deviceId,
                    MapHandleAliasKind,
                    message.Message.Handle!,
                    messageId,
                    cancellationToken);
            }

            await UpsertMessageEntityAsync(
                connection,
                transaction,
                deviceId,
                messageId,
                stableConversation.ConversationId,
                stableMessage,
                cancellationToken);
        }

        var projectedMessages = stableMessages
            .GroupBy(message => message.MessageKey, StringComparer.OrdinalIgnoreCase)
            .Select(group => MergeProjectedMessage(group))
            .ToArray();
        var projectedConversations = stableConversationMap.Values
            .GroupBy(conversation => conversation.ConversationId, StringComparer.OrdinalIgnoreCase)
            .Select(group => MergeProjectedConversation(group))
            .ToArray();

        await ExecuteNonQueryAsync(
            connection,
            transaction,
            "DELETE FROM messages WHERE device_id = $deviceId;",
            [("$deviceId", deviceId)],
            cancellationToken);
        await ExecuteNonQueryAsync(
            connection,
            transaction,
            "DELETE FROM conversations WHERE device_id = $deviceId;",
            [("$deviceId", deviceId)],
            cancellationToken);

        foreach (var message in projectedMessages)
        {
            await ExecuteNonQueryAsync(
                connection,
                transaction,
                """
                INSERT INTO messages (
                    device_id,
                    message_key,
                    conversation_id,
                    folder,
                    sort_utc,
                    sort_ticks,
                    handle,
                    conversation_display_name,
                    is_group,
                    preview,
                    json,
                    updated_utc
                )
                VALUES (
                    $deviceId,
                    $messageKey,
                    $conversationId,
                    $folder,
                    $sortUtc,
                    $sortTicks,
                    $handle,
                    $conversationDisplayName,
                    $isGroup,
                    $preview,
                    $json,
                    $updatedUtc
                );
                """,
                [
                    ("$deviceId", deviceId),
                    ("$messageKey", message.MessageKey),
                    ("$conversationId", message.ConversationId),
                    ("$folder", message.Message.Folder),
                    ("$sortUtc", (object?)message.SortTimestampUtc?.ToString("O") ?? DBNull.Value),
                    ("$sortTicks", message.SortTimestampUtc?.UtcTicks ?? 0L),
                    ("$handle", (object?)message.Message.Handle ?? DBNull.Value),
                    ("$conversationDisplayName", message.ConversationDisplayName),
                    ("$isGroup", message.IsGroup ? 1 : 0),
                    ("$preview", (object?)BuildPreview(message.Message) ?? DBNull.Value),
                    ("$json", JsonSerializer.Serialize(message, SerializerOptions)),
                    ("$updatedUtc", DateTimeOffset.UtcNow.ToString("O"))
                ],
                cancellationToken);
        }

        foreach (var conversation in projectedConversations)
        {
            await ExecuteNonQueryAsync(
                connection,
                transaction,
                """
                INSERT INTO conversations (
                    device_id,
                    conversation_id,
                    display_name,
                    is_group,
                    last_message_utc,
                    last_message_ticks,
                    unread_count,
                    message_count,
                    last_preview,
                    participants_json,
                    source_folders_json,
                    json,
                    updated_utc
                )
                VALUES (
                    $deviceId,
                    $conversationId,
                    $displayName,
                    $isGroup,
                    $lastMessageUtc,
                    $lastMessageTicks,
                    $unreadCount,
                    $messageCount,
                    $lastPreview,
                    $participantsJson,
                    $sourceFoldersJson,
                    $json,
                    $updatedUtc
                );
                """,
                [
                    ("$deviceId", deviceId),
                    ("$conversationId", conversation.ConversationId),
                    ("$displayName", conversation.DisplayName),
                    ("$isGroup", conversation.IsGroup ? 1 : 0),
                    ("$lastMessageUtc", (object?)conversation.LastMessageUtc?.ToString("O") ?? DBNull.Value),
                    ("$lastMessageTicks", conversation.LastMessageUtc?.UtcTicks ?? 0L),
                    ("$unreadCount", conversation.UnreadCount),
                    ("$messageCount", conversation.MessageCount),
                    ("$lastPreview", (object?)conversation.LastPreview ?? DBNull.Value),
                    ("$participantsJson", JsonSerializer.Serialize(conversation.Participants, SerializerOptions)),
                    ("$sourceFoldersJson", JsonSerializer.Serialize(conversation.SourceFolders, SerializerOptions)),
                    ("$json", JsonSerializer.Serialize(conversation, SerializerOptions)),
                    ("$updatedUtc", DateTimeOffset.UtcNow.ToString("O"))
                ],
                cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    private static SynthesizedMessageRecord MergeProjectedMessage(
        IEnumerable<SynthesizedMessageRecord> candidates)
    {
        var ordered = candidates
            .OrderByDescending(CalculateProjectedMessageScore)
            .ThenByDescending(message => message.SortTimestampUtc ?? DateTimeOffset.MinValue)
            .ToArray();
        var preferred = ordered[0];
        var mergedParticipants = ordered
            .SelectMany(message => message.Participants)
            .GroupBy(participant => participant.Key, StringComparer.OrdinalIgnoreCase)
            .Select(group => group
                .OrderByDescending(participant => participant.Phones.Count + participant.Emails.Count)
                .ThenByDescending(participant => participant.DisplayName?.Length ?? 0)
                .First())
            .OrderBy(participant => participant.DisplayName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(participant => participant.Key, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var previewSource = ordered
            .Select(item => item.Message)
            .FirstOrDefault(message => !string.IsNullOrWhiteSpace(BuildPreview(message)));
        var isGroup = ordered.Any(message => message.IsGroup)
            || mergedParticipants.Count(participant => !participant.IsSelf) > 1;
        var displayName = isGroup
            ? ordered
                .Where(message => message.IsGroup)
                .Select(message => message.ConversationDisplayName)
                .FirstOrDefault(value => !string.IsNullOrWhiteSpace(value))
            : null;

        return preferred with
        {
            ConversationDisplayName = displayName
                ?? ordered
                    .Select(message => message.ConversationDisplayName)
                    .FirstOrDefault(value => !string.IsNullOrWhiteSpace(value))
                ?? preferred.ConversationDisplayName,
            IsGroup = isGroup,
            Participants = mergedParticipants,
            Message = previewSource is null ? preferred.Message : preferred.Message with
            {
                Subject = string.IsNullOrWhiteSpace(preferred.Message.Subject) ? previewSource.Subject : preferred.Message.Subject,
                Body = string.IsNullOrWhiteSpace(preferred.Message.Body) ? previewSource.Body : preferred.Message.Body
            }
        };
    }

    private static ConversationSnapshot MergeProjectedConversation(
        IEnumerable<ConversationSnapshot> candidates)
    {
        var ordered = candidates
            .OrderByDescending(conversation => conversation.LastMessageUtc ?? DateTimeOffset.MinValue)
            .ThenByDescending(conversation => conversation.MessageCount)
            .ThenByDescending(conversation => conversation.UnreadCount)
            .ToArray();
        var preferred = ordered[0];
        var mergedParticipants = ordered
            .SelectMany(conversation => conversation.Participants)
            .GroupBy(participant => participant.Key, StringComparer.OrdinalIgnoreCase)
            .Select(group => group
                .OrderByDescending(participant => participant.Phones.Count + participant.Emails.Count)
                .ThenByDescending(participant => participant.DisplayName?.Length ?? 0)
                .First())
            .OrderBy(participant => participant.DisplayName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(participant => participant.Key, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var mergedSourceFolders = ordered
            .SelectMany(conversation => conversation.SourceFolders)
            .Where(folder => !string.IsNullOrWhiteSpace(folder))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(folder => folder, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var isGroup = ordered.Any(conversation => conversation.IsGroup)
            || mergedParticipants.Count(participant => !participant.IsSelf) > 1;
        var filteredParticipants = isGroup
            ? FilterSyntheticGroupDescriptorParticipants(mergedParticipants)
            : mergedParticipants;
        var displayName = isGroup
            ? ordered
                .Where(conversation => conversation.IsGroup)
                .Select(conversation => conversation.DisplayName)
                .FirstOrDefault(value => !string.IsNullOrWhiteSpace(value))
            : null;

        return preferred with
        {
            DisplayName = displayName
                ?? preferred.DisplayName,
            IsGroup = isGroup,
            MessageCount = ordered.Max(conversation => conversation.MessageCount),
            UnreadCount = ordered.Max(conversation => conversation.UnreadCount),
            LastPreview = ordered
                .Select(conversation => conversation.LastPreview)
                .FirstOrDefault(value => !string.IsNullOrWhiteSpace(value))
                ?? preferred.LastPreview,
            Participants = filteredParticipants,
            SourceFolders = mergedSourceFolders
        };
    }

    private static IReadOnlyList<ConversationParticipantRecord> FilterSyntheticGroupDescriptorParticipants(
        IReadOnlyList<ConversationParticipantRecord> participants)
    {
        if (participants.Count <= 2)
        {
            return participants;
        }

        var filtered = participants
            .Where(participant => !LooksLikeSyntheticGroupDescriptorParticipant(participant))
            .ToArray();

        return filtered.Length > 0 ? filtered : participants;
    }

    private static bool LooksLikeSyntheticGroupDescriptorParticipant(ConversationParticipantRecord participant)
    {
        if (participant.IsSelf)
        {
            return false;
        }

        if (participant.Key.StartsWith("raw:to you", StringComparison.OrdinalIgnoreCase)
            || participant.Key.StartsWith("name:to you", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return participant.Phones.Any(phone =>
            phone.Contains(',', StringComparison.Ordinal)
            || phone.StartsWith("To ", StringComparison.OrdinalIgnoreCase));
    }

    private static int CalculateProjectedMessageScore(SynthesizedMessageRecord message)
    {
        var score = 0;
        if (!string.IsNullOrWhiteSpace(message.Message.Handle))
        {
            score += 4;
        }

        if (!string.IsNullOrWhiteSpace(message.Message.Body))
        {
            score += 3;
        }

        if (!string.IsNullOrWhiteSpace(message.Message.Subject))
        {
            score += 2;
        }

        score += message.Participants.Count;
        score += string.IsNullOrWhiteSpace(message.ConversationDisplayName) ? 0 : 1;
        return score;
    }

    public async Task<IReadOnlyList<ContactRecord>> GetContactsAsync(
        string deviceId,
        int? limit,
        CancellationToken cancellationToken)
    {
        return await ReadRecordsAsync<ContactRecord>(
            """
            SELECT json
            FROM contacts
            WHERE device_id = $deviceId
            ORDER BY display_name ASC
            """,
            [
                ("$deviceId", deviceId),
                ("$limit", limit)
            ],
            limit,
            cancellationToken);
    }

    public async Task<IReadOnlyList<ContactRecord>> SearchContactsAsync(
        string deviceId,
        string query,
        int limit,
        CancellationToken cancellationToken)
    {
        return await ReadRecordsAsync<ContactRecord>(
            """
            SELECT json
            FROM contacts
            WHERE device_id = $deviceId
              AND search_name LIKE $pattern
            ORDER BY display_name ASC
            """,
            [
                ("$deviceId", deviceId),
                ("$pattern", $"%{query.Trim().ToLowerInvariant()}%"),
                ("$limit", limit)
            ],
            limit,
            cancellationToken);
    }

    public async Task<IReadOnlyList<SynthesizedMessageRecord>> GetStoredMessagesAsync(
        string deviceId,
        string? folder,
        string? conversationId,
        int? limit,
        CancellationToken cancellationToken)
    {
        var conditions = new List<string> { "device_id = $deviceId" };
        var parameters = new List<(string Name, object? Value)>
        {
            ("$deviceId", deviceId),
            ("$limit", limit)
        };

        if (!string.IsNullOrWhiteSpace(folder))
        {
            conditions.Add("folder = $folder");
            parameters.Add(("$folder", folder));
        }

        if (!string.IsNullOrWhiteSpace(conversationId))
        {
            conditions.Add("conversation_id = $conversationId");
            parameters.Add(("$conversationId", conversationId));
        }

        var sql =
            $"""
            SELECT json
            FROM messages
            WHERE {string.Join(" AND ", conditions)}
            ORDER BY sort_ticks DESC, message_key DESC
            """;

        return await ReadRecordsAsync<SynthesizedMessageRecord>(sql, parameters, limit, cancellationToken);
    }

    public async Task<IReadOnlyList<MessageRecord>> GetCompletedSendIntentMessagesAsync(
        string deviceId,
        int limit,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT
                recipient_raw,
                recipient_display_name,
                body,
                message_handle,
                created_utc,
                completed_utc
            FROM send_intents
            WHERE device_id = $deviceId
              AND result_success = 1
            ORDER BY COALESCE(completed_utc, created_utc) DESC
            LIMIT $limit;
            """;
        command.Parameters.AddWithValue("$deviceId", deviceId);
        command.Parameters.AddWithValue("$limit", Math.Max(1, limit));

        var messages = new List<MessageRecord>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var recipientRaw = reader.GetString(0);
            var recipientDisplayName = reader.IsDBNull(1) ? recipientRaw : reader.GetString(1);
            var body = reader.GetString(2);
            var handle = reader.IsDBNull(3) ? null : reader.GetString(3);
            var createdUtc = DateTimeOffset.Parse(reader.GetString(4), CultureInfo.InvariantCulture);
            var completedUtc = reader.IsDBNull(5)
                ? createdUtc
                : DateTimeOffset.Parse(reader.GetString(5), CultureInfo.InvariantCulture);

            var recipientPhones = TryExtractEmail(recipientRaw, out var recipientEmail)
                ? Array.Empty<string>()
                : [PhoneNumberNormalizer.Normalize(recipientRaw) ?? StripAddressPrefix(recipientRaw)];
            var recipientEmails = !string.IsNullOrWhiteSpace(recipientEmail)
                ? new[] { recipientEmail }
                : Array.Empty<string>();

            messages.Add(
                new MessageRecord(
                    "sent",
                    handle,
                    "SMS_GSM",
                    body,
                    completedUtc.ToLocalTime().ToString("yyyyMMdd'T'HHmmss", CultureInfo.InvariantCulture),
                    "Me",
                    "name:Me",
                    recipientRaw,
                    (uint?)body.Length,
                    0,
                    null,
                    true,
                    true,
                    false,
                    body,
                    "SMSGSM",
                    "Read",
                    [new MessageParticipantRecord("Me", Array.Empty<string>(), Array.Empty<string>())],
                    [new MessageParticipantRecord(recipientDisplayName, recipientPhones, recipientEmails)]));
        }

        return messages;
    }

    public async Task<IReadOnlyList<ConversationSnapshot>> GetConversationsAsync(
        string deviceId,
        int? limit,
        CancellationToken cancellationToken)
    {
        return await ReadRecordsAsync<ConversationSnapshot>(
            """
            SELECT json
            FROM conversations
            WHERE device_id = $deviceId
            ORDER BY last_message_ticks DESC, conversation_id DESC
            """,
            [
                ("$deviceId", deviceId),
                ("$limit", limit)
            ],
            limit,
            cancellationToken);
    }

    public async Task UpsertNotificationAsync(
        string deviceId,
        NotificationRecord notification,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await ExecuteNonQueryAsync(
            connection,
            null,
            """
            INSERT INTO notifications (
                device_id,
                notification_uid,
                active,
                received_utc,
                updated_utc,
                removed_utc,
                app_identifier,
                category,
                title,
                subtitle,
                message,
                positive_action_label,
                negative_action_label,
                json
            )
            VALUES (
                $deviceId,
                $notificationUid,
                1,
                $receivedUtc,
                $updatedUtc,
                NULL,
                $appIdentifier,
                $category,
                $title,
                $subtitle,
                $message,
                $positiveActionLabel,
                $negativeActionLabel,
                $json
            )
            ON CONFLICT(device_id, notification_uid) DO UPDATE SET
                active = 1,
                received_utc = excluded.received_utc,
                updated_utc = excluded.updated_utc,
                removed_utc = NULL,
                app_identifier = excluded.app_identifier,
                category = excluded.category,
                title = excluded.title,
                subtitle = excluded.subtitle,
                message = excluded.message,
                positive_action_label = excluded.positive_action_label,
                negative_action_label = excluded.negative_action_label,
                json = excluded.json;
            """,
            [
                ("$deviceId", deviceId),
                ("$notificationUid", (long)notification.NotificationUid),
                ("$receivedUtc", notification.ReceivedAtUtc.ToString("O")),
                ("$updatedUtc", DateTimeOffset.UtcNow.ToString("O")),
                ("$appIdentifier", (object?)notification.AppIdentifier ?? DBNull.Value),
                ("$category", notification.Category.ToString()),
                ("$title", (object?)notification.Title ?? DBNull.Value),
                ("$subtitle", (object?)notification.Subtitle ?? DBNull.Value),
                ("$message", (object?)notification.Message ?? DBNull.Value),
                ("$positiveActionLabel", (object?)notification.PositiveActionLabel ?? DBNull.Value),
                ("$negativeActionLabel", (object?)notification.NegativeActionLabel ?? DBNull.Value),
                ("$json", JsonSerializer.Serialize(notification, SerializerOptions))
            ],
            cancellationToken);
    }

    public async Task MarkNotificationRemovedAsync(
        string deviceId,
        uint notificationUid,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await ExecuteNonQueryAsync(
            connection,
            null,
            """
            UPDATE notifications
            SET active = 0,
                updated_utc = $updatedUtc,
                removed_utc = $removedUtc
            WHERE device_id = $deviceId
              AND notification_uid = $notificationUid;
            """,
            [
                ("$deviceId", deviceId),
                ("$notificationUid", (long)notificationUid),
                ("$updatedUtc", DateTimeOffset.UtcNow.ToString("O")),
                ("$removedUtc", DateTimeOffset.UtcNow.ToString("O"))
            ],
            cancellationToken);
    }

    public async Task<IReadOnlyList<StoredNotificationRecord>> GetNotificationsAsync(
        string deviceId,
        bool activeOnly,
        string? appIdentifier,
        int? limit,
        CancellationToken cancellationToken)
    {
        var conditions = new List<string> { "device_id = $deviceId" };
        var parameters = new List<(string Name, object? Value)>
        {
            ("$deviceId", deviceId),
            ("$limit", limit)
        };

        if (activeOnly)
        {
            conditions.Add("active = 1");
        }

        if (!string.IsNullOrWhiteSpace(appIdentifier))
        {
            conditions.Add("app_identifier = $appIdentifier");
            parameters.Add(("$appIdentifier", appIdentifier.Trim()));
        }

        var sql =
            $"""
            SELECT device_id, active, updated_utc, removed_utc, json
            FROM notifications
            WHERE {string.Join(" AND ", conditions)}
            ORDER BY updated_utc DESC, notification_uid DESC
            """;

        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = sql + (limit is > 0 ? " LIMIT $limit;" : ";");

        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var results = new List<StoredNotificationRecord>();
        while (await reader.ReadAsync(cancellationToken))
        {
            var notification = JsonSerializer.Deserialize<NotificationRecord>(reader.GetString(4), SerializerOptions);
            if (notification is null)
            {
                continue;
            }

            var updatedUtc = ParseDateTimeOffset(reader.GetString(2));
            var removedUtc = reader.IsDBNull(3) ? (DateTimeOffset?)null : ParseDateTimeOffset(reader.GetString(3));
            results.Add(
                new StoredNotificationRecord(
                    reader.GetString(0),
                    reader.GetInt64(1) == 1,
                    updatedUtc,
                    removedUtc,
                    notification));
        }

        return results;
    }

    public async Task<int> CountActiveNotificationsAsync(string deviceId, CancellationToken cancellationToken)
    {
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT COUNT(*)
            FROM notifications
            WHERE device_id = $deviceId
              AND active = 1;
            """;
        command.Parameters.AddWithValue("$deviceId", deviceId);
        var scalar = await command.ExecuteScalarAsync(cancellationToken);
        return scalar is long count ? checked((int)count) : 0;
    }

    public async Task<ResolvedRecipientRecord?> ResolveRecipientAsync(
        string deviceId,
        string? contactId,
        string? contactName,
        string? preferredNumber,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(contactId) && string.IsNullOrWhiteSpace(contactName))
        {
            return null;
        }

        var contacts = await GetContactsAsync(deviceId, null, cancellationToken);
        var candidates = contacts.AsEnumerable();

        if (!string.IsNullOrWhiteSpace(contactId))
        {
            candidates = candidates.Where(
                contact => string.Equals(contact.UniqueIdentifier, contactId, StringComparison.OrdinalIgnoreCase));
        }
        else if (!string.IsNullOrWhiteSpace(contactName))
        {
            var trimmed = contactName.Trim();
            var exact = contacts.Where(
                contact => string.Equals(contact.DisplayName, trimmed, StringComparison.OrdinalIgnoreCase)).ToArray();
            candidates = exact.Length > 0
                ? exact
                : contacts.Where(
                    contact => contact.DisplayName.Contains(trimmed, StringComparison.OrdinalIgnoreCase));
        }

        foreach (var contact in candidates)
        {
            var recipient = ResolvePhone(contact, preferredNumber);
            if (!string.IsNullOrWhiteSpace(recipient))
            {
                return new ResolvedRecipientRecord(contact, recipient);
            }
        }

        return null;
    }

    public async Task<ResolvedConversationRecipientRecord?> ResolveConversationRecipientAsync(
        string deviceId,
        string conversationId,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(conversationId))
        {
            return null;
        }

        var resolvedConversationId = await ResolveConversationIdAsync(deviceId, conversationId, cancellationToken)
            ?? conversationId;
        var conversations = await GetConversationsAsync(deviceId, null, cancellationToken);
        var conversation = conversations.FirstOrDefault(
            item => string.Equals(item.ConversationId, resolvedConversationId, StringComparison.OrdinalIgnoreCase));
        if (conversation is null || conversation.IsGroup)
        {
            return null;
        }

        var participant = conversation.Participants.FirstOrDefault(
            item => !item.IsSelf && item.Phones.Any(phone => !string.IsNullOrWhiteSpace(phone)));
        if (participant is null)
        {
            return null;
        }

        var recipient = participant.Phones.FirstOrDefault(phone => !string.IsNullOrWhiteSpace(phone));
        return string.IsNullOrWhiteSpace(recipient)
            ? null
            : new ResolvedConversationRecipientRecord(conversation, participant, recipient);
    }

    private async Task InsertMapObservationAsync(
        SqliteConnection connection,
        System.Data.Common.DbTransaction transaction,
        string deviceId,
        string observationId,
        string? sessionId,
        DateTimeOffset observedUtc,
        string sourceKind,
        string? eventType,
        string? folder,
        string? oldFolder,
        string? handle,
        MessageRecord? message,
        object rawPayload,
        CancellationToken cancellationToken)
    {
        var sortUtc = message is null ? null : ParseMapSortUtc(message.Datetime);
        await ExecuteNonQueryAsync(
            connection,
            transaction,
            """
            INSERT INTO map_observations (
                device_id,
                observation_id,
                session_id,
                observed_utc,
                source_kind,
                event_type,
                folder,
                old_folder,
                handle,
                sort_utc,
                sender_addressing,
                recipient_addressing,
                body_hash,
                preview,
                raw_json
            )
            VALUES (
                $deviceId,
                $observationId,
                $sessionId,
                $observedUtc,
                $sourceKind,
                $eventType,
                $folder,
                $oldFolder,
                $handle,
                $sortUtc,
                $senderAddressing,
                $recipientAddressing,
                $bodyHash,
                $preview,
                $rawJson
            );
            """,
            [
                ("$deviceId", deviceId),
                ("$observationId", observationId),
                ("$sessionId", (object?)sessionId ?? DBNull.Value),
                ("$observedUtc", observedUtc.ToString("O")),
                ("$sourceKind", sourceKind),
                ("$eventType", (object?)eventType ?? DBNull.Value),
                ("$folder", (object?)folder ?? DBNull.Value),
                ("$oldFolder", (object?)oldFolder ?? DBNull.Value),
                ("$handle", (object?)handle ?? DBNull.Value),
                ("$sortUtc", (object?)sortUtc?.ToString("O") ?? DBNull.Value),
                ("$senderAddressing", (object?)message?.SenderAddressing ?? DBNull.Value),
                ("$recipientAddressing", (object?)message?.RecipientAddressing ?? DBNull.Value),
                ("$bodyHash", (object?)ComputeHash(message?.Body ?? message?.Subject) ?? DBNull.Value),
                ("$preview", message is null ? DBNull.Value : (object?)BuildPreview(message) ?? DBNull.Value),
                ("$rawJson", JsonSerializer.Serialize(rawPayload, SerializerOptions))
            ],
            cancellationToken);
    }

    private async Task<string> ResolveOrCreateThreadIdAsync(
        SqliteConnection connection,
        System.Data.Common.DbTransaction transaction,
        string deviceId,
        string aliasKey,
        IReadOnlyList<PendingStableMessage> conversationMessages,
        string? continuityThreadId,
        CancellationToken cancellationToken)
    {
        var existing = await ResolveAliasAsync(
            connection,
            transaction,
            "thread_aliases",
            "thread_id",
            deviceId,
            ProjectionConversationAliasKind,
            aliasKey,
            cancellationToken);
        if (!string.IsNullOrWhiteSpace(existing))
        {
            return existing!;
        }

        if (!string.IsNullOrWhiteSpace(continuityThreadId))
        {
            return continuityThreadId!;
        }

        return CreateStableId("th");
    }

    private async Task<IReadOnlyDictionary<string, string>> DetermineContinuityOwnersAsync(
        SqliteConnection connection,
        System.Data.Common.DbTransaction transaction,
        string deviceId,
        IReadOnlyList<ConversationSnapshot> conversations,
        IReadOnlyDictionary<string, PendingStableMessage[]> pendingMessagesByConversationId,
        CancellationToken cancellationToken)
    {
        var claimsByThreadId = new Dictionary<string, List<ContinuityClaim>>(StringComparer.OrdinalIgnoreCase);

        foreach (var conversation in conversations)
        {
            if (!pendingMessagesByConversationId.TryGetValue(conversation.ConversationId, out var conversationMessages))
            {
                continue;
            }

            var continuityThreadId = SelectContinuityThreadId(conversationMessages);
            if (string.IsNullOrWhiteSpace(continuityThreadId))
            {
                continue;
            }

            if (!claimsByThreadId.TryGetValue(continuityThreadId!, out var claims))
            {
                claims = [];
                claimsByThreadId[continuityThreadId!] = claims;
            }

            claims.Add(
                new ContinuityClaim(
                    conversation,
                    continuityThreadId!,
                    conversationMessages.Count(message => string.Equals(message.ExistingThreadId, continuityThreadId, StringComparison.OrdinalIgnoreCase)),
                    conversationMessages.Length));
        }

        var owners = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var pair in claimsByThreadId)
        {
            if (pair.Value.Count == 1)
            {
                owners[pair.Value[0].Conversation.ConversationId] = pair.Key;
                continue;
            }

            var existingProjection = await GetThreadProjectionAsync(
                connection,
                transaction,
                deviceId,
                pair.Key,
                cancellationToken);
            var winner = pair.Value
                .OrderByDescending(claim => ScoreContinuityClaim(claim, existingProjection?.Projection))
                .ThenByDescending(claim => claim.MessagesFromExistingThread)
                .ThenByDescending(claim => claim.TotalMessages)
                .ThenBy(claim => claim.Conversation.ConversationId, StringComparer.OrdinalIgnoreCase)
                .First();
            owners[winner.Conversation.ConversationId] = pair.Key;
        }

        return owners;
    }

    private static string? SelectContinuityThreadId(IReadOnlyList<PendingStableMessage> conversationMessages)
    {
        if (conversationMessages.Count < 2)
        {
            return null;
        }

        var ranked = conversationMessages
            .Where(message => !string.IsNullOrWhiteSpace(message.ExistingThreadId))
            .GroupBy(message => message.ExistingThreadId!, StringComparer.OrdinalIgnoreCase)
            .Select(
                group => new
                {
                    ThreadId = group.Key,
                    Count = group.Count()
                })
            .OrderByDescending(item => item.Count)
            .ThenBy(item => item.ThreadId, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (ranked.Length == 0)
        {
            return null;
        }

        if (ranked.Length == 1)
        {
            return ranked[0].ThreadId;
        }

        return null;
    }

    private static int ScoreContinuityClaim(
        ContinuityClaim claim,
        ConversationSnapshot? existingProjection)
    {
        var score = claim.MessagesFromExistingThread * 10 + claim.TotalMessages;
        if (existingProjection is null)
        {
            return score;
        }

        if (claim.Conversation.IsGroup == existingProjection.IsGroup)
        {
            score += 100;
        }

        if (string.Equals(claim.Conversation.DisplayName, existingProjection.DisplayName, StringComparison.OrdinalIgnoreCase))
        {
            score += 10;
        }

        var existingParticipantKeys = existingProjection.Participants
            .Select(participant => participant.Key)
            .Where(key => !string.IsNullOrWhiteSpace(key))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        score += claim.Conversation.Participants.Count(
            participant => !string.IsNullOrWhiteSpace(participant.Key)
                && existingParticipantKeys.Contains(participant.Key)) * 5;

        return score;
    }

    private async Task<(int Revision, ConversationSnapshot Projection)?> GetThreadProjectionAsync(
        SqliteConnection connection,
        System.Data.Common.DbTransaction transaction,
        string deviceId,
        string threadId,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction as SqliteTransaction;
        command.CommandText =
            """
            SELECT revision, json
            FROM thread_entities
            WHERE device_id = $deviceId
              AND thread_id = $threadId
            LIMIT 1;
            """;
        command.Parameters.AddWithValue("$deviceId", deviceId);
        command.Parameters.AddWithValue("$threadId", threadId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        var projection = JsonSerializer.Deserialize<ConversationSnapshot>(reader.GetString(1), SerializerOptions);
        return projection is null ? null : (reader.GetInt32(0), projection);
    }

    private static async Task<string?> GetExistingThreadIdForMessageAsync(
        SqliteConnection connection,
        System.Data.Common.DbTransaction transaction,
        string deviceId,
        string messageId,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction as SqliteTransaction;
        command.CommandText =
            """
            SELECT thread_id
            FROM message_entities
            WHERE device_id = $deviceId
              AND message_id = $messageId
            LIMIT 1;
            """;
        command.Parameters.AddWithValue("$deviceId", deviceId);
        command.Parameters.AddWithValue("$messageId", messageId);

        var scalar = await command.ExecuteScalarAsync(cancellationToken);
        return scalar?.ToString();
    }

    private async Task UpsertThreadAliasAsync(
        SqliteConnection connection,
        System.Data.Common.DbTransaction transaction,
        string deviceId,
        string aliasKind,
        string aliasKey,
        string threadId,
        CancellationToken cancellationToken)
    {
        await ExecuteNonQueryAsync(
            connection,
            transaction,
            """
            INSERT INTO thread_aliases (
                device_id,
                alias_kind,
                alias_key,
                thread_id,
                created_utc
            )
            VALUES (
                $deviceId,
                $aliasKind,
                $aliasKey,
                $threadId,
                $createdUtc
            )
            ON CONFLICT(device_id, alias_kind, alias_key) DO UPDATE SET
                thread_id = excluded.thread_id;
            """,
            [
                ("$deviceId", deviceId),
                ("$aliasKind", aliasKind),
                ("$aliasKey", aliasKey),
                ("$threadId", threadId),
                ("$createdUtc", DateTimeOffset.UtcNow.ToString("O"))
            ],
            cancellationToken);
    }

    private async Task UpsertThreadEntityAsync(
        SqliteConnection connection,
        System.Data.Common.DbTransaction transaction,
        string deviceId,
        string threadId,
        int revision,
        ConversationSnapshot conversation,
        CancellationToken cancellationToken)
    {
        await ExecuteNonQueryAsync(
            connection,
            transaction,
            """
            INSERT INTO thread_entities (
                device_id,
                thread_id,
                revision,
                state,
                display_name,
                is_group,
                last_message_utc,
                last_message_ticks,
                unread_count,
                message_count,
                source_folders_json,
                json,
                updated_utc
            )
            VALUES (
                $deviceId,
                $threadId,
                $revision,
                'active',
                $displayName,
                $isGroup,
                $lastMessageUtc,
                $lastMessageTicks,
                $unreadCount,
                $messageCount,
                $sourceFoldersJson,
                $json,
                $updatedUtc
            )
            ON CONFLICT(device_id, thread_id) DO UPDATE SET
                revision = excluded.revision,
                state = excluded.state,
                display_name = excluded.display_name,
                is_group = excluded.is_group,
                last_message_utc = excluded.last_message_utc,
                last_message_ticks = excluded.last_message_ticks,
                unread_count = excluded.unread_count,
                message_count = excluded.message_count,
                source_folders_json = excluded.source_folders_json,
                json = excluded.json,
                updated_utc = excluded.updated_utc;
            """,
            [
                ("$deviceId", deviceId),
                ("$threadId", threadId),
                ("$revision", revision),
                ("$displayName", conversation.DisplayName),
                ("$isGroup", conversation.IsGroup ? 1 : 0),
                ("$lastMessageUtc", (object?)conversation.LastMessageUtc?.ToString("O") ?? DBNull.Value),
                ("$lastMessageTicks", conversation.LastMessageUtc?.UtcTicks ?? 0L),
                ("$unreadCount", conversation.UnreadCount),
                ("$messageCount", conversation.MessageCount),
                ("$sourceFoldersJson", JsonSerializer.Serialize(conversation.SourceFolders, SerializerOptions)),
                ("$json", JsonSerializer.Serialize(conversation, SerializerOptions)),
                ("$updatedUtc", DateTimeOffset.UtcNow.ToString("O"))
            ],
            cancellationToken);
    }

    private async Task<string> ResolveOrCreateMessageIdAsync(
        SqliteConnection connection,
        System.Data.Common.DbTransaction transaction,
        string deviceId,
        string projectionMessageKey,
        string? handle,
        CancellationToken cancellationToken)
    {
        if (!string.IsNullOrWhiteSpace(handle))
        {
            var byHandle = await ResolveAliasAsync(
                connection,
                transaction,
                "message_aliases",
                "message_id",
                deviceId,
                MapHandleAliasKind,
                handle!,
                cancellationToken);
            if (!string.IsNullOrWhiteSpace(byHandle))
            {
                return byHandle!;
            }
        }

        var byProjection = await ResolveAliasAsync(
            connection,
            transaction,
            "message_aliases",
            "message_id",
            deviceId,
            ProjectionMessageAliasKind,
            projectionMessageKey,
            cancellationToken);
        return byProjection ?? CreateStableId("msg");
    }

    private async Task UpsertMessageAliasAsync(
        SqliteConnection connection,
        System.Data.Common.DbTransaction transaction,
        string deviceId,
        string aliasKind,
        string aliasKey,
        string messageId,
        CancellationToken cancellationToken)
    {
        await ExecuteNonQueryAsync(
            connection,
            transaction,
            """
            INSERT INTO message_aliases (
                device_id,
                alias_kind,
                alias_key,
                message_id,
                created_utc
            )
            VALUES (
                $deviceId,
                $aliasKind,
                $aliasKey,
                $messageId,
                $createdUtc
            )
            ON CONFLICT(device_id, alias_kind, alias_key) DO UPDATE SET
                message_id = excluded.message_id;
            """,
            [
                ("$deviceId", deviceId),
                ("$aliasKind", aliasKind),
                ("$aliasKey", aliasKey),
                ("$messageId", messageId),
                ("$createdUtc", DateTimeOffset.UtcNow.ToString("O"))
            ],
            cancellationToken);
    }

    private async Task UpsertMessageEntityAsync(
        SqliteConnection connection,
        System.Data.Common.DbTransaction transaction,
        string deviceId,
        string messageId,
        string threadId,
        SynthesizedMessageRecord message,
        CancellationToken cancellationToken)
    {
        await ExecuteNonQueryAsync(
            connection,
            transaction,
            """
            INSERT INTO message_entities (
                device_id,
                message_id,
                thread_id,
                handle,
                sort_utc,
                sort_ticks,
                folder,
                visibility_state,
                assignment_state,
                json,
                updated_utc
            )
            VALUES (
                $deviceId,
                $messageId,
                $threadId,
                $handle,
                $sortUtc,
                $sortTicks,
                $folder,
                'durable',
                'active',
                $json,
                $updatedUtc
            )
            ON CONFLICT(device_id, message_id) DO UPDATE SET
                thread_id = excluded.thread_id,
                handle = excluded.handle,
                sort_utc = excluded.sort_utc,
                sort_ticks = excluded.sort_ticks,
                folder = excluded.folder,
                visibility_state = excluded.visibility_state,
                assignment_state = excluded.assignment_state,
                json = excluded.json,
                updated_utc = excluded.updated_utc;
            """,
            [
                ("$deviceId", deviceId),
                ("$messageId", messageId),
                ("$threadId", threadId),
                ("$handle", (object?)message.Message.Handle ?? DBNull.Value),
                ("$sortUtc", (object?)message.SortTimestampUtc?.ToString("O") ?? DBNull.Value),
                ("$sortTicks", message.SortTimestampUtc?.UtcTicks ?? 0L),
                ("$folder", message.Message.Folder),
                ("$json", JsonSerializer.Serialize(message, SerializerOptions)),
                ("$updatedUtc", DateTimeOffset.UtcNow.ToString("O"))
            ],
            cancellationToken);
    }

    private static async Task<string?> ResolveAliasAsync(
        SqliteConnection connection,
        System.Data.Common.DbTransaction transaction,
        string tableName,
        string targetColumn,
        string deviceId,
        string aliasKind,
        string aliasKey,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction as SqliteTransaction;
        command.CommandText =
            $"""
            SELECT {targetColumn}
            FROM {tableName}
            WHERE device_id = $deviceId
              AND alias_kind = $aliasKind
              AND alias_key = $aliasKey
            LIMIT 1;
            """;
        command.Parameters.AddWithValue("$deviceId", deviceId);
        command.Parameters.AddWithValue("$aliasKind", aliasKind);
        command.Parameters.AddWithValue("$aliasKey", aliasKey);

        var scalar = await command.ExecuteScalarAsync(cancellationToken);
        return scalar?.ToString();
    }

    private static bool AreEquivalent(ConversationSnapshot left, ConversationSnapshot right)
    {
        return JsonSerializer.Serialize(left, SerializerOptions)
            == JsonSerializer.Serialize(right, SerializerOptions);
    }

    private async Task<IReadOnlyList<T>> ReadRecordsAsync<T>(
        string sql,
        IEnumerable<(string Name, object? Value)> parameters,
        int? limit,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = sql + (limit is > 0 ? " LIMIT $limit;" : ";");

        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var results = new List<T>();
        while (await reader.ReadAsync(cancellationToken))
        {
            var json = reader.GetString(0);
            var value = JsonSerializer.Deserialize<T>(json, SerializerOptions);
            if (value is not null)
            {
                results.Add(value);
            }
        }

        return results;
    }

    private static async Task ExecuteNonQueryAsync(
        SqliteConnection connection,
        System.Data.Common.DbTransaction? transaction,
        string sql,
        IEnumerable<(string Name, object? Value)> parameters,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction as SqliteTransaction;
        command.CommandText = sql;

        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        }

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private void EnsureDatabaseProtection()
    {
        if (!encryptDatabaseAtRest
            || databaseProtectionUnavailable
            || !OperatingSystem.IsWindows()
            || !File.Exists(databasePath))
        {
            return;
        }

        try
        {
            if ((getFileAttributes(databasePath) & FileAttributes.Encrypted) != 0)
            {
                return;
            }

            encryptFile(databasePath);
        }
        catch (Exception exception) when (
            exception is IOException
            or UnauthorizedAccessException
            or PlatformNotSupportedException
            or NotSupportedException)
        {
            databaseProtectionUnavailable = true;
            logger?.LogWarning(
                exception,
                "Windows file encryption is not available for {DatabasePath}. Continuing without EFS protection. Set ADIT_ENCRYPT_DB_AT_REST=false to disable the attempt.",
                databasePath);
        }
    }

    private static string BuildContactKey(ContactRecord contact)
    {
        return !string.IsNullOrWhiteSpace(contact.UniqueIdentifier)
            ? contact.UniqueIdentifier!
            : $"name:{contact.DisplayName.ToLowerInvariant()}";
    }

    private static string? ResolvePhone(ContactRecord contact, string? preferredNumber)
    {
        if (!string.IsNullOrWhiteSpace(preferredNumber))
        {
            foreach (var phone in contact.Phones)
            {
                if (string.Equals(phone.Normalized, preferredNumber, StringComparison.OrdinalIgnoreCase)
                    || string.Equals(phone.Raw, preferredNumber, StringComparison.OrdinalIgnoreCase))
                {
                    return phone.Normalized ?? phone.Raw;
                }
            }
        }

        return contact.Phones
            .Select(phone => phone.Normalized ?? phone.Raw)
            .FirstOrDefault(number => !string.IsNullOrWhiteSpace(number));
    }

    private static string? BuildPreview(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return null;
        }

        var trimmed = text.Trim();
        return trimmed.Length <= 160 ? trimmed : $"{trimmed[..160]}...";
    }

    private static string? BuildPreview(MessageRecord message)
    {
        var text = string.IsNullOrWhiteSpace(message.Body) ? message.Subject : message.Body;
        return BuildPreview(text);
    }

    private static string BuildRecipientPointKey(string recipient)
    {
        if (TryExtractEmail(recipient, out var email))
        {
            return $"email:{email}";
        }

        var normalized = PhoneNumberNormalizer.Normalize(recipient);
        if (!string.IsNullOrWhiteSpace(normalized))
        {
            return $"phone:{normalized}";
        }

        return $"raw:{StripAddressPrefix(recipient).Trim().ToLowerInvariant()}";
    }

    private static string? ComputeHash(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(value.Trim()));
        return Convert.ToHexString(bytes[..8]).ToLowerInvariant();
    }

    private static string CreateStableId(string prefix)
    {
        return $"{prefix}_{Guid.NewGuid():N}";
    }

    private sealed record PendingStableMessage(
        SynthesizedMessageRecord Projection,
        string MessageId,
        string? ExistingThreadId);

    private sealed record ContinuityClaim(
        ConversationSnapshot Conversation,
        string ExistingThreadId,
        int MessagesFromExistingThread,
        int TotalMessages);

    private static DateTimeOffset? ParseMapSortUtc(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        return DateTimeOffset.TryParseExact(
            value,
            "yyyyMMdd'T'HHmmss",
            null,
            System.Globalization.DateTimeStyles.AssumeLocal,
            out var parsed)
            ? parsed.ToUniversalTime()
            : null;
    }

    private static DateTimeOffset ParseDateTimeOffset(string value)
    {
        return DateTimeOffset.TryParse(value, out var parsed) ? parsed : DateTimeOffset.MinValue;
    }

    private static bool TryExtractEmail(string raw, out string email)
    {
        var stripped = StripAddressPrefix(raw);
        if (!stripped.Contains('@', StringComparison.Ordinal))
        {
            email = string.Empty;
            return false;
        }

        email = stripped.Trim().ToLowerInvariant();
        return email.Length > 0;
    }

    private static string StripAddressPrefix(string raw)
    {
        var trimmed = raw.Trim();
        foreach (var prefix in SendIntentAddressPrefixes)
        {
            if (trimmed.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            {
                return trimmed[prefix.Length..].Trim();
            }
        }

        return trimmed;
    }

    private static readonly string[] SendIntentAddressPrefixes =
    [
        "e:",
        "email:",
        "mailto:",
        "name:",
        "tel:",
        "sms:",
        "phone:"
    ];
}
