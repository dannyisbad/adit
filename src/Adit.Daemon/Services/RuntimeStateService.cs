using Adit.Core.Models;

namespace Adit.Daemon.Services;

public sealed class RuntimeStateService
{
    private readonly object gate = new();
    private DaemonRuntimeSnapshot snapshot = new(
        Phase: "starting",
        LastReason: "startup",
        LastAttemptUtc: null,
        LastSuccessfulSyncUtc: null,
        LastContactsRefreshUtc: null,
        LastError: null,
        ConsecutiveFailures: 0,
        ContactCount: 0,
        MessageCount: 0,
        NotificationCount: 0,
        ConversationCount: 0,
        AutoEvictPhoneLink: true,
        NotificationsMode: NotificationMode.Auto,
        NotificationsEnabled: true,
        Target: null,
        MapSession: null,
        AncsSession: null);

    public DaemonRuntimeSnapshot GetSnapshot()
    {
        lock (gate)
        {
            return snapshot;
        }
    }

    public DaemonRuntimeSnapshot Update(Func<DaemonRuntimeSnapshot, DaemonRuntimeSnapshot> updater)
    {
        lock (gate)
        {
            snapshot = updater(snapshot);
            return snapshot;
        }
    }

    public DaemonRuntimeSnapshot MarkSyncStarting(string reason, bool autoEvictPhoneLink, BluetoothEndpointRecord? target = null)
    {
        return Update(
            current => current with
            {
                Phase = "syncing",
                LastReason = reason,
                LastAttemptUtc = DateTimeOffset.UtcNow,
                AutoEvictPhoneLink = autoEvictPhoneLink,
                Target = target ?? current.Target,
                LastError = null
            });
    }

    public DaemonRuntimeSnapshot MarkNoDevice(string reason, bool autoEvictPhoneLink)
    {
        return Update(
            current => current with
            {
                Phase = "no_device",
                LastReason = reason,
                LastAttemptUtc = DateTimeOffset.UtcNow,
                AutoEvictPhoneLink = autoEvictPhoneLink,
                Target = null
            });
    }

    public DaemonRuntimeSnapshot MarkReady(
        string reason,
        bool autoEvictPhoneLink,
        BluetoothEndpointRecord target,
        int contactCount,
        int messageCount,
        int conversationCount,
        DateTimeOffset? lastContactsRefreshUtc)
    {
        return Update(
            current => current with
            {
                Phase = "ready",
                LastReason = reason,
                LastAttemptUtc = DateTimeOffset.UtcNow,
                LastSuccessfulSyncUtc = DateTimeOffset.UtcNow,
                LastContactsRefreshUtc = lastContactsRefreshUtc ?? current.LastContactsRefreshUtc,
                LastError = null,
                ConsecutiveFailures = 0,
                ContactCount = contactCount,
                MessageCount = messageCount,
                NotificationCount = current.NotificationCount,
                ConversationCount = conversationCount,
                AutoEvictPhoneLink = autoEvictPhoneLink,
                Target = target
            });
    }

    public DaemonRuntimeSnapshot MarkError(
        string reason,
        bool autoEvictPhoneLink,
        Exception exception,
        BluetoothEndpointRecord? target = null)
    {
        return Update(
            current => current with
            {
                Phase = "error",
                LastReason = reason,
                LastAttemptUtc = DateTimeOffset.UtcNow,
                LastError = exception.ToString(),
                ConsecutiveFailures = current.ConsecutiveFailures + 1,
                AutoEvictPhoneLink = autoEvictPhoneLink,
                Target = target ?? current.Target
            });
    }

    public DaemonRuntimeSnapshot MarkDegraded(
        string reason,
        bool autoEvictPhoneLink,
        Exception exception,
        BluetoothEndpointRecord? target = null)
    {
        return Update(
            current => current with
            {
                Phase = "degraded",
                LastReason = reason,
                LastAttemptUtc = DateTimeOffset.UtcNow,
                LastError = exception.ToString(),
                ConsecutiveFailures = current.ConsecutiveFailures + 1,
                AutoEvictPhoneLink = autoEvictPhoneLink,
                Target = target ?? current.Target
            });
    }

    public DaemonRuntimeSnapshot SetNotificationsMode(string mode, bool enabled)
    {
        return Update(
            current => current with
            {
                NotificationsMode = NotificationMode.Normalize(mode),
                NotificationsEnabled = enabled
            });
    }

    public DaemonRuntimeSnapshot UpdateNotificationCount(int notificationCount)
    {
        return Update(
            current => current with
            {
                NotificationCount = Math.Max(0, notificationCount)
            });
    }

    public DaemonRuntimeSnapshot UpdateTransportState(SessionStateChangedRecord state)
    {
        return Update(
            current => state.Transport switch
            {
                "map" => current with { MapSession = state },
                "ancs" => current with { AncsSession = state },
                _ => current
            });
    }
}
