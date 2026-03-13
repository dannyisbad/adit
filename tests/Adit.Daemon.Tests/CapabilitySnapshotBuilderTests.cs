using Adit.Core.Models;
using Adit.Daemon.Services;

namespace Adit.Daemon.Tests;

public sealed class CapabilitySnapshotBuilderTests
{
    [Fact]
    public void Build_WhenNotificationsDisabled_ReportsPrototypeBootstrapFlow()
    {
        var runtime = new DaemonRuntimeSnapshot(
            Phase: "ready",
            LastReason: "timer",
            LastAttemptUtc: DateTimeOffset.UtcNow,
            LastSuccessfulSyncUtc: DateTimeOffset.UtcNow,
            LastContactsRefreshUtc: DateTimeOffset.UtcNow,
            LastError: null,
            ConsecutiveFailures: 0,
            ContactCount: 12,
            MessageCount: 24,
            NotificationCount: 0,
            ConversationCount: 5,
            AutoEvictPhoneLink: true,
            NotificationsMode: NotificationMode.Off,
            NotificationsEnabled: false,
            Target: new BluetoothEndpointRecord(
                "classic",
                "classic-1",
                "Riley's iPhone",
                true,
                "2a:7c:4f:91:5e:b3",
                "2a7c4f915eb3",
                true,
                true,
                "container-1",
                "Communication.Phone"),
            MapSession: new SessionStateChangedRecord("map", DeviceSessionPhase.Connected, DateTimeOffset.UtcNow, "already_open", null),
            AncsSession: null);

        var snapshot = CapabilitySnapshotBuilder.Build(runtime, new DaemonOptions());
        var bootstrap = CapabilitySnapshotBuilder.BuildNotificationsBootstrap(runtime, new DaemonOptions());
        var setup = CapabilitySnapshotBuilder.BuildSetup(runtime, new DaemonOptions());

        Assert.Equal("ready", snapshot.Messaging.State);
        Assert.Equal("ready", snapshot.Contacts.State);
        Assert.Equal("disabled", snapshot.Notifications.State);
        Assert.Equal("prototype", snapshot.Notifications.Stability);
        Assert.Equal("link_to_windows_once", snapshot.Notifications.RecommendedBootstrap);
        Assert.Equal("recommended", bootstrap.State);
        Assert.True(bootstrap.CanAttemptEnable);
        Assert.Equal("core_ready", setup.State);
        Assert.Equal("link_to_windows_once", setup.SupportedFlow);
        Assert.False(setup.ExperimentalPairingApiEnabled);
    }

    [Fact]
    public void BuildDoctor_WhenNotificationsEnabledButDisconnected_ReportsDegraded()
    {
        var runtime = new DaemonRuntimeSnapshot(
            Phase: "ready",
            LastReason: "timer",
            LastAttemptUtc: DateTimeOffset.UtcNow,
            LastSuccessfulSyncUtc: DateTimeOffset.UtcNow,
            LastContactsRefreshUtc: DateTimeOffset.UtcNow,
            LastError: null,
            ConsecutiveFailures: 0,
            ContactCount: 12,
            MessageCount: 24,
            NotificationCount: 0,
            ConversationCount: 5,
            AutoEvictPhoneLink: true,
            NotificationsMode: NotificationMode.On,
            NotificationsEnabled: true,
            Target: new BluetoothEndpointRecord(
                "classic",
                "classic-1",
                "Riley's iPhone",
                true,
                "2a:7c:4f:91:5e:b3",
                "2a7c4f915eb3",
                true,
                true,
                "container-1",
                "Communication.Phone"),
            MapSession: new SessionStateChangedRecord("map", DeviceSessionPhase.Connected, DateTimeOffset.UtcNow, "already_open", null),
            AncsSession: new SessionStateChangedRecord("ancs", DeviceSessionPhase.Disconnected, DateTimeOffset.UtcNow, "gatt_session_closed", "Unreachable"));

        var doctor = CapabilitySnapshotBuilder.BuildDoctor(runtime, new DaemonOptions());

        Assert.Equal("degraded", doctor.Overall);
        Assert.Equal("not_ready", doctor.Capabilities.Notifications.State);
        Assert.Contains(doctor.NextSteps, step => step.Contains("POST /v1/notifications/enable", StringComparison.Ordinal));
        Assert.Equal("notifications_recovering", doctor.Setup.State);
    }

    [Fact]
    public void Build_WhenNoDevice_UsesSingleBootstrapFlowForCoreCapabilities()
    {
        var runtime = new DaemonRuntimeSnapshot(
            Phase: "waiting",
            LastReason: "startup",
            LastAttemptUtc: DateTimeOffset.UtcNow,
            LastSuccessfulSyncUtc: null,
            LastContactsRefreshUtc: null,
            LastError: "No device",
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

        var options = new DaemonOptions { EnableExperimentalPairingApi = true };
        var capabilities = CapabilitySnapshotBuilder.Build(runtime, options);
        var setup = CapabilitySnapshotBuilder.BuildSetup(runtime, options);

        Assert.Equal("no_device", capabilities.Messaging.State);
        Assert.Equal("link_to_windows_once", capabilities.Messaging.RecommendedBootstrap);
        Assert.Equal("link_to_windows_once", capabilities.Contacts.RecommendedBootstrap);
        Assert.Equal("needs_bootstrap", setup.State);
        Assert.True(setup.ExperimentalPairingApiEnabled);
    }
}
