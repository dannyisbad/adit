using Adit.Core.Models;

namespace Adit.Daemon.Services;

public static class CapabilitySnapshotBuilder
{
    private const string LinkToWindowsBootstrap = "link_to_windows_once";
    private const string NotificationsEnableAction = "POST /v1/notifications/enable";

    public static DaemonCapabilitiesSnapshot Build(
        DaemonRuntimeSnapshot runtime,
        DaemonOptions options)
    {
        return new DaemonCapabilitiesSnapshot(
            BuildMessaging(runtime),
            BuildContacts(runtime),
            BuildNotifications(runtime, options));
    }

    public static NotificationsBootstrapSnapshot BuildNotificationsBootstrap(
        DaemonRuntimeSnapshot runtime,
        DaemonOptions options)
    {
        var notifications = BuildNotifications(runtime, options);
        var state = notifications.State switch
        {
            "ready" => "bootstrapped",
            "disabled" => "recommended",
            _ => "pending"
        };

        return new NotificationsBootstrapSnapshot(
            State: state,
            Mode: runtime.NotificationsMode,
            Enabled: runtime.NotificationsEnabled,
            CanAttemptEnable: runtime.Target is not null,
            RecommendedFlow: LinkToWindowsBootstrap,
            Reason: notifications.Reason,
            Detail: notifications.Detail);
    }

    public static DoctorSnapshot BuildDoctor(
        DaemonRuntimeSnapshot runtime,
        DaemonOptions options)
    {
        var capabilities = Build(runtime, options);
        var bootstrap = BuildNotificationsBootstrap(runtime, options);
        var setup = BuildSetup(runtime, options);
        var nextSteps = BuildNextSteps(runtime, capabilities, bootstrap);
        var overall = capabilities.Messaging.State == "ready" && capabilities.Contacts.State == "ready"
            ? runtime.NotificationsEnabled && capabilities.Notifications.State != "ready"
                ? "degraded"
                : "ready"
            : runtime.Target is null
                ? "waiting_for_device"
                : "starting";
        var summary = overall switch
        {
            "ready" when capabilities.Notifications.State == "ready" =>
                "Messaging, contacts, and notifications are ready.",
            "ready" =>
                "Messaging and contacts are ready. Notifications remain optional.",
            "degraded" =>
                "Core features are ready, but notifications need additional bootstrap or recovery.",
            "waiting_for_device" =>
                "No paired iPhone target is currently available.",
            _ =>
                "The daemon is still converging on a stable runtime."
        };

        return new DoctorSnapshot(overall, summary, nextSteps, capabilities, bootstrap, setup);
    }

    public static SetupGuideSnapshot BuildGuide(
        DaemonRuntimeSnapshot runtime,
        DaemonOptions options)
    {
        var capabilities = Build(runtime, options);
        var bootstrap = BuildNotificationsBootstrap(runtime, options);
        var setup = BuildSetup(runtime, options);
        var doctor = BuildDoctor(runtime, options);
        var steps = BuildGuideSteps(runtime, capabilities, bootstrap);
        var actions = BuildGuideActions(runtime, capabilities, bootstrap);
        return new SetupGuideSnapshot(
            Overall: doctor.Overall,
            Summary: doctor.Summary,
            Runtime: runtime,
            Setup: setup,
            Doctor: doctor,
            Capabilities: capabilities,
            NotificationsBootstrap: bootstrap,
            Steps: steps,
            Actions: actions,
            Integrations: BuildGuideIntegrations());
    }

    public static SupportedSetupSnapshot BuildSetup(
        DaemonRuntimeSnapshot runtime,
        DaemonOptions options)
    {
        if (runtime.Target is null)
        {
            return new SupportedSetupSnapshot(
                SupportedFlow: LinkToWindowsBootstrap,
                State: "needs_bootstrap",
                ExperimentalPairingApiEnabled: options.EnableExperimentalPairingApi,
                RecommendedAction: "Complete one-time Link to Windows pairing, then start adit.",
                Reason: "v1 supports a single bootstrap flow so classic messaging and ANCS setup do not drift apart.");
        }

        if (!runtime.NotificationsEnabled)
        {
            return new SupportedSetupSnapshot(
                SupportedFlow: LinkToWindowsBootstrap,
                State: "core_ready",
                ExperimentalPairingApiEnabled: options.EnableExperimentalPairingApi,
                RecommendedAction: "Messaging and contacts are ready. Call POST /v1/notifications/enable if you want to opt back into notifications.",
                Reason: "The LTW bootstrap is already good enough for the stable messaging path, but notifications are currently opted out.");
        }

        if (runtime.AncsSession?.Phase == DeviceSessionPhase.Connected)
        {
            return new SupportedSetupSnapshot(
                SupportedFlow: LinkToWindowsBootstrap,
                State: "complete",
                ExperimentalPairingApiEnabled: options.EnableExperimentalPairingApi,
                RecommendedAction: "No action required.",
                Reason: null);
        }

        return new SupportedSetupSnapshot(
            SupportedFlow: LinkToWindowsBootstrap,
            State: runtime.NotificationsMode == NotificationMode.Auto ? "adopting_notifications" : "notifications_recovering",
            ExperimentalPairingApiEnabled: options.EnableExperimentalPairingApi,
            RecommendedAction: runtime.NotificationsMode == NotificationMode.Auto
                ? "No action required unless notifications stay unhealthy. The daemon is adopting the existing Link to Windows pairing."
                : "Re-run POST /v1/notifications/enable after verifying Link to Windows is still paired and the iPhone is nearby.",
            Reason: runtime.NotificationsMode == NotificationMode.Auto
                ? "Link to Windows pairing already exists, so the daemon is trying to adopt notifications automatically."
                : "Notifications were manually enabled and are still recovering.");
    }

    private static CapabilityStateRecord BuildMessaging(DaemonRuntimeSnapshot runtime)
    {
        if (runtime.Target is null)
        {
            return new CapabilityStateRecord(
                State: "no_device",
                Stability: "stable",
                Enabled: true,
                Reason: "No paired classic iPhone target is available.",
                RecommendedAction: "Complete one-time Link to Windows pairing so messaging and contacts can come online.",
                RecommendedBootstrap: LinkToWindowsBootstrap,
                Detail: runtime.LastError);
        }

        if (runtime.MapSession?.Phase == DeviceSessionPhase.Connected)
        {
            return new CapabilityStateRecord(
                State: "ready",
                Stability: "stable",
                Enabled: true,
                Reason: null,
                RecommendedAction: null,
                RecommendedBootstrap: null,
                Detail: runtime.MapSession.Detail);
        }

        if (runtime.MessageCount > 0)
        {
            return new CapabilityStateRecord(
                State: "cached",
                Stability: "stable",
                Enabled: true,
                Reason: "Using cached messages while MAP realtime is reconnecting.",
                RecommendedAction: "Call POST /v1/sync/now if you need an immediate refresh.",
                RecommendedBootstrap: null,
                Detail: runtime.MapSession?.Detail ?? runtime.LastReason);
        }

        return new CapabilityStateRecord(
            State: "starting",
            Stability: "stable",
            Enabled: true,
            Reason: "MAP messaging has not connected yet.",
            RecommendedAction: "Wait for the next sync cycle or call POST /v1/sync/now.",
            RecommendedBootstrap: null,
            Detail: runtime.MapSession?.Detail ?? runtime.LastReason);
    }

    private static CapabilityStateRecord BuildContacts(DaemonRuntimeSnapshot runtime)
    {
        if (runtime.Target is null)
        {
            return new CapabilityStateRecord(
                State: "no_device",
                Stability: "stable",
                Enabled: true,
                Reason: "No paired classic iPhone target is available.",
                RecommendedAction: "Complete one-time Link to Windows pairing so messaging and contacts can come online.",
                RecommendedBootstrap: LinkToWindowsBootstrap,
                Detail: runtime.LastError);
        }

        if (runtime.ContactCount > 0)
        {
            return new CapabilityStateRecord(
                State: "ready",
                Stability: "stable",
                Enabled: true,
                Reason: null,
                RecommendedAction: null,
                RecommendedBootstrap: null,
                Detail: runtime.LastContactsRefreshUtc?.ToString("O"));
        }

        return new CapabilityStateRecord(
            State: "starting",
            Stability: "stable",
            Enabled: true,
            Reason: "PBAP contacts have not been cached yet.",
            RecommendedAction: "Wait for the next sync cycle or call POST /v1/sync/now.",
            RecommendedBootstrap: null,
            Detail: runtime.LastReason);
    }

    private static CapabilityStateRecord BuildNotifications(
        DaemonRuntimeSnapshot runtime,
        DaemonOptions options)
    {
        if (!runtime.NotificationsEnabled)
        {
            return new CapabilityStateRecord(
                State: "disabled",
                Stability: "prototype",
                Enabled: false,
                Reason: "Notifications are currently turned off for this daemon.",
                RecommendedAction: "Call POST /v1/notifications/enable to adopt notifications from the existing Link to Windows pairing.",
                RecommendedBootstrap: LinkToWindowsBootstrap,
                Detail: runtime.AncsSession?.Detail);
        }

        if (runtime.AncsSession?.Phase == DeviceSessionPhase.Connected)
        {
            return new CapabilityStateRecord(
                State: "ready",
                Stability: "prototype",
                Enabled: true,
                Reason: null,
                RecommendedAction: null,
                RecommendedBootstrap: LinkToWindowsBootstrap,
                Detail: runtime.AncsSession.Detail);
        }

        if (runtime.Target is null)
        {
            return new CapabilityStateRecord(
                State: "no_device",
                Stability: "prototype",
                Enabled: true,
                Reason: "Notifications are enabled, but no paired iPhone target is available.",
                RecommendedAction: "Reconnect the iPhone and let the daemon retry notification adoption.",
                RecommendedBootstrap: LinkToWindowsBootstrap,
                Detail: runtime.AncsSession?.Detail ?? runtime.LastError);
        }

        return new CapabilityStateRecord(
            State: runtime.NotificationsMode == NotificationMode.Auto ? "adopting" : "not_ready",
            Stability: "prototype",
            Enabled: true,
            Reason: runtime.NotificationsMode == NotificationMode.Auto
                ? "The daemon is trying to adopt notifications from the existing Link to Windows pairing."
                : "ANCS is enabled but the notification session is not healthy yet.",
            RecommendedAction: NotificationsEnableAction,
            RecommendedBootstrap: LinkToWindowsBootstrap,
            Detail: runtime.AncsSession?.Detail ?? runtime.AncsSession?.Error ?? runtime.LastError);
    }

    private static IReadOnlyList<string> BuildNextSteps(
        DaemonRuntimeSnapshot runtime,
        DaemonCapabilitiesSnapshot capabilities,
        NotificationsBootstrapSnapshot bootstrap)
    {
        var steps = new List<string>();

        if (capabilities.Messaging.State == "no_device")
        {
            steps.Add("Complete one-time Link to Windows pairing so messaging and contacts can come online.");
        }
        else if (capabilities.Messaging.State != "ready")
        {
            steps.Add("Let MAP finish connecting or call POST /v1/sync/now.");
        }

        if (!runtime.NotificationsEnabled)
        {
            steps.Add("If you want notifications again, call POST /v1/notifications/enable.");
        }
        else if (bootstrap.State != "bootstrapped")
        {
            steps.Add(runtime.NotificationsMode == NotificationMode.Auto
                ? "Notifications are being adopted automatically from the LTW pairing. If they stay unhealthy, call POST /v1/notifications/enable to retry manually."
                : "Notifications are enabled but not healthy yet. Re-run POST /v1/notifications/enable after verifying the iPhone is nearby and unlocked.");
        }

        if (steps.Count == 0)
        {
            steps.Add("No action required.");
        }

        return steps;
    }

    private static IReadOnlyList<SetupGuideStepRecord> BuildGuideSteps(
        DaemonRuntimeSnapshot runtime,
        DaemonCapabilitiesSnapshot capabilities,
        NotificationsBootstrapSnapshot bootstrap)
    {
        var steps = new List<SetupGuideStepRecord>
        {
            new(
                Id: "bootstrap_pairing",
                Title: "Complete Link to Windows pairing",
                Status: runtime.Target is null ? "current" : "complete",
                Blocking: runtime.Target is null,
                Summary: runtime.Target is null
                    ? "A paired iPhone target is required before the daemon can bring messaging and contacts online."
                    : $"Using paired target {runtime.Target.Name}.",
                RecommendedAction: runtime.Target is null
                    ? "Complete one-time Link to Windows pairing, then start adit."
                    : null,
                Detail: runtime.Target?.Id),
            new(
                Id: "core_sync",
                Title: "Cache contacts and messages",
                Status: capabilities.Messaging.State == "ready" && capabilities.Contacts.State == "ready"
                    ? "complete"
                    : runtime.Target is null
                        ? "pending"
                        : "current",
                Blocking: runtime.Target is not null
                    && (capabilities.Messaging.State != "ready" || capabilities.Contacts.State != "ready"),
                Summary: capabilities.Messaging.State == "ready" && capabilities.Contacts.State == "ready"
                    ? $"Core sync is ready with {runtime.ContactCount} contacts, {runtime.MessageCount} messages, and {runtime.ConversationCount} conversations cached."
                    : runtime.Target is null
                        ? "Core sync will begin after a paired iPhone target is available."
                        : "Messaging and contact caching are still converging.",
                RecommendedAction: runtime.Target is null
                    ? "Complete one-time Link to Windows pairing so messaging and contacts can come online."
                    : capabilities.Messaging.RecommendedAction ?? capabilities.Contacts.RecommendedAction,
                Detail: capabilities.Messaging.Detail ?? capabilities.Contacts.Detail),
            new(
                Id: "notifications",
                Title: "Adopt notifications",
                Status: !runtime.NotificationsEnabled
                    ? "optional"
                    : bootstrap.State == "bootstrapped"
                        ? "complete"
                        : runtime.Target is null
                            ? "pending"
                            : "current",
                Blocking: false,
                Summary: !runtime.NotificationsEnabled
                    ? "Notifications are optional and currently turned off."
                    : bootstrap.State == "bootstrapped"
                        ? "Notifications are connected and ready."
                        : runtime.Target is null
                            ? "Notification adoption waits on a paired iPhone target."
                            : "The daemon is still adopting or recovering the ANCS notification session.",
                RecommendedAction: !runtime.NotificationsEnabled
                    ? "Call POST /v1/notifications/enable if you want notifications."
                    : capabilities.Notifications.RecommendedAction,
                Detail: capabilities.Notifications.Detail)
        };

        return steps;
    }

    private static IReadOnlyList<SetupGuideActionRecord> BuildGuideActions(
        DaemonRuntimeSnapshot runtime,
        DaemonCapabilitiesSnapshot capabilities,
        NotificationsBootstrapSnapshot bootstrap)
    {
        var actions = new List<SetupGuideActionRecord>
        {
            new(
                Id: "open_phone_link",
                Title: "Open Phone Link and confirm LTW pairing",
                Status: runtime.Target is null ? "recommended" : "available",
                Kind: "manual",
                Method: null,
                Path: null,
                CliCommand: null,
                Description: runtime.Target is null
                    ? "Required before the daemon can reach MAP and PBAP."
                    : "Useful if Windows stops exposing the paired iPhone endpoints."),
            new(
                Id: "daemon_doctor",
                Title: "Run daemon doctor",
                Status: capabilities.Messaging.State == "ready" && capabilities.Contacts.State == "ready"
                    ? "available"
                    : "recommended",
                Kind: "cli_command",
                Method: null,
                Path: null,
                CliCommand: "dotnet run --project src\\Adit.Daemon -- doctor",
                Description: "Print the daemon's current readiness summary and next steps."),
            new(
                Id: "trigger_sync",
                Title: "Trigger an immediate sync",
                Status: runtime.Target is null ? "blocked" : "available",
                Kind: "daemon_request",
                Method: "POST",
                Path: "/v1/sync/now",
                CliCommand: "dotnet run --project src\\Adit.Daemon -- sync",
                Description: runtime.Target is null
                    ? "Blocked until a paired iPhone target is available."
                    : "Useful when messaging or contacts are still converging.")
        };

        if (!runtime.NotificationsEnabled || bootstrap.State != "bootstrapped")
        {
            actions.Add(
                new SetupGuideActionRecord(
                    Id: "enable_notifications",
                    Title: "Enable or retry notifications",
                    Status: runtime.Target is null ? "blocked" : "available",
                    Kind: "daemon_request",
                    Method: "POST",
                    Path: "/v1/notifications/enable",
                    CliCommand: "dotnet run --project src\\Adit.Daemon -- notifications-enable",
                    Description: runtime.Target is null
                        ? "Blocked until the paired iPhone target is visible again."
                        : "Use this when notifications are disabled or ANCS adoption needs a manual retry."));
        }

        return actions;
    }

    private static IReadOnlyList<SetupGuideIntegrationRecord> BuildGuideIntegrations()
    {
        return
        [
            new SetupGuideIntegrationRecord(
                Id: "claude_code_project_mcp",
                Title: "Claude Code project MCP server",
                Status: "recommended",
                Kind: "project_file",
                Summary: "Use the repo's checked-in .mcp.json so Claude Code can launch the adit MCP server without hand-editing global config.",
                Path: ".mcp.json",
                CliCommand: "claude mcp add-json adit --scope project '{\"command\":\"node\",\"args\":[\"./sdk/mcp-server/server.js\"],\"env\":{\"ADIT_URL\":\"http://127.0.0.1:5037\"}}'",
                RecommendedPrompt: "Set this up for Claude Code in this repo."),
            new SetupGuideIntegrationRecord(
                Id: "claude_code_project_subagent",
                Title: "Claude Code Adit operator",
                Status: "recommended",
                Kind: "project_file",
                Summary: "Use the checked-in project subagent so Claude starts with the daemon setup guide, safe recipient resolution, and the repo's preferred recovery flow.",
                Path: ".claude/agents/adit-operator.md",
                CliCommand: null,
                RecommendedPrompt: "Use the adit-operator subagent for setup and messaging tasks."),
            new SetupGuideIntegrationRecord(
                Id: "repo_mcp_sdk",
                Title: "Standalone MCP server package",
                Status: "available",
                Kind: "repo_sdk",
                Summary: "Use sdk/mcp-server when the host supports MCP but cannot consume the repo's project files directly.",
                Path: "sdk/mcp-server",
                CliCommand: "npm install ./sdk/mcp-server",
                RecommendedPrompt: null),
            new SetupGuideIntegrationRecord(
                Id: "repo_claudebot_skill",
                Title: "Standalone Claudebot skill",
                Status: "available",
                Kind: "repo_skill",
                Summary: "Use sdk/claudebot-skill as the canonical prompt text for hosts that need Adit setup and safe-send instructions outside the shared Claude Code path.",
                Path: "sdk/claudebot-skill/SKILL.md",
                CliCommand: null,
                RecommendedPrompt: null)
        ];
    }
}
