using Adit.Core.Models;

namespace Adit.Daemon.Services;

public sealed record DaemonEventRecord(
    long Sequence,
    DateTimeOffset TimestampUtc,
    string Type,
    object? Payload);

public sealed record DaemonRuntimeSnapshot(
    string Phase,
    string LastReason,
    DateTimeOffset? LastAttemptUtc,
    DateTimeOffset? LastSuccessfulSyncUtc,
    DateTimeOffset? LastContactsRefreshUtc,
    string? LastError,
    int ConsecutiveFailures,
    int ContactCount,
    int MessageCount,
    int NotificationCount,
    int ConversationCount,
    bool AutoEvictPhoneLink,
    string NotificationsMode,
    bool NotificationsEnabled,
    BluetoothEndpointRecord? Target,
    SessionStateChangedRecord? MapSession,
    SessionStateChangedRecord? AncsSession);

public sealed record ResolvedRecipientRecord(
    ContactRecord Contact,
    string Recipient);

public sealed record ResolvedConversationRecipientRecord(
    ConversationSnapshot Conversation,
    ConversationParticipantRecord Participant,
    string Recipient);

public sealed record ResolvedMessagePlan(
    BluetoothEndpointRecord Target,
    string Recipient,
    string ResolutionSource,
    ResolvedRecipientRecord? ResolvedContact,
    ResolvedConversationRecipientRecord? ResolvedConversation);

public sealed record CapabilityStateRecord(
    string State,
    string Stability,
    bool Enabled,
    string? Reason,
    string? RecommendedAction,
    string? RecommendedBootstrap,
    string? Detail);

public sealed record DaemonCapabilitiesSnapshot(
    CapabilityStateRecord Messaging,
    CapabilityStateRecord Contacts,
    CapabilityStateRecord Notifications);

public sealed record NotificationsBootstrapSnapshot(
    string State,
    string Mode,
    bool Enabled,
    bool CanAttemptEnable,
    string RecommendedFlow,
    string? Reason,
    string? Detail);

public sealed record SupportedSetupSnapshot(
    string SupportedFlow,
    string State,
    bool ExperimentalPairingApiEnabled,
    string RecommendedAction,
    string? Reason);

public sealed record SetupGuideStepRecord(
    string Id,
    string Title,
    string Status,
    bool Blocking,
    string Summary,
    string? RecommendedAction,
    string? Detail);

public sealed record SetupGuideActionRecord(
    string Id,
    string Title,
    string Status,
    string Kind,
    string? Method,
    string? Path,
    string? CliCommand,
    string Description);

public sealed record SetupGuideIntegrationRecord(
    string Id,
    string Title,
    string Status,
    string Kind,
    string Summary,
    string? Path,
    string? CliCommand,
    string? RecommendedPrompt);

public sealed record DoctorSnapshot(
    string Overall,
    string Summary,
    IReadOnlyList<string> NextSteps,
    DaemonCapabilitiesSnapshot Capabilities,
    NotificationsBootstrapSnapshot NotificationsBootstrap,
    SupportedSetupSnapshot Setup);

public sealed record NotificationsToggleResult(
    string Mode,
    bool Enabled,
    bool Ready,
    string Status,
    string? Reason,
    BluetoothEndpointRecord? Target,
    BluetoothLeDeviceRecord? LeTarget,
    DaemonRuntimeSnapshot Runtime);

public sealed record SetupGuideSnapshot(
    string Overall,
    string Summary,
    DaemonRuntimeSnapshot Runtime,
    SupportedSetupSnapshot Setup,
    DoctorSnapshot Doctor,
    DaemonCapabilitiesSnapshot Capabilities,
    NotificationsBootstrapSnapshot NotificationsBootstrap,
    IReadOnlyList<SetupGuideStepRecord> Steps,
    IReadOnlyList<SetupGuideActionRecord> Actions,
    IReadOnlyList<SetupGuideIntegrationRecord> Integrations);

public sealed record ThreadChooserHeadSnapshot(
    int StructDim,
    int SemanticScalarDim,
    int SemanticVecDim,
    int HiddenDim,
    int CandidateEncoderLayers,
    int CandidateEncoderHeads,
    bool SemanticResidualLogit);

public sealed record ThreadChooserCandidateScoreSnapshot(
    int Index,
    string ThreadId,
    bool IsGroup,
    double Logit,
    double Probability,
    IReadOnlyDictionary<string, double> Semantic);

public sealed record ThreadChooserLastScoreSnapshot(
    DateTimeOffset TimestampUtc,
    string SampleId,
    int CandidateCount,
    int PredictedIndex,
    string PredictedThreadId,
    IReadOnlyList<double> Scores,
    IReadOnlyList<double> Probabilities,
    IReadOnlyList<ThreadChooserCandidateScoreSnapshot> Candidates);

public sealed record ThreadChooserLastRunSnapshot(
    DateTimeOffset TimestampUtc,
    int TotalMessages,
    int ScoredSamples,
    int RerankedMessages,
    ThreadChooserLastScoreSnapshot? LastScore);

public sealed record ThreadChooserStatusSnapshot(
    bool Enabled,
    string Status,
    string? Reason,
    bool ScriptExists,
    bool CheckpointExists,
    string PythonPath,
    bool SidecarRunning,
    bool ServiceHealthy,
    int? ProcessId,
    string ServiceUrl,
    string ScriptPath,
    string CheckpointPath,
    string ConfiguredModelName,
    int Port,
    int MaxCandidates,
    int HistoryTurns,
    string? ResolvedModelName,
    string? SemanticCachePath,
    string? Device,
    string? Dtype,
    bool? IncludeCandidateScore,
    bool? IncludeCandidateDisplayNameInQwen,
    ThreadChooserHeadSnapshot? Head,
    DateTimeOffset? LastHealthCheckUtc,
    string? LastError,
    ThreadChooserLastRunSnapshot? LastRun);

public static class NotificationMode
{
    public const string Auto = "auto";
    public const string On = "on";
    public const string Off = "off";

    public static string Normalize(string? value)
    {
        return value?.Trim().ToLowerInvariant() switch
        {
            On => On,
            Off => Off,
            _ => Auto
        };
    }
}
