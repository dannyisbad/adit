using System.Diagnostics;
using System.Globalization;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.RegularExpressions;
using Adit.Core.Models;

namespace Adit.Daemon.Services;

public sealed class LearnedThreadReranker : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
    };

    private static readonly HashSet<string> GenericPreviews = new(StringComparer.OrdinalIgnoreCase)
    {
        "ok",
        "okay",
        "k",
        "kk",
        "yes",
        "no",
        "yup",
        "yep",
        "lol",
        "lmao",
        "teehee",
        "sounds good",
        "sounds good.",
        "sounds good!",
        "sure",
        "bet",
        "nice",
        "true",
        "facts",
        "thanks",
        "thank you"
    };

    private readonly ILogger<LearnedThreadReranker> logger;
    private readonly DaemonOptions options;
    private readonly DaemonEventHub eventHub;
    private readonly HttpClient httpClient = new()
    {
        Timeout = TimeSpan.FromSeconds(120)
    };
    private readonly SemaphoreSlim startupLock = new(1, 1);
    private readonly object statusGate = new();
    private Process? serviceProcess;
    private ThreadChooserStatusSnapshot statusSnapshot;
    private int sampleDumpCount;

    public LearnedThreadReranker(
        ILogger<LearnedThreadReranker> logger,
        DaemonOptions options,
        DaemonEventHub eventHub)
    {
        this.logger = logger;
        this.options = options;
        this.eventHub = eventHub;
        statusSnapshot = CreateBaseStatusSnapshot();
    }

    public async Task<ConversationSynthesisResult> TryRerankAsync(
        ConversationSynthesisResult synthesis,
        IReadOnlyList<StoredNotificationRecord> notifications,
        CancellationToken cancellationToken)
    {
        if (!options.EnableLearnedThreadChooser || synthesis.Messages.Count < 2)
        {
            return synthesis;
        }

        if (!await EnsureServiceAsync(cancellationToken))
        {
            return synthesis;
        }

        var orderedMessages = synthesis.Messages
            .OrderBy(message => message.SortTimestampUtc ?? DateTimeOffset.MinValue)
            .ThenBy(message => message.MessageKey, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var buckets = new Dictionary<string, RuntimeBucket>(StringComparer.OrdinalIgnoreCase);
        var finalMessages = new List<SynthesizedMessageRecord>(orderedMessages.Length);
        var rerankedCount = 0;
        var scoredSamples = 0;
        ThreadChooserLastScoreSnapshot? lastScore = null;

        foreach (var message in orderedMessages)
        {
            var sample = BuildSample(message, buckets, notifications);
            RuntimeScoreResponse? score = null;
            if (sample.CandidateThreads.Count >= 2)
            {
                score = await ScoreSampleAsync(sample, cancellationToken);
                if (score is not null)
                {
                    scoredSamples += 1;
                    lastScore = ToLastScoreSnapshot(score);
                }
            }

            var chosenThreadId = score?.PredictedThreadId;
            var assigned = AssignMessage(message, sample, chosenThreadId, buckets);
            if (!string.Equals(assigned.ConversationId, message.ConversationId, StringComparison.OrdinalIgnoreCase))
            {
                rerankedCount += 1;
            }

            finalMessages.Add(assigned);
        }

        if (scoredSamples > 0 || rerankedCount > 0)
        {
            var runSnapshot = new ThreadChooserLastRunSnapshot(
                TimestampUtc: DateTimeOffset.UtcNow,
                TotalMessages: orderedMessages.Length,
                ScoredSamples: scoredSamples,
                RerankedMessages: rerankedCount,
                LastScore: lastScore);
            UpdateStatusSnapshot(
                current => current with
                {
                    Status = current.ServiceHealthy ? "ready" : current.Status,
                    Reason = current.ServiceHealthy ? "Thread chooser is healthy." : current.Reason,
                    LastRun = runSnapshot,
                    LastError = current.ServiceHealthy ? null : current.LastError
                });
            eventHub.Publish(
                "thread_chooser.run",
                new
                {
                    totalMessages = runSnapshot.TotalMessages,
                    scoredSamples = runSnapshot.ScoredSamples,
                    rerankedMessages = runSnapshot.RerankedMessages,
                    lastScore = runSnapshot.LastScore
                });
        }

        if (rerankedCount == 0)
        {
            return synthesis;
        }

        var reordered = finalMessages
            .OrderByDescending(message => message.SortTimestampUtc ?? DateTimeOffset.MinValue)
            .ThenBy(message => message.MessageKey, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var conversations = BuildConversations(reordered);

        logger.LogInformation("Learned thread reranker reassigned {MessageCount} message(s).", rerankedCount);
        return new ConversationSynthesisResult(synthesis.SelfPhones, reordered, conversations);
    }

    public async Task<ThreadChooserStatusSnapshot> GetStatusAsync(CancellationToken cancellationToken)
    {
        var baseSnapshot = CreateBaseStatusSnapshot();
        if (!baseSnapshot.Enabled || !baseSnapshot.ScriptExists || !baseSnapshot.CheckpointExists)
        {
            UpdateStatusSnapshot(_ => baseSnapshot);
            return GetStatusSnapshot();
        }

        var health = await TryGetHealthAsync(cancellationToken);
        if (health is not null)
        {
            UpdateStatusFromHealth(health);
            return GetStatusSnapshot();
        }

        var running = serviceProcess is { HasExited: false };
        UpdateStatusSnapshot(
            current => current with
            {
                Status = running ? "starting" : "idle",
                Reason = running
                    ? "Thread chooser sidecar has started but is not healthy yet."
                    : "Thread chooser is enabled and will start on demand during sync.",
                SidecarRunning = running,
                ServiceHealthy = false,
                ProcessId = running ? serviceProcess?.Id : null,
                LastHealthCheckUtc = DateTimeOffset.UtcNow
            });
        return GetStatusSnapshot();
    }

    public void Dispose()
    {
        startupLock.Dispose();
        httpClient.Dispose();
        TryStopServiceProcess();
    }

    private async Task<bool> EnsureServiceAsync(CancellationToken cancellationToken)
    {
        var baseSnapshot = CreateBaseStatusSnapshot();
        if (!baseSnapshot.ScriptExists || !baseSnapshot.CheckpointExists)
        {
            logger.LogDebug(
                "Learned thread chooser disabled because script/checkpoint is missing. Script={ScriptPath} Checkpoint={CheckpointPath}",
                options.ThreadChooserScriptPath,
                options.ThreadChooserCheckpointPath);
            UpdateStatusSnapshot(_ => baseSnapshot);
            return false;
        }

        if (await TryGetHealthAsync(cancellationToken) is not null)
        {
            return true;
        }

        await startupLock.WaitAsync(cancellationToken);
        try
        {
            if (await TryGetHealthAsync(cancellationToken) is not null)
            {
                return true;
            }

            if (serviceProcess is null || serviceProcess.HasExited)
            {
                StartServiceProcess();
            }

            var deadline = DateTimeOffset.UtcNow.AddSeconds(90);
            while (DateTimeOffset.UtcNow < deadline && !cancellationToken.IsCancellationRequested)
            {
                if (await TryGetHealthAsync(cancellationToken) is not null)
                {
                    return true;
                }

                await Task.Delay(1000, cancellationToken);
            }

            logger.LogWarning("Timed out waiting for learned thread chooser sidecar to become healthy.");
            UpdateStatusSnapshot(
                current => current with
                {
                    Status = "unhealthy",
                    Reason = "Thread chooser sidecar did not become healthy before the startup timeout.",
                    SidecarRunning = serviceProcess is { HasExited: false },
                    ServiceHealthy = false,
                    ProcessId = serviceProcess is { HasExited: false } ? serviceProcess.Id : null,
                    LastHealthCheckUtc = DateTimeOffset.UtcNow,
                    LastError = "Timed out waiting for the thread chooser sidecar to become healthy."
                });
            eventHub.Publish(
                "thread_chooser.unhealthy",
                new
                {
                    status = "unhealthy",
                    reason = "startup_timeout",
                    processId = serviceProcess is { HasExited: false } ? (int?)serviceProcess.Id : null
                });
            return false;
        }
        finally
        {
            startupLock.Release();
        }
    }

    private void StartServiceProcess()
    {
        TryStopServiceProcess();

        var pythonPath = File.Exists(options.ThreadChooserPythonPath)
            ? options.ThreadChooserPythonPath
            : "python";
        var args =
            $"\"{options.ThreadChooserScriptPath}\" serve --checkpoint \"{options.ThreadChooserCheckpointPath}\" --model-name \"{options.ThreadChooserModelName}\" --port {options.ThreadChooserPort}";
        var startInfo = new ProcessStartInfo
        {
            FileName = pythonPath,
            Arguments = args,
            WorkingDirectory = Directory.GetCurrentDirectory(),
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };

        serviceProcess = new Process { StartInfo = startInfo, EnableRaisingEvents = true };
        serviceProcess.OutputDataReceived += (_, eventArgs) =>
        {
            if (!string.IsNullOrWhiteSpace(eventArgs.Data))
            {
                logger.LogInformation("thread-chooser: {Line}", eventArgs.Data);
            }
        };
        serviceProcess.ErrorDataReceived += (_, eventArgs) =>
        {
            if (!string.IsNullOrWhiteSpace(eventArgs.Data))
            {
                logger.LogWarning("thread-chooser: {Line}", eventArgs.Data);
            }
        };

        serviceProcess.Start();
        serviceProcess.BeginOutputReadLine();
        serviceProcess.BeginErrorReadLine();
        UpdateStatusSnapshot(
            current => current with
            {
                Status = "starting",
                Reason = "Thread chooser sidecar is starting.",
                SidecarRunning = true,
                ServiceHealthy = false,
                ProcessId = serviceProcess.Id,
                LastHealthCheckUtc = DateTimeOffset.UtcNow,
                LastError = null
            });
        eventHub.Publish(
            "thread_chooser.sidecar_started",
            new
            {
                processId = serviceProcess.Id,
                pythonPath,
                scriptPath = options.ThreadChooserScriptPath,
                checkpointPath = options.ThreadChooserCheckpointPath,
                modelName = options.ThreadChooserModelName,
                port = options.ThreadChooserPort
            });
        logger.LogInformation(
            "Started learned thread chooser sidecar using {PythonPath} {ScriptPath}.",
            pythonPath,
            options.ThreadChooserScriptPath);
    }

    private async Task<ThreadChooserHealthResponse?> TryGetHealthAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var response = await httpClient.GetAsync(GetUri("/healthz"), cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                UpdateStatusSnapshot(
                    current => current with
                    {
                        Status = serviceProcess is { HasExited: false } ? "starting" : "idle",
                        Reason = "Thread chooser sidecar did not return a healthy status.",
                        SidecarRunning = serviceProcess is { HasExited: false },
                        ServiceHealthy = false,
                        ProcessId = serviceProcess is { HasExited: false } ? serviceProcess.Id : null,
                        LastHealthCheckUtc = DateTimeOffset.UtcNow
                    });
                return null;
            }

            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);
            var payload = JsonSerializer.Deserialize<ThreadChooserHealthResponse>(responseBody, JsonOptions);
            if (payload is null)
            {
                UpdateStatusSnapshot(
                    current => current with
                    {
                        Status = "unhealthy",
                        Reason = "Thread chooser health response could not be parsed.",
                        ServiceHealthy = false,
                        LastHealthCheckUtc = DateTimeOffset.UtcNow,
                        LastError = "Health response was empty or invalid."
                    });
                return null;
            }

            return payload;
        }
        catch
        {
            return null;
        }
    }

    private async Task<RuntimeScoreResponse?> ScoreSampleAsync(RuntimeThreadChoiceSample sample, CancellationToken cancellationToken)
    {
        try
        {
            var payload = JsonSerializer.Serialize(sample, JsonOptions);
            using var content = new StringContent(payload);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json")
            {
                CharSet = "utf-8"
            };
            using var response = await httpClient.PostAsync(GetUri("/score"), content, cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                var body = await response.Content.ReadAsStringAsync(cancellationToken);
                DumpSampleForDebug(sample);
                logger.LogWarning(
                    "Learned thread chooser rejected runtime sample with status {StatusCode}. Body={Body}",
                    (int)response.StatusCode,
                    body);
                UpdateStatusSnapshot(
                    current => current with
                    {
                        Status = current.ServiceHealthy ? "ready" : "unhealthy",
                        Reason = "Thread chooser rejected a runtime sample.",
                        LastError = $"Score request returned {(int)response.StatusCode}.",
                        LastHealthCheckUtc = DateTimeOffset.UtcNow
                    });
                eventHub.Publish(
                    "thread_chooser.score_rejected",
                    new
                    {
                        sampleId = sample.SampleId,
                        statusCode = (int)response.StatusCode,
                        body
                    });
                return null;
            }

            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);
            var result = JsonSerializer.Deserialize<RuntimeScoreResponse>(responseBody, JsonOptions);
            if (result is null)
            {
                UpdateStatusSnapshot(
                    current => current with
                    {
                        Status = current.ServiceHealthy ? "ready" : "unhealthy",
                        Reason = "Thread chooser returned an empty score payload.",
                        LastError = "Score response was empty or invalid."
                    });
                return null;
            }

            var lastScore = ToLastScoreSnapshot(result);
            UpdateStatusSnapshot(
                current => current with
                {
                    Status = "ready",
                    Reason = "Thread chooser is healthy.",
                    LastError = null
                });
            eventHub.Publish(
                "thread_chooser.score",
                new
                {
                    sampleId = result.SampleId,
                    candidateCount = result.CandidateCount,
                    predictedIndex = result.PredictedIndex,
                    predictedThreadId = result.PredictedThreadId,
                    candidates = result.Candidates
                        .OrderByDescending(candidate => candidate.Probability)
                        .Take(3)
                        .ToArray()
                });
            return result;
        }
        catch (Exception exception)
        {
            logger.LogWarning(exception, "Learned thread chooser scoring failed. Falling back to heuristic thread choice.");
            UpdateStatusSnapshot(
                current => current with
                {
                    Status = current.ServiceHealthy ? "ready" : "unhealthy",
                    Reason = "Thread chooser scoring failed. Falling back to the heuristic chooser.",
                    LastError = exception.Message
                });
            eventHub.Publish(
                "thread_chooser.score_failed",
                new
                {
                    sampleId = sample.SampleId,
                    error = exception.Message
                });
            return null;
        }
    }

    private ThreadChooserStatusSnapshot GetStatusSnapshot()
    {
        lock (statusGate)
        {
            return statusSnapshot;
        }
    }

    private void UpdateStatusSnapshot(Func<ThreadChooserStatusSnapshot, ThreadChooserStatusSnapshot> updater)
    {
        lock (statusGate)
        {
            statusSnapshot = updater(statusSnapshot);
        }
    }

    private ThreadChooserStatusSnapshot CreateBaseStatusSnapshot()
    {
        var scriptExists = File.Exists(options.ThreadChooserScriptPath);
        var checkpointExists = File.Exists(options.ThreadChooserCheckpointPath);
        var pythonPath = File.Exists(options.ThreadChooserPythonPath)
            ? options.ThreadChooserPythonPath
            : "python";
        var running = serviceProcess is { HasExited: false };

        return new ThreadChooserStatusSnapshot(
            Enabled: options.EnableLearnedThreadChooser,
            Status: !options.EnableLearnedThreadChooser
                ? "disabled"
                : scriptExists && checkpointExists
                    ? "idle"
                    : "missing_artifacts",
            Reason: !options.EnableLearnedThreadChooser
                ? "Learned thread chooser is disabled."
                : scriptExists && checkpointExists
                    ? "Thread chooser is enabled and will start on demand during sync."
                    : "Thread chooser assets are missing.",
            ScriptExists: scriptExists,
            CheckpointExists: checkpointExists,
            PythonPath: pythonPath,
            SidecarRunning: running,
            ServiceHealthy: false,
            ProcessId: running ? serviceProcess?.Id : null,
            ServiceUrl: GetServiceRoot(),
            ScriptPath: options.ThreadChooserScriptPath,
            CheckpointPath: options.ThreadChooserCheckpointPath,
            ConfiguredModelName: options.ThreadChooserModelName,
            Port: options.ThreadChooserPort,
            MaxCandidates: options.ThreadChooserMaxCandidates,
            HistoryTurns: options.ThreadChooserHistoryTurns,
            ResolvedModelName: null,
            SemanticCachePath: null,
            Device: null,
            Dtype: null,
            IncludeCandidateScore: null,
            IncludeCandidateDisplayNameInQwen: null,
            Head: null,
            LastHealthCheckUtc: null,
            LastError: null,
            LastRun: null);
    }

    private void UpdateStatusFromHealth(ThreadChooserHealthResponse payload)
    {
        var previous = GetStatusSnapshot();
        UpdateStatusSnapshot(
            current => current with
            {
                Status = "ready",
                Reason = "Thread chooser is healthy.",
                ScriptExists = true,
                CheckpointExists = true,
                PythonPath = File.Exists(options.ThreadChooserPythonPath)
                    ? options.ThreadChooserPythonPath
                    : "python",
                SidecarRunning = serviceProcess is { HasExited: false },
                ServiceHealthy = true,
                ProcessId = serviceProcess is { HasExited: false } ? serviceProcess.Id : null,
                ServiceUrl = GetServiceRoot(),
                ResolvedModelName = payload.ModelName,
                SemanticCachePath = payload.SemanticCache,
                Device = payload.Device,
                Dtype = payload.Dtype,
                IncludeCandidateScore = payload.IncludeCandidateScore,
                IncludeCandidateDisplayNameInQwen = payload.IncludeCandidateDisplayNameInQwen,
                Head = new ThreadChooserHeadSnapshot(
                    payload.Head.StructDim,
                    payload.Head.SemanticScalarDim,
                    payload.Head.SemanticVecDim,
                    payload.Head.HiddenDim,
                    payload.Head.CandidateEncoderLayers,
                    payload.Head.CandidateEncoderHeads,
                    payload.Head.SemanticResidualLogit),
                LastHealthCheckUtc = DateTimeOffset.UtcNow,
                LastError = null
            });

        if (!previous.ServiceHealthy || !string.Equals(previous.Status, "ready", StringComparison.Ordinal))
        {
            eventHub.Publish(
                "thread_chooser.ready",
                new
                {
                    modelName = payload.ModelName,
                    checkpoint = payload.Checkpoint,
                    device = payload.Device,
                    dtype = payload.Dtype,
                    port = options.ThreadChooserPort
                });
        }
    }

    private static ThreadChooserLastScoreSnapshot ToLastScoreSnapshot(RuntimeScoreResponse result)
    {
        return new ThreadChooserLastScoreSnapshot(
            TimestampUtc: DateTimeOffset.UtcNow,
            SampleId: result.SampleId,
            CandidateCount: result.CandidateCount,
            PredictedIndex: result.PredictedIndex,
            PredictedThreadId: result.PredictedThreadId,
            Scores: result.Scores,
            Probabilities: result.Probabilities,
            Candidates: result.Candidates
                .Select(
                    candidate => new ThreadChooserCandidateScoreSnapshot(
                        candidate.Index,
                        candidate.ThreadId,
                        candidate.IsGroup,
                        candidate.Logit,
                        candidate.Probability,
                        new Dictionary<string, double>(candidate.Semantic, StringComparer.OrdinalIgnoreCase)))
                .ToArray());
    }

    private string GetServiceRoot()
    {
        return $"http://127.0.0.1:{options.ThreadChooserPort}";
    }

    private SynthesizedMessageRecord AssignMessage(
        SynthesizedMessageRecord message,
        RuntimeThreadChoiceSample sample,
        string? chosenThreadId,
        IDictionary<string, RuntimeBucket> buckets)
    {
        var selectedThreadId = !string.IsNullOrWhiteSpace(chosenThreadId)
            && sample.CandidateThreads.Any(candidate => string.Equals(candidate.ThreadId, chosenThreadId, StringComparison.OrdinalIgnoreCase))
            ? chosenThreadId!
            : sample.FallbackThreadId;

        RuntimeBucket bucket;
        if (!buckets.TryGetValue(selectedThreadId, out bucket!))
        {
            var template = sample.CandidateThreads.First(candidate => string.Equals(candidate.ThreadId, selectedThreadId, StringComparison.OrdinalIgnoreCase));
            bucket = new RuntimeBucket(template.ThreadId, template.DisplayName, template.IsGroup, message.Participants);
            buckets[selectedThreadId] = bucket;
        }

        var assigned = message with
        {
            ConversationId = bucket.ConversationId,
            ConversationDisplayName = bucket.DisplayName,
            IsGroup = bucket.IsGroup
        };
        bucket.AddMessage(assigned);
        return assigned;
    }

    private RuntimeThreadChoiceSample BuildSample(
        SynthesizedMessageRecord message,
        IReadOnlyDictionary<string, RuntimeBucket> buckets,
        IReadOnlyList<StoredNotificationRecord> notifications)
    {
        var targetPreview = BuildPreview(message.Message);
        var targetUtc = message.SortTimestampUtc;
        var targetParticipantKeys = message.Participants
            .Where(participant => !participant.IsSelf)
            .Select(participant => participant.Key)
            .Where(key => !string.IsNullOrWhiteSpace(key))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        var targetSenderKeys = BuildTargetSenderVariants(message);

        var candidateRows = new List<(RuntimeThreadChoiceCandidate Candidate, double Score)>();
        foreach (var bucket in buckets.Values)
        {
            var history = bucket.BuildHistory(options.ThreadChooserHistoryTurns);
            var lastHistory = history.LastOrDefault();
            var previewOverlap = TokenOverlap(targetPreview, lastHistory?.Preview);
            var participantOverlap = bucket.Participants
                .Count(participant => !participant.IsSelf && targetParticipantKeys.Contains(participant.Key));
            var deltaSeconds = targetUtc.HasValue && lastHistory?.ParsedSortUtc is not null
                ? Math.Abs((targetUtc.Value - lastHistory.ParsedSortUtc.Value).TotalSeconds)
                : (double?)null;
            var score = 0d;
            if (deltaSeconds is not null)
            {
                if (deltaSeconds <= 5 * 60)
                {
                    score += 4d;
                }
                else if (deltaSeconds <= 30 * 60)
                {
                    score += 3d;
                }
                else if (deltaSeconds <= 2 * 3600)
                {
                    score += 2d;
                }
                else if (deltaSeconds <= 12 * 3600)
                {
                    score += 1d;
                }
            }

            if (previewOverlap > 0)
            {
                score += previewOverlap * 4d;
            }

            if (participantOverlap > 0)
            {
                score += Math.Min(3d, participantOverlap * 1.5d);
            }

            if (bucket.IsGroup == message.IsGroup)
            {
                score += 0.5d;
            }

            if (LooksGeneric(targetPreview))
            {
                score += 0.5d;
            }

            var senderOverlap = lastHistory is not null
                && BuildHistorySenderVariants(lastHistory).Overlaps(targetSenderKeys);
            if (senderOverlap)
            {
                score += 1.5d;
            }

            if (score <= 0 && participantOverlap <= 0 && !senderOverlap)
            {
                continue;
            }

            candidateRows.Add((
                new RuntimeThreadChoiceCandidate(
                    bucket.ConversationId,
                    bucket.DisplayName,
                    bucket.IsGroup,
                    bucket.Participants.Select(ToRuntimeParticipant).ToArray(),
                    history,
                    new RuntimeCandidateFeatures(
                        previewOverlap,
                        deltaSeconds)),
                score));
        }

        candidateRows = candidateRows
            .OrderByDescending(item => item.Score)
            .ThenBy(item => item.Candidate.Features.DeltaSeconds ?? double.MaxValue)
            .Take(Math.Max(1, options.ThreadChooserMaxCandidates - 1))
            .ToList();

        if (!candidateRows.Any(item => string.Equals(item.Candidate.ThreadId, message.ConversationId, StringComparison.OrdinalIgnoreCase)))
        {
            var existingCurrentHistory = buckets.TryGetValue(message.ConversationId, out var currentBucket)
                ? currentBucket.BuildHistory(options.ThreadChooserHistoryTurns)
                : Array.Empty<RuntimeHistoryTurn>();
            var existingCurrentLast = existingCurrentHistory.LastOrDefault();
            candidateRows.Insert(
                0,
                (
                    new RuntimeThreadChoiceCandidate(
                        message.ConversationId,
                        message.ConversationDisplayName,
                        message.IsGroup,
                        message.Participants.Select(ToRuntimeParticipant).ToArray(),
                        existingCurrentHistory,
                        new RuntimeCandidateFeatures(
                            TokenOverlap(targetPreview, existingCurrentLast?.Preview),
                            targetUtc.HasValue && existingCurrentLast?.ParsedSortUtc is not null
                                ? Math.Abs((targetUtc.Value - existingCurrentLast.ParsedSortUtc.Value).TotalSeconds)
                                : null)),
                    double.MaxValue));
        }

        var nearbyNotifications = notifications
            .Where(notification => IsMessagesNotification(notification.Notification))
            .Where(notification => IsNear(targetUtc, notification.Notification.ReceivedAtUtc, TimeSpan.FromHours(2)))
            .Take(3)
            .Select(notification => new RuntimeNearbyNotification(
                notification.Notification.NotificationUid,
                notification.Notification.ReceivedAtUtc.ToString("O"),
                notification.Notification.Title,
                notification.Notification.Subtitle,
                notification.Notification.Message,
                notification.Notification.AppIdentifier))
            .ToArray();

        var candidates = candidateRows
            .Select(item => item.Candidate)
            .DistinctBy(candidate => candidate.ThreadId, StringComparer.OrdinalIgnoreCase)
            .Take(options.ThreadChooserMaxCandidates)
            .ToArray();

        if (candidates.Length == 0)
        {
            candidates =
            [
                new RuntimeThreadChoiceCandidate(
                    message.ConversationId,
                    message.ConversationDisplayName,
                    message.IsGroup,
                    message.Participants.Select(ToRuntimeParticipant).ToArray(),
                    Array.Empty<RuntimeHistoryTurn>(),
                    new RuntimeCandidateFeatures(0d, null))
            ];
        }

        return new RuntimeThreadChoiceSample(
            $"runtime::{message.MessageKey}",
            new RuntimeTargetMessage(
                message.SortTimestampUtc?.ToString("O"),
                message.IsGroup,
                message.Participants.Select(ToRuntimeParticipant).ToArray(),
                new RuntimeTargetPayload(
                    message.Message.Folder,
                    message.Message.Subject,
                    message.Message.Body,
                    message.Message.SenderName,
                    message.Message.SenderAddressing,
                    message.Message.Originators,
                    message.Message.Recipients)),
            candidates,
            nearbyNotifications,
            new RuntimeSampleMetadata(
                targetPreview,
                LooksGeneric(targetPreview)),
            message.ConversationId);
    }

    private static IReadOnlyList<ConversationSnapshot> BuildConversations(IReadOnlyList<SynthesizedMessageRecord> messages)
    {
        return messages
            .GroupBy(message => message.ConversationId, StringComparer.OrdinalIgnoreCase)
            .Select(group =>
            {
                var ordered = group
                    .OrderByDescending(message => message.SortTimestampUtc ?? DateTimeOffset.MinValue)
                    .ThenByDescending(message => message.Message.Datetime ?? string.Empty, StringComparer.OrdinalIgnoreCase)
                    .ToArray();
                var mergedParticipants = MergeParticipants(ordered.SelectMany(message => message.Participants));
                var latest = ordered[0];
                var latestPreview = ordered.FirstOrDefault(message => !string.IsNullOrWhiteSpace(BuildPreview(message.Message))) ?? latest;
                return new ConversationSnapshot(
                    latest.ConversationId,
                    latest.ConversationDisplayName,
                    ordered.Any(message => message.IsGroup) || mergedParticipants.Count(participant => !participant.IsSelf) > 1,
                    latest.SortTimestampUtc,
                    ordered.Length,
                    ordered.Count(message => IsUnread(message.Message)),
                    BuildPreview(latestPreview.Message),
                    mergedParticipants,
                    ordered
                        .Select(message => message.Message.Folder)
                        .Where(folder => !string.IsNullOrWhiteSpace(folder))
                        .Distinct(StringComparer.OrdinalIgnoreCase)
                        .OrderBy(folder => folder, StringComparer.OrdinalIgnoreCase)
                        .ToArray(),
                    ResolveLastSenderDisplayName(latestPreview));
            })
            .OrderByDescending(conversation => conversation.LastMessageUtc ?? DateTimeOffset.MinValue)
            .ThenByDescending(conversation => conversation.MessageCount)
            .ToArray();
    }

    private static IReadOnlyList<ConversationParticipantRecord> MergeParticipants(IEnumerable<ConversationParticipantRecord> participants)
    {
        return participants
            .GroupBy(participant => participant.Key, StringComparer.OrdinalIgnoreCase)
            .Select(group =>
            {
                var best = group
                    .OrderByDescending(participant => participant.Phones.Count + participant.Emails.Count)
                    .ThenByDescending(participant => participant.DisplayName?.Length ?? 0)
                    .First();
                return best with
                {
                    Phones = group
                        .SelectMany(participant => participant.Phones)
                        .Distinct(StringComparer.OrdinalIgnoreCase)
                        .OrderBy(phone => phone, StringComparer.OrdinalIgnoreCase)
                        .ToArray(),
                    Emails = group
                        .SelectMany(participant => participant.Emails)
                        .Distinct(StringComparer.OrdinalIgnoreCase)
                        .OrderBy(email => email, StringComparer.OrdinalIgnoreCase)
                        .ToArray(),
                    IsSelf = group.Any(participant => participant.IsSelf)
                };
            })
            .OrderBy(participant => participant.DisplayName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(participant => participant.Key, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static string? ResolveLastSenderDisplayName(SynthesizedMessageRecord message)
    {
        if (!string.IsNullOrWhiteSpace(message.Message.SenderName))
        {
            return message.Message.SenderName;
        }

        return message.Participants.FirstOrDefault(participant => !participant.IsSelf)?.DisplayName;
    }

    private static bool IsUnread(MessageRecord message)
    {
        return string.Equals(message.Folder, "inbox", StringComparison.OrdinalIgnoreCase)
            && (message.Read == false
                || string.Equals(message.Status, "Unread", StringComparison.OrdinalIgnoreCase));
    }

    private static string? BuildPreview(MessageRecord message)
    {
        var body = string.IsNullOrWhiteSpace(message.Body) ? message.Subject : message.Body;
        return BuildPreview(body);
    }

    private static string? BuildPreview(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var trimmed = value.Trim();
        return trimmed.Length <= 160 ? trimmed : $"{trimmed[..160]}...";
    }

    private static bool LooksGeneric(string? preview)
    {
        var normalized = NormalizeText(preview);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return true;
        }

        if (GenericPreviews.Contains(normalized))
        {
            return true;
        }

        var tokens = Tokenize(normalized);
        return tokens.Length <= 2 && normalized.Length <= 16;
    }

    private static string NormalizeText(string? value)
    {
        return string.IsNullOrWhiteSpace(value)
            ? string.Empty
            : Regex.Replace(value, "\\s+", " ").Trim().ToLowerInvariant();
    }

    private static string[] Tokenize(string? value)
    {
        return NormalizeText(value)
            .Replace(",", " ", StringComparison.Ordinal)
            .Replace(".", " ", StringComparison.Ordinal)
            .Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    }

    private static double TokenOverlap(string? left, string? right)
    {
        var leftTokens = Tokenize(left).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var rightTokens = Tokenize(right).ToHashSet(StringComparer.OrdinalIgnoreCase);
        if (leftTokens.Count == 0 || rightTokens.Count == 0)
        {
            return 0d;
        }

        return (double)leftTokens.Intersect(rightTokens, StringComparer.OrdinalIgnoreCase).Count()
            / leftTokens.Union(rightTokens, StringComparer.OrdinalIgnoreCase).Count();
    }

    private static HashSet<string> BuildTargetSenderVariants(SynthesizedMessageRecord message)
    {
        var variants = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        AddVariant(variants, message.Message.SenderName);
        AddVariant(variants, message.Message.SenderAddressing);
        foreach (var originator in message.Message.Originators)
        {
            AddVariant(variants, originator.Name);
            foreach (var phone in originator.Phones)
            {
                AddVariant(variants, phone);
            }
        }

        return variants;
    }

    private static HashSet<string> BuildHistorySenderVariants(RuntimeHistoryTurn historyTurn)
    {
        var variants = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        AddVariant(variants, historyTurn.SenderName);
        AddVariant(variants, historyTurn.SenderAddressing);
        return variants;
    }

    private static void AddVariant(ISet<string> variants, string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return;
        }

        var normalized = NormalizeText(raw);
        if (!string.IsNullOrWhiteSpace(normalized))
        {
            variants.Add(normalized);
        }

        var digits = new string(raw.Where(char.IsDigit).ToArray());
        if (!string.IsNullOrWhiteSpace(digits))
        {
            variants.Add(digits);
        }
    }

    private static bool IsNear(DateTimeOffset? reference, DateTimeOffset candidate, TimeSpan window)
    {
        return reference.HasValue && (reference.Value - candidate).Duration() <= window;
    }

    private static bool IsMessagesNotification(NotificationRecord notification)
    {
        return string.Equals(notification.AppIdentifier, "com.apple.MobileSMS", StringComparison.OrdinalIgnoreCase)
            || string.Equals(notification.AppIdentifier, "com.apple.MobileSMS.notification", StringComparison.OrdinalIgnoreCase);
    }

    private static RuntimeParticipant ToRuntimeParticipant(ConversationParticipantRecord participant)
    {
        return new RuntimeParticipant(
            participant.Key,
            participant.DisplayName,
            participant.Phones,
            participant.Emails,
            participant.IsSelf);
    }

    private Uri GetUri(string path)
    {
        return new($"{GetServiceRoot()}{path}", UriKind.Absolute);
    }

    private void TryStopServiceProcess()
    {
        try
        {
            if (serviceProcess is { HasExited: false })
            {
                serviceProcess.Kill(entireProcessTree: true);
                serviceProcess.WaitForExit(5000);
            }
        }
        catch
        {
            // best effort
        }
        finally
        {
            serviceProcess?.Dispose();
            serviceProcess = null;
            UpdateStatusSnapshot(
                current => current with
                {
                    SidecarRunning = false,
                    ServiceHealthy = false,
                    ProcessId = null,
                    Status = current.Enabled && current.ScriptExists && current.CheckpointExists ? "idle" : current.Status,
                    Reason = current.Enabled && current.ScriptExists && current.CheckpointExists
                        ? "Thread chooser is enabled and will start on demand during sync."
                        : current.Reason
                });
        }
    }

    private void DumpSampleForDebug(RuntimeThreadChoiceSample sample)
    {
        if (Interlocked.Increment(ref sampleDumpCount) != 1)
        {
            return;
        }

        try
        {
            var path = Path.Combine(Path.GetTempPath(), "adit-thread-runtime-sample.json");
            File.WriteAllText(path, JsonSerializer.Serialize(sample, JsonOptions));
            logger.LogWarning("Dumped first failing runtime chooser sample to {Path}", path);
        }
        catch (Exception exception)
        {
            logger.LogDebug(exception, "Failed to dump runtime chooser sample.");
        }
    }

    private sealed class RuntimeBucket
    {
        private readonly Dictionary<string, ConversationParticipantRecord> participants = new(StringComparer.OrdinalIgnoreCase);
        private readonly List<SynthesizedMessageRecord> messages = [];

        public RuntimeBucket(
            string conversationId,
            string displayName,
            bool isGroup,
            IEnumerable<ConversationParticipantRecord> seedParticipants)
        {
            ConversationId = conversationId;
            DisplayName = displayName;
            IsGroup = isGroup;
            foreach (var participant in seedParticipants)
            {
                participants[participant.Key] = participant;
            }
        }

        public string ConversationId { get; }

        public string DisplayName { get; private set; }

        public bool IsGroup { get; private set; }

        public IReadOnlyList<ConversationParticipantRecord> Participants => participants.Values.OrderBy(participant => participant.DisplayName, StringComparer.OrdinalIgnoreCase).ToArray();

        public void AddMessage(SynthesizedMessageRecord message)
        {
            messages.Add(message);
            foreach (var participant in message.Participants)
            {
                if (!participants.TryGetValue(participant.Key, out var current))
                {
                    participants[participant.Key] = participant;
                    continue;
                }

                participants[participant.Key] = current with
                {
                    DisplayName = current.DisplayName.Length >= participant.DisplayName.Length
                        ? current.DisplayName
                        : participant.DisplayName,
                    Phones = current.Phones
                        .Concat(participant.Phones)
                        .Distinct(StringComparer.OrdinalIgnoreCase)
                        .OrderBy(phone => phone, StringComparer.OrdinalIgnoreCase)
                        .ToArray(),
                    Emails = current.Emails
                        .Concat(participant.Emails)
                        .Distinct(StringComparer.OrdinalIgnoreCase)
                        .OrderBy(email => email, StringComparer.OrdinalIgnoreCase)
                        .ToArray(),
                    IsSelf = current.IsSelf || participant.IsSelf
                };
            }

            if (message.IsGroup)
            {
                IsGroup = true;
            }

            if (string.IsNullOrWhiteSpace(DisplayName)
                || (!string.IsNullOrWhiteSpace(message.ConversationDisplayName)
                    && message.ConversationDisplayName.Length > DisplayName.Length))
            {
                DisplayName = message.ConversationDisplayName;
            }
        }

        public IReadOnlyList<RuntimeHistoryTurn> BuildHistory(int maxTurns)
        {
            return messages
                .OrderBy(message => message.SortTimestampUtc ?? DateTimeOffset.MinValue)
                .ThenBy(message => message.MessageKey, StringComparer.OrdinalIgnoreCase)
                .TakeLast(maxTurns)
                .Select(message => new RuntimeHistoryTurn(
                    message.MessageKey,
                    message.SortTimestampUtc?.ToString("O"),
                    BuildPreview(message.Message),
                    message.Message.SenderName,
                    message.Message.SenderAddressing,
                    message.Message.Body,
                    message.Message.Folder,
                    message.Message.Status))
                .ToArray();
        }
    }

    private sealed record RuntimeThreadChoiceSample(
        string SampleId,
        RuntimeTargetMessage Message,
        IReadOnlyList<RuntimeThreadChoiceCandidate> CandidateThreads,
        IReadOnlyList<RuntimeNearbyNotification> NearbyNotifications,
        RuntimeSampleMetadata Metadata,
        string FallbackThreadId);

    private sealed record RuntimeTargetMessage(
        string? SortUtc,
        bool IsGroup,
        IReadOnlyList<RuntimeParticipant> Participants,
        RuntimeTargetPayload Message);

    private sealed record RuntimeTargetPayload(
        string Folder,
        string? Subject,
        string? Body,
        [property: System.Text.Json.Serialization.JsonPropertyName("senderName")]
        string? SenderName,
        [property: System.Text.Json.Serialization.JsonPropertyName("senderAddressing")]
        string? SenderAddressing,
        IReadOnlyList<MessageParticipantRecord> Originators,
        IReadOnlyList<MessageParticipantRecord> Recipients);

    private sealed record RuntimeThreadChoiceCandidate(
        string ThreadId,
        string DisplayName,
        bool IsGroup,
        IReadOnlyList<RuntimeParticipant> Participants,
        IReadOnlyList<RuntimeHistoryTurn> History,
        RuntimeCandidateFeatures Features);

    private sealed record RuntimeParticipant(
        string Key,
        [property: System.Text.Json.Serialization.JsonPropertyName("displayName")]
        string DisplayName,
        IReadOnlyList<string> Phones,
        IReadOnlyList<string> Emails,
        [property: System.Text.Json.Serialization.JsonPropertyName("isSelf")]
        bool IsSelf);

    private sealed record RuntimeHistoryTurn(
        string MessageKey,
        string? SortUtc,
        string? Preview,
        string? SenderName,
        string? SenderAddressing,
        string? Body,
        string Folder,
        string? Status)
    {
        [System.Text.Json.Serialization.JsonIgnore]
        public DateTimeOffset? ParsedSortUtc => DateTimeOffset.TryParse(SortUtc, null, DateTimeStyles.RoundtripKind, out var parsed) ? parsed : null;
    }

    private sealed record RuntimeCandidateFeatures(
        double PreviewOverlap,
        double? DeltaSeconds);

    private sealed record RuntimeNearbyNotification(
        uint NotificationUid,
        string ReceivedUtc,
        string? Title,
        string? Subtitle,
        string? Message,
        string? AppIdentifier);

    private sealed record RuntimeSampleMetadata(
        string? Preview,
        bool GenericPreview);

    private sealed record ThreadChooserHealthResponse(
        string Status,
        string Checkpoint,
        string ModelName,
        string? SemanticCache,
        string Device,
        string Dtype,
        int MaxHistoryTurns,
        bool IncludeCandidateScore,
        bool IncludeCandidateDisplayNameInQwen,
        ThreadChooserHealthHead Head);

    private sealed record ThreadChooserHealthHead(
        int StructDim,
        int SemanticScalarDim,
        int SemanticVecDim,
        int HiddenDim,
        int CandidateEncoderLayers,
        int CandidateEncoderHeads,
        bool SemanticResidualLogit);

    private sealed record RuntimeScoreResponse(
        string SampleId,
        int CandidateCount,
        int PredictedIndex,
        string PredictedThreadId,
        IReadOnlyList<double> Scores,
        IReadOnlyList<double> Probabilities,
        IReadOnlyList<RuntimeCandidateScore> Candidates);

    private sealed record RuntimeCandidateScore(
        int Index,
        string ThreadId,
        bool IsGroup,
        double Probability,
        double Logit,
        IReadOnlyDictionary<string, double> Semantic);
}
