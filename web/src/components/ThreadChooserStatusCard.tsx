import type {
  ThreadChooserCandidateScoreSnapshot,
  ThreadChooserStatusSnapshot,
} from "../lib/types";
import { cn, timeAgo } from "../lib/utils";

interface ThreadChooserStatusCardProps {
  status: ThreadChooserStatusSnapshot | null;
  compact?: boolean;
}

const STATUS_TONE: Record<string, string> = {
  ready: "text-success",
  starting: "text-syncing",
  idle: "text-text-secondary",
  disabled: "text-text-secondary",
  missing_artifacts: "text-error",
  unhealthy: "text-error",
};

function formatProbability(value: number): string {
  return `${(value * 100).toFixed(1)}%`;
}

function formatNumber(value: number): string {
  return Number.isInteger(value) ? String(value) : value.toFixed(3);
}

function summarizeSemantic(candidate: ThreadChooserCandidateScoreSnapshot): string {
  const orderedKeys = ["lift", "cond_nll", "base_nll", "semantic_ok", "target_len_log1p"];
  const entries = orderedKeys
    .filter((key) => key in candidate.semantic)
    .map((key) => `${key}=${formatNumber(candidate.semantic[key] ?? 0)}`);
  return entries.slice(0, 3).join(" | ");
}

export function ThreadChooserStatusCard({
  status,
  compact = false,
}: ThreadChooserStatusCardProps) {
  if (!status) {
    return (
      <div className="bg-surface rounded-xl border border-border p-4 text-sm text-text-secondary">
        Loading thread chooser status...
      </div>
    );
  }

  const tone = STATUS_TONE[status.status] ?? "text-text-secondary";
  const currentModel = status.resolvedModelName ?? status.configuredModelName;
  const lastRun = status.lastRun;
  const lastScore = lastRun?.lastScore ?? null;
  const topCandidates = [...(lastScore?.candidates ?? [])]
    .sort((left, right) => right.probability - left.probability)
    .slice(0, compact ? 2 : 3);

  return (
    <div className="bg-surface rounded-xl border border-border p-4 space-y-4">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div
            className={cn("text-sm font-semibold uppercase", tone)}
            style={{ fontFamily: "var(--font-mono)" }}
          >
            {status.status}
          </div>
          <p className="text-sm text-text-secondary leading-relaxed mt-1">
            {status.reason ?? "No model detail reported."}
          </p>
        </div>
        <div
          className="text-[11px] text-text-secondary text-right shrink-0"
          style={{ fontFamily: "var(--font-mono)" }}
        >
          {status.lastHealthCheckUtc ? `health ${timeAgo(status.lastHealthCheckUtc)}` : "no health probe yet"}
        </div>
      </div>

      <div className={cn("grid gap-3", compact ? "grid-cols-2" : "grid-cols-2 md:grid-cols-3")}>
        <Metric label="Enabled" value={status.enabled ? "Yes" : "No"} />
        <Metric label="Healthy" value={status.serviceHealthy ? "Yes" : "No"} />
        <Metric label="Running" value={status.sidecarRunning ? "Yes" : "No"} />
        <Metric label="Model" value={currentModel} mono />
        <Metric label="Port" value={String(status.port)} mono />
        <Metric label="History Turns" value={String(status.historyTurns)} mono />
      </div>

      {status.head && !compact && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <Metric label="Struct Dim" value={String(status.head.structDim)} mono />
          <Metric label="Scalar Dim" value={String(status.head.semanticScalarDim)} mono />
          <Metric label="Vector Dim" value={String(status.head.semanticVecDim)} mono />
          <Metric label="Encoder Heads" value={String(status.head.candidateEncoderHeads)} mono />
        </div>
      )}

      {lastRun && (
        <div className="space-y-2">
          <div
            className="text-[11px] text-text-secondary uppercase tracking-wider"
            style={{ fontFamily: "var(--font-mono)" }}
          >
            Last Run
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <Metric label="When" value={timeAgo(lastRun.timestampUtc)} mono />
            <Metric label="Messages" value={String(lastRun.totalMessages)} mono />
            <Metric label="Scored" value={String(lastRun.scoredSamples)} mono />
            <Metric label="Reranked" value={String(lastRun.rerankedMessages)} mono />
          </div>
        </div>
      )}

      {lastScore && (
        <div className="space-y-2">
          <div
            className="text-[11px] text-text-secondary uppercase tracking-wider"
            style={{ fontFamily: "var(--font-mono)" }}
          >
            Last Score
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <Metric label="Sample" value={lastScore.sampleId} mono />
            <Metric label="Predicted Thread" value={lastScore.predictedThreadId} mono />
            <Metric label="Candidates" value={String(lastScore.candidateCount)} mono />
            <Metric label="When" value={timeAgo(lastScore.timestampUtc)} mono />
          </div>
          {topCandidates.length > 0 && (
            <div className="space-y-2">
              {topCandidates.map((candidate) => (
                <div
                  key={`${candidate.threadId}-${candidate.index}`}
                  className="bg-base rounded-lg border border-border px-3 py-2"
                >
                  <div className="flex items-center justify-between gap-3">
                    <div
                      className="text-xs text-text-primary truncate"
                      style={{ fontFamily: "var(--font-mono)" }}
                    >
                      {candidate.threadId}
                    </div>
                    <div
                      className="text-[11px] text-text-secondary shrink-0"
                      style={{ fontFamily: "var(--font-mono)" }}
                    >
                      {formatProbability(candidate.probability)} | logit {formatNumber(candidate.logit)}
                    </div>
                  </div>
                  {!compact && (
                    <div className="text-[11px] text-text-secondary mt-1 leading-relaxed">
                      {summarizeSemantic(candidate) || "No semantic scalars returned."}
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {!compact && (
        <details className="rounded-lg border border-border bg-base">
          <summary
            className="px-3 py-2 text-[11px] text-text-secondary cursor-pointer"
            style={{ fontFamily: "var(--font-mono)" }}
          >
            thread chooser details
          </summary>
          <div className="px-3 py-2 border-t border-border space-y-2 text-[11px] text-text-secondary">
            <div style={{ fontFamily: "var(--font-mono)" }}>python: {status.pythonPath}</div>
            <div style={{ fontFamily: "var(--font-mono)" }}>service: {status.serviceUrl}</div>
            <div style={{ fontFamily: "var(--font-mono)" }}>script: {status.scriptPath}</div>
            <div style={{ fontFamily: "var(--font-mono)" }}>checkpoint: {status.checkpointPath}</div>
            {status.semanticCachePath && (
              <div style={{ fontFamily: "var(--font-mono)" }}>semantic cache: {status.semanticCachePath}</div>
            )}
            {status.lastError && (
              <pre
                className="bg-surface rounded p-2 overflow-x-auto whitespace-pre-wrap"
                style={{ fontFamily: "var(--font-mono)" }}
              >
                {status.lastError}
              </pre>
            )}
          </div>
        </details>
      )}
    </div>
  );
}

function Metric({
  label,
  value,
  mono,
}: {
  label: string;
  value: string;
  mono?: boolean;
}) {
  return (
    <div className="bg-base rounded-lg border border-border px-3 py-2">
      <div className="text-[11px] text-text-secondary">{label}</div>
      <div
        className="text-xs text-text-primary mt-1 truncate"
        style={mono ? { fontFamily: "var(--font-mono)" } : undefined}
      >
        {value}
      </div>
    </div>
  );
}
