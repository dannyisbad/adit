import type { CapabilityStateRecord } from "../lib/types";
import { cn } from "../lib/utils";

interface CapabilityCardProps {
  label: string;
  sublabel?: string;
  capability: CapabilityStateRecord;
  beta?: boolean;
}

const stateColors: Record<string, string> = {
  ready: "bg-success",
  cached: "bg-success",
  starting: "bg-syncing",
  adopting: "bg-syncing",
  no_device: "bg-error",
  disabled: "bg-text-secondary",
  not_ready: "bg-error",
};

const stateLabels: Record<string, string> = {
  ready: "Ready",
  cached: "Cached",
  starting: "Starting",
  adopting: "Adopting",
  no_device: "No Device",
  disabled: "Disabled",
  not_ready: "Not Ready",
};

export function CapabilityCard({ label, sublabel, capability, beta }: CapabilityCardProps) {
  const dotColor = stateColors[capability.state] ?? "bg-text-secondary";
  const stateLabel = stateLabels[capability.state] ?? capability.state;
  const isAttentionState =
    capability.state === "not_ready" ||
    capability.state === "no_device" ||
    capability.state === "adopting" ||
    capability.state === "starting";

  return (
    <div className="bg-surface rounded-xl border border-border p-3.5">
      <div className="flex items-center gap-2 mb-1.5">
        <span className={cn("w-2 h-2 rounded-full shrink-0", dotColor)} />
        <span className="text-sm font-medium text-text-primary truncate">{label}</span>
        {beta && (
          <span
            className="text-[9px] px-1.5 py-0.5 rounded-full bg-syncing/15 text-syncing font-semibold uppercase tracking-wider shrink-0"
            style={{ fontFamily: "var(--font-mono)" }}
          >
            Beta
          </span>
        )}
      </div>
      <div
        className="text-[11px] text-text-secondary leading-relaxed"
        style={{ fontFamily: "var(--font-mono)" }}
      >
        {sublabel && <>{sublabel} &middot; </>}{stateLabel}
      </div>
      {capability.reason && (
        <p className="text-[11px] text-text-secondary leading-relaxed mt-1.5">{capability.reason}</p>
      )}
      {capability.recommendedAction && isAttentionState && (
        <p className="mt-1 text-[11px] text-text-primary leading-relaxed">
          {capability.recommendedAction}
        </p>
      )}
      {capability.detail && capability.detail !== capability.reason && (
        <p
          className="mt-1 text-[10px] text-text-secondary"
          style={{ fontFamily: "var(--font-mono)" }}
        >
          {capability.detail}
        </p>
      )}
      {capability.stability === "prototype" && !beta && (
        <span
          className="inline-block mt-1.5 text-[10px] px-1.5 py-0.5 rounded bg-syncing/10 text-syncing"
          style={{ fontFamily: "var(--font-mono)" }}
        >
          prototype
        </span>
      )}
    </div>
  );
}
