import { useState, useEffect } from "react";
import { useApp } from "../context/AppContext";
import { timeAgo, cn, normalizeSessionPhase } from "../lib/utils";

export function StatusStrip() {
  const { state } = useApp();
  const [, setTick] = useState(0);

  // Update "X ago" every 5 seconds
  useEffect(() => {
    const id = setInterval(() => setTick((t) => t + 1), 5000);
    return () => clearInterval(id);
  }, []);

  const runtime = state.runtime;
  const target = runtime?.target;
  const phase = runtime?.phase ?? "starting";
  const ancsPhase = normalizeSessionPhase(runtime?.ancsSession?.phase);
  const errorText = state.error?.trim();
  const notificationsDegraded =
    !!runtime?.notificationsEnabled &&
    ancsPhase !== "Connected" &&
    ancsPhase !== "Unknown";

  const dotColor =
    errorText
      ? "bg-error"
      : notificationsDegraded
      ? "bg-syncing"
      : state.connectionStatus === "connected" && (phase === "ready" || phase === "syncing")
      ? phase === "syncing"
        ? "bg-syncing"
        : "bg-success"
      : state.connectionStatus === "disconnected"
        ? "bg-error"
        : "bg-syncing";

  const statusText =
    errorText
      ? errorText
      : notificationsDegraded && target
        ? `Connected to ${target.name} · notifications ${ancsPhase.toLowerCase()}`
      : state.connectionStatus === "disconnected"
        ? "Disconnected"
        : target
          ? `Connected to ${target.name}`
          : "Waiting for device";

  const syncText = !errorText && runtime?.lastSuccessfulSyncUtc
    ? `Last sync: ${timeAgo(runtime.lastSuccessfulSyncUtc)}`
    : "";

  return (
    <div
      className={cn(
        "h-7 flex items-center gap-2 px-4 border-t border-border bg-sidebar text-[11px] text-text-secondary shrink-0",
      )}
      style={{ fontFamily: "var(--font-mono)" }}
    >
      <span className={cn("w-2 h-2 rounded-full shrink-0", dotColor)} />
      <span className="truncate">{statusText}</span>
      {syncText && (
        <>
          <span className="text-border">|</span>
          <span>{syncText}</span>
        </>
      )}
    </div>
  );
}
