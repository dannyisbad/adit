import { useState, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { useApp } from "../context/AppContext";
import { api } from "../lib/api";
import { useEvent } from "../lib/ws";
import { timeAgo, cn, normalizeSessionPhase } from "../lib/utils";
import { CapabilityCard } from "../components/CapabilityCard";
import { EventLog } from "../components/EventLog";
import { ContactsList } from "../components/ContactsList";
import { ThreadChooserStatusCard } from "../components/ThreadChooserStatusCard";
import type {
  DoctorSnapshot,
  DaemonCapabilitiesSnapshot,
  NotificationsBootstrapSnapshot,
  ThreadChooserStatusSnapshot,
} from "../lib/types";

type SettingsTab = "status" | "events" | "contacts";

const TABS: { id: SettingsTab; label: string }[] = [
  { id: "status", label: "Status" },
  { id: "events", label: "Event Log" },
  { id: "contacts", label: "Contacts" },
];

export function Settings() {
  const navigate = useNavigate();
  const { state } = useApp();
  const [tab, setTab] = useState<SettingsTab>("status");
  const [doctor, setDoctor] = useState<DoctorSnapshot | null>(null);
  const [capabilities, setCapabilities] = useState<DaemonCapabilitiesSnapshot | null>(null);
  const [notifBootstrap, setNotifBootstrap] = useState<NotificationsBootstrapSnapshot | null>(null);
  const [threadChooser, setThreadChooser] = useState<ThreadChooserStatusSnapshot | null>(null);
  const [syncing, setSyncing] = useState(false);
  const [togglingNotifs, setTogglingNotifs] = useState(false);
  const [, setTick] = useState(0);

  const fetchStatus = useCallback(async () => {
    try {
      const [doctorRes, capRes, threadChooserRes] = await Promise.all([
        api.getDoctor(),
        api.getCapabilities(),
        api.getThreadChooserStatus(),
      ]);
      setDoctor(doctorRes.doctor);
      setCapabilities(capRes.capabilities);
      setNotifBootstrap(capRes.notificationsBootstrap);
      setThreadChooser(threadChooserRes);
    } catch {
      // silent
    }
  }, []);

  useEffect(() => {
    fetchStatus();
  }, [fetchStatus]);

  useEvent("runtime.updated", () => fetchStatus());
  useEvent("sync.completed", () => {
    fetchStatus();
    setSyncing(false);
  });
  useEvent("transport.state", () => fetchStatus());
  useEvent("*", (event) => {
    if (event.type.startsWith("thread_chooser.")) {
      fetchStatus();
    }
  });

  useEffect(() => {
    const id = setInterval(() => setTick((t) => t + 1), 5000);
    return () => clearInterval(id);
  }, []);

  const handleSync = useCallback(async () => {
    setSyncing(true);
    try {
      await api.triggerSync("settings_manual");
    } catch {
      setSyncing(false);
    }
  }, []);

  const handleToggleNotifications = useCallback(async () => {
    setTogglingNotifs(true);
    try {
      if (state.runtime?.notificationsEnabled) {
        await api.disableNotifications();
      } else {
        await api.enableNotifications();
      }
      await fetchStatus();
    } catch {
      // silent
    } finally {
      setTogglingNotifs(false);
    }
  }, [state.runtime?.notificationsEnabled, fetchStatus]);

  const runtime = state.runtime;
  const target = runtime?.target;

  const healthTone: HealthTone =
    doctor?.overall === "ready"
      ? "success"
      : doctor?.overall === "degraded"
        ? "syncing"
        : doctor?.overall === "waiting_for_device"
          ? "error"
          : "neutral";

  return (
    <div className="flex flex-col h-full bg-base">
      {/* Header */}
      <div className="flex items-center gap-3 px-6 py-4 border-b border-border bg-sidebar shrink-0">
        <button
          onClick={() => navigate("/")}
          className="text-accent hover:text-accent-hover transition-colors"
        >
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="15 18 9 12 15 6" />
          </svg>
        </button>
        <h1
          className="text-xl font-semibold text-text-primary"
          style={{ fontFamily: "var(--font-display)" }}
        >
          Settings
        </h1>
      </div>

      {/* Tabs */}
      <div className="flex items-center gap-6 px-6 border-b border-border bg-sidebar shrink-0">
        {TABS.map((t) => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={cn(
              "relative py-3 text-sm transition-colors",
              tab === t.id
                ? "text-text-primary font-medium"
                : "text-text-secondary hover:text-text-primary",
            )}
          >
            {t.label}
            <div
              className={cn(
                "absolute bottom-0 left-0 right-0 h-0.5 bg-accent rounded-full transition-opacity",
                tab === t.id ? "opacity-100" : "opacity-0",
              )}
            />
          </button>
        ))}
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto min-h-0">
        {tab === "status" ? (
          <div className="max-w-2xl mx-auto p-6 space-y-6">
            {/* Health Banner */}
            {doctor && <HealthBanner doctor={doctor} tone={healthTone} />}

            {/* Capabilities */}
            {capabilities && (
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
                <CapabilityCard label="Messaging" sublabel="MAP" capability={capabilities.messaging} />
                <CapabilityCard label="Contacts" sublabel="PBAP" capability={capabilities.contacts} />
                <CapabilityCard label="Notifications" sublabel="ANCS" capability={capabilities.notifications} beta />
              </div>
            )}

            {/* Connection: Device + Transport Sessions */}
            <Section title="Connection">
              <div className="bg-surface rounded-xl border border-border p-5">
                {target ? (
                  <div className="space-y-4">
                    <div>
                      <p
                        className="text-[11px] font-medium text-text-secondary mb-2.5 uppercase tracking-wider"
                        style={{ fontFamily: "var(--font-mono)" }}
                      >
                        Device
                      </p>
                      <div className="space-y-2.5">
                        <InfoRow label="Name" value={target.name} />
                        <InfoRow label="Transport" value={target.transport} mono />
                        <InfoRow label="Device ID" value={target.id} mono />
                        {target.bluetoothAddress && (
                          <InfoRow label="Bluetooth" value={target.bluetoothAddress} mono />
                        )}
                        <InfoRow
                          label="Connected"
                          value={target.isConnected ? "Yes" : "No"}
                          color={target.isConnected ? "text-success" : "text-error"}
                        />
                      </div>
                    </div>

                    <div className="border-t border-border" />

                    <div>
                      <p
                        className="text-[11px] font-medium text-text-secondary mb-2.5 uppercase tracking-wider"
                        style={{ fontFamily: "var(--font-mono)" }}
                      >
                        Sessions
                      </p>
                      <div className="space-y-3">
                        <SessionRow label="MAP (Messaging)" session={runtime?.mapSession} />
                        <SessionRow label="ANCS (Notifications)" session={runtime?.ancsSession} />
                      </div>
                    </div>
                  </div>
                ) : (
                  <p className="text-sm text-text-secondary">No device connected.</p>
                )}
              </div>
            </Section>

            {/* Sync + Notifications side by side */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <Section title="Sync">
                <div className="bg-surface rounded-xl border border-border p-5 space-y-2.5 h-full flex flex-col">
                  <div className="space-y-2.5 flex-1">
                    <InfoRow label="Phase" value={runtime?.phase ?? "unknown"} mono />
                    <InfoRow label="Last Reason" value={runtime?.lastReason ?? "\u2014"} mono />
                    <InfoRow label="Last Sync" value={timeAgo(runtime?.lastSuccessfulSyncUtc)} mono />
                    <InfoRow label="Contacts Refresh" value={timeAgo(runtime?.lastContactsRefreshUtc)} mono />
                    <InfoRow
                      label="Failures"
                      value={String(runtime?.consecutiveFailures ?? 0)}
                      mono
                      color={(runtime?.consecutiveFailures ?? 0) > 0 ? "text-error" : undefined}
                    />
                    {runtime?.lastError && (
                      <div>
                        <p className="text-[11px] font-medium text-error mb-1">Last Error:</p>
                        <pre
                          className="text-[10px] text-text-secondary bg-base rounded p-2 overflow-x-auto max-h-24 leading-relaxed"
                          style={{ fontFamily: "var(--font-mono)" }}
                        >
                          {runtime.lastError}
                        </pre>
                      </div>
                    )}
                  </div>
                  <button
                    onClick={handleSync}
                    disabled={syncing}
                    className="w-full py-2 rounded-lg bg-accent text-white text-sm hover:bg-accent-hover transition-colors disabled:opacity-50"
                  >
                    {syncing ? "Syncing\u2026" : "Sync Now"}
                  </button>
                </div>
              </Section>

              <Section title="Notifications" badge="Beta">
                <div className="bg-surface rounded-xl border border-border p-5 space-y-2.5 h-full flex flex-col">
                  <div className="space-y-2.5 flex-1">
                    <InfoRow
                      label="Enabled"
                      value={runtime?.notificationsEnabled ? "Yes" : "No"}
                      color={runtime?.notificationsEnabled ? "text-success" : "text-text-secondary"}
                    />
                    <InfoRow label="Mode" value={runtime?.notificationsMode ?? "\u2014"} mono />
                    <InfoRow label="Count" value={String(runtime?.notificationCount ?? 0)} mono />
                    {notifBootstrap && (
                      <>
                        <InfoRow label="Bootstrap" value={notifBootstrap.state} mono />
                        {notifBootstrap.reason && (
                          <p className="text-[11px] text-text-secondary leading-relaxed">{notifBootstrap.reason}</p>
                        )}
                      </>
                    )}
                  </div>
                  <button
                    onClick={handleToggleNotifications}
                    disabled={togglingNotifs}
                    className={cn(
                      "w-full py-2 rounded-lg text-sm transition-colors disabled:opacity-50",
                      runtime?.notificationsEnabled
                        ? "bg-base border border-border text-text-primary hover:bg-base/80"
                        : "bg-accent text-white hover:bg-accent-hover",
                    )}
                  >
                    {togglingNotifs
                      ? "Updating\u2026"
                      : runtime?.notificationsEnabled
                        ? "Disable Notifications"
                        : "Enable Notifications"}
                  </button>
                </div>
              </Section>
            </div>

            {/* Thread Chooser Model */}
            <Section title="Model">
              <ThreadChooserStatusCard status={threadChooser} />
            </Section>

            {/* Cache Stats */}
            <Section title="Cache">
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                <StatCard label="Contacts" value={runtime?.contactCount ?? 0} />
                <StatCard label="Messages" value={runtime?.messageCount ?? 0} />
                <StatCard label="Conversations" value={runtime?.conversationCount ?? 0} />
                <StatCard label="Notifications" value={runtime?.notificationCount ?? 0} />
              </div>
            </Section>

            {/* Raw Runtime */}
            <details className="bg-surface rounded-xl border border-border">
              <summary
                className="px-5 py-3 text-xs text-text-secondary cursor-pointer hover:text-text-primary transition-colors"
                style={{ fontFamily: "var(--font-mono)" }}
              >
                runtime snapshot
              </summary>
              <pre
                className="px-5 py-3 border-t border-border text-[11px] text-text-secondary overflow-x-auto max-h-96 leading-relaxed"
                style={{ fontFamily: "var(--font-mono)" }}
              >
                {JSON.stringify(runtime, null, 2)}
              </pre>
            </details>
          </div>
        ) : tab === "events" ? (
          <EventLog />
        ) : (
          <ContactsList />
        )}
      </div>
    </div>
  );
}

/* ---- Helper Components ---- */

type HealthTone = "success" | "syncing" | "error" | "neutral";

const HEALTH_COLORS: Record<HealthTone, { border: string; bg: string; dot: string; text: string }> = {
  success: { border: "border-success/30", bg: "bg-success/5", dot: "bg-success", text: "text-success" },
  syncing: { border: "border-syncing/30", bg: "bg-syncing/5", dot: "bg-syncing", text: "text-syncing" },
  error: { border: "border-error/30", bg: "bg-error/5", dot: "bg-error", text: "text-error" },
  neutral: { border: "border-border", bg: "bg-surface", dot: "bg-text-secondary", text: "text-text-secondary" },
};

function HealthBanner({ doctor, tone }: { doctor: DoctorSnapshot; tone: HealthTone }) {
  const c = HEALTH_COLORS[tone];

  return (
    <div className={cn("rounded-xl border p-5", c.border, c.bg)}>
      <div className="flex items-start gap-3">
        <span className={cn("w-2.5 h-2.5 rounded-full mt-1 shrink-0", c.dot)} />
        <div className="min-w-0">
          <span
            className={cn("text-sm font-semibold uppercase", c.text)}
            style={{ fontFamily: "var(--font-mono)" }}
          >
            {doctor.overall}
          </span>
          <p className="text-sm text-text-secondary leading-relaxed mt-1">
            {doctor.summary}
          </p>
          {doctor.nextSteps.length > 0 && (
            <div className="mt-3 space-y-1">
              {doctor.nextSteps.map((step, i) => (
                <p
                  key={i}
                  className="text-xs text-text-secondary leading-relaxed pl-3 border-l-2 border-border"
                >
                  {step}
                </p>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function Section({
  title,
  badge,
  children,
}: {
  title: string;
  badge?: string;
  children: React.ReactNode;
}) {
  return (
    <div>
      <div className="flex items-center gap-2 mb-2.5">
        <h2
          className="text-base font-medium text-text-primary"
          style={{ fontFamily: "var(--font-display)" }}
        >
          {title}
        </h2>
        {badge && (
          <span
            className="text-[9px] px-1.5 py-0.5 rounded-full bg-syncing/15 text-syncing font-semibold uppercase tracking-wider"
            style={{ fontFamily: "var(--font-mono)" }}
          >
            {badge}
          </span>
        )}
      </div>
      {children}
    </div>
  );
}

function InfoRow({
  label,
  value,
  mono,
  color,
}: {
  label: string;
  value: string;
  mono?: boolean;
  color?: string;
}) {
  return (
    <div className="flex items-center justify-between gap-4">
      <span className="text-xs text-text-secondary shrink-0">{label}</span>
      <span
        className={cn("text-xs text-right truncate max-w-[60%]", color ?? "text-text-primary")}
        style={mono ? { fontFamily: "var(--font-mono)" } : undefined}
      >
        {value}
      </span>
    </div>
  );
}

function SessionRow({
  label,
  session,
}: {
  label: string;
  session?: { phase: string | number; timestampUtc: string; detail?: string | null; error?: string | null } | null;
}) {
  if (!session) {
    return (
      <div className="flex items-center justify-between">
        <span className="text-xs font-medium text-text-primary">{label}</span>
        <span
          className="text-[11px] text-text-secondary"
          style={{ fontFamily: "var(--font-mono)" }}
        >
          No session
        </span>
      </div>
    );
  }

  const phase = normalizeSessionPhase(session.phase);
  const phaseColor =
    phase === "Connected"
      ? "text-success"
      : phase === "Connecting"
        ? "text-syncing"
        : phase === "Faulted"
          ? "text-error"
          : "text-text-secondary";

  return (
    <div>
      <div className="flex items-center justify-between">
        <span className="text-xs font-medium text-text-primary">{label}</span>
        <span
          className={cn("text-[11px] font-medium", phaseColor)}
          style={{ fontFamily: "var(--font-mono)" }}
        >
          {phase}
        </span>
      </div>
      {session.detail && (
        <p className="text-[11px] text-text-secondary mt-0.5">{session.detail}</p>
      )}
      {session.error && (
        <p className="text-[11px] text-error mt-0.5 truncate">{session.error}</p>
      )}
    </div>
  );
}

function StatCard({ label, value }: { label: string; value: number }) {
  return (
    <div className="bg-surface rounded-xl border border-border p-3.5 text-center">
      <p
        className="text-lg font-semibold text-text-primary"
        style={{ fontFamily: "var(--font-mono)" }}
      >
        {value.toLocaleString()}
      </p>
      <p className="text-[11px] text-text-secondary mt-0.5">{label}</p>
    </div>
  );
}
