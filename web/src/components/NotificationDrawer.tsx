import { useState, useEffect, useCallback, useMemo } from "react";
import { api } from "../lib/api";
import { useEvent } from "../lib/ws";
import { useApp } from "../context/AppContext";
import {
  notificationTime,
  humanizeAppId,
  hashColor,
  cn,
  normalizeSessionPhase,
} from "../lib/utils";
import type { StoredNotificationRecord } from "../lib/types";
import { NotificationCategoryLabels } from "../lib/types";

interface NotificationDrawerProps {
  open: boolean;
  onClose: () => void;
}

export function NotificationDrawer({ open, onClose }: NotificationDrawerProps) {
  const { state } = useApp();
  const [notifications, setNotifications] = useState<StoredNotificationRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [actioningUid, setActioningUid] = useState<number | null>(null);

  const fetchNotifications = useCallback(async () => {
    try {
      const res = await api.listNotifications({ activeOnly: true, limit: 50 });
      setNotifications(res.notifications);
    } catch {
      // silent
    } finally {
      setLoading(false);
    }
  }, []);

  // Fetch when drawer opens
  useEffect(() => {
    if (open) {
      setLoading(true);
      fetchNotifications();
    }
  }, [open, fetchNotifications]);

  // Live updates while open
  useEvent("notification.received", useCallback(() => {
    if (open) fetchNotifications();
  }, [open, fetchNotifications]));

  useEvent("notification.removed", useCallback(() => {
    if (open) fetchNotifications();
  }, [open, fetchNotifications]));

  const handleAction = useCallback(
    async (uid: number, action: "positive" | "negative") => {
      setActioningUid(uid);
      try {
        await api.performNotificationAction(uid, action);
        setNotifications((prev) =>
          prev.filter((n) => n.notification.notificationUid !== uid),
        );
      } catch {
        // silent
      } finally {
        setActioningUid(null);
      }
    },
    [],
  );

  // Close on Escape
  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [open, onClose]);

  // Group by app
  const grouped = useMemo(() => {
    const map = new Map<string, StoredNotificationRecord[]>();
    // Sort newest first
    const sorted = [...notifications].sort(
      (a, b) =>
        new Date(b.notification.receivedAtUtc).getTime() -
        new Date(a.notification.receivedAtUtc).getTime(),
    );
    for (const n of sorted) {
      const app = humanizeAppId(n.notification.appIdentifier);
      const list = map.get(app) ?? [];
      list.push(n);
      map.set(app, list);
    }
    return map;
  }, [notifications]);

  const notificationsEnabled = state.runtime?.notificationsEnabled ?? false;
  const ancsPhase = normalizeSessionPhase(state.runtime?.ancsSession?.phase);
  const ancsDetail = state.runtime?.ancsSession?.detail ?? null;
  const ancsError = state.runtime?.ancsSession?.error ?? null;
  const activeCount = notifications.filter((n) => n.isActive).length;
  const totalActiveCount = state.runtime?.notificationCount ?? activeCount;
  const showingSubset = totalActiveCount > activeCount;

  return (
    <>
      {/* Backdrop */}
      {open && (
        <div
          className="fixed inset-0 bg-black/30 backdrop-blur-[2px] z-40 transition-opacity"
          onClick={onClose}
        />
      )}

      {/* Drawer */}
      <div
        className={cn(
          "fixed top-0 right-0 h-full w-96 max-w-[90vw] bg-sidebar border-l border-border z-50 flex flex-col transition-transform duration-200 ease-out shadow-2xl",
          open ? "translate-x-0" : "translate-x-full",
        )}
      >
        {/* Header */}
        <div className="px-4 py-4 border-b border-border flex items-center gap-3 shrink-0 bg-sidebar">
          <h2
            className="text-lg font-semibold text-text-primary flex-1 flex items-center gap-2"
            style={{ fontFamily: "var(--font-display)" }}
          >
            Notifications
            <span
              className="text-[9px] font-semibold uppercase tracking-widest px-1.5 py-0.5 rounded bg-accent/10 text-accent"
              style={{ fontFamily: "var(--font-mono)" }}
            >
              beta
            </span>
          </h2>
          <button
            onClick={onClose}
            aria-label="Close notifications"
            className="p-1.5 rounded-lg text-text-secondary hover:text-text-primary hover:bg-surface transition-colors"
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
              <path d="M18 6L6 18M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Status banners */}
        {!notificationsEnabled && (
          <div className="mx-3 mt-3 px-3 py-2.5 rounded-lg bg-surface border border-border text-xs text-text-secondary flex items-center gap-2">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" className="shrink-0 opacity-60">
              <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9" />
              <line x1="1" y1="1" x2="23" y2="23" />
            </svg>
            <span>
              Notifications are disabled.
              <button
                onClick={() => api.enableNotifications().catch(() => {})}
                className="ml-1 text-accent hover:text-accent-hover font-medium"
              >
                Enable
              </button>
            </span>
          </div>
        )}
        {notificationsEnabled && ancsPhase === "Connecting" && (
          <div className="mx-3 mt-3 px-3 py-2.5 rounded-lg bg-syncing/10 border border-syncing/20 text-xs text-syncing flex items-center gap-2">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" className="shrink-0 animate-spin" style={{ animationDuration: "2s" }}>
              <path d="M21 12a9 9 0 1 1-6.219-8.56" />
            </svg>
            <span>
              Reconnecting to notification service&hellip;
              {ancsDetail && <span className="block mt-1 text-[11px] opacity-80">{ancsDetail}</span>}
            </span>
          </div>
        )}
        {notificationsEnabled && ancsPhase === "Disconnected" && (
          <div className="mx-3 mt-3 px-3 py-2.5 rounded-lg bg-syncing/10 border border-syncing/20 text-xs text-syncing flex items-center gap-2">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" className="shrink-0 opacity-60">
              <circle cx="12" cy="12" r="10" />
              <line x1="15" y1="9" x2="9" y2="15" />
              <line x1="9" y1="9" x2="15" y2="15" />
            </svg>
            <span>
              Notification service is disconnected.
              {ancsDetail && <span className="block mt-1 text-[11px] opacity-80">{ancsDetail}</span>}
            </span>
          </div>
        )}
        {notificationsEnabled && ancsPhase === "Faulted" && (
          <div className="mx-3 mt-3 px-3 py-2.5 rounded-lg bg-error/10 border border-error/20 text-xs text-error flex items-center gap-2">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" className="shrink-0">
              <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" />
              <line x1="12" y1="9" x2="12" y2="13" />
              <line x1="12" y1="17" x2="12.01" y2="17" />
            </svg>
            <span>
              Notifications are degraded.
              {ancsDetail && <span className="block mt-1 text-[11px] opacity-80">{ancsDetail}</span>}
              {ancsError && <span className="block mt-1 text-[11px] opacity-80">{ancsError}</span>}
              <button
                onClick={() => api.enableNotifications().catch(() => {})}
                className="mt-1.5 text-[11px] underline block"
              >
                Retry notification bootstrap
              </button>
            </span>
          </div>
        )}

        {/* List */}
        <div className="flex-1 overflow-y-auto">
          {loading ? (
            <div className="px-4 py-4 space-y-3">
              {Array.from({ length: 4 }).map((_, i) => (
                <div key={i} className="space-y-2">
                  <div className="h-3 w-24 skeleton" />
                  <div className="h-3 w-48 skeleton" />
                </div>
              ))}
            </div>
          ) : notifications.length === 0 ? (
            <div className="px-4 py-16 text-center">
              <div className="inline-flex items-center justify-center w-14 h-14 rounded-2xl bg-surface border border-border/60 mb-4">
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" className="text-text-secondary/40">
                  <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9" />
                  <path d="M13.73 21a2 2 0 0 1-3.46 0" />
                </svg>
              </div>
              <p
                className="text-sm text-text-primary font-medium"
                style={{ fontFamily: "var(--font-display)" }}
              >
                {notificationsEnabled ? "All clear" : "Notifications disabled"}
              </p>
              <p className="text-xs text-text-secondary mt-1.5 leading-relaxed max-w-[200px] mx-auto">
                {notificationsEnabled
                  ? "New notifications from your iPhone will appear here."
                  : "Enable notifications to mirror alerts from your iPhone."}
              </p>
            </div>
          ) : (
            (() => {
              let rowIndex = 0;
              return Array.from(grouped.entries()).map(([appName, items]) => (
                <div key={appName} className="border-b border-border/40 last:border-b-0">
                  {/* App group header */}
                  <div className="flex items-center gap-2.5 px-4 py-2.5 sticky top-0 bg-sidebar/95 backdrop-blur-sm">
                    <div
                      className="w-5 h-5 rounded-md flex items-center justify-center text-white text-[8px] font-bold shrink-0"
                      style={{ backgroundColor: hashColor(appName) }}
                    >
                      {appName.slice(0, 2).toUpperCase()}
                    </div>
                    <span
                      className="text-[11px] text-text-secondary font-medium uppercase tracking-wider flex-1"
                      style={{ fontFamily: "var(--font-mono)" }}
                    >
                      {appName}
                    </span>
                    <span
                      className="text-[10px] text-text-secondary/60 tabular-nums"
                      style={{ fontFamily: "var(--font-mono)" }}
                    >
                      {items.length}
                    </span>
                  </div>

                  {items.map((stored) => (
                    <NotificationRow
                      key={stored.notification.notificationUid}
                      stored={stored}
                      index={rowIndex++}
                      actioning={actioningUid === stored.notification.notificationUid}
                      onAction={handleAction}
                    />
                  ))}
                </div>
              ));
            })()
          )}
        </div>

        {/* Footer count */}
        <div
          className="px-4 py-2.5 border-t border-border text-[11px] text-text-secondary shrink-0 bg-sidebar"
          style={{ fontFamily: "var(--font-mono)" }}
        >
          {showingSubset
            ? `Showing ${activeCount} of ${totalActiveCount} active notifications`
            : `${activeCount} active notification${activeCount !== 1 ? "s" : ""}`}
        </div>
      </div>
    </>
  );
}

function NotificationRow({
  stored,
  index,
  actioning,
  onAction,
}: {
  stored: StoredNotificationRecord;
  index: number;
  actioning: boolean;
  onAction: (uid: number, action: "positive" | "negative") => void;
}) {
  const n = stored.notification;
  const categoryLabel = NotificationCategoryLabels[n.category] ?? "";
  const appColor = hashColor(humanizeAppId(n.appIdentifier));

  return (
    <div
      className="animate-notif-in group flex hover:bg-surface/50 transition-colors"
      style={{ animationDelay: `${Math.min(index, 12) * 30}ms` }}
    >
      {/* Subtle left accent */}
      <div
        className="w-0.5 shrink-0 opacity-0 group-hover:opacity-100 transition-opacity"
        style={{ backgroundColor: appColor }}
      />

      <div className="flex-1 px-4 py-2.5 min-w-0">
        {/* Title row */}
        <div className="flex items-center gap-2 mb-0.5">
          {n.title && (
            <span className="text-sm text-text-primary font-medium truncate">
              {n.title}
            </span>
          )}
          {categoryLabel && categoryLabel !== "Other" && (
            <span
              className="text-[10px] text-text-secondary/60 shrink-0"
              style={{ fontFamily: "var(--font-mono)" }}
            >
              {categoryLabel}
            </span>
          )}
          <span
            className="text-[10px] text-text-secondary ml-auto shrink-0"
            style={{ fontFamily: "var(--font-mono)" }}
          >
            {notificationTime(n.date, n.receivedAtUtc)}
          </span>
        </div>

        {/* Subtitle */}
        {n.subtitle && (
          <p className="text-xs text-text-secondary">{n.subtitle}</p>
        )}

        {/* Body */}
        {n.message && (
          <p className="text-xs text-text-secondary mt-0.5 line-clamp-2 leading-relaxed">
            {n.message}
          </p>
        )}

        {/* Actions — always show a dismiss option */}
        <div className="flex items-center gap-2 mt-1.5">
          {n.positiveActionLabel && (
            <button
              onClick={() => onAction(n.notificationUid, "positive")}
              disabled={actioning}
              className="text-[11px] px-2 py-0.5 rounded bg-accent/10 text-accent hover:bg-accent/20 font-medium transition-colors disabled:opacity-50"
            >
              {n.positiveActionLabel}
            </button>
          )}
          <button
            onClick={() => onAction(n.notificationUid, "negative")}
            disabled={actioning}
            className="text-[11px] px-2 py-0.5 rounded bg-base text-text-secondary hover:text-text-primary transition-colors disabled:opacity-50"
          >
            {n.negativeActionLabel || "Dismiss"}
          </button>
        </div>
      </div>
    </div>
  );
}
