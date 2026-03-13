import { useState, useEffect, useCallback, useRef } from "react";
import { useEvent } from "../lib/ws";
import { api } from "../lib/api";
import { humanizeAppId, hashColor, cn } from "../lib/utils";
import type { DaemonEventRecord } from "../lib/types";

interface ToastItem {
  uid: number;
  appName: string;
  title?: string | null;
  subtitle?: string | null;
  message?: string | null;
  positiveLabel?: string | null;
  negativeLabel?: string | null;
  color: string;
  addedAt: number;
}

interface TimerEntry {
  timer: ReturnType<typeof setTimeout>;
  remaining: number;
  startedAt: number;
}

const TOAST_DURATION = 6000;
const MAX_TOASTS = 3;

export function NotificationToasts() {
  const [toasts, setToasts] = useState<ToastItem[]>([]);
  const timersRef = useRef<Map<number, TimerEntry>>(new Map());
  const [hoveredUid, setHoveredUid] = useState<number | null>(null);

  const dismiss = useCallback((uid: number) => {
    setToasts((prev) => prev.filter((t) => t.uid !== uid));
    const entry = timersRef.current.get(uid);
    if (entry) {
      clearTimeout(entry.timer);
      timersRef.current.delete(uid);
    }
  }, []);

  const startTimer = useCallback((uid: number, duration: number) => {
    const existing = timersRef.current.get(uid);
    if (existing) clearTimeout(existing.timer);

    const timer = setTimeout(() => {
      setToasts((prev) => prev.filter((t) => t.uid !== uid));
      timersRef.current.delete(uid);
    }, duration);
    timersRef.current.set(uid, { timer, remaining: duration, startedAt: Date.now() });
  }, []);

  const handleMouseEnter = useCallback((uid: number) => {
    setHoveredUid(uid);
    const entry = timersRef.current.get(uid);
    if (entry) {
      clearTimeout(entry.timer);
      const elapsed = Date.now() - entry.startedAt;
      const remaining = Math.max(entry.remaining - elapsed, 500);
      timersRef.current.set(uid, { timer: entry.timer, remaining, startedAt: Date.now() });
    }
  }, []);

  const handleMouseLeave = useCallback((uid: number) => {
    setHoveredUid((prev) => (prev === uid ? null : prev));
    const entry = timersRef.current.get(uid);
    if (entry) {
      startTimer(uid, entry.remaining);
    }
  }, [startTimer]);

  const handleAction = useCallback(
    async (uid: number, action: "positive" | "negative") => {
      dismiss(uid);
      try {
        await api.performNotificationAction(uid, action);
      } catch {
        // silent
      }
    },
    [dismiss],
  );

  useEvent("notification.received", useCallback((ev: DaemonEventRecord) => {
    const payload = ev.payload as {
      notification?: {
        notificationUid?: number;
        appIdentifier?: string | null;
        title?: string | null;
        subtitle?: string | null;
        message?: string | null;
        positiveActionLabel?: string | null;
        negativeActionLabel?: string | null;
        eventFlags?: number;
      };
    } | undefined;

    const n = payload?.notification;
    if (!n?.notificationUid) return;

    // Skip silent notifications (flag bit 0) and pre-existing (flag bit 2)
    if (n.eventFlags && (n.eventFlags & (1 | 4)) !== 0) return;

    // Skip if no useful content
    if (!n.title && !n.message) return;

    const appName = humanizeAppId(n.appIdentifier);
    const uid = n.notificationUid;

    const toast: ToastItem = {
      uid,
      appName,
      title: n.title,
      subtitle: n.subtitle,
      message: n.message,
      positiveLabel: n.positiveActionLabel,
      negativeLabel: n.negativeActionLabel,
      color: hashColor(appName),
      addedAt: Date.now(),
    };

    setToasts((prev) => {
      const filtered = prev.filter((t) => t.uid !== uid);
      const trimmed = filtered.slice(-(MAX_TOASTS - 1));
      return [...trimmed, toast];
    });

    startTimer(uid, TOAST_DURATION);
  }, [startTimer]));

  // Cleanup timers on unmount
  useEffect(() => {
    return () => {
      timersRef.current.forEach((entry) => clearTimeout(entry.timer));
    };
  }, []);

  if (toasts.length === 0) return null;

  return (
    <div className="fixed bottom-10 right-4 z-50 flex flex-col gap-2.5 w-80">
      {toasts.map((toast) => {
        const isHovered = hoveredUid === toast.uid;
        return (
          <div
            key={toast.uid}
            className="animate-toast-enter rounded-xl shadow-xl overflow-hidden group border border-border/80 bg-surface"
            onMouseEnter={() => handleMouseEnter(toast.uid)}
            onMouseLeave={() => handleMouseLeave(toast.uid)}
          >
            <div className="flex">
              {/* Colored accent stripe */}
              <div
                className="w-1 shrink-0"
                style={{ backgroundColor: toast.color }}
              />

              <div className="flex-1 p-3 pl-3">
                <div className="flex items-start gap-2.5">
                  {/* App icon */}
                  <div
                    className="w-8 h-8 rounded-lg flex items-center justify-center text-white text-[10px] font-bold shrink-0 shadow-sm"
                    style={{ backgroundColor: toast.color }}
                  >
                    {toast.appName.slice(0, 2).toUpperCase()}
                  </div>

                  <div className="flex-1 min-w-0">
                    {/* App name */}
                    <p
                      className="text-[10px] text-text-secondary uppercase tracking-wider leading-none mb-1"
                      style={{ fontFamily: "var(--font-mono)" }}
                    >
                      {toast.appName}
                    </p>

                    {/* Title */}
                    {toast.title && (
                      <p className="text-[13px] text-text-primary font-semibold leading-snug">
                        {toast.title}
                        {toast.subtitle && (
                          <span className="font-normal text-text-secondary text-xs">
                            {" \u2014 "}
                            {toast.subtitle}
                          </span>
                        )}
                      </p>
                    )}

                    {/* Body */}
                    {toast.message && (
                      <p className="text-xs text-text-secondary mt-0.5 line-clamp-2 leading-relaxed">
                        {toast.message}
                      </p>
                    )}

                    {/* Actions */}
                    {(toast.positiveLabel || toast.negativeLabel) && (
                      <div className="flex items-center gap-2 mt-2">
                        {toast.positiveLabel && (
                          <button
                            onClick={() => handleAction(toast.uid, "positive")}
                            className="text-[11px] px-2.5 py-1 rounded-lg bg-accent/10 text-accent hover:bg-accent/20 font-medium transition-colors"
                          >
                            {toast.positiveLabel}
                          </button>
                        )}
                        {toast.negativeLabel && (
                          <button
                            onClick={() => handleAction(toast.uid, "negative")}
                            className="text-[11px] px-2.5 py-1 rounded-lg bg-base text-text-secondary hover:text-text-primary transition-colors"
                          >
                            {toast.negativeLabel}
                          </button>
                        )}
                      </div>
                    )}
                  </div>

                  {/* Dismiss X — visible on hover */}
                  <button
                    onClick={() => dismiss(toast.uid)}
                    aria-label="Dismiss notification"
                    className={cn(
                      "shrink-0 w-5 h-5 flex items-center justify-center rounded text-text-secondary/50 hover:text-text-primary transition-all",
                      "opacity-0 group-hover:opacity-100",
                    )}
                  >
                    <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round">
                      <path d="M18 6L6 18M6 6l12 12" />
                    </svg>
                  </button>
                </div>
              </div>
            </div>

            {/* Progress bar */}
            <div className="h-[2px] bg-border/20">
              <div
                className="h-full origin-left"
                style={{
                  backgroundColor: toast.color,
                  opacity: 0.5,
                  animation: `toast-progress ${TOAST_DURATION}ms linear forwards`,
                  animationPlayState: isHovered ? "paused" : "running",
                }}
              />
            </div>
          </div>
        );
      })}
    </div>
  );
}
