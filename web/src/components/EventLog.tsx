import { useState, useEffect, useRef, useCallback } from "react";
import { api } from "../lib/api";
import { useEvent } from "../lib/ws";
import { cn } from "../lib/utils";
import type { DaemonEventRecord, ThreadChooserStatusSnapshot } from "../lib/types";
import { ThreadChooserStatusCard } from "./ThreadChooserStatusCard";

const MAX_EVENTS = 500;

export function EventLog() {
  const [events, setEvents] = useState<DaemonEventRecord[]>([]);
  const [threadChooser, setThreadChooser] = useState<ThreadChooserStatusSnapshot | null>(null);
  const [filter, setFilter] = useState("");
  const [autoScroll, setAutoScroll] = useState(true);
  const [expandedSeq, setExpandedSeq] = useState<number | null>(null);
  const [paused, setPaused] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);
  const bufferRef = useRef<DaemonEventRecord[]>([]);

  const refreshThreadChooser = useCallback(() => {
    api.getThreadChooserStatus().then(setThreadChooser).catch(() => {});
  }, []);

  // Load recent events on mount
  useEffect(() => {
    api.getRecentEvents(100).then((res) => {
      const sorted = [...res.events].sort((a, b) => a.sequence - b.sequence);
      setEvents(sorted);
      bufferRef.current = sorted;
    }).catch(() => {});
    refreshThreadChooser();
  }, [refreshThreadChooser]);

  // Live events from WebSocket
  useEvent("*", useCallback((ev: DaemonEventRecord) => {
    // Skip our synthetic ws.connected/ws.disconnected
    if (ev.type.startsWith("ws.")) return;
    bufferRef.current = [...bufferRef.current, ev].slice(-MAX_EVENTS);
    if (ev.type.startsWith("thread_chooser.") || ev.type === "sync.completed") {
      refreshThreadChooser();
    }
    if (!paused) {
      setEvents(bufferRef.current);
    }
  }, [paused, refreshThreadChooser]));

  // Auto-scroll
  useEffect(() => {
    if (autoScroll && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [events, autoScroll]);

  const handleScroll = useCallback(() => {
    const el = scrollRef.current;
    if (!el) return;
    setAutoScroll(el.scrollHeight - el.scrollTop - el.clientHeight < 40);
  }, []);

  const handleResume = useCallback(() => {
    setPaused(false);
    setEvents(bufferRef.current);
  }, []);

  // Filtered events
  const filtered = filter
    ? events.filter((e) => e.type.toLowerCase().includes(filter.toLowerCase()))
    : events;

  // Collect unique event types for quick filter
  const eventTypes = [...new Set(events.map((e) => e.type))].sort();

  return (
    <div className="flex flex-col h-full">
      {/* Controls */}
      <div className="flex items-center gap-2 px-4 py-2 border-b border-border">
        <input
          type="text"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="Filter events..."
          className="flex-1 rounded bg-base border border-border px-2 py-1 text-xs text-text-primary placeholder:text-text-secondary outline-none focus:border-accent transition-colors"
          style={{ fontFamily: "var(--font-mono)" }}
        />
        <button
          onClick={() => setPaused(!paused)}
          className={cn(
            "text-[11px] px-2 py-1 rounded transition-colors",
            paused
              ? "bg-syncing/10 text-syncing"
              : "bg-surface text-text-secondary hover:text-text-primary",
          )}
          style={{ fontFamily: "var(--font-mono)" }}
        >
          {paused ? "paused" : "live"}
        </button>
        {paused && (
          <button
            onClick={handleResume}
            className="text-[11px] px-2 py-1 rounded bg-accent/10 text-accent hover:bg-accent/20 transition-colors"
            style={{ fontFamily: "var(--font-mono)" }}
          >
            resume
          </button>
        )}
        <span
          className="text-[11px] text-text-secondary"
          style={{ fontFamily: "var(--font-mono)" }}
        >
          {filtered.length}/{events.length}
        </span>
      </div>

      <div className="px-4 py-3 border-b border-border/50">
        <ThreadChooserStatusCard status={threadChooser} compact />
      </div>

      {/* Quick type filters */}
      {eventTypes.length > 0 && (
        <div className="flex items-center gap-1 px-4 py-1.5 overflow-x-auto border-b border-border/50">
          <button
            onClick={() => setFilter("")}
            className={cn(
              "text-[10px] px-1.5 py-0.5 rounded shrink-0 transition-colors",
              !filter ? "bg-accent text-white" : "text-text-secondary hover:text-text-primary",
            )}
            style={{ fontFamily: "var(--font-mono)" }}
          >
            all
          </button>
          {eventTypes.map((t) => (
            <button
              key={t}
              onClick={() => setFilter(filter === t ? "" : t)}
              className={cn(
                "text-[10px] px-1.5 py-0.5 rounded shrink-0 transition-colors",
                filter === t ? "bg-accent text-white" : "text-text-secondary hover:text-text-primary",
              )}
              style={{ fontFamily: "var(--font-mono)" }}
            >
              {t}
            </button>
          ))}
        </div>
      )}

      {/* Event stream */}
      <div
        ref={scrollRef}
        onScroll={handleScroll}
        className="flex-1 overflow-y-auto"
      >
        {filtered.length === 0 ? (
          <div className="px-4 py-8 text-center text-xs text-text-secondary">
            {events.length === 0 ? "No events yet." : "No events match filter."}
          </div>
        ) : (
          filtered.map((ev) => (
            <EventRow
              key={`${ev.sequence}-${ev.type}`}
              event={ev}
              expanded={expandedSeq === ev.sequence}
              onToggle={() =>
                setExpandedSeq(expandedSeq === ev.sequence ? null : ev.sequence)
              }
            />
          ))
        )}
      </div>
    </div>
  );
}

function EventRow({
  event,
  expanded,
  onToggle,
}: {
  event: DaemonEventRecord;
  expanded: boolean;
  onToggle: () => void;
}) {
  const time = new Date(event.timestampUtc).toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });

  const typeColor = event.type.includes("error") || event.type.includes("failed")
    ? "text-error"
    : event.type.includes("completed") || event.type.includes("ready")
      ? "text-success"
      : event.type.includes("started") || event.type.includes("enabled")
        ? "text-syncing"
        : "text-text-secondary";

  return (
    <div className="border-b border-border/30">
      <button
        onClick={onToggle}
        className="w-full flex items-center gap-2 px-4 py-1.5 text-left hover:bg-surface/50 transition-colors"
      >
        <span
          className="text-[11px] text-text-secondary shrink-0 w-16"
          style={{ fontFamily: "var(--font-mono)" }}
        >
          {time}
        </span>
        <span
          className="text-[11px] text-text-secondary shrink-0 w-8 text-right"
          style={{ fontFamily: "var(--font-mono)" }}
        >
          #{event.sequence}
        </span>
        <span
          className={cn("text-xs font-medium truncate", typeColor)}
          style={{ fontFamily: "var(--font-mono)" }}
        >
          {event.type}
        </span>
        <svg
          width="12"
          height="12"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          className={cn(
            "shrink-0 ml-auto text-text-secondary transition-transform",
            expanded && "rotate-90",
          )}
        >
          <polyline points="9 18 15 12 9 6" />
        </svg>
      </button>
      {expanded && event.payload != null && (
        <pre
          className="px-4 py-2 mx-4 mb-2 rounded bg-base border border-border text-[11px] text-text-secondary overflow-x-auto leading-relaxed max-h-64"
          style={{ fontFamily: "var(--font-mono)" }}
        >
          {JSON.stringify(event.payload, null, 2)}
        </pre>
      )}
    </div>
  );
}
