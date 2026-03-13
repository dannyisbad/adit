import { useEffect, useRef, useCallback } from "react";
import type { DaemonEventRecord } from "./types";

type EventHandler = (event: DaemonEventRecord) => void;

function normalizeEventRecord(raw: unknown): DaemonEventRecord | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }

  const candidate = raw as Record<string, unknown>;
  const type = candidate.type ?? candidate.Type;
  if (typeof type !== "string" || type.length === 0) {
    return null;
  }

  const sequence = candidate.sequence ?? candidate.Sequence;
  const timestampUtc = candidate.timestampUtc ?? candidate.TimestampUtc;
  const payload = candidate.payload ?? candidate.Payload;

  return {
    sequence: typeof sequence === "number" ? sequence : 0,
    timestampUtc: typeof timestampUtc === "string" ? timestampUtc : new Date().toISOString(),
    type,
    payload,
  };
}

class WebSocketManager {
  private socket: WebSocket | null = null;
  private listeners = new Map<string, Set<EventHandler>>();
  private wildcardListeners = new Set<EventHandler>();
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private attempt = 0;
  private refCount = 0;
  private _connected = false;

  get connected() {
    return this._connected;
  }

  /** Increment the reference count and connect if this is the first consumer. */
  acquire() {
    this.refCount++;
    if (this.refCount === 1) {
      this.doConnect();
    }
  }

  /** Decrement the reference count and disconnect when no consumers remain. */
  release() {
    this.refCount = Math.max(0, this.refCount - 1);
    if (this.refCount === 0) {
      this.doDisconnect();
    }
  }

  on(type: string, handler: EventHandler) {
    if (type === "*") {
      this.wildcardListeners.add(handler);
    } else {
      let set = this.listeners.get(type);
      if (!set) {
        set = new Set();
        this.listeners.set(type, set);
      }
      set.add(handler);
    }
    return () => this.off(type, handler);
  }

  off(type: string, handler: EventHandler) {
    if (type === "*") {
      this.wildcardListeners.delete(handler);
    } else {
      this.listeners.get(type)?.delete(handler);
    }
  }

  private doConnect() {
    if (this.socket) return;

    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    const url = `${proto}//${location.host}/v1/ws`;

    try {
      this.socket = new WebSocket(url);
    } catch {
      this.scheduleReconnect();
      return;
    }

    this.socket.onopen = () => {
      this.attempt = 0;
      this._connected = true;
      this.emit({ sequence: 0, timestampUtc: new Date().toISOString(), type: "ws.connected" });
    };

    this.socket.onmessage = (ev) => {
      try {
        const data = normalizeEventRecord(JSON.parse(ev.data as string));
        if (!data) {
          return;
        }
        this.emit(data);
      } catch {
        // ignore malformed messages
      }
    };

    this.socket.onclose = () => {
      this.socket = null;
      this._connected = false;
      this.emit({ sequence: 0, timestampUtc: new Date().toISOString(), type: "ws.disconnected" });
      if (this.refCount > 0) {
        this.scheduleReconnect();
      }
    };

    this.socket.onerror = () => {
      // onclose will fire after onerror
    };
  }

  private doDisconnect() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.socket) {
      this.socket.onclose = null;
      this.socket.close();
      this.socket = null;
    }
    this._connected = false;
    this.attempt = 0;
  }

  private emit(event: DaemonEventRecord) {
    this.listeners.get(event.type)?.forEach((h) => h(event));
    this.wildcardListeners.forEach((h) => h(event));
  }

  private scheduleReconnect() {
    if (this.refCount === 0) return;
    const delay = Math.min(1000 * Math.pow(2, this.attempt), 30000);
    this.attempt++;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (this.refCount > 0) {
        this.doConnect();
      }
    }, delay);
  }
}

// Singleton — safe to acquire/release across StrictMode double-mounts.
const manager = new WebSocketManager();

export function getWsManager(): WebSocketManager {
  return manager;
}

/**
 * React hook: subscribe to daemon events by type.
 * Pass "*" to receive all events.
 */
export function useEvent(type: string, handler: EventHandler) {
  const handlerRef = useRef(handler);
  handlerRef.current = handler;

  const stableHandler = useCallback((ev: DaemonEventRecord) => {
    handlerRef.current(ev);
  }, []);

  useEffect(() => {
    return manager.on(type, stableHandler);
  }, [type, stableHandler]);
}

/**
 * React hook: acquire the WebSocket on mount, release on unmount.
 * Safe under StrictMode — the ref-counted singleton survives mount/unmount/remount.
 */
export function useWsConnection() {
  useEffect(() => {
    manager.acquire();
    return () => manager.release();
  }, []);
}
