import type {
  StatusResponse,
  CapabilitiesResponse,
  DoctorResponse,
  DaemonRuntimeSnapshot,
  DevicesResponse,
  ConversationsResponse,
  ConversationMessagesResponse,
  ContactsResponse,
  ContactSearchResponse,
  SendMessageResponse,
  RecentEventsResponse,
  NotificationsResponse,
  ThreadChooserStatusSnapshot,
} from "./types";
import { getDaemonAuthToken } from "./auth";

class ApiError extends Error {
  constructor(
    public status: number,
    public body: unknown,
  ) {
    super(`API ${status}`);
    this.name = "ApiError";
  }
}

async function get<T>(path: string): Promise<T> {
  const res = await fetch(path, {
    headers: buildHeaders(),
  });
  if (!res.ok) throw new ApiError(res.status, await res.json().catch(() => null));
  return res.json() as Promise<T>;
}

async function post<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(path, {
    method: "POST",
    headers: buildHeaders(body !== undefined),
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) throw new ApiError(res.status, await res.json().catch(() => null));
  return res.json() as Promise<T>;
}

function buildHeaders(hasJsonBody = false): HeadersInit | undefined {
  const headers: Record<string, string> = {};
  if (hasJsonBody) headers["Content-Type"] = "application/json";

  const authToken = getDaemonAuthToken();
  if (authToken) headers.Authorization = `Bearer ${authToken}`;

  return Object.keys(headers).length > 0 ? headers : undefined;
}

export const api = {
  getStatus: () => get<StatusResponse>("/v1/status"),

  getRuntime: () => get<DaemonRuntimeSnapshot>("/v1/runtime"),

  getCapabilities: () => get<CapabilitiesResponse>("/v1/capabilities"),

  getDoctor: () => get<DoctorResponse>("/v1/doctor"),

  getThreadChooserStatus: () => get<ThreadChooserStatusSnapshot>("/v1/thread-chooser/status"),

  getDevices: () => get<DevicesResponse>("/v1/devices"),

  listConversations: (limit?: number) =>
    get<ConversationsResponse>(
      `/v1/conversations${limit ? `?limit=${limit}` : ""}`,
    ),

  getConversation: (id: string, limit?: number) =>
    get<ConversationMessagesResponse>(
      `/v1/conversations/${encodeURIComponent(id)}${limit ? `?limit=${limit}` : ""}`,
    ),

  listContacts: () => get<ContactsResponse>("/v1/contacts"),

  searchContacts: (query: string, limit?: number) =>
    get<ContactSearchResponse>(
      `/v1/contacts/search?query=${encodeURIComponent(query)}${limit ? `&limit=${limit}` : ""}`,
    ),

  sendMessage: (opts: {
    recipient?: string;
    contactName?: string;
    body: string;
    autoSyncAfterSend?: boolean;
  }) =>
    post<SendMessageResponse>("/v1/messages/send", {
      recipient: opts.recipient,
      contactName: opts.contactName,
      body: opts.body,
      autoSyncAfterSend: opts.autoSyncAfterSend ?? true,
    }),

  triggerSync: (reason?: string) =>
    post<{ accepted: boolean; reason: string }>(
      `/v1/sync/now${reason ? `?reason=${encodeURIComponent(reason)}` : ""}`,
    ),

  getRecentEvents: (limit?: number) =>
    get<RecentEventsResponse>(
      `/v1/events/recent${limit ? `?limit=${limit}` : ""}`,
    ),

  listNotifications: (opts?: { activeOnly?: boolean; limit?: number }) => {
    const params = new URLSearchParams();
    if (opts?.activeOnly !== undefined) params.set("activeOnly", String(opts.activeOnly));
    if (opts?.limit) params.set("limit", String(opts.limit));
    const qs = params.toString();
    return get<NotificationsResponse>(`/v1/notifications${qs ? `?${qs}` : ""}`);
  },

  performNotificationAction: (uid: number, action: "positive" | "negative") =>
    post<{ notificationUid: number; action: string; accepted: boolean }>(
      `/v1/notifications/${uid}/actions/${action}`,
    ),

  enableNotifications: () =>
    post<unknown>("/v1/notifications/enable"),

  disableNotifications: () =>
    post<unknown>("/v1/notifications/disable"),
};

export { ApiError };
export function isUnauthorizedError(error: unknown): error is ApiError {
  return error instanceof ApiError && error.status === 401;
}
