const DEFAULT_BASE_URL = resolveDefaultBaseUrl();

export class AditError extends Error {
  constructor(message, { status, body } = {}) {
    super(message);
    this.name = "AditError";
    this.status = status ?? null;
    this.body = body ?? null;
  }
}

export class AditClient {
  constructor(options = {}) {
    const baseUrl = options.baseUrl ?? DEFAULT_BASE_URL;
    this.baseUrl = baseUrl.endsWith("/") ? baseUrl.slice(0, -1) : baseUrl;
    this.fetchImpl = options.fetch ?? globalThis.fetch;

    if (typeof this.fetchImpl !== "function") {
      throw new TypeError("AditClient requires a fetch implementation.");
    }
  }

  async getStatus() {
    return this.#request("GET", "/v1/status");
  }

  async getInfo() {
    return this.#request("GET", "/v1/info");
  }

  async getRuntime() {
    return this.#request("GET", "/v1/runtime");
  }

  async getCapabilities() {
    return this.#request("GET", "/v1/capabilities");
  }

  async getDoctor() {
    return this.#request("GET", "/v1/doctor");
  }

  async getSetupGuide() {
    return this.#request("GET", "/v1/setup/guide");
  }

  async checkSetup() {
    return this.#request("POST", "/v1/setup/check");
  }

  async getAgentContext() {
    return this.#request("GET", "/v1/agent/context");
  }

  async listDevices() {
    return this.#request("GET", "/v1/devices");
  }

  async listContacts(options = {}) {
    return this.#request("GET", "/v1/contacts", { query: options });
  }

  async searchContacts(query, options = {}) {
    if (!query) {
      throw new TypeError("searchContacts requires a query string.");
    }

    return this.#request("GET", "/v1/contacts/search", {
      query: { query, ...options }
    });
  }

  async listNotifications(options = {}) {
    return this.#request("GET", "/v1/notifications", { query: options });
  }

  async listMessageFolders(options = {}) {
    return this.#request("GET", "/v1/messages/folders", { query: options });
  }

  async listMessages(options = {}) {
    return this.#request("GET", "/v1/messages", { query: options });
  }

  async listCachedMessages(options = {}) {
    return this.#request("GET", "/v1/cache/messages", { query: options });
  }

  async listConversations(options = {}) {
    return this.#request("GET", "/v1/conversations", { query: options });
  }

  async getConversationMessages(conversationId, options = {}) {
    if (!conversationId) {
      throw new TypeError("getConversationMessages requires a conversationId.");
    }

    return this.#request(
      "GET",
      `/v1/conversations/${encodeURIComponent(conversationId)}`,
      { query: options }
    );
  }

  async triggerSync(reason = "manual") {
    return this.#request("POST", "/v1/sync/now", {
      query: { reason }
    });
  }

  async getRecentEvents(limit = 25) {
    return this.#request("GET", "/v1/events/recent", { query: { limit } });
  }

  async checkNotifications() {
    return this.#request("POST", "/v1/notifications/check");
  }

  async enableNotifications() {
    return this.#request("POST", "/v1/notifications/enable");
  }

  async disableNotifications() {
    return this.#request("POST", "/v1/notifications/disable");
  }

  getWebSocketUrl() {
    const url = new URL(this.baseUrl);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    url.pathname = "/v1/ws";
    url.search = "";
    return url.toString();
  }

  connectWebSocket({ WebSocketImpl } = {}) {
    const WebSocketCtor = WebSocketImpl ?? globalThis.WebSocket;
    if (typeof WebSocketCtor !== "function") {
      throw new TypeError("connectWebSocket requires a WebSocket implementation.");
    }

    return new WebSocketCtor(this.getWebSocketUrl());
  }

  async performNotificationAction(notificationUid, action) {
    if (!Number.isInteger(notificationUid) || notificationUid <= 0) {
      throw new TypeError("performNotificationAction requires a positive integer notificationUid.");
    }

    if (action !== "positive" && action !== "negative") {
      throw new TypeError("performNotificationAction requires action to be 'positive' or 'negative'.");
    }

    return this.#request(
      "POST",
      `/v1/notifications/${encodeURIComponent(notificationUid)}/actions/${encodeURIComponent(action)}`
    );
  }

  async resolveMessage(options = {}) {
    validateMessageOptions(options, { allowBodyless: true });
    return this.#request("POST", "/v1/messages/resolve", { body: options });
  }

  async sendMessage(options) {
    validateMessageOptions(options);

    return this.#request("POST", "/v1/messages/send", { body: options });
  }

  async replyToConversation(conversationId, body, options = {}) {
    if (!conversationId) {
      throw new TypeError("replyToConversation requires a conversationId.");
    }

    return this.sendMessage({ ...options, conversationId, body });
  }

  async #request(method, path, { query, body } = {}) {
    const url = new URL(`${this.baseUrl}${path}`);
    appendQuery(url, query);

    const response = await this.fetchImpl(url, {
      method,
      headers: body ? { "content-type": "application/json" } : undefined,
      body: body ? JSON.stringify(body) : undefined
    });

    const payload = await readPayload(response);
    if (!response.ok) {
      const message =
        payload && typeof payload === "object" && typeof payload.error === "string"
          ? payload.error
          : `Adit request failed with status ${response.status}.`;
      throw new AditError(message, { status: response.status, body: payload });
    }

    return payload;
  }
}

function appendQuery(url, query) {
  if (!query || typeof query !== "object") {
    return;
  }

  for (const [key, value] of Object.entries(query)) {
    if (value === undefined || value === null || value === "") {
      continue;
    }

    if (typeof value === "boolean") {
      url.searchParams.set(key, value ? "true" : "false");
      continue;
    }

    url.searchParams.set(key, String(value));
  }
}

async function readPayload(response) {
  const text = await response.text();
  if (!text) {
    return null;
  }

  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

export function createClient(options) {
  return new AditClient(options);
}

function validateMessageOptions(options, { allowBodyless = false } = {}) {
  if (!options || typeof options !== "object") {
    throw new TypeError("Message requests require an options object.");
  }

  if (!allowBodyless && !options.body) {
    throw new TypeError("sendMessage requires a body.");
  }

  if (
    !options.recipient &&
    !options.contactId &&
    !options.contactName &&
    !options.conversationId
  ) {
    throw new TypeError(
      "Message requests require recipient, contactId, contactName, or conversationId."
    );
  }
}

function resolveDefaultBaseUrl() {
  const envValue =
    typeof process === "object" &&
    process !== null &&
    process.env &&
    typeof process.env.ADIT_URL === "string"
      ? process.env.ADIT_URL.trim()
      : "";
  return envValue || "http://127.0.0.1:5037";
}
