const DEFAULT_BASE_URL = resolveDefaultBaseUrl();
const DEFAULT_AUTH_TOKEN = resolveDefaultAuthToken();

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
    this.authToken = normalizeOptionalString(options.authToken ?? DEFAULT_AUTH_TOKEN);
    this.fetchImpl = options.fetch ?? globalThis.fetch;

    if (typeof this.fetchImpl !== "function") {
      throw new TypeError("AditClient requires a fetch implementation.");
    }
  }

  async getAgentContext() {
    return this.#request("GET", "/v1/agent/context");
  }

  async getSetupGuide() {
    return this.#request("GET", "/v1/setup/guide");
  }

  async checkSetup() {
    return this.#request("POST", "/v1/setup/check");
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

  async resolveMessage(options = {}) {
    validateMessageOptions(options, { allowBodyless: true });
    return this.#request("POST", "/v1/messages/resolve", { body: options });
  }

  async sendMessage(options = {}) {
    validateMessageOptions(options);
    return this.#request("POST", "/v1/messages/send", { body: options });
  }

  async triggerSync(reason = "manual") {
    return this.#request("POST", "/v1/sync/now", {
      query: { reason }
    });
  }

  async #request(method, path, { query, body } = {}) {
    const url = new URL(`${this.baseUrl}${path}`);
    appendQuery(url, query);
    const headers = {};
    if (body) {
      headers["content-type"] = "application/json";
    }
    if (this.authToken) {
      headers.authorization = `Bearer ${this.authToken}`;
    }

    const response = await this.fetchImpl(url, {
      method,
      headers: Object.keys(headers).length > 0 ? headers : undefined,
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

function resolveDefaultAuthToken() {
  const envValue =
    typeof process === "object" &&
    process !== null &&
    process.env &&
    typeof process.env.ADIT_AUTH_TOKEN === "string"
      ? process.env.ADIT_AUTH_TOKEN.trim()
      : "";
  return envValue || "";
}

function normalizeOptionalString(value) {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : "";
}
