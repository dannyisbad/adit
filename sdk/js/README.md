# adit-js

Zero-dependency Node ESM client for the local `Adit.Daemon` HTTP API.

## Requirements

- Node `18+`
- A running daemon, defaulting to `http://127.0.0.1:5037`
- Optional `ADIT_URL` env var if the daemon is not on the default port

## Install

```bash
npm install ./sdk/js
```

## Setup-First Flow

Treat the daemon's setup contract as the source of truth before you do device or send work:

1. Call `getSetupGuide()` first during setup or recovery.
2. Call `checkSetup()` when you need a fresh readiness snapshot.
3. Call `getAgentContext()` when you want setup, capability, doctor, and workflow guidance in one payload.

In the current v1 posture, the supported setup flow is one-time `Link to Windows` pairing first. `enableNotifications()` is a manual retry or opt-in endpoint after that setup, not a second pairing ceremony.

## Usage

```js
import { AditClient } from "adit-js";

const client = new AditClient({
  baseUrl: "http://127.0.0.1:5037"
});

const setupGuide = await client.getSetupGuide();
const setupCheck = await client.checkSetup();
const agent = await client.getAgentContext();

const info = await client.getInfo();
const status = await client.getStatus();
const runtime = await client.getRuntime();
const capabilities = await client.getCapabilities();
const doctor = await client.getDoctor();
const devices = await client.listDevices();
const contacts = await client.searchContacts("mom");
const notifications = await client.listNotifications();
const notificationsCheck = await client.checkNotifications();
const conversations = await client.listConversations();
const cachedMessages = await client.listCachedMessages({ limit: 10 });

const folders = await client.listMessageFolders({
  nameContains: "iPhone"
});

const messages = await client.listMessages({
  nameContains: "iPhone",
  folder: "inbox",
  limit: 10
});

const sent = await client.sendMessage({
  contactName: "mom",
  body: "hello from adit-js",
  autoSyncAfterSend: true
});

const preparedReply = await client.resolveMessage({
  conversationId: "th_seeded_mom",
  body: "sounds good"
});

await client.replyToConversation("th_seeded_mom", "reply from adit-js");

await client.enableNotifications();
await client.disableNotifications();
await client.triggerSync("manual");
const events = await client.getRecentEvents();
const wsUrl = client.getWebSocketUrl();
```

## API

- `getStatus()`
- `getInfo()`
- `getRuntime()`
- `getCapabilities()`
- `getDoctor()`
- `getSetupGuide()`
- `checkSetup()`
- `getAgentContext()`
- `listDevices()`
- `listContacts({ deviceId, nameContains, evictPhoneLink })`
- `searchContacts(query, { deviceId, nameContains, limit })`
- `listNotifications({ deviceId, nameContains, activeOnly, appIdentifier, limit })`
- `listMessageFolders({ deviceId, nameContains, evictPhoneLink })`
- `listMessages({ deviceId, nameContains, folder, limit, evictPhoneLink })`
- `listCachedMessages({ deviceId, nameContains, folder, conversationId, limit })`
- `listConversations({ deviceId, nameContains, limit })`
- `getConversationMessages(conversationId, { deviceId, nameContains, limit })`
- `triggerSync(reason = "manual")`
- `getRecentEvents(limit = 25)`
- `checkNotifications()`
- `enableNotifications()`
- `disableNotifications()`
- `getWebSocketUrl()`
- `connectWebSocket({ WebSocketImpl })`
- `performNotificationAction(notificationUid, action)`
- `resolveMessage({ recipient, body, contactId, contactName, preferredNumber, conversationId, deviceId, nameContains, evictPhoneLink })`
- `sendMessage({ recipient, body, contactId, contactName, preferredNumber, conversationId, deviceId, nameContains, autoSyncAfterSend, evictPhoneLink })`
- `replyToConversation(conversationId, body, options = {})`

Failed requests throw `AditError`, which includes:

- `status`
- `body`
