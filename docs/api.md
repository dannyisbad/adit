# API Reference

This is the current local daemon HTTP surface exposed by [src/Adit.Daemon/Program.cs](../src/Adit.Daemon/Program.cs).

## Always-On Routes

### Discovery And Health

- `GET /`
- `GET /v1/info`
- `GET /v1/status`
- `GET /v1/runtime`
- `GET /v1/capabilities`
- `GET /v1/doctor`

### Setup And Agent Context

- `GET /v1/setup/guide`
- `POST /v1/setup/check`
- `GET /v1/agent/context`

### Events And Live Updates

- `POST /v1/sync/now`
- `GET /v1/events`
- `GET /v1/events/recent`
- `GET /v1/thread-chooser/status`
- `GET /v1/ws`

### Devices, Notifications, Contacts, And Messages

- `GET /v1/devices`
- `POST /v1/notifications/check`
- `POST /v1/notifications/enable`
- `POST /v1/notifications/disable`
- `POST /v1/bootstrap/notifications/check`
- `POST /v1/bootstrap/notifications/enable`
- `POST /v1/bootstrap/notifications/disable`
- `GET /v1/contacts`
- `GET /v1/contacts/search`
- `GET /v1/notifications`
- `POST /v1/notifications/{notificationUid:long}/actions/{action}`
- `GET /v1/messages/folders`
- `GET /v1/messages`
- `GET /v1/cache/messages`
- `GET /v1/conversations`
- `GET /v1/conversations/{conversationId}`
- `POST /v1/messages/resolve`
- `POST /v1/messages/send`

## Experimental Routes

These routes exist only when `ADIT_ENABLE_EXPERIMENTAL_PAIRING_API=true`:

- `GET /v1/pairing/candidates`
- `POST /v1/pairing/pair`
- `POST /v1/pairing/unpair`

## Security

- The daemon only accepts loopback bind URLs.
- Browser-origin checks reject cross-site access to:
  - `GET /v1/ws`
- `GET /v1/events`
- all state-changing browser requests (`POST`, `PUT`, `PATCH`, `DELETE`)
- If `ADIT_AUTH_TOKEN` is set, API clients must send `Authorization: Bearer <token>`.
- The hosted browser UI prompts for the token after a `401` response and stores it in browser local storage on that machine.
- For websocket or SSE clients that cannot set headers during connection setup, the daemon also accepts `access_token` as a query parameter on `/v1/ws` and `/v1/events`.
