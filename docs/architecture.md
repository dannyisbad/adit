# Architecture

The repo is being split into product code and lab code.

## Layers

### Probe

[src/Adit.Probe](../src/Adit.Probe)

Purpose:

- keep the protocol lab alive
- preserve one-off experiments
- exercise hidden BLE, MAP, PBAP, ANCS, AMS, and RFCOMM paths without contaminating the product surface

This project is allowed to be messy. It is the place for traces, wire dumps, and transport experiments.

### Core

[src/Adit.Core](../src/Adit.Core)

Purpose:

- reusable transport adapters
- typed models
- services for device discovery, MAP messaging, PBAP contacts, and process control

Current extracted services:

- `DeviceCatalog`
- `PhoneLinkProcessCatalog`
- `PhoneLinkProcessController`
- `PbapContactsService`
- `MapMessagingService`
- `AncsSession`
- `MapRealtimeSession`

### Daemon

[src/Adit.Daemon](../src/Adit.Daemon)

Purpose:

- expose a stable local HTTP API
- hide Bluetooth transport details from client code
- become the foundation for wrappers, a UI, and automation
- centralize setup and recovery state so agents and UIs do not have to reconstruct it client-side

Representative API surface:

- `GET /v1/status`
- `GET /v1/runtime`
- `GET /v1/capabilities`
- `GET /v1/doctor`
- `GET /v1/setup/guide`
- `POST /v1/setup/check`
- `GET /v1/agent/context`
- `GET /v1/events/recent`
- `GET /v1/thread-chooser/status`
- `GET /v1/ws`
- `GET /v1/devices`
- `GET /v1/contacts`
- `GET /v1/conversations`
- `GET /v1/messages/folders`
- `GET /v1/messages`
- `POST /v1/messages/resolve`
- `POST /v1/messages/send`

See `README.md` or `src/Adit.Daemon/Program.cs` for the fuller current endpoint list.

### SDKs

- [sdk/js](../sdk/js)
- [sdk/python](../sdk/python)
- [sdk/mcp-server](../sdk/mcp-server)
- [sdk/claudebot-skill](../sdk/claudebot-skill)

Purpose:

- give external tools a zero-drama way to call the daemon
- give agent runtimes a dry-run resolution path before writes
- keep language-specific code thin and boring

## Design Rules

- `Probe` is where reverse-engineering happens.
- `Core` is where code becomes reusable.
- `Daemon` is where contracts become stable.
- SDKs stay thin and dependency-light.

## Near-Term Roadmap

1. Keep hardening the extracted `ANCS` and `MAP` realtime session surfaces in `Adit.Core`.
2. Harden the durable local store already in the daemon for contacts, messages, and notification-derived events.
3. Keep improving MAP + ANCS fusion into synthetic conversations on Windows.
4. Expand the live-update surfaces on top of the existing SSE and WebSocket streams.
