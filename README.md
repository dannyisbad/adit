# adit

A Windows-first iPhone bridge daemon. Send and receive iMessages, sync
contacts, stream iOS notifications over Bluetooth from a stock iPhone to a
stock Windows PC. No jailbreak, no iCloud.

Uses the protocols a stock iPhone actually exposes: **MAP** (messaging),
**PBAP** (contacts), **ANCS** (notifications). The daemon runs locally and
exposes a full HTTP API and WebSocket event stream.

The API is stable and usable day-to-day. The React frontend is a WIP and may
break. Notifications are beta.

## What works

- SMS/iMessage send and receive via MAP, with live MNS push events
- Contact sync via PBAP, with normalized phone number resolution
- iOS notification streaming and action execution via ANCS (beta)
- Stable local conversation and message IDs (`th_...` / `msg_...`)
- SQLite cache with background sync, fusion coordinator, raw observation persistence
- REST API with endpoints for messaging, contacts, notifications, sync, health, diagnostics
- WebSocket event stream
- JS, Python, MCP, and Claude Code SDKs
- Learned thread chooser for conversation routing (experimental, off by default)

## What this is not

Not full iMessage parity. No media sync, no Apple conversation IDs, no
reaction/effect support. This works within what Bluetooth MAP/PBAP/ANCS
actually provides.

## Architecture

```
src/Adit.Core          Bluetooth / MAP / PBAP / ANCS interop layer
 |
 +-- Models/            typed C# models mirroring protocol records
 +-- Services/
      +-- MapMessagingService       MAP send, read, folder listing, MNS events
      +-- PbapContactsService       phonebook sync, normalized number resolution
      +-- AncsSession               ANCS notification receive + action execution
      +-- DeviceCatalog             paired device discovery
      +-- PhoneLinkProcessController  Phone Link eviction and process control

src/Adit.Daemon        ASP.NET Core HTTP API, sync engine, SQLite cache
 |
 +-- Program.cs         REST endpoints + WebSocket + static file serving
 +-- Services/
      +-- SqliteCacheStore          messages, contacts, notifications, observations
      +-- DeviceSyncService         background MAP/PBAP sync orchestration
      +-- FusionCoordinator         per-device mutation serialization

web/                   React 19 + TypeScript + Tailwind v4 + Vite (WIP)
src/Adit.Probe         protocol reverse-engineering lab
sdk/                   JS, Python, MCP server, agent skill
training/              thread chooser ML pipeline
tests/                 core, daemon, and probe tests
docs/                  API reference, architecture, troubleshooting
```

## Prerequisites

- Windows 10/11 with Bluetooth
- iPhone (stock, no jailbreak)
- .NET 10 SDK
- Microsoft **Phone Link** installed

`Adit.Core` directly references internal assemblies from the installed Phone
Link package. If auto-discovery fails, set `PhoneLinkInstallDir` manually:

```powershell
$env:PhoneLinkInstallDir = (Get-AppxPackage Microsoft.YourPhone).InstallLocation
```

## Getting started

The setup flow is still a work in progress. If you get stuck, ask Claude or
Codex.

### Pair via Phone Link first

You must complete iPhone pairing through Phone Link before running adit. Adit
bootstraps from the Link to Windows pairing state.

1. Open **Phone Link** on Windows
2. Follow the **Link to Windows** setup on your iPhone
3. Accept every permission prompt on the iPhone
4. Wait until Phone Link finishes its first sync

Do not start adit until this is done. After pairing, adit takes over. Phone
Link does not need to stay open.

### Build and run

```powershell
dotnet build Adit.sln
cd web && npm install && npm run build && cd ..
dotnet run --project src\Adit.Daemon -- serve
```

The daemon binds to `http://127.0.0.1:5037` (localhost only). The frontend is
served at the same address if `web/dist` was built.

## Security Model

- The daemon binds to loopback only by default. Remote exposure still requires an explicit external proxy or tunnel.
- Browser-origin checks reject cross-site websocket/event-stream requests and cross-site browser writes to localhost.
- If you set `ADIT_AUTH_TOKEN`, non-browser API clients must send `Authorization: Bearer <token>`.
- The hosted browser UI prompts for the token on first `401` and stores it in browser local storage on that machine.
- WebSocket clients can also authenticate with `?access_token=...` when a client library cannot set headers during the upgrade.

### Verify

```powershell
Invoke-RestMethod http://127.0.0.1:5037/v1/doctor
Invoke-RestMethod http://127.0.0.1:5037/v1/capabilities
```

Quick checks without the full runtime:

```powershell
dotnet run --project src\Adit.Daemon -- devices
dotnet run --project src\Adit.Daemon -- doctor
```

## API

Local REST API. Full reference: [docs/api.md](docs/api.md).

Health and setup:
`GET /v1/status` · `/v1/runtime` · `/v1/capabilities` · `/v1/doctor` · `/v1/setup/guide`

Messaging:
`GET /v1/conversations` · `/v1/conversations/{id}` · `POST /v1/messages/send` · `/v1/sync/now`

Contacts:
`GET /v1/contacts` · `/v1/contacts/search`

Notifications (beta):
`GET /v1/notifications` · `POST /v1/notifications/{uid}/actions/{action}` · `/v1/notifications/enable` · `/v1/notifications/disable`

Real-time:
`GET /v1/ws` (WebSocket) · `/v1/events/recent`

## SDKs

| Surface | Path | Use case |
|---|---|---|
| JavaScript | `sdk/js` | Node client |
| Python | `sdk/python` | Python client |
| MCP Server | `sdk/mcp-server` | Stdio MCP server for agent runtimes |
| Agent Skill | `sdk/claudebot-skill` | Agent instructions |
| Claude Code | `.mcp.json` + `.claude/agents/` | Open repo in Claude Code, approve the `adit` server, ask it to "set this up" |

All SDKs wrap the local daemon API. None speak Bluetooth directly.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `ADIT_URL` | `http://127.0.0.1:5037` | Daemon bind address (localhost only) |
| `ADIT_AUTH_TOKEN` | unset | Optional bearer token for SDKs, CLI tools, MCP, and other non-browser API clients |
| `ADIT_AUTO_EVICT_PHONE_LINK` | `true` (post-bootstrap) | Evict Phone Link to claim MAP/PBAP |
| `ADIT_ENCRYPT_DB_AT_REST` | `true` | Windows EFS encryption for SQLite cache |
| `ADIT_ENABLE_LEARNED_THREAD_CHOOSER` | `false` | Experimental ML thread routing. Weights (`.pt`) are stored with [Git LFS](https://git-lfs.com) — run `git lfs pull` after cloning. |
| `ADIT_ENABLE_EXPERIMENTAL_PAIRING_API` | `false` | Native pairing lab endpoints |

## Docs

- [API reference](docs/api.md)
- [Architecture](docs/architecture.md)
- [Setup troubleshooting](docs/setup-troubleshooting.md)
- [Thread model data](docs/thread-model-data.md)

## License

MIT
