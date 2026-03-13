# Agent Setup

Adit is designed so an agent can set it up and operate it with minimal guesswork. The daemon exposes a structured setup contract that tells you exactly what is ready, what is blocked, and what to do next.

## Quick Start

```
dotnet build Adit.sln
cd web && npm install && npm run build && cd ..
dotnet run --project src/Adit.Daemon -- serve
```

Then hit `GET /v1/setup/guide`. If `state` is `needs_bootstrap`, tell the user to complete Link to Windows pairing on their iPhone — that is the only step an agent cannot do. Once `state` is `complete`, you are operational.

## Why It Works

- **The daemon is the source of truth.** `GET /v1/setup/guide` returns structured steps with `status`, `blocking`, and `recommendedAction` fields. Follow them instead of inventing your own checklist.
- **No config to hand-write.** `.mcp.json` wires the MCP server and `.claude/agents/adit-operator.md` provides the operator persona — both are checked in.
- **The only human-required step is physical.** Link to Windows pairing needs the user's iPhone, Bluetooth, and tapping "accept" on permission prompts. Everything else is automatable.

## Key Endpoints for Agents

| Endpoint | Purpose |
|---|---|
| `GET /v1/setup/guide` | Structured setup steps, actions, and integration pointers |
| `POST /v1/setup/check` | Fresh setup snapshot on demand |
| `GET /v1/doctor` | Capability-by-capability readiness check |
| `GET /v1/agent/context` | Combined runtime and workflow summary for agent consumption |
| `POST /v1/sync/now` | Force an immediate sync |

## Claude Code

Open the repo in Claude Code and approve the project `adit` MCP server. The checked-in surfaces handle the rest:

- [`.mcp.json`](.mcp.json) — project-scoped MCP server pointing at `./sdk/mcp-server/server.js`
- [`.claude/agents/adit-operator.md`](.claude/agents/adit-operator.md) — Adit operator subagent with setup-first workflow, safe send rules, and recovery guidance

Ask Claude to "set this up" and it will follow the daemon's setup guide automatically.

## Other Agent Surfaces

- [`sdk/mcp-server`](sdk/mcp-server) — standalone MCP server for hosts that support MCP but not project files
- [`sdk/claudebot-skill`](sdk/claudebot-skill) — canonical prompt instructions for safe setup and messaging outside Claude Code
- [`sdk/js`](sdk/js) / [`sdk/python`](sdk/python) — thin client wrappers over the daemon HTTP API
- [`sdks/README.md`](sdks/README.md) — agent-facing index of all SDK surfaces
