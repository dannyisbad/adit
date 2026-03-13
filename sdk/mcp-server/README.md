# adit-mcp-server

MCP stdio server for the local `Adit.Daemon`.

It exposes a small agent-focused toolset on top of the daemon:

- `adit_agent_context`
- `adit_setup_guide`
- `adit_setup_check`
- `adit_list_conversations`
- `adit_get_conversation`
- `adit_search_contacts`
- `adit_list_notifications`
- `adit_resolve_message`
- `adit_send_message`
- `adit_trigger_sync`

## Fastest Claude Code Setup In This Repo

This repo already ships the project-scoped Claude Code surfaces:

- [`../../.mcp.json`](../../.mcp.json) for the `adit` MCP server
- [`../../.claude/agents/adit-operator.md`](../../.claude/agents/adit-operator.md) for a setup-first Adit operator

Open the repo in Claude Code, approve the project `adit` MCP server, then ask Claude to "set this up".

The first tool call during setup or recovery should be `adit_setup_guide`. Re-check with `adit_setup_check` before you debug deeper. Those tools mirror the daemon's `GET /v1/setup/guide` and `POST /v1/setup/check` endpoints and are the source of truth for runtime readiness.

## Install

```bash
npm install ./sdk/mcp-server
```

## Run

```bash
ADIT_URL=http://127.0.0.1:5037 npx adit-mcp
```

If `ADIT_URL` is omitted, the server defaults to `http://127.0.0.1:5037`.

## Manual Claude Code Setup In Another Repo

Project `.mcp.json`:

```json
{
  "mcpServers": {
    "adit": {
      "command": "node",
      "args": ["./sdk/mcp-server/server.js"],
      "env": {
        "ADIT_URL": "http://127.0.0.1:5037"
      }
    }
  }
}
```

Or add the same server through the Claude Code CLI:

```bash
claude mcp add-json adit --scope project '{"command":"node","args":["/absolute/path/to/repo/sdk/mcp-server/server.js"],"env":{"ADIT_URL":"http://127.0.0.1:5037"}}'
```

## Recommended Flow

1. Call `adit_setup_guide` or `adit_agent_context` once at session start.
2. Use `adit_list_conversations` or `adit_search_contacts` to find the target.
3. Prefer `adit_resolve_message` before sending when the model is inferring a recipient.
4. Use `conversationId` for replies to a known one-to-one thread.
5. Call `adit_send_message` only after the resolved recipient looks correct.
