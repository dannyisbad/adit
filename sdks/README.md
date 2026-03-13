# SDK Index

The repo historically used a singular `sdk/` folder, so the actual implementations still live there.

Use this index as the agent-facing map:

- JavaScript client: [`../sdk/js`](../sdk/js)
- Python client: [`../sdk/python`](../sdk/python)
- MCP server: [`../sdk/mcp-server`](../sdk/mcp-server)
- Claudebot skill: [`../sdk/claudebot-skill`](../sdk/claudebot-skill)

All SDK surfaces follow the same daemon-first setup contract. During setup or recovery, start with the wrapper or tool equivalent of `GET /v1/setup/guide`, then refresh with `POST /v1/setup/check`, and use `GET /v1/agent/context` when you need the combined readiness and workflow snapshot.

## Claude Code Fast Path

In this repo, start with the checked-in Claude Code surfaces instead of hand-editing config:

- [`../.mcp.json`](../.mcp.json)
- [`../.claude/agents/adit-operator.md`](../.claude/agents/adit-operator.md)

Open the repo in Claude Code, approve the project `adit` server, then ask Claude to "set this up".

## Recommended Agent Stack

1. Project `.mcp.json` plus `.claude/agents/adit-operator.md` for shared Claude Code usage in this repo.
2. `sdk/mcp-server` if the host supports MCP but cannot consume the repo's project files directly.
3. `sdk/js` or `sdk/python` for direct programmatic integrations.
4. `sdk/claudebot-skill` for agent instructions and safe send guidance outside the shared Claude Code path.
