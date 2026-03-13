# adit-claudebot-skill

Reusable skill folder for agents that need to operate the local `Adit.Daemon` safely.

The canonical instructions live in [SKILL.md](./SKILL.md).

## Claude Code Fast Path

In this repo, the easiest setup path is already checked in:

- [`../../.mcp.json`](../../.mcp.json) wires the project-scoped `adit` MCP server
- [`../../.claude/agents/adit-operator.md`](../../.claude/agents/adit-operator.md) gives Claude a setup-first Adit specialist

Open the repo in Claude Code, approve the project `adit` server, then ask Claude to "set this up".

Use `SKILL.md` as the canonical prompt text when you need the same behavior in another host or another repo.
