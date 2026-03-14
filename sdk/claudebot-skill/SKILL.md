# adit

Use this skill when the user wants an agent to set up Adit in Claude, inspect conversations, search contacts, or send messages through the local `Adit.Daemon`.

## Goal

Operate the daemon through stable local APIs and the daemon's own setup contract instead of hand-rolling Bluetooth or Windows pairing logic.

## Setup Mode

Treat requests like "set this up", "wire this into Claude", "fix the Adit MCP", and "recover setup" as setup mode.

1. Prefer the repo's checked-in Claude Code surfaces when you are inside this checkout.
2. Start with the daemon setup contract, not a custom checklist.
3. If MCP is available, call `adit_setup_guide` first and `adit_setup_check` when you need a refreshed snapshot.
4. If MCP is not available yet, call `GET /v1/setup/guide` or `POST /v1/setup/check` directly.
5. Use `steps`, `actions`, and `integrations` from the setup guide before suggesting manual edits.
6. Prefer project-scoped setup in this repo:
   - `.mcp.json` should point Claude Code at `./sdk/mcp-server/server.js`
   - if the daemon is using `ADIT_AUTH_TOKEN`, `.mcp.json` should pass it through in the MCP server env
   - `.claude/agents/adit-operator.md` should exist so Claude has an Adit-specific operator
7. If setup is blocked on a real-world prerequisite like Link to Windows pairing, the phone being nearby/unlocked, or a daemon build/runtime failure, report the blocker clearly instead of inventing a workaround.

## Default Operating Workflow

1. Call `GET /v1/agent/context` or `adit_agent_context` first.
2. During setup or recovery work, prefer `GET /v1/setup/guide` / `POST /v1/setup/check` or `adit_setup_guide` / `adit_setup_check`.
3. Read `setup`, `capabilities`, `doctor`, and any `integrations` before attempting writes.
4. Prefer cached `conversationId` values from `GET /v1/conversations` for reply flows.
5. If the recipient is inferred, call `POST /v1/messages/resolve` or `adit_resolve_message` before `POST /v1/messages/send`.
6. Only send after the resolved target and recipient are clearly correct.

## Safe Send Rules

- Prefer `conversationId` for replies to known one-to-one threads.
- If `conversationId` is unavailable, use `contactId` or `contactName`.
- Use raw `recipient` only when the phone number is explicit and trusted.
- If resolution is ambiguous, stop and ask the user before sending.
- Never assume that omitting recipient fields is safe.

## Useful MCP Tools

- `adit_agent_context`
- `adit_setup_guide`
- `adit_setup_check`
- `adit_list_conversations`
- `adit_get_conversation`
- `adit_search_contacts`
- `adit_resolve_message`
- `adit_send_message`
- `adit_trigger_sync`

## Useful HTTP Endpoints

- `GET /v1/agent/context`
- `GET /v1/setup/guide`
- `POST /v1/setup/check`
- `GET /v1/conversations`
- `GET /v1/conversations/{conversationId}`
- `GET /v1/contacts/search?query=...`
- `POST /v1/messages/resolve`
- `POST /v1/messages/send`
- `POST /v1/sync/now`

## Example Resolve Payload

```json
{
  "conversationId": "th_123",
  "body": "on my way"
}
```

## Example Send Payload

```json
{
  "conversationId": "th_123",
  "body": "on my way",
  "autoSyncAfterSend": true
}
```
