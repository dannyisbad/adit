#!/usr/bin/env node

import { pathToFileURL } from "node:url";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { AditClient, AditError } from "./lib/adit-client.js";

export function createServer({ client = new AditClient() } = {}) {
  const server = new McpServer({
    name: "adit-mcp",
    version: "1.0.0"
  });

  server.registerTool(
    "adit_agent_context",
    {
      title: "Adit Agent Context",
      description: "Return daemon readiness, capabilities, and the recommended agent workflow.",
      inputSchema: {}
    },
    async () => callTool(() => client.getAgentContext())
  );

  server.registerTool(
    "adit_setup_guide",
    {
      title: "Adit Setup Guide",
      description: "Return the daemon's structured setup and recovery guide.",
      inputSchema: {}
    },
    async () => callTool(() => client.getSetupGuide())
  );

  server.registerTool(
    "adit_setup_check",
    {
      title: "Adit Setup Check",
      description: "Re-check current setup readiness using the daemon's structured setup contract.",
      inputSchema: {}
    },
    async () => callTool(() => client.checkSetup())
  );

  server.registerTool(
    "adit_list_conversations",
    {
      title: "List Conversations",
      description: "List cached conversations from the local Adit daemon.",
      inputSchema: {
        limit: z.number().int().positive().max(200).optional(),
        deviceId: z.string().optional(),
        nameContains: z.string().optional()
      }
    },
    async args => callTool(() => client.listConversations(args))
  );

  server.registerTool(
    "adit_get_conversation",
    {
      title: "Get Conversation",
      description: "Read cached messages for a stable local conversation id.",
      inputSchema: {
        conversationId: z.string(),
        limit: z.number().int().positive().max(200).optional(),
        deviceId: z.string().optional(),
        nameContains: z.string().optional()
      }
    },
    async ({ conversationId, ...options }) =>
      callTool(() => client.getConversationMessages(conversationId, options))
  );

  server.registerTool(
    "adit_search_contacts",
    {
      title: "Search Contacts",
      description: "Search cached contacts before resolving or sending a message.",
      inputSchema: {
        query: z.string(),
        limit: z.number().int().positive().max(100).optional(),
        deviceId: z.string().optional(),
        nameContains: z.string().optional()
      }
    },
    async ({ query, ...options }) => callTool(() => client.searchContacts(query, options))
  );

  server.registerTool(
    "adit_list_notifications",
    {
      title: "List Notifications",
      description: "List cached iPhone notifications from the daemon.",
      inputSchema: {
        limit: z.number().int().positive().max(100).optional(),
        activeOnly: z.boolean().optional(),
        appIdentifier: z.string().optional(),
        deviceId: z.string().optional(),
        nameContains: z.string().optional()
      }
    },
    async args => callTool(() => client.listNotifications(args))
  );

  server.registerTool(
    "adit_resolve_message",
    {
      title: "Resolve Message",
      description: "Dry-run target and recipient resolution with no send side effects.",
      inputSchema: {
        recipient: z.string().optional(),
        body: z.string().optional(),
        contactId: z.string().optional(),
        contactName: z.string().optional(),
        preferredNumber: z.string().optional(),
        conversationId: z.string().optional(),
        deviceId: z.string().optional(),
        nameContains: z.string().optional(),
        evictPhoneLink: z.boolean().optional()
      }
    },
    async args => callTool(() => client.resolveMessage(args))
  );

  server.registerTool(
    "adit_send_message",
    {
      title: "Send Message",
      description: "Send a message through the local Adit daemon.",
      inputSchema: {
        body: z.string(),
        recipient: z.string().optional(),
        contactId: z.string().optional(),
        contactName: z.string().optional(),
        preferredNumber: z.string().optional(),
        conversationId: z.string().optional(),
        deviceId: z.string().optional(),
        nameContains: z.string().optional(),
        autoSyncAfterSend: z.boolean().optional(),
        evictPhoneLink: z.boolean().optional()
      }
    },
    async args => callTool(() => client.sendMessage(args))
  );

  server.registerTool(
    "adit_trigger_sync",
    {
      title: "Trigger Sync",
      description: "Request an immediate daemon sync cycle.",
      inputSchema: {
        reason: z.string().optional()
      }
    },
    async ({ reason }) => callTool(() => client.triggerSync(reason))
  );

  return server;
}

async function main() {
  const server = createServer();
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

async function callTool(action) {
  try {
    return toToolResult(await action());
  } catch (error) {
    return toToolError(error);
  }
}

function toToolResult(payload) {
  return {
    content: [
      {
        type: "text",
        text: JSON.stringify(payload, null, 2)
      }
    ],
    structuredContent: payload
  };
}

if (isEntrypoint(import.meta.url)) {
  main().catch(error => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}

function isEntrypoint(metaUrl) {
  return typeof process.argv[1] === "string" && pathToFileURL(process.argv[1]).href === metaUrl;
}

function toToolError(error) {
  const message =
    error instanceof AditError
      ? error.message
      : error instanceof Error
        ? error.message
        : String(error);

  return {
    content: [
      {
        type: "text",
        text: message
      }
    ],
    structuredContent: {
      ok: false,
      error: message,
      status: error instanceof AditError ? error.status : null,
      body: error instanceof AditError ? error.body : null
    },
    isError: true
  };
}
