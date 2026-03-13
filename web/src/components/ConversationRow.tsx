import type { ConversationSnapshot } from "../lib/types";
import {
  cleanDisplayText,
  conversationTitle,
  conversationParticipantCount,
  conversationPreviewText,
  participantSummary,
  relativeTime,
  cn,
} from "../lib/utils";
import { Avatar } from "./Avatar";

interface ConversationRowProps {
  conversation: ConversationSnapshot;
  isActive: boolean;
  onClick: () => void;
}

export function ConversationRow({
  conversation,
  isActive,
  onClick,
}: ConversationRowProps) {
  const title = cleanDisplayText(conversationTitle(conversation, 4)) || "Unknown conversation";
  const groupSummary = conversation.isGroup
    ? participantSummary(conversation.participants, 4)
    : "";
  const groupCount = conversation.isGroup
    ? conversationParticipantCount(conversation)
    : 0;
  const preview = conversationPreviewText(conversation);
  const showGroupSummary =
    conversation.isGroup
    && groupSummary
    && groupSummary.toLowerCase() !== title.toLowerCase();
  const groupMeta = conversation.isGroup
    ? [
      groupCount > 0 ? `${groupCount} members` : null,
      showGroupSummary ? groupSummary : null,
    ].filter(Boolean).join(" | ")
    : "";

  return (
    <button
      onClick={onClick}
      className={cn(
        "w-full flex items-center gap-3 px-4 py-3 text-left transition-colors relative",
        isActive
          ? "bg-base border-l-[3px] border-accent"
          : "hover:bg-surface border-l-[3px] border-transparent",
      )}
    >
      <Avatar name={title} size={42} />
      <div className="flex-1 min-w-0">
        <div className="flex items-baseline justify-between gap-2">
          <span
            className="font-medium text-sm truncate text-text-primary"
            style={{ fontFamily: "var(--font-display)" }}
          >
            {title}
          </span>
          <span
            className="text-[11px] text-text-secondary shrink-0"
            style={{ fontFamily: "var(--font-mono)" }}
          >
            {relativeTime(conversation.lastMessageUtc)}
          </span>
        </div>
        {groupMeta && (
          <div className="text-[11px] text-text-secondary truncate mt-0.5">
            {groupMeta}
          </div>
        )}
        <div className="flex items-center gap-2 mt-0.5">
          <span className="text-xs text-text-secondary truncate flex-1">
            {preview}
          </span>
          {conversation.unreadCount > 0 && (
            <span className="w-2 h-2 rounded-full bg-accent shrink-0" />
          )}
        </div>
      </div>
    </button>
  );
}
