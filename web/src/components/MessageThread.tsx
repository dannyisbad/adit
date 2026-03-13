import { useRef, useEffect, useState, useCallback } from "react";
import { useApp } from "../context/AppContext";
import {
  cleanDisplayText,
  conversationTitle,
  conversationParticipantCount,
  dateSeparatorLabel,
  describeMessageContent,
  isSentMessage,
  messageTimestampSource,
  messageTimestampValue,
  participantAddressSummary,
  participantSummary,
  cn,
} from "../lib/utils";
import { MessageBubble } from "./MessageBubble";

function comparableMessageText(value: string): string {
  return cleanDisplayText(value, { multiline: true })
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function MessageThread() {
  const { state } = useApp();
  const scrollRef = useRef<HTMLDivElement>(null);
  const [autoScroll, setAutoScroll] = useState(true);
  const prevMessageCount = useRef(0);

  useEffect(() => {
    if (autoScroll && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
    prevMessageCount.current = state.activeMessages.length;
  }, [state.activeMessages, autoScroll]);

  const handleScroll = useCallback(() => {
    const element = scrollRef.current;
    if (!element) return;
    const nearBottom = element.scrollHeight - element.scrollTop - element.clientHeight < 80;
    setAutoScroll(nearBottom);
  }, []);

  if (!state.activeConversationId) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <p className="text-text-secondary text-sm">Select a conversation</p>
      </div>
    );
  }

  type RenderedMessage = {
    message: typeof state.activeMessages[number];
    reactions: typeof state.activeMessages;
  };

  const canAttachReactionToMessage = (
    reactionMessage: typeof state.activeMessages[number],
    anchorMessage: typeof state.activeMessages[number],
  ) => {
    const reactionContent = describeMessageContent(reactionMessage.message);
    if (reactionContent.kind !== "reaction" || !reactionContent.secondaryText) {
      return false;
    }

    const anchorContent = describeMessageContent(anchorMessage.message);
    if (anchorContent.kind !== "text") {
      return false;
    }

    const reactionQuote = comparableMessageText(reactionContent.secondaryText);
    const anchorText = comparableMessageText(anchorContent.primaryText);
    if (!reactionQuote || !anchorText) {
      return false;
    }

    if (anchorText === reactionQuote || anchorText.includes(reactionQuote) || reactionQuote.includes(anchorText)) {
      return true;
    }

    const reactionTokens = Array.from(new Set(reactionQuote.split(" ").filter(Boolean)));
    const anchorTokens = Array.from(new Set(anchorText.split(" ").filter(Boolean)));
    if (reactionTokens.length === 0 || anchorTokens.length === 0) {
      return false;
    }

    const reactionTokenSet = new Set(reactionTokens);
    const overlapCount = anchorTokens.filter((token) => reactionTokenSet.has(token)).length;
    const requiredOverlap = Math.max(2, Math.ceil(Math.min(reactionTokens.length, anchorTokens.length) * 0.6));
    if (overlapCount < requiredOverlap) {
      return false;
    }

    const reactionTimestamp = messageTimestampValue(reactionMessage);
    const anchorTimestamp = messageTimestampValue(anchorMessage);
    if (reactionTimestamp === null || anchorTimestamp === null) {
      return true;
    }

    return reactionTimestamp >= anchorTimestamp
      && reactionTimestamp - anchorTimestamp <= 1000 * 60 * 60 * 48;
  };

  const renderedMessages: RenderedMessage[] = [];
  for (const message of state.activeMessages) {
    const messageContent = describeMessageContent(message.message);
    if (messageContent.kind === "reaction" && renderedMessages.length > 0) {
      let attached = false;
      for (let index = renderedMessages.length - 1; index >= 0 && renderedMessages.length - index <= 8; index--) {
        const candidate = renderedMessages[index];
        if (candidate && canAttachReactionToMessage(message, candidate.message)) {
          candidate.reactions.push(message);
          attached = true;
          break;
        }
      }

      if (attached) {
        continue;
      }
    }

    renderedMessages.push({ message, reactions: [] });
  }

  const groups: { label: string; messages: RenderedMessage[] }[] = [];
  let currentLabel = "";
  for (const item of renderedMessages) {
    const timestamp = messageTimestampSource(item.message);
    const label = timestamp ? dateSeparatorLabel(timestamp) : "";
    if (groups.length === 0 || label !== currentLabel) {
      currentLabel = label;
      groups.push({ label, messages: [] });
    }
    groups[groups.length - 1]?.messages.push(item);
  }

  const activeConversation = state.conversations.find(
    (conversation) => conversation.conversationId === state.activeConversationId,
  );

  const otherParticipants = activeConversation?.participants.filter((participant) => !participant.isSelf) ?? [];
  const displayPhone = cleanDisplayText(otherParticipants[0]?.phones[0] ?? otherParticipants[0]?.emails[0]);
  const title = cleanDisplayText(conversationTitle(activeConversation, 6)) || state.activeConversationId;
  const groupSummary = activeConversation?.isGroup
    ? participantSummary(activeConversation.participants, 6)
    : "";
  const groupCount = activeConversation?.isGroup && activeConversation
    ? conversationParticipantCount(activeConversation)
    : 0;
  const showGroupSummary = Boolean(
    activeConversation?.isGroup
    && groupSummary
    && groupSummary.toLowerCase() !== title.toLowerCase(),
  );
  const groupMeta = activeConversation?.isGroup
    ? [
      groupCount > 0 ? `${groupCount} member${groupCount === 1 ? "" : "s"}` : null,
      showGroupSummary ? groupSummary : null,
    ].filter(Boolean).join(" | ")
    : "";
  const groupAddresses = activeConversation?.isGroup
    ? participantAddressSummary(activeConversation.participants, 3)
    : "";

  return (
    <div className="flex-1 flex flex-col min-h-0">
      <div className="px-5 py-3 border-b border-border shrink-0">
        <h2
          className="text-lg font-semibold text-text-primary"
          style={{ fontFamily: "var(--font-display)" }}
        >
          {title}
        </h2>
        {activeConversation?.isGroup ? (
          <div className="mt-1 space-y-0.5">
            <div className="text-xs text-text-secondary leading-relaxed">
              {groupMeta || `${otherParticipants.length} member${otherParticipants.length === 1 ? "" : "s"}`}
            </div>
            {groupAddresses && (
              <div
                className="text-[11px] text-text-secondary truncate"
                style={{ fontFamily: "var(--font-mono)" }}
              >
                {groupAddresses}
              </div>
            )}
          </div>
        ) : displayPhone ? (
          <span
            className="text-xs text-text-secondary"
            style={{ fontFamily: "var(--font-mono)" }}
          >
            {displayPhone}
          </span>
        ) : null}
      </div>

      <div
        ref={scrollRef}
        onScroll={handleScroll}
        className={cn(
          "flex-1 overflow-y-auto py-4",
          !autoScroll && "scroll-shadow-top",
        )}
      >
        {state.activeMessages.length === 0 ? (
          <div className="flex items-center justify-center h-full">
            <p className="text-text-secondary text-sm">No messages yet</p>
          </div>
        ) : (
          groups.map((group) => (
            <div key={`${group.label || "undated"}-${group.messages[0]?.message.messageKey ?? "empty"}`}>
              {group.label && (
                <div className="flex items-center gap-3 px-5 py-3">
                  <div className="flex-1 h-px bg-border" />
                  <span
                    className="text-[11px] text-text-secondary uppercase tracking-wider"
                    style={{ fontFamily: "var(--font-mono)", fontVariant: "small-caps" }}
                  >
                    {group.label}
                  </span>
                  <div className="flex-1 h-px bg-border" />
                </div>
              )}
              {group.messages.map((entry, index) => {
                const message = entry.message;
                const isLast = index === group.messages.length - 1;
                const isNew = index >= prevMessageCount.current;
                const nextMessage = group.messages[index + 1];
                const showTime =
                  isLast
                  || (nextMessage && isSentMessage(nextMessage.message.message) !== isSentMessage(message.message));
                return (
                  <MessageBubble
                    key={message.messageKey}
                    message={message}
                    reactions={entry.reactions}
                    showTime={showTime}
                    animate={isNew}
                  />
                );
              })}
            </div>
          ))
        )}
      </div>
    </div>
  );
}
