import { useState, useRef, useEffect, useCallback, type KeyboardEvent } from "react";
import { useApp } from "../context/AppContext";
import { api } from "../lib/api";
import type { SynthesizedMessageRecord } from "../lib/types";

export function MessageInput() {
  const { state, dispatch } = useApp();
  const [text, setText] = useState("");
  const [sending, setSending] = useState(false);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const activeConv = state.conversations.find(
    (c) => c.conversationId === state.activeConversationId,
  );
  const isGroupConversation = activeConv?.isGroup ?? false;

  const canSend =
    text.trim().length > 0 &&
    !sending &&
    state.connectionStatus !== "disconnected" &&
    !!state.activeConversationId &&
    !isGroupConversation;

  useEffect(() => {
    const ta = textareaRef.current;
    if (!ta) return;
    ta.style.height = "auto";
    ta.style.height = `${Math.min(ta.scrollHeight, 120)}px`;
  }, [text]);

  const send = useCallback(async () => {
    if (!canSend || !activeConv) return;

    const body = text.trim();
    const optimisticKey = `opt-${Date.now()}`;
    setText("");
    setSending(true);
    dispatch({ type: "SET_ERROR", error: null });

    // Group reconstruction is best-effort today; sending through the first visible participant
    // would silently misaddress the message.
    if (activeConv.isGroup) {
      dispatch({
        type: "SET_ERROR",
        error: "Group sends are not supported yet.",
      });
      setSending(false);
      return;
    }

    // Find recipient phone
    const otherParticipants = activeConv.participants.filter((p) => !p.isSelf);
    const recipient = otherParticipants[0]?.phones[0];
    const contactName = otherParticipants[0]?.displayName;

    // Optimistic message
    const optimistic: SynthesizedMessageRecord = {
      messageKey: optimisticKey,
      conversationId: activeConv.conversationId,
      conversationDisplayName: activeConv.displayName,
      isGroup: activeConv.isGroup,
      sortTimestampUtc: new Date().toISOString(),
      participants: activeConv.participants,
      message: {
        folder: "outbox",
        body,
        sent: true,
        type: "SMS_GSM",
        originators: [],
        recipients: [],
      },
    };
    dispatch({ type: "APPEND_OPTIMISTIC_MESSAGE", message: optimistic });

    try {
      await api.sendMessage({
        recipient: recipient ?? undefined,
        contactName: !recipient ? contactName : undefined,
        body,
        autoSyncAfterSend: true,
      });
    } catch (err) {
      dispatch({ type: "REMOVE_ACTIVE_MESSAGE", messageKey: optimisticKey });
      setText(body);
      dispatch({
        type: "SET_ERROR",
        error: `Failed to send: ${(err as Error).message}`,
      });
    } finally {
      setSending(false);
      textareaRef.current?.focus();
    }
  }, [canSend, activeConv, text, dispatch]);

  const handleKeyDown = useCallback(
    (e: KeyboardEvent<HTMLTextAreaElement>) => {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        send();
      }
    },
    [send],
  );

  if (!state.activeConversationId) return null;

  return (
    <div className="border-t border-border px-4 py-3 shrink-0">
      <div className="flex items-end gap-2">
        <textarea
          ref={textareaRef}
          value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={isGroupConversation ? "Group send is not supported yet" : "Message"}
          rows={1}
          disabled={sending || isGroupConversation}
          className="flex-1 resize-none rounded-lg bg-surface border border-border px-3 py-2 text-sm text-text-primary placeholder:text-text-secondary outline-none focus:border-accent transition-colors disabled:opacity-50"
          style={{ fontFamily: "var(--font-body)", maxHeight: 120 }}
        />
        <button
          onClick={send}
          disabled={!canSend}
          aria-label="Send message"
          className="shrink-0 w-9 h-9 rounded-lg bg-accent text-white flex items-center justify-center transition-colors hover:bg-accent-hover disabled:opacity-30 disabled:cursor-not-allowed"
        >
          <svg
            width="18"
            height="18"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <path d="M22 2L11 13" />
            <path d="M22 2L15 22L11 13L2 9L22 2Z" />
          </svg>
        </button>
      </div>
    </div>
  );
}
