import { useState, useMemo, useCallback, type ReactNode } from "react";
import { useApp } from "../context/AppContext";
import { cleanDisplayText, conversationPreviewText } from "../lib/utils";
import { SearchInput } from "./SearchInput";
import { ConversationRow } from "./ConversationRow";

interface ConversationListProps {
  searchFocusTrigger: number;
  headerActions?: ReactNode;
  onConversationSelect?: (id: string) => void;
}

export function ConversationList({
  searchFocusTrigger,
  headerActions,
  onConversationSelect,
}: ConversationListProps) {
  const { state, selectConversation } = useApp();
  const [search, setSearch] = useState("");

  const filtered = useMemo(() => {
    if (!search.trim()) return state.conversations;
    const q = search.toLowerCase();
    return state.conversations.filter((c) =>
      cleanDisplayText(c.displayName).toLowerCase().includes(q)
      || conversationPreviewText(c).toLowerCase().includes(q)
      || cleanDisplayText(c.lastSenderDisplayName).toLowerCase().includes(q)
      || c.participants.some((participant) =>
        cleanDisplayText(participant.displayName).toLowerCase().includes(q),
      ),
    );
  }, [state.conversations, search]);

  const handleSelect = useCallback(
    (id: string) => {
      selectConversation(id);
      onConversationSelect?.(id);
    },
    [selectConversation, onConversationSelect],
  );

  return (
    <div className="flex flex-col h-full">
      <div className="px-4 pt-4 pb-1 flex items-center justify-between">
        <h1
          className="text-xl font-semibold text-text-primary"
          style={{ fontFamily: "var(--font-display)" }}
        >
          Messages
        </h1>
        {headerActions && (
          <div className="flex items-center gap-1">
            {headerActions}
          </div>
        )}
      </div>

      <SearchInput
        value={search}
        onChange={setSearch}
        placeholder="Search conversations"
        focusTrigger={searchFocusTrigger}
      />

      <div className="flex-1 overflow-y-auto">
        {state.loading ? (
          <div className="px-4 py-3 space-y-3">
            {Array.from({ length: 6 }).map((_, i) => (
              <div key={i} className="flex items-center gap-3">
                <div className="w-[42px] h-[42px] rounded-full skeleton" />
                <div className="flex-1 space-y-2">
                  <div className="h-3.5 w-24 skeleton" />
                  <div className="h-3 w-40 skeleton" />
                </div>
              </div>
            ))}
          </div>
        ) : filtered.length === 0 ? (
          <div className="px-4 py-8 text-center text-sm text-text-secondary">
            {search ? "No conversations match your search." : "No conversations yet."}
          </div>
        ) : (
          filtered.map((c) => (
            <ConversationRow
              key={c.conversationId}
              conversation={c}
              isActive={c.conversationId === state.activeConversationId}
              onClick={() => handleSelect(c.conversationId)}
            />
          ))
        )}
      </div>
    </div>
  );
}
