import { useState, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { useApp } from "../context/AppContext";
import { ConversationList } from "../components/ConversationList";
import { NotificationDrawer } from "../components/NotificationDrawer";
import { NotificationToasts } from "../components/NotificationToast";
import { MessageThread } from "../components/MessageThread";
import { MessageInput } from "../components/MessageInput";
import { StatusStrip } from "../components/StatusStrip";
import { cn } from "../lib/utils";

export function Messages() {
  const navigate = useNavigate();
  const { state, selectConversation } = useApp();
  const [mobileShowThread, setMobileShowThread] = useState(false);
  const [searchFocus, setSearchFocus] = useState(0);
  const [drawerOpen, setDrawerOpen] = useState(false);

  // When active conversation changes on mobile, show the thread
  useEffect(() => {
    if (state.activeConversationId) {
      setMobileShowThread(true);
    }
  }, [state.activeConversationId]);

  const handleBack = useCallback(() => {
    setMobileShowThread(false);
  }, []);

  const handleConversationSelect = useCallback(
    (id: string) => {
      selectConversation(id);
      setMobileShowThread(true);
    },
    [selectConversation],
  );

  // Keyboard shortcuts
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.ctrlKey && e.key === "k") {
        e.preventDefault();
        setSearchFocus((n) => n + 1);
      }
      if (e.key === "Escape") {
        setSearchFocus(0);
        setDrawerOpen(false);
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, []);

  // Navigate conversations with keyboard
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.target instanceof HTMLTextAreaElement || e.target instanceof HTMLInputElement) return;
      if (e.key !== "ArrowUp" && e.key !== "ArrowDown") return;
      if (drawerOpen) return;
      e.preventDefault();

      const conversations = state.conversations;
      if (conversations.length === 0) return;

      const currentIdx = conversations.findIndex(
        (c) => c.conversationId === state.activeConversationId,
      );

      let nextIdx: number;
      if (e.key === "ArrowDown") {
        nextIdx = currentIdx < conversations.length - 1 ? currentIdx + 1 : 0;
      } else {
        nextIdx = currentIdx > 0 ? currentIdx - 1 : conversations.length - 1;
      }

      const next = conversations[nextIdx];
      if (next) selectConversation(next.conversationId);
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [state.conversations, state.activeConversationId, selectConversation, drawerOpen]);

  const notifCount = state.runtime?.notificationCount ?? 0;

  const headerActions = (
    <>
      {/* Notification bell */}
      <button
        onClick={() => setDrawerOpen(true)}
        aria-label="Open notifications"
        className="relative p-1.5 rounded-lg text-text-secondary hover:text-text-primary hover:bg-surface transition-colors"
        title="Notifications"
      >
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9" />
          <path d="M13.73 21a2 2 0 0 1-3.46 0" />
        </svg>
        {notifCount > 0 && (
          <span className="absolute -top-0.5 -right-0.5 w-4 h-4 rounded-full bg-accent text-white text-[9px] flex items-center justify-center font-bold">
            {notifCount > 99 ? "99" : notifCount}
          </span>
        )}
      </button>

      {/* Settings gear */}
      <button
        onClick={() => navigate("/settings")}
        aria-label="Open settings"
        className="p-1.5 rounded-lg text-text-secondary hover:text-text-primary hover:bg-surface transition-colors"
        title="Settings"
      >
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <circle cx="12" cy="12" r="3" />
          <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z" />
        </svg>
      </button>
    </>
  );

  return (
    <div className="flex flex-col h-full">
      <div className="flex flex-1 min-h-0">
        {/* Sidebar — always conversation list */}
        <div
          className={cn(
            "w-full md:w-80 border-r border-border bg-sidebar shrink-0 flex flex-col",
            mobileShowThread ? "hidden md:flex" : "flex",
          )}
        >
          <ConversationList
            searchFocusTrigger={searchFocus}
            headerActions={headerActions}
            onConversationSelect={handleConversationSelect}
          />
        </div>

        {/* Thread */}
        <div
          className={cn(
            "flex-1 flex flex-col min-w-0 bg-base",
            !mobileShowThread ? "hidden md:flex" : "flex",
          )}
        >
          {/* Mobile back button */}
          {mobileShowThread && state.activeConversationId && (
            <button
              onClick={handleBack}
              className="md:hidden flex items-center gap-1 px-3 py-2 text-sm text-accent border-b border-border"
            >
              <svg
                width="16"
                height="16"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              >
                <polyline points="15 18 9 12 15 6" />
              </svg>
              Back
            </button>
          )}
          <MessageThread />
          <MessageInput />
        </div>
      </div>

      {/* Notification drawer — slides over from right */}
      <StatusStrip />

      <NotificationDrawer
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
      />

      {/* Toast popups — bottom right */}
      <NotificationToasts />
    </div>
  );
}
