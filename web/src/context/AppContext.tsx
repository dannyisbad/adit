import {
  createContext,
  useContext,
  useReducer,
  useEffect,
  useCallback,
  useRef,
  type ReactNode,
  type Dispatch,
} from "react";
import type {
  DaemonRuntimeSnapshot,
  ConversationSnapshot,
  SynthesizedMessageRecord,
  SupportedSetupSnapshot,
} from "../lib/types";
import { api } from "../lib/api";
import { sortConversationsByLatest, sortMessagesChronologically } from "../lib/utils";
import { useEvent, useWsConnection } from "../lib/ws";

export interface AppState {
  runtime: DaemonRuntimeSnapshot | null;
  setup: SupportedSetupSnapshot | null;
  conversations: ConversationSnapshot[];
  activeConversationId: string | null;
  activeMessages: SynthesizedMessageRecord[];
  connectionStatus: "connecting" | "connected" | "disconnected";
  loading: boolean;
  error: string | null;
}

const initialState: AppState = {
  runtime: null,
  setup: null,
  conversations: [],
  activeConversationId: null,
  activeMessages: [],
  connectionStatus: "connecting",
  loading: true,
  error: null,
};

type Action =
  | { type: "SET_RUNTIME"; runtime: DaemonRuntimeSnapshot; setup: SupportedSetupSnapshot }
  | { type: "SET_CONVERSATIONS"; conversations: ConversationSnapshot[] }
  | { type: "SET_ACTIVE_CONVERSATION"; id: string | null }
  | { type: "SET_ACTIVE_MESSAGES"; messages: SynthesizedMessageRecord[] }
  | { type: "APPEND_OPTIMISTIC_MESSAGE"; message: SynthesizedMessageRecord }
  | { type: "REMOVE_ACTIVE_MESSAGE"; messageKey: string }
  | { type: "SET_CONNECTION_STATUS"; status: AppState["connectionStatus"] }
  | { type: "SET_LOADING"; loading: boolean }
  | { type: "SET_ERROR"; error: string | null };

function reducer(state: AppState, action: Action): AppState {
  switch (action.type) {
    case "SET_RUNTIME":
      return {
        ...state,
        runtime: action.runtime,
        setup: action.setup,
        loading: false,
        error: null,
      };
    case "SET_CONVERSATIONS":
      return { ...state, conversations: sortConversationsByLatest(action.conversations) };
    case "SET_ACTIVE_CONVERSATION":
      return { ...state, activeConversationId: action.id, activeMessages: [] };
    case "SET_ACTIVE_MESSAGES":
      return { ...state, activeMessages: sortMessagesChronologically(action.messages) };
    case "APPEND_OPTIMISTIC_MESSAGE":
      return {
        ...state,
        activeMessages: sortMessagesChronologically([...state.activeMessages, action.message]),
      };
    case "REMOVE_ACTIVE_MESSAGE":
      return {
        ...state,
        activeMessages: state.activeMessages.filter(
          (message) => message.messageKey !== action.messageKey,
        ),
      };
    case "SET_CONNECTION_STATUS":
      return { ...state, connectionStatus: action.status };
    case "SET_LOADING":
      return { ...state, loading: action.loading };
    case "SET_ERROR":
      return { ...state, error: action.error };
  }
}

interface AppContextValue {
  state: AppState;
  dispatch: Dispatch<Action>;
  selectConversation: (id: string) => void;
  refreshConversations: () => Promise<void>;
  refreshActiveConversation: () => Promise<void>;
}

const AppContext = createContext<AppContextValue | null>(null);

export function useApp() {
  const ctx = useContext(AppContext);
  if (!ctx) throw new Error("useApp must be inside AppProvider");
  return ctx;
}

export function AppProvider({ children }: { children: ReactNode }) {
  const [state, dispatch] = useReducer(reducer, initialState);
  const activeConversationRequestId = useRef(0);

  useWsConnection();

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const [capRes, convRes] = await Promise.all([
          api.getCapabilities(),
          api.listConversations(100).catch(() => null),
        ]);
        if (cancelled) return;
        dispatch({ type: "SET_RUNTIME", runtime: capRes.runtime, setup: capRes.setup });
        if (convRes) {
          dispatch({ type: "SET_CONVERSATIONS", conversations: convRes.conversations });
        }
      } catch (err) {
        if (!cancelled) {
          dispatch({ type: "SET_ERROR", error: (err as Error).message });
          dispatch({ type: "SET_LOADING", loading: false });
        }
      }
    }

    load();
    return () => {
      cancelled = true;
    };
  }, []);

  const refreshConversations = useCallback(async () => {
    try {
      const res = await api.listConversations(100);
      dispatch({ type: "SET_CONVERSATIONS", conversations: res.conversations });
    } catch {
      // silent
    }
  }, []);

  const refreshActiveConversation = useCallback(async () => {
    if (!state.activeConversationId) return;

    const requestId = ++activeConversationRequestId.current;

    try {
      const res = await api.getConversation(state.activeConversationId, 100);
      if (activeConversationRequestId.current !== requestId) {
        return;
      }
      dispatch({ type: "SET_ACTIVE_MESSAGES", messages: res.messages });
      dispatch({ type: "SET_ERROR", error: null });
    } catch (err) {
      if (activeConversationRequestId.current !== requestId) {
        return;
      }
      dispatch({ type: "SET_ERROR", error: (err as Error).message });
    }
  }, [state.activeConversationId]);

  useEvent("ws.connected", () => {
    dispatch({ type: "SET_CONNECTION_STATUS", status: "connected" });
  });

  useEvent("ws.disconnected", () => {
    dispatch({ type: "SET_CONNECTION_STATUS", status: "disconnected" });
  });

  useEvent("hello", (ev) => {
    const payload = ev.payload as { runtime?: DaemonRuntimeSnapshot } | undefined;
    if (payload?.runtime) {
      api
        .getCapabilities()
        .then((cap) => {
          dispatch({ type: "SET_RUNTIME", runtime: cap.runtime, setup: cap.setup });
        })
        .catch(() => {});
    }
  });

  useEvent("sync.completed", () => {
    refreshConversations();
    refreshActiveConversation();
    api
      .getCapabilities()
      .then((cap) => {
        dispatch({ type: "SET_RUNTIME", runtime: cap.runtime, setup: cap.setup });
      })
      .catch(() => {});
  });

  useEvent("runtime.updated", () => {
    api
      .getCapabilities()
      .then((cap) => {
        dispatch({ type: "SET_RUNTIME", runtime: cap.runtime, setup: cap.setup });
      })
      .catch(() => {});
  });

  useEvent("map.event", () => {
    refreshConversations();
    refreshActiveConversation();
  });

  useEvent("messages.updated", () => {
    refreshConversations();
    refreshActiveConversation();
  });

  const selectConversation = useCallback((id: string) => {
    dispatch({ type: "SET_ACTIVE_CONVERSATION", id });
    const requestId = ++activeConversationRequestId.current;
    api
      .getConversation(id, 100)
      .then((res) => {
        if (activeConversationRequestId.current !== requestId) {
          return;
        }
        dispatch({ type: "SET_ACTIVE_MESSAGES", messages: res.messages });
        dispatch({ type: "SET_ERROR", error: null });
      })
      .catch((err) => {
        if (activeConversationRequestId.current !== requestId) {
          return;
        }
        dispatch({ type: "SET_ERROR", error: (err as Error).message });
      });
  }, []);

  return (
    <AppContext.Provider
      value={{
        state,
        dispatch,
        selectConversation,
        refreshConversations,
        refreshActiveConversation,
      }}
    >
      {children}
    </AppContext.Provider>
  );
}
