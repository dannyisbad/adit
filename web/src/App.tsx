import { useState } from "react";
import { Routes, Route, Navigate } from "react-router-dom";
import { useApp } from "./context/AppContext";
import { SetupWizard } from "./pages/SetupWizard";
import { Messages } from "./pages/Messages";
import { Settings } from "./pages/Settings";
import { clearDaemonAuthToken, getDaemonAuthToken, hasSavedDaemonAuthToken, setDaemonAuthToken } from "./lib/auth";

export default function App() {
  const { state } = useApp();
  const [tokenInput, setTokenInput] = useState(() => getDaemonAuthToken() ?? "");
  const savedToken = hasSavedDaemonAuthToken();

  if (state.authRequired) {
    const handleSave = () => {
      if (!tokenInput.trim()) {
        return;
      }
      setDaemonAuthToken(tokenInput);
      window.location.reload();
    };

    const handleClear = () => {
      clearDaemonAuthToken();
      setTokenInput("");
      window.location.reload();
    };

    return (
      <div className="flex items-center justify-center h-screen bg-base px-6">
        <div className="w-full max-w-md rounded-2xl border border-border bg-surface p-8 shadow-sm">
          <h1
            className="text-2xl font-semibold text-text-primary mb-2"
            style={{ fontFamily: "var(--font-display)" }}
          >
            Daemon token required
          </h1>
          <p className="text-sm text-text-secondary mb-5">
            This daemon has bearer-token auth enabled. Enter the token to continue using the hosted UI on this computer.
          </p>
          <label className="block text-xs font-medium text-text-secondary uppercase tracking-wider mb-2">
            Bearer Token
          </label>
          <input
            value={tokenInput}
            onChange={(event) => setTokenInput(event.target.value)}
            placeholder="Paste ADIT_AUTH_TOKEN"
            className="w-full rounded-xl border border-border bg-base px-3 py-2.5 text-sm text-text-primary outline-none focus:border-accent"
            autoFocus
          />
          <p className="text-xs text-text-secondary mt-3">
            {state.error ?? "The daemon returned 401 until a valid token is provided."}
          </p>
          <div className="flex items-center gap-3 mt-6">
            <button
              onClick={handleSave}
              disabled={!tokenInput.trim()}
              className="flex-1 rounded-xl bg-accent px-4 py-2.5 text-sm text-white transition-colors hover:bg-accent-hover disabled:opacity-50"
            >
              Save Token
            </button>
            <button
              onClick={() => window.location.reload()}
              className="rounded-xl border border-border px-4 py-2.5 text-sm text-text-secondary transition-colors hover:text-text-primary"
            >
              Retry
            </button>
          </div>
          {savedToken ? (
            <button
              onClick={handleClear}
              className="mt-4 text-sm text-text-secondary underline-offset-2 hover:text-text-primary hover:underline"
            >
              Remove saved token
            </button>
          ) : null}
        </div>
      </div>
    );
  }

  // Show loading state while we fetch initial data
  if (state.loading) {
    return (
      <div className="flex items-center justify-center h-screen bg-base">
        <div className="flex flex-col items-center gap-4">
          <div className="w-8 h-8 rounded-full border-2 border-border border-t-accent animate-spin" />
          <p
            className="text-sm text-text-secondary"
            style={{ fontFamily: "var(--font-body)" }}
          >
            Connecting to daemon...
          </p>
        </div>
      </div>
    );
  }

  // If there's a connection error and no runtime, show error state
  if (state.error && !state.runtime) {
    return (
      <div className="flex items-center justify-center h-screen bg-base">
        <div className="max-w-sm text-center p-8">
          <h1
            className="text-xl font-semibold text-text-primary mb-2"
            style={{ fontFamily: "var(--font-display)" }}
          >
            Can't reach the daemon
          </h1>
          <p className="text-sm text-text-secondary mb-4">
            Make sure adit is running on this computer.
          </p>
          <button
            onClick={() => window.location.reload()}
            className="px-4 py-2 rounded-lg bg-accent text-white text-sm hover:bg-accent-hover transition-colors"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  // Setup is needed whenever the daemon says so — not gated by localStorage.
  const needsSetup = state.setup?.state === "needs_bootstrap";

  return (
    <Routes>
      <Route path="/setup" element={<SetupWizard />} />
      <Route path="/settings" element={<Settings />} />
      <Route
        path="/"
        element={needsSetup ? <Navigate to="/setup" replace /> : <Messages />}
      />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
