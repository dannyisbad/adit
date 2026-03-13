import { Routes, Route, Navigate } from "react-router-dom";
import { useApp } from "./context/AppContext";
import { SetupWizard } from "./pages/SetupWizard";
import { Messages } from "./pages/Messages";
import { Settings } from "./pages/Settings";

export default function App() {
  const { state } = useApp();

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
