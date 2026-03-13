import { useState, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "../lib/api";
import { useApp } from "../context/AppContext";
import { SetupStep } from "../components/SetupStep";
import type {
  CapabilitiesResponse,
  DaemonRuntimeSnapshot,
  SupportedSetupSnapshot,
} from "../lib/types";

type Step = "connect" | "syncing" | "done";

export function SetupWizard() {
  const { state, dispatch } = useApp();
  const navigate = useNavigate();
  const [step, setStep] = useState<Step>(() =>
    deriveStepFromSnapshot(state.setup, state.runtime),
  );
  const [contactCount, setContactCount] = useState(state.runtime?.contactCount ?? 0);
  const [messageCount, setMessageCount] = useState(state.runtime?.messageCount ?? 0);
  const [conversationCount, setConversationCount] = useState(
    state.runtime?.conversationCount ?? 0,
  );
  const [deviceName, setDeviceName] = useState<string | null>(
    state.runtime?.target?.name ?? null,
  );

  const applyCapabilitiesSnapshot = useCallback(
    (cap: CapabilitiesResponse, fallbackDeviceName?: string | null) => {
      dispatch({ type: "SET_RUNTIME", runtime: cap.runtime, setup: cap.setup });

      setContactCount(cap.runtime.contactCount);
      setMessageCount(cap.runtime.messageCount);
      setConversationCount(cap.runtime.conversationCount);
      setDeviceName(cap.runtime.target?.name ?? fallbackDeviceName ?? null);

      const setupState = cap.setup.state;
      const msgState = cap.capabilities.messaging.state;

      if (
        setupState === "complete" ||
        setupState === "core_ready" ||
        msgState === "ready" ||
        msgState === "cached"
      ) {
        setStep("done");
        return;
      }

      if (setupState !== "needs_bootstrap" || cap.runtime.target) {
        setStep("syncing");
      }
    },
    [dispatch],
  );

  useEffect(() => {
    if (!state.setup || !state.runtime) return;

    setContactCount(state.runtime.contactCount);
    setMessageCount(state.runtime.messageCount);
    setConversationCount(state.runtime.conversationCount);
    setDeviceName(state.runtime.target?.name ?? null);

    if (state.setup.state === "complete" || state.setup.state === "core_ready") {
      setStep("done");
      return;
    }

    if (state.setup.state !== "needs_bootstrap" && state.runtime.target) {
      setStep("syncing");
    }
  }, [state.setup, state.runtime]);

  // Poll for device in step 1
  useEffect(() => {
    if (step !== "connect") return;

    const id = setInterval(async () => {
      try {
        const devices = await api.getDevices();
        const iphone = devices.endpoints.find(
          (e) => e.name.toLowerCase().includes("iphone") && e.isPaired,
        );
        if (iphone) {
          const cap = await api.getCapabilities();
          applyCapabilitiesSnapshot(cap, iphone.name);
        }
      } catch {
        // daemon not ready yet
      }
    }, 3000);

    // Also check if we already have a target from the runtime
    if (state.runtime?.target && state.setup?.state !== "needs_bootstrap") {
      setDeviceName(state.runtime.target.name);
      setStep("syncing");
    }

    return () => clearInterval(id);
  }, [applyCapabilitiesSnapshot, state.runtime?.target, state.setup?.state, step]);

  // Poll for sync readiness in step 2
  useEffect(() => {
    if (step !== "syncing") return;

    const id = setInterval(async () => {
      try {
        const cap = await api.getCapabilities();
        applyCapabilitiesSnapshot(cap, deviceName);
      } catch {
        // retry
      }
    }, 3000);

    // Trigger a sync to speed things up
    api.triggerSync("setup_wizard").catch(() => {});

    return () => clearInterval(id);
  }, [applyCapabilitiesSnapshot, deviceName, step]);

  const handleOpenPhoneLink = useCallback(() => {
    // Try the protocol URI first; falls back to nothing on failure
    window.open("ms-phone:", "_blank");
  }, []);

  const handleFinish = useCallback(() => {
    navigate("/");
  }, [navigate]);

  const stepNumber = step === "connect" ? 1 : step === "syncing" ? 2 : 3;

  if (step === "connect") {
    return (
      <SetupStep
        step={stepNumber}
        totalSteps={3}
        title="Connect Your iPhone"
        description="Open Phone Link on this PC and pair your iPhone. Phone Link is pre-installed on Windows 11 - search for it in the Start menu."
      >
        <div className="flex flex-col gap-3">
          <button
            onClick={handleOpenPhoneLink}
            className="w-full py-2.5 rounded-lg bg-accent text-white text-sm font-medium hover:bg-accent-hover transition-colors"
          >
            Open Phone Link
          </button>
          <div className="flex items-center gap-2 justify-center">
            <div className="w-1.5 h-1.5 rounded-full bg-syncing animate-pulse" />
            <span className="text-xs text-text-secondary">
              Waiting for your iPhone to appear...
            </span>
          </div>
        </div>
      </SetupStep>
    );
  }

  if (step === "syncing") {
    return (
      <SetupStep
        step={stepNumber}
        totalSteps={3}
        title="Getting Your Messages"
        description="Pulling contacts and messages from your iPhone. This usually takes less than a minute."
      >
        <div className="space-y-3">
          <ProgressRow label="Contacts" count={contactCount} />
          <ProgressRow label="Messages" count={messageCount} />
          <ProgressRow label="Conversations" count={conversationCount} />
        </div>
        <div className="flex items-center gap-2 justify-center mt-4">
          <div className="w-1.5 h-1.5 rounded-full bg-syncing animate-pulse" />
          <span className="text-xs text-text-secondary">Syncing...</span>
        </div>
      </SetupStep>
    );
  }

  // step === "done"
  return (
    <SetupStep
      step={stepNumber}
      totalSteps={3}
      title="You're All Set"
      description={`Found ${conversationCount} conversations and ${contactCount} contacts on ${deviceName ?? "your iPhone"}.`}
      status="complete"
    >
      <button
        onClick={handleFinish}
        className="w-full py-2.5 rounded-lg bg-accent text-white text-sm font-medium hover:bg-accent-hover transition-colors"
      >
        Open Messages
      </button>
    </SetupStep>
  );
}

function ProgressRow({ label, count }: { label: string; count: number }) {
  return (
    <div className="flex items-center justify-between bg-base rounded-lg px-3 py-2">
      <span className="text-sm text-text-primary">{label}</span>
      <span
        className="text-sm text-text-secondary tabular-nums"
        style={{ fontFamily: "var(--font-mono)" }}
      >
        {count}
      </span>
    </div>
  );
}

function deriveStepFromSnapshot(
  setup: SupportedSetupSnapshot | null,
  runtime: DaemonRuntimeSnapshot | null,
): Step {
  if (setup?.state === "complete" || setup?.state === "core_ready") {
    return "done";
  }

  if ((setup && setup.state !== "needs_bootstrap") || runtime?.target) {
    return "syncing";
  }

  return "connect";
}
