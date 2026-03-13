import type { ReactNode } from "react";
import { cn } from "../lib/utils";

interface SetupStepProps {
  step: number;
  totalSteps: number;
  title: string;
  description: string;
  children: ReactNode;
  status?: "active" | "complete" | "waiting";
}

export function SetupStep({
  step,
  totalSteps,
  title,
  description,
  children,
  status = "active",
}: SetupStepProps) {
  return (
    <div className="flex flex-col items-center justify-center min-h-screen p-8 bg-base">
      {/* Progress dots */}
      <div className="flex gap-2 mb-8">
        {Array.from({ length: totalSteps }).map((_, i) => (
          <div
            key={i}
            className={cn(
              "w-2 h-2 rounded-full transition-colors",
              i + 1 < step
                ? "bg-success"
                : i + 1 === step
                  ? "bg-accent"
                  : "bg-border",
            )}
          />
        ))}
      </div>

      {/* Content card */}
      <div className="max-w-md w-full bg-surface rounded-2xl p-8 border border-border shadow-sm">
        {status === "complete" && (
          <div className="flex justify-center mb-4">
            <div className="w-12 h-12 rounded-full bg-success/10 flex items-center justify-center">
              <svg
                width="24"
                height="24"
                viewBox="0 0 24 24"
                fill="none"
                stroke="var(--color-success)"
                strokeWidth="2.5"
                strokeLinecap="round"
                strokeLinejoin="round"
              >
                <polyline points="20 6 9 17 4 12" />
              </svg>
            </div>
          </div>
        )}

        <h1
          className="text-2xl font-semibold text-text-primary text-center mb-2"
          style={{ fontFamily: "var(--font-display)" }}
        >
          {title}
        </h1>

        <p className="text-sm text-text-secondary text-center mb-6 leading-relaxed">
          {description}
        </p>

        {children}
      </div>

      {/* Step indicator */}
      <p
        className="mt-6 text-[11px] text-text-secondary"
        style={{ fontFamily: "var(--font-mono)" }}
      >
        Step {step} of {totalSteps}
      </p>
    </div>
  );
}
