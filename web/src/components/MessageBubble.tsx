import type { SynthesizedMessageRecord } from "../lib/types";
import {
  cleanDisplayText,
  describeMessageContent,
  messageTime,
  messageTimestampSource,
  isSentMessage,
  resolveMessageSenderLabel,
  cn,
} from "../lib/utils";

interface MessageBubbleProps {
  message: SynthesizedMessageRecord;
  reactions?: SynthesizedMessageRecord[];
  showTime?: boolean;
  animate?: boolean;
}

function reactionBadge(summary: string): string {
  const normalized = cleanDisplayText(summary).toLowerCase();
  if (!normalized) return "*";
  if (normalized.startsWith("reacted ")) {
    return cleanDisplayText(summary.slice("Reacted ".length)) || "*";
  }
  if (normalized.startsWith("loved")) return "<3";
  if (normalized.startsWith("liked")) return "+1";
  if (normalized.startsWith("disliked")) return "-1";
  if (normalized.startsWith("laughed")) return "Ha";
  if (normalized.startsWith("questioned")) return "?";
  if (normalized.startsWith("emphasized")) return "!!";
  return cleanDisplayText(summary).slice(0, 2);
}

export function MessageBubble({ message, reactions = [], showTime, animate }: MessageBubbleProps) {
  const isSent = isSentMessage(message.message);
  const content = describeMessageContent(message.message);
  const senderLabel = message.isGroup && !isSent
    ? resolveMessageSenderLabel(message)
    : undefined;
  const reactionItems = reactions.map((reaction) => {
    const reactionContent = describeMessageContent(reaction.message);
    const reactionSender = resolveMessageSenderLabel(reaction);
    return {
      key: reaction.messageKey,
      badge: reactionBadge(reactionContent.primaryText),
      summary: reactionSender
        ? `${reactionSender}: ${cleanDisplayText(reactionContent.primaryText)}`
        : cleanDisplayText(reactionContent.primaryText),
      quote: reactionContent.secondaryText
        ? cleanDisplayText(reactionContent.secondaryText, { multiline: true })
        : "",
    };
  });

  return (
    <div
      className={cn(
        "flex mb-1 px-4",
        isSent ? "justify-end" : "justify-start",
        animate && "animate-message-in",
      )}
    >
      <div className="max-w-[65%] flex flex-col gap-0.5">
        {senderLabel && (
          <span className="text-[11px] text-text-secondary px-1">
            {senderLabel}
          </span>
        )}
        <div
          className={cn(
            "relative rounded-xl px-3.5 py-2 text-sm leading-relaxed break-words",
            isSent
              ? "bg-accent/12 text-accent"
              : "bg-received text-text-primary",
          )}
        >
          {content.kind === "reaction" ? (
            <div className="space-y-1">
              <div className="text-[11px] font-medium opacity-80">
                {cleanDisplayText(content.primaryText)}
              </div>
              {content.secondaryText && (
                <div className="border-l-2 border-current/20 pl-2 italic opacity-90">
                  "{cleanDisplayText(content.secondaryText, { multiline: true })}"
                </div>
              )}
            </div>
          ) : content.kind === "placeholder" ? (
            <span className="italic opacity-70">
              {cleanDisplayText(content.primaryText)}
            </span>
          ) : (
            cleanDisplayText(content.primaryText, { multiline: true })
          )}

          {reactionItems.length > 0 && (
            <div className="absolute -top-2 right-2 group/reactions">
              <div className="flex items-center justify-end gap-1">
                {reactionItems.slice(0, 4).map((reaction) => (
                  <span
                    key={reaction.key}
                    className="min-w-6 h-6 px-1.5 rounded-full border border-border bg-base/95 shadow-sm text-[11px] font-medium text-text-primary flex items-center justify-center backdrop-blur-sm"
                  >
                    {reaction.badge}
                  </span>
                ))}
                {reactionItems.length > 4 && (
                  <span className="min-w-6 h-6 px-1.5 rounded-full border border-border bg-base/95 shadow-sm text-[10px] font-medium text-text-secondary flex items-center justify-center backdrop-blur-sm">
                    +{reactionItems.length - 4}
                  </span>
                )}
              </div>
              <div className="pointer-events-none absolute right-0 top-7 z-20 hidden min-w-52 max-w-72 rounded-xl border border-border bg-base/98 px-3 py-2 text-[11px] text-text-primary shadow-xl group-hover/reactions:block">
                <div className="space-y-1.5">
                  {reactionItems.map((reaction) => (
                    <div key={reaction.key} className="leading-relaxed">
                      <div>{reaction.summary}</div>
                      {reaction.quote && (
                        <div className="mt-0.5 border-l-2 border-border pl-2 italic text-text-secondary">
                          "{reaction.quote}"
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}
        </div>
        {showTime && (
          <span
            className={cn(
              "text-[11px] text-text-secondary px-1",
              isSent ? "text-right" : "text-left",
            )}
            style={{ fontFamily: "var(--font-mono)" }}
          >
            {messageTime(messageTimestampSource(message))}
          </span>
        )}
      </div>
    </div>
  );
}
