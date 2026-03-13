import type {
  ConversationParticipantRecord,
  ConversationSnapshot,
  MessageRecord,
  SynthesizedMessageRecord,
} from "./types";

const MAP_LIKE_TIMESTAMP_RE = /^\d{8}T\d{6}$/;
const MOJIBAKE_RE = /[\u00C3\u00C2\u00E2\u00F0\u00EF]/;
const PLACEHOLDER_PARTICIPANT_RE = /^(?:\(unnamed\)|unnamed|unknown|null|\(null\))$/i;
const ZERO_WIDTH_RE = /[\u200b-\u200d\u2060\ufeff\u034f]/g;
const THIN_SPACE_RE = /[\u2000-\u200a\u202f\u205f\u3000]/g;
const KNOWN_REACTION_PREFIX_RE = /^(?:laughed at|loved|liked|disliked|questioned|emphasized|reacted(?:\s+.+?)?)$/i;
const EMOJI_ONLY_RE = /^[\p{Extended_Pictographic}\p{Emoji_Presentation}\uFE0F\u200D\s]+$/u;

export interface MessageDisplayContent {
  kind: "text" | "reaction" | "placeholder";
  primaryText: string;
  secondaryText?: string;
}

export type NormalizedSessionPhase =
  | "Disconnected"
  | "Connecting"
  | "Connected"
  | "Faulted"
  | "Unknown";

export function normalizeSessionPhase(
  phase: string | number | null | undefined,
): NormalizedSessionPhase {
  if (phase === 0 || phase === "0") return "Disconnected";
  if (phase === 1 || phase === "1") return "Connecting";
  if (phase === 2 || phase === "2") return "Connected";
  if (phase === 3 || phase === "3") return "Faulted";

  const normalized = typeof phase === "string" ? phase.trim().toLowerCase() : "";
  switch (normalized) {
    case "disconnected":
      return "Disconnected";
    case "connecting":
      return "Connecting";
    case "connected":
      return "Connected";
    case "faulted":
      return "Faulted";
    default:
      return "Unknown";
  }
}

/**
 * Generate a deterministic HSL color from a string (contact name).
 * Returns a warm-toned hue with consistent saturation/lightness.
 */
export function hashColor(name: string): string {
  let hash = 0;
  for (let i = 0; i < name.length; i++) {
    hash = name.charCodeAt(i) + ((hash << 5) - hash);
  }
  const hue = Math.abs(hash) % 360;
  return `hsl(${hue}, 45%, 55%)`;
}

/**
 * Extract initials from a display name (up to 2 characters).
 */
export function initials(name: string): string {
  const stripped = name.replace(/[\p{Extended_Pictographic}\p{Emoji_Presentation}\uFE0F\u200D]/gu, "").trim();
  const parts = (stripped || name.trim()).split(/\s+/);
  if (parts.length === 0) return "?";
  if (parts.length === 1) return (parts[0]?.[0] ?? "?").toUpperCase();
  return (
    (parts[0]?.[0] ?? "") + (parts[parts.length - 1]?.[0] ?? "")
  ).toUpperCase();
}

function mojibakeScore(value: string): number {
  const suspiciousCount = value.match(/[\u00C3\u00C2\u00E2\u00F0\u00EF]/g)?.length ?? 0;
  const replacementCount = value.match(/\uFFFD/g)?.length ?? 0;
  return suspiciousCount + replacementCount * 2;
}

function repairMojibake(value: string): string {
  if (!MOJIBAKE_RE.test(value)) return value;

  const chars = Array.from(value);
  if (chars.some((char) => char.charCodeAt(0) > 255)) {
    return value;
  }

  const bytes = Uint8Array.from(chars, (char) => char.charCodeAt(0));
  const decoded = new TextDecoder().decode(bytes);
  if (!decoded || decoded === value) return value;

  return mojibakeScore(decoded) < mojibakeScore(value) ? decoded : value;
}

export function cleanDisplayText(
  value: string | null | undefined,
  options: { multiline?: boolean } = {},
): string {
  if (!value) return "";

  const normalized = repairMojibake(value)
    .replace(/\u00a0/g, " ")
    .replace(THIN_SPACE_RE, " ")
    .replace(ZERO_WIDTH_RE, "")
    .replace(/\r\n?/g, "\n");

  if (options.multiline) {
    return normalized.trim();
  }

  return normalized.replace(/\s+/g, " ").trim();
}

function cleanParticipantName(value: string | null | undefined): string {
  const cleaned = cleanDisplayText(value);
  if (!cleaned || PLACEHOLDER_PARTICIPANT_RE.test(cleaned)) {
    return "";
  }
  return cleaned;
}

function normalizeQuotedText(value: string): string {
  return cleanDisplayText(value, { multiline: true })
    .replace(/[“”]/g, "\"")
    .replace(/[‘’]/g, "'");
}

function capitalizeLabel(value: string): string {
  const cleaned = cleanDisplayText(value);
  if (!cleaned) return "Reacted";
  const lowered = cleaned.toLowerCase();
  return lowered.charAt(0).toUpperCase() + lowered.slice(1);
}

interface ParsedReactionText {
  summary: string;
  quotedText: string;
}

function parseReactionText(value: string | null | undefined): ParsedReactionText | null {
  const normalized = normalizeQuotedText(value ?? "");
  if (!normalized) {
    return null;
  }

  const prefixToQuote = normalized.match(/^(?<prefix>[\s\S]+?)\s+to\s+"(?<quote>[\s\S]+)"$/u);
  if (prefixToQuote?.groups) {
    const prefix = cleanDisplayText(prefixToQuote.groups.prefix);
    const quote = cleanDisplayText(prefixToQuote.groups.quote, { multiline: true });
    const prefixLooksValid = KNOWN_REACTION_PREFIX_RE.test(prefix) || EMOJI_ONLY_RE.test(prefix);
    if (prefixLooksValid && quote) {
      return {
        summary: EMOJI_ONLY_RE.test(prefix)
          ? `Reacted ${prefix.replace(/\s+/g, " ").trim()}`
          : capitalizeLabel(prefix),
        quotedText: quote,
      };
    }
  }

  const directVerb = normalized.match(/^(?<prefix>Laughed at|Loved|Liked|Disliked|Questioned|Emphasized|Reacted(?:\s+[\s\S]+?)?)\s+"(?<quote>[\s\S]+)"$/iu);
  if (directVerb?.groups) {
    const prefix = cleanDisplayText(directVerb.groups.prefix);
    const quote = cleanDisplayText(directVerb.groups.quote, { multiline: true });
    if (prefix && quote) {
      return {
        summary: capitalizeLabel(prefix),
        quotedText: quote,
      };
    }
  }

  return null;
}

export function describeMessageContent(
  message: Pick<MessageRecord, "body" | "subject" | "attachmentSize" | "size">,
): MessageDisplayContent {
  const text = cleanDisplayText(message.body || message.subject || "", { multiline: true });
  if (text) {
    const reaction = parseReactionText(text);
    if (reaction) {
      return {
        kind: "reaction",
        primaryText: reaction.summary,
        secondaryText: reaction.quotedText,
      };
    }

    return {
      kind: "text",
      primaryText: text,
    };
  }

  if ((message.attachmentSize ?? 0) > 0) {
    return {
      kind: "placeholder",
      primaryText: "Attachment",
    };
  }

  if ((message.size ?? 0) === 0) {
    return {
      kind: "placeholder",
      primaryText: "No text content",
    };
  }

  return {
    kind: "placeholder",
    primaryText: "Message unavailable",
  };
}

function summarizePreviewText(value: string | null | undefined): string {
  const preview = cleanDisplayText(value);
  if (!preview) {
    return "";
  }

  const reaction = parseReactionText(preview);
  if (reaction) {
    return `${reaction.summary} "${reaction.quotedText}"`;
  }

  return preview;
}

/**
 * Parse MAP/ANCS date format (yyyyMMddTHHmmss) into a JS Date.
 * These values are treated as local time because the source format has no zone.
 */
export function parseAncsDate(dateStr: string | null | undefined): Date | null {
  if (!dateStr || !MAP_LIKE_TIMESTAMP_RE.test(dateStr)) return null;
  const year = parseInt(dateStr.slice(0, 4));
  const month = parseInt(dateStr.slice(4, 6)) - 1;
  const day = parseInt(dateStr.slice(6, 8));
  const hour = parseInt(dateStr.slice(9, 11));
  const min = parseInt(dateStr.slice(11, 13));
  const sec = parseInt(dateStr.slice(13, 15));
  if (Number.isNaN(year) || Number.isNaN(month) || Number.isNaN(day)) return null;
  const parsed = new Date(year, month, day, hour, min, sec);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

export function parseTimestamp(value: string | null | undefined): Date | null {
  const trimmed = cleanDisplayText(value);
  if (!trimmed) return null;

  const mapLike = parseAncsDate(trimmed);
  if (mapLike) return mapLike;

  const parsed = new Date(trimmed);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

export function timestampValue(value: string | null | undefined): number | null {
  return parseTimestamp(value)?.getTime() ?? null;
}

export function messageTimestampSource(message: SynthesizedMessageRecord): string | null {
  return message.sortTimestampUtc ?? message.message.datetime ?? null;
}

export function messageTimestampValue(message: SynthesizedMessageRecord): number | null {
  return timestampValue(messageTimestampSource(message));
}

function compareNullableAsc(left: number | null, right: number | null): number {
  if (left === null && right === null) return 0;
  if (left === null) return 1;
  if (right === null) return -1;
  return left - right;
}

function compareNullableDesc(left: number | null, right: number | null): number {
  if (left === null && right === null) return 0;
  if (left === null) return 1;
  if (right === null) return -1;
  return right - left;
}

export function sortMessagesChronologically(
  messages: SynthesizedMessageRecord[],
): SynthesizedMessageRecord[] {
  return messages
    .map((message, index) => ({ message, index }))
    .sort((left, right) => {
      const byTimestamp = compareNullableAsc(
        messageTimestampValue(left.message),
        messageTimestampValue(right.message),
      );
      if (byTimestamp !== 0) return byTimestamp;
      const bySource = messageSortSourceRank(left.message) - messageSortSourceRank(right.message);
      if (bySource !== 0) return bySource;
      const byMapTimestamp = compareNullableAsc(
        timestampValue(left.message.message.datetime),
        timestampValue(right.message.message.datetime),
      );
      if (byMapTimestamp !== 0) return byMapTimestamp;
      const byKey = left.message.messageKey.localeCompare(right.message.messageKey);
      if (byKey !== 0) return byKey;
      return left.index - right.index;
    })
    .map(({ message }) => message);
}

export function sortConversationsByLatest(
  conversations: ConversationSnapshot[],
): ConversationSnapshot[] {
  return conversations
    .map((conversation, index) => ({ conversation, index }))
    .sort((left, right) => {
      const byTimestamp = compareNullableDesc(
        timestampValue(left.conversation.lastMessageUtc),
        timestampValue(right.conversation.lastMessageUtc),
      );
      if (byTimestamp !== 0) return byTimestamp;
      return left.index - right.index;
    })
    .map(({ conversation }) => conversation);
}

function normalizeComparable(value: string | null | undefined): string {
  const cleaned = cleanDisplayText(value);
  if (!cleaned) return "";
  return cleaned
    .toLowerCase()
    .replace(/^e:/, "")
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePhoneToken(value: string | null | undefined): string {
  if (!value) return "";
  return value.replace(/^e:/i, "").replace(/\D+/g, "");
}

/**
 * Format a timestamp as a relative time string.
 */
export function relativeTime(iso: string | null | undefined): string {
  const date = parseTimestamp(iso);
  if (!date) return "";

  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffSec = Math.floor(diffMs / 1000);

  if (diffSec < 60) return "now";
  const diffMin = Math.floor(diffSec / 60);
  if (diffMin < 60) return `${diffMin}m`;
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return `${diffHr}h`;
  const diffDay = Math.floor(diffHr / 24);
  if (diffDay === 1) return "Yesterday";
  if (diffDay < 7) return `${diffDay}d`;

  return date.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

/**
 * Format a date for time separators in message threads.
 */
export function dateSeparatorLabel(iso: string): string {
  const date = parseTimestamp(iso);
  if (!date) return "";

  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const msgDay = new Date(date.getFullYear(), date.getMonth(), date.getDate());
  const diffDays = Math.floor(
    (today.getTime() - msgDay.getTime()) / (1000 * 60 * 60 * 24),
  );

  if (diffDays === 0) return "Today";
  if (diffDays === 1) return "Yesterday";
  return date.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: date.getFullYear() !== now.getFullYear() ? "numeric" : undefined,
  });
}

/**
 * Format a timestamp for display on individual messages.
 */
export function messageTime(iso: string | null | undefined): string {
  const date = parseTimestamp(iso);
  if (!date) return "";

  return date.toLocaleTimeString("en-US", {
    hour: "numeric",
    minute: "2-digit",
  });
}

/**
 * Infer whether a synthesized message should render as outbound.
 * MAP often leaves `sent` null even when the folder is clearly `sent`.
 */
export function isSentMessage(message: {
  sent?: boolean | null;
  folder?: string | null;
}): boolean {
  if (message.sent === true) return true;
  const folder = message.folder?.toLowerCase();
  return folder === "sent" || folder === "outbox";
}

/**
 * Format "X seconds ago" for the status strip.
 */
export function timeAgo(iso: string | null | undefined): string {
  const date = parseTimestamp(iso);
  if (!date) return "never";

  const diffSec = Math.floor((Date.now() - date.getTime()) / 1000);
  if (diffSec < 5) return "just now";
  if (diffSec < 60) return `${diffSec}s ago`;
  const diffMin = Math.floor(diffSec / 60);
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHr = Math.floor(diffMin / 60);
  return `${diffHr}h ago`;
}

/**
 * Determine the primary phone number for a conversation participant.
 */
export function primaryPhone(
  phones: string[],
): string | undefined {
  return phones[0];
}

export function participantDisplayLabel(
  participant: ConversationParticipantRecord,
): string {
  const name = cleanParticipantName(participant.displayName);
  if (name) return name;

  return cleanDisplayText(primaryPhone(participant.phones) ?? participant.emails[0]);
}

export function participantSummary(
  participants: ConversationParticipantRecord[],
  maxNames = 3,
): string {
  const names = participants
    .filter((participant) => !participant.isSelf)
    .map((participant) => participantDisplayLabel(participant))
    .filter(Boolean)
    .filter((name, index, all) => all.findIndex((candidate) => normalizeComparable(candidate) === normalizeComparable(name)) === index);

  if (names.length === 0) return "";
  if (names.length <= maxNames) return names.join(", ");
  return `${names.slice(0, maxNames).join(", ")} +${names.length - maxNames} more`;
}

export function participantAddressSummary(
  participants: ConversationParticipantRecord[],
  maxItems = 3,
): string {
  const addresses = participants
    .filter((participant) => !participant.isSelf)
    .map((participant) => cleanDisplayText(primaryPhone(participant.phones) ?? participant.emails[0]))
    .filter(Boolean)
    .filter((address, index, all) => all.findIndex((candidate) => normalizeComparable(candidate) === normalizeComparable(address)) === index);

  if (addresses.length === 0) return "";
  if (addresses.length <= maxItems) return addresses.join(" | ");
  return `${addresses.slice(0, maxItems).join(" | ")} +${addresses.length - maxItems} more`;
}

export function conversationParticipantCount(
  conversation: ConversationSnapshot,
): number {
  return conversation.participants.filter((participant) => !participant.isSelf).length;
}

function looksLikeSyntheticGroupTitle(title: string): boolean {
  const normalized = normalizeComparable(title);
  if (!normalized) return false;
  return normalized.startsWith("to you ")
    || normalized.startsWith("you ")
    || normalized.startsWith("to mom ")
    || normalized.startsWith("to dad ");
}

export function conversationTitle(
  conversation: ConversationSnapshot | null | undefined,
  maxNames = 4,
): string {
  const rawTitle = cleanDisplayText(conversation?.displayName);
  if (!conversation) return rawTitle || "Unknown conversation";

  if (!conversation.isGroup) {
    return rawTitle || "Unknown conversation";
  }

  const participantTitle = participantSummary(conversation.participants, maxNames);
  if (participantTitle && (!rawTitle || looksLikeSyntheticGroupTitle(rawTitle))) {
    return participantTitle;
  }

  return rawTitle || participantTitle || "Group conversation";
}

export function resolveMessageSenderLabel(
  synthesizedMessage: SynthesizedMessageRecord,
): string | undefined {
  const senderCandidates = [
    synthesizedMessage.message.senderName,
    synthesizedMessage.message.senderAddressing,
    ...synthesizedMessage.message.originators.map((originator) => originator.name),
  ]
    .map((value) => cleanDisplayText(value))
    .filter(Boolean) as string[];

  const participants = synthesizedMessage.participants.filter((participant) => !participant.isSelf);
  for (const candidate of senderCandidates) {
    const candidatePhone = normalizePhoneToken(candidate);
    if (candidatePhone) {
      const phoneMatch = participants.find((participant) =>
        participant.phones.some((phone) => normalizePhoneToken(phone) === candidatePhone),
      );
      if (phoneMatch) {
        return participantDisplayLabel(phoneMatch);
      }
    }

    const candidateName = normalizeComparable(candidate);
    if (candidateName) {
      const nameMatch = participants.find((participant) => {
        const normalizedDisplayName = normalizeComparable(participantDisplayLabel(participant));
        return normalizedDisplayName === candidateName
          || normalizedDisplayName.includes(candidateName)
          || candidateName.includes(normalizedDisplayName);
      });
      if (nameMatch) {
        return participantDisplayLabel(nameMatch);
      }
    }
  }

  const fallbackOriginator = synthesizedMessage.message.originators
    .map((originator) => cleanParticipantName(originator.name))
    .find(Boolean);
  if (fallbackOriginator) return fallbackOriginator;

  const fallbackSender = cleanParticipantName(synthesizedMessage.message.senderName);
  if (fallbackSender) return fallbackSender;

  return participants[0] ? participantDisplayLabel(participants[0]) : undefined;
}

function previewAlreadyIncludesSender(preview: string, sender: string): boolean {
  const normalizedPreview = normalizeComparable(preview);
  const normalizedSender = normalizeComparable(sender);
  if (!normalizedPreview || !normalizedSender) return false;

  return normalizedPreview === normalizedSender
    || normalizedPreview.startsWith(`${normalizedSender} `)
    || normalizedPreview.startsWith(normalizedSender);
}

export function conversationPreviewText(
  conversation: ConversationSnapshot,
): string {
  const preview = summarizePreviewText(conversation.lastPreview);
  const lastSender = cleanParticipantName(conversation.lastSenderDisplayName);

  if (!preview) {
    return conversation.isGroup && lastSender
      ? `Latest from ${lastSender}`
      : "No text content";
  }

  if (!conversation.isGroup || !lastSender || previewAlreadyIncludesSender(preview, lastSender)) {
    return preview;
  }

  return `${lastSender}: ${preview}`;
}

function messageSortSourceRank(message: SynthesizedMessageRecord): number {
  const folder = message.message.folder?.toLowerCase();
  const type = message.message.type?.toUpperCase();
  const messageType = message.message.messageType?.toUpperCase();
  if (folder === "notification" || type === "ANCS" || messageType === "ANCS") {
    return 1;
  }

  if (folder === "outbox") {
    return 2;
  }

  return 0;
}

/**
 * Best-effort relative time for a notification.
 * Prefers the ANCS original date over the daemon's receivedAtUtc.
 */
export function notificationTime(
  ancsDate: string | null | undefined,
  receivedAtUtc: string | null | undefined,
): string {
  const parsed = parseAncsDate(ancsDate);
  if (parsed) return relativeTime(parsed.toISOString());
  return relativeTime(receivedAtUtc);
}

/**
 * Extract a human-readable app name from a reverse-DNS bundle identifier.
 * e.g. "com.disney.disneyplus" -> "Disney+" (known), "com.retrofitness.app" -> "Retrofitness"
 */
export function humanizeAppId(appIdentifier: string | null | undefined): string {
  if (!appIdentifier) return "Unknown";

  const parts = appIdentifier.split(".");
  if (parts.length <= 1) return appIdentifier;

  const skip = new Set([
    "com", "org", "net", "io", "co", "app", "dev", "me", "tv",
    "uk", "us", "au", "in", "jp", "de", "fr", "ca", "br",
    "ios", "mobile", "push", "client", "release", "beta", "alpha",
  ]);

  const meaningful = parts.filter(
    (segment) => !skip.has(segment.toLowerCase()) && segment.length > 1,
  );

  if (meaningful.length === 0) return parts[parts.length - 1] ?? "Unknown";

  const seen = new Set<string>();
  const unique = meaningful.filter((segment) => {
    const lowered = segment.toLowerCase();
    if (seen.has(lowered)) return false;
    seen.add(lowered);
    return true;
  });

  const best = unique.reduce((left, right) => (left.length >= right.length ? left : right));
  return best.charAt(0).toUpperCase() + best.slice(1);
}

/**
 * Classnames helper - joins truthy class strings.
 */
export function cn(...classes: (string | false | null | undefined)[]): string {
  return classes.filter(Boolean).join(" ");
}
