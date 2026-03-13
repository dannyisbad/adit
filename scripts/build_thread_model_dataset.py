#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import random
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from thread_model_banks import (
    DEFAULT_ENV_PATH,
    DEFAULT_MODEL,
    generate_style_packs,
    generate_thread_windows,
    load_style_packs,
    load_thread_windows,
)
from thread_model_synth import (
    build_synth_samples as build_synth_samples_from_windows,
    dump_validation_report,
    validate_dataset,
)


DEFAULT_ADIT_DB = Path(os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local"))) / "Adit" / "adit.db"


GENERIC_PREVIEWS = {
    "ok",
    "okay",
    "k",
    "kk",
    "yes",
    "no",
    "yup",
    "yep",
    "lol",
    "lmao",
    "teehee",
    "sounds good",
    "sounds good.",
    "sounds good!",
    "sure",
    "bet",
    "nice",
    "true",
    "facts",
    "thanks",
    "thank you",
}

SERVICE_LABEL_HINTS = (
    "cvs",
    "walgreens",
    "usps",
    "ups",
    "fedex",
    "delta",
    "uber",
    "lyft",
    "opentable",
    "doordash",
    "grubhub",
    "instacart",
    "bank",
    "capital one",
    "chase",
    "boa",
)

STYLE_PACKS = {
    "family_names": [
        "mom",
        "dad",
        "grandma",
        "Aunt Lisa",
        "Aunt Jean",
        "Aunt Beth",
        "Maren",
        "Tessa",
        "ellis",
        "Nina",
        "romy",
    ],
    "friend_names": [
        "rowan",
        "monke",
        "teddy",
        "alex",
        "sam",
        "josh",
        "maya",
        "chris",
    ],
    "generic_replies": [
        "ok",
        "teehee",
        "sounds good",
        "lol",
        "yup",
        "bet",
        "kk",
        "i'm down",
        "on my way",
        "true",
        "got it",
    ],
    "planning_lines": [
        "i'll see you guys soon",
        "we're 10 out",
        "parking now",
        "i'm outside",
        "tell me when you're free",
        "what time are we leaving",
        "i can bring food",
    ],
    "reaction_templates": [
        'Loved "{preview}"',
        'Laughed at "{preview}"',
        'Disliked "{preview}"',
        'Reacted ❤️ to "{preview}"',
    ],
}


def parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    value = value.replace("Z", "+00:00")
    if "." in value:
        head, tail = value.split(".", 1)
        tz_index = max(tail.find("+"), tail.find("-"))
        if tz_index == -1:
            frac = tail
            suffix = ""
        else:
            frac = tail[:tz_index]
            suffix = tail[tz_index:]
        if len(frac) > 6:
            tail = f"{frac[:6]}{suffix}"
            value = f"{head}.{tail}"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.lower().split())


def tokenize(value: str | None) -> list[str]:
    return [token for token in normalize_text(value).replace(",", " ").replace(".", " ").split(" ") if token]


def token_overlap(left: str | None, right: str | None) -> float:
    left_tokens = set(tokenize(left))
    right_tokens = set(tokenize(right))
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def looks_generic(preview: str | None) -> bool:
    normalized = normalize_text(preview)
    if not normalized:
        return True
    if normalized in GENERIC_PREVIEWS:
        return True
    tokens = tokenize(normalized)
    return len(tokens) <= 2 and len(normalized) <= 16


@dataclass
class MessageRow:
    device_id: str
    message_key: str
    conversation_id: str
    sort_utc: datetime | None
    conversation_display_name: str
    is_group: bool
    preview: str | None
    payload: dict[str, Any]


@dataclass
class ConversationRow:
    device_id: str
    conversation_id: str
    display_name: str
    is_group: bool
    last_message_utc: datetime | None
    last_preview: str | None
    participants: list[dict[str, Any]]
    payload: dict[str, Any]


@dataclass
class NotificationRow:
    device_id: str
    notification_uid: int
    app_identifier: str | None
    title: str | None
    subtitle: str | None
    message: str | None
    received_utc: datetime | None
    payload: dict[str, Any]


def load_cache(db_path: Path) -> tuple[list[MessageRow], list[ConversationRow], list[NotificationRow]]:
    connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    messages = []
    for row in cursor.execute(
        """
        SELECT device_id, message_key, conversation_id, sort_utc, conversation_display_name, is_group, preview, json
        FROM messages
        ORDER BY sort_ticks DESC
        """
    ):
        payload = json.loads(row["json"])
        messages.append(
            MessageRow(
                device_id=row["device_id"],
                message_key=row["message_key"],
                conversation_id=row["conversation_id"],
                sort_utc=parse_utc(row["sort_utc"]),
                conversation_display_name=row["conversation_display_name"] or "",
                is_group=bool(row["is_group"]),
                preview=row["preview"],
                payload=payload,
            )
        )

    conversations = []
    for row in cursor.execute(
        """
        SELECT device_id, conversation_id, display_name, is_group, last_message_utc, last_preview, participants_json, json
        FROM conversations
        ORDER BY last_message_ticks DESC
        """
    ):
        payload = json.loads(row["json"])
        participants_json = json.loads(row["participants_json"]) if row["participants_json"] else payload.get("participants", [])
        conversations.append(
            ConversationRow(
                device_id=row["device_id"],
                conversation_id=row["conversation_id"],
                display_name=row["display_name"] or "",
                is_group=bool(row["is_group"]),
                last_message_utc=parse_utc(row["last_message_utc"]),
                last_preview=row["last_preview"],
                participants=participants_json or [],
                payload=payload,
            )
        )

    notifications = []
    for row in cursor.execute(
        """
        SELECT device_id, notification_uid, app_identifier, title, subtitle, message, received_utc, json
        FROM notifications
        ORDER BY received_utc DESC
        """
    ):
        payload = json.loads(row["json"])
        notifications.append(
            NotificationRow(
                device_id=row["device_id"],
                notification_uid=row["notification_uid"],
                app_identifier=row["app_identifier"],
                title=row["title"],
                subtitle=row["subtitle"],
                message=row["message"],
                received_utc=parse_utc(row["received_utc"]),
                payload=payload,
            )
        )

    connection.close()
    return messages, conversations, notifications


def participant_keys(payload: dict[str, Any]) -> set[str]:
    return {
        participant.get("key", "")
        for participant in payload.get("participants", [])
        if participant.get("key") and not participant.get("isSelf")
    }


def preview_bucket(preview: str | None) -> str:
    normalized = normalize_text(preview)
    if not normalized:
        return "empty"
    if looks_reaction_preview(normalized):
        return "reaction"
    if looks_generic(normalized):
        return "generic"
    return "specific"


def looks_service_like_value(value: str | None) -> bool:
    normalized = normalize_text(value)
    if not normalized:
        return False
    digit_run = "".join(ch for ch in normalized if ch.isdigit())
    if digit_run and 4 <= len(digit_run) <= 6:
        return True
    return any(hint in normalized for hint in SERVICE_LABEL_HINTS)


def build_history(
    messages_by_conversation: dict[str, list[MessageRow]],
    conversation_id: str,
    reference_utc: datetime | None,
    limit: int,
    *,
    exclude_message_key: str | None = None,
) -> list[dict[str, Any]]:
    source = messages_by_conversation.get(conversation_id, [])
    filtered = []
    for message in source:
        if exclude_message_key and message.message_key == exclude_message_key:
            continue
        if reference_utc is not None and message.sort_utc is not None and message.sort_utc > reference_utc:
            continue
        filtered.append(message)
    tail = filtered[-limit:]
    history = []
    for message in tail:
        raw_message = message.payload.get("message", {})
        history.append(
            {
                "message_key": message.message_key,
                "sort_utc": message.payload.get("sortTimestampUtc"),
                "preview": message.preview,
                "sender_name": raw_message.get("senderName"),
                "sender_addressing": raw_message.get("senderAddressing"),
                "body": raw_message.get("body"),
                "folder": raw_message.get("folder"),
                "status": raw_message.get("status"),
            }
        )
    return history


def score_candidate(
    target: MessageRow,
    target_participants: set[str],
    candidate_is_group: bool,
    candidate_participants: list[dict[str, Any]],
    candidate_history: list[dict[str, Any]],
    reference_utc: datetime | None,
) -> dict[str, Any]:
    last_history = candidate_history[-1] if candidate_history else None
    preview_overlap = token_overlap(target.preview, last_history.get("preview") if last_history else None)
    candidate_participants = {
        participant.get("key", "")
        for participant in candidate_participants
        if participant.get("key") and not participant.get("isSelf")
    }
    participant_overlap = len(target_participants & candidate_participants)
    candidate_last_utc = parse_utc(last_history.get("sort_utc")) if last_history else None
    if reference_utc and candidate_last_utc:
        delta_seconds = abs((reference_utc - candidate_last_utc).total_seconds())
    else:
        delta_seconds = None

    score = 0.0
    if delta_seconds is not None:
        if delta_seconds <= 5 * 60:
            score += 4.0
        elif delta_seconds <= 30 * 60:
            score += 3.0
        elif delta_seconds <= 2 * 3600:
            score += 2.0
        elif delta_seconds <= 12 * 3600:
            score += 1.0
    if preview_overlap > 0:
        score += preview_overlap * 4.0
    if participant_overlap > 0:
        score += min(3.0, participant_overlap * 1.5)
    if candidate_is_group == target.is_group:
        score += 0.5
    if looks_generic(target.preview):
        score += 0.5

    return {
        "candidate_score": round(score, 4),
        "preview_overlap": round(preview_overlap, 4),
        "participant_overlap": participant_overlap,
        "delta_seconds": int(delta_seconds) if delta_seconds is not None else None,
        "candidate_is_group": candidate_is_group,
    }


def extract_matching_notifications(target: MessageRow, notifications: list[NotificationRow]) -> list[dict[str, Any]]:
    raw_message = target.payload.get("message", {})
    preview = target.preview or raw_message.get("body") or raw_message.get("subject")
    sender_name = raw_message.get("senderName") or target.conversation_display_name
    matches: list[tuple[float, NotificationRow]] = []
    for notification in notifications:
        if notification.app_identifier not in ("com.apple.MobileSMS", "com.apple.MobileSMS.notification"):
            continue
        overlap = token_overlap(preview, notification.message)
        name_match = token_overlap(sender_name, notification.title)
        if overlap == 0 and name_match == 0:
            continue
        distance = abs((target.sort_utc - notification.received_utc).total_seconds()) if target.sort_utc and notification.received_utc else 999999
        score = overlap * 4 + name_match * 2 - min(distance / 3600.0, 3.0)
        matches.append((score, notification))

    matches.sort(key=lambda item: item[0], reverse=True)
    result = []
    for _, notification in matches[:3]:
        result.append(
            {
                "notification_uid": notification.notification_uid,
                "received_utc": notification.payload.get("receivedAtUtc"),
                "title": notification.title,
                "subtitle": notification.subtitle,
                "message": notification.message,
                "app_identifier": notification.app_identifier,
            }
        )
    return result


def candidate_participant_key_set(participants: list[dict[str, Any]]) -> set[str]:
    keys: set[str] = set()
    for participant in participants:
        if participant.get("isSelf"):
            continue
        for raw in (
            participant.get("key"),
            participant.get("displayName"),
            *(participant.get("phones") or []),
            *(participant.get("emails") or []),
        ):
            normalized = normalize_text(raw)
            if normalized:
                keys.add(normalized)
        for raw in participant.get("phones") or []:
            digits = "".join(ch for ch in str(raw) if ch.isdigit())
            if digits:
                keys.add(digits)
    return keys


def target_sender_variants(target: MessageRow) -> set[str]:
    raw_message = target.payload.get("message", {})
    variants: set[str] = set()
    for raw in (
        raw_message.get("senderName"),
        raw_message.get("senderAddressing"),
        target.preview,
        target.conversation_display_name,
    ):
        normalized = normalize_text(raw)
        if normalized:
            variants.add(normalized)
        digits = "".join(ch for ch in str(raw or "") if ch.isdigit())
        if digits:
            variants.add(digits)
    return variants


def history_sender_variants(history_turn: dict[str, Any]) -> set[str]:
    variants: set[str] = set()
    for raw in (history_turn.get("sender_name"), history_turn.get("sender_addressing")):
        normalized = normalize_text(raw)
        if normalized:
            variants.add(normalized)
        digits = "".join(ch for ch in str(raw or "") if ch.isdigit())
        if digits:
            variants.add(digits)
    return variants


def compute_future_corroboration(
    messages_by_conversation: dict[str, list[MessageRow]],
    conversation_id: str,
    reference_utc: datetime | None,
    *,
    exclude_message_key: str | None = None,
) -> dict[str, Any]:
    if reference_utc is None:
        return {
            "future_turns_6h": 0,
            "future_turns_24h": 0,
            "future_nonempty_24h": 0,
            "future_first_gap_seconds": None,
            "future_corrob_strength": 0,
        }

    turns_6h = 0
    turns_24h = 0
    nonempty_24h = 0
    first_gap_seconds: int | None = None
    for message in messages_by_conversation.get(conversation_id, []):
        if exclude_message_key and message.message_key == exclude_message_key:
            continue
        if message.sort_utc is None or message.sort_utc <= reference_utc:
            continue
        delta_seconds = int((message.sort_utc - reference_utc).total_seconds())
        if first_gap_seconds is None:
            first_gap_seconds = delta_seconds
        if delta_seconds <= 6 * 3600:
            turns_6h += 1
        if delta_seconds <= 24 * 3600:
            turns_24h += 1
            raw_message = message.payload.get("message", {})
            preview = message.preview or raw_message.get("body") or raw_message.get("subject")
            if normalize_text(preview):
                nonempty_24h += 1

    strength = 0
    if turns_6h >= 1:
        strength += 1
    if turns_6h >= 2:
        strength += 1
    if nonempty_24h >= 1:
        strength += 1

    return {
        "future_turns_6h": turns_6h,
        "future_turns_24h": turns_24h,
        "future_nonempty_24h": nonempty_24h,
        "future_first_gap_seconds": first_gap_seconds,
        "future_corrob_strength": strength,
    }


def make_candidate_pool(
    target: MessageRow,
    conversations: list[ConversationRow],
    messages_by_conversation: dict[str, list[MessageRow]],
    *,
    history_limit: int,
) -> tuple[ConversationRow | None, list[dict[str, Any]], dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    reference_utc = target.sort_utc
    conversations_by_id = {conversation.conversation_id: conversation for conversation in conversations}
    gold_conversation = conversations_by_id.get(target.conversation_id)
    if not gold_conversation:
        return None, [], {}, [], {}

    target_participants = participant_keys(target.payload)
    target_preview = target.preview or target.payload.get("message", {}).get("body") or target.payload.get("message", {}).get("subject")

    gold_history = build_history(
        messages_by_conversation,
        gold_conversation.conversation_id,
        reference_utc,
        history_limit,
        exclude_message_key=target.message_key,
    )
    gold_features = score_candidate(
        target,
        target_participants,
        gold_conversation.is_group,
        gold_conversation.participants,
        gold_history,
        reference_utc,
    )
    gold_key_set = candidate_participant_key_set(gold_conversation.participants)
    gold_analysis = {
        "participant_keys": gold_key_set,
        "exact_participant_match": bool(target_participants and target_participants == gold_key_set),
        "service_like": looks_service_like_value(gold_conversation.display_name),
    }

    target_sender_keys = target_sender_variants(target)
    candidate_rows: list[dict[str, Any]] = []
    for conversation in conversations:
        if conversation.conversation_id == target.conversation_id:
            continue
        history = build_history(
            messages_by_conversation,
            conversation.conversation_id,
            reference_utc,
            history_limit,
        )
        features = score_candidate(
            target,
            target_participants,
            conversation.is_group,
            conversation.participants,
            history,
            reference_utc,
        )
        if features["candidate_score"] <= 0:
            continue
        key_set = candidate_participant_key_set(conversation.participants)
        last_history = history[-1] if history else {}
        sender_overlap = bool(target_sender_keys & history_sender_variants(last_history))
        candidate_rows.append(
            {
                "conversation": conversation,
                "history": history,
                "features": features,
                "participant_keys": key_set,
                "exact_participant_match": bool(target_participants and target_participants == key_set),
                "service_like": looks_service_like_value(conversation.display_name),
                "same_groupness": conversation.is_group == gold_conversation.is_group,
                "preview_overlap": token_overlap(target_preview, last_history.get("preview")),
                "sender_overlap": sender_overlap,
            }
        )

    return gold_conversation, candidate_rows, gold_features, gold_history, gold_analysis


def choose_variant_modes(
    target: MessageRow,
    candidate_rows: list[dict[str, Any]],
    *,
    explicit_modes: list[str] | None = None,
) -> list[str]:
    if explicit_modes:
        modes = explicit_modes
    else:
        modes = ["top"]
        if looks_generic(target.preview) or preview_bucket(target.preview) in {"empty", "reaction"}:
            modes.append("recency")
            modes.append("geometry")
        if any(row["conversation"].is_group != target.is_group for row in candidate_rows):
            modes.append("cross-group")
        if any(row["service_like"] for row in candidate_rows) or looks_service_like_value(target.conversation_display_name):
            modes.append("service")
        if len(modes) == 1:
            modes.append("geometry")

    deduped: list[str] = []
    seen: set[str] = set()
    for mode in modes:
        if mode not in seen:
            deduped.append(mode)
            seen.add(mode)
    return deduped


def sort_candidate_rows_for_mode(
    rows: list[dict[str, Any]],
    *,
    mode: str,
) -> list[dict[str, Any]]:
    def delta_key(row: dict[str, Any]) -> float:
        delta = row["features"].get("delta_seconds")
        return float(delta) if delta is not None else 10**12

    if mode == "top":
        return sorted(rows, key=lambda row: (-row["features"]["candidate_score"], delta_key(row)))
    if mode == "recency":
        return sorted(
            rows,
            key=lambda row: (
                delta_key(row),
                -row["preview_overlap"],
                -row["features"]["participant_overlap"],
                -float(row["sender_overlap"]),
                -row["features"]["candidate_score"],
            ),
        )
    if mode == "geometry":
        return sorted(
            rows,
            key=lambda row: (
                -float(row["exact_participant_match"]),
                -row["features"]["participant_overlap"],
                -float(row["same_groupness"]),
                -float(row["sender_overlap"]),
                delta_key(row),
                -row["features"]["candidate_score"],
            ),
        )
    if mode == "cross-group":
        return sorted(
            rows,
            key=lambda row: (
                -float(not row["same_groupness"]),
                -row["features"]["participant_overlap"],
                -row["preview_overlap"],
                delta_key(row),
                -row["features"]["candidate_score"],
            ),
        )
    if mode == "service":
        return sorted(
            rows,
            key=lambda row: (
                -float(row["service_like"]),
                -float(row["sender_overlap"]),
                -row["features"]["participant_overlap"],
                delta_key(row),
                -row["features"]["candidate_score"],
            ),
        )
    raise ValueError(f"Unknown candidate mode: {mode}")


def stratify_real_samples(
    samples: list[dict[str, Any]],
    *,
    limit: int,
    max_per_conversation: int,
    max_per_message: int,
    seed: int,
) -> list[dict[str, Any]]:
    if len(samples) <= limit:
        return samples

    rng = random.Random(seed)
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for sample in samples:
        metadata = sample.get("metadata", {})
        bucket = (
            metadata.get("variant_kind", "top"),
            "sent" if metadata.get("target_is_outbound") else "inbox",
            "group" if sample.get("message", {}).get("is_group") else "direct",
            metadata.get("preview_bucket", "unknown"),
            "service" if metadata.get("service_like") else "human",
        )
        grouped[bucket].append(sample)

    ordered_buckets = list(grouped.items())
    for _, bucket_samples in ordered_buckets:
        rng.shuffle(bucket_samples)
    rng.shuffle(ordered_buckets)

    selected: list[dict[str, Any]] = []
    by_conversation: dict[str, int] = defaultdict(int)
    by_message: dict[str, int] = defaultdict(int)
    seen_sample_ids: set[str] = set()

    while len(selected) < limit:
        progress = False
        for _, bucket_samples in ordered_buckets:
            while bucket_samples:
                sample = bucket_samples.pop(0)
                sample_id = sample["sample_id"]
                message_key = sample.get("message", {}).get("message_key", sample_id)
                gold_thread_id = sample.get("gold_thread_id", "")
                if sample_id in seen_sample_ids:
                    continue
                if by_conversation[gold_thread_id] >= max_per_conversation:
                    continue
                if by_message[message_key] >= max_per_message:
                    continue
                selected.append(sample)
                seen_sample_ids.add(sample_id)
                by_conversation[gold_thread_id] += 1
                by_message[message_key] += 1
                progress = True
                break
            if len(selected) >= limit:
                break
        if not progress:
            break

    return selected


def build_real_eval_samples(
    messages: list[MessageRow],
    conversations: list[ConversationRow],
    notifications: list[NotificationRow],
    *,
    limit: int,
    max_candidates: int,
    history_limit: int,
    seed: int,
    hard_only: bool = False,
    slice_name: str = "all",
    max_per_conversation: int = 4,
    max_per_message: int = 2,
    variant_modes: list[str] | None = None,
) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    messages_by_conversation: dict[str, list[MessageRow]] = defaultdict(list)
    for message in sorted(messages, key=lambda item: item.sort_utc or datetime.min.replace(tzinfo=timezone.utc)):
        messages_by_conversation[message.conversation_id].append(message)

    all_samples: list[dict[str, Any]] = []
    for target in messages:
        reference_utc = target.sort_utc
        (
            gold_conversation,
            candidate_rows,
            gold_features,
            gold_history,
            gold_analysis,
        ) = make_candidate_pool(
            target,
            conversations,
            messages_by_conversation,
            history_limit=history_limit,
        )
        if not gold_conversation:
            continue
        if not candidate_rows:
            continue

        future_corrob = compute_future_corroboration(
            messages_by_conversation,
            gold_conversation.conversation_id,
            reference_utc,
            exclude_message_key=target.message_key,
        )
        variant_candidates = choose_variant_modes(target, candidate_rows, explicit_modes=variant_modes)
        raw_message = target.payload.get("message", {})
        nearby_notifications = extract_matching_notifications(target, notifications)

        seen_variant_signatures: set[tuple[str, ...]] = set()
        for variant_kind in variant_candidates:
            ranked_rows = sort_candidate_rows_for_mode(candidate_rows, mode=variant_kind)
            negatives = ranked_rows[: max_candidates - 1]
            if len(negatives) < max_candidates - 1:
                continue
            signature = tuple(sorted(row["conversation"].conversation_id for row in negatives))
            if signature in seen_variant_signatures:
                continue
            seen_variant_signatures.add(signature)

            candidates = [
                {
                    "thread_id": gold_conversation.conversation_id,
                    "display_name": gold_conversation.display_name,
                    "is_group": gold_conversation.is_group,
                    "participants": gold_conversation.participants,
                    "history": gold_history,
                    "features": gold_features,
                }
            ]
            for row in negatives:
                conversation = row["conversation"]
                candidates.append(
                    {
                        "thread_id": conversation.conversation_id,
                        "display_name": conversation.display_name,
                        "is_group": conversation.is_group,
                        "participants": conversation.participants,
                        "history": row["history"],
                        "features": row["features"],
                    }
                )

            rng.shuffle(candidates)
            sample = {
                "sample_id": f"real::{target.message_key}::{variant_kind}",
                "source": "real_cache",
                "label_source": "cache_conversation_id",
                "gold_thread_id": target.conversation_id,
                "message": {
                    "message_key": target.message_key,
                    "sort_utc": target.payload.get("sortTimestampUtc"),
                    "conversation_display_name": target.conversation_display_name,
                    "is_group": target.is_group,
                    "participants": target.payload.get("participants", []),
                    "message": raw_message,
                },
                "candidate_threads": candidates[:max_candidates],
                "nearby_notifications": nearby_notifications,
                "metadata": {
                    "generic_preview": looks_generic(target.preview),
                    "preview": target.preview,
                    "preview_bucket": preview_bucket(target.preview),
                    "candidate_count": min(len(candidates), max_candidates),
                    "target_participant_count": len(participant_keys(target.payload)),
                    "target_is_outbound": normalize_text(raw_message.get("folder")) == "sent",
                    "target_has_text": bool(normalize_text(target.preview or raw_message.get("body") or raw_message.get("subject"))),
                    "target_body_missing": not bool(normalize_text(raw_message.get("body"))),
                    "target_subject_present": bool(normalize_text(raw_message.get("subject"))),
                    "variant_kind": variant_kind,
                    "service_like": looks_service_like_value(target.conversation_display_name) or gold_analysis["service_like"],
                    "self_test_like": False,
                    "future_corrob": future_corrob,
                    "gold_exact_participant_match": gold_analysis["exact_participant_match"],
                    "gold_delta_seconds": gold_features.get("delta_seconds"),
                    "nearby_notification_count": len(nearby_notifications),
                },
            }
            sample["metadata"]["self_test_like"] = is_self_test_like_real_sample(sample)
            if hard_only and not is_structurally_hard_real_sample(sample):
                continue
            if slice_name != "all" and not matches_real_slice(sample, slice_name):
                continue
            all_samples.append(sample)

    return stratify_real_samples(
        all_samples,
        limit=limit,
        max_per_conversation=max_per_conversation,
        max_per_message=max_per_message,
        seed=seed,
    )


def is_structurally_hard_real_sample(sample: dict[str, Any]) -> bool:
    preview = normalize_text(sample.get("metadata", {}).get("preview"))
    generic_or_empty = preview_bucket(preview) in {"empty", "generic", "reaction"}
    overlaps = [int(candidate.get("features", {}).get("participant_overlap", 0)) for candidate in sample.get("candidate_threads", [])]
    if not overlaps:
        return False
    max_overlap = max(overlaps)
    tied_max = overlaps.count(max_overlap) > 1
    exact_easy = unique_gold_max_exact_participant_match(sample)
    return generic_or_empty or max_overlap == 0 or tied_max or not exact_easy


SELF_TEST_PATTERNS = (
    "adit",
    "smoke",
    "self-send",
    "daemon live test",
    "codex smoke",
    "probe",
    "obex",
    "map takeover",
    "map handle chase",
    "map mns event",
    "map send test",
    "raw 9fa notify",
)


def is_self_test_like_real_sample(sample: dict[str, Any]) -> bool:
    preview = normalize_text(sample.get("metadata", {}).get("preview"))
    if any(pattern in preview for pattern in SELF_TEST_PATTERNS):
        return True
    gold = next(
        (candidate for candidate in sample.get("candidate_threads", []) if candidate.get("thread_id") == sample.get("gold_thread_id")),
        None,
    )
    return bool(gold and normalize_text(gold.get("display_name")) == "my number")


def looks_reaction_preview(value: str | None) -> bool:
    preview = normalize_text(value)
    return preview.startswith(("laughed at", "liked", "disliked", "reacted"))


def candidate_participant_keys(candidate: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    for participant in candidate.get("participants", []):
        key = normalize_text(participant.get("key"))
        if key:
            keys.add(key)
        display = normalize_text(participant.get("displayName"))
        if display:
            keys.add(display)
        for phone in participant.get("phones", []) or []:
            normalized = normalize_text(phone)
            if normalized:
                keys.add(normalized)
        for email in participant.get("emails", []) or []:
            normalized = normalize_text(email)
            if normalized:
                keys.add(normalized)
    return keys


def target_participant_keys_from_sample(sample: dict[str, Any]) -> set[str]:
    raw_message = sample.get("message", {}).get("message", {})
    payload = {
        "participants": sample.get("message", {}).get("participants", []),
        "senderName": raw_message.get("senderName"),
        "senderAddressing": raw_message.get("senderAddressing"),
        "recipientAddressing": raw_message.get("recipientAddressing"),
        "originators": raw_message.get("originators", []),
        "recipients": raw_message.get("recipients", []),
    }
    return participant_keys(payload)


def unique_gold_max_exact_participant_match(sample: dict[str, Any]) -> bool:
    gold_thread_id = sample.get("gold_thread_id")
    target_keys = target_participant_keys_from_sample(sample)
    scores: list[int] = []
    gold_index = -1
    for index, candidate in enumerate(sample.get("candidate_threads", [])):
        candidate_keys = candidate_participant_keys(candidate)
        exact = int(bool(target_keys) and target_keys == candidate_keys)
        scores.append(exact)
        if candidate.get("thread_id") == gold_thread_id:
            gold_index = index
    if gold_index < 0 or not scores:
        return False
    max_score = max(scores)
    return scores[gold_index] == max_score and scores.count(max_score) == 1


def unique_gold_max_participant_overlap(sample: dict[str, Any]) -> bool:
    gold_thread_id = sample.get("gold_thread_id")
    overlaps = [int(candidate.get("features", {}).get("participant_overlap", 0)) for candidate in sample.get("candidate_threads", [])]
    if not overlaps:
        return False
    max_overlap = max(overlaps)
    gold_overlap = None
    for candidate in sample.get("candidate_threads", []):
        if candidate.get("thread_id") == gold_thread_id:
            gold_overlap = int(candidate.get("features", {}).get("participant_overlap", 0))
            break
    if gold_overlap is None:
        return False
    return gold_overlap == max_overlap and overlaps.count(max_overlap) == 1


def is_service_like_sample(sample: dict[str, Any]) -> bool:
    metadata = sample.get("metadata", {})
    if metadata.get("service_like"):
        return True
    gold = next(
        (candidate for candidate in sample.get("candidate_threads", []) if candidate.get("thread_id") == sample.get("gold_thread_id")),
        None,
    )
    if gold and looks_service_like_value(gold.get("display_name")):
        return True
    return False


def matches_real_slice(sample: dict[str, Any], slice_name: str) -> bool:
    preview = sample.get("metadata", {}).get("preview")
    preview_kind = preview_bucket(preview)
    generic_or_empty = preview_kind in {"empty", "generic", "reaction"}
    overlaps = [int(candidate.get("features", {}).get("participant_overlap", 0)) for candidate in sample.get("candidate_threads", [])]
    max_overlap = max(overlaps) if overlaps else 0
    tied_overlap = bool(overlaps) and overlaps.count(max_overlap) > 1
    exact_easy = unique_gold_max_exact_participant_match(sample)
    overlap_easy = unique_gold_max_participant_overlap(sample)
    future_strength = int(sample.get("metadata", {}).get("future_corrob", {}).get("future_corrob_strength", 0))
    outbound = bool(sample.get("metadata", {}).get("target_is_outbound"))
    service_like = is_service_like_sample(sample)
    notification_sparse = int(sample.get("metadata", {}).get("nearby_notification_count", 0)) == 0

    if slice_name == "clean":
        return not is_self_test_like_real_sample(sample)
    if slice_name == "semantic-stress":
        return (not is_self_test_like_real_sample(sample)) and generic_or_empty
    if slice_name == "tied-overlap":
        return (not is_self_test_like_real_sample(sample)) and tied_overlap
    if slice_name == "reaction":
        return (not is_self_test_like_real_sample(sample)) and looks_reaction_preview(preview)
    if slice_name == "proof":
        return (
            (not is_self_test_like_real_sample(sample))
            and generic_or_empty
            and not exact_easy
            and (not overlap_easy or tied_overlap)
            and future_strength >= 1
        )
    if slice_name == "headline-proof":
        return (
            matches_real_slice(sample, "proof")
            and not outbound
            and not service_like
        )
    if slice_name == "self-outbound-diag":
        return outbound or is_self_test_like_real_sample(sample)
    if slice_name == "outbound-identity-risk":
        return (
            (not is_self_test_like_real_sample(sample))
            and outbound
            and not service_like
            and generic_or_empty
        )
    if slice_name == "direct-identity-risk":
        return (
            (not is_self_test_like_real_sample(sample))
            and not sample.get("message", {}).get("is_group")
            and not service_like
            and (generic_or_empty or not exact_easy or not overlap_easy)
        )
    if slice_name == "service-shortcode-diag":
        return service_like and not is_self_test_like_real_sample(sample)
    if slice_name == "notification-sparse-diag":
        return (not is_self_test_like_real_sample(sample)) and notification_sparse
    if slice_name == "stale-corroborated":
        gold_delta_seconds = sample.get("metadata", {}).get("gold_delta_seconds")
        return (
            (not is_self_test_like_real_sample(sample))
            and generic_or_empty
            and isinstance(gold_delta_seconds, int)
            and gold_delta_seconds >= 3600
            and future_strength >= 1
        )
    raise ValueError(f"Unknown slice_name: {slice_name}")


def make_participant_pool(rng: random.Random) -> list[str]:
    family = rng.sample(STYLE_PACKS["family_names"], k=rng.randint(3, 6))
    friends = rng.sample(STYLE_PACKS["friend_names"], k=rng.randint(2, 4))
    return family + friends


def choose_preview(rng: random.Random, category: str) -> str:
    if category == "generic":
        return rng.choice(STYLE_PACKS["generic_replies"])
    if category == "plan":
        return rng.choice(STYLE_PACKS["planning_lines"])
    preview = rng.choice(STYLE_PACKS["planning_lines"])
    if rng.random() < 0.35:
        preview = rng.choice(STYLE_PACKS["reaction_templates"]).format(preview=preview)
    return preview


def build_synth_samples(*, count: int, max_candidates: int, seed: int) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    base_time = datetime.now(tz=timezone.utc).replace(microsecond=0)
    samples: list[dict[str, Any]] = []

    for index in range(count):
        participant_pool = make_participant_pool(rng)
        thread_count = rng.randint(3, 6)
        threads = []
        for thread_index in range(thread_count):
            is_group = rng.random() < 0.55
            if is_group:
                thread_participants = rng.sample(participant_pool, k=rng.randint(3, min(6, len(participant_pool))))
                display_name = ", ".join(thread_participants[:3])
                if len(thread_participants) > 3:
                    display_name += f" +{len(thread_participants) - 3}"
            else:
                thread_participants = [rng.choice(participant_pool)]
                display_name = thread_participants[0]

            history = []
            turn_count = rng.randint(3, 8)
            for turn_index in range(turn_count):
                category = "generic" if rng.random() < 0.4 else "plan"
                preview = choose_preview(rng, category)
                speaker = rng.choice(thread_participants)
                history.append(
                    {
                        "message_key": f"synth-{index}-{thread_index}-{turn_index}",
                        "sort_utc": (base_time - timedelta(minutes=(thread_count - thread_index) * 14 + (turn_count - turn_index) * 3)).isoformat(),
                        "preview": preview,
                        "sender_name": speaker,
                        "body": preview,
                        "folder": "inbox",
                        "status": "Unread",
                    }
                )

            threads.append(
                {
                    "thread_id": f"synth-thread-{index}-{thread_index}",
                    "display_name": display_name,
                    "is_group": is_group,
                    "participants": [
                        {"key": f"name:{name.lower()}", "displayName": name, "phones": [], "emails": [], "isSelf": False}
                        for name in thread_participants
                    ],
                    "history": history,
                }
            )

        gold = rng.choice(threads)
        target_preview = choose_preview(rng, "generic" if rng.random() < 0.7 else "plan")
        target_sender = rng.choice([participant["displayName"] for participant in gold["participants"]])
        target_message = {
            "message_key": f"synth-target-{index}",
            "sort_utc": base_time.isoformat(),
            "conversation_display_name": gold["display_name"],
            "is_group": gold["is_group"],
            "participants": gold["participants"],
            "message": {
                "folder": "inbox",
                "handle": None,
                "type": "SMS_GSM",
                "datetime": base_time.strftime("%Y%m%dT%H%M%S"),
                "senderName": target_sender if rng.random() < 0.7 else "",
                "senderAddressing": target_sender if rng.random() < 0.5 else "",
                "recipientAddressing": "",
                "body": target_preview if rng.random() < 0.8 else "",
                "subject": "" if rng.random() < 0.8 else target_preview,
                "status": "Unread",
                "messageType": "SMSGSM",
            },
        }

        candidate_threads = []
        for thread in threads:
            features = {
                "candidate_score": 0.0,
                "preview_overlap": token_overlap(target_preview, thread["history"][-1]["preview"]),
                "participant_overlap": len(
                    {participant["displayName"] for participant in thread["participants"]}
                    & {participant["displayName"] for participant in gold["participants"]}
                ),
                "delta_seconds": int(abs((base_time - parse_utc(thread["history"][-1]["sort_utc"])).total_seconds())),
                "candidate_is_group": thread["is_group"],
            }
            candidate_threads.append({**thread, "features": features})

        candidate_threads.sort(key=lambda item: item["features"]["delta_seconds"])
        candidate_threads = candidate_threads[:max_candidates]
        rng.shuffle(candidate_threads)

        samples.append(
            {
                "sample_id": f"synth::{index}",
                "source": "synthetic",
                "label_source": "latent_thread_id",
                "gold_thread_id": gold["thread_id"],
                "message": target_message,
                "candidate_threads": candidate_threads,
                "nearby_notifications": [
                    {
                        "notification_uid": index,
                        "received_utc": base_time.isoformat(),
                        "title": gold["display_name"] if gold["is_group"] else target_sender,
                        "subtitle": target_sender if gold["is_group"] else "",
                        "message": target_preview,
                        "app_identifier": "com.apple.MobileSMS",
                    }
                ],
                "metadata": {
                    "generic_preview": looks_generic(target_preview),
                    "preview": target_preview,
                    "candidate_count": len(candidate_threads),
                    "target_participant_count": len(gold["participants"]),
                },
            }
        )

    return samples


def write_dataset(samples: list[dict[str, Any]], output_path: Path, *, meta: dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "meta": meta,
        "samples": samples,
    }
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def load_dataset(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Build real eval slices and synthetic candidate-set datasets for thread matching.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    real_parser = subparsers.add_parser("mine-real-eval", help="Extract a real eval slice from the local Adit cache.")
    real_parser.add_argument("--db", type=Path, default=DEFAULT_ADIT_DB)
    real_parser.add_argument("--out", type=Path, default=Path("artifacts") / "thread-model" / "real_eval.json")
    real_parser.add_argument("--limit", type=int, default=80)
    real_parser.add_argument("--max-candidates", type=int, default=8)
    real_parser.add_argument("--history-limit", type=int, default=8)
    real_parser.add_argument("--seed", type=int, default=7)
    real_parser.add_argument("--hard-only", action="store_true", help="Keep only structurally hard real samples (generic/empty preview, zero overlap, or tied max overlap).")
    real_parser.add_argument(
        "--slice",
        type=str,
        default="all",
        choices=[
            "all",
            "clean",
            "semantic-stress",
            "tied-overlap",
            "reaction",
            "proof",
            "headline-proof",
            "self-outbound-diag",
            "outbound-identity-risk",
            "direct-identity-risk",
            "service-shortcode-diag",
            "notification-sparse-diag",
            "stale-corroborated",
        ],
        help="Optional curated real slice to export instead of the full mined pool.",
    )
    real_parser.add_argument("--max-per-conversation", type=int, default=4)
    real_parser.add_argument("--max-per-message", type=int, default=2)
    real_parser.add_argument(
        "--variants",
        type=str,
        default="auto",
        help="Comma-separated candidate-set variant modes. Use auto, or any of: top,recency,geometry,cross-group,service",
    )

    synth_parser = subparsers.add_parser("generate-synth", help="Generate synthetic candidate-set training data.")
    synth_parser.add_argument("--out", type=Path, default=Path("artifacts") / "thread-model" / "synthetic_train.json")
    synth_parser.add_argument("--count", type=int, default=5000)
    synth_parser.add_argument("--max-candidates", type=int, default=8)
    synth_parser.add_argument("--seed", type=int, default=7)
    synth_parser.add_argument("--style-packs", type=Path)
    synth_parser.add_argument("--thread-windows", type=Path, default=Path("artifacts") / "thread-model" / "thread_windows.json")
    synth_parser.add_argument("--replay-failure-boost", type=float, default=0.0)
    synth_parser.add_argument("--focus-ambiguity-reason", action="append", default=[])
    synth_parser.add_argument("--focus-ambiguity-boost", type=float, default=0.0)

    style_parser = subparsers.add_parser("generate-style-packs", help="Generate reusable language/style packs with Claude Opus.")
    style_parser.add_argument("--out", type=Path, default=Path("artifacts") / "thread-model" / "style_packs.json")
    style_parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_PATH)
    style_parser.add_argument("--model", type=str, default=DEFAULT_MODEL)
    style_parser.add_argument("--max-tokens", type=int, default=12000)

    window_parser = subparsers.add_parser("generate-thread-windows", help="Generate realistic overlapping thread windows with Claude Opus.")
    window_parser.add_argument("--out", type=Path, default=Path("artifacts") / "thread-model" / "thread_windows.json")
    window_parser.add_argument("--style-packs", type=Path, default=Path("artifacts") / "thread-model" / "style_packs.json")
    window_parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_PATH)
    window_parser.add_argument("--model", type=str, default=DEFAULT_MODEL)
    window_parser.add_argument("--batch-count", type=int, default=1)
    window_parser.add_argument("--windows-per-batch", type=int, default=10)
    window_parser.add_argument("--max-tokens", type=int, default=16000)

    validate_parser = subparsers.add_parser("validate-dataset", help="Validate a generated real or synthetic dataset.")
    validate_parser.add_argument("--dataset", type=Path, required=True)

    args = parser.parse_args()

    if args.command == "mine-real-eval":
        messages, conversations, notifications = load_cache(args.db)
        samples = build_real_eval_samples(
            messages,
            conversations,
            notifications,
            limit=args.limit,
            max_candidates=args.max_candidates,
            history_limit=args.history_limit,
            seed=args.seed,
            hard_only=args.hard_only,
            slice_name=args.slice,
            max_per_conversation=args.max_per_conversation,
            max_per_message=args.max_per_message,
            variant_modes=None if args.variants == "auto" else [item.strip() for item in args.variants.split(",") if item.strip()],
        )
        write_dataset(
            samples,
            args.out,
            meta={
                "kind": "real_eval",
                "db": str(args.db),
                "limit": args.limit,
                "max_candidates": args.max_candidates,
                "history_limit": args.history_limit,
                "hard_only": args.hard_only,
                "slice": args.slice,
                "max_per_conversation": args.max_per_conversation,
                "max_per_message": args.max_per_message,
                "variants": args.variants,
                "sample_count": len(samples),
                "seed": args.seed,
            },
        )
        print(f"Wrote {len(samples)} real eval samples to {args.out}")
        return

    if args.command == "generate-synth":
        thread_windows = load_thread_windows(args.thread_windows)
        style_packs = load_style_packs(args.style_packs)
        samples = build_synth_samples_from_windows(
            count=args.count,
            max_candidates=args.max_candidates,
            seed=args.seed,
            thread_windows=thread_windows,
            style_packs=style_packs,
            replay_failure_boost=args.replay_failure_boost,
            focus_ambiguity_reasons=args.focus_ambiguity_reason,
            focus_ambiguity_boost=args.focus_ambiguity_boost,
        )
        validation = validate_dataset(samples)
        write_dataset(
            samples,
            args.out,
            meta={
                "kind": "synthetic_candidate_sets",
                "count": args.count,
                "max_candidates": args.max_candidates,
                "sample_count": len(samples),
                "seed": args.seed,
                "replay_failure_boost": args.replay_failure_boost,
                "focus_ambiguity_reasons": args.focus_ambiguity_reason,
                "focus_ambiguity_boost": args.focus_ambiguity_boost,
                "style_packs": style_packs,
                "thread_windows": str(args.thread_windows),
                "validation": {
                    "quality_score": validation["quality_score"],
                    "issues": validation["issues"],
                },
            },
        )
        print(f"Wrote {len(samples)} synthetic samples to {args.out}")
        print(dump_validation_report(str(args.out), validation))
        return

    if args.command == "generate-style-packs":
        payload = generate_style_packs(
            out_path=args.out,
            env_file=args.env_file,
            model=args.model,
            max_tokens=args.max_tokens,
        )
        print(
            json.dumps(
                {
                    "out": str(args.out),
                    "family_names": len(payload["style_packs"]["family_names"]),
                    "friend_names": len(payload["style_packs"]["friend_names"]),
                    "service_senders": len(payload["style_packs"]["service_senders"]),
                    "generic_replies": len(payload["style_packs"]["generic_replies"]),
                    "planning_lines": len(payload["style_packs"]["planning_lines"]),
                    "group_title_variants": len(payload["style_packs"]["group_title_variants"]),
                    "ambiguous_messages": len(payload["style_packs"]["ambiguous_messages"]),
                },
                indent=2,
            )
        )
        return

    if args.command == "generate-thread-windows":
        payload = generate_thread_windows(
            out_path=args.out,
            style_packs=load_style_packs(args.style_packs),
            env_file=args.env_file,
            model=args.model,
            batch_count=args.batch_count,
            windows_per_batch=args.windows_per_batch,
            max_tokens=args.max_tokens,
        )
        print(
            json.dumps(
                {
                    "out": str(args.out),
                    "window_count": payload["meta"]["window_count"],
                    "batch_count": payload["meta"]["batch_count"],
                    "windows_per_batch": payload["meta"]["windows_per_batch"],
                },
                indent=2,
            )
        )
        return

    if args.command == "validate-dataset":
        payload = load_dataset(args.dataset)
        samples = payload["samples"]
        print(dump_validation_report(str(args.dataset), validate_dataset(samples)))
        return

    raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
