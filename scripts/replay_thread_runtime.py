#!/usr/bin/env python3
from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional


ROOT = Path(__file__).resolve().parents[1]
for extra in (ROOT / "scripts", ROOT / "training"):
    extra_str = str(extra)
    if extra_str not in sys.path:
        sys.path.insert(0, extra_str)

from build_thread_model_dataset import MessageRow, NotificationRow, load_cache  # type: ignore
from thread_scoring_sidecar import ThreadChooserRuntime  # type: ignore


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


def normalize_text(value: Optional[str]) -> str:
    return "" if not value else " ".join(value.lower().split())


def digits_only(value: Optional[str]) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def tokenize(value: Optional[str]) -> List[str]:
    normalized = normalize_text(value).replace(",", " ").replace(".", " ")
    return [token for token in normalized.split(" ") if token]


def token_overlap(left: Optional[str], right: Optional[str]) -> float:
    left_tokens = set(tokenize(left))
    right_tokens = set(tokenize(right))
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def build_preview_from_message(message: MessageRow) -> Optional[str]:
    if message.preview:
        return build_preview(message.preview)
    payload = message.payload.get("message", {})
    body = payload.get("body") or payload.get("subject")
    return build_preview(body)


def build_preview(value: Optional[str]) -> Optional[str]:
    if not value or not str(value).strip():
        return None
    trimmed = str(value).strip()
    return trimmed if len(trimmed) <= 160 else f"{trimmed[:160]}..."


def looks_generic(preview: Optional[str]) -> bool:
    normalized = normalize_text(preview)
    if not normalized:
        return True
    if normalized in GENERIC_PREVIEWS:
        return True
    tokens = tokenize(normalized)
    return len(tokens) <= 2 and len(normalized) <= 16


def add_variant(variants: set[str], raw: Optional[str]) -> None:
    normalized = normalize_text(raw)
    if normalized:
        variants.add(normalized)
    digits = digits_only(raw)
    if digits:
        variants.add(digits)


def target_sender_variants(message: MessageRow) -> set[str]:
    raw = message.payload.get("message", {})
    variants: set[str] = set()
    add_variant(variants, raw.get("senderName"))
    add_variant(variants, raw.get("senderAddressing"))
    for originator in raw.get("originators", []) or []:
        add_variant(variants, originator.get("name"))
        for phone in originator.get("phones", []) or []:
            add_variant(variants, phone)
    return variants


def history_sender_variants(turn: Dict[str, Any]) -> set[str]:
    variants: set[str] = set()
    add_variant(variants, turn.get("sender_name"))
    add_variant(variants, turn.get("sender_addressing"))
    return variants


def nonself_participant_keys(participants: List[Dict[str, Any]]) -> set[str]:
    keys: set[str] = set()
    for participant in participants:
        if participant.get("isSelf"):
            continue
        key = str(participant.get("key") or "").strip()
        if key:
            keys.add(key)
    return keys


def explicit_counterparty_keys(participants: List[Dict[str, Any]]) -> List[str]:
    keys: set[str] = set()
    for participant in participants:
        if participant.get("isSelf"):
            continue
        key = normalize_text(participant.get("key"))
        if key.startswith("phone:") or key.startswith("email:"):
            keys.add(key)
        for phone in participant.get("phones", []) or []:
            normalized = normalize_text(phone)
            if normalized:
                keys.add(f"phone:{normalized}")
            digits = digits_only(phone)
            if digits:
                keys.add(f"phone_digits:{digits}")
        for email in participant.get("emails", []) or []:
            normalized = normalize_text(email)
            if normalized:
                keys.add(f"email:{normalized}")
    return sorted(keys)


def is_messages_notification(notification: NotificationRow) -> bool:
    return notification.app_identifier in ("com.apple.MobileSMS", "com.apple.MobileSMS.notification")


def is_near(reference: Optional[datetime], candidate: Optional[datetime], window: timedelta) -> bool:
    return reference is not None and candidate is not None and abs(reference - candidate) <= window


def canonical_participant(participant: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "key": participant.get("key") or "",
        "displayName": participant.get("displayName") or "",
        "phones": sorted({str(phone) for phone in participant.get("phones", []) or []}, key=str.lower),
        "emails": sorted({str(email) for email in participant.get("emails", []) or []}, key=str.lower),
        "isSelf": bool(participant.get("isSelf")),
    }


class RuntimeBucket:
    def __init__(self, conversation_id: str, display_name: str, is_group: bool, seed_participants: List[Dict[str, Any]]) -> None:
        self.conversation_id = conversation_id
        self.display_name = display_name
        self.is_group = is_group
        self._participants: Dict[str, Dict[str, Any]] = {}
        self._messages: List[MessageRow] = []
        for participant in seed_participants:
            canonical = canonical_participant(participant)
            key = canonical["key"]
            if key:
                self._participants[key] = canonical

    @property
    def participants(self) -> List[Dict[str, Any]]:
        return sorted(
            self._participants.values(),
            key=lambda participant: ((participant.get("displayName") or "").lower(), (participant.get("key") or "").lower()),
        )

    def add_message(self, message: MessageRow) -> None:
        self._messages.append(message)
        for participant in message.payload.get("participants", []) or []:
            canonical = canonical_participant(participant)
            key = canonical["key"]
            if not key:
                continue
            current = self._participants.get(key)
            if current is None:
                self._participants[key] = canonical
                continue
            self._participants[key] = {
                "key": key,
                "displayName": current["displayName"]
                if len(str(current["displayName"])) >= len(str(canonical["displayName"]))
                else canonical["displayName"],
                "phones": sorted({*current.get("phones", []), *canonical.get("phones", [])}, key=str.lower),
                "emails": sorted({*current.get("emails", []), *canonical.get("emails", [])}, key=str.lower),
                "isSelf": bool(current.get("isSelf")) or bool(canonical.get("isSelf")),
            }

        if message.is_group:
            self.is_group = True
        if (not self.display_name) or (message.conversation_display_name and len(message.conversation_display_name) > len(self.display_name)):
            self.display_name = message.conversation_display_name

    def build_history(self, max_turns: int) -> List[Dict[str, Any]]:
        ordered = sorted(
            self._messages,
            key=lambda item: (item.sort_utc or datetime.min.replace(tzinfo=timezone.utc), item.message_key),
        )
        tail = ordered[-max_turns:]
        history: List[Dict[str, Any]] = []
        for message in tail:
            raw = message.payload.get("message", {})
            history.append(
                {
                    "message_key": message.message_key,
                    "sort_utc": message.payload.get("sortTimestampUtc"),
                    "preview": build_preview_from_message(message),
                    "sender_name": raw.get("senderName"),
                    "sender_addressing": raw.get("senderAddressing"),
                    "body": raw.get("body"),
                    "folder": raw.get("folder"),
                    "status": raw.get("status"),
                }
            )
        return history


def build_runtime_sample(
    message: MessageRow,
    buckets: Dict[str, RuntimeBucket],
    notifications: List[NotificationRow],
    *,
    max_candidates: int,
    max_history_turns: int,
) -> Dict[str, Any]:
    target_preview = build_preview_from_message(message)
    target_utc = message.sort_utc
    target_participants = message.payload.get("participants", []) or []
    target_participant_keys = nonself_participant_keys(target_participants)
    target_sender_keys = target_sender_variants(message)

    candidate_rows: List[tuple[Dict[str, Any], float]] = []
    for bucket in buckets.values():
        history = bucket.build_history(max_history_turns)
        last_history = history[-1] if history else None
        preview_overlap = token_overlap(target_preview, last_history.get("preview") if last_history else None)
        participant_overlap = sum(
            1
            for participant in bucket.participants
            if not participant.get("isSelf") and str(participant.get("key") or "") in target_participant_keys
        )
        delta_seconds = (
            abs((target_utc - datetime.fromisoformat(str(last_history["sort_utc"]).replace("Z", "+00:00"))).total_seconds())
            if target_utc is not None and last_history and last_history.get("sort_utc")
            else None
        )
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
        if bucket.is_group == message.is_group:
            score += 0.5
        if looks_generic(target_preview):
            score += 0.5

        sender_overlap = bool(last_history and history_sender_variants(last_history) & target_sender_keys)
        if sender_overlap:
            score += 1.5

        if score <= 0 and participant_overlap <= 0 and not sender_overlap:
            continue

        candidate_rows.append(
            (
                {
                    "thread_id": bucket.conversation_id,
                    "display_name": bucket.display_name,
                    "is_group": bucket.is_group,
                    "participants": bucket.participants,
                    "history": history,
                    "features": {
                        "preview_overlap": preview_overlap,
                        "delta_seconds": delta_seconds,
                    },
                },
                score,
            )
        )

    candidate_rows.sort(
        key=lambda item: (
            -item[1],
            item[0]["features"]["delta_seconds"] if item[0]["features"]["delta_seconds"] is not None else float("inf"),
        )
    )
    candidate_rows = candidate_rows[: max(1, max_candidates - 1)]

    if not any(row[0]["thread_id"].lower() == message.conversation_id.lower() for row in candidate_rows):
        existing_current_history = buckets[message.conversation_id].build_history(max_history_turns) if message.conversation_id in buckets else []
        existing_current_last = existing_current_history[-1] if existing_current_history else None
        delta_seconds = (
            abs((target_utc - datetime.fromisoformat(str(existing_current_last["sort_utc"]).replace("Z", "+00:00"))).total_seconds())
            if target_utc is not None and existing_current_last and existing_current_last.get("sort_utc")
            else None
        )
        candidate_rows.insert(
            0,
            (
                {
                    "thread_id": message.conversation_id,
                    "display_name": message.conversation_display_name,
                    "is_group": message.is_group,
                    "participants": target_participants,
                    "history": existing_current_history,
                    "features": {
                        "preview_overlap": token_overlap(target_preview, existing_current_last.get("preview") if existing_current_last else None),
                        "delta_seconds": delta_seconds,
                    },
                },
                float("inf"),
            ),
        )

    nearby_notifications = [
        {
            "notification_uid": notification.notification_uid,
            "received_utc": notification.payload.get("receivedAtUtc"),
            "title": notification.title,
            "subtitle": notification.subtitle,
            "message": notification.message,
            "app_identifier": notification.app_identifier,
        }
        for notification in notifications
        if is_messages_notification(notification) and is_near(target_utc, notification.received_utc, timedelta(hours=2))
    ][:3]

    deduped_candidates: List[Dict[str, Any]] = []
    seen_thread_ids: set[str] = set()
    for candidate, _score in candidate_rows:
        thread_id = candidate["thread_id"]
        if thread_id.lower() in seen_thread_ids:
            continue
        seen_thread_ids.add(thread_id.lower())
        deduped_candidates.append(candidate)
        if len(deduped_candidates) >= max_candidates:
            break

    if not deduped_candidates:
        deduped_candidates = [
            {
                "thread_id": message.conversation_id,
                "display_name": message.conversation_display_name,
                "is_group": message.is_group,
                "participants": target_participants,
                "history": [],
                "features": {
                    "preview_overlap": 0.0,
                    "delta_seconds": None,
                },
            }
        ]

    return {
        "sample_id": f"runtime::{message.message_key}",
        "gold_thread_id": message.conversation_id,
        "fallback_thread_id": message.conversation_id,
        "message": {
            "message_key": message.message_key,
            "sort_utc": message.payload.get("sortTimestampUtc"),
            "conversation_display_name": message.conversation_display_name,
            "is_group": message.is_group,
            "participants": target_participants,
            "message": message.payload.get("message", {}),
        },
        "candidate_threads": deduped_candidates,
        "nearby_notifications": nearby_notifications,
        "metadata": {
            "preview": target_preview,
            "generic_preview": looks_generic(target_preview),
        },
    }


def assign_message(
    message: MessageRow,
    sample: Dict[str, Any],
    chosen_thread_id: Optional[str],
    buckets: Dict[str, RuntimeBucket],
) -> MessageRow:
    thread_ids = {candidate["thread_id"].lower(): candidate for candidate in sample["candidate_threads"]}
    selected_thread_id = (
        chosen_thread_id
        if chosen_thread_id and chosen_thread_id.lower() in thread_ids
        else sample["fallback_thread_id"]
    )
    candidate = thread_ids[selected_thread_id.lower()]
    if selected_thread_id not in buckets:
        buckets[selected_thread_id] = RuntimeBucket(
            selected_thread_id,
            candidate["display_name"],
            bool(candidate["is_group"]),
            message.payload.get("participants", []) or [],
        )
    bucket = buckets[selected_thread_id]
    assigned = dataclasses.replace(
        message,
        conversation_id=selected_thread_id,
        conversation_display_name=bucket.display_name,
        is_group=bucket.is_group,
    )
    bucket.add_message(assigned)
    return assigned


def replay_messages(
    messages: List[MessageRow],
    notifications: List[NotificationRow],
    *,
    max_candidates: int,
    max_history_turns: int,
    score_fn: Callable[[Dict[str, Any]], Optional[Dict[str, Any]]],
    max_failure_details: int = 25,
) -> Dict[str, Any]:
    ordered = sorted(
        messages,
        key=lambda item: (item.sort_utc or datetime.min.replace(tzinfo=timezone.utc), item.message_key),
    )
    buckets: Dict[str, RuntimeBucket] = {}
    steps: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []

    scored_samples = 0
    reranked_messages = 0
    correct = 0
    direct_total = 0
    direct_correct = 0
    group_total = 0
    group_correct = 0
    overgroup_errors = 0
    undergroup_errors = 0
    direct_wrong_counterparty_errors = 0

    for index, message in enumerate(ordered):
        sample = build_runtime_sample(
            message,
            buckets,
            notifications,
            max_candidates=max_candidates,
            max_history_turns=max_history_turns,
        )
        score = score_fn(sample) if len(sample["candidate_threads"]) >= 2 else None
        if score is not None:
            scored_samples += 1
        predicted_thread_id = score["predicted_thread_id"] if score is not None else sample["fallback_thread_id"]
        predicted_candidate = next(
            candidate
            for candidate in (score["candidates"] if score is not None else [{"thread_id": sample["fallback_thread_id"], "is_group": message.is_group, "identity_compatible": True, "probability": 1.0, "logit": 0.0}])
            if candidate["thread_id"] == predicted_thread_id
        )
        assigned = assign_message(message, sample, predicted_thread_id, buckets)

        gold_thread_id = message.conversation_id
        exact_match = predicted_thread_id.lower() == gold_thread_id.lower()
        correct += int(exact_match)
        reranked_messages += int(predicted_thread_id.lower() != gold_thread_id.lower())

        target_is_group = bool(message.is_group)
        pred_is_group = bool(predicted_candidate.get("is_group"))
        if target_is_group:
            group_total += 1
            group_correct += int(exact_match)
            if not pred_is_group and not exact_match:
                undergroup_errors += 1
        else:
            direct_total += 1
            direct_correct += int(exact_match)
            if pred_is_group and not exact_match:
                overgroup_errors += 1

        target_explicit_keys = set(score.get("target_explicit_counterparty_keys", [])) if score is not None else set(explicit_counterparty_keys(sample["message"]["participants"]))
        gold_candidate = next(candidate for candidate in sample["candidate_threads"] if candidate["thread_id"] == gold_thread_id)
        gold_identity_compatible = bool(target_explicit_keys & set(explicit_counterparty_keys(gold_candidate.get("participants", []) or [])))
        pred_identity_compatible = bool(predicted_candidate.get("identity_compatible"))
        if (not target_is_group) and target_explicit_keys and gold_identity_compatible and not pred_identity_compatible:
            direct_wrong_counterparty_errors += 1

        step = {
            "index": index,
            "message_key": message.message_key,
            "sort_utc": message.payload.get("sortTimestampUtc"),
            "preview": sample["metadata"]["preview"],
            "candidate_count": len(sample["candidate_threads"]),
            "gold_thread_id": gold_thread_id,
            "predicted_thread_id": predicted_thread_id,
            "exact_match": exact_match,
            "target_is_group": target_is_group,
            "predicted_is_group": pred_is_group,
            "target_explicit_counterparty_keys": sorted(target_explicit_keys),
            "predicted_identity_compatible": pred_identity_compatible,
            "direct_wrong_counterparty": (not target_is_group) and target_explicit_keys and gold_identity_compatible and not pred_identity_compatible,
            "reranked": predicted_thread_id.lower() != gold_thread_id.lower(),
            "top_probability": float(predicted_candidate.get("probability", 1.0)),
        }
        steps.append(step)
        if not exact_match and len(failures) < max_failure_details:
            failures.append(
                {
                    **step,
                    "sample": sample,
                    "score": score,
                    "assigned_conversation_id": assigned.conversation_id,
                }
            )

    total = len(ordered)
    metrics = {
        "top1": (correct / total) if total else 0.0,
        "direct_top1": (direct_correct / direct_total) if direct_total else 0.0,
        "group_top1": (group_correct / group_total) if group_total else 0.0,
        "direct_wrong_counterparty_errors": float(direct_wrong_counterparty_errors),
        "overgroup_errors": float(overgroup_errors),
        "undergroup_errors": float(undergroup_errors),
        "total_messages": float(total),
        "scored_messages": float(scored_samples),
        "reranked_messages": float(reranked_messages),
    }
    first_failure = failures[0] if failures else None
    return {
        "metrics": metrics,
        "steps": steps,
        "failures": failures,
        "first_failure": first_failure,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay the cached daemon message stream through the learned thread chooser.")
    parser.add_argument("--db", required=True, help="Path to the local Adit SQLite cache.")
    parser.add_argument("--checkpoint", required=True, help="Checkpoint to score during replay.")
    parser.add_argument("--model-name", help="Override frozen LM model name for semantic featurization.")
    parser.add_argument("--semantic-cache", help="Optional semantic cache path for model-name inference.")
    parser.add_argument("--device", default="cuda")
    parser.add_argument("--dtype", choices=["float16", "bfloat16", "float32"])
    parser.add_argument("--max-candidates", type=int, default=6)
    parser.add_argument("--max-history-turns", type=int, default=8)
    parser.add_argument("--candidate-encoder-heads", type=int, default=4)
    parser.add_argument("--include-candidate-score", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--include-candidate-display-name-in-qwen", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--include-nearby-notifications-in-qwen", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--zero-semantic-scalars", action="store_true")
    parser.add_argument("--zero-semantic-vectors", action="store_true")
    parser.add_argument("--limit", type=int, help="Optional limit applied to the newest N cached messages before replay.")
    parser.add_argument("--output", help="Optional JSON path for the replay summary.")
    parser.add_argument("--failure-samples-out", help="Optional JSON dataset path for failure samples.")
    return parser.parse_args()


def json_default(value: Any) -> Any:
    if isinstance(value, set):
        return sorted(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")


def main() -> None:
    args = parse_args()
    db_path = Path(args.db).expanduser().resolve()
    messages, _conversations, notifications = load_cache(db_path)
    if args.limit and args.limit > 0:
        messages = sorted(
            messages,
            key=lambda item: (item.sort_utc or datetime.min.replace(tzinfo=timezone.utc), item.message_key),
        )[-args.limit :]

    runtime = ThreadChooserRuntime(
        checkpoint_path=Path(args.checkpoint),
        model_name=args.model_name,
        semantic_cache=args.semantic_cache,
        device=args.device,
        dtype=args.dtype,
        max_history_turns=args.max_history_turns,
        include_candidate_score=args.include_candidate_score,
        include_candidate_display_name_in_qwen=args.include_candidate_display_name_in_qwen,
        include_nearby_notifications_in_qwen=args.include_nearby_notifications_in_qwen,
        zero_semantic_scalars=args.zero_semantic_scalars,
        zero_semantic_vectors=args.zero_semantic_vectors,
        candidate_encoder_heads=args.candidate_encoder_heads,
    )

    result = replay_messages(
        messages,
        notifications,
        max_candidates=args.max_candidates,
        max_history_turns=args.max_history_turns,
        score_fn=runtime.score_payload,
    )
    result["checkpoint"] = str(Path(args.checkpoint).resolve())
    result["db_path"] = str(db_path)
    result["runtime"] = runtime.health_payload()
    print(json.dumps(result, indent=2, default=json_default))

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, indent=2, default=json_default), encoding="utf-8")

    if args.failure_samples_out:
        samples = [failure["sample"] for failure in result["failures"]]
        payload = {
            "meta": {
                "source": "runtime_replay_failure_samples",
                "checkpoint": str(Path(args.checkpoint).resolve()),
                "db_path": str(db_path),
                "count": len(samples),
            },
            "samples": samples,
        }
        failure_path = Path(args.failure_samples_out)
        failure_path.parent.mkdir(parents=True, exist_ok=True)
        failure_path.write_text(json.dumps(payload, indent=2, default=json_default), encoding="utf-8")


if __name__ == "__main__":
    main()
