#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import torch

REPO_ROOT = Path(__file__).resolve().parents[1]
TRAINING_DIR = REPO_ROOT / "training"
if str(TRAINING_DIR) not in sys.path:
    sys.path.insert(0, str(TRAINING_DIR))

from thread_fused_stack import (  # type: ignore
    FrozenCausalSemanticFeaturizer,
    FusedThreadChooser,
    SemanticConfig,
    build_candidate_example,
    checkpoint_runtime_config,
    explicit_counterparty_keys,
    infer_head_config,
    parse_iso_utc,
)
from thread_scoring_sidecar import resolve_model_name  # type: ignore


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

MESSAGES_APPS = {"com.apple.MobileSMS", "com.apple.MobileSMS.notification"}


@dataclass
class CacheMessage:
    device_id: str
    message_key: str
    conversation_id: str
    conversation_display_name: str
    is_group: bool
    sort_utc: Optional[datetime]
    payload: Dict[str, Any]

    @property
    def participants(self) -> List[Dict[str, Any]]:
        participants = self.payload.get("participants", [])
        return participants if isinstance(participants, list) else []

    @property
    def message(self) -> Dict[str, Any]:
        message = self.payload.get("message", {})
        return message if isinstance(message, dict) else {}


@dataclass
class AssignedMessage:
    message_key: str
    conversation_id: str
    conversation_display_name: str
    is_group: bool
    sort_utc: Optional[datetime]
    participants: List[Dict[str, Any]]
    message: Dict[str, Any]
    original_conversation_id: str


@dataclass
class CacheNotification:
    device_id: str
    updated_utc: Optional[datetime]
    notification_uid: int
    app_identifier: str
    title: Optional[str]
    subtitle: Optional[str]
    message: Optional[str]
    received_utc: Optional[datetime]
    payload: Dict[str, Any]


class RuntimeBucket:
    def __init__(
        self,
        conversation_id: str,
        display_name: str,
        is_group: bool,
        seed_participants: Sequence[Dict[str, Any]],
    ) -> None:
        self.conversation_id = conversation_id
        self.display_name = display_name or ""
        self.is_group = bool(is_group)
        self._participants: Dict[str, Dict[str, Any]] = {}
        self._messages: List[AssignedMessage] = []
        for participant in seed_participants:
            key = str(participant.get("key") or "").strip()
            if key:
                self._participants[key] = canonicalize_participant(participant)

    @property
    def participants(self) -> List[Dict[str, Any]]:
        return sorted(
            self._participants.values(),
            key=lambda participant: (
                normalize_text(participant.get("displayName")),
                normalize_text(participant.get("key")),
            ),
        )

    def add_message(self, message: AssignedMessage) -> None:
        self._messages.append(message)
        for participant in message.participants:
            key = str(participant.get("key") or "").strip()
            if not key:
                continue
            candidate = canonicalize_participant(participant)
            current = self._participants.get(key)
            if current is None:
                self._participants[key] = candidate
                continue
            self._participants[key] = merge_participants(current, candidate)
        if message.is_group:
            self.is_group = True
        if not self.display_name or (
            message.conversation_display_name
            and len(message.conversation_display_name) > len(self.display_name)
        ):
            self.display_name = message.conversation_display_name

    def build_history(self, max_turns: int) -> List[Dict[str, Any]]:
        ordered = sorted(
            self._messages,
            key=lambda item: (item.sort_utc or datetime.min.replace(tzinfo=timezone.utc), item.message_key),
        )
        result = []
        for message in ordered[-max_turns:]:
            result.append(
                {
                    "message_key": message.message_key,
                    "sort_utc": message.sort_utc.isoformat() if message.sort_utc is not None else None,
                    "preview": build_preview_from_payload(message.message),
                    "sender_name": message.message.get("senderName"),
                    "sender_addressing": message.message.get("senderAddressing"),
                    "body": message.message.get("body"),
                    "folder": message.message.get("folder") or "",
                    "status": message.message.get("status"),
                }
            )
        return result


class ReplayScorer:
    def __init__(
        self,
        *,
        checkpoint_path: Path,
        device: str,
        dtype: Optional[str],
        model_name: Optional[str],
        semantic_cache: Optional[str],
        max_history_turns: int,
        include_candidate_score: Optional[bool],
        include_candidate_display_name_in_qwen: Optional[bool],
        include_nearby_notifications_in_qwen: Optional[bool],
        zero_structural: bool,
        zero_semantic_scalars: bool,
        zero_semantic_vectors: bool,
    ) -> None:
        self.checkpoint_path = checkpoint_path.resolve()
        self.checkpoint = torch.load(self.checkpoint_path, map_location="cpu", weights_only=False)
        self.runtime_cfg = checkpoint_runtime_config(self.checkpoint)
        self.device = device
        self.dtype = dtype or ("float16" if device.startswith("cuda") else "float32")
        self.max_history_turns = max_history_turns
        self.include_candidate_score = (
            bool(include_candidate_score)
            if include_candidate_score is not None
            else bool(self.runtime_cfg.get("include_candidate_score", False))
        )
        self.include_candidate_display_name_in_qwen = (
            bool(include_candidate_display_name_in_qwen)
            if include_candidate_display_name_in_qwen is not None
            else bool(self.runtime_cfg.get("include_candidate_display_name_in_qwen", False))
        )
        self.include_nearby_notifications_in_qwen = (
            bool(include_nearby_notifications_in_qwen)
            if include_nearby_notifications_in_qwen is not None
            else bool(self.runtime_cfg.get("include_nearby_notifications_in_qwen", False))
        )
        self.zero_structural = zero_structural
        self.zero_semantic_scalars = zero_semantic_scalars
        self.zero_semantic_vectors = zero_semantic_vectors

        state_dict = self.checkpoint["state_dict"]
        self.struct_dim = int(state_dict["struct_proj.1.weight"].shape[1])
        self.semantic_scalar_dim = int(state_dict["semantic_scalar_proj.1.weight"].shape[1])
        self.semantic_vec_dim = int(state_dict["semantic_vec_proj.1.weight"].shape[1])
        cfg = infer_head_config(
            self.checkpoint,
            struct_dim=self.struct_dim,
            semantic_scalar_dim=self.semantic_scalar_dim,
            semantic_vec_dim=self.semantic_vec_dim,
        )
        self.model = FusedThreadChooser(cfg)
        self.model.load_state_dict(state_dict)
        self.model.to(self.device)
        self.model.eval()

        self.model_name: Optional[str] = None
        self.semantic_cache_path: Optional[Path] = None
        self.featurizer: Optional[FrozenCausalSemanticFeaturizer] = None
        if not (self.zero_semantic_scalars and self.zero_semantic_vectors):
            self.model_name, self.semantic_cache_path = resolve_model_name(
                self.checkpoint_path,
                self.checkpoint,
                model_name,
                semantic_cache,
            )
            self.featurizer = FrozenCausalSemanticFeaturizer(
                SemanticConfig(
                    model_name=self.model_name,
                    device=self.device,
                    dtype=self.dtype,
                    use_hidden_states=True,
                    use_mid_layer=True,
                    cache_batch_size=8,
                    max_length=1024,
                )
            )
            if self.featurizer.hidden_size != self.semantic_vec_dim:
                raise ValueError(
                    f"Frozen model hidden size {self.featurizer.hidden_size} does not match checkpoint semantic_vec_dim {self.semantic_vec_dim}"
                )

    def score(self, sample: Dict[str, Any]) -> Dict[str, Any]:
        example = build_candidate_example(
            sample,
            include_candidate_score=self.include_candidate_score,
            max_history_turns=self.max_history_turns,
            include_candidate_display_name_in_qwen=self.include_candidate_display_name_in_qwen,
            include_nearby_notifications_in_qwen=self.include_nearby_notifications_in_qwen,
        )

        struct = np.asarray(example.candidate_struct, dtype=np.float32)
        if struct.ndim != 2 or struct.shape[0] == 0:
            raise ValueError("sample must produce at least one candidate row")
        if struct.shape[1] < self.struct_dim:
            raise ValueError(
                f"structural feature dimension mismatch: sample produced {struct.shape[1]}, checkpoint expects {self.struct_dim}"
            )
        if struct.shape[1] > self.struct_dim:
            struct = struct[:, : self.struct_dim]
        if self.zero_structural:
            struct = np.zeros_like(struct)

        if self.featurizer is None:
            semantic_scalars = np.zeros((struct.shape[0], self.semantic_scalar_dim), dtype=np.float32)
            semantic_vecs = np.zeros((struct.shape[0], self.semantic_vec_dim), dtype=np.float16)
        else:
            semantic_scalars, semantic_vecs = self.featurizer.featurize_example(example)

        semantic_scalars = np.asarray(semantic_scalars, dtype=np.float32)
        semantic_vecs = np.asarray(semantic_vecs, dtype=np.float16)
        if semantic_scalars.shape[1] != self.semantic_scalar_dim:
            raise ValueError(
                f"semantic scalar dimension mismatch: sample produced {semantic_scalars.shape[1]}, checkpoint expects {self.semantic_scalar_dim}"
            )
        if semantic_vecs.shape[1] != self.semantic_vec_dim:
            raise ValueError(
                f"semantic vector dimension mismatch: sample produced {semantic_vecs.shape[1]}, checkpoint expects {self.semantic_vec_dim}"
            )
        if self.zero_semantic_scalars:
            semantic_scalars = np.zeros_like(semantic_scalars)
        if self.zero_semantic_vectors:
            semantic_vecs = np.zeros_like(semantic_vecs)

        with torch.inference_mode():
            struct_t = torch.from_numpy(struct).unsqueeze(0).to(self.device, dtype=torch.float32)
            semantic_scalars_t = torch.from_numpy(semantic_scalars).unsqueeze(0).to(self.device, dtype=torch.float32)
            semantic_vecs_t = torch.from_numpy(semantic_vecs).unsqueeze(0).to(self.device, dtype=torch.float32)
            mask_t = torch.ones((1, struct.shape[0]), dtype=torch.bool, device=self.device)
            logits = self.model(struct_t, semantic_scalars_t, semantic_vecs_t, mask_t)[0]
            probs = torch.softmax(logits, dim=-1)

        logits_list = [float(value) for value in logits.detach().cpu().tolist()]
        probs_list = [float(value) for value in probs.detach().cpu().tolist()]
        predicted_index = int(torch.argmax(probs).item())
        candidates = []
        semantic_names = ["cond_nll", "base_nll", "lift", "semantic_ok", "target_len_log1p"]
        for idx, candidate in enumerate(sample["candidate_threads"]):
            candidates.append(
                {
                    "index": idx,
                    "thread_id": candidate["thread_id"],
                    "display_name": candidate.get("display_name") or "",
                    "is_group": bool(candidate.get("is_group")),
                    "logit": logits_list[idx],
                    "probability": probs_list[idx],
                    "semantic": {
                        name: float(semantic_scalars[idx, pos])
                        for pos, name in enumerate(semantic_names)
                    },
                }
            )

        return {
            "sample_id": sample["sample_id"],
            "candidate_count": len(candidates),
            "predicted_index": predicted_index,
            "predicted_thread_id": candidates[predicted_index]["thread_id"],
            "scores": logits_list,
            "probabilities": probs_list,
            "candidates": candidates,
        }


def parse_utc(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    parsed = parse_iso_utc(value)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_text(value: Optional[str]) -> str:
    return " ".join((value or "").lower().split())


def tokenize(value: Optional[str]) -> List[str]:
    normalized = normalize_text(value)
    if not normalized:
        return []
    return [token for token in normalized.replace(",", " ").replace(".", " ").split(" ") if token]


def token_overlap(left: Optional[str], right: Optional[str]) -> float:
    left_tokens = set(tokenize(left))
    right_tokens = set(tokenize(right))
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def looks_generic(preview: Optional[str]) -> bool:
    normalized = normalize_text(preview)
    if not normalized:
        return True
    if normalized in GENERIC_PREVIEWS:
        return True
    return len(tokenize(normalized)) <= 2 and len(normalized) <= 16


def build_preview_from_payload(message_payload: Dict[str, Any]) -> Optional[str]:
    body = message_payload.get("body") or message_payload.get("subject")
    return build_preview(str(body) if body is not None else None)


def build_preview(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    trimmed = value.strip()
    if not trimmed:
        return None
    return trimmed if len(trimmed) <= 160 else f"{trimmed[:160]}..."


def add_variant(variants: set[str], raw: Optional[str]) -> None:
    if not raw:
        return
    normalized = normalize_text(raw)
    if normalized:
        variants.add(normalized)
    digits = "".join(ch for ch in raw if ch.isdigit())
    if digits:
        variants.add(digits)


def build_target_sender_variants(message: CacheMessage) -> set[str]:
    variants: set[str] = set()
    payload = message.message
    add_variant(variants, payload.get("senderName"))
    add_variant(variants, payload.get("senderAddressing"))
    for originator in payload.get("originators", []) or []:
        if not isinstance(originator, dict):
            continue
        add_variant(variants, originator.get("name"))
        for phone in originator.get("phones", []) or []:
            add_variant(variants, phone)
    return variants


def build_history_sender_variants(history_turn: Dict[str, Any]) -> set[str]:
    variants: set[str] = set()
    add_variant(variants, history_turn.get("sender_name"))
    add_variant(variants, history_turn.get("sender_addressing"))
    return variants


def is_near(reference: Optional[datetime], candidate: Optional[datetime], window: timedelta) -> bool:
    return reference is not None and candidate is not None and abs(reference - candidate) <= window


def canonicalize_participant(participant: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "key": str(participant.get("key") or ""),
        "displayName": str(participant.get("displayName") or ""),
        "phones": sorted(
            {str(phone) for phone in (participant.get("phones") or []) if str(phone).strip()},
            key=lambda value: value.lower(),
        ),
        "emails": sorted(
            {str(email) for email in (participant.get("emails") or []) if str(email).strip()},
            key=lambda value: value.lower(),
        ),
        "isSelf": bool(participant.get("isSelf")),
    }


def merge_participants(current: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
    current_name = str(current.get("displayName") or "")
    candidate_name = str(candidate.get("displayName") or "")
    return {
        "key": current.get("key") or candidate.get("key") or "",
        "displayName": current_name if len(current_name) >= len(candidate_name) else candidate_name,
        "phones": sorted(
            {*(current.get("phones") or []), *(candidate.get("phones") or [])},
            key=lambda value: value.lower(),
        ),
        "emails": sorted(
            {*(current.get("emails") or []), *(candidate.get("emails") or [])},
            key=lambda value: value.lower(),
        ),
        "isSelf": bool(current.get("isSelf")) or bool(candidate.get("isSelf")),
    }


def choose_device_id(messages: Sequence[CacheMessage], explicit_device_id: Optional[str]) -> str:
    if explicit_device_id:
        return explicit_device_id
    latest_by_device: Dict[str, datetime] = {}
    for message in messages:
        moment = message.sort_utc or datetime.min.replace(tzinfo=timezone.utc)
        current = latest_by_device.get(message.device_id)
        if current is None or moment > current:
            latest_by_device[message.device_id] = moment
    if not latest_by_device:
        raise ValueError("no messages were found in the cache")
    return max(latest_by_device.items(), key=lambda item: item[1])[0]


def load_cache(db_path: Path) -> tuple[List[CacheMessage], List[CacheNotification]]:
    connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    messages: List[CacheMessage] = []
    for row in cursor.execute(
        """
        SELECT device_id, message_key, conversation_id, conversation_display_name, is_group, sort_utc, json
        FROM messages
        ORDER BY sort_ticks ASC, message_key ASC
        """
    ):
        payload = json.loads(row["json"])
        messages.append(
            CacheMessage(
                device_id=row["device_id"],
                message_key=row["message_key"],
                conversation_id=row["conversation_id"],
                conversation_display_name=row["conversation_display_name"] or "",
                is_group=bool(row["is_group"]),
                sort_utc=parse_utc(row["sort_utc"]),
                payload=payload,
            )
        )

    notifications: List[CacheNotification] = []
    for row in cursor.execute(
        """
        SELECT device_id, updated_utc, notification_uid, app_identifier, title, subtitle, message, received_utc, json
        FROM notifications
        ORDER BY updated_utc DESC, notification_uid DESC
        """
    ):
        payload = json.loads(row["json"])
        notifications.append(
            CacheNotification(
                device_id=row["device_id"],
                updated_utc=parse_utc(row["updated_utc"]),
                notification_uid=int(row["notification_uid"]),
                app_identifier=row["app_identifier"] or "",
                title=row["title"],
                subtitle=row["subtitle"],
                message=row["message"],
                received_utc=parse_utc(row["received_utc"]),
                payload=payload,
            )
        )

    connection.close()
    return messages, notifications


def dedupe_notifications(notifications: Sequence[CacheNotification]) -> List[CacheNotification]:
    seen: set[str] = set()
    deduped: List[CacheNotification] = []
    for notification in notifications:
        key = "|".join(
            [
                notification.app_identifier or "",
                str(notification.notification_uid),
                notification.received_utc.isoformat() if notification.received_utc is not None else "",
                notification.title or "",
                notification.message or "",
            ]
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(notification)
    return deduped[:256]


def build_sample(
    message: CacheMessage,
    buckets: Dict[str, RuntimeBucket],
    notifications: Sequence[CacheNotification],
    *,
    max_candidates: int,
    history_turns: int,
) -> Dict[str, Any]:
    target_preview = build_preview_from_payload(message.message)
    target_participant_keys = {
        str(participant.get("key") or "")
        for participant in message.participants
        if not participant.get("isSelf") and str(participant.get("key") or "").strip()
    }
    target_sender_keys = build_target_sender_variants(message)
    candidate_rows: List[tuple[Dict[str, Any], float]] = []

    for bucket in buckets.values():
        history = bucket.build_history(history_turns)
        last_history = history[-1] if history else None
        preview_overlap = token_overlap(target_preview, last_history.get("preview") if last_history else None)
        participant_overlap = sum(
            1
            for participant in bucket.participants
            if not participant.get("isSelf") and str(participant.get("key") or "") in target_participant_keys
        )
        delta_seconds: Optional[float]
        if message.sort_utc is not None and last_history is not None:
            history_sort_utc = parse_utc(last_history.get("sort_utc"))
            delta_seconds = (
                abs((message.sort_utc - history_sort_utc).total_seconds())
                if history_sort_utc is not None
                else None
            )
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
        if bucket.is_group == message.is_group:
            score += 0.5
        if looks_generic(target_preview):
            score += 0.5
        sender_overlap = bool(last_history) and bool(build_history_sender_variants(last_history) & target_sender_keys)
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
                        "preview_overlap": round(preview_overlap, 6),
                        "delta_seconds": delta_seconds,
                    },
                },
                score,
            )
        )

    candidate_rows.sort(
        key=lambda item: (
            -item[1],
            item[0]["features"]["delta_seconds"]
            if item[0]["features"]["delta_seconds"] is not None
            else float("inf"),
        )
    )
    candidate_rows = candidate_rows[: max(1, max_candidates - 1)]

    if not any(
        candidate["thread_id"].lower() == message.conversation_id.lower()
        for candidate, _ in candidate_rows
    ):
        current_bucket = buckets.get(message.conversation_id)
        current_history = current_bucket.build_history(history_turns) if current_bucket is not None else []
        current_last = current_history[-1] if current_history else None
        current_delta = None
        if message.sort_utc is not None and current_last is not None:
            current_history_sort = parse_utc(current_last.get("sort_utc"))
            current_delta = (
                abs((message.sort_utc - current_history_sort).total_seconds())
                if current_history_sort is not None
                else None
            )
        candidate_rows.insert(
            0,
            (
                {
                    "thread_id": message.conversation_id,
                    "display_name": message.conversation_display_name,
                    "is_group": message.is_group,
                    "participants": [canonicalize_participant(participant) for participant in message.participants],
                    "history": current_history,
                    "features": {
                        "preview_overlap": round(
                            token_overlap(target_preview, current_last.get("preview") if current_last else None),
                            6,
                        ),
                        "delta_seconds": current_delta,
                    },
                },
                float("inf"),
            ),
        )

    nearby_notifications = []
    for notification in notifications:
        if notification.app_identifier not in MESSAGES_APPS:
            continue
        if not is_near(message.sort_utc, notification.received_utc, timedelta(hours=2)):
            continue
        nearby_notifications.append(
            {
                "notification_uid": notification.notification_uid,
                "received_utc": notification.received_utc.isoformat() if notification.received_utc is not None else None,
                "title": notification.title,
                "subtitle": notification.subtitle,
                "message": notification.message,
                "app_identifier": notification.app_identifier,
            }
        )
        if len(nearby_notifications) >= 3:
            break

    candidates: List[Dict[str, Any]] = []
    seen_thread_ids: set[str] = set()
    for candidate, _ in candidate_rows:
        thread_id = str(candidate["thread_id"])
        key = thread_id.lower()
        if key in seen_thread_ids:
            continue
        seen_thread_ids.add(key)
        candidates.append(candidate)
        if len(candidates) >= max_candidates:
            break

    if not candidates:
        candidates = [
            {
                "thread_id": message.conversation_id,
                "display_name": message.conversation_display_name,
                "is_group": message.is_group,
                "participants": [canonicalize_participant(participant) for participant in message.participants],
                "history": [],
                "features": {
                    "preview_overlap": 0.0,
                    "delta_seconds": None,
                },
            }
        ]

    return {
        "sample_id": f"runtime::{message.message_key}",
        "message": {
            "message_key": message.message_key,
            "sort_utc": message.sort_utc.isoformat() if message.sort_utc is not None else None,
            "conversation_display_name": message.conversation_display_name,
            "is_group": message.is_group,
            "participants": [canonicalize_participant(participant) for participant in message.participants],
            "message": {
                "folder": message.message.get("folder") or "",
                "subject": message.message.get("subject"),
                "body": message.message.get("body"),
                "senderName": message.message.get("senderName"),
                "senderAddressing": message.message.get("senderAddressing"),
                "originators": message.message.get("originators", []) or [],
                "recipients": message.message.get("recipients", []) or [],
            },
        },
        "candidate_threads": candidates,
        "nearby_notifications": nearby_notifications,
        "metadata": {
            "preview": target_preview,
            "generic_preview": looks_generic(target_preview),
        },
        "fallback_thread_id": message.conversation_id,
        "gold_thread_id": message.conversation_id,
    }


def assign_message(
    message: CacheMessage,
    sample: Dict[str, Any],
    chosen_thread_id: Optional[str],
    buckets: Dict[str, RuntimeBucket],
) -> AssignedMessage:
    candidate_ids = {str(candidate["thread_id"]).lower() for candidate in sample["candidate_threads"]}
    selected_thread_id = (
        chosen_thread_id
        if chosen_thread_id and chosen_thread_id.lower() in candidate_ids
        else str(sample["fallback_thread_id"])
    )
    bucket = buckets.get(selected_thread_id)
    if bucket is None:
        template = next(
            candidate
            for candidate in sample["candidate_threads"]
            if str(candidate["thread_id"]).lower() == selected_thread_id.lower()
        )
        bucket = RuntimeBucket(
            conversation_id=str(template["thread_id"]),
            display_name=str(template.get("display_name") or ""),
            is_group=bool(template.get("is_group")),
            seed_participants=template.get("participants", []),
        )
        buckets[selected_thread_id] = bucket

    assigned = AssignedMessage(
        message_key=message.message_key,
        conversation_id=bucket.conversation_id,
        conversation_display_name=bucket.display_name,
        is_group=bucket.is_group,
        sort_utc=message.sort_utc,
        participants=[canonicalize_participant(participant) for participant in message.participants],
        message=dict(message.message),
        original_conversation_id=message.conversation_id,
    )
    bucket.add_message(assigned)
    return assigned


def load_focus_message_keys(path: Optional[Path]) -> set[str]:
    if path is None:
        return set()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "samples" in payload:
        payload = payload["samples"]
    if not isinstance(payload, list):
        raise ValueError("focus pack must be a JSON list or an object with a 'samples' list")
    keys: set[str] = set()
    for item in payload:
        if not isinstance(item, dict):
            continue
        message = item.get("message")
        if isinstance(message, dict):
            key = message.get("message_key")
            if key:
                keys.add(str(key))
    return keys


def compute_metric_summary(decisions: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(decisions)
    scored = sum(1 for decision in decisions if decision["scored"])
    correct = sum(1 for decision in decisions if decision["correct"])
    scored_correct = sum(1 for decision in decisions if decision["scored"] and decision["correct"])
    reranked = sum(1 for decision in decisions if decision["reranked"])
    direct_total = sum(1 for decision in decisions if not decision["target_is_group"])
    direct_correct = sum(1 for decision in decisions if not decision["target_is_group"] and decision["correct"])
    sent_total = sum(1 for decision in decisions if decision["target_is_outbound"])
    sent_correct = sum(1 for decision in decisions if decision["target_is_outbound"] and decision["correct"])
    overgroup_errors = sum(
        1
        for decision in decisions
        if (not decision["correct"]) and (not decision["target_is_group"]) and decision["predicted_is_group"]
    )
    undergroup_errors = sum(
        1
        for decision in decisions
        if (not decision["correct"]) and decision["target_is_group"] and (not decision["predicted_is_group"])
    )
    direct_group_mass_values = [
        decision["direct_group_mass"]
        for decision in decisions
        if (not decision["target_is_group"]) and decision["scored"]
    ]
    direct_identity_rows = [
        decision
        for decision in decisions
        if (not decision["target_is_group"]) and decision["target_has_explicit_identity"] and decision["gold_identity_match"]
    ]
    sent_identity_rows = [
        decision
        for decision in decisions
        if decision["target_is_outbound"] and decision["target_has_explicit_identity"] and decision["gold_identity_match"]
    ]

    def frac(numerator: int, denominator: int) -> float:
        return float(numerator / denominator) if denominator else 0.0

    return {
        "n": total,
        "scored_n": scored,
        "top1": frac(correct, total),
        "scored_top1": frac(scored_correct, scored),
        "reranked_messages": reranked,
        "direct_top1": frac(direct_correct, direct_total),
        "sent_top1": frac(sent_correct, sent_total),
        "direct_identity_match_top1": frac(
            sum(1 for decision in direct_identity_rows if decision["pred_identity_match"]),
            len(direct_identity_rows),
        ),
        "sent_identity_match_top1": frac(
            sum(1 for decision in sent_identity_rows if decision["pred_identity_match"]),
            len(sent_identity_rows),
        ),
        "direct_wrong_counterparty_errors": sum(
            1 for decision in direct_identity_rows if not decision["pred_identity_match"]
        ),
        "sent_wrong_counterparty_errors": sum(
            1 for decision in sent_identity_rows if not decision["pred_identity_match"]
        ),
        "overgroup_errors": overgroup_errors,
        "undergroup_errors": undergroup_errors,
        "direct_group_mass": (
            float(sum(direct_group_mass_values) / len(direct_group_mass_values))
            if direct_group_mass_values
            else 0.0
        ),
    }


def run_replay(args: argparse.Namespace) -> Dict[str, Any]:
    messages, notifications = load_cache(Path(args.db))
    device_id = choose_device_id(messages, args.device_id)
    filtered_messages = [message for message in messages if message.device_id == device_id]
    filtered_notifications = dedupe_notifications(
        [notification for notification in notifications if notification.device_id == device_id]
    )
    if args.limit_messages and args.limit_messages > 0:
        filtered_messages = filtered_messages[-args.limit_messages :]

    focus_message_keys = load_focus_message_keys(Path(args.focus_pack)) if args.focus_pack else set()
    scorer: Optional[ReplayScorer] = None
    if args.policy == "checkpoint":
        if not args.checkpoint:
            raise ValueError("--checkpoint is required when --policy=checkpoint")
        scorer = ReplayScorer(
            checkpoint_path=Path(args.checkpoint),
            device=args.device,
            dtype=args.dtype,
            model_name=args.model_name,
            semantic_cache=args.semantic_cache,
            max_history_turns=args.history_turns,
            include_candidate_score=args.include_candidate_score,
            include_candidate_display_name_in_qwen=args.include_candidate_display_name_in_qwen,
            include_nearby_notifications_in_qwen=args.include_nearby_notifications_in_qwen,
            zero_structural=args.zero_structural,
            zero_semantic_scalars=args.zero_semantic_scalars,
            zero_semantic_vectors=args.zero_semantic_vectors,
        )

    buckets: Dict[str, RuntimeBucket] = {}
    decisions: List[Dict[str, Any]] = []
    ordered_messages = sorted(
        filtered_messages,
        key=lambda item: (item.sort_utc or datetime.min.replace(tzinfo=timezone.utc), item.message_key),
    )
    for index, message in enumerate(ordered_messages):
        sample = build_sample(
            message,
            buckets,
            filtered_notifications,
            max_candidates=args.max_candidates,
            history_turns=args.history_turns,
        )
        score: Optional[Dict[str, Any]] = None
        if scorer is not None and len(sample["candidate_threads"]) >= 2:
            score = scorer.score(sample)
        chosen_thread_id = score["predicted_thread_id"] if score is not None else None
        assigned = assign_message(message, sample, chosen_thread_id, buckets)

        target_participants = sample["message"]["participants"]
        target_identity_keys = set(explicit_counterparty_keys(target_participants))
        gold_candidate = next(
            candidate
            for candidate in sample["candidate_threads"]
            if candidate["thread_id"] == sample["gold_thread_id"]
        )
        predicted_candidate = next(
            candidate
            for candidate in sample["candidate_threads"]
            if candidate["thread_id"] == assigned.conversation_id
        )
        gold_identity_keys = set(explicit_counterparty_keys(gold_candidate.get("participants", [])))
        predicted_identity_keys = set(explicit_counterparty_keys(predicted_candidate.get("participants", [])))
        probabilities = score["probabilities"] if score is not None else [1.0]
        predicted_index = score["predicted_index"] if score is not None else 0
        direct_group_mass = (
            sum(
                probability
                for probability, candidate in zip(probabilities, sample["candidate_threads"])
                if candidate.get("is_group")
            )
            if score is not None
            else float(bool(predicted_candidate.get("is_group")))
        )

        decisions.append(
            {
                "index": index,
                "message_key": message.message_key,
                "sort_utc": message.sort_utc.isoformat() if message.sort_utc is not None else None,
                "preview": sample["metadata"]["preview"],
                "target_is_group": bool(message.is_group),
                "target_is_outbound": normalize_text(message.message.get("folder")) == "sent",
                "target_has_explicit_identity": bool(target_identity_keys),
                "gold_thread_id": message.conversation_id,
                "predicted_thread_id": assigned.conversation_id,
                "correct": assigned.conversation_id == message.conversation_id,
                "reranked": assigned.conversation_id != message.conversation_id,
                "scored": score is not None,
                "candidate_count": len(sample["candidate_threads"]),
                "predicted_is_group": bool(predicted_candidate.get("is_group")),
                "gold_is_group": bool(gold_candidate.get("is_group")),
                "gold_identity_match": bool(target_identity_keys & gold_identity_keys),
                "pred_identity_match": bool(target_identity_keys & predicted_identity_keys),
                "predicted_probability": probabilities[predicted_index],
                "direct_group_mass": direct_group_mass,
                "focus": message.message_key in focus_message_keys,
                "top_candidates": (
                    sorted(score["candidates"], key=lambda candidate: candidate["probability"], reverse=True)[:3]
                    if score is not None
                    else [
                        {
                            "index": 0,
                            "thread_id": assigned.conversation_id,
                            "display_name": predicted_candidate.get("display_name") or "",
                            "is_group": bool(predicted_candidate.get("is_group")),
                            "probability": 1.0,
                            "logit": 0.0,
                        }
                    ]
                ),
            }
        )

    focus_decisions = [decision for decision in decisions if decision["focus"]]
    errors = [decision for decision in decisions if not decision["correct"]]
    report = {
        "policy": args.policy,
        "db": str(Path(args.db).resolve()),
        "device_id": device_id,
        "checkpoint": str(Path(args.checkpoint).resolve()) if args.checkpoint else None,
        "model_name": scorer.model_name if scorer is not None else None,
        "semantic_cache": str(scorer.semantic_cache_path) if scorer is not None and scorer.semantic_cache_path else None,
        "device": args.device,
        "dtype": scorer.dtype if scorer is not None else args.dtype,
        "history_turns": args.history_turns,
        "max_candidates": args.max_candidates,
        "messages_total": len(filtered_messages),
        "notifications_total": len(filtered_notifications),
        "metrics": compute_metric_summary(decisions),
        "focus_metrics": compute_metric_summary(focus_decisions),
        "focus_message_count": len(focus_message_keys),
        "focus_messages_seen": len(focus_decisions),
        "error_count": len(errors),
        "first_error": errors[0] if errors else None,
        "errors": errors[: args.error_limit],
    }
    if args.trace_limit > 0:
        report["trace"] = decisions[: args.trace_limit]
    return report


def build_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sequential runtime replay for the learned thread chooser against the local Adit cache."
    )
    parser.add_argument("--db", required=True, help="Path to the local adit.db cache")
    parser.add_argument("--policy", choices=["fallback", "checkpoint"], default="checkpoint")
    parser.add_argument("--checkpoint")
    parser.add_argument("--model-name")
    parser.add_argument("--semantic-cache")
    parser.add_argument("--device", default="cuda" if torch.cuda.is_available() else "cpu")
    parser.add_argument("--dtype", choices=["float16", "bfloat16", "float32"])
    parser.add_argument("--device-id")
    parser.add_argument("--history-turns", type=int, default=8)
    parser.add_argument("--max-candidates", type=int, default=6)
    parser.add_argument("--limit-messages", type=int)
    parser.add_argument("--focus-pack")
    parser.add_argument("--out")
    parser.add_argument("--error-limit", type=int, default=20)
    parser.add_argument("--trace-limit", type=int, default=0)
    parser.add_argument("--include-candidate-score", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--include-candidate-display-name-in-qwen", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--include-nearby-notifications-in-qwen", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--zero-structural", action="store_true")
    parser.add_argument("--zero-semantic-scalars", action="store_true")
    parser.add_argument("--zero-semantic-vectors", action="store_true")
    return parser


def main() -> None:
    args = build_argparser().parse_args()
    report = run_replay(args)
    payload = json.dumps(report, indent=2)
    if args.out:
        Path(args.out).write_text(payload + "\n", encoding="utf-8")
    print(payload)


if __name__ == "__main__":
    main()
