from __future__ import annotations

import hashlib
import json
import random
from collections import Counter
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Sequence


SELF_SENDER_NAME = "Me"
SELF_SENDER_ADDRESSING = "name:Me"
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
MAX_HISTORY_MESSAGES = 8
STYLE_EMOJI_SUFFIXES = ["", "", "", "!", "!!", " haha", " lol", " :)"]
GENERIC_COLLISION_REPLIES = ["ok", "kk", "yup", "sounds good", "teehee", "true", "bet"]


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(str(value).lower().split())


def digits_only(value: str | None) -> str:
    if not value:
        return ""
    return "".join(character for character in str(value) if character.isdigit())


def slugify_name(value: str | None) -> str:
    normalized = normalize_text(value)
    slug = "".join(character if character.isalnum() else "." for character in normalized)
    slug = slug.strip(".")
    while ".." in slug:
        slug = slug.replace("..", ".")
    return slug


def _stable_identity_seed(participant_id: str, participant: dict[str, Any]) -> str:
    return str(participant.get("_identity_seed") or f"{participant_id}:{participant.get('display_name', '')}")


def _stable_int(seed: str) -> int:
    return int(hashlib.sha1(seed.encode("utf-8")).hexdigest(), 16)


def _looks_shortcode_like_name(value: str | None) -> bool:
    compact = normalize_text(value).replace(" ", "")
    digits = digits_only(compact)
    return 4 <= len(digits) <= 6 and digits == compact


def _participant_phone(
    participant_id: str,
    participants_by_id: dict[str, dict[str, Any]],
) -> str | None:
    participant = participants_by_id[participant_id]
    display_name = str(participant.get("display_name", ""))
    if _looks_shortcode_like_name(display_name):
        return f"+{digits_only(display_name)}"

    seed = _stable_int(_stable_identity_seed(participant_id, participant))
    area_codes = ("650", "845", "917", "929", "415", "718", "510", "408")
    area = area_codes[seed % len(area_codes)]
    exchange = 200 + ((seed // len(area_codes)) % 700)
    line = 1000 + ((seed // (len(area_codes) * 700)) % 9000)
    return f"+1{area}{exchange:03d}{line:04d}"


def _participant_email(
    participant_id: str,
    participants_by_id: dict[str, dict[str, Any]],
) -> str | None:
    participant = participants_by_id[participant_id]
    display_name = str(participant.get("display_name", ""))
    if _looks_shortcode_like_name(display_name):
        return None

    seed = _stable_int(_stable_identity_seed(participant_id, participant))
    if seed % 6 != 0:
        return None

    slug = slugify_name(display_name) or f"contact{seed % 1000}"
    suffix = seed % 97
    domain = "mail.test" if seed % 2 == 0 else "example.test"
    return f"{slug}{suffix}@{domain}"


def _participant_contact_fields(
    participant_id: str,
    participants_by_id: dict[str, dict[str, Any]],
    *,
    is_self: bool,
) -> dict[str, Any]:
    if is_self:
        return {
            "key": "self:me",
            "displayName": SELF_SENDER_NAME,
            "phones": [],
            "emails": [],
            "isSelf": True,
        }

    participant = participants_by_id[participant_id]
    cached = participant.get("_contact_fields")
    if cached:
        return dict(cached)

    phone = _participant_phone(participant_id, participants_by_id)
    email = _participant_email(participant_id, participants_by_id)
    if phone:
        key = f"phone:{phone}"
        phones = [phone]
        emails: list[str] = []
    elif email:
        key = f"email:{email}"
        phones = []
        emails = [email]
    else:
        key = f"name:{normalize_text(participant.get('display_name'))}"
        phones = []
        emails = []

    fields = {
        "key": key,
        "displayName": participant["display_name"],
        "phones": phones,
        "emails": emails,
        "isSelf": False,
    }
    participant["_contact_fields"] = dict(fields)
    return fields


def _participant_primary_address(
    participant_id: str,
    participants_by_id: dict[str, dict[str, Any]],
    *,
    is_self: bool = False,
) -> str:
    if is_self:
        return SELF_SENDER_ADDRESSING
    fields = _participant_contact_fields(participant_id, participants_by_id, is_self=False)
    if fields["phones"]:
        return str(fields["phones"][0])
    if fields["emails"]:
        return str(fields["emails"][0])
    return str(fields["displayName"])


def _participant_party(
    participant_id: str,
    participants_by_id: dict[str, dict[str, Any]],
    *,
    is_self: bool = False,
) -> dict[str, Any]:
    if is_self:
        return {
            "name": SELF_SENDER_NAME,
            "phones": [],
            "emails": [],
        }
    fields = _participant_contact_fields(participant_id, participants_by_id, is_self=False)
    return {
        "name": fields["displayName"],
        "phones": list(fields["phones"]),
        "emails": list(fields["emails"]),
    }


def tokenize(value: str | None) -> list[str]:
    return [
        token
        for token in (
            normalize_text(value)
            .replace(",", " ")
            .replace(".", " ")
            .replace("!", " ")
            .replace("?", " ")
            .replace(":", " ")
            .replace(";", " ")
            .replace('"', " ")
            .split(" ")
        )
        if token
    ]


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
    return len(tokens) <= 2 and len(normalized) <= 18


def truncate_for_notification(text: str, *, max_len: int) -> str:
    text = " ".join(str(text).split())
    if len(text) <= max_len:
        return text
    return text[: max_len - 3].rstrip() + "..."


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (len(ordered) - 1) * p
    lower = int(rank)
    upper = min(len(ordered) - 1, lower + 1)
    if lower == upper:
        return float(ordered[lower])
    fraction = rank - lower
    return float(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction)


@dataclass(frozen=True)
class CorruptionProfile:
    name: str
    drop_sender_name: bool
    drop_sender_addressing: bool
    blank_body: bool
    use_subject_only: bool
    ancs_truncate: bool
    descriptor_truncated: bool
    whole_hour_skew: bool
    notification_present_prob: float
    notification_stale_prob: float
    jitter_seconds: int
    history_blank_prob: float
    history_sender_sparse_prob: float


@dataclass(frozen=True)
class DifficultyRecipe:
    name: str
    same_sender_distractors: int
    alias_negative_count: int
    force_stale_gold: bool
    force_recent_wrong: bool
    force_text_collision: bool
    min_group_negatives: int
    max_group_negatives: int


DIFFICULTY_RECIPES: dict[str, DifficultyRecipe] = {
    "baseline": DifficultyRecipe(
        name="baseline",
        same_sender_distractors=0,
        alias_negative_count=0,
        force_stale_gold=False,
        force_recent_wrong=False,
        force_text_collision=False,
        min_group_negatives=0,
        max_group_negatives=1,
    ),
    "same_sender_semantic": DifficultyRecipe(
        name="same_sender_semantic",
        same_sender_distractors=2,
        alias_negative_count=1,
        force_stale_gold=False,
        force_recent_wrong=True,
        force_text_collision=True,
        min_group_negatives=0,
        max_group_negatives=1,
    ),
    "stale_semantic": DifficultyRecipe(
        name="stale_semantic",
        same_sender_distractors=1,
        alias_negative_count=1,
        force_stale_gold=True,
        force_recent_wrong=True,
        force_text_collision=True,
        min_group_negatives=0,
        max_group_negatives=1,
    ),
    "topic_semantic": DifficultyRecipe(
        name="topic_semantic",
        same_sender_distractors=2,
        alias_negative_count=1,
        force_stale_gold=True,
        force_recent_wrong=True,
        force_text_collision=False,
        min_group_negatives=0,
        max_group_negatives=1,
    ),
    "group_overlap_semantic": DifficultyRecipe(
        name="group_overlap_semantic",
        same_sender_distractors=1,
        alias_negative_count=1,
        force_stale_gold=False,
        force_recent_wrong=True,
        force_text_collision=True,
        min_group_negatives=2,
        max_group_negatives=3,
    ),
    "service_collision": DifficultyRecipe(
        name="service_collision",
        same_sender_distractors=1,
        alias_negative_count=1,
        force_stale_gold=True,
        force_recent_wrong=True,
        force_text_collision=True,
        min_group_negatives=0,
        max_group_negatives=1,
    ),
    "outbound_sparse": DifficultyRecipe(
        name="outbound_sparse",
        same_sender_distractors=1,
        alias_negative_count=1,
        force_stale_gold=True,
        force_recent_wrong=True,
        force_text_collision=False,
        min_group_negatives=0,
        max_group_negatives=1,
    ),
}


CORRUPTION_PROFILES = [
    CorruptionProfile(
        name="cleanish",
        drop_sender_name=False,
        drop_sender_addressing=False,
        blank_body=False,
        use_subject_only=False,
        ancs_truncate=False,
        descriptor_truncated=False,
        whole_hour_skew=False,
        notification_present_prob=0.55,
        notification_stale_prob=0.05,
        jitter_seconds=8,
        history_blank_prob=0.04,
        history_sender_sparse_prob=0.04,
    ),
    CorruptionProfile(
        name="map_sparse_sender",
        drop_sender_name=True,
        drop_sender_addressing=False,
        blank_body=False,
        use_subject_only=False,
        ancs_truncate=False,
        descriptor_truncated=False,
        whole_hour_skew=False,
        notification_present_prob=0.5,
        notification_stale_prob=0.08,
        jitter_seconds=12,
        history_blank_prob=0.08,
        history_sender_sparse_prob=0.16,
    ),
    CorruptionProfile(
        name="map_sparse_body",
        drop_sender_name=False,
        drop_sender_addressing=True,
        blank_body=True,
        use_subject_only=True,
        ancs_truncate=True,
        descriptor_truncated=False,
        whole_hour_skew=False,
        notification_present_prob=0.45,
        notification_stale_prob=0.08,
        jitter_seconds=16,
        history_blank_prob=0.14,
        history_sender_sparse_prob=0.08,
    ),
    CorruptionProfile(
        name="descriptor_truncated",
        drop_sender_name=False,
        drop_sender_addressing=False,
        blank_body=False,
        use_subject_only=False,
        ancs_truncate=True,
        descriptor_truncated=True,
        whole_hour_skew=False,
        notification_present_prob=0.5,
        notification_stale_prob=0.08,
        jitter_seconds=18,
        history_blank_prob=0.06,
        history_sender_sparse_prob=0.06,
    ),
    CorruptionProfile(
        name="whole_hour_skew",
        drop_sender_name=False,
        drop_sender_addressing=False,
        blank_body=False,
        use_subject_only=False,
        ancs_truncate=False,
        descriptor_truncated=False,
        whole_hour_skew=True,
        notification_present_prob=0.48,
        notification_stale_prob=0.05,
        jitter_seconds=20,
        history_blank_prob=0.05,
        history_sender_sparse_prob=0.06,
    ),
    CorruptionProfile(
        name="reactionish_sparse",
        drop_sender_name=True,
        drop_sender_addressing=True,
        blank_body=True,
        use_subject_only=True,
        ancs_truncate=True,
        descriptor_truncated=False,
        whole_hour_skew=False,
        notification_present_prob=0.38,
        notification_stale_prob=0.18,
        jitter_seconds=24,
        history_blank_prob=0.22,
        history_sender_sparse_prob=0.18,
    ),
]


def _participant_lookup(window: dict[str, Any]) -> dict[str, dict[str, Any]]:
    window_id = str(window.get("window_id") or window.get("id") or "window")
    return {
        participant["id"]: {
            **participant,
            "_identity_seed": f"{window_id}:{participant['id']}",
        }
        for participant in window["participants"]
    }


def _self_participant_id(window: dict[str, Any], participants_by_id: dict[str, dict[str, Any]]) -> str:
    self_id = str(window.get("self_participant_id", "")).strip()
    if self_id and self_id in participants_by_id:
        return self_id
    coverage: dict[str, int] = {}
    for thread in window["threads"]:
        for participant_id in thread["participant_ids"]:
            coverage[participant_id] = coverage.get(participant_id, 0) + 1
    if not coverage:
        raise ValueError("window has no participants.")
    return max(coverage.items(), key=lambda item: (item[1], item[0]))[0]


def _visible_participant_ids(thread: dict[str, Any], self_id: str) -> list[str]:
    return [participant_id for participant_id in thread["participant_ids"] if participant_id != self_id]


def _participant_record(
    participant_id: str,
    participants_by_id: dict[str, dict[str, Any]],
    *,
    is_self: bool,
) -> dict[str, Any]:
    return _participant_contact_fields(participant_id, participants_by_id, is_self=is_self)


def _thread_participants(
    thread: dict[str, Any],
    participants_by_id: dict[str, dict[str, Any]],
    self_id: str,
) -> list[dict[str, Any]]:
    visible_ids = _visible_participant_ids(thread, self_id)
    return [
        _participant_record(participant_id, participants_by_id, is_self=False)
        for participant_id in visible_ids
    ]


def _service_like(thread: dict[str, Any], participants_by_id: dict[str, dict[str, Any]], self_id: str) -> bool:
    if thread.get("sender_kind") in {"service", "shortcode"}:
        return True
    for participant_id in _visible_participant_ids(thread, self_id):
        name = participants_by_id[participant_id]["display_name"]
        digits = "".join(character for character in name if character.isdigit())
        if len(digits) >= 4 and len(digits) == len(name.replace(" ", "")):
            return True
    return False


def _message_sort_key(message: dict[str, Any]) -> tuple[int, str]:
    return (int(message.get("minutes_ago", 0)), message.get("text", ""))


def _target_anchor_index(thread: dict[str, Any], target: dict[str, Any]) -> int | None:
    explicit_index = target.get("message_index")
    if explicit_index is not None:
        index = int(explicit_index)
        if 0 <= index < len(thread["messages"]):
            return index

    candidates: list[int] = []
    for index, message in enumerate(thread["messages"]):
        if (
            message["speaker_id"] == target["speaker_id"]
            and message["text"] == target["text"]
            and int(message.get("minutes_ago", 0)) == int(target.get("minutes_ago", 0))
        ):
            candidates.append(index)
    if candidates:
        return candidates[-1]
    return None


def _history_messages_for_target(thread: dict[str, Any], target: dict[str, Any]) -> list[dict[str, Any]]:
    anchor_index = _target_anchor_index(thread, target)
    if anchor_index is not None:
        return thread["messages"][:anchor_index]
    target_minutes = int(target.get("minutes_ago", 0))
    return [
        message
        for message in thread["messages"]
        if int(message.get("minutes_ago", 0)) > target_minutes
    ]


def _sample_gap_minutes(
    rng: random.Random,
    *,
    weights: tuple[float, float, float, float, float],
) -> int:
    buckets = [
        (0, 5),
        (5, 30),
        (30, 180),
        (180, 720),
        (720, 1440),
    ]
    start, end = rng.choices(buckets, weights=weights, k=1)[0]
    if end <= start:
        return start
    return rng.randint(start, end)


def _choose_difficulty_recipe(
    *,
    target: dict[str, Any],
    gold_thread: dict[str, Any],
    self_id: str,
    rng: random.Random,
) -> DifficultyRecipe:
    reasons = set(target.get("ambiguity_reasons", []))
    is_outbound = target["speaker_id"] == self_id
    generic = looks_generic(target["text"]) or target.get("kind", "plain") != "plain"
    is_group = bool(gold_thread["is_group"])
    if is_outbound:
        return DIFFICULTY_RECIPES["outbound_sparse"]
    if "service_sender" in reasons:
        return DIFFICULTY_RECIPES["service_collision"]
    if (
        not generic
        and (
            "speaker_shared_across_threads" in reasons
            or "overlapping_participants" in reasons
            or "planning_overlap" in reasons
        )
    ):
        return DIFFICULTY_RECIPES["topic_semantic"]
    if is_group and generic:
        return DIFFICULTY_RECIPES["group_overlap_semantic"]
    if generic and ("speaker_shared_across_threads" in reasons or "overlapping_participants" in reasons):
        return rng.choice(
            [
                DIFFICULTY_RECIPES["same_sender_semantic"],
                DIFFICULTY_RECIPES["stale_semantic"],
            ]
        )
    if generic:
        return DIFFICULTY_RECIPES["stale_semantic"]
    return DIFFICULTY_RECIPES["baseline"]


def _sample_thread_offset_minutes(
    rng: random.Random,
    *,
    target: dict[str, Any],
    thread: dict[str, Any],
    is_gold: bool,
    is_plausible: bool,
    self_id: str,
    recipe: DifficultyRecipe,
) -> int:
    target_is_outbound = target["speaker_id"] == self_id
    generic = looks_generic(target["text"]) or target.get("kind", "plain") != "plain"
    if is_gold:
        if recipe.force_stale_gold:
            weights = (0.02, 0.08, 0.22, 0.36, 0.32)
        elif target_is_outbound:
            weights = (0.10, 0.26, 0.34, 0.22, 0.08)
        elif generic:
            weights = (0.12, 0.28, 0.34, 0.19, 0.07)
        else:
            weights = (0.18, 0.32, 0.31, 0.15, 0.04)
    else:
        if recipe.force_recent_wrong and is_plausible:
            weights = (0.62, 0.26, 0.08, 0.03, 0.01)
        elif is_plausible:
            weights = (0.42, 0.34, 0.16, 0.06, 0.02)
        else:
            weights = (0.50, 0.28, 0.15, 0.05, 0.02)
        if thread.get("is_group") and not target.get("target_is_group", thread.get("is_group")):
            weights = (0.54, 0.26, 0.13, 0.05, 0.02)
    return _sample_gap_minutes(rng, weights=weights)


def _render_history_message(
    *,
    message: dict[str, Any],
    participants_by_id: dict[str, dict[str, Any]],
    target_time: datetime,
    target_minutes_ago: int,
    thread_offset_minutes: int,
    profile: CorruptionProfile,
    self_id: str,
    rng: random.Random,
) -> dict[str, Any]:
    speaker_name = participants_by_id[message["speaker_id"]]["display_name"]
    sender_addressing = (
        SELF_SENDER_ADDRESSING
        if message["speaker_id"] == self_id
        else _participant_primary_address(message["speaker_id"], participants_by_id)
    )
    delta_from_target = max(1, int(message.get("minutes_ago", 0)) - target_minutes_ago)
    total_gap_minutes = thread_offset_minutes + delta_from_target
    timestamp = target_time - timedelta(minutes=total_gap_minutes)
    is_outbound = message["speaker_id"] == self_id
    sparse_sender = rng.random() < profile.history_sender_sparse_prob
    sparse_body = rng.random() < profile.history_blank_prob
    body = "" if sparse_body else message["text"]
    preview = body or (message["text"] if sparse_body and rng.random() < 0.35 else "")
    return {
        "message_key": f"hist::{message['speaker_id']}::{message.get('minutes_ago', 0)}::{abs(hash(message['text'])) % 100000}",
        "sort_utc": timestamp.isoformat(),
        "preview": preview,
        "sender_name": "" if sparse_sender else (SELF_SENDER_NAME if is_outbound else speaker_name),
        "sender_addressing": "" if sparse_sender else sender_addressing,
        "body": body,
        "folder": "sent" if is_outbound else "inbox",
        "status": "Read" if rng.random() < 0.65 else "Unread",
        "kind": message.get("kind", "plain"),
    }


def _render_thread_history(
    *,
    thread: dict[str, Any],
    target: dict[str, Any],
    participants_by_id: dict[str, dict[str, Any]],
    target_time: datetime,
    profile: CorruptionProfile,
    self_id: str,
    rng: random.Random,
    thread_offset_minutes: int,
) -> list[dict[str, Any]]:
    older_messages = _history_messages_for_target(thread, target)
    if not older_messages:
        return []
    if any(message["speaker_id"] == self_id for message in older_messages):
        retained_messages = []
        for message in older_messages:
            if (
                message["speaker_id"] == self_id
                and len(older_messages) > 2
                and rng.random() < 0.72
            ):
                continue
            retained_messages.append(message)
        older_messages = retained_messages or older_messages[-1:]
    tail = older_messages[-MAX_HISTORY_MESSAGES:]
    rendered = [
        _render_history_message(
            message=message,
            participants_by_id=participants_by_id,
            target_time=target_time,
            target_minutes_ago=int(target.get("minutes_ago", 0)),
            thread_offset_minutes=thread_offset_minutes,
            profile=profile,
            self_id=self_id,
            rng=rng,
        )
        for message in tail
    ]
    rendered.sort(key=lambda item: item["sort_utc"])
    return rendered


def _thread_descriptor(display_name: str, *, truncated: bool) -> str:
    if not truncated:
        return display_name
    if "," in display_name:
        parts = [part.strip() for part in display_name.split(",") if part.strip()]
        if len(parts) >= 3:
            return f"To you, {parts[0]}, {parts[1]} +{max(len(parts) - 2, 1)}"
    return truncate_for_notification(display_name, max_len=22)


def _format_recipient_addressing(thread: dict[str, Any], participants_by_id: dict[str, dict[str, Any]], self_id: str) -> str:
    recipients = [
        _participant_primary_address(participant_id, participants_by_id)
        for participant_id in _visible_participant_ids(thread, self_id)
    ]
    return ", ".join(recipients)


def _build_notification(
    *,
    target: dict[str, Any],
    thread: dict[str, Any],
    participants_by_id: dict[str, dict[str, Any]],
    target_time: datetime,
    profile: CorruptionProfile,
    target_index: int,
    self_id: str,
    rng: random.Random,
) -> list[dict[str, Any]]:
    if target["speaker_id"] == self_id:
        return []
    if rng.random() > profile.notification_present_prob:
        return []

    speaker_name = participants_by_id[target["speaker_id"]]["display_name"]
    received_at = target_time + timedelta(seconds=profile.jitter_seconds)
    if rng.random() < profile.notification_stale_prob:
        received_at += timedelta(minutes=rng.choice((2, 5, 10)))
    preview = target["text"]
    if profile.ancs_truncate:
        preview = truncate_for_notification(preview, max_len=24)

    title = (
        _thread_descriptor(thread["display_name"], truncated=profile.descriptor_truncated)
        if thread["is_group"]
        else speaker_name
    )
    subtitle = speaker_name if thread["is_group"] else ""

    return [
        {
            "notification_uid": target_index,
            "received_utc": received_at.isoformat(),
            "title": title,
            "subtitle": subtitle,
            "message": preview,
            "app_identifier": "com.apple.MobileSMS",
        }
    ]


def _build_target_message(
    *,
    target: dict[str, Any],
    thread: dict[str, Any],
    participants_by_id: dict[str, dict[str, Any]],
    target_time: datetime,
    profile: CorruptionProfile,
    sample_id: str,
    self_id: str,
) -> dict[str, Any]:
    speaker_name = participants_by_id[target["speaker_id"]]["display_name"]
    map_time = target_time + (timedelta(hours=3) if profile.whole_hour_skew else timedelta())
    is_outbound = target["speaker_id"] == self_id
    body = "" if profile.blank_body else target["text"]
    subject = target["text"] if profile.use_subject_only else ""
    sender_name = SELF_SENDER_NAME if is_outbound else speaker_name
    sender_addressing = (
        SELF_SENDER_ADDRESSING
        if is_outbound
        else _participant_primary_address(target["speaker_id"], participants_by_id)
    )
    visible_participants = _visible_participant_ids(thread, self_id)
    originators = [] if is_outbound else [_participant_party(target["speaker_id"], participants_by_id)]
    recipients = (
        [_participant_party(participant_id, participants_by_id) for participant_id in visible_participants]
        if is_outbound
        else [_participant_party(self_id, participants_by_id, is_self=True)]
    )

    if profile.drop_sender_name:
        sender_name = ""
    if profile.drop_sender_addressing:
        sender_addressing = ""

    return {
        "message_key": f"target::{sample_id}",
        "sort_utc": map_time.isoformat(),
        "conversation_display_name": thread["display_name"],
        "is_group": bool(thread["is_group"]),
        "participants": _thread_participants(thread, participants_by_id, self_id),
        "message": {
            "folder": "sent" if is_outbound else "inbox",
            "handle": (
                _participant_primary_address(visible_participants[0], participants_by_id)
                if is_outbound and visible_participants
                else _participant_primary_address(target["speaker_id"], participants_by_id)
            ),
            "type": "SMS_GSM",
            "datetime": map_time.strftime("%Y%m%dT%H%M%S"),
            "senderName": sender_name,
            "senderAddressing": sender_addressing,
            "recipientAddressing": _format_recipient_addressing(thread, participants_by_id, self_id) if is_outbound else "",
            "body": body,
            "subject": subject,
            "status": "Unread",
            "messageType": "SMSGSM",
            "originators": originators,
            "recipients": recipients,
        },
    }


def _candidate_features(
    *,
    target_text: str,
    target_thread: dict[str, Any],
    candidate_thread: dict[str, Any],
    target_time: datetime,
    candidate_history: list[dict[str, Any]],
    target_visible_participants: set[str],
    candidate_visible_participants: set[str],
) -> dict[str, Any]:
    candidate_last = candidate_history[-1] if candidate_history else None
    delta_seconds = None
    preview_overlap = 0.0
    if candidate_last and candidate_last.get("sort_utc"):
        delta_seconds = abs(
            int(
                (
                    target_time
                    - datetime.fromisoformat(candidate_last["sort_utc"])
                ).total_seconds()
            )
        )
        preview_overlap = token_overlap(target_text, candidate_last.get("preview"))

    participant_overlap = len(target_visible_participants & candidate_visible_participants)
    candidate_score = 0.0
    if delta_seconds is not None:
        if delta_seconds <= 5 * 60:
            candidate_score += 4.0
        elif delta_seconds <= 30 * 60:
            candidate_score += 3.0
        elif delta_seconds <= 2 * 3600:
            candidate_score += 2.0
        elif delta_seconds <= 12 * 3600:
            candidate_score += 1.0
    if preview_overlap > 0:
        candidate_score += preview_overlap * 3.5
    if participant_overlap > 0:
        candidate_score += min(3.0, participant_overlap * 1.25)
    if candidate_thread["is_group"] == target_thread["is_group"]:
        candidate_score += 0.5
    if looks_generic(target_text):
        candidate_score += 0.5
    return {
        "candidate_score": round(candidate_score, 4),
        "preview_overlap": round(preview_overlap, 4),
        "participant_overlap": participant_overlap,
        "delta_seconds": delta_seconds,
        "candidate_is_group": bool(candidate_thread["is_group"]),
    }


def _target_sender_name(
    target: dict[str, Any],
    participants_by_id: dict[str, dict[str, Any]],
    self_id: str,
) -> str:
    speaker_id = target.get("speaker_id")
    if speaker_id == self_id:
        return SELF_SENDER_NAME
    if speaker_id and speaker_id in participants_by_id:
        return participants_by_id[speaker_id]["display_name"]
    return ""


def _history_collision_priority(
    *,
    target: dict[str, Any],
    gold_thread: dict[str, Any],
    candidate_thread: dict[str, Any],
    candidate_history: list[dict[str, Any]],
    self_id: str,
    participants_by_id: dict[str, dict[str, Any]],
) -> tuple[int, int, int, int]:
    if candidate_thread["thread_id"] == gold_thread["thread_id"] or not candidate_history:
        return (-1, -1, -1, -1)

    same_sender = int(
        _target_matches_history_sender(target, candidate_history, participants_by_id, self_id)
        or _target_sender_in_thread(target, candidate_thread, self_id)
    )
    same_groupness = int(bool(candidate_thread["is_group"]) == bool(gold_thread["is_group"]))
    overlap = len(_visible_participant_set(candidate_thread, self_id) & _visible_participant_set(gold_thread, self_id))
    recent = 0
    last = candidate_history[-1]
    if last.get("sort_utc"):
        recent = int(datetime.fromisoformat(last["sort_utc"]).timestamp())
    return (same_sender, same_groupness, overlap, recent)


def _inject_history_collisions(
    *,
    candidate_entries: list[dict[str, Any]],
    target: dict[str, Any],
    gold_thread: dict[str, Any],
    recipe: DifficultyRecipe,
    participants_by_id: dict[str, dict[str, Any]],
    self_id: str,
    rng: random.Random,
) -> None:
    if not recipe.force_text_collision:
        return

    target_text = normalize_text(target.get("text"))
    if not target_text:
        return

    target_sender = _target_sender_name(target, participants_by_id, self_id)
    generic_target = looks_generic(target_text)
    eligible = sorted(
        candidate_entries,
        key=lambda item: _history_collision_priority(
            target=target,
            gold_thread=gold_thread,
            candidate_thread=item["thread"],
            candidate_history=item["history"],
            self_id=self_id,
            participants_by_id=participants_by_id,
        ),
        reverse=True,
    )
    eligible = [item for item in eligible if item["thread"]["thread_id"] != gold_thread["thread_id"] and item["history"]]
    if not eligible:
        return

    desired = 1
    if recipe.name in {"group_overlap_semantic", "service_collision"}:
        desired = 2

    injected = 0
    for item in eligible:
        if injected >= desired:
            break
        history = item["history"]
        if not history:
            continue
        last = history[-1]
        collision_text = target["text"]
        if generic_target and rng.random() < 0.3:
            collision_text = rng.choice([target["text"], *GENERIC_COLLISION_REPLIES])
        last["preview"] = collision_text
        if last.get("body") or rng.random() < 0.55:
            last["body"] = collision_text
        if target_sender and (
            _target_sender_in_thread(target, item["thread"], self_id)
        ):
            if target.get("speaker_id") == self_id:
                last["folder"] = "sent"
                last["sender_name"] = SELF_SENDER_NAME
                last["sender_addressing"] = SELF_SENDER_ADDRESSING
            else:
                last["folder"] = "inbox"
                last["sender_name"] = target_sender
                last["sender_addressing"] = target_sender
        injected += 1


def _candidate_count(max_candidates: int, rng: random.Random) -> int:
    return min(max_candidates, rng.choice((4, 5, 6, max_candidates)))


def _target_sender_in_thread(target: dict[str, Any], thread: dict[str, Any], self_id: str) -> bool:
    speaker_id = target.get("speaker_id")
    return bool(speaker_id and speaker_id != self_id and speaker_id in thread.get("participant_ids", []))


def _visible_participant_set(thread: dict[str, Any], self_id: str) -> set[str]:
    return set(_visible_participant_ids(thread, self_id))


def _target_matches_history_sender(
    target: dict[str, Any],
    history: list[dict[str, Any]],
    participants_by_id: dict[str, dict[str, Any]],
    self_id: str,
) -> bool:
    speaker_id = target.get("speaker_id")
    if not speaker_id:
        return False
    if speaker_id == self_id:
        target_sender = SELF_SENDER_NAME.lower()
    else:
        target_sender = participants_by_id[speaker_id]["display_name"].lower()
    for history_message in history:
        sender_name = str(history_message.get("sender_name") or history_message.get("senderName") or "").strip().lower()
        if sender_name and sender_name == target_sender:
            return True
    return False


def _map_shadow_messages(
    *,
    donor_thread: dict[str, Any],
    donor_self_id: str,
    target_participant_ids: list[str],
    target_self_id: str,
    target_text: str,
    recipe: DifficultyRecipe,
    rng: random.Random,
) -> list[dict[str, Any]]:
    donor_visible = _visible_participant_ids(donor_thread, donor_self_id)
    target_visible = [participant_id for participant_id in target_participant_ids if participant_id != target_self_id]
    donor_to_target: dict[str, str] = {donor_self_id: target_self_id}
    if target_visible:
        for index, participant_id in enumerate(donor_visible):
            donor_to_target[participant_id] = target_visible[min(index, len(target_visible) - 1)]
    collision_index = None
    if recipe.force_text_collision and donor_thread.get("messages"):
        collision_index = max(0, len(donor_thread["messages"]) - 1 - rng.randint(0, min(2, len(donor_thread["messages"]) - 1)))

    remapped: list[dict[str, Any]] = []
    for index, message in enumerate(donor_thread.get("messages", [])):
        mapped_speaker = donor_to_target.get(message["speaker_id"])
        if not mapped_speaker:
            mapped_speaker = target_visible[0] if target_visible else target_self_id
        text = message["text"]
        if collision_index is not None and index == collision_index:
            text = rng.choice([target_text, rng.choice(GENERIC_COLLISION_REPLIES)])
        remapped.append(
            {
                **message,
                "speaker_id": mapped_speaker,
                "text": text,
            }
        )
    return remapped


def _make_shadow_thread(
    *,
    gold_thread: dict[str, Any],
    donor_thread: dict[str, Any],
    target: dict[str, Any],
    target_self_id: str,
    donor_self_id: str,
    recipe: DifficultyRecipe,
    shadow_index: int,
    rng: random.Random,
) -> dict[str, Any]:
    return {
        "thread_id": f"shadow::{gold_thread['thread_id']}::{donor_thread['thread_id']}::{shadow_index}",
        "display_name": gold_thread["display_name"],
        "is_group": bool(gold_thread["is_group"]),
        "participant_ids": list(gold_thread["participant_ids"]),
        "theme": f"shadow::{recipe.name}",
        "messages": _map_shadow_messages(
            donor_thread=donor_thread,
            donor_self_id=donor_self_id,
            target_participant_ids=list(gold_thread["participant_ids"]),
            target_self_id=target_self_id,
            target_text=target["text"],
            recipe=recipe,
            rng=rng,
        ),
    }


def _ordered_candidate_pools(
    *,
    current_window: dict[str, Any],
    windows: list[dict[str, Any]],
    gold_thread: dict[str, Any],
    target: dict[str, Any],
    self_id: str,
    participants_by_id: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    target_is_group = bool(gold_thread["is_group"])
    target_service = _service_like(gold_thread, participants_by_id, self_id) or "service_sender" in target.get("ambiguity_reasons", [])
    plausible_ids = [thread_id for thread_id in target["plausible_thread_ids"] if thread_id != gold_thread["thread_id"]]
    target_speaker_id = target["speaker_id"]

    local_threads = [thread for thread in current_window["threads"] if thread["thread_id"] != gold_thread["thread_id"]]
    cross_window_threads = [
        thread
        for window in windows
        if window["window_id"] != current_window["window_id"]
        for thread in window["threads"]
    ]

    def rank(thread: dict[str, Any], *, cross_window: bool) -> tuple[int, int, int, int, str]:
        shared = len(set(thread["participant_ids"]) & set(gold_thread["participant_ids"]))
        speaker_shared = 1 if target_speaker_id in thread.get("participant_ids", []) and target_speaker_id != self_id else 0
        service = 1 if _service_like(thread, participants_by_id if not cross_window else _participant_lookup(next(window for window in windows if thread in window["threads"])), self_id) else 0
        same_group = 1 if bool(thread["is_group"]) == target_is_group else 0
        plausible = 1 if thread["thread_id"] in plausible_ids else 0
        service_bonus = 1 if target_service and service else 0
        return (
            plausible,
            speaker_shared,
            service_bonus,
            same_group,
            shared,
            thread["thread_id"],
        )

    local_threads = sorted(local_threads, key=lambda item: rank(item, cross_window=False), reverse=True)
    cross_window_threads = sorted(cross_window_threads, key=lambda item: rank(item, cross_window=True), reverse=True)
    return local_threads, cross_window_threads


def _select_candidate_threads(
    *,
    windows: list[dict[str, Any]],
    current_window: dict[str, Any],
    target: dict[str, Any],
    gold_thread: dict[str, Any],
    max_candidates: int,
    recipe: DifficultyRecipe,
    rng: random.Random,
) -> list[dict[str, Any]]:
    participants_by_id = _participant_lookup(current_window)
    self_id = _self_participant_id(current_window, participants_by_id)
    target_is_group = bool(gold_thread["is_group"])
    desired_count = _candidate_count(max_candidates, rng)
    min_group_negatives = recipe.min_group_negatives
    max_group_negatives = max(recipe.min_group_negatives, recipe.max_group_negatives)

    selected = [gold_thread]
    selected_ids = {gold_thread["thread_id"]}
    group_negative_count = 0

    def try_add(thread: dict[str, Any]) -> bool:
        nonlocal group_negative_count
        if thread["thread_id"] in selected_ids:
            return False
        is_group_negative = bool(thread["is_group"])
        if is_group_negative and group_negative_count >= max_group_negatives:
            return False
        selected.append(thread)
        selected_ids.add(thread["thread_id"])
        if is_group_negative:
            group_negative_count += 1
        return True

    local_threads, cross_window_threads = _ordered_candidate_pools(
        current_window=current_window,
        windows=windows,
        gold_thread=gold_thread,
        target=target,
        self_id=self_id,
        participants_by_id=participants_by_id,
    )

    local_same_sender = [
        thread
        for thread in local_threads
        if _target_sender_in_thread(target, thread, self_id)
    ]
    local_group_threads = [thread for thread in local_threads if thread["is_group"]]
    local_same_geometry = [
        thread
        for thread in local_threads
        if _visible_participant_set(thread, self_id) == _visible_participant_set(gold_thread, self_id)
    ]

    for thread_id in target["plausible_thread_ids"]:
        if thread_id == gold_thread["thread_id"]:
            continue
        match = next((thread for thread in local_threads if thread["thread_id"] == thread_id), None)
        if match:
            try_add(match)

    for thread in local_same_sender:
        if len(selected) >= desired_count:
            break
        if group_negative_count >= max_group_negatives and thread["is_group"]:
            continue
        try_add(thread)
        if sum(1 for item in selected[1:] if _target_sender_in_thread(target, item, self_id)) >= recipe.same_sender_distractors:
            break

    if recipe.force_text_collision:
        for thread in local_same_geometry:
            if len(selected) >= desired_count:
                break
            try_add(thread)

    if group_negative_count < min_group_negatives:
        for pool in (local_group_threads, [thread for thread in cross_window_threads if thread["is_group"]]):
            for thread in pool:
                if len(selected) >= desired_count:
                    break
                try_add(thread)
                if group_negative_count >= min_group_negatives:
                    break
            if group_negative_count >= min_group_negatives or len(selected) >= desired_count:
                break

    pools = [local_threads, cross_window_threads]
    for pool in pools:
        for thread in pool:
            if len(selected) >= desired_count:
                break
            try_add(thread)
        if len(selected) >= desired_count:
            break

    if len(selected) < 3:
        for pool in pools:
            for thread in pool:
                if thread["thread_id"] in selected_ids:
                    continue
                selected.append(thread)
                selected_ids.add(thread["thread_id"])
                if len(selected) >= min(3, desired_count):
                    break
            if len(selected) >= min(3, desired_count):
                break

    return selected[:max_candidates]


def _shared_tokens(text: str) -> set[str]:
    return {
        token
        for token in tokenize(text)
        if token not in {"the", "a", "an", "to", "you", "we", "i", "im", "i'm", "it", "is", "are"}
    }


def _derive_ambiguous_targets(window: dict[str, Any]) -> list[dict[str, Any]]:
    explicit_targets = window.get("ambiguous_targets", [])
    if explicit_targets:
        return explicit_targets

    self_id = window["self_participant_id"]
    derived: list[dict[str, Any]] = []
    for thread in window["threads"]:
        thread_member_set = set(thread["participant_ids"])
        recent_messages = sorted(thread["messages"], key=_message_sort_key)[-5:]
        for message_index, message in enumerate(recent_messages, start=max(0, len(thread["messages"]) - len(recent_messages))):
            text = message["text"]
            plausible_thread_ids = [thread["thread_id"]]
            reasons: set[str] = set()
            tokens = _shared_tokens(text)
            for other_thread in window["threads"]:
                if other_thread["thread_id"] == thread["thread_id"]:
                    continue
                other_member_set = set(other_thread["participant_ids"])
                shared_participants = len(thread_member_set & other_member_set)
                speaker_shared = message["speaker_id"] in other_member_set
                lexical_overlap = max(
                    (token_overlap(text, candidate["text"]) for candidate in other_thread["messages"]),
                    default=0.0,
                )
                shared_entities = {
                    str(value).strip().lower()
                    for value in (thread.get("shared_entity"), other_thread.get("shared_entity"))
                    if str(value).strip()
                }
                domain_overlap = len(set(thread.get("domain_tags", [])) & set(other_thread.get("domain_tags", [])))
                service_overlap = thread.get("sender_kind") == other_thread.get("sender_kind") == "service"
                should_add = False
                if looks_generic(text):
                    should_add = shared_participants > 0 or speaker_shared or bool(shared_entities) or domain_overlap > 0
                    if should_add:
                        reasons.add("generic_reply")
                elif lexical_overlap >= 0.28 and (shared_participants > 0 or speaker_shared or bool(shared_entities)):
                    should_add = True
                    reasons.add("lexical_collision")
                elif tokens & {"when", "where", "outside", "parking", "leaving", "coming", "free", "soon"} and (
                    shared_participants > 0 or bool(shared_entities) or domain_overlap > 0
                ):
                    should_add = True
                    reasons.add("planning_overlap")
                elif service_overlap and ("service" in thread.get("domain_tags", []) or "service" in other_thread.get("domain_tags", [])):
                    should_add = True
                    reasons.add("service_sender")

                if not should_add:
                    continue

                if shared_participants > 0:
                    reasons.add("overlapping_participants")
                if speaker_shared:
                    reasons.add("speaker_shared_across_threads")
                plausible_thread_ids.append(other_thread["thread_id"])

            plausible_thread_ids = list(dict.fromkeys(plausible_thread_ids))
            if len(plausible_thread_ids) < 2:
                continue

            derived.append(
                {
                    "target_id": f"{thread['thread_id']}::m{message_index}",
                    "gold_thread_id": thread["thread_id"],
                    "speaker_id": message["speaker_id"],
                    "text": text,
                    "minutes_ago": int(message["minutes_ago"]),
                    "message_index": message_index,
                    "kind": message.get("kind", "plain"),
                    "ambiguity_reasons": sorted(reasons) or ["ambiguous_reply"],
                    "plausible_thread_ids": plausible_thread_ids,
                }
            )

    return derived


def _with_style_variation(text: str, rng: random.Random) -> str:
    variant = text.strip()
    if not variant:
        return variant
    if rng.random() < 0.35:
        variant += rng.choice(STYLE_EMOJI_SUFFIXES)
    if rng.random() < 0.12:
        variant = variant.lower()
    return " ".join(variant.split())


def _reaction_anchor_preview(
    *,
    target: dict[str, Any],
    gold_thread: dict[str, Any],
    rng: random.Random,
) -> str:
    messages = list(gold_thread.get("messages", []))
    if not messages:
        return target["text"]

    target_index = int(target.get("message_index", len(messages) - 1))
    upper = max(0, min(target_index, len(messages)))
    prior = messages[:upper]
    ranked: list[str] = []

    def add_candidate(text: str) -> None:
        normalized = normalize_text(text)
        if not normalized:
            return
        if normalized not in ranked:
            ranked.append(normalized)

    for message in reversed(prior[-4:]):
        text = str(message.get("text", ""))
        if text and not looks_generic(text):
            add_candidate(text)

    for message in reversed(prior[-4:]):
        text = str(message.get("text", ""))
        if text:
            add_candidate(text)

    if not ranked:
        add_candidate(target["text"])

    preview = ranked[0] if ranked else target["text"]
    if len(preview) > 140:
        preview = preview[:137].rstrip() + "..."
    return preview


def _choose_augmented_target_text(
    *,
    target: dict[str, Any],
    gold_thread: dict[str, Any],
    recipe: DifficultyRecipe,
    style_packs: dict[str, list[Any]] | None,
    rng: random.Random,
) -> str:
    if not style_packs:
        return target["text"]

    ambiguous_messages = [
        item
        for item in style_packs.get("ambiguous_messages", [])
        if isinstance(item, dict)
        and item.get("text")
        and (
            item.get("plausible_in_group")
            if gold_thread["is_group"]
            else item.get("plausible_in_direct")
        )
    ]
    generic_ambiguous_messages = [
        item
        for item in ambiguous_messages
        if "generic" in str(item.get("intent_tag", "")).lower()
        or looks_generic(item.get("text"))
    ]
    semantic_ambiguous_messages = [
        item
        for item in ambiguous_messages
        if item.get("text") and not looks_generic(item.get("text"))
    ]
    planning_lines = [str(item) for item in style_packs.get("planning_lines", []) if str(item).strip()]
    generic_replies = [str(item) for item in style_packs.get("generic_replies", []) if str(item).strip()]
    reaction_templates = [str(item) for item in style_packs.get("reaction_templates", []) if "{preview}" in str(item)]

    reasons = set(target.get("ambiguity_reasons", []))
    original = target["text"]
    replacement = original

    if recipe.name == "topic_semantic":
        if "planning_overlap" in reasons and planning_lines and rng.random() < 0.25:
            replacement = rng.choice(planning_lines)
        elif semantic_ambiguous_messages and rng.random() < 0.15:
            replacement = str(rng.choice(semantic_ambiguous_messages)["text"])
        else:
            replacement = original
    elif recipe.force_text_collision and ambiguous_messages and rng.random() < 0.8:
        collision_pool = generic_ambiguous_messages or ambiguous_messages
        replacement = str(rng.choice(collision_pool)["text"])
    elif target.get("kind") != "plain" and reaction_templates and rng.random() < 0.7:
        quoted_preview = _reaction_anchor_preview(
            target=target,
            gold_thread=gold_thread,
            rng=rng,
        )
        replacement = rng.choice(reaction_templates).format(
            preview=quoted_preview or rng.choice(planning_lines or generic_replies or [original])
        )
    elif looks_generic(original):
        if generic_replies and rng.random() < 0.65:
            replacement = rng.choice(generic_replies)
        elif generic_ambiguous_messages:
            replacement = str(rng.choice(generic_ambiguous_messages)["text"])
        elif ambiguous_messages:
            replacement = str(rng.choice(ambiguous_messages)["text"])
    elif "planning_overlap" in reasons and planning_lines and rng.random() < 0.75:
        replacement = rng.choice(planning_lines)
    elif "service_sender" in reasons and ambiguous_messages and rng.random() < 0.6:
        replacement = str(rng.choice(ambiguous_messages)["text"])
    elif rng.random() < 0.35 and ambiguous_messages:
        replacement = str(rng.choice(ambiguous_messages)["text"])

    return _with_style_variation(replacement, rng)


def build_synth_samples(
    *,
    count: int,
    max_candidates: int,
    seed: int,
    thread_windows: list[dict[str, Any]],
    style_packs: dict[str, list[Any]] | None = None,
    replay_failure_boost: float = 0.0,
    focus_ambiguity_reasons: Sequence[str] | None = None,
    focus_ambiguity_boost: float = 0.0,
) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    if not thread_windows:
        raise ValueError("thread_windows must not be empty.")

    base_time = datetime.now(tz=timezone.utc).replace(microsecond=0)
    target_pool = [
        (window, target)
        for window in thread_windows
        for target in _derive_ambiguous_targets(window)
    ]
    if not target_pool:
        raise ValueError("thread_windows did not yield any ambiguous targets.")

    generic_targets = [item for item in target_pool if looks_generic(item[1]["text"])]
    outbound_targets = [item for item in target_pool if item[1]["speaker_id"] == item[0]["self_participant_id"]]
    non_outbound_targets = [item for item in target_pool if item[1]["speaker_id"] != item[0]["self_participant_id"]]
    generic_non_outbound_targets = [item for item in generic_targets if item[1]["speaker_id"] != item[0]["self_participant_id"]]
    service_targets = [item for item in target_pool if "service_sender" in item[1].get("ambiguity_reasons", [])]
    replay_failure_targets = [
        item
        for item in target_pool
        if (
            "live_runtime_failure" in item[1].get("ambiguity_reasons", [])
            or "replay_contamination" in item[1].get("ambiguity_reasons", [])
        )
    ]
    focus_reason_set = {str(reason).strip() for reason in (focus_ambiguity_reasons or []) if str(reason).strip()}
    focused_targets = [
        item
        for item in target_pool
        if focus_reason_set & set(item[1].get("ambiguity_reasons", []))
    ]
    same_sender_targets = [
        item
        for item in target_pool
        if (
            item[1]["speaker_id"] != item[0]["self_participant_id"]
            and "speaker_shared_across_threads" in item[1].get("ambiguity_reasons", [])
            and "service_sender" not in item[1].get("ambiguity_reasons", [])
        )
    ]
    topic_semantic_targets = [
        item
        for item in target_pool
        if (
            item[1]["speaker_id"] != item[0]["self_participant_id"]
            and not looks_generic(item[1]["text"])
            and "service_sender" not in item[1].get("ambiguity_reasons", [])
            and (
                "speaker_shared_across_threads" in item[1].get("ambiguity_reasons", [])
                or "overlapping_participants" in item[1].get("ambiguity_reasons", [])
                or "planning_overlap" in item[1].get("ambiguity_reasons", [])
            )
        )
    ]
    semantic_fork_targets = [
        item
        for item in target_pool
        if (
            item[1]["speaker_id"] != item[0]["self_participant_id"]
            and (
                looks_generic(item[1]["text"])
                or "lexical_collision" in item[1].get("ambiguity_reasons", [])
                or "planning_overlap" in item[1].get("ambiguity_reasons", [])
            )
            and "service_sender" not in item[1].get("ambiguity_reasons", [])
        )
    ]
    direct_targets = [
        item
        for item in target_pool
        if (
            item[1]["speaker_id"] != item[0]["self_participant_id"]
            and not next(thread for thread in item[0]["threads"] if thread["thread_id"] == item[1]["gold_thread_id"])["is_group"]
        )
    ]
    group_targets = [
        item
        for item in target_pool
        if next(thread for thread in item[0]["threads"] if thread["thread_id"] == item[1]["gold_thread_id"])["is_group"]
    ]

    samples: list[dict[str, Any]] = []
    for index in range(count):
        if focused_targets and focus_ambiguity_boost > 0 and rng.random() < focus_ambiguity_boost:
            window, target = rng.choice(focused_targets)
        elif replay_failure_targets and replay_failure_boost > 0 and rng.random() < replay_failure_boost:
            window, target = rng.choice(replay_failure_targets)
        else:
            roll = rng.random()
            if roll < 0.10 and outbound_targets:
                window, target = rng.choice(outbound_targets)
            elif roll < 0.18 and service_targets:
                window, target = rng.choice(service_targets)
            elif roll < 0.42 and topic_semantic_targets:
                window, target = rng.choice(topic_semantic_targets)
            elif roll < 0.62 and same_sender_targets:
                window, target = rng.choice(same_sender_targets)
            elif roll < 0.76 and semantic_fork_targets:
                window, target = rng.choice(semantic_fork_targets)
            elif roll < 0.86 and generic_non_outbound_targets:
                window, target = rng.choice(generic_non_outbound_targets)
            elif roll < 0.94 and direct_targets:
                window, target = rng.choice(direct_targets)
            elif roll < 0.98 and group_targets:
                window, target = rng.choice(group_targets)
            elif non_outbound_targets:
                window, target = rng.choice(non_outbound_targets)
            else:
                window, target = rng.choice(target_pool)

        participants_by_id = _participant_lookup(window)
        self_id = _self_participant_id(window, participants_by_id)
        gold_thread = next(
            thread for thread in window["threads"] if thread["thread_id"] == target["gold_thread_id"]
        )
        recipe = _choose_difficulty_recipe(
            target=target,
            gold_thread=gold_thread,
            self_id=self_id,
            rng=rng,
        )
        target = {
            **target,
            "target_is_group": bool(gold_thread["is_group"]),
            "text": _choose_augmented_target_text(
                target=target,
                gold_thread=gold_thread,
                recipe=recipe,
                style_packs=style_packs,
                rng=rng,
            ),
        }
        profile = rng.choice(CORRUPTION_PROFILES)
        sample_time = base_time + timedelta(minutes=index * 11)
        target_time = sample_time - timedelta(minutes=int(target.get("minutes_ago", 0)))

        candidate_threads = _select_candidate_threads(
            windows=thread_windows,
            current_window=window,
            target=target,
            gold_thread=gold_thread,
            max_candidates=max(3, max_candidates - recipe.alias_negative_count),
            recipe=recipe,
            rng=rng,
        )

        local_threads = [
            thread
            for thread in window["threads"]
            if thread["thread_id"] != gold_thread["thread_id"]
        ]
        shadow_donors = [
            thread
            for thread in local_threads
            if _target_sender_in_thread(target, thread, self_id)
            or bool(thread["is_group"]) == bool(gold_thread["is_group"])
        ]
        if not shadow_donors:
            shadow_donors = list(local_threads)
        for shadow_index in range(recipe.alias_negative_count):
            if len(candidate_threads) >= max_candidates or not shadow_donors:
                break
            donor_thread = shadow_donors[shadow_index % len(shadow_donors)]
            candidate_threads.append(
                _make_shadow_thread(
                    gold_thread=gold_thread,
                    donor_thread=donor_thread,
                    target=target,
                    target_self_id=self_id,
                    donor_self_id=self_id,
                    recipe=recipe,
                    shadow_index=shadow_index,
                    rng=rng,
                )
            )
        candidate_threads = candidate_threads[:max_candidates]

        candidate_entries = []
        target_visible = set(_visible_participant_ids(gold_thread, self_id))
        for thread in candidate_threads:
            if thread in window["threads"] or str(thread.get("thread_id", "")).startswith("shadow::"):
                source_window = window
            else:
                source_window = next(
                    candidate_window
                    for candidate_window in thread_windows
                    if thread in candidate_window["threads"]
                )
            candidate_participants_by_id = _participant_lookup(source_window)
            candidate_self_id = _self_participant_id(source_window, candidate_participants_by_id)
            history = _render_thread_history(
                thread=thread,
                target=target,
                participants_by_id=candidate_participants_by_id,
                target_time=target_time,
                profile=profile,
                self_id=candidate_self_id,
                rng=rng,
                thread_offset_minutes=_sample_thread_offset_minutes(
                    rng,
                    target=target,
                    thread=thread,
                    is_gold=thread["thread_id"] == gold_thread["thread_id"],
                    is_plausible=thread["thread_id"] in target["plausible_thread_ids"],
                    self_id=candidate_self_id,
                    recipe=recipe,
                ),
            )
            candidate_visible = set(_visible_participant_ids(thread, candidate_self_id))
            candidate_entries.append(
                {
                    "thread": thread,
                    "history": history,
                    "candidate_visible": candidate_visible,
                    "candidate_participants_by_id": candidate_participants_by_id,
                    "candidate_self_id": candidate_self_id,
                }
            )

        _inject_history_collisions(
            candidate_entries=candidate_entries,
            target=target,
            gold_thread=gold_thread,
            recipe=recipe,
            participants_by_id=participants_by_id,
            self_id=self_id,
            rng=rng,
        )

        rendered_candidates = []
        for entry in candidate_entries:
            thread = entry["thread"]
            history = entry["history"]
            candidate_visible = entry["candidate_visible"]
            candidate_participants_by_id = entry["candidate_participants_by_id"]
            candidate_self_id = entry["candidate_self_id"]
            features = _candidate_features(
                target_text=target["text"],
                target_thread=gold_thread,
                candidate_thread=thread,
                target_time=target_time,
                candidate_history=history,
                target_visible_participants=target_visible,
                candidate_visible_participants=candidate_visible,
            )
            rendered_candidates.append(
                {
                    "thread_id": thread["thread_id"],
                    "display_name": thread["display_name"],
                    "is_group": bool(thread["is_group"]),
                    "participants": _thread_participants(thread, candidate_participants_by_id, candidate_self_id),
                    "history": history,
                    "features": features,
                }
            )

        rng.shuffle(rendered_candidates)
        sample_id = f"synth::{window['window_id']}::{target['target_id']}::{index}"
        target_message = _build_target_message(
            target=target,
            thread=gold_thread,
            participants_by_id=participants_by_id,
            target_time=target_time,
            profile=profile,
            sample_id=sample_id,
            self_id=self_id,
        )
        notifications = _build_notification(
            target=target,
            thread=gold_thread,
            participants_by_id=participants_by_id,
            target_time=target_time,
            profile=profile,
            target_index=index + 1,
            self_id=self_id,
            rng=rng,
        )

        samples.append(
            {
                "sample_id": sample_id,
                "source": "synthetic_llm_window",
                "label_source": "latent_thread_id",
                "gold_thread_id": gold_thread["thread_id"],
                "message": target_message,
                "candidate_threads": rendered_candidates,
                "nearby_notifications": notifications,
                "metadata": {
                    "generic_preview": looks_generic(target["text"]),
                    "preview": target["text"],
                    "candidate_count": len(rendered_candidates),
                    "target_participant_count": len(target_visible),
                    "corruption_profile": profile.name,
                    "window_id": window["window_id"],
                    "ambiguity_reasons": list(target.get("ambiguity_reasons", [])),
                    "plausible_thread_count": len(target["plausible_thread_ids"]),
                    "target_is_outbound": target["speaker_id"] == self_id,
                    "target_is_group": bool(gold_thread["is_group"]),
                    "difficulty_recipe": recipe.name,
                },
            }
        )

    return samples


def _participant_explicit_keys(participants: list[dict[str, Any]]) -> set[str]:
    keys: set[str] = set()
    for participant in participants:
        if participant.get("isSelf"):
            continue
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
    return keys


def validate_dataset(samples: list[dict[str, Any]]) -> dict[str, Any]:
    candidate_counts = [len(sample["candidate_threads"]) for sample in samples]
    group_count = sum(1 for sample in samples if sample["message"]["is_group"])
    generic_count = sum(1 for sample in samples if sample["metadata"].get("generic_preview"))
    sent_target_count = sum(1 for sample in samples if sample["message"]["message"].get("folder") == "sent")
    sent_history_count = 0
    history_count = 0
    missing_sender_count = 0
    blank_body_count = 0
    truncated_notification_count = 0
    notification_present_count = 0
    unique_previews = set()
    unique_windows = set()
    multi_plausible_count = 0
    overlap_heavy_count = 0
    direct_group_candidate_counts: list[int] = []
    gold_deltas: list[float] = []
    unique_display_name_gold_count = 0
    unique_exact_participant_gold_count = 0
    same_sender_multi_count = 0
    stale_same_sender_count = 0
    generic_history_collision_count = 0
    cross_window_distractor_count = 0
    distractor_count = 0
    explicit_target_identity_count = 0
    explicit_gold_identity_count = 0
    explicit_nonself_participant_count = 0
    nonself_participant_count = 0
    shared_explicit_distractor_count = 0
    incompatible_explicit_distractor_count = 0
    runtime_replay_failure_count = 0
    replay_contamination_count = 0

    for sample in samples:
        ambiguity_reasons = set(sample["metadata"].get("ambiguity_reasons", []))
        if "live_runtime_failure" in ambiguity_reasons:
            runtime_replay_failure_count += 1
        if "replay_contamination" in ambiguity_reasons:
            replay_contamination_count += 1
        unique_previews.add(normalize_text(sample["metadata"].get("preview")))
        unique_windows.add(str(sample["metadata"].get("window_id", "")))
        if sample["metadata"].get("plausible_thread_count", 0) >= 2:
            multi_plausible_count += 1
        if sample["nearby_notifications"]:
            notification_present_count += 1
        if any("..." in notification.get("message", "") for notification in sample["nearby_notifications"]):
            truncated_notification_count += 1
        if not sample["message"]["message"].get("senderName"):
            missing_sender_count += 1
        if not sample["message"]["message"].get("body"):
            blank_body_count += 1
        target_explicit_keys = _participant_explicit_keys(sample["message"].get("participants", []) or [])
        if target_explicit_keys:
            explicit_target_identity_count += 1
        for participant in sample["message"].get("participants", []) or []:
            if participant.get("isSelf"):
                continue
            nonself_participant_count += 1
            if participant.get("phones") or participant.get("emails"):
                explicit_nonself_participant_count += 1

        gold_candidates = [
            candidate
            for candidate in sample["candidate_threads"]
            if candidate["thread_id"] == sample["gold_thread_id"]
        ]
        target_display_name = normalize_text(sample["message"].get("conversation_display_name"))
        display_name_matches = [
            candidate
            for candidate in sample["candidate_threads"]
            if normalize_text(candidate.get("display_name")) == target_display_name
        ]
        if len(display_name_matches) == 1 and display_name_matches[0]["thread_id"] == sample["gold_thread_id"]:
            unique_display_name_gold_count += 1
        if gold_candidates:
            gold_explicit_keys = _participant_explicit_keys(gold_candidates[0].get("participants", []) or [])
            if target_explicit_keys and (gold_explicit_keys & target_explicit_keys):
                explicit_gold_identity_count += 1
            delta_seconds = gold_candidates[0]["features"].get("delta_seconds")
            if delta_seconds is not None:
                gold_deltas.append(float(delta_seconds))
            gold_participants = {
                participant["key"]
                for participant in gold_candidates[0]["participants"]
                if not participant.get("isSelf")
            }
            exact_participant_matches = [
                candidate
                for candidate in sample["candidate_threads"]
                if {
                    participant["key"]
                    for participant in candidate["participants"]
                    if not participant.get("isSelf")
                }
                == gold_participants
            ]
            if len(exact_participant_matches) == 1:
                unique_exact_participant_gold_count += 1
            if any(
                len(
                    gold_participants
                    & {
                        participant["key"]
                        for participant in candidate["participants"]
                        if not participant.get("isSelf")
                    }
                )
                > 0
                for candidate in sample["candidate_threads"]
                if candidate["thread_id"] != sample["gold_thread_id"]
            ):
                overlap_heavy_count += 1

            target_sender = normalize_text(
                sample["message"]["message"].get("senderName")
                or sample["message"]["message"].get("senderAddressing")
            ).replace("name:", "")
            same_sender_candidates = 0
            if target_sender:
                for candidate in sample["candidate_threads"]:
                    sender_match = False
                    for history_message in candidate["history"]:
                        history_sender = normalize_text(
                            history_message.get("sender_name")
                            or history_message.get("senderName")
                        ).replace("name:", "")
                        if history_sender and history_sender == target_sender:
                            sender_match = True
                            break
                    if sender_match:
                        same_sender_candidates += 1
            if same_sender_candidates >= 2:
                same_sender_multi_count += 1

            if sample["metadata"].get("generic_preview"):
                target_preview = normalize_text(sample["metadata"].get("preview"))
                if any(
                    target_preview
                    and (
                        normalize_text(history_message.get("preview")) == target_preview
                        or normalize_text(history_message.get("body")) == target_preview
                    )
                    for candidate in sample["candidate_threads"]
                    if candidate["thread_id"] != sample["gold_thread_id"]
                    for history_message in candidate["history"]
                ):
                    generic_history_collision_count += 1

            wrong_recent = any(
                candidate["thread_id"] != sample["gold_thread_id"]
                and candidate["features"].get("delta_seconds") is not None
                and float(candidate["features"]["delta_seconds"]) <= 1800
                for candidate in sample["candidate_threads"]
            )
            if (
                delta_seconds is not None
                and float(delta_seconds) >= 7200
                and wrong_recent
                and same_sender_candidates >= 2
            ):
                stale_same_sender_count += 1
            if target_explicit_keys:
                if any(
                    candidate["thread_id"] != sample["gold_thread_id"]
                    and (_participant_explicit_keys(candidate.get("participants", []) or []) & target_explicit_keys)
                    and _participant_explicit_keys(candidate.get("participants", []) or []) != gold_explicit_keys
                    for candidate in sample["candidate_threads"]
                ):
                    shared_explicit_distractor_count += 1
                if any(
                    candidate["thread_id"] != sample["gold_thread_id"]
                    and not (_participant_explicit_keys(candidate.get("participants", []) or []) & target_explicit_keys)
                    for candidate in sample["candidate_threads"]
                ):
                    incompatible_explicit_distractor_count += 1

        target_is_group = bool(sample["message"]["is_group"])
        group_negatives = sum(
            1
            for candidate in sample["candidate_threads"]
            if candidate["thread_id"] != sample["gold_thread_id"] and candidate["is_group"]
        )
        if not target_is_group:
            direct_group_candidate_counts.append(group_negatives)

        for candidate in sample["candidate_threads"]:
            if candidate["thread_id"] != sample["gold_thread_id"]:
                distractor_count += 1
                if not str(candidate["thread_id"]).startswith(str(sample["metadata"].get("window_id"))):
                    cross_window_distractor_count += 1
            for history_message in candidate["history"]:
                history_count += 1
                if history_message.get("folder") == "sent":
                    sent_history_count += 1

    profile_counts = Counter(sample["metadata"]["corruption_profile"] for sample in samples)
    reason_counts = Counter(
        reason
        for sample in samples
        for reason in sample["metadata"].get("ambiguity_reasons", [])
    )

    sample_count = max(len(samples), 1)
    group_ratio = group_count / sample_count
    generic_ratio = generic_count / sample_count
    sent_target_ratio = sent_target_count / sample_count
    sent_history_ratio = sent_history_count / max(history_count, 1)
    notification_presence_ratio = notification_present_count / sample_count
    truncated_notification_ratio = truncated_notification_count / sample_count
    multi_plausible_ratio = multi_plausible_count / sample_count
    overlap_heavy_ratio = overlap_heavy_count / sample_count
    direct_group_candidate_avg = sum(direct_group_candidate_counts) / max(len(direct_group_candidate_counts), 1)
    unique_display_name_gold_ratio = unique_display_name_gold_count / sample_count
    unique_exact_participant_gold_ratio = unique_exact_participant_gold_count / sample_count
    same_sender_multi_ratio = same_sender_multi_count / sample_count
    stale_same_sender_ratio = stale_same_sender_count / sample_count
    generic_history_collision_ratio = generic_history_collision_count / max(generic_count, 1)
    cross_window_distractor_ratio = cross_window_distractor_count / max(distractor_count, 1)
    explicit_target_identity_ratio = explicit_target_identity_count / sample_count
    explicit_gold_identity_ratio = explicit_gold_identity_count / sample_count
    explicit_nonself_participant_ratio = explicit_nonself_participant_count / max(nonself_participant_count, 1)
    shared_explicit_distractor_ratio = shared_explicit_distractor_count / sample_count
    incompatible_explicit_distractor_ratio = incompatible_explicit_distractor_count / sample_count
    runtime_replay_failure_ratio = runtime_replay_failure_count / sample_count
    replay_contamination_ratio = replay_contamination_count / sample_count

    quality_score = 0
    quality_score += 15 if 0.25 <= group_ratio <= 0.75 else 6
    quality_score += 12 if 0.35 <= generic_ratio <= 0.7 else 4
    quality_score += 12 if 0.08 <= sent_target_ratio <= 0.25 else 0
    quality_score += 12 if 0.08 <= sent_history_ratio <= 0.25 else 0
    quality_score += 10 if 0.25 <= notification_presence_ratio <= 0.7 else 4
    quality_score += 12 if 0 <= direct_group_candidate_avg <= 1.0 else 0
    quality_score += 10 if percentile(gold_deltas, 0.5) >= 600 else 0
    quality_score += 8 if percentile(gold_deltas, 0.9) >= 21600 else 0
    quality_score += 9 if len(unique_previews) >= min(250, max(60, len(samples) // 8)) else 3
    quality_score += 6 if unique_display_name_gold_ratio <= 0.55 else 0
    quality_score += 6 if unique_exact_participant_gold_ratio <= 0.6 else 0
    quality_score += 5 if same_sender_multi_ratio >= 0.25 else 0
    quality_score += 5 if stale_same_sender_ratio >= 0.08 else 0
    quality_score += 5 if generic_history_collision_ratio >= 0.18 else 0
    quality_score += 2 if cross_window_distractor_ratio <= 0.55 else 0
    quality_score += 6 if explicit_target_identity_ratio >= 0.7 else 0
    quality_score += 5 if shared_explicit_distractor_ratio >= 0.12 else 0
    quality_score += 4 if incompatible_explicit_distractor_ratio >= 0.25 else 0

    issues = []
    if sent_target_ratio < 0.08:
        issues.append("no_outbound_targets")
    if sent_history_ratio < 0.08:
        issues.append("no_sent_history")
    if notification_presence_ratio > 0.8:
        issues.append("notifications_always_present")
    if notification_presence_ratio < 0.2:
        issues.append("too_few_notifications")
    if direct_group_candidate_avg > 1.0:
        issues.append("too_many_group_distractors_for_direct_targets")
    if percentile(gold_deltas, 0.5) < 600:
        issues.append("gold_deltas_too_local")
    if len(unique_previews) < min(250, max(60, len(samples) // 8)):
        issues.append("too_few_unique_previews")
    if len(unique_windows) < 40:
        issues.append("too_few_latent_windows")
    if unique_display_name_gold_ratio > 0.7:
        issues.append("display_name_shortcut_too_strong")
    if unique_exact_participant_gold_ratio > 0.7:
        issues.append("participant_geometry_shortcut_too_strong")
    if same_sender_multi_ratio < 0.2:
        issues.append("too_few_same_sender_collisions")
    if stale_same_sender_ratio < 0.05:
        issues.append("too_few_stale_same_sender_cases")
    if generic_history_collision_ratio < 0.1:
        issues.append("generic_history_collision_too_low")
    if cross_window_distractor_ratio > 0.65:
        issues.append("too_many_cross_window_fillers")
    if explicit_target_identity_ratio < 0.6:
        issues.append("too_few_explicit_target_identities")
    if shared_explicit_distractor_ratio < 0.1:
        issues.append("too_few_shared_explicit_identity_distractors")
    if incompatible_explicit_distractor_ratio < 0.2:
        issues.append("too_few_explicit_identity_conflicts")
    quality_score = min(100, quality_score)

    return {
        "sample_count": len(samples),
        "avg_candidate_count": round(sum(candidate_counts) / max(len(candidate_counts), 1), 3),
        "group_ratio": round(group_ratio, 4),
        "generic_ratio": round(generic_ratio, 4),
        "sent_target_ratio": round(sent_target_ratio, 4),
        "sent_history_ratio": round(sent_history_ratio, 4),
        "notification_presence_ratio": round(notification_presence_ratio, 4),
        "missing_sender_ratio": round(missing_sender_count / sample_count, 4),
        "blank_body_ratio": round(blank_body_count / sample_count, 4),
        "truncated_notification_ratio": round(truncated_notification_ratio, 4),
        "multi_plausible_ratio": round(multi_plausible_ratio, 4),
        "overlap_heavy_ratio": round(overlap_heavy_ratio, 4),
        "unique_preview_count": len(unique_previews),
        "unique_window_count": len(unique_windows),
        "direct_group_candidate_avg": round(direct_group_candidate_avg, 4),
        "unique_display_name_gold_ratio": round(unique_display_name_gold_ratio, 4),
        "unique_exact_participant_gold_ratio": round(unique_exact_participant_gold_ratio, 4),
        "same_sender_multi_ratio": round(same_sender_multi_ratio, 4),
        "stale_same_sender_ratio": round(stale_same_sender_ratio, 4),
        "generic_history_collision_ratio": round(generic_history_collision_ratio, 4),
        "cross_window_distractor_ratio": round(cross_window_distractor_ratio, 4),
        "explicit_target_identity_ratio": round(explicit_target_identity_ratio, 4),
        "explicit_gold_identity_ratio": round(explicit_gold_identity_ratio, 4),
        "explicit_nonself_participant_ratio": round(explicit_nonself_participant_ratio, 4),
        "shared_explicit_distractor_ratio": round(shared_explicit_distractor_ratio, 4),
        "incompatible_explicit_distractor_ratio": round(incompatible_explicit_distractor_ratio, 4),
        "runtime_replay_failure_ratio": round(runtime_replay_failure_ratio, 4),
        "replay_contamination_ratio": round(replay_contamination_ratio, 4),
        "gold_delta_mean_seconds": round(sum(gold_deltas) / max(len(gold_deltas), 1), 2),
        "gold_delta_p50_seconds": round(percentile(gold_deltas, 0.5), 2),
        "gold_delta_p90_seconds": round(percentile(gold_deltas, 0.9), 2),
        "corruption_profiles": profile_counts,
        "ambiguity_reasons": reason_counts,
        "quality_score": quality_score,
        "issues": issues,
    }


def dump_validation_report(dataset_path: str, payload: dict[str, Any]) -> str:
    return json.dumps(
        {
            "dataset": dataset_path,
            "sample_count": payload["sample_count"],
            "avg_candidate_count": payload["avg_candidate_count"],
            "group_ratio": payload["group_ratio"],
            "generic_ratio": payload["generic_ratio"],
            "sent_target_ratio": payload["sent_target_ratio"],
            "sent_history_ratio": payload["sent_history_ratio"],
            "notification_presence_ratio": payload["notification_presence_ratio"],
            "missing_sender_ratio": payload["missing_sender_ratio"],
            "blank_body_ratio": payload["blank_body_ratio"],
            "truncated_notification_ratio": payload["truncated_notification_ratio"],
            "multi_plausible_ratio": payload["multi_plausible_ratio"],
            "overlap_heavy_ratio": payload["overlap_heavy_ratio"],
            "unique_preview_count": payload["unique_preview_count"],
            "unique_window_count": payload["unique_window_count"],
            "direct_group_candidate_avg": payload["direct_group_candidate_avg"],
            "unique_display_name_gold_ratio": payload["unique_display_name_gold_ratio"],
            "unique_exact_participant_gold_ratio": payload["unique_exact_participant_gold_ratio"],
            "same_sender_multi_ratio": payload["same_sender_multi_ratio"],
            "stale_same_sender_ratio": payload["stale_same_sender_ratio"],
            "generic_history_collision_ratio": payload["generic_history_collision_ratio"],
            "cross_window_distractor_ratio": payload["cross_window_distractor_ratio"],
            "explicit_target_identity_ratio": payload["explicit_target_identity_ratio"],
            "explicit_gold_identity_ratio": payload["explicit_gold_identity_ratio"],
            "explicit_nonself_participant_ratio": payload["explicit_nonself_participant_ratio"],
            "shared_explicit_distractor_ratio": payload["shared_explicit_distractor_ratio"],
            "incompatible_explicit_distractor_ratio": payload["incompatible_explicit_distractor_ratio"],
            "runtime_replay_failure_ratio": payload["runtime_replay_failure_ratio"],
            "replay_contamination_ratio": payload["replay_contamination_ratio"],
            "gold_delta_mean_seconds": payload["gold_delta_mean_seconds"],
            "gold_delta_p50_seconds": payload["gold_delta_p50_seconds"],
            "gold_delta_p90_seconds": payload["gold_delta_p90_seconds"],
            "quality_score": payload["quality_score"],
            "issues": payload["issues"],
            "top_profiles": payload["corruption_profiles"].most_common(6),
            "top_ambiguity_reasons": payload["ambiguity_reasons"].most_common(8),
        },
        indent=2,
        ensure_ascii=False,
    )
