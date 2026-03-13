#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anthropic
from dotenv import dotenv_values


def _default_env_path() -> Path:
    override = os.getenv("THREAD_MODEL_ENV_FILE") or os.getenv("ADIT_ANTHROPIC_ENV_FILE")
    if override:
        return Path(override).expanduser()
    cwd_env = Path(".env")
    if cwd_env.exists():
        return cwd_env
    home_env = Path.home() / ".env"
    if home_env.exists():
        return home_env
    return cwd_env


DEFAULT_ENV_PATH = _default_env_path()
DEFAULT_MODEL = "claude-opus-4-6"

DEFAULT_STYLE_PACKS: dict[str, list[Any]] = {
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
        "Priya",
        "Leila",
        "Noor",
        "Soph",
        "Bea",
        "Tomás",
        "Jake",
    ],
    "service_senders": [
        "42302",
        "24273",
        "OpenTable",
        "DoorDash",
        "CVS",
        "Walgreens",
        "United",
        "Delta",
        "Uber",
        "Lyft",
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
    "group_title_variants": [
        "family",
        "the cousins",
        "weekend crew",
        "to you, mom, dad +3",
    ],
    "ambiguous_messages": [
        {
            "text": "teehee",
            "intent_tag": "generic_ack",
            "plausible_in_group": True,
            "plausible_in_direct": True,
            "style_tags": ["generic", "light"],
        }
    ],
}

STYLE_PACKS_TOOL = {
    "name": "emit_style_packs",
    "description": "Emit reusable language/style packs for synthetic message-thread datasets.",
    "input_schema": {
        "type": "object",
        "properties": {
            "family_names": {"type": "array", "items": {"type": "string"}},
            "friend_names": {"type": "array", "items": {"type": "string"}},
            "service_senders": {"type": "array", "items": {"type": "string"}},
            "generic_replies": {"type": "array", "items": {"type": "string"}},
            "planning_lines": {"type": "array", "items": {"type": "string"}},
            "reaction_templates": {"type": "array", "items": {"type": "string"}},
            "group_title_variants": {"type": "array", "items": {"type": "string"}},
            "ambiguous_messages": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "text": {"type": "string"},
                        "intent_tag": {"type": "string"},
                        "plausible_in_group": {"type": "boolean"},
                        "plausible_in_direct": {"type": "boolean"},
                        "style_tags": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": [
                        "text",
                        "intent_tag",
                        "plausible_in_group",
                        "plausible_in_direct",
                        "style_tags",
                    ],
                },
            },
        },
        "required": [
            "family_names",
            "friend_names",
            "service_senders",
            "generic_replies",
            "planning_lines",
            "reaction_templates",
            "group_title_variants",
            "ambiguous_messages",
        ],
    },
}

THREAD_WINDOWS_TOOL = {
    "name": "emit_thread_windows",
    "description": "Emit simple realistic overlapping thread windows for synthetic message-thread matching datasets.",
    "input_schema": {
        "type": "object",
        "properties": {
            "windows": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "window_id": {"type": "string"},
                        "theme": {"type": "string"},
                        "self_participant_id": {"type": "string"},
                        "participants": {"type": "array", "items": {"type": "string"}},
                        "ambiguous_targets": {
                            "type": "array",
                            "minItems": 2,
                            "maxItems": 3,
                            "items": {
                                "type": "object",
                                "properties": {
                                    "target_id": {"type": "string"},
                                    "gold_thread_id": {"type": "string"},
                                    "speaker_id": {"type": "string"},
                                    "text": {"type": "string"},
                                    "minutes_ago": {"type": "integer"},
                                    "kind": {"type": "string"},
                                    "ambiguity_reasons": {"type": "array", "items": {"type": "string"}},
                                    "plausible_thread_ids": {"type": "array", "items": {"type": "string"}},
                                },
                                "required": [
                                    "target_id",
                                    "gold_thread_id",
                                    "speaker_id",
                                    "text",
                                    "minutes_ago",
                                    "ambiguity_reasons",
                                    "kind",
                                    "plausible_thread_ids",
                                ],
                            },
                        },
                        "threads": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "thread_id": {"type": "string"},
                                    "type": {"type": "string"},
                                    "name": {"type": "string"},
                                    "sender_kind": {"type": "string"},
                                    "activity_state": {"type": "string"},
                                    "shared_entity": {"type": "string"},
                                    "domain_tags": {"type": "array", "items": {"type": "string"}},
                                    "participants": {"type": "array", "items": {"type": "string"}},
                                    "messages": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "from": {"type": "string"},
                                                "body": {"type": "string"},
                                                "ts": {"type": "string"},
                                            },
                                            "required": ["from", "body", "ts"],
                                        },
                                    },
                                },
                                "required": ["thread_id", "type", "name", "participants", "messages"],
                            },
                        },
                    },
                    "required": ["window_id", "theme", "self_participant_id", "participants", "ambiguous_targets", "threads"],
                },
            }
        },
        "required": ["windows"],
    },
}

class BankGenerationError(RuntimeError):
    pass


@dataclass
class AnthropicConfig:
    api_key: str
    model: str
    env_file: Path | None
    timeout_seconds: float


def load_anthropic_config(
    *,
    env_file: Path | None = None,
    api_key_env: str = "ANTHROPIC_API_KEY",
    model: str | None = None,
) -> AnthropicConfig:
    env_path = env_file or DEFAULT_ENV_PATH
    env_values = dotenv_values(env_path) if env_path and env_path.exists() else {}
    api_key = os.getenv(api_key_env) or env_values.get(api_key_env)
    if not api_key:
        raise BankGenerationError(
            f"Missing {api_key_env}. Set it in the environment or in {env_path}."
        )

    resolved_model = (
        model
        or os.getenv("THREAD_MODEL_ANTHROPIC_MODEL")
        or os.getenv("CLAUDE_MODEL")
        or env_values.get("CLAUDE_MODEL")
        or DEFAULT_MODEL
    )
    timeout_seconds = float(
        os.getenv("THREAD_MODEL_ANTHROPIC_TIMEOUT_SECONDS")
        or env_values.get("THREAD_MODEL_ANTHROPIC_TIMEOUT_SECONDS")
        or 120
    )
    return AnthropicConfig(
        api_key=api_key,
        model=resolved_model,
        env_file=env_path if env_path.exists() else None,
        timeout_seconds=timeout_seconds,
    )


def merge_style_packs(override: dict[str, Any] | None = None) -> dict[str, list[Any]]:
    merged: dict[str, list[Any]] = {
        key: list(value)
        for key, value in DEFAULT_STYLE_PACKS.items()
    }
    if not override:
        return merged

    for key, value in override.items():
        if not isinstance(value, list) or not value:
            continue
        base = merged.get(key, [])
        if key == "ambiguous_messages":
            seen = {
                str(item.get("text", "")).strip().lower()
                for item in base
                if isinstance(item, dict)
            }
            merged[key] = list(base)
            for item in value:
                token = str(item.get("text", "")).strip().lower() if isinstance(item, dict) else ""
                if not token:
                    continue
                if token not in seen:
                    merged[key].append(item)
                    seen.add(token)
            continue

        seen = {str(item).strip().lower() for item in base}
        merged[key] = list(base)
        for item in value:
            normalized = str(item).strip()
            if not normalized:
                continue
            token = normalized.lower()
            if token not in seen:
                merged[key].append(normalized)
                seen.add(token)

    return merged


def _normalize_string_list(values: list[Any], *, placeholder: str | None = None) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        if placeholder and placeholder not in text:
            continue
        token = text.lower()
        if token in seen:
            continue
        seen.add(token)
        result.append(text)
    return result


def _normalize_ambiguous_messages(values: list[Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in values:
        if not isinstance(item, dict):
            continue
        text = str(item.get("text", "")).strip()
        if not text:
            continue
        normalized = {
            "text": text,
            "intent_tag": str(item.get("intent_tag", "generic")).strip() or "generic",
            "plausible_in_group": bool(item.get("plausible_in_group", True)),
            "plausible_in_direct": bool(item.get("plausible_in_direct", True)),
            "style_tags": [str(tag).strip() for tag in item.get("style_tags", []) if str(tag).strip()],
        }
        token = normalized["text"].lower()
        if token in seen:
            continue
        seen.add(token)
        result.append(normalized)
    return result


def normalize_style_packs(raw: dict[str, Any]) -> dict[str, list[Any]]:
    packs = raw.get("style_packs", raw)
    return merge_style_packs(
        {
            "family_names": _normalize_string_list(packs.get("family_names", [])),
            "friend_names": _normalize_string_list(packs.get("friend_names", [])),
            "service_senders": _normalize_string_list(packs.get("service_senders", [])),
            "generic_replies": _normalize_string_list(packs.get("generic_replies", [])),
            "planning_lines": _normalize_string_list(packs.get("planning_lines", [])),
            "reaction_templates": _normalize_string_list(
                packs.get("reaction_templates", []),
                placeholder="{preview}",
            ),
            "group_title_variants": _normalize_string_list(packs.get("group_title_variants", [])),
            "ambiguous_messages": _normalize_ambiguous_messages(packs.get("ambiguous_messages", [])),
        }
    )


def _pick_rotating_examples(values: list[str], *, count: int, batch_index: int) -> list[str]:
    if not values:
        return []
    if len(values) <= count:
        return values
    offset = (batch_index * max(1, count - 1)) % len(values)
    rotated = values[offset:] + values[:offset]
    return rotated[:count]


def _format_ambiguous_examples(values: list[dict[str, Any]], *, count: int, batch_index: int) -> str:
    if not values:
        return ""
    offset = (batch_index * max(1, count - 1)) % len(values)
    rotated = values[offset:] + values[:offset]
    picked = rotated[:count]
    formatted = []
    for item in picked:
        mode = []
        if item.get("plausible_in_direct"):
            mode.append("direct")
        if item.get("plausible_in_group"):
            mode.append("group")
        mode_text = "/".join(mode) if mode else "unknown"
        formatted.append(f"{item['text']} [{item.get('intent_tag', 'generic')}|{mode_text}]")
    return ", ".join(formatted)


def _infer_self_participant_id(participant_ids: set[str], threads: list[dict[str, Any]]) -> str | None:
    coverage: dict[str, int] = {}
    for thread in threads:
        for participant_id in thread.get("participant_ids", []):
            if participant_id in participant_ids:
                coverage[participant_id] = coverage.get(participant_id, 0) + 1
    if not coverage:
        return None
    best_id, _ = max(
        coverage.items(),
        key=lambda item: (item[1], item[0]),
    )
    return best_id


def load_style_packs(path: Path | None) -> dict[str, list[Any]]:
    if not path:
        return merge_style_packs()
    if not path.exists():
        return merge_style_packs()
    raw = json.loads(path.read_text(encoding="utf-8"))
    return normalize_style_packs(raw)


def normalize_thread_windows(raw: dict[str, Any]) -> list[dict[str, Any]]:
    windows = raw.get("windows", raw)
    if not isinstance(windows, list):
        raise BankGenerationError("Thread-window payload must contain a windows array.")

    normalized_windows: list[dict[str, Any]] = []
    seen_window_ids: dict[str, int] = {}
    for index, item in enumerate(windows):
        if not isinstance(item, dict):
            continue

        participants = []
        participant_ids: set[str] = set()
        participant_name_to_id: dict[str, str] = {}
        raw_participants = item.get("participants", [])
        for participant_index, participant in enumerate(raw_participants):
            if isinstance(participant, str):
                display_name = participant.strip()
                participant_id = f"p{participant_index + 1}"
                aliases = []
                relationship = None
                style_tags = []
            elif isinstance(participant, dict):
                participant_id = str(participant.get("id", "")).strip() or f"p{participant_index + 1}"
                display_name = str(participant.get("display_name", "")).strip()
                aliases = _normalize_string_list(participant.get("aliases", []))
                relationship = str(participant.get("relationship", "")).strip() or None
                style_tags = [str(tag).strip() for tag in participant.get("style_tags", []) if str(tag).strip()]
            else:
                continue
            if not participant_id or not display_name:
                continue
            participant_ids.add(participant_id)
            participant_name_to_id[display_name.lower()] = participant_id
            participants.append(
                {
                    "id": participant_id,
                    "display_name": display_name,
                    "aliases": aliases,
                    "relationship": relationship,
                    "style_tags": style_tags,
                }
            )

        parsed_timestamps: list[datetime] = []
        for thread in item.get("threads", []):
            if not isinstance(thread, dict):
                continue
            for message in thread.get("messages", []):
                if not isinstance(message, dict):
                    continue
                raw_ts = message.get("ts")
                if not raw_ts:
                    continue
                try:
                    parsed_timestamps.append(datetime.fromisoformat(str(raw_ts)))
                except ValueError:
                    continue
        base_ts = max(parsed_timestamps) if parsed_timestamps else None

        raw_window_id = str(item.get("window_id", f"window-{index+1}")).strip() or f"window-{index+1}"
        seen_window_ids[raw_window_id] = seen_window_ids.get(raw_window_id, 0) + 1
        window_id = raw_window_id if seen_window_ids[raw_window_id] == 1 else f"{raw_window_id}__{seen_window_ids[raw_window_id]}"

        threads = []
        thread_ids: set[str] = set()
        thread_id_map: dict[str, str] = {}
        for thread in item.get("threads", []):
            if not isinstance(thread, dict):
                continue
            raw_thread_id = str(thread.get("thread_id", "")).strip()
            display_name = str(thread.get("display_name", "") or thread.get("name", "")).strip()
            raw_participant_refs = thread.get("participant_ids", []) or thread.get("participants", [])
            participant_refs = []
            for value in raw_participant_refs:
                token = str(value).strip()
                if not token:
                    continue
                participant_id = token if token in participant_ids else participant_name_to_id.get(token.lower())
                if participant_id and participant_id not in participant_refs:
                    participant_refs.append(participant_id)
            if not raw_thread_id or not display_name or not participant_refs:
                continue
            thread_id = f"{window_id}::{raw_thread_id}"
            messages = []
            for message in thread.get("messages", []):
                if not isinstance(message, dict):
                    continue
                raw_speaker = str(message.get("speaker_id", "") or message.get("from", "")).strip()
                speaker_id = raw_speaker if raw_speaker in participant_ids else participant_name_to_id.get(raw_speaker.lower(), "")
                text = str(message.get("text", "") or message.get("body", "")).strip()
                if not speaker_id or not text or speaker_id not in participant_ids:
                    continue
                minutes_ago = message.get("minutes_ago")
                if minutes_ago is None and message.get("ts") and base_ts:
                    try:
                        parsed_ts = datetime.fromisoformat(str(message.get("ts")))
                        minutes_ago = max(0, int((base_ts - parsed_ts).total_seconds() // 60))
                    except ValueError:
                        minutes_ago = 0
                messages.append(
                    {
                        "speaker_id": speaker_id,
                        "text": text,
                        "minutes_ago": int(minutes_ago or 0),
                        "kind": str(message.get("kind", "plain")).strip() or "plain",
                    }
                )
            min_history_messages = 0 if bool(thread.get("allow_sparse_history")) else 3
            if len(messages) < min_history_messages:
                continue
            thread_ids.add(thread_id)
            thread_id_map[raw_thread_id] = thread_id
            threads.append(
                {
                    "thread_id": thread_id,
                    "display_name": display_name,
                    "is_group": bool(
                        thread.get(
                            "is_group",
                            (
                                str(thread.get("type", "")).lower() == "group"
                                if thread.get("type") is not None
                                else len(participant_refs) > 1
                            ),
                        )
                    ),
                    "participant_ids": participant_refs,
                    "theme": str(thread.get("theme", "")).strip() or None,
                    "sender_kind": str(thread.get("sender_kind", "")).strip() or None,
                    "activity_state": str(thread.get("activity_state", "")).strip() or None,
                    "shared_entity": str(thread.get("shared_entity", "")).strip() or None,
                    "domain_tags": [str(tag).strip() for tag in thread.get("domain_tags", []) if str(tag).strip()],
                    "messages": sorted(messages, key=lambda message: message["minutes_ago"], reverse=True),
                }
            )

        raw_self_id = str(item.get("self_participant_id", "")).strip()
        self_participant_id = raw_self_id if raw_self_id in participant_ids else _infer_self_participant_id(participant_ids, threads)
        if not self_participant_id:
            continue

        self_scoped_threads = []
        self_scoped_thread_ids: set[str] = set()
        for thread in threads:
            participant_refs = list(dict.fromkeys(thread["participant_ids"]))
            if self_participant_id not in participant_refs:
                if not thread["is_group"] and len(participant_refs) == 1:
                    participant_refs = [self_participant_id, participant_refs[0]]
                else:
                    continue

            nonself_participants = [value for value in participant_refs if value != self_participant_id]
            if not nonself_participants:
                continue

            normalized_thread = {
                **thread,
                "participant_ids": [self_participant_id, *nonself_participants],
                "is_group": thread["is_group"] or len(nonself_participants) > 1,
            }
            self_scoped_threads.append(normalized_thread)
            self_scoped_thread_ids.add(normalized_thread["thread_id"])
        threads = self_scoped_threads
        thread_ids = self_scoped_thread_ids

        ambiguous_targets = []
        for target in item.get("ambiguous_targets", []):
            if not isinstance(target, dict):
                continue
            target_id = str(target.get("target_id", "")).strip()
            raw_gold_thread_id = str(target.get("gold_thread_id", "")).strip()
            raw_speaker_id = str(target.get("speaker_id", "")).strip()
            speaker_id = (
                raw_speaker_id
                if raw_speaker_id in participant_ids
                else participant_name_to_id.get(raw_speaker_id.lower(), "")
            )
            text = str(target.get("text", "")).strip()
            raw_plausible_thread_ids = [
                str(value).strip()
                for value in target.get("plausible_thread_ids", [])
                if str(value).strip()
            ]
            gold_thread_id = thread_id_map.get(raw_gold_thread_id, "")
            plausible_thread_ids = [
                thread_id_map[value]
                for value in raw_plausible_thread_ids
                if value in thread_id_map
            ]
            if (
                not target_id
                or not gold_thread_id
                or speaker_id not in participant_ids
                or not text
                or len(plausible_thread_ids) < 2
            ):
                continue
            ambiguous_targets.append(
                {
                    "target_id": target_id,
                    "gold_thread_id": gold_thread_id,
                    "speaker_id": speaker_id,
                    "text": text,
                    "minutes_ago": int(target.get("minutes_ago", 0)),
                    "kind": str(target.get("kind", "plain")).strip() or "plain",
                    "ambiguity_reasons": [str(value).strip() for value in target.get("ambiguity_reasons", []) if str(value).strip()],
                    "plausible_thread_ids": [value for value in plausible_thread_ids if value in thread_ids],
                }
            )

        if len(participants) < 3 or len(threads) < 3:
            continue
        if not any(not thread["is_group"] for thread in threads):
            continue
        if not any(thread["is_group"] for thread in threads):
            continue

        normalized_windows.append(
            {
                "window_id": window_id,
                "theme": str(item.get("theme", "")).strip() or None,
                "self_participant_id": self_participant_id,
                "participants": participants,
                "threads": threads,
                "ambiguous_targets": ambiguous_targets,
            }
        )

    if not normalized_windows:
        raise BankGenerationError("Thread-window payload contained no usable windows.")
    return normalized_windows


def _extract_jsonl_objects(text: str) -> list[dict[str, Any]]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    objects: list[dict[str, Any]] = []
    for line in lines:
        if not line.startswith("{"):
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            objects.append(value)
    if not objects:
        raise BankGenerationError("Claude response did not contain any parseable JSONL windows.")
    return objects


def load_thread_windows(path: Path | None) -> list[dict[str, Any]]:
    if not path:
        return []
    if not path.exists():
        raise BankGenerationError(f"Thread-window file not found: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    return normalize_thread_windows(raw)


def build_style_pack_prompt() -> str:
    return """
Generate reusable message-thread synthetic data banks.

Rules:
- Emit only tool input. No prose.
- All strings should look like natural text messages or contact names.
- Keep everything modern, casual, and realistic.
- Prefer short messages that could plausibly collide across different threads.
- Include family logistics, friend banter, roommate/coworker/class coordination, vague acknowledgements, reactions, and terse service/business senders.
- The reaction_templates strings MUST contain the exact placeholder {preview}.
- group_title_variants should include natural group names and truncation-like descriptors.
- service_senders should include a mix of shortcodes and app/business sender names.
- Avoid duplicates and placeholders like <name>.
- No slurs, no explicit content, no policy talk, no AI/meta references.
""".strip()


def build_thread_window_prompt(
    *,
    batch_index: int,
    window_count: int,
    style_packs: dict[str, list[Any]],
) -> str:
    family_names = ", ".join(_pick_rotating_examples(style_packs["family_names"], count=12, batch_index=batch_index))
    friend_names = ", ".join(_pick_rotating_examples(style_packs["friend_names"], count=12, batch_index=batch_index))
    service_senders = ", ".join(_pick_rotating_examples(style_packs["service_senders"], count=10, batch_index=batch_index))
    generic_examples = ", ".join(_pick_rotating_examples(style_packs["generic_replies"], count=12, batch_index=batch_index))
    planning_examples = ", ".join(_pick_rotating_examples(style_packs["planning_lines"], count=12, batch_index=batch_index))
    group_examples = ", ".join(_pick_rotating_examples(style_packs["group_title_variants"], count=10, batch_index=batch_index))
    ambiguous_examples = _format_ambiguous_examples(style_packs["ambiguous_messages"], count=14, batch_index=batch_index)

    archetypes = [
        "family logistics + service overlap",
        "friends + event planning",
        "roommates + package/delivery",
        "coworker/class coordination",
        "travel + pickup coordination",
        "hobby/sports + side direct pings",
    ]
    primary_archetype = archetypes[batch_index % len(archetypes)]
    secondary_archetype = archetypes[(batch_index + 2) % len(archetypes)]

    return f"""
Emit exactly {window_count} windows as structured tool input.

Rules:
- 4 to 8 participants total per window.
- 3 to 6 concurrent threads per window.
- Each window must represent one phone owner's world. Set `self_participant_id` to that person.
- Every thread must include `self_participant_id`.
- Direct threads should be `self + one other`.
- Group threads should be `self + two or more others`.
- At least one direct thread and one group thread.
- Across the whole batch, cover at least four of these domains:
  - family logistics
  - close-friend banter/check-ins
  - roommates / housing logistics
  - coworkers / class-project coordination
  - hobby / sports / event planning
  - service / business / shortcode transactional threads
- Bias this batch toward: `{primary_archetype}` and `{secondary_archetype}`.
- At least one thread in most windows should feel friend-coded, roommate-coded, coworker-coded, or service-coded, not family-coded.
- At least two threads per window must share at least one participant.
- Include outbound/self turns in many windows, not just inbound-only traffic.
- Some windows should contain one stale-but-correct thread where the last relevant message is 30 to 1440 minutes older than a recent plausible distractor.
- Every window should include 2 to 3 explicit `ambiguous_targets`.
- Most ambiguous targets should be hard for structure alone:
  - at least one other plausible thread should have the same visible participant count as the gold thread
  - in many windows, the gold thread and a distractor should share the same sender/speaker across threads
  - in many windows, the gold thread should be older/staler while a wrong but plausible thread is more recent
  - many ambiguous targets should be generic or underspecified enough that semantics or discourse fit matters
- Across the batch, include multiple windows where a reaction-style or quoted-message target belongs to an older thread, while the same sender is active in a newer plausible distractor thread.
- For those reaction/quoted targets, make the quoted content actually match the gold thread's prior messages rather than random filler.
- Include several windows where the only strong clue is topical continuity or discourse fit, not participant geometry or recency alone.
- Across the batch, include multiple windows where the same person is active in multiple direct/group threads at once.
- Across the batch, include multiple windows where the same generic text could plausibly fit 2 to 4 threads.
- Use `sender_kind`, `activity_state`, `shared_entity`, and `domain_tags` when they help express why threads are confusable.
- Prefer to set `sender_kind`, `activity_state`, `shared_entity`, and `domain_tags` on most threads, not just occasionally.
- Messages should feel mundane and realistic:
  - generic replies like {generic_examples}
  - planning/check-in language like {planning_examples}
  - short ambiguous texts like {ambiguous_examples}
- Names should feel like family/friends inspired by family=[{family_names}] and friends=[{friend_names}].
- Service/business senders can be inspired by [{service_senders}].
- Group names/descriptors should feel like {group_examples}.
- Include plenty of generic short replies, overlapping family logistics, and same-people-in-multiple-threads situations.
- Include at least one service/business or shortcode thread somewhere in the batch.
- Include at least one non-family-heavy window where the main ambiguity is among friends, roommates, coworkers, classmates, or services.
- Vary recency: not every thread should be hot and recent.
- Do not make the ambiguity trivial through unique thread names. The point is overlapping humans, overlapping senders, recency conflicts, and mundane language.
- Do not use placeholders like <name>.
- Do not use explicit sexual content, slurs, or AI/meta references.
- Do not make every window dramatic. Most should be mundane.
""".strip()


def _create_client(config: AnthropicConfig) -> anthropic.Anthropic:
    return anthropic.Anthropic(api_key=config.api_key, timeout=config.timeout_seconds)


def _extract_tool_input(response: Any, tool_name: str) -> dict[str, Any]:
    for block in response.content:
        if getattr(block, "type", None) == "tool_use" and getattr(block, "name", None) == tool_name:
            payload = getattr(block, "input", None)
            if isinstance(payload, dict):
                return payload
    raise BankGenerationError(f"Claude response did not contain tool input for {tool_name}.")


def _request_thread_windows_batch(
    *,
    client: anthropic.Anthropic,
    model: str,
    batch_index: str,
    window_count: int,
    style_packs: dict[str, list[Any]],
    max_tokens: int,
) -> list[dict[str, Any]]:
    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        temperature=0.9,
        system="You generate realistic overlapping texting windows as structured tool input for a synthetic message-thread matching dataset.",
        tools=[THREAD_WINDOWS_TOOL],
        tool_choice={"type": "tool", "name": THREAD_WINDOWS_TOOL["name"]},
        messages=[
            {
                "role": "user",
                "content": build_thread_window_prompt(
                    batch_index=int(str(batch_index).split(".")[0]),
                    window_count=window_count,
                    style_packs=style_packs,
                ),
            }
        ],
    )
    return normalize_thread_windows(_extract_tool_input(response, THREAD_WINDOWS_TOOL["name"]))


def _request_thread_windows_with_fallback(
    *,
    client: anthropic.Anthropic,
    model: str,
    batch_label: str,
    window_count: int,
    style_packs: dict[str, list[Any]],
    max_tokens: int,
) -> list[dict[str, Any]]:
    try:
        return _request_thread_windows_batch(
            client=client,
            model=model,
            batch_index=batch_label,
            window_count=window_count,
            style_packs=style_packs,
            max_tokens=max_tokens,
        )
    except BankGenerationError:
        if window_count <= 1:
            last_error: BankGenerationError | None = None
            for attempt in range(1, 4):
                try:
                    return _request_thread_windows_batch(
                        client=client,
                        model=model,
                        batch_index=f"{batch_label}.retry{attempt}",
                        window_count=window_count,
                        style_packs=style_packs,
                        max_tokens=max_tokens,
                    )
                except BankGenerationError as exc:
                    last_error = exc
            if last_error is not None:
                raise last_error
            raise
        first_count = max(1, window_count // 2)
        second_count = window_count - first_count
        split_max_tokens = max(3200, max_tokens // 2)
        return [
            *_request_thread_windows_with_fallback(
                client=client,
                model=model,
                batch_label=f"{batch_label}.1",
                window_count=first_count,
                style_packs=style_packs,
                max_tokens=split_max_tokens,
            ),
            *_request_thread_windows_with_fallback(
                client=client,
                model=model,
                batch_label=f"{batch_label}.2",
                window_count=second_count,
                style_packs=style_packs,
                max_tokens=split_max_tokens,
            ),
        ]


def generate_style_packs(
    *,
    out_path: Path,
    env_file: Path | None = None,
    model: str | None = None,
    max_tokens: int = 12000,
) -> dict[str, Any]:
    config = load_anthropic_config(env_file=env_file, model=model)
    client = _create_client(config)
    response = client.messages.create(
        model=config.model,
        max_tokens=max_tokens,
        temperature=0.8,
        system="You generate reusable language banks for synthetic messaging and thread-ranking datasets.",
        tools=[STYLE_PACKS_TOOL],
        tool_choice={"type": "tool", "name": STYLE_PACKS_TOOL["name"]},
        messages=[{"role": "user", "content": build_style_pack_prompt()}],
    )

    normalized_packs = normalize_style_packs(_extract_tool_input(response, STYLE_PACKS_TOOL["name"]))
    payload = {
        "meta": {
            "kind": "style_packs",
            "model": config.model,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "env_file": str(config.env_file) if config.env_file else None,
        },
        "style_packs": normalized_packs,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload


def generate_thread_windows(
    *,
    out_path: Path,
    style_packs: dict[str, list[Any]] | None = None,
    env_file: Path | None = None,
    model: str | None = None,
    batch_count: int = 1,
    windows_per_batch: int = 10,
    max_tokens: int = 16000,
) -> dict[str, Any]:
    config = load_anthropic_config(env_file=env_file, model=model)
    client = _create_client(config)
    merged_style_packs = merge_style_packs(style_packs)

    windows: list[dict[str, Any]] = []
    for batch_index in range(batch_count):
        print(
            f"[thread-model] generating Opus windows batch {batch_index + 1}/{batch_count} "
            f"(target={windows_per_batch}, model={config.model}, timeout={config.timeout_seconds}s)",
            flush=True,
        )
        windows.extend(
            _request_thread_windows_with_fallback(
                client=client,
                model=config.model,
                batch_label=str(batch_index + 1),
                window_count=windows_per_batch,
                style_packs=merged_style_packs,
                max_tokens=max_tokens,
            )
        )

    payload = {
        "meta": {
            "kind": "thread_windows",
            "model": config.model,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "batch_count": batch_count,
            "windows_per_batch": windows_per_batch,
            "window_count": len(windows),
            "env_file": str(config.env_file) if config.env_file else None,
        },
        "windows": windows,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Anthropic-backed language banks and realistic thread windows.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    style_parser = subparsers.add_parser("generate-style-packs", help="Generate reusable language/style packs.")
    style_parser.add_argument("--out", type=Path, required=True)
    style_parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_PATH)
    style_parser.add_argument("--model", type=str, default=DEFAULT_MODEL)
    style_parser.add_argument("--max-tokens", type=int, default=12000)

    window_parser = subparsers.add_parser("generate-thread-windows", help="Generate realistic overlapping thread windows.")
    window_parser.add_argument("--out", type=Path, required=True)
    window_parser.add_argument("--style-packs", type=Path)
    window_parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_PATH)
    window_parser.add_argument("--model", type=str, default=DEFAULT_MODEL)
    window_parser.add_argument("--batch-count", type=int, default=1)
    window_parser.add_argument("--windows-per-batch", type=int, default=10)
    window_parser.add_argument("--max-tokens", type=int, default=16000)

    args = parser.parse_args()

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

    raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
