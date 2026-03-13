#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Sequence


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


def normalize_text(value: str | None) -> str:
    return "" if not value else " ".join(str(value).lower().split())


def digits_only(value: str | None) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def looks_generic(value: str | None) -> bool:
    normalized = normalize_text(value)
    if not normalized:
        return True
    if normalized in GENERIC_PREVIEWS:
        return True
    tokens = [token for token in normalized.replace(",", " ").replace(".", " ").split(" ") if token]
    return len(tokens) <= 2 and len(normalized) <= 18


def build_target_text(sample: dict[str, Any]) -> str:
    metadata = sample.get("metadata", {})
    preview = str(metadata.get("preview") or "").strip()
    if preview:
        return preview
    message = sample.get("message", {}).get("message", {})
    body = str(message.get("body") or "").strip()
    if body:
        return body
    subject = str(message.get("subject") or "").strip()
    return subject


def build_history_text(turn: dict[str, Any]) -> str:
    preview = str(turn.get("preview") or "").strip()
    if preview:
        return preview
    body = str(turn.get("body") or "").strip()
    return body


def parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = str(value).replace("Z", "+00:00")
    return datetime.fromisoformat(raw)


def find_participant_id_for_sender(
    sender_name: str | None,
    sender_addressing: str | None,
    thread_participants: list[dict[str, Any]],
    participant_id_by_key: dict[str, str],
    self_id: str,
) -> str:
    addressing_digits = digits_only(sender_addressing)
    sender_name_norm = normalize_text(sender_name)
    for participant in thread_participants:
        if participant.get("isSelf"):
            continue
        key = str(participant.get("key") or "")
        if key and key in participant_id_by_key:
            if sender_addressing and normalize_text(sender_addressing) == normalize_text(key):
                return participant_id_by_key[key]
            if addressing_digits and digits_only(key) == addressing_digits:
                return participant_id_by_key[key]
        if sender_name_norm:
            if sender_name_norm == normalize_text(participant.get("displayName")):
                return participant_id_by_key[key]
            phones = participant.get("phones", []) or []
            emails = participant.get("emails", []) or []
            if any(sender_name_norm == normalize_text(phone) for phone in phones):
                return participant_id_by_key[key]
            if any(sender_name_norm == normalize_text(email) for email in emails):
                return participant_id_by_key[key]
    return self_id


def participant_relationship(participant: dict[str, Any]) -> str:
    key = str(participant.get("key") or "")
    display_name = str(participant.get("displayName") or "")
    digits = digits_only(key) or digits_only(display_name)
    if 4 <= len(digits) <= 6:
        return "service"
    return "contact"


def sender_kind_for_thread(participants: list[dict[str, Any]]) -> str:
    nonself = [participant for participant in participants if not participant.get("isSelf")]
    if any(participant_relationship(participant) == "service" for participant in nonself):
        return "shortcode"
    return "person"


def domain_tags_for_thread(participants: list[dict[str, Any]], base_domain_tag: str) -> list[str]:
    tags = {base_domain_tag}
    if any(participant_relationship(participant) == "service" for participant in participants if not participant.get("isSelf")):
        tags.add("service")
    else:
        tags.add("human")
    return sorted(tags)


def convert_sample(
    sample: dict[str, Any],
    index: int,
    *,
    window_prefix: str,
    style_tag: str,
    theme_prefix: str,
    base_domain_tag: str,
    base_ambiguity_reasons: Sequence[str],
) -> dict[str, Any]:
    window_id = f"{window_prefix}_{index+1:03d}"
    self_id = "p_self"
    participant_defs: list[dict[str, Any]] = [
        {
            "id": self_id,
            "display_name": "Me",
            "aliases": ["Me", "name:Me"],
            "relationship": "self",
            "style_tags": ["self", "outbound"],
        }
    ]
    participant_id_by_key: dict[str, str] = {}

    def ensure_participant(participant: dict[str, Any]) -> str:
        key = str(participant.get("key") or "").strip()
        if not key:
            key = f"name:{normalize_text(participant.get('displayName')) or 'unknown'}"
        if key in participant_id_by_key:
            return participant_id_by_key[key]
        participant_id = f"p_{len(participant_id_by_key)+1:03d}"
        participant_id_by_key[key] = participant_id
        aliases = [str(participant.get("displayName") or key), key]
        aliases.extend(str(phone).strip() for phone in participant.get("phones", []) or [] if str(phone).strip())
        aliases.extend(str(email).strip() for email in participant.get("emails", []) or [] if str(email).strip())
        aliases = [alias for alias in aliases if alias]
        relationship = participant_relationship(participant)
        participant_defs.append(
            {
                "id": participant_id,
                "display_name": str(participant.get("displayName") or key),
                "aliases": list(dict.fromkeys(aliases)),
                "relationship": relationship,
                "style_tags": [relationship, style_tag],
            }
        )
        return participant_id

    target_participants = sample.get("message", {}).get("participants", []) or []
    for participant in target_participants:
        if not participant.get("isSelf"):
            ensure_participant(participant)
    for candidate in sample.get("candidate_threads", []):
        for participant in candidate.get("participants", []) or []:
            if not participant.get("isSelf"):
                ensure_participant(participant)

    threads: list[dict[str, Any]] = []
    plausible_thread_ids: list[str] = []
    target_sort_utc = parse_iso_utc(sample.get("message", {}).get("sort_utc"))
    for candidate_index, candidate in enumerate(sample.get("candidate_threads", [])):
        thread_id = f"{window_id}::t{candidate_index+1:02d}"
        plausible_thread_ids.append(thread_id)
        candidate_participants = candidate.get("participants", []) or []
        participant_ids = [self_id]
        participant_ids.extend(
            ensure_participant(participant)
            for participant in candidate_participants
            if not participant.get("isSelf")
        )
        participant_ids = list(dict.fromkeys(participant_ids))

        history_messages = []
        for turn_index, turn in enumerate(candidate.get("history", []) or []):
            turn_sort_utc = parse_iso_utc(turn.get("sort_utc"))
            minutes_ago = (
                max(1, int(round((target_sort_utc - turn_sort_utc).total_seconds() / 60)))
                if target_sort_utc is not None and turn_sort_utc is not None
                else max(1, turn_index + 1)
            )
            speaker_id = (
                self_id
                if str(turn.get("folder") or "").lower() == "sent"
                else find_participant_id_for_sender(
                    turn.get("sender_name"),
                    turn.get("sender_addressing"),
                    candidate_participants,
                    participant_id_by_key,
                    self_id,
                )
            )
            history_messages.append(
                {
                    "id": f"{thread_id}::m{turn_index+1:02d}",
                    "speaker_id": speaker_id,
                    "text": build_history_text(turn),
                    "minutes_ago": minutes_ago,
                    "kind": "plain",
                }
            )

        threads.append(
            {
                "thread_id": thread_id,
                "display_name": candidate.get("display_name") or "",
                "is_group": bool(candidate.get("is_group")),
                "allow_sparse_history": True,
                "participant_ids": participant_ids,
                "sender_kind": sender_kind_for_thread(candidate_participants),
                "domain_tags": domain_tags_for_thread(candidate_participants, base_domain_tag),
                "messages": history_messages,
            }
        )

    gold_target = sample.get("message", {})
    gold_message = gold_target.get("message", {})
    gold_thread_id = plausible_thread_ids[
        next(
            index
            for index, candidate in enumerate(sample.get("candidate_threads", []))
            if candidate.get("thread_id") == sample.get("gold_thread_id")
        )
    ]
    gold_candidate = next(
        candidate for candidate in sample.get("candidate_threads", []) if candidate.get("thread_id") == sample.get("gold_thread_id")
    )
    target_speaker_id = (
        self_id
        if str(gold_message.get("folder") or "").lower() == "sent"
        else find_participant_id_for_sender(
            gold_message.get("senderName"),
            gold_message.get("senderAddressing"),
            gold_candidate.get("participants", []) or [],
            participant_id_by_key,
            self_id,
        )
    )
    ambiguity_reasons = list(dict.fromkeys(base_ambiguity_reasons))
    target_text = build_target_text(sample)
    if looks_generic(target_text):
        ambiguity_reasons.append("generic_reply")
    if str(gold_message.get("folder") or "").lower() == "sent":
        ambiguity_reasons.append("outbound_sparse")
    if any("service" in thread.get("domain_tags", []) for thread in threads if thread["thread_id"] != gold_thread_id):
        ambiguity_reasons.append("service_sender")

    return {
        "window_id": window_id,
        "theme": f"{theme_prefix} {gold_target.get('conversation_display_name') or 'unknown'}",
        "self_participant_id": self_id,
        "participants": participant_defs,
        "threads": threads,
        "ambiguous_targets": [
            {
                "target_id": f"{window_id}::target",
                "gold_thread_id": gold_thread_id,
                "speaker_id": target_speaker_id,
                "text": target_text,
                "minutes_ago": 0,
                "kind": "plain",
                "ambiguity_reasons": sorted(set(ambiguity_reasons)),
                "plausible_thread_ids": plausible_thread_ids,
            }
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert failure samples into latent thread windows for synth generation.")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sample-id", action="append", default=[])
    parser.add_argument("--sample-ids-file", type=Path)
    parser.add_argument("--source-tag", type=str, default="runtime_replay_failures")
    parser.add_argument("--window-prefix", type=str, default="replay_failure")
    parser.add_argument("--style-tag", type=str, default="replay_failure")
    parser.add_argument("--theme-prefix", type=str, default="runtime replay failure")
    parser.add_argument("--ambiguity-reason", action="append", default=[])
    return parser.parse_args()


def default_ambiguity_reasons(source_tag: str) -> list[str]:
    if source_tag == "runtime_replay_failures":
        return ["runtime_replay_failure", "replay_contamination", "live_runtime_failure"]
    return [source_tag]


def load_selected_sample_ids(args: argparse.Namespace) -> list[str]:
    sample_ids: list[str] = list(args.sample_id or [])
    if args.sample_ids_file:
        lines = args.sample_ids_file.read_text(encoding="utf-8").splitlines()
        sample_ids.extend(line.strip() for line in lines if line.strip())
    return list(dict.fromkeys(sample_ids))


def select_samples(samples: Iterable[dict[str, Any]], sample_ids: Sequence[str]) -> list[dict[str, Any]]:
    sample_list = list(samples)
    if not sample_ids:
        return sample_list
    sample_id_set = set(sample_ids)
    selected = [sample for sample in sample_list if str(sample.get("sample_id") or "") in sample_id_set]
    missing = [sample_id for sample_id in sample_ids if sample_id not in {str(sample.get("sample_id") or "") for sample in selected}]
    if missing:
        raise ValueError(f"Missing requested sample ids: {missing}")
    return selected


def main() -> None:
    args = parse_args()
    payload = json.loads(args.input.read_text(encoding="utf-8"))
    samples = payload.get("samples", [])
    selected_sample_ids = load_selected_sample_ids(args)
    selected_samples = select_samples(samples, selected_sample_ids)
    base_ambiguity_reasons = args.ambiguity_reason or default_ambiguity_reasons(args.source_tag)
    windows = [
        convert_sample(
            sample,
            index,
            window_prefix=args.window_prefix,
            style_tag=args.style_tag,
            theme_prefix=args.theme_prefix,
            base_domain_tag=args.source_tag,
            base_ambiguity_reasons=base_ambiguity_reasons,
        )
        for index, sample in enumerate(selected_samples)
    ]
    output = {
        "kind": "thread_windows",
        "source": args.source_tag,
        "source_dataset": str(args.input),
        "selected_sample_ids": selected_sample_ids,
        "window_count": len(windows),
        "windows": windows,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {len(windows)} failure-derived windows to {args.output}")


if __name__ == "__main__":
    main()
