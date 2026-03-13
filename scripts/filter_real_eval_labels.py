#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def normalize_text(value: str | None) -> str:
    return "" if not value else " ".join(str(value).lower().split())


def explicit_counterparty_keys(participants: list[dict[str, Any]]) -> set[str]:
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
            digits = "".join(ch for ch in str(phone) if ch.isdigit())
            if digits:
                keys.add(f"phone_digits:{digits}")
        for email in participant.get("emails", []) or []:
            normalized = normalize_text(email)
            if normalized:
                keys.add(f"email:{normalized}")
    return keys


def nonself_participant_count(participants: list[dict[str, Any]]) -> int:
    return sum(1 for participant in participants if not participant.get("isSelf"))


def suspicious_gold_label(sample: dict[str, Any]) -> dict[str, Any] | None:
    message = sample.get("message", {})
    if message.get("is_group"):
        return None
    target_keys = explicit_counterparty_keys(message.get("participants", []) or [])
    if not target_keys:
        return None

    candidates = sample.get("candidate_threads", [])
    gold = next((candidate for candidate in candidates if candidate.get("thread_id") == sample.get("gold_thread_id")), None)
    if gold is None:
        return None

    exact_direct_candidates = [
        candidate
        for candidate in candidates
        if (not candidate.get("is_group"))
        and explicit_counterparty_keys(candidate.get("participants", []) or []) == target_keys
        and nonself_participant_count(candidate.get("participants", []) or []) == 1
    ]
    if not exact_direct_candidates:
        return None

    gold_keys = explicit_counterparty_keys(gold.get("participants", []) or [])
    if gold_keys == target_keys and (not gold.get("is_group")) and nonself_participant_count(gold.get("participants", []) or []) == 1:
        return None

    return {
        "sample_id": sample.get("sample_id"),
        "preview": sample.get("metadata", {}).get("preview"),
        "target_keys": sorted(target_keys),
        "gold_thread_id": gold.get("thread_id"),
        "gold_display_name": gold.get("display_name"),
        "gold_is_group": bool(gold.get("is_group")),
        "gold_nonself_count": nonself_participant_count(gold.get("participants", []) or []),
        "exact_direct_candidates": [candidate.get("thread_id") for candidate in exact_direct_candidates],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Filter suspicious poisoned labels from a real eval dataset.")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = json.loads(args.input.read_text(encoding="utf-8"))
    samples = payload.get("samples", [])
    kept: list[dict[str, Any]] = []
    removed: list[dict[str, Any]] = []
    for sample in samples:
        finding = suspicious_gold_label(sample)
        if finding is None:
            kept.append(sample)
        else:
            removed.append(finding)

    meta = dict(payload.get("meta", {}))
    meta.update(
        {
            "source_dataset": str(args.input),
            "filtered_sample_count": len(kept),
            "removed_suspicious_label_count": len(removed),
        }
    )
    output = {"meta": meta, "samples": kept}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")

    report = {
        "input": str(args.input),
        "output": str(args.output),
        "input_sample_count": len(samples),
        "filtered_sample_count": len(kept),
        "removed_suspicious_label_count": len(removed),
        "removed_samples": removed,
    }
    print(json.dumps(report, indent=2))
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main()
