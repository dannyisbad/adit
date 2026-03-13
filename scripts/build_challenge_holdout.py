import argparse
import json
import math
import random
from collections import Counter, defaultdict
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from thread_model_banks import load_style_packs, load_thread_windows
from thread_model_synth import (
    CORRUPTION_PROFILES,
    SELF_SENDER_ADDRESSING,
    SELF_SENDER_NAME,
    _build_notification,
    _build_target_message,
    _candidate_features,
    _choose_difficulty_recipe,
    _derive_ambiguous_targets,
    _ordered_candidate_pools,
    _participant_lookup,
    _render_thread_history,
    _sample_thread_offset_minutes,
    _self_participant_id,
    _service_like,
    _target_sender_in_thread,
    _thread_participants,
    _visible_participant_ids,
    looks_generic,
    normalize_text,
    validate_dataset,
)


DEFAULT_SPEC_PATH = Path("artifacts") / "thread-model" / "from-gpt" / "synthetic_challenge_holdout_spec.json"
DEFAULT_STYLE_PACKS = Path("artifacts") / "thread-model" / "style_packs_opus_v2.json"


PRONOUN_TOKENS = {
    "it",
    "this",
    "that",
    "they",
    "them",
    "she",
    "he",
    "her",
    "him",
    "there",
    "here",
    "same",
    "one",
    "that one",
    "this one",
    "which day",
    "which one",
}

DEFAULT_WINDOW_SOURCES = [
    Path("artifacts") / "thread-model" / "thread_windows_opus_mega_plus.json",
    Path("artifacts") / "thread-model" / "thread_windows_opus_mega.json",
    Path("artifacts") / "thread-model" / "thread_windows_opus_diverse.json",
    Path("artifacts") / "thread-model" / "thread_windows_opus_diverse_small.json",
    Path("artifacts") / "thread-model" / "thread_windows_opus_bank.json",
    Path("artifacts") / "thread-model" / "thread_windows_opus_small.json",
    Path("artifacts") / "thread-model" / "thread_windows_opus_v4_reaction_extra_seq6.json",
    Path("artifacts") / "thread-model" / "challenge_thread_windows_opus_v1.json",
    Path("artifacts") / "thread-model" / "challenge_thread_windows_opus_v2.json",
]


@dataclass(frozen=True)
class TargetUnit:
    source_file: str
    window: dict[str, Any]
    target: dict[str, Any]
    gold_thread: dict[str, Any]
    tags: frozenset[str]
    source_index: int


def _load_spec(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _prefix_window_namespace(window: dict[str, Any], prefix: str) -> dict[str, Any]:
    clone = deepcopy(window)
    original_window_id = clone["window_id"]
    clone["window_id"] = f"{prefix}::{original_window_id}"
    thread_map: dict[str, str] = {}
    for thread in clone["threads"]:
        old_id = thread["thread_id"]
        new_id = f"{prefix}::{old_id}"
        thread_map[old_id] = new_id
        thread["thread_id"] = new_id
    for target in clone.get("ambiguous_targets", []):
        target["target_id"] = f"{prefix}::{target['target_id']}"
        target["gold_thread_id"] = thread_map.get(target["gold_thread_id"], target["gold_thread_id"])
        target["plausible_thread_ids"] = [thread_map.get(item, item) for item in target.get("plausible_thread_ids", [])]
    return clone


def load_unseen_windows(paths: Iterable[Path]) -> list[dict[str, Any]]:
    windows: list[dict[str, Any]] = []
    for index, path in enumerate(paths):
        if not path.exists():
            continue
        loaded = load_thread_windows(path)
        prefix = f"holdsrc{index+1}:{path.stem}"
        windows.extend(_prefix_window_namespace(window, prefix) for window in loaded)
    return windows


def _find_thread(window: dict[str, Any], thread_id: str) -> dict[str, Any]:
    return next(thread for thread in window["threads"] if thread["thread_id"] == thread_id)


def _history_messages_for_target(thread: dict[str, Any], target: dict[str, Any]) -> list[dict[str, Any]]:
    target_id = target.get("target_id", "")
    # explicit windows generally use target_id only; fall back to minutes_ago cut
    target_minutes = int(target.get("minutes_ago", 0))
    return [message for message in thread["messages"] if int(message.get("minutes_ago", 0)) > target_minutes]


def _thread_recentness_minutes(thread: dict[str, Any], target: dict[str, Any]) -> int | None:
    history = _history_messages_for_target(thread, target)
    if not history:
        return None
    return min(int(message.get("minutes_ago", 0)) for message in history)


def _shared_sender_across_plausible(window: dict[str, Any], target: dict[str, Any]) -> bool:
    self_id = window["self_participant_id"]
    return sum(1 for thread_id in target.get("plausible_thread_ids", []) if _target_sender_in_thread(target, _find_thread(window, thread_id), self_id)) >= 2


def _dm_vs_group(window: dict[str, Any], target: dict[str, Any]) -> bool:
    gold = _find_thread(window, target["gold_thread_id"])
    gold_group = bool(gold["is_group"])
    for thread_id in target.get("plausible_thread_ids", []):
        if thread_id == gold["thread_id"]:
            continue
        if bool(_find_thread(window, thread_id)["is_group"]) != gold_group:
            return True
    return False


def _service_collision(window: dict[str, Any], target: dict[str, Any]) -> bool:
    participants = _participant_lookup(window)
    self_id = window["self_participant_id"]
    return any(_service_like(_find_thread(window, thread_id), participants, self_id) for thread_id in target.get("plausible_thread_ids", []))


def _same_sender_or_shared_active_participant(window: dict[str, Any], target: dict[str, Any]) -> bool:
    reasons = " ".join(target.get("ambiguity_reasons", [])).lower()
    return (
        _shared_sender_across_plausible(window, target)
        or "same sender" in reasons
        or "shared active participant" in reasons
        or "sender alone doesn't disambiguate" in reasons
        or "speaker" in reasons
    )


def _stale_gold_vs_recent(window: dict[str, Any], target: dict[str, Any]) -> bool:
    reasons = " ".join(target.get("ambiguity_reasons", [])).lower()
    if "stale" in reasons or "recent" in reasons or "recency bias" in reasons:
        return True
    gold_recent = _thread_recentness_minutes(_find_thread(window, target["gold_thread_id"]), target)
    distractor_recents = [
        _thread_recentness_minutes(_find_thread(window, thread_id), target)
        for thread_id in target.get("plausible_thread_ids", [])
        if thread_id != target["gold_thread_id"]
    ]
    distractor_recents = [value for value in distractor_recents if value is not None]
    return gold_recent is not None and any(value + 30 < gold_recent for value in distractor_recents)


def _generic_or_short_or_missing(target: dict[str, Any]) -> bool:
    text = str(target.get("text", "")).strip()
    if not text:
        return True
    if looks_generic(text):
        return True
    return len(text.split()) <= 3


def _reaction_or_quote(target: dict[str, Any]) -> bool:
    kind = str(target.get("kind", "")).lower()
    text = str(target.get("text", "")).lower()
    return "reaction" in kind or "quote" in kind or "\"" in text or "laughed at" in text or "liked “" in text or "liked \"" in text


def _anaphora_or_pronoun(target: dict[str, Any]) -> bool:
    text = normalize_text(target.get("text"))
    if not text:
        return False
    tokens = set(text.split())
    if tokens & {token for token in PRONOUN_TOKENS if " " not in token}:
        return True
    return any(phrase in text for phrase in PRONOUN_TOKENS if " " in phrase)


def _classify_tags(window: dict[str, Any], target: dict[str, Any]) -> set[str]:
    tags: set[str] = set()
    if _same_sender_or_shared_active_participant(window, target):
        tags.add("same_sender_or_shared_active_participant")
    if _stale_gold_vs_recent(window, target):
        tags.add("stale_gold_vs_recent")
    if _generic_or_short_or_missing(target):
        tags.add("generic_or_short_or_missing_text")
    if _reaction_or_quote(target):
        tags.add("reaction_or_quote")
    if _dm_vs_group(window, target):
        tags.add("dm_vs_group")
    if _anaphora_or_pronoun(target):
        tags.add("anaphora_or_pronoun")
    if _service_collision(window, target):
        tags.add("service_collision")
    return tags


def build_target_units(windows: list[dict[str, Any]]) -> list[TargetUnit]:
    units: list[TargetUnit] = []
    for source_index, window in enumerate(windows):
        targets = _derive_ambiguous_targets(window)
        for target in targets:
            gold_thread = _find_thread(window, target["gold_thread_id"])
            tags = _classify_tags(window, target)
            units.append(
                TargetUnit(
                    source_file=window["window_id"].split("::", 1)[0],
                    window=window,
                    target=target,
                    gold_thread=gold_thread,
                    tags=frozenset(tags),
                    source_index=source_index,
                )
            )
    return units


def _quota_count(total: int, spec_value: Any) -> tuple[int, int]:
    if isinstance(spec_value, list) and len(spec_value) == 2:
        lo, hi = spec_value
        return math.floor(total * float(lo)), math.ceil(total * float(hi))
    value = float(spec_value)
    return math.ceil(total * value), total


def _target_score(unit: TargetUnit, deficits: dict[str, int], window_counts: Counter[str], used_texts: set[str]) -> tuple[int, int, int, int, int]:
    covered = sum(1 for tag, deficit in deficits.items() if deficit > 0 and tag in unit.tags)
    rare_text_penalty = 1 if normalize_text(unit.target["text"]) in used_texts else 0
    tag_count = len(unit.tags)
    window_penalty = window_counts[unit.window["window_id"]]
    outbound_bonus = 1 if unit.target["speaker_id"] == unit.window["self_participant_id"] else 0
    return (covered, outbound_bonus, tag_count, -window_penalty, -rare_text_penalty)


def _select_units_for_closed(units: list[TargetUnit], target_size: int, quota_map: dict[str, Any], seed: int) -> list[TargetUnit]:
    rng = random.Random(seed)
    quota_tags = {
        "same_sender_or_shared_active_participant": quota_map["same_sender_or_shared_active_participant_min"],
        "stale_gold_vs_recent": quota_map["stale_gold_vs_recent_distractor_range"],
        "generic_or_short_or_missing_text": quota_map["generic_or_short_or_missing_text_range"],
        "reaction_or_quote": quota_map["reaction_or_quote_range"],
        "dm_vs_group": quota_map["dm_vs_group_range"],
        "anaphora_or_pronoun": quota_map["anaphora_or_pronoun_range"],
        "service_collision": quota_map["service_collision_range"],
    }
    deficits = {tag: _quota_count(target_size, spec)[0] for tag, spec in quota_tags.items()}
    pool = units[:]
    rng.shuffle(pool)
    selected: list[TargetUnit] = []
    window_counts: Counter[str] = Counter()
    used_texts: set[str] = set()
    while pool and len(selected) < target_size:
        best = max(pool, key=lambda unit: _target_score(unit, deficits, window_counts, used_texts))
        selected.append(best)
        pool.remove(best)
        window_counts[best.window["window_id"]] += 1
        used_texts.add(normalize_text(best.target["text"]))
        for tag in best.tags:
            if tag in deficits and deficits[tag] > 0:
                deficits[tag] -= 1
    return selected


def _select_units_for_open(units: list[TargetUnit], target_size: int, seed: int, exclude_ids: set[str]) -> list[TargetUnit]:
    rng = random.Random(seed)
    pool = [
        unit for unit in units
        if unit.target["target_id"] not in exclude_ids
        and (len(unit.target.get("plausible_thread_ids", [])) >= 2)
        and ("same_sender_or_shared_active_participant" in unit.tags or "stale_gold_vs_recent" in unit.tags or "reaction_or_quote" in unit.tags)
    ]
    rng.shuffle(pool)
    selected: list[TargetUnit] = []
    window_counts: Counter[str] = Counter()
    used_texts: set[str] = set()
    while pool and len(selected) < target_size:
        best = max(
            pool,
            key=lambda unit: (
                len(unit.tags),
                1 if "reaction_or_quote" in unit.tags else 0,
                1 if "stale_gold_vs_recent" in unit.tags else 0,
                -window_counts[unit.window["window_id"]],
                -int(normalize_text(unit.target["text"]) in used_texts),
            ),
        )
        selected.append(best)
        pool.remove(best)
        window_counts[best.window["window_id"]] += 1
        used_texts.add(normalize_text(best.target["text"]))
    return selected


def _challenge_profile_for(unit: TargetUnit, rng: random.Random) -> Any:
    if "reaction_or_quote" in unit.tags:
        preferred = ["reactionish_sparse", "map_sparse_body", "descriptor_truncated"]
    elif "stale_gold_vs_recent" in unit.tags:
        preferred = ["whole_hour_skew", "map_sparse_sender", "cleanish"]
    elif "service_collision" in unit.tags:
        preferred = ["map_sparse_sender", "descriptor_truncated", "cleanish"]
    else:
        preferred = ["cleanish", "map_sparse_sender", "map_sparse_body"]
    by_name = {profile.name: profile for profile in CORRUPTION_PROFILES}
    return by_name[rng.choice(preferred)]


def _candidate_count(rng: random.Random) -> int:
    return rng.choice((4, 5, 6))


def _pick_recipe(unit: TargetUnit, rng: random.Random) -> Any:
    target = dict(unit.target)
    target["target_is_group"] = bool(unit.gold_thread["is_group"])
    return _choose_difficulty_recipe(target=target, gold_thread=unit.gold_thread, self_id=unit.window["self_participant_id"], rng=rng)


def _select_closed_candidates(unit: TargetUnit, all_windows: list[dict[str, Any]], rng: random.Random) -> list[dict[str, Any]]:
    window = unit.window
    target = unit.target
    gold_thread = unit.gold_thread
    recipe = _pick_recipe(unit, rng)
    desired = _candidate_count(rng)
    self_id = window["self_participant_id"]
    local_threads, cross_threads = _ordered_candidate_pools(
        current_window=window,
        windows=all_windows,
        gold_thread=gold_thread,
        target=target,
        self_id=self_id,
        participants_by_id=_participant_lookup(window),
    )
    selected = [gold_thread]
    selected_ids = {gold_thread["thread_id"]}
    for thread_id in target.get("plausible_thread_ids", []):
        if thread_id == gold_thread["thread_id"]:
            continue
        match = next((thread for thread in local_threads if thread["thread_id"] == thread_id), None)
        if match and match["thread_id"] not in selected_ids:
            selected.append(match)
            selected_ids.add(match["thread_id"])
        if len(selected) >= desired:
            break

    def try_add_from(pool: list[dict[str, Any]]) -> None:
        for thread in pool:
            if len(selected) >= desired:
                return
            if thread["thread_id"] in selected_ids:
                continue
            selected.append(thread)
            selected_ids.add(thread["thread_id"])

    same_sender_local = [thread for thread in local_threads if _target_sender_in_thread(target, thread, self_id)]
    same_sender_cross = [thread for thread in cross_threads if _target_sender_in_thread(target, thread, window["self_participant_id"])]
    same_geometry_local = [
        thread
        for thread in local_threads
        if set(_visible_participant_ids(thread, self_id)) == set(_visible_participant_ids(gold_thread, self_id))
    ]
    try_add_from(same_sender_local)
    try_add_from(same_geometry_local)
    try_add_from(local_threads)
    try_add_from(same_sender_cross)
    try_add_from(cross_threads)
    return selected[:desired]


def _select_open_candidates(unit: TargetUnit, all_windows: list[dict[str, Any]], rng: random.Random) -> list[dict[str, Any]]:
    closed = _select_closed_candidates(unit, all_windows, rng)
    gold_id = unit.gold_thread["thread_id"]
    remaining = [thread for thread in closed if thread["thread_id"] != gold_id]
    desired = max(4, min(6, len(closed)))
    if len(remaining) >= desired:
        return remaining[:desired]
    window = unit.window
    self_id = window["self_participant_id"]
    local_threads, cross_threads = _ordered_candidate_pools(
        current_window=window,
        windows=all_windows,
        gold_thread=unit.gold_thread,
        target=unit.target,
        self_id=self_id,
        participants_by_id=_participant_lookup(window),
    )
    selected_ids = {thread["thread_id"] for thread in remaining}
    for pool in (local_threads, cross_threads):
        for thread in pool:
            if len(remaining) >= desired:
                break
            if thread["thread_id"] in selected_ids or thread["thread_id"] == gold_id:
                continue
            remaining.append(thread)
            selected_ids.add(thread["thread_id"])
        if len(remaining) >= desired:
            break
    return remaining[:desired]


def _render_sample(
    *,
    unit: TargetUnit,
    candidate_threads: list[dict[str, Any]],
    all_windows: list[dict[str, Any]],
    sample_prefix: str,
    sample_index: int,
    open_set: bool,
    rng: random.Random,
) -> dict[str, Any]:
    window = unit.window
    target = dict(unit.target)
    gold_thread = unit.gold_thread
    participants_by_id = _participant_lookup(window)
    self_id = _self_participant_id(window, participants_by_id)
    recipe = _pick_recipe(unit, rng)
    profile = _challenge_profile_for(unit, rng)
    base_time = datetime.now(tz=timezone.utc).replace(microsecond=0)
    sample_time = base_time + timedelta(minutes=sample_index * 13)
    target_time = sample_time - timedelta(minutes=int(target.get("minutes_ago", 0)))
    target_visible = set(_visible_participant_ids(gold_thread, self_id))

    rendered_candidates = []
    for thread in candidate_threads:
        source_window = window if thread in window["threads"] else next(candidate_window for candidate_window in all_windows if thread in candidate_window["threads"])
        candidate_participants = _participant_lookup(source_window)
        candidate_self_id = _self_participant_id(source_window, candidate_participants)
        history = _render_thread_history(
            thread=thread,
            target=target,
            participants_by_id=candidate_participants,
            target_time=target_time,
            profile=profile,
            self_id=candidate_self_id,
            rng=rng,
            thread_offset_minutes=_sample_thread_offset_minutes(
                rng,
                target={**target, "target_is_group": bool(gold_thread["is_group"])},
                thread=thread,
                is_gold=thread["thread_id"] == gold_thread["thread_id"],
                is_plausible=thread["thread_id"] in target["plausible_thread_ids"],
                self_id=candidate_self_id,
                recipe=recipe,
            ),
        )
        candidate_visible = set(_visible_participant_ids(thread, candidate_self_id))
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
                "participants": _thread_participants(thread, candidate_participants, candidate_self_id),
                "history": history,
                "features": features,
            }
        )

    rng.shuffle(rendered_candidates)
    sample_id = f"{sample_prefix}::{unit.window['window_id']}::{unit.target['target_id']}::{sample_index}"
    target_message = _build_target_message(
        target=target,
        thread=gold_thread,
        participants_by_id=participants_by_id,
        target_time=target_time,
        profile=profile,
        sample_id=sample_id,
        self_id=self_id,
    )
    # mask target-side display name to avoid trivial key leakage in the benchmark
    target_message["conversation_display_name"] = ""
    target_message["participants"] = _sparsify_target_participants(
        target_message["participants"],
        unit=unit,
        rng=rng,
    )
    notifications = _build_notification(
        target=target,
        thread=gold_thread,
        participants_by_id=participants_by_id,
        target_time=target_time,
        profile=profile,
        target_index=sample_index + 1,
        self_id=self_id,
        rng=rng,
    )
    metadata = {
        "generic_preview": looks_generic(target["text"]),
        "preview": target["text"],
        "candidate_count": len(rendered_candidates),
        "target_participant_count": len(target_visible),
        "window_id": unit.window["window_id"],
        "challenge_tags": sorted(unit.tags),
        "challenge_open_set": open_set,
        "gold_present": not open_set,
        "target_is_outbound": target["speaker_id"] == self_id,
        "target_is_group": bool(gold_thread["is_group"]),
        "ambiguity_reasons": list(target.get("ambiguity_reasons", [])),
        "plausible_thread_count": len(target.get("plausible_thread_ids", [])),
        "corruption_profile": profile.name,
        "difficulty_recipe": recipe.name,
    }
    sample = {
        "sample_id": sample_id,
        "source": "synthetic_challenge_holdout",
        "label_source": "latent_thread_id" if not open_set else "latent_thread_absent",
        "gold_thread_id": None if open_set else gold_thread["thread_id"],
        "message": target_message,
        "candidate_threads": rendered_candidates,
        "nearby_notifications": notifications,
        "metadata": metadata,
    }
    return sample


def _sparsify_target_participants(
    participants: list[dict[str, Any]],
    *,
    unit: TargetUnit,
    rng: random.Random,
) -> list[dict[str, Any]]:
    if not participants:
        return participants
    visible = list(participants)
    tags = unit.tags
    gold_is_group = bool(unit.gold_thread["is_group"])
    if gold_is_group and ({"reaction_or_quote", "stale_gold_vs_recent", "dm_vs_group"} & tags):
        roll = rng.random()
        if roll < 0.30:
            return []
        if roll < 0.80:
            keep = min(len(visible), rng.choice((1, 2)))
            rng.shuffle(visible)
            return visible[:keep]
    if (not gold_is_group) and ("same_sender_or_shared_active_participant" in tags) and ("generic_or_short_or_missing_text" in tags):
        if rng.random() < 0.22:
            return []
    return participants


def _exact_name_shortcut(sample: dict[str, Any]) -> bool:
    target_name = normalize_text(sample["message"].get("conversation_display_name"))
    if not target_name:
        return False
    matches = [candidate for candidate in sample["candidate_threads"] if normalize_text(candidate.get("display_name")) == target_name]
    return len(matches) == 1 and sample.get("gold_thread_id") and matches[0]["thread_id"] == sample["gold_thread_id"]


def _exact_participant_match(sample: dict[str, Any]) -> bool:
    target_keys = sorted(participant["key"] for participant in sample["message"].get("participants", []))
    if not target_keys:
        return False
    matches = [
        candidate for candidate in sample["candidate_threads"]
        if sorted(participant["key"] for participant in candidate.get("participants", [])) == target_keys
    ]
    return len(matches) == 1 and sample.get("gold_thread_id") and matches[0]["thread_id"] == sample["gold_thread_id"]


def _min_delta_hit(sample: dict[str, Any]) -> bool:
    deltas = [(candidate["thread_id"], candidate.get("features", {}).get("delta_seconds")) for candidate in sample["candidate_threads"]]
    valid = [(thread_id, delta) for thread_id, delta in deltas if isinstance(delta, int)]
    if not valid:
        return False
    min_delta = min(delta for _, delta in valid)
    matches = [thread_id for thread_id, delta in valid if delta == min_delta]
    return len(matches) == 1 and sample.get("gold_thread_id") in matches


def audit_holdout(samples: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [sample for sample in samples if sample["metadata"].get("gold_present")]
    open_set = [sample for sample in samples if not sample["metadata"].get("gold_present")]
    tag_counts: Counter[str] = Counter()
    for sample in closed:
        tag_counts.update(sample["metadata"].get("challenge_tags", []))
    return {
        "sample_count": len(samples),
        "closed_count": len(closed),
        "open_count": len(open_set),
        "avg_candidate_count": round(sum(len(sample["candidate_threads"]) for sample in samples) / max(len(samples), 1), 3),
        "tag_ratios_closed": {tag: round(tag_counts[tag] / max(len(closed), 1), 4) for tag in sorted(tag_counts)},
        "closed_exact_name_shortcut_ratio": round(sum(1 for sample in closed if _exact_name_shortcut(sample)) / max(len(closed), 1), 4),
        "closed_exact_participant_shortcut_ratio": round(sum(1 for sample in closed if _exact_participant_match(sample)) / max(len(closed), 1), 4),
        "closed_unique_min_delta_ratio": round(sum(1 for sample in closed if _min_delta_hit(sample)) / max(len(closed), 1), 4),
        "open_generic_ratio": round(sum(1 for sample in open_set if sample["metadata"].get("generic_preview")) / max(len(open_set), 1), 4),
    }


def audit_open_only(samples: list[dict[str, Any]]) -> dict[str, Any]:
    tag_counts: Counter[str] = Counter()
    for sample in samples:
        tag_counts.update(sample["metadata"].get("challenge_tags", []))
    return {
        "sample_count": len(samples),
        "avg_candidate_count": round(sum(len(sample["candidate_threads"]) for sample in samples) / max(len(samples), 1), 3),
        "tag_ratios": {tag: round(tag_counts[tag] / max(len(samples), 1), 4) for tag in sorted(tag_counts)},
        "generic_ratio": round(sum(1 for sample in samples if sample["metadata"].get("generic_preview")) / max(len(samples), 1), 4),
        "outbound_ratio": round(sum(1 for sample in samples if sample["metadata"].get("target_is_outbound")) / max(len(samples), 1), 4),
        "group_ratio": round(sum(1 for sample in samples if sample["metadata"].get("target_is_group")) / max(len(samples), 1), 4),
    }


def write_dataset(samples: list[dict[str, Any]], output_path: Path, meta: dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"meta": meta, "samples": samples}
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build synthetic challenge holdouts for thread-choice evaluation.")
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC_PATH)
    parser.add_argument("--style-packs", type=Path, default=DEFAULT_STYLE_PACKS)
    parser.add_argument("--window", dest="window_paths", action="append", type=Path, default=[])
    parser.add_argument("--closed-out", type=Path, default=Path("artifacts") / "thread-model" / "challenge_closed_v1.json")
    parser.add_argument("--open-out", type=Path, default=Path("artifacts") / "thread-model" / "challenge_open_v1.json")
    parser.add_argument("--audit-out", type=Path, default=Path("artifacts") / "thread-model" / "challenge_holdout_audit_v1.json")
    parser.add_argument("--seed", type=int, default=17)
    args = parser.parse_args()

    window_paths = args.window_paths or DEFAULT_WINDOW_SOURCES
    windows = load_unseen_windows(window_paths)
    if not windows:
        raise SystemExit("No usable challenge window sources found.")
    spec = _load_spec(args.spec)
    _ = load_style_packs(args.style_packs)  # validates file; currently generation uses windows, not banks directly
    units = build_target_units(windows)
    closed_target_size = int(spec["closed_set_holdout"]["target_size"])
    open_target_size = int(spec["companion_open_set_holdout"]["target_size"])
    closed_units = _select_units_for_closed(units, closed_target_size, spec["closed_set_holdout"]["slice_quotas"], seed=args.seed)
    open_units = _select_units_for_open(units, open_target_size, seed=args.seed + 1, exclude_ids={unit.target["target_id"] for unit in closed_units})

    rng = random.Random(args.seed)
    closed_samples = [
        _render_sample(unit=unit, candidate_threads=_select_closed_candidates(unit, windows, rng), all_windows=windows, sample_prefix="challenge_closed", sample_index=index, open_set=False, rng=rng)
        for index, unit in enumerate(closed_units)
    ]
    open_samples = [
        _render_sample(unit=unit, candidate_threads=_select_open_candidates(unit, windows, rng), all_windows=windows, sample_prefix="challenge_open", sample_index=index, open_set=True, rng=rng)
        for index, unit in enumerate(open_units)
    ]

    closed_audit = audit_holdout(closed_samples)
    open_audit = audit_holdout(open_samples)
    validation_closed = validate_dataset(closed_samples)
    validation_open = audit_open_only(open_samples)

    write_dataset(
        closed_samples,
        args.closed_out,
        meta={
            "kind": "synthetic_challenge_closed",
            "seed": args.seed,
            "window_sources": [str(path) for path in window_paths],
            "spec": str(args.spec),
            "sample_count": len(closed_samples),
        },
    )
    write_dataset(
        open_samples,
        args.open_out,
        meta={
            "kind": "synthetic_challenge_open",
            "seed": args.seed,
            "window_sources": [str(path) for path in window_paths],
            "spec": str(args.spec),
            "sample_count": len(open_samples),
        },
    )
    args.audit_out.parent.mkdir(parents=True, exist_ok=True)
    args.audit_out.write_text(
        json.dumps(
            {
                "spec": str(args.spec),
                "closed_audit": closed_audit,
                "open_audit": open_audit,
                "closed_validation": validation_closed,
                "open_validation": validation_open,
                "window_count": len(windows),
                "target_unit_count": len(units),
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "closed_out": str(args.closed_out),
                "open_out": str(args.open_out),
                "audit_out": str(args.audit_out),
                "closed_count": len(closed_samples),
                "open_count": len(open_samples),
                "window_count": len(windows),
                "target_unit_count": len(units),
                "closed_shortcut_exact_name": closed_audit["closed_exact_name_shortcut_ratio"],
                "closed_shortcut_exact_participants": closed_audit["closed_exact_participant_shortcut_ratio"],
                "closed_shortcut_min_delta": closed_audit["closed_unique_min_delta_ratio"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
