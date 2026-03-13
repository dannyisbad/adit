from __future__ import annotations

import argparse
import dataclasses
import json
import math
import os
import random
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader, Dataset


# =========================
# Utilities
# =========================


def parse_iso_utc(value: Optional[str]) -> Optional[datetime]:
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
    return datetime.fromisoformat(value)


def normalize_ws(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def normalize_name(text: Optional[str]) -> str:
    text = normalize_ws(text).lower()
    text = text.replace("\u200b", "").replace("\u200a", "")
    return text


def digits_only(text: Optional[str]) -> str:
    if not text:
        return ""
    return "".join(ch for ch in text if ch.isdigit())


def canonical_key_variants(participant: Dict[str, Any]) -> set[str]:
    out: set[str] = set()
    key = normalize_name(participant.get("key"))
    if key:
        out.add(key)
    display = normalize_name(participant.get("displayName"))
    if display:
        out.add(display)
    for phone in participant.get("phones", []) or []:
        d = digits_only(phone)
        if d:
            out.add(d)
            out.add(normalize_name(phone))
    for email in participant.get("emails", []) or []:
        e = normalize_name(email)
        if e:
            out.add(e)
    return out


def explicit_identity_variants(participant: Dict[str, Any]) -> set[str]:
    out: set[str] = set()
    key = normalize_name(participant.get("key"))
    if key.startswith("phone:") or key.startswith("email:"):
        out.add(key)
    for phone in participant.get("phones", []) or []:
        normalized = normalize_name(phone)
        if normalized:
            out.add(f"phone:{normalized}")
        digits = digits_only(phone)
        if digits:
            out.add(f"phone_digits:{digits}")
    for email in participant.get("emails", []) or []:
        normalized = normalize_name(email)
        if normalized:
            out.add(f"email:{normalized}")
    return out


def explicit_counterparty_keys(participants: Sequence[Dict[str, Any]]) -> List[str]:
    keys: set[str] = set()
    for participant in participants:
        if participant.get("isSelf"):
            continue
        keys.update(explicit_identity_variants(participant))
    return sorted(keys)


def message_surface_text(sample: Dict[str, Any]) -> str:
    """
    Prefer metadata.preview because it aligns body/subject sparsity into one visible text field.
    Fall back to body, then subject. Return empty string when genuinely absent.
    """
    metadata = sample.get("metadata", {})
    preview = normalize_ws(metadata.get("preview"))
    if preview:
        return preview
    msg = sample["message"]["message"]
    body = normalize_ws(msg.get("body"))
    if body:
        return body
    subject = normalize_ws(msg.get("subject"))
    if subject:
        return subject
    return ""


def history_surface_text(turn: Dict[str, Any]) -> str:
    preview = normalize_ws(turn.get("preview"))
    if preview:
        return preview
    body = normalize_ws(turn.get("body"))
    if body:
        return body
    return ""


def compute_seconds_delta(a: Optional[str], b: Optional[str]) -> Optional[float]:
    da = parse_iso_utc(a)
    db = parse_iso_utc(b)
    if da is None or db is None:
        return None
    return (da - db).total_seconds()


def safe_log1p(value: Optional[float], clip: float = 7 * 24 * 3600) -> float:
    if value is None:
        return 0.0
    value = max(0.0, min(float(value), clip))
    return math.log1p(value)


def bool_f(v: Any) -> float:
    return 1.0 if bool(v) else 0.0


# =========================
# Data loading and feature extraction
# =========================


DISALLOWED_FIELDS = {
    # obvious leakage / identifiers / synth-only helpers
    "candidate_score",  # optional ablation only
    "thread_id",
    "message_key",
    "window_id",
    "corruption_profile",
    "ambiguity_reasons",
    "plausible_thread_count",
    "label_source",
    # do not feed target conversation display name into the headline model
    "conversation_display_name",
}


@dataclass
class CandidateExample:
    sample_id: str
    gold_index: int
    target_features: Dict[str, float]
    candidate_struct: List[List[float]]
    candidate_text_contexts: List[str]
    candidate_text_targets: List[str]
    candidate_is_group: List[int]
    candidate_ids: List[str]
    metadata: Dict[str, Any]


class ThreadChoiceDataset(Dataset):
    def __init__(
        self,
        json_path: str | Path,
        include_candidate_score: bool = False,
        max_history_turns: int = 8,
        include_candidate_display_name_in_qwen: bool = False,
        include_nearby_notifications_in_qwen: bool = False,
    ) -> None:
        payload = json.load(open(json_path, "r", encoding="utf-8"))
        self.meta = payload.get("meta", {})
        self.samples_raw = payload["samples"]
        self.include_candidate_score = include_candidate_score
        self.max_history_turns = max_history_turns
        self.include_candidate_display_name_in_qwen = include_candidate_display_name_in_qwen
        self.include_nearby_notifications_in_qwen = include_nearby_notifications_in_qwen
        self.examples = [self._convert_sample(s) for s in self.samples_raw]
        self.struct_names = self._struct_feature_names()

    def _struct_feature_names(self) -> List[str]:
        names = [
            # target-global features copied onto every candidate
            "target_is_group",
            "target_is_outbound",
            "target_has_text",
            "target_body_missing",
            "target_subject_present",
            "target_generic_preview",
            "target_participant_count_log1p",
            "target_text_len_log1p",
            "nearby_notification_count_log1p",
            "candidate_count_log1p",
            # candidate-local features
            "candidate_is_group",
            "group_mismatch",
            "participant_overlap_raw",
            "participant_overlap_ratio_target",
            "participant_overlap_ratio_candidate",
            "participant_jaccard",
            "exact_participant_match",
            "candidate_participant_count_log1p",
            "extra_candidate_participants_log1p",
            "preview_overlap",
            "delta_missing",
            "delta_seconds_log1p",
            "history_len_log1p",
            "history_sent_ratio",
            "history_nonempty_ratio",
            "history_has_self_turn",
            "last_turn_is_sent",
            "last_turn_has_text",
            "last_turn_age_log1p",
            "target_sender_matches_candidate_participant",
            "last_sender_matches_target_sender",
            "any_history_sender_matches_target_sender",
            "candidate_has_explicit_counterparty",
            "explicit_overlap_raw",
            "explicit_overlap_ratio_target",
            "explicit_overlap_ratio_candidate",
            "explicit_exact_counterparty_match",
        ]
        if self.include_candidate_score:
            names.append("candidate_score")
        return names

    def _target_sender_variants(self, sample: Dict[str, Any]) -> set[str]:
        msg = sample["message"]["message"]
        out: set[str] = set()
        for raw in [msg.get("senderName"), msg.get("senderAddressing")]:
            norm = normalize_name(raw)
            if norm:
                out.add(norm)
            d = digits_only(raw)
            if d:
                out.add(d)
        for side in ["originators", "recipients"]:
            for item in msg.get(side, []) or []:
                norm = normalize_name(item.get("name"))
                if norm:
                    out.add(norm)
                d = digits_only(item.get("name"))
                if d:
                    out.add(d)
                for p in item.get("phones", []) or []:
                    d = digits_only(p)
                    if d:
                        out.add(d)
        return out

    def _candidate_participant_variants(self, candidate: Dict[str, Any]) -> set[str]:
        out: set[str] = set()
        for p in candidate.get("participants", []) or []:
            out |= canonical_key_variants(p)
        return out

    def _participant_set_keys(self, participants: List[Dict[str, Any]]) -> set[str]:
        out = set()
        for p in participants:
            key = normalize_name(p.get("key"))
            if key:
                out.add(key)
        return out

    def _turn_sender_variants(self, turn: Dict[str, Any]) -> set[str]:
        out: set[str] = set()
        for raw in [turn.get("sender_name"), turn.get("sender_addressing")]:
            norm = normalize_name(raw)
            if norm:
                out.add(norm)
            d = digits_only(raw)
            if d:
                out.add(d)
        return out

    def _speaker_slots(self, sample: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, str]:
        """
        Map candidate participants and observed senders to local speaker slots so the LM gets turn structure
        without direct access to raw display-name equality.
        """
        slots: Dict[str, str] = {}
        idx = 1
        for p in candidate.get("participants", []) or []:
            label = "SELF" if p.get("isSelf") else f"P{idx}"
            if not p.get("isSelf"):
                idx += 1
            for k in canonical_key_variants(p):
                slots[k] = label
        # self aliases
        for alias in ["name:me", "me", "self"]:
            slots[alias] = "SELF"
        return slots

    def _role_for_sender(self, slots: Dict[str, str], sender_variants: set[str]) -> str:
        for k in sender_variants:
            if k in slots:
                return slots[k]
        if any(k in {"name:me", "me", "self"} for k in sender_variants):
            return "SELF"
        return "PX"

    def _serialize_candidate(self, sample: Dict[str, Any], candidate: Dict[str, Any]) -> Tuple[str, str]:
        target_text = message_surface_text(sample)
        target_msg = sample["message"]["message"]
        slots = self._speaker_slots(sample, candidate)

        header_parts = [
            f"[THREAD {'GROUP' if candidate.get('is_group') else 'DIRECT'}]",
            f"[PARTICIPANTS {len(candidate.get('participants', []) or [])}]",
        ]
        # Candidate display name stays off by default because it can become a too-easy shortcut.
        if self.include_candidate_display_name_in_qwen:
            dn = normalize_ws(candidate.get("display_name"))
            if dn:
                header_parts.append(f"[TITLE {dn}]")
        lines = [" ".join(header_parts)]

        history = candidate.get("history", [])[-self.max_history_turns :]
        prev_t: Optional[datetime] = None
        for turn in history:
            text = history_surface_text(turn)
            sender_role = self._role_for_sender(slots, self._turn_sender_variants(turn))
            turn_t = parse_iso_utc(turn.get("sort_utc"))
            if prev_t is not None and turn_t is not None:
                gap_min = max(0, int((turn_t - prev_t).total_seconds() // 60))
                gap_tok = f"[+{gap_min}m]"
            else:
                gap_tok = "[+0m]"
            prev_t = turn_t
            dir_tok = "OUT" if turn.get("folder") == "sent" else "IN"
            text = text if text else "<EMPTY>"
            lines.append(f"{gap_tok}[{sender_role}][{dir_tok}] {text}")

        if self.include_nearby_notifications_in_qwen:
            for notification in sample.get("nearby_notifications", [])[:3]:
                notif_bits = []
                title = normalize_ws(notification.get("title"))
                subtitle = normalize_ws(notification.get("subtitle"))
                message = normalize_ws(notification.get("message"))
                if title:
                    notif_bits.append(f"[TITLE {title}]")
                if subtitle:
                    notif_bits.append(f"[SUBTITLE {subtitle}]")
                notif_bits.append(message if message else "<EMPTY>")
                lines.append("[NOTIF] " + " ".join(notif_bits))

        # target prefix without target text; target text is scored separately
        target_sender_role = "SELF" if target_msg.get("folder") == "sent" else self._role_for_sender(
            slots, self._target_sender_variants(sample)
        )
        if history:
            last_hist_t = parse_iso_utc(history[-1].get("sort_utc"))
            tgt_t = parse_iso_utc(sample["message"].get("sort_utc"))
            if last_hist_t is not None and tgt_t is not None:
                gap_min = max(0, int((tgt_t - last_hist_t).total_seconds() // 60))
                gap_tok = f"[+{gap_min}m]"
            else:
                gap_tok = "[+?m]"
        else:
            gap_tok = "[NO_HISTORY]"

        dir_tok = "OUT" if target_msg.get("folder") == "sent" else "IN"
        context = "\n".join(lines) + f"\n{gap_tok}[TARGET][{target_sender_role}][{dir_tok}] "
        return context, target_text

    def _convert_sample(self, sample: Dict[str, Any]) -> CandidateExample:
        target = sample["message"]
        msg = target["message"]
        target_text = message_surface_text(sample)
        target_participants = target.get("participants", []) or []
        target_participant_keys = self._participant_set_keys(target_participants)
        target_explicit_keys = set(explicit_counterparty_keys(target_participants))
        target_participant_count = max(1, len(target_participants))
        target_sender_variants = self._target_sender_variants(sample)
        tgt_time = parse_iso_utc(target.get("sort_utc"))
        candidate_count = len(sample["candidate_threads"])

        target_features = {
            "target_is_group": bool_f(target.get("is_group")),
            "target_is_outbound": bool_f(msg.get("folder") == "sent"),
            "target_has_text": bool_f(bool(target_text)),
            "target_body_missing": bool_f(not normalize_ws(msg.get("body"))),
            "target_subject_present": bool_f(bool(normalize_ws(msg.get("subject")))),
            "target_generic_preview": bool_f(sample.get("metadata", {}).get("generic_preview")),
            "target_participant_count_log1p": math.log1p(len(target_participants)),
            "target_text_len_log1p": math.log1p(len(target_text)),
            "nearby_notification_count_log1p": math.log1p(len(sample.get("nearby_notifications", []))),
            "candidate_count_log1p": math.log1p(candidate_count),
        }

        cand_struct: List[List[float]] = []
        cand_contexts: List[str] = []
        cand_targets: List[str] = []
        cand_is_group: List[int] = []
        cand_ids: List[str] = []
        cand_identity_compatible: List[bool] = []

        for cand in sample["candidate_threads"]:
            cand_ids.append(cand["thread_id"])
            cand_is_group.append(int(bool(cand.get("is_group"))))
            cand_participants = cand.get("participants", []) or []
            cand_participant_keys = self._participant_set_keys(cand_participants)
            cand_explicit_keys = set(explicit_counterparty_keys(cand_participants))
            overlap = len(target_participant_keys & cand_participant_keys)
            cand_count = max(1, len(cand_participants))
            union = len(target_participant_keys | cand_participant_keys) or 1
            exact_participant_match = float(target_participant_keys == cand_participant_keys)
            explicit_overlap = len(target_explicit_keys & cand_explicit_keys)
            candidate_has_explicit = float(bool(cand_explicit_keys))
            explicit_exact_match = float(bool(target_explicit_keys) and target_explicit_keys == cand_explicit_keys)
            participant_variants = self._candidate_participant_variants(cand)
            target_sender_match = float(bool(target_sender_variants & participant_variants))
            cand_identity_compatible.append(bool(target_explicit_keys & cand_explicit_keys))

            history = cand.get("history", []) or []
            history_len = len(history)
            history_sent_ratio = (
                sum(1 for h in history if h.get("folder") == "sent") / history_len if history_len else 0.0
            )
            history_nonempty_ratio = (
                sum(1 for h in history if history_surface_text(h)) / history_len if history_len else 0.0
            )
            history_has_self_turn = float(any(h.get("folder") == "sent" for h in history))
            last_turn = history[-1] if history else None
            last_turn_is_sent = float(last_turn is not None and last_turn.get("folder") == "sent")
            last_turn_has_text = float(last_turn is not None and bool(history_surface_text(last_turn)))
            last_turn_age = (
                (tgt_time - parse_iso_utc(last_turn.get("sort_utc"))).total_seconds()
                if last_turn is not None and tgt_time is not None and parse_iso_utc(last_turn.get("sort_utc")) is not None
                else None
            )
            last_sender_matches_target = 0.0
            any_hist_sender_matches_target = 0.0
            if history:
                any_hist_sender_matches_target = float(
                    any(bool(self._turn_sender_variants(h) & target_sender_variants) for h in history)
                )
                last_sender_matches_target = float(
                    bool(self._turn_sender_variants(last_turn) & target_sender_variants) if last_turn else False
                )

            feat = cand.get("features", {})
            row = [
                target_features["target_is_group"],
                target_features["target_is_outbound"],
                target_features["target_has_text"],
                target_features["target_body_missing"],
                target_features["target_subject_present"],
                target_features["target_generic_preview"],
                target_features["target_participant_count_log1p"],
                target_features["target_text_len_log1p"],
                target_features["nearby_notification_count_log1p"],
                target_features["candidate_count_log1p"],
                float(bool(cand.get("is_group"))),
                float(bool(cand.get("is_group")) != bool(target.get("is_group"))),
                float(overlap),
                overlap / max(1, len(target_participant_keys)),
                overlap / max(1, len(cand_participant_keys)),
                overlap / union,
                exact_participant_match,
                math.log1p(len(cand_participants)),
                math.log1p(max(0, len(cand_participants) - overlap)),
                float(feat.get("preview_overlap") or 0.0),
                float(feat.get("delta_seconds") is None),
                safe_log1p(feat.get("delta_seconds")),
                math.log1p(history_len),
                float(history_sent_ratio),
                float(history_nonempty_ratio),
                history_has_self_turn,
                last_turn_is_sent,
                last_turn_has_text,
                safe_log1p(last_turn_age),
                target_sender_match,
                last_sender_matches_target,
                any_hist_sender_matches_target,
                candidate_has_explicit,
                float(explicit_overlap),
                explicit_overlap / max(1, len(target_explicit_keys)),
                explicit_overlap / max(1, len(cand_explicit_keys)),
                explicit_exact_match,
            ]
            if self.include_candidate_score:
                row.append(float(feat.get("candidate_score") or 0.0))
            cand_struct.append(row)
            context, t_text = self._serialize_candidate(sample, cand)
            cand_contexts.append(context)
            cand_targets.append(t_text)

        gold_thread_id = sample.get("gold_thread_id")
        if gold_thread_id is None:
            gold_index = -1
        else:
            try:
                gold_index = next(i for i, c in enumerate(sample["candidate_threads"]) if c["thread_id"] == gold_thread_id)
            except StopIteration as exc:
                raise ValueError(f"gold_thread_id {gold_thread_id!r} not present in candidate_threads") from exc
        metadata = dict(sample.get("metadata", {}))
        metadata["gold_thread_id"] = gold_thread_id
        metadata["candidate_thread_ids"] = cand_ids
        metadata["target_explicit_counterparty_keys"] = sorted(target_explicit_keys)
        metadata["candidate_explicit_counterparty_keys"] = [
            sorted(explicit_counterparty_keys(cand.get("participants", []) or []))
            for cand in sample["candidate_threads"]
        ]
        metadata["candidate_identity_compatible"] = cand_identity_compatible
        metadata["candidate_exact_structural_match"] = [
            bool(
                target_participant_keys == self._participant_set_keys(cand.get("participants", []) or [])
                or (
                    bool(target_explicit_keys)
                    and target_explicit_keys == set(explicit_counterparty_keys(cand.get("participants", []) or []))
                )
            )
            for cand in sample["candidate_threads"]
        ]
        metadata["target_has_explicit_counterparty"] = bool(target_explicit_keys)

        return CandidateExample(
            sample_id=normalize_ws(str(sample.get("sample_id") or "")) or "runtime-sample",
            gold_index=gold_index,
            target_features=target_features,
            candidate_struct=cand_struct,
            candidate_text_contexts=cand_contexts,
            candidate_text_targets=cand_targets,
            candidate_is_group=cand_is_group,
            candidate_ids=cand_ids,
            metadata=metadata,
        )

    def __len__(self) -> int:
        return len(self.examples)

    def __getitem__(self, idx: int) -> CandidateExample:
        return self.examples[idx]


def build_candidate_example(
    sample: Dict[str, Any],
    *,
    include_candidate_score: bool = False,
    max_history_turns: int = 8,
    include_candidate_display_name_in_qwen: bool = False,
    include_nearby_notifications_in_qwen: bool = False,
) -> CandidateExample:
    helper = ThreadChoiceDataset.__new__(ThreadChoiceDataset)
    helper.include_candidate_score = include_candidate_score
    helper.max_history_turns = max_history_turns
    helper.include_candidate_display_name_in_qwen = include_candidate_display_name_in_qwen
    helper.include_nearby_notifications_in_qwen = include_nearby_notifications_in_qwen
    helper.struct_names = helper._struct_feature_names()
    return helper._convert_sample(sample)


# =========================
# Semantic featurizer
# =========================


@dataclass
class SemanticConfig:
    model_name: str
    device: str = "cuda" if torch.cuda.is_available() else "cpu"
    dtype: str = "float16"
    use_hidden_states: bool = True
    use_mid_layer: bool = True
    cache_batch_size: int = 8
    max_length: int = 1024
    low_cpu_mem_usage: bool = True


class FrozenCausalSemanticFeaturizer:
    """
    Frozen-Qwen feature extraction for each (candidate thread context, target message) pair.

    Output per candidate:
      - cond_nll: average NLL of the target text conditioned on candidate context
      - base_nll: average NLL of the target text with no candidate history context
      - lift: base_nll - cond_nll
      - semantic_ok: 1 when target text exists, else 0
      - hidden_vec: mean pooled target-span hidden state from a mid layer (and optionally final layer)

    The target conversation_display_name and notification titles are deliberately excluded here to avoid
    trivial identity shortcuts.
    """

    def __init__(self, config: SemanticConfig) -> None:
        self.config = config
        try:
            from transformers import AutoModelForCausalLM, AutoTokenizer  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                "transformers is required for semantic featurization. Install it locally before running featurize."
            ) from exc

        dtype_map = {
            "float16": torch.float16,
            "bfloat16": torch.bfloat16,
            "float32": torch.float32,
        }
        torch_dtype = dtype_map[config.dtype]
        self.tokenizer = AutoTokenizer.from_pretrained(config.model_name, trust_remote_code=True)
        if self.tokenizer.pad_token_id is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token
        self.model = AutoModelForCausalLM.from_pretrained(
            config.model_name,
            trust_remote_code=True,
            torch_dtype=torch_dtype,
            low_cpu_mem_usage=config.low_cpu_mem_usage,
        ).to(config.device)
        self.model.eval()
        self.hidden_size = int(getattr(self.model.config, "hidden_size", 1024))
        n_layers = int(getattr(self.model.config, "num_hidden_layers", 24))
        self.mid_layer = max(1, n_layers // 2)

    def _pad_batch(self, sequences: Sequence[List[int]]) -> Tuple[torch.Tensor, torch.Tensor]:
        max_len = max(len(seq) for seq in sequences)
        pad_id = int(self.tokenizer.pad_token_id)
        input_ids = torch.full((len(sequences), max_len), pad_id, dtype=torch.long)
        attn = torch.zeros((len(sequences), max_len), dtype=torch.long)
        for i, seq in enumerate(sequences):
            seq_t = torch.tensor(seq, dtype=torch.long)
            input_ids[i, : len(seq)] = seq_t
            attn[i, : len(seq)] = 1
        return input_ids.to(self.config.device), attn.to(self.config.device)

    def _score_batch(self, pairs: Sequence[Tuple[str, str]]) -> List[Tuple[float, float, float, np.ndarray]]:
        base_prefix = "[THREAD UNKNOWN]\n[TARGET] "
        valid_indices: List[int] = []
        target_token_lists: List[List[int]] = []
        context_token_lists: List[List[int]] = []
        empty_results: List[Optional[Tuple[float, float, float, np.ndarray]]] = [None] * len(pairs)
        zero_vec = np.zeros((self.hidden_size,), dtype=np.float16)

        contexts = [context for context, _ in pairs]
        targets = [target for _, target in pairs]
        tokenized_contexts = self.tokenizer(contexts, add_special_tokens=False)["input_ids"]
        tokenized_targets = self.tokenizer(targets, add_special_tokens=False)["input_ids"]
        base_prefix_ids = self.tokenizer(base_prefix, add_special_tokens=False)["input_ids"]

        for i, (ctx_ids, tgt_ids) in enumerate(zip(tokenized_contexts, tokenized_targets)):
            if not tgt_ids:
                empty_results[i] = (0.0, 0.0, 0.0, zero_vec)
                continue
            valid_indices.append(i)
            context_token_lists.append(list(ctx_ids))
            target_token_lists.append(list(tgt_ids))

        if not valid_indices:
            return [item if item is not None else (0.0, 0.0, 0.0, zero_vec) for item in empty_results]

        cond_sequences: List[List[int]] = []
        cond_target_spans: List[Tuple[int, int]] = []
        base_sequences: List[List[int]] = []
        base_target_spans: List[Tuple[int, int]] = []

        for ctx_ids, tgt_ids in zip(context_token_lists, target_token_lists):
            full = (ctx_ids + tgt_ids)[-self.config.max_length :]
            full_len = len(ctx_ids) + len(tgt_ids)
            kept_start = max(0, full_len - self.config.max_length)
            target_start = max(0, len(ctx_ids) - kept_start)
            target_len = min(len(tgt_ids), len(full) - target_start)
            cond_sequences.append(full)
            cond_target_spans.append((target_start, target_len))

            base_full = (base_prefix_ids + tgt_ids)[-self.config.max_length :]
            base_len = len(base_prefix_ids) + len(tgt_ids)
            base_kept_start = max(0, base_len - self.config.max_length)
            base_target_start = max(0, len(base_prefix_ids) - base_kept_start)
            base_target_len = min(len(tgt_ids), len(base_full) - base_target_start)
            base_sequences.append(base_full)
            base_target_spans.append((base_target_start, base_target_len))

        cond_ids, cond_attn = self._pad_batch(cond_sequences)
        base_ids, base_attn = self._pad_batch(base_sequences)

        with torch.inference_mode():
            cond_out = self.model(
                input_ids=cond_ids,
                attention_mask=cond_attn,
                output_hidden_states=self.config.use_hidden_states,
            )
            base_out = self.model(
                input_ids=base_ids,
                attention_mask=base_attn,
                output_hidden_states=False,
            )

            cond_log_probs = F.log_softmax(cond_out.logits[:, :-1, :], dim=-1)
            cond_labels = cond_ids[:, 1:]
            base_log_probs = F.log_softmax(base_out.logits[:, :-1, :], dim=-1)
            base_labels = base_ids[:, 1:]

        batch_results: List[Tuple[float, float, float, np.ndarray]] = []
        hidden_states = cond_out.hidden_states[self.mid_layer if self.config.use_mid_layer else -1] if self.config.use_hidden_states else None

        for batch_idx, ((target_start, target_len), (base_start, base_len)) in enumerate(zip(cond_target_spans, base_target_spans)):
            if target_len <= 0 or base_len <= 0:
                batch_results.append((0.0, 0.0, 0.0, zero_vec))
                continue

            label_start = max(0, target_start - 1)
            label_end = max(label_start, target_start + target_len - 1)
            gather = cond_log_probs[batch_idx : batch_idx + 1, label_start:label_end, :].gather(
                -1, cond_labels[batch_idx : batch_idx + 1, label_start:label_end].unsqueeze(-1)
            ).squeeze(-1)
            cond_nll = float(-gather.mean().item()) if gather.numel() else 0.0

            label_start_b = max(0, base_start - 1)
            label_end_b = max(label_start_b, base_start + base_len - 1)
            gather_b = base_log_probs[batch_idx : batch_idx + 1, label_start_b:label_end_b, :].gather(
                -1, base_labels[batch_idx : batch_idx + 1, label_start_b:label_end_b].unsqueeze(-1)
            ).squeeze(-1)
            base_nll = float(-gather_b.mean().item()) if gather_b.numel() else cond_nll

            hidden_vec = zero_vec
            if hidden_states is not None:
                target_slice = hidden_states[batch_idx, target_start : target_start + target_len]
                if target_slice.numel() > 0:
                    hidden_vec = target_slice.mean(dim=0).detach().float().cpu().numpy().astype(np.float16)

            batch_results.append((cond_nll, base_nll, base_nll - cond_nll, hidden_vec))

        results: List[Tuple[float, float, float, np.ndarray]] = [None] * len(pairs)  # type: ignore[assignment]
        for original_idx, result in zip(valid_indices, batch_results):
            results[original_idx] = result
        for i, result in enumerate(results):
            if result is None:
                results[i] = empty_results[i] if empty_results[i] is not None else (0.0, 0.0, 0.0, zero_vec)
        return results  # type: ignore[return-value]

    def _score_sequence(self, context: str, target_text: str) -> Tuple[float, float, float, np.ndarray]:
        return self._score_batch([(context, target_text)])[0]

    def featurize_example(self, example: CandidateExample) -> Tuple[np.ndarray, np.ndarray]:
        if not example.candidate_text_contexts:
            return (
                np.zeros((0, 5), dtype=np.float32),
                np.zeros((0, self.hidden_size), dtype=np.float32),
            )

        pairs = list(zip(example.candidate_text_contexts, example.candidate_text_targets))
        sample_scalars: List[List[float]] = []
        sample_vecs: List[np.ndarray] = []
        for idx, (cond_nll, base_nll, lift, hidden_vec) in enumerate(self._score_batch(pairs)):
            target = example.candidate_text_targets[idx]
            sample_scalars.append([
                cond_nll,
                base_nll,
                lift,
                float(bool(target)),
                math.log1p(len(target)),
            ])
            sample_vecs.append(hidden_vec)

        return (
            np.asarray(sample_scalars, dtype=np.float32),
            np.stack(sample_vecs, axis=0).astype(np.float32, copy=False),
        )

    def featurize_dataset(self, dataset: ThreadChoiceDataset, output_path: str | Path) -> None:
        payload: Dict[str, Any] = {
            "sample_ids": [],
            "candidate_semantic_scalars": [],
            "candidate_semantic_vectors": [],
            "semantic_dim": self.hidden_size,
            "model_name": self.config.model_name,
        }
        total = len(dataset.examples)
        for start in range(0, total, self.config.cache_batch_size):
            batch_examples = dataset.examples[start : start + self.config.cache_batch_size]
            flat_pairs: List[Tuple[str, str]] = []
            counts: List[int] = []
            for ex in batch_examples:
                pairs = list(zip(ex.candidate_text_contexts, ex.candidate_text_targets))
                counts.append(len(pairs))
                flat_pairs.extend(pairs)

            flat_results = self._score_batch(flat_pairs)
            offset = 0
            for ex, count in zip(batch_examples, counts):
                sample_scalars: List[List[float]] = []
                sample_vecs: List[np.ndarray] = []
                for i, (cond_nll, base_nll, lift, hidden_vec) in enumerate(flat_results[offset : offset + count]):
                    target = ex.candidate_text_targets[i]
                    sample_scalars.append([
                        cond_nll,
                        base_nll,
                        lift,
                        float(bool(target)),
                        math.log1p(len(target)),
                    ])
                    sample_vecs.append(hidden_vec)
                offset += count
                payload["sample_ids"].append(ex.sample_id)
                payload["candidate_semantic_scalars"].append(sample_scalars)
                payload["candidate_semantic_vectors"].append(np.stack(sample_vecs, axis=0))
            processed = min(start + len(batch_examples), total)
            if processed == total or processed % max(100, self.config.cache_batch_size * 10) == 0:
                print(f"featurized {processed}/{total} samples", flush=True)

        torch.save(payload, output_path)


# =========================
# Fusion head
# =========================


@dataclass
class HeadConfig:
    struct_dim: int
    semantic_scalar_dim: int
    semantic_vec_dim: int
    hidden_dim: int = 256
    candidate_encoder_layers: int = 2
    candidate_encoder_heads: int = 4
    dropout: float = 0.10
    overgroup_lambda: float = 0.15
    struct_branch_dropout: float = 0.0
    semantic_residual_logit: bool = False
    semantic_struct_gate: bool = False
    structural_residual_logit: bool = False


class FusedThreadChooser(nn.Module):
    def __init__(self, cfg: HeadConfig) -> None:
        super().__init__()
        self.cfg = cfg
        self.struct_proj = nn.Sequential(
            nn.LayerNorm(cfg.struct_dim),
            nn.Linear(cfg.struct_dim, cfg.hidden_dim),
            nn.GELU(),
            nn.Dropout(cfg.dropout),
        )
        self.semantic_scalar_proj = nn.Sequential(
            nn.LayerNorm(cfg.semantic_scalar_dim),
            nn.Linear(cfg.semantic_scalar_dim, cfg.hidden_dim // 4),
            nn.GELU(),
            nn.Dropout(cfg.dropout),
        )
        self.semantic_vec_proj = nn.Sequential(
            nn.LayerNorm(cfg.semantic_vec_dim),
            nn.Linear(cfg.semantic_vec_dim, cfg.hidden_dim),
            nn.GELU(),
            nn.Dropout(cfg.dropout),
        )
        if cfg.semantic_struct_gate:
            self.semantic_gate = nn.Sequential(
                nn.LayerNorm(cfg.struct_dim),
                nn.Linear(cfg.struct_dim, cfg.hidden_dim // 4),
                nn.GELU(),
                nn.Dropout(cfg.dropout),
                nn.Linear(cfg.hidden_dim // 4, 1),
            )
        else:
            self.semantic_gate = None
        fused_dim = cfg.hidden_dim + cfg.hidden_dim // 4 + cfg.hidden_dim
        self.pre_fuse = nn.Sequential(
            nn.Linear(fused_dim, cfg.hidden_dim),
            nn.GELU(),
            nn.Dropout(cfg.dropout),
        )
        if cfg.structural_residual_logit:
            self.struct_out = nn.Sequential(
                nn.LayerNorm(cfg.hidden_dim),
                nn.Linear(cfg.hidden_dim, cfg.hidden_dim // 2),
                nn.GELU(),
                nn.Dropout(cfg.dropout),
                nn.Linear(cfg.hidden_dim // 2, 1),
            )
            self.struct_residual_scale = nn.Parameter(torch.tensor(1.0))
        else:
            self.struct_out = None
            self.register_parameter("struct_residual_scale", None)
        enc_layer = nn.TransformerEncoderLayer(
            d_model=cfg.hidden_dim,
            nhead=cfg.candidate_encoder_heads,
            dim_feedforward=cfg.hidden_dim * 4,
            dropout=cfg.dropout,
            batch_first=True,
            activation="gelu",
            norm_first=True,
        )
        self.encoder = nn.TransformerEncoder(enc_layer, num_layers=cfg.candidate_encoder_layers)
        self.out = nn.Sequential(
            nn.LayerNorm(cfg.hidden_dim),
            nn.Linear(cfg.hidden_dim, cfg.hidden_dim // 2),
            nn.GELU(),
            nn.Dropout(cfg.dropout),
            nn.Linear(cfg.hidden_dim // 2, 1),
        )
        if cfg.semantic_residual_logit:
            sem_dim = cfg.hidden_dim // 4 + cfg.hidden_dim
            self.semantic_out = nn.Sequential(
                nn.LayerNorm(sem_dim),
                nn.Linear(sem_dim, cfg.hidden_dim // 2),
                nn.GELU(),
                nn.Dropout(cfg.dropout),
                nn.Linear(cfg.hidden_dim // 2, 1),
            )
            self.semantic_residual_scale = nn.Parameter(torch.tensor(1.0))
        else:
            self.semantic_out = None
            self.register_parameter("semantic_residual_scale", None)

    def forward(
        self,
        struct_x: torch.Tensor,
        semantic_scalars: torch.Tensor,
        semantic_vecs: torch.Tensor,
        candidate_mask: torch.Tensor,
    ) -> torch.Tensor:
        h_struct = self.struct_proj(struct_x)
        if self.training and self.cfg.struct_branch_dropout > 0.0:
            keep = torch.rand(h_struct.shape[0], 1, 1, device=h_struct.device) >= self.cfg.struct_branch_dropout
            h_struct = h_struct * keep
        h_scalars = self.semantic_scalar_proj(semantic_scalars)
        h_sem = self.semantic_vec_proj(semantic_vecs)
        if self.semantic_gate is not None:
            semantic_gate = torch.sigmoid(self.semantic_gate(struct_x))
            h_scalars = h_scalars * semantic_gate
            h_sem = h_sem * semantic_gate
        semantic_direct = torch.cat([h_scalars, h_sem], dim=-1)
        h = torch.cat([h_struct, h_scalars, h_sem], dim=-1)
        h = self.pre_fuse(h)
        # key padding mask expects True where padding should be ignored
        h = self.encoder(h, src_key_padding_mask=~candidate_mask.bool())
        logits = self.out(h).squeeze(-1)
        if self.struct_out is not None:
            logits = logits + self.struct_residual_scale * self.struct_out(h_struct).squeeze(-1)
        if self.semantic_out is not None:
            logits = logits + self.semantic_residual_scale * self.semantic_out(semantic_direct).squeeze(-1)
        logits = logits.masked_fill(~candidate_mask.bool(), -1e9)
        return logits


# =========================
# Train/eval helpers
# =========================


class CachedSemanticDataset(Dataset):
    def __init__(
        self,
        base_dataset: ThreadChoiceDataset,
        semantic_cache_path: str | Path,
        indices: Optional[List[int]] = None,
        zero_structural: bool = False,
        zero_semantic_scalars: bool = False,
        zero_semantic_vectors: bool = False,
        struct_dim_limit: Optional[int] = None,
    ) -> None:
        cache = torch.load(semantic_cache_path, map_location="cpu", weights_only=False)
        sample_id_to_pos = {sid: i for i, sid in enumerate(cache["sample_ids"])}
        self.base = base_dataset
        self.indices = indices if indices is not None else list(range(len(base_dataset)))
        self.zero_structural = zero_structural
        self.zero_semantic_scalars = zero_semantic_scalars
        self.zero_semantic_vectors = zero_semantic_vectors
        self.struct_dim_limit = struct_dim_limit
        self.scalar_dim = len(cache["candidate_semantic_scalars"][0][0])
        self.vec_dim = int(cache["semantic_dim"])
        self.cache_scalars = cache["candidate_semantic_scalars"]
        self.cache_vecs = cache["candidate_semantic_vectors"]
        self.sample_id_to_pos = sample_id_to_pos

    def __len__(self) -> int:
        return len(self.indices)

    def __getitem__(self, idx: int) -> Dict[str, Any]:
        ex = self.base.examples[self.indices[idx]]
        pos = self.sample_id_to_pos[ex.sample_id]
        struct = np.asarray(ex.candidate_struct, dtype=np.float32)
        if self.struct_dim_limit is not None:
            struct = struct[:, : self.struct_dim_limit]
        semantic_scalars = np.asarray(self.cache_scalars[pos], dtype=np.float32)
        semantic_vecs = np.asarray(self.cache_vecs[pos], dtype=np.float32)
        if self.zero_structural:
            struct = np.zeros_like(struct)
        if self.zero_semantic_scalars:
            semantic_scalars = np.zeros_like(semantic_scalars)
        if self.zero_semantic_vectors:
            semantic_vecs = np.zeros_like(semantic_vecs)
        return {
            "sample_id": ex.sample_id,
            "gold_index": ex.gold_index,
            "struct": struct,
            "semantic_scalars": semantic_scalars,
            "semantic_vecs": semantic_vecs,
            "candidate_is_group": np.asarray(ex.candidate_is_group, dtype=np.int64),
            "candidate_identity_compatible": np.asarray(
                ex.metadata.get("candidate_identity_compatible", [False] * len(ex.candidate_is_group)),
                dtype=np.bool_,
            ),
            "candidate_exact_structural_match": np.asarray(
                ex.metadata.get("candidate_exact_structural_match", [False] * len(ex.candidate_is_group)),
                dtype=np.bool_,
            ),
            "target_is_group": float(ex.target_features["target_is_group"]),
            "target_has_explicit_identity": float(bool(ex.metadata.get("target_has_explicit_counterparty"))),
            "metadata": ex.metadata,
        }


def collate_candidate_sets(batch: List[Dict[str, Any]]) -> Dict[str, torch.Tensor]:
    max_c = max(item["struct"].shape[0] for item in batch)
    struct_dim = batch[0]["struct"].shape[1]
    scalar_dim = batch[0]["semantic_scalars"].shape[1]
    vec_dim = batch[0]["semantic_vecs"].shape[1]

    B = len(batch)
    struct = torch.zeros(B, max_c, struct_dim, dtype=torch.float32)
    sem_scalars = torch.zeros(B, max_c, scalar_dim, dtype=torch.float32)
    sem_vecs = torch.zeros(B, max_c, vec_dim, dtype=torch.float32)
    mask = torch.zeros(B, max_c, dtype=torch.bool)
    cand_is_group = torch.zeros(B, max_c, dtype=torch.bool)
    cand_identity_compatible = torch.zeros(B, max_c, dtype=torch.bool)
    cand_exact_structural_match = torch.zeros(B, max_c, dtype=torch.bool)
    gold = torch.zeros(B, dtype=torch.long)
    target_is_group = torch.zeros(B, dtype=torch.float32)
    target_has_explicit_identity = torch.zeros(B, dtype=torch.float32)

    meta: List[Dict[str, Any]] = []
    sample_ids: List[str] = []
    for i, item in enumerate(batch):
        c = item["struct"].shape[0]
        struct[i, :c] = torch.from_numpy(item["struct"])
        sem_scalars[i, :c] = torch.from_numpy(item["semantic_scalars"])
        sem_vecs[i, :c] = torch.from_numpy(item["semantic_vecs"])
        mask[i, :c] = True
        cand_is_group[i, :c] = torch.from_numpy(item["candidate_is_group"].astype(np.bool_))
        cand_identity_compatible[i, :c] = torch.from_numpy(item["candidate_identity_compatible"].astype(np.bool_))
        cand_exact_structural_match[i, :c] = torch.from_numpy(item["candidate_exact_structural_match"].astype(np.bool_))
        gold[i] = int(item["gold_index"])
        target_is_group[i] = float(item["target_is_group"])
        target_has_explicit_identity[i] = float(item["target_has_explicit_identity"])
        meta.append(item["metadata"])
        sample_ids.append(item["sample_id"])

    return {
        "struct": struct,
        "semantic_scalars": sem_scalars,
        "semantic_vecs": sem_vecs,
        "mask": mask,
        "candidate_is_group": cand_is_group,
        "candidate_identity_compatible": cand_identity_compatible,
        "candidate_exact_structural_match": cand_exact_structural_match,
        "gold": gold,
        "target_is_group": target_is_group,
        "target_has_explicit_identity": target_has_explicit_identity,
        "metadata": meta,
        "sample_ids": sample_ids,
    }


def checkpoint_runtime_config(checkpoint: Dict[str, Any]) -> Dict[str, Any]:
    report = checkpoint.get("report")
    if isinstance(report, dict):
        cfg = report.get("config")
        if isinstance(cfg, dict):
            return cfg
    cfg = checkpoint.get("config")
    return cfg if isinstance(cfg, dict) else {}


def infer_head_config(
    checkpoint: Dict[str, Any],
    *,
    struct_dim: int,
    semantic_scalar_dim: int,
    semantic_vec_dim: int,
    default_candidate_encoder_heads: int = 4,
) -> HeadConfig:
    state_dict = checkpoint["state_dict"]
    runtime_cfg = checkpoint_runtime_config(checkpoint)
    hidden_dim = int(state_dict["struct_proj.1.weight"].shape[0])
    layer_ids = [
        int(match.group(1))
        for key in state_dict
        for match in [re.match(r"encoder\.layers\.(\d+)\.", key)]
        if match is not None
    ]
    candidate_encoder_layers = (max(layer_ids) + 1) if layer_ids else 2
    semantic_residual_logit = bool(runtime_cfg.get("semantic_residual_logit", "semantic_out.0.weight" in state_dict))
    semantic_struct_gate = bool(runtime_cfg.get("semantic_struct_gate", "semantic_gate.0.weight" in state_dict))
    structural_residual_logit = bool(runtime_cfg.get("structural_residual_logit", "struct_out.0.weight" in state_dict))
    return HeadConfig(
        struct_dim=struct_dim,
        semantic_scalar_dim=semantic_scalar_dim,
        semantic_vec_dim=semantic_vec_dim,
        hidden_dim=hidden_dim,
        candidate_encoder_layers=candidate_encoder_layers,
        candidate_encoder_heads=int(runtime_cfg.get("candidate_encoder_heads", default_candidate_encoder_heads)),
        dropout=float(runtime_cfg.get("dropout", 0.10)),
        overgroup_lambda=float(runtime_cfg.get("overgroup_lambda", 0.15)),
        struct_branch_dropout=float(runtime_cfg.get("struct_branch_dropout", 0.0)),
        semantic_residual_logit=semantic_residual_logit,
        semantic_struct_gate=semantic_struct_gate,
        structural_residual_logit=structural_residual_logit,
    )


def split_by_window(dataset: ThreadChoiceDataset, dev_windows: int = 2, test_windows: int = 2, seed: int = 7) -> Dict[str, List[int]]:
    buckets: Dict[str, List[int]] = defaultdict(list)
    for i, ex in enumerate(dataset.examples):
        wid = str(ex.metadata.get("window_id", f"real::{i}"))
        buckets[wid].append(i)
    window_ids = sorted(buckets)
    rng = random.Random(seed)
    rng.shuffle(window_ids)
    dev_ids = set(window_ids[:dev_windows])
    test_ids = set(window_ids[dev_windows : dev_windows + test_windows])
    splits = {"train": [], "dev": [], "test": []}
    for wid, idxs in buckets.items():
        if wid in dev_ids:
            splits["dev"].extend(idxs)
        elif wid in test_ids:
            splits["test"].extend(idxs)
        else:
            splits["train"].extend(idxs)
    return splits


def compute_loss(
    logits: torch.Tensor,
    gold: torch.Tensor,
    candidate_is_group: torch.Tensor,
    candidate_identity_compatible: torch.Tensor,
    candidate_exact_structural_match: torch.Tensor,
    target_is_group: torch.Tensor,
    target_has_explicit_identity: torch.Tensor,
    overgroup_lambda: float = 0.15,
    identity_incompatible_lambda: float = 0.20,
    exact_match_preference_lambda: float = 0.15,
) -> torch.Tensor:
    ce = F.cross_entropy(logits, gold)
    probs = logits.softmax(dim=-1)
    # Penalize probability mass on group candidates when the target is direct.
    direct_mask = (target_is_group < 0.5).float().unsqueeze(-1)
    group_mass_direct = (probs * candidate_is_group.float() * direct_mask).sum(dim=-1)
    overgroup_pen = group_mass_direct.mean()
    explicit_direct_mask = (
        (target_is_group < 0.5)
        & (target_has_explicit_identity > 0.5)
        & candidate_identity_compatible.any(dim=-1)
    ).float()
    incompatible_mass = (probs * (~candidate_identity_compatible).float()).sum(dim=-1)
    identity_pen = (
        (incompatible_mass * explicit_direct_mask).sum()
        / explicit_direct_mask.sum().clamp_min(1.0)
    )
    exact_match_mask = (
        (target_is_group < 0.5)
        & candidate_exact_structural_match.gather(1, gold.unsqueeze(-1)).squeeze(-1)
    ).float()
    non_exact_mass = (probs * (~candidate_exact_structural_match).float()).sum(dim=-1)
    exact_match_pen = (
        (non_exact_mass * exact_match_mask).sum()
        / exact_match_mask.sum().clamp_min(1.0)
    )
    return (
        ce
        + overgroup_lambda * overgroup_pen
        + identity_incompatible_lambda * identity_pen
        + exact_match_preference_lambda * exact_match_pen
    )


def run_eval(model: nn.Module, loader: DataLoader, device: str) -> Dict[str, float]:
    model.eval()
    total = 0
    correct = 0
    mrr = 0.0
    direct_total = 0
    direct_correct = 0
    group_total = 0
    group_correct = 0
    sent_total = 0
    sent_correct = 0
    generic_total = 0
    generic_correct = 0
    no_text_total = 0
    no_text_correct = 0
    overgroup_errors = 0
    undergroup_errors = 0
    direct_group_mass = 0.0
    direct_identity_total = 0
    direct_identity_match = 0
    direct_wrong_counterparty_errors = 0
    sent_identity_total = 0
    sent_identity_match = 0
    sent_wrong_counterparty_errors = 0

    with torch.no_grad():
        for batch in loader:
            logits = model(
                batch["struct"].to(device),
                batch["semantic_scalars"].to(device),
                batch["semantic_vecs"].to(device),
                batch["mask"].to(device),
            )
            probs = logits.softmax(dim=-1)
            pred = logits.argmax(dim=-1).cpu()
            gold = batch["gold"]
            total += gold.numel()
            correct_mask = pred.eq(gold)
            correct += int(correct_mask.sum().item())

            for i in range(gold.numel()):
                order = torch.argsort(logits[i], descending=True)
                rank = int((order == gold[i].to(order.device)).nonzero(as_tuple=False)[0].item()) + 1
                mrr += 1.0 / rank
                meta = batch["metadata"][i]
                tgt_is_group = bool(batch["target_is_group"][i].item() > 0.5)
                pred_is_group = bool(batch["candidate_is_group"][i, pred[i]].item())
                gold_is_group = bool(batch["candidate_is_group"][i, gold[i]].item())
                pred_idx = int(pred[i].item())
                gold_idx = int(gold[i].item())
                candidate_identity_sets = [
                    set(candidate_keys)
                    for candidate_keys in meta.get("candidate_explicit_counterparty_keys", [])
                ]
                target_identity_keys = set(meta.get("target_explicit_counterparty_keys", []))
                pred_identity_match = (
                    pred_idx < len(candidate_identity_sets)
                    and bool(target_identity_keys & candidate_identity_sets[pred_idx])
                )
                gold_identity_match = (
                    gold_idx < len(candidate_identity_sets)
                    and bool(target_identity_keys & candidate_identity_sets[gold_idx])
                )
                if not tgt_is_group:
                    direct_total += 1
                    direct_correct += int(correct_mask[i].item())
                    direct_group_mass += float((probs[i] * batch["candidate_is_group"][i].to(device).float()).sum().item())
                    if target_identity_keys and gold_identity_match:
                        direct_identity_total += 1
                        direct_identity_match += int(pred_identity_match)
                        if not pred_identity_match:
                            direct_wrong_counterparty_errors += 1
                else:
                    group_total += 1
                    group_correct += int(correct_mask[i].item())
                if meta.get("generic_preview"):
                    generic_total += 1
                    generic_correct += int(correct_mask[i].item())
                preview = normalize_ws(meta.get("preview"))
                if not preview:
                    no_text_total += 1
                    no_text_correct += int(correct_mask[i].item())
                # sent slice is inferred from the structural feature position 1 (target_is_outbound)
                if batch["struct"][i, 0, 1].item() > 0.5:
                    sent_total += 1
                    sent_correct += int(correct_mask[i].item())
                    if target_identity_keys and gold_identity_match:
                        sent_identity_total += 1
                        sent_identity_match += int(pred_identity_match)
                        if not pred_identity_match:
                            sent_wrong_counterparty_errors += 1
                if not correct_mask[i].item():
                    if (not gold_is_group) and pred_is_group:
                        overgroup_errors += 1
                    if gold_is_group and (not pred_is_group):
                        undergroup_errors += 1

    def frac(a: int, b: int) -> float:
        return float(a / b) if b else 0.0

    return {
        "top1": frac(correct, total),
        "mrr": mrr / total if total else 0.0,
        "direct_top1": frac(direct_correct, direct_total),
        "group_top1": frac(group_correct, group_total),
        "sent_top1": frac(sent_correct, sent_total),
        "direct_identity_match_top1": frac(direct_identity_match, direct_identity_total),
        "sent_identity_match_top1": frac(sent_identity_match, sent_identity_total),
        "generic_top1": frac(generic_correct, generic_total),
        "no_text_top1": frac(no_text_correct, no_text_total),
        "overgroup_errors": float(overgroup_errors),
        "undergroup_errors": float(undergroup_errors),
        "direct_wrong_counterparty_errors": float(direct_wrong_counterparty_errors),
        "sent_wrong_counterparty_errors": float(sent_wrong_counterparty_errors),
        "direct_group_mass": direct_group_mass / direct_total if direct_total else 0.0,
        "n": float(total),
    }


def train_head(
    train_ds: CachedSemanticDataset,
    dev_ds: CachedSemanticDataset,
    test_ds: Optional[CachedSemanticDataset] = None,
    real_ds: Optional[CachedSemanticDataset] = None,
    hard_eval_sets: Optional[Dict[str, CachedSemanticDataset]] = None,
    init_checkpoint: Optional[str] = None,
    epochs: int = 20,
    batch_size: int = 64,
    lr: float = 2e-4,
    weight_decay: float = 1e-2,
    device: str = "cuda" if torch.cuda.is_available() else "cpu",
    overgroup_lambda: float = 0.15,
    identity_incompatible_lambda: float = 0.20,
    exact_match_preference_lambda: float = 0.15,
    struct_branch_dropout: float = 0.0,
    semantic_residual_logit: bool = False,
    semantic_struct_gate: bool = False,
    structural_residual_logit: bool = False,
    seed: int = 7,
) -> Tuple[FusedThreadChooser, Dict[str, float]]:
    torch.manual_seed(seed)
    random.seed(seed)
    np.random.seed(seed)

    cfg = HeadConfig(
        struct_dim=train_ds[0]["struct"].shape[1],
        semantic_scalar_dim=train_ds[0]["semantic_scalars"].shape[1],
        semantic_vec_dim=train_ds[0]["semantic_vecs"].shape[1],
        overgroup_lambda=overgroup_lambda,
        struct_branch_dropout=struct_branch_dropout,
        semantic_residual_logit=semantic_residual_logit,
        semantic_struct_gate=semantic_struct_gate,
        structural_residual_logit=structural_residual_logit,
    )
    model = FusedThreadChooser(cfg).to(device)
    if init_checkpoint:
        state = torch.load(init_checkpoint, map_location="cpu")
        state_dict = state["state_dict"] if isinstance(state, dict) and "state_dict" in state else state
        model.load_state_dict(state_dict)
    opt = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=weight_decay)

    pin_memory = device.startswith("cuda")
    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True, collate_fn=collate_candidate_sets, pin_memory=pin_memory)
    dev_loader = DataLoader(dev_ds, batch_size=batch_size, shuffle=False, collate_fn=collate_candidate_sets, pin_memory=pin_memory)
    test_loader = (
        DataLoader(test_ds, batch_size=batch_size, shuffle=False, collate_fn=collate_candidate_sets, pin_memory=pin_memory)
        if test_ds is not None and len(test_ds) > 0
        else None
    )
    real_loader = (
        DataLoader(real_ds, batch_size=batch_size, shuffle=False, collate_fn=collate_candidate_sets, pin_memory=pin_memory) if real_ds is not None else None
    )
    hard_eval_loaders = {
        name: DataLoader(ds, batch_size=batch_size, shuffle=False, collate_fn=collate_candidate_sets, pin_memory=pin_memory)
        for name, ds in (hard_eval_sets or {}).items()
        if ds is not None and len(ds) > 0
    }

    best_score = -1e9
    best_state = None
    best_metrics: Dict[str, float] = {}

    for epoch in range(1, epochs + 1):
        model.train()
        for batch in train_loader:
            opt.zero_grad(set_to_none=True)
            logits = model(
                batch["struct"].to(device),
                batch["semantic_scalars"].to(device),
                batch["semantic_vecs"].to(device),
                batch["mask"].to(device),
            )
            loss = compute_loss(
                logits,
                batch["gold"].to(device),
                batch["candidate_is_group"].to(device),
                batch["candidate_identity_compatible"].to(device),
                batch["candidate_exact_structural_match"].to(device),
                batch["target_is_group"].to(device),
                batch["target_has_explicit_identity"].to(device),
                overgroup_lambda=overgroup_lambda,
                identity_incompatible_lambda=identity_incompatible_lambda,
                exact_match_preference_lambda=exact_match_preference_lambda,
            )
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()

        dev_metrics = run_eval(model, dev_loader, device)
        print(
            f"epoch {epoch}/{epochs} dev_top1={dev_metrics['top1']:.4f} "
            f"dev_mrr={dev_metrics['mrr']:.4f} "
            f"dev_overgroup={dev_metrics['overgroup_errors']:.0f} "
            f"dev_group_mass={dev_metrics['direct_group_mass']:.4f} "
            f"dev_wrong_direct={dev_metrics['direct_wrong_counterparty_errors']:.0f}",
            flush=True,
        )
        # choose checkpoint on synthetic held-out windows, but account for asymmetric error cost
        score = (
            dev_metrics["top1"]
            - 0.25 * dev_metrics["direct_group_mass"]
            - 0.01 * dev_metrics["overgroup_errors"]
            - 0.05 * dev_metrics["direct_wrong_counterparty_errors"]
        )
        if score > best_score:
            best_score = score
            best_state = {k: v.detach().cpu() for k, v in model.state_dict().items()}
            best_metrics = {f"dev_{k}": v for k, v in dev_metrics.items()}
            if test_loader is not None:
                best_metrics.update({f"test_{k}": v for k, v in run_eval(model, test_loader, device).items()})
            if real_loader is not None:
                best_metrics.update({f"real_{k}": v for k, v in run_eval(model, real_loader, device).items()})
            for name, loader in hard_eval_loaders.items():
                best_metrics.update({f"{name}_{k}": v for k, v in run_eval(model, loader, device).items()})

    if best_state is not None:
        model.load_state_dict(best_state)
    return model, best_metrics


# =========================
# CLI
# =========================


def cmd_featurize(args: argparse.Namespace) -> None:
    ds = ThreadChoiceDataset(
        args.input,
        include_candidate_score=args.include_candidate_score,
        max_history_turns=args.max_history_turns,
        include_candidate_display_name_in_qwen=args.include_candidate_display_name_in_qwen,
        include_nearby_notifications_in_qwen=args.include_nearby_notifications_in_qwen,
    )
    featurizer = FrozenCausalSemanticFeaturizer(
        SemanticConfig(
            model_name=args.model_name,
            device=args.device,
            dtype=args.dtype,
            use_hidden_states=not args.no_hidden_states,
            use_mid_layer=not args.use_final_layer,
            cache_batch_size=args.batch_size,
            max_length=args.max_length,
        )
    )
    featurizer.featurize_dataset(ds, args.output)
    print(f"saved semantic cache -> {args.output}")


def cmd_train(args: argparse.Namespace) -> None:
    if bool(args.hard_real) != bool(args.hard_real_cache):
        raise ValueError("--hard-real and --hard-real-cache must be provided together.")

    synth_ds = ThreadChoiceDataset(
        args.synthetic,
        include_candidate_score=args.include_candidate_score,
        max_history_turns=args.max_history_turns,
        include_candidate_display_name_in_qwen=args.include_candidate_display_name_in_qwen,
        include_nearby_notifications_in_qwen=args.include_nearby_notifications_in_qwen,
    )
    real_ds = ThreadChoiceDataset(
        args.real,
        include_candidate_score=args.include_candidate_score,
        max_history_turns=args.max_history_turns,
        include_candidate_display_name_in_qwen=args.include_candidate_display_name_in_qwen,
        include_nearby_notifications_in_qwen=args.include_nearby_notifications_in_qwen,
    )
    hard_real_ds = (
        ThreadChoiceDataset(
            args.hard_real,
            include_candidate_score=args.include_candidate_score,
            max_history_turns=args.max_history_turns,
            include_candidate_display_name_in_qwen=args.include_candidate_display_name_in_qwen,
            include_nearby_notifications_in_qwen=args.include_nearby_notifications_in_qwen,
        )
        if args.hard_real
        else None
    )
    splits = split_by_window(synth_ds, dev_windows=args.dev_windows, test_windows=args.test_windows, seed=args.seed)
    train_cached = CachedSemanticDataset(
        synth_ds,
        args.synthetic_cache,
        indices=splits["train"],
        zero_structural=args.zero_structural,
        zero_semantic_scalars=args.zero_semantic_scalars,
        zero_semantic_vectors=args.zero_semantic_vectors,
    )
    dev_cached = CachedSemanticDataset(
        synth_ds,
        args.synthetic_cache,
        indices=splits["dev"],
        zero_structural=args.zero_structural,
        zero_semantic_scalars=args.zero_semantic_scalars,
        zero_semantic_vectors=args.zero_semantic_vectors,
    )
    test_cached = CachedSemanticDataset(
        synth_ds,
        args.synthetic_cache,
        indices=splits["test"],
        zero_structural=args.zero_structural,
        zero_semantic_scalars=args.zero_semantic_scalars,
        zero_semantic_vectors=args.zero_semantic_vectors,
    )
    real_cached = CachedSemanticDataset(
        real_ds,
        args.real_cache,
        zero_structural=args.zero_structural,
        zero_semantic_scalars=args.zero_semantic_scalars,
        zero_semantic_vectors=args.zero_semantic_vectors,
    )
    hard_eval_sets = (
        {
            args.hard_real_name: CachedSemanticDataset(
                hard_real_ds,
                args.hard_real_cache,
                zero_structural=args.zero_structural,
                zero_semantic_scalars=args.zero_semantic_scalars,
                zero_semantic_vectors=args.zero_semantic_vectors,
            )
        }
        if hard_real_ds is not None and args.hard_real_cache
        else None
    )

    model, metrics = train_head(
        train_cached,
        dev_cached,
        test_ds=test_cached,
        real_ds=real_cached,
        hard_eval_sets=hard_eval_sets,
        init_checkpoint=args.init_checkpoint,
        epochs=args.epochs,
        batch_size=args.batch_size,
        lr=args.lr,
        weight_decay=args.weight_decay,
        device=args.device,
        overgroup_lambda=args.overgroup_lambda,
        identity_incompatible_lambda=args.identity_incompatible_lambda,
        exact_match_preference_lambda=args.exact_match_preference_lambda,
        struct_branch_dropout=args.struct_branch_dropout,
        semantic_residual_logit=args.semantic_residual_logit,
        semantic_struct_gate=args.semantic_struct_gate,
        structural_residual_logit=args.structural_residual_logit,
        seed=args.seed,
    )
    out = {
        "metrics": metrics,
        "split_counts": {name: len(indices) for name, indices in splits.items()},
        "struct_feature_names": synth_ds.struct_names,
        "synthetic_meta": synth_ds.meta,
        "real_meta": real_ds.meta,
        "hard_real_meta": hard_real_ds.meta if hard_real_ds is not None else None,
        "config": {k: v for k, v in vars(args).items() if k != "func"},
    }
    torch.save({"state_dict": model.state_dict(), "report": out}, args.output)
    print(json.dumps(out, indent=2))
    print(f"saved model/report -> {args.output}")


def cmd_eval(args: argparse.Namespace) -> None:
    checkpoint = torch.load(args.checkpoint, map_location="cpu")
    expected_struct_dim = int(checkpoint["state_dict"]["struct_proj.1.weight"].shape[1])
    dataset = ThreadChoiceDataset(
        args.input,
        include_candidate_score=args.include_candidate_score,
        max_history_turns=args.max_history_turns,
        include_candidate_display_name_in_qwen=args.include_candidate_display_name_in_qwen,
        include_nearby_notifications_in_qwen=args.include_nearby_notifications_in_qwen,
    )
    cached = CachedSemanticDataset(
        dataset,
        args.cache,
        zero_structural=args.zero_structural,
        zero_semantic_scalars=args.zero_semantic_scalars,
        zero_semantic_vectors=args.zero_semantic_vectors,
        struct_dim_limit=expected_struct_dim,
    )
    loader = DataLoader(cached, batch_size=args.batch_size, shuffle=False, collate_fn=collate_candidate_sets)

    sample = cached[0]
    cfg = infer_head_config(
        checkpoint,
        struct_dim=sample["struct"].shape[1],
        semantic_scalar_dim=sample["semantic_scalars"].shape[1],
        semantic_vec_dim=sample["semantic_vecs"].shape[1],
    )
    model = FusedThreadChooser(cfg)
    model.load_state_dict(checkpoint["state_dict"])
    model.to(args.device)

    metrics = run_eval(model, loader, args.device)
    out = {
        "checkpoint": args.checkpoint,
        "input": args.input,
        "cache": args.cache,
        "metrics": metrics,
        "config": {k: v for k, v in vars(args).items() if k != "func"},
    }
    print(json.dumps(out, indent=2))


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Thread-model fused chooser stack")
    sub = p.add_subparsers(dest="cmd", required=True)

    pf = sub.add_parser("featurize")
    pf.add_argument("--input", required=True)
    pf.add_argument("--output", required=True)
    pf.add_argument("--model-name", required=True)
    pf.add_argument("--device", default="cuda" if torch.cuda.is_available() else "cpu")
    pf.add_argument("--dtype", default="float16")
    pf.add_argument("--batch-size", type=int, default=8)
    pf.add_argument("--max-length", type=int, default=1024)
    pf.add_argument("--max-history-turns", type=int, default=8)
    pf.add_argument("--no-hidden-states", action="store_true")
    pf.add_argument("--use-final-layer", action="store_true")
    pf.add_argument("--include-candidate-score", action="store_true")
    pf.add_argument("--include-candidate-display-name-in-qwen", action="store_true")
    pf.add_argument("--include-nearby-notifications-in-qwen", action="store_true")
    pf.set_defaults(func=cmd_featurize)

    pt = sub.add_parser("train")
    pt.add_argument("--synthetic", required=True)
    pt.add_argument("--real", required=True)
    pt.add_argument("--synthetic-cache", required=True)
    pt.add_argument("--real-cache", required=True)
    pt.add_argument("--hard-real")
    pt.add_argument("--hard-real-cache")
    pt.add_argument("--hard-real-name", default="hard")
    pt.add_argument("--output", required=True)
    pt.add_argument("--init-checkpoint")
    pt.add_argument("--device", default="cuda" if torch.cuda.is_available() else "cpu")
    pt.add_argument("--epochs", type=int, default=20)
    pt.add_argument("--batch-size", type=int, default=64)
    pt.add_argument("--lr", type=float, default=2e-4)
    pt.add_argument("--weight-decay", type=float, default=1e-2)
    pt.add_argument("--overgroup-lambda", type=float, default=0.15)
    pt.add_argument("--identity-incompatible-lambda", type=float, default=0.20)
    pt.add_argument("--exact-match-preference-lambda", type=float, default=0.15)
    pt.add_argument("--struct-branch-dropout", type=float, default=0.0)
    pt.add_argument("--semantic-residual-logit", action="store_true")
    pt.add_argument("--semantic-struct-gate", action="store_true")
    pt.add_argument("--structural-residual-logit", action="store_true")
    pt.add_argument("--seed", type=int, default=7)
    pt.add_argument("--dev-windows", type=int, default=2)
    pt.add_argument("--test-windows", type=int, default=2)
    pt.add_argument("--max-history-turns", type=int, default=8)
    pt.add_argument("--include-candidate-score", action="store_true")
    pt.add_argument("--include-candidate-display-name-in-qwen", action="store_true")
    pt.add_argument("--include-nearby-notifications-in-qwen", action="store_true")
    pt.add_argument("--zero-structural", action="store_true")
    pt.add_argument("--zero-semantic-scalars", action="store_true")
    pt.add_argument("--zero-semantic-vectors", action="store_true")
    pt.set_defaults(func=cmd_train)

    pe = sub.add_parser("eval")
    pe.add_argument("--input", required=True)
    pe.add_argument("--cache", required=True)
    pe.add_argument("--checkpoint", required=True)
    pe.add_argument("--device", default="cuda" if torch.cuda.is_available() else "cpu")
    pe.add_argument("--batch-size", type=int, default=64)
    pe.add_argument("--max-history-turns", type=int, default=8)
    pe.add_argument("--include-candidate-score", action="store_true")
    pe.add_argument("--include-candidate-display-name-in-qwen", action="store_true")
    pe.add_argument("--include-nearby-notifications-in-qwen", action="store_true")
    pe.add_argument("--zero-structural", action="store_true")
    pe.add_argument("--zero-semantic-scalars", action="store_true")
    pe.add_argument("--zero-semantic-vectors", action="store_true")
    pe.set_defaults(func=cmd_eval)

    return p


def main() -> None:
    args = build_argparser().parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
