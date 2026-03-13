from __future__ import annotations

import argparse
import json
import sys
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import torch

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from thread_fused_stack import (  # type: ignore
    FrozenCausalSemanticFeaturizer,
    FusedThreadChooser,
    SemanticConfig,
    build_candidate_example,
    checkpoint_runtime_config,
    infer_head_config,
)


SEMANTIC_SCALAR_NAMES = [
    "cond_nll",
    "base_nll",
    "lift",
    "semantic_ok",
    "target_len_log1p",
]


def load_json_payload(path: str) -> Dict[str, Any]:
    if path == "-":
        payload = json.load(sys.stdin)
    else:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("input JSON must be an object")
    return payload


def extract_single_sample(payload: Dict[str, Any]) -> Dict[str, Any]:
    if "samples" in payload:
        samples = payload.get("samples")
        if not isinstance(samples, list) or len(samples) != 1:
            raise ValueError("payload.samples must contain exactly one sample")
        sample = samples[0]
    else:
        sample = payload

    if not isinstance(sample, dict):
        raise ValueError("sample payload must be a JSON object")
    sample = normalize_runtime_sample(sample)
    if not sample.get("candidate_threads"):
        raise ValueError("sample must include at least one candidate thread")
    return sample


def rename_key(obj: Dict[str, Any], old: str, new: str) -> None:
    if old in obj and new not in obj:
        obj[new] = obj.pop(old)


def normalize_runtime_sample(sample: Dict[str, Any]) -> Dict[str, Any]:
    sample = dict(sample)
    rename_key(sample, "sampleId", "sample_id")
    rename_key(sample, "candidateThreads", "candidate_threads")
    rename_key(sample, "nearbyNotifications", "nearby_notifications")
    rename_key(sample, "fallbackThreadId", "fallback_thread_id")
    rename_key(sample, "goldThreadId", "gold_thread_id")

    message = sample.get("message")
    if isinstance(message, dict):
        rename_key(message, "sortUtc", "sort_utc")
        rename_key(message, "isGroup", "is_group")
        rename_key(message, "conversationDisplayName", "conversation_display_name")
        nested = message.get("message")
        if isinstance(nested, dict):
            # training path expects senderName/senderAddressing camelCase, so keep them as-is
            pass

    metadata = sample.get("metadata")
    if isinstance(metadata, dict):
        rename_key(metadata, "genericPreview", "generic_preview")

    nearby = sample.get("nearby_notifications")
    if isinstance(nearby, list):
        for item in nearby:
            if not isinstance(item, dict):
                continue
            rename_key(item, "notificationUid", "notification_uid")
            rename_key(item, "receivedUtc", "received_utc")
            rename_key(item, "appIdentifier", "app_identifier")

    candidates = sample.get("candidate_threads")
    if isinstance(candidates, list):
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            rename_key(candidate, "threadId", "thread_id")
            rename_key(candidate, "displayName", "display_name")
            rename_key(candidate, "isGroup", "is_group")
            rename_key(candidate, "candidateScore", "candidate_score")
            features = candidate.get("features")
            if isinstance(features, dict):
                rename_key(features, "previewOverlap", "preview_overlap")
                rename_key(features, "deltaSeconds", "delta_seconds")
                rename_key(features, "candidateScore", "candidate_score")
            history = candidate.get("history")
            if isinstance(history, list):
                for turn in history:
                    if not isinstance(turn, dict):
                        continue
                    rename_key(turn, "messageKey", "message_key")
                    rename_key(turn, "sortUtc", "sort_utc")
                    rename_key(turn, "senderName", "sender_name")
                    rename_key(turn, "senderAddressing", "sender_addressing")
    return sample


def resolve_existing_path(path_like: Optional[str], base_dir: Path) -> Optional[Path]:
    if not path_like:
        return None
    raw = Path(path_like).expanduser()
    candidates = []
    if raw.is_absolute():
        candidates.append(raw)
    else:
        parent_dir = base_dir.parent
        candidates.extend([
            Path.cwd() / raw,
            base_dir / raw,
            base_dir / raw.name,
            parent_dir / raw,
            parent_dir / raw.name,
        ])
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    return None


def resolve_model_name(
    checkpoint_path: Path,
    checkpoint: Dict[str, Any],
    explicit_model_name: Optional[str],
    semantic_cache_hint: Optional[str],
) -> tuple[str, Optional[Path]]:
    if explicit_model_name:
        return explicit_model_name, resolve_existing_path(semantic_cache_hint, checkpoint_path.parent)

    runtime_cfg = checkpoint_runtime_config(checkpoint)
    cache_hints = [
        semantic_cache_hint,
        runtime_cfg.get("semantic_cache"),
        runtime_cfg.get("synthetic_cache"),
        runtime_cfg.get("real_cache"),
    ]
    for hint in cache_hints:
        cache_path = resolve_existing_path(hint, checkpoint_path.parent)
        if cache_path is None:
            continue
        cache = torch.load(cache_path, map_location="cpu", weights_only=False)
        model_name = cache.get("model_name")
        if model_name:
            return str(model_name), cache_path

    raise ValueError(
        "Unable to determine the frozen Qwen model name. Pass --model-name or point --semantic-cache at a semantic cache .pt file."
    )


class ThreadChooserRuntime:
    def __init__(
        self,
        *,
        checkpoint_path: Path,
        model_name: Optional[str],
        semantic_cache: Optional[str],
        device: str,
        dtype: Optional[str],
        max_history_turns: Optional[int],
        include_candidate_score: Optional[bool],
        include_candidate_display_name_in_qwen: Optional[bool],
        include_nearby_notifications_in_qwen: Optional[bool],
        zero_semantic_scalars: bool,
        zero_semantic_vectors: bool,
        candidate_encoder_heads: int,
    ) -> None:
        self.checkpoint_path = checkpoint_path.resolve()
        self.checkpoint = torch.load(self.checkpoint_path, map_location="cpu", weights_only=False)
        self.runtime_cfg = checkpoint_runtime_config(self.checkpoint)
        self.device = device
        self.dtype = dtype or ("float16" if device.startswith("cuda") else "float32")
        self.zero_semantic_scalars = bool(zero_semantic_scalars)
        self.zero_semantic_vectors = bool(zero_semantic_vectors)
        self.max_history_turns = (
            int(max_history_turns)
            if max_history_turns is not None
            else int(self.runtime_cfg.get("max_history_turns", 8))
        )
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
        self.lock = threading.Lock()

        state_dict = self.checkpoint["state_dict"]
        struct_dim = int(state_dict["struct_proj.1.weight"].shape[1])
        semantic_scalar_dim = int(state_dict["semantic_scalar_proj.1.weight"].shape[1])
        semantic_vec_dim = int(state_dict["semantic_vec_proj.1.weight"].shape[1])

        self.semantic_cache_path: Optional[Path] = None
        self.model_name: Optional[str] = None
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
            if self.featurizer.hidden_size != semantic_vec_dim:
                raise ValueError(
                    f"Frozen model hidden size {self.featurizer.hidden_size} does not match checkpoint semantic_vec_dim {semantic_vec_dim}"
                )

        cfg = infer_head_config(
            self.checkpoint,
            struct_dim=struct_dim,
            semantic_scalar_dim=semantic_scalar_dim,
            semantic_vec_dim=semantic_vec_dim,
            default_candidate_encoder_heads=candidate_encoder_heads,
        )
        self.model = FusedThreadChooser(cfg)
        self.model.load_state_dict(state_dict)
        self.model.to(self.device)
        self.model.eval()

    def health_payload(self) -> Dict[str, Any]:
        return {
            "status": "ok",
            "checkpoint": str(self.checkpoint_path),
            "model_name": self.model_name,
            "semantic_cache": str(self.semantic_cache_path) if self.semantic_cache_path is not None else None,
            "device": self.device,
            "dtype": self.dtype,
            "max_history_turns": self.max_history_turns,
            "include_candidate_score": self.include_candidate_score,
            "include_candidate_display_name_in_qwen": self.include_candidate_display_name_in_qwen,
            "include_nearby_notifications_in_qwen": self.include_nearby_notifications_in_qwen,
            "zero_semantic_scalars": self.zero_semantic_scalars,
            "zero_semantic_vectors": self.zero_semantic_vectors,
            "head": {
                "struct_dim": self.model.cfg.struct_dim,
                "semantic_scalar_dim": self.model.cfg.semantic_scalar_dim,
                "semantic_vec_dim": self.model.cfg.semantic_vec_dim,
                "hidden_dim": self.model.cfg.hidden_dim,
                "candidate_encoder_layers": self.model.cfg.candidate_encoder_layers,
                "candidate_encoder_heads": self.model.cfg.candidate_encoder_heads,
                "semantic_residual_logit": self.model.cfg.semantic_residual_logit,
            },
        }

    def score_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        sample = extract_single_sample(payload)
        example = build_candidate_example(
            sample,
            include_candidate_score=self.include_candidate_score,
            max_history_turns=self.max_history_turns,
            include_candidate_display_name_in_qwen=self.include_candidate_display_name_in_qwen,
            include_nearby_notifications_in_qwen=self.include_nearby_notifications_in_qwen,
        )

        return self.score_example(example)

    def score_example(self, example: Any) -> Dict[str, Any]:
        struct = np.asarray(example.candidate_struct, dtype=np.float32)
        if struct.ndim != 2 or struct.shape[0] == 0:
            raise ValueError("sample must produce at least one candidate row")
        if struct.shape[1] > self.model.cfg.struct_dim:
            struct = struct[:, : self.model.cfg.struct_dim]
        if struct.shape[1] != self.model.cfg.struct_dim:
            raise ValueError(
                f"structural feature dimension mismatch: sample produced {struct.shape[1]}, checkpoint expects {self.model.cfg.struct_dim}"
            )

        if self.featurizer is not None:
            semantic_scalars, semantic_vecs = self.featurizer.featurize_example(example)
        else:
            semantic_scalars = np.zeros((len(example.candidate_ids), self.model.cfg.semantic_scalar_dim), dtype=np.float32)
            semantic_vecs = np.zeros((len(example.candidate_ids), self.model.cfg.semantic_vec_dim), dtype=np.float32)
        if self.zero_semantic_scalars:
            semantic_scalars.fill(0.0)
        if self.zero_semantic_vectors:
            semantic_vecs.fill(0.0)
        if semantic_scalars.shape[1] != self.model.cfg.semantic_scalar_dim:
            raise ValueError(
                f"semantic scalar dimension mismatch: sample produced {semantic_scalars.shape[1]}, checkpoint expects {self.model.cfg.semantic_scalar_dim}"
            )
        if semantic_vecs.shape[1] != self.model.cfg.semantic_vec_dim:
            raise ValueError(
                f"semantic vector dimension mismatch: sample produced {semantic_vecs.shape[1]}, checkpoint expects {self.model.cfg.semantic_vec_dim}"
            )

        with self.lock, torch.inference_mode():
            struct_t = torch.from_numpy(struct).unsqueeze(0).to(self.device)
            semantic_scalars_t = torch.from_numpy(semantic_scalars).unsqueeze(0).to(self.device)
            semantic_vecs_t = torch.from_numpy(semantic_vecs).unsqueeze(0).to(self.device)
            candidate_mask = torch.ones((1, struct.shape[0]), dtype=torch.bool, device=self.device)
            logits = self.model(
                struct_t,
                semantic_scalars_t,
                semantic_vecs_t,
                candidate_mask,
            )[0]
            probs = torch.softmax(logits, dim=-1)

        logits_list = [float(v) for v in logits.detach().cpu().tolist()]
        probs_list = [float(v) for v in probs.detach().cpu().tolist()]
        predicted_index = int(torch.argmax(probs).item())
        candidates = []
        for idx, thread_id in enumerate(example.candidate_ids):
            semantic_detail = {
                name: float(semantic_scalars[idx, pos])
                for pos, name in enumerate(SEMANTIC_SCALAR_NAMES)
            }
            candidates.append({
                "index": idx,
                "thread_id": thread_id,
                "is_group": bool(example.candidate_is_group[idx]),
                "logit": logits_list[idx],
                "probability": probs_list[idx],
                "identity_compatible": bool(example.metadata.get("candidate_identity_compatible", [False] * len(example.candidate_ids))[idx]),
                "exact_structural_match": bool(example.metadata.get("candidate_exact_structural_match", [False] * len(example.candidate_ids))[idx]),
                "semantic": semantic_detail,
            })

        return {
            "sample_id": example.sample_id,
            "candidate_count": len(example.candidate_ids),
            "predicted_index": predicted_index,
            "predicted_thread_id": example.candidate_ids[predicted_index],
            "scores": logits_list,
            "probabilities": probs_list,
            "target_explicit_counterparty_keys": example.metadata.get("target_explicit_counterparty_keys", []),
            "candidates": candidates,
        }


class ThreadScoreHandler(BaseHTTPRequestHandler):
    runtime: ThreadChooserRuntime

    def log_message(self, format: str, *args: Any) -> None:
        sys.stderr.write("%s - - [%s] %s\n" % (self.address_string(), self.log_date_time_string(), format % args))

    def _read_json(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        payload = json.loads(raw.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("request body must be a JSON object")
        return payload

    def _write_json(self, status: HTTPStatus, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path not in {"/health", "/healthz"}:
            self._write_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        self._write_json(HTTPStatus.OK, self.runtime.health_payload())

    def do_POST(self) -> None:
        if self.path != "/score":
            self._write_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        try:
            result = self.runtime.score_payload(self._read_json())
        except ValueError as exc:
            self._write_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return
        except Exception as exc:  # pragma: no cover
            self._write_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})
            return
        self._write_json(HTTPStatus.OK, result)


def add_runtime_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--model-name")
    parser.add_argument("--semantic-cache")
    parser.add_argument("--device", default="cuda" if torch.cuda.is_available() else "cpu")
    parser.add_argument("--dtype", choices=["float16", "bfloat16", "float32"])
    parser.add_argument("--max-history-turns", type=int)
    parser.add_argument("--candidate-encoder-heads", type=int, default=4)
    parser.add_argument("--include-candidate-score", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--include-candidate-display-name-in-qwen", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--include-nearby-notifications-in-qwen", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--zero-semantic-scalars", action="store_true")
    parser.add_argument("--zero-semantic-vectors", action="store_true")


def build_runtime(args: argparse.Namespace) -> ThreadChooserRuntime:
    return ThreadChooserRuntime(
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


def cmd_score(args: argparse.Namespace) -> None:
    runtime = build_runtime(args)
    result = runtime.score_payload(load_json_payload(args.input))
    print(json.dumps(result, indent=2))


def cmd_serve(args: argparse.Namespace) -> None:
    runtime = build_runtime(args)
    handler_cls = type("BoundThreadScoreHandler", (ThreadScoreHandler,), {"runtime": runtime})
    server = ThreadingHTTPServer((args.host, args.port), handler_cls)
    print(
        json.dumps(
            {
                "status": "listening",
                "host": args.host,
                "port": args.port,
                "checkpoint": str(runtime.checkpoint_path),
                "model_name": runtime.model_name,
                "device": runtime.device,
            }
        ),
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def build_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Persistent runtime sidecar for the fused thread chooser")
    sub = parser.add_subparsers(dest="cmd", required=True)

    score = sub.add_parser("score")
    add_runtime_args(score)
    score.add_argument("--input", required=True, help="Path to a sample JSON object, or - for stdin")
    score.set_defaults(func=cmd_score)

    serve = sub.add_parser("serve")
    add_runtime_args(serve)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.set_defaults(func=cmd_serve)

    return parser


def main() -> None:
    args = build_argparser().parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
