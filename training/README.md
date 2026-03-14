# Thread Chooser Training Assets

This directory contains the checked-in runtime assets and local training entrypoints for the learned thread chooser.

## Layout

- `thread_fused_stack.py`: train/eval entrypoint for the fused chooser
- `thread_scoring_sidecar.py`: long-lived daemon scoring sidecar
- `models/`: shipped checkpoints used by the daemon

The stack targets the repaired `synth-v2` / `real_eval` candidate-set problem:

- input: one ambiguous target message + 3–8 candidate threads
- output: best thread

The repo keeps the training scripts plus the shipped runtime checkpoint under `training/`.
Generated datasets, semantic caches, evaluation slices, and reports are local artifacts and belong under `../artifacts/thread-model/`.

Current checked-in runtime checkpoint:

- `models/thread-chooser-learned-optin-qwen3-1.7b.pt`

Headline design choices baked into the code:

- frozen causal LM semantic branch
- listwise chooser over the whole candidate set
- structural + semantic fusion
- **no `candidate_score` in the default headline model**
- explicit guardrails against trivial leakage:
  - target `conversation_display_name` is excluded
  - candidate `thread_id` / `message_key` / synth-only metadata are excluded
  - notification titles/subtitles are excluded from the semantic branch
- asymmetric penalty for **over-grouping** on direct targets

## Recommended flow

1. Generate or export your local JSON inputs under `../artifacts/thread-model/`.
2. Precompute semantic features for synth and real with the frozen base model.
3. Train the chooser on cached semantic features plus structural features.
4. Evaluate on the real evaluation slice and run ablations afterward.

The repo does not ship runnable training datasets, semantic caches, or evaluation slices, so the training and eval commands are intentionally omitted from this public-facing note.

## Runtime example

```powershell
python .\thread_scoring_sidecar.py serve `
  --checkpoint .\models\thread-chooser-learned-optin-qwen3-1.7b.pt `
  --model-name Qwen/Qwen3-1.7B-Base `
  --port 5048
```

## Easy ablations

- structural-only: zero out semantic scalars/vectors
- structural + conditional likelihood only: keep semantic scalars, zero semantic vectors
- structural + hidden states only: keep vectors, zero likelihood scalars
- semantic-only: zero structural features
- + `candidate_score` upper-bound run: add `--include-candidate-score`
- harder real holdout: use `real_eval_hard.json` from the dataset factory and a matching semantic cache
