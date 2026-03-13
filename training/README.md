# Thread Chooser Training Assets

This directory contains the curated training and runtime assets for the learned thread chooser.

## Layout

- `thread_fused_stack.py`: train/eval entrypoint for the fused chooser
- `thread_scoring_sidecar.py`: long-lived daemon scoring sidecar
- `datasets/`: curated synthetic dataset and supporting inputs
- `models/`: shipped checkpoints used by the daemon
- `reports/`: alignment summaries

The stack targets the repaired `synth-v2` / `real_eval` candidate-set problem:

- input: one ambiguous target message + 3–8 candidate threads
- output: best thread

The repo keeps only the curated synthetic dataset, the shipped runtime checkpoint, and the structural baseline under `training/`.
Generated caches, evaluation slices, and experiment outputs belong under `../artifacts/thread-model/`.

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

1. Precompute semantic features for synth and real with the frozen base model.
2. Train the small chooser on cached semantic features + structural features.
3. Evaluate on the real evaluation slice.
4. Run ablations afterward.

## Example commands

```powershell
python .\thread_fused_stack.py featurize `
  --input .\datasets\thread-chooser-synth-v2-aligned.json `
  --output ..\artifacts\thread-model\synth_semantic.pt `
  --model-name Qwen/Qwen3-0.6B-Base

python .\thread_fused_stack.py featurize `
  --input ..\artifacts\thread-model\real_eval.json `
  --output ..\artifacts\thread-model\real_semantic.pt `
  --model-name Qwen/Qwen3-0.6B-Base

python .\thread_fused_stack.py train `
  --synthetic .\datasets\thread-chooser-synth-v2-aligned.json `
  --real ..\artifacts\thread-model\real_eval.json `
  --synthetic-cache ..\artifacts\thread-model\synth_semantic.pt `
  --real-cache ..\artifacts\thread-model\real_semantic.pt `
  --output ..\artifacts\thread-model\fused_model_experiment.pt

python .\thread_fused_stack.py eval `
  --input ..\artifacts\thread-model\real_eval.json `
  --cache ..\artifacts\thread-model\real_semantic.pt `
  --checkpoint ..\artifacts\thread-model\fused_model_experiment.pt

python .\thread_scoring_sidecar.py serve `
  --checkpoint .\models\thread-chooser-fused-headline.pt `
  --model-name Qwen/Qwen3-0.6B-Base `
  --port 5048
```

## Easy ablations

- structural-only: zero out semantic scalars/vectors
- structural + conditional likelihood only: keep semantic scalars, zero semantic vectors
- structural + hidden states only: keep vectors, zero likelihood scalars
- semantic-only: zero structural features
- + `candidate_score` upper-bound run: add `--include-candidate-score`
- harder real holdout: use `real_eval_hard.json` from the dataset factory and a matching semantic cache
