import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anthropic

from thread_model_banks import (
    BankGenerationError,
    DEFAULT_ENV_PATH,
    DEFAULT_MODEL,
    THREAD_WINDOWS_TOOL,
    _create_client,
    _extract_tool_input,
    load_anthropic_config,
    load_style_packs,
    normalize_thread_windows,
)


def build_challenge_window_prompt(*, batch_index: int, style_packs: dict[str, list[Any]]) -> str:
    generic_examples = ", ".join(style_packs.get("generic_replies", [])[:8]) or "ok, sure, sounds good"
    planning_examples = ", ".join(style_packs.get("planning_lines", [])[:8]) or "on my way, send me the address"
    reaction_examples = ", ".join(style_packs.get("reaction_templates", [])[:5]) or "\"{preview}\" haha"
    family_names = ", ".join(style_packs.get("family_names", [])[:8])
    friend_names = ", ".join(style_packs.get("friend_names", [])[:8])
    service_senders = ", ".join(style_packs.get("service_senders", [])[:6])
    group_examples = ", ".join(style_packs.get("group_title_variants", [])[:6])
    domain_buckets = [
        "friends + event planning",
        "roommates + housing logistics",
        "coworkers/class project + side direct chats",
        "family logistics + travel pickup",
        "service notification + human planning overlap",
        "hobby/sports + direct followups",
    ]
    focus = domain_buckets[batch_index % len(domain_buckets)]
    service_clause = (
        "- This window must include at least one service/business or shortcode thread, and at least one ambiguous target where a human thread and the service thread plausibly collide on the same external object or event.\n"
        if "service" in focus
        else ""
    )
    return f"""
Emit exactly 1 window as structured tool input.

Rules:
- 5 to 8 participants total.
- 4 to 6 concurrent threads.
- One phone owner world. Set `self_participant_id` correctly.
- At least one direct thread and one group thread.
- Bias this window toward: `{focus}`.
- Use names inspired by family=[{family_names}] and friends=[{friend_names}].
- Service/business senders can be inspired by [{service_senders}].
- Group names/descriptors should feel like {group_examples}.
{service_clause}- Messages should feel mundane and realistic, using language like:
  - generic replies: {generic_examples}
  - planning/check-in lines: {planning_examples}
  - reaction-style text templates: {reaction_examples}

This window is for a semantic challenge holdout, so structure must tie often enough that discourse fit matters.

Requirements for this single window:
- Include exactly 3 explicit `ambiguous_targets`.
- Target 1 must be a reaction-style or quoted-message ambiguity.
  - The quoted/reaction content must actually match prior gold-thread content.
  - The same sender must also be active in a newer plausible distractor thread.
- Target 2 must be a stale-gold vs recent-distractor case.
  - Gold thread older/staler by 30 to 1440 minutes.
  - Distractor(s) more recent.
  - Participant geometry should be tied or near-tied.
- Target 3 must be a short/generic/anaphoric case.
  - Examples: ok, same, i'll be there, wait which day, did you see this, send that again.
  - Only discourse fit should clearly resolve it.

Across the 3 targets:
- At least one DM-vs-group ambiguity.
- At least one same-sender-across-multiple-threads ambiguity.
- At least one case with pronoun/anaphora/ellipsis.
- Every ambiguous target should have 2 to 4 plausible thread ids.
- At least one wrong plausible thread should be more recent than gold.
- Do not make the ambiguity trivial through unique thread names.
- Do not use placeholders like <name>.
- Do not use explicit sexual content, slurs, or AI/meta references.
- Keep it realistic under Apple MAP + ANCS style observability: sparse, mundane, overlapping, and annoying.
""".strip()


def request_one_window(
    *,
    client: anthropic.Anthropic,
    model: str,
    batch_index: int,
    style_packs: dict[str, list[Any]],
    max_tokens: int,
) -> list[dict[str, Any]]:
    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        temperature=0.9,
        system="You generate realistic overlapping messaging-thread windows as structured tool input for a semantic challenge holdout.",
        tools=[THREAD_WINDOWS_TOOL],
        tool_choice={"type": "tool", "name": THREAD_WINDOWS_TOOL["name"]},
        messages=[
            {
                "role": "user",
                "content": build_challenge_window_prompt(batch_index=batch_index, style_packs=style_packs),
            }
        ],
    )
    payload = _extract_tool_input(response, THREAD_WINDOWS_TOOL["name"])
    return normalize_thread_windows(payload)


def generate_challenge_windows(
    *,
    out_path: Path,
    style_packs_path: Path,
    env_file: Path | None,
    model: str | None,
    batch_count: int,
    max_tokens: int,
) -> dict[str, Any]:
    config = load_anthropic_config(env_file=env_file, model=model)
    client = _create_client(config)
    style_packs = load_style_packs(style_packs_path)
    windows: list[dict[str, Any]] = []
    for batch_index in range(batch_count):
        print(
            f"[thread-model] generating challenge Opus window {batch_index + 1}/{batch_count} "
            f"(model={config.model}, timeout={config.timeout_seconds}s)",
            flush=True,
        )
        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                windows.extend(
                    request_one_window(
                        client=client,
                        model=config.model,
                        batch_index=batch_index + 1,
                        style_packs=style_packs,
                        max_tokens=max_tokens,
                    )
                )
                last_error = None
                break
            except Exception as exc:  # noqa: BLE001
                last_error = exc
        if last_error is not None:
            raise BankGenerationError(f"Failed challenge window batch {batch_index + 1}: {last_error}") from last_error

    payload = {
        "meta": {
            "kind": "challenge_thread_windows",
            "model": config.model,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "batch_count": batch_count,
            "window_count": len(windows),
            "env_file": str(config.env_file) if config.env_file else None,
        },
        "windows": windows,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate semantic-challenge thread windows with Claude Opus.")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--style-packs", type=Path, default=Path("artifacts") / "thread-model" / "style_packs_opus_v2.json")
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_PATH)
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL)
    parser.add_argument("--batch-count", type=int, default=8)
    parser.add_argument("--max-tokens", type=int, default=7000)
    args = parser.parse_args()

    payload = generate_challenge_windows(
        out_path=args.out,
        style_packs_path=args.style_packs,
        env_file=args.env_file,
        model=args.model,
        batch_count=args.batch_count,
        max_tokens=args.max_tokens,
    )
    print(
        json.dumps(
            {
                "out": str(args.out),
                "window_count": len(payload["windows"]),
                "batch_count": args.batch_count,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
