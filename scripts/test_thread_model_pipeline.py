import unittest
from copy import deepcopy
from datetime import datetime, timedelta, timezone

from build_thread_model_dataset import MessageRow, matches_real_slice
from replay_thread_runtime import assign_message, build_runtime_sample
from thread_model_banks import merge_style_packs, normalize_thread_windows
from thread_model_synth import build_synth_samples, validate_dataset


SAMPLE_WINDOWS = {
    "windows": [
        {
            "window_id": "w1",
            "theme": "family logistics",
            "self_participant_id": "p0",
            "participants": [
                {"id": "p0", "display_name": "Riley", "aliases": ["me"], "relationship": "self", "style_tags": ["brief"]},
                {"id": "p1", "display_name": "mom", "aliases": ["mom"], "relationship": "family", "style_tags": ["brief"]},
                {"id": "p2", "display_name": "dad", "aliases": ["dad"], "relationship": "family", "style_tags": ["brief"]},
                {"id": "p3", "display_name": "grandma", "aliases": ["grandma"], "relationship": "family", "style_tags": ["warm"]},
                {"id": "p4", "display_name": "rowan", "aliases": ["row"], "relationship": "friend", "style_tags": ["casual"]},
            ],
            "threads": [
                {
                    "thread_id": "t1",
                    "display_name": "mom",
                    "is_group": False,
                    "participant_ids": ["p0", "p1"],
                    "messages": [
                        {"speaker_id": "p1", "text": "we're 10 out", "minutes_ago": 50, "kind": "plain"},
                        {"speaker_id": "p0", "text": "sounds good", "minutes_ago": 24, "kind": "plain"},
                        {"speaker_id": "p1", "text": "parking now", "minutes_ago": 20, "kind": "plain"},
                        {"speaker_id": "p1", "text": "teehee", "minutes_ago": 5, "kind": "plain"},
                    ],
                },
                {
                    "thread_id": "t2",
                    "display_name": "mom, dad, grandma",
                    "is_group": True,
                    "participant_ids": ["p0", "p1", "p2", "p3"],
                    "messages": [
                        {"speaker_id": "p3", "text": "i'll see you guys soon", "minutes_ago": 55, "kind": "plain"},
                        {"speaker_id": "p0", "text": "i can bring food", "minutes_ago": 30, "kind": "plain"},
                        {"speaker_id": "p1", "text": "we're 10 out", "minutes_ago": 18, "kind": "plain"},
                        {"speaker_id": "p3", "text": "teehee", "minutes_ago": 3, "kind": "plain"},
                    ],
                },
                {
                    "thread_id": "t3",
                    "display_name": "rowan",
                    "is_group": False,
                    "participant_ids": ["p0", "p4"],
                    "messages": [
                        {"speaker_id": "p4", "text": "tell me when you're free", "minutes_ago": 61, "kind": "plain"},
                        {"speaker_id": "p0", "text": "i'm down", "minutes_ago": 14, "kind": "plain"},
                        {"speaker_id": "p4", "text": "ok", "minutes_ago": 8, "kind": "plain"},
                        {"speaker_id": "p4", "text": "sounds good", "minutes_ago": 2, "kind": "plain"},
                    ],
                },
            ],
            "ambiguous_targets": [
                {
                    "target_id": "a1",
                    "gold_thread_id": "t2",
                    "speaker_id": "p3",
                    "text": "teehee",
                    "minutes_ago": 1,
                    "kind": "plain",
                    "ambiguity_reasons": ["generic_reply", "overlapping_participants"],
                    "plausible_thread_ids": ["t1", "t2"],
                },
                {
                    "target_id": "a2",
                    "gold_thread_id": "t1",
                    "speaker_id": "p0",
                    "text": "ok",
                    "minutes_ago": 12,
                    "kind": "plain",
                    "ambiguity_reasons": ["generic_reply", "overlapping_participants"],
                    "plausible_thread_ids": ["t1", "t3"],
                }
            ],
        }
    ]
}


class ThreadModelPipelineTests(unittest.TestCase):
    def test_runtime_replay_builder_preserves_contamination_state(self) -> None:
        base = datetime(2026, 3, 13, 0, 0, tzinfo=timezone.utc)

        def make_message(
            *,
            message_key: str,
            conversation_id: str,
            display_name: str,
            is_group: bool,
            sort_utc: datetime,
            participants: list[dict[str, object]],
            folder: str,
            body: str,
            sender_name: str,
        ) -> MessageRow:
            return MessageRow(
                device_id="dev",
                message_key=message_key,
                conversation_id=conversation_id,
                sort_utc=sort_utc,
                conversation_display_name=display_name,
                is_group=is_group,
                preview=body,
                payload={
                    "sortTimestampUtc": sort_utc.isoformat(),
                    "participants": participants,
                    "message": {
                        "folder": folder,
                        "subject": None,
                        "body": body,
                        "senderName": sender_name,
                        "senderAddressing": None,
                        "originators": [],
                        "recipients": [],
                    },
                },
            )

        self_participant = {"key": "name:me", "displayName": "Me", "phones": [], "emails": [], "isSelf": True}
        mom = {"key": "phone:+12025550111", "displayName": "mom", "phones": ["+12025550111"], "emails": [], "isSelf": False}
        dad = {"key": "phone:+15555550123", "displayName": "dad", "phones": ["+15555550123"], "emails": [], "isSelf": False}

        direct_mom = make_message(
            message_key="m1",
            conversation_id="phone:+12025550111",
            display_name="mom",
            is_group=False,
            sort_utc=base,
            participants=[self_participant, mom],
            folder="inbox",
            body="parking now",
            sender_name="mom",
        )
        group_family = make_message(
            message_key="m2",
            conversation_id="group:family",
            display_name="mom, dad",
            is_group=True,
            sort_utc=base + timedelta(minutes=5),
            participants=[self_participant, mom, dad],
            folder="inbox",
            body="ok",
            sender_name="mom",
        )
        follow_up = make_message(
            message_key="m3",
            conversation_id="phone:+12025550111",
            display_name="mom",
            is_group=False,
            sort_utc=base + timedelta(minutes=10),
            participants=[self_participant, mom],
            folder="sent",
            body="ok",
            sender_name="Me",
        )

        buckets: dict[str, object] = {}

        sample1 = build_runtime_sample(direct_mom, buckets, [], max_candidates=6, max_history_turns=8)
        assign_message(direct_mom, sample1, direct_mom.conversation_id, buckets)

        sample2 = build_runtime_sample(group_family, buckets, [], max_candidates=6, max_history_turns=8)
        self.assertEqual(len(sample2["candidate_threads"]), 2)
        assign_message(group_family, sample2, direct_mom.conversation_id, buckets)

        sample3 = build_runtime_sample(follow_up, buckets, [], max_candidates=6, max_history_turns=8)
        contaminated = next(candidate for candidate in sample3["candidate_threads"] if candidate["thread_id"] == direct_mom.conversation_id)
        contaminated_keys = {participant["key"] for participant in contaminated["participants"]}
        self.assertFalse(contaminated["is_group"])
        self.assertIn("phone:+15555550123", contaminated_keys)

    def test_merge_style_packs_dedupes_strings_and_messages(self) -> None:
        merged = merge_style_packs(
            {
                "generic_replies": ["ok", "new one"],
                "ambiguous_messages": [
                    {
                        "text": "teehee",
                        "intent_tag": "generic_ack",
                        "plausible_in_group": True,
                        "plausible_in_direct": True,
                        "style_tags": ["generic"],
                    },
                    {
                        "text": "another one",
                        "intent_tag": "generic_ack",
                        "plausible_in_group": True,
                        "plausible_in_direct": True,
                        "style_tags": ["generic"],
                    },
                ],
            }
        )
        self.assertIn("new one", merged["generic_replies"])
        self.assertEqual(merged["generic_replies"].count("ok"), 1)
        self.assertEqual(
            len([item for item in merged["ambiguous_messages"] if item["text"] == "teehee"]),
            1,
        )
        self.assertTrue(any(item["text"] == "another one" for item in merged["ambiguous_messages"]))

    def test_normalize_thread_windows_keeps_valid_window(self) -> None:
        windows = normalize_thread_windows(SAMPLE_WINDOWS)
        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0]["window_id"], "w1")
        self.assertEqual(windows[0]["self_participant_id"], "p0")
        self.assertEqual(len(windows[0]["threads"]), 3)
        self.assertEqual(len(windows[0]["ambiguous_targets"]), 2)

    def test_normalize_thread_windows_allows_marked_sparse_failure_histories(self) -> None:
        sparse_windows = {
            "windows": [
                {
                    "window_id": "s1",
                    "theme": "sparse failure",
                    "self_participant_id": "p0",
                    "participants": [
                        {"id": "p0", "display_name": "Me", "aliases": ["me"], "relationship": "self", "style_tags": ["self"]},
                        {"id": "p1", "display_name": "Avery", "aliases": ["+12025559052"], "relationship": "friend", "style_tags": ["failure"]},
                        {"id": "p2", "display_name": "Jordan", "aliases": ["+13105559052"], "relationship": "friend", "style_tags": ["failure"]},
                    ],
                    "threads": [
                        {
                            "thread_id": "t1",
                            "display_name": "Avery",
                            "is_group": False,
                            "allow_sparse_history": True,
                            "participant_ids": ["p0", "p1"],
                            "messages": [{"speaker_id": "p1", "text": "hey", "minutes_ago": 5, "kind": "plain"}],
                        },
                        {
                            "thread_id": "t2",
                            "display_name": "Avery, Jordan",
                            "is_group": True,
                            "allow_sparse_history": True,
                            "participant_ids": ["p0", "p1", "p2"],
                            "messages": [],
                        },
                        {
                            "thread_id": "t3",
                            "display_name": "Jordan",
                            "is_group": False,
                            "allow_sparse_history": True,
                            "participant_ids": ["p0", "p2"],
                            "messages": [{"speaker_id": "p2", "text": "hello", "minutes_ago": 8, "kind": "plain"}],
                        },
                    ],
                    "ambiguous_targets": [
                        {
                            "target_id": "s1::target",
                            "gold_thread_id": "t2",
                            "speaker_id": "p2",
                            "text": "hello again",
                            "minutes_ago": 0,
                            "kind": "plain",
                            "ambiguity_reasons": ["held_out_counterparty_regression"],
                            "plausible_thread_ids": ["t1", "t2", "t3"],
                        }
                    ],
                }
            ]
        }
        windows = normalize_thread_windows(sparse_windows)
        self.assertEqual(len(windows), 1)
        self.assertEqual(len(windows[0]["threads"]), 3)
        self.assertEqual(windows[0]["ambiguous_targets"][0]["ambiguity_reasons"], ["held_out_counterparty_regression"])

    def test_build_synth_samples_and_validate(self) -> None:
        windows = normalize_thread_windows(SAMPLE_WINDOWS)
        samples = build_synth_samples(
            count=30,
            max_candidates=4,
            seed=7,
            thread_windows=windows,
        )
        self.assertEqual(len(samples), 30)
        self.assertTrue(all(sample["gold_thread_id"] for sample in samples))
        self.assertTrue(all(len(sample["candidate_threads"]) >= 3 for sample in samples))
        self.assertTrue(any(sample["message"]["message"]["folder"] == "sent" for sample in samples))
        self.assertTrue(any(history["folder"] == "sent" for sample in samples for candidate in sample["candidate_threads"] for history in candidate["history"]))
        self.assertTrue(
            all(
                history["sort_utc"] < sample["message"]["sort_utc"]
                for sample in samples
                for candidate in sample["candidate_threads"]
                for history in candidate["history"]
            )
        )
        validation = validate_dataset(samples)
        self.assertGreater(validation["quality_score"], 0)
        self.assertGreaterEqual(validation["multi_plausible_ratio"], 1.0)
        self.assertGreater(validation["sent_target_ratio"], 0.0)
        self.assertGreater(validation["sent_history_ratio"], 0.0)

    def test_build_synth_samples_focuses_requested_ambiguity_reason(self) -> None:
        windows = normalize_thread_windows(deepcopy(SAMPLE_WINDOWS))
        windows[0]["ambiguous_targets"][0]["ambiguity_reasons"].append("held_out_counterparty_regression")
        samples = build_synth_samples(
            count=12,
            max_candidates=4,
            seed=11,
            thread_windows=windows,
            focus_ambiguity_reasons=["held_out_counterparty_regression"],
            focus_ambiguity_boost=1.0,
        )
        self.assertEqual(len(samples), 12)
        self.assertTrue(
            all(
                "held_out_counterparty_regression" in sample["metadata"].get("ambiguity_reasons", [])
                for sample in samples
            )
        )

    def test_matches_real_slice_headline_proof_excludes_self_test(self) -> None:
        sample = {
            "gold_thread_id": "g1",
            "message": {
                "message_key": "m1",
                "is_group": False,
                "participants": [{"key": "name:mom", "displayName": "mom", "isSelf": False}],
                "message": {"folder": "inbox", "senderName": "", "senderAddressing": "", "originators": [], "recipients": []},
            },
            "candidate_threads": [
                {
                    "thread_id": "g1",
                    "display_name": "mom",
                    "participants": [{"key": "name:mom", "displayName": "mom", "isSelf": False}],
                    "features": {"participant_overlap": 1},
                },
                {
                    "thread_id": "g2",
                    "display_name": "dad",
                    "participants": [{"key": "name:dad", "displayName": "dad", "isSelf": False}],
                    "features": {"participant_overlap": 1},
                },
            ],
            "metadata": {
                "preview": "teehee",
                "generic_preview": True,
                "target_is_outbound": False,
                "future_corrob": {"future_corrob_strength": 1},
                "nearby_notification_count": 0,
            },
        }
        self.assertTrue(matches_real_slice(sample, "headline-proof"))
        sample["metadata"]["preview"] = "map takeover smoke"
        self.assertFalse(matches_real_slice(sample, "headline-proof"))


if __name__ == "__main__":
    unittest.main()
