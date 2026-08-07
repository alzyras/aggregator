from __future__ import annotations

from datetime import datetime, timezone

from django.test import TestCase

from ingestion.normalizers.base import build_dedupe_hash
from ingestion.normalizers.utils import parse_timestamp
from providers.todoist.normalizer import normalize_todoist


class NormalizerUtilsTests(TestCase):
    def test_parse_timestamp_seconds(self):
        value = 1700000000
        parsed = parse_timestamp(value)

        self.assertEqual(
            parsed, datetime.fromtimestamp(value, tz=timezone.utc)
        )

    def test_parse_timestamp_milliseconds(self):
        value = 1700000000000
        parsed = parse_timestamp(value)

        expected = datetime.fromtimestamp(value / 1000, tz=timezone.utc)
        self.assertEqual(parsed, expected)

    def test_parse_timestamp_string(self):
        parsed = parse_timestamp("2025-01-01T12:00:00Z")
        self.assertEqual(
            parsed, datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
        )

    def test_parse_timestamp_date_string_is_utc_aware(self):
        parsed = parse_timestamp("2025-01-01")

        self.assertEqual(
            parsed,
            datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
        )

    def test_parse_timestamp_invalid(self):
        self.assertIsNone(parse_timestamp("not-a-date"))

    def test_build_dedupe_hash_stability(self):
        payload = {
            "source": "asana",
            "source_entity_type": "task",
            "source_entity_id": "123",
            "event_type": "task_updated",
            "source_event_version": "v1",
        }
        hash_a = build_dedupe_hash(payload)
        hash_b = build_dedupe_hash(payload)

        payload_changed = {**payload, "event_type": "task_completed"}
        hash_c = build_dedupe_hash(payload_changed)

        self.assertEqual(hash_a, hash_b)
        self.assertNotEqual(hash_a, hash_c)

    def test_todoist_normalizer_honors_embedded_provider_settings(self):
        events = normalize_todoist(
            {
                "id": "todoist-1",
                "content": "Completed task",
                "completed": True,
                "completed_at": "2025-03-01T10:00:00Z",
                "__todoist_settings": {
                    "include_completed": False,
                },
            }
        )

        self.assertEqual(events, [])

    def test_todoist_checked_task_emits_completion(self):
        timestamp = "2026-03-01T09:00:00Z"

        events = normalize_todoist(
            {
                "id": "todoist-checked",
                "content": "Completed task",
                "added_at": timestamp,
                "completed_at": timestamp,
                "checked": True,
            }
        )

        self.assertEqual(
            {event["event_type"] for event in events},
            {"task_completed"},
        )

    def test_todoist_deleted_event_wins_over_same_timestamp_update(self):
        events = normalize_todoist(
            {
                "id": "todoist-deleted",
                "content": "Deleted task",
                "is_deleted": True,
                "updated_at": "2026-03-01T09:00:00Z",
            }
        )

        self.assertEqual(
            {event["event_type"] for event in events},
            {"task_deleted"},
        )
