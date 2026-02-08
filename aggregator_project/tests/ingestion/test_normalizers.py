from __future__ import annotations

from datetime import datetime, timezone

from django.test import TestCase

from ingestion.normalizers.base import build_dedupe_hash
from ingestion.normalizers.utils import parse_timestamp


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

    def test_parse_timestamp_invalid(self):
        self.assertIsNone(parse_timestamp("not-a-date"))

    def test_build_dedupe_hash_stability(self):
        payload = {
            "source": "asana",
            "source_entity_type": "task",
            "source_entity_id": "123",
            "start_time": "2025-01-01",
            "end_time": None,
            "metric_type": None,
            "metric_value": None,
            "status": "open",
        }
        hash_a = build_dedupe_hash(payload)
        hash_b = build_dedupe_hash(payload)

        payload_changed = {**payload, "status": "completed"}
        hash_c = build_dedupe_hash(payload_changed)

        self.assertEqual(hash_a, hash_b)
        self.assertNotEqual(hash_a, hash_c)
