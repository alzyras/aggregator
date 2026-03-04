from __future__ import annotations

from datetime import datetime, timezone

from django.test import TestCase

from ingestion.normalizers.utils import CANONICAL_EVENT_TYPES
from providers.asana.normalizer import normalize_asana
from providers.google_fit.normalizer import normalize_google_fit
from providers.habitica.normalizer import normalize_habitica
from providers.todoist.normalizer import normalize_todoist


class CanonicalEventTypeTests(TestCase):
    def test_asana_event_types_are_canonical(self):
        task = {
            "gid": "t1",
            "resource_type": "task",
            "name": "Task",
            "notes": "Notes",
            "created_at": "2025-01-01T12:00:00.000Z",
            "modified_at": "2025-01-02T12:00:00.000Z",
            "completed": True,
            "completed_at": "2025-01-03T12:00:00.000Z",
        }
        events = normalize_asana(task)
        for event in events:
            self.assertIn(event["event_type"], CANONICAL_EVENT_TYPES)
            self.assertEqual(event["source_entity_type"], "task")

    def test_habitica_event_types_are_canonical(self):
        habit = {
            "id": "h1",
            "type": "habit",
            "text": "Drink water",
            "notes": "",
            "history": [{"date": 1700000000000, "value": 1}],
            "updatedAt": "2025-01-01T12:00:00.000Z",
        }
        events = normalize_habitica(habit)
        for event in events:
            self.assertIn(event["event_type"], CANONICAL_EVENT_TYPES)
            self.assertIn(event["source_entity_type"], {"habit", "daily", "todo"})

    def test_todoist_event_types_are_canonical(self):
        task = {
            "id": "1",
            "content": "Todo",
            "completed": False,
            "created_at": "2025-01-01T12:00:00Z",
        }
        event = normalize_todoist(task)
        self.assertIn(event["event_type"], CANONICAL_EVENT_TYPES)

    def test_google_fit_event_types_are_canonical(self):
        record = {
            "record_type": "steps",
            "data_type": "steps",
            "unit": "count",
            "value": 1000,
            "timestamp": datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
            "user_id": "u1",
            "id": "r1",
        }
        event = normalize_google_fit(record)
        self.assertIn(event["event_type"], CANONICAL_EVENT_TYPES)
