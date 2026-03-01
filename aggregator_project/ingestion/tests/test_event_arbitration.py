from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ingestion.normalizers.utils import arbitrate_events


@pytest.mark.parametrize(
    "events,expected_types",
    [
        (
            [
                {"event_type": "task_state", "source_entity_id": "1", "start_time": datetime(2026, 3, 1, tzinfo=timezone.utc)},
                {"event_type": "task_completed", "source_entity_id": "1", "start_time": datetime(2026, 3, 1, tzinfo=timezone.utc)},
            ],
            ["task_completed"],
        ),
        (
            [
                {"event_type": "task_state", "source_entity_id": "1", "start_time": datetime(2026, 3, 1, tzinfo=timezone.utc)},
                {"event_type": "task_updated", "source_entity_id": "1", "start_time": datetime(2026, 3, 1, tzinfo=timezone.utc)},
            ],
            ["task_updated"],
        ),
        (
            [
                {"event_type": "task_state", "source_entity_id": "1", "start_time": datetime(2026, 3, 1, tzinfo=timezone.utc)},
                {"event_type": "task_created", "source_entity_id": "1", "start_time": datetime(2026, 3, 1, tzinfo=timezone.utc)},
            ],
            ["task_created"],
        ),
    ],
)
def test_arbitrate_priority(events, expected_types):
    result = arbitrate_events(events)
    assert [ev["event_type"] for ev in result] == expected_types
