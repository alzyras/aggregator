from __future__ import annotations

from datetime import datetime, timezone

from providers.asana.normalizer import normalize_asana
from providers.habitica.normalizer import normalize_habitica
from providers.todoist.normalizer import normalize_todoist


def _iso(dt: datetime) -> str:
    return dt.replace(microsecond=0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def test_asana_completion_wins_at_timestamp():
    ts = _iso(datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc))
    events = normalize_asana(
        {
            "gid": "t1",
            "resource_type": "task",
            "name": "One",
            "notes": "n",
            "created_at": ts,
            "modified_at": ts,
            "completed": True,
            "completed_at": ts,
        }
    )
    types = {ev["event_type"] for ev in events}
    assert types == {"task_completed"}


def test_habitica_completion_drops_state_same_timestamp():
    ts = _iso(datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc))
    events = normalize_habitica(
        {
            "id": "h1",
            "type": "todo",
            "text": "Do it",
            "notes": "",
            "completed": True,
            "dateCompleted": ts,
            "value": 1,
        }
    )
    types = {ev["event_type"] for ev in events}
    assert types == {"task_completed"}


def test_todoist_completion_wins_over_update_same_timestamp():
    ts = _iso(datetime(2026, 3, 1, 9, 0, 0, tzinfo=timezone.utc))
    events = normalize_todoist(
        {
            "id": "td1",
            "content": "Todo",
            "description": "d",
            "added_at": ts,
            "completed_at": ts,
            "is_deleted": False,
            "is_archived": False,
            "checked": True,
            "due": None,
        }
    )
    types = {ev["event_type"] for ev in events}
    assert types == {"task_completed"}
