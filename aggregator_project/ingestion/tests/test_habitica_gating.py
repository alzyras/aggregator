from __future__ import annotations

import pytest

from providers.habitica.normalizer import normalize_habitica


@pytest.fixture
def habit_task():
    return {
        "id": "habit1",
        "type": "habit",
        "text": "Drink water",
        "history": [
            {"date": "2024-01-01T10:00:00Z", "value": 1},
            {"date": "2024-01-02T10:00:00Z", "value": 2},
        ],
        "dateCreated": "2023-12-31T00:00:00Z",
        "updatedAt": "2024-01-02T12:00:00Z",
    }


@pytest.fixture
def daily_task():
    return {
        "id": "daily1",
        "type": "daily",
        "text": "Walk",
        "completed": True,
        "dateCompleted": "2024-01-03T09:00:00Z",
        "dateCreated": "2023-12-30T00:00:00Z",
        "updatedAt": "2024-01-03T10:00:00Z",
    }


@pytest.fixture
def todo_task():
    return {
        "id": "todo1",
        "type": "todo",
        "text": "Ship feature",
        "completed": True,
        "dateCompleted": "2024-01-04T11:00:00Z",
        "dateCreated": "2023-12-29T00:00:00Z",
        "updatedAt": "2024-01-04T12:00:00Z",
    }


def run(task, **overrides):
    settings = {
        "sync_habits": True,
        "sync_dailies": True,
        "sync_todos": True,
        "emit_history_occurrences": True,
        "emit_completion_occurrences": True,
        "task_state_created": True,
        "task_state_updated": True,
        "task_state_completed": True,
    }
    settings.update(overrides)
    task = dict(task)
    task["_habitica_settings"] = settings
    return normalize_habitica(task)


def test_habit_history_disabled(habit_task):
    events = run(habit_task, emit_history_occurrences=False)
    assert all(e["event_type"] != "metric_recorded" for e in events)


def test_daily_completion_disabled(daily_task):
    events = run(daily_task, emit_completion_occurrences=False)
    assert all(e["event_type"] != "task_completed" for e in events)


def test_task_state_gating(todo_task):
    events = run(todo_task, task_state_created=False, task_state_updated=False, task_state_completed=True)
    assert any(e["event_type"] == "task_state" and e["external_status"] == "completed" for e in events)
    assert all(
        not (e["event_type"] == "task_state" and e["external_status"] == "open")
        for e in events
    )


def test_timestamp_selection(todo_task):
    events = run(todo_task)
    completed_events = [e for e in events if e["event_type"] == "task_completed"]
    assert completed_events
    assert completed_events[0]["source_event_version"].startswith("2024-01-04")


def test_empty_history_habit(habit_task):
    habit_task_empty = dict(habit_task)
    habit_task_empty["history"] = []
    events = run(habit_task_empty)
    assert all(e["event_type"] != "metric_recorded" for e in events)
    assert any(e["event_type"] == "task_state" for e in events)
