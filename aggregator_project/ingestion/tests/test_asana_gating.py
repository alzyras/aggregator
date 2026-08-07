from __future__ import annotations

from providers.asana.normalizer import normalize_asana


def normalize(task: dict, **overrides) -> list[dict]:
    settings = {
        "sync_tasks": True,
        "sync_subtasks": True,
        "include_completed": True,
        "include_archived": False,
        "emit_task_created": True,
        "emit_task_updated": True,
        "emit_task_completed": True,
        "emit_task_reopened": True,
        "emit_task_deleted": True,
        "task_state_created": True,
        "task_state_updated": True,
        "task_state_completed": True,
    }
    settings.update(overrides)
    t = dict(task)
    t["__asana_settings"] = settings
    return normalize_asana(t)


def test_created_event_gated_off():
    task = {
        "gid": "t1",
        "resource_type": "task",
        "name": "Task",
        "created_at": "2025-01-01T10:00:00Z",
    }
    events = normalize(task, emit_task_created=False)
    assert all(e["event_type"] != "task_created" for e in events)


def test_completed_event_uses_completed_at():
    task = {
        "gid": "t2",
        "resource_type": "task",
        "name": "Task",
        "completed": True,
        "completed_at": "2025-02-02T15:00:00Z",
        "created_at": "2025-02-01T10:00:00Z",
        "modified_at": "2025-02-02T16:00:00Z",
    }
    events = normalize(task)
    completed = [e for e in events if e["event_type"] == "task_completed"]
    assert completed
    assert completed[0]["source_event_version"].startswith("2025-02-02T15:00:00")


def test_snapshot_skips_when_timestamp_missing():
    task = {
        "gid": "t3",
        "resource_type": "task",
        "name": "Task",
        "completed": False,
        "created_at": None,
        "modified_at": None,
        "completed_at": None,
    }
    events = normalize(task, task_state_created=True, task_state_updated=True, task_state_completed=True)
    assert all(e["event_type"] != "task_state" for e in events)


def test_completed_tasks_filtered_when_excluded():
    task = {
        "gid": "t4",
        "resource_type": "task",
        "name": "Task",
        "completed": True,
        "completed_at": "2025-03-01T12:00:00Z",
    }
    events = normalize(task, include_completed=False)
    assert events == []


def test_subtasks_filtered_when_disabled():
    task = {
        "gid": "t5",
        "resource_type": "task",
        "resource_subtype": "subtask",
        "name": "Subtask",
        "created_at": "2025-03-01T12:00:00Z",
    }
    events = normalize(task, sync_subtasks=False)
    assert events == []
