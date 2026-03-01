from __future__ import annotations

from providers.todoist.normalizer import normalize_todoist


def norm(task: dict, **overrides):
    settings = {
        "sync_tasks": True,
        "include_completed": True,
        "include_archived": False,
        "emit_task_created": True,
        "emit_task_updated": True,
        "emit_task_completed": True,
        "emit_task_deleted": True,
        "task_state_created": True,
        "task_state_updated": True,
        "task_state_completed": True,
    }
    settings.update(overrides)
    t = dict(task)
    t["__todoist_settings"] = settings
    return normalize_todoist(t)


def test_created_gated_off():
    task = {"id": 1, "content": "Task", "added_at": "2025-01-01T10:00:00Z"}
    events = norm(task, emit_task_created=False)
    assert all(e["event_type"] != "task_created" for e in events)


def test_completed_uses_completed_at():
    task = {
        "id": 2,
        "content": "Task",
        "completed": True,
        "completed_at": "2025-02-02T10:00:00Z",
        "added_at": "2025-02-01T10:00:00Z",
    }
    events = norm(task)
    completed = [e for e in events if e["event_type"] == "task_completed"]
    assert completed
    assert completed[0]["source_event_version"].startswith("2025-02-02")


def test_task_state_skips_missing_timestamps():
    task = {"id": 3, "content": "Task", "completed": False}
    events = norm(task, task_state_created=True, task_state_updated=True, task_state_completed=True)
    assert all(e["event_type"] != "task_state" for e in events)


def test_include_completed_false_filters():
    task = {"id": 4, "content": "Task", "completed": True, "completed_at": "2025-03-01T10:00:00Z"}
    events = norm(task, include_completed=False)
    assert events == []


def test_deleted_event():
    task = {
        "id": 5,
        "content": "Task",
        "is_deleted": True,
        "updated_at": "2025-04-01T10:00:00Z",
    }
    events = norm(task)
    assert any(e["event_type"] == "task_deleted" for e in events)
