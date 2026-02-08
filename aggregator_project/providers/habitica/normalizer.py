from __future__ import annotations

from typing import Any

from ingestion.normalizers.utils import build_actor_fields, parse_timestamp


def normalize_habitica(raw: dict[str, Any]) -> dict[str, Any]:
    task = raw.get("task") or raw
    occurrence = raw.get("occurrence") or {}
    task_type = raw.get("task_type") or task.get("type") or "todo"
    actor = raw.get("actor")

    occurred_at = parse_timestamp(
        occurrence.get("date")
        or occurrence.get("dateCompleted")
        or task.get("dateCompleted")
    )

    if task_type == "habit":
        event_type = "habit_scored"
        external_status = "scored"
        metric_value = occurrence.get("value")
        metric_type = "score" if metric_value is not None else None
    elif task_type == "daily":
        event_type = "daily_completed"
        external_status = "completed"
        metric_value = occurrence.get("value")
        metric_type = "score" if metric_value is not None else None
    else:
        event_type = "todo_completed"
        external_status = "completed"
        metric_value = None
        metric_type = None

    source_event_version = occurred_at.isoformat() if occurred_at else None

    payload = {
        "source": "habitica",
        "source_entity_type": task_type,
        "source_entity_id": str(task.get("id") or task.get("_id") or ""),
        "event_type": event_type,
        "title": task.get("text") or task.get("name"),
        "description": task.get("notes"),
        "start_time": occurred_at,
        "end_time": None,
        "metric_type": metric_type,
        "metric_value": metric_value,
        "metric_unit": "points" if metric_type else None,
        "external_status": external_status,
        "source_event_version": source_event_version,
    }
    payload.update(build_actor_fields(actor, default_type="user"))
    return payload
