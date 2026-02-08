from __future__ import annotations

from datetime import datetime, time, timezone
from typing import Any

from django.utils.dateparse import parse_date

from ingestion.normalizers.utils import build_actor_fields, parse_timestamp


def normalize_habitica(raw: dict[str, Any]) -> list[dict[str, Any]]:
    task = raw
    task_type = task.get("type") or "todo"
    actor = task.get("actor")

    events: list[dict[str, Any]] = []

    if task_type == "habit":
        events.extend(_habit_occurrences(task, actor))
    elif task_type == "daily":
        events.extend(_daily_occurrences(task, actor))
    else:
        events.extend(_todo_occurrences(task, actor))

    events.append(_task_state(task, actor, task_type))
    return events


def _habit_occurrences(task: dict[str, Any], actor: dict[str, Any] | None) -> list[dict[str, Any]]:
    history = task.get("history") or []
    events: list[dict[str, Any]] = []
    for entry in history:
        occurred_at = _parse_occurrence_timestamp(entry.get("date"))
        if not occurred_at:
            continue
        payload = _base_event(
            task=task,
            actor=actor,
            task_type="habit",
            event_type="habit_scored",
            occurred_at=occurred_at,
            metric_value=entry.get("value"),
            metric_type="score" if entry.get("value") is not None else None,
            external_status="scored",
            source_event_version=occurred_at.isoformat(),
            raw={"task": task, "occurrence": entry},
        )
        events.append(payload)
    return events


def _daily_occurrences(task: dict[str, Any], actor: dict[str, Any] | None) -> list[dict[str, Any]]:
    history = task.get("history") or []
    events: list[dict[str, Any]] = []
    if history:
        for entry in history:
            if entry.get("completed") is False:
                continue
            occurred_at = _parse_occurrence_timestamp(entry.get("date"))
            if not occurred_at:
                continue
            payload = _base_event(
                task=task,
                actor=actor,
                task_type="daily",
                event_type="daily_completed",
                occurred_at=occurred_at,
                metric_value=entry.get("value"),
                metric_type="score" if entry.get("value") is not None else None,
                external_status="completed",
                source_event_version=occurred_at.isoformat(),
                raw={"task": task, "occurrence": entry},
            )
            events.append(payload)
        return events

    if task.get("completed") and task.get("dateCompleted"):
        occurred_at = _parse_occurrence_timestamp(task.get("dateCompleted"))
        if occurred_at:
            payload = _base_event(
                task=task,
                actor=actor,
                task_type="daily",
                event_type="daily_completed",
                occurred_at=occurred_at,
                metric_value=task.get("value"),
                metric_type="value" if task.get("value") is not None else None,
                external_status="completed",
                source_event_version=occurred_at.isoformat(),
                raw={"task": task, "occurrence": {"date": task.get("dateCompleted")}},
            )
            events.append(payload)
    return events


def _todo_occurrences(task: dict[str, Any], actor: dict[str, Any] | None) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if task.get("completed") and task.get("dateCompleted"):
        occurred_at = _parse_occurrence_timestamp(task.get("dateCompleted"))
        if occurred_at:
            payload = _base_event(
                task=task,
                actor=actor,
                task_type="todo",
                event_type="todo_completed",
                occurred_at=occurred_at,
                metric_value=task.get("value"),
                metric_type="value" if task.get("value") is not None else None,
                external_status="completed",
                source_event_version=occurred_at.isoformat(),
                raw={"task": task, "occurrence": {"date": task.get("dateCompleted")}},
            )
            events.append(payload)
    return events


def _task_state(task: dict[str, Any], actor: dict[str, Any] | None, task_type: str) -> dict[str, Any]:
    updated_at = _parse_occurrence_timestamp(task.get("updatedAt"))
    created_at = _parse_occurrence_timestamp(task.get("dateCreated"))
    occurred_at = updated_at or created_at or datetime.now(timezone.utc)
    status = "completed" if task.get("completed") else "open"
    metric_value = task.get("value")
    metric_type = "value" if metric_value is not None else None
    source_version = (
        updated_at.isoformat()
        if updated_at
        else created_at.isoformat()
        if created_at
        else occurred_at.isoformat()
    )
    return _base_event(
        task=task,
        actor=actor,
        task_type=task_type,
        event_type="task_state",
        occurred_at=occurred_at,
        metric_value=metric_value,
        metric_type=metric_type,
        external_status=status,
        source_event_version=source_version,
        raw=task,
    )


def _base_event(
    *,
    task: dict[str, Any],
    actor: dict[str, Any] | None,
    task_type: str,
    event_type: str,
    occurred_at: datetime,
    metric_value: Any,
    metric_type: str | None,
    external_status: str | None,
    source_event_version: str | None,
    raw: dict[str, Any],
) -> dict[str, Any]:
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
        "raw": raw,
    }
    payload.update(build_actor_fields(actor, default_type="user"))
    return payload


def _parse_occurrence_timestamp(value: Any) -> datetime | None:
    parsed = parse_timestamp(value)
    if parsed:
        return parsed
    if isinstance(value, str):
        date_value = parse_date(value)
        if date_value:
            return datetime.combine(date_value, time(0, 0), tzinfo=timezone.utc)
    return None
