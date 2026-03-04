from __future__ import annotations

from datetime import datetime, time, timezone
from typing import Any

from django.utils.dateparse import parse_date

from ingestion.normalizers.utils import (
    build_actor_fields,
    canonical_event_type,
    parse_timestamp,
    fingerprint_from_fields,
    arbitrate_events,
)
from providers.habitica.settings import get_habitica_settings


def normalize_habitica(raw: dict[str, Any]) -> list[dict[str, Any]]:
    task = raw
    task_type = task.get("type") or "todo"
    actor = task.get("actor")
    settings = get_habitica_settings(task.get("_habitica_settings"))
    prev_hash = task.get("__prev_change_hash")

    if task_type == "habit" and not settings.get("sync_habits"):
        return []
    if task_type == "daily" and not settings.get("sync_dailies"):
        return []
    if task_type == "todo" and not settings.get("sync_todos"):
        return []

    events: list[dict[str, Any]] = []
    if task_type == "habit":
        events.extend(_habit_occurrences(task, actor, settings))
    elif task_type == "daily":
        events.extend(_daily_occurrences(task, actor, settings))
    else:
        events.extend(_todo_occurrences(task, actor, settings))

    events.extend(_task_state_events(task, actor, task_type, settings))
    seen = set()
    deduped = []
    for ev in events:
        key = (ev["event_type"], ev.get("source_event_version"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(ev)
    return arbitrate_events(deduped)


def _habit_occurrences(
    task: dict[str, Any], actor: dict[str, Any] | None, settings: dict[str, bool]
) -> list[dict[str, Any]]:
    if not settings.get("emit_history_occurrences"):
        return []
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
            event_type=canonical_event_type("habit_scored"),
            occurred_at=occurred_at,
            metric_value=entry.get("value"),
            metric_type="score" if entry.get("value") is not None else None,
            external_status="scored",
            source_event_version=occurred_at.isoformat(),
            raw={"task": task, "occurrence": entry},
        )
        events.append(payload)
    return events


def _daily_occurrences(
    task: dict[str, Any], actor: dict[str, Any] | None, settings: dict[str, bool]
) -> list[dict[str, Any]]:
    if not settings.get("emit_completion_occurrences"):
        return []
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
                event_type=canonical_event_type("daily_completed"),
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
                event_type=canonical_event_type("daily_completed"),
                occurred_at=occurred_at,
                metric_value=task.get("value"),
                metric_type="value" if task.get("value") is not None else None,
                external_status="completed",
                source_event_version=occurred_at.isoformat(),
                raw={"task": task, "occurrence": {"date": task.get("dateCompleted")}},
            )
            events.append(payload)
    return events


def _todo_occurrences(
    task: dict[str, Any], actor: dict[str, Any] | None, settings: dict[str, bool]
) -> list[dict[str, Any]]:
    if not settings.get("emit_completion_occurrences"):
        return []
    events: list[dict[str, Any]] = []
    if task.get("completed") and task.get("dateCompleted"):
        occurred_at = _parse_occurrence_timestamp(task.get("dateCompleted"))
        if occurred_at:
            payload = _base_event(
                task=task,
                actor=actor,
                task_type="todo",
                event_type=canonical_event_type("todo_completed"),
                occurred_at=occurred_at,
                metric_value=task.get("value"),
                metric_type="value" if task.get("value") is not None else None,
                external_status="completed",
                source_event_version=occurred_at.isoformat(),
                raw={"task": task, "occurrence": {"date": task.get("dateCompleted")}},
        )
        events.append(payload)
    return events


def _task_state_events(
    task: dict[str, Any],
    actor: dict[str, Any] | None,
    task_type: str,
    settings: dict[str, bool],
) -> list[dict[str, Any]]:
    if not any(
        (
            settings.get("task_state_completed"),
            settings.get("task_state_created"),
            settings.get("task_state_updated"),
        )
    ):
        return []

    updated_at = _parse_occurrence_timestamp(task.get("updatedAt"))
    created_at = _parse_occurrence_timestamp(task.get("dateCreated"))
    completed_at = _parse_occurrence_timestamp(task.get("dateCompleted"))
    status = "completed" if task.get("completed") else "open"
    metric_value = task.get("value")
    metric_type = "value" if metric_value is not None else None

    candidates: list[tuple[str, datetime | None]] = []
    if settings.get("task_state_created"):
        candidates.append(("created", created_at))
    if settings.get("task_state_updated"):
        candidates.append(("updated", updated_at))
    if settings.get("task_state_completed"):
        candidates.append(("completed", completed_at))

    events: list[dict[str, Any]] = []
    seen_versions: set[str] = set()
    fallback_time = updated_at or created_at or datetime.now(timezone.utc)
    completion_occurrence_ts = None
    if settings.get("emit_completion_occurrences") and task.get("dateCompleted"):
        completion_occurrence_ts = _parse_occurrence_timestamp(task.get("dateCompleted"))
    # Also consider completion timestamps from history entries for dailies
    if task_type == "daily" and settings.get("emit_completion_occurrences"):
        for entry in task.get("history") or []:
            if entry.get("completed") is False:
                continue
            ts = _parse_occurrence_timestamp(entry.get("date"))
            if ts:
                completion_occurrence_ts = ts
                break
    if task.get("completed") and completion_occurrence_ts:
        return events

    for label, occurred_at in candidates:
        if not occurred_at:
            continue
        if label == "completed" and completion_occurrence_ts and occurred_at == completion_occurrence_ts:
            # Avoid duplicate: skip completed snapshot when completion occurrence at same timestamp exists.
            continue
        source_version = occurred_at.isoformat()
        if source_version in seen_versions:
            continue
        seen_versions.add(source_version)
        events.append(
            _base_event(
                task=task,
                actor=actor,
                task_type=task_type,
                event_type=canonical_event_type("task_state"),
                occurred_at=occurred_at or fallback_time,
                metric_value=metric_value,
                metric_type=metric_type,
                external_status=status if label != "completed" else "completed",
                source_event_version=source_version,
                raw=task,
            )
        )

    return events


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
