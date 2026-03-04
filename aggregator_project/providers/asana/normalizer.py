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
from providers.asana.settings import get_asana_settings


def normalize_asana(raw: dict[str, Any]) -> list[dict[str, Any]]:
    task = raw
    actor_default = task.get("last_modified_by") or task.get("created_by")
    settings = get_asana_settings(task.get("__asana_settings"))
    prev_hash = task.get("__prev_change_hash")

    if not settings.get("sync_tasks"):
        return []
    if not settings.get("include_archived"):
        if task.get("archived") or task.get("resource_subtype") == "archived":
            return []
    if not settings.get("include_completed") and task.get("completed") is True:
        return []
    if not settings.get("sync_subtasks") and task.get("resource_subtype") == "subtask":
        return []

    events: list[dict[str, Any]] = []
    created_at = parse_timestamp(task.get("created_at"))
    modified_at = parse_timestamp(task.get("modified_at"))
    completed_at = parse_timestamp(task.get("completed_at"))
    start_at = _parse_occurrence_timestamp(task.get("start_at"))
    due_at = _parse_occurrence_timestamp(task.get("due_at"))

    if created_at and settings.get("emit_task_created"):
        events.append(
            _base_event(
                task=task,
                actor=task.get("created_by") or actor_default,
                event_type=canonical_event_type("task_created"),
                occurred_at=created_at,
                external_status="open",
                source_event_version=created_at.isoformat(),
                raw={"task": task, "occurrence": {"created_at": task.get("created_at")}},
            )
        )

    completion_emitted = False
    if (
        completed_at
        and task.get("completed") is True
        and settings.get("emit_task_completed")
    ):
        events.append(
            _base_event(
                task=task,
                actor=task.get("completed_by") or actor_default,
                event_type=canonical_event_type("task_completed"),
                occurred_at=completed_at,
                external_status="completed",
                source_event_version=completed_at.isoformat(),
                raw={"task": task, "occurrence": {"completed_at": task.get("completed_at")}},
            )
        )
        completion_emitted = True

    if (
        task.get("completed") is False
        and task.get("completed_at")
        and settings.get("emit_task_reopened")
    ):
        reopened_at = modified_at or created_at
        if reopened_at:
            events.append(
                _base_event(
                    task=task,
                    actor=actor_default,
                    event_type=canonical_event_type("task_reopened"),
                    occurred_at=reopened_at,
                    external_status="open",
                    source_event_version=reopened_at.isoformat(),
                    raw={"task": task, "occurrence": {"reopened_at": reopened_at.isoformat()}},
                )
            )

    if (
        (task.get("archived") or task.get("resource_subtype") == "archived")
        and settings.get("emit_task_deleted")
    ):
        archived_at = modified_at or created_at
        if archived_at:
            events.append(
                _base_event(
                    task=task,
                    actor=actor_default,
                    event_type=canonical_event_type("task_deleted"),
                    occurred_at=archived_at,
                    external_status="deleted",
                    source_event_version=archived_at.isoformat(),
                    raw={"task": task, "occurrence": {"archived_at": archived_at.isoformat()}},
                )
            )

    if modified_at and settings.get("emit_task_updated"):
        if not completion_emitted or modified_at != completed_at:
            if not (created_at and modified_at == created_at):
                new_hash = _change_fingerprint(task)
                if new_hash != prev_hash:
                    task["__change_hash"] = new_hash
                    events.append(
                        _base_event(
                            task=task,
                            actor=actor_default,
                            event_type=canonical_event_type("task_updated"),
                            occurred_at=modified_at,
                            external_status="completed" if task.get("completed") else "open",
                            source_event_version=modified_at.isoformat(),
                            raw={"task": task, "occurrence": {"modified_at": task.get("modified_at")}},
                        )
                    )

    # Skip task_state when a completion occurrence exists at the same timestamp to avoid triples.
    state_events = _task_state_events(
        task=task,
        actor=actor_default,
        created_at=created_at,
        modified_at=modified_at,
        completed_at=completed_at,
        start_at=start_at,
        due_at=due_at,
        settings=settings,
    )
    if completion_emitted and completed_at:
        state_events = [
            ev for ev in state_events if ev.get("source_event_version") != completed_at.isoformat()
        ]
    events.extend(state_events)

    seen_versions = set()
    deduped = []
    for ev in events:
        key = (ev["event_type"], ev.get("source_event_version"))
        if key in seen_versions:
            continue
        seen_versions.add(key)
        deduped.append(ev)

    return arbitrate_events(deduped)


def _task_state_events(
    *,
    task: dict[str, Any],
    actor: dict[str, Any] | None,
    created_at: datetime | None,
    modified_at: datetime | None,
    completed_at: datetime | None,
    start_at: datetime | None,
    due_at: datetime | None,
    settings: dict[str, bool],
) -> list[dict[str, Any]]:
    if not any(
        (
            settings.get("task_state_created"),
            settings.get("task_state_updated"),
            settings.get("task_state_completed"),
        )
    ):
        return []

    candidates: list[tuple[str, datetime | None]] = []
    if settings.get("task_state_created"):
        candidates.append(("created", created_at))
    if settings.get("task_state_updated"):
        candidates.append(("updated", modified_at))
    if settings.get("task_state_completed"):
        candidates.append(("completed", completed_at))

    fallback = modified_at or created_at or start_at or due_at or datetime.now(timezone.utc)
    status = "completed" if task.get("completed") else "open"
    events: list[dict[str, Any]] = []
    seen: set[str] = set()
    for label, occurred_at in candidates:
        if not occurred_at:
            continue
        source_version = occurred_at.isoformat()
        if source_version in seen:
            continue
        seen.add(source_version)
        events.append(
            _base_event(
                task=task,
                actor=actor,
                event_type=canonical_event_type("task_state"),
                occurred_at=occurred_at or fallback,
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
    event_type: str,
    occurred_at: datetime,
    external_status: str | None,
    source_event_version: str | None,
    raw: dict[str, Any],
) -> dict[str, Any]:
    payload = {
        "source": "asana",
        "source_entity_type": task.get("resource_type") or "task",
        "source_entity_id": str(task.get("gid") or task.get("id") or ""),
        "event_type": canonical_event_type(event_type),
        "title": task.get("name"),
        "description": task.get("notes"),
        "start_time": occurred_at,
        "end_time": None,
        "metric_type": None,
        "metric_value": None,
        "metric_unit": None,
        "external_status": external_status,
        "source_event_version": source_event_version,
        "raw": raw,
    }
    payload.update(build_actor_fields(actor, default_type="user"))
    return payload


def _change_fingerprint(task: dict[str, Any]) -> str:
    tags_raw = task.get("tags") or []
    tags_normalized = []
    for tag in tags_raw:
        if isinstance(tag, dict):
            val = tag.get("gid") or tag.get("id") or tag.get("name")
        else:
            val = tag
        if val is not None:
            tags_normalized.append(str(val))

    return fingerprint_from_fields(
        {
            "name": task.get("name"),
            "notes": task.get("notes"),
            "completed": task.get("completed"),
            "due_at": task.get("due_at"),
            "start_at": task.get("start_at"),
            "assignee": (task.get("assignee") or {}).get("gid"),
            "priority": task.get("priority"),
            "tags": sorted(tags_normalized),
            "archived": task.get("archived"),
        }
    )


def _parse_occurrence_timestamp(value: Any) -> datetime | None:
    parsed = parse_timestamp(value)
    if parsed:
        return parsed
    if isinstance(value, str):
        date_value = parse_date(value)
        if date_value:
            return datetime.combine(date_value, time(0, 0), tzinfo=timezone.utc)
    return None
