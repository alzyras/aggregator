from __future__ import annotations

from typing import Any

from ingestion.normalizers.utils import (
    canonical_event_type,
    parse_timestamp,
    fingerprint_from_fields,
    arbitrate_events,
)
from providers.todoist.settings import get_todoist_settings


def normalize_todoist(raw: dict[str, Any]) -> list[dict[str, Any]]:
    settings = get_todoist_settings(raw.get("__todoist_settings"))
    if not settings.get("sync_tasks"):
        return []

    if not settings.get("include_completed") and raw.get("completed"):
        return []
    if not settings.get("include_archived") and raw.get("is_archived"):
        return []

    external_status = raw.get("status") or ("completed" if raw.get("completed") else "open")
    due = raw.get("due") or {}
    due_date = due.get("date") or due.get("datetime") or raw.get("due_date")
    created_at = parse_timestamp(raw.get("added_at") or raw.get("created_at"))
    completed_at = parse_timestamp(raw.get("completed_at"))
    updated_at = parse_timestamp(raw.get("updated_at") or raw.get("sync_updated_at") or raw.get("date_updated"))

    events: list[dict[str, Any]] = []

    prev_hash = raw.get("__prev_change_hash")
    new_hash = _change_fingerprint(raw)

    if created_at and settings.get("emit_task_created"):
        events.append(
            _base_event(
                raw=raw,
                event_type=canonical_event_type("task_created"),
                occurred_at=created_at,
                external_status="open",
                source_event_version=created_at.isoformat(),
            )
        )

    completion_emitted = False
    if completed_at and settings.get("emit_task_completed") and raw.get("completed"):
        events.append(
            _base_event(
                raw=raw,
                event_type=canonical_event_type("task_completed"),
                occurred_at=completed_at,
                external_status="completed",
                source_event_version=completed_at.isoformat(),
            )
        )
        completion_emitted = True

    if settings.get("emit_task_deleted") and raw.get("is_deleted"):
        ts = updated_at or completed_at or created_at
        if ts:
            events.append(
                _base_event(
                    raw=raw,
                    event_type=canonical_event_type("task_deleted"),
                    occurred_at=ts,
                    external_status="deleted",
                    source_event_version=ts.isoformat(),
                )
            )

    if (
        updated_at
        and settings.get("emit_task_updated")
        and new_hash != prev_hash
        and not (completion_emitted and completed_at and updated_at == completed_at)
    ):
        events.append(
            _base_event(
                raw=raw,
                event_type=canonical_event_type("task_updated"),
                occurred_at=updated_at,
                external_status=external_status,
                source_event_version=updated_at.isoformat(),
            )
        )

    if settings.get("task_state_created") and created_at:
        events.append(
            _base_event(
                raw=raw,
                event_type=canonical_event_type("task_state"),
                occurred_at=created_at,
                external_status="open",
                source_event_version=created_at.isoformat(),
            )
        )
    if settings.get("task_state_updated") and updated_at:
        events.append(
            _base_event(
                raw=raw,
                event_type=canonical_event_type("task_state"),
                occurred_at=updated_at,
                external_status=external_status,
                source_event_version=updated_at.isoformat(),
            )
        )
    if settings.get("task_state_completed") and completed_at:
        events.append(
            _base_event(
                raw=raw,
                event_type=canonical_event_type("task_state"),
                occurred_at=completed_at,
                external_status="completed",
                source_event_version=completed_at.isoformat(),
            )
        )

    seen = set()
    deduped = []
    for ev in events:
        key = (ev["event_type"], ev.get("source_event_version"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(ev)
    return arbitrate_events(deduped)


def _base_event(*, raw: dict[str, Any], event_type: str, occurred_at: Any, external_status: str | None, source_event_version: str | None) -> dict[str, Any]:
    return {
        "source": "todoist",
        "source_entity_type": raw.get("resource_type") or raw.get("type") or "task",
        "source_entity_id": str(raw.get("id") or raw.get("gid") or ""),
        "event_type": canonical_event_type(event_type),
        "title": raw.get("content") or raw.get("title"),
        "description": raw.get("description"),
        "start_time": occurred_at,
        "end_time": None,
        "metric_type": None,
        "metric_value": None,
        "metric_unit": None,
        "external_status": external_status,
        "source_event_version": source_event_version,
    }


def _change_fingerprint(task: dict[str, Any]) -> str:
    due = task.get("due") or {}
    return fingerprint_from_fields(
        {
            "content": task.get("content") or task.get("title"),
            "description": task.get("description"),
            "completed": task.get("completed"),
            "due": due.get("date") or due.get("datetime") or task.get("due_date"),
            "priority": task.get("priority"),
            "labels": sorted(task.get("labels") or []),
            "is_deleted": task.get("is_deleted"),
            "is_archived": task.get("is_archived"),
            "parent_id": task.get("parent_id"),
            "project_id": task.get("project_id"),
            "section_id": task.get("section_id"),
        }
    )
