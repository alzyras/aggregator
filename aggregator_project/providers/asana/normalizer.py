from __future__ import annotations

from typing import Any

from ingestion.normalizers.utils import (
    derive_task_event_type,
    extract_source_event_version,
    parse_timestamp,
)


def normalize_asana(raw: dict[str, Any]) -> dict[str, Any]:
    if raw.get("task_id"):
        completed = raw.get("completed") is True
        completed_at = parse_timestamp(raw.get("date") or raw.get("completed_at"))
        version = raw.get("date") or raw.get("completed_at") or raw.get("created_at")
        return {
            "source": "asana",
            "source_entity_type": "task",
            "source_entity_id": str(raw.get("task_id") or ""),
            "event_type": "task_completed" if completed else "task_updated",
            "title": raw.get("task_name"),
            "description": raw.get("task_description"),
            "start_time": parse_timestamp(raw.get("created_at")),
            "end_time": completed_at,
            "metric_type": "time_to_completion",
            "metric_value": raw.get("time_to_completion"),
            "metric_unit": "seconds",
            "external_status": "completed" if completed else None,
            "source_event_version": str(version) if version is not None else None,
        }

    completed = raw.get("completed") or False
    external_status = "completed" if completed else "open"
    start_time = parse_timestamp(raw.get("created_at") or raw.get("start_at"))
    end_time = parse_timestamp(raw.get("completed_at") or raw.get("due_at"))
    return {
        "source": "asana",
        "source_entity_type": raw.get("resource_type") or "task",
        "source_entity_id": str(raw.get("gid") or raw.get("id") or ""),
        "event_type": derive_task_event_type(
            raw,
            completed=completed,
            status=external_status,
        ),
        "title": raw.get("name"),
        "description": raw.get("notes"),
        "start_time": start_time,
        "end_time": end_time,
        "metric_type": None,
        "metric_value": None,
        "metric_unit": None,
        "external_status": external_status,
        "source_event_version": extract_source_event_version(
            raw,
            "modified_at",
            "updated_at",
            "completed_at",
            "created_at",
        ),
    }
