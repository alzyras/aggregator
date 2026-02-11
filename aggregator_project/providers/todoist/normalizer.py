from __future__ import annotations

from typing import Any

from ingestion.normalizers.utils import (
    canonical_event_type,
    derive_task_event_type,
    extract_source_event_version,
    parse_timestamp,
)


def normalize_todoist(raw: dict[str, Any]) -> dict[str, Any]:
    external_status = raw.get("status") or ("completed" if raw.get("completed") else "open")
    due = raw.get("due") or {}
    due_date = due.get("date") or due.get("datetime") or raw.get("due_date")
    start_time = parse_timestamp(raw.get("created_at"))
    end_time = parse_timestamp(raw.get("completed_at") or due_date)
    return {
        "source": "todoist",
        "source_entity_type": raw.get("type") or "task",
        "source_entity_id": str(raw.get("id") or raw.get("gid") or ""),
        "event_type": canonical_event_type(
            derive_task_event_type(
                raw,
                completed=raw.get("completed"),
                status=external_status,
            )
        ),
        "title": raw.get("content") or raw.get("title"),
        "description": raw.get("description"),
        "start_time": start_time,
        "end_time": end_time,
        "metric_type": None,
        "metric_value": None,
        "metric_unit": None,
        "external_status": external_status,
        "source_event_version": extract_source_event_version(
            raw,
            "updated_at",
            "completed_at",
            "created_at",
            "date_added",
        ),
    }
