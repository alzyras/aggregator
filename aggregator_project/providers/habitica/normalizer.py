from __future__ import annotations

from typing import Any

from ingestion.normalizers.utils import (
    derive_task_event_type,
    extract_source_event_version,
    parse_timestamp,
)


def normalize_habitica(raw: dict[str, Any]) -> dict[str, Any]:
    if raw.get("item_id"):
        completed = raw.get("completed") is True
        version = raw.get("date_completed") or raw.get("date_created")
        return {
            "source": "habitica",
            "source_entity_type": "task",
            "source_entity_id": str(raw.get("item_id") or ""),
            "event_type": "task_completed" if completed else "task_updated",
            "title": raw.get("item_name"),
            "description": raw.get("notes"),
            "start_time": parse_timestamp(raw.get("date_created")),
            "end_time": parse_timestamp(raw.get("date_completed")),
            "metric_type": raw.get("item_type"),
            "metric_value": raw.get("value"),
            "metric_unit": None,
            "external_status": "completed" if completed else None,
            "source_event_version": str(version) if version is not None else None,
        }

    start_time = parse_timestamp(raw.get("created_at") or raw.get("createdAt"))
    end_time = parse_timestamp(raw.get("completed_at") or raw.get("updatedAt"))
    external_status = raw.get("status")
    return {
        "source": "habitica",
        "source_entity_type": raw.get("type") or "task",
        "source_entity_id": str(raw.get("id") or raw.get("_id") or ""),
        "event_type": derive_task_event_type(
            raw,
            completed=raw.get("completed"),
            status=external_status,
        ),
        "title": raw.get("text") or raw.get("name"),
        "description": raw.get("notes"),
        "start_time": start_time,
        "end_time": end_time,
        "metric_type": raw.get("metric_type"),
        "metric_value": raw.get("metric_value"),
        "metric_unit": raw.get("metric_unit"),
        "external_status": external_status,
        "source_event_version": extract_source_event_version(
            raw,
            "updatedAt",
            "updated_at",
            "completed_at",
            "createdAt",
            "created_at",
        ),
    }
