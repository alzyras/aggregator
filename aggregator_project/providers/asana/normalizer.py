from __future__ import annotations

from typing import Any

from ingestion.normalizers.utils import parse_timestamp


def normalize_asana(raw: dict[str, Any]) -> dict[str, Any]:
    completed = raw.get("completed") or False
    start_time = parse_timestamp(raw.get("created_at") or raw.get("start_at"))
    end_time = parse_timestamp(raw.get("completed_at") or raw.get("due_at"))
    return {
        "source": "asana",
        "source_entity_type": raw.get("resource_type") or "task",
        "source_entity_id": str(raw.get("gid") or raw.get("id") or ""),
        "title": raw.get("name"),
        "description": raw.get("notes"),
        "start_time": start_time,
        "end_time": end_time,
        "metric_type": None,
        "metric_value": None,
        "metric_unit": None,
        "status": "completed" if completed else "open",
    }
