from __future__ import annotations

from typing import Any

from ingestion.normalizers.utils import parse_timestamp


def normalize_habitica(raw: dict[str, Any]) -> dict[str, Any]:
    start_time = parse_timestamp(raw.get("created_at") or raw.get("createdAt"))
    end_time = parse_timestamp(raw.get("completed_at") or raw.get("updatedAt"))
    return {
        "source": "habitica",
        "source_entity_type": raw.get("type") or "task",
        "source_entity_id": str(raw.get("id") or raw.get("_id") or ""),
        "title": raw.get("text") or raw.get("name"),
        "description": raw.get("notes"),
        "start_time": start_time,
        "end_time": end_time,
        "metric_type": raw.get("metric_type"),
        "metric_value": raw.get("metric_value"),
        "metric_unit": raw.get("metric_unit"),
        "status": raw.get("status"),
    }
