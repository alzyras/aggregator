from __future__ import annotations

from typing import Any

from ingestion.normalizers.utils import parse_timestamp


def normalize_google_fit(raw: dict[str, Any]) -> dict[str, Any]:
    start_time = parse_timestamp(
        raw.get("start_time")
        or raw.get("startTime")
        or raw.get("startTimeMillis")
        or raw.get("start_time_ms")
    )
    end_time = parse_timestamp(
        raw.get("end_time")
        or raw.get("endTime")
        or raw.get("endTimeMillis")
        or raw.get("end_time_ms")
    )
    return {
        "source": "google_fit",
        "source_entity_type": raw.get("type") or raw.get("dataType") or "activity",
        "source_entity_id": str(
            raw.get("id")
            or raw.get("dataPointId")
            or raw.get("uid")
            or raw.get("sessionId")
            or ""
        ),
        "title": raw.get("name") or raw.get("activityName"),
        "description": raw.get("description"),
        "start_time": start_time,
        "end_time": end_time,
        "metric_type": raw.get("metric_type") or raw.get("metricType") or raw.get("dataType"),
        "metric_value": raw.get("metric_value") or raw.get("value"),
        "metric_unit": raw.get("metric_unit") or raw.get("unit"),
        "status": raw.get("status"),
    }
