from __future__ import annotations

from typing import Any

from ingestion.normalizers.utils import canonical_event_type, extract_source_event_version, parse_timestamp


def normalize_google_fit(raw: dict[str, Any]) -> dict[str, Any]:
    if raw.get("record_type"):
        timestamp = parse_timestamp(raw.get("timestamp"))
        data_type = raw.get("data_type") or raw.get("record_type") or "activity"
        value = raw.get("value") or raw.get("steps") or raw.get("heart_rate")
        version = raw.get("timestamp")
        return {
            "source": "google_fit",
            "source_entity_type": "activity",
            "source_entity_id": str(raw.get("id") or raw.get("record_id") or ""),
            "event_type": canonical_event_type("activity_recorded"),
            "title": data_type,
            "description": raw.get("metadata"),
            "start_time": timestamp,
            "end_time": timestamp,
            "metric_type": data_type,
            "metric_value": value,
            "metric_unit": raw.get("unit"),
            "external_status": None,
            "source_event_version": str(version) if version is not None else None,
        }

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
        "event_type": canonical_event_type("activity_recorded"),
        "title": raw.get("name") or raw.get("activityName"),
        "description": raw.get("description"),
        "start_time": start_time,
        "end_time": end_time,
        "metric_type": raw.get("metric_type") or raw.get("metricType") or raw.get("dataType"),
        "metric_value": raw.get("metric_value") or raw.get("value"),
        "metric_unit": raw.get("metric_unit") or raw.get("unit"),
        "external_status": raw.get("status"),
        "source_event_version": extract_source_event_version(
            raw,
            "updated_at",
            "updatedAt",
            "endTime",
            "endTimeMillis",
            "startTime",
            "startTimeMillis",
        ),
    }
