from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from django.utils.dateparse import parse_datetime


def parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        # Assume milliseconds if larger than year 3000 in seconds
        seconds = value / 1000 if value > 32503680000 else value
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    if isinstance(value, str):
        parsed = parse_datetime(value)
        if parsed:
            return parsed
    return None


def serialize_raw(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: serialize_raw(val) for key, val in value.items()}
    if isinstance(value, list):
        return [serialize_raw(item) for item in value]
    return str(value)


def extract_source_event_version(raw: dict[str, Any], *candidates: str) -> str | None:
    for key in candidates:
        value = raw.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def canonical_task_event_type(value: Any) -> str | None:
    if not value:
        return None
    normalized = str(value).strip().lower()
    mapping = {
        "created": "task_created",
        "create": "task_created",
        "task_created": "task_created",
        "completed": "task_completed",
        "complete": "task_completed",
        "done": "task_completed",
        "task_completed": "task_completed",
        "reopened": "task_reopened",
        "reopen": "task_reopened",
        "task_reopened": "task_reopened",
        "deleted": "task_deleted",
        "delete": "task_deleted",
        "removed": "task_deleted",
        "archived": "task_deleted",
        "task_deleted": "task_deleted",
        "updated": "task_updated",
        "update": "task_updated",
        "task_updated": "task_updated",
    }
    return mapping.get(normalized)


def derive_task_event_type(raw: dict[str, Any], *, completed: bool | None, status: str | None) -> str:
    explicit = canonical_task_event_type(raw.get("event_type") or raw.get("action"))
    if explicit:
        return explicit

    deleted_flags = [
        raw.get("deleted"),
        raw.get("is_deleted"),
        raw.get("isDeleted"),
        raw.get("archived"),
        raw.get("is_archived"),
        raw.get("isArchived"),
    ]
    if any(flag is True for flag in deleted_flags):
        return "task_deleted"

    if raw.get("reopened") is True or raw.get("is_reopened") is True:
        return "task_reopened"

    normalized_status = (status or "").lower()
    if completed is True or normalized_status in {"completed", "done"}:
        return "task_completed"

    created_at = raw.get("created_at") or raw.get("createdAt")
    updated_at = (
        raw.get("updated_at")
        or raw.get("updatedAt")
        or raw.get("modified_at")
        or raw.get("modifiedAt")
    )
    if created_at and (not updated_at or created_at == updated_at):
        return "task_created"

    return "task_updated"
