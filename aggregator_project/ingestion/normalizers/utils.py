from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import hashlib
import json

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


def fingerprint_from_fields(fields: dict[str, Any]) -> str:
    """Deterministic hash for change detection."""
    serialized = json.dumps(fields, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


PRIORITY_ORDER = {
    "task_completed": 1,
    "task_updated": 2,
    "task_created": 3,
    "task_state": 4,
}


def arbitrate_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Keep at most one lifecycle event per (entity, timestamp) by priority.

    We normalise the timestamp key aggressively to collapse cases where
    providers give slightly different microsecond values or when only the
    source_event_version carries the timestamp.
    """

    def _timestamp_key(ev: dict[str, Any]) -> Any:
        # Prefer start_time; fall back to source_event_version.
        ts = ev.get("start_time")
        parsed = parse_timestamp(ts)
        if not parsed:
            parsed = parse_timestamp(ev.get("source_event_version"))
        if isinstance(parsed, datetime):
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            # Collapse to whole-second precision to avoid microsecond drift.
            parsed = parsed.replace(microsecond=0)
            return ("dt", parsed)
        raw = ts or ev.get("source_event_version")
        return ("raw", raw)

    chosen: dict[tuple[str, Any], dict[str, Any]] = {}
    for ev in events:
        key = (ev.get("source_entity_id"), _timestamp_key(ev))
        prio = PRIORITY_ORDER.get(ev.get("event_type"), 99)
        current = chosen.get(key)
        if current is None or PRIORITY_ORDER.get(current.get("event_type"), 99) > prio:
            chosen[key] = ev
    return list(chosen.values())


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


CANONICAL_EVENT_TYPES = {
    "task_created",
    "task_updated",
    "task_completed",
    "task_reopened",
    "task_deleted",
    "task_state",
    "metric_recorded",
}


def canonical_event_type(value: Any) -> str:
    if not value:
        return "task_updated"
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
        "task_state": "task_state",
        "metric_recorded": "metric_recorded",
        "activity_recorded": "metric_recorded",
        "habit_scored": "metric_recorded",
        "daily_completed": "task_completed",
        "todo_completed": "task_completed",
    }
    return mapping.get(normalized, "task_updated")


def build_actor_fields(
    actor: dict[str, Any] | None,
    *,
    default_type: str | None = None,
) -> dict[str, Any]:
    if not actor:
        return {
            "external_actor_id": None,
            "external_actor_type": None,
            "external_actor_display_name": None,
            "external_actor_raw": None,
        }

    actor_id = (
        actor.get("accountId")
        or actor.get("id")
        or actor.get("_id")
        or actor.get("userId")
        or actor.get("gid")
    )
    display_name = (
        actor.get("displayName")
        or actor.get("publicName")
        or actor.get("display_name")
        or actor.get("name")
        or actor.get("profile", {}).get("name")
        or actor.get("auth", {}).get("local", {}).get("username")
    )
    actor_type = actor.get("accountType") or actor.get("type") or default_type

    return {
        "external_actor_id": str(actor_id) if actor_id is not None else None,
        "external_actor_type": actor_type,
        "external_actor_display_name": display_name,
        "external_actor_raw": actor,
    }


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
