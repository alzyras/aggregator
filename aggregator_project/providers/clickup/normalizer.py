from __future__ import annotations

from typing import Any

from ingestion.normalizers.utils import arbitrate_events, canonical_event_type, parse_timestamp
from providers.clickup.settings import get_clickup_settings


def normalize_clickup_task(raw: dict[str, Any]) -> list[dict[str, Any]]:
    settings = get_clickup_settings(raw.get("__clickup_settings"))
    task_id = str(raw.get("id") or "")
    if not task_id:
        return []

    status_data = raw.get("status") or {}
    if not isinstance(status_data, dict):
        status_data = {}
    status_name = str(status_data.get("status") or raw.get("status") or "open")
    completed_value = raw.get("date_done")
    is_complete = str(status_data.get("type") or "").lower() == "closed" or completed_value not in {
        None,
        "",
        "0",
        0,
    }
    if is_complete and not settings.get("include_closed"):
        return []

    created_at = _timestamp(raw.get("date_created"))
    updated_at = _timestamp(raw.get("date_updated"))
    completed_at = _timestamp(raw.get("date_done"))
    events: list[dict[str, Any]] = []

    if created_at and settings.get("emit_task_created"):
        events.append(_event(raw, "task_created", created_at, status_name))
    completion_emitted = False
    if is_complete and completed_at and settings.get("emit_task_completed"):
        events.append(_event(raw, "task_completed", completed_at, status_name))
        completion_emitted = True
    if (
        updated_at
        and settings.get("emit_task_updated")
        and not (completion_emitted and updated_at == completed_at)
    ):
        events.append(_event(raw, "task_updated", updated_at, status_name))
    if updated_at and settings.get("emit_task_state"):
        events.append(_event(raw, "task_state", updated_at, status_name))
    return arbitrate_events(events)


def _event(raw: dict[str, Any], event_type: str, occurred_at, external_status: str) -> dict[str, Any]:
    return {
        "source": "clickup",
        "source_entity_type": "task",
        "source_entity_id": str(raw.get("id") or ""),
        "event_type": canonical_event_type(event_type),
        "title": raw.get("name") or raw.get("id"),
        "description": raw.get("markdown_description") or raw.get("description") or "",
        "start_time": occurred_at,
        "end_time": None,
        "metric_type": None,
        "metric_value": None,
        "metric_unit": None,
        "external_status": external_status,
        "source_event_version": occurred_at.isoformat(),
    }


def _timestamp(value: Any):
    if isinstance(value, str) and value.isdigit():
        value = int(value)
    return parse_timestamp(value)
