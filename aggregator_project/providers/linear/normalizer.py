from __future__ import annotations

from typing import Any

from ingestion.normalizers.utils import (
    arbitrate_events,
    canonical_event_type,
    parse_timestamp,
)
from providers.linear.settings import get_linear_settings


def normalize_linear_issue(raw: dict[str, Any]) -> list[dict[str, Any]]:
    settings = get_linear_settings(raw.get("__linear_settings"))
    issue_id = str(raw.get("id") or "")
    if not issue_id:
        return []
    state = raw.get("state") or {}
    state_type = str(state.get("type") or "unstarted").lower()
    if state_type == "completed" and not settings.get("include_completed"):
        return []
    if state_type == "canceled" and not settings.get("include_canceled"):
        return []
    if raw.get("archivedAt") and not settings.get("include_archived"):
        return []

    created_at = parse_timestamp(raw.get("createdAt"))
    updated_at = parse_timestamp(raw.get("updatedAt"))
    terminal_at = parse_timestamp(raw.get("completedAt") or raw.get("canceledAt"))
    external_status = str(state.get("name") or state_type)
    events: list[dict[str, Any]] = []

    if created_at and settings.get("emit_task_created"):
        events.append(_event(raw, "task_created", created_at, "unstarted"))
    if (
        state_type in {"completed", "canceled"}
        and terminal_at
        and settings.get("emit_task_completed")
    ):
        events.append(_event(raw, "task_completed", terminal_at, external_status))
    if (
        updated_at
        and settings.get("emit_task_updated")
        and not (terminal_at and updated_at == terminal_at)
    ):
        events.append(_event(raw, "task_updated", updated_at, external_status))
    if updated_at and settings.get("emit_task_state"):
        events.append(_event(raw, "task_state", updated_at, external_status))
    return arbitrate_events(events)


def _event(
    raw: dict[str, Any], event_type: str, occurred_at, external_status: str
) -> dict[str, Any]:
    return {
        "source": "linear",
        "source_entity_type": "issue",
        "source_entity_id": str(raw.get("id") or ""),
        "event_type": canonical_event_type(event_type),
        "title": raw.get("title") or raw.get("identifier") or raw.get("id"),
        "description": raw.get("description") or "",
        "start_time": occurred_at,
        "end_time": None,
        "metric_type": None,
        "metric_value": None,
        "metric_unit": None,
        "external_status": external_status,
        "source_event_version": occurred_at.isoformat(),
    }
