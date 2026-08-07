from __future__ import annotations

from ingestion.normalizers.utils import arbitrate_events, canonical_event_type, parse_timestamp
from providers.trello.settings import get_trello_settings


def normalize_trello_card(raw: dict) -> list[dict]:
    settings = get_trello_settings(raw.get("__trello_settings"))
    card_id = str(raw.get("id") or "")
    if not card_id:
        return []
    is_closed = bool(raw.get("closed"))
    if is_closed and not settings.get("include_closed"):
        return []

    context = raw.get("__trello_planner_context") or {}
    status = "closed" if is_closed else str(context.get("list_name") or "open")
    created_at = parse_timestamp(raw.get("__trello_created_at"))
    updated_at = parse_timestamp(raw.get("dateLastActivity"))
    events: list[dict] = []
    if created_at and settings.get("emit_task_created"):
        events.append(_event(raw, "task_created", created_at, status))
    completion_emitted = False
    if is_closed and updated_at and settings.get("emit_task_completed"):
        events.append(_event(raw, "task_completed", updated_at, "closed"))
        completion_emitted = True
    if (
        updated_at
        and settings.get("emit_task_updated")
        and not completion_emitted
    ):
        events.append(_event(raw, "task_updated", updated_at, status))
    if updated_at and settings.get("emit_task_state"):
        events.append(_event(raw, "task_state", updated_at, status))
    return arbitrate_events(events)


def _event(raw: dict, event_type: str, occurred_at, external_status: str) -> dict:
    return {
        "source": "trello",
        "source_entity_type": "card",
        "source_entity_id": str(raw.get("id") or ""),
        "event_type": canonical_event_type(event_type),
        "title": raw.get("name") or raw.get("id"),
        "description": raw.get("desc") or "",
        "start_time": occurred_at,
        "end_time": None,
        "metric_type": None,
        "metric_value": None,
        "metric_unit": None,
        "external_status": external_status,
        "source_event_version": occurred_at.isoformat(),
    }
