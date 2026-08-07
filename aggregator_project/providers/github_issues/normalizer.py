from __future__ import annotations

from typing import Any

from ingestion.normalizers.utils import (
    arbitrate_events,
    canonical_event_type,
    parse_timestamp,
)
from providers.github_issues.identity import issue_identity
from providers.github_issues.settings import get_github_settings


def normalize_github_issue(raw: dict[str, Any]) -> list[dict[str, Any]]:
    settings = get_github_settings(raw.get("__github_settings"))
    if raw.get("pull_request") and not settings.get("include_pull_requests"):
        return []
    state = str(raw.get("state") or "open").lower()
    if state == "closed" and not settings.get("include_closed"):
        return []
    identity = issue_identity(raw)
    if not identity:
        return []

    created_at = parse_timestamp(raw.get("created_at"))
    updated_at = parse_timestamp(raw.get("updated_at"))
    closed_at = parse_timestamp(raw.get("closed_at"))
    events: list[dict[str, Any]] = []

    if created_at and settings.get("emit_task_created"):
        events.append(_event(raw, identity, "task_created", created_at, "open"))
    if state == "closed" and closed_at and settings.get("emit_task_completed"):
        events.append(_event(raw, identity, "task_completed", closed_at, "closed"))
    if (
        updated_at
        and settings.get("emit_task_updated")
        and not (closed_at and updated_at == closed_at)
    ):
        events.append(_event(raw, identity, "task_updated", updated_at, state))
    if updated_at and settings.get("emit_task_state"):
        events.append(_event(raw, identity, "task_state", updated_at, state))
    return arbitrate_events(events)


def _event(
    raw: dict[str, Any],
    identity: str,
    event_type: str,
    occurred_at,
    external_status: str,
) -> dict[str, Any]:
    return {
        "source": "github",
        "source_entity_type": "pull_request" if raw.get("pull_request") else "issue",
        "source_entity_id": identity,
        "event_type": canonical_event_type(event_type),
        "title": raw.get("title") or identity,
        "description": raw.get("body") or "",
        "start_time": occurred_at,
        "end_time": None,
        "metric_type": None,
        "metric_value": None,
        "metric_unit": None,
        "external_status": external_status,
        "source_event_version": occurred_at.isoformat(),
    }
