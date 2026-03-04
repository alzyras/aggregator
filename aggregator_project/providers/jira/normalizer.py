from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from ingestion.normalizers.utils import (
    arbitrate_events,
    build_actor_fields,
    canonical_event_type,
    fingerprint_from_fields,
    parse_timestamp,
)
from providers.jira.settings import get_jira_config


def normalize_jira(raw: dict[str, Any]) -> list[dict[str, Any]]:
    config = get_jira_config({"jira": raw.get("__jira_config") or {}})
    fields = raw.get("fields") or {}
    issue_id = str(raw.get("id") or raw.get("key") or "")
    issue_key = str(raw.get("key") or issue_id)
    if not issue_id:
        return []

    title = fields.get("summary")
    description = _stringify_description(fields.get("description"))
    created_at = parse_timestamp(fields.get("created"))
    updated_at = parse_timestamp(fields.get("updated"))
    resolved_at = parse_timestamp(fields.get("resolutiondate"))
    status = fields.get("status") or {}
    status_name = status.get("name")
    status_category = _status_category_key(status)

    issue_events: list[dict[str, Any]] = []

    if config.get("emit_task_created") and created_at:
        issue_events.append(
            _issue_event(
                raw=raw,
                issue_id=issue_id,
                issue_key=issue_key,
                title=title,
                description=description,
                event_type="task_created",
                occurred_at=created_at,
                external_status=status_name,
                source_event_version=f"issue:{issue_id}:created:{created_at.isoformat()}",
                actor=fields.get("creator") or fields.get("reporter"),
            )
        )

    completion_timestamps: set[str] = set()
    if config.get("include_changelog"):
        issue_events.extend(
            _changelog_events(
                raw=raw,
                issue_id=issue_id,
                issue_key=issue_key,
                title=title,
                description=description,
                status_name=status_name,
                config=config,
                completion_timestamps=completion_timestamps,
            )
        )

    if config.get("emit_task_completed") and not completion_timestamps and _is_done_status(status_category):
        completed_at = resolved_at or updated_at
        if completed_at:
            version = f"issue:{issue_id}:completed:{completed_at.isoformat()}"
            completion_timestamps.add(completed_at.isoformat())
            issue_events.append(
                _issue_event(
                    raw=raw,
                    issue_id=issue_id,
                    issue_key=issue_key,
                    title=title,
                    description=description,
                    event_type="task_completed",
                    occurred_at=completed_at,
                    external_status=status_name,
                    source_event_version=version,
                    actor=fields.get("assignee") or fields.get("reporter"),
                )
            )

    if config.get("emit_task_deleted") and _is_deleted_status(status_name):
        deleted_at = updated_at or created_at
        if deleted_at:
            issue_events.append(
                _issue_event(
                    raw=raw,
                    issue_id=issue_id,
                    issue_key=issue_key,
                    title=title,
                    description=description,
                    event_type="task_deleted",
                    occurred_at=deleted_at,
                    external_status=status_name,
                    source_event_version=f"issue:{issue_id}:deleted:{deleted_at.isoformat()}",
                    actor=fields.get("assignee") or fields.get("reporter"),
                )
            )

    if config.get("emit_task_updated") and updated_at:
        if not (created_at and updated_at == created_at):
            if updated_at.isoformat() not in completion_timestamps:
                prev_hash = raw.get("__prev_change_hash")
                new_hash = _issue_fingerprint(raw)
                if new_hash != prev_hash:
                    raw["__change_hash"] = new_hash
                    issue_events.append(
                        _issue_event(
                            raw=raw,
                            issue_id=issue_id,
                            issue_key=issue_key,
                            title=title,
                            description=description,
                            event_type="task_updated",
                            occurred_at=updated_at,
                            external_status=status_name,
                            source_event_version=f"issue:{issue_id}:updated:{updated_at.isoformat()}",
                            actor=fields.get("assignee") or fields.get("reporter"),
                        )
                    )

    if config.get("emit_task_state") and updated_at:
        issue_events.append(
            _issue_event(
                raw=raw,
                issue_id=issue_id,
                issue_key=issue_key,
                title=title,
                description=description,
                event_type="task_state",
                occurred_at=updated_at,
                external_status=status_name,
                source_event_version=f"issue:{issue_id}:state:{updated_at.isoformat()}",
                actor=fields.get("assignee") or fields.get("reporter"),
            )
        )

    deduped_issue_events = _dedupe_events(issue_events)
    deduped_issue_events = arbitrate_events(deduped_issue_events)

    metric_events: list[dict[str, Any]] = []
    if config.get("include_worklogs") and config.get("emit_worklog_metrics"):
        metric_events.extend(_worklog_events(raw, issue_key, config))

    return deduped_issue_events + _dedupe_events(metric_events)


def _changelog_events(
    *,
    raw: dict[str, Any],
    issue_id: str,
    issue_key: str,
    title: str | None,
    description: str | None,
    status_name: str | None,
    config: dict[str, Any],
    completion_timestamps: set[str],
) -> list[dict[str, Any]]:
    histories = ((raw.get("changelog") or {}).get("histories") or [])
    events: list[dict[str, Any]] = []
    for history in histories:
        occurred_at = parse_timestamp(history.get("created"))
        if not occurred_at:
            continue
        actor = history.get("author")
        history_id = str(history.get("id") or occurred_at.isoformat())
        emitted_status_event = False
        has_non_status_change = False

        for item in history.get("items") or []:
            field_name = (item.get("field") or item.get("fieldId") or "").lower()
            if field_name != "status":
                has_non_status_change = True
                continue

            from_category = _status_category_from_name(item.get("fromString"))
            to_category = _status_category_from_name(item.get("toString"))
            if config.get("emit_task_completed") and not _is_done_status(from_category) and _is_done_status(to_category):
                events.append(
                    _issue_event(
                        raw=raw,
                        issue_id=issue_id,
                        issue_key=issue_key,
                        title=title,
                        description=description,
                        event_type="task_completed",
                        occurred_at=occurred_at,
                        external_status=item.get("toString") or status_name,
                        source_event_version=f"history:{issue_id}:{history_id}:task_completed",
                        actor=actor,
                    )
                )
                completion_timestamps.add(occurred_at.isoformat())
                emitted_status_event = True
            elif config.get("emit_task_reopened") and _is_done_status(from_category) and not _is_done_status(to_category):
                events.append(
                    _issue_event(
                        raw=raw,
                        issue_id=issue_id,
                        issue_key=issue_key,
                        title=title,
                        description=description,
                        event_type="task_reopened",
                        occurred_at=occurred_at,
                        external_status=item.get("toString") or status_name,
                        source_event_version=f"history:{issue_id}:{history_id}:task_reopened",
                        actor=actor,
                    )
                )
                emitted_status_event = True

        if config.get("emit_task_updated") and has_non_status_change and not emitted_status_event:
            events.append(
                _issue_event(
                    raw=raw,
                    issue_id=issue_id,
                    issue_key=issue_key,
                    title=title,
                    description=description,
                    event_type="task_updated",
                    occurred_at=occurred_at,
                    external_status=status_name,
                    source_event_version=f"history:{issue_id}:{history_id}:task_updated",
                    actor=actor,
                )
            )
    return events


def _worklog_events(raw: dict[str, Any], issue_key: str, config: dict[str, Any]) -> list[dict[str, Any]]:
    fields = raw.get("fields") or {}
    candidates = []
    field_worklogs = ((fields.get("worklog") or {}).get("worklogs")) or []
    expanded_worklogs = raw.get("_expanded_worklogs") or []
    candidates.extend(field_worklogs)
    candidates.extend(expanded_worklogs)
    if not candidates:
        return []

    events: list[dict[str, Any]] = []
    for worklog in candidates:
        worklog_id = str(worklog.get("id") or "")
        if not worklog_id:
            continue
        occurred_at = (
            parse_timestamp(worklog.get("started"))
            or parse_timestamp(worklog.get("created"))
            or parse_timestamp(worklog.get("updated"))
        )
        if not occurred_at:
            continue
        seconds_spent = worklog.get("timeSpentSeconds")
        if seconds_spent is None:
            continue
        payload = {
            "source": "jira",
            "source_entity_type": "worklog",
            "source_entity_id": worklog_id,
            "event_type": canonical_event_type("metric_recorded"),
            "title": f"Worklog for {issue_key}",
            "description": worklog.get("comment"),
            "start_time": occurred_at,
            "end_time": None,
            "metric_type": "time_spent",
            "metric_value": Decimal(str(seconds_spent)),
            "metric_unit": "seconds",
            "external_status": "recorded",
            "source_event_version": f"worklog:{worklog_id}:{occurred_at.isoformat()}",
            "raw": {
                "issue": raw,
                "worklog": worklog,
                "config": {"include_worklogs": bool(config.get("include_worklogs"))},
            },
        }
        payload.update(build_actor_fields(worklog.get("author"), default_type="user"))
        events.append(payload)
    return events


def _issue_event(
    *,
    raw: dict[str, Any],
    issue_id: str,
    issue_key: str,
    title: str | None,
    description: str | None,
    event_type: str,
    occurred_at: datetime,
    external_status: str | None,
    source_event_version: str,
    actor: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = {
        "source": "jira",
        "source_entity_type": "issue",
        "source_entity_id": issue_id,
        "event_type": canonical_event_type(event_type),
        "title": title or issue_key,
        "description": description,
        "start_time": occurred_at,
        "end_time": None,
        "metric_type": None,
        "metric_value": None,
        "metric_unit": None,
        "external_status": external_status,
        "source_event_version": source_event_version,
        "raw": raw,
    }
    payload.update(build_actor_fields(actor, default_type="user"))
    return payload


def _dedupe_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    deduped = []
    for event in events:
        key = (event.get("event_type"), event.get("source_event_version"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(event)
    return deduped


def _status_category_key(status: dict[str, Any]) -> str:
    category = status.get("statusCategory") or {}
    key = (
        category.get("key")
        or category.get("name")
        or status.get("statusCategory")
        or ""
    )
    return str(key).strip().lower().replace(" ", "_")


def _status_category_from_name(value: str | None) -> str:
    if not value:
        return ""
    normalized = str(value).strip().lower()
    if normalized in {"done", "closed", "resolved"}:
        return "done"
    if normalized in {"in progress", "in_progress", "doing"}:
        return "in_progress"
    if normalized in {"to do", "todo", "open", "backlog"}:
        return "todo"
    return normalized


def _is_done_status(value: str | None) -> bool:
    normalized = (value or "").strip().lower()
    return normalized in {"done", "closed", "resolved"}


def _is_deleted_status(value: str | None) -> bool:
    normalized = (value or "").strip().lower()
    return normalized in {"deleted", "removed"}


def _stringify_description(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def _issue_fingerprint(raw: dict[str, Any]) -> str:
    fields = raw.get("fields") or {}
    assignee = fields.get("assignee") or {}
    return fingerprint_from_fields(
        {
            "summary": fields.get("summary"),
            "description": _stringify_description(fields.get("description")),
            "status": ((fields.get("status") or {}).get("name")),
            "updated": fields.get("updated"),
            "assignee": assignee.get("accountId") or assignee.get("emailAddress"),
            "labels": sorted(fields.get("labels") or []),
            "priority": ((fields.get("priority") or {}).get("name")),
        }
    )

