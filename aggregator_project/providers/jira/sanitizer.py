from __future__ import annotations

from typing import Any


FIELD_KEEP_KEYS = {
    "summary",
    "description",
    "status",
    "issuetype",
    "priority",
    "created",
    "updated",
    "resolutiondate",
    "assignee",
    "reporter",
    "project",
    "parent",
}


def sanitize_raw(raw: dict[str, Any]) -> dict[str, Any]:
    sanitized = {
        "__jira_planner_context": raw.get("__jira_planner_context"),
        "id": raw.get("id"),
        "key": raw.get("key"),
        "self": raw.get("self"),
    }
    fields = raw.get("fields")
    if isinstance(fields, dict):
        sanitized["fields"] = {
            key: value for key, value in fields.items() if key in FIELD_KEEP_KEYS
        }
    return {key: value for key, value in sanitized.items() if value is not None}
