from __future__ import annotations

from typing import Any


KEEP_KEYS = {
    "__asana_planner_context",
    "gid",
    "resource_type",
    "name",
    "completed",
    "completed_at",
    "created_at",
    "modified_at",
    "due_at",
    "due_on",
    "permalink_url",
    "resource_subtype",
    "assignee_status",
    "workspace",
    "projects",
    "memberships",
}


def sanitize_raw(raw: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in raw.items() if key in KEEP_KEYS}
