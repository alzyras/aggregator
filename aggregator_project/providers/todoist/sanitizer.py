from __future__ import annotations

from typing import Any


KEEP_KEYS = {
    "__todoist_planner_context",
    "id",
    "content",
    "description",
    "checked",
    "completed",
    "is_completed",
    "is_deleted",
    "is_archived",
    "added_at",
    "created_at",
    "updated_at",
    "completed_at",
    "due",
    "url",
    "project_id",
    "section_id",
    "parent_id",
    "labels",
    "priority",
}


def sanitize_raw(raw: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in raw.items() if key in KEEP_KEYS}
