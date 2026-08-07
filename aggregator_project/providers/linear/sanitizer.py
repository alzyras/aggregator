from __future__ import annotations

from typing import Any


KEEP_KEYS = {
    "__linear_planner_context",
    "id",
    "identifier",
    "title",
    "description",
    "url",
    "createdAt",
    "updatedAt",
    "completedAt",
    "canceledAt",
    "archivedAt",
    "dueDate",
    "priority",
    "priorityLabel",
    "state",
    "team",
    "project",
    "cycle",
    "assignee",
    "labels",
}


def sanitize_raw(raw: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in raw.items() if key in KEEP_KEYS}
