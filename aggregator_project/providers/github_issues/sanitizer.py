from __future__ import annotations

from typing import Any


KEEP_KEYS = {
    "__github_repository",
    "__github_planner_context",
    "id",
    "node_id",
    "number",
    "title",
    "body",
    "state",
    "state_reason",
    "html_url",
    "repository_url",
    "created_at",
    "updated_at",
    "closed_at",
    "labels",
    "assignees",
    "milestone",
    "pull_request",
}


def sanitize_raw(raw: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in raw.items() if key in KEEP_KEYS}
