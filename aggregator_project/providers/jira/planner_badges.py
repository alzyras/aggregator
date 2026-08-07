from __future__ import annotations

from typing import Any


def planner_badges(_item, latest_event_raw: dict[str, Any]) -> list[str]:
    context = latest_event_raw.get("__jira_planner_context") or {}
    badges = []
    project = str(context.get("project") or "").strip()
    epic = str(context.get("epic") or "").strip()
    if project:
        badges.append(project)
    if epic:
        badges.append(epic)
    return badges
