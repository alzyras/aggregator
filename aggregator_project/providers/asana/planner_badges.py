from __future__ import annotations

from typing import Any


def planner_badges(_item, latest_event_raw: dict[str, Any]) -> list[str]:
    context = latest_event_raw.get("__asana_planner_context") or {}
    badges = []
    workspace_name = str(context.get("workspace_name") or "").strip()
    project_name = str(context.get("project_name") or "").strip()
    if workspace_name:
        badges.append(workspace_name)
    if project_name:
        badges.append(project_name)
    return badges
