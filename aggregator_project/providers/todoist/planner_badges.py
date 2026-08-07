from __future__ import annotations

from typing import Any


def planner_badges(_item, latest_event_raw: dict[str, Any]) -> list[str]:
    context = latest_event_raw.get("__todoist_planner_context") or {}
    badges = []
    project_name = str(context.get("project_name") or "").strip()
    section_name = str(context.get("section_name") or "").strip()
    if project_name:
        badges.append(project_name)
    if section_name:
        badges.append(section_name)
    return badges
