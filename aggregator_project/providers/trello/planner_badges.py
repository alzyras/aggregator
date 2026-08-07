from __future__ import annotations


def planner_badges(_item, latest_event_raw: dict) -> list[str]:
    context = latest_event_raw.get("__trello_planner_context") or {}
    badges = []
    list_name = str(context.get("list_name") or "").strip()
    if list_name:
        badges.append(list_name)
    for label in context.get("labels") or []:
        value = str(label).strip()
        if value and value not in badges:
            badges.append(value)
        if len(badges) >= 3:
            break
    return badges
