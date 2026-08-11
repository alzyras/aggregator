from __future__ import annotations

import hashlib
from datetime import date
from typing import Any

from django.conf import settings
from django.db.models import Max, Q
from django.db.models.functions import Coalesce
from django.utils import timezone

from ingestion.services.cache import cache_get, cache_set, workspace_cache_key
from planner.models import PlannerItem, PlannerItemState, PlannerPlan


DAILY_BRIEF_CACHE_NAMESPACE = "daily-brief-v1"
TASK_LIMIT = 5


def build_daily_brief(
    *,
    workspace,
    user,
    cache_version: int | None = None,
    now=None,
) -> dict[str, Any]:
    """Build a compact daily view from the user's canonical Planner state."""
    now = now or timezone.now()
    today = timezone.localdate(now)
    plan = (
        PlannerPlan.objects.filter(workspace=workspace, user=user)
        .order_by("id")
        .first()
    )
    local_token = _planner_local_token(workspace=workspace, user=user, plan=plan)
    cache_key = workspace_cache_key(
        workspace,
        DAILY_BRIEF_CACHE_NAMESPACE,
        user.id,
        plan.id if plan else "none",
        today.isoformat(),
        local_token,
        cache_version=cache_version,
    )
    cached = cache_get(cache_key)
    if isinstance(cached, dict):
        return cached

    brief = _build_daily_brief(
        workspace=workspace,
        user=user,
        plan=plan,
        today=today,
    )
    cache_set(cache_key, brief, timeout=settings.CACHE_DEFAULT_TIMEOUT_SECONDS)
    return brief


def _build_daily_brief(*, workspace, user, plan: PlannerPlan | None, today: date) -> dict[str, Any]:
    states = []
    if plan:
        states = list(
            PlannerItemState.objects.filter(plan=plan, item__is_active=True)
            .select_related("item", "item__connector_account")
            .order_by("planned_order", "id")
        )

    focus_states = []
    today_states = []
    upcoming_states = []
    assigned_item_ids = set()
    assigned_source_keys = set()
    for state in states:
        item = state.item
        assigned_item_ids.add(item.id)
        source_key = _source_key(item)
        if source_key:
            assigned_source_keys.add(source_key)
        if state.planner_status == PlannerItemState.PLANNER_STATUS_DOING:
            focus_states.append(state)
            continue
        if state.planner_status != PlannerItemState.PLANNER_STATUS_BACKLOG:
            continue
        scheduled_day = _planned_day(state.planned_start)
        if scheduled_day == today:
            today_states.append(state)
        elif scheduled_day and scheduled_day > today:
            upcoming_states.append(state)

    focus_states.sort(key=lambda state: (not state.pinned, state.planned_order, state.id))
    today_states.sort(key=lambda state: (state.planned_start, state.planned_order, state.id))
    upcoming_states.sort(key=lambda state: (state.planned_start, state.planned_order, state.id))

    inbox_items = []
    unplanned_items = (
        PlannerItem.objects.for_workspace(workspace)
        .filter(
            Q(user=user) | Q(user__isnull=True),
            is_active=True,
            last_synced_at__isnull=False,
        )
        .exclude(id__in=assigned_item_ids)
        .select_related("connector_account")
        .annotate(inbox_created_sort=Coalesce("source_created_at", "created_at"))
        .order_by("-inbox_created_sort", "-created_at", "-id")
    )
    for item in unplanned_items:
        source_key = _source_key(item)
        if source_key and source_key in assigned_source_keys:
            continue
        inbox_items.append(item)

    focus_minutes = sum(state.estimated_minutes or 0 for state in focus_states)
    today_minutes = sum(state.estimated_minutes or 0 for state in today_states)
    return {
        "today": today,
        "focus": [_state_task(state) for state in focus_states[:TASK_LIMIT]],
        "focus_count": len(focus_states),
        "focus_minutes": focus_minutes,
        "today_tasks": [_state_task(state) for state in today_states[:TASK_LIMIT]],
        "today_count": len(today_states),
        "today_minutes": today_minutes,
        "upcoming": [_state_task(state) for state in upcoming_states[:TASK_LIMIT]],
        "upcoming_count": len(upcoming_states),
        "triage": [_item_task(item) for item in inbox_items[:TASK_LIMIT]],
        "inbox_count": len(inbox_items),
        "has_tasks": bool(focus_states or today_states or upcoming_states or inbox_items),
    }


def _planner_local_token(*, workspace, user, plan: PlannerPlan | None) -> str:
    item_updates = (
        PlannerItem.objects.for_workspace(workspace)
        .filter(Q(user=user) | Q(user__isnull=True))
        .aggregate(latest=Max("updated_at"))
    )
    state_latest = None
    if plan:
        state_latest = PlannerItemState.objects.filter(plan=plan).aggregate(
            latest=Max("last_planned_at")
        )["latest"]
    payload = "|".join(
        [
            str(plan.id) if plan else "none",
            item_updates["latest"].isoformat() if item_updates["latest"] else "",
            state_latest.isoformat() if state_latest else "",
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]


def _state_task(state: PlannerItemState) -> dict[str, Any]:
    return {
        **_item_task(state.item),
        "collection": state.collection,
        "estimated_minutes": state.estimated_minutes,
        "planned_start": state.planned_start,
        "pinned": state.pinned,
    }


def _item_task(item: PlannerItem) -> dict[str, Any]:
    return {
        "item_id": item.id,
        "title": item.title,
        "source": item.source,
        "source_label": item.get_source_display(),
        "source_url": item.source_url or "",
        "created_at": item.source_created_at or item.created_at,
    }


def _source_key(item: PlannerItem) -> tuple[str, str] | None:
    if not item.source or not item.source_entity_id:
        return None
    return item.source, item.source_entity_id


def _planned_day(value) -> date | None:
    if not value:
        return None
    if timezone.is_aware(value):
        return timezone.localtime(value).date()
    return value.date()
