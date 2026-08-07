from __future__ import annotations

from collections import Counter
from typing import Any

from django.db.models import Q
from django.utils import timezone

from planner.models import PlannerItem, PlannerItemState, PlannerPlan
from workspaces.models import Workspace

MAX_SNAPSHOT_TASKS = 120
MAX_DESCRIPTION_LENGTH = 600


def build_workspace_snapshot(*, workspace: Workspace, user) -> dict[str, Any]:
    items = list(
        PlannerItem.objects.for_workspace(workspace)
        .filter(Q(user=user) | Q(user__isnull=True), is_active=True)
        .select_related("connector_account")
        .order_by("-source_created_at", "-created_at", "-id")[:MAX_SNAPSHOT_TASKS]
    )
    plan = (
        PlannerPlan.objects.filter(workspace=workspace, user=user)
        .order_by("id")
        .first()
    )
    state_by_item: dict[int, PlannerItemState] = {}
    if plan and items:
        state_by_item = {
            state.item_id: state
            for state in PlannerItemState.objects.filter(
                plan=plan,
                item_id__in=[item.id for item in items],
            )
        }

    status_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()
    tasks = []
    for item in items:
        state = state_by_item.get(item.id)
        planner_status = (
            state.planner_status if state else PlannerItemState.PLANNER_STATUS_INBOX
        )
        status_counts[planner_status] += 1
        source_counts[item.source or "unknown"] += 1
        tasks.append(
            {
                "id": item.id,
                "title": item.title,
                "description": (item.description or "")[:MAX_DESCRIPTION_LENGTH],
                "provider": item.source or "unknown",
                "connector": item.connector_account.display_name
                if item.connector_account
                else "",
                "planner_status": planner_status,
                "source_status": item.source_status or "",
                "completed_at_source": item.external_completed,
                "pinned": bool(state and state.pinned),
                "created_at": _iso(item.source_created_at or item.created_at),
                "last_synced_at": _iso(item.last_synced_at),
            }
        )

    return {
        "generated_at": timezone.now().isoformat(),
        "workspace_id": workspace.id,
        "task_count_in_snapshot": len(tasks),
        "snapshot_limit": MAX_SNAPSHOT_TASKS,
        "status_counts": dict(sorted(status_counts.items())),
        "provider_counts": dict(sorted(source_counts.items())),
        "tasks": tasks,
    }


def _iso(value) -> str:
    return value.isoformat() if value else ""
