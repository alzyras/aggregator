from __future__ import annotations

import hashlib

from django.db.models import Max, Q

from planner.models import PlannerItem, PlannerItemState, PlannerPlan


def planner_local_cache_token(*, workspace, user, plan: PlannerPlan | None) -> str:
    """Version derived Planner snapshots after local planning writes."""
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
