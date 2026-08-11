from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from django.conf import settings
from django.db.models import Q
from django.utils import timezone

from ingestion.services.cache import cache_get, cache_set, workspace_cache_key
from planner.models import PlannerItem, PlannerItemState, PlannerPlan
from planner.services.cache import planner_local_cache_token
from workspaces.models import Workspace

ACTIVITY_PULSE_CACHE_NAMESPACE = "activity-pulse-v1"
STATUS_ORDER = ["inbox", "backlog", "doing", "done"]
STATUS_LABELS = {
    "inbox": "Inbox",
    "backlog": "To do",
    "doing": "In progress",
    "done": "Done",
}
STALE_AFTER_DAYS = 14


def build_activity_snapshot(
    *,
    workspace: Workspace,
    user,
    cache_version: int | None = None,
) -> dict[str, Any]:
    plan = (
        PlannerPlan.objects.filter(workspace=workspace, user=user)
        .order_by("id")
        .first()
    )
    local_token = planner_local_cache_token(
        workspace=workspace,
        user=user,
        plan=plan,
    )
    cache_key = workspace_cache_key(
        workspace,
        ACTIVITY_PULSE_CACHE_NAMESPACE,
        user.id,
        plan.id if plan else "none",
        local_token,
        cache_version=cache_version,
    )
    cached = cache_get(cache_key)
    if isinstance(cached, dict):
        return cached

    snapshot = _build_activity_snapshot(
        workspace=workspace,
        user=user,
        plan=plan,
    )
    cache_set(cache_key, snapshot, timeout=settings.CACHE_DEFAULT_TIMEOUT_SECONDS)
    return snapshot


def _build_activity_snapshot(
    *,
    workspace: Workspace,
    user,
    plan: PlannerPlan | None,
) -> dict[str, Any]:
    items = list(
        PlannerItem.objects.for_workspace(workspace)
        .filter(Q(user=user) | Q(user__isnull=True), is_active=True)
        .select_related("connector_account")
        .order_by("-source_created_at", "-created_at", "-id")
    )
    states = {}
    if plan and items:
        states = {
            state.item_id: state
            for state in PlannerItemState.objects.filter(
                plan=plan,
                item_id__in=[item.id for item in items],
            )
        }

    now = timezone.now()
    status_counts: Counter[str] = Counter()
    provider_counts: dict[str, Counter[str]] = defaultdict(Counter)
    age_counts = Counter({"today": 0, "week": 0, "month": 0, "older": 0})
    attention = []
    external_completed = 0

    for item in items:
        state = states.get(item.id)
        status = (
            state.planner_status if state else PlannerItemState.PLANNER_STATUS_INBOX
        )
        source = item.source or "unknown"
        created_at = item.source_created_at or item.created_at
        age_days = max((now - created_at).days, 0) if created_at else 0
        status_counts[status] += 1
        provider_counts[source][status] += 1
        provider_counts[source]["total"] += 1
        external_completed += int(item.external_completed)

        if age_days <= 1:
            age_counts["today"] += 1
        elif age_days <= 7:
            age_counts["week"] += 1
        elif age_days <= 30:
            age_counts["month"] += 1
        else:
            age_counts["older"] += 1

        if (
            status != PlannerItemState.PLANNER_STATUS_DONE
            and age_days >= STALE_AFTER_DAYS
        ):
            attention.append(
                {
                    "title": item.title,
                    "source": source,
                    "status": status,
                    "status_label": STATUS_LABELS.get(status, status.title()),
                    "age_days": age_days,
                    "source_url": item.source_url or "",
                }
            )

    total = len(items)
    status_rows = [
        {
            "status": status,
            "label": STATUS_LABELS[status],
            "count": status_counts[status],
            "percent": _percent(status_counts[status], total),
        }
        for status in STATUS_ORDER
    ]
    providers = []
    for source, counts in sorted(
        provider_counts.items(), key=lambda pair: (-pair[1]["total"], pair[0])
    ):
        providers.append(
            {
                "source": source,
                "label": source.replace("_", " ").title(),
                "total": counts["total"],
                "inbox": counts["inbox"],
                "backlog": counts["backlog"],
                "doing": counts["doing"],
                "done": counts["done"],
                "percent": _percent(counts["total"], total),
            }
        )
    age_rows = [
        {
            "key": "today",
            "label": "0-1 days",
            "count": age_counts["today"],
            "percent": _percent(age_counts["today"], total),
        },
        {
            "key": "week",
            "label": "2-7 days",
            "count": age_counts["week"],
            "percent": _percent(age_counts["week"], total),
        },
        {
            "key": "month",
            "label": "8-30 days",
            "count": age_counts["month"],
            "percent": _percent(age_counts["month"], total),
        },
        {
            "key": "older",
            "label": "31+ days",
            "count": age_counts["older"],
            "percent": _percent(age_counts["older"], total),
        },
    ]
    attention.sort(key=lambda row: (-row["age_days"], row["title"].lower()))
    return {
        "total": total,
        "status_counts": dict(status_counts),
        "status_rows": status_rows,
        "providers": providers,
        "age_rows": age_rows,
        "stale_count": len(attention),
        "external_completed": external_completed,
        "completion_percent": _percent(status_counts["done"], total),
        "attention": attention[:12],
        "generated_at": now,
    }


def _percent(value: int, total: int) -> int:
    return round((value / total) * 100) if total else 0
