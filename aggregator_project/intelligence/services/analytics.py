from __future__ import annotations

from collections import Counter, defaultdict
from datetime import timedelta

from django.db.models import Count, Q
from django.utils import timezone

from intelligence.models import TaskAnalysis, UnifiedTag
from planner.models import PlannerItem, PlannerItemState, PlannerPlan

STALE_AFTER_DAYS = 14
WEEKS = 8


def build_insights_snapshot(*, workspace, user) -> dict:
    items = list(
        PlannerItem.objects.for_workspace(workspace)
        .filter(Q(user=user) | Q(user__isnull=True), is_active=True)
        .select_related("connector_account", "intelligence_analysis")
        .prefetch_related("tag_assignments__tag")
        .order_by("-source_created_at", "-created_at", "-id")
    )
    plan = (
        PlannerPlan.objects.for_workspace(workspace).filter(user=user).order_by("id").first()
    )
    states = {}
    if plan and items:
        states = {
            state.item_id: state
            for state in PlannerItemState.objects.filter(
                plan=plan,
                item_id__in=[item.id for item in items],
            ).only("item_id", "planner_status", "last_planned_at")
        }

    now = timezone.now()
    tag_stats: dict[int, Counter] = defaultdict(Counter)
    task_type_counts: Counter[str] = Counter()
    energy_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    weekly = _weekly_buckets(now)
    tagged_items = 0
    enriched_items = 0
    stale_items = 0
    completed = 0

    for item in items:
        state = states.get(item.id)
        status = state.planner_status if state else PlannerItemState.PLANNER_STATUS_INBOX
        is_done = status == PlannerItemState.PLANNER_STATUS_DONE or item.external_completed
        created_at = item.source_created_at or item.created_at
        age_days = max((now - created_at).days, 0) if created_at else 0
        tag_assignments = list(item.tag_assignments.all())
        if tag_assignments:
            tagged_items += 1
        analysis = getattr(item, "intelligence_analysis", None)
        if analysis and analysis.status == TaskAnalysis.STATUS_READY:
            enriched_items += 1
        if analysis and analysis.task_type:
            task_type_counts[analysis.task_type] += 1
        if analysis and analysis.energy:
            energy_counts[analysis.energy] += 1
        source_counts[item.source] += 1
        status_counts[status] += 1
        completed += int(is_done)
        stale = not is_done and age_days >= STALE_AFTER_DAYS
        stale_items += int(stale)

        for assignment in tag_assignments:
            stats = tag_stats[assignment.tag_id]
            stats["total"] += 1
            stats["completed"] += int(is_done)
            stats["active"] += int(not is_done)
            stats["stale"] += int(stale)

        _add_weekly(weekly, created_at, "incoming")
        if is_done:
            done_at = state.last_planned_at if state and state.last_planned_at else item.source_updated_at
            _add_weekly(weekly, done_at, "completed")

    tag_map = {
        tag.id: tag
        for tag in UnifiedTag.objects.for_workspace(workspace).filter(id__in=tag_stats.keys())
    }
    tag_rows = []
    for tag_id, stats in tag_stats.items():
        total = stats["total"]
        completion_rate = _percent(stats["completed"], total)
        tag_rows.append(
            {
                "id": tag_id,
                "name": tag_map[tag_id].name,
                "kind": tag_map[tag_id].get_kind_display(),
                "color": tag_map[tag_id].color,
                "total": total,
                "completed": stats["completed"],
                "active": stats["active"],
                "stale": stats["stale"],
                "completion_rate": completion_rate,
            }
        )
    tag_rows.sort(key=lambda row: (-row["total"], row["name"].lower()))

    strengths = sorted(
        [row for row in tag_rows if row["total"] >= 2 and row["completion_rate"] >= 60],
        key=lambda row: (-row["completion_rate"], -row["completed"], row["name"].lower()),
    )[:5]
    growth = sorted(
        [row for row in tag_rows if row["active"] or row["stale"]],
        key=lambda row: (-row["stale"], row["completion_rate"], -row["active"], row["name"].lower()),
    )[:5]

    total = len(items)
    weekly_max = max(
        (max(bucket["incoming"], bucket["completed"]) for bucket in weekly),
        default=1,
    )
    weekly_max = max(weekly_max, 1)
    for bucket in weekly:
        bucket["incoming_percent"] = round((bucket["incoming"] / weekly_max) * 100)
        bucket["completed_percent"] = round((bucket["completed"] / weekly_max) * 100)
    return {
        "total": total,
        "tagged_items": tagged_items,
        "enriched_items": enriched_items,
        "tag_coverage": _percent(tagged_items, total),
        "ai_coverage": _percent(enriched_items, total),
        "completed": completed,
        "completion_rate": _percent(completed, total),
        "stale_items": stale_items,
        "status_counts": dict(status_counts),
        "source_rows": _counter_rows(source_counts, limit=6),
        "task_type_rows": _counter_rows(task_type_counts, limit=7),
        "energy_rows": _counter_rows(energy_counts, limit=3),
        "tag_rows": tag_rows[:18],
        "strengths": strengths,
        "growth": growth,
        "weekly": weekly,
        "weekly_max": weekly_max,
        "generated_at": now,
    }


def build_tag_catalog(*, workspace, user) -> list[dict]:
    tags = (
        UnifiedTag.objects.for_workspace(workspace)
        .annotate(
            task_count=Count(
                "task_assignments",
                filter=Q(task_assignments__item__user=user)
                | Q(task_assignments__item__user__isnull=True),
            )
        )
        .order_by("kind", "-task_count", "name")
    )
    return [
        {
            "id": tag.id,
            "name": tag.name,
            "kind": tag.get_kind_display(),
            "color": tag.color,
            "task_count": tag.task_count,
            "is_system": tag.is_system,
        }
        for tag in tags
    ]


def _counter_rows(counter: Counter[str], *, limit: int) -> list[dict]:
    total = sum(counter.values())
    return [
        {
            "label": key.replace("_", " ").title(),
            "count": count,
            "percent": _percent(count, total),
        }
        for key, count in counter.most_common(limit)
    ]


def _weekly_buckets(now) -> list[dict]:
    current_start = (now - timedelta(days=now.weekday())).date()
    buckets = []
    for offset in range(WEEKS - 1, -1, -1):
        start = current_start - timedelta(weeks=offset)
        buckets.append({"start": start, "label": start.strftime("%d %b"), "incoming": 0, "completed": 0})
    return buckets


def _add_weekly(buckets: list[dict], value, field: str) -> None:
    if not value:
        return
    date = value.date()
    for bucket in buckets:
        if bucket["start"] <= date < bucket["start"] + timedelta(days=7):
            bucket[field] += 1
            return


def _percent(value: int, total: int) -> int:
    return round((value / total) * 100) if total else 0
