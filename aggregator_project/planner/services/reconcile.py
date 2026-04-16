from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from connectors.models import ConnectorAccount
from events.models import Event
from planner.models import PlannerItem, PlannerItemState, PlannerPlan


TASK_EVENT_TYPES = {"task_state", "task_completed"}


@dataclass
class PlannerReconcileResult:
    item: PlannerItem | None
    created: bool


def _should_create_item(event: Event) -> bool:
    return bool(getattr(settings, "PLANNER_AUTO_CREATE", True))


def _should_auto_complete() -> bool:
    return bool(getattr(settings, "PLANNER_AUTO_COMPLETE", False))


def reconcile_from_event(event: Event) -> PlannerReconcileResult:
    if event.event_type not in TASK_EVENT_TYPES:
        return PlannerReconcileResult(item=None, created=False)

    now = timezone.now()
    connector_account = event.connector_account
    defaults = {
        "workspace": event.workspace,
        "user": None,
        "source": event.source,
        "source_entity_id": event.source_entity_id,
        "title": event.title or event.source_entity_id,
        "description": event.description,
        "source_url": None,
        "source_status": event.external_status or event.event_type,
        "source_updated_at": event.start_time or event.created_at,
        "last_synced_at": now,
        "external_completed": event.event_type == "task_completed",
        "is_active": True,
    }

    item = (
        PlannerItem.objects
        .for_workspace(event.workspace)
        .filter(
            connector_account=connector_account,
            source=event.source,
            source_entity_id=event.source_entity_id,
        )
        .first()
    )

    if not item:
        if not _should_create_item(event):
            return PlannerReconcileResult(item=None, created=False)
        item = PlannerItem(**defaults, connector_account=connector_account)
        item.save()
        return PlannerReconcileResult(item=item, created=True)

    updated_fields: list[str] = []
    for field, value in defaults.items():
        current = getattr(item, field)
        if value is not None and current != value:
            setattr(item, field, value)
            updated_fields.append(field)
    if updated_fields:
        item.save(update_fields=updated_fields + ["updated_at"])

    _mark_matching_pending_writebacks(item)

    if defaults["external_completed"] and _should_auto_complete():
        _auto_complete_item(item)

    return PlannerReconcileResult(item=item, created=False)


def _mark_matching_pending_writebacks(item: PlannerItem) -> None:
    from planner.services.writeback import mark_matching_pending_intents_synced

    mark_matching_pending_intents_synced(item)


def _auto_complete_item(item: PlannerItem) -> None:
    plan = (
        PlannerPlan.objects
        .for_workspace(item.workspace)
        .filter(user=item.user)
        .order_by("created_at")
        .first()
    )
    if not plan:
        return
    state = PlannerItemState.objects.filter(plan=plan, item=item).first()
    if not state:
        return
    if state.planned_status != PlannerItemState.STATUS_DONE:
        state.planned_status = PlannerItemState.STATUS_DONE
        state.last_planned_at = timezone.now()
        state.save(update_fields=["planned_status", "last_planned_at"])


def ensure_item_state(
    plan: PlannerPlan,
    item: PlannerItem,
    planner_status: str | None = None,
) -> PlannerItemState:
    state = PlannerItemState.objects.filter(plan=plan, item=item).first()
    if state:
        return state
    if planner_status == PlannerItemState.PLANNER_STATUS_INBOX:
        first_order = (
            PlannerItemState.objects
            .filter(plan=plan, planner_status=PlannerItemState.PLANNER_STATUS_INBOX)
            .order_by("planned_order")
            .values_list("planned_order", flat=True)
            .first()
        )
        next_order = (first_order or 0) - 1
    else:
        last_order = (
            PlannerItemState.objects
            .filter(plan=plan)
            .order_by("-planned_order")
            .values_list("planned_order", flat=True)
            .first()
        )
        next_order = (last_order or 0) + 1
    return PlannerItemState.objects.create(
        plan=plan,
        item=item,
        planner_status=planner_status or PlannerItemState.PLANNER_STATUS_INBOX,
        planned_order=next_order,
        last_planned_at=timezone.now(),
    )


def add_items_from_events(
    *,
    workspace,
    user,
    events: Iterable[Event],
    plan: PlannerPlan,
) -> int:
    created_count = 0
    with transaction.atomic():
        for event in events:
            item, created = _get_or_create_item_from_event(event, workspace, user)
            if not item:
                continue
            ensure_item_state(plan, item, planner_status=PlannerItemState.PLANNER_STATUS_INBOX)
            if created:
                created_count += 1
    return created_count


def _get_or_create_item_from_event(
    event: Event,
    workspace,
    user,
) -> tuple[PlannerItem | None, bool]:
    external_completed = _is_completed_event(event)
    if external_completed:
        return None, False

    item = (
        PlannerItem.objects
        .for_workspace(workspace)
        .filter(
            connector_account=event.connector_account,
            source=event.source,
            source_entity_id=event.source_entity_id,
        )
        .first()
    )
    if item:
        if item.user is None:
            item.user = user
            item.save(update_fields=["user"])
        return item, False

    return (
        PlannerItem.objects.create(
            workspace=workspace,
            user=user,
            connector_account=event.connector_account,
            source=event.source,
            source_entity_id=event.source_entity_id,
            title=event.title or event.source_entity_id,
            description=event.description,
            source_url=None,
            source_status=event.external_status or event.event_type,
            source_updated_at=event.start_time or event.created_at,
            last_synced_at=timezone.now(),
            external_completed=external_completed,
            is_active=True,
        ),
        True,
    )


def _is_completed_event(event: Event) -> bool:
    if event.event_type == "task_completed":
        return True
    status = (event.external_status or "").lower()
    return status in {"completed", "done", "closed", "resolved"}
