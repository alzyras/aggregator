from __future__ import annotations

from connectors.models import ConnectorAccount
from django.db import transaction
from django.utils import timezone
from events.models import Event
from ingestion.models import Job
from ingestion.providers import (
    STATUS_WRITEBACK_FAILED,
    STATUS_WRITEBACK_NOOP,
    STATUS_WRITEBACK_SUCCESS,
    STATUS_WRITEBACK_UNSUPPORTED,
    get_provider_spec,
)
from ingestion.services.jobs import enqueue_job
from planner.models import PlannerItem, PlannerItemState

JOB_TYPE = "planner_status_writeback"
JOB_NAME = "planner_status_writeback"


def queue_status_writeback(
    *,
    state: PlannerItemState,
    created_by=None,
) -> Job | None:
    item = state.item
    account = item.connector_account
    if account is None:
        _mark_unsupported(state, "This task is not linked to a connector account.")
        return None
    if not _account_can_write(account):
        _mark_unsupported(state, "Connector account is not connected.")
        return None

    spec = get_provider_spec(account.source)
    if not spec or spec.status_writer_factory is None:
        _mark_unsupported(state, "This provider does not support planner status writeback.")
        return None

    with transaction.atomic():
        job = Job(
            workspace=item.workspace,
            connector_account=account,
            job_type=JOB_TYPE,
            job_name=JOB_NAME,
            input_params={
                "planner_item_state_id": state.id,
                "planner_item_id": item.id,
                "planner_status": state.planner_status,
            },
            created_by=created_by,
        )
        job.full_clean()
        job.save()
        state.external_status_requested = state.planner_status
        state.writeback_status = PlannerItemState.WRITEBACK_STATUS_PENDING
        state.last_writeback_job_id = job.id
        state.last_writeback_error = ""
        state.save(
            update_fields=[
                "external_status_requested",
                "writeback_status",
                "last_writeback_job_id",
                "last_writeback_error",
            ]
        )
        enqueue_job(job.id)
    return job


def execute_status_writeback_job(job: Job) -> dict:
    state_id = job.input_params.get("planner_item_state_id")
    item_id = job.input_params.get("planner_item_id")
    requested_status = job.input_params.get("planner_status")
    if not state_id or not item_id or not requested_status:
        raise ValueError("Planner writeback job is missing required input params.")

    state = (
        PlannerItemState.objects
        .select_related("item", "item__connector_account", "plan")
        .get(id=state_id, item_id=item_id)
    )
    item = state.item
    account = item.connector_account
    if item.workspace_id != job.workspace_id or state.plan.workspace_id != job.workspace_id:
        raise ValueError("Planner item does not belong to job workspace.")
    if account is None:
        return _mark_unsupported(state, "This task is not linked to a connector account.")
    if account.id != job.connector_account_id:
        raise ValueError("Planner item connector does not match job connector.")
    if state.external_status_requested != requested_status or state.planner_status != requested_status:
        return {"status": "stale", "ignored": True}
    if not _account_can_write(account):
        return _mark_unsupported(state, "Connector account is not connected.")

    spec = get_provider_spec(account.source)
    if not spec or spec.status_writer_factory is None:
        return _mark_unsupported(state, "This provider does not support planner status writeback.")

    now = timezone.now()
    state.writeback_status = PlannerItemState.WRITEBACK_STATUS_PENDING
    state.last_writeback_attempted_at = now
    state.last_writeback_error = ""
    state.save(
        update_fields=[
            "writeback_status",
            "last_writeback_attempted_at",
            "last_writeback_error",
        ]
    )

    writer = spec.status_writer_factory(account)
    result = writer.apply_planner_status(
        source_entity_id=item.source_entity_id,
        planner_status=requested_status,
        item=item,
        source_entity_type=_latest_source_entity_type(item),
    )
    return _apply_result(state, result)


def mark_status_writeback_job_failed(job: Job, message: str) -> None:
    state_id = job.input_params.get("planner_item_state_id")
    requested_status = job.input_params.get("planner_status")
    if not state_id:
        return
    state = PlannerItemState.objects.filter(id=state_id).first()
    if not state:
        return
    if requested_status and state.external_status_requested != requested_status:
        return
    state.writeback_status = PlannerItemState.WRITEBACK_STATUS_FAILED
    state.last_writeback_error = message
    state.last_writeback_attempted_at = timezone.now()
    state.save(
        update_fields=[
            "writeback_status",
            "last_writeback_error",
            "last_writeback_attempted_at",
        ]
    )


def mark_status_writeback_job_retrying(job: Job, message: str) -> None:
    state_id = job.input_params.get("planner_item_state_id")
    requested_status = job.input_params.get("planner_status")
    if not state_id:
        return
    state = PlannerItemState.objects.filter(id=state_id).first()
    if not state:
        return
    if requested_status and state.external_status_requested != requested_status:
        return
    state.writeback_status = PlannerItemState.WRITEBACK_STATUS_PENDING
    state.last_writeback_error = ""
    state.last_writeback_attempted_at = timezone.now()
    state.save(
        update_fields=[
            "writeback_status",
            "last_writeback_error",
            "last_writeback_attempted_at",
        ]
    )


def revert_state_to_source_status(state: PlannerItemState) -> None:
    state.planner_status = _planner_status_from_source(state.item)
    state.planned_status = _planned_status_from_planner_status(state.planner_status)
    state.external_status_requested = None
    state.writeback_status = PlannerItemState.WRITEBACK_STATUS_NONE
    state.last_writeback_error = ""
    state.last_planned_at = timezone.now()
    state.save(
        update_fields=[
            "planner_status",
            "planned_status",
            "external_status_requested",
            "writeback_status",
            "last_writeback_error",
            "last_planned_at",
        ]
    )


def planned_status_for_planner_status(planner_status: str) -> str:
    return _planned_status_from_planner_status(planner_status)


def writeback_payload(state: PlannerItemState, job: Job | None = None) -> dict:
    message = state.last_writeback_error
    if state.writeback_status == PlannerItemState.WRITEBACK_STATUS_PENDING and not message:
        message = "Saving to source..."
    elif state.writeback_status == PlannerItemState.WRITEBACK_STATUS_SYNCED and not message:
        message = "Saved to source."
    return {
        "writeback_status": state.writeback_status,
        "writeback_message": message,
        "job_id": str(job.id) if job else str(state.last_writeback_job_id or ""),
    }


def _apply_result(state: PlannerItemState, result) -> dict:
    item = state.item
    now = timezone.now()
    update_item_fields: list[str] = []
    if result.source_status is not None and item.source_status != result.source_status:
        item.source_status = result.source_status
        update_item_fields.append("source_status")
    if result.external_completed is not None and item.external_completed != result.external_completed:
        item.external_completed = result.external_completed
        update_item_fields.append("external_completed")
    if result.status in {STATUS_WRITEBACK_SUCCESS, STATUS_WRITEBACK_NOOP}:
        item.last_synced_at = now
        update_item_fields.append("last_synced_at")
    if update_item_fields:
        item.save(update_fields=update_item_fields + ["updated_at"])

    if result.status in {STATUS_WRITEBACK_SUCCESS, STATUS_WRITEBACK_NOOP}:
        state.writeback_status = PlannerItemState.WRITEBACK_STATUS_SYNCED
        state.last_writeback_error = ""
        state.last_writeback_succeeded_at = now
    elif result.status == STATUS_WRITEBACK_UNSUPPORTED:
        state.writeback_status = PlannerItemState.WRITEBACK_STATUS_UNSUPPORTED
        state.last_writeback_error = result.message
    elif result.status == STATUS_WRITEBACK_FAILED:
        state.writeback_status = PlannerItemState.WRITEBACK_STATUS_FAILED
        state.last_writeback_error = result.message
    else:
        state.writeback_status = PlannerItemState.WRITEBACK_STATUS_FAILED
        state.last_writeback_error = f"Unknown writeback result: {result.status}"

    state.save(
        update_fields=[
            "writeback_status",
            "last_writeback_error",
            "last_writeback_succeeded_at",
        ]
    )
    return {
        "status": result.status,
        "source_status": result.source_status,
        "external_completed": result.external_completed,
        "message": result.message,
    }


def _mark_unsupported(state: PlannerItemState, message: str) -> dict:
    state.external_status_requested = state.planner_status
    state.writeback_status = PlannerItemState.WRITEBACK_STATUS_UNSUPPORTED
    state.last_writeback_error = message
    state.save(
        update_fields=[
            "external_status_requested",
            "writeback_status",
            "last_writeback_error",
        ]
    )
    return {"status": STATUS_WRITEBACK_UNSUPPORTED, "message": message}


def _account_can_write(account: ConnectorAccount) -> bool:
    return (
        account.is_active
        and account.revoked_at is None
        and account.status == ConnectorAccount.STATUS_CONNECTED
    )


def _latest_source_entity_type(item: PlannerItem) -> str | None:
    return (
        Event.objects
        .for_workspace(item.workspace)
        .filter(
            connector_account=item.connector_account,
            source=item.source,
            source_entity_id=item.source_entity_id,
        )
        .order_by("-created_at")
        .values_list("source_entity_type", flat=True)
        .first()
    )


def _planned_status_from_planner_status(planner_status: str) -> str:
    if planner_status == PlannerItemState.PLANNER_STATUS_DONE:
        return PlannerItemState.STATUS_DONE
    if planner_status == PlannerItemState.PLANNER_STATUS_DOING:
        return PlannerItemState.STATUS_IN_PROGRESS
    return PlannerItemState.STATUS_PLANNED


def _planner_status_from_source(item: PlannerItem) -> str:
    if item.external_completed:
        return PlannerItemState.PLANNER_STATUS_DONE
    status = (item.source_status or "").lower().replace("-", "_")
    if status in {"completed", "done", "closed", "resolved"}:
        return PlannerItemState.PLANNER_STATUS_DONE
    if status in {"in_progress", "in progress", "doing"}:
        return PlannerItemState.PLANNER_STATUS_DOING
    return PlannerItemState.PLANNER_STATUS_BACKLOG
