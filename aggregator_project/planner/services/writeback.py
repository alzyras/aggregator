from __future__ import annotations

import hashlib

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
from ingestion.services.jobs import ACTIVE_JOB_STATUSES, create_job
from planner.models import PlannerItem, PlannerItemState, PlannerStatusIntent

JOB_TYPE = "planner_status_writeback"
JOB_NAME = "planner_status_writeback"
DESCRIPTION_JOB_TYPE = "planner_description_writeback"
DESCRIPTION_JOB_NAME = "planner_description_writeback"


def queue_status_writeback(
    *,
    state: PlannerItemState,
    created_by=None,
) -> Job | None:
    item = state.item
    account = item.connector_account

    if account is None:
        intent = _create_intent(state=state, requested_status=state.planner_status)
        _mark_unsupported(state, "This task is not linked to a connector account.", intent=intent)
        return None
    if not _account_can_write(account):
        intent = _create_intent(state=state, requested_status=state.planner_status)
        _mark_unsupported(state, "Connector account is not connected.", intent=intent)
        return None

    spec = get_provider_spec(account.source)
    if not spec or spec.status_writer_factory is None:
        intent = _create_intent(state=state, requested_status=state.planner_status)
        _mark_unsupported(state, "This provider does not support planner status writeback.", intent=intent)
        return None

    idempotency_key = _intent_idempotency_key(state)
    existing_job = (
        Job.objects
        .filter(idempotency_key=idempotency_key, status__in=ACTIVE_JOB_STATUSES)
        .order_by("queued_at")
        .first()
    )
    if existing_job:
        _mark_existing_job_pending(state, existing_job)
        return existing_job

    intent = _create_intent(state=state, requested_status=state.planner_status)
    with transaction.atomic():
        job = create_job(
            workspace=item.workspace,
            connector_account=account,
            job_type=JOB_TYPE,
            job_name=JOB_NAME,
            input_params={
                "planner_item_state_id": state.id,
                "planner_item_id": item.id,
                "planner_status": state.planner_status,
                "planner_status_intent_id": intent.id,
            },
            created_by=created_by,
            idempotency_key=idempotency_key,
        )
        if str(job.input_params.get("planner_status_intent_id")) != str(intent.id):
            _mark_intent_stale(intent, "Planner writeback job was deduplicated into an existing active job.")
            _mark_existing_job_pending(state, job)
            return job
        intent.job = job
        intent.save(update_fields=["job"])
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
    return job


def retry_failed_status_writeback(
    *,
    state: PlannerItemState,
    created_by=None,
) -> Job | None:
    intent = (
        PlannerStatusIntent.objects
        .filter(state=state, status=PlannerStatusIntent.STATUS_FAILED)
        .order_by("-requested_at")
        .first()
    )
    if intent:
        state.planner_status = intent.requested_planner_status
        state.planned_status = _planned_status_from_planner_status(intent.requested_planner_status)
        state.last_planned_at = timezone.now()
        state.save(update_fields=["planner_status", "planned_status", "last_planned_at"])
    return queue_status_writeback(state=state, created_by=created_by)


def queue_description_writeback(
    *,
    item: PlannerItem,
    created_by=None,
) -> Job | None:
    account = item.connector_account
    description = item.description or ""

    if account is None:
        _mark_description_unsupported(item, "This task is not linked to a connector account.")
        return None
    if not _account_can_write(account):
        _mark_description_unsupported(item, "Connector account is not connected.")
        return None

    spec = get_provider_spec(account.source)
    if not spec or spec.description_writer_factory is None:
        _mark_description_unsupported(item, "This provider does not support planner description writeback.")
        return None

    idempotency_key = _description_idempotency_key(item, description)
    existing_job = (
        Job.objects
        .filter(idempotency_key=idempotency_key, status__in=ACTIVE_JOB_STATUSES)
        .order_by("queued_at")
        .first()
    )
    if existing_job:
        _mark_existing_description_job_pending(item, existing_job, description)
        return existing_job

    with transaction.atomic():
        job = create_job(
            workspace=item.workspace,
            connector_account=account,
            job_type=DESCRIPTION_JOB_TYPE,
            job_name=DESCRIPTION_JOB_NAME,
            input_params={
                "planner_item_id": item.id,
                "description": description,
            },
            created_by=created_by,
            idempotency_key=idempotency_key,
        )
        _mark_existing_description_job_pending(item, job, description)
    return job


def retry_failed_description_writeback(
    *,
    item: PlannerItem,
    created_by=None,
) -> Job | None:
    return queue_description_writeback(item=item, created_by=created_by)


def _mark_existing_job_pending(state: PlannerItemState, job: Job) -> None:
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


def _mark_existing_description_job_pending(item: PlannerItem, job: Job, description: str) -> None:
    item.description_external_requested = description
    item.description_writeback_status = PlannerItem.DESCRIPTION_WRITEBACK_STATUS_PENDING
    item.last_description_writeback_job_id = job.id
    item.last_description_writeback_error = ""
    item.save(
        update_fields=[
            "description_external_requested",
            "description_writeback_status",
            "last_description_writeback_job_id",
            "last_description_writeback_error",
        ]
    )


def execute_status_writeback_job(job: Job) -> dict:
    state_id = job.input_params.get("planner_item_state_id")
    item_id = job.input_params.get("planner_item_id")
    requested_status = job.input_params.get("planner_status")
    intent_id = job.input_params.get("planner_status_intent_id")
    if not state_id or not item_id or not requested_status:
        raise ValueError("Planner writeback job is missing required input params.")

    state = (
        PlannerItemState.objects
        .select_related("item", "item__connector_account", "plan")
        .get(id=state_id, item_id=item_id)
    )
    intent = _get_intent(intent_id=intent_id, job=job)
    item = state.item
    account = item.connector_account
    if item.workspace_id != job.workspace_id or state.plan.workspace_id != job.workspace_id:
        raise ValueError("Planner item does not belong to job workspace.")
    if intent and intent.workspace_id != job.workspace_id:
        raise ValueError("Planner writeback intent does not belong to job workspace.")
    if account is None:
        return _mark_unsupported(state, "This task is not linked to a connector account.", intent=intent)
    if account.id != job.connector_account_id:
        raise ValueError("Planner item connector does not match job connector.")
    if state.external_status_requested != requested_status or state.planner_status != requested_status:
        _mark_intent_stale(intent, "Planner status changed before this writeback ran.")
        return {"status": "stale", "ignored": True}
    if not _account_can_write(account):
        return _mark_unsupported(state, "Connector account is not connected.", intent=intent)

    spec = get_provider_spec(account.source)
    if not spec or spec.status_writer_factory is None:
        return _mark_unsupported(state, "This provider does not support planner status writeback.", intent=intent)

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
    if intent:
        intent.status = PlannerStatusIntent.STATUS_PENDING
        intent.attempts = job.attempt_count + 1
        intent.last_attempted_at = now
        intent.last_error = ""
        intent.save(update_fields=["status", "attempts", "last_attempted_at", "last_error"])

    writer = spec.status_writer_factory(account)
    result = writer.apply_planner_status(
        source_entity_id=item.source_entity_id,
        planner_status=requested_status,
        item=item,
        source_entity_type=_latest_source_entity_type(item),
    )
    return _apply_result(state, result, intent=intent)


def execute_description_writeback_job(job: Job) -> dict:
    item_id = job.input_params.get("planner_item_id")
    requested_description = job.input_params.get("description")
    if not item_id or requested_description is None:
        raise ValueError("Planner description writeback job is missing required input params.")
    requested_description = str(requested_description)

    item = (
        PlannerItem.objects
        .select_related("connector_account")
        .get(id=item_id)
    )
    account = item.connector_account
    if item.workspace_id != job.workspace_id:
        raise ValueError("Planner item does not belong to job workspace.")
    if account is None:
        return _mark_description_unsupported(item, "This task is not linked to a connector account.")
    if account.id != job.connector_account_id:
        raise ValueError("Planner item connector does not match job connector.")
    if (item.description or "") != requested_description:
        return {"status": "stale", "ignored": True}
    if item.description_external_requested != requested_description:
        return {"status": "stale", "ignored": True}
    if not _account_can_write(account):
        return _mark_description_unsupported(item, "Connector account is not connected.")

    spec = get_provider_spec(account.source)
    if not spec or spec.description_writer_factory is None:
        return _mark_description_unsupported(item, "This provider does not support planner description writeback.")

    now = timezone.now()
    item.description_writeback_status = PlannerItem.DESCRIPTION_WRITEBACK_STATUS_PENDING
    item.last_description_writeback_attempted_at = now
    item.last_description_writeback_error = ""
    item.save(
        update_fields=[
            "description_writeback_status",
            "last_description_writeback_attempted_at",
            "last_description_writeback_error",
        ]
    )

    writer = spec.description_writer_factory(account)
    result = writer.update_description(
        source_entity_id=item.source_entity_id,
        description=requested_description,
        item=item,
        source_entity_type=_latest_source_entity_type(item),
    )
    return _apply_description_result(item, result, requested_description=requested_description)


def mark_status_writeback_job_failed(job: Job, message: str) -> None:
    state_id = job.input_params.get("planner_item_state_id")
    requested_status = job.input_params.get("planner_status")
    if not state_id:
        return
    state = (
        PlannerItemState.objects
        .select_related("item")
        .filter(id=state_id)
        .first()
    )
    if not state:
        return
    if requested_status and state.external_status_requested != requested_status:
        return
    intent = _get_intent(intent_id=job.input_params.get("planner_status_intent_id"), job=job)
    provider_status = _planner_status_from_source(state.item)
    attempted_label = _planner_status_label(str(requested_status or state.external_status_requested or ""))
    provider_label = _planner_status_label(provider_status)
    state.planner_status = provider_status
    state.planned_status = _planned_status_from_planner_status(provider_status)
    state.writeback_status = PlannerItemState.WRITEBACK_STATUS_FAILED
    state.last_writeback_error = (
        f"Tried to set {attempted_label}, but the source stayed {provider_label}: {message}"
    )
    state.last_writeback_attempted_at = timezone.now()
    state.save(
        update_fields=[
            "planner_status",
            "planned_status",
            "writeback_status",
            "last_writeback_error",
            "last_writeback_attempted_at",
        ]
    )
    if intent:
        intent.status = PlannerStatusIntent.STATUS_FAILED
        intent.attempts = job.attempt_count
        intent.last_error = state.last_writeback_error
        intent.resolved_provider_status = state.item.source_status or ""
        intent.resolved_external_completed = state.item.external_completed
        intent.completed_at = timezone.now()
        intent.save(
            update_fields=[
                "status",
                "attempts",
                "last_error",
                "resolved_provider_status",
                "resolved_external_completed",
                "completed_at",
            ]
        )


def mark_description_writeback_job_failed(job: Job, message: str) -> None:
    item_id = job.input_params.get("planner_item_id")
    requested_description = job.input_params.get("description")
    if not item_id or requested_description is None:
        return
    item = PlannerItem.objects.filter(id=item_id).first()
    if not item:
        return
    requested_description = str(requested_description)
    if item.description_external_requested != requested_description:
        return
    if (item.description or "") != requested_description:
        return
    item.description_writeback_status = PlannerItem.DESCRIPTION_WRITEBACK_STATUS_FAILED
    item.last_description_writeback_error = message
    item.last_description_writeback_attempted_at = timezone.now()
    item.save(
        update_fields=[
            "description_writeback_status",
            "last_description_writeback_error",
            "last_description_writeback_attempted_at",
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
    intent = _get_intent(intent_id=job.input_params.get("planner_status_intent_id"), job=job)
    if intent:
        intent.status = PlannerStatusIntent.STATUS_PENDING
        intent.attempts = job.attempt_count
        intent.last_attempted_at = timezone.now()
        intent.last_error = ""
        intent.save(update_fields=["status", "attempts", "last_attempted_at", "last_error"])


def mark_description_writeback_job_retrying(job: Job, message: str) -> None:
    item_id = job.input_params.get("planner_item_id")
    requested_description = job.input_params.get("description")
    if not item_id or requested_description is None:
        return
    item = PlannerItem.objects.filter(id=item_id).first()
    if not item:
        return
    requested_description = str(requested_description)
    if item.description_external_requested != requested_description:
        return
    if (item.description or "") != requested_description:
        return
    item.description_writeback_status = PlannerItem.DESCRIPTION_WRITEBACK_STATUS_PENDING
    item.last_description_writeback_error = ""
    item.last_description_writeback_attempted_at = timezone.now()
    item.save(
        update_fields=[
            "description_writeback_status",
            "last_description_writeback_error",
            "last_description_writeback_attempted_at",
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


def mark_matching_pending_intents_synced(item: PlannerItem) -> None:
    provider_status = _planner_status_from_source(item)
    states = PlannerItemState.objects.filter(item=item, writeback_status=PlannerItemState.WRITEBACK_STATUS_PENDING)
    for state in states:
        if state.external_status_requested != provider_status:
            continue
        now = timezone.now()
        state.writeback_status = PlannerItemState.WRITEBACK_STATUS_SYNCED
        state.last_writeback_error = ""
        state.last_writeback_succeeded_at = now
        state.save(update_fields=["writeback_status", "last_writeback_error", "last_writeback_succeeded_at"])
        PlannerStatusIntent.objects.filter(
            item=item,
            state=state,
            requested_planner_status=provider_status,
            status=PlannerStatusIntent.STATUS_PENDING,
        ).update(
            status=PlannerStatusIntent.STATUS_SYNCED,
            resolved_provider_status=item.source_status or "",
            resolved_external_completed=item.external_completed,
            completed_at=now,
            last_error="",
        )


def planned_status_for_planner_status(planner_status: str) -> str:
    return _planned_status_from_planner_status(planner_status)


def writeback_payload(state: PlannerItemState, job: Job | None = None) -> dict:
    message = state.last_writeback_error
    if state.writeback_status == PlannerItemState.WRITEBACK_STATUS_PENDING and not message:
        message = "Saving to source..."
    elif state.writeback_status == PlannerItemState.WRITEBACK_STATUS_SYNCED:
        message = ""
    return {
        "writeback_status": state.writeback_status,
        "writeback_message": message,
        "job_id": str(job.id) if job else str(state.last_writeback_job_id or ""),
    }


def description_writeback_payload(item: PlannerItem, job: Job | None = None) -> dict:
    message = item.last_description_writeback_error
    if item.description_writeback_status == PlannerItem.DESCRIPTION_WRITEBACK_STATUS_PENDING and not message:
        message = "Saving description to source..."
    elif item.description_writeback_status == PlannerItem.DESCRIPTION_WRITEBACK_STATUS_SYNCED:
        message = ""
    return {
        "description_writeback_status": item.description_writeback_status,
        "description_writeback_message": message,
        "description_job_id": str(job.id) if job else str(item.last_description_writeback_job_id or ""),
    }


def _apply_result(state: PlannerItemState, result, *, intent: PlannerStatusIntent | None = None) -> dict:
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

    intent_status = None
    if result.status in {STATUS_WRITEBACK_SUCCESS, STATUS_WRITEBACK_NOOP}:
        state.writeback_status = PlannerItemState.WRITEBACK_STATUS_SYNCED
        state.last_writeback_error = ""
        state.last_writeback_succeeded_at = now
        intent_status = PlannerStatusIntent.STATUS_SYNCED
    elif result.status == STATUS_WRITEBACK_UNSUPPORTED:
        state.writeback_status = PlannerItemState.WRITEBACK_STATUS_UNSUPPORTED
        state.last_writeback_error = result.message
        intent_status = PlannerStatusIntent.STATUS_UNSUPPORTED
    elif result.status == STATUS_WRITEBACK_FAILED:
        state.writeback_status = PlannerItemState.WRITEBACK_STATUS_FAILED
        state.last_writeback_error = result.message
        intent_status = PlannerStatusIntent.STATUS_FAILED
    else:
        state.writeback_status = PlannerItemState.WRITEBACK_STATUS_FAILED
        state.last_writeback_error = f"Unknown writeback result: {result.status}"
        intent_status = PlannerStatusIntent.STATUS_FAILED

    state.save(
        update_fields=[
            "writeback_status",
            "last_writeback_error",
            "last_writeback_succeeded_at",
        ]
    )
    if intent:
        intent.status = intent_status
        intent.last_error = state.last_writeback_error
        intent.resolved_provider_status = item.source_status or ""
        intent.resolved_external_completed = item.external_completed
        intent.completed_at = now
        intent.save(
            update_fields=[
                "status",
                "last_error",
                "resolved_provider_status",
                "resolved_external_completed",
                "completed_at",
            ]
        )
    return {
        "status": result.status,
        "source_status": result.source_status,
        "external_completed": result.external_completed,
        "message": result.message,
    }


def _apply_description_result(item: PlannerItem, result, *, requested_description: str) -> dict:
    now = timezone.now()
    update_item_fields: list[str] = []
    if result.description is not None and item.description == requested_description:
        if item.description != result.description:
            item.description = result.description
            update_item_fields.append("description")
    if result.status in {STATUS_WRITEBACK_SUCCESS, STATUS_WRITEBACK_NOOP}:
        item.description_writeback_status = PlannerItem.DESCRIPTION_WRITEBACK_STATUS_SYNCED
        item.last_description_writeback_error = ""
        item.last_description_writeback_succeeded_at = now
        item.last_synced_at = now
        update_item_fields.extend([
            "description_writeback_status",
            "last_description_writeback_error",
            "last_description_writeback_succeeded_at",
            "last_synced_at",
        ])
    elif result.status == STATUS_WRITEBACK_UNSUPPORTED:
        item.description_writeback_status = PlannerItem.DESCRIPTION_WRITEBACK_STATUS_UNSUPPORTED
        item.last_description_writeback_error = result.message
        update_item_fields.extend(["description_writeback_status", "last_description_writeback_error"])
    elif result.status == STATUS_WRITEBACK_FAILED:
        item.description_writeback_status = PlannerItem.DESCRIPTION_WRITEBACK_STATUS_FAILED
        item.last_description_writeback_error = result.message
        update_item_fields.extend(["description_writeback_status", "last_description_writeback_error"])
    else:
        item.description_writeback_status = PlannerItem.DESCRIPTION_WRITEBACK_STATUS_FAILED
        item.last_description_writeback_error = f"Unknown writeback result: {result.status}"
        update_item_fields.extend(["description_writeback_status", "last_description_writeback_error"])

    if update_item_fields:
        item.save(update_fields=list(dict.fromkeys(update_item_fields + ["updated_at"])))
    return {
        "status": result.status,
        "description": result.description,
        "message": result.message,
    }


def _mark_unsupported(
    state: PlannerItemState,
    message: str,
    *,
    intent: PlannerStatusIntent | None = None,
) -> dict:
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
    if intent:
        intent.status = PlannerStatusIntent.STATUS_UNSUPPORTED
        intent.last_error = message
        intent.completed_at = timezone.now()
        intent.save(update_fields=["status", "last_error", "completed_at"])
    return {"status": STATUS_WRITEBACK_UNSUPPORTED, "message": message}


def _mark_description_unsupported(item: PlannerItem, message: str) -> dict:
    item.description_external_requested = item.description or ""
    item.description_writeback_status = PlannerItem.DESCRIPTION_WRITEBACK_STATUS_UNSUPPORTED
    item.last_description_writeback_error = message
    item.save(
        update_fields=[
            "description_external_requested",
            "description_writeback_status",
            "last_description_writeback_error",
        ]
    )
    return {"status": STATUS_WRITEBACK_UNSUPPORTED, "message": message}


def _mark_intent_stale(intent: PlannerStatusIntent | None, message: str) -> None:
    if not intent:
        return
    intent.status = PlannerStatusIntent.STATUS_STALE
    intent.last_error = message
    intent.completed_at = timezone.now()
    intent.save(update_fields=["status", "last_error", "completed_at"])


def _create_intent(*, state: PlannerItemState, requested_status: str) -> PlannerStatusIntent:
    item = state.item
    return PlannerStatusIntent.objects.create(
        workspace=item.workspace,
        plan=state.plan,
        item=item,
        state=state,
        connector_account=item.connector_account,
        requested_planner_status=requested_status,
        provider_status_at_request=item.source_status or "",
        provider_completed_at_request=item.external_completed,
    )


def _get_intent(*, intent_id, job: Job) -> PlannerStatusIntent | None:
    if intent_id:
        return PlannerStatusIntent.objects.filter(id=intent_id).first()
    return job.planner_status_intents.order_by("-requested_at").first()


def _account_can_write(account: ConnectorAccount) -> bool:
    return (
        account.is_active
        and account.revoked_at is None
        and account.status == ConnectorAccount.STATUS_CONNECTED
    )


def _intent_idempotency_key(state: PlannerItemState) -> str:
    return f"{JOB_TYPE}:{state.plan_id}:{state.item_id}:{state.planner_status}"


def _description_idempotency_key(item: PlannerItem, description: str) -> str:
    digest = hashlib.sha256(description.encode("utf-8")).hexdigest()
    return f"{DESCRIPTION_JOB_TYPE}:{item.id}:{digest}"


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


def _planner_status_label(planner_status: str) -> str:
    labels = {
        PlannerItemState.PLANNER_STATUS_INBOX: "Inbox",
        PlannerItemState.PLANNER_STATUS_BACKLOG: "To do",
        PlannerItemState.PLANNER_STATUS_DOING: "In progress",
        PlannerItemState.PLANNER_STATUS_DONE: "Done",
    }
    return labels.get(planner_status, planner_status or "unknown")
