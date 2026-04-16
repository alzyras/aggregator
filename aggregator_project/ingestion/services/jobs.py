from __future__ import annotations

import logging
import os
import socket
import traceback
import uuid

from connectors.models import ConnectorAccount
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from ingestion.models import Job, JobAttempt
from ingestion.services.sync import sync_connector_account

logger = logging.getLogger(__name__)

ACTIVE_JOB_STATUSES = {Job.STATUS_QUEUED, Job.STATUS_RUNNING}


class NonRetryableJobError(Exception):
    """Raised when retrying would not change the outcome."""


def create_job(
    *,
    workspace,
    job_type: str,
    job_name: str,
    connector_account: ConnectorAccount | None = None,
    input_params: dict | None = None,
    created_by=None,
    priority: int = 0,
    delay=None,
    max_attempts: int | None = None,
    idempotency_key: str = "",
) -> Job:
    now = timezone.now()
    next_run_at = now + delay if delay else now
    with transaction.atomic():
        if idempotency_key:
            existing = (
                Job.objects
                .select_for_update()
                .filter(
                    idempotency_key=idempotency_key,
                    status__in=ACTIVE_JOB_STATUSES,
                )
                .order_by("queued_at")
                .first()
            )
            if existing:
                if existing.status == Job.STATUS_QUEUED and existing.next_run_at and existing.next_run_at > next_run_at:
                    existing.next_run_at = next_run_at
                    existing.save(update_fields=["next_run_at"])
                logger.info(
                    "job_deduped",
                    extra={"job_id": str(existing.id), "idempotency_key": idempotency_key},
                )
                return existing

        job = Job(
            workspace=workspace,
            connector_account=connector_account,
            job_type=job_type,
            job_name=job_name,
            priority=priority,
            idempotency_key=idempotency_key,
            next_run_at=next_run_at,
            max_attempts=max_attempts or _default_max_attempts(job_type),
            input_params=input_params or {},
            created_by=created_by,
        )
        job.full_clean()
        job.save()
    logger.info("job_queued", extra={"job_id": str(job.id), "job_type": job.job_type})
    return job


def enqueue_job(job_id):
    now = timezone.now()
    Job.objects.filter(id=job_id, status=Job.STATUS_QUEUED, next_run_at__isnull=True).update(
        next_run_at=now
    )
    logger.info("job_queued", extra={"job_id": str(job_id)})
    return job_id


def run_job(job_id):
    recover_stale_jobs()
    if _is_concurrency_full():
        _defer_job(job_id)
        logger.info("job_deferred_concurrency", extra={"job_id": str(job_id)})
        return Job.objects.filter(id=job_id).first()

    claim = _claim_job(job_id)
    if not claim:
        return Job.objects.filter(id=job_id).first()
    job, attempt = claim

    try:
        output = execute_job(job)
        job.status = Job.STATUS_SUCCESS
        job.output_summary = output or {}
        job.error_message = ""
        job.error_traceback = ""
        attempt.status = JobAttempt.STATUS_SUCCESS
        attempt.output_summary = job.output_summary
        if job.job_type == "sync" and job.connector_account_id:
            ConnectorAccount.objects.filter(id=job.connector_account_id).update(
                last_sync_status=ConnectorAccount.SYNC_STATUS_SUCCESS
            )
        logger.info(
            "job_success",
            extra={"job_id": str(job.id), "workspace_id": job.workspace_id, "status": job.status},
        )
    except Exception as exc:  # noqa: BLE001
        _handle_job_exception(job=job, attempt=attempt, exc=exc)
    finally:
        now = timezone.now()
        job.finished_at = now
        job.locked_at = None
        job.locked_by = ""
        job.lease_expires_at = None
        job.save(
            update_fields=[
                "status",
                "output_summary",
                "error_message",
                "error_traceback",
                "finished_at",
                "attempt_count",
                "next_run_at",
                "locked_at",
                "locked_by",
                "lease_expires_at",
            ]
        )
        attempt.finished_at = now
        attempt.save(
            update_fields=[
                "status",
                "output_summary",
                "error_message",
                "finished_at",
            ]
        )
    return job


def _claim_job(job_id) -> tuple[Job, JobAttempt] | None:
    with transaction.atomic():
        try:
            job = Job.objects.select_for_update(skip_locked=True).get(id=job_id)
        except ObjectDoesNotExist:
            logger.info("job_missing_or_locked", extra={"job_id": str(job_id)})
            return None
        now = timezone.now()
        if job.status != Job.STATUS_QUEUED:
            return None
        if job.next_run_at and job.next_run_at > now:
            return None
        owner = _lock_owner()
        job.status = Job.STATUS_RUNNING
        job.started_at = now
        job.finished_at = None
        job.locked_at = now
        job.locked_by = owner
        job.lease_expires_at = _lease_expires_at(now)
        if job.max_attempts is None:
            job.max_attempts = _default_max_attempts(job.job_type)
        job.save(
            update_fields=[
                "status",
                "started_at",
                "finished_at",
                "locked_at",
                "locked_by",
                "lease_expires_at",
                "max_attempts",
            ]
        )
        attempt_number = (
            JobAttempt.objects
            .filter(job=job)
            .order_by("-attempt_number")
            .values_list("attempt_number", flat=True)
            .first()
            or 0
        ) + 1
        attempt = JobAttempt.objects.create(
            job=job,
            attempt_number=attempt_number,
            worker_id=owner,
            started_at=now,
        )
        logger.info(
            "job_started",
            extra={
                "job_id": str(job.id),
                "workspace_id": job.workspace_id,
                "attempt_number": attempt.attempt_number,
            },
        )
        return job, attempt


def _handle_job_exception(*, job: Job, attempt: JobAttempt, exc: Exception) -> None:
    job.attempt_count += 1
    job.error_message = str(exc)
    job.error_traceback = traceback.format_exc()
    attempt.error_message = str(exc)
    retryable = _is_retryable_exception(exc)
    if job.job_type == "sync" and job.connector_account_id:
        ConnectorAccount.objects.filter(id=job.connector_account_id).update(
            last_sync_status=ConnectorAccount.SYNC_STATUS_FAILED,
            last_sync_at=timezone.now(),
        )

    if retryable and job.attempt_count < _max_attempts(job):
        job.status = Job.STATUS_QUEUED
        job.next_run_at = timezone.now() + _retry_delay(job.attempt_count)
        attempt.status = JobAttempt.STATUS_RETRYING
        if job.job_type == "planner_status_writeback":
            from planner.services.writeback import mark_status_writeback_job_retrying

            mark_status_writeback_job_retrying(job, str(exc))
        logger.info(
            "job_retry_scheduled",
            extra={
                "job_id": str(job.id),
                "workspace_id": job.workspace_id,
                "status": job.status,
                "attempt_count": job.attempt_count,
                "max_attempts": _max_attempts(job),
            },
        )
        return

    job.status = Job.STATUS_FAILED
    attempt.status = JobAttempt.STATUS_FAILED
    if job.job_type == "planner_status_writeback":
        from planner.services.writeback import mark_status_writeback_job_failed

        mark_status_writeback_job_failed(job, str(exc))
    logger.info(
        "job_failed",
        extra={
            "job_id": str(job.id),
            "workspace_id": job.workspace_id,
            "status": job.status,
            "retryable": retryable,
        },
    )


def execute_job(job: Job):
    if job.job_type == "sync":
        return _execute_sync_job(job)
    if job.job_type == "planner_status_writeback":
        from planner.services.writeback import execute_status_writeback_job

        return execute_status_writeback_job(job)
    raise NonRetryableJobError(f"Unsupported job type: {job.job_type}")


def _execute_sync_job(job: Job):
    since_raw = job.input_params.get("since")
    full_sync = bool(job.input_params.get("full_sync"))
    since = None if full_sync else parse_datetime(since_raw) if since_raw else None
    connector_account = job.connector_account
    if connector_account is None:
        raise NonRetryableJobError("Sync jobs require a connector account.")
    if connector_account.workspace_id != job.workspace_id:
        raise NonRetryableJobError("Connector account does not belong to workspace.")

    sync_kwargs = {
        "workspace": job.workspace,
        "connector_account": connector_account,
        "since": since,
    }
    if full_sync:
        sync_kwargs["full_sync"] = True
    stats = sync_connector_account(**sync_kwargs)
    return {"results": [stats]}


def _is_concurrency_full() -> bool:
    running_count = Job.objects.filter(status=Job.STATUS_RUNNING).filter(
        Q(lease_expires_at__gt=timezone.now())
        | Q(lease_expires_at__isnull=True, locked_at__gte=_stale_lock_cutoff())
        | Q(lease_expires_at__isnull=True, locked_at__isnull=True, started_at__gte=_stale_lock_cutoff())
    ).count()
    return running_count >= settings.JOB_MAX_CONCURRENCY


def _defer_job(job_id):
    now = timezone.now()
    Job.objects.filter(id=job_id, status=Job.STATUS_QUEUED).update(
        next_run_at=now + _retry_delay(1)
    )


def _retry_delay(attempt_count: int):
    delay_seconds = min(60 * max(attempt_count, 1), 300)
    return timezone.timedelta(seconds=delay_seconds)


def _max_attempts(job: Job | None = None) -> int:
    if job and job.max_attempts:
        return max(int(job.max_attempts), 1)
    if job:
        return _default_max_attempts(job.job_type)
    return max(int(getattr(settings, "JOB_MAX_ATTEMPTS", 3)), 1)


def _default_max_attempts(job_type: str) -> int:
    if job_type == "planner_status_writeback":
        retries = max(int(getattr(settings, "PLANNER_STATUS_WRITEBACK_MAX_RETRIES", 3)), 0)
        return retries + 1
    return max(int(getattr(settings, "JOB_MAX_ATTEMPTS", 3)), 1)


def _is_retryable_exception(exc: Exception) -> bool:
    return not isinstance(exc, (NonRetryableJobError, ValidationError, ValueError))


def _stale_lock_cutoff():
    timeout_seconds = max(int(getattr(settings, "JOB_STALE_RUNNING_SECONDS", 900)), 1)
    return timezone.now() - timezone.timedelta(seconds=timeout_seconds)


def _lease_expires_at(now=None):
    timeout_seconds = max(int(getattr(settings, "JOB_STALE_RUNNING_SECONDS", 900)), 1)
    return (now or timezone.now()) + timezone.timedelta(seconds=timeout_seconds)


def recover_stale_jobs() -> int:
    now = timezone.now()
    stale_ids = list(_stale_running_jobs(now).values_list("id", flat=True))
    recovered = 0

    for job_id in stale_ids:
        with transaction.atomic():
            try:
                job = Job.objects.select_for_update(skip_locked=True).get(id=job_id)
            except ObjectDoesNotExist:
                continue
            if job.status != Job.STATUS_RUNNING or not _job_is_stale(job, now):
                continue

            recovered += 1
            job.attempt_count += 1
            job.locked_at = None
            job.locked_by = ""
            job.lease_expires_at = None
            job.finished_at = now
            job.error_message = "Recovered stale running job."

            exhausted = job.attempt_count >= _max_attempts(job)
            if exhausted:
                job.status = Job.STATUS_FAILED
                job.next_run_at = now
                attempt_status = JobAttempt.STATUS_FAILED
                attempt_error = "Recovered stale running job after max attempts; marking failed."
                if job.job_type == "planner_status_writeback":
                    from planner.services.writeback import mark_status_writeback_job_failed

                    mark_status_writeback_job_failed(job, attempt_error)
            else:
                job.status = Job.STATUS_QUEUED
                job.next_run_at = now
                attempt_status = JobAttempt.STATUS_RETRYING
                attempt_error = "Recovered stale running job for retry."
                if job.job_type == "planner_status_writeback":
                    from planner.services.writeback import mark_status_writeback_job_retrying

                    mark_status_writeback_job_retrying(job, attempt_error)

            job.error_message = attempt_error
            job.save(
                update_fields=[
                    "status",
                    "attempt_count",
                    "next_run_at",
                    "locked_at",
                    "locked_by",
                    "lease_expires_at",
                    "finished_at",
                    "error_message",
                ]
            )
            JobAttempt.objects.filter(
                job=job,
                status=JobAttempt.STATUS_RUNNING,
                finished_at__isnull=True,
            ).update(
                status=attempt_status,
                finished_at=now,
                error_message=attempt_error,
            )

    if recovered:
        logger.warning("stale_jobs_recovered", extra={"count": recovered})
    return recovered


def _stale_running_jobs(now):
    return Job.objects.filter(status=Job.STATUS_RUNNING).filter(
        Q(lease_expires_at__lt=now)
        | Q(lease_expires_at__isnull=True, locked_at__lt=_stale_lock_cutoff())
        | Q(lease_expires_at__isnull=True, locked_at__isnull=True, started_at__isnull=True)
        | Q(lease_expires_at__isnull=True, locked_at__isnull=True, started_at__lt=_stale_lock_cutoff())
    )


def _job_is_stale(job: Job, now) -> bool:
    if job.lease_expires_at and job.lease_expires_at < now:
        return True
    if job.lease_expires_at:
        return False
    cutoff = _stale_lock_cutoff()
    if job.locked_at and job.locked_at < cutoff:
        return True
    if not job.locked_at and job.started_at is None:
        return True
    return not job.locked_at and job.started_at < cutoff


def _lock_owner() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def queue_sync_jobs(
    *,
    workspace,
    created_by=None,
    sources: list[str] | None = None,
    since: str | None = None,
    connector_account_id: int | str | None = None,
    full_sync: bool = False,
) -> list[Job]:
    run_group_id = str(uuid.uuid4())
    accounts = ConnectorAccount.objects.for_workspace(workspace).filter(
        is_active=True,
        status=ConnectorAccount.STATUS_CONNECTED,
        revoked_at__isnull=True,
    )
    if sources:
        accounts = accounts.filter(source__in=sources)
    if connector_account_id:
        accounts = accounts.filter(id=connector_account_id)
    accounts = accounts.order_by("source", "created_at")

    jobs: list[Job] = []
    for account in accounts:
        input_params = {
            "source": account.source,
            "full_sync": full_sync,
            "run_group_id": run_group_id,
        }
        if since:
            input_params["since"] = since
        idempotency_key = _sync_idempotency_key(
            workspace_id=workspace.id,
            account_id=account.id,
            since=since,
            full_sync=full_sync,
        )
        job = create_job(
            workspace=workspace,
            connector_account=account,
            job_type="sync",
            job_name="sync_connector",
            input_params=input_params,
            created_by=created_by,
            idempotency_key=idempotency_key,
        )
        jobs.append(job)
    return jobs


def _sync_idempotency_key(*, workspace_id: int, account_id: int, since: str | None, full_sync: bool) -> str:
    scope = "full" if full_sync else since or "incremental"
    return f"sync:{workspace_id}:{account_id}:{scope}"
