from __future__ import annotations

import logging
import os
import socket
import traceback
import uuid

from connectors.models import ConnectorAccount
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from ingestion.models import Job
from ingestion.services.sync import sync_connector_account

logger = logging.getLogger(__name__)


def enqueue_job(job_id):
    Job.objects.filter(id=job_id, status=Job.STATUS_QUEUED, next_run_at__isnull=True).update(
        next_run_at=timezone.now()
    )
    logger.info("job_queued", extra={"job_id": str(job_id)})
    return job_id


def run_job(job_id):
    recover_stale_jobs()
    if _is_concurrency_full():
        _defer_job(job_id)
        logger.info("job_deferred_concurrency", extra={"job_id": str(job_id)})
        return Job.objects.filter(id=job_id).first()

    with transaction.atomic():
        try:
            job = Job.objects.select_for_update(skip_locked=True).get(id=job_id)
        except ObjectDoesNotExist:
            logger.info("job_missing_or_locked", extra={"job_id": str(job_id)})
            return None
        now = timezone.now()
        if job.status != Job.STATUS_QUEUED:
            return job
        if job.next_run_at and job.next_run_at > now:
            return job
        job.status = Job.STATUS_RUNNING
        job.started_at = now
        job.locked_at = now
        job.locked_by = _lock_owner()
        job.save(update_fields=["status", "started_at", "locked_at", "locked_by"])
        logger.info(
            "job_started",
            extra={"job_id": str(job.id), "workspace_id": job.workspace_id, "status": job.status},
        )

    try:
        output = execute_job(job)
        job.status = Job.STATUS_SUCCESS
        job.output_summary = output or {}
        if job.job_type == "sync" and job.connector_account_id:
            ConnectorAccount.objects.filter(id=job.connector_account_id).update(
                last_sync_status=ConnectorAccount.SYNC_STATUS_SUCCESS
            )
        logger.info(
            "job_success",
            extra={"job_id": str(job.id), "workspace_id": job.workspace_id, "status": job.status},
        )
    except Exception as exc:  # noqa: BLE001
        job.attempt_count += 1
        job.error_message = str(exc)
        job.error_traceback = traceback.format_exc()
        if job.job_type == "sync" and job.connector_account_id:
            ConnectorAccount.objects.filter(id=job.connector_account_id).update(
                last_sync_status=ConnectorAccount.SYNC_STATUS_FAILED,
                last_sync_at=timezone.now(),
            )
        if job.attempt_count < _max_attempts(job):
            job.status = Job.STATUS_QUEUED
            job.next_run_at = timezone.now() + _retry_delay(job.attempt_count)
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
        else:
            job.status = Job.STATUS_FAILED
            if job.job_type == "planner_status_writeback":
                from planner.services.writeback import mark_status_writeback_job_failed

                mark_status_writeback_job_failed(job, str(exc))
            logger.info(
                "job_failed",
                extra={"job_id": str(job.id), "workspace_id": job.workspace_id, "status": job.status},
            )
    finally:
        job.finished_at = timezone.now()
        job.locked_at = None
        job.locked_by = ""
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
            ]
        )
    return job


def execute_job(job: Job):
    if job.job_type == "sync":
        return _execute_sync_job(job)
    if job.job_type == "planner_status_writeback":
        from planner.services.writeback import execute_status_writeback_job

        return execute_status_writeback_job(job)
    raise ValueError(f"Unsupported job type: {job.job_type}")


def _execute_sync_job(job: Job):
    since_raw = job.input_params.get("since")
    full_sync = bool(job.input_params.get("full_sync"))
    since = None if full_sync else parse_datetime(since_raw) if since_raw else None
    connector_account = job.connector_account
    if connector_account is None:
        raise ValueError("Sync jobs require a connector account.")
    if connector_account.workspace_id != job.workspace_id:
        raise ValueError("Connector account does not belong to workspace.")

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
    cutoff = _stale_lock_cutoff()
    running_count = Job.objects.filter(status=Job.STATUS_RUNNING).filter(
        Q(locked_at__gte=cutoff) | Q(locked_at__isnull=True, started_at__gte=cutoff)
    ).count()
    return running_count >= settings.JOB_MAX_CONCURRENCY


def _defer_job(job_id):
    now = timezone.now()
    Job.objects.filter(id=job_id, status=Job.STATUS_QUEUED).update(
        next_run_at=now + _retry_delay(1)
    )


def _retry_delay(attempt_count: int):
    delay_seconds = min(60 * attempt_count, 300)
    return timezone.timedelta(seconds=delay_seconds)


def _max_attempts(job: Job | None = None) -> int:
    if job and job.job_type == "planner_status_writeback":
        retries = max(int(getattr(settings, "PLANNER_STATUS_WRITEBACK_MAX_RETRIES", 3)), 0)
        return retries + 1
    return max(int(getattr(settings, "JOB_MAX_ATTEMPTS", 3)), 1)


def _stale_lock_cutoff():
    timeout_seconds = max(int(getattr(settings, "JOB_STALE_RUNNING_SECONDS", 900)), 1)
    return timezone.now() - timezone.timedelta(seconds=timeout_seconds)


def recover_stale_jobs() -> int:
    now = timezone.now()
    stale_jobs = Job.objects.filter(status=Job.STATUS_RUNNING).filter(
        Q(locked_at__lt=_stale_lock_cutoff())
        | Q(locked_at__isnull=True, started_at__isnull=True)
        | Q(locked_at__isnull=True, started_at__lt=_stale_lock_cutoff())
    )
    updated = stale_jobs.update(
        status=Job.STATUS_QUEUED,
        next_run_at=now,
        locked_at=None,
        locked_by="",
        error_message="Recovered stale running job for retry.",
    )
    if updated:
        logger.warning("stale_jobs_recovered", extra={"count": updated})
    return updated


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
        job = Job(
            workspace=workspace,
            connector_account=account,
            job_type="sync",
            job_name="sync_connector",
            input_params=input_params,
            created_by=created_by,
        )
        job.full_clean()
        job.save()
        enqueue_job(job.id)
        jobs.append(job)
    return jobs
