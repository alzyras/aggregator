from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
import time
import uuid

from django.conf import settings
from django.utils import timezone

from connectors.models import ConnectorAccount
from ingestion.models import Job
from ingestion.services.cache import get_workspace_refresh_policy
from ingestion.services.jobs import queue_sync_jobs
from workspaces.models import Workspace


@dataclass(frozen=True)
class RefreshQueueResult:
    jobs: tuple[Job, ...]
    full_sync_count: int
    incremental_sync_count: int

    @property
    def queued_count(self) -> int:
        return len(self.jobs)


_next_due_refresh_check = 0.0


def refresh_interval(policy) -> timedelta:
    return timedelta(seconds=max(1, round(86_400 / policy.refreshes_per_day)))


def get_workspace_refresh_snapshot(*, workspace, now=None) -> dict[str, object]:
    """Return lightweight, shared freshness state for Planner, Events, and Sync."""
    now = now or timezone.now()
    policy = get_workspace_refresh_policy(workspace)
    interval = refresh_interval(policy)
    accounts = list(
        ConnectorAccount.objects.for_workspace(workspace)
        .filter(
            is_active=True,
            status=ConnectorAccount.STATUS_CONNECTED,
            revoked_at__isnull=True,
        )
        .only("id", "last_sync_at", "last_sync_status", "last_full_sync_at")
    )
    account_ids = [account.id for account in accounts]
    active_sync_account_ids = set(
        Job.objects.for_workspace(workspace)
        .filter(
            job_type="sync",
            status__in=[Job.STATUS_QUEUED, Job.STATUS_RUNNING],
            connector_account_id__in=account_ids,
        )
        .values_list("connector_account_id", flat=True)
    )

    sync_times = [
        account.last_sync_at
        for account in accounts
        if account.last_sync_at
        and account.last_sync_status != ConnectorAccount.SYNC_STATUS_FAILED
    ]
    failed_count = sum(
        account.last_sync_status == ConnectorAccount.SYNC_STATUS_FAILED
        for account in accounts
    )
    stale_account_ids = [
        account.id
        for account in accounts
        if (
            account.last_sync_at is None
            or account.last_sync_status == ConnectorAccount.SYNC_STATUS_FAILED
            or now - account.last_sync_at >= interval
        )
    ]
    next_times = [
        account.last_sync_at + interval
        for account in accounts
        if account.last_sync_at is not None
    ]
    all_checked_at = min(sync_times) if len(sync_times) == len(accounts) and sync_times else None
    latest_updated_at = max(sync_times) if sync_times else None
    return {
        "policy": policy,
        "connected_count": len(accounts),
        "refreshes_per_day": policy.refreshes_per_day,
        "interval_minutes": max(1, round(interval.total_seconds() / 60)),
        "auto_refresh_enabled": policy.auto_refresh_enabled,
        "full_refresh_interval_days": policy.full_refresh_interval_days,
        "all_checked_at": all_checked_at,
        "latest_updated_at": latest_updated_at,
        "next_refresh_at": min(next_times) if next_times else None,
        "is_refreshing": bool(active_sync_account_ids),
        "refreshing_count": len(active_sync_account_ids),
        "stale_count": len(stale_account_ids),
        "failed_count": failed_count,
        "is_current": bool(accounts) and not stale_account_ids,
        "has_connected_sources": bool(accounts),
    }


def queue_workspace_refresh(
    *,
    workspace,
    created_by=None,
    sources: list[str] | None = None,
    connector_account_id: int | str | None = None,
    force_full: bool = False,
    due_only: bool = False,
    reason: str = "manual",
    now=None,
) -> RefreshQueueResult:
    """Queue one coherent refresh run, selecting full or incremental per account."""
    now = now or timezone.now()
    policy = get_workspace_refresh_policy(workspace)
    accounts = ConnectorAccount.objects.for_workspace(workspace).filter(
        is_active=True,
        status=ConnectorAccount.STATUS_CONNECTED,
        revoked_at__isnull=True,
    )
    if sources:
        accounts = accounts.filter(source__in=sources)
    if connector_account_id is not None:
        accounts = accounts.filter(id=connector_account_id)
    accounts = list(accounts.order_by("source", "created_at"))

    active_account_ids = set(
        Job.objects.for_workspace(workspace)
        .filter(
            job_type="sync",
            status__in=[Job.STATUS_QUEUED, Job.STATUS_RUNNING],
            connector_account_id__in=[account.id for account in accounts],
        )
        .values_list("connector_account_id", flat=True)
    )
    run_group_id = str(uuid.uuid4())
    jobs: list[Job] = []
    full_sync_count = 0
    incremental_sync_count = 0
    for account in accounts:
        if account.id in active_account_ids:
            continue
        if due_only and not _is_due(account=account, policy=policy, now=now):
            continue
        full_sync = force_full or _needs_full_sync(account=account, policy=policy, now=now)
        queued = queue_sync_jobs(
            workspace=workspace,
            created_by=created_by,
            connector_account_id=account.id,
            full_sync=full_sync,
            run_group_id=run_group_id,
            refresh_reason=reason,
            priority=10 if reason == "scheduled" else 1,
        )
        jobs.extend(queued)
        if queued:
            if full_sync:
                full_sync_count += len(queued)
            else:
                incremental_sync_count += len(queued)
    return RefreshQueueResult(
        jobs=tuple(jobs),
        full_sync_count=full_sync_count,
        incremental_sync_count=incremental_sync_count,
    )


def queue_due_workspace_refreshes(*, now=None) -> list[RefreshQueueResult]:
    """Schedule only accounts that have outlived their workspace refresh interval."""
    now = now or timezone.now()
    workspace_ids = (
        ConnectorAccount.objects.filter(
            is_active=True,
            status=ConnectorAccount.STATUS_CONNECTED,
            revoked_at__isnull=True,
        )
        .values_list("workspace_id", flat=True)
        .distinct()
    )
    results: list[RefreshQueueResult] = []
    for workspace in Workspace.objects.filter(id__in=workspace_ids).iterator():
        policy = get_workspace_refresh_policy(workspace)
        if not policy.auto_refresh_enabled:
            continue
        result = queue_workspace_refresh(
            workspace=workspace,
            due_only=True,
            reason="scheduled",
            now=now,
        )
        if result.jobs:
            results.append(result)
    return results


def maybe_queue_due_workspace_refreshes() -> int:
    """Keep worker polling cheap while relying on job idempotency across workers."""
    global _next_due_refresh_check
    current = time.monotonic()
    if current < _next_due_refresh_check:
        return 0
    cadence = max(int(getattr(settings, "AUTO_REFRESH_SCHEDULER_TICK_SECONDS", 60)), 5)
    _next_due_refresh_check = current + cadence
    return sum(result.queued_count for result in queue_due_workspace_refreshes())


def _is_due(*, account: ConnectorAccount, policy, now) -> bool:
    return account.last_sync_at is None or now - account.last_sync_at >= refresh_interval(policy)


def _needs_full_sync(*, account: ConnectorAccount, policy, now) -> bool:
    if account.last_full_sync_at is None:
        return True
    return now - account.last_full_sync_at >= timedelta(
        days=policy.full_refresh_interval_days
    )
