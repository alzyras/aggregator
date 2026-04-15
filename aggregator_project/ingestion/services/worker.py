from __future__ import annotations

import logging
import threading
import time

from django.conf import settings
from django.db import close_old_connections
from django.utils import timezone

from ingestion.models import Job
from ingestion.services.jobs import recover_stale_jobs, run_job

logger = logging.getLogger(__name__)


def run_worker_loop(poll_seconds: int = 5) -> None:
    while True:
        close_old_connections()
        try:
            recover_stale_jobs()
            now = timezone.now()
            job = (
                Job.objects
                .filter(status=Job.STATUS_QUEUED, next_run_at__lte=now)
                .order_by("priority", "queued_at")
                .first()
            )
            if job:
                run_job(job.id)
                continue
        except Exception:  # noqa: BLE001
            logger.exception("worker_loop_error")
            close_old_connections()
        time.sleep(poll_seconds)


def start_worker_thread(poll_seconds: int = 5) -> None:
    if not settings.DEBUG:
        return
    thread = threading.Thread(
        target=run_worker_loop,
        args=(poll_seconds,),
        daemon=True,
        name="job-worker",
    )
    thread.start()
