from __future__ import annotations

from unittest.mock import patch

from django.test import TestCase, override_settings
from django.utils import timezone

from ingestion.models import Job
from ingestion.services.jobs import run_job
from workspaces.models import Workspace


class JobConcurrencyTests(TestCase):
    def setUp(self) -> None:
        self.workspace = Workspace.objects.create(name="Workspace A")

    @override_settings(JOB_MAX_CONCURRENCY=4)
    def test_fifth_job_is_deferred_when_concurrency_full(self):
        for _ in range(4):
            Job.objects.create(
                workspace=self.workspace,
                job_type="sync",
                job_name="sync_connector",
                status=Job.STATUS_RUNNING,
                started_at=timezone.now(),
            )

        queued_job = Job.objects.create(
            workspace=self.workspace,
            job_type="sync",
            job_name="sync_connector",
            status=Job.STATUS_QUEUED,
            next_run_at=timezone.now(),
        )

        run_job(queued_job.id)
        queued_job.refresh_from_db()
        self.assertEqual(queued_job.status, Job.STATUS_QUEUED)
        self.assertIsNotNone(queued_job.next_run_at)

    @override_settings(JOB_MAX_CONCURRENCY=1)
    def test_deferred_job_runs_after_slot_frees(self):
        running_job = Job.objects.create(
            workspace=self.workspace,
            job_type="sync",
            job_name="sync_connector",
            status=Job.STATUS_RUNNING,
            started_at=timezone.now(),
        )
        queued_job = Job.objects.create(
            workspace=self.workspace,
            job_type="sync",
            job_name="sync_connector",
            status=Job.STATUS_QUEUED,
            next_run_at=timezone.now(),
        )

        run_job(queued_job.id)
        queued_job.refresh_from_db()
        self.assertEqual(queued_job.status, Job.STATUS_QUEUED)

        running_job.status = Job.STATUS_SUCCESS
        running_job.finished_at = timezone.now()
        running_job.save(update_fields=["status", "finished_at"])

        queued_job.next_run_at = timezone.now()
        queued_job.save(update_fields=["next_run_at"])

        with patch("ingestion.services.jobs.execute_job", return_value={}):
            run_job(queued_job.id)

        queued_job.refresh_from_db()
        self.assertEqual(queued_job.status, Job.STATUS_SUCCESS)

    @override_settings(JOB_MAX_CONCURRENCY=2)
    def test_running_jobs_never_exceed_limit(self):
        Job.objects.create(
            workspace=self.workspace,
            job_type="sync",
            job_name="sync_connector",
            status=Job.STATUS_RUNNING,
            started_at=timezone.now(),
        )
        Job.objects.create(
            workspace=self.workspace,
            job_type="sync",
            job_name="sync_connector",
            status=Job.STATUS_RUNNING,
            started_at=timezone.now(),
        )

        queued_job = Job.objects.create(
            workspace=self.workspace,
            job_type="sync",
            job_name="sync_connector",
            status=Job.STATUS_QUEUED,
            next_run_at=timezone.now(),
        )

        run_job(queued_job.id)
        self.assertEqual(
            Job.objects.filter(status=Job.STATUS_RUNNING).count(),
            2,
        )
