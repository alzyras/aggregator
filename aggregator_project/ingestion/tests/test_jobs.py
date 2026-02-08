from __future__ import annotations

from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.urls import reverse

from ingestion.models import Job
from ingestion.services import jobs as job_service
from workspaces.models import Workspace, WorkspaceMember


class JobTests(TestCase):
    def setUp(self) -> None:
        self.user_a = self._create_user("user_a")
        self.user_b = self._create_user("user_b")

        self.workspace_a = Workspace.objects.create(name="Workspace A")
        WorkspaceMember.objects.create(
            workspace=self.workspace_a,
            user=self.user_a,
            role=WorkspaceMember.ROLE_OWNER,
        )

        self.workspace_b = Workspace.objects.create(name="Workspace B")
        WorkspaceMember.objects.create(
            workspace=self.workspace_b,
            user=self.user_b,
            role=WorkspaceMember.ROLE_OWNER,
        )

    def test_user_action_creates_queued_job(self):
        self.client.force_login(self.user_a)
        response = self.client.post(reverse("sync_view"))
        self.assertEqual(response.status_code, 302)

        job = Job.objects.for_workspace(self.workspace_a).get(job_name="sync_all")
        self.assertEqual(job.status, Job.STATUS_QUEUED)
        self.assertEqual(job.created_by_id, self.user_a.id)
        self.assertIsNotNone(job.next_run_at)

    def test_worker_updates_lifecycle_success(self):
        job = Job.objects.create(
            workspace=self.workspace_a,
            job_type="sync",
            job_name="sync_source",
            input_params={"source": "asana"},
        )

        with patch("ingestion.services.jobs.sync_source", return_value={"inserted": 1}):
            result = job_service.run_job(job.id)

        self.assertEqual(result.status, Job.STATUS_SUCCESS)
        self.assertEqual(result.output_summary, {"results": [{"inserted": 1}]})
        self.assertIsNotNone(result.started_at)
        self.assertIsNotNone(result.finished_at)

    def test_failed_job_stores_error(self):
        job = Job.objects.create(
            workspace=self.workspace_a,
            job_type="sync",
            job_name="sync_source",
            input_params={"source": "asana"},
        )

        with patch("ingestion.services.jobs.sync_source", side_effect=RuntimeError("boom")):
            result = job_service.run_job(job.id)

        self.assertEqual(result.status, Job.STATUS_FAILED)
        self.assertIn("boom", result.error_message)
        self.assertTrue(result.error_traceback)

    def test_jobs_are_workspace_isolated(self):
        Job.objects.create(
            workspace=self.workspace_a,
            job_type="sync",
            job_name="sync_all",
        )
        Job.objects.create(
            workspace=self.workspace_b,
            job_type="sync",
            job_name="sync_all",
        )

        self.client.force_login(self.user_a)
        response = self.client.get(reverse("jobs_list"))
        self.assertEqual(response.status_code, 200)
        jobs = response.context["jobs"]
        self.assertEqual(jobs.count(), 1)
        self.assertEqual(jobs.first().workspace_id, self.workspace_a.id)

    @override_settings(DEBUG=True)
    def test_debug_immediate_sync_runs_job(self):
        self.client.force_login(self.user_a)
        with patch("ingestion.services.jobs.execute_job", return_value={}):
            response = self.client.post(reverse("sync_view"), {"run_immediately": "1"})
        self.assertEqual(response.status_code, 302)

        job = Job.objects.for_workspace(self.workspace_a).get(job_name="sync_all")
        self.assertEqual(job.status, Job.STATUS_SUCCESS)

    def _create_user(self, username: str):
        user_model = get_user_model()
        return user_model.objects.create_user(
            username=username,
            email=f"{username}@example.com",
            password="password123",
        )
