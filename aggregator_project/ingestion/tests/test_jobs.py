from __future__ import annotations

from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.test import TestCase, override_settings
from django.urls import reverse

from connectors.models import ConnectorAccount
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
        account = ConnectorAccount.objects.create(
            workspace=self.workspace_a,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        self.client.force_login(self.user_a)
        response = self.client.post(reverse("sync_view"))
        self.assertEqual(response.status_code, 302)

        job = Job.objects.for_workspace(self.workspace_a).get(job_name="sync_connector")
        self.assertEqual(job.status, Job.STATUS_QUEUED)
        self.assertEqual(job.created_by_id, self.user_a.id)
        self.assertIsNotNone(job.next_run_at)
        self.assertEqual(job.connector_account_id, account.id)

    def test_worker_updates_lifecycle_success(self):
        account = ConnectorAccount.objects.create(
            workspace=self.workspace_a,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        job = Job.objects.create(
            workspace=self.workspace_a,
            connector_account=account,
            job_type="sync",
            job_name="sync_connector",
            input_params={"source": "asana"},
        )

        with patch(
            "ingestion.services.jobs.sync_connector_account",
            return_value={"inserted": 1},
        ):
            result = job_service.run_job(job.id)

        self.assertEqual(result.status, Job.STATUS_SUCCESS)
        self.assertEqual(result.output_summary, {"results": [{"inserted": 1}]})
        self.assertIsNotNone(result.started_at)
        self.assertIsNotNone(result.finished_at)

    def test_failed_job_stores_error(self):
        account = ConnectorAccount.objects.create(
            workspace=self.workspace_a,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        job = Job.objects.create(
            workspace=self.workspace_a,
            connector_account=account,
            job_type="sync",
            job_name="sync_connector",
            input_params={"source": "asana"},
        )

        with patch.dict("os.environ", {"JOB_MAX_ATTEMPTS": "1"}):
            with patch(
                "ingestion.services.jobs.sync_connector_account",
                side_effect=RuntimeError("boom"),
            ):
                result = job_service.run_job(job.id)

        self.assertEqual(result.status, Job.STATUS_FAILED)
        self.assertIn("boom", result.error_message)
        self.assertTrue(result.error_traceback)

    def test_jobs_are_workspace_isolated(self):
        account_a = ConnectorAccount.objects.create(
            workspace=self.workspace_a,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        account_b = ConnectorAccount.objects.create(
            workspace=self.workspace_b,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        Job.objects.create(
            workspace=self.workspace_a,
            connector_account=account_a,
            job_type="sync",
            job_name="sync_connector",
        )
        Job.objects.create(
            workspace=self.workspace_b,
            connector_account=account_b,
            job_type="sync",
            job_name="sync_connector",
        )

        self.client.force_login(self.user_a)
        response = self.client.get(reverse("jobs_list"))
        self.assertEqual(response.status_code, 200)
        jobs = response.context["jobs"]
        self.assertEqual(jobs.count(), 1)
        self.assertEqual(jobs.first().workspace_id, self.workspace_a.id)

    @override_settings(DEBUG=True)
    def test_debug_immediate_sync_runs_job(self):
        ConnectorAccount.objects.create(
            workspace=self.workspace_a,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        self.client.force_login(self.user_a)
        with patch("ingestion.services.jobs.execute_job", return_value={}):
            response = self.client.post(reverse("sync_view"), {"run_immediately": "1"})
        self.assertEqual(response.status_code, 302)

        job = Job.objects.for_workspace(self.workspace_a).get(job_name="sync_connector")
        self.assertEqual(job.status, Job.STATUS_SUCCESS)

    def test_sync_jobs_created_per_connector_account(self):
        ConnectorAccount.objects.create(
            workspace=self.workspace_a,
            source="asana",
            display_name="Asana One",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        ConnectorAccount.objects.create(
            workspace=self.workspace_a,
            source="asana",
            display_name="Asana Two",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        self.client.force_login(self.user_a)
        response = self.client.post(reverse("sync_view"))
        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            Job.objects.for_workspace(self.workspace_a).filter(job_name="sync_connector").count(),
            2,
        )

    def test_job_rejects_mismatched_connector_account(self):
        account_b = ConnectorAccount.objects.create(
            workspace=self.workspace_b,
            source="asana",
            display_name="Asana B",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        with self.assertRaises(ValidationError):
            job = Job(
                workspace=self.workspace_a,
                connector_account=account_b,
                job_type="sync",
                job_name="sync_connector",
            )
            job.full_clean()

    def test_failure_does_not_block_other_connector_jobs(self):
        account_one = ConnectorAccount.objects.create(
            workspace=self.workspace_a,
            source="asana",
            display_name="Asana One",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        account_two = ConnectorAccount.objects.create(
            workspace=self.workspace_a,
            source="asana",
            display_name="Asana Two",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        job_one = Job.objects.create(
            workspace=self.workspace_a,
            connector_account=account_one,
            job_type="sync",
            job_name="sync_connector",
            input_params={"source": "asana"},
        )
        job_two = Job.objects.create(
            workspace=self.workspace_a,
            connector_account=account_two,
            job_type="sync",
            job_name="sync_connector",
            input_params={"source": "asana"},
        )

        def stub_sync(workspace, connector_account, since=None):
            if connector_account.id == account_one.id:
                raise RuntimeError("boom")
            return {"inserted": 1}

        with patch("ingestion.services.jobs.sync_connector_account", side_effect=stub_sync):
            result_one = job_service.run_job(job_one.id)
            result_two = job_service.run_job(job_two.id)

        self.assertEqual(result_one.status, Job.STATUS_QUEUED)
        self.assertEqual(result_two.status, Job.STATUS_SUCCESS)

    def test_retry_is_scoped_to_single_connector_job(self):
        account_one = ConnectorAccount.objects.create(
            workspace=self.workspace_a,
            source="asana",
            display_name="Asana One",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        account_two = ConnectorAccount.objects.create(
            workspace=self.workspace_a,
            source="asana",
            display_name="Asana Two",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        job_one = Job.objects.create(
            workspace=self.workspace_a,
            connector_account=account_one,
            job_type="sync",
            job_name="sync_connector",
            input_params={"source": "asana"},
        )
        job_two = Job.objects.create(
            workspace=self.workspace_a,
            connector_account=account_two,
            job_type="sync",
            job_name="sync_connector",
            input_params={"source": "asana"},
        )

        with patch(
            "ingestion.services.jobs.sync_connector_account",
            side_effect=RuntimeError("boom"),
        ):
            job_service.run_job(job_one.id)

        job_one.refresh_from_db()
        job_two.refresh_from_db()
        self.assertEqual(job_one.attempt_count, 1)
        self.assertEqual(job_two.attempt_count, 0)
    def _create_user(self, username: str):
        user_model = get_user_model()
        return user_model.objects.create_user(
            username=username,
            email=f"{username}@example.com",
            password="password123",
        )
