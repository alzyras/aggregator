from __future__ import annotations

from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from connectors.encryption import EncryptionError
from connectors.models import ConnectorAccount
from ingestion import views as ingestion_views
from ingestion.models import Job, JobAttempt
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
        attempt = JobAttempt.objects.get(job=result)
        self.assertEqual(attempt.status, JobAttempt.STATUS_SUCCESS)
        self.assertEqual(attempt.attempt_number, 1)

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

        with self.settings(JOB_MAX_ATTEMPTS=1):
            with patch(
                "ingestion.services.jobs.sync_connector_account",
                side_effect=RuntimeError("boom"),
            ):
                result = job_service.run_job(job.id)

        self.assertEqual(result.status, Job.STATUS_FAILED)
        self.assertIn("boom", result.error_message)
        self.assertTrue(result.error_traceback)

    @override_settings(JOB_MAX_ATTEMPTS=3)
    def test_encryption_failure_requires_reconnect_without_retry(self):
        account = ConnectorAccount.objects.create(
            workspace=self.workspace_a,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"unreadable-token",
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
            side_effect=EncryptionError("Invalid encryption token or key."),
        ):
            result = job_service.run_job(job.id)

        account.refresh_from_db()
        self.assertEqual(result.status, Job.STATUS_FAILED)
        self.assertEqual(result.attempt_count, 1)
        self.assertEqual(account.status, ConnectorAccount.STATUS_ERROR)
        self.assertFalse(account.is_active)
        self.assertEqual(account.last_sync_status, ConnectorAccount.SYNC_STATUS_FAILED)
        self.assertEqual(
            account.last_error,
            ConnectorAccount.RECONNECT_REQUIRED_ERROR,
        )
        self.assertEqual(result.attempts.get().status, JobAttempt.STATUS_FAILED)

    @override_settings(JOB_STALE_RUNNING_SECONDS=60)
    def test_recovers_stale_running_jobs_for_retry(self):
        account = ConnectorAccount.objects.create(
            workspace=self.workspace_a,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        stale_job = Job.objects.create(
            workspace=self.workspace_a,
            connector_account=account,
            job_type="sync",
            job_name="sync_connector",
            status=Job.STATUS_RUNNING,
            locked_at=timezone.now() - timedelta(minutes=10),
            locked_by="dead-worker",
        )

        recovered = job_service.recover_stale_jobs()

        stale_job.refresh_from_db()
        self.assertEqual(recovered, 1)
        self.assertEqual(stale_job.status, Job.STATUS_QUEUED)
        self.assertLessEqual(stale_job.next_run_at, timezone.now())
        self.assertIsNone(stale_job.locked_at)
        self.assertEqual(stale_job.locked_by, "")

    @override_settings(JOB_STALE_RUNNING_SECONDS=60)
    def test_recovers_expired_lease_for_retry(self):
        account = ConnectorAccount.objects.create(
            workspace=self.workspace_a,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        stale_job = Job.objects.create(
            workspace=self.workspace_a,
            connector_account=account,
            job_type="sync",
            job_name="sync_connector",
            status=Job.STATUS_RUNNING,
            locked_at=timezone.now(),
            lease_expires_at=timezone.now() - timedelta(seconds=1),
            locked_by="dead-worker",
        )
        JobAttempt.objects.create(
            job=stale_job,
            attempt_number=1,
            worker_id="dead-worker",
            status=JobAttempt.STATUS_RUNNING,
        )

        recovered = job_service.recover_stale_jobs()

        stale_job.refresh_from_db()
        self.assertEqual(recovered, 1)
        self.assertEqual(stale_job.status, Job.STATUS_QUEUED)
        self.assertIsNone(stale_job.lease_expires_at)

        with patch("ingestion.services.jobs.execute_job", return_value={}):
            result = job_service.run_job(stale_job.id)

        self.assertEqual(result.status, Job.STATUS_SUCCESS)
        self.assertEqual(
            list(result.attempts.order_by("attempt_number").values_list("attempt_number", flat=True)),
            [1, 2],
        )

    @override_settings(JOB_STALE_RUNNING_SECONDS=60)
    def test_stale_recovery_marks_exhausted_job_failed(self):
        account = ConnectorAccount.objects.create(
            workspace=self.workspace_a,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        stale_job = Job.objects.create(
            workspace=self.workspace_a,
            connector_account=account,
            job_type="sync",
            job_name="sync_connector",
            status=Job.STATUS_RUNNING,
            locked_at=timezone.now(),
            lease_expires_at=timezone.now() - timedelta(seconds=1),
            locked_by="dead-worker",
            max_attempts=1,
        )
        JobAttempt.objects.create(
            job=stale_job,
            attempt_number=1,
            worker_id="dead-worker",
            status=JobAttempt.STATUS_RUNNING,
        )

        recovered = job_service.recover_stale_jobs()

        stale_job.refresh_from_db()
        self.assertEqual(recovered, 1)
        self.assertEqual(stale_job.status, Job.STATUS_FAILED)
        self.assertEqual(stale_job.attempt_count, 1)
        self.assertLessEqual(stale_job.next_run_at, timezone.now())
        self.assertEqual(stale_job.attempts.get().status, JobAttempt.STATUS_FAILED)

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

    def test_queue_sync_jobs_assigns_same_run_group_id_per_invocation(self):
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
            source="todoist",
            display_name="Todoist Main",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )

        jobs = job_service.queue_sync_jobs(
            workspace=self.workspace_a,
            created_by=self.user_a,
        )

        self.assertEqual(len(jobs), 2)
        run_group_ids = {job.input_params.get("run_group_id") for job in jobs}
        self.assertEqual(len(run_group_ids), 1)
        run_group_id = run_group_ids.pop()
        self.assertTrue(run_group_id)

        stored_jobs = Job.objects.filter(id__in=[job.id for job in jobs]).order_by("id")
        self.assertEqual(
            {job.input_params.get("run_group_id") for job in stored_jobs},
            {run_group_id},
        )

    def test_create_job_dedupes_active_idempotency_key(self):
        account = ConnectorAccount.objects.create(
            workspace=self.workspace_a,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )

        first = job_service.create_job(
            workspace=self.workspace_a,
            connector_account=account,
            job_type="sync",
            job_name="sync_connector",
            input_params={"source": "asana"},
            idempotency_key="sync:test",
        )
        second = job_service.create_job(
            workspace=self.workspace_a,
            connector_account=account,
            job_type="sync",
            job_name="sync_connector",
            input_params={"source": "asana"},
            idempotency_key="sync:test",
        )

        self.assertEqual(first.id, second.id)
        self.assertEqual(Job.objects.filter(idempotency_key="sync:test").count(), 1)

    def test_sync_view_groups_runs_and_builds_connector_overview(self):
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
            source="habitica",
            display_name="Habitica Main",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        now = timezone.now()
        run_group_id = "run-group-123"
        Job.objects.create(
            workspace=self.workspace_a,
            connector_account=account_one,
            job_type="sync",
            job_name="sync_connector",
            status=Job.STATUS_SUCCESS,
            started_at=now - timedelta(minutes=4),
            finished_at=now - timedelta(minutes=3),
            input_params={"source": "asana", "run_group_id": run_group_id},
            created_by=self.user_a,
        )
        Job.objects.create(
            workspace=self.workspace_a,
            connector_account=account_two,
            job_type="sync",
            job_name="sync_connector",
            status=Job.STATUS_RUNNING,
            started_at=now - timedelta(minutes=3),
            input_params={"source": "habitica", "run_group_id": run_group_id},
            created_by=self.user_a,
        )

        self.client.force_login(self.user_a)
        response = self.client.get(reverse("sync_view"))

        self.assertEqual(response.status_code, 200)
        run_groups = response.context["run_groups"]
        self.assertEqual(len(run_groups), 1)
        self.assertEqual(run_groups[0]["run_id"], run_group_id)
        self.assertEqual(run_groups[0]["status"], Job.STATUS_RUNNING)

        connector_rows = response.context["connector_run_rows"]
        connector_labels = [row["connector_label"] for row in connector_rows]
        self.assertIn("Asana (Asana One)", connector_labels)
        self.assertIn("Habitica (Habitica Main)", connector_labels)
        self.assertEqual(len(response.context["run_overview_runs"]), 30)
        allowed_statuses = {"success", "failed", "running", "queued", "cancelled", "missing"}
        for row in connector_rows:
            self.assertEqual(len(row["cells"]), 30)
            for cell in row["cells"]:
                self.assertIn("status", cell)
                self.assertIn("size_class", cell)
                self.assertIn("title", cell)
                self.assertIn("aria_label", cell)
                self.assertIn("tooltip", cell)
                self.assertIn(cell["status"], allowed_statuses)
                self.assertIn(
                    cell["size_class"],
                    {
                        "run-bubble-size-small",
                        "run-bubble-size-medium",
                        "run-bubble-size-large",
                    },
                )

    def test_sync_view_uses_latest_connector_run_for_each_day(self):
        account = ConnectorAccount.objects.create(
            workspace=self.workspace_a,
            source="asana",
            display_name="Asana One",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        now = timezone.now()
        day_key = now.date().isoformat()
        Job.objects.create(
            workspace=self.workspace_a,
            connector_account=account,
            job_type="sync",
            job_name="sync_connector",
            status=Job.STATUS_QUEUED,
            queued_at=now - timedelta(hours=2),
            started_at=now - timedelta(hours=2),
            input_params={"source": "asana", "run_group_id": "day-run-old"},
            created_by=self.user_a,
        )
        Job.objects.create(
            workspace=self.workspace_a,
            connector_account=account,
            job_type="sync",
            job_name="sync_connector",
            status=Job.STATUS_FAILED,
            queued_at=now - timedelta(hours=1),
            started_at=now - timedelta(hours=1),
            finished_at=now - timedelta(minutes=50),
            input_params={"source": "asana", "run_group_id": "day-run-new"},
            created_by=self.user_a,
        )

        self.client.force_login(self.user_a)
        response = self.client.get(reverse("sync_view"))

        self.assertEqual(response.status_code, 200)
        columns = response.context["run_overview_runs"]
        column_index = next(
            index for index, column in enumerate(columns) if column["day_key"] == day_key
        )
        connector_row = next(
            row for row in response.context["connector_run_rows"]
            if row["connector_label"] == "Asana (Asana One)"
        )
        self.assertEqual(connector_row["cells"][column_index]["status"], Job.STATUS_FAILED)

    def test_sync_view_legacy_jobs_without_run_group_id_are_individual_runs(self):
        account = ConnectorAccount.objects.create(
            workspace=self.workspace_a,
            source="asana",
            display_name="Asana Legacy",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        job_one = Job.objects.create(
            workspace=self.workspace_a,
            connector_account=account,
            job_type="sync",
            job_name="sync_connector",
            status=Job.STATUS_SUCCESS,
            input_params={"source": "asana"},
            created_by=self.user_a,
        )
        job_two = Job.objects.create(
            workspace=self.workspace_a,
            connector_account=account,
            job_type="sync",
            job_name="sync_connector",
            status=Job.STATUS_SUCCESS,
            input_params={"source": "asana"},
            created_by=self.user_a,
        )

        self.client.force_login(self.user_a)
        response = self.client.get(reverse("sync_view"))

        self.assertEqual(response.status_code, 200)
        run_ids = {run["run_id"] for run in response.context["run_groups"]}
        self.assertIn(str(job_one.id), run_ids)
        self.assertIn(str(job_two.id), run_ids)

    def test_sync_view_run_status_precedence_failed_over_running(self):
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
            source="todoist",
            display_name="Todoist Main",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        now = timezone.now()
        run_group_id = "run-group-precedence"
        Job.objects.create(
            workspace=self.workspace_a,
            connector_account=account_one,
            job_type="sync",
            job_name="sync_connector",
            status=Job.STATUS_RUNNING,
            started_at=now - timedelta(minutes=2),
            input_params={"source": "asana", "run_group_id": run_group_id},
            created_by=self.user_a,
        )
        Job.objects.create(
            workspace=self.workspace_a,
            connector_account=account_two,
            job_type="sync",
            job_name="sync_connector",
            status=Job.STATUS_FAILED,
            started_at=now - timedelta(minutes=2),
            finished_at=now - timedelta(minutes=1),
            input_params={"source": "todoist", "run_group_id": run_group_id},
            created_by=self.user_a,
        )

        self.client.force_login(self.user_a)
        response = self.client.get(reverse("sync_view"))

        self.assertEqual(response.status_code, 200)
        run_groups = response.context["run_groups"]
        self.assertEqual(run_groups[0]["status"], Job.STATUS_FAILED)

    def test_sync_view_normalizes_unknown_status_to_missing(self):
        self.assertEqual(
            ingestion_views._normalize_bubble_status("unknown_status"),
            "missing",
        )
        self.assertEqual(
            ingestion_views._normalize_bubble_status(None),
            "missing",
        )
        self.assertEqual(
            ingestion_views._normalize_bubble_status(Job.STATUS_SUCCESS),
            Job.STATUS_SUCCESS,
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
