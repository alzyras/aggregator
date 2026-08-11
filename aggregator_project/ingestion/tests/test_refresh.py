from __future__ import annotations

from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from connectors.models import ConnectorAccount
from ingestion.models import Job, WorkspaceRefreshPolicy
from ingestion.providers import ProviderSpec
from ingestion.services.refresh import (
    get_workspace_refresh_snapshot,
    queue_due_workspace_refreshes,
    queue_workspace_refresh,
)
from ingestion.services.sync import sync_connector_account
from workspaces.models import Workspace, WorkspaceMember


class RefreshEngineTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="refresh-owner",
            password="test-password",
        )
        self.workspace = Workspace.objects.create(name="Refresh workspace")
        WorkspaceMember.objects.create(
            workspace=self.workspace,
            user=self.user,
            role=WorkspaceMember.ROLE_OWNER,
        )
        self.account = self._account()

    def _account(self, **overrides):
        defaults = {
            "workspace": self.workspace,
            "source": "asana",
            "display_name": "Asana",
            "auth_type": ConnectorAccount.AUTH_API_TOKEN,
            "encrypted_access_token": b"token",
            "status": ConnectorAccount.STATUS_CONNECTED,
            "is_active": True,
        }
        defaults.update(overrides)
        return ConnectorAccount.objects.create(**defaults)

    def test_first_refresh_is_a_full_sync(self):
        result = queue_workspace_refresh(
            workspace=self.workspace,
            created_by=self.user,
        )

        self.assertEqual(result.queued_count, 1)
        self.assertEqual(result.full_sync_count, 1)
        job = result.jobs[0]
        self.assertTrue(job.input_params["full_sync"])
        self.assertEqual(job.input_params["refresh_reason"], "manual")

    def test_due_refresh_is_incremental_after_a_full_sync(self):
        now = timezone.now()
        policy = WorkspaceRefreshPolicy.objects.create(
            workspace=self.workspace,
            refreshes_per_day=12,
            full_refresh_interval_days=7,
        )
        self.account.last_sync_at = now - timedelta(hours=3)
        self.account.last_full_sync_at = now - timedelta(days=1)
        self.account.save(update_fields=["last_sync_at", "last_full_sync_at"])

        results = queue_due_workspace_refreshes(now=now)

        self.assertEqual(len(results), 1)
        job = results[0].jobs[0]
        self.assertFalse(job.input_params["full_sync"])
        self.assertEqual(job.input_params["refresh_reason"], "scheduled")
        self.assertEqual(job.priority, 10)
        self.assertEqual(policy.refreshes_per_day, 12)

    def test_recent_sync_is_not_scheduled_again(self):
        now = timezone.now()
        WorkspaceRefreshPolicy.objects.create(
            workspace=self.workspace,
            refreshes_per_day=12,
        )
        self.account.last_sync_at = now - timedelta(minutes=10)
        self.account.last_full_sync_at = now - timedelta(days=1)
        self.account.save(update_fields=["last_sync_at", "last_full_sync_at"])

        results = queue_due_workspace_refreshes(now=now)

        self.assertEqual(results, [])
        self.assertFalse(Job.objects.filter(workspace=self.workspace).exists())

    def test_snapshot_uses_slowest_connected_source_for_workspace_freshness(self):
        now = timezone.now()
        WorkspaceRefreshPolicy.objects.create(
            workspace=self.workspace,
            refreshes_per_day=12,
        )
        self.account.last_sync_at = now - timedelta(minutes=5)
        self.account.save(update_fields=["last_sync_at"])
        slow_account = self._account(
            source="todoist",
            display_name="Todoist",
            last_sync_at=now - timedelta(hours=3),
        )

        snapshot = get_workspace_refresh_snapshot(workspace=self.workspace, now=now)

        self.assertEqual(snapshot["connected_count"], 2)
        self.assertEqual(snapshot["all_checked_at"], slow_account.last_sync_at)
        self.assertEqual(snapshot["stale_count"], 1)
        self.assertFalse(snapshot["is_current"])

    def test_snapshot_marks_failed_source_as_needing_attention(self):
        now = timezone.now()
        WorkspaceRefreshPolicy.objects.create(
            workspace=self.workspace,
            refreshes_per_day=12,
        )
        self.account.last_sync_at = now - timedelta(minutes=5)
        self.account.last_sync_status = ConnectorAccount.SYNC_STATUS_FAILED
        self.account.save(update_fields=["last_sync_at", "last_sync_status"])

        snapshot = get_workspace_refresh_snapshot(workspace=self.workspace, now=now)

        self.assertEqual(snapshot["failed_count"], 1)
        self.assertEqual(snapshot["stale_count"], 1)
        self.assertIsNone(snapshot["all_checked_at"])
        self.assertFalse(snapshot["is_current"])

    def test_incremental_sync_uses_cursor_with_overlap_and_advances_cursor(self):
        now = timezone.now()
        self.account.sync_cursor_at = now - timedelta(minutes=10)
        self.account.last_full_sync_at = now - timedelta(days=1)
        self.account.save(update_fields=["sync_cursor_at", "last_full_sync_at"])

        class StubClient:
            def fetch_since(self, since):
                self.since = since
                return []

        client = StubClient()
        spec = ProviderSpec(
            source="asana",
            label="Asana",
            client_factory=lambda _account: client,
            normalizer=lambda raw: raw,
            required_fields=[],
            auth_type="api_token",
            validate_credentials=lambda _credentials: (True, "ok"),
            form_class=None,
            icon="asana",
        )
        with patch("ingestion.services.sync.get_provider_spec", return_value=spec):
            result = sync_connector_account(self.workspace, self.account)

        self.account.refresh_from_db()
        self.assertEqual(result["mode"], "incremental")
        self.assertLessEqual(client.since, now - timedelta(minutes=14))
        self.assertGreater(self.account.sync_cursor_at, now - timedelta(seconds=5))
        self.assertIsNotNone(self.account.last_incremental_sync_at)

    def test_refresh_now_endpoint_uses_shared_queue(self):
        self.client.force_login(self.user)

        response = self.client.post(reverse("refresh_now"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["queued"], 1)
        self.assertTrue(
            Job.objects.for_workspace(self.workspace)
            .filter(job_type="sync", connector_account=self.account)
            .exists()
        )

    def test_refresh_state_endpoint_reports_workspace_progress(self):
        policy = WorkspaceRefreshPolicy.objects.create(workspace=self.workspace)
        self.client.force_login(self.user)

        response = self.client.get(reverse("refresh_state"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["cache_version"], policy.cache_version)
        self.assertEqual(response.json()["connected_count"], 1)
        self.assertFalse(response.json()["is_refreshing"])
        self.assertTrue(response.json()["has_connected_sources"])
        self.assertIn("no-store", response.headers["Cache-Control"])

    def test_refresh_now_runs_incremental_sync_even_when_workspace_is_current(self):
        now = timezone.now()
        WorkspaceRefreshPolicy.objects.create(
            workspace=self.workspace,
            refreshes_per_day=12,
        )
        self.account.last_sync_at = now - timedelta(minutes=5)
        self.account.last_full_sync_at = now - timedelta(days=1)
        self.account.save(update_fields=["last_sync_at", "last_full_sync_at"])
        self.client.force_login(self.user)

        response = self.client.post(reverse("refresh_now"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["queued"], 1)
        self.assertTrue(response.json()["has_connected_sources"])
        self.assertTrue(response.json()["is_current"])
        job = Job.objects.get(workspace=self.workspace, connector_account=self.account)
        self.assertFalse(job.input_params["full_sync"])

    def test_owner_can_change_workspace_refresh_policy(self):
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("sync_view"),
            {
                "action": "save_policy",
                "auto_refresh_enabled": "on",
                "refreshes_per_day": "24",
                "full_refresh_interval_days": "3",
            },
        )

        self.assertRedirects(response, reverse("sync_view"))
        policy = WorkspaceRefreshPolicy.objects.get(workspace=self.workspace)
        self.assertEqual(policy.refreshes_per_day, 24)
        self.assertEqual(policy.full_refresh_interval_days, 3)

    def test_member_cannot_change_workspace_refresh_policy(self):
        member = get_user_model().objects.create_user(
            username="refresh-member",
            password="test-password",
        )
        WorkspaceMember.objects.create(
            workspace=self.workspace,
            user=member,
            role=WorkspaceMember.ROLE_MEMBER,
        )
        WorkspaceRefreshPolicy.objects.create(
            workspace=self.workspace,
            refreshes_per_day=12,
        )
        self.client.force_login(member)

        response = self.client.post(
            reverse("sync_view"),
            {
                "action": "save_policy",
                "refreshes_per_day": "24",
                "full_refresh_interval_days": "3",
            },
        )

        self.assertRedirects(response, reverse("sync_view"))
        policy = WorkspaceRefreshPolicy.objects.get(workspace=self.workspace)
        self.assertEqual(policy.refreshes_per_day, 12)
