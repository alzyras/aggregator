from __future__ import annotations

from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from connectors.models import ConnectorAccount
from events.models import Event
from ingestion.models import Job
from workspaces.models import Workspace, WorkspaceMember


class StatsViewTests(TestCase):
    def setUp(self) -> None:
        self.user = self._create_user("stats-user")
        self.workspace = Workspace.objects.create(name="Stats Workspace")
        WorkspaceMember.objects.create(
            workspace=self.workspace,
            user=self.user,
            role=WorkspaceMember.ROLE_OWNER,
        )
        self.client.force_login(self.user)

    def _create_user(self, username: str):
        user_model = get_user_model()
        return user_model.objects.create_user(
            username=username,
            email=f"{username}@example.com",
            password="password123",
        )

    def test_stats_view_returns_workspace_aggregates(self):
        account_asana = ConnectorAccount.objects.create(
            workspace=self.workspace,
            source="asana",
            display_name="Work",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
            last_sync_at=timezone.now(),
            last_sync_status=ConnectorAccount.SYNC_STATUS_SUCCESS,
        )
        account_habitica = ConnectorAccount.objects.create(
            workspace=self.workspace,
            source="habitica",
            display_name="Main",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )

        now = timezone.now()
        Event.objects.create(
            workspace=self.workspace,
            connector_account=account_asana,
            source="asana",
            source_entity_type="task",
            source_entity_id="asana-1",
            event_type="task_completed",
            title="Asana Task",
            start_time=now - timedelta(days=1),
            external_actor_display_name="Alex",
            raw={},
            source_event_version="v-asana-1",
            dedupe_hash="dedupe-asana-1",
        )
        Event.objects.create(
            workspace=self.workspace,
            connector_account=account_habitica,
            source="habitica",
            source_entity_type="task",
            source_entity_id="habitica-1",
            event_type="task_updated",
            title="Habitica Task",
            start_time=now - timedelta(days=2),
            external_actor_display_name="Taylor",
            raw={},
            source_event_version="v-habitica-1",
            dedupe_hash="dedupe-habitica-1",
        )

        Job.objects.create(
            workspace=self.workspace,
            connector_account=account_asana,
            job_type="sync",
            job_name="sync_asana",
            status=Job.STATUS_SUCCESS,
            started_at=now - timedelta(minutes=5),
            finished_at=now - timedelta(minutes=1),
            input_params={"source": "asana"},
            output_summary={"inserted": 1},
            created_by=self.user,
        )

        response = self.client.get(reverse("stats_view"))

        self.assertEqual(response.status_code, 200)
        self.assertIn("provider_totals", response.context)
        self.assertIn("event_type_totals", response.context)
        self.assertIn("completion_series", response.context)
        self.assertIn("top_entities", response.context)
        self.assertIn("connector_sync_rows", response.context)
        self.assertGreaterEqual(len(response.context["provider_totals"]), 1)
        self.assertEqual(len(response.context["completion_series"]), 30)
