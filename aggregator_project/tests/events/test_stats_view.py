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

    def _create_connector(self, *, workspace, source: str, display_name: str) -> ConnectorAccount:
        return ConnectorAccount.objects.create(
            workspace=workspace,
            source=source,
            display_name=display_name,
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )

    def _create_event(
        self,
        *,
        workspace,
        connector_account,
        source: str,
        event_type: str,
        source_entity_id: str,
        start_time,
    ) -> Event:
        return Event.objects.create(
            workspace=workspace,
            connector_account=connector_account,
            source=source,
            source_entity_type="task",
            source_entity_id=source_entity_id,
            event_type=event_type,
            title=f"Event {source_entity_id}",
            start_time=start_time,
            raw={},
            source_event_version=f"v-{source_entity_id}",
            dedupe_hash=f"dedupe-{source_entity_id}",
        )

    def _create_sync_job(
        self,
        *,
        workspace,
        connector_account,
        finished_at,
        output_summary,
    ) -> Job:
        return Job.objects.create(
            workspace=workspace,
            connector_account=connector_account,
            job_type="sync",
            job_name="sync_connector",
            status=Job.STATUS_SUCCESS,
            queued_at=finished_at - timedelta(minutes=2),
            started_at=finished_at - timedelta(minutes=1),
            finished_at=finished_at,
            input_params={"source": connector_account.source},
            output_summary=output_summary,
            created_by=self.user,
        )

    def test_stats_view_returns_required_context_keys(self):
        connector = self._create_connector(
            workspace=self.workspace,
            source="asana",
            display_name="Asana Main",
        )
        now = timezone.now()
        self._create_event(
            workspace=self.workspace,
            connector_account=connector,
            source="asana",
            event_type="task_completed",
            source_entity_id="a1",
            start_time=now - timedelta(days=1),
        )

        response = self.client.get(reverse("stats_view"))

        self.assertEqual(response.status_code, 200)
        self.assertIn("source_totals", response.context)
        self.assertIn("completion_daily_series", response.context)
        self.assertIn("completion_monthly_series", response.context)
        self.assertIn("selected_source", response.context)
        self.assertIn("sync_source_rows", response.context)
        self.assertEqual(len(response.context["completion_daily_series"]), 30)
        self.assertEqual(len(response.context["completion_monthly_series"]), 12)

    def test_stats_view_source_filter_scopes_chart_data(self):
        asana = self._create_connector(
            workspace=self.workspace,
            source="asana",
            display_name="Asana Main",
        )
        habitica = self._create_connector(
            workspace=self.workspace,
            source="habitica",
            display_name="Habitica Main",
        )
        now = timezone.now()
        self._create_event(
            workspace=self.workspace,
            connector_account=asana,
            source="asana",
            event_type="task_completed",
            source_entity_id="a2",
            start_time=now - timedelta(days=1),
        )
        self._create_event(
            workspace=self.workspace,
            connector_account=habitica,
            source="habitica",
            event_type="task_completed",
            source_entity_id="h1",
            start_time=now - timedelta(days=1),
        )

        response = self.client.get(reverse("stats_view"), data={"source": "asana"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["selected_source"], "asana")
        self.assertEqual({row["source"] for row in response.context["source_totals"]}, {"asana"})
        self.assertEqual(sum(item["value"] for item in response.context["completion_daily_series"]), 1)

    def test_sync_source_rows_are_workspace_scoped(self):
        asana = self._create_connector(
            workspace=self.workspace,
            source="asana",
            display_name="Asana Main",
        )
        now = timezone.now()
        self._create_sync_job(
            workspace=self.workspace,
            connector_account=asana,
            finished_at=now - timedelta(minutes=2),
            output_summary={"results": [{"inserted": 7}]},
        )

        other_workspace = Workspace.objects.create(name="Other Workspace")
        other_user = self._create_user("other-user")
        WorkspaceMember.objects.create(
            workspace=other_workspace,
            user=other_user,
            role=WorkspaceMember.ROLE_OWNER,
        )
        other_connector = self._create_connector(
            workspace=other_workspace,
            source="todoist",
            display_name="Other Todoist",
        )
        Job.objects.create(
            workspace=other_workspace,
            connector_account=other_connector,
            job_type="sync",
            job_name="sync_connector",
            status=Job.STATUS_SUCCESS,
            started_at=now - timedelta(minutes=4),
            finished_at=now - timedelta(minutes=3),
            input_params={"source": "todoist"},
            output_summary={"results": [{"inserted": 12}]},
            created_by=other_user,
        )

        response = self.client.get(reverse("stats_view"))

        self.assertEqual(response.status_code, 200)
        row_sources = {row["source"] for row in response.context["sync_source_rows"]}
        self.assertIn("asana", row_sources)
        self.assertNotIn("todoist", row_sources)

    def test_sync_source_rows_use_latest_finished_job_and_summary_count(self):
        connector = self._create_connector(
            workspace=self.workspace,
            source="asana",
            display_name="Asana Main",
        )
        now = timezone.now()
        self._create_sync_job(
            workspace=self.workspace,
            connector_account=connector,
            finished_at=now - timedelta(hours=2),
            output_summary={"results": [{"inserted": 2}]},
        )
        self._create_sync_job(
            workspace=self.workspace,
            connector_account=connector,
            finished_at=now - timedelta(minutes=15),
            output_summary={"results": [{"inserted": 9}]},
        )

        response = self.client.get(reverse("stats_view"))

        row = next(item for item in response.context["sync_source_rows"] if item["source"] == "asana")
        self.assertEqual(row["last_sync_event_count"], 9)

    def test_sync_source_rows_fallback_to_none_when_summary_missing_count(self):
        connector = self._create_connector(
            workspace=self.workspace,
            source="habitica",
            display_name="Habitica Main",
        )
        now = timezone.now()
        self._create_sync_job(
            workspace=self.workspace,
            connector_account=connector,
            finished_at=now - timedelta(minutes=10),
            output_summary={"results": [{"skipped": 3}]},
        )

        response = self.client.get(reverse("stats_view"))

        row = next(item for item in response.context["sync_source_rows"] if item["source"] == "habitica")
        self.assertIsNone(row["last_sync_event_count"])
