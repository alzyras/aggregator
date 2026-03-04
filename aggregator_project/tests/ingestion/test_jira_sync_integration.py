from __future__ import annotations

from unittest.mock import patch

from cryptography.fernet import Fernet
from django.test import TestCase, override_settings

from connectors.encryption import encrypt_value
from connectors.models import ConnectorAccount
from events.models import Event
from ingestion.providers import get_provider_spec
from ingestion.services.sync import sync_connector_account
from providers.jira.client import JiraClient
from workspaces.models import Workspace


@override_settings(
    ENCRYPTION_KEY=Fernet.generate_key(),
    DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
)
class JiraSyncIntegrationTests(TestCase):
    def setUp(self) -> None:
        self.workspace = Workspace.objects.create(name="Workspace")
        self.account_a = self._create_account("Jira A", "https://a.atlassian.net")
        self.account_b = self._create_account("Jira B", "https://b.atlassian.net")

    def _create_account(self, name: str, base_url: str) -> ConnectorAccount:
        return ConnectorAccount.objects.create(
            workspace=self.workspace,
            source="jira",
            display_name=name,
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=encrypt_value("token"),
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
            config={
                "jira": {
                    "deployment_type": "cloud",
                    "auth_method": "cloud_api_token",
                    "base_url": base_url,
                    "email": "dev@example.com",
                    "include_changelog": True,
                    "include_worklogs": False,
                    "emit_worklog_metrics": False,
                    "emit_task_created": True,
                    "emit_task_updated": True,
                    "emit_task_completed": True,
                    "emit_task_reopened": True,
                    "emit_task_deleted": False,
                    "emit_task_state": False,
                    "project_keys": [],
                    "jql_filter": "ORDER BY updated DESC",
                    "issue_types": [],
                    "include_status_categories": ["todo", "in_progress", "done"],
                    "timezone": "UTC",
                    "initial_backfill_days": 365,
                    "incremental_lookback_minutes": 30,
                    "page_size": 100,
                }
            },
        )

    def _issue(self, entity_id: str, key: str) -> dict:
        return {
            "id": entity_id,
            "key": key,
            "fields": {
                "summary": key,
                "description": "desc",
                "created": "2026-02-01T09:00:00.000+0000",
                "updated": "2026-02-02T09:00:00.000+0000",
                "status": {
                    "name": "In Progress",
                    "statusCategory": {"key": "indeterminate", "name": "In Progress"},
                },
                "assignee": {"accountId": "u1", "displayName": "User One"},
                "reporter": {"accountId": "u2", "displayName": "User Two"},
            },
            "changelog": {"histories": []},
        }

    def test_sync_inserts_events_for_each_connector_account(self):
        def _fetch_since(client_self, since=None):  # noqa: ANN001
            if client_self.account.id == self.account_a.id:
                return [self._issue("1001", "ENG-1")]
            return [self._issue("2001", "OPS-1")]

        with patch.object(JiraClient, "fetch_since", _fetch_since):
            stats_a = sync_connector_account(self.workspace, self.account_a, full_sync=True)
            stats_b = sync_connector_account(self.workspace, self.account_b, full_sync=True)

        self.assertGreater(stats_a["inserted"], 0)
        self.assertGreater(stats_b["inserted"], 0)
        account_a_events = Event.objects.for_workspace(self.workspace).filter(
            connector_account=self.account_a
        )
        account_b_events = Event.objects.for_workspace(self.workspace).filter(
            connector_account=self.account_b
        )
        self.assertTrue(account_a_events.exists())
        self.assertTrue(account_b_events.exists())

    def test_existing_providers_still_registered(self):
        self.assertIsNotNone(get_provider_spec("asana"))
        self.assertIsNotNone(get_provider_spec("habitica"))
        self.assertIsNotNone(get_provider_spec("todoist"))
        self.assertIsNotNone(get_provider_spec("google_fit"))
        self.assertIsNotNone(get_provider_spec("jira"))
