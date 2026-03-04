from __future__ import annotations

from unittest.mock import patch

from cryptography.fernet import Fernet
from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.urls import reverse

from connectors.encryption import encrypt_value
from connectors.models import ConnectorAccount
from providers.jira.settings import MASKED_SECRET
from workspaces.models import Workspace, WorkspaceMember


@override_settings(ENCRYPTION_KEY=Fernet.generate_key())
class JiraConnectorViewTests(TestCase):
    def setUp(self) -> None:
        user_model = get_user_model()
        self.user = user_model.objects.create_user(
            username="jira-user",
            email="jira-user@example.com",
            password="password123",
        )
        self.client.force_login(self.user)
        self.workspace = Workspace.objects.create(name="Workspace")
        WorkspaceMember.objects.create(
            workspace=self.workspace,
            user=self.user,
            role=WorkspaceMember.ROLE_OWNER,
        )

    def _connect_payload(self) -> dict[str, str]:
        return {
            "display_name": "Jira Main",
            "deployment_type": "cloud",
            "base_url": "https://example.atlassian.net",
            "auth_method": "cloud_api_token",
            "email": "dev@example.com",
            "api_token": "secret-token",
            "project_keys": "ENG,OPS",
            "jql_filter": "project = ENG",
            "include_status_categories": ["todo", "in_progress", "done"],
            "issue_types": ["Task", "Bug"],
            "include_changelog": "on",
            "include_worklogs": "",
            "initial_backfill_days": "365",
            "incremental_lookback_minutes": "30",
            "page_size": "100",
            "timezone": "UTC",
        }

    @patch("connectors.views.verify_credentials", return_value=(True, "ok"))
    def test_connect_jira_persists_config_and_secret(self, _mock_verify):
        response = self.client.post(
            reverse("connect_provider", args=["jira"]),
            data=self._connect_payload(),
        )
        self.assertEqual(response.status_code, 302)
        account = ConnectorAccount.objects.get(workspace=self.workspace, source="jira")
        self.assertEqual(account.auth_type, ConnectorAccount.AUTH_API_TOKEN)
        self.assertEqual(account.get_access_token(), "secret-token")
        self.assertEqual(account.config["jira"]["base_url"], "https://example.atlassian.net")
        self.assertEqual(account.config["jira"]["project_keys"], ["ENG", "OPS"])

    @patch("connectors.views.verify_credentials", return_value=(True, "ok"))
    def test_edit_jira_keeps_masked_secret(self, _mock_verify):
        account = ConnectorAccount.objects.create(
            workspace=self.workspace,
            source="jira",
            display_name="Jira",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=encrypt_value("existing-token"),
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
            config={
                "jira": {
                    "deployment_type": "cloud",
                    "auth_method": "cloud_api_token",
                    "base_url": "https://example.atlassian.net",
                    "email": "dev@example.com",
                    "project_keys": ["ENG"],
                    "jql_filter": "project = ENG",
                    "issue_types": [],
                    "include_status_categories": ["todo", "in_progress", "done"],
                    "exclude_done_before_days": None,
                    "timezone": "UTC",
                    "include_comments": False,
                    "include_worklogs": False,
                    "include_changelog": True,
                    "include_sprints": False,
                    "include_attachments_metadata": False,
                    "include_linked_issues": False,
                    "emit_task_created": True,
                    "emit_task_updated": True,
                    "emit_task_completed": True,
                    "emit_task_reopened": True,
                    "emit_task_deleted": False,
                    "emit_task_state": False,
                    "emit_worklog_metrics": False,
                    "full_sync": False,
                    "initial_backfill_days": 365,
                    "incremental_lookback_minutes": 30,
                    "page_size": 100,
                }
            },
        )
        payload = self._connect_payload()
        payload["display_name"] = "Jira Updated"
        payload["api_token"] = MASKED_SECRET
        payload["jql_filter"] = "project in (ENG, OPS)"
        response = self.client.post(
            reverse("update_connector_account", args=[account.id]),
            data=payload,
        )

        self.assertEqual(response.status_code, 302)
        account.refresh_from_db()
        self.assertEqual(account.get_access_token(), "existing-token")
        self.assertEqual(account.display_name, "Jira Updated")
        self.assertEqual(account.config["jira"]["jql_filter"], "project in (ENG, OPS)")

