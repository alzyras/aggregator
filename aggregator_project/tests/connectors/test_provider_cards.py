from __future__ import annotations

from dataclasses import replace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from connectors.forms import AsanaConnectForm
from connectors.models import ConnectorAccount
from events.models import Event
from ingestion.models import Job
from ingestion.providers import ProviderSpec, get_provider_specs
from workspaces.models import Workspace, WorkspaceMember


class ProviderCardTests(TestCase):
    def setUp(self) -> None:
        self.user = self._create_user("tester")
        self.client.force_login(self.user)
        self.workspace = Workspace.objects.create(name="Workspace")
        WorkspaceMember.objects.create(
            workspace=self.workspace,
            user=self.user,
            role=WorkspaceMember.ROLE_OWNER,
        )

    def _create_user(self, username: str):
        user_model = get_user_model()
        return user_model.objects.create_user(
            username=username,
            email=f"{username}@example.com",
            password="password123",
        )

    def _stub_spec(self) -> ProviderSpec:
        base_spec = next(
            spec for spec in get_provider_specs() if spec.source == "asana"
        )
        return replace(base_spec, form_class=AsanaConnectForm)

    def test_provider_specs_enabled_by_default(self):
        spec = self._stub_spec()

        with patch("connectors.views.get_provider_specs", return_value=[spec]):
            response = self.client.get(reverse("plugins_view"))

        self.assertEqual(response.status_code, 200)
        specs = response.context["provider_specs"]
        self.assertEqual(len(specs), 1)
        self.assertTrue(specs[0]["enabled"])

    def test_provider_specs_disabled_when_not_enabled(self):
        spec = self._stub_spec()

        with patch("connectors.views.get_provider_specs", return_value=[spec]):
            with patch.dict("os.environ", {"ENABLED_CONNECTORS": "todoist"}):
                response = self.client.get(reverse("plugins_view"))

        specs = response.context["provider_specs"]
        self.assertFalse(specs[0]["enabled"])

    def test_disabled_provider_cannot_be_connected_by_direct_post(self):
        spec = self._stub_spec()

        with patch("connectors.views.get_provider_specs", return_value=[spec]):
            with patch.dict("os.environ", {"ENABLED_CONNECTORS": "todoist"}):
                response = self.client.post(
                    reverse("connect_provider", args=["asana"]),
                    data={
                        "display_name": "Work",
                        "access_token": "token",
                        "workspace_gids": "12345",
                    },
                )

        self.assertEqual(response.status_code, 302)
        self.assertFalse(
            ConnectorAccount.objects.filter(
                workspace=self.workspace,
                source="asana",
            ).exists()
        )

    def test_disabled_provider_cannot_queue_sync(self):
        account = ConnectorAccount.objects.create(
            workspace=self.workspace,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )

        with patch.dict("os.environ", {"ENABLED_CONNECTORS": "todoist"}):
            response = self.client.post(
                reverse("sync_connector_account", args=[account.id])
            )

        self.assertEqual(response.status_code, 302)
        self.assertFalse(Job.objects.filter(connector_account=account).exists())

    def test_connected_plugins_show_status(self):
        spec = self._stub_spec()
        ConnectorAccount.objects.create(
            workspace=self.workspace,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )

        with patch("connectors.views.get_provider_specs", return_value=[spec]):
            response = self.client.get(reverse("plugins_view"))

        rows = response.context["connector_rows"]
        self.assertEqual(rows[0]["status_key"], ConnectorAccount.STATUS_CONNECTED)

    def test_plugins_view_shows_counts_and_sync_status(self):
        spec = self._stub_spec()
        account = ConnectorAccount.objects.create(
            workspace=self.workspace,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
            last_sync_status=ConnectorAccount.SYNC_STATUS_SUCCESS,
        )
        Event.objects.create(
            workspace=self.workspace,
            connector_account=account,
            source="asana",
            source_entity_type="task",
            source_entity_id="1",
            event_type="task_updated",
            raw={},
            dedupe_hash="hash-1",
        )

        with patch("connectors.views.get_provider_specs", return_value=[spec]):
            response = self.client.get(reverse("plugins_view"))

        rows = response.context["connector_rows"]
        self.assertEqual(rows[0]["event_count"], 1)
        self.assertEqual(rows[0]["last_sync_status"], "Success")
