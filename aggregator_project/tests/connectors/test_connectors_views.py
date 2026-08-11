from __future__ import annotations

from dataclasses import replace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from connectors.forms import AsanaConnectForm
from connectors.models import ConnectorAccount
from ingestion.models import Job
from ingestion.providers import ProviderSpec
from workspaces.models import Workspace, WorkspaceMember


class ConnectorViewsTests(TestCase):
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

    def _stub_spec(self, validate_result=(True, "ok")) -> ProviderSpec:
        base_spec = next(spec for spec in _real_specs() if spec.source == "asana")

        def validate(_credentials):
            return validate_result

        return replace(base_spec, validate_credentials=validate, form_class=AsanaConnectForm)

    def test_connect_provider_invalid_form(self):
        spec = self._stub_spec()

        with patch("connectors.views.get_provider_specs", return_value=[spec]):
            response = self.client.post(
                reverse("connect_provider", args=["asana"]), data={"display_name": "Work"}
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(ConnectorAccount.objects.count(), 0)

    def test_connect_provider_validation_failure(self):
        spec = self._stub_spec(validate_result=(False, "Missing access token."))

        with patch("connectors.views.get_provider_specs", return_value=[spec]):
            response = self.client.post(
                reverse("connect_provider", args=["asana"]),
                data={
                    "display_name": "Work",
                    "access_token": "token",
                    "workspace_gids": "12345",
                },
            )

        self.assertEqual(response.status_code, 200)
        account = ConnectorAccount.objects.filter(
            workspace=self.workspace, source="asana"
        ).first()
        self.assertIsNone(account)

    def test_connect_provider_verify_failure(self):
        spec = self._stub_spec()

        with patch("connectors.views.get_provider_specs", return_value=[spec]):
            with patch(
                "connectors.views.verify_credentials",
                return_value=(False, "Bad credentials\nmore"),
            ):
                response = self.client.post(
                    reverse("connect_provider", args=["asana"]),
                    data={
                        "display_name": "Work",
                        "access_token": "token",
                        "workspace_gids": "12345",
                    },
                )

        self.assertEqual(response.status_code, 302)
        account = ConnectorAccount.objects.get(
            workspace=self.workspace, source="asana"
        )
        self.assertEqual(account.status, ConnectorAccount.STATUS_ERROR)
        self.assertEqual(account.last_error, "Bad credentials")
        self.assertFalse(account.is_active)

    def test_connect_provider_success(self):
        spec = self._stub_spec()

        with patch("connectors.views.get_provider_specs", return_value=[spec]):
            with patch(
                "connectors.views.verify_credentials", return_value=(True, "ok")
            ):
                response = self.client.post(
                    reverse("connect_provider", args=["asana"]),
                    data={
                        "display_name": "Work",
                        "access_token": "token",
                        "workspace_gids": "12345",
                    },
                )

        self.assertEqual(response.status_code, 302)
        account = ConnectorAccount.objects.get(
            workspace=self.workspace, source="asana"
        )
        self.assertEqual(account.status, ConnectorAccount.STATUS_CONNECTED)
        self.assertIsNotNone(account.last_verified_at)
        self.assertTrue(account.is_active)

    def test_connecting_two_instances_creates_two_accounts(self):
        spec = self._stub_spec()

        with patch("connectors.views.get_provider_specs", return_value=[spec]):
            with patch(
                "connectors.views.verify_credentials", return_value=(True, "ok")
            ):
                self.client.post(
                    reverse("connect_provider", args=["asana"]),
                    data={
                        "display_name": "Work",
                        "access_token": "token",
                        "workspace_gids": "12345",
                    },
                )
                self.client.post(
                    reverse("connect_provider", args=["asana"]),
                    data={
                        "display_name": "Personal",
                        "access_token": "token-2",
                        "workspace_gids": "67890",
                    },
                )

        self.assertEqual(
            ConnectorAccount.objects.filter(workspace=self.workspace, source="asana").count(),
            2,
        )

    def test_remove_connector_account_clears_credentials(self):
        account = ConnectorAccount.objects.create(
            workspace=self.workspace,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )

        response = self.client.post(
            reverse("remove_connector_account", args=[account.id])
        )

        self.assertEqual(response.status_code, 302)
        account.refresh_from_db()
        self.assertEqual(account.status, ConnectorAccount.STATUS_REVOKED)
        self.assertFalse(account.is_active)

    def test_sync_now_creates_single_job(self):
        account = ConnectorAccount.objects.create(
            workspace=self.workspace,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )

        response = self.client.post(
            reverse("sync_connector_account", args=[account.id])
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(Job.objects.count(), 1)
        job = Job.objects.get()
        self.assertEqual(job.connector_account_id, account.id)

    def test_plugins_view_marks_reconnect_required_account(self):
        ConnectorAccount.objects.create(
            workspace=self.workspace,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"unreadable-token",
            status=ConnectorAccount.STATUS_ERROR,
            is_active=False,
            last_error=ConnectorAccount.RECONNECT_REQUIRED_ERROR,
        )

        response = self.client.get(reverse("plugins_view"))

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.context["connector_rows"][0]["requires_reconnect"])
        self.assertContains(response, "Reconnect")
        self.assertContains(response, "Credentials need reconnecting.")

    def test_edit_with_masked_unreadable_token_prompts_for_replacement(self):
        account = ConnectorAccount.objects.create(
            workspace=self.workspace,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"unreadable-token",
            status=ConnectorAccount.STATUS_ERROR,
            is_active=False,
            last_error=ConnectorAccount.RECONNECT_REQUIRED_ERROR,
        )

        response = self.client.post(
            reverse("update_connector_account", args=[account.id]),
            data={
                "display_name": "Asana",
                "access_token": "*********************",
                "workspace_gids": "12345",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Saved credentials cannot be read.")

    def test_reconnect_with_replacement_credentials_restores_account(self):
        account = ConnectorAccount.objects.create(
            workspace=self.workspace,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"unreadable-token",
            status=ConnectorAccount.STATUS_ERROR,
            is_active=False,
            last_error=ConnectorAccount.RECONNECT_REQUIRED_ERROR,
        )

        with patch("connectors.views.verify_credentials", return_value=(True, "ok")):
            response = self.client.post(
                reverse("update_connector_account", args=[account.id]),
                data={
                    "display_name": "Asana reconnected",
                    "access_token": "new-access-token",
                    "workspace_gids": "12345",
                },
            )

        account.refresh_from_db()
        self.assertEqual(response.status_code, 302)
        self.assertEqual(account.status, ConnectorAccount.STATUS_CONNECTED)
        self.assertTrue(account.is_active)
        self.assertIsNone(account.last_error)
        self.assertEqual(account.get_access_token(), "new-access-token")
        refresh_job = Job.objects.get(connector_account=account, job_type="sync")
        self.assertTrue(refresh_job.input_params["full_sync"])
        self.assertEqual(refresh_job.input_params["refresh_reason"], "reconnect")


def _real_specs():
    from ingestion.providers import get_provider_specs

    return get_provider_specs()
