from __future__ import annotations

from unittest.mock import patch

from cryptography.fernet import Fernet
from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.urls import reverse

from connectors.models import ConnectorAccount
from ingestion.providers import get_provider_spec
from providers.github_issues.settings import MASKED_TOKEN as GITHUB_MASK
from workspaces.models import Workspace, WorkspaceMember


@override_settings(ENCRYPTION_KEY=Fernet.generate_key())
class ModularProviderContractTests(TestCase):
    def setUp(self) -> None:
        self.user = get_user_model().objects.create_user(
            username="provider-contract",
            password="password123",
        )
        self.workspace = Workspace.objects.create(name="Provider contract")
        WorkspaceMember.objects.create(
            workspace=self.workspace,
            user=self.user,
            role=WorkspaceMember.ROLE_OWNER,
        )
        self.client.force_login(self.user)

    def test_every_provider_owns_connector_lifecycle_hooks(self):
        for source in (
            "asana",
            "todoist",
            "habitica",
            "jira",
            "google_fit",
            "github",
            "linear",
            "clickup",
            "trello",
        ):
            spec = get_provider_spec(source)
            self.assertIsNotNone(spec, source)
            self.assertIsNotNone(spec.connection_verifier, source)
            self.assertIsNotNone(spec.credentials_applier, source)
            self.assertIsNotNone(spec.form_initial_factory, source)
            self.assertIsNotNone(spec.masked_credentials_resolver, source)

    @patch("connectors.views.verify_credentials", return_value=(True, "ok"))
    def test_github_connect_and_edit_use_provider_owned_hooks(self, _verify):
        response = self.client.post(
            reverse("connect_provider", args=["github"]),
            data={
                "display_name": "Work GitHub",
                "api_token": "original-token",
                "repositories": "acme/app",
                "include_closed": "on",
                "emit_task_created": "on",
                "emit_task_updated": "on",
                "emit_task_completed": "on",
            },
        )
        self.assertEqual(response.status_code, 302)
        account = ConnectorAccount.objects.get(
            workspace=self.workspace, source="github"
        )
        self.assertEqual(account.get_access_token(), "original-token")
        self.assertEqual(account.scopes["github"]["repositories"], ["acme/app"])

        response = self.client.post(
            reverse("update_connector_account", args=[account.id]),
            data={
                "display_name": "Updated GitHub",
                "api_token": GITHUB_MASK,
                "repositories": "acme/app, acme/api",
                "include_closed": "on",
                "emit_task_created": "on",
                "emit_task_updated": "on",
                "emit_task_completed": "on",
            },
        )
        self.assertEqual(response.status_code, 302)
        account.refresh_from_db()
        self.assertEqual(account.get_access_token(), "original-token")
        self.assertEqual(
            account.scopes["github"]["repositories"], ["acme/app", "acme/api"]
        )
