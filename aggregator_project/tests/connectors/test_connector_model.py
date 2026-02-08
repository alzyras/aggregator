from __future__ import annotations

from cryptography.fernet import Fernet
from django.db import IntegrityError
from django.test import TestCase, override_settings

from connectors.models import ConnectorAccount
from workspaces.models import Workspace


class ConnectorAccountModelTests(TestCase):
    def test_unique_account_per_workspace_source(self):
        workspace = Workspace.objects.create(name="Test workspace")
        ConnectorAccount.objects.create(
            workspace=workspace,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            credentials="",
        )

        with self.assertRaises(IntegrityError):
            ConnectorAccount.objects.create(
                workspace=workspace,
                source="asana",
                display_name="Asana 2",
                auth_type=ConnectorAccount.AUTH_API_TOKEN,
                credentials="",
            )

    def test_set_get_credentials_round_trip(self):
        workspace = Workspace.objects.create(name="Test workspace")
        account = ConnectorAccount.objects.create(
            workspace=workspace,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            credentials="",
        )
        payload = {"access_token": "secret-token"}

        account.set_credentials(payload)
        account.save(update_fields=["credentials", "credentials_encrypted"])

        self.assertEqual(account.get_credentials(), payload)

    def test_set_credentials_marks_encrypted_when_key_present(self):
        workspace = Workspace.objects.create(name="Test workspace")
        account = ConnectorAccount.objects.create(
            workspace=workspace,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            credentials="",
        )

        with override_settings(ENCRYPTION_KEY=Fernet.generate_key()):
            account.set_credentials({"token": "value"})
            self.assertTrue(account.credentials_encrypted)
