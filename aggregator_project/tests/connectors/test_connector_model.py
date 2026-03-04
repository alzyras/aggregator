from __future__ import annotations

from cryptography.fernet import Fernet
from django.test import TestCase, override_settings

from connectors.models import ConnectorAccount
from workspaces.models import Workspace


@override_settings(ENCRYPTION_KEY=Fernet.generate_key())
class ConnectorAccountModelTests(TestCase):
    def test_multiple_accounts_per_source_allowed(self):
        workspace = Workspace.objects.create(name="Test workspace")
        ConnectorAccount.objects.create(
            workspace=workspace,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"",
        )
        ConnectorAccount.objects.create(
            workspace=workspace,
            source="asana",
            display_name="Asana 2",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"",
        )

        self.assertEqual(
            ConnectorAccount.objects.filter(workspace=workspace, source="asana").count(),
            2,
        )

    def test_access_token_round_trip(self):
        workspace = Workspace.objects.create(name="Test workspace")
        account = ConnectorAccount.objects.create(
            workspace=workspace,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"",
        )
        account.set_access_token("secret-token")
        account.save(update_fields=["encrypted_access_token"])

        account.refresh_from_db()
        self.assertEqual(account.get_access_token(), "secret-token")
