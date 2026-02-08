from __future__ import annotations

import os

from cryptography.fernet import Fernet
from django.test import TestCase, override_settings
from django.utils import timezone

from connectors.encryption import decrypt_value, encrypt_value
from connectors.models import ConnectorAccount
from connectors.services import get_required_account
from providers.asana.client import AsanaClient
from workspaces.models import Workspace


@override_settings(ENCRYPTION_KEY=Fernet.generate_key())
class ConnectorTokenTests(TestCase):
    def setUp(self) -> None:
        self.workspace_a = Workspace.objects.create(name="Workspace A")
        self.workspace_b = Workspace.objects.create(name="Workspace B")

    def test_token_is_encrypted_at_rest(self):
        account = ConnectorAccount.objects.create(
            workspace=self.workspace_a,
            provider="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"",
        )
        account.set_access_token("plain-token")
        account.save(update_fields=["encrypted_access_token"])

        stored = ConnectorAccount.objects.get(id=account.id).encrypted_access_token
        self.assertIsInstance(stored, (bytes, bytearray))
        self.assertNotEqual(stored, b"plain-token")
        self.assertNotIn(b"plain-token", stored)
        self.assertEqual(decrypt_value(stored), "plain-token")

    def test_workspace_isolation_for_provider_tokens(self):
        account_a = ConnectorAccount.objects.create(
            workspace=self.workspace_a,
            provider="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=encrypt_value("token-a"),
        )
        account_b = ConnectorAccount.objects.create(
            workspace=self.workspace_b,
            provider="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=encrypt_value("token-b"),
        )

        self.assertNotEqual(account_a.workspace_id, account_b.workspace_id)
        self.assertEqual(
            ConnectorAccount.objects.filter(provider="asana").count(),
            2,
        )

    def test_revoked_account_cannot_be_used(self):
        ConnectorAccount.objects.create(
            workspace=self.workspace_a,
            provider="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=encrypt_value("token"),
            revoked_at=timezone.now(),
        )
        with self.assertRaises(ValueError):
            _ = get_required_account("asana", self.workspace_a)

    def test_no_env_fallback_for_tokens(self):
        os.environ["ASANA_ACCESS_TOKEN"] = "env-token"
        try:
            with self.assertRaises(ValueError):
                _ = AsanaClient(self.workspace_a)
        finally:
            os.environ.pop("ASANA_ACCESS_TOKEN", None)
