from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

from cryptography.fernet import Fernet
from django.test import TestCase, override_settings

from connectors.encryption import encrypt_value
from connectors.models import ConnectorAccount
from events.models import Event
from ingestion.providers import ProviderSpec
from ingestion.services.sync import sync_connector_account
from providers.todoist.normalizer import normalize_todoist
from providers.todoist.client import TodoistClient
from workspaces.models import Workspace


@override_settings(
    DATABASES={
        "default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}
    },
    ENCRYPTION_KEY=Fernet.generate_key(),
)
class IncrementalSyncTests(TestCase):
    def setUp(self):
        self.workspace = Workspace.objects.create(name="w")
        self.account = ConnectorAccount.objects.create(
            workspace=self.workspace,
            source="todoist",
            display_name="Todoist",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=encrypt_value("token"),
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )

    def _spec(self):
        def ok_creds(_):
            return True, "ok"

        return ProviderSpec(
            source="todoist",
            label="Todoist",
            client_factory=lambda acc: TodoistClient(acc),
            normalizer=normalize_todoist,
            required_fields=[],
            auth_type="api_token",
            validate_credentials=ok_creds,
            form_class=None,
            icon="bi-check2-square",
        )

    def test_incremental_uses_latest_event(self):
        # Seed one event with completed_at
        Event.objects.create(
            workspace=self.workspace,
            connector_account=self.account,
            source="todoist",
            source_entity_type="task",
            source_entity_id="1",
            event_type="task_completed",
            start_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
            external_status="completed",
            raw={},
            dedupe_hash="h1",
            source_event_version="2025-01-01T10:00:00Z",
        )

        newer_task = {
            "id": 2,
            "content": "New",
            "completed": False,
            "added_at": "2025-02-01T10:00:00Z",
        }
        older_task = {
            "id": 3,
            "content": "Old",
            "completed": False,
            "added_at": "2024-12-01T10:00:00Z",
        }

        spec = self._spec()
        with patch("ingestion.services.sync.get_provider_spec", return_value=spec):
            with patch.object(TodoistClient, "fetch_since", return_value=[newer_task, older_task]):
                stats = sync_connector_account(self.workspace, self.account, since=None)

        self.assertEqual(stats["inserted"], 1)
        self.assertEqual(Event.objects.for_workspace(self.workspace).filter(source_entity_id="2").count(), 1)
        self.assertEqual(Event.objects.for_workspace(self.workspace).filter(source_entity_id="3").count(), 0)

    def test_full_sync_ingests_all(self):
        task_a = {"id": 4, "content": "Old", "added_at": "2024-01-01T00:00:00Z"}
        task_b = {"id": 5, "content": "New", "added_at": "2025-01-01T00:00:00Z"}
        spec = self._spec()
        with patch("ingestion.services.sync.get_provider_spec", return_value=spec):
            with patch.object(TodoistClient, "fetch_since", return_value=[task_a, task_b]):
                stats = sync_connector_account(self.workspace, self.account, since=None)
        # Now simulate full sync (since=None bypassed in caller), here we pass since=None again
        self.assertGreaterEqual(stats["inserted"], 2)
