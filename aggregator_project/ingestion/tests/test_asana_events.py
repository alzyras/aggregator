from __future__ import annotations

from unittest.mock import patch

from cryptography.fernet import Fernet
from django.test import TestCase, override_settings

from connectors.encryption import encrypt_value
from connectors.models import ConnectorAccount
from events.models import Event
from ingestion.providers import ProviderSpec
from ingestion.services.sync import sync_connector_account
from providers.asana.client import AsanaClient
from providers.asana.settings import get_asana_settings
from providers.asana.normalizer import normalize_asana
from workspaces.models import Workspace


@override_settings(
    DATABASES={
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": ":memory:",
        }
    },
    ENCRYPTION_KEY=Fernet.generate_key(),
)
class AsanaEventTests(TestCase):
    def _build_spec(self):
        def ok_credentials(_credentials):
            return True, "ok"

        return ProviderSpec(
            source="asana",
            label="Asana",
            client_factory=lambda account: AsanaClient(account),
            normalizer=normalize_asana,
            required_fields=[],
            auth_type="api_token",
            validate_credentials=ok_credentials,
            form_class=None,
            icon="bi-kanban",
        )

    def _build_account(self, workspace: Workspace) -> ConnectorAccount:
        account = ConnectorAccount.objects.create(
            workspace=workspace,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=encrypt_value("token"),
            external_account_id="workspace-gid",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        # Enable task_state explicitly for tests (defaults are off).
        account.scopes = {"asana": {**get_asana_settings(None), "task_state_updated": True}}
        account.save(update_fields=["scopes"])
        return account

    def test_lifecycle_events_are_arbitrated_per_timestamp(self):
        workspace = Workspace.objects.create(name="Workspace")
        account = self._build_account(workspace)

        task = {
            "gid": "t1",
            "resource_type": "task",
            "name": "Task",
            "notes": "Notes",
            "created_at": "2025-01-01T12:00:00.000Z",
            "modified_at": "2025-01-02T12:00:00.000Z",
            "completed": True,
            "completed_at": "2025-01-03T12:00:00.000Z",
            "created_by": {"gid": "u1", "name": "Creator"},
            "completed_by": {"gid": "u2", "name": "Completer"},
        }

        def stub_fetch(_self, _user_id, _api_token, task_type):
            if task_type == "habits":
                return []
            return []

        spec = self._build_spec()
        task["__asana_settings"] = get_asana_settings(account.scopes)

        with patch("ingestion.services.sync.get_provider_spec", return_value=spec):
            with patch.object(AsanaClient, "fetch_since", return_value=[task]):
                sync_connector_account(workspace, account)

        events = Event.objects.for_workspace(workspace)
        self.assertEqual(events.count(), 3)
        self.assertEqual(events.filter(event_type="task_state").count(), 0)
        self.assertEqual(events.filter(event_type="task_completed").count(), 1)
        self.assertEqual(events.filter(event_type="task_created").count(), 1)
        self.assertEqual(events.filter(event_type="task_updated").count(), 1)

        completed_event = events.get(event_type="task_completed")
        self.assertEqual(completed_event.external_actor_id, "u2")
        self.assertEqual(completed_event.external_actor_display_name, "Completer")

    def test_dedupe_keeps_one_updated_event_per_timestamp(self):
        workspace = Workspace.objects.create(name="Workspace")
        account = self._build_account(workspace)

        task = {
            "gid": "t2",
            "resource_type": "task",
            "name": "Task",
            "notes": "Notes",
            "created_at": "2025-01-01T12:00:00.000Z",
            "modified_at": "2025-01-02T12:00:00.000Z",
            "completed": False,
            "completed_at": None,
        }

        spec = self._build_spec()
        task["__asana_settings"] = get_asana_settings(account.scopes)

        with patch("ingestion.services.sync.get_provider_spec", return_value=spec):
            with patch.object(AsanaClient, "fetch_since", return_value=[task]):
                sync_connector_account(workspace, account)
                sync_connector_account(workspace, account)

        events = Event.objects.for_workspace(workspace)
        self.assertEqual(events.filter(event_type="task_updated").count(), 1)
        self.assertEqual(events.filter(event_type="task_state").count(), 0)

    def test_source_entity_type(self):
        workspace = Workspace.objects.create(name="Workspace")
        account = self._build_account(workspace)

        task = {
            "gid": "t3",
            "resource_type": "task",
            "name": "Task",
            "notes": "Notes",
            "created_at": "2025-01-01T12:00:00.000Z",
            "modified_at": "2025-01-02T12:00:00.000Z",
            "completed": False,
        }

        spec = self._build_spec()

        with patch("ingestion.services.sync.get_provider_spec", return_value=spec):
            with patch.object(AsanaClient, "fetch_since", return_value=[task]):
                sync_connector_account(workspace, account)

        event = Event.objects.for_workspace(workspace).first()
        self.assertEqual(event.source_entity_type, "task")
