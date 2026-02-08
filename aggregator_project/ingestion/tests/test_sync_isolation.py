from __future__ import annotations

from unittest.mock import patch

from django.test import TestCase, override_settings

from connectors.forms import AsanaConnectForm
from connectors.models import ConnectorAccount
from core.constants import SOURCE_ASANA
from events.models import Event
from ingestion.providers import ProviderSpec
from ingestion.services import sync as sync_service
from workspaces.models import Workspace


@override_settings(
    DATABASES={
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": ":memory:",
        }
    }
)
class SyncIsolationTests(TestCase):
    def test_sync_writes_only_to_target_workspace(self):
        workspace_a = Workspace.objects.create(name="Workspace A")
        workspace_b = Workspace.objects.create(name="Workspace B")

        raw_items = [{"gid": "123", "name": "Test Task", "completed": False}]

        class StubClient:
            def __init__(self, _account):
                self.account = _account

            def fetch_since(self, since=None):
                return raw_items

        def stub_normalizer(raw):
            return {
                "source": SOURCE_ASANA,
                "source_entity_type": "task",
                "source_entity_id": raw.get("gid"),
                "event_type": "task_updated",
                "title": raw.get("name"),
                "description": None,
                "start_time": None,
                "end_time": None,
                "metric_type": None,
                "metric_value": None,
                "metric_unit": None,
                "external_status": "open",
                "source_event_version": raw.get("version") or "v1",
                "raw": raw,
            }

        def ok_credentials(_credentials):
            return True, "ok"

        spec = ProviderSpec(
            source="asana",
            label="Asana",
            client_factory=lambda _account: StubClient(_account),
            normalizer=stub_normalizer,
            required_fields=[],
            auth_type="api_token",
            validate_credentials=ok_credentials,
            form_class=AsanaConnectForm,
            icon="bi-kanban",
        )

        def stub_get_provider_spec(source: str):
            return spec if source == "asana" else None

        account_a = ConnectorAccount.objects.create(
            workspace=workspace_a,
            source=SOURCE_ASANA,
            display_name="Asana A",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )

        with patch("ingestion.services.sync.get_provider_spec", stub_get_provider_spec):
            sync_service.sync_connector_account(workspace_a, account_a)

        self.assertEqual(Event.objects.for_workspace(workspace_a).count(), 1)
        self.assertEqual(Event.objects.for_workspace(workspace_b).count(), 0)

    def test_sync_rejects_mismatched_workspace(self):
        workspace_a = Workspace.objects.create(name="Workspace A")
        workspace_b = Workspace.objects.create(name="Workspace B")

        class StubClient:
            def __init__(self, _account):
                self.account = _account

        def stub_normalizer(raw):
            return {
                "source": SOURCE_ASANA,
                "source_entity_type": "task",
                "source_entity_id": raw.get("gid"),
                "event_type": "task_updated",
                "title": raw.get("name"),
                "description": None,
                "start_time": None,
                "end_time": None,
                "metric_type": None,
                "metric_value": None,
                "metric_unit": None,
                "external_status": "open",
                "source_event_version": raw.get("version") or "v1",
                "raw": raw,
            }

        def ok_credentials(_credentials):
            return True, "ok"

        spec = ProviderSpec(
            source="asana",
            label="Asana",
            client_factory=lambda _account: StubClient(_account),
            normalizer=stub_normalizer,
            required_fields=[],
            auth_type="api_token",
            validate_credentials=ok_credentials,
            form_class=AsanaConnectForm,
            icon="bi-kanban",
        )

        def stub_get_provider_spec(source: str):
            return spec if source == "asana" else None

        account_b = ConnectorAccount.objects.create(
            workspace=workspace_b,
            source=SOURCE_ASANA,
            display_name="Asana B",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )

        with patch("ingestion.services.sync.get_provider_spec", stub_get_provider_spec):
            with self.assertRaises(ValueError):
                sync_service.sync_connector_account(workspace_a, account_b)

        self.assertEqual(Event.objects.for_workspace(workspace_a).count(), 0)
