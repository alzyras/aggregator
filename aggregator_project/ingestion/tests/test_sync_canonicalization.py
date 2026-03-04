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
class SyncCanonicalizationTests(TestCase):
    def _build_spec(self, raw_items):
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
                "event_type": raw.get("event_type"),
                "title": raw.get("name"),
                "description": None,
                "start_time": None,
                "end_time": None,
                "metric_type": None,
                "metric_value": None,
                "metric_unit": None,
                "external_status": raw.get("external_status"),
                "source_event_version": raw.get("version"),
                "raw": raw,
            }

        def ok_credentials(_credentials):
            return True, "ok"

        return ProviderSpec(
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

    def _sync(self, raw_items):
        workspace = Workspace.objects.create(name="Workspace")
        account = ConnectorAccount.objects.create(
            workspace=workspace,
            source=SOURCE_ASANA,
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        spec = self._build_spec(raw_items)

        def stub_get_provider_spec(source: str):
            return spec if source == "asana" else None

        with patch("ingestion.services.sync.get_provider_spec", stub_get_provider_spec):
            sync_service.sync_connector_account(workspace, account)
        return workspace

    def test_legacy_event_type_is_mapped_to_canonical(self):
        workspace = self._sync(
            [
                {
                    "gid": "task-1",
                    "name": "Task 1",
                    "event_type": "daily_completed",
                    "external_status": "completed",
                    "version": "v1",
                }
            ]
        )

        event = Event.objects.for_workspace(workspace).get()
        self.assertEqual(event.event_type, "task_completed")
        self.assertEqual(event.external_status, "completed")

    def test_unknown_event_type_falls_back_to_task_updated(self):
        workspace = self._sync(
            [
                {
                    "gid": "task-2",
                    "name": "Task 2",
                    "event_type": "some_vendor_event",
                    "external_status": "custom_state",
                    "version": "v1",
                }
            ]
        )

        event = Event.objects.for_workspace(workspace).get()
        self.assertEqual(event.event_type, "task_updated")
        self.assertEqual(event.external_status, "custom_state")
