from __future__ import annotations

from unittest.mock import patch

from django.test import TestCase, override_settings

from connectors.forms import AsanaConnectForm
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
            def __init__(self, _workspace):
                self.workspace = _workspace

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
            client_factory=lambda _workspace: StubClient(_workspace),
            normalizer=stub_normalizer,
            required_fields=[],
            auth_type="api_token",
            validate_credentials=ok_credentials,
            form_class=AsanaConnectForm,
            icon="bi-kanban",
        )

        def stub_get_provider_spec(source: str):
            return spec if source == "asana" else None

        with patch("ingestion.services.sync.get_provider_spec", stub_get_provider_spec):
            sync_service.sync_source("asana", workspace_a)

        self.assertEqual(Event.objects.for_workspace(workspace_a).count(), 1)
        self.assertEqual(Event.objects.for_workspace(workspace_b).count(), 0)

    def test_sync_fails_without_connector_account(self):
        workspace = Workspace.objects.create(name="Workspace A")

        class StubClient:
            def __init__(self, _workspace):
                raise ValueError("No active connector account found.")

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
            client_factory=lambda _workspace: StubClient(_workspace),
            normalizer=stub_normalizer,
            required_fields=[],
            auth_type="api_token",
            validate_credentials=ok_credentials,
            form_class=AsanaConnectForm,
            icon="bi-kanban",
        )

        def stub_get_provider_spec(source: str):
            return spec if source == "asana" else None

        with patch("ingestion.services.sync.get_provider_spec", stub_get_provider_spec):
            run = sync_service.sync_source("asana", workspace)

        self.assertEqual(run.status, run.STATUS_FAILURE)
        self.assertEqual(Event.objects.for_workspace(workspace).count(), 0)
