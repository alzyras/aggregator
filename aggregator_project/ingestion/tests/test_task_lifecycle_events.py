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
class TaskLifecycleEventTests(TestCase):
    def _build_spec(self, raw_items):
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
            }

        def ok_credentials(_credentials):
            return True, "ok"

        return ProviderSpec(
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

    def _sync_raw_items(self, workspace, raw_items):
        spec = self._build_spec(raw_items)

        def stub_get_provider_spec(source: str):
            return spec if source == "asana" else None

        with patch("ingestion.services.sync.get_provider_spec", stub_get_provider_spec):
            sync_service.sync_source("asana", workspace)

    def test_append_only_lifecycle_events(self):
        workspace = Workspace.objects.create(name="Test workspace")

        self._sync_raw_items(
            workspace,
            [
                {
                    "gid": "task-1",
                    "name": "Task",
                    "event_type": "task_created",
                    "external_status": "open",
                    "version": "v1",
                }
            ],
        )
        self._sync_raw_items(
            workspace,
            [
                {
                    "gid": "task-1",
                    "name": "Task",
                    "event_type": "task_completed",
                    "external_status": "completed",
                    "version": "v2",
                }
            ],
        )
        self._sync_raw_items(
            workspace,
            [
                {
                    "gid": "task-1",
                    "name": "Task",
                    "event_type": "task_deleted",
                    "external_status": "deleted",
                    "version": "v3",
                }
            ],
        )

        events = Event.objects.for_workspace(workspace).order_by("created_at")
        self.assertEqual(events.count(), 3)
        self.assertEqual(
            [event.event_type for event in events],
            ["task_created", "task_completed", "task_deleted"],
        )

    def test_existing_events_are_not_mutated(self):
        workspace = Workspace.objects.create(name="Test workspace")

        self._sync_raw_items(
            workspace,
            [
                {
                    "gid": "task-2",
                    "name": "Original",
                    "event_type": "task_created",
                    "external_status": "open",
                    "version": "v1",
                }
            ],
        )
        self._sync_raw_items(
            workspace,
            [
                {
                    "gid": "task-2",
                    "name": "Updated",
                    "event_type": "task_updated",
                    "external_status": "open",
                    "version": "v2",
                }
            ],
        )

        first_event = Event.objects.for_workspace(workspace).get(
            source_entity_id="task-2",
            source_event_version="v1",
        )
        self.assertEqual(first_event.title, "Original")
        self.assertEqual(Event.objects.for_workspace(workspace).count(), 2)

    def test_dedupe_same_lifecycle_event(self):
        workspace = Workspace.objects.create(name="Test workspace")
        raw = {
            "gid": "task-3",
            "name": "Duplicate",
            "event_type": "task_completed",
            "external_status": "completed",
            "version": "v1",
        }

        self._sync_raw_items(workspace, [raw])
        self._sync_raw_items(workspace, [raw])

        self.assertEqual(Event.objects.for_workspace(workspace).count(), 1)
