from __future__ import annotations

import copy
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone

from connectors.forms import AsanaConnectForm
from connectors.models import ConnectorAccount
from core.constants import SOURCE_ASANA
from events.models import Event
from ingestion.normalizers.base import build_dedupe_hash
from ingestion.providers import ProviderSpec
from ingestion.services.sync import sync_source
from workspaces.models import Workspace


class SyncServiceTests(TestCase):
    def _build_spec(self, raw_items, normalizer):
        class StubClient:
            def __init__(self, _workspace):
                self.workspace = _workspace

            def fetch_since(self, since=None):
                return raw_items

        def empty_credentials():
            return {}

        def ok_credentials(_credentials):
            return True, "ok"

        return ProviderSpec(
            source="asana",
            label="Asana",
            client_factory=lambda _workspace: StubClient(_workspace),
            normalizer=normalizer,
            required_fields=[],
            auth_type="api_token",
            env_credentials=empty_credentials,
            validate_credentials=ok_credentials,
            form_class=AsanaConnectForm,
            icon="bi-kanban",
        )

    def test_unknown_provider_marks_failure(self):
        workspace = Workspace.objects.create(name="Test workspace")

        with patch("ingestion.services.sync.get_provider_spec", return_value=None):
            run = sync_source("unknown", workspace)

        self.assertEqual(run.status, run.STATUS_FAILURE)
        self.assertIn("Unknown provider source", run.error)
        self.assertIsNotNone(run.finished_at)

    def test_skips_missing_source_entity_id(self):
        workspace = Workspace.objects.create(name="Test workspace")
        raw_items = [{"gid": None, "name": "Missing id"}]

        def normalizer(raw):
            return {
                "source": SOURCE_ASANA,
                "source_entity_type": "task",
                "source_entity_id": raw.get("gid"),
                "title": raw.get("name"),
                "description": None,
                "start_time": None,
                "end_time": None,
                "metric_type": None,
                "metric_value": None,
                "metric_unit": None,
                "status": "open",
            }

        spec = self._build_spec(raw_items, normalizer)

        with patch("ingestion.services.sync.get_provider_spec", return_value=spec):
            run = sync_source("asana", workspace)

        self.assertEqual(Event.objects.for_workspace(workspace).count(), 0)
        self.assertEqual(run.stats.get("inserted"), 0)
        self.assertEqual(run.stats.get("skipped"), 1)
        self.assertEqual(run.stats.get("total"), 1)

    def test_duplicate_event_increments_skipped(self):
        workspace = Workspace.objects.create(name="Test workspace")
        raw_items = [{"gid": "123", "name": "Duplicate"}]

        def normalizer(raw):
            return {
                "source": SOURCE_ASANA,
                "source_entity_type": "task",
                "source_entity_id": raw.get("gid"),
                "title": raw.get("name"),
                "description": None,
                "start_time": None,
                "end_time": None,
                "metric_type": None,
                "metric_value": None,
                "metric_unit": None,
                "status": "open",
            }

        normalized = normalizer(raw_items[0])
        dedupe_hash = build_dedupe_hash(normalized)
        Event.objects.create(
            workspace=workspace,
            dedupe_hash=dedupe_hash,
            raw={},
            **normalized,
        )

        spec = self._build_spec(raw_items, normalizer)

        with patch("ingestion.services.sync.get_provider_spec", return_value=spec):
            run = sync_source("asana", workspace)

        self.assertEqual(Event.objects.for_workspace(workspace).count(), 1)
        self.assertEqual(run.stats.get("inserted"), 0)
        self.assertEqual(run.stats.get("skipped"), 1)

    def test_success_updates_last_sync_at(self):
        workspace = Workspace.objects.create(name="Test workspace")
        account = ConnectorAccount.objects.create(
            workspace=workspace,
            source=SOURCE_ASANA,
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            credentials="",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )
        raw_items = []

        def normalizer(_raw):
            return {}

        spec = self._build_spec(raw_items, normalizer)

        with patch("ingestion.services.sync.get_provider_spec", return_value=spec):
            run = sync_source("asana", workspace)

        account.refresh_from_db()
        self.assertEqual(run.status, run.STATUS_SUCCESS)
        self.assertIsNotNone(account.last_sync_at)
        self.assertLessEqual(account.last_sync_at, timezone.now())

    def test_raw_payload_is_preserved(self):
        workspace = Workspace.objects.create(name="Test workspace")
        expected_raw = {"gid": "123", "name": "Original", "extra": {"flag": True}}
        raw_items = [copy.deepcopy(expected_raw)]

        def normalizer(raw):
            raw.pop("extra", None)
            return {
                "source": SOURCE_ASANA,
                "source_entity_type": "task",
                "source_entity_id": raw.get("gid"),
                "title": raw.get("name"),
                "description": None,
                "start_time": None,
                "end_time": None,
                "metric_type": None,
                "metric_value": None,
                "metric_unit": None,
                "status": "open",
            }

        spec = self._build_spec(raw_items, normalizer)

        with patch("ingestion.services.sync.get_provider_spec", return_value=spec):
            sync_source("asana", workspace)

        event = Event.objects.for_workspace(workspace).get()
        self.assertEqual(event.raw, expected_raw)
