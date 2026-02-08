from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

from cryptography.fernet import Fernet
from django.test import TestCase, override_settings

from connectors.encryption import encrypt_value
from connectors.models import ConnectorAccount
from events.models import Event
from ingestion.normalizers.base import build_dedupe_hash
from ingestion.providers import ProviderSpec
from ingestion.services.sync import sync_connector_account
from providers.habitica.client import HabiticaClient
from providers.habitica.normalizer import normalize_habitica
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
class ActorFieldTests(TestCase):
    def _build_account(self, workspace: Workspace, source: str = "habitica") -> ConnectorAccount:
        return ConnectorAccount.objects.create(
            workspace=workspace,
            source=source,
            display_name=source.title(),
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=encrypt_value("token"),
            external_account_id="user-id",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )

    def test_habitica_actor_fields_persist(self):
        workspace = Workspace.objects.create(name="Workspace")
        account = self._build_account(workspace)

        def ok_credentials(_credentials):
            return True, "ok"

        spec = ProviderSpec(
            source="habitica",
            label="Habitica",
            client_factory=lambda acc: HabiticaClient(acc),
            normalizer=normalize_habitica,
            required_fields=[],
            auth_type="api_token",
            validate_credentials=ok_credentials,
            form_class=None,
            icon="bi-check2-circle",
        )

        habit = {
            "id": "h1",
            "type": "habit",
            "text": "Hydrate",
            "notes": "",
            "history": [
                {"date": 1700000000000, "value": 1},
            ],
        }
        user_profile = {
            "id": "user-id",
            "profile": {"name": "Test User"},
            "auth": {"local": {"username": "tester"}},
        }

        def stub_fetch_tasks(_self, _user_id, _api_token, task_type):
            if task_type == "habits":
                return [habit]
            return []

        with patch("ingestion.services.sync.get_provider_spec", return_value=spec):
            with patch.object(HabiticaClient, "_fetch_tasks", new=stub_fetch_tasks):
                with patch.object(HabiticaClient, "get_user_profile", return_value=user_profile):
                    sync_connector_account(workspace, account)

        event = Event.objects.for_workspace(workspace).get()
        self.assertEqual(event.external_actor_id, "user-id")
        self.assertEqual(event.external_actor_type, "user")
        self.assertEqual(event.external_actor_display_name, "Test User")
        self.assertEqual(event.external_actor_raw.get("id"), "user-id")

    def test_actor_fields_optional(self):
        workspace = Workspace.objects.create(name="Workspace")
        account = self._build_account(workspace, source="dummy")

        def ok_credentials(_credentials):
            return True, "ok"

        raw_items = [{"id": "1"}]

        class StubClient:
            def __init__(self, _account):
                self.account = _account

            def fetch_since(self, since=None):
                return raw_items

        def stub_normalizer(raw):
            return {
                "source": "dummy",
                "source_entity_type": "task",
                "source_entity_id": raw.get("id"),
                "event_type": "task_updated",
                "title": "X",
                "description": None,
                "start_time": None,
                "end_time": None,
                "metric_type": None,
                "metric_value": None,
                "metric_unit": None,
                "external_status": None,
                "source_event_version": "v1",
            }

        spec = ProviderSpec(
            source="dummy",
            label="Dummy",
            client_factory=lambda acc: StubClient(acc),
            normalizer=stub_normalizer,
            required_fields=[],
            auth_type="api_token",
            validate_credentials=ok_credentials,
            form_class=None,
            icon="bi-circle",
        )

        with patch("ingestion.services.sync.get_provider_spec", return_value=spec):
            sync_connector_account(workspace, account)

        event = Event.objects.for_workspace(workspace).get()
        self.assertIsNone(event.external_actor_id)
        self.assertIsNone(event.external_actor_type)
        self.assertIsNone(event.external_actor_display_name)
        self.assertIsNone(event.external_actor_raw)

    def test_dedupe_ignores_actor_fields(self):
        workspace = Workspace.objects.create(name="Workspace")
        account = self._build_account(workspace, source="dummy")

        def ok_credentials(_credentials):
            return True, "ok"

        raw_items = [
            {"id": "1", "actor_id": "a"},
            {"id": "1", "actor_id": "b"},
        ]

        class StubClient:
            def __init__(self, _account):
                self.account = _account

            def fetch_since(self, since=None):
                return raw_items

        def stub_normalizer(raw):
            return {
                "source": "dummy",
                "source_entity_type": "task",
                "source_entity_id": raw.get("id"),
                "event_type": "task_updated",
                "title": "X",
                "description": None,
                "start_time": None,
                "end_time": None,
                "metric_type": None,
                "metric_value": None,
                "metric_unit": None,
                "external_status": None,
                "source_event_version": "v1",
                "external_actor_id": raw.get("actor_id"),
                "external_actor_type": "user",
            }

        spec = ProviderSpec(
            source="dummy",
            label="Dummy",
            client_factory=lambda acc: StubClient(acc),
            normalizer=stub_normalizer,
            required_fields=[],
            auth_type="api_token",
            validate_credentials=ok_credentials,
            form_class=None,
            icon="bi-circle",
        )

        payload_a = stub_normalizer(raw_items[0])
        payload_b = stub_normalizer(raw_items[1])
        self.assertEqual(build_dedupe_hash(payload_a), build_dedupe_hash(payload_b))

        with patch("ingestion.services.sync.get_provider_spec", return_value=spec):
            stats = sync_connector_account(workspace, account)

        self.assertEqual(stats["inserted"], 1)
        self.assertEqual(Event.objects.for_workspace(workspace).count(), 1)

    def test_actor_queries_are_workspace_scoped(self):
        workspace_a = Workspace.objects.create(name="Workspace A")
        workspace_b = Workspace.objects.create(name="Workspace B")
        account_a = self._build_account(workspace_a, source="dummy")
        account_b = self._build_account(workspace_b, source="dummy")

        Event.objects.create(
            workspace=workspace_a,
            connector_account=account_a,
            source="dummy",
            source_entity_type="task",
            source_entity_id="1",
            event_type="task_updated",
            raw={},
            dedupe_hash="hash-a",
            external_actor_id="actor-1",
        )
        Event.objects.create(
            workspace=workspace_b,
            connector_account=account_b,
            source="dummy",
            source_entity_type="task",
            source_entity_id="2",
            event_type="task_updated",
            raw={},
            dedupe_hash="hash-b",
            external_actor_id="actor-1",
        )

        self.assertEqual(
            Event.objects.for_workspace(workspace_a)
            .filter(external_actor_id="actor-1")
            .count(),
            1,
        )
        self.assertEqual(
            Event.objects.for_workspace(workspace_b)
            .filter(external_actor_id="actor-1")
            .count(),
            1,
        )
