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
class HabiticaOccurrenceTests(TestCase):
    def _build_spec(self):
        def ok_credentials(_credentials):
            return True, "ok"

        return ProviderSpec(
            source="habitica",
            label="Habitica",
            client_factory=lambda account: HabiticaClient(account),
            normalizer=normalize_habitica,
            required_fields=[],
            auth_type="api_token",
            validate_credentials=ok_credentials,
            form_class=None,
            icon="bi-check2-circle",
        )

    def _build_account(self, workspace: Workspace) -> ConnectorAccount:
        return ConnectorAccount.objects.create(
            workspace=workspace,
            source="habitica",
            display_name="Habitica",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=encrypt_value("token"),
            external_account_id="user-id",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )

    def test_habit_and_daily_history_generates_multiple_events(self):
        workspace = Workspace.objects.create(name="Workspace")
        account = self._build_account(workspace)

        habit = {
            "id": "h1",
            "type": "habit",
            "text": "Drink water",
            "notes": "",
            "history": [
                {"date": 1700000000000, "value": 1},
                {"date": 1700003600000, "value": -1},
            ],
        }
        daily = {
            "id": "d1",
            "type": "daily",
            "text": "Read",
            "notes": "",
            "history": [
                {"date": 1700007200000, "value": 1, "completed": True},
                {"date": 1700010800000, "value": 1, "completed": True},
            ],
        }
        todo = {
            "id": "t1",
            "type": "todo",
            "text": "File taxes",
            "notes": "",
            "completed": True,
            "dateCompleted": "2025-01-02T12:00:00.000Z",
        }

        def stub_fetch_tasks(_self, _user_id, _api_token, task_type):
            if task_type == "habits":
                return [habit]
            if task_type == "dailys":
                return [daily]
            if task_type == "completedTodos":
                return [todo]
            return []

        spec = self._build_spec()

        with patch("ingestion.services.sync.get_provider_spec", return_value=spec):
            with patch.object(HabiticaClient, "_fetch_tasks", new=stub_fetch_tasks):
                with patch.object(HabiticaClient, "get_user_profile", return_value={}):
                    sync_connector_account(workspace, account)

        events = Event.objects.for_workspace(workspace).order_by("start_time")
        self.assertEqual(events.count(), 5)

        habit_events = events.filter(source_entity_type="habit")
        self.assertEqual(habit_events.count(), 2)
        expected_habit_times = {
            datetime.fromtimestamp(1700000000000 / 1000, tz=timezone.utc),
            datetime.fromtimestamp(1700003600000 / 1000, tz=timezone.utc),
        }
        self.assertEqual({e.start_time for e in habit_events}, expected_habit_times)

        daily_events = events.filter(source_entity_type="daily")
        self.assertEqual(daily_events.count(), 2)
        expected_daily_times = {
            datetime.fromtimestamp(1700007200000 / 1000, tz=timezone.utc),
            datetime.fromtimestamp(1700010800000 / 1000, tz=timezone.utc),
        }
        self.assertEqual({e.start_time for e in daily_events}, expected_daily_times)

        todo_event = events.get(source_entity_type="todo")
        self.assertEqual(todo_event.event_type, "todo_completed")
        self.assertEqual(
            todo_event.start_time,
            datetime(2025, 1, 2, 12, 0, tzinfo=timezone.utc),
        )

    def test_dedupe_preserves_distinct_occurrences(self):
        workspace = Workspace.objects.create(name="Workspace")
        account = self._build_account(workspace)

        habit = {
            "id": "h1",
            "type": "habit",
            "text": "Stretch",
            "notes": "",
            "history": [
                {"date": 1700000000000, "value": 1},
                {"date": 1700003600000, "value": 1},
            ],
        }

        def stub_fetch_tasks(_self, _user_id, _api_token, task_type):
            if task_type == "habits":
                return [habit]
            return []

        spec = self._build_spec()

        with patch("ingestion.services.sync.get_provider_spec", return_value=spec):
            with patch.object(HabiticaClient, "_fetch_tasks", new=stub_fetch_tasks):
                with patch.object(HabiticaClient, "get_user_profile", return_value={}):
                    sync_connector_account(workspace, account)
                    sync_connector_account(workspace, account)

        events = Event.objects.for_workspace(workspace)
        self.assertEqual(events.count(), 2)
        self.assertEqual(
            {e.source_event_version for e in events},
            {
                datetime.fromtimestamp(1700000000000 / 1000, tz=timezone.utc).isoformat(),
                datetime.fromtimestamp(1700003600000 / 1000, tz=timezone.utc).isoformat(),
            },
        )
