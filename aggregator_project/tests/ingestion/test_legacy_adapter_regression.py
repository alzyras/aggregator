from __future__ import annotations

from unittest.mock import patch

from cryptography.fernet import Fernet
from django.test import TestCase, override_settings
import pandas as pd

from connectors.encryption import encrypt_value
from connectors.models import ConnectorAccount
from events.models import Event
from ingestion.services.sync import sync_connector_account
from workspaces.models import Workspace

from aggregator.plugins.asana import get_done_tasks_df as legacy_asana
from aggregator.plugins.habitica import get_habits_dailies_df as legacy_habitica
from aggregator.plugins.habitica import get_todos_df as legacy_todos


@override_settings(
    DATABASES={
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": ":memory:",
        }
    },
    ENCRYPTION_KEY=Fernet.generate_key(),
)
class LegacyAdapterRegressionTests(TestCase):
    def setUp(self) -> None:
        self.workspace = Workspace.objects.create(name="Workspace")

    def test_asana_adapter_matches_legacy_output(self):
        completed_at = "2025-01-02T12:00:00.000Z"
        created_at = "2025-01-01T12:00:00.000Z"
        projects = [
            {
                "gid": "p1",
                "name": "Project",
                "created_at": created_at,
                "notes": "Notes",
                "owner": {"name": "Owner"},
            }
        ]
        tasks = [
            {
                "gid": "t1",
                "name": "Task 1",
                "completed": True,
                "completed_at": completed_at,
                "created_at": created_at,
                "notes": "Desc",
                "assignee": {"name": "Assignee", "email": "a@example.com"},
                "created_by": {"name": "Creator", "email": "c@example.com"},
                "subtasks": [],
            }
        ]

        with patch.object(legacy_asana, "get_workspace_info", return_value={"gid": "w1", "name": "Workspace"}), \
            patch.object(legacy_asana, "get_projects", return_value=projects), \
            patch.object(legacy_asana, "get_completed_tasks", return_value=tasks), \
            patch.object(legacy_asana, "get_completed_subtasks", return_value=[]):
            legacy_df = legacy_asana.get_asana_completed_tasks_df("token", "w1", 10)
        self.assertIsNotNone(legacy_df)

        account = ConnectorAccount.objects.create(
            workspace=self.workspace,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=encrypt_value("token"),
            external_account_id="w1",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )

        sync_connector_account(self.workspace, account)

        expected_entities = {row["task_id"] for row in legacy_df.to_dict(orient="records")}
        expected_events = {
            (
                row["task_id"],
                "task_completed",
                str(row["date"]),
            )
            for row in legacy_df.to_dict(orient="records")
        }

        events = Event.objects.for_workspace(self.workspace).filter(source="asana")
        actual_entities = {event.source_entity_id for event in events}
        actual_events = {
            (event.source_entity_id, event.event_type, str(event.source_event_version))
            for event in events
        }

        self.assertEqual(expected_entities, actual_entities)
        self.assertTrue(expected_events.issubset(actual_events))
        self.assertGreaterEqual(events.count(), len(expected_events))

    def test_habitica_adapter_matches_legacy_output(self):
        def stub_fetch_completed_items(_user_id, _api_token, item_type, _tag_dict):
            if item_type == "habit":
                return [
                    {
                        "item_id": "h1",
                        "item_name": "Habit",
                        "item_type": "habit",
                        "value": 1,
                        "date_created": "2025-01-01 00:00:00",
                        "date_completed": "2025-01-02 00:00:00",
                        "notes": "Notes",
                        "priority": 1,
                        "tags": "",
                        "completed": True,
                    }
                ]
            if item_type == "daily":
                return [
                    {
                        "item_id": "d1",
                        "item_name": "Daily",
                        "item_type": "daily",
                        "value": 2,
                        "date_created": "2025-01-03 00:00:00",
                        "date_completed": "2025-01-04 00:00:00",
                        "notes": "Daily Notes",
                        "priority": 1,
                        "tags": "",
                        "completed": True,
                    }
                ]
            return []

        stub_todos = [
            {
                "item_id": "t1",
                "item_name": "Todo",
                "item_type": "todo",
                "value": 3,
                "date_created": "2025-01-05 00:00:00",
                "date_completed": "2025-01-06 00:00:00",
                "notes": "Todo Notes",
                "priority": 1,
                "tags": "",
                "completed": True,
            }
        ]

        with patch.object(legacy_habitica, "fetch_tags", return_value={}), \
            patch.object(legacy_habitica, "fetch_completed_items", side_effect=stub_fetch_completed_items), \
            patch.object(legacy_todos, "get_completed_todos", return_value=stub_todos):
            legacy_df = legacy_habitica.fetch_all_data("user", "token")
            todos_df = legacy_todos.create_dataframe(stub_todos)
        self.assertIsNotNone(legacy_df)

        account = ConnectorAccount.objects.create(
            workspace=self.workspace,
            source="habitica",
            display_name="Habitica",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=encrypt_value("token"),
            external_account_id="user",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )

        sync_connector_account(self.workspace, account)

        combined_df = legacy_df
        if todos_df is not None:
            combined_df = pd.concat([legacy_df, todos_df], ignore_index=True)

        expected_entities = {row["item_id"] for row in combined_df.to_dict(orient="records")}
        expected_events = {
            (
                row["item_id"],
                "task_completed",
                str(row["date_completed"]),
            )
            for row in combined_df.to_dict(orient="records")
        }

        events = Event.objects.for_workspace(self.workspace).filter(source="habitica")
        actual_entities = {event.source_entity_id for event in events}
        actual_events = {
            (event.source_entity_id, event.event_type, str(event.source_event_version))
            for event in events
        }

        self.assertEqual(expected_entities, actual_entities)
        self.assertTrue(expected_events.issubset(actual_events))
        self.assertGreaterEqual(events.count(), len(expected_events))
