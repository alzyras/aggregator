from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import Mock, patch

from cryptography.fernet import Fernet
from django.test import TestCase, override_settings

from connectors.encryption import encrypt_value
from connectors.models import ConnectorAccount
from planner.models import PlannerItemState
from providers.clickup.client import ClickUpClient
from providers.clickup.forms import ClickUpConnectForm
from providers.clickup.normalizer import normalize_clickup_task
from providers.clickup.status_writer import ClickUpStatusWriter
from workspaces.models import Workspace


@override_settings(ENCRYPTION_KEY=Fernet.generate_key())
class ClickUpProviderTests(TestCase):
    def setUp(self) -> None:
        workspace = Workspace.objects.create(name="ClickUp workspace")
        self.account = ConnectorAccount.objects.create(
            workspace=workspace,
            source="clickup",
            display_name="ClickUp",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=encrypt_value("clickup-token"),
            status=ConnectorAccount.STATUS_CONNECTED,
            scopes={
                "clickup": {
                    "list_ids": ["901234567890"],
                    "include_closed": True,
                    "todo_status": "to do",
                    "in_progress_status": "in progress",
                    "done_status": "complete",
                    "emit_task_created": True,
                    "emit_task_updated": True,
                    "emit_task_completed": True,
                    "emit_task_state": False,
                }
            },
        )

    def test_form_parses_list_ids(self):
        form = ClickUpConnectForm(
            data={
                "api_token": "clickup-token",
                "list_ids": "901234567890, 901234567891\n901234567890",
                "include_closed": "on",
                "todo_status": "to do",
                "in_progress_status": "in progress",
                "done_status": "complete",
                "emit_task_created": "on",
                "emit_task_updated": "on",
                "emit_task_completed": "on",
            }
        )

        self.assertTrue(form.is_valid(), form.errors)
        self.assertEqual(form.cleaned_data["list_ids"], ["901234567890", "901234567891"])

    def test_fetch_list_tasks_adds_context_and_respects_since(self):
        client = ClickUpClient(self.account)
        response = Mock()
        response.json.return_value = {"tasks": [_task()], "last_page": True}
        client.session.get = Mock(return_value=response)

        tasks = client.fetch_since(datetime(2026, 1, 1, tzinfo=timezone.utc))

        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]["__clickup_planner_context"]["list_name"], "Roadmap")
        call = client.session.get.call_args
        self.assertEqual(call.args[0], "https://api.clickup.com/api/v2/list/901234567890/task")
        self.assertEqual(call.kwargs["headers"]["Authorization"], "clickup-token")
        self.assertEqual(call.kwargs["params"]["include_markdown_description"], "true")
        self.assertEqual(call.kwargs["params"]["page"], 0)

    def test_normalizer_emits_created_and_completed_events(self):
        raw = _task(completed=True)
        raw["date_done"] = "1782910800000"
        raw["date_updated"] = raw["date_done"]
        raw["__clickup_settings"] = self.account.scopes["clickup"]

        events = normalize_clickup_task(raw)

        self.assertEqual([event["event_type"] for event in events], ["task_created", "task_completed"])
        self.assertTrue(all(event["source_entity_id"] == "task-123" for event in events))
        self.assertEqual(events[-1]["external_status"], "Complete")

    def test_status_and_description_writeback_use_clickup_payloads(self):
        response = Mock()
        response.json.return_value = {
            "status": {"status": "Complete", "type": "closed"},
            "markdown_description": "Saved description",
        }
        writer = ClickUpStatusWriter(self.account)

        with patch("providers.clickup.status_writer.requests.put", return_value=response) as request:
            result = writer.apply_planner_status(
                source_entity_id="task-123",
                planner_status=PlannerItemState.PLANNER_STATUS_DONE,
            )
            description_result = writer.update_description(
                source_entity_id="task-123",
                description="Saved description",
            )

        self.assertTrue(result.external_completed)
        self.assertEqual(description_result.description, "Saved description")
        self.assertEqual(request.call_args_list[0].kwargs["json"], {"status": "complete"})
        self.assertEqual(request.call_args_list[1].kwargs["json"], {"description": "Saved description"})


def _task(*, completed: bool = False) -> dict:
    return {
        "id": "task-123",
        "name": "Ship project dashboard",
        "description": "Fallback description",
        "markdown_description": "Detailed dashboard brief",
        "status": {"status": "Complete" if completed else "In Progress", "type": "closed" if completed else "custom"},
        "date_created": "1782727200000",
        "date_updated": "1782820800000",
        "date_done": None,
        "url": "https://app.clickup.com/t/task-123",
        "list": {"id": "901234567890", "name": "Roadmap"},
        "folder": {"id": "folder-1", "name": "Product"},
        "space": {"id": "space-1", "name": "Company"},
        "priority": {"priority": "high"},
    }
