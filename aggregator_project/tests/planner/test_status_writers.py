from __future__ import annotations

from unittest.mock import Mock, patch

from connectors.models import ConnectorAccount
from cryptography.fernet import Fernet
from django.test import TestCase, override_settings
from planner.models import PlannerItemState
from providers.asana.status_writer import AsanaStatusWriter
from providers.habitica.status_writer import HabiticaStatusWriter
from providers.jira.status_writer import JiraStatusWriter
from providers.todoist.status_writer import TodoistStatusWriter
from workspaces.models import Workspace


@override_settings(ENCRYPTION_KEY=Fernet.generate_key())
class StatusWriterTests(TestCase):
    def setUp(self) -> None:
        self.workspace = Workspace.objects.create(name="Writer Workspace")

    def test_asana_maps_done_to_completed(self):
        account = self._account("asana")
        response = Mock()
        response.json.return_value = {"data": {"completed": True}}

        with patch("providers.asana.status_writer.requests.put", return_value=response) as request:
            result = AsanaStatusWriter(account).apply_planner_status(
                source_entity_id="task-1",
                planner_status=PlannerItemState.PLANNER_STATUS_DONE,
            )

        self.assertEqual(result.source_status, "completed")
        self.assertTrue(result.external_completed)
        request.assert_called_once()
        self.assertEqual(request.call_args.kwargs["json"], {"data": {"completed": True}})

    def test_todoist_maps_open_status_to_reopen(self):
        account = self._account("todoist")
        response = Mock()

        with patch("providers.todoist.status_writer.requests.post", return_value=response) as request:
            result = TodoistStatusWriter(account).apply_planner_status(
                source_entity_id="task-1",
                planner_status=PlannerItemState.PLANNER_STATUS_BACKLOG,
            )

        self.assertEqual(result.source_status, "open")
        self.assertFalse(result.external_completed)
        self.assertTrue(request.call_args.args[0].endswith("/tasks/task-1/reopen"))

    def test_jira_chooses_transition_by_status_category(self):
        account = self._account("jira", config={"jira": {"base_url": "https://example.atlassian.net"}})
        writer = JiraStatusWriter(account)
        writer.client.get_issue_transitions = Mock(return_value=[
            {
                "id": "11",
                "to": {"name": "Selected for Development", "statusCategory": {"key": "indeterminate"}},
            },
            {
                "id": "21",
                "to": {"name": "Done", "statusCategory": {"key": "done"}},
            },
        ])
        writer.client.transition_issue = Mock(return_value={})

        result = writer.apply_planner_status(
            source_entity_id="ABC-1",
            planner_status=PlannerItemState.PLANNER_STATUS_DONE,
        )

        writer.client.transition_issue.assert_called_once_with("ABC-1", "21")
        self.assertEqual(result.source_status, "Done")
        self.assertTrue(result.external_completed)

    def test_jira_reports_failed_when_no_matching_transition_exists(self):
        account = self._account("jira", config={"jira": {"base_url": "https://example.atlassian.net"}})
        writer = JiraStatusWriter(account)
        writer.client.get_issue_transitions = Mock(return_value=[
            {"id": "11", "to": {"name": "Blocked", "statusCategory": {"key": "indeterminate"}}},
        ])
        writer.client.transition_issue = Mock(return_value={})

        result = writer.apply_planner_status(
            source_entity_id="ABC-1",
            planner_status=PlannerItemState.PLANNER_STATUS_DONE,
        )

        writer.client.transition_issue.assert_not_called()
        self.assertEqual(result.status, "failed")

    def test_habitica_only_supports_todos(self):
        account = self._account("habitica", external_account_id="user-1")

        result = HabiticaStatusWriter(account).apply_planner_status(
            source_entity_id="daily-1",
            planner_status=PlannerItemState.PLANNER_STATUS_DONE,
            source_entity_type="daily",
        )

        self.assertEqual(result.status, "unsupported")

    def test_habitica_maps_done_todo_to_completed(self):
        account = self._account("habitica", external_account_id="user-1")
        response = Mock()
        response.json.return_value = {"data": {"completed": True}}

        with patch("providers.habitica.status_writer.requests.put", return_value=response) as request:
            result = HabiticaStatusWriter(account).apply_planner_status(
                source_entity_id="todo-1",
                planner_status=PlannerItemState.PLANNER_STATUS_DONE,
                source_entity_type="todo",
            )

        self.assertEqual(result.source_status, "completed")
        self.assertTrue(result.external_completed)
        self.assertEqual(request.call_args.kwargs["json"], {"completed": True})

    def _account(self, source: str, **kwargs) -> ConnectorAccount:
        account = ConnectorAccount.objects.create(
            workspace=self.workspace,
            source=source,
            display_name=source.title(),
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"",
            status=ConnectorAccount.STATUS_CONNECTED,
            **kwargs,
        )
        account.set_access_token("token")
        account.save(update_fields=["encrypted_access_token"])
        return account
