from __future__ import annotations

from unittest.mock import Mock, patch

from connectors.models import ConnectorAccount
from cryptography.fernet import Fernet
from django.test import TestCase, override_settings
from planner.models import PlannerItemState
from providers.asana.status_writer import AsanaStatusWriter
from providers.habitica.status_writer import HabiticaStatusWriter
from providers.github_issues.status_writer import GitHubIssuesStatusWriter
from providers.jira.status_writer import JiraStatusWriter
from providers.linear.status_writer import ISSUE_UPDATE_MUTATION, LinearStatusWriter
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

    def test_asana_updates_description_notes(self):
        account = self._account("asana")
        response = Mock()
        response.json.return_value = {"data": {"notes": "New notes"}}

        with patch("providers.asana.status_writer.requests.put", return_value=response) as request:
            result = AsanaStatusWriter(account).update_description(
                source_entity_id="task-1",
                description="New notes",
            )

        self.assertEqual(result.description, "New notes")
        request.assert_called_once()
        self.assertEqual(request.call_args.kwargs["json"], {"data": {"notes": "New notes"}})

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

    def test_todoist_updates_description(self):
        account = self._account("todoist")
        response = Mock()
        response.content = b'{"description":"New notes"}'
        response.json.return_value = {"description": "New notes"}

        with patch("providers.todoist.status_writer.requests.post", return_value=response) as request:
            result = TodoistStatusWriter(account).update_description(
                source_entity_id="task-1",
                description="New notes",
            )

        self.assertEqual(result.description, "New notes")
        self.assertEqual(
            request.call_args.args[0],
            "https://api.todoist.com/api/v1/tasks/task-1",
        )
        self.assertEqual(request.call_args.kwargs["json"], {"description": "New notes"})

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

    def test_jira_updates_cloud_description_as_adf(self):
        account = self._account("jira", config={"jira": {"base_url": "https://example.atlassian.net"}})
        writer = JiraStatusWriter(account)
        writer.client._request = Mock(return_value={})

        result = writer.update_description(source_entity_id="ABC-1", description="Line one\nLine two")

        self.assertEqual(result.description, "Line one\nLine two")
        writer.client._request.assert_called_once_with(
            "PUT",
            "/rest/api/3/issue/ABC-1",
            json={
                "fields": {
                    "description": {
                        "type": "doc",
                        "version": 1,
                        "content": [
                            {"type": "paragraph", "content": [{"type": "text", "text": "Line one"}]},
                            {"type": "paragraph", "content": [{"type": "text", "text": "Line two"}]},
                        ],
                    }
                }
            },
            headers={"Content-Type": "application/json"},
        )

    def test_jira_updates_server_description_as_plain_text(self):
        account = self._account(
            "jira",
            config={"jira": {"base_url": "https://jira.example.test", "deployment_type": "server"}},
        )
        writer = JiraStatusWriter(account)
        writer.client._request = Mock(return_value={})

        writer.update_description(source_entity_id="ABC-1", description="Plain notes")

        writer.client._request.assert_called_once_with(
            "PUT",
            "/rest/api/2/issue/ABC-1",
            json={"fields": {"description": "Plain notes"}},
            headers={"Content-Type": "application/json"},
        )

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

    def test_habitica_updates_description_notes(self):
        account = self._account("habitica", external_account_id="user-1")
        response = Mock()
        response.json.return_value = {"data": {"notes": "New notes"}}

        with patch("providers.habitica.status_writer.requests.put", return_value=response) as request:
            result = HabiticaStatusWriter(account).update_description(
                source_entity_id="todo-1",
                description="New notes",
                source_entity_type="todo",
            )

        self.assertEqual(result.description, "New notes")
        self.assertEqual(request.call_args.kwargs["json"], {"notes": "New notes"})

    def test_github_maps_done_to_closed(self):
        account = self._account("github")
        response = Mock()
        response.json.return_value = {"state": "closed"}

        with patch(
            "providers.github_issues.status_writer.requests.patch",
            return_value=response,
        ) as request:
            result = GitHubIssuesStatusWriter(account).apply_planner_status(
                source_entity_id="acme/app#42",
                planner_status=PlannerItemState.PLANNER_STATUS_DONE,
            )

        self.assertEqual(result.source_status, "closed")
        self.assertTrue(result.external_completed)
        self.assertTrue(request.call_args.args[0].endswith("/repos/acme/app/issues/42"))
        self.assertEqual(
            request.call_args.kwargs["json"],
            {"state": "closed", "state_reason": "completed"},
        )

    def test_github_updates_issue_body(self):
        account = self._account("github")
        response = Mock()
        response.json.return_value = {"body": "New notes"}

        with patch(
            "providers.github_issues.status_writer.requests.patch",
            return_value=response,
        ) as request:
            result = GitHubIssuesStatusWriter(account).update_description(
                source_entity_id="acme/app#42",
                description="New notes",
            )

        self.assertEqual(result.description, "New notes")
        self.assertEqual(request.call_args.kwargs["json"], {"body": "New notes"})

    def test_linear_maps_doing_to_started_workflow_state(self):
        account = self._account("linear")
        writer = LinearStatusWriter(account)
        writer.api.request = Mock(
            side_effect=[
                {
                    "issue": {
                        "state": {"id": "todo", "name": "Todo", "type": "unstarted"},
                        "team": {
                            "states": {
                                "nodes": [
                                    {"id": "started-2", "name": "Review", "type": "started", "position": 2},
                                    {"id": "started-1", "name": "In Progress", "type": "started", "position": 1},
                                ]
                            }
                        },
                    }
                },
                {
                    "issueUpdate": {
                        "success": True,
                        "issue": {
                            "state": {"id": "started-1", "name": "In Progress", "type": "started"}
                        },
                    }
                },
            ]
        )

        result = writer.apply_planner_status(
            source_entity_id="issue-id",
            planner_status=PlannerItemState.PLANNER_STATUS_DOING,
        )

        self.assertEqual(result.source_status, "In Progress")
        mutation_call = writer.api.request.call_args_list[1]
        self.assertEqual(mutation_call.args[0], ISSUE_UPDATE_MUTATION)
        self.assertEqual(
            mutation_call.args[1],
            {"id": "issue-id", "input": {"stateId": "started-1"}},
        )

    def test_linear_updates_description(self):
        account = self._account("linear")
        writer = LinearStatusWriter(account)
        writer.api.request = Mock(
            return_value={
                "issueUpdate": {
                    "success": True,
                    "issue": {"id": "issue-id", "description": "New notes"},
                }
            }
        )

        result = writer.update_description(
            source_entity_id="issue-id",
            description="New notes",
        )

        self.assertEqual(result.description, "New notes")
        self.assertEqual(
            writer.api.request.call_args.args[1],
            {"id": "issue-id", "input": {"description": "New notes"}},
        )

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
