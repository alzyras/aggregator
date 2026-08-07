from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import Mock

from cryptography.fernet import Fernet
from django.test import TestCase, override_settings

from connectors.encryption import encrypt_value
from connectors.models import ConnectorAccount
from providers.linear.client import ASSIGNED_ISSUES_QUERY, LinearClient
from providers.linear.forms import LinearConnectForm
from providers.linear.normalizer import normalize_linear_issue
from providers.linear.settings import get_linear_settings
from workspaces.models import Workspace


@override_settings(ENCRYPTION_KEY=Fernet.generate_key())
class LinearProviderTests(TestCase):
    def setUp(self) -> None:
        workspace = Workspace.objects.create(name="Linear workspace")
        self.account = ConnectorAccount.objects.create(
            workspace=workspace,
            source="linear",
            display_name="Linear",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=encrypt_value("lin_api_key"),
            status=ConnectorAccount.STATUS_CONNECTED,
            scopes={
                "linear": {
                    "team_keys": ["ENG"],
                    "only_assigned_to_me": True,
                    "include_completed": True,
                    "include_canceled": False,
                    "include_archived": False,
                    "emit_task_created": True,
                    "emit_task_updated": True,
                    "emit_task_completed": True,
                    "emit_task_state": False,
                }
            },
        )

    def test_form_parses_team_keys(self):
        form = LinearConnectForm(
            data={
                "api_key": "lin_api_key",
                "team_keys": "ENG, PRODUCT\nOPS",
                "only_assigned_to_me": "on",
                "include_completed": "on",
                "emit_task_created": "on",
                "emit_task_updated": "on",
                "emit_task_completed": "on",
            }
        )

        self.assertTrue(form.is_valid(), form.errors)
        self.assertEqual(form.cleaned_data["team_keys"], ["ENG", "PRODUCT", "OPS"])

    def test_fetch_assigned_issues_paginates_and_filters_team(self):
        client = LinearClient(self.account)
        client.api.request = Mock(
            side_effect=[
                {
                    "viewer": {
                        "assignedIssues": {
                            "nodes": [
                                _issue(team="ENG"),
                                _issue(issue_id="other", team="OPS"),
                            ],
                            "pageInfo": {"hasNextPage": True, "endCursor": "cursor-1"},
                        }
                    }
                },
                {
                    "viewer": {
                        "assignedIssues": {
                            "nodes": [_issue(issue_id="second", team="ENG")],
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                        }
                    }
                },
            ]
        )

        issues = client.fetch_since(datetime(2026, 7, 1, tzinfo=timezone.utc))

        self.assertEqual([issue["id"] for issue in issues], ["issue-1", "second"])
        first_call = client.api.request.call_args_list[0]
        self.assertEqual(first_call.args[0], ASSIGNED_ISSUES_QUERY)
        self.assertEqual(
            first_call.args[1]["filter"],
            {"updatedAt": {"gt": "2026-07-01T00:00:00Z"}},
        )
        self.assertEqual(
            client.api.request.call_args_list[1].args[1]["after"], "cursor-1"
        )

    def test_normalizer_emits_completed_event(self):
        raw = _issue(state_type="completed")
        raw["completedAt"] = "2026-07-05T11:00:00Z"
        raw["updatedAt"] = raw["completedAt"]
        raw["__linear_settings"] = get_linear_settings(self.account.scopes)

        events = normalize_linear_issue(raw)

        self.assertEqual(
            [event["event_type"] for event in events],
            ["task_created", "task_completed"],
        )
        self.assertEqual(events[-1]["source_entity_id"], "issue-1")
        self.assertEqual(events[-1]["external_status"], "Done")


def _issue(
    *,
    issue_id: str = "issue-1",
    team: str = "ENG",
    state_type: str = "started",
) -> dict:
    return {
        "id": issue_id,
        "identifier": "ENG-42",
        "title": "Ship unified inbox",
        "description": "Details",
        "url": "https://linear.app/acme/issue/ENG-42/ship-unified-inbox",
        "createdAt": "2026-06-30T10:00:00Z",
        "updatedAt": "2026-07-02T10:00:00Z",
        "completedAt": None,
        "canceledAt": None,
        "archivedAt": None,
        "dueDate": None,
        "priority": 2,
        "priorityLabel": "High",
        "state": {
            "id": "state-1",
            "name": "Done" if state_type == "completed" else "In Progress",
            "type": state_type,
        },
        "team": {"id": "team-1", "key": team, "name": "Engineering"},
        "project": {"id": "project-1", "name": "Inbox"},
        "cycle": None,
        "assignee": {"id": "user-1", "name": "Tom", "email": "tom@example.com"},
        "labels": {"nodes": [{"id": "label-1", "name": "Feature", "color": "#fff"}]},
    }
