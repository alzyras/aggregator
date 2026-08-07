from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import Mock

from cryptography.fernet import Fernet
from django.test import TestCase, override_settings

from connectors.encryption import encrypt_value
from connectors.models import ConnectorAccount
from providers.github_issues.client import GitHubIssuesClient
from providers.github_issues.forms import GitHubIssuesConnectForm
from providers.github_issues.normalizer import normalize_github_issue
from providers.github_issues.settings import get_github_settings
from workspaces.models import Workspace


@override_settings(ENCRYPTION_KEY=Fernet.generate_key())
class GitHubProviderTests(TestCase):
    def setUp(self) -> None:
        workspace = Workspace.objects.create(name="GitHub workspace")
        self.account = ConnectorAccount.objects.create(
            workspace=workspace,
            source="github",
            display_name="GitHub",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=encrypt_value("secret"),
            status=ConnectorAccount.STATUS_CONNECTED,
            scopes={
                "github": {
                    "repositories": [],
                    "include_closed": True,
                    "include_pull_requests": False,
                    "emit_task_created": True,
                    "emit_task_updated": True,
                    "emit_task_completed": True,
                    "emit_task_state": False,
                }
            },
        )

    def test_form_parses_repository_list(self):
        form = GitHubIssuesConnectForm(
            data={
                "api_token": "secret",
                "repositories": "openai/openai-python\norg/project, org/project",
                "include_closed": "on",
                "emit_task_created": "on",
                "emit_task_updated": "on",
                "emit_task_completed": "on",
            }
        )

        self.assertTrue(form.is_valid(), form.errors)
        self.assertEqual(
            form.cleaned_data["repositories"],
            ["openai/openai-python", "org/project"],
        )

    def test_fetch_assigned_issues_filters_pull_requests_and_adds_context(self):
        client = GitHubIssuesClient(self.account)
        response = Mock()
        response.links = {}
        response.json.return_value = [
            _issue(number=12),
            {
                **_issue(number=13),
                "pull_request": {"url": "https://api.github.com/pulls/13"},
            },
        ]
        client.session.get = Mock(return_value=response)

        issues = client.fetch_since(datetime(2026, 7, 1, tzinfo=timezone.utc))

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["__github_repository"], "acme/app")
        self.assertEqual(issues[0]["__github_planner_context"]["labels"], ["bug"])
        call = client.session.get.call_args
        self.assertEqual(call.args[0], "https://api.github.com/issues")
        self.assertEqual(call.kwargs["params"]["filter"], "assigned")
        self.assertEqual(call.kwargs["params"]["state"], "all")
        self.assertEqual(call.kwargs["params"]["since"], "2026-07-01T00:00:00Z")

    def test_normalizer_emits_created_and_closed_events_with_stable_identity(self):
        raw = _issue(number=14, state="closed")
        raw["closed_at"] = "2026-07-03T12:00:00Z"
        raw["updated_at"] = raw["closed_at"]
        raw["__github_repository"] = "acme/app"
        raw["__github_settings"] = get_github_settings(self.account.scopes)

        events = normalize_github_issue(raw)

        self.assertEqual(
            [event["event_type"] for event in events],
            ["task_created", "task_completed"],
        )
        self.assertTrue(
            all(event["source_entity_id"] == "acme/app#14" for event in events)
        )
        self.assertEqual(events[-1]["external_status"], "closed")


def _issue(*, number: int, state: str = "open") -> dict:
    return {
        "id": number,
        "number": number,
        "title": f"Issue {number}",
        "body": "Details",
        "state": state,
        "html_url": f"https://github.com/acme/app/issues/{number}",
        "repository_url": "https://api.github.com/repos/acme/app",
        "created_at": "2026-06-30T10:00:00Z",
        "updated_at": "2026-07-02T10:00:00Z",
        "closed_at": None,
        "labels": [{"name": "bug", "color": "ff0000"}],
        "assignees": [{"login": "octocat"}],
        "milestone": None,
    }
