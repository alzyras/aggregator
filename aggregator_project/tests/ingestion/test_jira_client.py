from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

from cryptography.fernet import Fernet
from django.test import TestCase, override_settings

from connectors.encryption import encrypt_value
from connectors.models import ConnectorAccount
from providers.jira.client import JiraClient
from workspaces.models import Workspace


class _Response:
    def __init__(self, status_code: int, payload: dict, headers: dict | None = None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = str(payload)

    def json(self) -> dict:
        return self._payload


@override_settings(
    ENCRYPTION_KEY=Fernet.generate_key(),
    DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
)
class JiraClientTests(TestCase):
    def setUp(self) -> None:
        workspace = Workspace.objects.create(name="W")
        self.account = ConnectorAccount.objects.create(
            workspace=workspace,
            source="jira",
            display_name="Jira",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=encrypt_value("secret"),
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
            config={
                "jira": {
                    "deployment_type": "cloud",
                    "auth_method": "cloud_api_token",
                    "base_url": "https://example.atlassian.net",
                    "email": "dev@example.com",
                    "project_keys": ["ENG"],
                    "jql_filter": "project = ENG",
                    "issue_types": ["Task"],
                    "include_status_categories": ["todo", "in_progress", "done"],
                    "include_changelog": True,
                    "include_comments": False,
                    "include_worklogs": False,
                    "include_sprints": False,
                    "include_attachments_metadata": False,
                    "include_linked_issues": False,
                    "exclude_done_before_days": None,
                    "timezone": "UTC",
                    "incremental_lookback_minutes": 30,
                    "initial_backfill_days": 365,
                    "page_size": 100,
                }
            },
        )

    def test_fetch_since_paginates_and_builds_incremental_jql(self):
        client = JiraClient(self.account)
        page_1 = _Response(
            200,
            {
                "issues": [{"id": "1", "key": "ENG-1", "fields": {}}],
                "total": 2,
            },
        )
        page_2 = _Response(
            200,
            {
                "issues": [{"id": "2", "key": "ENG-2", "fields": {}}],
                "total": 2,
            },
        )
        since = datetime(2026, 3, 1, 9, 30, tzinfo=timezone.utc)

        with patch.object(client.session, "request", side_effect=[page_1, page_2]) as mocked:
            issues = client.fetch_since(since=since)

        self.assertEqual(len(issues), 2)
        self.assertEqual(mocked.call_count, 2)
        first_params = mocked.call_args_list[0].kwargs["params"]
        self.assertIn("updated >=", first_params["jql"])
        self.assertIn("expand", first_params)
        self.assertIn("changelog", first_params["expand"])

    def test_request_retries_on_rate_limit(self):
        client = JiraClient(self.account)
        limited = _Response(429, {"errorMessages": ["rate limit"]}, headers={"Retry-After": "1"})
        success = _Response(200, {"issues": [], "total": 0})

        with patch("providers.jira.client.time.sleep") as mocked_sleep:
            with patch.object(client.session, "request", side_effect=[limited, success]) as mocked_request:
                items = client.fetch_since(since=None)

        self.assertEqual(items, [])
        self.assertEqual(mocked_request.call_count, 2)
        mocked_sleep.assert_called()

    def test_search_endpoint_fallback_on_410(self):
        client = JiraClient(self.account)
        removed = _Response(
            410,
            {"errorMessages": ["The requested API has been removed."]},
        )
        success = _Response(200, {"issues": [], "total": 0})

        with patch.object(client.session, "request", side_effect=[removed, success]) as mocked_request:
            items = client.fetch_since(since=None)

        self.assertEqual(items, [])
        self.assertEqual(mocked_request.call_count, 2)
        first_url = mocked_request.call_args_list[0].args[1]
        second_url = mocked_request.call_args_list[1].args[1]
        self.assertIn("/rest/api/3/search/jql", first_url)
        self.assertIn("/rest/api/3/search", second_url)
