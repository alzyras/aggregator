from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from providers.todoist.client import BASE_URL, PAGE_SIZE, TodoistClient
from providers.todoist.verify import verify_todoist


class TodoistProviderTests(SimpleTestCase):
    def test_client_uses_v1_pagination_and_merges_completed_tasks(self):
        account = self._account(include_completed=True)
        calls = []

        def get(url, *, headers, params, timeout):
            calls.append((url, dict(params)))
            if url == f"{BASE_URL}/projects":
                return self._response({"results": [{"id": "p1", "name": "Work"}]})
            if url == f"{BASE_URL}/sections":
                return self._response({"results": [{"id": "s1", "name": "Next"}]})
            if url.endswith("/tasks/completed/by_completion_date"):
                return self._response(
                    {
                        "items": [
                            {
                                "id": "done-1",
                                "content": "Finished",
                                "project_id": "p1",
                                "section_id": "s1",
                                "completed_at": "2026-07-02T10:00:00Z",
                            }
                        ],
                        "next_cursor": None,
                    }
                )
            if url == f"{BASE_URL}/tasks" and params.get("cursor") == "page-2":
                return self._response(
                    {
                        "results": [
                            {
                                "id": "active-2",
                                "content": "Second",
                                "updated_at": "2026-07-03T10:00:00Z",
                            }
                        ],
                        "next_cursor": None,
                    }
                )
            if url == f"{BASE_URL}/tasks":
                return self._response(
                    {
                        "results": [
                            {
                                "id": "active-1",
                                "content": "First",
                                "project_id": "p1",
                                "section_id": "s1",
                                "updated_at": "2026-07-03T09:00:00Z",
                            }
                        ],
                        "next_cursor": "page-2",
                    }
                )
            raise AssertionError(f"Unexpected Todoist URL: {url}")

        with patch("providers.todoist.client.requests.get", side_effect=get):
            tasks = TodoistClient(account).fetch_since(
                datetime(2026, 7, 1, tzinfo=timezone.utc)
            )

        self.assertEqual(
            {task["id"] for task in tasks}, {"active-1", "active-2", "done-1"}
        )
        completed = next(task for task in tasks if task["id"] == "done-1")
        self.assertTrue(completed["checked"])
        active = next(task for task in tasks if task["id"] == "active-1")
        self.assertEqual(
            active["__todoist_planner_context"],
            {"project_name": "Work", "section_name": "Next"},
        )
        task_calls = [call for call in calls if call[0] == f"{BASE_URL}/tasks"]
        self.assertEqual(task_calls[0][1]["limit"], PAGE_SIZE)
        self.assertEqual(task_calls[1][1]["cursor"], "page-2")

    def test_client_skips_completed_endpoint_when_disabled(self):
        account = self._account(include_completed=False)
        urls = []

        def get(url, *, headers, params, timeout):
            urls.append(url)
            return self._response({"results": [], "next_cursor": None})

        with patch("providers.todoist.client.requests.get", side_effect=get):
            TodoistClient(account).fetch_since(None)

        self.assertNotIn(f"{BASE_URL}/tasks/completed/by_completion_date", urls)

    def test_verifier_uses_unified_v1_api(self):
        response = self._response({"results": []})
        response.status_code = 200

        with patch(
            "providers.todoist.verify.requests.get", return_value=response
        ) as request:
            result = verify_todoist({"api_token": "token"})

        self.assertEqual(result, (True, "Connected."))
        request.assert_called_once_with(
            "https://api.todoist.com/api/v1/projects",
            headers={"Authorization": "Bearer token"},
            params={"limit": 1},
            timeout=10,
        )

    def _account(self, *, include_completed: bool):
        return SimpleNamespace(
            scopes={"todoist": {"include_completed": include_completed}},
            get_access_token=lambda: "token",
        )

    def _response(self, payload):
        response = Mock()
        response.status_code = 200
        response.json.return_value = payload
        return response
