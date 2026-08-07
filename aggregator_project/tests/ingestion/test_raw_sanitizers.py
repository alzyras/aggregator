from __future__ import annotations

from django.test import SimpleTestCase

from providers.asana.sanitizer import sanitize_raw as sanitize_asana
from providers.habitica.sanitizer import sanitize_raw as sanitize_habitica
from providers.github_issues.sanitizer import sanitize_raw as sanitize_github
from providers.jira.sanitizer import sanitize_raw as sanitize_jira
from providers.linear.sanitizer import sanitize_raw as sanitize_linear
from providers.todoist.sanitizer import sanitize_raw as sanitize_todoist


class RawSanitizerTests(SimpleTestCase):
    def test_asana_sanitizer_removes_noise_and_keeps_diagnostics(self):
        raw = {
            "gid": "task-1",
            "name": "Task",
            "completed": False,
            "__asana_planner_context": {"workspace_name": "Product", "project_name": "Launch"},
            "custom_fields": [{"secret": "noise"}],
            "html_notes": "<p>large body</p>",
        }

        sanitized = sanitize_asana(raw)

        self.assertEqual(sanitized["gid"], "task-1")
        self.assertEqual(sanitized["name"], "Task")
        self.assertEqual(sanitized["__asana_planner_context"]["project_name"], "Launch")
        self.assertNotIn("custom_fields", sanitized)
        self.assertNotIn("html_notes", sanitized)

    def test_todoist_sanitizer_keeps_status_fields(self):
        raw = {
            "id": "task-1",
            "content": "Task",
            "is_completed": True,
            "__todoist_planner_context": {"project_name": "Ops", "section_name": "Today"},
            "unknown_blob": {"too": "large"},
        }

        sanitized = sanitize_todoist(raw)

        self.assertTrue(sanitized["is_completed"])
        self.assertEqual(sanitized["__todoist_planner_context"]["project_name"], "Ops")
        self.assertNotIn("unknown_blob", sanitized)

    def test_jira_sanitizer_limits_fields(self):
        raw = {
            "id": "10001",
            "key": "ABC-1",
            "__jira_planner_context": {"project": "ABC", "epic": "Growth"},
            "fields": {
                "summary": "Task",
                "status": {"name": "Done"},
                "attachment": [{"filename": "large.bin"}],
            },
        }

        sanitized = sanitize_jira(raw)

        self.assertEqual(sanitized["key"], "ABC-1")
        self.assertEqual(sanitized["__jira_planner_context"]["epic"], "Growth")
        self.assertIn("status", sanitized["fields"])
        self.assertNotIn("attachment", sanitized["fields"])

    def test_habitica_sanitizer_keeps_planner_context(self):
        raw = {
            "id": "habit-1",
            "text": "Drink water",
            "__habitica_planner_context": {"task_type": "Habit", "tags": ["Health"]},
            "noise": {"drop": True},
        }

        sanitized = sanitize_habitica(raw)

        self.assertEqual(sanitized["__habitica_planner_context"]["task_type"], "Habit")
        self.assertNotIn("noise", sanitized)

    def test_github_sanitizer_keeps_source_link_and_drops_repository_payload(self):
        raw = {
            "number": 42,
            "html_url": "https://github.com/acme/app/issues/42",
            "__github_planner_context": {"repository": "acme/app"},
            "repository": {"large": "payload"},
        }

        sanitized = sanitize_github(raw)

        self.assertEqual(sanitized["number"], 42)
        self.assertIn("html_url", sanitized)
        self.assertNotIn("repository", sanitized)

    def test_linear_sanitizer_keeps_workflow_context_and_drops_noise(self):
        raw = {
            "id": "issue-1",
            "state": {"name": "In Progress", "type": "started"},
            "__linear_planner_context": {"identifier": "ENG-42"},
            "noise": {"large": True},
        }

        sanitized = sanitize_linear(raw)

        self.assertEqual(sanitized["state"]["type"], "started")
        self.assertNotIn("noise", sanitized)
