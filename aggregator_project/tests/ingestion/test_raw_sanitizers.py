from __future__ import annotations

from django.test import SimpleTestCase

from providers.asana.sanitizer import sanitize_raw as sanitize_asana
from providers.jira.sanitizer import sanitize_raw as sanitize_jira
from providers.todoist.sanitizer import sanitize_raw as sanitize_todoist


class RawSanitizerTests(SimpleTestCase):
    def test_asana_sanitizer_removes_noise_and_keeps_diagnostics(self):
        raw = {
            "gid": "task-1",
            "name": "Task",
            "completed": False,
            "custom_fields": [{"secret": "noise"}],
            "html_notes": "<p>large body</p>",
        }

        sanitized = sanitize_asana(raw)

        self.assertEqual(sanitized["gid"], "task-1")
        self.assertEqual(sanitized["name"], "Task")
        self.assertNotIn("custom_fields", sanitized)
        self.assertNotIn("html_notes", sanitized)

    def test_todoist_sanitizer_keeps_status_fields(self):
        raw = {
            "id": "task-1",
            "content": "Task",
            "is_completed": True,
            "unknown_blob": {"too": "large"},
        }

        sanitized = sanitize_todoist(raw)

        self.assertTrue(sanitized["is_completed"])
        self.assertNotIn("unknown_blob", sanitized)

    def test_jira_sanitizer_limits_fields(self):
        raw = {
            "id": "10001",
            "key": "ABC-1",
            "fields": {
                "summary": "Task",
                "status": {"name": "Done"},
                "attachment": [{"filename": "large.bin"}],
            },
        }

        sanitized = sanitize_jira(raw)

        self.assertEqual(sanitized["key"], "ABC-1")
        self.assertIn("status", sanitized["fields"])
        self.assertNotIn("attachment", sanitized["fields"])
