from __future__ import annotations

from django.test import TestCase

from providers.jira.forms import JiraConnectForm


class JiraConnectFormTests(TestCase):
    def _base_payload(self) -> dict[str, object]:
        return {
            "deployment_type": "cloud",
            "base_url": "https://example.atlassian.net",
            "auth_method": "cloud_api_token",
            "email": "dev@example.com",
            "api_token": "token",
            "project_keys": "ENG, OPS",
            "jql_filter": "project = ENG",
            "issue_types": ["Task", "Bug"],
            "include_status_categories": ["todo", "in_progress", "done"],
            "include_changelog": "on",
            "initial_backfill_days": 365,
            "incremental_lookback_minutes": 30,
            "page_size": 100,
            "timezone": "UTC",
        }

    def test_cloud_requires_email_and_token(self):
        payload = self._base_payload()
        payload["email"] = ""
        payload["api_token"] = ""
        form = JiraConnectForm(payload)
        self.assertFalse(form.is_valid())
        self.assertIn("email", form.errors)
        self.assertIn("api_token", form.errors)

    def test_personal_access_token_requires_pat_token(self):
        payload = self._base_payload()
        payload["auth_method"] = "personal_access_token"
        payload["api_token"] = ""
        payload["pat_token"] = ""
        form = JiraConnectForm(payload)
        self.assertFalse(form.is_valid())
        self.assertIn("pat_token", form.errors)

    def test_project_keys_validation(self):
        payload = self._base_payload()
        payload["project_keys"] = "ENG,invalid-key"
        form = JiraConnectForm(payload)
        self.assertFalse(form.is_valid())
        self.assertIn("project_keys", form.errors)

    def test_valid_form_parses_lists_and_defaults(self):
        form = JiraConnectForm(self._base_payload())
        self.assertTrue(form.is_valid(), form.errors.as_json())
        cleaned = form.cleaned_data
        self.assertEqual(cleaned["project_keys"], ["ENG", "OPS"])
        self.assertEqual(cleaned["issue_types"], ["Task", "Bug"])
        self.assertEqual(cleaned["page_size"], 100)

