from __future__ import annotations

from unittest.mock import Mock, patch

from django.test import TestCase

from connectors.services.verify import verify_credentials
from providers.jira.verify import verify_jira


class JiraVerifyTests(TestCase):
    def _credentials(self) -> dict[str, str]:
        return {
            "deployment_type": "cloud",
            "base_url": "https://example.atlassian.net",
            "auth_method": "cloud_api_token",
            "email": "dev@example.com",
            "api_token": "token",
        }

    @patch("providers.jira.verify.requests.get")
    def test_verify_jira_success(self, mock_get):
        mock_get.return_value = Mock(status_code=200)
        ok, message = verify_jira(self._credentials())
        self.assertTrue(ok)
        self.assertEqual(message, "Connected.")

    @patch("providers.jira.verify.requests.get")
    def test_verify_jira_auth_failure(self, mock_get):
        mock_get.return_value = Mock(status_code=401)
        ok, message = verify_jira(self._credentials())
        self.assertFalse(ok)
        self.assertIn("Invalid Jira credentials", message)

    @patch("providers.jira.verify.requests.get")
    def test_verify_jira_rate_limited(self, mock_get):
        mock_get.return_value = Mock(status_code=429)
        ok, message = verify_jira(self._credentials())
        self.assertFalse(ok)
        self.assertIn("rate limited", message.lower())

    @patch("providers.jira.verify.requests.get")
    def test_verify_service_dispatches_jira(self, mock_get):
        mock_get.return_value = Mock(status_code=200)
        ok, message = verify_credentials("jira", self._credentials())
        self.assertTrue(ok)
        self.assertEqual(message, "Connected.")

