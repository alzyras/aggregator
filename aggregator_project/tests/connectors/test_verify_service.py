from __future__ import annotations

from django.test import TestCase

from connectors.services.verify import verify_credentials


class VerifyServiceTests(TestCase):
    def test_unknown_provider_returns_error(self):
        ok, message = verify_credentials("unknown", {})

        self.assertFalse(ok)
        self.assertEqual(message, "Unsupported provider.")
