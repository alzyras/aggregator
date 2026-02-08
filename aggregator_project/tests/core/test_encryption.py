from __future__ import annotations

from cryptography.fernet import Fernet
from django.test import TestCase, override_settings

from core.encryption import decrypt_payload, encrypt_payload, EncryptionError


class EncryptionTests(TestCase):
    def test_encrypt_decrypt_round_trip(self):
        key = Fernet.generate_key()
        payload = {"token": "secret", "count": 3}

        with override_settings(ENCRYPTION_KEY=key):
            result = encrypt_payload(payload)

            self.assertTrue(result.encrypted)
            self.assertNotEqual(result.payload, "")
            self.assertNotIn("secret", result.payload)
            self.assertEqual(decrypt_payload(result.payload), payload)

    def test_decrypt_invalid_token_raises(self):
        key = Fernet.generate_key()
        with override_settings(ENCRYPTION_KEY=key):
            with self.assertRaises(EncryptionError):
                decrypt_payload("not-a-valid-token")
