from __future__ import annotations

from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase, override_settings


class RuntimeConfigTests(TestCase):
    @override_settings(DEBUG=False, SECRET_KEY="production-secret")
    def test_external_database_requires_external_pg_host(self):
        env = {
            "APP_ROLE": "web",
            "ENCRYPTION_KEY": "key",
            "DB_DEPLOYMENT_MODE": "external",
            "PGHOST": "postgres",
        }
        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(CommandError):
                call_command("validate_runtime_config", stdout=StringIO(), stderr=StringIO())

    @override_settings(DEBUG=False, SECRET_KEY="production-secret")
    def test_valid_external_database_config_passes(self):
        env = {
            "APP_ROLE": "worker",
            "ENCRYPTION_KEY": "key",
            "DB_DEPLOYMENT_MODE": "external",
            "PGHOST": "db.example.com",
        }
        out = StringIO()
        with patch.dict("os.environ", env, clear=True):
            call_command("validate_runtime_config", stdout=out)

        self.assertIn("Runtime config validation passed", out.getvalue())

    @override_settings(DEBUG=False, SECRET_KEY="production-secret")
    def test_strict_mode_rejects_default_database_credentials_warning(self):
        env = {
            "APP_ROLE": "web",
            "ENCRYPTION_KEY": "key",
            "DB_DEPLOYMENT_MODE": "built_in",
            "PGHOST": "postgres",
            "PGUSER": "postgres",
            "PGPASSWORD": "postgres",
        }
        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(CommandError):
                call_command(
                    "validate_runtime_config",
                    "--strict",
                    stdout=StringIO(),
                    stderr=StringIO(),
                )
