from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

from django.core.management import call_command
from django.test import TestCase


class EnsureDbCommandTests(TestCase):
    def setUp(self) -> None:
        os.environ.setdefault("PGDATABASE", "aggregator")
        os.environ.setdefault("PGHOST", "localhost")
        os.environ.setdefault("PGPORT", "5432")
        os.environ.setdefault("PGUSER", "postgres")
        os.environ.setdefault("PGPASSWORD", "")

    def _build_mock_connection(self, fetchone_result):
        cursor = MagicMock()
        cursor.fetchone.return_value = fetchone_result
        cursor.__enter__.return_value = cursor

        conn = MagicMock()
        conn.cursor.return_value = cursor
        conn.__enter__.return_value = conn
        return conn, cursor

    @patch("core.management.commands.ensure_db.psycopg.connect")
    def test_ensure_db_idempotent_when_exists(self, mock_connect):
        conn, cursor = self._build_mock_connection((1,))
        mock_connect.return_value = conn

        call_command("ensure_db")

        executed = " ".join(str(call.args[0]) for call in cursor.execute.call_args_list)
        assert "CREATE DATABASE" not in executed

    @patch("core.management.commands.ensure_db.psycopg.connect")
    def test_ensure_db_creates_when_missing(self, mock_connect):
        conn, cursor = self._build_mock_connection(None)
        mock_connect.return_value = conn

        call_command("ensure_db")

        executed = " ".join(str(call.args[0]) for call in cursor.execute.call_args_list)
        assert "CREATE DATABASE" in executed
