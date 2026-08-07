from __future__ import annotations

import json
import re
import sqlite3
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from connectors.models import ConnectorAccount
from events.models import Event
from planner.models import PlannerItem, PlannerItemState
from workspaces.models import Workspace

MAX_EVENTS = 5_000
MAX_ROWS = 200
MAX_QUERY_LENGTH = 10_000


class QueryRejected(ValueError):
    pass


@dataclass(frozen=True)
class QueryResult:
    columns: list[str]
    rows: list[list[Any]]
    truncated: bool
    duration_ms: int


def workspace_counts(workspace: Workspace) -> dict[str, int]:
    return {
        "events": Event.objects.for_workspace(workspace).count(),
        "tasks": PlannerItem.objects.for_workspace(workspace)
        .filter(is_active=True)
        .count(),
        "connectors": ConnectorAccount.objects.for_workspace(workspace)
        .filter(is_active=True)
        .count(),
    }


def execute_workspace_query(workspace: Workspace, query: str) -> QueryResult:
    normalized = _validate_query(query)
    started = time.monotonic()
    connection = _build_snapshot(workspace)
    try:
        connection.set_authorizer(_read_only_authorizer)
        callbacks = 0

        def progress_handler() -> int:
            nonlocal callbacks
            callbacks += 1
            return 1 if callbacks > 500 else 0

        connection.set_progress_handler(progress_handler, 1_000)
        cursor = connection.execute(normalized)
        columns = [description[0] for description in cursor.description or []]
        rows = cursor.fetchmany(MAX_ROWS + 1)
        truncated = len(rows) > MAX_ROWS
        serialized_rows = [list(row) for row in rows[:MAX_ROWS]]
    except sqlite3.DatabaseError as exc:
        message = str(exc)
        if "not authorized" in message.lower():
            raise QueryRejected("Only read-only SELECT queries are allowed.") from exc
        if "interrupted" in message.lower():
            raise QueryRejected(
                "Query took too long. Narrow the result and try again."
            ) from exc
        raise QueryRejected(message) from exc
    finally:
        connection.close()

    duration_ms = max(1, round((time.monotonic() - started) * 1_000))
    return QueryResult(columns, serialized_rows, truncated, duration_ms)


def _validate_query(query: str) -> str:
    if not isinstance(query, str) or not query.strip():
        raise QueryRejected("Enter a query first.")
    if len(query) > MAX_QUERY_LENGTH:
        raise QueryRejected("Query is too long.")
    normalized = re.sub(
        r"\A(?:\s|--[^\n]*(?:\n|\Z)|/\*.*?\*/)*", "", query, flags=re.S
    ).strip()
    if not re.match(r"(?is)^(select|with|explain)\b", normalized):
        raise QueryRejected("Only SELECT, WITH, and EXPLAIN queries are allowed.")
    return normalized.rstrip().rstrip(";")


def _read_only_authorizer(
    action: int, _arg1: str, _arg2: str, _db: str, _trigger: str
) -> int:
    denied_actions = {
        sqlite3.SQLITE_ALTER_TABLE,
        sqlite3.SQLITE_ATTACH,
        sqlite3.SQLITE_CREATE_INDEX,
        sqlite3.SQLITE_CREATE_TABLE,
        sqlite3.SQLITE_CREATE_TEMP_INDEX,
        sqlite3.SQLITE_CREATE_TEMP_TABLE,
        sqlite3.SQLITE_CREATE_TEMP_TRIGGER,
        sqlite3.SQLITE_CREATE_TEMP_VIEW,
        sqlite3.SQLITE_CREATE_TRIGGER,
        sqlite3.SQLITE_CREATE_VIEW,
        sqlite3.SQLITE_DELETE,
        sqlite3.SQLITE_DETACH,
        sqlite3.SQLITE_DROP_INDEX,
        sqlite3.SQLITE_DROP_TABLE,
        sqlite3.SQLITE_DROP_TEMP_INDEX,
        sqlite3.SQLITE_DROP_TEMP_TABLE,
        sqlite3.SQLITE_DROP_TEMP_TRIGGER,
        sqlite3.SQLITE_DROP_TEMP_VIEW,
        sqlite3.SQLITE_DROP_TRIGGER,
        sqlite3.SQLITE_DROP_VIEW,
        sqlite3.SQLITE_INSERT,
        sqlite3.SQLITE_PRAGMA,
        sqlite3.SQLITE_REINDEX,
        sqlite3.SQLITE_TRANSACTION,
        sqlite3.SQLITE_UPDATE,
    }
    return sqlite3.SQLITE_DENY if action in denied_actions else sqlite3.SQLITE_OK


def _build_snapshot(workspace: Workspace) -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE events (
            id TEXT, source TEXT, event_type TEXT, title TEXT, description TEXT,
            start_time TEXT, end_time TEXT, metric_type TEXT, metric_value REAL,
            metric_unit TEXT, external_status TEXT, created_at TEXT, raw_json TEXT
        );
        CREATE TABLE tasks (
            id INTEGER, source TEXT, title TEXT, description TEXT, planner_status TEXT,
            source_status TEXT, external_completed INTEGER, source_created_at TEXT,
            created_at TEXT, last_synced_at TEXT, connector TEXT
        );
        CREATE TABLE connectors (
            id INTEGER, source TEXT, name TEXT, status TEXT,
            last_sync_at TEXT, last_sync_status TEXT
        );
        """
    )
    _load_events(connection, workspace)
    _load_tasks(connection, workspace)
    _load_connectors(connection, workspace)
    return connection


def _load_events(connection: sqlite3.Connection, workspace: Workspace) -> None:
    events = list(
        Event.objects.for_workspace(workspace).order_by("-created_at")[:MAX_EVENTS]
    )
    connection.executemany(
        "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                str(event.id),
                event.source,
                event.event_type,
                event.title or "",
                event.description or "",
                _iso(event.start_time),
                _iso(event.end_time),
                event.metric_type or "",
                _number(event.metric_value),
                event.metric_unit or "",
                event.external_status or "",
                _iso(event.created_at),
                json.dumps(event.raw, default=str, separators=(",", ":")),
            )
            for event in events
        ],
    )


def _load_tasks(connection: sqlite3.Connection, workspace: Workspace) -> None:
    items = list(
        PlannerItem.objects.for_workspace(workspace)
        .filter(is_active=True)
        .select_related("connector_account")
        .order_by("-created_at")
    )
    state_by_item: dict[int, str] = {}
    for item_id, status in (
        PlannerItemState.objects.filter(item_id__in=[item.id for item in items])
        .order_by("last_planned_at")
        .values_list("item_id", "planner_status")
    ):
        state_by_item[item_id] = status
    connection.executemany(
        "INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                item.id,
                item.source or "",
                item.title,
                item.description or "",
                state_by_item.get(item.id, "inbox"),
                item.source_status or "",
                int(item.external_completed),
                _iso(item.source_created_at),
                _iso(item.created_at),
                _iso(item.last_synced_at),
                item.connector_account.display_name if item.connector_account else "",
            )
            for item in items
        ],
    )


def _load_connectors(connection: sqlite3.Connection, workspace: Workspace) -> None:
    accounts = (
        ConnectorAccount.objects.for_workspace(workspace)
        .filter(is_active=True)
        .order_by("source")
    )
    connection.executemany(
        "INSERT INTO connectors VALUES (?, ?, ?, ?, ?, ?)",
        [
            (
                account.id,
                account.source,
                account.display_name,
                account.status,
                _iso(account.last_sync_at),
                account.last_sync_status or "",
            )
            for account in accounts
        ],
    )


def _iso(value) -> str:
    return value.isoformat() if value else ""


def _number(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None
