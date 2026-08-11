from __future__ import annotations

import json

from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_POST

from ingestion.services.refresh import get_workspace_refresh_snapshot
from plugin_system.registry import plugin_required
from plugins.sql_explorer.query_engine import (
    QueryRejected,
    execute_workspace_query,
    workspace_counts,
)

DEFAULT_QUERY = """SELECT source, event_type, COUNT(*) AS event_count
FROM events
GROUP BY source, event_type
ORDER BY event_count DESC
LIMIT 100"""

TABLES = [
    {
        "name": "events",
        "columns": "source, event_type, title, description, start_time, metric_value, external_status, created_at, raw_json",
    },
    {
        "name": "tasks",
        "columns": "source, title, description, planner_status, source_status, external_completed, source_created_at, connector",
    },
    {
        "name": "connectors",
        "columns": "source, name, status, last_sync_at, last_sync_status",
    },
]


@login_required
@plugin_required("sql-explorer")
@ensure_csrf_cookie
def index(request: HttpRequest):
    refresh_state = get_workspace_refresh_snapshot(workspace=request.workspace)
    return render(
        request,
        "plugins/sql_explorer/index.html",
        {
            "default_query": DEFAULT_QUERY,
            "tables": TABLES,
            "snapshot_counts": workspace_counts(request.workspace),
            "refresh_state": refresh_state,
        },
    )


@login_required
@plugin_required("sql-explorer")
@require_POST
def run_query(request: HttpRequest) -> JsonResponse:
    try:
        payload = json.loads(request.body or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON."}, status=400)
    query = payload.get("query")
    if not isinstance(query, str):
        return JsonResponse({"error": "query must be a string."}, status=400)

    try:
        result = execute_workspace_query(request.workspace, query)
    except QueryRejected as exc:
        return JsonResponse({"error": str(exc)}, status=400)

    return JsonResponse(
        {
            "columns": result.columns,
            "rows": result.rows,
            "truncated": result.truncated,
            "duration_ms": result.duration_ms,
        }
    )
