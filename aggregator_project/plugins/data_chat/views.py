from __future__ import annotations

import json

from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_POST

from ingestion.services.refresh import get_workspace_refresh_snapshot
from plugin_system.registry import plugin_required
from plugins.data_chat.client import (
    DataChatConfigurationError,
    DataChatError,
    OpenAIDataChatClient,
    chat_is_configured,
    chat_model,
)
from plugins.data_chat.context import build_workspace_snapshot

MAX_MESSAGE_LENGTH = 4_000
MAX_HISTORY_MESSAGES = 8


@login_required
@plugin_required("data-chat")
@ensure_csrf_cookie
def index(request: HttpRequest):
    refresh_state = get_workspace_refresh_snapshot(workspace=request.workspace)
    snapshot = build_workspace_snapshot(
        workspace=request.workspace,
        user=request.user,
        cache_version=refresh_state["policy"].cache_version,
    )
    return render(
        request,
        "plugins/data_chat/index.html",
        {
            "chat_configured": chat_is_configured(),
            "chat_model": chat_model(),
            "snapshot": snapshot,
            "refresh_state": refresh_state,
            "suggestions": [
                "What should I focus on next?",
                "Which tasks look stale or blocked?",
                "Summarize my workload by provider.",
                "What can I finish quickly?",
            ],
        },
    )


@login_required
@plugin_required("data-chat")
@require_POST
def ask(request: HttpRequest) -> JsonResponse:
    try:
        payload = json.loads(request.body or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON."}, status=400)

    message = payload.get("message")
    if not isinstance(message, str) or not message.strip():
        return JsonResponse({"error": "Enter a question first."}, status=400)
    if len(message) > MAX_MESSAGE_LENGTH:
        return JsonResponse({"error": "Question is too long."}, status=400)
    history = _clean_history(payload.get("history"))
    if history is None:
        return JsonResponse({"error": "Invalid conversation history."}, status=400)
    messages = [*history, {"role": "user", "content": message.strip()}]

    snapshot = build_workspace_snapshot(workspace=request.workspace, user=request.user)
    try:
        answer = OpenAIDataChatClient().ask(
            messages=messages,
            snapshot=snapshot,
            workspace_id=request.workspace.id,
            user_id=request.user.id,
        )
    except DataChatConfigurationError as exc:
        return JsonResponse({"error": str(exc)}, status=503)
    except DataChatError as exc:
        return JsonResponse({"error": str(exc)}, status=502)

    return JsonResponse(
        {
            "answer": answer.text,
            "model": answer.model,
            "response_id": answer.response_id,
            "usage": answer.usage,
        }
    )


def _clean_history(value) -> list[dict[str, str]] | None:
    if value is None:
        return []
    if not isinstance(value, list):
        return None
    cleaned = []
    for item in value[-MAX_HISTORY_MESSAGES:]:
        if not isinstance(item, dict):
            return None
        role = item.get("role")
        content = item.get("content")
        if role not in {"user", "assistant"} or not isinstance(content, str):
            return None
        cleaned.append({"role": role, "content": content[:MAX_MESSAGE_LENGTH]})
    return cleaned
