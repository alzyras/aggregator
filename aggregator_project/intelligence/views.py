from __future__ import annotations

import json

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.db.models import Q
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_POST

from intelligence.forms import WorkspaceAISettingsForm
from intelligence.models import ChatThread, TaskTag, UnifiedTag, WorkspaceAISettings
from intelligence.services.analytics import build_insights_snapshot, build_tag_catalog
from intelligence.services.backends import AIBackendError, backend_configuration
from intelligence.services.chat import ask_workspace_chat
from intelligence.services.enrichment import queue_workspace_enrichment
from intelligence.services.taxonomy import get_or_create_tag
from planner.models import PlannerItem
from workspaces.models import WorkspaceMember

MAX_CHAT_MESSAGE_LENGTH = 4_000
MAX_MANUAL_TAGS = 10


@login_required
def dashboard(request: HttpRequest) -> HttpResponse:
    return render(
        request,
        "intelligence/dashboard.html",
        {
            "insights": build_insights_snapshot(workspace=request.workspace, user=request.user),
            "ai_config": backend_configuration(request.workspace),
            "can_manage_ai": _can_manage_ai(request),
        },
    )


@login_required
def tag_catalog(request: HttpRequest) -> HttpResponse:
    return render(
        request,
        "intelligence/tags.html",
        {
            "tags": build_tag_catalog(workspace=request.workspace, user=request.user),
            "can_manage_ai": _can_manage_ai(request),
        },
    )


@login_required
def ai_settings(request: HttpRequest) -> HttpResponse:
    if not _can_manage_ai(request):
        messages.error(request, "Only workspace owners and admins can change AI settings.")
        return redirect("intelligence:dashboard")
    instance, _created = WorkspaceAISettings.objects.get_or_create(workspace=request.workspace)
    if request.method == "POST":
        form = WorkspaceAISettingsForm(request.POST, instance=instance)
        if form.is_valid():
            form.save()
            messages.success(request, "AI settings saved. New and updated tasks will be enriched automatically.")
            return redirect("intelligence:dashboard")
    else:
        form = WorkspaceAISettingsForm(instance=instance)
    return render(
        request,
        "intelligence/settings.html",
        {"form": form, "ai_config": backend_configuration(request.workspace)},
    )


@login_required
@require_POST
def queue_analysis(request: HttpRequest) -> HttpResponse:
    if not _can_manage_ai(request):
        return JsonResponse({"error": "Workspace owner or admin access required."}, status=403)
    force = request.POST.get("force") in {"1", "true", "on"}
    result = queue_workspace_enrichment(
        workspace=request.workspace,
        created_by=request.user,
        force=force,
    )
    messages.success(
        request,
        f"Tagged {result['rules_applied']} tasks and queued {result['queued']} AI enrichments.",
    )
    return redirect("intelligence:dashboard")


@login_required
@require_POST
def update_task_tags(request: HttpRequest, item_id: int) -> JsonResponse:
    payload = _json_payload(request)
    names = payload.get("tags")
    if not isinstance(names, list):
        return JsonResponse({"error": "tags must be an array."}, status=400)
    if len(names) > MAX_MANUAL_TAGS:
        return JsonResponse({"error": f"Use at most {MAX_MANUAL_TAGS} manual tags."}, status=400)
    item = get_object_or_404(
        PlannerItem.objects.for_workspace(request.workspace).filter(
            Q(user=request.user) | Q(user__isnull=True),
            is_active=True,
        ),
        id=item_id,
    )
    names = [name for name in names if isinstance(name, str) and name.strip()]
    if len(names) != len(set(name.casefold().strip() for name in names)):
        return JsonResponse({"error": "Manual tags must be unique."}, status=400)

    with transaction.atomic():
        selected_tags = [
            get_or_create_tag(
                workspace=request.workspace,
                name=name,
                kind=UnifiedTag.KIND_OTHER,
                is_system=False,
            )
            for name in names
        ]
        selected_ids = {tag.id for tag in selected_tags}
        TaskTag.objects.filter(item=item, source=TaskTag.SOURCE_MANUAL).exclude(
            tag_id__in=selected_ids
        ).delete()
        for tag in selected_tags:
            TaskTag.objects.update_or_create(
                item=item,
                tag=tag,
                defaults={"source": TaskTag.SOURCE_MANUAL, "confidence": None, "evidence": ""},
            )
    tags = list(
        TaskTag.objects.filter(item=item)
        .select_related("tag")
        .order_by("tag__name")
        .values("tag__name", "tag__kind", "tag__color", "source")
    )
    return JsonResponse({"status": "ok", "tags": tags})


@login_required
@ensure_csrf_cookie
def chat(request: HttpRequest) -> HttpResponse:
    thread_id = request.GET.get("thread")
    threads = ChatThread.objects.for_workspace(request.workspace).filter(user=request.user)
    thread = None
    if thread_id:
        thread = get_object_or_404(threads, id=thread_id)
    elif threads.exists():
        thread = threads.first()
    if thread:
        messages_qs = thread.messages.all()
    else:
        messages_qs = []
    return render(
        request,
        "intelligence/chat.html",
        {
            "threads": threads[:20],
            "thread": thread,
            "chat_messages": messages_qs,
            "ai_config": backend_configuration(request.workspace),
            "suggestions": [
                "What should I focus on next?",
                "What types of work do I finish most reliably?",
                "Which tagged work is carrying over too long?",
                "Summarize the most important risks this week.",
            ],
        },
    )


@login_required
@require_POST
def chat_ask(request: HttpRequest) -> JsonResponse:
    payload = _json_payload(request)
    message = payload.get("message")
    if not isinstance(message, str) or not message.strip():
        return JsonResponse({"error": "Enter a question first."}, status=400)
    message = message.strip()
    if len(message) > MAX_CHAT_MESSAGE_LENGTH:
        return JsonResponse({"error": "Question is too long."}, status=400)
    thread_id = payload.get("thread_id")
    threads = ChatThread.objects.for_workspace(request.workspace).filter(user=request.user)
    if thread_id:
        thread = get_object_or_404(threads, id=thread_id)
    else:
        thread = ChatThread.objects.create(workspace=request.workspace, user=request.user)
    try:
        user_message, answer, metadata = ask_workspace_chat(thread=thread, message=message)
    except AIBackendError as exc:
        return JsonResponse({"error": str(exc)}, status=503)
    return JsonResponse(
        {
            "thread_id": thread.id,
            "thread_url": f"{reverse('intelligence:chat')}?thread={thread.id}",
            "title": thread.title,
            "user_message": {"id": user_message.id, "content": user_message.content},
            "answer": {"id": answer.id, "content": answer.content},
            "model": metadata["model"],
            "usage": metadata["usage"],
        }
    )


@login_required
@require_POST
def delete_chat_thread(request: HttpRequest, thread_id: int) -> HttpResponse:
    thread = get_object_or_404(
        ChatThread.objects.for_workspace(request.workspace).filter(user=request.user),
        id=thread_id,
    )
    thread.delete()
    return redirect("intelligence:chat")


def _can_manage_ai(request: HttpRequest) -> bool:
    return WorkspaceMember.objects.filter(
        workspace=request.workspace,
        user=request.user,
        role__in=[WorkspaceMember.ROLE_OWNER, WorkspaceMember.ROLE_ADMIN],
    ).exists()


def _json_payload(request: HttpRequest) -> dict:
    try:
        payload = json.loads(request.body.decode("utf-8")) if request.body else {}
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}
