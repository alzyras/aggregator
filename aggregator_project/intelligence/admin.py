from __future__ import annotations

from django.contrib import admin

from intelligence.models import (
    ChatMessage,
    ChatThread,
    TaskAnalysis,
    TaskTag,
    UnifiedTag,
    WorkspaceAISettings,
)


@admin.register(WorkspaceAISettings)
class WorkspaceAISettingsAdmin(admin.ModelAdmin):
    list_display = ("workspace", "backend", "model", "is_enabled", "last_verified_at")
    list_select_related = ("workspace",)
    exclude = ("encrypted_api_key",)


@admin.register(UnifiedTag)
class UnifiedTagAdmin(admin.ModelAdmin):
    list_display = ("name", "kind", "workspace", "is_system", "created_at")
    list_filter = ("kind", "is_system")
    search_fields = ("name", "slug", "workspace__name")
    list_select_related = ("workspace",)


@admin.register(TaskTag)
class TaskTagAdmin(admin.ModelAdmin):
    list_display = ("item", "tag", "source", "confidence", "created_at")
    list_filter = ("source", "tag__kind")
    list_select_related = ("item", "tag")
    search_fields = ("item__title", "tag__name")


@admin.register(TaskAnalysis)
class TaskAnalysisAdmin(admin.ModelAdmin):
    list_display = ("item", "status", "task_type", "difficulty", "energy", "analyzed_at")
    list_filter = ("status", "task_type", "energy")
    list_select_related = ("item",)
    search_fields = ("item__title", "summary")


@admin.register(ChatThread)
class ChatThreadAdmin(admin.ModelAdmin):
    list_display = ("title", "workspace", "user", "updated_at")
    list_select_related = ("workspace", "user")
    search_fields = ("title", "workspace__name", "user__username")


@admin.register(ChatMessage)
class ChatMessageAdmin(admin.ModelAdmin):
    list_display = ("thread", "role", "model", "created_at")
    list_select_related = ("thread",)
    search_fields = ("content", "thread__title")
