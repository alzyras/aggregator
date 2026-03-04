from __future__ import annotations

from django.contrib import admin

from workspaces.models import Workspace, WorkspaceMember


@admin.register(Workspace)
class WorkspaceAdmin(admin.ModelAdmin):
    list_display = ("id", "name", "created_at")
    search_fields = ("name",)


@admin.register(WorkspaceMember)
class WorkspaceMemberAdmin(admin.ModelAdmin):
    list_display = ("workspace", "user", "role", "created_at")
    list_select_related = ("workspace", "user")
    search_fields = ("workspace__name", "user__username", "user__email")
    list_filter = ("role",)
