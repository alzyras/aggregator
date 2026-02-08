from __future__ import annotations

from django.contrib import admin

from ingestion.models import SyncRun


@admin.register(SyncRun)
class SyncRunAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "workspace",
        "source",
        "status",
        "started_at",
        "finished_at",
        "updated_at",
    )
    list_select_related = ("workspace",)
    list_filter = ("source", "status")
    search_fields = ("error", "workspace__name")
    ordering = ("-started_at",)

    def has_module_permission(self, request):
        return request.user.is_superuser

    def has_view_permission(self, request, obj=None):
        return request.user.is_superuser

    def has_add_permission(self, request):
        return request.user.is_superuser

    def has_change_permission(self, request, obj=None):
        return request.user.is_superuser

    def has_delete_permission(self, request, obj=None):
        return request.user.is_superuser
