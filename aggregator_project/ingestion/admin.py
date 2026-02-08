from __future__ import annotations

from django.contrib import admin

from ingestion.models import Job


@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "workspace",
        "job_type",
        "job_name",
        "status",
        "queued_at",
        "started_at",
        "finished_at",
    )
    list_select_related = ("workspace",)
    list_filter = ("job_type", "status")
    search_fields = ("error_message", "workspace__name")
    ordering = ("-queued_at",)

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
