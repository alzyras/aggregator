from __future__ import annotations

from django.contrib import admin

from ingestion.models import Job, JobAttempt


@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "workspace",
        "job_type",
        "job_name",
        "status",
        "attempt_count",
        "max_attempts",
        "queued_at",
        "started_at",
        "finished_at",
    )
    list_select_related = ("workspace",)
    list_filter = ("job_type", "status")
    search_fields = ("error_message", "workspace__name", "idempotency_key")
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


@admin.register(JobAttempt)
class JobAttemptAdmin(admin.ModelAdmin):
    list_display = (
        "job",
        "attempt_number",
        "status",
        "worker_id",
        "started_at",
        "finished_at",
    )
    list_select_related = ("job",)
    list_filter = ("status",)
    search_fields = ("job__id", "worker_id", "error_message")
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
