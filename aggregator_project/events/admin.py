from __future__ import annotations

from django.contrib import admin

from events.models import Event


@admin.register(Event)
class EventAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "workspace",
        "source",
        "source_entity_type",
        "source_entity_id",
        "title",
        "start_time",
        "status",
        "updated_at",
    )
    list_select_related = ("workspace",)
    list_filter = ("source", "source_entity_type", "status")
    search_fields = ("title", "source_entity_id", "workspace__name")
    ordering = ("-start_time",)

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
