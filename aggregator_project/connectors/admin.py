from __future__ import annotations

from django.contrib import admin

from connectors.models import ConnectorAccount


@admin.register(ConnectorAccount)
class ConnectorAccountAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "workspace",
        "source",
        "display_name",
        "auth_type",
        "status",
        "is_active",
        "last_sync_at",
        "last_verified_at",
        "updated_at",
    )
    list_select_related = ("workspace",)
    list_filter = ("source", "auth_type", "status", "is_active")
    search_fields = ("display_name", "workspace__name")
    exclude = ("encrypted_access_token", "encrypted_refresh_token")

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
