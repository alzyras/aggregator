from __future__ import annotations

from django.contrib import admin

from connectors.models import ConnectorAccount


@admin.register(ConnectorAccount)
class ConnectorAccountAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "source",
        "display_name",
        "auth_type",
        "status",
        "is_active",
        "last_sync_at",
        "last_verified_at",
        "updated_at",
    )
    list_filter = ("source", "auth_type", "status", "is_active")
    search_fields = ("display_name",)
