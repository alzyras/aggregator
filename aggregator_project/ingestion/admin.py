from __future__ import annotations

from django.contrib import admin

from ingestion.models import SyncRun


@admin.register(SyncRun)
class SyncRunAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "source",
        "status",
        "started_at",
        "finished_at",
        "updated_at",
    )
    list_filter = ("source", "status")
    search_fields = ("error",)
    ordering = ("-started_at",)
