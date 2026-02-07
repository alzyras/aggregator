from __future__ import annotations

from django.contrib import admin

from events.models import Event


@admin.register(Event)
class EventAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "source",
        "source_entity_type",
        "source_entity_id",
        "title",
        "start_time",
        "status",
        "updated_at",
    )
    list_filter = ("source", "source_entity_type", "status")
    search_fields = ("title", "source_entity_id")
    ordering = ("-start_time",)
