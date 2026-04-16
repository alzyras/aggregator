from __future__ import annotations

from django.contrib import admin

from planner.models import PlannerItem, PlannerItemState, PlannerPlan, PlannerStatusIntent


@admin.register(PlannerItem)
class PlannerItemAdmin(admin.ModelAdmin):
    list_display = ("title", "workspace", "source", "source_status", "external_completed", "last_synced_at")
    list_filter = ("source", "external_completed", "is_active")
    search_fields = ("title", "source_entity_id", "workspace__name")
    list_select_related = ("workspace", "connector_account")


@admin.register(PlannerPlan)
class PlannerPlanAdmin(admin.ModelAdmin):
    list_display = ("name", "workspace", "user", "created_at")
    list_select_related = ("workspace", "user")


@admin.register(PlannerItemState)
class PlannerItemStateAdmin(admin.ModelAdmin):
    list_display = ("plan", "item", "planner_status", "writeback_status", "last_writeback_job_id")
    list_filter = ("planner_status", "writeback_status")
    search_fields = ("item__title", "last_writeback_error")
    list_select_related = ("plan", "item")


@admin.register(PlannerStatusIntent)
class PlannerStatusIntentAdmin(admin.ModelAdmin):
    list_display = (
        "item",
        "requested_planner_status",
        "status",
        "job",
        "attempts",
        "requested_at",
        "completed_at",
    )
    list_filter = ("status", "requested_planner_status")
    search_fields = ("item__title", "last_error", "job__id")
    list_select_related = ("item", "job", "workspace")
