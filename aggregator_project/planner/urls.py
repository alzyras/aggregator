from __future__ import annotations

from django.urls import path

from planner import views

urlpatterns = [
    path("", views.planner_list, name="planner_list"),
    path("calendar/", views.planner_calendar, name="planner_calendar"),
    path("item/<int:item_id>/status", views.update_planned_status, name="planner_item_status"),
    path("item/<int:item_id>/planner-status", views.update_planner_status, name="planner_item_planner_status"),
    path("item/<int:item_id>/pin", views.toggle_pin, name="planner_item_pin"),
    path("item/<int:item_id>/schedule", views.update_planned_schedule, name="planner_item_schedule"),
    path("reorder", views.reorder_items, name="planner_item_reorder"),
    path("add-from-sources", views.add_from_sources, name="planner_add_from_sources"),
    path("sources/preview", views.preview_sources, name="planner_sources_preview"),
    path("sources/add", views.add_selected_sources, name="planner_sources_add"),
]
