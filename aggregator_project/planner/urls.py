from __future__ import annotations

from django.urls import path

from planner import views

urlpatterns = [
    path("", views.planner_list, name="planner_list"),
    path("calendar/", views.planner_calendar, name="planner_calendar"),
    path("item/<uuid:item_id>/status", views.update_planned_status, name="planner_item_status"),
    path("item/<uuid:item_id>/pin", views.toggle_pin, name="planner_item_pin"),
    path("item/<uuid:item_id>/schedule", views.update_planned_schedule, name="planner_item_schedule"),
    path("reorder", views.reorder_items, name="planner_item_reorder"),
    path("add-from-sources", views.add_from_sources, name="planner_add_from_sources"),
]
