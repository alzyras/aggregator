from __future__ import annotations

from django.contrib import admin
from django.urls import include, path

from events import views as event_views
from ingestion import views as ingestion_views
from plugin_system.registry import get_plugin_specs

urlpatterns = [
    path("admin/", admin.site.urls),
    path("accounts/", include("allauth.urls")),
    path("plugins/", include("plugin_system.urls")),
    path("", include("connectors.urls")),
    path("planner/", include("planner.urls")),
    path("insights/", include("intelligence.urls")),
    path("events/", event_views.event_list, name="event_list"),
    path("events/<uuid:pk>/", event_views.event_detail, name="event_detail"),
    path("stats/", event_views.stats_view, name="stats_view"),
    path("sync/", ingestion_views.sync_view, name="sync_view"),
    path("jobs/", ingestion_views.jobs_list, name="jobs_list"),
]

for plugin_spec in get_plugin_specs():
    urlpatterns.append(
        path(f"plugins/{plugin_spec.plugin_id}/", include(plugin_spec.urlconf))
    )
