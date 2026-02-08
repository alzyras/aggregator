from __future__ import annotations

from django.contrib import admin
from django.urls import include, path

from events import views as event_views
from ingestion import views as ingestion_views

urlpatterns = [
    path("admin/", admin.site.urls),
    path("accounts/", include("allauth.urls")),
    path("", include("connectors.urls")),
    path("events/", event_views.event_list, name="event_list"),
    path("events/<uuid:pk>/", event_views.event_detail, name="event_detail"),
    path("sync/", ingestion_views.sync_view, name="sync_view"),
    path("jobs/", ingestion_views.jobs_list, name="jobs_list"),
]
