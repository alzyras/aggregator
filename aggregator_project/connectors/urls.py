from __future__ import annotations

from django.urls import path

from connectors import views

urlpatterns = [
    path("", views.plugins_view, name="plugins_view"),
    path("connectors/", views.plugins_view, name="connectors_view"),
    path("connectors/connect/<str:source>/", views.connect_provider, name="connect_provider"),
    path(
        "connectors/<int:account_id>/edit/",
        views.update_connector_account,
        name="update_connector_account",
    ),
    path(
        "connectors/<int:account_id>/remove/",
        views.remove_connector_account,
        name="remove_connector_account",
    ),
    path(
        "connectors/<int:account_id>/sync/",
        views.sync_connector_account_view,
        name="sync_connector_account",
    ),
]
