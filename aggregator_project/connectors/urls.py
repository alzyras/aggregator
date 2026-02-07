from __future__ import annotations

from django.urls import path

from connectors import views

urlpatterns = [
    path("plugins/connect/<str:source>/", views.connect_provider, name="connect_provider"),
    path("plugins/disconnect/<str:source>/", views.disconnect_provider, name="disconnect_provider"),
]
