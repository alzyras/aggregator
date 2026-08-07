from django.urls import path

from plugin_system import views

app_name = "plugin_system"

urlpatterns = [
    path("", views.plugin_catalog, name="catalog"),
    path("<slug:plugin_id>/toggle/", views.toggle_plugin, name="toggle"),
]
