from aggregator.core.apps import AppConfig


class TogglConfig(AppConfig):
    name = "toggl"
    verbose_name = "Toggl"
    service_class_path = "aggregator.plugins.toggl.services.TogglService"
