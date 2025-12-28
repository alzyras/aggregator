from aggregator.core.apps import AppConfig


class AsanaConfig(AppConfig):
    name = "asana"
    verbose_name = "Asana"
    service_class_path = "aggregator.plugins.asana.services.AsanaService"
