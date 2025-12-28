from aggregator.core.apps import AppConfig


class HabiticaConfig(AppConfig):
    name = "habitica"
    verbose_name = "Habitica"
    service_class_path = "aggregator.plugins.habitica.services.HabiticaService"
