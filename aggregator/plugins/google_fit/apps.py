from aggregator.core.apps import AppConfig


class GoogleFitConfig(AppConfig):
    name = "google_fit"
    verbose_name = "Google Fit"
    service_class_path = "aggregator.plugins.google_fit.services.GoogleFitService"
