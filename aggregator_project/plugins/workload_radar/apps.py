from django.apps import AppConfig

from plugin_system.registry import PluginSpec


class WorkloadRadarConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "plugins.workload_radar"
    verbose_name = "Workload Radar plugin"

    plugin_spec = PluginSpec(
        plugin_id="workload-radar",
        label="Workload Radar",
        description="See seven-day task capacity, overload risk, and unplanned work before it piles up.",
        nav_label="Radar",
        url_name="workload_radar:index",
        urlconf="plugins.workload_radar.urls",
        icon="RAD",
        order=50,
        default_enabled=False,
    )
