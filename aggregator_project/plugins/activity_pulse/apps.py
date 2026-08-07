from django.apps import AppConfig

from plugin_system.registry import PluginSpec


class ActivityPulseConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "plugins.activity_pulse"
    verbose_name = "Activity Pulse plugin"

    plugin_spec = PluginSpec(
        plugin_id="activity-pulse",
        label="Activity Pulse",
        description="See workload, aging, and completion patterns across every task provider.",
        nav_label="Activity",
        url_name="activity_pulse:index",
        urlconf="plugins.activity_pulse.urls",
        icon="ACT",
        order=30,
        default_enabled=False,
    )
