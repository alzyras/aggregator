from django.apps import AppConfig

from plugin_system.registry import PluginSpec


class DailyBriefConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "plugins.daily_brief"
    verbose_name = "Daily Brief plugin"

    plugin_spec = PluginSpec(
        plugin_id="daily-brief",
        label="Daily Brief",
        description="Start with a focused, current view of what needs attention today.",
        nav_label="Brief",
        url_name="daily_brief:index",
        urlconf="plugins.daily_brief.urls",
        icon="BRF",
        order=40,
        default_enabled=False,
    )
