from django.apps import AppConfig

from plugin_system.registry import PluginSpec


class SqlExplorerConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "plugins.sql_explorer"
    verbose_name = "SQL Explorer plugin"

    plugin_spec = PluginSpec(
        plugin_id="sql-explorer",
        label="SQL Explorer",
        description="Query a read-only snapshot of your workspace events, tasks, and connectors.",
        nav_label="SQL",
        url_name="sql_explorer:index",
        urlconf="plugins.sql_explorer.urls",
        icon="SQL",
        order=10,
        default_enabled=False,
    )
