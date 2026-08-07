from django.apps import AppConfig

from plugin_system.registry import PluginSpec


class DataChatConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "plugins.data_chat"
    verbose_name = "Data Chat plugin"

    plugin_spec = PluginSpec(
        plugin_id="data-chat",
        label="Data Chat",
        description="Ask grounded questions about tasks and workload across your connected providers.",
        nav_label="Ask",
        url_name="data_chat:index",
        urlconf="plugins.data_chat.urls",
        icon="AI",
        order=20,
        default_enabled=False,
    )
