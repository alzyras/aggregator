from django.contrib import admin

from plugin_system.models import PluginActivation


@admin.register(PluginActivation)
class PluginActivationAdmin(admin.ModelAdmin):
    list_display = ("workspace", "plugin_id", "enabled", "updated_at")
    list_filter = ("enabled", "plugin_id")
    search_fields = ("workspace__name", "plugin_id")
