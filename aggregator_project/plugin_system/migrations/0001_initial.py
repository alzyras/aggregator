from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    initial = True

    dependencies = [("workspaces", "0001_initial")]

    operations = [
        migrations.CreateModel(
            name="PluginActivation",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("plugin_id", models.SlugField(max_length=64)),
                ("enabled", models.BooleanField(default=False)),
                ("settings", models.JSONField(blank=True, default=dict)),
                (
                    "workspace",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="plugin_activations",
                        to="workspaces.workspace",
                    ),
                ),
            ],
        ),
        migrations.AddConstraint(
            model_name="pluginactivation",
            constraint=models.UniqueConstraint(
                fields=("workspace", "plugin_id"),
                name="unique_workspace_plugin_activation",
            ),
        ),
        migrations.AddIndex(
            model_name="pluginactivation",
            index=models.Index(
                fields=["workspace", "enabled"],
                name="plugin_syst_workspa_bc8cf6_idx",
            ),
        ),
    ]
