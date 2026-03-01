from __future__ import annotations

from django import forms


class AsanaConnectForm(forms.Form):
    access_token = forms.CharField(
        label="Access token", max_length=255, widget=forms.PasswordInput
    )
    workspace_gids = forms.CharField(widget=forms.HiddenInput)

    sync_tasks = forms.BooleanField(label="Sync tasks", required=False, initial=True)
    sync_subtasks = forms.BooleanField(
        label="Sync subtasks", required=False, initial=True
    )
    include_completed = forms.BooleanField(
        label="Include completed", required=False, initial=True
    )
    include_archived = forms.BooleanField(
        label="Include archived", required=False, initial=False
    )

    emit_task_created = forms.BooleanField(
        label="Created", required=False, initial=True
    )
    emit_task_updated = forms.BooleanField(
        label="Updated", required=False, initial=True
    )
    emit_task_completed = forms.BooleanField(
        label="Completed", required=False, initial=True
    )
    emit_task_reopened = forms.BooleanField(
        label="Reopened", required=False, initial=True
    )
    emit_task_deleted = forms.BooleanField(
        label="Deleted", required=False, initial=True
    )

    task_state_created = forms.BooleanField(
        label="Created", required=False, initial=False
    )
    task_state_updated = forms.BooleanField(
        label="Updated", required=False, initial=False
    )
    task_state_completed = forms.BooleanField(
        label="Completed", required=False, initial=False
    )

    def clean_workspace_gids(self) -> list[str]:
        raw_value = self.cleaned_data.get("workspace_gids", "")
        if not raw_value:
            raise forms.ValidationError("At least one workspace GID is required.")
        gids = [value.strip() for value in raw_value.split(",") if value.strip()]
        if not gids:
            raise forms.ValidationError("At least one workspace GID is required.")
        invalid = [value for value in gids if not value.isdigit()]
        if invalid:
            raise forms.ValidationError("Workspace GIDs must be numeric.")
        return gids

    def clean(self) -> dict[str, object]:
        cleaned = super().clean()
        if "workspace_gids" in cleaned:
            cleaned["workspace_gids"] = cleaned["workspace_gids"]
        return cleaned

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name, field in self.fields.items():
            if isinstance(field.widget, forms.CheckboxInput):
                field.widget.attrs.update({"class": "field-checkbox"})
                if field_name.startswith("task_state_"):
                    field.widget.attrs["data-group"] = "task_state"
                if field_name.startswith("sync_") or field_name.startswith("include_"):
                    field.widget.attrs["data-group"] = "sync_scope"
                if field_name.startswith("emit_task_"):
                    field.widget.attrs["data-group"] = "event_types"
            else:
                field.widget.attrs.update({"class": "field-input"})
