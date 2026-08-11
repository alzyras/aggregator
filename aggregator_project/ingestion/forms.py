from __future__ import annotations

from django import forms

from ingestion.models import WorkspaceRefreshPolicy


class WorkspaceRefreshPolicyForm(forms.ModelForm):
    class Meta:
        model = WorkspaceRefreshPolicy
        fields = [
            "auto_refresh_enabled",
            "refreshes_per_day",
            "full_refresh_interval_days",
        ]
        widgets = {
            "auto_refresh_enabled": forms.CheckboxInput(
                attrs={"class": "field-checkbox"}
            ),
            "refreshes_per_day": forms.NumberInput(
                attrs={"class": "field-input", "min": 1, "max": 96}
            ),
            "full_refresh_interval_days": forms.NumberInput(
                attrs={"class": "field-input", "min": 1, "max": 31}
            ),
        }
