from __future__ import annotations

from django import forms


class AsanaConnectForm(forms.Form):
    access_token = forms.CharField(
        label="Access Token", max_length=255, widget=forms.PasswordInput
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields.values():
            field.widget.attrs.update({"class": "field-input"})


class TodoistConnectForm(forms.Form):
    api_token = forms.CharField(
        label="API Token", max_length=255, widget=forms.PasswordInput
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields.values():
            field.widget.attrs.update({"class": "field-input"})


class HabiticaConnectForm(forms.Form):
    user_id = forms.CharField(label="User ID", max_length=255)
    api_token = forms.CharField(
        label="API Token", max_length=255, widget=forms.PasswordInput
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields.values():
            field.widget.attrs.update({"class": "field-input"})


class GoogleFitConnectForm(forms.Form):
    client_id = forms.CharField(label="Client ID", max_length=255, required=False)
    client_secret = forms.CharField(
        label="Client Secret",
        max_length=255,
        required=False,
        widget=forms.PasswordInput,
    )
    refresh_token = forms.CharField(
        label="Refresh Token",
        max_length=255,
        required=False,
        widget=forms.PasswordInput,
    )
    access_token = forms.CharField(
        label="Access Token (optional)",
        max_length=255,
        required=False,
        widget=forms.PasswordInput,
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields.values():
            field.widget.attrs.update({"class": "field-input"})

    def clean(self):
        cleaned = super().clean()
        access_token = cleaned.get("access_token")
        refresh_token = cleaned.get("refresh_token")
        client_id = cleaned.get("client_id")
        client_secret = cleaned.get("client_secret")
        if not access_token and not refresh_token:
            raise forms.ValidationError(
                "Provide an access token or a refresh token to verify."
            )
        if refresh_token and (not client_id or not client_secret):
            raise forms.ValidationError(
                "Client ID and Client Secret are required with a refresh token."
            )
        return cleaned
