from __future__ import annotations

import os

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect, render
from django.utils import timezone

from connectors.models import ConnectorAccount
from connectors.services import sanitize_error
from connectors.services.verify import verify_credentials
from events.models import Event
from ingestion.providers import get_provider_specs


@login_required
def dashboard(request):
    provider_cards = _build_provider_cards()
    recent_events = Event.objects.order_by("-created_at")[:8]
    return render(
        request,
        "dashboard.html",
        {"provider_cards": provider_cards, "recent_events": recent_events},
    )


@login_required
def connect_provider(request, source: str):
    if request.method != "POST":
        return redirect("dashboard")

    spec = next((spec for spec in get_provider_specs() if spec.source == source), None)
    if not spec:
        messages.error(request, "Unknown provider.")
        return redirect("dashboard")

    form = spec.form_class(request.POST)
    if not form.is_valid():
        messages.error(request, "Please fix the errors and try again.")
        provider_cards = _build_provider_cards(overrides={spec.source: form})
        return render(request, "dashboard.html", {"provider_cards": provider_cards})

    credentials = form.cleaned_data
    ok, message = spec.validate_credentials(credentials)
    if not ok:
        messages.error(request, message)
        provider_cards = _build_provider_cards(overrides={spec.source: form})
        return render(request, "dashboard.html", {"provider_cards": provider_cards})

    account, _created = ConnectorAccount.objects.get_or_create(
        source=spec.source,
        defaults={
            "display_name": spec.label,
            "auth_type": spec.auth_type,
            "credentials": "",
        },
    )
    if account.status == ConnectorAccount.STATUS_CONNECTED:
        messages.info(request, f"{spec.label} is already connected.")
        return redirect("dashboard")

    account.display_name = spec.label
    account.auth_type = spec.auth_type
    account.set_credentials(credentials)
    account.status = ConnectorAccount.STATUS_CONNECTING
    account.is_active = True
    account.last_error = None
    account.save()

    ok, message = verify_credentials(spec.source, credentials)
    if ok:
        account.status = ConnectorAccount.STATUS_CONNECTED
        account.last_verified_at = timezone.now()
        account.last_error = None
        account.is_active = True
        account.save(
            update_fields=[
                "status",
                "last_verified_at",
                "last_error",
                "is_active",
                "credentials",
                "credentials_encrypted",
                "updated_at",
            ]
        )
        messages.success(request, f"Connected {spec.label} successfully.")
        return redirect("dashboard")

    account.status = ConnectorAccount.STATUS_ERROR
    account.last_error = sanitize_error(message)
    account.is_active = False
    account.save(
        update_fields=[
            "status",
            "last_error",
            "is_active",
            "credentials",
            "credentials_encrypted",
            "updated_at",
        ]
    )
    messages.error(request, f"{spec.label} connection failed: {account.last_error}")
    return redirect("dashboard")


@login_required
def disconnect_provider(request, source: str):
    if request.method != "POST":
        return redirect("dashboard")

    account = ConnectorAccount.objects.filter(source=source).first()
    if not account:
        messages.info(request, "No connector to disconnect.")
        return redirect("dashboard")

    account.status = ConnectorAccount.STATUS_DISCONNECTED
    account.last_error = None
    account.last_verified_at = None
    account.is_active = False
    account.clear_credentials()
    account.save(
        update_fields=[
            "status",
            "last_error",
            "last_verified_at",
            "is_active",
            "credentials",
            "credentials_encrypted",
            "updated_at",
        ]
    )
    messages.success(request, "Connector disconnected.")
    return redirect("dashboard")


def _enabled_plugins() -> set[str]:
    raw = os.getenv("ENABLED_PLUGINS", "")
    plugins = {value.strip().lower() for value in raw.split(",") if value.strip()}
    return plugins


def _build_provider_cards(overrides: dict[str, object] | None = None):
    cards = []
    overrides = overrides or {}
    enabled_set = _enabled_plugins()
    for spec in get_provider_specs():
        enabled = True if not enabled_set else spec.source in enabled_set
        account = ConnectorAccount.objects.filter(source=spec.source).first()
        if account:
            status = account.status
            last_verified_at = account.last_verified_at
            last_error = account.last_error
        else:
            status = ConnectorAccount.STATUS_DISCONNECTED
            last_verified_at = None
            last_error = None

        if enabled:
            ui_status = status
        else:
            ui_status = ConnectorAccount.STATUS_DISCONNECTED

        if enabled and status == ConnectorAccount.STATUS_CONNECTED:
            display_status = "connected"
        elif enabled and status == ConnectorAccount.STATUS_CONNECTING:
            display_status = "connecting"
        elif enabled and status == ConnectorAccount.STATUS_ERROR:
            display_status = "error"
        else:
            display_status = "inactive"

        form = overrides.get(spec.source) or spec.form_class()
        cards.append(
            {
                "spec": spec,
                "status": status,
                "display_status": display_status,
                "enabled": enabled,
                "form": form,
                "last_verified_at": last_verified_at,
                "last_error": last_error,
                "account": account,
            }
        )
    return cards
