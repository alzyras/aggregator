from __future__ import annotations

import os

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db.models import Count
from django.urls import reverse
from django.shortcuts import redirect, render
from django.utils import timezone
import json

from connectors.encryption import encrypt_value
from connectors.models import ConnectorAccount
from connectors.services import sanitize_error
from connectors.services.verify import verify_credentials
from events.models import Event
from ingestion.models import Job
from ingestion.providers import get_provider_spec, get_provider_specs
from ingestion.services.jobs import queue_sync_jobs


STATUS_LABELS = {
    ConnectorAccount.STATUS_CONNECTED: "Connected",
    ConnectorAccount.STATUS_VALIDATING: "Validating",
    ConnectorAccount.STATUS_ERROR: "Error",
    ConnectorAccount.STATUS_REVOKED: "Revoked",
    "syncing": "Syncing",
}
SYNC_RESULT_LABELS = {
    ConnectorAccount.SYNC_STATUS_SUCCESS: "Success",
    ConnectorAccount.SYNC_STATUS_FAILED: "Failed",
}


@login_required
def plugins_view(request):
    return _render_plugins_view(request)


@login_required
def connect_provider(request, source: str):
    if request.method != "POST":
        return redirect("plugins_view")

    spec = get_provider_spec(source)
    if not spec:
        messages.error(request, "Unknown provider.")
        return redirect("plugins_view")

    form = spec.form_class(request.POST)
    display_name = (request.POST.get("display_name") or spec.label).strip() or spec.label
    if not form.is_valid():
        messages.error(request, "Please fix the errors and try again.")
        return _render_plugins_view(
            request,
            overrides={spec.source: form},
            modal_state="configure",
            selected_source=spec.source,
        )

    credentials = form.cleaned_data
    ok, message = spec.validate_credentials(credentials)
    if not ok:
        form.add_error(None, message)
        return _render_plugins_view(
            request,
            overrides={spec.source: form},
            modal_state="configure",
            selected_source=spec.source,
        )

    account = ConnectorAccount.objects.create(
        workspace=request.workspace,
        source=spec.source,
        display_name=display_name,
        auth_type=spec.auth_type,
        encrypted_access_token=encrypt_value(""),
        status=ConnectorAccount.STATUS_VALIDATING,
        is_active=True,
    )

    _apply_credentials(account, spec.source, credentials)
    account.status = ConnectorAccount.STATUS_VALIDATING
    account.is_active = True
    account.last_error = None
    account.revoked_at = None
    account.save(
        update_fields=[
            "display_name",
            "status",
            "is_active",
            "last_error",
            "encrypted_access_token",
            "encrypted_refresh_token",
            "token_expires_at",
            "scopes",
            "external_account_id",
            "revoked_at",
            "updated_at",
        ]
    )

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
                "updated_at",
            ]
        )
        messages.success(request, f"Connected {spec.label} successfully.")
        return redirect("plugins_view")

    account.status = ConnectorAccount.STATUS_ERROR
    account.last_error = sanitize_error(message)
    account.is_active = False
    account.save(
        update_fields=[
            "status",
            "last_error",
            "is_active",
            "updated_at",
        ]
    )
    messages.error(request, f"{spec.label} connection failed: {account.last_error}")
    return redirect("plugins_view")


@login_required
def update_connector_account(request, account_id: int):
    if request.method != "POST":
        return redirect("plugins_view")

    account = ConnectorAccount.objects.for_workspace(request.workspace).filter(
        id=account_id
    ).first()
    if not account:
        messages.info(request, "Connector account not found.")
        return redirect("plugins_view")

    spec = get_provider_spec(account.source)
    if not spec:
        messages.error(request, "Unknown provider.")
        return redirect("plugins_view")

    form = spec.form_class(request.POST)
    display_name = (request.POST.get("display_name") or account.display_name).strip() or account.display_name
    if not form.is_valid():
        messages.error(request, "Please fix the errors and try again.")
        return _render_plugins_view(
            request,
            overrides={spec.source: form},
            modal_state="configure",
            selected_source=spec.source,
            edit_account=account,
        )

    credentials = form.cleaned_data
    credentials = _resolve_masked_credentials(account, spec.source, credentials)
    ok, message = spec.validate_credentials(credentials)
    if not ok:
        form.add_error(None, message)
        return _render_plugins_view(
            request,
            overrides={spec.source: form},
            modal_state="configure",
            selected_source=spec.source,
            edit_account=account,
        )

    account.display_name = display_name
    _apply_credentials(account, spec.source, credentials)
    account.status = ConnectorAccount.STATUS_VALIDATING
    account.is_active = True
    account.last_error = None
    account.save(
        update_fields=[
            "display_name",
            "status",
            "is_active",
            "last_error",
            "encrypted_access_token",
            "encrypted_refresh_token",
            "token_expires_at",
            "scopes",
            "external_account_id",
            "updated_at",
        ]
    )

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
                "updated_at",
            ]
        )
        messages.success(request, f"Updated {spec.label} successfully.")
        return redirect("plugins_view")

    account.status = ConnectorAccount.STATUS_ERROR
    account.last_error = sanitize_error(message)
    account.is_active = False
    account.save(
        update_fields=[
            "status",
            "last_error",
            "is_active",
            "updated_at",
        ]
    )
    messages.error(request, f"{spec.label} update failed: {account.last_error}")
    return redirect("plugins_view")


@login_required
def remove_connector_account(request, account_id: int):
    if request.method != "POST":
        return redirect("plugins_view")

    account = ConnectorAccount.objects.for_workspace(request.workspace).filter(
        id=account_id
    ).first()
    if not account:
        messages.info(request, "No connector to remove.")
        return redirect("plugins_view")

    account.status = ConnectorAccount.STATUS_REVOKED
    account.last_error = None
    account.last_verified_at = None
    account.is_active = False
    account.revoked_at = timezone.now()
    account.clear_tokens()
    account.save(
        update_fields=[
            "status",
            "last_error",
            "last_verified_at",
            "is_active",
            "encrypted_access_token",
            "encrypted_refresh_token",
            "token_expires_at",
            "scopes",
            "external_account_id",
            "revoked_at",
            "updated_at",
        ]
    )
    messages.success(request, "Connector removed.")
    return redirect("plugins_view")


@login_required
def sync_connector_account_view(request, account_id: int):
    if request.method != "POST":
        return redirect("plugins_view")

    account = ConnectorAccount.objects.for_workspace(request.workspace).filter(
        id=account_id
    ).first()
    if not account:
        messages.info(request, "No connector to sync.")
        return redirect("plugins_view")

    full_sync = request.POST.get("full_sync") in {"1", "true", "on"}
    jobs = queue_sync_jobs(
        workspace=request.workspace,
        created_by=request.user,
        connector_account_id=account.id,
        full_sync=full_sync,
    )
    if not jobs:
        messages.warning(request, "Connector is not active yet.")
        return redirect("plugins_view")
    messages.success(request, "Sync job queued.")
    return redirect(f"{reverse('plugins_view')}?highlight={account.id}")


def _enabled_plugins() -> set[str]:
    raw = os.getenv("ENABLED_PLUGINS", "")
    return {value.strip().lower() for value in raw.split(",") if value.strip()}


def _provider_description(spec) -> str:
    if spec.required_fields:
        labels = [field[1] for field in spec.required_fields]
        if len(labels) == 1:
            return f"Requires {labels[0].lower()}."
        return "Requires " + ", ".join(label.lower() for label in labels[:-1]) + f" and {labels[-1].lower()}."
    return "Connect credentials to start syncing."


def _render_plugins_view(
    request,
    overrides: dict[str, object] | None = None,
    modal_state: str | None = None,
    selected_source: str | None = None,
    edit_account: ConnectorAccount | None = None,
):
    overrides = overrides or {}
    enabled_set = _enabled_plugins()

    provider_specs = []
    spec_map = {}
    for spec in get_provider_specs():
        enabled = True if not enabled_set else spec.source in enabled_set
        form = overrides.get(spec.source)
        if not form and edit_account and spec.source == edit_account.source:
            if spec.source == "habitica":
                from providers.habitica.settings import habitica_form_initial

                form = spec.form_class(initial=habitica_form_initial(edit_account))
            elif spec.source == "asana":
                from providers.asana.settings import asana_form_initial

                form = spec.form_class(initial=asana_form_initial(edit_account))
            elif spec.source == "todoist":
                from providers.todoist.settings import todoist_form_initial

                form = spec.form_class(initial=todoist_form_initial(edit_account))
            else:
                form = spec.form_class()
        if not form:
            form = spec.form_class()
        provider_specs.append(
            {
                "source": spec.source,
                "label": spec.label,
                "description": _provider_description(spec),
                "icon": spec.icon,
                "enabled": enabled,
                "form": form,
            }
        )
        spec_map[spec.source] = spec

    accounts = (
        ConnectorAccount.objects.for_workspace(request.workspace)
        .order_by("source", "created_at")
    )
    event_counts = {
        row["connector_account_id"]: row["count"]
        for row in Event.objects.for_workspace(request.workspace)
        .values("connector_account_id")
        .annotate(count=Count("id"))
    }
    latest_jobs = (
        Job.objects.for_workspace(request.workspace)
        .filter(job_type="sync", connector_account__isnull=False)
        .select_related("connector_account")
        .order_by("-queued_at")
    )
    latest_job_by_connector = {}
    for job in latest_jobs:
        if job.connector_account_id not in latest_job_by_connector:
            latest_job_by_connector[job.connector_account_id] = job

    connector_rows = []
    for account in accounts:
        spec = spec_map.get(account.source)
        latest_job = latest_job_by_connector.get(account.id)
        status_key = account.status
        if latest_job:
            if latest_job.status == Job.STATUS_FAILED:
                status_key = ConnectorAccount.STATUS_ERROR
            elif latest_job.status == Job.STATUS_RUNNING:
                status_key = "syncing"
            elif latest_job.status == Job.STATUS_QUEUED:
                status_key = "syncing"
            elif latest_job.finished_at and account.status == ConnectorAccount.STATUS_CONNECTED:
                status_key = ConnectorAccount.STATUS_CONNECTED
        status_label = STATUS_LABELS.get(status_key, status_key.title())
        last_sync_status = SYNC_RESULT_LABELS.get(account.last_sync_status, "—")
        edit_settings = {}
        edit_workspaces = ""
        edit_user_id = ""
        edit_token = ""
        if account.source == "asana":
            from providers.asana.settings import (
                MASKED_TOKEN as ASANA_MASK,
                get_asana_settings,
                get_asana_workspace_gids,
            )

            edit_settings = get_asana_settings(account.scopes)
            edit_workspaces = ",".join(get_asana_workspace_gids(account.scopes))
            edit_token = ASANA_MASK
        elif account.source == "habitica":
            from providers.habitica.settings import (
                MASKED_TOKEN as HABITICA_MASK,
                get_habitica_settings,
            )

            edit_settings = get_habitica_settings(account.scopes)
            edit_user_id = account.external_account_id or ""
            edit_token = HABITICA_MASK

        connector_rows.append(
            {
                "account": account,
                "spec": spec,
                "status_key": status_key,
                "status_label": status_label,
                "last_sync_status": last_sync_status,
                "event_count": event_counts.get(account.id, 0),
                "edit_settings_json": json.dumps(edit_settings),
                "edit_workspaces": edit_workspaces,
                "edit_user_id": edit_user_id,
                "edit_token": edit_token,
            }
        )

    context = {
        "provider_specs": provider_specs,
        "connector_rows": connector_rows,
        "modal_state": modal_state,
        "selected_source": selected_source,
        "edit_account": edit_account,
    }
    return render(request, "plugins.html", context)


def _apply_credentials(account: ConnectorAccount, provider: str, credentials: dict) -> None:
    if provider in {"asana", "todoist"}:
        if provider == "asana":
            from providers.asana.settings import apply_asana_settings, is_masked_token

            token = credentials.get("access_token") or credentials.get("api_token")
            if token and not is_masked_token(token):
                account.set_access_token(token)
            account.set_refresh_token(None)
            workspace_gids = credentials.get("workspace_gids") or []
            account.external_account_id = workspace_gids[0] if workspace_gids else None
            apply_asana_settings(account, credentials)
            account.token_expires_at = None
            return

        if provider == "todoist":
            from providers.todoist.settings import apply_todoist_settings, is_masked_token

            token = credentials.get("api_token") or credentials.get("access_token")
            if token and not is_masked_token(token):
                account.set_access_token(token)
            account.set_refresh_token(None)
            account.external_account_id = None
            apply_todoist_settings(account, credentials)
            account.token_expires_at = None
            return
    if provider == "habitica":
        from providers.habitica.settings import apply_habitica_settings, is_masked_token

        token = credentials.get("api_token")
        if token and not is_masked_token(token):
            account.set_access_token(token)
        account.set_refresh_token(None)
        account.external_account_id = credentials.get("user_id")
        apply_habitica_settings(account, credentials)
        account.token_expires_at = None
        return
    if provider == "google_fit":
        access_token = credentials.get("access_token")
        refresh_token = credentials.get("refresh_token")
        client_id = credentials.get("client_id")
        client_secret = credentials.get("client_secret")
        if access_token:
            account.set_access_token(access_token)
        if refresh_token:
            account.set_refresh_token(refresh_token)
        account.external_account_id = None
        account.scopes = {
            "client_id": client_id,
            "client_secret": client_secret,
        }
        account.token_expires_at = None
        return


def _resolve_masked_credentials(
    account: ConnectorAccount, provider: str, credentials: dict
) -> dict:
    if provider == "habitica":
        from providers.habitica.settings import is_masked_token

        token = credentials.get("api_token")
        if is_masked_token(token):
            credentials = {**credentials, "api_token": account.get_access_token()}
    if provider == "asana":
        from providers.asana.settings import is_masked_token

        token = credentials.get("access_token")
        if is_masked_token(token):
            credentials = {**credentials, "access_token": account.get_access_token()}
    if provider == "todoist":
        from providers.todoist.settings import is_masked_token

        token = credentials.get("api_token")
        if is_masked_token(token):
            credentials = {**credentials, "api_token": account.get_access_token()}
    return credentials
