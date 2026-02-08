from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Type, TYPE_CHECKING

from django import forms

from django.apps import apps

if TYPE_CHECKING:
    from workspaces.models import Workspace
    from connectors.models import ConnectorAccount

ClientFactory = Callable[["Workspace", "ConnectorAccount | None"], Any]
Normalizer = Callable[[dict[str, Any]], dict[str, Any]]
CredentialsValidator = Callable[[dict[str, Any]], tuple[bool, str]]


@dataclass(frozen=True)
class ProviderSpec:
    source: str
    label: str
    client_factory: ClientFactory
    normalizer: Normalizer
    required_fields: list[tuple[str, str, str]]
    auth_type: str
    validate_credentials: CredentialsValidator
    form_class: Type[forms.Form]
    icon: str


def get_provider_specs() -> list[ProviderSpec]:
    specs: list[ProviderSpec] = []
    for app_config in apps.get_app_configs():
        spec = getattr(app_config, "provider_spec", None)
        if spec is not None:
            specs.append(spec)
    return specs


def get_provider_sources() -> list[str]:
    return [spec.source for spec in get_provider_specs()]


def get_provider_choices() -> list[tuple[str, str]]:
    return [(spec.source, spec.label) for spec in get_provider_specs()]


def get_provider_spec(source: str) -> ProviderSpec | None:
    for spec in get_provider_specs():
        if spec.source == source:
            return spec
    return None
