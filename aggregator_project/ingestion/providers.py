from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Protocol, Type

from django import forms
from django.apps import apps

if TYPE_CHECKING:
    from connectors.models import ConnectorAccount
    from planner.models import PlannerItem

ClientFactory = Callable[["ConnectorAccount"], Any]
Normalizer = Callable[[dict[str, Any]], dict[str, Any] | list[dict[str, Any]]]
CredentialsValidator = Callable[[dict[str, Any]], tuple[bool, str]]
CredentialsApplier = Callable[["ConnectorAccount", dict[str, Any]], None]
FormInitialFactory = Callable[["ConnectorAccount"], dict[str, Any]]
MaskedCredentialsResolver = Callable[
    ["ConnectorAccount", dict[str, Any]], dict[str, Any]
]
StatusWriterFactory = Callable[["ConnectorAccount"], "ProviderStatusWriter"]
DescriptionWriterFactory = Callable[["ConnectorAccount"], "ProviderDescriptionWriter"]
RawSanitizer = Callable[[dict[str, Any]], dict[str, Any]]
PlannerBadgeExtractor = Callable[["PlannerItem", dict[str, Any]], list[str]]
SourceUrlExtractor = Callable[[dict[str, Any]], str | None]

STATUS_WRITEBACK_SUCCESS = "success"
STATUS_WRITEBACK_UNSUPPORTED = "unsupported"
STATUS_WRITEBACK_NOOP = "noop"
STATUS_WRITEBACK_FAILED = "failed"


@dataclass(frozen=True)
class StatusWritebackResult:
    status: str
    source_status: str | None = None
    external_completed: bool | None = None
    message: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DescriptionWritebackResult:
    status: str
    description: str | None = None
    message: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


class ProviderStatusWriter(Protocol):
    def apply_planner_status(
        self,
        *,
        source_entity_id: str,
        planner_status: str,
        item: "PlannerItem | None" = None,
        source_entity_type: str | None = None,
    ) -> StatusWritebackResult:
        ...


class ProviderDescriptionWriter(Protocol):
    def update_description(
        self,
        *,
        source_entity_id: str,
        description: str,
        item: "PlannerItem | None" = None,
        source_entity_type: str | None = None,
    ) -> DescriptionWritebackResult:
        ...


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
    connection_verifier: CredentialsValidator | None = None
    credentials_applier: CredentialsApplier | None = None
    form_initial_factory: FormInitialFactory | None = None
    masked_credentials_resolver: MaskedCredentialsResolver | None = None
    form_template: str | None = None
    status_writer_factory: StatusWriterFactory | None = None
    description_writer_factory: DescriptionWriterFactory | None = None
    raw_sanitizer: RawSanitizer | None = None
    planner_badge_extractor: PlannerBadgeExtractor | None = None
    source_url_extractor: SourceUrlExtractor | None = None


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
