import importlib
from abc import ABC, abstractmethod
from typing import Dict, Iterable, List, Optional, Type

from aggregator.core import signals
from aggregator.core.exceptions import PluginLoadError
from aggregator.settings import settings


class PluginService(ABC):
    """Contract every plugin service must implement."""

    name: str

    @abstractmethod
    def setup(self) -> bool:
        """Prepare persistence (migrations, tables, etc.)."""

    @abstractmethod
    def fetch_data(self):
        """Fetch data from the external system."""

    @abstractmethod
    def write_data(self, payload) -> tuple[int, int]:
        """Persist fetched data; returns (inserted, duplicates)."""


class AppConfig:
    """Django-style AppConfig analogue."""

    name: str
    verbose_name: str
    enabled: bool = True
    dependencies: tuple[str, ...] = ()
    service_class_path: Optional[str] = None

    def __init__(self, project_settings=None) -> None:
        self.settings = project_settings or settings
        if hasattr(self, "label"):
            self.label = getattr(self, "label")
        else:
            self.label = self.name
        self.enabled = self.enabled and self.settings.is_app_enabled(self.name)

    def ready(self) -> None:
        """Hook executed once after registry is ready."""

    def create_service(self) -> PluginService:
        """Instantiate the configured service."""
        if not self.service_class_path:
            raise PluginLoadError(f"No service_class_path set for {self.name}")

        module_path, class_name = self.service_class_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        service_cls: Type[PluginService] = getattr(module, class_name)
        service = service_cls(self.settings)
        return service


class AppRegistry:
    """Loads and manages all configured apps."""

    def __init__(self, project_settings=None) -> None:
        self.settings = project_settings or settings
        self.apps: Dict[str, AppConfig] = {}
        self.services: Dict[str, PluginService] = {}

    def load_apps(self) -> None:
        for dotted_path in self.settings.INSTALLED_APPS:
            config = self._load_app_config(dotted_path)
            if not config.enabled:
                continue
            self.apps[config.name] = config

        self._validate_dependencies()

        for config in self.apps.values():
            config.ready()
            service = config.create_service()
            self.services[config.name] = service
            signals.app_ready.send(sender=config, service=service)

    def _load_app_config(self, dotted_path: str) -> AppConfig:
        module_path, class_name = dotted_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        config_cls: Type[AppConfig] = getattr(module, class_name)
        return config_cls(self.settings)

    def _validate_dependencies(self) -> None:
        for config in self.apps.values():
            for dependency in config.dependencies:
                if dependency not in self.apps:
                    raise PluginLoadError(
                        f"{config.name} depends on {dependency}, which is not enabled"
                    )

    def iter_services(self) -> Iterable[PluginService]:
        return self.services.values()

    def get_service(self, name: str) -> Optional[PluginService]:
        return self.services.get(name)

    def get_app_configs(self) -> List[AppConfig]:
        return list(self.apps.values())
