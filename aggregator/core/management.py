import logging
import sys
from typing import List

from aggregator.core.apps import AppRegistry
from aggregator.core.exceptions import CommandError, ConfigurationError
from aggregator.core.logging import configure_logging
from aggregator.core.runner import AggregationRunner
from aggregator.settings import settings

logger = logging.getLogger(__name__)


def execute_from_command_line(command: str, argv: List[str] | None = None) -> None:
    """Entry point for manage.py commands."""
    argv = argv or []
    configure_logging()

    logger.info("Loading settings from %s", settings.environment)
    errors = settings.validate()
    if errors:
        raise ConfigurationError(f"Configuration errors: {errors}")

    registry = AppRegistry(settings)
    registry.load_apps()
    runner = AggregationRunner(registry.iter_services())

    if command == "run":
        runner.run_forever()
    elif command == "sync":
        runner.sync()
    elif command == "debug":
        _debug(registry)
    elif command == "llm_summary":
        _llm_summary(registry, argv)
    elif command == "llm_progress":
        _llm_progress(registry, argv)
    else:
        raise CommandError(f"Unknown command '{command}'. Expected run|sync|debug|llm_summary|llm_progress.")


def _debug(registry: AppRegistry) -> None:
    """Print debug information about installed apps."""
    logger.info("Installed apps:")
    for app_config in registry.get_app_configs():
        logger.info("- %s (enabled=%s)", app_config.name, app_config.enabled)
    sys.stdout.write("\n".join([app.name for app in registry.get_app_configs()]) + "\n")


def _llm_summary(registry: AppRegistry, argv: List[str] | None) -> None:
    """Ask a question against the LLM summary context."""
    question = " ".join(argv) if argv else "Provide a progress summary."
    service = registry.get_service("llm_summary")
    if not service:
        raise CommandError("llm_summary service not enabled. Add it to ENABLED_PLUGINS or INSTALLED_APPS.")
    answer = service.chat(question=question, period="last_12_months")
    sys.stdout.write(answer + "\n")


def _llm_progress(registry: AppRegistry, argv: List[str] | None) -> None:
    """Generate a progress summary for a given period."""
    period = argv[0] if argv else "last_month"
    service = registry.get_service("llm_summary")
    if not service:
        raise CommandError("llm_summary service not enabled. Add it to ENABLED_PLUGINS or INSTALLED_APPS.")
    answer = service.generate_progress_summary(period=period)
    sys.stdout.write(answer + "\n")
