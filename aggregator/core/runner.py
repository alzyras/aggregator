import logging
import time
from typing import Iterable

from aggregator.core import signals
from aggregator.core.apps import PluginService
from aggregator.settings import settings

logger = logging.getLogger(__name__)


class AggregationRunner:
    """Coordinates plugin lifecycle and execution."""

    def __init__(self, services: Iterable[PluginService]) -> None:
        self.services = list(services)

    def sync(self) -> None:
        """Prepare persistence for all plugins."""
        for service in self.services:
            try:
                logger.info("Setting up %s persistence", service.name)
                service.setup()
            except Exception:
                logger.exception("Failed to set up %s", service.name)

    def run_once(self) -> None:
        for service in self.services:
            logger.info("Processing %s", service.name)
            try:
                logger.info("Setting up %s persistence", service.name)
                service.setup()

                logger.info("Fetching %s data", service.name)
                data = service.fetch_data()
                signals.data_fetched.send(sender=service, data=data)

                record_count = 0
                if isinstance(data, dict):
                    record_count = sum(len(df) for df in data.values() if df is not None)
                elif hasattr(data, "__len__"):
                    record_count = len(data)  # type: ignore[arg-type]
                logger.info("%s data fetched (%s records)", service.name, record_count)

                if data is None:
                    logger.info("No data returned for %s", service.name)
                    continue

                logger.info("Writing %s data to storage", service.name)
                inserted, duplicates = service.write_data(data)
                signals.data_written.send(
                    sender=service,
                    inserted=inserted,
                    duplicates=duplicates,
                )
                logger.info(
                    "%s write complete (inserted=%s duplicates=%s)",
                    service.name,
                    inserted,
                    duplicates,
                )
            except Exception:
                logger.exception("Error while processing %s", service.name)

    def run_forever(self) -> None:
        while True:
            self.run_once()
            logger.info("Sleeping for %s seconds", settings.interval_seconds)
            time.sleep(settings.interval_seconds)
