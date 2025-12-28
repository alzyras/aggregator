import logging
from typing import Optional

from aggregator.settings import settings


def configure_logging(level: Optional[str] = None) -> None:
    """Configure logging once for the whole project."""
    log_level = level or settings.log_level
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        level=numeric_level,
        force=True,  # ensure we override any prior configuration
    )
