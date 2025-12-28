import os

from aggregator.settings.base import Settings


class ProdSettings(Settings):
    """Production overrides keep behavior explicit and boring."""

    def __init__(self) -> None:
        super().__init__()
        self.environment = "prod"
        self.log_level = os.environ.get("LOG_LEVEL", "INFO").upper()


settings = ProdSettings()
