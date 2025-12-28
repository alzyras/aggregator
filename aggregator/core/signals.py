from typing import Callable, List, Any


class Signal:
    """Minimal signal implementation for decoupled communication."""

    def __init__(self) -> None:
        self._receivers: List[Callable[..., Any]] = []

    def connect(self, receiver: Callable[..., Any]) -> None:
        if receiver not in self._receivers:
            self._receivers.append(receiver)

    def send(self, sender: Any = None, **kwargs: Any) -> None:
        for receiver in list(self._receivers):
            receiver(sender=sender, **kwargs)


# Lifecycle / data signals
app_ready = Signal()
data_fetched = Signal()
data_written = Signal()
