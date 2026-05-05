"""
Order watcher compatibility layer.
"""
from dataclasses import dataclass
from enum import Enum


class OrderStatus(str, Enum):
    """Order terminal/transition statuses used by batch service."""

    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"


@dataclass
class OrderUpdate:
    """Normalized order update event payload."""

    order_id: str
    symbol: str
    status: OrderStatus
    avg_price: float | None = None
    batch_id: int | None = None
    phase: str | None = None
    is_spot: bool = False


class SchedulerOrderWatcher:
    """Lightweight watcher stub, polling is handled in scheduler jobs."""

    def __init__(self, batch_service):
        self.batch_service = batch_service
        self._running = False

    async def start(self):
        self._running = True

    async def stop(self):
        self._running = False
