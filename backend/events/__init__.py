"""
Events package for Binance Arbitrage Platform.
Handles order watching (2 WebSockets) + polling scheduler.
"""
from .order_watcher import (
    OrderStatus, OrderUpdate,
    SpotWebSocketWatcher, FuturesWebSocketWatcher,
    UnifiedOrderWatcher
)
from .order_polling import OrderPollingScheduler
from .phase_service import PhaseService, PhaseServiceConfig

# 导出
__all__ = [
    'OrderStatus',
    'OrderUpdate',
    'SpotWebSocketWatcher',
    'FuturesWebSocketWatcher',
    'UnifiedOrderWatcher',
    'OrderPollingScheduler',
    'PhaseService',
    'PhaseServiceConfig',
]
