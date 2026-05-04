"""
Events package for Binance Arbitrage Platform.
Handles order watching (4 components) and simplified phase execution.
"""
from .order_watcher import (
    OrderStatus, OrderUpdate,
    SpotWebSocketWatcher, FuturesWebSocketWatcher,
    SpotPollingTask, FuturesPollingTask,
    UnifiedOrderWatcher
)
from .phase_service import PhaseService, PhaseServiceConfig

# 导出
__all__ = [
    'OrderStatus',
    'OrderUpdate',
    'SpotWebSocketWatcher',
    'FuturesWebSocketWatcher',
    'SpotPollingTask',
    'FuturesPollingTask',
    'UnifiedOrderWatcher',
    'PhaseService',
    'PhaseServiceConfig',
]
