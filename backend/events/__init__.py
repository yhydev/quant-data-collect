"""
Events package for Binance Arbitrage Platform.
Handles phase service and order update types.
"""
from .order_watcher import OrderStatus, OrderUpdate
from .phase_service import PhaseService, PhaseServiceConfig

# 导出
__all__ = [
    'OrderStatus',
    'OrderUpdate',
    'PhaseService',
    'PhaseServiceConfig',
]
