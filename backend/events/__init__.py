"""
Events package for Binance Arbitrage Platform.
Handles state machines and order watching.
Event system removed, simplified to direct calls.
"""
from .phase_machine import BatchPhaseMachine, PhaseState, BatchContext
from .order_watcher import OrderWatcher, OrderStatus, OrderUpdate, SchedulerOrderWatcher
from .phase_service import PhaseService, PhaseServiceConfig

# 导出
__all__ = [
    'BatchPhaseMachine',
    'PhaseState',
    'BatchContext',
    'OrderWatcher',
    'OrderStatus',
    'OrderUpdate',
    'SchedulerOrderWatcher',
    'PhaseService',
    'PhaseServiceConfig',
]
