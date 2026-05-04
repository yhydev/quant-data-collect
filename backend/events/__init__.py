"""
Events package for Binance Arbitrage Platform.
Handles event processing, state machines, and order watching.
"""
from .phase_machine import BatchPhaseMachine, PhaseState, BatchContext
from .order_watcher import OrderWatcher, OrderStatus, OrderUpdate, SchedulerOrderWatcher

# 延迟导入，避免循环导入
PhaseService = None
PhaseEventType = None
PhaseEvent = None
PhaseServiceConfig = None

def _get_phase_service_module():
    from .phase_service import PhaseService as PS, PhaseEventType as PET, PhaseEvent as PE, PhaseServiceConfig as PSC
    return PS, PET, PE, PSC

# 为了兼容旧代码，提供懒加载属性
class _PhaseServiceModule:
    @property
    def PhaseService(self):
        global PhaseService
        if PhaseService is None:
            PS, _, _, _ = _get_phase_service_module()
            PhaseService = PS
        return PhaseService
    
    @property
    def PhaseEventType(self):
        global PhaseEventType
        if PhaseEventType is None:
            _, PET, _, _ = _get_phase_service_module()
            PhaseEventType = PET
        return PhaseEventType
    
    @property
    def PhaseEvent(self):
        global PhaseEvent
        if PhaseEvent is None:
            _, _, PE, _ = _get_phase_service_module()
            PhaseEvent = PE
        return PhaseEvent
    
    @property
    def PhaseServiceConfig(self):
        global PhaseServiceConfig
        if PhaseServiceConfig is None:
            _, _, _, PSC = _get_phase_service_module()
            PhaseServiceConfig = PSC
        return PhaseServiceConfig

_phase_module = _PhaseServiceModule()

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
    'PhaseEventType',
    'PhaseEvent',
    'PhaseServiceConfig',
    '_phase_module',
]
