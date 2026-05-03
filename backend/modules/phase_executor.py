"""
Phase Execution Service - 简洁的解耦设计

完全透明的解耦：
- 事件源只发送消息：batch_id, source, event_type, params
- Service 创建/执行状态机
- 事件源与状态机完全透明
"""
import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, Optional, Any, List
from dataclasses import dataclass, field, asdict
from enum import Enum

logger = logging.getLogger(__name__)

from ..database import get_session, BatchExecute, PositionExecute
from . import create_collector, create_trader
from ..plugins.order_sequence import get_plugin
from .phase_machine import BatchPhaseMachine, PhaseState


SLIPPAGE = Decimal('0.001')


# ==================== Event Definitions ====================

class EventType(str, Enum):
    """Event types - simple and generic."""
    # Phase lifecycle
    WAKE = 'WAKE'                    # 唤醒批次
    EXECUTE = 'EXECUTE'                # 执行当前phase
    TIMEOUT = 'TIMEOUT'                # 超时
    
    # Order events
    ORDER_SUBMITTED = 'ORDER_SUBMITTED'      # 订单已提交
    ORDER_FILLED = 'ORDER_FILLED'          # 订单成交
    ORDER_CANCELLED = 'ORDER_CANCELLED'  # 订单取消
    ORDER_REJECTED = 'ORDER_REJECTED'    # 订单被拒
    
    # Manual control
    RETRY = 'RETRY'                  # 重试
    CANCEL = 'CANCEL'                # 取消
    RESET = 'RESET'                  # 重置


class EventSource(str, Enum):
    """Event sources."""
    SCHEDULER = 'scheduler'          # 定时任务
    WEBSOCKET = 'websocket'          # WebSocket推送
    POLLING = 'polling'              # 轮询
    MANUAL = 'manual'                # 手动操作


@dataclass
class PhaseMessage:
    """
    消息格式 - 简洁统一
    事件源只发送这个消息给Service
    """
    batch_id: int
    event_type: EventType
    source: EventSource
    params: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    @classmethod
    def create(cls, batch_id: int, event: EventType, 
              source: EventSource, **params) -> 'PhaseMessage':
        """Factory method for creating messages."""
        return cls(
            batch_id=batch_id,
            event_type=event,
            source=source,
            params=params
        )
    
    def __repr__(self):
        return f"PhaseMessage({self.event_type.value}, batch={self.batch_id}, from={self.source.value})"


@dataclass
class PhaseResult:
    """执行结果."""
    success: bool
    current_state: str = ''
    action_taken: str = ''
    message: str = ''
    data: Dict[str, Any] = field(default_factory=dict)


# ==================== Phase Service ====================

class PhaseService:
    """
    Phase Execution Service - 简洁的解耦服务
    
    职责（完全透明）：
    1. 接收 PhaseMessage
    2. 创建/加载状态机
    3. 执行状态机
    4. 返回结果
    
    事件源只需：
    - 发送 batch_id + event_type + source + params
    - 不需要知道状态机任何细节
    """
    
    def __init__(self,
                 collector_type: str = 'binance',
                 trader_type: str = 'binance',
                 order_plugin: str = 'futures_first'):
        """Initialize service with dependencies."""
        self.collector = create_collector(collector_type)
        self.trader = create_trader(trader_type)
        self.order_plugin = get_plugin(order_plugin)
        
        # Cache for active machines
        self._machines: Dict[int, BatchPhaseMachine] = {}
        
        # Configuration
        self._config = {
            'slippage': float(SLIPPAGE),
            'default_timeout': 300
        }
        
        logger.info("PhaseService initialized")
    
    # ==================== Public API ====================
    
    async def process(self, message: PhaseMessage) -> PhaseResult:
        """
        处理消息 - 核心方法
        
        Args:
            message: PhaseMessage with batch_id, event_type, source, params
            
        Returns:
            PhaseResult with execution result
        """
        logger.info(f"Processing: {message}")
        
        try:
            # 1. Get or create state machine
            machine = await self._get_machine(message.batch_id)
            
            # 2. Execute based on event type and current state
            result = await self._execute(machine, message)
            
            # 3. Save state
            machine.save_to_batch()
            
            return result
            
        except Exception as e:
            logger.error(f"Process error: {e}", exc_info=True)
            return PhaseResult(
                success=False,
                message=str(e)
            )
    
    async def get_state(self, batch_id: int) -> Optional[str]:
        """Get current state of a batch."""
        machine = self._machines.get(batch_id)
        return machine.state if machine else None
    
    async def remove(self, batch_id: int):
        """Remove state machine for a batch."""
        if batch_id in self._machines:
            del self._machines[batch_id]
    
    # ==================== Private Methods ====================
    
    async def _get_machine(self, batch_id: int) -> BatchPhaseMachine:
        """Get or create state machine."""
        if batch_id in self._machines:
            return self._machines[batch_id]
        
        # Load from database
        session = get_session()
        batch = session.query(BatchExecute).filter(
            BatchExecute.id == batch_id
        ).first()
        
        if not batch:
            session.close()
            raise ValueError(f"Batch {batch_id} not found")
        
        # Create machine
        machine = BatchPhaseMachine.load_from_batch(
            batch,
            collector=self.collector,
            trader=self.trader,
            order_plugin=self.order_plugin
        )
        
        self._machines[batch_id] = machine
        
        session.close()
        
        logger.info(f"Loaded machine for batch {batch_id}: state={machine.state}")
        
        return machine
    
    async def _execute(self, machine: BatchPhaseMachine, 
                  message: PhaseMessage) -> PhaseResult:
        """
        Execute based on event type.
        
        This is the core decision logic that maps events to state machine.
        """
        state = machine.state
        event = message.event_type
        params = message.params
        
        action_taken = ''
        
        # ================== State Machine Execution ==================
        
        # --- PENDING: Initialize params ---
        if state == PhaseState.PENDING:
            if event == EventType.EXECUTE or event == EventType.WAKE:
                machine.initialize_params()
                action_taken = 'initialize_params'
            else:
                return PhaseResult(
                    success=True,
                    current_state=state,
                    action_taken='ignored',
                    message=f'Event {event} ignored in PENDING'
                )
        
        # --- FIRST_ORDER_OPEN: Open first order ---
        elif state == PhaseState.FIRST_ORDER_OPEN:
            if event == EventType.EXECUTE:
                machine.open_first_order()
                action_taken = 'open_first_order'
            else:
                return PhaseResult(
                    success=True,
                    current_state=state,
                    action_taken='ignored',
                    message=f'Event {event} ignored in FIRST_ORDER_OPEN'
                )
        
        # --- FIRST_ORDER_WAIT: Watch order ---
        elif state == PhaseState.FIRST_ORDER_WAIT:
            if event == EventType.ORDER_FILLED:
                filled_price = params.get('filled_price', machine.context.contract_price)
                machine.first_order_filled(filled_price=filled_price)
                action_taken = 'first_order_filled'
                
                # Auto advance
                machine.proceed_to_second()
                action_taken += ' -> proceed_to_second'
                
            elif event == EventType.ORDER_CANCELLED:
                machine.retry_first_order()
                action_taken = 'retry_first_order'
                
            elif event == EventType.ORDER_REJECTED:
                machine.retry_first_order()
                action_taken = 'retry_first_order'
                
            elif event == EventType.TIMEOUT:
                # Mark as failed
                machine._context.complete_reason = 'TIMEOUT'
                machine.state = PhaseState.COMPLETED
                action_taken = 'timeout'
                
            else:
                # Just keep watching
                action_taken = 'watching'
        
        # --- FIRST_FILLED: Proceed to second ---
        elif state == PhaseState.FIRST_FILLED:
            if event == EventType.EXECUTE:
                machine.proceed_to_second()
                action_taken = 'proceed_to_second'
            else:
                action_taken = 'ignored'
        
        # --- SECOND_ORDER_OPEN: Open second order ---
        elif state == PhaseState.SECOND_ORDER_OPEN:
            if event == EventType.EXECUTE:
                machine.open_second_order()
                action_taken = 'open_second_order'
            else:
                action_taken = 'ignored'
        
        # --- SECOND_ORDER_WAIT: Watch order ---
        elif state == PhaseState.SECOND_ORDER_WAIT:
            if event == EventType.ORDER_FILLED:
                filled_price = params.get('filled_price', machine.context.spot_price)
                machine.second_order_filled(filled_price=filled_price)
                action_taken = 'second_order_filled'
                
            elif event == EventType.ORDER_CANCELLED:
                machine.proceed_to_second()
                action_taken = 'retry_second_order'
                
            elif event == EventType.TIMEOUT:
                machine._context.complete_reason = 'TIMEOUT'
                machine.state = PhaseState.COMPLETED
                action_taken = 'timeout'
                
            else:
                action_taken = 'watching'
        
        # --- COMPLETED: No more actions ---
        elif state == PhaseState.COMPLETED:
            action_taken = 'already_completed'
        
        # Unknown state
        else:
            return PhaseResult(
                success=False,
                current_state=state,
                action_taken='error',
                message=f'Unknown state: {state}'
            )
        
        return PhaseResult(
            success=True,
            current_state=machine.state,
            action_taken=action_taken,
            data={
                'batch_id': machine.context.batch_id,
                'order_sequence': machine.context.order_sequence
            }
        )
    
    # ==================== Batch Lifecycle ====================
    
    async def wake_batch(self, batch_id: int) -> PhaseResult:
        """Wake a pending batch."""
        session = get_session()
        
        # Check running contracts
        running = session.query(BatchExecute).filter(
            BatchExecute.execute_status == 'RUNNING'
        ).all()
        contracts_running = {b.position.contract for b in running}
        
        # Find pending batch
        batch = session.query(BatchExecute).filter(
            BatchExecute.id == batch_id,
            BatchExecute.execute_status == 'PENDING'
        ).first()
        
        if not batch:
            session.close()
            return PhaseResult(
                success=False,
                message=f"Batch {batch_id} not PENDING"
            )
        
        if batch.position.contract in contracts_running:
            session.close()
            return PhaseResult(
                success=False,
                message=f"Contract {batch.position.contract} already running"
            )
        
        # Wake batch
        batch.execute_status = 'RUNNING'
        batch.phase = PhaseState.PENDING
        batch.updated_at = datetime.utcnow()
        session.commit()
        session.close()
        
        # Execute
        message = PhaseMessage.create(
            batch_id=batch_id,
            event=EventType.WAKE,
            source=EventSource.SCHEDULER
        )
        
        return await self.process(message)
    
    async def execute_batch(self, batch_id: int) -> PhaseResult:
        """Execute a batch's current phase."""
        message = PhaseMessage.create(
            batch_id=batch_id,
            event=EventType.EXECUTE,
            source=EventSource.SCHEDULER
        )
        return await self.process(message)
    
    async def on_order_filled(self, batch_id: int, order_id: str, 
                          filled_price: float) -> PhaseResult:
        """Handle order filled event."""
        message = PhaseMessage.create(
            batch_id=batch_id,
            event=EventType.ORDER_FILLED,
            source=EventSource.WEBSOCKET,
            order_id=order_id,
            filled_price=filled_price
        )
        return await self.process(message)
    
    async def on_order_cancelled(self, batch_id: int, order_id: str) -> PhaseResult:
        """Handle order cancelled event."""
        message = PhaseMessage.create(
            batch_id=batch_id,
            event=EventType.ORDER_CANCELLED,
            source=EventSource.WEBSOCKET,
            order_id=order_id
        )
        return await self.process(message)


# ==================== Factory ====================

def create_phase_service(**config) -> PhaseService:
    """Factory function to create PhaseService."""
    return PhaseService(**config)