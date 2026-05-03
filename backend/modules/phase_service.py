"""
Phase Execution Service - 解耦事件源与状态机

统一的事件处理服务：
- PhaseService: 管理状态机，处理所有事件
- 事件源（WebSocket、轮询、定时任务）只负责发送事件
- 事件源与状态机完全解耦
"""
import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)

from .order_watcher import OrderWatcher, OrderStatus as OWStatus

from ..database import get_session, BatchExecute, PositionExecute
from .order_watcher import SchedulerOrderWatcher
from . import create_collector, create_trader
from ..plugins.order_sequence import get_plugin

from .phase_machine import BatchPhaseMachine, PhaseState


# Configuration
SLIPPAGE = Decimal('0.001')
DEFAULT_ORDER_TIMEOUT = 300


# Event types
class PhaseEventType(str, Enum):
    """Phase execution event types."""
    # Timer events
    BATCH_WAKE = 'BATCH_WAKE'           # 唤醒pending批次
    BATCH_EXECUTE = 'BATCH_EXECUTE'       # 执行批次
    BATCH_TIMEOUT = 'BATCH_TIMEOUT'     # 批次超时
    
    # Order events (from OrderWatcher)
    ORDER_FILLED = 'ORDER_FILLED'        # 订单成交
    ORDER_CANCELLED = 'ORDER_CANCELLED'   # 订单取消
    ORDER_REJECTED = 'ORDER_REJECTED'     # 订单被拒
    
    # Manual events
    BATCH_RETRY = 'BATCH_RETRY'        # 重试批次
    BATCH_CANCEL = 'BATCH_CANCEL'       # 取消批次


@dataclass
class PhaseEvent:
    """Phase execution event."""
    event_type: PhaseEventType
    batch_id: int
    data: Dict[str, Any] = field(default_factory=dict)
    source: str = 'unknown'  # 'scheduler', 'websocket', 'polling', 'manual'
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def __repr__(self):
        return f"PhaseEvent({self.event_type.value}, batch={self.batch_id}, source={self.source})"


@dataclass
class PhaseServiceConfig:
    """Phase service configuration."""
    collector_type: str = 'binance'
    trader_type: str = 'binance'
    order_plugin: str = 'futures_first'
    default_timeout: int = 300
    enable_auto_save: bool = True


class PhaseService:
    """
    Phase Execution Service - 统一的事件处理服务
    
    职责：
    1. 管理状态机实例 (batch_id -> machine)
    2. 处理所有事件源的事件
    3. 协调各组件（collector, trader, order_watcher）
    4. 持久化状态到数据库
    
    事件源（解耦）：
    - 定时任务 -> 发布 BATCH_WAKE, BATCH_EXECUTE
    - OrderWatcher -> 发布 ORDER_FILLED, ORDER_CANCELLED
    - 手动操作 -> 发布 BATCH_RETRY, BATCH_CANCEL
    """
    
    def __init__(self, config: PhaseServiceConfig = None):
        """Initialize phase service."""
        self.config = config or PhaseServiceConfig()
        
        # Core components
        self.collector = create_collector(self.config.collector_type)
        self.trader = create_trader(self.config.trader_type)
        self.order_plugin = get_plugin(self.config.order_plugin)
        
        # Order watcher (事件消费者)
        self.order_watcher = SchedulerOrderWatcher(self)
        
        # State machines: batch_id -> machine
        self._machines: Dict[int, BatchPhaseMachine] = {}
        
        # Event queue for async processing
        self._event_queue: asyncio.Queue = asyncio.Queue()
        
        # Running state
        self._running = False
        self._worker_task: Optional[asyncio.Task] = None
        
        # Event handlers
        self._event_handlers: Dict[PhaseEventType, Callable] = {
            PhaseEventType.BATCH_WAKE: self._handle_batch_wake,
            PhaseEventType.BATCH_EXECUTE: self._handle_batch_execute,
            PhaseEventType.BATCH_TIMEOUT: self._handle_batch_timeout,
            PhaseEventType.ORDER_FILLED: self._handle_order_filled,
            PhaseEventType.ORDER_CANCELLED: self._handle_order_cancelled,
            PhaseEventType.ORDER_REJECTED: self._handle_order_rejected,
            PhaseEventType.BATCH_RETRY: self._handle_batch_retry,
            PhaseEventType.BATCH_CANCEL: self._handle_batch_cancel,
        }
    
    # ==================== Lifecycle ====================
    
    async def start(self):
        """Start the service."""
        self._running = True
        
        # Start order watcher
        await self.order_watcher.start()
        
        # Start event worker
        self._worker_task = asyncio.create_task(self._event_worker())
        
        logger.info("PhaseService started")
    
    async def stop(self):
        """Stop the service gracefully."""
        self._running = False
        
        # Stop event worker
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        
        # Stop order watcher
        await self.order_watcher.stop()
        
        logger.info("PhaseService stopped")
    
    # ==================== Event Publishing (for event sources) ====================
    
    async def publish(self, event: PhaseEvent):
        """
        Publish an event to be processed.
        
        事件源调用此方法发布事件，而不是直接操作状态机。
        
        Args:
            event: Event to process
        """
        await self._event_queue.put(event)
        logger.debug(f"Event published: {event}")
    
    # Alias methods for convenience
    async def publish_batch_wake(self, batch_id: int):
        """Publish BATCH_WAKE event."""
        await self.publish(PhaseEvent(
            event_type=PhaseEventType.BATCH_WAKE,
            batch_id=batch_id,
            source='scheduler'
        ))
    
    async def publish_batch_execute(self, batch_id: int):
        """Publish BATCH_EXECUTE event."""
        await self.publish(PhaseEvent(
            event_type=PhaseEventType.BATCH_EXECUTE,
            batch_id=batch_id,
            source='scheduler'
        ))
    
    async def publish_order_filled(self, batch_id: int, order_id: str, filled_price: float):
        """Publish ORDER_FILLED event."""
        await self.publish(PhaseEvent(
            event_type=PhaseEventType.ORDER_FILLED,
            batch_id=batch_id,
            data={'order_id': order_id, 'filled_price': filled_price},
            source='order_watcher'
        ))
    
    async def publish_order_cancelled(self, batch_id: int, order_id: str):
        """Publish ORDER_CANCELLED event."""
        await self.publish(PhaseEvent(
            event_type=PhaseEventType.ORDER_CANCELLED,
            batch_id=batch_id,
            data={'order_id': order_id},
            source='order_watcher'
        ))
    
    # ==================== Event Worker ====================
    
    async def _event_worker(self):
        """Process events from queue."""
        while self._running:
            try:
                # Wait for event
                event = await asyncio.wait_for(
                    self._event_queue.get(),
                    timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            
            # Process event
            try:
                await self._process_event(event)
            except Exception as e:
                logger.error(f"Event processing error: {e}", exc_info=True)
    
    async def _process_event(self, event: PhaseEvent):
        """Process a single event."""
        logger.info(f"Processing event: {event}")
        
        handler = self._event_handlers.get(event.event_type)
        if not handler:
            logger.warning(f"No handler for event type: {event.event_type}")
            return
        
        await handler(event)
    
    # ==================== Event Handlers ====================
    
    async def _handle_batch_wake(self, event: PhaseEvent):
        """
        Handle BATCH_WAKE: 唤醒pending批次
        """
        session = get_session()
        
        # Get contracts already running
        contracts_running = set()
        running = session.query(BatchExecute).filter(
            BatchExecute.execute_status == 'RUNNING'
        ).all()
        for batch in running:
            contracts_running.add(batch.position.contract)
        
        # Find pending batches to wake
        pending = session.query(BatchExecute).filter(
            BatchExecute.execute_status == 'PENDING'
        ).order_by(BatchExecute.id).all()
        
        woken = 0
        for batch in pending:
            contract = batch.position.contract
            
            if contract in contracts_running:
                continue
            
            # Wake batch
            batch.execute_status = 'RUNNING'
            batch.phase = PhaseState.PENDING
            batch.updated_at = datetime.utcnow()
            session.commit()
            
            contracts_running.add(contract)
            woken += 1
            
            # Load or create state machine
            await self._get_or_create_machine(batch)
            
            logger.info(f"Batch {batch.id} woken: contract={contract}")
        
        if woken > 0:
            logger.info(f"Woke {woken} pending batches")
        
        session.close()
    
    async def _handle_batch_execute(self, event: PhaseEvent):
        """
        Handle BATCH_EXECUTE: 执行批次
        """
        session = get_session()
        
        batch = session.query(BatchExecute).filter(
            BatchExecute.id == event.batch_id
        ).first()
        
        if not batch:
            session.close()
            return
        
        # Check if already completed
        if batch.execute_status == 'COMPLETED':
            session.close()
            return
        
        # Check timeout
        elapsed = (datetime.utcnow() - batch.updated_at).total_seconds()
        if elapsed > batch.timeout:
            batch.execute_status = 'COMPLETED'
            batch.complete_reason = 'TIMEOUT'
            session.commit()
            logger.warning(f"Batch {batch.id} timeout")
            session.close()
            return
        
        # Get or create machine
        machine = await self._get_or_create_machine(batch)
        
        # Execute current phase
        try:
            await self._execute_current_phase(machine)
        except Exception as e:
            logger.error(f"Batch execution error: {e}")
            batch.execute_status = 'COMPLETED'
            batch.complete_reason = f'ERROR: {str(e)}'
            session.commit()
        
        session.close()
    
    async def _handle_batch_timeout(self, event: PhaseEvent):
        """Handle BATCH_TIMEOUT: 超时处理."""
        session = get_session()
        
        batch = session.query(BatchExecute).filter(
            BatchExecute.id == event.batch_id
        ).first()
        
        if batch and batch.execute_status == 'RUNNING':
            batch.execute_status = 'COMPLETED'
            batch.complete_reason = 'TIMEOUT'
            session.commit()
            
            # Remove machine
            if event.batch_id in self._machines:
                del self._machines[event.batch_id]
            
            logger.info(f"Batch {batch.id} marked as TIMEOUT")
        
        session.close()
    
    async def _handle_order_filled(self, event: PhaseEvent):
        """Handle ORDER_FILLED: 订单成交."""
        batch_id = event.batch_id
        filled_price = event.data.get('filled_price', 0)
        
        # Get machine
        machine = self._machines.get(batch_id)
        if not machine:
            logger.warning(f"No machine for batch {batch_id}")
            return
        
        # Determine which order was filled
        state = machine.state
        if state == PhaseState.FIRST_ORDER_WAIT:
            machine.first_order_filled(filled_price=filled_price)
        elif state == PhaseState.SECOND_ORDER_WAIT:
            machine.second_order_filled(filled_price=filled_price)
        
        # Auto advance
        if state == PhaseState.FIRST_ORDER_WAIT:
            machine.proceed_to_second()
        elif state == PhaseState.SECOND_ORDER_WAIT:
            logger.info(f"Batch {batch_id} completed")
    
    async def _handle_order_cancelled(self, event: PhaseEvent):
        """Handle ORDER_CANCELLED: 订单取消."""
        batch_id = event.batch_id
        
        machine = self._machines.get(batch_id)
        if not machine:
            return
        
        state = machine.state
        if state == PhaseState.FIRST_ORDER_WAIT:
            machine.retry_first_order()
        elif state == PhaseState.SECOND_ORDER_WAIT:
            machine.proceed_to_second()
    
    async def _handle_order_rejected(self, event: PhaseEvent):
        """Handle ORDER_REJECTED: 订单被拒."""
        await self._handle_order_cancelled(event)
    
    async def _handle_batch_retry(self, event: PhaseEvent):
        """Handle BATCH_RETRY: 重试批次."""
        session = get_session()
        
        batch = session.query(BatchExecute).filter(
            BatchExecute.id == event.batch_id
        ).first()
        
        if batch:
            batch.execute_status = 'RUNNING'
            batch.phase = PhaseState.PENDING
            batch.updated_at = datetime.utcnow()
            session.commit()
            
            # Create new machine
            await self._get_or_create_machine(batch)
        
        session.close()
    
    async def _handle_batch_cancel(self, event: PhaseEvent):
        """Handle BATCH_CANCEL: 取消批次."""
        session = get_session()
        
        batch = session.query(BatchExecute).filter(
            BatchExecute.id == event.batch_id
        ).first()
        
        if batch:
            batch.execute_status = 'COMPLETED'
            batch.complete_reason = 'CANCELLED'
            session.commit()
            session.close()
            
            # Remove machine
            if event.batch_id in self._machines:
                del self._machines[event.batch_id]
    
    # ==================== State Machine Management ====================
    
    async def _get_or_create_machine(self, batch: BatchExecute) -> BatchPhaseMachine:
        """Get or create state machine for batch."""
        batch_id = batch.id
        
        if batch_id in self._machines:
            return self._machines[batch_id]
        
        # Create new machine
        machine = BatchPhaseMachine.load_from_batch(
            batch,
            collector=self.collector,
            trader=self.trader,
            order_plugin=self.order_plugin,
            order_watcher=self.order_watcher
        )
        
        self._machines[batch_id] = machine
        
        # Set up order watcher callback
        await self.order_watcher.register_callback(batch_id, self.publish_order_filled)
        
        return machine
    
    async def _execute_current_phase(self, machine: BatchPhaseMachine):
        """Execute current phase of the state machine."""
        state = machine.state
        
        # Trigger appropriate transition based on current state
        if state == PhaseState.PENDING:
            machine.initialize_params()
        elif state == PhaseState.FIRST_ORDER_OPEN:
            machine.open_first_order()
        elif state == PhaseState.FIRST_ORDER_WAIT:
            # Already watching, just wait
            pass
        elif state == PhaseState.FIRST_FILLED:
            machine.proceed_to_second()
        elif state == PhaseState.SECOND_ORDER_OPEN:
            machine.open_second_order()
        elif state == PhaseState.SECOND_ORDER_WAIT:
            # Already watching, just wait
            pass
        elif state == PhaseState.COMPLETED:
            pass
    
    def get_machine(self, batch_id: int) -> Optional[BatchPhaseMachine]:
        """Get machine by batch ID."""
        return self._machines.get(batch_id)
    
    def get_all_machines(self) -> Dict[int, BatchPhaseMachine]:
        """Get all active machines."""
        return self._machines.copy()
    
    @property
    def active_count(self) -> int:
        """Number of active state machines."""
        return len(self._machines)


# ==================== Integration with Scheduler ====================

class SchedulerPhaseService:
    """
    Integration: Scheduler -> PhaseService
    Wraps PhaseService with scheduler integration.
    """
    
    def __init__(self):
        self.service = PhaseService()
    
    async def start(self):
        """Start the service."""
        await self.service.start()
    
    async def stop(self):
        """Stop the service."""
        await self.service.stop()
    
    @property
    def order_watcher(self):
        """Get order watcher for scheduler."""
        return self.service.order_watcher


# ==================== Integration with OrderWatcher ====================

class OrderWatcherCallback:
    """
    Integration: OrderWatcher -> PhaseService callback
    
    This class handles order status updates from OrderWatcher
    and publishes events to PhaseService.
    """
    
    def __init__(self, phase_service: PhaseService):
        self.phase_service = phase_service
        self._batch_order_map: Dict[str, int] = {}  # order_id -> batch_id
    
    async def on_order_update(self, order_id: str, batch_id: int, 
                          status: OWStatus, filled_price: float = None):
        """
        Handle order update from OrderWatcher.
        
        Args:
            order_id: Order ID
            batch_id: Batch ID
            status: Order status
            filled_price: Filled price (if filled)
        """
        self._batch_order_map[order_id] = batch_id
        
        if status == OWStatus.FILLED:
            await self.phase_service.publish_order_filled(
                batch_id=batch_id,
                order_id=order_id,
                filled_price=filled_price or 0
            )
        elif status == OWStatus.CANCELLED:
            await self.phase_service.publish(PhaseEvent(
                event_type=PhaseEventType.ORDER_CANCELLED,
                batch_id=batch_id,
                data={'order_id': order_id},
                source='websocket'
            ))
        elif status == OWStatus.REJECTED:
            await self.phase_service.publish(PhaseEvent(
                event_type=PhaseEventType.ORDER_REJECTED,
                batch_id=batch_id,
                data={'order_id': order_id},
                source='websocket'
            ))
    
    async def register_callback(self, batch_id: int, order_id: str):
        """Register callback for a batch/order."""
        self._batch_order_map[order_id] = batch_id