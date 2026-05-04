"""
Phase Execution Service - 事件路由层

职责：
1. 事件发布/订阅
2. 调用BatchExecutionService处理业务逻辑
3. 管理事件队列和分发
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)

from services.batch_service import BatchExecutionService

# Event types
class PhaseEventType(str, Enum):
    """Phase execution event types."""
    BATCH_WAKE = 'BATCH_WAKE'
    BATCH_EXECUTE = 'BATCH_EXECUTE'
    BATCH_TIMEOUT = 'BATCH_TIMEOUT'
    ORDER_FILLED = 'ORDER_FILLED'
    ORDER_CANCELLED = 'ORDER_CANCELLED'
    ORDER_REJECTED = 'ORDER_REJECTED'
    BATCH_RETRY = 'BATCH_RETRY'
    BATCH_CANCEL = 'BATCH_CANCEL'


@dataclass
class PhaseEvent:
    """Phase execution event."""
    event_type: PhaseEventType
    batch_id: int
    data: Dict[str, Any] = field(default_factory=dict)
    source: str = 'unknown'
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


class PhaseService:
    """
    Phase Execution Service - 事件路由层
    
    职责：
    1. 管理事件队列
    2. 路由事件到BatchExecutionService
    3. 提供事件发布接口
    """
    
    def __init__(self, config: PhaseServiceConfig = None):
        """Initialize phase service."""
        self.config = config or PhaseServiceConfig()
        
        # 创建BatchExecutionService (业务逻辑层)
        from services import create_collector, create_trader
        from plugins.order_sequence import get_plugin
        from events.order_watcher import SchedulerOrderWatcher
        
        collector = create_collector(self.config.collector_type)
        trader = create_trader(self.config.trader_type)
        order_plugin = get_plugin(self.config.order_plugin)
        order_watcher = SchedulerOrderWatcher(self)
        
        self.batch_service = BatchExecutionService(
            collector=collector,
            trader=trader,
            order_plugin=order_plugin,
            order_watcher=order_watcher
        )
        
        # Event queue for async processing
        self._event_queue: asyncio.Queue = asyncio.Queue()
        
        # Running state
        self._running = False
        self._worker_task: Optional[asyncio.Task] = None
        
        # Event handlers (只做路由，调用service层)
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
        await self.batch_service.order_watcher.start()
        
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
        await self.batch_service.order_watcher.stop()
        
        logger.info("PhaseService stopped")
    
    # ==================== Event Publishing ====================
    
    async def publish(self, event: PhaseEvent):
        """Publish an event to be processed."""
        await self._event_queue.put(event)
        logger.debug(f"Event published: {event}")
    
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
                event = await asyncio.wait_for(
                    self._event_queue.get(),
                    timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            
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
    
    # ==================== Event Handlers (只做路由，调用service) ====================
    
    async def _handle_batch_wake(self, event: PhaseEvent):
        """Handle BATCH_WAKE: 调用service唤醒批次"""
        woken = await self.batch_service.wake_pending_batches()
        if woken > 0:
            logger.info(f"Woke {woken} pending batches")
    
    async def _handle_batch_execute(self, event: PhaseEvent):
        """Handle BATCH_EXECUTE: 调用service执行批次"""
        success = await self.batch_service.execute_batch(event.batch_id)
        if not success:
            logger.warning(f"Failed to execute batch {event.batch_id}")
    
    async def _handle_batch_timeout(self, event: PhaseEvent):
        """Handle BATCH_TIMEOUT: 调用service处理超时"""
        await self.batch_service.timeout_batch(event.batch_id)
    
    async def _handle_order_filled(self, event: PhaseEvent):
        """Handle ORDER_FILLED: 调用service处理订单成交"""
        batch_id = event.batch_id
        filled_price = event.data.get('filled_price', 0)
        await self.batch_service.handle_order_filled(batch_id, filled_price)
    
    async def _handle_order_cancelled(self, event: PhaseEvent):
        """Handle ORDER_CANCELLED: 调用service处理订单取消"""
        await self.batch_service.handle_order_cancelled(event.batch_id)
    
    async def _handle_order_rejected(self, event: PhaseEvent):
        """Handle ORDER_REJECTED: 调用service处理订单拒绝"""
        await self.batch_service.handle_order_rejected(event.batch_id)
    
    async def _handle_batch_retry(self, event: PhaseEvent):
        """Handle BATCH_RETRY: 调用service重试批次"""
        await self.batch_service.retry_batch(event.batch_id)
    
    async def _handle_batch_cancel(self, event: PhaseEvent):
        """Handle BATCH_CANCEL: 调用service取消批次"""
        await self.batch_service.cancel_batch(event.batch_id)
    
    # ==================== Delegated Properties ====================
    
    # 不再缓存状态机，以下属性已移除：
    # get_machine, get_all_machines, active_count
    # 如需获取状态机，请通过 batch_service._create_machine_for_batch 从数据库重新创建
    
    @property
    def order_watcher(self):
        """Get order watcher."""
        return self.batch_service.order_watcher
