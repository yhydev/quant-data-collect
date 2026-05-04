"""
Order watcher with hybrid approach: WebSocket + Polling fallback.
Monitors order status changes and triggers callbacks.
"""
import asyncio
import logging
import time
import json
from typing import Callable, Optional, Dict, Any
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime

import aiohttp
import websockets

from models.database import get_async_session, get_session, BatchExecute

logger = logging.getLogger(__name__)


class OrderStatus(str, Enum):
    """Order status constants."""
    PENDING = 'NEW'           # 挂单中
    PARTIALLY_FILLED = 'PARTIALLY_FILLED'  # 部分成交
    FILLED = 'FILLED'         # 完全成交
    CANCELLED = 'CANCELLED'   # 已取消
    REJECTED = 'REJECTED'     # 被拒绝
    EXPIRED = 'EXPIRED'       # 已过期
    UNKNOWN = 'UNKNOWN'


@dataclass
class OrderUpdate:
    """Order update event."""
    order_id: str
    symbol: str
    status: OrderStatus
    filled_price: float = 0.0
    filled_quantity: float = 0.0
    avg_price: float = 0.0
    update_time: datetime = field(default_factory=datetime.utcnow)


@dataclass
class WatcherConfig:
    """Configuration for order watcher."""
    # WebSocket settings
    ws_url: str = "wss://fstream.binance.com/stream"
    ws_reconnect_interval: int = 5  # seconds
    ws_ping_interval: int = 30
    
    # Polling fallback settings
    use_polling_fallback: bool = True
    polling_intervals: list = field(default_factory=lambda: [1, 1, 2, 2, 5, 5, 10, 30, 60])
    
    # Timeout settings
    default_timeout: int = 300  # 5 minutes


class OrderWatcher:
    """
    Hybrid order watcher: WebSocket + Polling fallback.
    
    Primary: WebSocket for real-time updates
    Fallback: Polling when WebSocket fails
    """
    
    def __init__(
        self,
        trader: Any,
        config: WatcherConfig = None,
        on_order_update: Callable[[OrderUpdate], asyncio.coroutines] = None
    ):
        self.trader = trader
        self.config = config or WatcherConfig()
        self.on_order_update = on_order_update
        
        # State
        self._ws = None
        self._ws_task: Optional[asyncio.Task] = None
        self._running = False
        self._connected = False
        
        # Active watchers: order_id -> info
        self._watching_orders: Dict[str, Dict[str, Any]] = {}
        
        # Fallback polling tasks
        self._polling_tasks: Dict[str, asyncio.Task] = {}
        
        # Metrics
        self._ws_messages = 0
        self._polling_queries = 0
    
    async def start(self):
        """Start the watcher."""
        self._running = True
        
        # Start WebSocket listener
        self._ws_task = asyncio.create_task(self._ws_loop())
        
        logger.info("OrderWatcher started")
    
    async def stop(self):
        """Stop the watcher gracefully."""
        self._running = False
        
        # 1. Cancel WebSocket task
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
            self._ws_task = None
        
        # 2. Cancel all polling tasks
        for task in self._polling_tasks.values():
            task.cancel()
        self._polling_tasks.clear()
        
        # 3. Close WebSocket
        if self._ws:
            await self._ws.close()
            self._ws = None
        
        logger.info(f"OrderWatcher stopped. WS msgs: {self._ws_messages}, Polling queries: {self._polling_queries}")
    
    async def watch(self, order_id: str, symbol: str, timeout: int = None):
        """
        Watch an order for status changes.
        
        Args:
            order_id: Order ID to watch
            symbol: Trading symbol
            timeout: Watch timeout in seconds
        """
        if not self._running:
            raise RuntimeError("Watcher not started")
        
        timeout = timeout or self.config.default_timeout
        
        info = {
            'order_id': order_id,
            'symbol': symbol,
            'watched_at': datetime.utcnow(),
            'timeout': timeout,
            'status': OrderStatus.PENDING
        }
        
        self._watching_orders[order_id] = info
        
        # Start polling fallback if WebSocket not connected
        if self.config.use_polling_fallback and not self._connected:
            self._start_polling(order_id, symbol)
        
        logger.debug(f"Watching order {order_id} on {symbol}")
    
    async def unwatch(self, order_id: str):
        """Stop watching an order."""
        # Cancel polling task
        if order_id in self._polling_tasks:
            self._polling_tasks[order_id].cancel()
            del self._polling_tasks[order_id]
        
        # Remove from watching
        if order_id in self._watching_orders:
            del self._watching_orders[order_id]
    
    # ==================== WebSocket ====================
    
    async def _ws_loop(self):
        """WebSocket connection loop with auto-reconnect."""
        while self._running:
            try:
                await self._ws_connect()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"WebSocket error: {e}, reconnecting in {self.config.ws_reconnect_interval}s")
            
            if self._running:
                await asyncio.sleep(self.config.ws_reconnect_interval)
    
    async def _ws_connect(self):
        """Connect to WebSocket and subscribe."""
        logger.info("Connecting to WebSocket...")
        
        async with websockets.connect(
            self.config.ws_url,
            ping_interval=self.config.ws_ping_interval
        ) as ws:
            self._ws = ws
            self._connected = True
            
            # Subscribe to order updates for all watched orders
            await self._ws_subscribe()
            
            # Listen for messages
            try:
                async for message in ws:
                    if not self._running:
                        break
                    await self._ws_handle_message(message)
            except websockets.ConnectionClosed:
                self._connected = False
                raise
    
    async def _ws_subscribe(self):
        """Subscribe to order update streams."""
        if not self._ws or not self._watching_orders:
            return
        
        # Note: For spot orders, Binance requires user data stream (listenKey)
        # For futures, we can use <symbol>@executionReport
        # This is a simplified version - production should handle both
        symbols = set()
        for info in self._watching_orders.values():
            symbol = info['symbol'].lower()
            # For now, only subscribe to futures execution reports
            # Spot orders should use user data stream with listenKey
            symbols.add(f"{symbol}@executionReport")
        
        if symbols:
            await self._ws.send(json.dumps({
                "method": "SUBSCRIBE",
                "params": list(symbols),
                "id": int(time.time() * 1000)
            }))
            logger.debug(f"Subscribed to {len(symbols)} streams")
    
    async def _ws_handle_message(self, message: str):
        """Handle WebSocket message."""
        self._ws_messages += 1
        
        try:
            data = json.loads(message)
            
            # Check for error
            if 'error' in data:
                logger.error(f"WS error: {data}")
                return
            
            # Handle stream data
            if 'data' in data:
                await self._ws_process_update(data['data'])
            elif 'e' in data:  # Direct execution report
                await self._ws_process_update(data)
                
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON: {message[:100]}")
    
    async def _ws_process_update(self, data: Dict):
        """Process order execution report."""
        order_id = str(data.get('i'))  # orderId
        symbol = data.get('s')          # symbol
        status_code = data.get('X')     # orderStatus
        
        # Find matching order
        if order_id not in self._watching_orders:
            return
        
        info = self._watching_orders[order_id]
        
        # Parse status
        status = OrderStatus.UNKNOWN
        if status_code == 'NEW':
            status = OrderStatus.PENDING
        elif status_code == 'PARTIALLY_FILLED':
            status = OrderStatus.PARTIALLY_FILLED
        elif status_code == 'FILLED':
            status = OrderStatus.FILLED
        elif status_code == 'CANCELLED':
            status = OrderStatus.CANCELLED
        elif status_code == 'EXPIRED':
            status = OrderStatus.EXPIRED
        elif status_code == 'REJECTED':
            status = OrderStatus.REJECTED
        
        # Create update event
        update = OrderUpdate(
            order_id=order_id,
            symbol=symbol,
            status=status,
            filled_price=float(data.get('L', 0)),  # lastPrice
            filled_quantity=float(data.get('z', 0)),  # cumulativeFilledQty
            avg_price=float(data.get('ap', 0))  # avgPrice
        )
        
        logger.info(f"WS Order update: {order_id} -> {status.value}")
        
        # Trigger callback
        if self.on_order_update:
            await self.on_order_update(update)
        
        # Auto-unwatch if completed
        if status in (OrderStatus.FILLED, OrderStatus.CANCELLED, 
                      OrderStatus.REJECTED, OrderStatus.EXPIRED):
            await self.unwatch(order_id)
    
    # ==================== Polling Fallback ====================
    
    def _start_polling(self, order_id: str, symbol: str):
        """Start polling fallback for an order."""
        if order_id in self._polling_tasks:
            return
        
        task = asyncio.create_task(
            self._polling_loop(order_id, symbol)
        )
        self._polling_tasks[order_id] = task
    
    async def _polling_loop(self, order_id: str, symbol: str):
        """Polling loop with exponential backoff."""
        intervals = self.config.polling_intervals
        start_time = time.time()
        
        # Determine if this is a spot order based on watch info
        is_spot = False
        if order_id in self._watching_orders:
            info = self._watching_orders[order_id]
            is_spot = info.get('is_spot', False)
        
        for interval in intervals:
            if not self._running or order_id not in self._watching_orders:
                return
            
            # Check if WebSocket is connected - switch to WS
            if self._connected:
                logger.debug(f"WebSocket connected, stopping polling for {order_id}")
                return
            
            # Wait
            await asyncio.sleep(interval)
            self._polling_queries += 1
            
            # Check order status
            try:
                status_data = await self.trader.get_order_status(symbol, int(order_id), is_spot)
                status = status_data.get('status', 'UNKNOWN')
                
                if status == 'FILLED':
                    update = OrderUpdate(
                        order_id=order_id,
                        symbol=symbol,
                        status=OrderStatus.FILLED,
                        avg_price=float(status_data.get('avgPrice', 0))
                    )
                    logger.info(f"Polling order filled: {order_id}")
                    
                    if self.on_order_update:
                        await self.on_order_update(update)
                    
                    await self.unwatch(order_id)
                    return
                
                elif status in ('CANCELLED', 'EXPIRED', 'REJECTED'):
                    update = OrderUpdate(
                        order_id=order_id,
                        symbol=symbol,
                        status=OrderStatus(status)
                    )
                    
                    if self.on_order_update:
                        await self.on_order_update(update)
                    
                    await self.unwatch(order_id)
                    return
                    
            except Exception as e:
                logger.warning(f"Polling error for {order_id}: {e}")
        
        # Timeout - continue with long interval
        while True:
            if not self._running or order_id not in self._watching_orders:
                return
            
            await asyncio.sleep(60)
            self._polling_queries += 1
            
            # Check one more time
            try:
                status_data = await self.trader.get_order_status(symbol, int(order_id), is_spot)
                if status_data.get('status') == 'FILLED':
                    update = OrderUpdate(
                        order_id=order_id,
                        symbol=symbol,
                        status=OrderStatus.FILLED
                    )
                    if self.on_order_update:
                        await self.on_order_update(update)
                    await self.unwatch(order_id)
                    return
            except Exception:
                pass
    
    # ==================== Utilities ====================
    
    @property
    def is_connected(self) -> bool:
        """Check if WebSocket is connected."""
        return self._connected
    
    @property
    def active_watches(self) -> int:
        """Number of active order watches."""
        return len(self._watching_orders)
    
    def get_stats(self) -> Dict:
        """Get watcher statistics."""
        return {
            'connected': self._connected,
            'active_watches': self.active_watches,
            'ws_messages': self._ws_messages,
            'polling_queries': self._polling_queries
        }


# ==================== Integration with Scheduler ====================

class SchedulerOrderWatcher:
    """
    Integration layer: OrderWatcher -> Phase Scheduler.
    Maps order updates to phase transitions.
    """
    
    def __init__(self, scheduler: Any):
        self.scheduler = scheduler
        self.watcher = OrderWatcher(
            trader=scheduler.trader,
            on_order_update=self._on_order_update
        )
    
    async def start(self):
        """Start the watcher."""
        await self.watcher.start()
    
    async def stop(self):
        """Stop the watcher."""
        await self.watcher.stop()
    
    async def watch_order(self, batch_id: int, order_id: str, symbol: str, 
                       phase: str, timeout: int = 300, is_spot: bool = False):
        """
        Watch an order and trigger phase transition on fill.
        
        Args:
            batch_id: Batch ID
            order_id: Order ID
            symbol: Trading symbol
            phase: Current phase (e.g., 'FIRST_ORDER_WAIT')
            timeout: Watch timeout in seconds
            is_spot: True if this is a spot order
        """
        await self.watcher.watch(order_id, symbol, timeout)
        
        # Store phase mapping for callback
        self.watcher._watching_orders[order_id]['batch_id'] = batch_id
        self.watcher._watching_orders[order_id]['phase'] = phase
        self.watcher._watching_orders[order_id]['is_spot'] = is_spot
    
    async def _on_order_update(self, update: OrderUpdate):
        """Handle order update and trigger phase transition."""
        order_id = update.order_id
        
        if order_id not in self.watcher._watching_orders:
            return
        
        info = self.watcher._watching_orders[order_id]
        batch_id = info.get('batch_id')
        current_phase = info.get('phase')
        
        if not batch_id or not current_phase:
            return
        
        async with get_async_session() as session:
            from sqlalchemy import select
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.id == batch_id)
            )
            batch = result.scalar_one_or_none()
            
            if not batch:
                return
            
            # Map status to phase transition
            if update.status == OrderStatus.FILLED:
                # Determine next phase and action
                if current_phase == 'FIRST_ORDER_WAIT':
                    # Update batch with filled price
                    batch.first_side_filled_price = update.avg_price or batch.contract_price
                    batch.phase = 'FIRST_FILLED'
                    batch.updated_at = datetime.utcnow()
                    await session.commit()
                    
                elif current_phase == 'SECOND_ORDER_WAIT':
                    # Handle second order filled → transfer to savings
                    await self._handle_second_order_filled(session, batch, update.avg_price)
                
                # Unwatch after handling
                await self.watcher.unwatch(order_id)
                
            elif update.status in (OrderStatus.CANCELLED, OrderStatus.REJECTED):
                # Handle failure - re-open order
                if current_phase == 'FIRST_ORDER_WAIT':
                    batch.phase = 'PENDING'  # Will retry
                    batch.updated_at = datetime.utcnow()
                    await session.commit()
                elif current_phase == 'SECOND_ORDER_WAIT':
                    # Go back to first filled
                    batch.phase = 'FIRST_FILLED'
                    batch.updated_at = datetime.utcnow()
                    await session.commit()
                
                await self.watcher.unwatch(order_id)
    
    async def _handle_second_order_filled(self, session, batch: BatchExecute, filled_price: float = None):
        """Handle second order filled - transfer to savings and complete."""
        # Update filled price
        if filled_price:
            batch.second_side_filled_price = filled_price
        
        # Calculate actual quantity to transfer
        asset = batch.position.contract.replace('USDT', '')
        spot_quantity = (batch.batch_value or 1000) / batch.spot_price if batch.spot_price else 0
        
        # Transfer to savings
        transfer_result = await self.scheduler.trader.transfer_to_savings(
            batch.position.contract,
            round(spot_quantity, 6)
        )
        
        batch.execute_status = 'COMPLETED'
        batch.complete_reason = 'SUCCESS'
        batch.phase = 'COMPLETED'
        batch.updated_at = datetime.utcnow()
        await session.commit()
        
        # Check position complete
        await self.scheduler._check_position_complete(batch.position_execute_id)