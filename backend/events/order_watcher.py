"""
Order watcher split into 4 components:
- SpotWebSocketWatcher: WebSocket for spot order updates
- FuturesWebSocketWatcher: WebSocket for futures order updates  
- SpotPollingTask: Polling fallback for spot orders
- FuturesPollingTask: Polling fallback for futures orders
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

from models.database import get_async_session, BatchExecute

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
    is_spot: bool = False


@dataclass
class WatcherConfig:
    """Configuration for order watchers."""
    # WebSocket settings
    spot_ws_url: str = "wss://stream.binance.com:9443/stream"
    futures_ws_url: str = "wss://fstream.binance.com/stream"
    ws_reconnect_interval: int = 5  # seconds
    ws_ping_interval: int = 30
    
    # Polling settings
    polling_intervals: list = field(default_factory=lambda: [1, 1, 2, 2, 5, 5, 10, 30, 60])
    default_timeout: int = 300  # 5 minutes


class SpotWebSocketWatcher:
    """WebSocket watcher for spot order updates."""
    
    def __init__(self, config: WatcherConfig = None):
        self.config = config or WatcherConfig()
        self.on_order_update: Callable[[OrderUpdate], None] = None
        
        self._ws = None
        self._ws_task: Optional[asyncio.Task] = None
        self._running = False
        self._connected = False
        self._listen_key = None
        
        # Active orders being watched
        self._watching_orders: Dict[str, Dict[str, Any]] = {}
        
        # Metrics
        self._ws_messages = 0
    
    async def start(self):
        """Start the WebSocket watcher."""
        self._running = True
        self._ws_task = asyncio.create_task(self._ws_loop())
        logger.info("SpotWebSocketWatcher started")
    
    async def stop(self):
        """Stop the watcher."""
        self._running = False
        
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        
        if self._ws:
            await self._ws.close()
            self._ws = None
        
        logger.info(f"SpotWebSocketWatcher stopped. Messages: {self._ws_messages}")
    
    def watch(self, order_id: str, symbol: str, batch_id: int, phase: str):
        """Add an order to watch."""
        self._watching_orders[order_id] = {
            'symbol': symbol,
            'batch_id': batch_id,
            'phase': phase,
            'is_spot': True
        }
    
    def unwatch(self, order_id: str):
        """Remove an order from watching."""
        self._watching_orders.pop(order_id, None)
    
    async def _ws_loop(self):
        """WebSocket connection loop with reconnect."""
        while self._running:
            try:
                await self._ws_connect()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Spot WS error: {e}, reconnecting in {self.config.ws_reconnect_interval}s")
            
            if self._running:
                await asyncio.sleep(self.config.ws_reconnect_interval)
    
    async def _ws_connect(self):
        """Connect to spot WebSocket with user data stream."""
        # Get listenKey for user data stream
        listen_key = await self._get_listen_key()
        if not listen_key:
            raise Exception("Failed to get listenKey")
        
        ws_url = f"{self.config.spot_ws_url}?streams={listen_key}"
        
        async with websockets.connect(
            ws_url,
            ping_interval=self.config.ws_ping_interval
        ) as ws:
            self._ws = ws
            self._connected = True
            
            async for message in ws:
                if not self._running:
                    break
                await self._handle_message(message)
        
        self._connected = False
    
    async def _get_listen_key(self) -> Optional[str]:
        """Get listenKey for user data stream."""
        try:
            async with aiohttp.ClientSession() as session:
                # This should use the trader's API key/secret
                # Simplified for now
                return None
        except Exception as e:
            logger.error(f"Failed to get listenKey: {e}")
            return None
    
    async def _handle_message(self, message: str):
        """Handle WebSocket message."""
        self._ws_messages += 1
        
        try:
            data = json.loads(message)
            
            # Handle user data stream
            if 'data' in data:
                event_data = data['data']
                if event_data.get('e') == 'executionReport':
                    await self._process_execution_report(event_data)
                    
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON: {message[:100]}")
        except Exception as e:
            logger.error(f"Error handling spot WS message: {e}")
    
    async def _process_execution_report(self, data: Dict):
        """Process execution report from spot WebSocket."""
        order_id = str(data.get('i'))  # orderId
        
        if order_id not in self._watching_orders:
            return
        
        status_code = data.get('X')  # orderStatus
        status = self._parse_status(status_code)
        
        if status == OrderStatus.UNKNOWN:
            return
        
        update = OrderUpdate(
            order_id=order_id,
            symbol=data.get('s', ''),
            status=status,
            filled_price=float(data.get('L', 0)),
            filled_quantity=float(data.get('z', 0)),
            avg_price=float(data.get('ap', 0)),
            is_spot=True
        )
        
        logger.info(f"Spot WS update: {order_id} -> {status.value}")
        
        if self.on_order_update:
            await self.on_order_update(update)
        
        # Auto-unwatch if completed
        if status in (OrderStatus.FILLED, OrderStatus.CANCELLED, 
                      OrderStatus.REJECTED, OrderStatus.EXPIRED):
            self.unwatch(order_id)
    
    def _parse_status(self, status_code: str) -> OrderStatus:
        """Parse status code to OrderStatus."""
        mapping = {
            'NEW': OrderStatus.PENDING,
            'PARTIALLY_FILLED': OrderStatus.PARTIALLY_FILLED,
            'FILLED': OrderStatus.FILLED,
            'CANCELLED': OrderStatus.CANCELLED,
            'REJECTED': OrderStatus.REJECTED,
            'EXPIRED': OrderStatus.EXPIRED,
        }
        return mapping.get(status_code, OrderStatus.UNKNOWN)


class FuturesWebSocketWatcher:
    """WebSocket watcher for futures order updates."""
    
    def __init__(self, config: WatcherConfig = None):
        self.config = config or WatcherConfig()
        self.on_order_update: Callable[[OrderUpdate], None] = None
        
        self._ws = None
        self._ws_task: Optional[asyncio.Task] = None
        self._running = False
        self._connected = False
        
        # Active orders being watched
        self._watching_orders: Dict[str, Dict[str, Any]] = {}
        
        # Metrics
        self._ws_messages = 0
    
    async def start(self):
        """Start the WebSocket watcher."""
        self._running = True
        self._ws_task = asyncio.create_task(self._ws_loop())
        logger.info("FuturesWebSocketWatcher started")
    
    async def stop(self):
        """Stop the watcher."""
        self._running = False
        
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        
        if self._ws:
            await self._ws.close()
            self._ws = None
        
        logger.info(f"FuturesWebSocketWatcher stopped. Messages: {self._ws_messages}")
    
    def watch(self, order_id: str, symbol: str, batch_id: int, phase: str):
        """Add an order to watch."""
        self._watching_orders[order_id] = {
            'symbol': symbol.lower(),
            'batch_id': batch_id,
            'phase': phase,
            'is_spot': False
        }
    
    def unwatch(self, order_id: str):
        """Remove an order from watching."""
        self._watching_orders.pop(order_id, None)
    
    async def _ws_loop(self):
        """WebSocket connection loop with reconnect."""
        while self._running:
            try:
                await self._ws_connect()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Futures WS error: {e}, reconnecting in {self.config.ws_reconnect_interval}s")
            
            if self._running:
                await asyncio.sleep(self.config.ws_reconnect_interval)
    
    async def _ws_connect(self):
        """Connect to futures WebSocket."""
        async with websockets.connect(
            self.config.futures_ws_url,
            ping_interval=self.config.ws_ping_interval
        ) as ws:
            self._ws = ws
            self._connected = True
            
            # Subscribe to execution reports for all watched symbols
            await self._subscribe()
            
            async for message in ws:
                if not self._running:
                    break
                await self._handle_message(message)
        
        self._connected = False
    
    async def _subscribe(self):
        """Subscribe to order update streams."""
        symbols = set()
        for info in self._watching_orders.values():
            symbols.add(f"{info['symbol']}@executionReport")
        
        if symbols:
            await self._ws.send(json.dumps({
                "method": "SUBSCRIBE",
                "params": list(symbols),
                "id": int(time.time() * 1000)
            }))
            logger.debug(f"Subscribed to {len(symbols)} futures streams")
    
    async def _handle_message(self, message: str):
        """Handle WebSocket message."""
        self._ws_messages += 1
        
        try:
            data = json.loads(message)
            
            if 'error' in data:
                logger.error(f"Futures WS error: {data}")
                return
            
            # Handle stream data
            if 'data' in data:
                await self._process_execution_report(data['data'])
            elif 'e' in data:  # Direct execution report
                await self._process_execution_report(data)
                
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON: {message[:100]}")
        except Exception as e:
            logger.error(f"Error handling futures WS message: {e}")
    
    async def _process_execution_report(self, data: Dict):
        """Process execution report from futures WebSocket."""
        order_id = str(data.get('i'))  # orderId
        
        if order_id not in self._watching_orders:
            return
        
        status_code = data.get('X')  # orderStatus
        status = self._parse_status(status_code)
        
        if status == OrderStatus.UNKNOWN:
            return
        
        update = OrderUpdate(
            order_id=order_id,
            symbol=data.get('s', ''),
            status=status,
            filled_price=float(data.get('L', 0)),
            filled_quantity=float(data.get('z', 0)),
            avg_price=float(data.get('ap', 0)),
            is_spot=False
        )
        
        logger.info(f"Futures WS update: {order_id} -> {status.value}")
        
        if self.on_order_update:
            await self.on_order_update(update)
        
        # Auto-unwatch if completed
        if status in (OrderStatus.FILLED, OrderStatus.CANCELLED, 
                      OrderStatus.REJECTED, OrderStatus.EXPIRED):
            self.unwatch(order_id)
    
    def _parse_status(self, status_code: str) -> OrderStatus:
        """Parse status code to OrderStatus."""
        mapping = {
            'NEW': OrderStatus.PENDING,
            'PARTIALLY_FILLED': OrderStatus.PARTIALLY_FILLED,
            'FILLED': OrderStatus.FILLED,
            'CANCELLED': OrderStatus.CANCELLED,
            'REJECTED': OrderStatus.REJECTED,
            'EXPIRED': OrderStatus.EXPIRED,
        }
        return mapping.get(status_code, OrderStatus.UNKNOWN)


class SpotPollingTask:
    """Polling task for spot order status."""
    
    def __init__(self, trader, config: WatcherConfig = None):
        self.trader = trader
        self.config = config or WatcherConfig()
        self.on_order_update: Callable[[OrderUpdate], None] = None
        
        self._running = False
        self._tasks: Dict[str, asyncio.Task] = {}
        self._polling_queries = 0
    
    async def start(self):
        """Start the polling task manager."""
        self._running = True
        logger.info("SpotPollingTask started")
    
    async def stop(self):
        """Stop all polling tasks."""
        self._running = False
        
        for task in self._tasks.values():
            task.cancel()
        self._tasks.clear()
        
        logger.info(f"SpotPollingTask stopped. Queries: {self._polling_queries}")
    
    def watch(self, order_id: str, symbol: str, batch_id: int, phase: str):
        """Start polling for an order."""
        if order_id in self._tasks:
            return
        
        task = asyncio.create_task(
            self._poll_loop(order_id, symbol, batch_id, phase)
        )
        self._tasks[order_id] = task
    
    def unwatch(self, order_id: str):
        """Stop polling for an order."""
        if order_id in self._tasks:
            self._tasks[order_id].cancel()
            del self._tasks[order_id]
    
    async def _poll_loop(self, order_id: str, symbol: str, batch_id: int, phase: str):
        """Polling loop with exponential backoff."""
        intervals = self.config.polling_intervals
        start_time = time.time()
        
        for interval in intervals:
            if not self._running or order_id not in self._tasks:
                return
            
            await asyncio.sleep(interval)
            self._polling_queries += 1
            
            try:
                status_data = await self.trader.get_order_status(symbol, int(order_id), is_spot=True)
                status = status_data.get('status', 'UNKNOWN')
                
                if status == 'FILLED':
                    update = OrderUpdate(
                        order_id=order_id,
                        symbol=symbol,
                        status=OrderStatus.FILLED,
                        avg_price=float(status_data.get('avgPrice', 0)),
                        is_spot=True
                    )
                    
                    logger.info(f"Spot polling: order {order_id} filled")
                    
                    if self.on_order_update:
                        await self.on_order_update(update)
                    
                    self.unwatch(order_id)
                    return
                    
                elif status in ('CANCELLED', 'EXPIRED', 'REJECTED'):
                    update = OrderUpdate(
                        order_id=order_id,
                        symbol=symbol,
                        status=OrderStatus(status)
                    )
                    
                    if self.on_order_update:
                        await self.on_order_update(update)
                    
                    self.unwatch(order_id)
                    return
                    
            except Exception as e:
                logger.warning(f"Spot polling error for {order_id}: {e}")
        
        # Timeout - continue with long interval
        while self._running and order_id in self._tasks:
            await asyncio.sleep(60)
            self._polling_queries += 1
            
            try:
                status_data = await self.trader.get_order_status(symbol, int(order_id), is_spot=True)
                if status_data.get('status') == 'FILLED':
                    update = OrderUpdate(
                        order_id=order_id,
                        symbol=symbol,
                        status=OrderStatus.FILLED,
                        is_spot=True
                    )
                    if self.on_order_update:
                        await self.on_order_update(update)
                    self.unwatch(order_id)
                    return
            except Exception:
                pass


class FuturesPollingTask:
    """Polling task for futures order status."""
    
    def __init__(self, trader, config: WatcherConfig = None):
        self.trader = trader
        self.config = config or WatcherConfig()
        self.on_order_update: Callable[[OrderUpdate], None] = None
        
        self._running = False
        self._tasks: Dict[str, asyncio.Task] = {}
        self._polling_queries = 0
    
    async def start(self):
        """Start the polling task manager."""
        self._running = True
        logger.info("FuturesPollingTask started")
    
    async def stop(self):
        """Stop all polling tasks."""
        self._running = False
        
        for task in self._tasks.values():
            task.cancel()
        self._tasks.clear()
        
        logger.info(f"FuturesPollingTask stopped. Queries: {self._polling_queries}")
    
    def watch(self, order_id: str, symbol: str, batch_id: int, phase: str):
        """Start polling for an order."""
        if order_id in self._tasks:
            return
        
        task = asyncio.create_task(
            self._poll_loop(order_id, symbol, batch_id, phase)
        )
        self._tasks[order_id] = task
    
    def unwatch(self, order_id: str):
        """Stop polling for an order."""
        if order_id in self._tasks:
            self._tasks[order_id].cancel()
            del self._tasks[order_id]
    
    async def _poll_loop(self, order_id: str, symbol: str, batch_id: int, phase: str):
        """Polling loop with exponential backoff."""
        intervals = self.config.polling_intervals
        
        for interval in intervals:
            if not self._running or order_id not in self._tasks:
                return
            
            await asyncio.sleep(interval)
            self._polling_queries += 1
            
            try:
                status_data = await self.trader.get_order_status(symbol, int(order_id), is_spot=False)
                status = status_data.get('status', 'UNKNOWN')
                
                if status == 'FILLED':
                    update = OrderUpdate(
                        order_id=order_id,
                        symbol=symbol,
                        status=OrderStatus.FILLED,
                        avg_price=float(status_data.get('avgPrice', 0)),
                        is_spot=False
                    )
                    
                    logger.info(f"Futures polling: order {order_id} filled")
                    
                    if self.on_order_update:
                        await self.on_order_update(update)
                    
                    self.unwatch(order_id)
                    return
                    
                elif status in ('CANCELLED', 'EXPIRED', 'REJECTED'):
                    update = OrderUpdate(
                        order_id=order_id,
                        symbol=symbol,
                        status=OrderStatus(status)
                    )
                    
                    if self.on_order_update:
                        await self.on_order_update(update)
                    
                    self.unwatch(order_id)
                    return
                    
            except Exception as e:
                logger.warning(f"Futures polling error for {order_id}: {e}")
        
        # Timeout - continue with long interval
        while self._running and order_id in self._tasks:
            await asyncio.sleep(60)
            self._polling_queries += 1
            
            try:
                status_data = await self.trader.get_order_status(symbol, int(order_id), is_spot=False)
                if status_data.get('status') == 'FILLED':
                    update = OrderUpdate(
                        order_id=order_id,
                        symbol=symbol,
                        status=OrderStatus.FILLED,
                        is_spot=False
                    )
                    if self.on_order_update:
                        await self.on_order_update(update)
                    self.unwatch(order_id)
                    return
            except Exception:
                pass


class UnifiedOrderWatcher:
    """
    Unified order watcher that combines all 4 components.
    Simplifies integration with BatchExecutionService.
    """
    
    def __init__(self, batch_service, config: WatcherConfig = None):
        self.batch_service = batch_service
        self.config = config or WatcherConfig()
        
        # Create 4 components
        self.spot_ws = SpotWebSocketWatcher(self.config)
        self.futures_ws = FuturesWebSocketWatcher(self.config)
        self.spot_polling = SpotPollingTask(batch_service.trader, self.config)
        self.futures_polling = FuturesPollingTask(batch_service.trader, self.config)
        
        # Set callbacks
        self.spot_ws.on_order_update = self._on_order_update
        self.futures_ws.on_order_update = self._on_order_update
        self.spot_polling.on_order_update = self._on_order_update
        self.futures_polling.on_order_update = self._on_order_update
        
        self._running = False
    
    async def start(self):
        """Start all watchers."""
        self._running = True
        
        await self.spot_ws.start()
        await self.futures_ws.start()
        await self.spot_polling.start()
        await self.futures_polling.start()
        
        logger.info("UnifiedOrderWatcher started (4 components)")
    
    async def stop(self):
        """Stop all watchers."""
        self._running = False
        
        await self.spot_ws.stop()
        await self.futures_ws.stop()
        await self.spot_polling.stop()
        await self.futures_polling.stop()
        
        logger.info("UnifiedOrderWatcher stopped")
    
    async def watch_order(self, batch_id: int, order_id: str, symbol: str, 
                         phase: str, is_spot: bool = False):
        """Watch an order using appropriate watcher."""
        if is_spot:
            self.spot_ws.watch(order_id, symbol, batch_id, phase)
            # Start polling as fallback if WS not connected
            if not self.spot_ws._connected:
                self.spot_polling.watch(order_id, symbol, batch_id, phase)
        else:
            self.futures_ws.watch(order_id, symbol, batch_id, phase)
            # Start polling as fallback if WS not connected
            if not self.futures_ws._connected:
                self.futures_polling.watch(order_id, symbol, batch_id, phase)
    
    async def unwatch_order(self, order_id: str):
        """Stop watching an order from all watchers."""
        self.spot_ws.unwatch(order_id)
        self.futures_ws.unwatch(order_id)
        self.spot_polling.unwatch(order_id)
        self.futures_polling.unwatch(order_id)
    
    async def _on_order_update(self, update: OrderUpdate):
        """Handle order update and trigger phase transition."""
        order_id = update.order_id
        
        # Find which watcher has this order
        watcher = self.spot_ws if update.is_spot else self.futures_ws
        if order_id not in watcher._watching_orders:
            watcher = self.spot_polling if update.is_spot else self.futures_polling
            if order_id not in watcher._watching_orders:
                return
        
        info = watcher._watching_orders[order_id]
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
                await self.unwatch_order(order_id)
                
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
                
                await self.unwatch_order(order_id)
    
    async def _handle_second_order_filled(self, session, batch: BatchExecute, filled_price: float = None):
        """Handle second order filled - transfer to savings and complete."""
        # Update filled price
        if filled_price:
            batch.second_side_filled_price = filled_price
        
        # Calculate actual quantity to transfer
        spot_quantity = (batch.batch_value or 1000) / batch.spot_price if batch.spot_price else 0
        
        # Transfer to savings
        transfer_result = await self.batch_service.trader.transfer_to_savings(
            batch.position.contract,
            round(spot_quantity, 6)
        )
        
        batch.execute_status = 'COMPLETED'
        batch.complete_reason = 'SUCCESS'
        batch.phase = 'COMPLETED'
        batch.updated_at = datetime.utcnow()
        await session.commit()
        
        # Check position complete
        await self.batch_service._check_position_complete(batch.position_execute_id)
