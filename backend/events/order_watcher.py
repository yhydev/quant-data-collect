"""
Simplified order watchers: WS + Polling for spot/futures.
Watchers only detect status changes and call service.handle_order_update().
All business logic (phase transitions, transfers) is in BatchExecutionService.
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
    PENDING = 'NEW'
    PARTIALLY_FILLED = 'PARTIALLY_FILLED'
    FILLED = 'FILLED'
    CANCELLED = 'CANCELLED'
    REJECTED = 'REJECTED'
    EXPIRED = 'EXPIRED'
    UNKNOWN = 'UNKNOWN'


@dataclass
class OrderUpdate:
    """Order update event - passed to service."""
    order_id: str
    symbol: str
    status: OrderStatus
    filled_price: float = 0.0
    filled_qty: float = 0.0
    avg_price: float = 0.0
    batch_id: int = None
    phase: str = None
    is_spot: bool = False
    update_time: datetime = field(default_factory=datetime.utcnow)


@dataclass
class WatcherConfig:
    """Configuration for order watchers."""
    # WebSocket settings
    spot_ws_url: str = "wss://stream.binance.com:9443/stream"
    futures_ws_url: str = "wss://fstream.binance.com/stream"
    ws_reconnect_interval: int = 5
    ws_ping_interval: int = 30
    
    # Polling settings
    polling_intervals: list = field(default_factory=lambda: [1, 1, 2, 2, 5, 5, 10, 30, 60])
    default_timeout: int = 300


class SpotWebSocketWatcher:
    """WebSocket for spot order updates via user data stream."""
    
    def __init__(self, config: WatcherConfig = None):
        self.config = config or WatcherConfig()
        self.on_order_update: Callable[[OrderUpdate], None] = None
        
        self._ws = None
        self._ws_task: Optional[asyncio.Task] = None
        self._running = False
        self._connected = False
        
        # Active orders: order_id -> {batch_id, phase}
        self._watching: Dict[str, Dict] = {}
        
        self._ws_messages = 0
    
    async def start(self):
        self._running = True
        self._ws_task = asyncio.create_task(self._ws_loop())
        logger.info("SpotWebSocketWatcher started")
    
    async def stop(self):
        self._running = False
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        if self._ws:
            await self._ws.close()
        logger.info(f"SpotWebSocketWatcher stopped. Messages: {self._ws_messages}")
    
    def watch(self, order_id: str, symbol: str, batch_id: int, phase: str):
        """Register an order to watch."""
        self._watching[order_id] = {
            'symbol': symbol,
            'batch_id': batch_id,
            'phase': phase,
            'is_spot': True
        }
    
    def unwatch(self, order_id: str):
        self._watching.pop(order_id, None)
    
    async def _ws_loop(self):
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
        """Connect to spot user data stream (needs listenKey)."""
        listen_key = await self._get_listen_key()
        if not listen_key:
            raise Exception("Failed to get spot listenKey")
        
        ws_url = f"{self.config.spot_ws_url}?streams={listen_key}"
        
        async with websockets.connect(ws_url, ping_interval=self.config.ws_ping_interval) as ws:
            self._ws = ws
            self._connected = True
            
            async for message in ws:
                if not self._running:
                    break
                await self._handle_message(message)
        
        self._connected = False
    
    async def _get_listen_key(self) -> Optional[str]:
        """Get listenKey for spot user data stream."""
        # TODO: Implement with trader's API credentials
        return None
    
    async def _handle_message(self, message: str):
        self._ws_messages += 1
        try:
            data = json.loads(message)
            if 'data' in data and data.get('data', {}).get('e') == 'executionReport':
                await self._process_report(data['data'])
        except Exception as e:
            logger.error(f"Spot WS handle error: {e}")
    
    async def _process_report(self, data: Dict):
        order_id = str(data.get('i'))
        if order_id not in self._watching:
            return
        
        status = self._parse_status(data.get('X'))
        if status == OrderStatus.UNKNOWN:
            return
        
        info = self._watching[order_id]
        
        update = OrderUpdate(
            order_id=order_id,
            symbol=data.get('s', ''),
            status=status,
            filled_price=float(data.get('L', 0)),
            filled_qty=float(data.get('z', 0)),
            avg_price=float(data.get('ap', 0)),
            batch_id=info['batch_id'],
            phase=info['phase'],
            is_spot=True
        )
        
        logger.info(f"Spot WS: {order_id} -> {status.value}")
        
        if self.on_order_update:
            await self.on_order_update(update)
        
        if status in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED):
            self.unwatch(order_id)
    
    def _parse_status(self, code: str) -> OrderStatus:
        mapping = {
            'NEW': OrderStatus.PENDING,
            'PARTIALLY_FILLED': OrderStatus.PARTIALLY_FILLED,
            'FILLED': OrderStatus.FILLED,
            'CANCELLED': OrderStatus.CANCELLED,
            'REJECTED': OrderStatus.REJECTED,
            'EXPIRED': OrderStatus.EXPIRED,
        }
        return mapping.get(code, OrderStatus.UNKNOWN)


class FuturesWebSocketWatcher:
    """WebSocket for futures order updates via executionReport stream."""
    
    def __init__(self, config: WatcherConfig = None):
        self.config = config or WatcherConfig()
        self.on_order_update: Callable[[OrderUpdate], None] = None
        
        self._ws = None
        self._ws_task: Optional[asyncio.Task] = None
        self._running = False
        self._connected = False
        
        self._watching: Dict[str, Dict] = {}
        self._ws_messages = 0
    
    async def start(self):
        self._running = True
        self._ws_task = asyncio.create_task(self._ws_loop())
        logger.info("FuturesWebSocketWatcher started")
    
    async def stop(self):
        self._running = False
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        if self._ws:
            await self._ws.close()
        logger.info(f"FuturesWebSocketWatcher stopped. Messages: {self._ws_messages}")
    
    def watch(self, order_id: str, symbol: str, batch_id: int, phase: str):
        self._watching[order_id] = {
            'symbol': symbol.lower(),
            'batch_id': batch_id,
            'phase': phase,
            'is_spot': False
        }
    
    def unwatch(self, order_id: str):
        self._watching.pop(order_id, None)
    
    async def _ws_loop(self):
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
        symbols = set()
        for info in self._watching.values():
            symbols.add(f"{info['symbol']}@executionReport")
        
        if symbols:
            await self._ws.send(json.dumps({
                "method": "SUBSCRIBE",
                "params": list(symbols),
                "id": int(time.time() * 1000)
            }))
    
    async def _handle_message(self, message: str):
        self._ws_messages += 1
        try:
            data = json.loads(message)
            if 'error' in data:
                return
            if 'data' in data:
                await self._process_report(data['data'])
            elif 'e' in data:
                await self._process_report(data)
        except Exception as e:
            logger.error(f"Futures WS handle error: {e}")
    
    async def _process_report(self, data: Dict):
        order_id = str(data.get('i'))
        if order_id not in self._watching:
            return
        
        status = self._parse_status(data.get('X'))
        if status == OrderStatus.UNKNOWN:
            return
        
        info = self._watching[order_id]
        
        update = OrderUpdate(
            order_id=order_id,
            symbol=data.get('s', ''),
            status=status,
            filled_price=float(data.get('L', 0)),
            filled_qty=float(data.get('z', 0)),
            avg_price=float(data.get('ap', 0)),
            batch_id=info['batch_id'],
            phase=info['phase'],
            is_spot=False
        )
        
        logger.info(f"Futures WS: {order_id} -> {status.value}")
        
        if self.on_order_update:
            await self.on_order_update(update)
        
        if status in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED):
            self.unwatch(order_id)
    
    def _parse_status(self, code: str) -> OrderStatus:
        mapping = {
            'NEW': OrderStatus.PENDING,
            'PARTIALLY_FILLED': OrderStatus.PARTIALLY_FILLED,
            'FILLED': OrderStatus.FILLED,
            'CANCELLED': OrderStatus.CANCELLED,
            'REJECTED': OrderStatus.REJECTED,
            'EXPIRED': OrderStatus.EXPIRED,
        }
        return mapping.get(code, OrderStatus.UNKNOWN)


class SpotPollingTask:
    """Polling fallback for spot orders."""
    
    def __init__(self, trader, config: WatcherConfig = None):
        self.trader = trader
        self.config = config or WatcherConfig()
        self.on_order_update: Callable[[OrderUpdate], None] = None
        
        self._running = False
        self._tasks: Dict[str, asyncio.Task] = {}
        self._polling_queries = 0
    
    async def start(self):
        self._running = True
        logger.info("SpotPollingTask started")
    
    async def stop(self):
        self._running = False
        for task in self._tasks.values():
            task.cancel()
        self._tasks.clear()
        logger.info(f"SpotPollingTask stopped. Queries: {self._polling_queries}")
    
    def watch(self, order_id: str, symbol: str, batch_id: int, phase: str):
        if order_id in self._tasks:
            return
        task = asyncio.create_task(self._poll_loop(order_id, symbol, batch_id, phase))
        self._tasks[order_id] = task
    
    def unwatch(self, order_id: str):
        if order_id in self._tasks:
            self._tasks[order_id].cancel()
            del self._tasks[order_id]
    
    async def _poll_loop(self, order_id: str, symbol: str, batch_id: int, phase: str):
        intervals = self.config.polling_intervals
        
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
                        batch_id=batch_id,
                        phase=phase,
                        is_spot=True
                    )
                    logger.info(f"Spot polling: {order_id} filled")
                    if self.on_order_update:
                        await self.on_order_update(update)
                    self.unwatch(order_id)
                    return
                elif status in ('CANCELLED', 'EXPIRED', 'REJECTED'):
                    update = OrderUpdate(
                        order_id=order_id,
                        symbol=symbol,
                        status=OrderStatus(status),
                        batch_id=batch_id,
                        phase=phase,
                        is_spot=True
                    )
                    if self.on_order_update:
                        await self.on_order_update(update)
                    self.unwatch(order_id)
                    return
            except Exception as e:
                logger.warning(f"Spot polling error {order_id}: {e}")
        
        # Timeout - long interval
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
                        batch_id=batch_id,
                        phase=phase,
                        is_spot=True
                    )
                    if self.on_order_update:
                        await self.on_order_update(update)
                    self.unwatch(order_id)
                    return
            except Exception:
                pass


class FuturesPollingTask:
    """Polling fallback for futures orders."""
    
    def __init__(self, trader, config: WatcherConfig = None):
        self.trader = trader
        self.config = config or WatcherConfig()
        self.on_order_update: Callable[[OrderUpdate], None] = None
        
        self._running = False
        self._tasks: Dict[str, asyncio.Task] = {}
        self._polling_queries = 0
    
    async def start(self):
        self._running = True
        logger.info("FuturesPollingTask started")
    
    async def stop(self):
        self._running = False
        for task in self._tasks.values():
            task.cancel()
        self._tasks.clear()
        logger.info(f"FuturesPollingTask stopped. Queries: {self._polling_queries}")
    
    def watch(self, order_id: str, symbol: str, batch_id: int, phase: str):
        if order_id in self._tasks:
            return
        task = asyncio.create_task(self._poll_loop(order_id, symbol, batch_id, phase))
        self._tasks[order_id] = task
    
    def unwatch(self, order_id: str):
        if order_id in self._tasks:
            self._tasks[order_id].cancel()
            del self._tasks[order_id]
    
    async def _poll_loop(self, order_id: str, symbol: str, batch_id: int, phase: str):
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
                        batch_id=batch_id,
                        phase=phase,
                        is_spot=False
                    )
                    logger.info(f"Futures polling: {order_id} filled")
                    if self.on_order_update:
                        await self.on_order_update(update)
                    self.unwatch(order_id)
                    return
                elif status in ('CANCELLED', 'EXPIRED', 'REJECTED'):
                    update = OrderUpdate(
                        order_id=order_id,
                        symbol=symbol,
                        status=OrderStatus(status),
                        batch_id=batch_id,
                        phase=phase,
                        is_spot=False
                    )
                    if self.on_order_update:
                        await self.on_order_update(update)
                    self.unwatch(order_id)
                    return
            except Exception as e:
                logger.warning(f"Futures polling error {order_id}: {e}")
        
        # Timeout - long interval
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
                        batch_id=batch_id,
                        phase=phase,
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
    Unified entry point that combines 4 watchers.
    All watchers call service.handle_order_update() when status changes.
    """
    
    def __init__(self, batch_service, config: WatcherConfig = None):
        self.batch_service = batch_service
        self.config = config or WatcherConfig()
        
        # Create 4 components
        self.spot_ws = SpotWebSocketWatcher(self.config)
        self.futures_ws = FuturesWebSocketWatcher(self.config)
        self.spot_polling = SpotPollingTask(batch_service.trader, self.config)
        self.futures_polling = FuturesPollingTask(batch_service.trader, self.config)
        
        # All watchers call the same handler
        self.spot_ws.on_order_update = self._on_order_update
        self.futures_ws.on_order_update = self._on_order_update
        self.spot_polling.on_order_update = self._on_order_update
        self.futures_polling.on_order_update = self._on_order_update
        
        self._running = False
    
    async def start(self):
        self._running = True
        await self.spot_ws.start()
        await self.futures_ws.start()
        await self.spot_polling.start()
        await self.futures_polling.start()
        logger.info("UnifiedOrderWatcher started (4 components)")
    
    async def stop(self):
        self._running = False
        await self.spot_ws.stop()
        await self.futures_ws.stop()
        await self.spot_polling.stop()
        await self.futures_polling.stop()
        logger.info("UnifiedOrderWatcher stopped")
    
    def watch_order(self, batch_id: int, order_id: str, symbol: str, phase: str, is_spot: bool):
        """Watch an order using appropriate watcher (WS + polling fallback)."""
        if is_spot:
            self.spot_ws.watch(order_id, symbol, batch_id, phase)
            if not self.spot_ws._connected:
                self.spot_polling.watch(order_id, symbol, batch_id, phase)
        else:
            self.futures_ws.watch(order_id, symbol, batch_id, phase)
            if not self.futures_ws._connected:
                self.futures_polling.watch(order_id, symbol, batch_id, phase)
    
    def unwatch_order(self, order_id: str):
        """Stop watching an order from all watchers."""
        self.spot_ws.unwatch(order_id)
        self.futures_ws.unwatch(order_id)
        self.spot_polling.unwatch(order_id)
        self.futures_polling.unwatch(order_id)
    
    async def _on_order_update(self, update: OrderUpdate):
        """Forward to batch_service for business logic handling."""
        await self.batch_service.handle_order_update(update)
