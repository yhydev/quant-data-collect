"""
Order watchers: 2 WebSockets (spot/futures) + 1 polling scheduler task.
WebSockets: real-time order updates via streams.
Polling: centralized scheduler task that queries DB for waiting orders.
"""
import asyncio
import logging
import time
import json
from typing import Callable, Optional, Dict, Any, List
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime

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
    """Order update event passed to service."""
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
    
    def watch(self, order_id: str, symbol: str, batch_id: int, phase: str, is_spot: bool = True):
        """Register an order to watch via WS."""
        self._watching[order_id] = {
            'symbol': symbol,
            'batch_id': batch_id,
            'phase': phase,
            'is_spot': is_spot
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
    
    def watch(self, order_id: str, symbol: str, batch_id: int, phase: str, is_spot: bool = False):
        """Register an order to watch via WS."""
        self._watching[order_id] = {
            'symbol': symbol.lower(),
            'batch_id': batch_id,
            'phase': phase,
            'is_spot': is_spot
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


class UnifiedOrderWatcher:
    """
    Unified entry point for 2 WebSockets.
    Polling is now a separate scheduler task (not included here).
    """
    
    def __init__(self, batch_service, config: WatcherConfig = None):
        self.batch_service = batch_service
        self.config = config or WatcherConfig()
        
        # Create 2 WebSocket watchers
        self.spot_ws = SpotWebSocketWatcher(self.config)
        self.futures_ws = FuturesWebSocketWatcher(self.config)
        
        # Set callbacks to forward to batch_service
        self.spot_ws.on_order_update = self._on_order_update
        self.futures_ws.on_order_update = self._on_order_update
        
        self._running = False
    
    async def start(self):
        self._running = True
        await self.spot_ws.start()
        await self.futures_ws.start()
        logger.info("UnifiedOrderWatcher started (2 WebSockets)")
    
    async def stop(self):
        self._running = False
        await self.spot_ws.stop()
        await self.futures_ws.stop()
        logger.info("UnifiedOrderWatcher stopped")
    
    def watch_order(self, batch_id: int, order_id: str, symbol: str, phase: str, is_spot: bool):
        """Register an order to WebSocket watchers."""
        if is_spot:
            self.spot_ws.watch(order_id, symbol, batch_id, phase, is_spot)
        else:
            self.futures_ws.watch(order_id, symbol, batch_id, phase, is_spot)
    
    def unwatch_order(self, order_id: str):
        """Remove from all watchers."""
        self.spot_ws.unwatch(order_id)
        self.futures_ws.unwatch(order_id)
    
    async def _on_order_update(self, update: OrderUpdate):
        """Forward to batch_service."""
        await self.batch_service.handle_order_update(update)
