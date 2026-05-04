"""
Order polling scheduler: centralized task that queries DB for waiting orders
and polls Binance for status updates.
"""
import asyncio
import logging
from typing import Dict, List, Any
from datetime import datetime

from models.database import get_async_session, BatchExecute
from events.order_watcher import OrderUpdate, OrderStatus

logger = logging.getLogger(__name__)


class OrderPollingScheduler:
    """
    Centralized polling scheduler for order status.
    
    Runs periodically (e.g., every 5 seconds), queries DB for orders in
    FIRST_ORDER_WAIT or SECOND_ORDER_WAIT, then polls Binance API.
    """
    
    def __init__(self, batch_service, trader, config=None):
        self.batch_service = batch_service
        self.trader = trader
        self.config = config or {'interval': 5, 'max_retries': 3}
        
        self._running = False
        self._task: asyncio.Task = None
        self._polling_queries = 0
    
    async def start(self):
        """Start the polling scheduler."""
        self._running = True
        self._task = asyncio.create_task(self._poll_loop())
        logger.info(f"OrderPollingScheduler started (interval={self.config['interval']}s)")
    
    async def stop(self):
        """Stop the polling scheduler."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info(f"OrderPollingScheduler stopped. Queries: {self._polling_queries}")
    
    async def _poll_loop(self):
        """Main polling loop."""
        while self._running:
            try:
                await self._poll_once()
            except Exception as e:
                logger.error(f"Polling error: {e}", exc_info=True)
            
            await asyncio.sleep(self.config['interval'])
    
    async def _poll_once(self):
        """Query DB and poll Binance for all waiting orders."""
        # 1. Query DB for orders in waiting phases
        async with get_async_session() as session:
            from sqlalchemy import select, or_
            
            result = await session.execute(
                select(BatchExecute).where(
                    BatchExecute.execute_status == 'RUNNING',
                    or_(
                        BatchExecute.phase == 'FIRST_ORDER_WAIT',
                        BatchExecute.phase == 'SECOND_ORDER_WAIT'
                    )
                )
            )
            waiting_batches = result.scalars().all()
        
        if not waiting_batches:
            return
        
        # 2. Poll each order
        for batch in waiting_batches:
            if not self._running:
                return
            
            # Determine if spot order
            is_spot = self._is_spot_order(batch)
            order_id = self._get_order_id(batch)
            
            if not order_id:
                continue
            
            try:
                self._polling_queries += 1
                
                # Poll Binance API
                status_data = await self.trader.get_order_status(
                    batch.position.contract, 
                    int(order_id), 
                    is_spot=is_spot
                )
                
                status = status_data.get('status', 'UNKNOWN')
                avg_price = float(status_data.get('avgPrice', 0))
                
                # Check if terminal status
                if status == 'FILLED':
                    update = OrderUpdate(
                        order_id=order_id,
                        symbol=batch.position.contract,
                        status=OrderStatus.FILLED,
                        avg_price=avg_price,
                        batch_id=batch.id,
                        phase=batch.phase,
                        is_spot=is_spot
                    )
                    logger.info(f"Polling: Batch {batch.id} order {order_id} FILLED")
                    
                    await self.batch_service.handle_order_update(update)
                    
                elif status in ('CANCELLED', 'EXPIRED', 'REJECTED'):
                    update = OrderUpdate(
                        order_id=order_id,
                        symbol=batch.position.contract,
                        status=OrderStatus(status),
                        batch_id=batch.id,
                        phase=batch.phase,
                        is_spot=is_spot
                    )
                    logger.info(f"Polling: Batch {batch.id} order {order_id} {status}")
                    
                    await self.batch_service.handle_order_update(update)
                    
            except Exception as e:
                logger.warning(f"Polling error for batch {batch.id} order {order_id}: {e}")
    
    def _is_spot_order(self, batch: BatchExecute) -> bool:
        """Determine if the current waiting order is a spot order."""
        if batch.phase == 'FIRST_ORDER_WAIT':
            # First order is spot if order_sequence is spot_first
            return batch.order_sequence != 'futures_first'
        elif batch.phase == 'SECOND_ORDER_WAIT':
            # Second order is spot if order_sequence is futures_first
            return batch.order_sequence == 'futures_first'
        return False
    
    def _get_order_id(self, batch: BatchExecute) -> str:
        """Get the order ID for the current waiting phase."""
        if batch.phase == 'FIRST_ORDER_WAIT':
            return batch.first_side_order_id
        elif batch.phase == 'SECOND_ORDER_WAIT':
            return batch.second_side_order_id
        return None
