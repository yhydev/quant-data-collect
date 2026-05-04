"""
Batch polling methods for spot and futures orders.
Called by scheduler jobs, query DB for WAIT orders and poll Binance.
"""
import logging
from typing import List

from sqlalchemy import select, or_

from models.database import get_async_session, BatchExecute
from events.order_watcher import OrderUpdate, OrderStatus

logger = logging.getLogger(__name__)


async def poll_spot_orders(self):
    """
    Poll spot orders in FIRST_ORDER_WAIT or SECOND_ORDER_WAIT.
    Called by scheduler job (every N seconds).
    """
    # 1. Query DB for spot orders in WAIT phases
    async with get_async_session() as session:
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
    
    # 2. Filter spot orders and poll
    for batch in waiting_batches:
        is_spot = self._is_spot_order(batch)
        
        if not is_spot:
            continue
        
        await self._poll_single_order(batch, is_spot=True)


async def poll_futures_orders(self):
    """
    Poll futures orders in FIRST_ORDER_WAIT or SECOND_ORDER_WAIT.
    Called by scheduler job (every N seconds).
    """
    # 1. Query DB for futures orders in WAIT phases
    async with get_async_session() as session:
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
    
    # 2. Filter futures orders and poll
    for batch in waiting_batches:
        is_spot = self._is_spot_order(batch)
        
        if is_spot:
            continue
        
        await self._poll_single_order(batch, is_spot=False)


def _is_spot_order(self, batch: BatchExecute) -> bool:
    """Determine if current waiting order is a spot order."""
    if batch.phase == 'FIRST_ORDER_WAIT':
        # First order is spot if order_sequence is spot_first
        return batch.order_sequence != 'futures_first'
    elif batch.phase == 'SECOND_ORDER_WAIT':
        # Second order is spot if order_sequence is futures_first
        return batch.order_sequence == 'futures_first'
    return False


async def _poll_single_order(self, batch: BatchExecute, is_spot: bool):
    """Poll a single order from Binance."""
    order_id = self._get_order_id(batch)
    
    if not order_id:
        return
    
    try:
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
            logger.info(f"Poll: Batch {batch.id} order {order_id} FILLED")
            await self.handle_order_update(update)
            
        elif status in ('CANCELLED', 'EXPIRED', 'REJECTED'):
            update = OrderUpdate(
                order_id=order_id,
                symbol=batch.position.contract,
                status=OrderStatus(status),
                batch_id=batch.id,
                phase=batch.phase,
                is_spot=is_spot
            )
            logger.info(f"Poll: Batch {batch.id} order {order_id} {status}")
            await self.handle_order_update(update)
            
    except Exception as e:
        logger.warning(f"Poll error for batch {batch.id} order {order_id}: {e}")


def _get_order_id(self, batch: BatchExecute) -> str:
    """Get the order ID for the current waiting phase."""
    if batch.phase == 'FIRST_ORDER_WAIT':
        return batch.first_side_order_id
    elif batch.phase == 'SECOND_ORDER_WAIT':
        return batch.second_side_order_id
    return None
