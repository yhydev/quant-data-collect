"""
Portfolio management module.
Implements IPortfolio interface.
"""
from typing import List, Optional
from datetime import datetime
from sqlalchemy import select

from ..database import get_async_session, PositionExecute, BatchExecute, PositionOrder
from ..database import PositionStep, TradingHistory, FundingRateHistory, Earning
from ..interfaces import IPortfolio, Position as PositionModel, Earning as EarningModel


class PortfolioManager(IPortfolio):
    """Portfolio management."""
    
    def __init__(self):
        pass  # No session in __init__, use context managers
    
    async def get_positions(self) -> List[PositionModel]:
        """Get all positions."""
        async with get_async_session() as session:
            result = await session.execute(
                select(PositionExecute).where(
                    PositionExecute.offset == 'OPEN',
                    PositionExecute.execute_status.in_(['PENDING', 'RUNNING'])
                )
            )
            positions = list(result.scalars().all())
            
            result_list = []
            for pos in positions:
                result_list.append(PositionModel(
                    id=pos.id,
                    contract=pos.contract,
                    amount=pos.batch_num * pos.batch_position_value,
                    entry_price=0,  # Would need to calculate from orders
                    current_price=0,
                    pnl=0,
                    status=pos.execute_status
                ))
            return result_list
    
    async def get_earnings(self) -> List[EarningModel]:
        """Get all earnings."""
        async with get_async_session() as session:
            result = await session.execute(select(Earning))
            earnings = list(result.scalars().all())
            
            result_list = []
            for e in earnings:
                result_list.append(EarningModel(
                    id=e.id,
                    contract=e.contract,
                    amount=e.amount,
                    funding_earn=e.funding_earn,
                    interest_earn=e.interest_earn,
                    created_at=e.created_at.isoformat()
                ))
            return result_list
    
    async def create_position_execute(self, contract: str, batch_num: int,
                                  batch_position_value: float,
                                  offset: str) -> int:
        """Create position execute record."""
        async with get_async_session() as session:
            pos = PositionExecute(
                contract=contract,
                batch_num=batch_num,
                batch_position_value=batch_position_value,
                offset=offset,
                execute_status='PENDING'
            )
            session.add(pos)
            await session.commit()
            await session.refresh(pos)
            return pos.id
    
    async def create_batch_execute(self, position_execute_id: int,
                                    timeout: int = 300) -> int:
        """Create batch execute record."""
        async with get_async_session() as session:
            batch = BatchExecute(
                position_execute_id=position_execute_id,
                timeout=timeout,
                execute_status='PENDING',
                phase='PENDING'
            )
            session.add(batch)
            await session.commit()
            await session.refresh(batch)
            return batch.id
    
    async def update_batch_status(self, batch_id: int, status: str,
                                    phase: str = None) -> None:
        """Update batch status."""
        async with get_async_session() as session:
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.id == batch_id)
            )
            batch = result.scalar_one_or_none()
            if batch:
                batch.execute_status = status
                if phase:
                    batch.phase = phase
                await session.commit()
    
    async def update_position_status(self, position_id: int, status: str,
                                    complete_reason: str = None) -> None:
        """Update position status."""
        async with get_async_session() as session:
            result = await session.execute(
                select(PositionExecute).where(PositionExecute.id == position_id)
            )
            pos = result.scalar_one_or_none()
            if pos:
                pos.execute_status = status
                if complete_reason:
                    pos.complete_reason = complete_reason
                await session.commit()
    
    async def get_batch(self, batch_id: int) -> Optional[BatchExecute]:
        """Get batch by ID."""
        async with get_async_session() as session:
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.id == batch_id)
            )
            return result.scalar_one_or_none()
    
    async def get_batch_by_position(self, position_id: int) -> List[BatchExecute]:
        """Get all batches for a position."""
        async with get_async_session() as session:
            result = await session.execute(
                select(BatchExecute).where(BatchExecute.position_execute_id == position_id)
            )
            return list(result.scalars().all())
    
    async def add_order(self, batch_execute_id: int, order_id: str,
                       side: str, order_type: str, price: float,
                       amount: float) -> int:
        """Add order record."""
        async with get_async_session() as session:
            order = PositionOrder(
                batch_execute_id=batch_execute_id,
                order_id=order_id,
                side=side,
                order_type=order_type,
                price=price,
                amount=amount,
                status='PENDING'
            )
            session.add(order)
            await session.commit()
            await session.refresh(order)
            return order.id
    
    async def update_order_status(self, order_id: int, status: str,
                                  filled_amount: float = 0) -> None:
        """Update order status."""
        async with get_async_session() as session:
            result = await session.execute(
                select(PositionOrder).where(PositionOrder.id == order_id)
            )
            order = result.scalar_one_or_none()
            if order:
                order.status = status
                order.filled_amount = filled_amount
                await session.commit()
    
    async def add_step(self, batch_execute_id: int, step_name: str) -> int:
        """Add step record."""
        async with get_async_session() as session:
            step = PositionStep(
                batch_execute_id=batch_execute_id,
                step_name=step_name,
                status='RUNNING'
            )
            session.add(step)
            await session.commit()
            await session.refresh(step)
            return step.id
    
    async def update_step_status(self, step_id: int, status: str,
                                error_message: str = None) -> None:
        """Update step status."""
        async with get_async_session() as session:
            result = await session.execute(
                select(PositionStep).where(PositionStep.id == step_id)
            )
            step = result.scalar_one_or_none()
            if step:
                step.status = status
                if error_message:
                    step.error_message = error_message
                await session.commit()
    
    async def add_trading_history(self, contract: str, side: str,
                               order_id: str, price: float,
                               amount: float, value: float,
                               fee: float = 0, status: str = 'FILLED') -> int:
        """Add trading history."""
        async with get_async_session() as session:
            history = TradingHistory(
                contract=contract,
                side=side,
                order_id=order_id,
                price=price,
                amount=amount,
                value=value,
                fee=fee,
                status=status
            )
            session.add(history)
            await session.commit()
            await session.refresh(history)
            return history.id
    
    async def add_funding_rate(self, symbol: str, rate: float,
                            estimated_rate: float,
                            next_funding_time: int) -> int:
        """Add funding rate history."""
        async with get_async_session() as session:
            fr = FundingRateHistory(
                symbol=symbol,
                rate=rate,
                estimated_rate=estimated_rate,
                next_funding_time=next_funding_time
            )
            session.add(fr)
            await session.commit()
            await session.refresh(fr)
            return fr.id
    
    async def add_earning(self, contract: str, amount: float,
                         funding_rate: float, funding_earn: float = 0,
                         interest_earn: float = 0, pnl: float = 0) -> int:
        """Add earning record."""
        async with get_async_session() as session:
            earn = Earning(
                contract=contract,
                amount=amount,
                funding_rate=funding_rate,
                funding_earn=funding_earn,
                interest_earn=interest_earn,
                pnl=pnl,
                total_earn=funding_earn + interest_earn + pnl,
                status='OPEN'
            )
            session.add(earn)
            await session.commit()
            await session.refresh(earn)
            return earn.id
    
    async def close_earning(self, earn_id: int, pnl: float) -> None:
        """Close earning record."""
        async with get_async_session() as session:
            result = await session.execute(
                select(Earning).where(Earning.id == earn_id)
            )
            earn = result.scalar_one_or_none()
            if earn:
                earn.status = 'CLOSED'
                earn.pnl = pnl
                earn.total_earn = earn.funding_earn + earn.interest_earn + pnl
                earn.closed_at = datetime.utcnow()
                await session.commit()


# Lock manager implementation
class LockManager:
    """Concurrency control using database."""
    
    def __init__(self):
        pass  # No session in __init__, use context managers
    
    async def acquire(self, symbol: str, operation: str) -> bool:
        """Try to acquire lock for symbol."""
        from ..database import LockInfo
        from sqlalchemy import and_, select
        
        async with get_async_session() as session:
            # Check if already locked
            result = await session.execute(
                select(LockInfo).where(
                    and_(
                        LockInfo.symbol == symbol,
                        LockInfo.locked == True
                    )
                )
            )
            existing = result.scalar_one_or_none()
            
            if existing:
                return False
            
            # Create new lock
            lock = LockInfo(
                symbol=symbol,
                operation=operation,
                locked=True,
                locked_at=datetime.utcnow()
            )
            session.add(lock)
            await session.commit()
            return True
    
    async def release(self, symbol: str) -> None:
        """Release lock for symbol."""
        from ..database import LockInfo
        from sqlalchemy import select
        
        async with get_async_session() as session:
            result = await session.execute(
                select(LockInfo).where(LockInfo.symbol == symbol)
            )
            lock = result.scalar_one_or_none()
            
            if lock:
                lock.locked = False
                lock.released_at = datetime.utcnow()
                await session.commit()
    
    async def is_locked(self, symbol: str) -> bool:
        """Check if symbol is locked (async version)."""
        from ..database import LockInfo
        from sqlalchemy import select
        
        async with get_async_session() as session:
            result = await session.execute(
                select(LockInfo).where(
                    LockInfo.symbol == symbol,
                    LockInfo.locked == True
                )
            )
            lock = result.scalar_one_or_none()
            return lock is not None