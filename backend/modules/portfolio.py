"""
Portfolio management module.
Implements IPortfolio interface.
"""
from typing import List, Optional
from datetime import datetime
from ..database import (
    get_session, PositionExecute, BatchExecute, PositionOrder, 
    PositionStep, TradingHistory, FundingRateHistory, Earning
)
from ..interfaces import IPortfolio, Position as PositionModel, Earning as EarningModel


class PortfolioManager(IPortfolio):
    """Portfolio management."""
    
    def __init__(self):
        self.session = get_session()
    
    async def get_positions(self) -> List[PositionModel]:
        """Get all positions."""
        positions = self.session.query(PositionExecute).filter(
            PositionExecute.offset == 'OPEN',
            PositionExecute.execute_status.in_(['PENDING', 'RUNNING'])
        ).all()
        
        result = []
        for pos in positions:
            result.append(PositionModel(
                id=pos.id,
                contract=pos.contract,
                amount=pos.batch_num * pos.batch_position_value,
                entry_price=0,  # Would need to calculate from orders
                current_price=0,
                pnl=0,
                status=pos.execute_status
            ))
        return result
    
    async def get_earnings(self) -> List[EarningModel]:
        """Get all earnings."""
        earnings = self.session.query(Earning).all()
        
        result = []
        for e in earnings:
            result.append(EarningModel(
                id=e.id,
                contract=e.contract,
                amount=e.amount,
                funding_earn=e.funding_earn,
                interest_earn=e.interest_earn,
                created_at=e.created_at.isoformat()
            ))
        return result
    
    async def create_position_execute(self, contract: str, batch_num: int,
                                  batch_position_value: float,
                                  offset: str) -> int:
        """Create position execute record."""
        pos = PositionExecute(
            contract=contract,
            batch_num=batch_num,
            batch_position_value=batch_position_value,
            offset=offset,
            execute_status='PENDING'
        )
        self.session.add(pos)
        self.session.commit()
        self.session.refresh(pos)
        return pos.id
    
    async def create_batch_execute(self, position_execute_id: int,
                                    timeout: int = 300) -> int:
        """Create batch execute record."""
        batch = BatchExecute(
            position_execute_id=position_execute_id,
            timeout=timeout,
            execute_status='PENDING',
            phase='PENDING'
        )
        self.session.add(batch)
        self.session.commit()
        self.session.refresh(batch)
        return batch.id
    
    async def update_batch_status(self, batch_id: int, status: str,
                                    phase: str = None) -> None:
        """Update batch status."""
        batch = self.session.query(BatchExecute).filter(
            BatchExecute.id == batch_id
        ).first()
        if batch:
            batch.execute_status = status
            if phase:
                batch.phase = phase
            self.session.commit()
    
    async def update_position_status(self, position_id: int, status: str,
                                    complete_reason: str = None) -> None:
        """Update position status."""
        pos = self.session.query(PositionExecute).filter(
            PositionExecute.id == position_id
        ).first()
        if pos:
            pos.execute_status = status
            if complete_reason:
                pos.complete_reason = complete_reason
            self.session.commit()
    
    async def get_batch(self, batch_id: int) -> Optional[BatchExecute]:
        """Get batch by ID."""
        return self.session.query(BatchExecute).filter(
            BatchExecute.id == batch_id
        ).first()
    
    async def get_batch_by_position(self, position_id: int) -> List[BatchExecute]:
        """Get all batches for a position."""
        return self.session.query(BatchExecute).filter(
            BatchExecute.position_execute_id == position_id
        ).all()
    
    async def add_order(self, batch_execute_id: int, order_id: str,
                       side: str, order_type: str, price: float,
                       amount: float) -> int:
        """Add order record."""
        order = PositionOrder(
            batch_execute_id=batch_execute_id,
            order_id=order_id,
            side=side,
            order_type=order_type,
            price=price,
            amount=amount,
            status='PENDING'
        )
        self.session.add(order)
        self.session.commit()
        self.session.refresh(order)
        return order.id
    
    async def update_order_status(self, order_id: int, status: str,
                                  filled_amount: float = 0) -> None:
        """Update order status."""
        order = self.session.query(PositionOrder).filter(
            PositionOrder.id == order_id
        ).first()
        if order:
            order.status = status
            order.filled_amount = filled_amount
            self.session.commit()
    
    async def add_step(self, batch_execute_id: int, step_name: str) -> int:
        """Add step record."""
        step = PositionStep(
            batch_execute_id=batch_execute_id,
            step_name=step_name,
            status='RUNNING'
        )
        self.session.add(step)
        self.session.commit()
        self.session.refresh(step)
        return step.id
    
    async def update_step_status(self, step_id: int, status: str,
                                error_message: str = None) -> None:
        """Update step status."""
        step = self.session.query(PositionStep).filter(
            PositionStep.id == step_id
        ).first()
        if step:
            step.status = status
            if error_message:
                step.error_message = error_message
            self.session.commit()
    
    async def add_trading_history(self, contract: str, side: str,
                               order_id: str, price: float,
                               amount: float, value: float,
                               fee: float = 0, status: str = 'FILLED') -> int:
        """Add trading history."""
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
        self.session.add(history)
        self.session.commit()
        self.session.refresh(history)
        return history.id
    
    async def add_funding_rate(self, symbol: str, rate: float,
                            estimated_rate: float,
                            next_funding_time: int) -> int:
        """Add funding rate history."""
        fr = FundingRateHistory(
            symbol=symbol,
            rate=rate,
            estimated_rate=estimated_rate,
            next_funding_time=next_funding_time
        )
        self.session.add(fr)
        self.session.commit()
        self.session.refresh(fr)
        return fr.id
    
    async def add_earning(self, contract: str, amount: float,
                         funding_rate: float, funding_earn: float = 0,
                         interest_earn: float = 0, pnl: float = 0) -> int:
        """Add earning record."""
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
        self.session.add(earn)
        self.session.commit()
        self.session.refresh(earn)
        return earn.id
    
    async def close_earning(self, earn_id: int, pnl: float) -> None:
        """Close earning record."""
        earn = self.session.query(Earning).filter(
            Earning.id == earn_id
        ).first()
        if earn:
            earn.status = 'CLOSED'
            earn.pnl = pnl
            earn.total_earn = earn.funding_earn + earn.interest_earn + pnl
            earn.closed_at = datetime.utcnow()
            self.session.commit()


# Lock manager implementation
class LockManager:
    """Concurrency control using database."""
    
    def __init__(self):
        self.session = get_session()
    
    async def acquire(self, symbol: str, operation: str) -> bool:
        """Try to acquire lock for symbol."""
        from ..database import LockInfo
        from sqlalchemy import and_
        
        # Check if already locked
        existing = self.session.query(LockInfo).filter(
            and_(
                LockInfo.symbol == symbol,
                LockInfo.locked == True
            )
        ).first()
        
        if existing:
            return False
        
        # Create new lock
        lock = LockInfo(
            symbol=symbol,
            operation=operation,
            locked=True,
            locked_at=datetime.utcnow()
        )
        self.session.add(lock)
        self.session.commit()
        return True
    
    async def release(self, symbol: str) -> None:
        """Release lock for symbol."""
        from ..database import LockInfo
        
        lock = self.session.query(LockInfo).filter(
            LockInfo.symbol == symbol
        ).first()
        
        if lock:
            lock.locked = False
            lock.released_at = datetime.utcnow()
            self.session.commit()
    
    def is_locked(self, symbol: str) -> bool:
        """Check if symbol is locked."""
        from ..database import LockInfo
        
        lock = self.session.query(LockInfo).filter(
            LockInfo.symbol == symbol,
            LockInfo.locked == True
        ).first()
        
        return lock is not None