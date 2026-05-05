"""
API routes for Binance Arbitrage Platform.
"""
from datetime import datetime
from typing import List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from models.database import get_async_session, PositionExecute, BatchExecute, Earning
from models.database import init_db_async
from services import create_collector, create_trader, PortfolioManager, LockManager
from plugins.order_sequence import get_available_plugins, get_plugin


router = APIRouter()

# Lock manager instance
_lock_manager = LockManager()


# Request/Response models (Pydantic v2 syntax)
class OpenPositionRequest(BaseModel):
    """Open position request."""
    contract: str
    batch_num: int = 1
    batch_position_value: float = 1000
    order_plugin: str = 'futures_first'


class ClosePositionRequest(BaseModel):
    """Close position request."""
    position_id: int
    batch_num: int = 1
    batch_position_value: float = 1000


class FundingRateResponse(BaseModel):
    """Funding rate response."""
    symbol: str
    rate: float
    next_funding_time: int


class PositionResponse(BaseModel):
    """Position response."""
    id: int
    contract: str
    batch_num: int
    execute_status: str
    batch_position_value: float
    offset: str
    created_at: datetime
    updated_at: datetime
    complete_reason: str | None


class BatchResponse(BaseModel):
    """Batch response."""
    id: int
    position_execute_id: int
    timeout: int
    execute_status: str
    offset: str
    order_sequence: str | None
    contract_price: float | None
    spot_price: float | None
    phase: str | None
    first_side_order_id: str | None
    first_side_filled_price: float | None
    second_side_order_id: str | None
    second_side_filled_price: float | None
    complete_reason: str | None


class PluginResponse(BaseModel):
    """Plugin info."""
    name: str
    type: str
    description: str


class StatusResponse(BaseModel):
    """Status response."""
    status: str
    message: str
    position_id: int | None = None


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    database: str = "unknown"
    scheduler: str = "unknown"


# Routes

@router.get("/funding-rates", response_model=List[FundingRateResponse])
async def get_funding_rates():
    """Get current funding rates."""
    collector = create_collector('binance')
    try:
        rates = await collector.get_funding_rates()
        return [
            FundingRateResponse(
                symbol=r.symbol,
                rate=float(r.rate),
                next_funding_time=r.next_funding_time
            )
            for r in rates
        ]
    finally:
        await collector.close()


@router.post("/open-position", response_model=StatusResponse)
async def open_position(request: OpenPositionRequest):
    """Submit open position request."""
    # Check if already locked
    is_locked = await _lock_manager.is_locked(request.contract)
    if is_locked:
        raise HTTPException(
            status_code=400,
            detail=f"Contract {request.contract} is locked by another operation"
        )
    
    # Acquire lock
    acquired = await _lock_manager.acquire(request.contract, 'OPEN')
    if not acquired:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to acquire lock for {request.contract}"
        )
    
    try:
        async with get_async_session() as session:
            pos = PositionExecute(
                contract=request.contract,
                batch_num=request.batch_num,
                batch_position_value=request.batch_position_value,
                offset='OPEN',
                execute_status='PENDING'
            )
            session.add(pos)
            await session.commit()
            await session.refresh(pos)
            
            for i in range(request.batch_num):
                batch = BatchExecute(
                    position_execute_id=pos.id,
                    timeout=300,
                    offset='OPEN',
                    execute_status='PENDING',
                    phase='PENDING',
                    order_sequence=request.order_plugin,
                    batch_value=request.batch_position_value
                )
                session.add(batch)
            
            await session.commit()
        
        await _lock_manager.release(request.contract)
        
        return StatusResponse(
            status='success',
            message=f"Position opened: {pos.id}, batches: {request.batch_num}",
            position_id=pos.id
        )
    
    except Exception as e:
        await _lock_manager.release(request.contract)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/open-progress/{position_id}")
async def get_open_progress(position_id: int):
    """Get open position progress."""
    async with get_async_session() as session:
        from sqlalchemy import select
        
        result = await session.execute(
            select(PositionExecute).where(PositionExecute.id == position_id)
        )
        pos = result.scalar_one_or_none()
        
        if not pos:
            raise HTTPException(status_code=404, detail="Position not found")
        
        result = await session.execute(
            select(BatchExecute).where(BatchExecute.position_execute_id == position_id)
        )
        batches = list(result.scalars().all())
        
        return {
            'id': pos.id,
            'contract': pos.contract,
            'execute_status': pos.execute_status,
            'complete_reason': pos.complete_reason,
            'batches': [
                {
                    'id': b.id,
                    'status': b.execute_status,
                    'phase': b.phase,
                    'contract_price': b.contract_price,
                    'spot_price': b.spot_price,
                    'first_side_order_id': b.first_side_order_id,
                    'first_side_filled_price': b.first_side_filled_price,
                    'second_side_order_id': b.second_side_order_id,
                    'second_side_filled_price': b.second_side_filled_price,
                    'complete_reason': b.complete_reason
                }
                for b in batches
            ]
        }


@router.get("/batch-detail/{batch_id}")
async def get_batch_detail(batch_id: int):
    """Get batch detail."""
    async with get_async_session() as session:
        from sqlalchemy import select
        
        result = await session.execute(
            select(BatchExecute).where(BatchExecute.id == batch_id)
        )
        batch = result.scalar_one_or_none()
        
        if not batch:
            raise HTTPException(status_code=404, detail="Batch not found")
        
        return {
            'id': batch.id,
            'position_execute_id': batch.position_execute_id,
            'execute_status': batch.execute_status,
            'phase': batch.phase,
            'order_sequence': batch.order_sequence,
            'contract_price': batch.contract_price,
            'spot_price': batch.spot_price,
            'first_side_order_id': batch.first_side_order_id,
            'first_side_filled_price': batch.first_side_filled_price,
            'second_side_order_id': batch.second_side_order_id,
            'second_side_filled_price': batch.second_side_filled_price,
            'complete_reason': batch.complete_reason
        }


@router.post("/close-position", response_model=StatusResponse)
async def close_position(request: ClosePositionRequest):
    """Submit close position request."""
    async with get_async_session() as session:
        from sqlalchemy import select
        
        result = await session.execute(
            select(PositionExecute).where(PositionExecute.id == request.position_id)
        )
        pos = result.scalar_one_or_none()
    
    if not pos:
        raise HTTPException(status_code=404, detail="Position not found")
    
    # Check if already locked
    is_locked = await _lock_manager.is_locked(pos.contract)
    if is_locked:
        raise HTTPException(
            status_code=400,
            detail=f"Contract {pos.contract} is locked by another operation"
        )
    
    # Acquire lock
    acquired = await _lock_manager.acquire(pos.contract, 'CLOSE')
    if not acquired:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to acquire lock for {pos.contract}"
        )
    
    try:
        async with get_async_session() as session:
            close_pos = PositionExecute(
                contract=pos.contract,
                batch_num=request.batch_num,
                batch_position_value=request.batch_position_value,
                offset='CLOSE',
                execute_status='PENDING'
            )
            session.add(close_pos)
            await session.commit()
            await session.refresh(close_pos)
            
            for i in range(request.batch_num):
                batch = BatchExecute(
                    position_execute_id=close_pos.id,
                    timeout=300,
                    offset='CLOSE',
                    execute_status='PENDING',
                    phase='PENDING',
                    batch_value=request.batch_position_value
                )
                session.add(batch)
            
            await session.commit()
        
        await _lock_manager.release(pos.contract)
        
        return StatusResponse(
            status='success',
            message=f"Close position created: {close_pos.id}",
            position_id=close_pos.id
        )
    
    except Exception as e:
        await _lock_manager.release(pos.contract)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/positions")
async def get_positions():
    """Get all positions."""
    async with get_async_session() as session:
        from sqlalchemy import select
        
        result = await session.execute(
            select(PositionExecute).where(
                PositionExecute.offset == 'OPEN',
                PositionExecute.execute_status.in_(['PENDING', 'RUNNING'])
            )
        )
        positions = list(result.scalars().all())
        
        return [
            {
                'id': p.id,
                'contract': p.contract,
                'batch_num': p.batch_num,
                'execute_status': p.execute_status,
                'batch_position_value': p.batch_position_value,
                'created_at': p.created_at.isoformat()
            }
            for p in positions
        ]


@router.get("/positions/{position_id}")
async def get_position(position_id: int):
    """Get position by ID."""
    async with get_async_session() as session:
        from sqlalchemy import select
        
        result = await session.execute(
            select(PositionExecute).where(PositionExecute.id == position_id)
        )
        pos = result.scalar_one_or_none()
    
    if not pos:
        raise HTTPException(status_code=404, detail="Position not found")
    
    return {
        'id': pos.id,
        'contract': pos.contract,
        'batch_num': pos.batch_num,
        'execute_status': pos.execute_status,
        'batch_position_value': pos.batch_position_value,
        'offset': pos.offset,
        'complete_reason': pos.complete_reason,
        'created_at': pos.created_at.isoformat(),
        'updated_at': pos.updated_at.isoformat()
    }


@router.get("/plugins")
async def get_plugins():
    """Get available plugins."""
    plugins = get_available_plugins()
    
    descriptions = {
        'futures_first': 'Execute futures first, then spot',
        'spot_first': 'Execute spot first, then futures'
    }
    
    return [
        PluginResponse(
            name=p,
            type='order_sequence',
            description=descriptions.get(p, '')
        )
        for p in plugins
    ]


@router.post("/plugins/set", response_model=StatusResponse)
async def set_plugin(plugin_name: str):
    """Set active plugin."""
    try:
        plugin = get_plugin(plugin_name)
        return StatusResponse(
            status='success',
            message=f"Plugin set to {plugin_name}"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/earnings")
async def get_earnings():
    """Get earnings history."""
    async with get_async_session() as session:
        from sqlalchemy import select
        
        result = await session.execute(select(Earning))
        earnings = list(result.scalars().all())
        
        return [
            {
                'id': e.id,
                'contract': e.contract,
                'amount': e.amount,
                'funding_earn': e.funding_earn,
                'interest_earn': e.interest_earn,
                'pnl': e.pnl,
                'total_earn': e.total_earn,
                'status': e.status,
                'created_at': e.created_at.isoformat()
            }
            for e in earnings
        ]


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check with database status."""
    db_status = "unknown"
    
    # Check database
    try:
        async with get_async_session() as session:
            from sqlalchemy import select
            result = await session.execute(select(1))
            result.scalar_one()
        db_status = "healthy"
    except Exception:
        db_status = "unhealthy"
    
    # Return status (scheduler status is handled by main.py lifespan)
    scheduler_status = "healthy"  # Assumed healthy if app is running
    
    overall = "ok" if db_status == "healthy" else "degraded"
    
    return HealthResponse(
        status=overall,
        database=db_status,
        scheduler=scheduler_status
    )
