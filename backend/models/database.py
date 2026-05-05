"""
Database layer for Binance Arbitrage Platform.
Uses SQLAlchemy 2.0 AsyncORM for PostgreSQL.
"""
from datetime import datetime
from typing import Optional, List, AsyncGenerator
from contextlib import asynccontextmanager
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, ForeignKey, Text, select, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from settings import settings

Base = declarative_base()


class PositionExecute(Base):
    """仓位执行表 - 主记录"""
    __tablename__ = 'position_execute'
    
    id = Column(Integer, primary_key=True)
    contract = Column(String(20), nullable=False)  # 合约 symbol，如 BTCUSDT
    batch_num = Column(Integer, nullable=False)       # 批次数
    execute_status = Column(String(20), default='PENDING')  # PENDING | RUNNING | COMPLETED
    batch_position_value = Column(Float, nullable=False)  # 批次开仓价值 (USDT)
    offset = Column(String(10), nullable=False)       # OPEN | CLOSE
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    complete_reason = Column(String(50))           # TIMEOUT | SUCCESS | CANCELLED | ERROR | None
    
    batches = relationship("BatchExecute", back_populates="position")


class BatchExecute(Base):
    """批次仓位执行表 - 分批记录"""
    __tablename__ = 'batch_execute'
    
    id = Column(Integer, primary_key=True)
    position_execute_id = Column(Integer, ForeignKey('position_execute.id'), nullable=False)
    timeout = Column(Integer, default=3600)        # 成交超时时间(秒)
    first_order_wait_timeout = Column(Integer, default=300)  # FIRST_ORDER_WAIT 超时时间(秒)
    execute_status = Column(String(20), default='PENDING')  # PENDING | RUNNING | COMPLETED
    offset = Column(String(10), nullable=False)    # OPEN | CLOSE
    order_sequence = Column(String(20))            # 先做哪边: FUTURES_FIRST | SPOT_FIRST
    contract_price = Column(Float)                # 合约挂单价
    spot_price = Column(Float)                    # 现货挂单价
    batch_value = Column(Float)                 # 本批次开仓价值
    phase = Column(String(50))                   # 当前阶段
    first_side_order_id = Column(String(50))       # 第一边订单ID
    first_side_filled_price = Column(Float)         # 第一边成交价
    second_side_order_id = Column(String(50))      # 第二边订单ID
    second_side_filled_price = Column(Float)       # 第二边成交价
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    complete_reason = Column(String(50))         # TIMEOUT | SUCCESS | CANCELLED | ERROR
    
    position = relationship("PositionExecute", back_populates="batches", lazy="joined")
    orders = relationship("PositionOrder", back_populates="batch")
    phase_history = relationship(
        "BatchPhaseHistory",
        back_populates="batch",
        order_by="BatchPhaseHistory.id",
        cascade="all, delete-orphan",
    )


class BatchPhaseHistory(Base):
    """批次阶段变更历史"""
    __tablename__ = 'batch_phase_history'

    id = Column(Integer, primary_key=True)
    batch_execute_id = Column(Integer, ForeignKey('batch_execute.id'), nullable=False)
    from_phase = Column(String(50))
    to_phase = Column(String(50), nullable=False)
    trigger = Column(String(50), default='SYSTEM')
    note = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)

    batch = relationship("BatchExecute", back_populates="phase_history")


class PositionOrder(Base):
    """订单记录表"""
    __tablename__ = 'position_orders'
    
    id = Column(Integer, primary_key=True)
    batch_execute_id = Column(Integer, ForeignKey('batch_execute.id'), nullable=False)
    order_id = Column(String(50), nullable=False)  # 交易所订单ID
    side = Column(String(20), nullable=False)         # SPOT_BUY | SPOT_SELL | FUTURES_SHORT | FUTURES_COVER
    order_type = Column(String(20), nullable=False)    # LIMIT | MARKET
    price = Column(Float)
    amount = Column(Float)
    status = Column(String(20), default='PENDING')   # PENDING | FILLED | PARTIAL | CANCELLED
    filled_amount = Column(Float, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    batch = relationship("BatchExecute", back_populates="orders")


class PositionStep(Base):
    """步骤执行记录表"""
    __tablename__ = 'position_steps'
    
    id = Column(Integer, primary_key=True)
    batch_execute_id = Column(Integer, ForeignKey('batch_execute.id'), nullable=False)
    step_name = Column(String(50), nullable=False)   # 步骤名称
    status = Column(String(20), default='PENDING')  # PENDING | RUNNING | COMPLETED
    error_message = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class TradingHistory(Base):
    """交易历史表"""
    __tablename__ = 'trading_history'
    
    id = Column(Integer, primary_key=True)
    contract = Column(String(20), nullable=False)
    side = Column(String(20), nullable=False)          # OPEN_SHORT | CLOSE_SHORT | BUY_SPOT | SELL_SPOT
    order_id = Column(String(50))
    price = Column(Float)
    amount = Column(Float)
    value = Column(Float)
    fee = Column(Float, default=0)
    status = Column(String(20))
    executed_at = Column(DateTime, default=datetime.utcnow)


class FundingRateHistory(Base):
    """资金费率历史表"""
    __tablename__ = 'funding_rates_history'
    __table_args__ = (
        UniqueConstraint('symbol', 'next_funding_time', name='uq_funding_symbol_time'),
    )
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(20), nullable=False)
    rate = Column(Float)
    estimated_rate = Column(Float)
    next_funding_time = Column(Integer)
    recorded_at = Column(DateTime, default=datetime.utcnow)


class Earning(Base):
    """收益记录表"""
    __tablename__ = 'earnings'
    
    id = Column(Integer, primary_key=True)
    contract = Column(String(20), nullable=False)
    amount = Column(Float)                        # 仓位价值
    funding_earn = Column(Float, default=0)      # 资金费率收益
    funding_rate = Column(Float)                 # 当时的资金费率
    interest_earn = Column(Float, default=0)      # 理财利息收益
    pnl = Column(Float, default=0)               # 价差收益
    total_earn = Column(Float, default=0)        # 总收益
    status = Column(String(20), default='OPEN')   # OPEN | CLOSED
    created_at = Column(DateTime, default=datetime.utcnow)
    closed_at = Column(DateTime)


class PluginConfig(Base):
    """插件配置表"""
    __tablename__ = 'plugin_config'
    
    id = Column(Integer, primary_key=True)
    plugin_name = Column(String(50), nullable=False, unique=True)
    plugin_type = Column(String(50), nullable=False)
    is_active = Column(Boolean, default=True)
    config_json = Column(Text)                      # JSON配置
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class LockInfo(Base):
    """并发锁信息表"""
    __tablename__ = 'lock_info'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(20), nullable=False, unique=True)
    operation = Column(String(20), nullable=False)  # OPEN | CLOSE
    locked = Column(Boolean, default=False)
    locked_at = Column(DateTime)
    released_at = Column(DateTime)


# Database configuration
def get_database_url() -> str:
    """Get database URL from environment (sync version for compatibility)."""
    host = settings.get('database.host', 'localhost')
    port = settings.get('database.port', 5432)
    user = settings.get('database.user', 'postgres')
    password = settings.get('database.password', 'postgres')
    db = settings.get('database.name', 'arbitrage')
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def get_async_database_url() -> str:
    """Get async database URL from environment."""
    host = settings.get('database.host', 'localhost')
    port = settings.get('database.port', 5432)
    user = settings.get('database.user', 'postgres')
    password = settings.get('database.password', 'postgres')
    db = settings.get('database.name', 'arbitrage')
    # asyncpg requires postgresql+asyncpg://
    return f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}"


# Async engine and session (main)
_async_engine = None
_async_session_factory = None


async def init_db_async():
    """Initialize async database connection."""
    global _async_engine, _async_session_factory
    
    db_url = get_async_database_url()
    
    # Configure connection pool for better performance
    pool_size = int(settings.get('database.pool_size', 5))
    max_overflow = int(settings.get('database.max_overflow', 10))
    
    _async_engine = create_async_engine(
        db_url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,  # Verify connections before use
        echo=False
    )
    _async_session_factory = async_sessionmaker(
        _async_engine,
        class_=AsyncSession,
        expire_on_commit=False
    )
    
    # Create tables
    async with _async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    return _async_engine


@asynccontextmanager
async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """Get async database session (yield per operation)."""
    global _async_session_factory
    
    if _async_session_factory is None:
        await init_db_async()
    
    async with _async_session_factory() as session:
        yield session


# Synchronous session for backward compatibility
# Deprecated: Use get_async_session() instead
import warnings

def get_session():
    """Get database session (sync - deprecated, use get_async_session)."""
    warnings.warn(
        "get_session() is deprecated, use get_async_session() instead",
        DeprecationWarning,
        stacklevel=2
    )
    # Use synchronous engine for backward compatibility
    db_url = get_database_url()
    engine = create_engine(
        db_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True
    )
    Session = sessionmaker(bind=engine)
    return Session()


# Helper functions for database operations
class DBHelper:
    """Database helper functions."""
    
    @staticmethod
    async def create_position_execute(session: AsyncSession, contract: str, batch_num: int,
                              batch_position_value: float, offset: str) -> PositionExecute:
        """Create position execute record."""
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
        return pos
    
    @staticmethod
    async def create_batch_execute(session: AsyncSession, position_execute_id: int,
                           timeout: int = 3600,
                           first_order_wait_timeout: int = 300) -> BatchExecute:
        """Create batch execute record."""
        batch = BatchExecute(
            position_execute_id=position_execute_id,
            timeout=timeout,
            first_order_wait_timeout=first_order_wait_timeout,
            execute_status='PENDING',
            phase='PENDING'
        )
        session.add(batch)
        await session.commit()
        await session.refresh(batch)
        return batch
    
    @staticmethod
    async def get_pending_batches(session: AsyncSession) -> List[BatchExecute]:
        """Get all pending batches."""
        result = await session.execute(
            select(BatchExecute).where(BatchExecute.execute_status == 'PENDING')
        )
        return list(result.scalars().all())
    
    @staticmethod
    async def get_running_batches(session: AsyncSession) -> List[BatchExecute]:
        """Get all running batches."""
        result = await session.execute(
            select(BatchExecute).where(BatchExecute.execute_status == 'RUNNING')
        )
        return list(result.scalars().all())
    
    @staticmethod
    async def get_position_execute(session: AsyncSession, id: int) -> Optional[PositionExecute]:
        """Get position execute by ID."""
        result = await session.execute(
            select(PositionExecute).where(PositionExecute.id == id)
        )
        return result.scalar_one_or_none()
    
    @staticmethod
    async def get_all_positions(session: AsyncSession) -> List[PositionExecute]:
        """Get all position executes."""
        result = await session.execute(
            select(PositionExecute).where(
                PositionExecute.offset == 'OPEN',
                PositionExecute.execute_status.in_(['PENDING', 'RUNNING'])
            )
        )
        return list(result.scalars().all())
    
    @staticmethod
    async def is_symbol_locked(session: AsyncSession, symbol: str) -> bool:
        """Check if symbol is locked."""
        result = await session.execute(
            select(LockInfo).where(
                LockInfo.symbol == symbol,
                LockInfo.locked == True
            )
        )
        return result.scalar_one_or_none() is not None
    
    @staticmethod
    async def acquire_lock(session: AsyncSession, symbol: str, operation: str) -> bool:
        """Acquire lock for symbol."""
        if await DBHelper.is_symbol_locked(session, symbol):
            return False
        lock = LockInfo(symbol=symbol, operation=operation, locked=True)
        session.add(lock)
        await session.commit()
        return True
    
    @staticmethod
    async def release_lock(session: AsyncSession, symbol: str) -> None:
        """Release lock for symbol."""
        result = await session.execute(
            select(LockInfo).where(LockInfo.symbol == symbol)
        )
        lock = result.scalar_one_or_none()
        if lock:
            lock.locked = False
            await session.commit()
