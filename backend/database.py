"""
Database layer for Binance Arbitrage Platform.
Uses SQLAlchemy ORM for PostgreSQL.
"""
from datetime import datetime
from typing import Optional, List
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.pool import NullPool
import os

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
    timeout = Column(Integer, default=300)        # 成交超时时间(秒)
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
    
    position = relationship("PositionExecute", back_populates="batches")
    orders = relationship("PositionOrder", back_populates="batch")


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
    """Get database URL from environment."""
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5432')
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', 'postgres')
    db = os.getenv('POSTGRES_DB', 'arbitrage')
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def init_db():
    """Initialize database connection."""
    db_url = get_database_url()
    engine = create_engine(db_url, poolclass=NullPool)
    Base.metadata.create_all(engine)
    return engine


def get_session():
    """Get database session."""
    engine = init_db()
    Session = sessionmaker(bind=engine)
    return Session()


# Helper functions for database operations
class DBHelper:
    """Database helper functions."""
    
    @staticmethod
    def create_position_execute(session, contract: str, batch_num: int,
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
        session.commit()
        session.refresh(pos)
        return pos
    
    @staticmethod
    def create_batch_execute(session, position_execute_id: int,
                           timeout: int = 300) -> BatchExecute:
        """Create batch execute record."""
        batch = BatchExecute(
            position_execute_id=position_execute_id,
            timeout=timeout,
            execute_status='PENDING',
            phase='PENDING'
        )
        session.add(batch)
        session.commit()
        session.refresh(batch)
        return batch
    
    @staticmethod
    def get_pending_batches(session) -> List[BatchExecute]:
        """Get all pending batches."""
        return session.query(BatchExecute).filter(
            BatchExecute.execute_status == 'PENDING'
        ).all()
    
    @staticmethod
    def get_running_batches(session) -> List[BatchExecute]:
        """Get all running batches."""
        return session.query(BatchExecute).filter(
            BatchExecute.execute_status == 'RUNNING'
        ).all()
    
    @staticmethod
    def get_position_execute(session, id: int) -> Optional[PositionExecute]:
        """Get position execute by ID."""
        return session.query(PositionExecute).filter(
            PositionExecute.id == id
        ).first()
    
    @staticmethod
    def get_all_positions(session) -> List[PositionExecute]:
        """Get all position executes."""
        return session.query(PositionExecute).filter(
            PositionExecute.offset == 'OPEN'
        ).filter(
            PositionExecute.execute_status.in_(['PENDING', 'RUNNING'])
        ).all()
    
    @staticmethod
    def is_symbol_locked(session, symbol: str) -> bool:
        """Check if symbol is locked."""
        lock = session.query(LockInfo).filter(
            LockInfo.symbol == symbol,
            LockInfo.locked == True
        ).first()
        return lock is not None
    
    @staticmethod
    def acquire_lock(session, symbol: str, operation: str) -> bool:
        """Acquire lock for symbol."""
        if DBHelper.is_symbol_locked(session, symbol):
            return False
        lock = LockInfo(symbol=symbol, operation=operation, locked=True)
        session.add(lock)
        session.commit()
        return True
    
    @staticmethod
    def release_lock(session, symbol: str) -> None:
        """Release lock for symbol."""
        lock = session.query(LockInfo).filter(
            LockInfo.symbol == symbol
        ).first()
        if lock:
            lock.locked = False
            session.commit()