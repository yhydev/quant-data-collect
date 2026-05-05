"""
Interface definitions for Binance Arbitrage Platform.
Modules communicate through these abstractions.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum

# Import models for type hints
from ..database import PositionExecute, BatchExecute, Earning


class OrderSequence(Enum):
    """Order execution sequence options."""
    FUTURES_FIRST = "futures_first"  # 先合约后现货
    SPOT_FIRST = "spot_first"      # 先现货后合约


class FundingRate:
    """Funding rate data."""
    def __init__(self, symbol: str, rate: float, next_funding_time: int):
        self.symbol = symbol
        self.rate = rate
        self.next_funding_time = next_funding_time


class SpotPrice:
    """Spot price data."""
    def __init__(self, symbol: str, bid_price: float, ask_price: float):
        self.symbol = symbol
        self.bid_price = bid_price
        self.ask_price = ask_price


class ContractTicker:
    """Contract ticker data."""
    def __init__(self, symbol: str, mark_price: float, index_price: float):
        self.symbol = symbol
        self.mark_price = mark_price
        self.index_price = index_price


class TradeResult:
    """Trade execution result."""
    def __init__(self, success: bool, order_id: Optional[int] = None, 
                 executed_price: Optional[float] = None, message: str = ""):
        self.success = success
        self.order_id = order_id
        self.executed_price = executed_price
        self.message = message


class Position:
    """Position data."""
    def __init__(self, id: int, contract: str, amount: float, 
                 entry_price: float, current_price: float,
                 pnl: float, status: str):
        self.id = id
        self.contract = contract
        self.amount = amount
        self.entry_price = entry_price
        self.current_price = current_price
        self.pnl = pnl
        self.status = status


class Earning:
    """Earning record."""
    def __init__(self, id: int, contract: str, amount: float,
                 funding_earn: float, interest_earn: float,
                 created_at: str):
        self.id = id
        self.contract = contract
        self.amount = amount
        self.funding_earn = funding_earn
        self.interest_earn = interest_earn
        self.created_at = created_at


# Interface definitions

class ICollector(ABC):
    """Data collector interface."""
    
    @abstractmethod
    async def get_funding_rates(self) -> List[FundingRate]:
        """Get current funding rates for all contracts."""
        pass
    
    @abstractmethod
    async def get_spot_price(self, symbol: str) -> SpotPrice:
        """Get spot price for a symbol."""
        pass
    
    @abstractmethod
    async def get_contract_ticker(self, symbol: str) -> ContractTicker:
        """Get contract ticker (mark price, index price)."""
        pass


class ITrader(ABC):
    """Trading execution interface."""
    
    @abstractmethod
    async def open_futures_short(self, symbol: str, amount: float, 
                              price: float) -> TradeResult:
        """Open futures short position."""
        pass
    
    @abstractmethod
    async def close_futures_position(self, symbol: str, amount: float) -> TradeResult:
        """Close futures position."""
        pass
    
    @abstractmethod
    async def buy_spot(self, symbol: str, amount: float, 
                      price: float) -> TradeResult:
        """Buy spot asset."""
        pass
    
    @abstractmethod
    async def sell_spot(self, symbol: str, amount: float) -> TradeResult:
        """Sell spot asset."""
        pass
    
    @abstractmethod
    async def transfer_to_savings(self, symbol: str, amount: float) -> TradeResult:
        """Transfer spot to savings."""
        pass
    
    @abstractmethod
    async def transfer_from_savings(self, symbol: str, amount: float) -> TradeResult:
        """Transfer from savings to spot."""
        pass
    
    @abstractmethod
    async def get_order_status(self, symbol: str, order_id: int) -> dict:
        """Get order status."""
        pass

    @abstractmethod
    async def cancel_order(self, symbol: str, order_id: int, is_spot: bool = False) -> TradeResult:
        """Cancel an open order."""
        pass


@dataclass
class InitialParams:
    """Initial parameters returned by order plugin."""
    order_sequence: OrderSequence
    contract_price: float
    spot_price: float


class IOrderPlugin(ABC):
    """Order sequence plugin interface."""
    
    @abstractmethod
    def get_order_sequence(self) -> OrderSequence:
        """Return order execution sequence."""
        pass
    
    @abstractmethod
    async def get_initial_params(self, collector, contract: str, batch_value: float) -> InitialParams:
        """Get initial parameters (order sequence + prices) in one call."""
        pass


class ILockManager(ABC):
    """Concurrency control interface."""
    
    @abstractmethod
    async def acquire(self, symbol: str, operation: str) -> bool:
        """Try to acquire lock for symbol."""
        pass
    
    @abstractmethod
    async def release(self, symbol: str) -> None:
        """Release lock for symbol."""
        pass
    
    @abstractmethod
    def is_locked(self, symbol: str) -> bool:
        """Check if symbol is locked."""
        pass


class IPortfolio(ABC):
    """Portfolio management interface."""
    
    @abstractmethod
    async def get_positions(self) -> List[Position]:
        """Get all positions."""
        pass
    
    @abstractmethod
    async def get_earnings(self) -> List[Earning]:
        """Get all earnings."""
        pass
    
    @abstractmethod
    async def create_position_execute(self, contract: str, batch_num: int,
                                      batch_position_value: float,
                                      offset: str) -> int:
        """Create position execute record."""
        pass
    
    @abstractmethod
    async def create_batch_execute(self, position_execute_id: int,
                                   timeout: int) -> int:
        """Create batch execute record."""
        pass


class IStrategy(ABC):
    """Strategy interface (reserved for automation)."""
    
    @abstractmethod
    async def select_pairs(self, rates: List[FundingRate]) -> List[str]:
        """Select trading pairs based on funding rates."""
        pass
