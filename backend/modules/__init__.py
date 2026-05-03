"""
Core modules for Binance Arbitrage Platform.
"""
from .collector import BinanceCollector, MockCollector, create_collector
from .trader import BinanceTrader, MockTrader, create_trader
from .portfolio import PortfolioManager, LockManager
from .strategy import DefaultStrategy, HighFundingStrategy, create_strategy

__all__ = [
    'BinanceCollector',
    'MockCollector',
    'create_collector',
    'BinanceTrader', 
    'MockTrader',
    'create_trader',
    'PortfolioManager',
    'LockManager',
    'DefaultStrategy',
    'HighFundingStrategy',
    'create_strategy',
]