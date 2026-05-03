"""
Strategy module.
Reserved for automation - defaults to empty selection.
"""
from typing import List
from ..interfaces import IStrategy, FundingRate


class DefaultStrategy(IStrategy):
    """Default strategy - no automated selection."""
    
    async def select_pairs(self, rates: List[FundingRate]) -> List[str]:
        """Return empty list - manual selection only."""
        return []


class HighFundingStrategy(IStrategy):
    """Select pairs with highest funding rates."""
    
    def __init__(self, min_rate: float = 0.0001, top_n: int = 10):
        self.min_rate = min_rate
        self.top_n = top_n
    
    async def select_pairs(self, rates: List[FundingRate]) -> List[str]:
        """Select top N pairs with highest funding rates."""
        # Filter by minimum rate
        filtered = [r for r in rates if abs(r.rate) >= self.min_rate]
        
        # Sort by rate (absolute value)
        sorted_rates = sorted(filtered, key=lambda x: abs(x.rate), reverse=True)
        
        # Return top N symbols
        return [r.symbol for r in sorted_rates[:self.top_n]]


# Factory function
def create_strategy(strategy_type: str = 'default', **kwargs) -> IStrategy:
    """Create strategy instance."""
    if strategy_type == 'default':
        return DefaultStrategy()
    elif strategy_type == 'high_funding':
        return HighFundingStrategy(**kwargs)
    else:
        raise ValueError(f"Unknown strategy: {strategy_type}")