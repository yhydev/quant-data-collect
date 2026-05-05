"""
Order sequence plugins for Binance Arbitrage Platform.
Defines the order in which trades are executed.
"""
from decimal import Decimal

from models.interfaces import IOrderPlugin, OrderSequence, InitialParams

FUTURES_OPEN_MULTIPLIER = Decimal("1.002")
SPOT_OPEN_MULTIPLIER = Decimal("0.998")
SLIPPAGE = Decimal("0.001")  # 0.1%


class FuturesFirstPlugin(IOrderPlugin):
    """Execute futures first, then spot."""
    
    def get_order_sequence(self) -> OrderSequence:
        return OrderSequence.FUTURES_FIRST
    
    async def get_initial_params(self, collector, contract: str, batch_value: float) -> InitialParams:
        """Get initial params: futures first, spot second."""
        _ = batch_value
        contract_ticker = await collector.get_contract_ticker(contract)
        base_price = Decimal(str(contract_ticker.mark_price))

        # Default open pricing:
        # use a single base price for both pending orders
        # futures first at +0.2%, then spot at -0.2%
        contract_price = float(base_price * FUTURES_OPEN_MULTIPLIER)
        spot_price = float(base_price * SPOT_OPEN_MULTIPLIER)
        
        return InitialParams(
            order_sequence=OrderSequence.FUTURES_FIRST,
            contract_price=contract_price,
            spot_price=spot_price
        )


class SpotFirstPlugin(IOrderPlugin):
    """Execute spot first, then futures."""
    
    def get_order_sequence(self) -> OrderSequence:
        return OrderSequence.SPOT_FIRST
    
    async def get_initial_params(self, collector, contract: str, batch_value: float) -> InitialParams:
        """Get initial params: spot first, futures second."""
        _ = batch_value
        contract_ticker = await collector.get_contract_ticker(contract)
        base_price = Decimal(str(contract_ticker.mark_price))

        # Use the same base price for both sides (spot gets slippage)
        contract_price = float(base_price)
        spot_price = float(base_price * (Decimal("1") + SLIPPAGE))
        
        return InitialParams(
            order_sequence=OrderSequence.SPOT_FIRST,
            contract_price=contract_price,
            spot_price=spot_price
        )


# Registry for plugins
PLUGINS = {
    'futures_first': FuturesFirstPlugin,
    'spot_first': SpotFirstPlugin,
}


def get_plugin(plugin_name: str) -> IOrderPlugin:
    """Get plugin by name."""
    plugin_class = PLUGINS.get(plugin_name)
    if plugin_class is None:
        raise ValueError(f"Unknown plugin: {plugin_name}")
    return plugin_class()


def get_available_plugins() -> list:
    """Get list of available plugins."""
    return list(PLUGINS.keys())
