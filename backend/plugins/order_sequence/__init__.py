"""
Order sequence plugins for Binance Arbitrage Platform.
Defines the order in which trades are executed.
"""
from models.interfaces import IOrderPlugin, OrderSequence, InitialParams

SLIPPAGE = 0.001  # 0.1%


class FuturesFirstPlugin(IOrderPlugin):
    """Execute futures first, then spot."""
    
    def get_order_sequence(self) -> OrderSequence:
        return OrderSequence.FUTURES_FIRST
    
    async def get_initial_params(self, collector, contract: str, batch_value: float) -> InitialParams:
        """Get initial params: futures first, spot second."""
        contract_ticker = await collector.get_contract_ticker(contract)
        spot_price_obj = await collector.get_spot_price(contract)
        
        # Calculate prices with slippage (futures gets slippage)
        contract_price = float(contract_ticker.mark_price * (1 + SLIPPAGE))
        spot_price = float(spot_price_obj.ask_price)
        
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
        contract_ticker = await collector.get_contract_ticker(contract)
        spot_price_obj = await collector.get_spot_price(contract)
        
        # Calculate prices with slippage (spot gets slippage)
        contract_price = float(contract_ticker.mark_price)
        spot_price = float(spot_price_obj.ask_price * (1 + SLIPPAGE))
        
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
