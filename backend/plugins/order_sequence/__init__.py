"""
Order sequence plugins.
Plug-in system to choose execution order.
"""
from ..interfaces import IOrderPlugin, OrderSequence


class FuturesFirstPlugin(IOrderPlugin):
    """Execute futures first, then spot."""
    
    def get_order_sequence(self) -> OrderSequence:
        return OrderSequence.FUTURES_FIRST


class SpotFirstPlugin(IOrderPlugin):
    """Execute spot first, then futures."""
    
    def get_order_sequence(self) -> OrderSequence:
        return OrderSequence.SPOT_FIRST


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