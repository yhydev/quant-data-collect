"""Thin Binance execution service for arbitrage actions."""

from __future__ import annotations

from models.interfaces import TradeResult


class ArbitrageService:
    """Pure execution wrapper. No business decision logic inside."""

    async def open_futures_short(self, trader, symbol: str, amount: float, price: float) -> TradeResult:
        return await trader.open_futures_short(symbol, amount, price)

    async def buy_spot(self, trader, symbol: str, amount: float, price: float) -> TradeResult:
        return await trader.buy_spot(symbol, amount, price)

    async def close_futures_position(self, trader, symbol: str, amount: float) -> TradeResult:
        return await trader.close_futures_position(symbol, amount)

    async def sell_spot(self, trader, symbol: str, amount: float) -> TradeResult:
        return await trader.sell_spot(symbol, amount)

    async def transfer_to_savings(self, trader, symbol: str, quantity: float) -> TradeResult:
        return await trader.transfer_to_savings(symbol, quantity)

    async def transfer_from_savings(self, trader, symbol: str, quantity: float) -> TradeResult:
        return await trader.transfer_from_savings(symbol, quantity)

    async def get_order_status(self, trader, symbol: str, order_id: int, is_spot: bool = False) -> dict:
        return await trader.get_order_status(symbol, order_id, is_spot=is_spot)
