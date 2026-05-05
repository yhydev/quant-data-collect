"""
Trading execution module.
Implements ITrader interface.
"""
import os
import time
import asyncio
from typing import Optional
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from binance import AsyncClient
from binance.exceptions import BinanceAPIException
from models.interfaces import ITrader, TradeResult
from settings import settings


class BinanceTrader(ITrader):
    """Binance trading execution."""
    
    def __init__(self, api_key: str = None, api_secret: str = None,
                 testnet: bool = False):
        self.api_key = api_key or settings.get('binance.api_key', os.getenv('BINANCE_API_KEY', ''))
        self.api_secret = api_secret or settings.get('binance.secret_key', os.getenv('BINANCE_SECRET_KEY', ''))
        self.testnet = bool(settings.get('binance.testnet', testnet))
        self.proxy = settings.get('binance.auth_http_proxy', '').strip() or None
        self.client: Optional[AsyncClient] = None

    async def _get_client(self) -> AsyncClient:
        """Get or create python-binance async client."""
        if self.client is None:
            self.client = await AsyncClient.create(
                api_key=self.api_key,
                api_secret=self.api_secret,
                testnet=self.testnet,
                https_proxy=self.proxy,
                session_params={"trust_env": True},
            )
        return self.client

    async def _get_lot_step(self, symbol: str, is_spot: bool) -> Decimal:
        """Get LOT_SIZE stepSize for symbol."""
        client = await self._get_client()
        data = await (client.get_exchange_info() if is_spot else client.futures_exchange_info())

        for item in data.get('symbols', []):
            if item.get('symbol') != symbol:
                continue
            for f in item.get('filters', []):
                if f.get('filterType') == 'LOT_SIZE':
                    return Decimal(str(f.get('stepSize', '0.000001')))
            break

        return Decimal('0.000001')

    async def _get_price_tick(self, symbol: str, is_spot: bool) -> Decimal:
        """Get PRICE_FILTER tickSize for symbol."""
        client = await self._get_client()
        data = await (client.get_exchange_info() if is_spot else client.futures_exchange_info())

        for item in data.get('symbols', []):
            if item.get('symbol') != symbol:
                continue
            for f in item.get('filters', []):
                if f.get('filterType') == 'PRICE_FILTER':
                    return Decimal(str(f.get('tickSize', '0.000001')))
            break

        return Decimal('0.000001')

    def _floor_to_step(self, quantity: Decimal, step: Decimal) -> Decimal:
        """Floor quantity to exchange step size."""
        if step <= 0:
            return quantity
        return (quantity / step).to_integral_value(rounding=ROUND_DOWN) * step

    def _ceil_to_step(self, quantity: Decimal, step: Decimal) -> Decimal:
        """Ceil quantity to exchange step size."""
        if step <= 0:
            return quantity
        return (quantity / step).to_integral_value(rounding=ROUND_UP) * step

    def _format_quantity(self, quantity: Decimal) -> str:
        """Format quantity to Binance-friendly string."""
        return format(quantity.normalize(), 'f')

    def _format_price(self, price: Decimal, tick: Decimal) -> str:
        """Format price with exchange tick precision."""
        adjusted = self._floor_to_step(price, tick)
        return format(adjusted.normalize(), 'f')

    def _format_exception_message(self, error: Exception) -> str:
        """Format exception details for consistent API error output."""
        if isinstance(error, BinanceAPIException):
            return (
                f"BinanceAPIException: http_code={error.status_code}, "
                f"code={error.code}, message={error.message}"
            )
        return str(error)
    
    async def close(self):
        """Close session."""
        if self.client is not None:
            await self.client.close_connection()
            self.client = None
    
    async def open_futures_short(self, symbol: str, amount: float, 
                               price: float) -> TradeResult:
        """Open futures short position (sell)."""
        try:
            client = await self._get_client()
            
            step = await self._get_lot_step(symbol, is_spot=False)
            tick = await self._get_price_tick(symbol, is_spot=False)
            raw_quantity = Decimal(str(amount)) / Decimal(str(price))
            quantity = self._ceil_to_step(raw_quantity, step)
            if quantity <= 0:
                return TradeResult(success=False, message="Quantity too small after step rounding")
            
            data = await client.futures_create_order(
                symbol=symbol,
                side='SELL',
                positionSide='SHORT',
                type='LIMIT',
                timeInForce='GTC',
                quantity=self._format_quantity(quantity),
                price=self._format_price(Decimal(str(price)), tick),
            )
            return TradeResult(
                success=True,
                order_id=data.get('orderId'),
                message=f"Order placed: {data.get('orderId')}"
            )
        except Exception as e:
            return TradeResult(success=False, message=self._format_exception_message(e))
    
    async def close_futures_position(self, symbol: str, amount: float) -> TradeResult:
        """Close futures position by position value (buy to cover)."""
        try:
            client = await self._get_client()
            data = await client.futures_position_information(symbol=symbol)
            position = None
            for p in data:
                if p.get('symbol') == symbol and float(p.get('positionAmt', 0)) < 0:
                    position = p
                    break

            if not position:
                return TradeResult(success=False, message="No short position found")

            position_quantity = abs(Decimal(str(position.get('positionAmt', 0))))

            price_data = await client.futures_symbol_ticker(symbol=symbol)
            current_price = Decimal(str(price_data.get('price', 0)))

            if current_price <= 0:
                return TradeResult(success=False, message="Invalid futures price")

            step = await self._get_lot_step(symbol, is_spot=False)
            target_quantity = self._ceil_to_step(Decimal(str(amount)) / current_price, step)
            max_closeable = self._floor_to_step(position_quantity, step)
            quantity = min(max_closeable, target_quantity)
            if quantity <= 0:
                return TradeResult(success=False, message="Quantity too small after step rounding")

            order = await client.futures_create_order(
                symbol=symbol,
                side='BUY',
                positionSide='SHORT',
                type='MARKET',
                quantity=self._format_quantity(quantity),
            )
            return TradeResult(
                success=True,
                order_id=order.get('orderId'),
                executed_price=float(order.get('avgPrice', 0) or 0),
                message="Position closed"
            )
        except Exception as e:
            return TradeResult(success=False, message=self._format_exception_message(e))
    
    async def buy_spot(self, symbol: str, amount: float, 
                   price: float) -> TradeResult:
        """Buy spot asset."""
        try:
            client = await self._get_client()
            
            step = await self._get_lot_step(symbol, is_spot=True)
            tick = await self._get_price_tick(symbol, is_spot=True)
            raw_quantity = Decimal(str(amount)) / Decimal(str(price))
            quantity = self._ceil_to_step(raw_quantity, step)
            if quantity <= 0:
                return TradeResult(success=False, message="Quantity too small after step rounding")
            
            data = await client.create_order(
                symbol=symbol,
                side='BUY',
                type='LIMIT',
                timeInForce='GTC',
                quantity=self._format_quantity(quantity),
                price=self._format_price(Decimal(str(price)), tick),
            )
            return TradeResult(success=True, order_id=data.get('orderId'), message="Order placed")
        except Exception as e:
            return TradeResult(success=False, message=self._format_exception_message(e))
    
    async def sell_spot(self, symbol: str, amount: float) -> TradeResult:
        """Sell spot asset by position value."""
        try:
            client = await self._get_client()
            data = await client.get_account()
            balance = Decimal('0')
            for bal in data.get('balances', []):
                if bal.get('asset') == symbol.replace('USDT', ''):
                    balance = Decimal(str(bal.get('free', 0)))
                    break
            
            if balance <= 0:
                return TradeResult(success=False, message="No balance to sell")

            price_data = await client.get_symbol_ticker(symbol=symbol)
            current_price = Decimal(str(price_data.get('price', 0)))

            if current_price <= 0:
                return TradeResult(success=False, message="Invalid spot price")

            step = await self._get_lot_step(symbol, is_spot=True)
            target_quantity = self._ceil_to_step(Decimal(str(amount)) / current_price, step)
            max_sellable = self._floor_to_step(balance, step)
            quantity = min(max_sellable, target_quantity)
            if quantity <= 0:
                return TradeResult(success=False, message="Quantity too small after step rounding")

            data = await client.create_order(
                symbol=symbol,
                side='SELL',
                type='MARKET',
                quantity=self._format_quantity(quantity),
            )
            return TradeResult(success=True, order_id=data.get('orderId'), message="Sold")
        except Exception as e:
            return TradeResult(success=False, message=self._format_exception_message(e))
    
    async def transfer_to_savings(self, symbol: str, quantity: float) -> TradeResult:
        """Subscribe asset from spot to Simple Earn flexible."""
        try:
            client = await self._get_client()
            asset = symbol.replace('USDT', '')
            req_qty = Decimal(str(quantity))
            if req_qty <= 0:
                return TradeResult(success=False, message="Transfer amount must be positive")

            balance_data = await client.get_asset_balance(asset=asset)
            free_balance = Decimal(str((balance_data or {}).get('free', '0')))
            transfer_qty = min(req_qty, free_balance)
            if transfer_qty <= 0:
                return TradeResult(success=False, message=f"Insufficient free spot balance for {asset}")

            transfer_qty_str = self._format_quantity(transfer_qty)
            products = await client.get_simple_earn_flexible_product_list(
                asset=asset,
                current=1,
                size=100,
            )
            rows = products.get('rows', []) if isinstance(products, dict) else []
            if not rows:
                return TradeResult(success=False, message=f"No flexible Simple Earn product for asset {asset}")

            product_id = rows[0].get('productId')
            if not product_id:
                return TradeResult(success=False, message=f"Missing productId for asset {asset}")

            await client.subscribe_simple_earn_flexible_product(
                productId=product_id,
                amount=transfer_qty_str,
            )
            return TradeResult(
                success=True,
                message=f"Transferred to savings (requested={req_qty}, actual={transfer_qty_str})",
            )
        except Exception as e:
            return TradeResult(success=False, message=self._format_exception_message(e))
    
    async def transfer_from_savings(self, symbol: str, amount: float) -> TradeResult:
        """Redeem asset from Simple Earn flexible to spot."""
        try:
            client = await self._get_client()
            asset = symbol.replace('USDT', '')
            positions = await client.get_simple_earn_flexible_product_position(
                asset=asset,
                current=1,
                size=100,
            )
            rows = positions.get('rows', []) if isinstance(positions, dict) else []
            product_id = rows[0].get('productId') if rows else None
            if not product_id:
                products = await client.get_simple_earn_flexible_product_list(
                    asset=asset,
                    current=1,
                    size=100,
                )
                p_rows = products.get('rows', []) if isinstance(products, dict) else []
                if not p_rows:
                    return TradeResult(success=False, message=f"No flexible Simple Earn product for asset {asset}")
                product_id = p_rows[0].get('productId')
            if not product_id:
                return TradeResult(success=False, message=f"Missing productId for asset {asset}")

            await client.redeem_simple_earn_flexible_product(
                productId=product_id,
                amount=str(amount),
                redeemAll=False,
            )
            return TradeResult(success=True, message="Transferred from savings")
        except Exception as e:
            return TradeResult(success=False, message=self._format_exception_message(e))
    
    async def get_order_status(self, symbol: str, order_id: int, is_spot: bool = False) -> dict:
        """Get order status.
        
        Args:
            symbol: Trading symbol
            order_id: Order ID
            is_spot: True if spot order, False if futures order
        """
        try:
            client = await self._get_client()
            if is_spot:
                return await client.get_order(symbol=symbol, orderId=str(order_id))
            return await client.futures_get_order(symbol=symbol, orderId=str(order_id))
        except Exception as e:
            return {'status': 'ERROR', 'message': self._format_exception_message(e)}


# Mock trader for testing
class MockTrader(ITrader):
    """Mock trader for testing."""
    
    def __init__(self):
        self.orders = {}
        self.positions = {}
        self.balances = {
            'BTC': 1.0,
            'ETH': 10.0,
            'BNB': 100.0,
            'USDT': 100000.0
        }
    
    async def open_futures_short(self, symbol: str, amount: float, 
                              price: float) -> TradeResult:
        order_id = int(time.time() * 1000)
        self.orders[order_id] = {
            'symbol': symbol,
            'side': 'SELL',
            'price': price,
            'status': 'FILLED'
        }
        return TradeResult(success=True, order_id=order_id, executed_price=price)
    
    async def close_futures_position(self, symbol: str, amount: float) -> TradeResult:
        return TradeResult(success=True, message="Position closed")
    
    async def buy_spot(self, symbol: str, amount: float, 
                  price: float) -> TradeResult:
        order_id = int(time.time() * 1000)
        self.orders[order_id] = {
            'symbol': symbol,
            'side': 'BUY',
            'price': price,
            'status': 'FILLED'
        }
        return TradeResult(success=True, order_id=order_id)
    
    async def sell_spot(self, symbol: str, amount: float) -> TradeResult:
        return TradeResult(success=True, message="Sold")
    
    async def transfer_to_savings(self, symbol: str, amount: float) -> TradeResult:
        return TradeResult(success=True, message="Transferred to savings")
    
    async def transfer_from_savings(self, symbol: str, amount: float) -> TradeResult:
        return TradeResult(success=True, message="Transferred from savings")
    
    async def get_order_status(self, symbol: str, order_id: int, is_spot: bool = False) -> dict:
        """Get order status."""
        _ = is_spot
        if order_id in self.orders:
            order = self.orders[order_id]
            return {
                'status': order.get('status', 'FILLED'),
                'symbol': order.get('symbol'),
                'orderId': order_id,
                'avgPrice': order.get('price', 0)
            }
        return {'status': 'UNKNOWN', 'symbol': symbol}


# Factory function
def create_trader(trader_type: str = 'binance', **kwargs) -> ITrader:
    """Create trader instance."""
    if trader_type == 'binance':
        return BinanceTrader(**kwargs)
    elif trader_type == 'mock':
        return MockTrader()
    else:
        raise ValueError(f"Unknown trader type: {trader_type}")
