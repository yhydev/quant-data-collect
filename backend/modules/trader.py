"""
Trading execution module.
Implements ITrader interface.
"""
import os
import time
import asyncio
from typing import Optional
from decimal import Decimal
import aiohttp
import hashlib
import hmac
from .interfaces import ITrader, TradeResult


class BinanceTrader(ITrader):
    """Binance trading execution."""
    
    def __init__(self, api_key: str = None, api_secret: str = None,
                 testnet: bool = False):
        self.api_key = api_key or os.getenv('BINANCE_API_KEY', '')
        self.api_secret = api_secret or os.getenv('BINANCE_SECRET_KEY', '')
        
        if testnet:
            self.base_url = "https://testnet.binance.vision/api"
            self.futures_url = "https://testnet.binance.vision/api"
        else:
            self.base_url = "https://api.binance.com/api"
            self.futures_url = "https://fapi.binance.com/fapi"
        
        self.session = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    def _sign(self, params: dict) -> str:
        """Sign request params."""
        query_string = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature
    
    def _headers(self) -> dict:
        """Get request headers."""
        return {
            'X-MBX-APIKEY': self.api_key,
            'Content-Type': 'application/json'
        }
    
    async def close(self):
        """Close session."""
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def open_futures_short(self, symbol: str, amount: float, 
                              price: float) -> TradeResult:
        """Open futures short position (sell)."""
        try:
            session = await self._get_session()
            
            # Calculate quantity
            quantity = round(amount / price, 4)
            
            params = {
                'symbol': symbol,
                'side': 'SELL',
                'positionSide': 'SHORT',
                'type': 'LIMIT',
                'timeInForce': 'GTC',
                'quantity': str(quantity),
                'price': str(price),
                'timestamp': int(time.time() * 1000)
            }
            params['signature'] = self._sign(params)
            
            async with session.post(
                f"{self.futures_url}/v1/order",
                params=params,
                headers=self._headers()
            ) as resp:
                if resp.status in [200, 201]:
                    data = await resp.json()
                    return TradeResult(
                        success=True,
                        order_id=data.get('orderId'),
                        message=f"Order placed: {data.get('orderId')}"
                    )
                else:
                    text = await resp.text()
                    return TradeResult(
                        success=False,
                        message=f"Error: {text}"
                    )
        except Exception as e:
            return TradeResult(success=False, message=str(e))
    
    async def close_futures_position(self, symbol: str, amount: float) -> TradeResult:
        """Close futures position (buy to cover)."""
        try:
            session = await self._get_session()
            
            # Get current position first
            params = {
                'symbol': symbol,
                'timestamp': int(time.time() * 1000)
            }
            params['signature'] = self._sign(params)
            
            async with session.get(
                f"{self.futures_url}/v2/positionRisk",
                params=params,
                headers=self._headers()
            ) as resp:
                if resp.status != 200:
                    return TradeResult(success=False, message="Failed to get position")
                
                data = await resp.json()
                position = None
                for p in data:
                    if p.get('symbol') == symbol and float(p.get('positionAmt', 0)) < 0:
                        position = p
                        break
                
                if not position:
                    return TradeResult(success=False, message="No short position found")
                
                quantity = abs(float(position.get('positionAmt', 0)))

            # Place market order to close
            params = {
                'symbol': symbol,
                'side': 'BUY',
                'positionSide': 'SHORT',
                'type': 'MARKET',
                'quantity': str(quantity),
                'timestamp': int(time.time() * 1000)
            }
            params['signature'] = self._sign(params)
            
            async with session.post(
                f"{self.futures_url}/v1/order",
                params=params,
                headers=self._headers()
            ) as resp:
                if resp.status in [200, 201]:
                    data = await resp.json()
                    return TradeResult(
                        success=True,
                        order_id=data.get('orderId'),
                        executed_price=float(data.get('avgPrice', 0)),
                        message=f"Position closed"
                    )
                else:
                    text = await resp.text()
                    return TradeResult(success=False, message=f"Error: {text}")
        except Exception as e:
            return TradeResult(success=False, message=str(e))
    
    async def buy_spot(self, symbol: str, amount: float, 
                  price: float) -> TradeResult:
        """Buy spot asset."""
        try:
            session = await self._get_session()
            
            # Calculate quantity
            quantity = round(amount / price, 6)
            
            params = {
                'symbol': symbol,
                'side': 'BUY',
                'type': 'LIMIT',
                'timeInForce': 'GTC',
                'quantity': str(quantity),
                'price': str(price),
                'timestamp': int(time.time() * 1000)
            }
            params['signature'] = self._sign(params)
            
            async with session.post(
                f"{self.base_url}/v3/order",
                params=params,
                headers=self._headers()
            ) as resp:
                if resp.status in [200, 201]:
                    data = await resp.json()
                    return TradeResult(
                        success=True,
                        order_id=data.get('orderId'),
                        message=f"Order placed"
                    )
                else:
                    text = await resp.text()
                    return TradeResult(success=False, message=f"Error: {text}")
        except Exception as e:
            return TradeResult(success=False, message=str(e))
    
    async def sell_spot(self, symbol: str, amount: float) -> TradeResult:
        """Sell spot asset."""
        try:
            session = await self._get_session()
            
            # First get balance
            params = {
                'timestamp': int(time.time() * 1000)
            }
            params['signature'] = self._sign(params)
            
            async with session.get(
                f"{self.base_url}/v3/account",
                params=params,
                headers=self._headers()
            ) as resp:
                if resp.status != 200:
                    return TradeResult(success=False, message="Failed to get account")
                
                data = await resp.json()
                balance = 0
                for bal in data.get('balances', []):
                    if bal.get('asset') == symbol.replace('USDT', ''):
                        balance = float(bal.get('free', 0))
                        break
            
            if balance <= 0:
                return TradeResult(success=False, message="No balance to sell")

            # Place market order
            params = {
                'symbol': symbol,
                'side': 'SELL',
                'type': 'MARKET',
                'quantity': str(balance),
                'timestamp': int(time.time() * 1000)
            }
            params['signature'] = self._sign(params)
            
            async with session.post(
                f"{self.base_url}/v3/order",
                params=params,
                headers=self._headers()
            ) as resp:
                if resp.status in [200, 201]:
                    data = await resp.json()
                    return TradeResult(
                        success=True,
                        order_id=data.get('orderId'),
                        message=f"Sold"
                    )
                else:
                    text = await resp.text()
                    return TradeResult(success=False, message=f"Error: {text}")
        except Exception as e:
            return TradeResult(success=False, message=str(e))
    
    async def transfer_to_savings(self, symbol: str, amount: float) -> TradeResult:
        """Transfer spot to savings."""
        try:
            session = await self._get_session()
            
            asset = symbol.replace('USDT', '')
            
            params = {
                'asset': asset,
                'amount': str(amount),
                'type': '1',  # 1 = main to savings
                'timestamp': int(time.time() * 1000)
            }
            params['signature'] = self._sign(params)
            
            async with session.post(
                f"{self.base_url}/v3/asset/transfer",
                params=params,
                headers=self._headers()
            ) as resp:
                if resp.status in [200, 201]:
                    return TradeResult(success=True, message="Transferred to savings")
                else:
                    text = await resp.text()
                    return TradeResult(success=False, message=f"Error: {text}")
        except Exception as e:
            return TradeResult(success=False, message=str(e))
    
    async def transfer_from_savings(self, symbol: str, amount: float) -> TradeResult:
        """Transfer from savings to spot."""
        try:
            session = await self._get_session()
            
            asset = symbol.replace('USDT', '')
            
            params = {
                'asset': asset,
                'amount': str(amount),
                'type': '2',  # 2 = savings to main
                'timestamp': int(time.time() * 1000)
            }
            params['signature'] = self._sign(params)
            
            async with session.post(
                f"{self.base_url}/v3/asset/transfer",
                params=params,
                headers=self._headers()
            ) as resp:
                if resp.status in [200, 201]:
                    return TradeResult(success=True, message="Transferred from savings")
                else:
                    text = await resp.text()
                    return TradeResult(success=False, message=f"Error: {text}")
        except Exception as e:
            return TradeResult(success=False, message=str(e))
    
    async def get_order_status(self, symbol: str, order_id: int) -> dict:
        """Get order status."""
        try:
            session = await self._get_session()
            
            params = {
                'symbol': symbol,
                'orderId': str(order_id),
                'timestamp': int(time.time() * 1000)
            }
            params['signature'] = self._sign(params)
            
            async with session.get(
                f"{self.futures_url}/v1/order",
                params=params,
                headers=self._headers()
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                return {}
        except Exception as e:
            return {'status': 'ERROR', 'message': str(e)}


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


# Factory function
def create_trader(trader_type: str = 'binance', **kwargs) -> ITrader:
    """Create trader instance."""
    if trader_type == 'binance':
        return BinanceTrader(**kwargs)
    elif trader_type == 'mock':
        return MockTrader()
    else:
        raise ValueError(f"Unknown trader type: {trader_type}")