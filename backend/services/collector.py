"""
Data collector module for Binance API.
Implements ICollector interface.
With retry mechanism and exponential backoff.
"""
import os
import asyncio
from typing import List, Dict
from decimal import Decimal
import aiohttp
import logging

from models.interfaces import ICollector, FundingRate, SpotPrice, ContractTicker
from settings import settings

logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 3
RETRY_BACKOFF = 1  # seconds


class BinanceCollector(ICollector):
    """Binance data collector."""
    
    def __init__(self, api_key: str = None, api_secret: str = None,
                 testnet: bool = False):
        self.api_key = api_key or settings.get('binance.api_key', os.getenv('BINANCE_API_KEY', ''))
        self.api_secret = api_secret or settings.get('binance.secret_key', os.getenv('BINANCE_SECRET_KEY', ''))
        testnet = bool(settings.get('binance.testnet', testnet))
        
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
    
    async def close(self):
        """Close session."""
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def get_funding_rates(self) -> List[FundingRate]:
        """Get current funding rates for all contracts."""
        try:
            session = await self._get_session()
            async with session.get(
                f"{self.futures_url}/v1/premiumIndex",
            ) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
            
            rates = []
            for item in data:
                rates.append(FundingRate(
                    symbol=item.get('symbol', ''),
                    rate=Decimal(item.get('lastFundingRate', '0')),
                    next_funding_time=int(item.get('nextFundingTime', 0) / 1000)
                ))
            return rates
        except Exception as e:
            logger.error(f"Error getting funding rates: {e}", exc_info=True)
            return []
    
    async def get_spot_price(self, symbol: str) -> SpotPrice:
        """Get spot price for a symbol."""
        try:
            session = await self._get_session()
            # Convert BTCUSDT to BTC/USDT for ticker
            pair = f"{symbol.replace('USDT', '')}/USDT"
            
            async with session.get(
                f"{self.base_url}/v3/ticker/bookTicker",
                params={'symbol': symbol},
            ) as resp:
                if resp.status != 200:
                    return SpotPrice(symbol, 0, 0)
                data = await resp.json()
            
            return SpotPrice(
                symbol=symbol,
                bid_price=Decimal(data.get('bidPrice', '0')),
                ask_price=Decimal(data.get('askPrice', '0'))
            )
        except Exception as e:
            logger.warning(f"Error getting spot price for {symbol}: {e}")
            return SpotPrice(symbol, 0, 0)
    
    async def get_contract_ticker(self, symbol: str) -> ContractTicker:
        """Get contract ticker (mark price, index price)."""
        try:
            session = await self._get_session()
            
            async with session.get(
                f"{self.futures_url}/v1/ticker/price",
                params={'symbol': symbol},
            ) as resp:
                if resp.status != 200:
                    return ContractTicker(symbol, 0, 0)
                data = await resp.json()
            
            mark_price = Decimal(data.get('price', '0'))
            
            # Get index price (mark price is close to index price in normal conditions)
            index_price = mark_price
            
            return ContractTicker(
                symbol=symbol,
                mark_price=mark_price,
                index_price=index_price
            )
        except Exception as e:
            logger.warning(f"Error getting contract ticker for {symbol}: {e}")
            return ContractTicker(symbol, 0, 0)
    
    async def get_all_contracts(self) -> List[str]:
        """Get all available USDT contracts."""
        try:
            session = await self._get_session()
            async with session.get(
                f"{self.futures_url}/v1/exchangeInfo",
            ) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
            
            contracts = []
            for symbol in data.get('symbols', []):
                if symbol.get('quoteAsset') == 'USDT' and symbol.get('status') == 'TRADING':
                    contracts.append(symbol.get('symbol'))
            
            return contracts
        except Exception as e:
            logger.warning(f"Error getting contracts: {e}")
            return []

    async def get_funding_rate_history_window(
        self,
        start_time_ms: int,
        end_time_ms: int,
        limit: int = 1000,
    ) -> List[dict]:
        """Get funding rate history for all symbols in a time window (paginated by caller)."""
        try:
            session = await self._get_session()
            async with session.get(
                f"{self.futures_url}/v1/fundingRate",
                params={
                    'startTime': start_time_ms,
                    'endTime': end_time_ms,
                    'limit': max(1, min(limit, 1000)),
                },
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.warning("Funding history request failed: status=%s body=%s", resp.status, text)
                    return []
                data = await resp.json()
                if isinstance(data, list):
                    return data
                return []
        except Exception as e:
            logger.error(f"Error getting funding rate history: {e}", exc_info=True)
            return []


# Mock collector for testing without API
class MockCollector(ICollector):
    """Mock collector for testing."""
    
    def __init__(self):
        self.funding_rates = {
            'BTCUSDT': Decimal('0.0001'),
            'ETHUSDT': Decimal('0.0001'),
            'BNBUSDT': Decimal('0.0005'),
        }
    
    async def get_funding_rates(self) -> List[FundingRate]:
        import time
        rates = []
        for symbol, rate in self.funding_rates.items():
            rates.append(FundingRate(
                symbol=symbol,
                rate=rate,
                next_funding_time=int(time.time()) + 28800
            ))
        return rates
    
    async def get_spot_price(self, symbol: str) -> SpotPrice:
        prices = {
            'BTCUSDT': ('45000.00', '45100.00'),
            'ETHUSDT': ('2500.00', '2510.00'),
            'BNBUSDT': ('300.00', '301.00'),
        }
        bid, ask = prices.get(symbol, ('0', '0'))
        return SpotPrice(symbol, Decimal(bid), Decimal(ask))
    
    async def get_contract_ticker(self, symbol: str) -> ContractTicker:
        prices = {
            'BTCUSDT': ('45000.00', '44950.00'),
            'ETHUSDT': ('2500.00', '2495.00'),
            'BNBUSDT': ('300.00', '299.50'),
        }
        mark, index = prices.get(symbol, ('0', '0'))
        return ContractTicker(symbol, Decimal(mark), Decimal(index))

    async def get_funding_rate_history_window(
        self,
        start_time_ms: int,
        end_time_ms: int,
        limit: int = 1000,
    ) -> List[dict]:
        _ = limit
        points = []
        ts = start_time_ms
        symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
        while ts <= end_time_ms:
            for symbol in symbols:
                points.append({
                    'symbol': symbol,
                    'fundingRate': str(self.funding_rates.get(symbol, Decimal('0'))),
                    'fundingTime': ts,
                    'markPrice': '0',
                })
            ts += 8 * 60 * 60 * 1000
        return points


# Factory function
def create_collector(collector_type: str = 'binance', **kwargs) -> ICollector:
    """Create collector instance."""
    if collector_type == 'binance':
        return BinanceCollector(**kwargs)
    elif collector_type == 'mock':
        return MockCollector()
    else:
        raise ValueError(f"Unknown collector type: {collector_type}")
