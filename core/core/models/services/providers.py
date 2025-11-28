from typing import Protocol, Optional, Dict, Any, List
import aiohttp
import asyncio
import logging
from core.config import settings

logger = logging.getLogger(__name__)

class ProviderClient(Protocol):
    """Provider interface"""
    name: str
    rate_limit: int
    
    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]: ...

class AlphaVantageProvider:
    """Alpha Vantage provider implementation"""
    
    def __init__(self):
        self.name = "alpha_vantage"
        self.rate_limit = 5  # requests per minute
        self.base_url = "https://www.alphavantage.co/query"
        self.api_key = settings.alpha_vantage_api_key
        self.enabled = bool(self.api_key)

    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None

        try:
            async with aiohttp.ClientSession() as session:
                params = {
                    "function": "GLOBAL_QUOTE",
                    "symbol": symbol,
                    "apikey": self.api_key,
                }
                async with session.get(self.base_url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return self._parse_response(data, symbol)
        except Exception as e:
            logger.error(f"Alpha Vantage error for {symbol}: {e}")
            
        return None

    def _parse_response(self, data: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
        """Parse Alpha Vantage response"""
        quote_data = data.get("Global Quote", {})
        if not quote_data:
            return None

        return {
            "ticker": symbol,
            "price": self._safe_float(quote_data.get("05. price")),
            "previous_close": self._safe_float(quote_data.get("08. previous close")),
            "change_value": self._safe_float(quote_data.get("09. change")),
            "change_percent": self._safe_float(str(quote_data.get("10. change percent", "0")).rstrip("%")),
            "volume": self._safe_int(quote_data.get("06. volume")),
            "open_price": self._safe_float(quote_data.get("02. open")),
            "high_price": self._safe_float(quote_data.get("03. high")),
            "low_price": self._safe_float(quote_data.get("04. low")),
            "provider": self.name,
        }

    def _safe_float(self, value: Any) -> Optional[float]:
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        try:
            return int(float(value)) if value else None
        except (ValueError, TypeError):
            return None

class FinnhubProvider:
    """Finnhub provider implementation"""
    
    def __init__(self):
        self.name = "finnhub"
        self.rate_limit = 60
        self.base_url = "https://finnhub.io/api/v1"
        self.api_key = settings.finnhub_api_key
        self.enabled = bool(self.api_key)

    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None

        try:
            async with aiohttp.ClientSession() as session:
                params = {"symbol": symbol, "token": self.api_key}
                async with session.get(f"{self.base_url}/quote", params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return self._parse_response(data, symbol)
        except Exception as e:
            logger.error(f"Finnhub error for {symbol}: {e}")
            
        return None

    def _parse_response(self, data: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
        if not data or "c" not in data:
            return None

        return {
            "ticker": symbol,
            "price": data.get("c"),
            "change_value": data.get("d"),
            "change_percent": data.get("dp"),
            "high_price": data.get("h"),
            "low_price": data.get("l"),
            "open_price": data.get("o"),
            "previous_close": data.get("pc"),
            "provider": self.name,
        }

class ProviderManager:
    """Manager for provider selection and fallback"""
    
    def __init__(self):
        self.providers: List[ProviderClient] = [
            AlphaVantageProvider(),
            FinnhubProvider(),
            # Add other providers here
        ]
        
        self.enabled_providers = [p for p in self.providers if p.enabled]
        logger.info(f"✅ Enabled providers: {[p.name for p in self.enabled_providers]}")

    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get quote from providers with fallback"""
        for provider in self.enabled_providers:
            try:
                quote = await provider.get_quote(symbol)
                if quote and quote.get("price"):
                    logger.info(f"✅ Got quote for {symbol} from {provider.name}")
                    return quote
            except Exception as e:
                logger.warning(f"Provider {provider.name} failed for {symbol}: {e}")
                continue
                
        logger.warning(f"❌ All providers failed for {symbol}")
        return None

    async def get_quotes(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Get multiple quotes in parallel"""
        tasks = [self.get_quote(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        quotes = []
        for result in results:
            if isinstance(result, dict):
                quotes.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Quote fetch error: {result}")
                
        return quotes

# Global provider manager
provider_manager = ProviderManager()
