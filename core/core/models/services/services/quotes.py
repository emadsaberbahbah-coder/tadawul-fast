from typing import List, Optional, Dict, Any
import logging
from datetime import datetime
from models.schemas import Quote, QuotesResponse, QuoteStatus
from core.cache import cache
from services.providers import provider_manager

logger = logging.getLogger(__name__)

class QuoteService:
    """Service layer for quote management"""
    
    def __init__(self, cache, provider_manager):
        self.cache = cache
        self.provider_manager = provider_manager

    async def get_quote(self, symbol: str) -> Quote:
        """Get single quote with cache fallback"""
        # Try cache first
        cached_quote = self.cache.get(symbol)
        if cached_quote:
            logger.debug(f"Cache hit for {symbol}")
            return self._create_quote_model(cached_quote, QuoteStatus.OK)

        # Fetch from provider
        provider_data = await self.provider_manager.get_quote(symbol)
        if provider_data:
            # Cache the result
            self.cache.set(symbol, provider_data)
            return self._create_quote_model(provider_data, QuoteStatus.OK)

        # No data available
        return Quote(
            ticker=symbol,
            status=QuoteStatus.NO_DATA,
            message="No data available from providers"
        )

    async def get_quotes(self, symbols: List[str]) -> QuotesResponse:
        """Get multiple quotes with efficient caching"""
        quotes = []
        cache_hits = 0
        cache_misses = 0
        sources_used = set()

        # Check cache first
        for symbol in symbols:
            cached_quote = self.cache.get(symbol)
            if cached_quote:
                cache_hits += 1
                quotes.append(self._create_quote_model(cached_quote, QuoteStatus.OK))
                if cached_quote.get("provider"):
                    sources_used.add(cached_quote["provider"])
            else:
                cache_misses += 1
                quotes.append(Quote(
                    ticker=symbol,
                    status=QuoteStatus.NO_DATA,
                    message="Not in cache"
                ))

        # Fetch missing quotes from providers
        missing_symbols = [symbol for symbol in symbols 
                          if symbol not in [q.ticker for q in quotes if q.status == QuoteStatus.OK]]
        
        if missing_symbols:
            provider_quotes = await self.provider_manager.get_quotes(missing_symbols)
            
            for provider_quote in provider_quotes:
                if provider_quote and provider_quote.get("price"):
                    symbol = provider_quote["ticker"]
                    # Update cache
                    self.cache.set(symbol, provider_quote)
                    # Update quote in response
                    for i, quote in enumerate(quotes):
                        if quote.ticker == symbol:
                            quotes[i] = self._create_quote_model(provider_quote, QuoteStatus.OK)
                            sources_used.add(provider_quote.get("provider", "unknown"))
                            break

        # Prepare metadata
        meta = {
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "sources": list(sources_used),
            "total_symbols": len(symbols),
            "successful_quotes": len([q for q in quotes if q.status == QuoteStatus.OK])
        }

        return QuotesResponse(
            timestamp=datetime.utcnow(),
            symbols=quotes,
            meta=meta
        )

    def _create_quote_model(self, data: Dict[str, Any], status: QuoteStatus) -> Quote:
        """Create Quote model from provider data"""
        return Quote(
            ticker=data.get("ticker", ""),
            status=status,
            price=data.get("price"),
            previous_close=data.get("previous_close"),
            change_value=data.get("change_value"),
            change_percent=data.get("change_percent"),
            volume=data.get("volume"),
            market_cap=data.get("market_cap"),
            open_price=data.get("open_price"),
            high_price=data.get("high_price"),
            low_price=data.get("low_price"),
            currency=data.get("currency"),
            exchange=data.get("exchange"),
            sector=data.get("sector"),
            country=data.get("country"),
            provider=data.get("provider"),
            as_of=datetime.utcnow(),
        )

    def update_cache(self, quotes: List[Quote]) -> Dict[str, Any]:
        """Update cache with new quotes"""
        updated_count = 0
        errors = []

        for quote in quotes:
            if quote.status == QuoteStatus.OK and quote.price:
                try:
                    quote_data = quote.dict()
                    self.cache.set(quote.ticker, quote_data)
                    updated_count += 1
                except Exception as e:
                    errors.append(f"Failed to update {quote.ticker}: {e}")

        return {
            "updated_count": updated_count,
            "errors": errors,
            "total_cached": len(self.cache.data)
        }

# Global quote service instance
quote_service = QuoteService(cache, provider_manager)
