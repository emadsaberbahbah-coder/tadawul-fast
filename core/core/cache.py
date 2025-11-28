from typing import Optional, Dict, Any, List, Tuple, Protocol
import time
import json
import datetime
from pathlib import Path
import logging
from core.config import settings

logger = logging.getLogger(__name__)

class QuoteCache(Protocol):
    """Cache interface for quote storage"""
    def get(self, symbol: str) -> Optional[Dict[str, Any]]: ...
    def set(self, symbol: str, quote: Dict[str, Any], ttl: int) -> None: ...
    def cleanup_expired(self) -> int: ...
    def get_stats(self) -> Dict[str, Any]: ...

class TTLCache:
    """Enhanced TTL cache implementation"""
    
    def __init__(self, ttl_seconds: int = None, max_size: int = None):
        self.cache_path = Path(__file__).parent.parent / "quote_cache.json"
        self.backup_dir = Path(__file__).parent.parent / "cache_backups"
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        self.data: Dict[str, Dict[str, Any]] = {}
        self.ttl = ttl_seconds or settings.cache_default_ttl
        self.max_size = max_size or settings.cache_max_size
        
        self.metrics = {
            "hits": 0,
            "misses": 0,
            "updates": 0,
            "expired_removals": 0,
            "errors": 0,
        }
        
        self._load_cache()

    def _is_valid(self, item: Dict[str, Any], now: Optional[float] = None) -> bool:
        """Check if cache item is still valid"""
        if now is None:
            now = time.time()
        timestamp = item.get("_cache_timestamp", 0)
        return (now - timestamp) < self.ttl

    def _load_cache(self) -> bool:
        """Load cache from disk"""
        try:
            if not self.cache_path.exists():
                logger.info("No cache file found, starting with empty cache")
                return True

            with open(self.cache_path, "r", encoding="utf-8") as f:
                raw_data = json.load(f)

            now = time.time()
            valid_items: Dict[str, Dict[str, Any]] = {}
            
            # Handle different cache formats
            if isinstance(raw_data, dict) and "data" in raw_data:
                for item in raw_data["data"]:
                    if isinstance(item, dict) and "ticker" in item and self._is_valid(item, now):
                        valid_items[item["ticker"]] = item
            elif isinstance(raw_data, dict):
                for key, item in raw_data.items():
                    if isinstance(item, dict) and self._is_valid(item, now):
                        valid_items[key] = item

            self.data = valid_items
            logger.info(f"✅ Cache loaded with {len(valid_items)} valid items")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to load cache: {e}")
            self.metrics["errors"] += 1
            return False

    def save_cache(self) -> bool:
        """Save cache to disk atomically"""
        try:
            if len(self.data) > self.max_size:
                self._enforce_size_limit()
                
            cache_data = {
                "metadata": {
                    "version": "1.2",
                    "saved_at": datetime.datetime.utcnow().isoformat() + "Z",
                    "item_count": len(self.data),
                    "ttl_seconds": self.ttl,
                },
                "data": list(self.data.values())
            }
            
            temp_path = self.cache_path.with_suffix(".tmp")
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False, default=str)
            
            temp_path.replace(self.cache_path)
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to save cache: {e}")
            self.metrics["errors"] += 1
            return False

    def _enforce_size_limit(self):
        """Enforce cache size limit"""
        if len(self.data) <= self.max_size:
            return
            
        items_to_remove = len(self.data) - self.max_size
        sorted_items = sorted(
            self.data.items(), 
            key=lambda x: x[1].get("_cache_timestamp", 0)
        )
        
        for ticker, _ in sorted_items[:items_to_remove]:
            del self.data[ticker]
            self.metrics["expired_removals"] += 1

    def get(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get quote from cache"""
        symbol = symbol.upper().strip()
        
        if symbol in self.data and self._is_valid(self.data[symbol]):
            self.metrics["hits"] += 1
            # Remove cache metadata before returning
            clean_data = {k: v for k, v in self.data[symbol].items() 
                         if not k.startswith("_")}
            return clean_data
        
        # Remove expired entry
        if symbol in self.data:
            del self.data[symbol]
            self.save_cache()
            
        self.metrics["misses"] += 1
        return None

    def set(self, symbol: str, quote: Dict[str, Any], ttl: int = None) -> None:
        """Set quote in cache"""
        symbol = symbol.upper().strip()
        quote_data = quote.copy()
        quote_data["_cache_timestamp"] = time.time()
        quote_data["_last_updated"] = datetime.datetime.utcnow().isoformat() + "Z"
        
        self.data[symbol] = quote_data
        self.metrics["updates"] += 1
        self.save_cache()

    def cleanup_expired(self) -> int:
        """Clean up expired entries"""
        now = time.time()
        expired_tickers = [
            ticker for ticker, item in self.data.items() 
            if not self._is_valid(item, now)
        ]
        
        for ticker in expired_tickers:
            del self.data[ticker]
            self.metrics["expired_removals"] += 1
            
        if expired_tickers:
            self.save_cache()
            logger.info(f"🧹 Removed {len(expired_tickers)} expired cache entries")
            
        return len(expired_tickers)

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        now = time.time()
        valid_count = sum(1 for item in self.data.values() if self._is_valid(item, now))
        expired_count = len(self.data) - valid_count
        
        total_requests = self.metrics["hits"] + self.metrics["misses"]
        hit_rate = (self.metrics["hits"] / total_requests * 100) if total_requests > 0 else 0
        
        return {
            "total_items": len(self.data),
            "valid_items": valid_count,
            "expired_items": expired_count,
            "max_size": self.max_size,
            "ttl_seconds": self.ttl,
            "performance": {
                "hits": self.metrics["hits"],
                "misses": self.metrics["misses"],
                "hit_rate": round(hit_rate, 2),
                "updates": self.metrics["updates"],
                "expired_removals": self.metrics["expired_removals"],
                "errors": self.metrics["errors"],
            }
        }

# Global cache instance
cache = TTLCache()
