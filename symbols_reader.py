# symbols_reader.py
# Enhanced Google Sheets symbol reader with robust error handling and caching
# Aligned with:
#  - main.py (GOOGLE_SHEETS_CREDENTIALS JSON, SPREADSHEET_ID)
#  - Render env (SHEETS_SPREADSHEET_ID, SHEETS_TAB_SYMBOLS, SYMBOLS_CACHE_DIR)
#  - Market_Leaders page (Ticker, Company Name, Sector, Trading Market)

from __future__ import annotations

import os
import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SymbolRecord:
    """Data class for symbol records with validation."""
    symbol: str
    company_name: str
    trading_sector: str
    financial_market: str
    include_in_ranking: bool = True
    market_cap: Optional[float] = None
    last_updated: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "symbol": self.symbol,
            "company_name": self.company_name,
            "trading_sector": self.trading_sector,
            "financial_market": self.financial_market,
            "include_in_ranking": self.include_in_ranking,
            "market_cap": self.market_cap,
            "last_updated": self.last_updated or datetime.utcnow().isoformat() + "Z",
        }


class SymbolsReader:
    """
    Enhanced Google Sheets symbol reader with:
      - Shared env config with main.py
      - Service account JSON from GOOGLE_SHEETS_CREDENTIALS
      - Fallback to GOOGLE_APPLICATION_CREDENTIALS file
      - Local JSON cache (TTL)
    """

    def __init__(self) -> None:
        # Spreadsheet ID: prefer SPREADSHEET_ID (same as main.py)
        self.sheet_id: str = (
            os.getenv("SPREADSHEET_ID")
            or os.getenv("SHEETS_SPREADSHEET_ID")
            or os.getenv("GOOGLE_SHEETS_ID", "")
        )

        if not self.sheet_id:
            logger.warning(
                "No SPREADSHEET_ID / SHEETS_SPREADSHEET_ID / GOOGLE_SHEETS_ID configured. "
                "SymbolsReader will fall back to hardcoded symbols."
            )

        # Tab name for symbols (Market_Leaders)
        self.tab_name: str = os.getenv("SHEETS_TAB_SYMBOLS", "Market_Leaders")

        # Header row index (1-based). Default 2 (group row + header row in sheets)
        try:
            self.header_row: int = int(os.getenv("SHEETS_HEADER_ROW", "2"))
        except ValueError:
            self.header_row = 2

        # Cache configuration (directory + TTL)
        cache_dir_env = os.getenv("SYMBOLS_CACHE_DIR", "./symbols_cache")
        self.cache_dir = Path(cache_dir_env)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # TTL: env SYMBOLS_CACHE_TTL (seconds) -> timedelta
        ttl_seconds = int(os.getenv("SYMBOLS_CACHE_TTL", "1800"))  # default 30 min
        self.cache_ttl = timedelta(seconds=max(60, ttl_seconds))

        # Column mapping with flexible field detection
        self.column_mapping = self._detect_column_mapping()

        # Google Sheets client / worksheet
        self._client = None
        self._worksheet = None

        # Request throttling
        self.last_request_time = 0.0
        self.min_request_interval = 1.0  # seconds between gspread calls

        logger.info(
            f"SymbolsReader initialized: sheet_id={self.sheet_id}, "
            f"tab_name={self.tab_name}, header_row={self.header_row}, "
            f"cache_dir={self.cache_dir}, cache_ttl={self.cache_ttl}"
        )

    # -------------------------------------------------------------------------
    # Throttling
    # -------------------------------------------------------------------------
    def _throttle_requests(self) -> None:
        """Basic throttle to avoid rate limits."""
        now = time.time()
        elapsed = now - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        self.last_request_time = time.time()

    # -------------------------------------------------------------------------
    # Column mapping
    # -------------------------------------------------------------------------
    def _detect_column_mapping(self) -> Dict[str, Any]:
        """Define flexible column mapping for common naming patterns."""
        return {
            "symbol": ["Symbol", "Ticker", "Ticker Symbol", "Code"],
            "company_name": ["Company Name", "Company", "Name", "Security Name"],
            "trading_sector": ["Sector", "Trading Sector", "Industry", "Industry Sector"],
            "financial_market": ["Market", "Financial Market", "Exchange", "Trading Market"],
            "include_in_ranking": ["Include", "Include in Ranking", "Active", "Enabled"],
            "market_cap": ["Market Cap", "Market Capitalization", "Capitalization"],
        }

    # -------------------------------------------------------------------------
    # Cache helpers
    # -------------------------------------------------------------------------
    def _get_cache_key(self) -> str:
        """Cache file name based on sheet and tab configuration."""
        base = f"symbols_{self.sheet_id}_{self.tab_name}_{self.header_row}"
        return base.replace("/", "_").replace("\\", "_") + ".json"

    def _load_from_cache(self) -> Optional[List[Dict[str, Any]]]:
        """Load symbols from cache if TTL not expired."""
        try:
            cache_file = self.cache_dir / self._get_cache_key()
            if not cache_file.exists():
                return None

            raw = cache_file.read_text(encoding="utf-8")
            payload = json.loads(raw)

            ts = payload.get("timestamp")
            if not ts:
                return None

            cache_time = datetime.fromisoformat(ts)
            if datetime.now() - cache_time > self.cache_ttl:
                logger.info("Symbols cache expired")
                return None

            data = payload.get("data", [])
            logger.info(f"Loaded {len(data)} symbols from cache ({cache_file})")
            return data
        except Exception as e:
            logger.debug(f"Cache load failed: {e}")
            return None

    def _save_to_cache(self, data: List[Dict[str, Any]]) -> None:
        """Save symbols to cache."""
        try:
            cache_file = self.cache_dir / self._get_cache_key()
            payload = {
                "timestamp": datetime.now().isoformat(),
                "sheet_id": self.sheet_id,
                "tab_name": self.tab_name,
                "data": data,
            }
            cache_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            logger.info(f"Saved {len(data)} symbols to cache ({cache_file})")
        except Exception as e:
            logger.debug(f"Cache save failed: {e}")

    # -------------------------------------------------------------------------
    # Google Sheets client / worksheet
    # -------------------------------------------------------------------------
    def _get_google_client(self):
        """Get or create Google Sheets client using service account JSON."""
        try:
            import gspread
            from google.oauth2.service_account import Credentials
        except ImportError:
            logger.error("Google Sheets dependencies not available (gspread/google-auth)")
            return None

        if self._client is not None:
            return self._client

        # 1) Prefer JSON from env: GOOGLE_SHEETS_CREDENTIALS (same as main.py)
        creds_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")

        try:
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets.readonly",
                "https://www.googleapis.com/auth/drive.readonly",
            ]

            if creds_json:
                logger.info("Initializing Google client from GOOGLE_SHEETS_CREDENTIALS env")
                creds_dict = json.loads(creds_json)
                creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            else:
                # 2) Fallback to file path GOOGLE_APPLICATION_CREDENTIALS
                cred_file = os.getenv(
                    "GOOGLE_APPLICATION_CREDENTIALS",
                    "gcp-credentials-saudi-stocks.json",
                )
                if not os.path.exists(cred_file):
                    logger.error(
                        f"No GOOGLE_SHEETS_CREDENTIALS env and credentials file not found: {cred_file}"
                    )
                    return None

                logger.info(f"Initializing Google client from file: {cred_file}")
                creds = Credentials.from_service_account_file(cred_file, scopes=scopes)

            self._client = gspread.authorize(creds)
            logger.info("Google Sheets client initialized successfully (SymbolsReader)")
            return self._client

        except json.JSONDecodeError as e:
            logger.error(f"Invalid GOOGLE_SHEETS_CREDENTIALS JSON: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets client: {e}")
            return None

    def _get_worksheet(self):
        """Get the specific worksheet (Market_Leaders) with fallback."""
        if self._worksheet is not None:
            return self._worksheet

        if not self.sheet_id:
            logger.error("sheet_id not configured; cannot open spreadsheet")
            return None

        client = self._get_google_client()
        if client is None:
            return None

        try:
            from gspread.exceptions import WorksheetNotFound, SpreadsheetNotFound

            spreadsheet = client.open_by_key(self.sheet_id)
            logger.info(f"Opened spreadsheet: {spreadsheet.title}")

            try:
                self._worksheet = spreadsheet.worksheet(self.tab_name)
                logger.info(f"Using worksheet: {self.tab_name}")
            except WorksheetNotFound:
                worksheets = spreadsheet.worksheets()
                if worksheets:
                    self._worksheet = worksheets[0]
                    logger.warning(
                        f"Worksheet '{self.tab_name}' not found, using first sheet: "
                        f"{self._worksheet.title}"
                    )
                else:
                    logger.error("No worksheets found in spreadsheet")
                    return None

            return self._worksheet

        except SpreadsheetNotFound:
            logger.error(f"Spreadsheet not found with ID: {self.sheet_id}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error accessing worksheet: {e}")
            return None

    # -------------------------------------------------------------------------
    # Fallback hardcoded symbols
    # -------------------------------------------------------------------------
    def _get_hardcoded_symbols(self) -> List[Dict[str, Any]]:
        """
        Fallback symbols when Google Sheets is not available.
        These are just placeholders so the system can still operate.
        """
        return [
            {
                "symbol": "7201.SR",
                "company_name": "Saudi Company 7201",
                "trading_sector": "Materials",
                "financial_market": "Nomu",
                "include_in_ranking": True,
            },
            {
                "symbol": "9603.SR",
                "company_name": "Saudi Company 9603",
                "trading_sector": "Telecommunications",
                "financial_market": "Tadawul (TASI)",
                "include_in_ranking": True,
            },
            {
                "symbol": "9609.SR",
                "company_name": "Saudi Company 9609",
                "trading_sector": "Banks",
                "financial_market": "Tadawul (TASI)",
                "include_in_ranking": True,
            },
            {
                "symbol": "6070.SR",
                "company_name": "Saudi Company 6070",
                "trading_sector": "Transportation",
                "financial_market": "Nomu",
                "include_in_ranking": True,
            },
            {
                "symbol": "7200.SR",
                "company_name": "Saudi Company 7200",
                "trading_sector": "Petrochemicals",
                "financial_market": "Tadawul (TASI)",
                "include_in_ranking": True,
            },
        ]

    # -------------------------------------------------------------------------
    # Header / row parsing
    # -------------------------------------------------------------------------
    def _map_columns(self, headers: List[str]) -> Dict[str, int]:
        """Map header names to indices based on flexible matching."""
        column_map: Dict[str, int] = {}

        for field, possible_names in self.column_mapping.items():
            for idx, header in enumerate(headers):
                header_clean = str(header).strip().lower()
                for name in possible_names:
                    if name.lower() in header_clean:
                        column_map[field] = idx
                        logger.debug(f"Mapped '{field}' -> column {idx} ('{header}')")
                        break
                if field in column_map:
                    break

        if "symbol" not in column_map:
            column_map["symbol"] = 0
            logger.warning("Symbol column not found; using first column as symbol")

        return column_map

    def _parse_row(self, row: List[str], column_map: Dict[str, int]) -> Optional[SymbolRecord]:
        """Parse a single row into a SymbolRecord."""
        try:
            # Symbol (required)
            sym_idx = column_map.get("symbol", 0)
            symbol = str(row[sym_idx]).strip() if len(row) > sym_idx else ""
            if not symbol:
                return None

            # Company name
            company_idx = column_map.get("company_name", 1)
            company_name = (
                str(row[company_idx]).strip() if len(row) > company_idx else ""
            )

            # Trading sector
            sector_idx = column_map.get("trading_sector", 2)
            trading_sector = (
                str(row[sector_idx]).strip() if len(row) > sector_idx else ""
            )

            # Financial market
            market_idx = column_map.get("financial_market", 3)
            financial_market = (
                str(row[market_idx]).strip() if len(row) > market_idx else ""
            )

            # Include flag
            include_flag = True
            if "include_in_ranking" in column_map:
                inc_idx = column_map["include_in_ranking"]
                inc_val = str(row[inc_idx]).strip() if len(row) > inc_idx else ""
                include_flag = self._parse_boolean(inc_val)

            # Market cap
            market_cap = None
            if "market_cap" in column_map:
                cap_idx = column_map["market_cap"]
                cap_val = str(row[cap_idx]).strip() if len(row) > cap_idx else ""
                market_cap = self._parse_float(cap_val)

            return SymbolRecord(
                symbol=symbol,
                company_name=company_name,
                trading_sector=trading_sector,
                financial_market=financial_market,
                include_in_ranking=include_flag,
                market_cap=market_cap,
                last_updated=datetime.utcnow().isoformat() + "Z",
            )
        except Exception as e:
            logger.warning(f"Failed to parse row: {row} - {e}")
            return None

    def _parse_boolean(self, value: str) -> bool:
        """Parse boolean-ish values from string."""
        if not value:
            return True

        true_values = ["true", "yes", "y", "1", "include", "enabled", "active"]
        false_values = ["false", "no", "n", "0", "exclude", "disabled", "inactive"]

        v = value.lower().strip()
        if v in true_values:
            return True
        if v in false_values:
            return False
        return True

    def _parse_float(self, value: str) -> Optional[float]:
        """Parse float values from string with SAR / commas etc."""
        try:
            cleaned = (
                value.replace(",", "")
                .replace(" ", "")
                .replace("SAR", "")
                .replace("ر.س", "")
                .strip()
            )
            if cleaned:
                return float(cleaned)
        except Exception:
            pass
        return None

    # -------------------------------------------------------------------------
    # Public: fetch symbols
    # -------------------------------------------------------------------------
    def fetch_symbols(self, limit: Optional[int] = None, use_cache: bool = True) -> dict:
        """
        Fetch symbols from Google Sheets with caching and robust fallbacks.

        Returns standardized dict:
        {
          "ok": bool,
          "source": "google_sheets" | "cache" | "hardcoded_fallback",
          "sheet_title": "...",
          "worksheet": "Market_Leaders",
          "count": N,
          "data": [ {symbol, company_name, ...}, ... ],
          "timestamp": "...Z",
          "metadata": { ... }
        }
        """
        # Try cache first
        if use_cache:
            cached = self._load_from_cache()
            if cached is not None:
                if limit:
                    cached = cached[:limit]
                return self._build_response(
                    cached,
                    source="cache",
                    column_map=None,
                    error=None,
                )

        try:
            self._throttle_requests()
            ws = self._get_worksheet()
            if ws is None:
                logger.warning("Worksheet unavailable; using hardcoded symbols fallback")
                hard = self._get_hardcoded_symbols()
                if limit:
                    hard = hard[:limit]
                return self._build_response(hard, source="hardcoded_fallback")

            all_values = ws.get_all_values()
            if not all_values or len(all_values) < self.header_row:
                logger.warning("No data or insufficient header rows in worksheet")
                hard = self._get_hardcoded_symbols()
                if limit:
                    hard = hard[:limit]
                return self._build_response(hard, source="hardcoded_fallback")

            headers = all_values[self.header_row - 1]
            data_rows = all_values[self.header_row:]

            column_map = self._map_columns(headers)

            symbols: List[Dict[str, Any]] = []
            for row in data_rows:
                if not any(str(c).strip() for c in row):
                    continue

                rec = self._parse_row(row, column_map)
                if rec and rec.include_in_ranking:
                    symbols.append(rec.to_dict())

                if limit and len(symbols) >= limit:
                    break

            self._save_to_cache(symbols)

            return self._build_response(
                symbols,
                source="google_sheets",
                column_map=column_map,
                error=None,
            )

        except FileNotFoundError as e:
            logger.error(f"Credentials file not found: {e}")
            hard = self._get_hardcoded_symbols()
            if limit:
                hard = hard[:limit]
            return self._build_response(hard, source="hardcoded_fallback", error=str(e))
        except Exception as e:
            logger.error(f"Unexpected error fetching symbols: {e}")
            hard = self._get_hardcoded_symbols()
            if limit:
                hard = hard[:limit]
            return self._build_response(hard, source="hardcoded_fallback", error=str(e))

    # -------------------------------------------------------------------------
    # Response builder / cache utilities
    # -------------------------------------------------------------------------
    def _build_response(
        self,
        data: List[Dict[str, Any]],
        source: str = "unknown",
        column_map: Optional[Dict[str, int]] = None,
        error: Optional[str] = None,
    ) -> dict:
        """Build standardized response dictionary."""
        resp: Dict[str, Any] = {
            "ok": error is None,
            "source": source,
            "sheet_title": getattr(self._worksheet, "title", "Unknown")
            if self._worksheet
            else "Unknown",
            "worksheet": self.tab_name,
            "count": len(data),
            "data": data,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "metadata": {
                "sheet_id": self.sheet_id,
                "header_row": self.header_row,
                "cache_used": source == "cache",
            },
        }

        if column_map:
            resp["metadata"]["column_mapping"] = column_map

        if error:
            resp["error"] = error
            resp["ok"] = False

        return resp

    def clear_cache(self) -> dict:
        """Clear the symbols cache file for this configuration."""
        try:
            cache_file = self.cache_dir / self._get_cache_key()
            if cache_file.exists():
                cache_file.unlink()
                logger.info(f"Symbols cache cleared ({cache_file})")
                return {"status": "cleared", "cache_file": str(cache_file)}
            return {"status": "no_cache", "cache_file": str(cache_file)}
        except Exception as e:
            logger.error(f"Failed to clear symbols cache: {e}")
            return {"status": "error", "error": str(e)}

    def get_cache_info(self) -> dict:
        """Get info about the current cache file."""
        try:
            cache_file = self.cache_dir / self._get_cache_key()
            if not cache_file.exists():
                return {"exists": False}

            payload = json.loads(cache_file.read_text(encoding="utf-8"))
            ts = payload.get("timestamp", "2000-01-01T00:00:00")
            cache_time = datetime.fromisoformat(ts)
            age = datetime.now() - cache_time
            return {
                "exists": True,
                "age_seconds": age.total_seconds(),
                "age_minutes": round(age.total_seconds() / 60, 1),
                "symbol_count": len(payload.get("data", [])),
                "last_updated": ts,
                "cache_file": str(cache_file),
            }
        except Exception as e:
            logger.error(f"Failed to read cache info: {e}")
            return {"exists": False, "error": str(e)}


# Global instance (used by main.py via safe_import)
symbols_reader = SymbolsReader()


def fetch_symbols(limit: Optional[int] = None) -> dict:
    """
    Backward-compatible function used by main.py:
      - main.py calls: sr.fetch_symbols(limit)
    """
    return symbols_reader.fetch_symbols(limit=limit)
