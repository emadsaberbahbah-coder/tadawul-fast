#!/usr/bin/env python3
# core/providers/calendar_provider.py
"""
================================================================================
Calendar Provider — v1.0.1 (F1: FORWARD-LOOKING EVENT-CALENDAR LAYER)
================================================================================
NEW module (owner greenlight 2026-07-05; Forward-Looking Layer plan, Phase F1).

v1.0.1 (over v1.0.0) — token-safe logging + log dedup:
- The __main__ self-test silences httpx request logging (WARNING+): httpx's
  INFO line prints the full request URL INCLUDING api_token, which surfaced
  the key in the 2026-07-05 Render Shell contract check. The provider never
  logs URLs itself; this closes the one path that did.
- KSA-skip notice logs ONCE per fetch_event_context call (INFO); the
  per-endpoint duplicates moved to DEBUG.

WHAT THIS DOES
    Fetches known FUTURE FACTS — never predictions — from the EODHD Calendar /
    Dividends APIs, scoped to the symbols it is given (intended: decision
    symbols = Top_10 + holdings + Market_Leaders, called OFF the request path):
      * next earnings report date per symbol      -> "next_earnings_date"
      * next ex-dividend date per symbol          -> "next_ex_div_date"
      * upcoming IPO listings (market-wide window) -> fetch_ipos()
    The per-symbol keys feed two consumers built earlier the same day:
      * track_performance.py v6.14.0 Signal_History columns
        "Days To Earnings" / "Days To ExDiv" (computed from these dates), and
      * the Daily Brief / cockpit "UPCOMING EVENTS" surfacing (later wiring).

WHY IT IS SHAPED THIS WAY
    * Facts vs signals: earnings/ex-div dates are scheduled events. Using them
      as risk CONTEXT is honest without any backtest, because no directional
      claim is made. Anything predictive built ON these dates (e.g. H-EARN-01
      pre-earnings reliability haircut) stays hypothesis-gated per the
      registry rules — this module supplies dates, nothing more.
    * DEFAULT OFF (TFB_CALENDAR_ENABLED=1 to enable): with the flag unset every
      public function returns an empty result instantly — zero network calls,
      zero behaviour anywhere. Kill-switch discipline, same as
      TFB_EODHD_FX_MAP / TFB_YAHOO_SHARE_CLASS_DASH precedents.
    * KSA GAP, stated honestly: the EODHD All-in-One plan has NO Saudi/Gulf
      coverage, so `.SR` / 4-digit symbols are never sent to EODHD. They are
      returned with both dates = None (uniform shape for callers) and one
      summary log line names them. Saudi earnings/dividend dates need an
      Argaam/Tadawul path in a later build — this module does not fake them.
    * DEFENSIVE PAYLOAD PARSING: EODHD field names are parsed with fallbacks
      ("code"|"symbol", "report_date"|"date"). The `__main__` self-test below
      is the contract check — run it once from Render Shell after enabling and
      the real payload shape is validated before anything is wired to it.
    * FAIL-SAFE: every public function catches everything and returns an empty
      mapping/list with one logged error. A calendar hiccup must never break a
      sync, the tracker, or the brief.

ENV
    EODHD_API_KEY / EODHD_API_TOKEN / EODHD_KEY   (same chain as eodhd_provider)
    EODHD_BASE_URL                    default https://eodhd.com/api
    TFB_CALENDAR_ENABLED              "1" enables (DEFAULT OFF)
    TFB_CALENDAR_TIMEOUT_SEC          per-request timeout   (default 12)
    TFB_CALENDAR_DAYS_AHEAD           earnings look-ahead   (default 45)
    TFB_CALENDAR_DIV_DAYS_AHEAD       ex-div look-ahead     (default 90)
    TFB_CALENDAR_EARNINGS_CHUNK       symbols per earnings call (default 50)
    TFB_CALENDAR_CONCURRENCY          parallel dividend calls   (default 4)

VALIDATE FROM RENDER SHELL (after setting TFB_CALENDAR_ENABLED=1):
    python3 -m core.providers.calendar_provider AAPL MSFT.US 2222.SR
================================================================================
"""
from __future__ import annotations

import asyncio
import datetime as _dt
import json
import logging
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

import httpx

__version__ = "1.0.1"
PROVIDER_NAME = "calendar"

logger = logging.getLogger("core.providers.calendar_provider")

__all__ = [
    "__version__",
    "PROVIDER_NAME",
    "is_enabled",
    "fetch_event_context",
    "fetch_event_context_sync",
    "fetch_earnings_map",
    "fetch_next_exdiv_map",
    "fetch_ipos",
    "fetch_ipos_sync",
]

# ----------------------------------------------------------------------------- #
# Symbol helpers — prefer the canonical normalizer; degrade safely without it
# ----------------------------------------------------------------------------- #
try:  # pragma: no cover - environment dependent
    from core.symbols.normalize import is_ksa as _is_ksa  # type: ignore
    from core.symbols.normalize import to_eodhd_symbol as _to_eodhd  # type: ignore
except Exception:  # standalone fallback (mirrors eodhd_provider's guard style)
    _KSA_RE = re.compile(r"^\d{4}(\.SR)?$", re.IGNORECASE)

    def _is_ksa(symbol: str) -> bool:  # type: ignore
        s = (symbol or "").strip().upper()
        return s.endswith(".SR") or bool(_KSA_RE.match(s))

    def _to_eodhd(symbol: str, **_: Any) -> str:  # type: ignore
        s = (symbol or "").strip().upper()
        if not s or any(ch in s for ch in "=^/"):
            return s
        return s if "." in s else f"{s}.US"


# ----------------------------------------------------------------------------- #
# Env helpers (house pattern)
# ----------------------------------------------------------------------------- #
def _env_str(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _env_int(name: str, default: int, lo: int = 1, hi: int = 1000) -> int:
    try:
        return max(lo, min(hi, int(_env_str(name, str(default)))))
    except Exception:
        return default


def _api_key() -> str:
    return (
        _env_str("EODHD_API_KEY")
        or _env_str("EODHD_API_TOKEN")
        or _env_str("EODHD_KEY")
    )


def _base_url() -> str:
    return _env_str("EODHD_BASE_URL", "https://eodhd.com/api").rstrip("/")


def is_enabled() -> bool:
    """Gate: TFB_CALENDAR_ENABLED=1 AND an API key present. DEFAULT OFF."""
    if _env_str("TFB_CALENDAR_ENABLED", "0") != "1":
        return False
    if not _api_key():
        logger.warning("[calendar_provider v%s] enabled but EODHD_API_KEY missing "
                       "— returning empty results", __version__)
        return False
    return True


# ----------------------------------------------------------------------------- #
# Small internals
# ----------------------------------------------------------------------------- #
def _today() -> _dt.date:
    return _dt.datetime.now(_dt.timezone.utc).date()


def _iso(d: _dt.date) -> str:
    return d.strftime("%Y-%m-%d")


def _parse_date(raw: Any) -> Optional[_dt.date]:
    s = str(raw or "").strip()[:10]
    if not s:
        return None
    try:
        return _dt.datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def _split_symbols(symbols: List[str]) -> Tuple[Dict[str, str], List[str]]:
    """Return (eodhd_code -> ORIGINAL upper symbol) for non-KSA symbols, plus
    the KSA list (skipped from EODHD by policy — no Saudi coverage)."""
    code_map: Dict[str, str] = {}
    ksa: List[str] = []
    for raw in symbols or []:
        sym = str(raw or "").strip().upper()
        if not sym:
            continue
        if _is_ksa(sym):
            ksa.append(sym)
            continue
        try:
            code = (_to_eodhd(sym) or "").strip().upper()
        except Exception:
            code = ""
        if code:
            code_map.setdefault(code, sym)
    return code_map, ksa


async def _get_json(client: httpx.AsyncClient, path: str,
                    params: Dict[str, Any]) -> Any:
    """One GET, JSON-decoded. Raises on HTTP error; callers wrap it."""
    p = dict(params)
    p["api_token"] = _api_key()
    p.setdefault("fmt", "json")
    r = await client.get(f"{_base_url()}{path}", params=p)
    r.raise_for_status()
    return r.json()


def _client() -> httpx.AsyncClient:
    t = float(_env_int("TFB_CALENDAR_TIMEOUT_SEC", 12, lo=3, hi=60))
    return httpx.AsyncClient(
        timeout=httpx.Timeout(t),
        limits=httpx.Limits(max_keepalive_connections=8, max_connections=16),
        headers={"User-Agent": f"TFB-calendar-provider/{__version__}"},
    )


# ----------------------------------------------------------------------------- #
# Earnings calendar (batched: chunks of symbols per request)
# ----------------------------------------------------------------------------- #
async def fetch_earnings_map(symbols: List[str],
                             days_ahead: Optional[int] = None) -> Dict[str, str]:
    """{ORIGINAL_SYMBOL: 'YYYY-MM-DD' next earnings date}. Empty when disabled,
    on any error, or for symbols with nothing scheduled in the window."""
    if not is_enabled():
        return {}
    try:
        code_map, ksa = _split_symbols(symbols)
        if ksa:
            logger.debug("[calendar_provider v%s] KSA symbols skipped "
                         "(EODHD has no Saudi coverage): %s",
                         __version__, ",".join(sorted(ksa)))
        if not code_map:
            return {}
        today = _today()
        horizon = _iso(today + _dt.timedelta(
            days=days_ahead or _env_int("TFB_CALENDAR_DAYS_AHEAD", 45, hi=365)))
        chunk = _env_int("TFB_CALENDAR_EARNINGS_CHUNK", 50, lo=1, hi=200)
        codes = sorted(code_map)
        out: Dict[str, str] = {}
        async with _client() as client:
            for i in range(0, len(codes), chunk):
                batch = codes[i:i + chunk]
                try:
                    payload = await _get_json(client, "/calendar/earnings", {
                        "from": _iso(today), "to": horizon,
                        "symbols": ",".join(batch),
                    })
                except Exception as e:
                    logger.error("[calendar_provider v%s] earnings batch failed "
                                 "(%d syms): %s", __version__, len(batch), e)
                    continue
                items = payload.get("earnings") if isinstance(payload, dict) else payload
                for it in items or []:
                    if not isinstance(it, dict):
                        continue
                    code = str(it.get("code") or it.get("symbol") or "").strip().upper()
                    d = _parse_date(it.get("report_date") or it.get("date"))
                    orig = code_map.get(code)
                    if not orig or d is None or d < today:
                        continue
                    prev = _parse_date(out.get(orig))
                    if prev is None or d < prev:      # keep the EARLIEST upcoming
                        out[orig] = _iso(d)
        return out
    except Exception as e:  # blanket: a calendar hiccup must never propagate
        logger.error("[calendar_provider v%s] fetch_earnings_map failed: %s",
                     __version__, e)
        return {}


# ----------------------------------------------------------------------------- #
# Next ex-dividend date (per-symbol endpoint; bounded concurrency)
# ----------------------------------------------------------------------------- #
async def fetch_next_exdiv_map(symbols: List[str],
                               days_ahead: Optional[int] = None) -> Dict[str, str]:
    """{ORIGINAL_SYMBOL: 'YYYY-MM-DD' next ex-dividend date}. Only DECLARED
    dividends can appear — an empty entry means none announced in the window,
    which is the honest answer, not a failure."""
    if not is_enabled():
        return {}
    try:
        code_map, ksa = _split_symbols(symbols)
        if ksa:
            logger.debug("[calendar_provider v%s] KSA symbols skipped "
                         "(EODHD has no Saudi coverage): %s",
                         __version__, ",".join(sorted(ksa)))
        if not code_map:
            return {}
        today = _today()
        horizon = _iso(today + _dt.timedelta(
            days=days_ahead or _env_int("TFB_CALENDAR_DIV_DAYS_AHEAD", 90, hi=365)))
        sem = asyncio.Semaphore(_env_int("TFB_CALENDAR_CONCURRENCY", 4, lo=1, hi=16))
        out: Dict[str, str] = {}

        async def _one(client: httpx.AsyncClient, code: str, orig: str) -> None:
            async with sem:
                try:
                    payload = await _get_json(client, f"/div/{code}", {
                        "from": _iso(today), "to": horizon,
                    })
                except Exception as e:
                    logger.debug("[calendar_provider] div %s: %s", code, e)
                    return
                best: Optional[_dt.date] = None
                for it in payload if isinstance(payload, list) else []:
                    if not isinstance(it, dict):
                        continue
                    d = _parse_date(it.get("date"))   # EODHD 'date' = ex-date
                    if d is not None and d >= today and (best is None or d < best):
                        best = d
                if best is not None:
                    out[orig] = _iso(best)

        async with _client() as client:
            await asyncio.gather(*[_one(client, c, o) for c, o in code_map.items()])
        return out
    except Exception as e:
        logger.error("[calendar_provider v%s] fetch_next_exdiv_map failed: %s",
                     __version__, e)
        return {}


# ----------------------------------------------------------------------------- #
# IPO calendar (market-wide window; feeds pillar (c) rotation-candidate work)
# ----------------------------------------------------------------------------- #
async def fetch_ipos(days_ahead: int = 30) -> List[Dict[str, Any]]:
    """Upcoming IPOs in the window, normalized best-effort. List of dicts:
    {code, name, exchange, start_date, currency, price_from, price_to}."""
    if not is_enabled():
        return []
    try:
        today = _today()
        async with _client() as client:
            payload = await _get_json(client, "/calendar/ipos", {
                "from": _iso(today),
                "to": _iso(today + _dt.timedelta(days=max(1, min(days_ahead, 180)))),
            })
        items = payload.get("ipos") if isinstance(payload, dict) else payload
        out: List[Dict[str, Any]] = []
        for it in items or []:
            if not isinstance(it, dict):
                continue
            out.append({
                "code": str(it.get("code") or it.get("symbol") or "").strip().upper(),
                "name": str(it.get("name") or "").strip(),
                "exchange": str(it.get("exchange") or "").strip(),
                "start_date": str(it.get("start_date") or it.get("date") or "").strip()[:10],
                "currency": str(it.get("currency") or "").strip(),
                "price_from": it.get("price_from"),
                "price_to": it.get("price_to"),
            })
        out.sort(key=lambda x: x.get("start_date") or "9999-99-99")
        return out
    except Exception as e:
        logger.error("[calendar_provider v%s] fetch_ipos failed: %s", __version__, e)
        return []


# ----------------------------------------------------------------------------- #
# The one call consumers need: merged per-symbol event context
# ----------------------------------------------------------------------------- #
async def fetch_event_context(symbols: List[str]) -> Dict[str, Dict[str, Optional[str]]]:
    """{SYMBOL: {"next_earnings_date": str|None, "next_ex_div_date": str|None}}
    for EVERY input symbol (uniform shape — KSA symbols carry None/None).
    These keys are exactly what track_performance v6.14.0 reads to compute
    the Signal_History "Days To Earnings" / "Days To ExDiv" columns."""
    syms = [str(s or "").strip().upper() for s in (symbols or []) if str(s or "").strip()]
    base: Dict[str, Dict[str, Optional[str]]] = {
        s: {"next_earnings_date": None, "next_ex_div_date": None} for s in syms
    }
    if not syms or not is_enabled():
        return base
    try:
        ksa = sorted(s for s in syms if _is_ksa(s))
        if ksa:  # v1.0.1: one notice per context call (sub-fns log at DEBUG)
            logger.info("[calendar_provider v%s] KSA symbols carry no calendar "
                        "data (EODHD has no Saudi coverage): %s",
                        __version__, ",".join(ksa))
        earn, exdiv = await asyncio.gather(
            fetch_earnings_map(syms), fetch_next_exdiv_map(syms)
        )
        for s, d in earn.items():
            if s in base:
                base[s]["next_earnings_date"] = d
        for s, d in exdiv.items():
            if s in base:
                base[s]["next_ex_div_date"] = d
        return base
    except Exception as e:
        logger.error("[calendar_provider v%s] fetch_event_context failed: %s",
                     __version__, e)
        return base


# ----------------------------------------------------------------------------- #
# Sync wrappers (for worker/CLI callers outside an event loop)
# ----------------------------------------------------------------------------- #
def _run_sync(coro: Any) -> Any:
    try:
        return asyncio.run(coro)
    except RuntimeError:  # already inside a loop — run on a fresh one
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()


def fetch_event_context_sync(symbols: List[str]) -> Dict[str, Dict[str, Optional[str]]]:
    return _run_sync(fetch_event_context(symbols))


def fetch_ipos_sync(days_ahead: int = 30) -> List[Dict[str, Any]]:
    return _run_sync(fetch_ipos(days_ahead))


# ----------------------------------------------------------------------------- #
# Self-test / contract check:  python3 -m core.providers.calendar_provider AAPL ...
# ----------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    # v1.0.1: httpx logs full request URLs at INFO — including api_token.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    args = [a for a in sys.argv[1:] if a.strip()] or ["AAPL", "MSFT.US", "2222.SR"]
    print(f"calendar_provider v{__version__} | enabled={is_enabled()} "
          f"| base={_base_url()}")
    if not is_enabled():
        print("Set TFB_CALENDAR_ENABLED=1 (and EODHD_API_KEY) to run the live "
              "contract check. Disabled path returned:", 
              json.dumps(fetch_event_context_sync(args)))
        sys.exit(0)
    ctx = fetch_event_context_sync(args)
    print(json.dumps(ctx, indent=2, sort_keys=True))
    ipos = fetch_ipos_sync(30)
    print(f"upcoming IPOs (30d): {len(ipos)}"
          + (f" | first: {ipos[0]}" if ipos else ""))
