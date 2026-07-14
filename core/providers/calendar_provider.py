#!/usr/bin/env python3
# core/providers/calendar_provider.py
"""
================================================================================
Calendar Provider — v1.1.0 (F2: YAHOO EARNINGS/EX-DIV FALLBACK)
================================================================================
NEW module (owner greenlight 2026-07-05; Forward-Looking Layer plan, Phase F1).

v1.1.0 (over v1.0.1) — Yahoo fallback fills what EODHD leaves blank (Fix F2):
- EVIDENCE (Calendar_Events tab, 2026-07-13 11:28 run): NMM.US carried an
  EMPTY "Next Earnings Date" from EODHD — and every `.SR` symbol carries
  None by design (the documented KSA gap). A blind calendar row means the
  earnings-proximity context the Forward-Looking layer exists for is
  silently absent exactly where the operator holds positions.
- FIX: after the EODHD pass, symbols still missing an earnings date (and,
  in the same pass, an ex-div date) are retried through yfinance's
  Ticker(...).calendar — one bounded-concurrency thread-pool call per
  missing symbol, wrapped in the module timeout. Yahoo COVERS Saudi
  symbols, so the KSA gap now fills too (the skip notice says "trying
  Yahoo fallback"). EODHD stays PRIMARY: a date EODHD supplied is never
  overwritten. Only FUTURE dates are accepted (earliest upcoming wins).
  Yahoo earnings dates are frequently *estimates* — for the intended
  consumers (Days-To-Earnings context, proximity caution flags) an
  estimated date is honest and useful; nothing predictive hangs on it.
- Return shape UNCHANGED ({symbol: {next_earnings_date, next_ex_div_date}})
  so track_performance v6.14.0 and run_calendar_sync read it untouched;
  fallback fills are disclosed in one INFO line with counts + symbols.
- Import-safe: yfinance absent => fallback no-ops with one INFO line.
- GATE SPLIT: fetch_event_context now gates on the layer flag alone
  (_flag_enabled); a missing EODHD key degrades to Yahoo-only instead of
  returning all-None (each source no-ops independently). Flag OFF is
  byte-identical v1.0.1.
- KILL SWITCH: TFB_CAL_YAHOO_FALLBACK=0/false/off/no restores v1.0.1
  byte-identical behaviour (default ON — the live failure class violates
  it). Concurrency reuses TFB_CALENDAR_CONCURRENCY; per-symbol timeout
  reuses TFB_CALENDAR_TIMEOUT_SEC.
- Zero functions removed; additions: _yahoo_fallback_enabled,
  _yahoo_symbol_for_cal, _yf_calendar_dates, _yahoo_calendar_one,
  _yahoo_calendar_fill.

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
    TFB_CAL_YAHOO_FALLBACK            "0" disables the v1.1.0 Yahoo
                                      earnings/ex-div fallback (default ON)

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

try:  # v1.1.0 (Fix F2): optional — fallback no-ops when absent
    import yfinance as _yf  # type: ignore
except Exception:  # pragma: no cover - environment dependent
    _yf = None  # type: ignore

__version__ = "1.1.0"
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


def _flag_enabled() -> bool:
    """v1.1.0 (Fix F2): the LAYER master flag alone (TFB_CALENDAR_ENABLED).
    is_enabled() below additionally requires the EODHD key — correct for
    the EODHD sub-fetches, but with a Yahoo fallback in the module a
    missing EODHD key must degrade to Yahoo-only, not to nothing. The
    merged fetch_event_context gates on THIS; each source then degrades
    independently (EODHD fetches still no-op without their key)."""
    return _env_str("TFB_CALENDAR_ENABLED", "0").lower() in ("1", "true", "yes", "on")


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
# v1.1.0 (Fix F2) — Yahoo earnings/ex-div fallback for EODHD blanks
# ----------------------------------------------------------------------------- #
def _yahoo_fallback_enabled() -> bool:
    """v1.1.0 (Fix F2): fill EODHD-blank earnings/ex-div dates from Yahoo.
    Default ON; TFB_CAL_YAHOO_FALLBACK=0/false/off/no restores v1.0.1
    byte-identically."""
    return (os.getenv("TFB_CAL_YAHOO_FALLBACK") or "1").strip().lower() in (
        "1", "true", "yes", "on")


def _yahoo_symbol_for_cal(symbol: str) -> str:
    """Yahoo form of a TFB symbol for calendar lookups: `.US` strips to the
    bare ticker; `.SR` and other real exchange suffixes pass through
    (Yahoo uses the same `.SR` form for Tadawul)."""
    s = (symbol or "").strip().upper()
    if s.endswith(".US"):
        return s[:-3]
    return s


def _yf_calendar_dates(ysym: str) -> Tuple[Optional[_dt.date], Optional[_dt.date]]:
    """SYNC worker (runs in a thread): (next_earnings, next_exdiv) from
    yfinance Ticker(...).calendar. Tolerates the dict shape (yfinance
    >=0.2.28: {'Earnings Date': [date, ...], 'Ex-Dividend Date': date})
    and degrades to (None, None) on anything else. Never raises."""
    try:
        cal = _yf.Ticker(ysym).calendar  # type: ignore[union-attr]
    except Exception:
        return None, None
    if not isinstance(cal, dict) or not cal:
        return None, None

    def _coerce(v: Any) -> Optional[_dt.date]:
        if isinstance(v, _dt.datetime):
            return v.date()
        if isinstance(v, _dt.date):
            return v
        return _parse_date(v)

    earn: Optional[_dt.date] = None
    raw_e = cal.get("Earnings Date")
    for item in (raw_e if isinstance(raw_e, (list, tuple)) else [raw_e]):
        d = _coerce(item)
        if d is not None and (earn is None or d < earn):
            earn = d
    exdiv = _coerce(cal.get("Ex-Dividend Date"))
    return earn, exdiv


async def _yahoo_calendar_one(sem: asyncio.Semaphore, orig: str,
                              today: _dt.date,
                              out_earn: Dict[str, str],
                              out_exdiv: Dict[str, str]) -> None:
    """One symbol through the thread-pooled yfinance call, bounded by the
    shared semaphore and the module timeout. Only FUTURE dates land."""
    async with sem:
        try:
            earn, exdiv = await asyncio.wait_for(
                asyncio.to_thread(_yf_calendar_dates,
                                  _yahoo_symbol_for_cal(orig)),
                timeout=float(_env_int("TFB_CALENDAR_TIMEOUT_SEC", 12,
                                       lo=2, hi=120)),
            )
        except Exception as e:
            logger.debug("[calendar_provider] yahoo fallback %s: %s", orig, e)
            return
        if earn is not None and earn >= today:
            out_earn[orig] = _iso(earn)
        if exdiv is not None and exdiv >= today:
            out_exdiv[orig] = _iso(exdiv)


async def _yahoo_calendar_fill(base: Dict[str, Dict[str, Optional[str]]],
                               today: _dt.date) -> Tuple[int, int]:
    """Fill-only pass over `base` for symbols whose earnings date is still
    None (ex-div fills ride along where also None). EODHD values are never
    overwritten. Returns (earnings_filled, exdiv_filled). Never raises."""
    try:
        if not _yahoo_fallback_enabled():
            return 0, 0
        if _yf is None:
            logger.info("[calendar_provider v%s] yahoo fallback unavailable "
                        "(yfinance not importable) — EODHD blanks stay blank",
                        __version__)
            return 0, 0
        missing = [s for s, d in base.items()
                   if d.get("next_earnings_date") is None]
        if not missing:
            return 0, 0
        sem = asyncio.Semaphore(_env_int("TFB_CALENDAR_CONCURRENCY", 4,
                                         lo=1, hi=16))
        f_earn: Dict[str, str] = {}
        f_exdiv: Dict[str, str] = {}
        await asyncio.gather(*[
            _yahoo_calendar_one(sem, s, today, f_earn, f_exdiv)
            for s in missing
        ])
        n_e = n_d = 0
        for s, d in f_earn.items():
            if base.get(s, {}).get("next_earnings_date") is None:
                base[s]["next_earnings_date"] = d
                n_e += 1
        for s, d in f_exdiv.items():
            if base.get(s, {}).get("next_ex_div_date") is None:
                base[s]["next_ex_div_date"] = d
                n_d += 1
        if n_e or n_d:
            logger.info("[calendar_provider v%s F2] yahoo fallback filled "
                        "earnings=%d exdiv=%d of %d blank: %s",
                        __version__, n_e, n_d, len(missing),
                        ",".join(sorted(f_earn)[:20]))
        return n_e, n_d
    except Exception as e:
        logger.error("[calendar_provider v%s] yahoo fallback pass failed: %s",
                     __version__, e)
        return 0, 0


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
    if not syms or not _flag_enabled():
        return base
    if not is_enabled():  # v1.1.0: EODHD unconfigured — Yahoo-only degrade
        logger.info("[calendar_provider v%s] EODHD key missing — "
                    "Yahoo-only degraded mode", __version__)
    try:
        ksa = sorted(s for s in syms if _is_ksa(s))
        if ksa:  # v1.0.1: one notice per context call (sub-fns log at DEBUG)
            logger.info("[calendar_provider v%s] EODHD has no Saudi coverage "
                        "for: %s — trying the v1.1.0 Yahoo fallback",
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
        # v1.1.0 (Fix F2): Yahoo fills what EODHD left blank (fill-only).
        await _yahoo_calendar_fill(base, _today())
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
