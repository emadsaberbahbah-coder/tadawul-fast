#!/usr/bin/env python3
# core/providers/calendar_provider.py
"""
================================================================================
Calendar Provider — v1.3.2 (VENUE BATCHES + EODHD EXCHANGE CODES)
================================================================================
NEW module (owner greenlight 2026-07-05; Forward-Looking Layer plan, Phase F1).

v1.3.2 (2026-10-27) — US codes can no longer be starved by foreign codes
EODHD rejects. WHY: the first run with the v1.3.1 split armed (calendar_sync
run 40498374271, 27 Oct 14:45 Riyadh) raised the last-report rows from 3 to
34 of 327, but its step log reads "404 batches split 99 time(s), 200
request(s); EODHD rejected 33 code(s): ...; 42 code(s) dropped at the
request cap 200". The codes are batched in sorted order, so every batch
mixed venues; the 33 rejected codes (13 '.T' Tokyo, 12 '.L' London, 2 '.KL'
Bursa Malaysia, 'IG.MI', and five 'X.IS.US' / 'X.LS.US' codes the shared
normalizer builds for Istanbul and Lisbon) plus 'T82U.SI' spent the whole
budget on splits, and the 42 codes left were never requested - 27 of them
US names from TAK.US to ZTO.US (TSM, TDY, VICI, WTFC, UMBF ...). An offline
replay of the real 327-symbol universe reproduces the line exactly with
'T82U.SI' as the one further unknown code. WHAT (each behind its own gate,
default off = the v1.3.1 requests, results and log lines byte for byte):
- TFB_CALENDAR_VENUE_BATCHES: the earnings batches are built per venue (the
  code's exchange suffix; a code with more than one dot is its own venue),
  US first, so a 404 split never touches a US batch and the request cap can
  only ever fall on foreign codes.
- TFB_CALENDAR_SUFFIX_MAP: the earnings calendar is asked with the exchange
  codes EODHD uses for the venues it rejected: '.L' -> '.LSE' (EODHD
  exchanges API), '.KL' -> '.KLSE', 'X.IS.US' -> 'X.IS', 'X.LS.US' -> 'X.LS';
  answers map back to the ORIGINAL symbol (an answer under the untranslated
  code is accepted too). '.T', '.SI' and 'IG.MI' are left as they are - their
  EODHD codes are not established, so they keep being named in the WARNING
  line, which now also says what was translated. The shared to_eodhd_symbol
  (prices, fundamentals) and the per-symbol ex-dividend path are unchanged.
Additions (0 removed): _venue_batches_enabled, _suffix_map_enabled,
_calendar_code, _earnings_codes, _venue_of, _earnings_batches.

v1.3.1 (2026-10-27) — a 404 batch no longer loses every symbol in it. WHY:
the first armed run of the last-report columns (calendar_sync run
40489000970, 27 Oct 13:30 Riyadh) filled 3 of 327 rows: five of the
last-earnings batches (50 codes each) and the forward batch of the 31
decision symbols answered HTTP 404 for the WHOLE request, so every code in
them was dropped. The forward batch failed the same way on 26 Oct (v1.2.2)
-- the board names' next-earnings dates have been coming from Yahoo
estimates and the sync's prior-row carry instead of EODHD. EODHD answers
404 for the whole /calendar/earnings request when one code in `symbols` is
one it does not know (the failing batches mix venues whose suffixes EODHD
spells differently, e.g. '.L', '.T', '.KL', and a malformed 'AEFES.IS.US').
WHAT (only when TFB_CALENDAR_SPLIT_404 is on/1/true/yes; default off = the
v1.3.0 requests and results byte for byte): a batch that answers 404 is
split in halves and each half retried, recursively, so every code EODHD
accepts is kept; a single code that answers 404 is recorded as rejected;
any other error propagates to the caller's v1.3.0 handling (logged, batch
skipped). Requests per call are capped by TFB_CALENDAR_SPLIT_MAX_CALLS
(default 200, 1..1000), checked before every request: past the cap nothing
more is requested - pending halves and later batches - and their codes are
counted (review fix, Codex P2 on PR #853). One
WARNING line per call names the rejected codes (first 40) and the counts,
so the symbol mapping can be corrected from evidence. Applies to both
batched earnings paths: fetch_earnings_map (forward) and
fetch_last_earnings_map (last report). The ex-dividend path (per symbol)
and the Yahoo fallbacks are unchanged. Additions (0 removed):
_split_404_enabled, _is_http_404, _split_state, _earnings_items_split,
_earnings_split_payload, _log_split_state.

v1.3.0 (2026-10-27) — the most recent PAST earnings report (program register
A4, post-earnings quarantine data lane). WHY: the first funded ticket (FRST.US,
26 Oct 2026) was bought two sessions after a 22% EPS miss (Q3 reported 22 Oct
after the close; -4.1% and -3.7% on heavy volume the next sessions). Every
earnings path in this module looks FORWARD only (d >= today), so the system
could not know a name had just reported, let alone missed. ADDED (pure
addition, nothing existing changes): fetch_last_earnings_map() /
fetch_last_earnings_sync() — one batched EODHD /calendar/earnings call per
TFB_CALENDAR_EARNINGS_CHUNK symbols over [today - TFB_CALENDAR_DAYS_BACK
(default 21, 1..120), today - 1]; per symbol the LATEST report date in that
window plus its EPS surprise % (the payload's 'percent' when numeric, else
(actual - estimate) / |estimate| x 100 when both are numeric and the estimate
is not 0), actual, estimate and before/after-market timing. A same-day report
stays the forward path's business (next earnings = today). KSA symbols are
skipped exactly like the forward path (EODHD has no Saudi coverage); no Yahoo
fallback for past reports in this version. fetch_event_context is untouched,
so track_performance and the v1.1.x calendar sync read byte-identical data.
Consumer: scripts/run_calendar_sync.py v1.2.0 behind TFB_CAL_LAST_REPORT.
Zero functions removed; additions: fetch_last_earnings_map,
fetch_last_earnings_sync, _num.

v1.2.2 — retry Yahoo when either earnings or ex-dividend is missing.
Each event fills independently; existing EODHD dates are never overwritten.
Disabled or unavailable Yahoo fallback remains a no-op.

v1.2.1 — redact credentials from exception diagnostics before logging.
Calendar requests, fallback behavior, and returned event maps are unchanged.

v1.2.0 (over v1.1.0) — get_earnings_dates becomes the earnings tier (Fix F2b):
- EVIDENCE (Render shell, 2026-07-14 ~10:40 UTC, verbatim): Ticker.calendar
  raised YFRateLimitError for BOTH NMM and 2222.SR — the quoteSummary
  endpoint is throttled from the datacenter IP — while, in the SAME
  session, Ticker.get_earnings_dates(limit=8) delivered NMM 2026-08-20
  (cross-confirming EODHD) and 2222.SR 2026-08-04 (Aramco Q2 — the first
  KSA earnings date the platform has ever held). Two defects in v1.1.0:
  (a) it bet earnings on the one Yahoo endpoint that rate-limits here,
  (b) its broad except swallowed the throttle as (None, None), reporting
  "no data" when the truth was "blocked".
- FIX F2b: NEW _yf_earnings_dates_future() — earliest FUTURE date from
  get_earnings_dates (limit 12) — is tried FIRST for earnings;
  _yf_calendar_dates (unchanged) remains for ex-div and as the earnings
  backup. Per-symbol exceptions are now CLASSIFIED: anything whose type
  name contains "RateLimit" increments a throttle counter that the fill
  INFO line discloses ("throttled=N") — a blocked run can never again
  masquerade as an empty one. Both tiers keep the future-only /
  earliest-wins / EODHD-never-overwritten rules.
- Zero functions removed; addition: _yf_earnings_dates_future.
  _yahoo_calendar_one gains the throttle counter; _yf_calendar_dates'
  internal except is NARROWED (rate-limit re-raises, all else still
  degrades to (None, None)) so BOTH tiers classify throttling.

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
    TFB_CALENDAR_DAYS_BACK            v1.3.0 last-report look-back (default 21)
    TFB_CALENDAR_DIV_DAYS_AHEAD       ex-div look-ahead     (default 90)
    TFB_CALENDAR_EARNINGS_CHUNK       symbols per earnings call (default 50)
    TFB_CALENDAR_SPLIT_404            v1.3.1 "on" splits a 404 batch to keep
                                      the codes EODHD accepts (default off)
    TFB_CALENDAR_SPLIT_MAX_CALLS      v1.3.1 request cap per call (default 200)
    TFB_CALENDAR_VENUE_BATCHES        v1.3.2 "on" batches the earnings codes
                                      per venue, US first (default off)
    TFB_CALENDAR_SUFFIX_MAP           v1.3.2 "on" asks the earnings calendar
                                      with EODHD's exchange codes for '.L',
                                      '.KL', '.IS', '.LS' (default off)
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

from core.secret_redaction import safe_error_text

try:  # v1.1.0 (Fix F2): optional — fallback no-ops when absent
    import yfinance as _yf  # type: ignore
except Exception:  # pragma: no cover - environment dependent
    _yf = None  # type: ignore

__version__ = "1.3.2"
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
    "fetch_last_earnings_map",
    "fetch_last_earnings_sync",
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
# v1.3.1 - a 404 batch is split so the codes EODHD accepts are kept
# ----------------------------------------------------------------------------- #
def _split_404_enabled() -> bool:
    """TFB_CALENDAR_SPLIT_404 = on|1|true|yes (any case). Default off."""
    return _env_str("TFB_CALENDAR_SPLIT_404").lower() in ("on", "1", "true", "yes")


def _is_http_404(exc: BaseException) -> bool:
    """True for an HTTP 404 answer (httpx.HTTPStatusError or any exception
    carrying a response whose status_code is 404)."""
    resp = getattr(exc, "response", None)
    return getattr(resp, "status_code", None) == 404


def _split_state() -> Dict[str, Any]:
    return {"calls": 0, "rejected": [], "capped": 0, "split": 0,
            "cap": _env_int("TFB_CALENDAR_SPLIT_MAX_CALLS", 200, lo=1, hi=1000)}


async def _earnings_items_split(client: httpx.AsyncClient, base: Dict[str, Any],
                                batch: List[str], state: Dict[str, Any]) -> List[Any]:
    """The /calendar/earnings items for `batch` (params `base` + symbols). An
    HTTP 404 on more than one code splits the batch in halves (EODHD answers
    404 for the WHOLE request when it does not know one code); a single code
    that answers 404 is recorded in state['rejected']. Any other error
    propagates. The budget is checked BEFORE every request (review fix, Codex
    P2 on PR #853): once state['cap'] requests were made in this call, no
    further request is issued - pending halves and later batches alike - and
    their codes are counted in state['capped']."""
    if state["calls"] >= state["cap"]:
        state["capped"] += len(batch)
        return []
    state["calls"] += 1
    params = dict(base)
    params["symbols"] = ",".join(batch)
    try:
        payload = await _get_json(client, "/calendar/earnings", params)
    except Exception as e:
        if not _is_http_404(e):
            raise
        if len(batch) == 1:
            state["rejected"].append(batch[0])
            return []
        state["split"] += 1
        mid = len(batch) // 2
        left = await _earnings_items_split(client, base, batch[:mid], state)
        right = await _earnings_items_split(client, base, batch[mid:], state)
        return left + right
    items = payload.get("earnings") if isinstance(payload, dict) else payload
    return list(items or [])


async def _earnings_split_payload(client: httpx.AsyncClient, base: Dict[str, Any],
                                  batch: List[str], state: Dict[str, Any]) -> Dict[str, Any]:
    """_earnings_items_split shaped like the endpoint's own payload, so the
    callers' item loop is the v1.3.0 code."""
    return {"earnings": await _earnings_items_split(client, base, batch, state)}


def _log_split_state(label: str, state: Dict[str, Any]) -> None:
    """One WARNING line per call when a 404 split happened (no line when the
    gate is off or nothing answered 404)."""
    if not state["split"] and not state["rejected"] and not state["capped"]:
        return
    rej = state["rejected"]
    logger.warning("[calendar_provider v%s] %s: 404 batches split %d time(s), "
                   "%d request(s); EODHD rejected %d code(s)%s%s%s", __version__,
                   label, state["split"], state["calls"], len(rej),
                   (": " + ", ".join(rej[:40]) + (" ..." if len(rej) > 40 else "")) if rej else "",
                   ("; %d code(s) dropped at the request cap %d" % (state["capped"], state["cap"]))
                   if state["capped"] else "",
                   ("; sent as EODHD exchange codes: " + state["translated"])   # v1.3.2
                   if state.get("translated") else "")


# ----------------------------------------------------------------------------- #
# v1.3.2 - venue batches (US first) and EODHD's exchange codes for the venues
# it rejected on 27 Oct
# ----------------------------------------------------------------------------- #
def _venue_batches_enabled() -> bool:
    """TFB_CALENDAR_VENUE_BATCHES = on|1|true|yes (any case). Default off."""
    return _env_str("TFB_CALENDAR_VENUE_BATCHES").lower() in ("on", "1", "true", "yes")


def _suffix_map_enabled() -> bool:
    """TFB_CALENDAR_SUFFIX_MAP = on|1|true|yes (any case). Default off."""
    return _env_str("TFB_CALENDAR_SUFFIX_MAP").lower() in ("on", "1", "true", "yes")


def _calendar_code(code: str) -> str:
    """The code EODHD's earnings calendar knows for a to_eodhd_symbol code.
    Evidence (calendar_sync run 40498374271, 27 Oct): EODHD rejected every
    '.L' and '.KL' code and the 'X.IS.US' / 'X.LS.US' codes the shared
    normalizer builds for Istanbul and Lisbon (it does not list those
    suffixes, so it appends the default '.US'). EODHD's exchange codes for
    those venues: LSE (exchanges API), KLSE, IS, LS. Anything else comes back
    unchanged - '.T', '.SI' and 'IG.MI' were rejected too, but their EODHD
    codes are not established here. PURE."""
    venue_code = {"L": "LSE", "KL": "KLSE"}
    parts = code.split(".")
    if len(parts) == 3 and parts[0] and parts[1] in ("IS", "LS") and parts[2] == "US":
        return parts[0] + "." + parts[1]
    if len(parts) == 2 and parts[0] and parts[1] in venue_code:
        return parts[0] + "." + venue_code[parts[1]]
    return code


def _earnings_codes(code_map: Dict[str, str]
                    ) -> Tuple[Dict[str, str], Dict[str, str], str]:
    """(code to request -> ORIGINAL symbol, untranslated code -> ORIGINAL
    symbol, one-line summary of what was translated). With
    TFB_CALENDAR_SUFFIX_MAP off: (code_map itself, {}, '') - the v1.3.1
    requests."""
    if not _suffix_map_enabled():
        return code_map, {}, ""
    out: Dict[str, str] = {}
    alias: Dict[str, str] = {}
    counts: Dict[str, int] = {}
    for code, orig in code_map.items():
        cal = _calendar_code(code)
        if cal != code:
            alias.setdefault(code, orig)
            rule = code.split(".", 1)[1] + "->" + cal.split(".", 1)[1]
            counts[rule] = counts.get(rule, 0) + 1
        out.setdefault(cal, orig)
    return out, alias, ", ".join("%s x%d" % (k, counts[k]) for k in sorted(counts))


def _venue_of(code: str) -> str:
    """The venue a code is batched with: its exchange suffix ('US', 'LSE',
    'T' ...); a code with more than one dot ('X.IS.US', 'BRK.B.US') is a
    venue of its own, so it can never sit in a US batch. PURE."""
    parts = code.split(".")
    if len(parts) == 2:
        return parts[1]
    return "*" + ".".join(parts[1:])


def _earnings_batches(codes: List[str], chunk: int) -> List[List[str]]:
    """The /calendar/earnings batches for the sorted `codes`. With
    TFB_CALENDAR_VENUE_BATCHES off: consecutive slices of `chunk` (v1.3.1).
    On: one venue per batch, US first, then the other venues in code order
    (multi-dot venues last); each venue sliced by `chunk`."""
    if not _venue_batches_enabled():
        return [codes[i:i + chunk] for i in range(0, len(codes), chunk)]
    groups: Dict[str, List[str]] = {}
    for c in codes:
        groups.setdefault(_venue_of(c), []).append(c)
    out: List[List[str]] = []
    for v in sorted(groups, key=lambda v: (v != "US", v.startswith("*"), v)):
        g = groups[v]
        out.extend(g[i:i + chunk] for i in range(0, len(g), chunk))
    return out


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
        code_map, alias, translated = _earnings_codes(code_map)   # v1.3.2 (off: unchanged)
        codes = sorted(code_map)
        out: Dict[str, str] = {}
        split_on = _split_404_enabled()                  # v1.3.1
        state = _split_state() if split_on else {}
        if split_on and translated:                      # v1.3.2: named in the WARNING line
            state["translated"] = translated
        async with _client() as client:
            for batch in _earnings_batches(codes, chunk):    # v1.3.2 (off: the v1.3.1 slices)
                try:
                    if split_on:                         # v1.3.1: a 404 batch is split
                        payload = await _earnings_split_payload(
                            client, {"from": _iso(today), "to": horizon}, batch, state)
                    else:
                        payload = await _get_json(client, "/calendar/earnings", {
                            "from": _iso(today), "to": horizon,
                            "symbols": ",".join(batch),
                        })
                except Exception as e:
                    logger.error("[calendar_provider v%s] earnings batch failed "
                                 "(%d syms): %s", __version__, len(batch),
                                 safe_error_text(e, secret_values=(_api_key(),)))
                    continue
                items = payload.get("earnings") if isinstance(payload, dict) else payload
                for it in items or []:
                    if not isinstance(it, dict):
                        continue
                    code = str(it.get("code") or it.get("symbol") or "").strip().upper()
                    d = _parse_date(it.get("report_date") or it.get("date"))
                    orig = code_map.get(code) or alias.get(code)   # v1.3.2: alias {} when off
                    if not orig or d is None or d < today:
                        continue
                    prev = _parse_date(out.get(orig))
                    if prev is None or d < prev:      # keep the EARLIEST upcoming
                        out[orig] = _iso(d)
        if split_on:
            _log_split_state("earnings", state)
        return out
    except Exception as e:  # blanket: a calendar hiccup must never propagate
        logger.error("[calendar_provider v%s] fetch_earnings_map failed: %s",
                     __version__, safe_error_text(e, secret_values=(_api_key(),)))
        return {}


# ----------------------------------------------------------------------------- #
# v1.3.0 - the most recent PAST earnings report (batched like the forward map)
# ----------------------------------------------------------------------------- #
def _num(raw: Any) -> Optional[float]:
    """Finite float or None (bools, blanks and text are not numbers)."""
    if raw is None or isinstance(raw, bool):
        return None
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    return v if v == v and v not in (float("inf"), float("-inf")) else None


async def fetch_last_earnings_map(symbols: List[str],
                                  days_back: Optional[int] = None
                                  ) -> Dict[str, Dict[str, Any]]:
    """{ORIGINAL_SYMBOL: {"last_earnings_date": 'YYYY-MM-DD',
    "eps_surprise_pct": float|None, "eps_actual": float|None,
    "eps_estimate": float|None, "report_timing": str|None}} for the LATEST
    report in [today - days_back, today - 1]. Empty when disabled, on any
    error, or for symbols with no report in the window. KSA symbols skipped
    (no EODHD coverage)."""
    if not is_enabled():
        return {}
    try:
        code_map, _ksa = _split_symbols(symbols)
        if not code_map:
            return {}
        today = _today()
        back = days_back or _env_int("TFB_CALENDAR_DAYS_BACK", 21, lo=1, hi=120)
        start = today - _dt.timedelta(days=back)
        end = today - _dt.timedelta(days=1)
        chunk = _env_int("TFB_CALENDAR_EARNINGS_CHUNK", 50, lo=1, hi=200)
        code_map, alias, translated = _earnings_codes(code_map)   # v1.3.2 (off: unchanged)
        codes = sorted(code_map)
        out: Dict[str, Dict[str, Any]] = {}
        split_on = _split_404_enabled()                  # v1.3.1
        state = _split_state() if split_on else {}
        if split_on and translated:                      # v1.3.2: named in the WARNING line
            state["translated"] = translated
        async with _client() as client:
            for batch in _earnings_batches(codes, chunk):    # v1.3.2 (off: the v1.3.1 slices)
                try:
                    if split_on:                         # v1.3.1: a 404 batch is split
                        payload = await _earnings_split_payload(
                            client, {"from": _iso(start), "to": _iso(end)}, batch, state)
                    else:
                        payload = await _get_json(client, "/calendar/earnings", {
                            "from": _iso(start), "to": _iso(end),
                            "symbols": ",".join(batch),
                        })
                except Exception as e:
                    logger.error("[calendar_provider v%s] last-earnings batch "
                                 "failed (%d syms): %s", __version__, len(batch),
                                 safe_error_text(e, secret_values=(_api_key(),)))
                    continue
                items = payload.get("earnings") if isinstance(payload, dict) else payload
                for it in items or []:
                    if not isinstance(it, dict):
                        continue
                    code = str(it.get("code") or it.get("symbol") or "").strip().upper()
                    d = _parse_date(it.get("report_date") or it.get("date"))
                    orig = code_map.get(code) or alias.get(code)   # v1.3.2: alias {} when off
                    if not orig or d is None or d < start or d > end:
                        continue
                    prev = _parse_date((out.get(orig) or {}).get("last_earnings_date"))
                    if prev is not None and d <= prev:   # keep the LATEST report
                        continue
                    actual, estimate = _num(it.get("actual")), _num(it.get("estimate"))
                    surprise = _num(it.get("percent"))
                    if surprise is None and actual is not None and estimate:
                        surprise = (actual - estimate) / abs(estimate) * 100.0
                    timing = str(it.get("before_after_market") or "").strip() or None
                    out[orig] = {
                        "last_earnings_date": _iso(d),
                        "eps_surprise_pct": (round(surprise, 1)
                                             if surprise is not None else None),
                        "eps_actual": actual,
                        "eps_estimate": estimate,
                        "report_timing": timing,
                    }
        if split_on:
            _log_split_state("last-earnings", state)
        return out
    except Exception as e:  # blanket: a calendar hiccup must never propagate
        logger.error("[calendar_provider v%s] fetch_last_earnings_map failed: %s",
                     __version__, safe_error_text(e, secret_values=(_api_key(),)))
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
                    logger.debug("[calendar_provider] div %s: %s", code,
                                 safe_error_text(e, secret_values=(_api_key(),)))
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
                     __version__, safe_error_text(e, secret_values=(_api_key(),)))
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
        logger.error("[calendar_provider v%s] fetch_ipos failed: %s", __version__,
                     safe_error_text(e, secret_values=(_api_key(),)))
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
    except Exception as e:
        # v1.2.0 (F2b): rate-limit-class exceptions RE-RAISE so the caller
        # can count them — a blocked endpoint must never report as empty.
        if "ratelimit" in type(e).__name__.lower():
            raise
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


def _yf_earnings_dates_future(ysym: str) -> Optional[_dt.date]:
    """v1.2.0 (F2b) SYNC worker: earliest FUTURE date from yfinance's
    get_earnings_dates — the endpoint that KEEPS WORKING from datacenter
    IPs where Ticker.calendar rate-limits (Render shell proof 2026-07-14:
    calendar=YFRateLimitError while this returned NMM 2026-08-20 and
    2222.SR 2026-08-04 in the same second). Raises on rate-limit so the
    caller can classify; returns None on genuinely absent data."""
    df = _yf.Ticker(ysym).get_earnings_dates(limit=12)  # type: ignore[union-attr]
    idx = getattr(df, "index", None)
    if idx is None:
        return None
    today = _today()
    best: Optional[_dt.date] = None
    for x in idx:
        try:
            d = x.date() if hasattr(x, "date") else _parse_date(str(x)[:10])
        except Exception:
            d = _parse_date(str(x)[:10])
        if d is not None and d >= today and (best is None or d < best):
            best = d
    return best


async def _yahoo_calendar_one(sem: asyncio.Semaphore, orig: str,
                              today: _dt.date,
                              out_earn: Dict[str, str],
                              out_exdiv: Dict[str, str],
                              throttled: List[int]) -> None:
    """One symbol through the thread-pooled yfinance call, bounded by the
    shared semaphore and the module timeout. Only FUTURE dates land."""
    async with sem:
        ysym = _yahoo_symbol_for_cal(orig)
        tmo = float(_env_int("TFB_CALENDAR_TIMEOUT_SEC", 12, lo=2, hi=120))
        earn: Optional[_dt.date] = None
        exdiv: Optional[_dt.date] = None
        # Tier 1 (v1.2.0 F2b): get_earnings_dates — survives datacenter
        # throttling that kills Ticker.calendar. Earnings only.
        try:
            earn = await asyncio.wait_for(
                asyncio.to_thread(_yf_earnings_dates_future, ysym),
                timeout=tmo)
        except Exception as e:
            if "ratelimit" in type(e).__name__.lower():
                throttled[0] += 1
            logger.debug("[calendar_provider] yahoo earnings_dates %s: %s",
                         orig, safe_error_text(e))
        # Tier 2: Ticker.calendar — ex-div (its only source) + earnings
        # backup. Throttles here are classified too, never silently eaten.
        try:
            c_earn, exdiv = await asyncio.wait_for(
                asyncio.to_thread(_yf_calendar_dates, ysym), timeout=tmo)
            if earn is None:
                earn = c_earn
        except Exception as e:
            if "ratelimit" in type(e).__name__.lower():
                throttled[0] += 1
            logger.debug("[calendar_provider] yahoo calendar %s: %s", orig,
                         safe_error_text(e))
        if earn is not None and earn >= today:
            out_earn[orig] = _iso(earn)
        if exdiv is not None and exdiv >= today:
            out_exdiv[orig] = _iso(exdiv)


async def _yahoo_calendar_fill(base: Dict[str, Dict[str, Optional[str]]],
                               today: _dt.date) -> Tuple[int, int]:
    """Fill-only pass over `base` for symbols missing either event date.
    Earnings and ex-dividend fill independently. EODHD values are never
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
                   if (d.get("next_earnings_date") is None
                       or d.get("next_ex_div_date") is None)]
        if not missing:
            return 0, 0
        sem = asyncio.Semaphore(_env_int("TFB_CALENDAR_CONCURRENCY", 4,
                                         lo=1, hi=16))
        f_earn: Dict[str, str] = {}
        f_exdiv: Dict[str, str] = {}
        _throttled = [0]
        await asyncio.gather(*[
            _yahoo_calendar_one(sem, s, today, f_earn, f_exdiv, _throttled)
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
        if n_e or n_d or _throttled[0]:
            logger.info("[calendar_provider v%s F2] yahoo fallback filled "
                        "earnings=%d exdiv=%d of %d blank (throttled=%d): %s",
                        __version__, n_e, n_d, len(missing), _throttled[0],
                        ",".join(sorted(f_earn)[:20]))
        return n_e, n_d
    except Exception as e:
        logger.error("[calendar_provider v%s] yahoo fallback pass failed: %s",
                     __version__, safe_error_text(e))
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
                     __version__, safe_error_text(e, secret_values=(_api_key(),)))
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


def fetch_last_earnings_sync(symbols: List[str],
                             days_back: Optional[int] = None
                             ) -> Dict[str, Dict[str, Any]]:
    """v1.3.0: sync wrapper of fetch_last_earnings_map."""
    return _run_sync(fetch_last_earnings_map(symbols, days_back))


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
