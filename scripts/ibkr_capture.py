#!/usr/bin/env python3
"""
scripts/ibkr_capture.py - the latest read-only IBKR capture, for the plan pages
================================================================================
VERSION 1.0.0 (2026-11-13) - NEW MODULE

WHY (owner, 13 Nov 2026 11:51 Riyadh: "the action plan should be as per the cash
available on IBKR")
    The plan pages sized and checked their buys from the cash cell typed on the
    main page, then (1.6.0) from the workbook's cash ledger. Neither is the
    broker. A read-only capture of the broker account already exists: scheduled
    tasks write 'TFB_IBKR_Capture_<date>-<time>.json' (schema
    tfb.ibkr-capture/v1: the positions, balances and working orders exactly as
    IBKR returned them) to one Drive folder - daily at 08:40 Riyadh and every
    hour of the US and Tadawul sessions. The Apps Script pages read it; the
    Python pages (Daily Plan, Today Action Plan) could not, and showed "broker
    orders not read" from a tab last filled on 17 Oct.

WHAT
    latest(now)   -> the newest capture of the folder, parsed:
                     cash, settled cash, net value, positions, working SELL
                     stops and limits, working BUY orders. Never raises: an
                     unreadable capture is {"ok": False, "reason": ...}.
    parse(...)    -> pure; the same rejections as the workbook's reader (wrong
                     schema, a capture time in the future, or one that differs
                     from Drive's creation time by more than 10 minutes).

SAFETY
    Read-only: one Drive list and one Drive download, scope drive.readonly, with
    the service account the workflows already use. It never calls the broker.
    Nothing here prints an amount, a quantity or an account number: the log line
    carries the file name, its age and two counts.

ENV
    TFB_IBKR_CAPTURE_FOLDER_ID (the Drive folder; empty = not configured),
    TFB_IBKR_CAPTURE (on | off), GOOGLE_SHEETS_CREDENTIALS.
================================================================================
"""
from __future__ import annotations

import base64
import datetime as _dt
import json
import os
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

__version__ = "1.0.0"

UTC = _dt.timezone.utc
RIYADH = _dt.timezone(_dt.timedelta(hours=3))
SCHEMA = "tfb.ibkr-capture/v1"
PREFIX = "TFB_IBKR_Capture_"
MAX_BYTES = 400000
MAX_CLOCK_GAP_S = 600                                  # captured_at against Drive's creation time
FUTURE_SLACK_S = 300
_FOLDER_RE = re.compile(r"^[A-Za-z0-9_-]{10,200}$")
_DONE_RE = re.compile(r"FILLED|CANCEL|EXPIRED|INACTIVE|REJECT", re.I)
_DESC_RE = re.compile(r"^(buy|sell)\s+[\d,\.]+\s+(\S+)", re.I)
_STP_RE = re.compile(r"STP\s+(?:LMT\s+)?([\d,]*\.?\d+)", re.I)


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def enabled() -> bool:
    return _env("TFB_IBKR_CAPTURE", "on").lower() not in ("off", "0", "false", "no")


def _num(v: Any) -> Optional[float]:
    if v is None or isinstance(v, bool):
        return None
    try:
        x = float(str(v).replace(",", "").strip())
    except ValueError:
        return None
    return x if x == x and abs(x) != float("inf") else None


def _time(v: Any) -> Optional[_dt.datetime]:
    """An ISO time with an explicit offset (or Z) as an aware datetime; None otherwise."""
    s = str(v or "").strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    m = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d+)?([+-]\d{2}:\d{2})$", s)
    if not m:
        return None
    try:
        return _dt.datetime.fromisoformat(m.group(1) + (m.group(2) or "")[:7] + m.group(3))
    except ValueError:
        return None


def symbol_of(base: str) -> str:
    """IBKR's bare symbol as the workbook writes it: 'DDI' -> 'DDI.US'; one that already has a suffix is kept."""
    s = str(base or "").strip().upper().replace(" ", ".")
    return s if "." in s else (s + ".US" if s else "")


def parse(capture: Any, created: Optional[_dt.datetime], now: _dt.datetime, name: str = "") -> Dict[str, Any]:
    """The capture as the plan pages use it. PURE.
    {"ok", "reason", "name", "as_of", "age_h", "cash_usd", "settled_usd", "nav_usd", "stock_usd",
     "positions": {SYM: qty}, "stops": {SYM: [(price, qty)]}, "limits": {SYM: [(price, qty)]},
     "buys": {SYM: [(limit or None, qty)]}, "n_orders"}"""
    out: Dict[str, Any] = {"ok": False, "reason": "", "name": name, "as_of": None, "age_h": None, "cash_usd": None,
                           "settled_usd": None, "nav_usd": None, "stock_usd": None, "positions": {}, "stops": {},
                           "limits": {}, "buys": {}, "n_orders": 0}
    if not isinstance(capture, dict) or str(capture.get("schema")) != SCHEMA:
        out["reason"] = "capture schema not recognised"
        return out
    at = _time(capture.get("captured_at"))
    if at is None:
        out["reason"] = "capture time unreadable"
        return out
    if (at - now).total_seconds() > FUTURE_SLACK_S:
        out["reason"] = "capture time is in the future"
        return out
    if created is not None and abs((at - created).total_seconds()) > MAX_CLOCK_GAP_S:
        out["reason"] = "capture time disagrees with the file's creation time"
        return out
    bals = ((capture.get("balances") or {}).get("balances")) if isinstance(capture.get("balances"), dict) else None
    usd = base = None
    for b in bals or []:
        cc = str((b or {}).get("currency") or "").upper()
        if cc == "USD":
            usd = b
        elif cc == "BASE":
            base = b
    others = [b for b in bals or [] if str((b or {}).get("currency") or "").upper() not in ("USD", "BASE")
              and abs(_num((b or {}).get("cash_balance")) or 0.0) > 0.005]
    if others:
        out["reason"] = "capture holds cash in a currency other than USD"
        return out
    b = usd or base
    if not b:
        out["reason"] = "capture has no USD balance"
        return out
    cash, settled = _num(b.get("cash_balance")), _num(b.get("settled_cash"))
    if cash is None and settled is None:
        out["reason"] = "capture balance has no cash"
        return out
    out["cash_usd"] = cash if cash is not None else settled
    out["settled_usd"] = settled
    out["nav_usd"], out["stock_usd"] = _num(b.get("net_liquidation_value")), _num(b.get("stock_market_value"))
    pos = (capture.get("positions") or {}).get("positions") if isinstance(capture.get("positions"), dict) else None
    if not isinstance(pos, list):
        out["reason"] = "capture has no positions list"
        return out
    for p in pos:
        sym, q = symbol_of((p or {}).get("contract_description")), _num((p or {}).get("position"))
        if sym and q:
            out["positions"][sym] = out["positions"].get(sym, 0.0) + q
    orders = (capture.get("orders") or {}).get("orders") if isinstance(capture.get("orders"), dict) else None
    if not isinstance(orders, list):
        out["reason"] = "capture has no orders list"
        return out
    for o in orders:
        o = o or {}
        if _DONE_RE.search(str(o.get("order_status") or "")):
            continue
        m = _DESC_RE.match(str(o.get("primary_description") or ""))
        if not m:
            continue
        side = str(o.get("side") or m.group(1)).upper()
        sym = symbol_of(m.group(2))
        rem = _num(o.get("remaining_shares_qty"))
        qty = rem if rem is not None else _num(o.get("total_shares_qty"))
        if not sym or not qty or qty <= 0:
            continue
        typ = str(o.get("order_type") or "").upper()
        out["n_orders"] += 1
        if side == "BUY":
            out["buys"].setdefault(sym, []).append((_num(o.get("limit_price")), qty))
        elif "STOP" in typ or "STP" in typ:
            ms = _STP_RE.search(str(o.get("secondary_description") or ""))
            out["stops"].setdefault(sym, []).append((_num(ms.group(1)) if ms else None, qty))
        elif "LIMIT" in typ or "LMT" in typ:
            out["limits"].setdefault(sym, []).append((_num(o.get("limit_price")), qty))
    out["as_of"] = at
    out["age_h"] = max(0.0, (now - at).total_seconds() / 3600.0)
    out["ok"] = True
    return out


def stop_cover(cap: Dict[str, Any], symbol: str) -> Tuple[Optional[float], float]:
    """(the LOWEST working stop price on the symbol, the shares its stop orders cover). (None, 0) without a stop;
    a stop order whose price could not be read counts its shares and gives no price."""
    rows = (cap.get("stops") or {}).get(symbol) or []
    prices = [px for px, _ in rows if px]
    return (min(prices) if prices else None), sum(q for _, q in rows)


def reserved_usd(cap: Dict[str, Any], skip: Any = ()) -> Tuple[float, bool]:
    """(USD held by working BUY orders on symbols not in `skip`, whether every such order had a limit price)."""
    total, complete = 0.0, True
    for sym, rows in (cap.get("buys") or {}).items():
        if sym in skip:
            continue
        for px, qty in rows:
            if px is None:
                complete = False
            else:
                total += px * qty
    return total, complete


def _credentials_info() -> Optional[Dict[str, Any]]:
    raw = _env("GOOGLE_SHEETS_CREDENTIALS") or _env("GOOGLE_CREDENTIALS")
    if not raw:
        return None
    if not raw.startswith("{"):
        try:
            raw = base64.b64decode(raw).decode("utf-8", errors="replace").strip()
        except Exception:  # noqa: BLE001
            return None
    try:
        info = json.loads(raw)
    except ValueError:
        return None
    return info if isinstance(info, dict) else None


def _session() -> Any:
    from google.auth.transport.requests import AuthorizedSession  # noqa: PLC0415 - CI only
    from google.oauth2 import service_account  # noqa: PLC0415
    info = _credentials_info()
    if not info:
        raise RuntimeError("no service-account credentials")
    return AuthorizedSession(service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/drive.readonly"]))


def latest(now: Optional[_dt.datetime] = None, session: Any = None, folder: Optional[str] = None,
           out: Optional[Callable[[str], None]] = None) -> Dict[str, Any]:
    """The newest capture of the folder, parsed. Never raises; {"ok": False, "reason": ...} when it cannot be used."""
    now = now or _dt.datetime.now(UTC)
    say = out or (lambda m: None)
    res: Dict[str, Any] = {"ok": False, "reason": "", "name": ""}
    folder = _env("TFB_IBKR_CAPTURE_FOLDER_ID") if folder is None else folder
    if not enabled():
        res["reason"] = "TFB_IBKR_CAPTURE is off"
        return res
    if not folder:
        res["reason"] = "capture folder not set (TFB_IBKR_CAPTURE_FOLDER_ID)"
        return res
    if not _FOLDER_RE.match(folder):
        res["reason"] = "capture folder id invalid"
        return res
    try:
        sess = session or _session()
        r = sess.get("https://www.googleapis.com/drive/v3/files", timeout=30, params={
            "q": "'%s' in parents and trashed = false and name contains '%s'" % (folder, PREFIX),
            "orderBy": "createdTime desc", "pageSize": 10, "fields": "files(id,name,createdTime,size)",
            "supportsAllDrives": "true", "includeItemsFromAllDrives": "true"})
        if r.status_code != 200:
            res["reason"] = "Drive list answered HTTP %d (is the capture folder shared with the service account?)" % r.status_code
            return res
        files = [f for f in (r.json().get("files") or [])
                 if str(f.get("name", "")).startswith(PREFIX) and str(f.get("name", "")).lower().endswith(".json")]
        if not files:
            res["reason"] = "no capture file visible in the folder (is it shared with the service account?)"
            return res
        f = files[0]
        res["name"] = str(f.get("name"))
        if int(f.get("size") or 0) > MAX_BYTES:
            res["reason"] = "capture size invalid"
            return res
        g = sess.get("https://www.googleapis.com/drive/v3/files/%s" % f["id"], timeout=30,
                     params={"alt": "media", "supportsAllDrives": "true"})
        if g.status_code != 200:
            res["reason"] = "Drive download answered HTTP %d" % g.status_code
            return res
        try:
            body = json.loads(g.content.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            res["reason"] = "capture is not JSON"
            return res
        res = parse(body, _time(f.get("createdTime")), now, res["name"])
    except Exception as e:  # noqa: BLE001 - the pages fall back to the workbook's own figures
        res["reason"] = "capture read failed (%s)" % type(e).__name__
        return res
    if res["ok"]:
        say("IBKR capture %s read: %.1f h old, %d position(s), %d working order(s)" % (
            res["name"], res["age_h"], len(res["positions"]), res["n_orders"]))
    return res
