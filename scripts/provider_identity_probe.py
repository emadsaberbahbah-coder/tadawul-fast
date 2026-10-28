#!/usr/bin/env python3
# scripts/provider_identity_probe.py
"""
================================================================================
TFB Provider Identity Probe - v1.0.0 (read-only; WP-05 provider read-back)
================================================================================
v1.0.0 (2026-10-28) -- NEW SCRIPT
WHY (morning board, 28 Oct 08:09 and 08:49 Riyadh, both WITHHELD): the
  Commodities_FX leg of run 40604805750 was fresh on 430 of 453 rows
  (94.9 %, below TFB_AUDIT_MIN_FRESH_PCT 95). 16_Decision_Top10 therefore
  withheld the board and froze the stability day; the inline recovery
  rewrote the page and its audit stayed blocked at the same 430. 17 of the
  23 rows fail every day, so the page has 5 rows of slack:
  - 7 crypto pairs carry the wrong coin at a zero price, preserved since
    23 Sep: SUI-USD "Salmonation", GRT-USD "Golden Ratio Token", APT-USD
    "Apricot Finance", UNI-USD "UNICORN Token", COMP-USD "Compound Coin",
    IMX-USD "Impermax", STX-USD "Stox" (short-ticker collisions);
  - gold spot and 7 exotic crosses return no price: XAUUSD=X, BHDSAR=X,
    BRLBHD=X, AEDMYR=X, AEDPHP=X, AEDIDR=X, BHDPLN=X, AEDMXN=X;
  - lumber LBS=F and the dollar index DX=F return no price.
  WP-05 (11 Oct) shipped TFB_EODHD_CRYPTO_MAP=1 (BASE-USD -> EODHD
  BASE-USD.CC, default off) with the rule "arm only after a provider
  read-back". No read-back tool existed: the build container cannot reach
  the providers and the backend is token-gated.
WHAT: for each row of a JSON spec, probes explicit candidate codes:
  EODHD real-time per code, EODHD search per root (code, exchange, name,
  previous close) and the Yahoo chart per ticker (name, price, market
  time). Row verdicts:
  - CONFIRMED <codes>: an EODHD candidate and a Yahoo candidate agree
    within AGREE_PCT and, for a curated crypto root, a returned name passes
    the WP-05 name screen;
  - AVAILABLE <codes>: some candidate prices fresh (<= FRESH_H hours);
  - DEAD: nothing usable.
  A candidate whose name fails the curated screen is listed as a name
  mismatch. Output: a markdown table (stdout and the job summary) and a
  JSON evidence file. Read-only: no workbook, no Render, no state. The
  EODHD key never appears in output.
ROLLBACK: delete this file and .github/workflows/provider_identity_probe.yml;
  nothing else reads them.
Not a licensed financial adviser; the owner decides and executes every trade.
================================================================================
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from core.symbols.crypto_identity import crypto_name_matches  # noqa: E402

__version__ = "1.0.0"

UTC = _dt.timezone.utc
EODHD_RT = "https://eodhd.com/api/real-time/{code}"
EODHD_SEARCH = "https://eodhd.com/api/search/{query}"
YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
DEFAULT_SPEC = os.path.join(_REPO, "data", "provider_probe_cfx_2026-10-28.json")
UA = "Mozilla/5.0 (X11; Linux x86_64) TFB-provider-identity-probe/1.0"
AGREE_PCT = 3.0
FRESH_H = 72.0
KINDS = ("crypto", "fx", "future", "control")


def _out(msg: str) -> None:
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


def _scrub(text: Any, secret: str) -> str:
    s = str(text)
    if secret:
        s = s.replace(secret, "***")
    return s


def _num(x: Any) -> Optional[float]:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if v != v or v in (float("inf"), float("-inf")):
        return None
    return v


def _age_h(ts: Optional[_dt.datetime], now: _dt.datetime) -> Optional[float]:
    if ts is None:
        return None
    return round((now - ts).total_seconds() / 3600.0, 1)


def _epoch(x: Any) -> Optional[_dt.datetime]:
    v = _num(x)
    if v is None or v <= 0:
        return None
    try:
        return _dt.datetime.fromtimestamp(v, UTC)
    except (OverflowError, OSError, ValueError):
        return None


def _day(x: Any) -> Optional[_dt.datetime]:
    try:
        d = _dt.datetime.strptime(str(x or "")[:10], "%Y-%m-%d")
    except ValueError:
        return None
    return d.replace(hour=23, minute=59, tzinfo=UTC)


# ----------------------------------------------------------------------------- #
# spec
# ----------------------------------------------------------------------------- #
def load_spec(path_or_json: str) -> List[Dict[str, Any]]:
    """A spec is {"items": [{"row", "kind", "eodhd": [...], "search": "...",
    "yahoo": [...]}]}; given inline or as a repository path."""
    text = path_or_json.strip()
    if not text.startswith("{"):
        with open(text, encoding="utf-8") as fh:
            text = fh.read()
    spec = json.loads(text)
    items = []
    for raw in spec.get("items") or []:
        row = str(raw.get("row") or "").strip().upper()
        kind = str(raw.get("kind") or "").strip().lower()
        if not row or kind not in KINDS:
            raise ValueError(f"bad spec item: {raw!r}")
        items.append({
            "row": row, "kind": kind,
            "eodhd": [str(c).strip().upper() for c in raw.get("eodhd") or [] if str(c).strip()],
            "search": str(raw.get("search") or "").strip().upper(),
            "yahoo": [str(c).strip().upper() for c in raw.get("yahoo") or [] if str(c).strip()],
            "note": str(raw.get("note") or ""),
        })
    if not items:
        raise ValueError("spec has no items")
    return items


# ----------------------------------------------------------------------------- #
# parsers (recorded shapes)
# ----------------------------------------------------------------------------- #
def parse_eodhd_rt(payload: Any, now: _dt.datetime) -> Dict[str, Any]:
    if isinstance(payload, list):
        payload = payload[0] if payload else {}
    if not isinstance(payload, dict):
        return {"ok": False, "error": "no JSON object"}
    close = _num(payload.get("close"))
    ts = _epoch(payload.get("timestamp"))
    age = _age_h(ts, now)
    ok = close is not None and close > 0 and age is not None and age <= FRESH_H
    out = {"ok": ok, "price": close, "age_h": age, "code": str(payload.get("code") or "")}
    if not ok:
        out["error"] = ("no price" if close is None or close <= 0 else
                        "no timestamp" if age is None else f"stale {age:.0f} h")
    return out


def parse_eodhd_search(payload: Any, now: _dt.datetime) -> List[Dict[str, Any]]:
    out = []
    for r in payload if isinstance(payload, list) else []:
        if not isinstance(r, dict):
            continue
        close = _num(r.get("previousClose"))
        when = _day(r.get("previousCloseDate"))
        out.append({"code": f"{r.get('Code') or ''}.{r.get('Exchange') or ''}".upper().strip("."),
                    "name": str(r.get("Name") or ""), "type": str(r.get("Type") or ""),
                    "price": close, "age_h": _age_h(when, now)})
    return out


def parse_yahoo_chart(payload: Any, now: _dt.datetime) -> Dict[str, Any]:
    chart = (payload or {}).get("chart") if isinstance(payload, dict) else None
    if not isinstance(chart, dict):
        return {"ok": False, "error": "no chart object"}
    results = chart.get("result") or []
    if not results:
        err = chart.get("error") or {}
        return {"ok": False, "error": str(err.get("description") or err.get("code") or "no result")[:120]}
    meta = (results[0] or {}).get("meta") or {}
    price = _num(meta.get("regularMarketPrice"))
    ts = _epoch(meta.get("regularMarketTime"))
    age = _age_h(ts, now)
    name = str(meta.get("longName") or meta.get("shortName") or "")
    ok = price is not None and price > 0 and age is not None and age <= FRESH_H
    out = {"ok": ok, "price": price, "age_h": age, "name": name,
           "type": str(meta.get("instrumentType") or ""), "currency": str(meta.get("currency") or "")}
    if not ok:
        out["error"] = ("no price" if price is None or price <= 0 else
                        "no market time" if age is None else f"stale {age:.0f} h")
    return out


# ----------------------------------------------------------------------------- #
# HTTP
# ----------------------------------------------------------------------------- #
class Prober:
    def __init__(self, http: Any, key: str, now: _dt.datetime,
                 sleep: Callable[[float], None] = time.sleep, timeout: float = 15.0):
        self.http, self.key, self.now, self.sleep, self.timeout = http, key, now, sleep, timeout
        self.calls = 0
        self.answered = 0

    def _get(self, url: str, params: Dict[str, Any]) -> Tuple[int, Any, str]:
        last = ""
        for attempt in (1, 2):
            self.calls += 1
            try:
                resp = self.http.get(url, params=params, headers={"User-Agent": UA}, timeout=self.timeout)
            except Exception as exc:  # network error
                last = _scrub(f"{type(exc).__name__}: {exc}", self.key)[:160]
                if attempt == 1:
                    self.sleep(3.0)
                    continue
                return 0, None, last
            code = int(getattr(resp, "status_code", 0) or 0)
            if code in (429, 500, 502, 503, 504) and attempt == 1:
                self.sleep(5.0)
                continue
            self.answered += 1
            try:
                payload = resp.json()
            except ValueError:
                payload = None
            return code, payload, _scrub(str(getattr(resp, "text", "") or "")[:120], self.key)
        return 0, None, last

    def eodhd_rt(self, code: str) -> Dict[str, Any]:
        if not self.key:
            return {"ok": False, "error": "EODHD_API_KEY missing"}
        status, payload, text = self._get(EODHD_RT.format(code=code), {"api_token": self.key, "fmt": "json"})
        if status != 200:
            return {"ok": False, "error": f"HTTP {status}" + (f": {text}" if text else "")}
        return parse_eodhd_rt(payload, self.now)

    def eodhd_search(self, query: str, exchange: str) -> List[Dict[str, Any]]:
        if not self.key or not query:
            return []
        params = {"api_token": self.key, "fmt": "json", "limit": 15}
        if exchange:
            params["exchange"] = exchange
        status, payload, _ = self._get(EODHD_SEARCH.format(query=query), params)
        return parse_eodhd_search(payload, self.now) if status == 200 else []

    def yahoo(self, ticker: str) -> Dict[str, Any]:
        status, payload, text = self._get(YAHOO_CHART.format(ticker=ticker), {"range": "5d", "interval": "1d"})
        if payload is not None:
            out = parse_yahoo_chart(payload, self.now)
            if status != 200 and out.get("ok"):
                out["ok"] = False
            if status != 200:
                out.setdefault("error", f"HTTP {status}")
            return out
        return {"ok": False, "error": f"HTTP {status}" + (f": {text}" if text else "")}


# ----------------------------------------------------------------------------- #
# verdicts
# ----------------------------------------------------------------------------- #
def _name_ok(row: str, kind: str, name: str) -> Optional[bool]:
    if kind not in ("crypto", "control") or not name:
        return None
    return crypto_name_matches(row, name)


def judge(item: Dict[str, Any], cands: List[Dict[str, Any]]) -> Dict[str, Any]:
    """cands: {"provider", "code", "ok", "price", "age_h", "name"}."""
    row, kind = item["row"], item["kind"]
    ok = [c for c in cands if c.get("ok")]
    mismatches = [c for c in cands if c.get("name") and _name_ok(row, kind, c["name"]) is False]
    pairs = []
    for e in [c for c in ok if c["provider"] == "eodhd"]:
        for y in [c for c in ok if c["provider"] == "yahoo"]:
            if not y.get("price") or not e.get("price"):
                continue
            gap = abs(e["price"] / y["price"] - 1.0) * 100.0
            judged = [_name_ok(row, kind, n) for n in (y.get("name"), e.get("name")) if n]
            curated = kind in ("crypto", "control") and crypto_name_matches(row, "?") is not None
            if any(j is True for j in judged):
                ident: Optional[bool] = True
            elif any(j is False for j in judged) or (curated and not judged):
                ident = False           # a curated coin needs a name that passes the screen
            else:
                ident = None            # not judged (FX, futures, unknown roots)
            if gap <= AGREE_PCT and ident is not False:
                pairs.append((gap, e, y))
    if pairs:
        gap, e, y = sorted(pairs, key=lambda p: p[0])[0]
        verdict, best = "CONFIRMED", f"eodhd:{e['code']} = yahoo:{y['code']} ({gap:.1f} %)"
    elif ok:
        good = [c for c in ok if _name_ok(row, kind, c.get("name") or "") is not False]
        verdict = "AVAILABLE" if good else "NAME_MISMATCH"
        best = ", ".join(f"{c['provider']}:{c['code']}" for c in (good or ok))
    else:
        verdict, best = "DEAD", ""
    return {"row": row, "kind": kind, "verdict": verdict, "best": best,
            "mismatches": [f"{c['provider']}:{c['code']} \"{c['name'][:40]}\"" for c in mismatches],
            "candidates": cands}


def probe_item(p: Prober, item: Dict[str, Any]) -> Dict[str, Any]:
    cands: List[Dict[str, Any]] = []
    search = []
    if item["search"]:
        search = p.eodhd_search(item["search"], "CC" if item["kind"] in ("crypto", "control") else "FOREX")
    for code in item["eodhd"]:
        r = p.eodhd_rt(code)
        hit = next((s for s in search if s["code"] == code), None)
        cands.append({"provider": "eodhd", "code": code, "ok": bool(r.get("ok")), "price": r.get("price"),
                      "age_h": r.get("age_h"), "name": (hit or {}).get("name", ""), "error": r.get("error", "")})
    for t in item["yahoo"]:
        r = p.yahoo(t)
        cands.append({"provider": "yahoo", "code": t, "ok": bool(r.get("ok")), "price": r.get("price"),
                      "age_h": r.get("age_h"), "name": r.get("name", ""), "error": r.get("error", "")})
    out = judge(item, cands)
    out["search"] = [s for s in search if s["code"].startswith(item["search"] + "-")][:6] if item["search"] else []
    out["note"] = item.get("note", "")
    return out


def run(items: Sequence[Dict[str, Any]], p: Prober) -> Dict[str, Any]:
    rows = [probe_item(p, it) for it in items]
    counts: Dict[str, int] = {}
    for r in rows:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    return {"version": __version__, "now": p.now.isoformat(), "rows": rows, "counts": counts,
            "calls": p.calls, "answered": p.answered, "eodhd_key": bool(p.key)}


def _fmt_price(x: Any) -> str:
    v = _num(x)
    if v is None:
        return "-"
    return f"{v:,.6g}"


def render_markdown(rep: Dict[str, Any]) -> str:
    lines = [f"## Provider identity probe v{__version__} - {rep['now'][:16]} UTC", "",
             "Counts: " + ", ".join(f"{k} {v}" for k, v in sorted(rep["counts"].items()))
             + f" | requests {rep['calls']}, answered {rep['answered']}"
             + ("" if rep["eodhd_key"] else " | EODHD_API_KEY missing: EODHD not probed"), "",
             "| Row | Kind | Verdict | Best | Candidates | Name mismatches |",
             "|---|---|---|---|---|---|"]
    for r in rep["rows"]:
        cands = "; ".join(
            f"{c['provider']}:{c['code']} " + (f"{_fmt_price(c['price'])} ({c['age_h']} h)" if c.get("ok")
                                              else f"x {c.get('error') or 'no data'}")
            + (f" \"{c['name'][:28]}\"" if c.get("name") else "")
            for c in r["candidates"])
        search = "; ".join(f"{s['code']} \"{s['name'][:24]}\" {_fmt_price(s['price'])}" for s in r.get("search") or [])
        if search:
            cands += f" | search: {search}"
        lines.append(f"| {r['row']} | {r['kind']} | {r['verdict']} | {r['best'] or '-'} | "
                     f"{cands.replace('|', '/')} | {'; '.join(r['mismatches']) or '-'} |")
    lines += ["", "Read-only probe; nothing is armed or written. Not a licensed financial adviser."]
    return "\n".join(lines) + "\n"


def make_http_session():  # pragma: no cover - live run
    import requests
    return requests.Session()


def main(argv: Optional[Sequence[str]] = None, *, http: Any = None, now: Optional[_dt.datetime] = None,
         sleep: Optional[Callable[[float], None]] = None) -> int:
    ap = argparse.ArgumentParser(description="TFB provider identity probe (read-only)")
    ap.add_argument("--spec", default="", help="spec path or inline JSON (default: the 28 Oct CFX spec)")
    args = ap.parse_args(argv)
    try:
        items = load_spec(args.spec or os.environ.get("TFB_PROBE_SPEC", "") or DEFAULT_SPEC)
    except (OSError, ValueError) as exc:
        _out(f"[probe v{__version__}] bad spec: {exc}")
        return 2
    key = (os.environ.get("EODHD_API_KEY") or "").strip()
    p = Prober(http or make_http_session(), key, (now or _dt.datetime.now(UTC)).astimezone(UTC),
               sleep=sleep or time.sleep)
    rep = run(items, p)
    md = render_markdown(rep)
    _out(md)
    ev = (os.environ.get("TFB_PROBE_EVIDENCE_DIR") or "").strip()
    if ev:
        os.makedirs(ev, exist_ok=True)
        with open(os.path.join(ev, "provider_probe.json"), "w", encoding="utf-8") as fh:
            json.dump(rep, fh, indent=1, sort_keys=True)
        with open(os.path.join(ev, "provider_probe.md"), "w", encoding="utf-8") as fh:
            fh.write(md)
    summary = (os.environ.get("GITHUB_STEP_SUMMARY") or "").strip()
    if summary:
        try:
            with open(summary, "a", encoding="utf-8") as fh:
                fh.write(md)
        except OSError:
            pass
    _out(f"[probe v{__version__}] rows={len(rep['rows'])} counts={rep['counts']} "
         f"requests={rep['calls']} answered={rep['answered']}")
    return 0 if rep["answered"] else 1


if __name__ == "__main__":
    sys.exit(main())
