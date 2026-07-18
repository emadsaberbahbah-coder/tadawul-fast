"""
core/shariah_authority.py — TFB Gen-2 Authority Table Loader (cached)
======================================================================
VERSION 1.0.0  (2026-07-18)  — NEW MODULE (Wave A0, deliverable #4)

WHY (Master Plan v2.1 §4.1/§4.2; consumers: Shadow selector, Weekly memo):
  * compliance_gate v1.0.0 takes an authority index but performs no I/O.
    refresh_shariah_authority v1.0.0 WRITES `_Shariah_Authority`. This module
    READS it — once, cached with TTL — and serves gate-shaped structures:
        get_authority_index() -> {sym: {status, as_of, source}}
        get_monitor_map()     -> {sym: MONITOR_STATUS}
  * Failure philosophy: public getters NEVER raise. Missing tab / missing
    credentials / network trouble => empty index + a cached error reason
    (short retry TTL) + one log line. The gate then resolves symbols through
    model-screen/UNKNOWN paths — degraded honestly, never silently wrong.
  * CONTINGENCY (documented): if the Render backend lacks Sheets credentials,
    debug_probe() says so plainly; the Shadow pipeline then loads authority
    inside the GitHub worker (which provably holds creds) and passes the
    index into the gate/attacher explicitly. This module works in both homes.

ENV:
  TFB_SHARIAH_SHEET_ID | DEFAULT_SPREADSHEET_ID          spreadsheet id
  GOOGLE_APPLICATION_CREDENTIALS | GOOGLE_SHEETS_CREDENTIALS(_B64)
  TFB_SHARIAH_CACHE_TTL_SEC        default 21600 (6h; errors retry at 300s)
"""

from __future__ import annotations

import base64
import json
import logging
import os
import tempfile
import threading
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

__version__ = "1.0.0"
SHARIAH_AUTHORITY_VERSION = __version__

logger = logging.getLogger(__name__)

TAB_AUTH = "_Shariah_Authority"
_ERROR_RETRY_TTL_SEC = 300.0

# --------------------------------------------------------------------------- #
# env / time indirection (test-injectable)                                     #
# --------------------------------------------------------------------------- #
def _now() -> float:
    return time.time()

def _ttl_sec() -> float:
    try:
        return float((os.getenv("TFB_SHARIAH_CACHE_TTL_SEC") or "21600").strip())
    except Exception:
        return 21600.0

def _sheet_id() -> Optional[str]:
    for v in (os.getenv("TFB_SHARIAH_SHEET_ID"), os.getenv("DEFAULT_SPREADSHEET_ID")):
        if v and str(v).strip():
            return str(v).strip()
    return None

# --------------------------------------------------------------------------- #
# fetch (lazy gspread; module-level indirection so tests patch _fetch_values)  #
# --------------------------------------------------------------------------- #
def _credentials_path() -> Optional[str]:
    path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if path:
        return path
    raw = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
    b64 = os.getenv("GOOGLE_SHEETS_CREDENTIALS_B64")
    if not (raw or b64):
        return None
    data = raw or base64.b64decode(b64).decode("utf-8")
    f = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
    f.write(data)
    f.close()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f.name
    return f.name

def _fetch_values() -> List[List[str]]:
    """Read all values of TAB_AUTH. Raises on any failure (caller caches)."""
    sid = _sheet_id()
    if not sid:
        raise RuntimeError("sheet_id_missing (TFB_SHARIAH_SHEET_ID / DEFAULT_SPREADSHEET_ID)")
    cred = _credentials_path()
    if not cred:
        raise RuntimeError("google_credentials_missing")
    import gspread  # lazy: present in backend requirements (6.2.1)
    from google.oauth2.service_account import Credentials
    creds = Credentials.from_service_account_file(
        cred, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
    ws = gspread.authorize(creds).open_by_key(sid).worksheet(TAB_AUTH)
    return ws.get_all_values()

# --------------------------------------------------------------------------- #
# parsing (pure; selftest fixtures use this directly)                          #
# --------------------------------------------------------------------------- #
def _as_date(v: Any) -> Optional[date]:
    s = str(v or "").strip()[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def _parse_records(values: List[List[str]]) -> Tuple[
        Dict[str, Dict[str, Any]], Dict[str, str], Dict[str, Any]]:
    """Header row 1 per refresh script: Symbol|Status|As Of|Source|Monitor|
    Rule Version|Doc Hash|Fetched At|Source URL. Malformed rows are skipped."""
    index: Dict[str, Dict[str, Any]] = {}
    monitor: Dict[str, str] = {}
    meta: Dict[str, Any] = {"rows": 0, "pass": 0, "fail": 0, "conflicts": 0,
                            "as_of": None, "rule_version": "", "doc_hash": ""}
    for row in (values or [])[1:]:
        if not row or not str(row[0]).strip():
            continue
        sym = str(row[0]).strip().upper()
        status = str(row[1]).strip().upper() if len(row) > 1 else ""
        if status not in ("PASS", "FAIL"):
            continue
        as_of = _as_date(row[2] if len(row) > 2 else None)
        source = str(row[3]).strip() if len(row) > 3 and str(row[3]).strip() \
            else "AL_RAJHI_OFFICIAL"
        index[sym] = {"status": status, "as_of": as_of, "source": source}
        mon = str(row[4]).strip().upper() if len(row) > 4 else ""
        if mon in ("PASS", "FAIL"):
            monitor[sym] = mon
            if mon != status:
                meta["conflicts"] += 1
        meta["rows"] += 1
        meta["pass"] += 1 if status == "PASS" else 0
        meta["fail"] += 1 if status == "FAIL" else 0
        if meta["as_of"] is None and as_of:
            meta["as_of"] = as_of
        if not meta["rule_version"] and len(row) > 5:
            meta["rule_version"] = str(row[5]).strip()
        if not meta["doc_hash"] and len(row) > 6:
            meta["doc_hash"] = str(row[6]).strip()
    return index, monitor, meta

# --------------------------------------------------------------------------- #
# cache                                                                        #
# --------------------------------------------------------------------------- #
_LOCK = threading.Lock()
_CACHE: Dict[str, Any] = {"ts": 0.0, "index": {}, "monitor": {},
                          "meta": {}, "error": "never_loaded"}

def _refresh_locked() -> None:
    try:
        values = _fetch_values()
        index, monitor, meta = _parse_records(values)
        _CACHE.update({"ts": _now(), "index": index, "monitor": monitor,
                       "meta": meta, "error": ""})
        logger.info("[shariah_authority v%s] loaded rows=%s pass=%s fail=%s "
                    "conflicts=%s as_of=%s", __version__, meta.get("rows"),
                    meta.get("pass"), meta.get("fail"), meta.get("conflicts"),
                    meta.get("as_of"))
    except Exception as exc:  # degraded, never raising
        _CACHE.update({"ts": _now(), "index": {}, "monitor": {},
                       "meta": {}, "error": f"{type(exc).__name__}:{exc}"})
        logger.warning("[shariah_authority v%s] load failed: %s",
                       __version__, _CACHE["error"])

def _ensure_fresh(force: bool = False) -> None:
    ttl = _ERROR_RETRY_TTL_SEC if _CACHE.get("error") else _ttl_sec()
    if force or (_now() - float(_CACHE.get("ts", 0.0))) >= ttl:
        with _LOCK:
            ttl = _ERROR_RETRY_TTL_SEC if _CACHE.get("error") else _ttl_sec()
            if force or (_now() - float(_CACHE.get("ts", 0.0))) >= ttl:
                _refresh_locked()

# --------------------------------------------------------------------------- #
# public API (never raises)                                                    #
# --------------------------------------------------------------------------- #
def get_authority_index(force: bool = False) -> Dict[str, Dict[str, Any]]:
    _ensure_fresh(force)
    return dict(_CACHE["index"])

def get_monitor_map(force: bool = False) -> Dict[str, str]:
    _ensure_fresh(force)
    return dict(_CACHE["monitor"])

def get_meta(force: bool = False) -> Dict[str, Any]:
    _ensure_fresh(force)
    return dict(_CACHE["meta"])

def last_error() -> str:
    return str(_CACHE.get("error") or "")

def authority_age_days(today: Optional[date] = None) -> Optional[int]:
    meta = get_meta()
    as_of = meta.get("as_of")
    if not as_of:
        return None
    return ((today or date.today()) - as_of).days

def clear_cache() -> None:
    with _LOCK:
        _CACHE.update({"ts": 0.0, "index": {}, "monitor": {},
                       "meta": {}, "error": "never_loaded"})

def debug_probe() -> str:
    """One honest line for shell verification (attempts a real load)."""
    idx = get_authority_index(force=True)
    if _CACHE.get("error"):
        return (f"[shariah_authority v{__version__}] DEGRADED: "
                f"{_CACHE['error']} -> index empty (gate falls back to "
                f"model-screen/UNKNOWN; worker-side loading is the contingency)")
    m = get_meta()
    return (f"[shariah_authority v{__version__}] OK rows={m.get('rows')} "
            f"pass={m.get('pass')} fail={m.get('fail')} "
            f"conflicts={m.get('conflicts')} as_of={m.get('as_of')} "
            f"age_days={authority_age_days()} sample={sorted(idx)[:3]}")

# --------------------------------------------------------------------------- #
# SELFTEST (offline; patches _fetch_values and _now)                           #
# --------------------------------------------------------------------------- #
def _selftest() -> int:
    global _fetch_values, _now
    checks: List[Tuple[str, bool]] = []
    fixture = [
        ["Symbol", "Status", "As Of", "Source", "Monitor", "Rule Version", "Doc Hash"],
        ["7010.SR", "PASS", "2026-07-15", "AL_RAJHI_OFFICIAL", "PASS", "RV1", "abc123"],
        ["1120.SR", "FAIL", "2026-07-15", "AL_RAJHI_OFFICIAL", "PASS", "", ""],
        ["2222.SR", "PASS", "2026-07-15", "", "", "", ""],
        ["", "", "", "", "", "", ""],
        ["BAD.SR", "MAYBE", "2026-07-15", "", "", "", ""],
    ]
    idx, mon, meta = _parse_records(fixture)
    checks.append(("parse: 3 valid rows", meta["rows"] == 3 and len(idx) == 3))
    checks.append(("parse: statuses + default source",
                   idx["7010.SR"]["status"] == "PASS"
                   and idx["1120.SR"]["status"] == "FAIL"
                   and idx["2222.SR"]["source"] == "AL_RAJHI_OFFICIAL"))
    checks.append(("parse: monitor + one conflict",
                   mon == {"7010.SR": "PASS", "1120.SR": "PASS"}
                   and meta["conflicts"] == 1))
    checks.append(("parse: malformed skipped", "BAD.SR" not in idx))
    checks.append(("parse: meta rule/hash from first row",
                   meta["rule_version"] == "RV1" and meta["doc_hash"] == "abc123"))

    clock = {"t": 1000.0}
    calls = {"n": 0}
    orig_fetch, orig_now = _fetch_values, _now
    _now = lambda: clock["t"]                      # type: ignore
    _fetch_values = lambda: (calls.__setitem__("n", calls["n"] + 1) or fixture)  # type: ignore
    try:
        clear_cache()
        i1 = get_authority_index()
        checks.append(("load via patched fetch", i1["7010.SR"]["status"] == "PASS"
                       and calls["n"] == 1 and last_error() == ""))
        get_authority_index(); get_monitor_map()
        checks.append(("TTL: cached, no refetch", calls["n"] == 1))
        clock["t"] += _ttl_sec() + 1
        get_authority_index()
        checks.append(("TTL: expiry refetches", calls["n"] == 2))
        get_authority_index(force=True)
        checks.append(("force refetches", calls["n"] == 3))

        def _boom() -> List[List[str]]:
            raise RuntimeError("google_credentials_missing")
        _fetch_values = _boom                      # type: ignore
        clear_cache()
        e1 = get_authority_index()
        checks.append(("error path: empty + reason cached, no raise",
                       e1 == {} and "google_credentials_missing" in last_error()))
        clock["t"] += _ERROR_RETRY_TTL_SEC - 1
        get_authority_index()
        ok_short = "google_credentials_missing" in last_error()
        clock["t"] += 2
        _fetch_values = lambda: fixture            # type: ignore
        e2 = get_authority_index()
        checks.append(("error path: short-TTL retry recovers",
                       ok_short and e2.get("7010.SR", {}).get("status") == "PASS"))
        checks.append(("age_days computed",
                       authority_age_days(date(2026, 7, 18)) == 3))
    finally:
        _fetch_values, _now = orig_fetch, orig_now
        clear_cache()

    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"[shariah_authority v{__version__}] SELFTEST {passed}/{len(checks)}")
    return 0 if passed == len(checks) else 1

if __name__ == "__main__":
    raise SystemExit(_selftest())
