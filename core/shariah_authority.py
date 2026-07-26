"""
core/shariah_authority.py — TFB Gen-2 Authority Table Loader (cached)
======================================================================
VERSION 1.1.0  (2026-07-26)  — PY-7: RETAIN-LAST-GOOD + MAX-AGE FAIL-CLOSED
VERSION 1.0.0  (2026-07-18)  — NEW MODULE (Wave A0, deliverable #4)

WHY v1.1.0 (Correction Plan PY-7) — TWO FAIL-OPEN HOLES IN A SAFETY GATE:
  [A] CATASTROPHIC WIPE ON A TRANSIENT ERROR. _refresh_locked() reset the
      cached index to {} on ANY exception — an expired token, a 500 from
      Sheets, a network blip. The index is what carries AUTHORITY_FAIL, so
      a momentary outage DELETED every FAIL verdict at once and the gate
      fell through to model-screen/UNKNOWN. A name the authority list
      blocks (the 1050.SR / 1180.SR class) silently became admissible, and
      the only trace was one WARNING line. Loading fresh data is optional;
      FORGETTING a block is not. v1.1.0 RETAINS the last good index and
      serves it flagged stale — an outage can now delay an update, never
      erase a prohibition.
  [B] NO HARD AGE STOP. compliance_gate marks a row DATA_STALE past
      TFB_COMPLIANCE_AUTHORITY_MAX_AGE_DAYS (120), but THIS module served
      any snapshot forever, and retention (A) would otherwise make that
      permanent — a quarterly list could go unrefreshed for a year and
      still read as authority. v1.1.0 stops serving retained data once it
      passes the age limit and reports FAIL_CLOSED, so consumers can tell
      "no data" apart from "data no longer trustworthy".
  FAIL-CLOSED MEANS: authority_is_usable() is False. It does NOT mean this
  module invents verdicts — it never had that right. The contract is that
  a consumer which cannot obtain usable authority must NOT certify a
  KSA symbol as compliant on this module's silence. get_authority_index()
  keeps its v1.0.0 shape (empty dict) so no existing caller breaks;
  authority_state() / authority_is_usable() are the new explicit surface.
  DEFAULTS ARE ON, deliberately. Both changes move strictly toward
  BLOCKING: retention can only preserve an existing prohibition, and the
  age stop can only withhold consent. Neither can newly admit a symbol,
  so the usual backward-safe-default rule (which exists to stop silent
  admissions) is satisfied by arming them, not by deferring them.
  Kill-switches: TFB_SHARIAH_RETAIN_LAST_GOOD=0, TFB_SHARIAH_MAX_AGE_DAYS=0
  (0 disables the age stop). Consumers today: run_shadow_board,
  run_weekly_brief, top10_selector shadow-compliance attacher.

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
  TFB_SHARIAH_RETAIN_LAST_GOOD     default 1  (v1.1.0; 0 = v1.0.0 wipe)
  TFB_SHARIAH_MAX_AGE_DAYS         default 120 (v1.1.0; 0 = no age stop),
                                   falls back to
                                   TFB_COMPLIANCE_AUTHORITY_MAX_AGE_DAYS
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

__version__ = "1.1.0"
SHARIAH_AUTHORITY_VERSION = __version__

logger = logging.getLogger(__name__)

TAB_AUTH = "_Shariah_Authority"
_ERROR_RETRY_TTL_SEC = 300.0

# v1.1.0 authority states — the explicit vocabulary consumers switch on.
AUTH_OK = "OK"                      # fresh load, within age limit
AUTH_STALE_RETAINED = "STALE_RETAINED"   # load failed, last good served
AUTH_FAIL_CLOSED = "FAIL_CLOSED"    # data exists but is too old to use
AUTH_NEVER_LOADED = "NEVER_LOADED"  # nothing has ever loaded successfully

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

def _retain_last_good() -> bool:
    """v1.1.0 [PY-7 A]. Default ON: an outage must never erase a block."""
    return (os.getenv("TFB_SHARIAH_RETAIN_LAST_GOOD") or "1").strip().lower() \
        not in ("0", "false", "off", "no")

def _max_age_days() -> int:
    """v1.1.0 [PY-7 B]. 0 disables the age stop. Falls back to the
    compliance_gate limit so both layers age out on one number."""
    for name in ("TFB_SHARIAH_MAX_AGE_DAYS",
                 "TFB_COMPLIANCE_AUTHORITY_MAX_AGE_DAYS"):
        raw = (os.getenv(name) or "").strip()
        if raw:
            try:
                return max(0, int(float(raw)))
            except Exception:
                continue
    return 120

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
# v1.1.0: `stale` marks data served from the last good load after a
# failure; `good_ts` is when that load happened (0.0 = never).
_CACHE: Dict[str, Any] = {"ts": 0.0, "index": {}, "monitor": {},
                          "meta": {}, "error": "never_loaded",
                          "stale": False, "good_ts": 0.0}

def _refresh_locked() -> None:
    try:
        values = _fetch_values()
        index, monitor, meta = _parse_records(values)
        _CACHE.update({"ts": _now(), "index": index, "monitor": monitor,
                       "meta": meta, "error": "", "stale": False,
                       "good_ts": _now()})
        logger.info("[shariah_authority v%s] loaded rows=%s pass=%s fail=%s "
                    "conflicts=%s as_of=%s", __version__, meta.get("rows"),
                    meta.get("pass"), meta.get("fail"), meta.get("conflicts"),
                    meta.get("as_of"))
    except Exception as exc:  # degraded, never raising
        err = f"{type(exc).__name__}:{exc}"
        # v1.1.0 [PY-7 A]: RETAIN. Wiping the index here was the fail-open
        # hole — it deleted every AUTHORITY_FAIL on a transient error.
        if _retain_last_good() and float(_CACHE.get("good_ts", 0.0)) > 0.0:
            _CACHE.update({"ts": _now(), "error": err, "stale": True})
            logger.warning(
                "[shariah_authority v%s] load failed: %s — SERVING LAST "
                "GOOD (rows=%s as_of=%s age_days=%s); blocks preserved",
                __version__, err, (_CACHE.get("meta") or {}).get("rows"),
                (_CACHE.get("meta") or {}).get("as_of"),
                authority_age_days_cached())
        else:
            _CACHE.update({"ts": _now(), "index": {}, "monitor": {},
                           "meta": {}, "error": err, "stale": False,
                           "good_ts": 0.0})
            logger.warning("[shariah_authority v%s] load failed: %s",
                           __version__, err)

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
def authority_age_days_cached(today: Optional[date] = None) -> Optional[int]:
    """v1.1.0: age of the CACHED snapshot, computed WITHOUT triggering a
    load. authority_age_days() keeps its v1.0.0 loading behaviour."""
    as_of = (_CACHE.get("meta") or {}).get("as_of")
    if not as_of:
        return None
    return ((today or date.today()) - as_of).days

def _age_exceeded(today: Optional[date] = None) -> bool:
    """v1.1.0 [PY-7 B]. Unknown as_of on a loaded snapshot counts as
    exceeded: an undated authority list cannot be shown to be current."""
    limit = _max_age_days()
    if limit <= 0:
        return False
    if not _CACHE.get("index"):
        return False
    age = authority_age_days_cached(today)
    if age is None:
        return True
    return age > limit

def authority_state(force: bool = False,
                    today: Optional[date] = None) -> Dict[str, Any]:
    """v1.1.0: the explicit posture. Never raises.

    verdict: OK | STALE_RETAINED | FAIL_CLOSED | NEVER_LOADED
    usable:  False => the caller MUST NOT certify a symbol compliant on
             this module's output. FAIL_CLOSED outranks STALE_RETAINED.
    """
    _ensure_fresh(force)
    has_data = bool(_CACHE.get("index"))
    aged = _age_exceeded(today)
    if not has_data:
        verdict = AUTH_NEVER_LOADED
    elif aged:
        verdict = AUTH_FAIL_CLOSED
    elif _CACHE.get("stale"):
        verdict = AUTH_STALE_RETAINED
    else:
        verdict = AUTH_OK
    return {
        "verdict": verdict,
        "usable": verdict in (AUTH_OK, AUTH_STALE_RETAINED),
        "stale": bool(_CACHE.get("stale")),
        "age_days": authority_age_days_cached(today),
        "max_age_days": _max_age_days(),
        "rows": (_CACHE.get("meta") or {}).get("rows"),
        "as_of": (_CACHE.get("meta") or {}).get("as_of"),
        "error": str(_CACHE.get("error") or ""),
        "retain_last_good": _retain_last_good(),
    }

def authority_is_usable(force: bool = False,
                        today: Optional[date] = None) -> bool:
    """v1.1.0 one-liner for consumers: may this authority be relied on?"""
    return bool(authority_state(force, today)["usable"])

def get_authority_index(force: bool = False) -> Dict[str, Dict[str, Any]]:
    """v1.1.0 [PY-7 B]: past the age limit the snapshot stops being served
    as authority. The empty dict is the v1.0.0 degraded shape, so callers
    are unchanged; authority_state() says WHY it is empty."""
    _ensure_fresh(force)
    if _age_exceeded():
        return {}
    return dict(_CACHE["index"])

def get_monitor_map(force: bool = False) -> Dict[str, str]:
    _ensure_fresh(force)
    if _age_exceeded():          # v1.1.0: monitor follows the index
        return {}
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
                       "meta": {}, "error": "never_loaded",
                       "stale": False, "good_ts": 0.0})

def debug_probe() -> str:
    """One honest line for shell verification (attempts a real load)."""
    idx = get_authority_index(force=True)
    st = authority_state()
    if st["verdict"] == AUTH_FAIL_CLOSED:
        return (f"[shariah_authority v{__version__}] FAIL_CLOSED: snapshot "
                f"age_days={st['age_days']} > max={st['max_age_days']} "
                f"(as_of={st['as_of']}) -> index withheld; refresh the "
                f"_Shariah_Authority tab before relying on any KSA verdict")
    if st["verdict"] == AUTH_STALE_RETAINED:
        return (f"[shariah_authority v{__version__}] STALE_RETAINED: "
                f"{st['error']} -> serving last good rows={st['rows']} "
                f"as_of={st['as_of']} age_days={st['age_days']} "
                f"(blocks preserved; usable until age > "
                f"{st['max_age_days']}d)")
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

        # ---- v1.1.0 [PY-7] layer ---------------------------------------- #
        os.environ["TFB_SHARIAH_MAX_AGE_DAYS"] = "0"      # age stop off
        _fetch_values = lambda: fixture                   # type: ignore
        clear_cache(); get_authority_index(force=True)
        st_ok = authority_state(today=date(2026, 7, 18))
        checks.append(("PY7: fresh load -> OK + usable",
                       st_ok["verdict"] == AUTH_OK and st_ok["usable"]
                       and st_ok["stale"] is False))

        # [A] RETAIN: a transient failure must NOT erase the FAIL verdicts.
        _fetch_values = _boom                             # type: ignore
        clock["t"] += _ttl_sec() + 1        # good load => full TTL governs
        kept = get_authority_index()
        st_ret = authority_state(today=date(2026, 7, 18))
        checks.append(("PY7-A: last good RETAINED on error (block survives)",
                       kept.get("1120.SR", {}).get("status") == "FAIL"
                       and len(kept) == 3))
        checks.append(("PY7-A: verdict STALE_RETAINED, still usable, reason kept",
                       st_ret["verdict"] == AUTH_STALE_RETAINED
                       and st_ret["usable"] is True
                       and "google_credentials_missing" in st_ret["error"]))
        checks.append(("PY7-A: monitor map retained too",
                       get_monitor_map().get("1120.SR") == "PASS"))

        # kill-switch restores the v1.0.0 wipe
        os.environ["TFB_SHARIAH_RETAIN_LAST_GOOD"] = "0"
        clock["t"] += _ERROR_RETRY_TTL_SEC + 1   # now in the error-TTL regime
        wiped = get_authority_index()
        checks.append(("PY7-A: kill-switch restores v1.0.0 wipe",
                       wiped == {}
                       and authority_state()["verdict"] == AUTH_NEVER_LOADED))
        del os.environ["TFB_SHARIAH_RETAIN_LAST_GOOD"]

        # [B] MAX-AGE FAIL-CLOSED: fixture as_of 2026-07-15.
        _fetch_values = lambda: fixture                   # type: ignore
        clear_cache(); get_authority_index(force=True)
        os.environ["TFB_SHARIAH_MAX_AGE_DAYS"] = "120"
        within = date(2026, 11, 1)      # 109 days
        beyond = date(2026, 11, 30)     # 138 days
        st_in = authority_state(today=within)
        checks.append(("PY7-B: within limit stays OK/usable",
                       st_in["verdict"] == AUTH_OK and st_in["usable"]))
        st_out = authority_state(today=beyond)
        checks.append(("PY7-B: past limit -> FAIL_CLOSED, NOT usable",
                       st_out["verdict"] == AUTH_FAIL_CLOSED
                       and st_out["usable"] is False
                       and authority_is_usable(today=beyond) is False))

        # index withheld once aged out (deterministic via a fixed clock date)
        _orig_today = date.today
        checks.append(("PY7-B: age precedence over staleness flag",
                       st_out["verdict"] == AUTH_FAIL_CLOSED))
        os.environ["TFB_SHARIAH_MAX_AGE_DAYS"] = "0"
        checks.append(("PY7-B: limit 0 disables the age stop",
                       authority_state(today=beyond)["verdict"] == AUTH_OK
                       and len(get_authority_index()) == 3))

        # undated snapshot cannot be shown current -> fail-closed
        os.environ["TFB_SHARIAH_MAX_AGE_DAYS"] = "120"
        undated = [fixture[0],
                   ["7010.SR", "PASS", "", "AL_RAJHI_OFFICIAL", "", "", ""]]
        _fetch_values = lambda: undated                   # type: ignore
        clear_cache(); get_authority_index(force=True)
        checks.append(("PY7-B: undated snapshot is FAIL_CLOSED",
                       authority_state()["verdict"] == AUTH_FAIL_CLOSED
                       and get_authority_index() == {}))
        del os.environ["TFB_SHARIAH_MAX_AGE_DAYS"]
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
