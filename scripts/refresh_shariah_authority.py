"""
scripts/refresh_shariah_authority.py — TFB Gen-2 Shariah Authority Refresher
=============================================================================
VERSION 1.0.0  (2026-07-18)  — NEW SCRIPT (Wave A0, deliverable #2)

WHY (Master Plan v2.1 §4.1, Decision_Log D-2/D-4 context):
  * compliance_gate v1.0.0 consumes an authority table (symbol/status/as_of/
    source). This script OWNS producing it: AL_RAJHI_OFFICIAL first, an
    operator-pasted upload tab as first-class fallback, Argaam as MONITOR
    only (never authority) — disagreement surfaces as monitor data so the
    gate can mark CONFLICT.
  * Every official fetch is ARCHIVED: sha256 doc hash + fetched_at + source
    URL are written into the table's meta row (provenance, §4.1).
  * DEVIATION NOTE (documented): §4.1 said "authority table in _Lists_Config";
    implemented as a dedicated tab `_Shariah_Authority` so the existing
    _Lists_Config GAS parsers are never disturbed. `_Shariah_Upload` is the
    operator paste surface. Both auto-created.

MODES:
  --selftest                offline fixture harness (no network, no sheets)
  --mode auto|fetch|upload|file   (default auto: fetch if URLs set, else upload tab)
  --from-file PATH --as-of YYYY-MM-DD    CSV with Symbol,Status[,AsOf]
  --dry-run                 parse + print, write nothing
  --sheet-id ID             override spreadsheet id
  --no-monitor              skip Argaam monitor pass

ENV:
  TFB_SHARIAH_OFFICIAL_URLS   comma-separated official URLs (repo Variable;
                              captured in E0.4 — empty => upload/file modes)
  TFB_SHARIAH_MONITOR_URLS    default: Argaam shariah-companies page
  TARGET_SHEET_ID / TRACK_SHEET_ID / DEFAULT_SPREADSHEET_ID   spreadsheet id
  GOOGLE_APPLICATION_CREDENTIALS | GOOGLE_SHEETS_CREDENTIALS(_B64)
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import sys
import tempfile
from datetime import date, datetime, timezone
from typing import Dict, List, Optional, Tuple

SCRIPT_VERSION = "1.0.0"
RULE_VERSION = "RAJHI-2026Q3-pending-official-capture"   # updated by E0.4
TAB_AUTH = "_Shariah_Authority"
TAB_UPLOAD = "_Shariah_Upload"
AUTH_HEADER = ["Symbol", "Status", "As Of", "Source", "Monitor", "Rule Version",
               "Doc Hash", "Fetched At (UTC)", "Source URL"]
UPLOAD_HEADER = ["Symbol", "Status (PASS/FAIL or متوافقة/غير متوافقة)", "As Of (optional YYYY-MM-DD)"]
DEFAULT_MONITOR_URLS = "https://www.argaam.com/ar/company/shariahcompanies/3/3/14"

PASS_TOKENS = ("PASS", "COMPLIANT", "متوافق", "مباح", "مجاز", "نقي")
FAIL_TOKENS = ("FAIL", "NON-COMPLIANT", "NONCOMPLIANT", "غير متوافق", "مخالف", "غير مجاز")

# --------------------------------------------------------------------------- #
# small utils                                                                  #
# --------------------------------------------------------------------------- #
def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def normalize_symbol(raw: str) -> Optional[str]:
    s = str(raw).strip().upper()
    if not s:
        return None
    if s.endswith(".SR"):
        s2 = s[:-3]
        return (s2 + ".SR") if s2.isdigit() and len(s2) == 4 else s
    if s.isdigit() and len(s) == 4:
        return s + ".SR"
    return None

def map_status(raw: str) -> Optional[str]:
    t = str(raw).strip().upper()
    if any(tok in t for tok in FAIL_TOKENS):
        return "FAIL"
    if any(tok in t for tok in PASS_TOKENS):
        return "PASS"
    return None

# --------------------------------------------------------------------------- #
# parsing — dependency-light (regex/plain-text; no bs4)                        #
# --------------------------------------------------------------------------- #
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[ \t\r\f\v]+")

def html_to_text(html: str) -> str:
    txt = _TAG_RE.sub(" ", html)
    txt = txt.replace("&nbsp;", " ").replace("&amp;", "&")
    return _WS_RE.sub(" ", txt)

_SEC_PASS_RE = re.compile(
    r"(?<!غير )(?<!non-)(?<!non )(المتوافقة|المباحة|المجازة|النقية|compliant)", re.I)
_SEC_FAIL_RE = re.compile(r"(غير المتوافقة|المخالفة|غير المجازة|non[- ]?compliant)", re.I)
_CODE_RE = re.compile(r"\b(\d{4})\b")

def parse_official_text(text: str) -> List[Tuple[str, str]]:
    """Split the document into PASS/FAIL sections by Arabic/English headers,
    then harvest 4-digit Tadawul codes per section. Negative lookbehinds keep
    PASS words from matching inside negated FAIL headers (غير المتوافقة /
    non-compliant). Codes seen before any header are ignored (conservative)."""
    marks: List[Tuple[int, str]] = []
    for m in _SEC_FAIL_RE.finditer(text):
        marks.append((m.start(), "FAIL"))
    for m in _SEC_PASS_RE.finditer(text):
        marks.append((m.start(), "PASS"))
    marks.sort()
    out: Dict[str, str] = {}
    for i, (pos, status) in enumerate(marks):
        end = marks[i + 1][0] if i + 1 < len(marks) else len(text)
        for code in _CODE_RE.findall(text[pos:end]):
            sym = normalize_symbol(code)
            if sym and sym not in out:             # first classification wins
                out[sym] = status
    return sorted(out.items())

def parse_csv_text(text: str) -> List[Tuple[str, str, Optional[str]]]:
    rows: List[Tuple[str, str, Optional[str]]] = []
    for line in text.splitlines():
        parts = [p.strip() for p in re.split(r"[,\t;]", line)]
        if len(parts) < 2:
            continue
        sym, st = normalize_symbol(parts[0]), map_status(parts[1])
        if sym and st:
            rows.append((sym, st, parts[2] if len(parts) > 2 and parts[2] else None))
    return rows

# --------------------------------------------------------------------------- #
# network (httpx — already in backend requirements; workflow installs it)      #
# --------------------------------------------------------------------------- #
def fetch_url(url: str, timeout: float = 30.0) -> Tuple[bytes, str]:
    import httpx
    headers = {"User-Agent": "Mozilla/5.0 (TFB shariah_refresh v" + SCRIPT_VERSION + ")"}
    with httpx.Client(follow_redirects=True, timeout=timeout, headers=headers) as c:
        r = c.get(url)
        r.raise_for_status()
        return r.content, (r.headers.get("content-type") or "")

def fetch_official(urls: List[str]) -> Tuple[List[Tuple[str, str]], str, str, str]:
    """Try candidates in order -> (pairs, doc_hash, used_url, note)."""
    last_err = "no_urls_configured"
    for url in urls:
        url = url.strip()
        if not url:
            continue
        try:
            raw, ctype = fetch_url(url)
            h = sha256_hex(raw)
            if "pdf" in ctype.lower() or raw[:4] == b"%PDF":
                return [], h, url, "PARSE_MANUAL_REQUIRED_PDF"
            pairs = parse_official_text(html_to_text(raw.decode("utf-8", "ignore")))
            if pairs:
                return pairs, h, url, "ok"
            last_err = f"parsed_zero_rows:{url}"
        except Exception as e:                      # noqa: BLE001
            last_err = f"{type(e).__name__}:{e}"
    return [], "", "", last_err

def fetch_monitor(urls: List[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for url in urls:
        url = url.strip()
        if not url:
            continue
        try:
            raw, _ = fetch_url(url)
            for sym, st in parse_official_text(html_to_text(raw.decode("utf-8", "ignore"))):
                out.setdefault(sym, st)
        except Exception:                           # noqa: BLE001
            continue                                # monitor is best-effort
    return out

# --------------------------------------------------------------------------- #
# sheets I/O (mirrors track_performance/run_dashboard_sync patterns)           #
# --------------------------------------------------------------------------- #
def _sheet_id(cli: Optional[str]) -> str:
    for v in (cli, os.getenv("TFB_SHARIAH_SHEET_ID"), os.getenv("TARGET_SHEET_ID"),
              os.getenv("TRACK_SHEET_ID"), os.getenv("DEFAULT_SPREADSHEET_ID")):
        if v and str(v).strip():
            return str(v).strip()
    raise SystemExit("::error::Spreadsheet ID missing (TARGET_SHEET_ID / DEFAULT_SPREADSHEET_ID).")

def _gspread_client():
    import gspread
    from google.oauth2.service_account import Credentials
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    raw = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
    b64 = os.getenv("GOOGLE_SHEETS_CREDENTIALS_B64")
    if not path and (raw or b64):
        data = raw or base64.b64decode(b64).decode("utf-8")
        f = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
        f.write(data); f.close()
        path = f.name
    if not path:
        raise SystemExit("::error::Google credentials missing.")
    creds = Credentials.from_service_account_file(path, scopes=scopes)
    return gspread.authorize(creds)

def _open_ws(sh, title: str, header: List[str]):
    try:
        ws = sh.worksheet(title)
    except Exception:                               # noqa: BLE001
        ws = sh.add_worksheet(title=title, rows=200, cols=max(10, len(header)))
        ws.update(values=[header], range_name="A1")
    return ws

def write_authority(sh, pairs: List[Tuple[str, str]], as_of: str, source: str,
                    monitor: Dict[str, str], doc_hash: str, src_url: str) -> Dict[str, int]:
    ws = _open_ws(sh, TAB_AUTH, AUTH_HEADER)
    fetched = _now_utc()
    body = [AUTH_HEADER]
    conflicts = 0
    for sym, st in pairs:
        mon = monitor.get(sym, "")
        if mon and mon != st:
            conflicts += 1
        body.append([sym, st, as_of, source, mon, RULE_VERSION, doc_hash, fetched, src_url])
    ws.clear()
    ws.update(values=body, range_name="A1")
    return {"rows": len(pairs), "conflicts": conflicts,
            "pass": sum(1 for _, s in pairs if s == "PASS"),
            "fail": sum(1 for _, s in pairs if s == "FAIL")}

def read_upload_tab(sh) -> Tuple[List[Tuple[str, str]], Optional[str]]:
    ws = _open_ws(sh, TAB_UPLOAD, UPLOAD_HEADER)
    vals = ws.get_all_values()
    pairs: List[Tuple[str, str]] = []
    as_of: Optional[str] = None
    for row in vals[1:]:
        if not row or not row[0].strip():
            continue
        sym = normalize_symbol(row[0])
        st = map_status(row[1] if len(row) > 1 else "")
        if sym and st:
            pairs.append((sym, st))
        if len(row) > 2 and row[2].strip() and not as_of:
            as_of = row[2].strip()[:10]
    return sorted(set(pairs)), as_of

def append_run_log(sh, status: str, message: str, details: Dict) -> None:
    try:
        ws = sh.worksheet("_Run_Log")
        ws.append_row([_now_utc(), "INFO", "shariah_refresh", TAB_AUTH, status,
                       message, "", "", "", json.dumps(details, ensure_ascii=False)[:900]],
                      value_input_option="RAW")
    except Exception:                               # noqa: BLE001
        pass                                        # logging must never kill the run

# --------------------------------------------------------------------------- #
# main                                                                         #
# --------------------------------------------------------------------------- #
def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="TFB Shariah authority refresher")
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--mode", choices=["auto", "fetch", "upload", "file"], default="auto")
    ap.add_argument("--from-file")
    ap.add_argument("--as-of")
    ap.add_argument("--sheet-id")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-monitor", action="store_true")
    args = ap.parse_args(argv)

    if args.selftest:
        return _selftest()

    official_urls = [u for u in (os.getenv("TFB_SHARIAH_OFFICIAL_URLS") or "").split(",") if u.strip()]
    monitor_urls = [u for u in (os.getenv("TFB_SHARIAH_MONITOR_URLS") or DEFAULT_MONITOR_URLS).split(",")]
    mode = args.mode
    if mode == "auto":
        mode = "fetch" if official_urls else "upload"

    pairs: List[Tuple[str, str]] = []
    source, doc_hash, src_url, as_of = "", "", "", (args.as_of or str(date.today()))

    if mode == "file":
        if not args.from_file:
            print("::error::--mode file requires --from-file"); return 2
        text = open(args.from_file, "r", encoding="utf-8-sig").read()
        rows = parse_csv_text(text)
        pairs = sorted({(s, st) for s, st, _ in rows})
        for _, _, d in rows:
            if d:
                as_of = d; break
        source, doc_hash = "OPERATOR_FILE", sha256_hex(text.encode())
        src_url = os.path.basename(args.from_file)
    elif mode == "fetch":
        pairs, doc_hash, src_url, note = fetch_official(official_urls)
        source = "AL_RAJHI_OFFICIAL"
        if not pairs:
            print(f"::warning::official fetch yielded no rows ({note}); "
                  f"falling back to upload tab")
            mode = "upload"
    if mode == "upload":
        sh = None if args.dry_run else _client_sheet(args.sheet_id)
        if sh is None and args.dry_run:
            print("::error::upload mode cannot dry-run without sheet access"); return 2
        pairs, up_as_of = read_upload_tab(sh)
        if up_as_of:
            as_of = up_as_of
        source, doc_hash, src_url = "OPERATOR_UPLOAD_TAB", "", TAB_UPLOAD
        if not pairs:
            msg = (f"[shariah_refresh v{SCRIPT_VERSION}] NO SOURCE: paste the official "
                   f"list into '{TAB_UPLOAD}' (Symbol, Status) then dispatch again — "
                   f"or set repo Variable TFB_SHARIAH_OFFICIAL_URLS (E0.4).")
            print("::warning::" + msg)
            append_run_log(sh, "NO_SOURCE", msg, {"version": SCRIPT_VERSION})
            return 0

    print(f"[shariah_refresh v{SCRIPT_VERSION}] mode={mode} rows={len(pairs)} "
          f"as_of={as_of} source={source} hash={doc_hash[:12]}")
    if args.dry_run:
        for sym, st in pairs[:20]:
            print("  ", sym, st)
        return 0

    sh = _client_sheet(args.sheet_id)
    monitor = {} if args.no_monitor else fetch_monitor(monitor_urls)
    stats = write_authority(sh, pairs, as_of, source, monitor, doc_hash, src_url)
    verdict = (f"[SHARIAH-AUTHORITY v{SCRIPT_VERSION}] rows={stats['rows']} "
               f"pass={stats['pass']} fail={stats['fail']} conflicts={stats['conflicts']} "
               f"source={source} as_of={as_of}")
    print(verdict)
    append_run_log(sh, "OK" if stats["conflicts"] == 0 else "CONFLICTS", verdict,
                   {**stats, "hash": doc_hash, "url": src_url, "version": SCRIPT_VERSION})
    return 0

def _client_sheet(sheet_id_cli: Optional[str]):
    gc = _gspread_client()
    return gc.open_by_key(_sheet_id(sheet_id_cli))

# --------------------------------------------------------------------------- #
# SELFTEST (offline)                                                           #
# --------------------------------------------------------------------------- #
_FIXTURE_HTML = """
<html><body><h2>قائمة الشركات المتوافقة مع الضوابط الشرعية</h2>
<td>7010</td><td>الاتصالات السعودية</td> <td>2222</td> <td>4200</td>
<h2>قائمة الشركات غير المتوافقة مع الضوابط</h2>
<td>1120</td><td>مصرف الراجحي؟ لا — بنك تقليدي مثال</td> <td>1180</td>
</body></html>
"""
_FIXTURE_CSV = "Symbol,Status,AsOf\n7010,متوافقة,2026-07-15\n1120.SR,غير متوافقة,\n9628,PASS,\nBADX,PASS,\n"

def _selftest() -> int:
    checks: List[Tuple[str, bool]] = []
    pairs = parse_official_text(html_to_text(_FIXTURE_HTML))
    d = dict(pairs)
    checks.append(("html: pass section harvested", d.get("7010.SR") == "PASS" and d.get("2222.SR") == "PASS" and d.get("4200.SR") == "PASS"))
    checks.append(("html: fail section harvested", d.get("1120.SR") == "FAIL" and d.get("1180.SR") == "FAIL"))
    checks.append(("html: exactly 5 unique symbols", len(pairs) == 5))
    rows = parse_csv_text(_FIXTURE_CSV)
    dd = {s: st for s, st, _ in rows}
    checks.append(("csv: arabic statuses mapped", dd.get("7010.SR") == "PASS" and dd.get("1120.SR") == "FAIL"))
    checks.append(("csv: 4-digit normalized + Nomu accepted", dd.get("9628.SR") == "PASS"))
    checks.append(("csv: junk symbol rejected", "BADX" not in dd and len(rows) == 3))
    checks.append(("csv: as_of picked", any(x == "2026-07-15" for *_, x in rows)))
    checks.append(("normalize: idempotent .SR", normalize_symbol("2222.SR") == "2222.SR"))
    checks.append(("status: english tokens", map_status("Non-Compliant") == "FAIL" and map_status("PASS") == "PASS"))
    h1, h2 = sha256_hex(b"abc"), sha256_hex(b"abc")
    checks.append(("hash: stable sha256", h1 == h2 and len(h1) == 64))
    mon = {"7010.SR": "FAIL", "2222.SR": "PASS"}
    conflicts = sum(1 for s, st in pairs if mon.get(s) and mon[s] != st)
    checks.append(("monitor: conflict counted once", conflicts == 1))

    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"[shariah_refresh v{SCRIPT_VERSION}] SELFTEST {passed}/{len(checks)}")
    return 0 if passed == len(checks) else 1

if __name__ == "__main__":
    sys.exit(main())
