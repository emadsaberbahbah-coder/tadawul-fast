# -*- coding: utf-8 -*-
"""tests/test_sync_eodhd_quota_p154.py

P-154 - run_dashboard_sync v6.60.0 EODHD QUOTA SENTINEL (observe-only).

Executes the REAL module functions. The provider usage endpoint is emulated
by a local HTTP server, so the real urllib poll path runs end to end (no
mock of the code under test). The only double is the Google Sheets service
boundary (a recorder).

Run:  python tests/test_sync_eodhd_quota_p154.py
      pytest -q tests/test_sync_eodhd_quota_p154.py
Optional:
  TFB_P154_TARGET=/path/to/run_dashboard_sync.py
  TFB_P154_BASE=/path/to/run_dashboard_sync_v6.59.0.py   (dual-tree AST proof)
  TFB_P154_EXPORT_DIR=/dir/with/the/2026-09-21/TSV/exports (real-row counts)
"""
import ast
import contextlib
import csv
import glob
import hashlib
import importlib.util
import io
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

_HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.environ.get("TFB_P154_TARGET") or os.path.join(
    os.path.dirname(_HERE), "scripts", "run_dashboard_sync.py")
BASE = os.environ.get("TFB_P154_BASE") or ""
EXPORT_DIR = os.environ.get("TFB_P154_EXPORT_DIR") or ""
GATE = "TFB_SYNC_EODHD_QUOTA"
SECRET = "SECRET-TOKEN-158-DO-NOT-LOG"
_ENV_KEYS = (GATE, "TFB_SYNC_EODHD_QUOTA_URL", "TFB_SYNC_EODHD_QUOTA_TIMEOUT_S",
             "EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_KEY",
             "TFB_SYNC_EODHD_QUOTA_WARN_PCT", "TFB_SYNC_EODHD_QUOTA_CRIT_PCT")


def _load(path, name):
    # The sync imports its sibling scripts/critical_symbol_identity.py; make
    # the target's own directory importable (the "direct run" layout).
    d = os.path.dirname(os.path.abspath(path))
    if d not in sys.path:
        sys.path.insert(0, d)
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# ------------------------------------------------ usage-endpoint emulator
class _Usage(BaseHTTPRequestHandler):
    state = {"used": 321500, "limit": 400000, "extra": 0, "mode": "ok",
             "hits": 0, "queries": []}

    def log_message(self, *a):   # keep the test output clean
        pass

    def do_GET(self):
        st = _Usage.state
        st["hits"] += 1
        st["queries"].append(self.path)
        if st["mode"] == "401":
            self.send_response(401)
            self.end_headers()
            self.wfile.write(b"Unauthenticated")
            return
        if st["mode"] == "slow":
            time.sleep(3.0)
        body = json.dumps({
            "name": "x", "apiRequests": str(st["used"]),
            "apiRequestsDate": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "dailyRateLimit": st["limit"], "extraLimit": st["extra"],
        }).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        try:
            self.wfile.write(body)
        except Exception:
            pass


# ------------------------------------------------ Sheets boundary recorder
class _Exec(object):
    def __init__(self, rec, kw):
        self.rec, self.kw = rec, kw

    def execute(self):
        if self.rec.fail:
            raise RuntimeError("sheets down")
        self.rec.appends.append(self.kw)
        return {}


class _Svc(object):
    def __init__(self):
        self.appends, self.fail = [], False

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def append(self, **kw):
        return _Exec(self, kw)


class _Sheets(object):
    def __init__(self):
        self.svc = _Svc()

    def _get_service(self):
        return self.svc


def _reset(mod):
    mod._EQ_STATE.update({"prev_used": None, "prev_date": None,
                          "prev_ts": None, "prev_page": None})


def _matrix(path):
    with open(path, encoding="utf-8", newline="") as fh:
        rd = csv.reader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)
        rows = [r for r in rd]
    return rows[0], rows[1:]


HDR = ["Symbol", "Warnings", "Last Updated (UTC)"]
QUIET = [["A.US", "yahoo_enrichment_applied", "2026-09-21T05:01:00+00:00"]]


def run_all():
    res = {}
    saved = {k: os.environ.get(k) for k in _ENV_KEYS}
    for k in _ENV_KEYS:
        os.environ.pop(k, None)
    srv = HTTPServer(("127.0.0.1", 0), _Usage)
    th = threading.Thread(target=srv.serve_forever, daemon=True)
    th.start()
    url = "http://127.0.0.1:%d/api/user" % srv.server_address[1]
    logbuf = io.StringIO()
    handler = logging.StreamHandler(logbuf)
    logging.getLogger().addHandler(handler)
    out = io.StringIO()
    try:
        with contextlib.redirect_stdout(out):
            new = _load(TARGET, "sync_p154_new")
            assert new.SCRIPT_VERSION >= "6.60.0", new.SCRIPT_VERSION

            # K1 - gate OFF: no network, no append, nothing printed by us.
            os.environ["TFB_SYNC_EODHD_QUOTA_URL"] = url
            os.environ["EODHD_API_KEY"] = SECRET
            for off in (None, "1", "true", "on", "enforce"):
                if off is None:
                    os.environ.pop(GATE, None)
                else:
                    os.environ[GATE] = off
                assert new._eodhd_quota_mode() == "off", off
                sh = _Sheets()
                new._append_runlog_eodhd_quota(sh, "sid", "Global_Markets", HDR, QUIET)
                assert sh.svc.appends == [] and _Usage.state["hits"] == 0
            res["K1_off"] = "no call, no line"

            # K2 - pure parse.
            p = new._eodhd_quota_parse(
                {"apiRequests": "321500", "dailyRateLimit": 400000,
                 "extraLimit": "0", "apiRequestsDate": "2026-09-20"}, "2026-09-20")
            assert (p["ok"], p["used"], p["limit"], p["pct"]) == (True, 321500, 400000, 80.4)
            st = new._eodhd_quota_parse(
                {"apiRequests": 399999, "dailyRateLimit": 400000,
                 "apiRequestsDate": "2026-09-20"}, "2026-09-21")
            assert st["stale_date"] and st["used"] == 0
            assert not new._eodhd_quota_parse("junk", "x")["ok"]
            res["K2_parse"] = [p["pct"], st["stale_date"]]

            # K3 - the REAL 2026-09-21 export rows (when provided).
            if EXPORT_DIR:
                got = {}
                t_run = datetime(2026, 9, 21, 4, 25, tzinfo=timezone.utc).timestamp()
                t_prev = datetime(2026, 9, 20, 22, 0, tzinfo=timezone.utc).timestamp()
                for page in ("Global_Markets", "Mutual_Funds", "Commodities_FX",
                             "Market_Leaders"):
                    f = glob.glob(os.path.join(EXPORT_DIR, "*-_%s.tsv" % page))
                    assert f, page
                    hdr, mat = _matrix(f[0])
                    a = new._eodhd_quota_count_rows(hdr, mat, t_run)
                    b = new._eodhd_quota_count_rows(hdr, mat, t_prev)
                    got[page] = [a, b["q402_new"]]
                gm = got["Global_Markets"][0]
                assert gm["q402_carried"] == 845 and gm["q402_new"] == 0, gm
                assert got["Global_Markets"][1] == 845, "same rows read as NEW from the night leg's clock"
                assert gm["f404"] == 47, gm
                assert got["Mutual_Funds"][0]["q402_carried"] == 1
                assert got["Market_Leaders"][0]["fetch_failed"] == 0
                cfx = got["Commodities_FX"][0]
                assert cfx["q402_new"] + cfx["q402_carried"] == 0 and cfx["fetch_failed"] == 35
                res["K3_real_export"] = got

            # K5 - observe, live counter at 80.4% -> WARN.
            os.environ[GATE] = "observe"
            _reset(new)
            sh = _Sheets()
            new._append_runlog_eodhd_quota(sh, "sid", "Market_Leaders", HDR, QUIET)
            assert len(sh.svc.appends) == 1 and _Usage.state["hits"] == 1
            kw = sh.svc.appends[0]
            row = kw["body"]["values"][0]
            assert kw["range"] == "'_Run_Log'!A1" and len(row) == 10
            assert row[1] == "WARNING" and row[2] == "run_dashboard_sync"
            assert row[3] == "Market_Leaders" and row[4] == "WARN"
            assert "used=321500/400000 (80.4%)" in row[5]
            assert "delta=first-sample" in row[5] and "selftest=PASS 5/5" in row[5]
            det = json.loads(row[9])
            assert det["state"] == "WARN" and det["used"] == 321500
            assert "run_id" in det and "ts_utc" in det, "RUN-META convention"
            res["K5_line"] = row[5]

            # K6 - second sample -> delta.
            _Usage.state["used"] += 4321
            new._append_runlog_eodhd_quota(sh, "sid", "Global_Markets", HDR, QUIET)
            m2 = sh.svc.appends[1]["body"]["values"][0][5]
            assert "delta=+4321 in " in m2 and "since Market_Leaders" in m2, m2
            res["K6_delta"] = m2.split(" | ")[2].split(" in ")[0]

            # K7 - failure modes never raise, never stall.
            _Usage.state["mode"] = "401"
            new._append_runlog_eodhd_quota(sh, "sid", "Commodities_FX", HDR, QUIET)
            r401 = sh.svc.appends[2]["body"]["values"][0]
            assert "used=unknown (poll_failed:HTTPError:401)" in r401[5] and r401[4] == "UNKNOWN"
            _Usage.state["mode"] = "slow"
            os.environ["TFB_SYNC_EODHD_QUOTA_TIMEOUT_S"] = "1"
            t0 = time.time()
            new._append_runlog_eodhd_quota(sh, "sid", "Mutual_Funds", HDR, QUIET)
            took = time.time() - t0
            rslow = sh.svc.appends[3]["body"]["values"][0]
            assert "used=unknown (poll_failed:" in rslow[5] and took < 2.9, took
            _Usage.state["mode"] = "ok"
            os.environ.pop("TFB_SYNC_EODHD_QUOTA_TIMEOUT_S", None)
            os.environ.pop("EODHD_API_KEY", None)
            new._append_runlog_eodhd_quota(sh, "sid", "Mutual_Funds", HDR, QUIET)
            assert "used=unknown (no_key)" in sh.svc.appends[4]["body"]["values"][0][5]
            os.environ["EODHD_API_KEY"] = SECRET
            res["K7_failures"] = [r401[4], rslow[4], "no_key"]

            # K9 - a fresh 402 row IS exhaustion, whatever the counter says.
            fresh = [["Z.US", "fetch_failed:HTTP 402",
                      datetime.now(timezone.utc).isoformat()]]
            _Usage.state["used"] = 1000
            new._append_runlog_eodhd_quota(sh, "sid", "Global_Markets", HDR, fresh)
            r9 = sh.svc.appends[5]["body"]["values"][0]
            assert r9[4] == "EXHAUSTED" and r9[1] == "WARNING" and "rows402 new=1" in r9[5]
            res["K9_exhausted"] = r9[4]

            # K11 - the sentinel's own append failure is annotated, not counted.
            sh.svc.fail = True
            before = list(new._RUNLOG_APPEND_FAILS)
            new._append_runlog_eodhd_quota(sh, "sid", "Global_Markets", HDR, QUIET)
            assert new._RUNLOG_APPEND_FAILS == before
            sh.svc.fail = False
            res["K11_append_fail"] = "annotated, exit code untouched"

        # K8 - token hygiene across every surface we emitted on.
        surfaces = out.getvalue() + logbuf.getvalue() + json.dumps(
            [a["body"] for a in sh.svc.appends])
        assert SECRET not in surfaces, "token leaked"
        assert any(SECRET in q for q in _Usage.state["queries"]), "poll did send the key"
        assert "::warning::" in out.getvalue()
        res["K8_token"] = "not present in stdout / logging / sheet payloads"

        # K10 - dual-tree AST: zero removals, one touched pre-existing def.
        if BASE:
            def defs(p):
                d = {}
                for n in ast.walk(ast.parse(open(p, encoding="utf-8").read())):
                    if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                        d.setdefault(n.name, []).append(ast.dump(n))
                return d
            fb, fd = defs(BASE), defs(TARGET)
            assert not (set(fb) - set(fd))
            changed = sorted(k for k in fb if fb[k] != fd.get(k))
            assert changed == ["_run_one_task"], changed
            res["K10_ast"] = {"removed": 0, "added": len(set(fd) - set(fb)),
                              "touched": changed}
    finally:
        logging.getLogger().removeHandler(handler)
        srv.shutdown()
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    return res


def test_p154_eodhd_quota_sentinel():
    run_all()


if __name__ == "__main__":
    r = run_all()
    stable = {k: v for k, v in r.items() if k not in ("K6_delta",)}
    for k in sorted(r):
        print(k, "->", json.dumps(r[k], sort_keys=True, default=str)[:240])
    print("PASS | digest", hashlib.sha256(
        json.dumps(stable, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16])
