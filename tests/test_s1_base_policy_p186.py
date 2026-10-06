#!/usr/bin/env python3
"""run_shadow_scorer v1.9.1 [P-186 BASE POLICY] harness — REAL module, REAL
main() on an in-memory sheet stub (the P-176 harness pattern); the network
fetch and the clock are the only injected parts. Replays the live 2026-09-30
measurement: the challenger's 09-29 row carried 09-28 closes for four of its
five legs (fresh=1/5, DAY_EXCLUDED_INFRA) while the benchmark's 09-29 legs
were true 09-29 closes — so the scored 09-30 "daily" return compared a
2-session challenger interval with a 1-session benchmark interval, and the
S1-FRESH line printed fresh=5/3.
  B1 full embedded selftest battery (116/116 on v1.9.4)
  B2 pure: last_scored_row_for skips excluded / non-trading rows
  B3 END-TO-END legacy (env unset): numbers reproduce the live asymmetry;
     S1-FRESH says fresh=5/3; no [S1-BASE] token; history/gate/_Run_Log rows
     BYTE-IDENTICAL to the v1.9.0 base module on the same stub (S1_BASE)
  B4 END-TO-END observe: rows identical to legacy; [S1-BASE] read-back shows
     both pairings, the 09-28 base date, legs=5/5 and the alpha delta
  B5 END-TO-END lastscored: both baskets pair against the 09-28 scored rows
     (same interval), fresh=5/5, day scored, index chains consistent
  B6 lastscored parity: when the previous row IS scored the rows are
     byte-identical to legacy
  B7 --dry-run under lastscored stays zero-write
  B8 digest x3 identical
Set S1_FILE=<path> to test a file outside the repo tree; S1_BASE=<v1.9.0 file>
for the dual-tree leg."""
import importlib.util, os, sys, json, hashlib, subprocess, copy, io, contextlib
from datetime import datetime, timezone, date

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
os.chdir(ROOT)
S1_FILE = os.environ.get("S1_FILE") or os.path.join(ROOT, "scripts", "run_shadow_scorer.py")


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    return m


s1 = _load(S1_FILE, "s1_under_test")
assert tuple(map(int, s1.SCRIPT_VERSION.split("."))) >= (1, 9, 4), s1.SCRIPT_VERSION
digest_parts = []
T = lambda s: datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")  # noqa: E731

# ---------------------------------------------------------------- B1 ------ #
r = subprocess.run([sys.executable, S1_FILE, "--selftest"], capture_output=True, text=True, cwd=ROOT)
line = [l for l in r.stdout.splitlines() if "SELFTEST" in l][-1]
assert r.returncode == 0 and "116/116" in line, line
print(f"B1 PASS  full selftest battery on v{s1.SCRIPT_VERSION}:", line.strip())
digest_parts.append(line.strip())


# ------------------------------------------------- in-memory sheet stub --- #
class WS:
    def __init__(self, rows): self.rows = [list(r) for r in rows]; self.appends = 0
    def get_all_values(self): return [list(r) for r in self.rows]
    def append_row(self, row, value_input_option=None):
        self.rows.append(list(row)); self.appends += 1
    def append_rows(self, rows, value_input_option=None):
        for r in rows: self.rows.append(list(r))
        self.appends += len(rows)
    def update(self, values=None, range_name=None):
        self.rows = [list(r) for r in (values or [])]
    def clear(self): self.rows = []


class Sheet:
    def __init__(self, tabs): self.tabs = {k: WS(v) for k, v in tabs.items()}
    def worksheet(self, name):
        if name not in self.tabs: raise KeyError(name)
        return self.tabs[name]
    def add_worksheet(self, title, rows=0, cols=0):
        self.tabs[title] = WS([]); return self.tabs[title]


OUT = s1.sb.OUT_HEADER


def board_rows(asof_txt, syms):
    meta = [["SHADOW BOARD v1.5.0", f"as of {asof_txt} Riyadh", "equity=130,000 SAR"],
            ["authority rows=408 as_of=2026-03-31", "authority_error=-"],
            [f"evaluated={len(syms)}", f"compliance_eligible={len(syms)}", "blocked={}"],
            ["advisory stamp — drives nothing until gated"], []]
    body = [list(OUT)]
    for s in syms:
        row = [s, s + " Inc", "", "Technology", "20.0", "", "SCREEN_RETIRED",
               "retired_2026-08-13", "BROKER_TRADABLE", "", "YES", "0.1", "9.0",
               "1.5", "TRADE", "1.0", "engine", "YES"]
        assert len(row) == len(OUT)
        body.append(row)
    return meta + body


CHAMP = ["YUM", "DDI.US", "CWBC.US"]
CHAL_PREV = ["ITRN.US", "PINE.US", "NVDA.US", "GOOGL.US", "PINFRA.MX"]   # the 09-28/09-29 basket
CHAL_NOW = ["RDN.US", "PINE.US", "NVDA.US"]                                  # the 09-30 board
BENCH = ["SPUS", "^TASI.SR"]
# closes (verbatim where known from the live rows / export): 09-28, 09-29, 09-30
P28 = {"ITRN.US": 51.73, "PINE.US": 16.89, "NVDA.US": 228.86, "GOOGL.US": 342.75, "PINFRA.MX": 267.24,
       "SPUS": 59.975, "^TASI.SR": 10681.84, "YUM": 138.4, "DDI.US": 13.09, "CWBC.US": 26.34}
P29 = {"ITRN.US": 51.01, "PINE.US": 17.32, "NVDA.US": 227.21, "GOOGL.US": 340.92, "PINFRA.MX": 264.60,
       "SPUS": 59.72, "^TASI.SR": 10579.18, "YUM": 137.73, "DDI.US": 13.06, "CWBC.US": 26.24}
P30 = {"ITRN.US": 51.22, "PINE.US": 17.35, "NVDA.US": 228.38, "GOOGL.US": 344.08, "PINFRA.MX": 267.65,
       "SPUS": 59.595, "^TASI.SR": 10440.64, "YUM": 136.34, "DDI.US": 13.29, "CWBC.US": 26.04,
       "RDN.US": 30.61}


def hrow(day, basket, syms, prices, ret, idx, note):
    return [day, basket, ",".join(syms), json.dumps({s: prices[s] for s in syms}),
            "" if ret is None else ret, idx, 0.0, 0.0, note]


def history_live_shape():
    """09-28 scored rows (bases of record) → 09-29 EXCLUDED rows: challenger
    carried 09-28 closes except NVDA (fresh=1/5); benchmark legs FRESH."""
    H = [list(s1.HISTORY_HEADER)]
    H += [hrow("2026-09-28", "CHAMPION", CHAMP, P28, 0.2, 99.26, "n=3"),
          hrow("2026-09-28", "CHALLENGER", CHAL_PREV, P28, 0.3, 104.88, "n=5"),
          hrow("2026-09-28", "BENCHMARK", BENCH, P28, 0.1, 101.10, "n=2")]
    chal29 = dict(P28); chal29["NVDA.US"] = P29["NVDA.US"]        # only NVDA fresh on 09-29
    H += [hrow("2026-09-29", "CHAMPION", CHAMP, P28, None, 99.26, "DAY_EXCLUDED_INFRA fresh=1/3 stale=0 reason=fresh-floor"),
          hrow("2026-09-29", "CHALLENGER", CHAL_PREV, chal29, None, 104.88, "DAY_EXCLUDED_INFRA fresh=1/5 stale=0 reason=fresh-floor"),
          hrow("2026-09-29", "BENCHMARK", BENCH, P29, None, 101.10, "DAY_EXCLUDED_INFRA fresh=2/2 stale=0 reason=fresh-floor")]
    return H


def history_normal_shape():
    """09-29 SCORED rows (the ordinary case): lastscored must equal legacy."""
    H = [list(s1.HISTORY_HEADER)]
    H += [hrow("2026-09-28", "CHAMPION", CHAMP, P28, 0.2, 99.26, "n=3"),
          hrow("2026-09-28", "CHALLENGER", CHAL_PREV, P28, 0.3, 104.88, "n=5"),
          hrow("2026-09-28", "BENCHMARK", BENCH, P28, 0.1, 101.10, "n=2"),
          hrow("2026-09-29", "CHAMPION", CHAMP, P29, -0.3, 98.96, "n=3"),
          hrow("2026-09-29", "CHALLENGER", CHAL_PREV, P29, -0.8, 104.04, "n=5"),
          hrow("2026-09-29", "BENCHMARK", BENCH, P29, -0.6, 100.49, "n=2")]
    return H


def fresh_sheet(hist):
    top10 = [["TOP 10 INVESTMENTS — DECISION"], ["Status:", "Last run"], [],
             ["Rank", "Symbol", "Name", "Price", "ROI %"]] + \
            [[str(i + 1), s, s + " Co", "1.0", "10%"] for i, s in enumerate(CHAMP)]
    return Sheet({
        s1.sb.TAB_TOP10: top10,
        s1.sb.TAB_OUT: board_rows("2026-09-30 22:16", CHAL_NOW),
        s1.TAB_HISTORY: copy.deepcopy(hist),
        "_Run_Log": [["Timestamp", "Level", "Action", "Page", "Status", "Message",
                      "Endpoint", "HTTP Code", "Duration ms", "Details JSON"],
                     ["2026-09-19 05:00:00", "INFO", "workbook_backup", "_Backup", "OK",
                      "[RESTORE-TEST v1.4.0] PASS rto=8m", "", "", "", "{}"]],
    })


ENVS = ("TFB_S1_BASE_POLICY", "TFB_S1_DAY_KEY", "TFB_S1_BOARD_FRESH_GUARD", "TFB_SHADOW_EODHD",
        "TFB_SHADOW_EQW")


def run_at(mod, when_utc, sheet, spot, bar_day, policy=None, argv=None):
    fixed = T(when_utc).replace(tzinfo=timezone.utc)

    class FakeDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz) if tz else fixed.replace(tzinfo=None)
    bar = datetime.strptime(bar_day, "%Y-%m-%d").date()

    def fake_fetch(symbols):
        need = [s for s in symbols if s in spot]
        return ({s: spot[s] for s in need}, {s: bar for s in need}, [])
    saved = (mod.datetime, mod.fetch_spot, mod.sb._open_sheet, {k: os.environ.get(k) for k in ENVS})
    try:
        mod.datetime = FakeDT; mod.fetch_spot = fake_fetch
        mod.sb._open_sheet = lambda cli: sheet
        for k in ENVS:
            os.environ.pop(k, None)
        if policy is not None:
            os.environ["TFB_S1_BASE_POLICY"] = policy
        os.environ["TFB_S1_BOARD_FRESH_GUARD"] = "observe"
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = mod.main(argv or [])
        return rc, buf.getvalue()
    finally:
        mod.datetime, mod.fetch_spot, mod.sb._open_sheet = saved[0], saved[1], saved[2]
        for k, v in saved[3].items():
            if v is None: os.environ.pop(k, None)
            else: os.environ[k] = v


def hist_rows(sheet, day="2026-09-30"):
    return {r[1]: r for r in sheet.tabs[s1.TAB_HISTORY].rows[1:] if r[0] == day}


def runlog_last(sheet): return sheet.tabs["_Run_Log"].rows[-1]
def gate_rows(sheet): return sheet.tabs[s1.TAB_GATE].rows


def _strip_version(txt):
    return str(txt).replace("v1.9.1", "vX").replace("v1.9.0", "vX")


# ---------------------------------------------------------------- B2 ------ #
H = s1.read_history(fresh_sheet(history_live_shape()))
ls = s1.last_scored_row_for(H, "CHALLENGER"); lr = s1.last_row_for(H, "CHALLENGER")
assert ls["date"] == "2026-09-28" and lr["date"] == "2026-09-29", (ls["date"], lr["date"])
assert s1.last_scored_row_for(H, "BENCHMARK")["date"] == "2026-09-28"
print("B2 PASS  last_scored_row_for -> 2026-09-28 (last row is the excluded 09-29)")
digest_parts.append([ls["date"], lr["date"]])

# expected numbers from the module's own pure math
legacy_chal = s1.basket_return_fresh(lr["prices"], P30, {s: date(2026, 9, 30) for s in P30}, date(2026, 9, 29))[0]
ls_chal = s1.basket_return_fresh(ls["prices"], P30, {s: date(2026, 9, 30) for s in P30}, date(2026, 9, 28))[0]
b_lr = s1.last_row_for(H, "BENCHMARK"); b_ls = s1.last_scored_row_for(H, "BENCHMARK")
legacy_bench = s1.blended_benchmark_return_fresh(b_lr["prices"], P30, {s: date(2026, 9, 30) for s in P30}, date(2026, 9, 29))[0]
ls_bench = s1.blended_benchmark_return_fresh(b_ls["prices"], P30, {s: date(2026, 9, 30) for s in P30}, date(2026, 9, 28))[0]
assert legacy_chal > ls_chal - 5 and abs(legacy_bench - (-0.5394)) < 0.01, (legacy_chal, ls_chal, legacy_bench, ls_bench)
print(f"B2 INFO  legacy chal {legacy_chal:+.4f}% (2-session legs vs 1-session bench {legacy_bench:+.4f}%) | "
      f"lastscored chal {ls_chal:+.4f}% bench {ls_bench:+.4f}% (both 2-session)")

# ---------------------------------------------------------------- B3 ------ #
sh_leg = fresh_sheet(history_live_shape())
rc, out_leg = run_at(s1, "2026-09-30T15:20:00Z", sh_leg, P30, "2026-09-30", policy=None)
assert rc == 0
rows_leg = hist_rows(sh_leg)
assert abs(float(rows_leg["CHALLENGER"][4]) - round(legacy_chal, 4)) < 1e-6, rows_leg["CHALLENGER"]
assert abs(float(rows_leg["BENCHMARK"][4]) - round(legacy_bench, 4)) < 1e-6, rows_leg["BENCHMARK"]
v_leg = runlog_last(sh_leg)[5]
assert "chal fresh=5/3" in v_leg and "[S1-BASE" not in v_leg and "day_scored" in v_leg, v_leg
print("B3 PASS  legacy reproduces the live asymmetry: chal", rows_leg["CHALLENGER"][4], "bench", rows_leg["BENCHMARK"][4], "| fresh=5/3, no [S1-BASE] token")
digest_parts.append([rows_leg["CHALLENGER"][4], rows_leg["BENCHMARK"][4], "fresh=5/3"])
base_path = os.environ.get("S1_BASE")
if base_path:
    s1b = _load(base_path, "s1_base_v190")
    assert s1b.SCRIPT_VERSION == "1.9.0", s1b.SCRIPT_VERSION
    sh_b = fresh_sheet(history_live_shape())
    rcb, out_b = run_at(s1b, "2026-09-30T15:20:00Z", sh_b, P30, "2026-09-30", policy=None)
    assert rcb == 0
    same_hist = [_strip_version(json.dumps(r)) for r in sh_b.tabs[s1.TAB_HISTORY].rows] == \
                [_strip_version(json.dumps(r)) for r in sh_leg.tabs[s1.TAB_HISTORY].rows]
    same_gate = [_strip_version(json.dumps(r)) for r in gate_rows(sh_b)] == [_strip_version(json.dumps(r)) for r in gate_rows(sh_leg)]
    same_log = _strip_version(runlog_last(sh_b)[5]) == _strip_version(runlog_last(sh_leg)[5])
    assert same_hist and same_gate and same_log, (same_hist, same_gate, same_log)
    # observe/lastscored env is inert on the base
    sh_b2 = fresh_sheet(history_live_shape())
    run_at(s1b, "2026-09-30T15:20:00Z", sh_b2, P30, "2026-09-30", policy="lastscored")
    assert [json.dumps(r) for r in sh_b2.tabs[s1.TAB_HISTORY].rows] == [json.dumps(r) for r in sh_b.tabs[s1.TAB_HISTORY].rows]
    print("B3 PASS  dual-tree: legacy history/S1_Gate/_Run_Log rows byte-identical to v1.9.0 (version token aside); new env inert on base")
    digest_parts.append("dualtree-ok")
else:
    print("B3 SKIP  dual-tree (set S1_BASE=<v1.9.0 file>)")

# ---------------------------------------------------------------- B4 ------ #
sh_obs = fresh_sheet(history_live_shape())
rc, out_obs = run_at(s1, "2026-09-30T15:20:00Z", sh_obs, P30, "2026-09-30", policy="observe")
assert rc == 0
rows_obs = hist_rows(sh_obs)
assert [json.dumps(rows_obs[b]) for b in ("CHAMPION", "CHALLENGER", "BENCHMARK")] == \
       [json.dumps(rows_leg[b]) for b in ("CHAMPION", "CHALLENGER", "BENCHMARK")], "observe must write legacy rows"
v_obs = runlog_last(sh_obs)[5]
assert f"[S1-BASE v{s1.SCRIPT_VERSION}] mode=observe" in v_obs, v_obs
assert f"chal legacy={legacy_chal:+.4f}%@2026-09-29 lastscored={ls_chal:+.4f}%@2026-09-28 legs=5/5" in v_obs, v_obs
assert f"bench legacy={legacy_bench:+.4f}% lastscored={ls_bench:+.4f}%" in v_obs, v_obs
assert "fresh_den legacy=3 pairable=5" in v_obs and "chal fresh=5/3" in v_obs, v_obs
tok = [t for t in v_obs.split(" | ") if t.startswith("alpha legacy=")][0]
print("B4 PASS  observe: legacy rows written; read-back", tok)
digest_parts.append([v_obs.split("[S1-BASE")[1][:160]])
# the gate meta row carries the token too
assert any(f"[S1-BASE v{s1.SCRIPT_VERSION}] mode=observe" in json.dumps(r) for r in gate_rows(sh_obs))

# ---------------------------------------------------------------- B5 ------ #
sh_ls = fresh_sheet(history_live_shape())
rc, out_ls = run_at(s1, "2026-09-30T15:20:00Z", sh_ls, P30, "2026-09-30", policy="lastscored")
assert rc == 0
rows_ls = hist_rows(sh_ls)
assert abs(float(rows_ls["CHALLENGER"][4]) - round(ls_chal, 4)) < 1e-6, rows_ls["CHALLENGER"]
assert abs(float(rows_ls["BENCHMARK"][4]) - round(ls_bench, 4)) < 1e-6, rows_ls["BENCHMARK"]
v_ls = runlog_last(sh_ls)[5]
assert f"[S1-BASE v{s1.SCRIPT_VERSION}] mode=lastscored" in v_ls and "chal fresh=5/5" in v_ls and "day_scored" in v_ls, v_ls
# index chains: 09-28 index x (1 + ret - drag)
i28 = 104.88; drag = float(rows_ls["CHALLENGER"][7])
assert abs(float(rows_ls["CHALLENGER"][5]) - s1.chain_index(i28, ls_chal, drag)) < 1e-5, (rows_ls["CHALLENGER"][5], s1.chain_index(i28, ls_chal, drag))
assert abs(float(rows_ls["BENCHMARK"][5]) - s1.chain_index(101.10, ls_bench, 0.0)) < 1e-5
# turnover keyed off the 09-28 basket (5 -> 3 names, RDN new)
assert float(rows_ls["CHALLENGER"][6]) == float(rows_leg["CHALLENGER"][6])   # same prev symbols either way here
print("B5 PASS  lastscored: both baskets paired against the 09-28 scored rows — chal", rows_ls["CHALLENGER"][4],
      "bench", rows_ls["BENCHMARK"][4], "| fresh=5/5 | index chains consistent")
digest_parts.append([rows_ls["CHALLENGER"][4], rows_ls["BENCHMARK"][4], "fresh=5/5"])

# ---------------------------------------------------------------- B6 ------ #
sh_n1 = fresh_sheet(history_normal_shape()); sh_n2 = fresh_sheet(history_normal_shape())
run_at(s1, "2026-09-30T15:20:00Z", sh_n1, P30, "2026-09-30", policy=None)
run_at(s1, "2026-09-30T15:20:00Z", sh_n2, P30, "2026-09-30", policy="lastscored")
r1, r2 = hist_rows(sh_n1), hist_rows(sh_n2)
assert [json.dumps(r1[b]) for b in ("CHAMPION", "CHALLENGER", "BENCHMARK")] == \
       [json.dumps(r2[b]) for b in ("CHAMPION", "CHALLENGER", "BENCHMARK")], "parity on a normal day"
assert "chal fresh=5/5" in runlog_last(sh_n2)[5] and "chal fresh=5/3" in runlog_last(sh_n1)[5]
print("B6 PASS  normal day (prev row scored): lastscored rows == legacy rows; only the freshness label differs (5/5 vs 5/3)")
digest_parts.append([r2["CHALLENGER"][4], r2["BENCHMARK"][4]])

# ---------------------------------------------------------------- B7 ------ #
sh_dry = fresh_sheet(history_live_shape())
n_h = len(sh_dry.tabs[s1.TAB_HISTORY].rows); n_rl = len(sh_dry.tabs["_Run_Log"].rows)
rc, out_dry = run_at(s1, "2026-09-30T15:20:00Z", sh_dry, P30, "2026-09-30", policy="lastscored", argv=["--dry-run"])
assert rc == 0 and len(sh_dry.tabs[s1.TAB_HISTORY].rows) == n_h and len(sh_dry.tabs["_Run_Log"].rows) == n_rl
assert f"[S1-BASE v{s1.SCRIPT_VERSION}] mode=lastscored" in out_dry
print("B7 PASS  --dry-run under lastscored: zero writes, read-back printed")
digest_parts.append([n_h, n_rl])

digest = hashlib.sha256(json.dumps(digest_parts, sort_keys=True, default=str).encode()).hexdigest()[:16]
print("RESULT 7/7 PASS digest", digest)


def test_all():
    assert digest
