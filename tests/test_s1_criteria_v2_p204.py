#!/usr/bin/env python3
"""run_shadow_scorer v1.9.3 [P-204 / P-201 / C5 / P-186] harness — REAL module,
REAL main() on an in-memory sheet stub (the P-176 harness shape); the network
fetch and the clock are the only injected parts.
  C1 full embedded selftest battery (116/116 on v1.9.3; base 104/104 if S1_BASE set)
  C2 the REAL read_history + window_cum on the REAL 2026-10-05 Shadow_History
     rows (fixture): challenger -0.2532 / champion -0.2519 / benchmark +2.5343
     since 2026-09-16 => alpha -2.7875 pp (the audit numbers, exact)
  C3 DUAL-TREE PARITY (if S1_BASE set): REAL main() under mode off produces
     byte-identical Shadow_History rows, S1_Gate body and _Run_Log verdict on
     base (v1.9.1) and delivered (v1.9.3)
  C4 observe: statuses + verdict unchanged vs off; criteria 3/4/5 details carry
     ` | v2:` would-outcomes; token line on the verdict, the meta cell and the
     _Run_Log JSON; history rows identical to off
  C5 enforce on the live-shaped evidence (negative window, zero MAE
     unpublished, empty _Corporate_Actions): 3 FAIL / 4 PENDING /
     5 NOT_EVALUABLE => verdict FAIL, _Run_Log status GATE_FAIL
  C6 enforce with a published Zero MAE column, model beating zero, a CA row and
     a positive window: 3/4/5 PASS; verdict NOT_DECIDABLE on criterion 1 only
  C7 --dry-run under observe: zero writes, token printed
Run x3, identical digest."""
import importlib.util, os, sys, json, hashlib, subprocess, copy, io, contextlib
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
DELIV = os.environ.get("S1_DELIV", "scripts/run_shadow_scorer.py")
BASE = os.environ.get("S1_BASE")   # optional v1.9.1 file for dual-tree parity
FIXTURE = os.environ.get("S1_FIXTURE", os.path.join(HERE, "fixtures", "shadow_history_2026-10-05.json"))

def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    return m

s1 = load(DELIV, "s1_deliv")
assert s1.SCRIPT_VERSION == "1.9.3", s1.SCRIPT_VERSION
s1b = load(BASE, "s1_base") if BASE else None
digest_parts = []

# ---------------------------------------------------------------- C1 ------ #
r = subprocess.run([sys.executable, DELIV, "--selftest"], capture_output=True, text=True)
line = [l for l in r.stdout.splitlines() if "SELFTEST" in l][-1]
assert r.returncode == 0 and "116/116" in line, line
print("C1 PASS  delivered selftest:", line.strip())
digest_parts.append(line.strip())
if s1b:
    rb = subprocess.run([sys.executable, BASE, "--selftest"], capture_output=True, text=True)
    lb = [l for l in rb.stdout.splitlines() if "SELFTEST" in l][-1]
    assert rb.returncode == 0 and "104/104" in lb, lb
    print("C1 PASS  base selftest:", lb.strip()); digest_parts.append(lb.strip())

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

# ---------------------------------------------------------------- C2 ------ #
FIX = json.load(open(FIXTURE, encoding="utf-8"))
hist_ws = WS([FIX["header"]] + FIX["rows"])
history = s1.read_history(Sheet({s1.TAB_HISTORY: hist_ws.rows}))
start = s1._window_start("2026-09-16")
idx = {b: [h for h in history if h["basket"] == b and h["date"] == "2026-10-04"][0]["cum_index"]
       for b in ("CHALLENGER", "CHAMPION", "BENCHMARK")}
wc = {b: s1.window_cum(history, b, start, idx[b]) for b in idx}
alpha = wc["CHALLENGER"] - wc["BENCHMARK"]
assert abs(wc["CHALLENGER"] - (-0.2532)) < 5e-4 and abs(wc["CHAMPION"] - (-0.2519)) < 5e-4 \
    and abs(wc["BENCHMARK"] - 2.5343) < 5e-4 and abs(alpha - (-2.7875)) < 1e-3, (wc, alpha)
cum_since_seed = (idx["CHALLENGER"] / 100.0 - 1) * 100 - (idx["BENCHMARK"] / 100.0 - 1) * 100
assert abs(cum_since_seed - 5.34) < 0.01, cum_since_seed
print(f"C2 PASS  REAL read_history + window_cum on the 10-05 tab: chal {wc['CHALLENGER']:+.4f} "
      f"champ {wc['CHAMPION']:+.4f} bench {wc['BENCHMARK']:+.4f} => alpha {alpha:+.4f} pp "
      f"(cumulative since seed {cum_since_seed:+.2f} pp)")
digest_parts.append({k: round(v, 4) for k, v in wc.items()})

# ------------------------------------------------- end-to-end fixtures --- #
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

CHAMP = ["DDI.US", "CWBC.US", "AER.US"]
CHAL = ["ITRN.US", "HCI.US", "BHF.US"]
BENCH = ["SPUS", "^TASI.SR"]
P0 = {"DDI.US": 13.31, "CWBC.US": 26.53, "AER.US": 144.34, "ITRN.US": 52.53, "HCI.US": 184.25,
      "BHF.US": 50.02, "SPUS": 60.31, "^TASI.SR": 10505.84}

def history_seed():
    """Two pre-boundary rows (09-14, 09-15) and one post-boundary row (10-02) per basket so
    window_cum has a base dated before 2026-09-16. Indexes shaped like the live tab."""
    H = [list(s1.HISTORY_HEADER)]
    def row(day, basket, syms, idx, note):
        return [day, basket, ",".join(syms), json.dumps({s: P0[s] for s in syms}), 0.1, idx, 0.0, 0.0, note]
    H += [row("2026-09-14", "CHAMPION", CHAMP, 101.0, "n=3"), row("2026-09-14", "CHALLENGER", CHAL, 107.0, "n=3"),
          row("2026-09-14", "BENCHMARK", BENCH, 99.0, "n=2"),
          row("2026-09-15", "CHAMPION", CHAMP, 101.391146, "n=3"), row("2026-09-15", "CHALLENGER", CHAL, 107.164026, "n=3"),
          row("2026-09-15", "BENCHMARK", BENCH, 99.04464, "n=2"),
          row("2026-10-02", "CHAMPION", CHAMP, 101.135775, "n=3"), row("2026-10-02", "CHALLENGER", CHAL, 106.892669, "n=3"),
          row("2026-10-02", "BENCHMARK", BENCH, 101.554736, "n=2"),
          ["2026-10-02", "BENCHMARK_EQW", ",".join(CHAL), json.dumps({s: P0[s] for s in CHAL}), 0.1, 101.0, 0.0, 0.0,
           "W7-INFORMATIONAL naive-eqw(all-board); n=3"]]
    return H

CAL_HDR = ["As Of (Riyadh)", "State", "N Checkpoints", "Mean Abs Error (pp)", "Mean Signed Error (pp)",
           "Band (pp)", "Min Sample", "By Horizon", "Detail", "Writer Version"]
def cal_rows(zero_col=None):
    hdr = list(CAL_HDR); row = ["2026-10-05 03:28:01", "PASS", "6275", "3.13", "-0.45", "10.0", "20",
                                "1W n=3324 |err|=2.59pp; 2W n=2951 |err|=3.74pp",
                                "mean |err| 3.13pp vs band 10.00pp over n=6275; signed -0.45pp", "6.41.0"]
    if zero_col is not None:
        hdr.append("Zero MAE (pp)"); row.append(str(zero_col))
    return [hdr, row]

def fresh_sheet(cal=None, ca_rows=None):
    top10 = [["TOP 10 INVESTMENTS — DECISION"], ["Status:", "Last run"], [],
             ["Rank", "Symbol", "Name", "Price", "ROI %"]] + \
            [[str(i + 1), s, s + " Co", "1.0", "10%"] for i, s in enumerate(CHAMP)]
    ca = [["Logged At", "Symbol", "Type", "Ex-Date", "Ratio", "Status", "Source", "Note", "Repair"]]
    if ca_rows:
        ca += ca_rows
    return Sheet({
        s1.sb.TAB_TOP10: top10,
        s1.sb.TAB_OUT: board_rows("2026-10-05 08:10", CHAL),
        s1.TAB_HISTORY: history_seed(),
        s1.TAB_REGRET: [list(s1.rg.LEDGER_HEADER)],
        s1.TAB_S1_CAL: cal if cal is not None else cal_rows(),
        "_Corporate_Actions": ca,
        "_Run_Log": [["Timestamp", "Level", "Action", "Page", "Status", "Message",
                      "Endpoint", "HTTP Code", "Duration ms", "Details JSON"],
                     ["2026-09-19 05:00:00", "INFO", "workbook_backup", "_Backup", "OK",
                      "[RESTORE-TEST v1.4.0] PASS rto=8m", "", "", "", "{}"]],
    })

T = lambda s: datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")  # noqa: E731
def run_at(mod, when_utc, sheet, spot_day, v2=None, argv=None):
    """Drive the REAL main(): fixed clock, injected spot fetch, stub sheet."""
    fixed = T(when_utc).replace(tzinfo=timezone.utc)
    class FakeDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz) if tz else fixed.replace(tzinfo=None)
    bar = datetime.strptime(spot_day, "%Y-%m-%d").date()
    spot = {k: v * 1.01 for k, v in P0.items()}
    def fake_fetch(symbols):
        need = [s for s in symbols if s in spot]
        return ({s: spot[s] for s in need}, {s: bar for s in need}, [])
    keys = ("TFB_S1_CRITERIA_V2", "TFB_S1_WINDOW_START", "TFB_S1_DAY_KEY", "TFB_S1_BOARD_FRESH_GUARD",
            "TFB_SHADOW_EODHD", "TFB_S1_BASE_POLICY", "TFB_S1_CAL_CONSUME")
    saved = (mod.datetime, mod.fetch_spot, mod.sb._open_sheet, {k: os.environ.get(k) for k in keys})
    try:
        mod.datetime = FakeDT; mod.fetch_spot = fake_fetch
        mod.sb._open_sheet = lambda cli: sheet
        for k in keys: os.environ.pop(k, None)
        if v2: os.environ["TFB_S1_CRITERIA_V2"] = v2
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

def gate_body(sheet): return [list(r) for r in sheet.tabs[s1.TAB_GATE].rows]
def crit(sheet):
    rows = gate_body(sheet); i = [k for k, r in enumerate(rows) if r and r[0] == "#"][0]
    return {int(r[0]): (r[2], r[3]) for r in rows[i + 1:] if r and str(r[0]).strip()}
def hist_rows(sheet): return [list(r) for r in sheet.tabs[s1.TAB_HISTORY].rows]
def runlog(sheet): return sheet.tabs["_Run_Log"].rows
WHEN, SPOT = "2026-10-05T15:40:00Z", "2026-10-05"

# ---------------------------------------------------------------- C3 ------ #
sh_off = fresh_sheet(); rc, out_off = run_at(s1, WHEN, sh_off, SPOT, v2=None)
assert rc == 0 and "[S1-CRITERIA-V2" not in out_off, out_off
c_off = crit(sh_off)
assert c_off[3][0] == "PASS" and "v2:" not in c_off[3][1] and c_off[5][0] == "PASS", c_off
if s1b:
    sh_b = fresh_sheet(); rcb, out_b = run_at(s1b, WHEN, sh_b, SPOT, v2=None)
    norm = lambda rows: [[str(c).replace("v1.9.1", "vX").replace("v1.9.3", "vX") for c in r] for r in rows]  # noqa: E731
    assert rcb == 0 and norm(hist_rows(sh_b)) == norm(hist_rows(sh_off)), "history parity"
    assert norm(gate_body(sh_b)) == norm(gate_body(sh_off)), "gate parity"
    vb, vd = runlog(sh_b)[-1][5], runlog(sh_off)[-1][5]
    assert vb.replace("v1.9.1", "vX") == vd.replace("v1.9.3", "vX"), (vb, vd)
    print("C3 PASS  dual-tree parity under mode off: history rows, S1_Gate body and verdict byte-identical (version token aside)")
else:
    print("C3 SKIP  dual-tree parity (set S1_BASE=<v1.9.1 file>)")
digest_parts.append(c_off)

# ---------------------------------------------------------------- C4 ------ #
sh_obs = fresh_sheet(); rc, out_obs = run_at(s1, WHEN, sh_obs, SPOT, v2="observe")
assert rc == 0, out_obs
c_obs = crit(sh_obs)
assert {k: v[0] for k, v in c_obs.items()} == {k: v[0] for k, v in c_off.items()}, (c_obs, c_off)
assert "v2: since 2026-09-16" in c_obs[3][1] and "would FAIL" in c_obs[3][1], c_obs[3]
assert "zero-baseline MAE not published" in c_obs[4][1] and "would PENDING" in c_obs[4][1], c_obs[4]
assert "0 rows (vacuous)" in c_obs[5][1] and "would NOT_EVALUABLE" in c_obs[5][1], c_obs[5]
assert gate_body(sh_obs)[0][2] == gate_body(sh_off)[0][2], "verdict changed under observe"
assert hist_rows(sh_obs) == hist_rows(sh_off), "observe must not touch Shadow_History"
tok = [l for l in out_obs.splitlines() if l.startswith("[S1-CRITERIA-V2 v1.9.3] mode=observe")]
# today's scored day moves every index by exactly +1.00 % (all spots x1.01, zero turnover), so the
# window return = seed_index x 1.01 / pre-boundary index - 1 for each basket
exp = {b: (i * 1.01 / p - 1) * 100 for b, i, p in (("chal", 106.892669, 107.164026), ("bench", 101.554736, 99.04464))}
exp_alpha = exp["chal"] - exp["bench"]
import re as _re
m_alpha = _re.search(r"alpha=([+-][0-9.]+)%", tok[0]) if tok else None
assert tok and m_alpha and abs(float(m_alpha.group(1)) - exp_alpha) < 0.02 and exp_alpha < 0 \
    and "zero_mae=n/a" in tok[0] and "model_mae=3.13pp" in tok[0] \
    and "ca_rows=0" in tok[0] and "set=" in tok[0], (tok, exp_alpha)
meta_cells = " ".join(str(c) for r in gate_body(sh_obs)[:6] for c in r)
assert "[S1-CRITERIA-V2 v1.9.3] mode=observe" in meta_cells, "meta cell token missing"
rl = runlog(sh_obs)[-1]
assert "[S1-CRITERIA-V2 v1.9.3]" in rl[5] and json.loads(rl[9]).get("criteria_v2", {}).get("mode") == "observe", rl
print("C4 PASS  observe: statuses/verdict unchanged, 3/4/5 annotated, token on verdict + meta + _Run_Log JSON, history untouched")
digest_parts.append(tok[0].split(" set=")[0])

# ---------------------------------------------------------------- C5 ------ #
sh_enf = fresh_sheet(); rc, out_enf = run_at(s1, WHEN, sh_enf, SPOT, v2="enforce")
assert rc == 0, out_enf
c_enf = crit(sh_enf)
assert c_enf[3][0] == "FAIL" and c_enf[4][0] == "PENDING" and c_enf[5][0] == "NOT_EVALUABLE", c_enf
assert gate_body(sh_enf)[0][2] == "verdict: FAIL", gate_body(sh_enf)[0]
assert runlog(sh_enf)[-1][4] == "GATE_FAIL", runlog(sh_enf)[-1]
assert hist_rows(sh_enf) == hist_rows(sh_off), "enforce must not touch Shadow_History"
print("C5 PASS  enforce on live-shaped evidence: 3 FAIL / 4 PENDING / 5 NOT_EVALUABLE => FAIL, _Run_Log GATE_FAIL, history untouched")
digest_parts.append({k: v[0] for k, v in c_enf.items()})

# ---------------------------------------------------------------- C6 ------ #
H6 = history_seed()
for r in H6[1:]:
    if r[1] == "BENCHMARK" and r[0] == "2026-10-02": r[5] = 97.0        # benchmark below its 09-15 base => window alpha > 0
sh6 = fresh_sheet(cal=cal_rows(zero_col=3.5), ca_rows=[["2026-10-01", "DDI.US", "DIVIDEND", "2026-10-15", "1.0", "CONFIRMED", "eodhd", "", "n/a"]])
sh6.tabs[s1.TAB_HISTORY] = WS(H6)
rc, out6 = run_at(s1, WHEN, sh6, SPOT, v2="enforce")
assert rc == 0, out6
c6 = crit(sh6)
assert c6[3][0] == "PASS" and c6[4][0] == "PASS" and c6[5][0] == "PASS", c6
assert "beats the baseline" in c6[4][1] and "rows=1" in c6[5][1], c6
assert gate_body(sh6)[0][2] == "verdict: NOT_DECIDABLE" and "criteria 1 still pending" in gate_body(sh6)[1][0], gate_body(sh6)[:2]
print("C6 PASS  enforce with Zero MAE column (3.50 vs model 3.13), a CA row and a positive window: 3/4/5 PASS; only criterion 1 pending")
digest_parts.append({k: v[0] for k, v in c6.items()})

# ---------------------------------------------------------------- C7 ------ #
sh7 = fresh_sheet(); before = (copy.deepcopy(hist_rows(sh7)), copy.deepcopy(runlog(sh7)))
rc, out7 = run_at(s1, WHEN, sh7, SPOT, v2="observe", argv=["--dry-run"])
assert rc == 0 and "[S1-CRITERIA-V2 v1.9.3] mode=observe" in out7, out7
assert (hist_rows(sh7), runlog(sh7)) == before and s1.TAB_GATE not in sh7.tabs, "dry-run wrote"
print("C7 PASS  --dry-run under observe: token printed, zero writes")
digest_parts.append("dryrun-ok")

digest = hashlib.sha256(json.dumps(digest_parts, sort_keys=True, default=str).encode()).hexdigest()[:16]
print("RUN-DIGEST", digest)
