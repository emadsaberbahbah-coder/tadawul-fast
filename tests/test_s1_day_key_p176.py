#!/usr/bin/env python3
"""run_shadow_scorer v1.9.0 [P-176] harness — REAL module, REAL main() on an
in-memory sheet stub; the network fetch and the clock are the only injected
parts. Replays the live 2026-09-28/29 loss: run #76 (the 09-28 slot) fired
after 21:00 UTC and keyed the 09-28 board as 2026-09-29; run #77 (the real
09-29 slot) was refused as a duplicate, silently.
  D1 full existing selftest battery (104/104 on v1.9.1)
  D2 pure replay table (#76, #77, on-time, 9h40m-late, boundary second)
  D3 END-TO-END default (slot): #76 keys 09-28 with DRIFT read-back; #77 then
     records 09-29 — the day is no longer lost
  D4 END-TO-END kill switch (wallclock): v1.8.0 dates reproduced (09-29 twice)
     and the duplicate refusal now writes a visible _Run_Log row
  D5 --dry-run on the duplicate path stays zero-write
  D6 board-fresh guard interplay on a past-midnight run: slot key reads the
     09-29 board as fresh; wall-clock key would flag it STALE
  D7 on-time parity: slot vs wallclock produce byte-identical history rows,
     S1_Gate body and _Run_Log verdict
Run x3, identical digest."""
import importlib.util, os, sys, json, hashlib, subprocess, copy, io, contextlib
from datetime import datetime, timezone, timedelta, date

spec = importlib.util.spec_from_file_location("s1", "scripts/run_shadow_scorer.py")
s1 = importlib.util.module_from_spec(spec); spec.loader.exec_module(s1)
assert s1.SCRIPT_VERSION == "1.9.1", s1.SCRIPT_VERSION
digest_parts = []

# ---------------------------------------------------------------- D1 ------ #
r = subprocess.run([sys.executable, "scripts/run_shadow_scorer.py", "--selftest"],
                   capture_output=True, text=True)
line = [l for l in r.stdout.splitlines() if "SELFTEST" in l][-1]
assert r.returncode == 0 and "104/104" in line, line
print("D1 PASS  full selftest battery on v1.9.0:", line.strip())
digest_parts.append(line.strip())

# ---------------------------------------------------------------- D2 ------ #
T = lambda s: datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")  # noqa: E731
table = [("2026-09-29T15:20:00Z", "2026-09-29"),   # on time
         ("2026-09-28T21:40:00Z", "2026-09-28"),   # run #76 (wall-clock said 09-29)
         ("2026-09-29T20:00:00Z", "2026-09-29"),   # run #77
         ("2026-09-30T01:00:00Z", "2026-09-29"),   # 9h40m late, past midnight Riyadh
         ("2026-09-30T15:19:59Z", "2026-09-29"),   # one second before the next slot
         ("2026-09-30T15:20:00Z", "2026-09-30")]
for when, want in table:
    got = str(s1.evidence_day(T(when), (15, 20)))
    assert got == want, (when, got, want)
print("D2 PASS  replay table:", " | ".join(f"{w}->{d}" for w, d in table))
digest_parts.append(table)

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
CHAL = ["ITRN.US", "PINFRA.MX", "GOOGL.US"]
BENCH = ["SPUS", "^TASI.SR"]
P0 = {"YUM": 138.0, "DDI.US": 13.0, "CWBC.US": 26.3, "ITRN.US": 51.7, "PINFRA.MX": 262.0,
      "GOOGL.US": 342.0, "SPUS": 55.0, "^TASI.SR": 10800.0}

def history_seed(day="2026-09-27"):
    H = [list(s1.HISTORY_HEADER)]
    def row(basket, syms):
        return [day, basket, ",".join(syms), json.dumps({s: P0[s] for s in syms}),
                0.1, 100.0, 0.0, 0.0, f"n={len(syms)}"]
    H += [row("CHAMPION", CHAMP), row("CHALLENGER", CHAL), row("BENCHMARK", BENCH),
          ["2026-09-27", "BENCHMARK_EQW", ",".join(CHAL), json.dumps({s: P0[s] for s in CHAL}),
           0.1, 100.0, 0.0, 0.0, "W7-INFORMATIONAL naive-eqw(all-board); n=3"]]
    return H

def fresh_sheet(board_asof, hist=None):
    top10 = [["TOP 10 INVESTMENTS — DECISION"], ["Status:", "Last run"], [],
             ["Rank", "Symbol", "Name", "Price", "ROI %"]] + \
            [[str(i + 1), s, s + " Co", "1.0", "10%"] for i, s in enumerate(CHAMP)]
    return Sheet({
        s1.sb.TAB_TOP10: top10,
        s1.sb.TAB_OUT: board_rows(board_asof, CHAL),
        s1.TAB_HISTORY: copy.deepcopy(hist or history_seed()),
        "_Run_Log": [["Timestamp", "Level", "Action", "Page", "Status", "Message",
                      "Endpoint", "HTTP Code", "Duration ms", "Details JSON"],
                     ["2026-09-19 05:00:00", "INFO", "workbook_backup", "_Backup", "OK",
                      "[RESTORE-TEST v1.4.0] PASS rto=8m", "", "", "", "{}"]],
    })

def run_at(when_utc, sheet, spot_day, mode=None, argv=None, guard="observe"):
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
    saved = (s1.datetime, s1.fetch_spot, s1.sb._open_sheet,
             {k: os.environ.get(k) for k in ("TFB_S1_DAY_KEY", "TFB_S1_BOARD_FRESH_GUARD",
                                              "TFB_SHADOW_EODHD")})
    try:
        s1.datetime = FakeDT; s1.fetch_spot = fake_fetch
        s1.sb._open_sheet = lambda cli: sheet
        if mode is None: os.environ.pop("TFB_S1_DAY_KEY", None)
        else: os.environ["TFB_S1_DAY_KEY"] = mode
        os.environ["TFB_S1_BOARD_FRESH_GUARD"] = guard
        os.environ.pop("TFB_SHADOW_EODHD", None)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = s1.main(argv or [])
        return rc, buf.getvalue()
    finally:
        s1.datetime, s1.fetch_spot, s1.sb._open_sheet = saved[0], saved[1], saved[2]
        for k, v in saved[3].items():
            if v is None: os.environ.pop(k, None)
            else: os.environ[k] = v

def hist_dates(sheet, basket="CHALLENGER"):
    return [r[0] for r in sheet.tabs[s1.TAB_HISTORY].rows[1:] if r[1] == basket]
def runlog(sheet): return sheet.tabs["_Run_Log"].rows
def gate_asof(sheet):
    return [c for c in sheet.tabs[s1.TAB_GATE].rows[0] if str(c).startswith("as of")][0]

# ---------------------------------------------------------------- D3 ------ #
sh = fresh_sheet("2026-09-28 23:32")
rc1, out1 = run_at("2026-09-28T21:40:00Z", sh, "2026-09-28")            # run #76
assert rc1 == 0
assert hist_dates(sh) == ["2026-09-27", "2026-09-28"], hist_dates(sh)
assert gate_asof(sh) == "as of 2026-09-28 Riyadh", gate_asof(sh)
v1 = runlog(sh)[-1]
assert v1[4] == "OK" and "[S1-DAY-KEY v1.9.1] mode=slot key=2026-09-28 wallclock=2026-09-29 slot=2026-09-28@15:20Z DRIFT" in v1[5], v1[5]
dk1 = json.loads(v1[9])["day_key"]; assert dk1["drift"] is True and dk1["key"] == "2026-09-28"
assert "day_scored" in v1[5], v1[5]
# board written for 09-29, then run #77 at 20:00Z (= 23:00 Riyadh, same day)
sh.tabs[s1.sb.TAB_OUT] = WS(board_rows("2026-09-29 22:23", CHAL))
rc2, out2 = run_at("2026-09-29T20:00:00Z", sh, "2026-09-29")            # run #77
assert rc2 == 0
assert hist_dates(sh) == ["2026-09-27", "2026-09-28", "2026-09-29"], hist_dates(sh)
v2 = runlog(sh)[-1]
assert v2[4] == "OK" and "DRIFT" not in v2[5] and "refusing duplicate" not in v2[5], v2[5]
assert gate_asof(sh) == "as of 2026-09-29 Riyadh"
assert "[S1-DAY-KEY v1.9.1] mode=slot key=2026-09-29 wallclock=2026-09-29" in out2
print("D3 PASS  default slot key: #76 -> 2026-09-28 (DRIFT read-back in verdict+JSON), #77 -> 2026-09-29 recorded; history", hist_dates(sh))
digest_parts.append([hist_dates(sh), v1[5].split(" | ")[0], gate_asof(sh)])

# ---------------------------------------------------------------- D4 ------ #
shw = fresh_sheet("2026-09-28 23:32")
rcw1, _ = run_at("2026-09-28T21:40:00Z", shw, "2026-09-28", mode="wallclock")
assert rcw1 == 0 and hist_dates(shw) == ["2026-09-27", "2026-09-29"], hist_dates(shw)   # v1.8.0 date
w1 = runlog(shw)[-1]; assert w1[4] == "OK" and "mode=wallclock" in w1[5] and "DRIFT" in w1[5]
n_before = len(runlog(shw)); h_before = copy.deepcopy(shw.tabs[s1.TAB_HISTORY].rows)
shw.tabs[s1.sb.TAB_OUT] = WS(board_rows("2026-09-29 22:23", CHAL))
rcw2, outw2 = run_at("2026-09-29T20:00:00Z", shw, "2026-09-29", mode="wallclock")
assert rcw2 == 0
assert shw.tabs[s1.TAB_HISTORY].rows == h_before, "duplicate must not append"
w2 = runlog(shw)[-1]
assert len(runlog(shw)) == n_before + 1 and w2[1] == "WARNING" and w2[4] == "DUPLICATE_REFUSED", w2
assert "2026-09-29 already recorded" in w2[5] and json.loads(w2[9])["duplicate_of"] == "2026-09-29"
assert "refusing duplicate" in outw2
print("D4 PASS  kill switch reproduces v1.8.0 (09-29 written by the late run, real 09-29 refused) — and the refusal is now a visible DUPLICATE_REFUSED row")
digest_parts.append([hist_dates(shw), w2[4], json.loads(w2[9])["day_key"]["mode"]])

# ---------------------------------------------------------------- D5 ------ #
n_rl = len(runlog(shw)); n_h = len(shw.tabs[s1.TAB_HISTORY].rows)
rcd, outd = run_at("2026-09-29T20:30:00Z", shw, "2026-09-29", mode="wallclock", argv=["--dry-run"])
assert rcd == 0 and len(runlog(shw)) == n_rl and len(shw.tabs[s1.TAB_HISTORY].rows) == n_h
assert "refusing duplicate" in outd
print("D5 PASS  --dry-run on the duplicate path: zero writes, refusal printed")
digest_parts.append([n_rl, n_h])

# ---------------------------------------------------------------- D6 ------ #
def late_run(mode):
    s = fresh_sheet("2026-09-29 22:23", hist=history_seed("2026-09-28"))
    rc, out = run_at("2026-09-30T01:00:00Z", s, "2026-09-29", mode=mode, guard="observe")
    assert rc == 0
    return s, runlog(s)[-1][5]
s_slot, msg_slot = late_run(None)
s_wall, msg_wall = late_run("wallclock")
assert "[S1-BOARD-FRESH v1.9.1] asof=2026-09-29 mode=observe" in msg_slot and "asof=2026-09-29 mode=observe STALE" not in msg_slot, msg_slot
assert "asof=2026-09-29 mode=observe STALE" in msg_wall, msg_wall
assert hist_dates(s_slot)[-1] == "2026-09-29" and hist_dates(s_wall)[-1] == "2026-09-30"
print("D6 PASS  past-midnight run: slot key reads the 09-29 board as fresh (keys 09-29); wall-clock flags STALE and keys 09-30")
digest_parts.append([msg_slot.split(" | [S1-BOARD")[1][:40], msg_wall.split(" | [S1-BOARD")[1][:46]])

# ---------------------------------------------------------------- D7 ------ #
def on_time(mode):
    s = fresh_sheet("2026-09-29 17:10")
    rc, out = run_at("2026-09-29T15:20:00Z", s, "2026-09-29", mode=mode)
    assert rc == 0
    return (s.tabs[s1.TAB_HISTORY].rows, s.tabs[s1.TAB_GATE].rows,
            [r[5] for r in runlog(s)[2:]], [json.loads(r[9]) for r in runlog(s)[2:]])
a = on_time(None); b = on_time("wallclock")
assert a[0] == b[0] and a[1] == b[1] and a[2] == b[2], "on-time output must be byte-identical"
assert a[3][0]["day_key"]["drift"] is False and a[3][0]["day_key"]["mode"] == "slot" \
    and b[3][0]["day_key"]["mode"] == "wallclock"
print("D7 PASS  on-time run: history rows, S1_Gate body and verdict byte-identical under slot vs wallclock (only the JSON mode field differs)")
digest_parts.append([hashlib.sha256(json.dumps(a[0]).encode()).hexdigest()[:12],
                     hashlib.sha256(json.dumps(a[1]).encode()).hexdigest()[:12]])

dig = hashlib.sha256(json.dumps(digest_parts, default=str, sort_keys=True).encode()).hexdigest()[:16]
print("RUN-DIGEST", dig)
