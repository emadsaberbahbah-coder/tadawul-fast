#!/usr/bin/env python3
"""run_shadow_scorer v1.8.0 K-harness — REAL module (full selftest battery +
the new guard's pure surface). K1 selftest parity | K2 asof parsing incl. the
LIVE meta shape | K3 mode reader | K4 override truth table incl. non-trading
precedence | K5 retry recovers a race and stops on the window; injectable
sleep, zero real waiting. Run x3, identical digest."""
import importlib.util, os, sys, json, hashlib, subprocess
spec = importlib.util.spec_from_file_location("s1", "scripts/run_shadow_scorer.py")
s1 = importlib.util.module_from_spec(spec); spec.loader.exec_module(s1)
assert s1.SCRIPT_VERSION == "1.8.0"

# K1: the full existing battery still passes on the revised module
r = subprocess.run([sys.executable, "scripts/run_shadow_scorer.py", "--selftest"],
                   capture_output=True, text=True)
line = [l for l in r.stdout.splitlines() if "SELFTEST" in l][-1]
assert r.returncode == 0 and "89/89" in line, line
print("K1 PASS  full existing selftest battery 89/89 on v1.8.0:", line.strip())

# K2: asof parsing — the EXACT live meta shape + failure modes
live_meta = [["SHADOW BOARD v1.3.0", "as of 2026-09-11 17:44 Riyadh",
              "equity=95,257 SAR"],
             ["authority rows=408 as_of=2026-03-31", "authority_error=-"]]
assert s1.board_asof_date(live_meta) == "2026-09-11"
assert s1.board_asof_date([]) is None
assert s1.board_asof_date([["no stamp here"], ["still none"]]) is None
# the authority as_of (different key) must NOT be mistaken for the board stamp
assert s1.board_asof_date([["authority rows=408 as_of=2026-03-31"]]) is None
print("K2 PASS  board_asof_date: live shape -> 2026-09-11; authority as_of ignored; fail-open None")

# K3: mode reader
for v, want in [("", "off"), ("junk", "off"), ("observe", "observe"),
                ("ENFORCE", "enforce")]:
    os.environ["TFB_S1_BOARD_FRESH_GUARD"] = v
    assert s1._board_fresh_mode() == want, (v, want)
os.environ.pop("TFB_S1_BOARD_FRESH_GUARD", None)
print("K3 PASS  mode reader off|observe|enforce with junk fail-off")

# K4: override truth table
T, F = True, False
cases = [ (F,"enforce",T,T), (T,"enforce",T,F), (F,"observe",T,F),
          (F,"enforce",F,F), (F,"off",T,F) ]
for nt, mode, stale, want in cases:
    assert s1.stale_board_override(nt, mode, stale) is want, (nt, mode, stale)
print("K4 PASS  stale-board override: only enforce+stale+trading-day; non-trading precedence holds")

# K5: retry — injectable sheet + sleep; recovers on 3rd read, and separately
# exhausts the window without ever really sleeping
class Sheet:
    def __init__(self, seq): self.seq=list(seq); self.i=0
    def worksheet(self, name):
        assert name == s1.sb.TAB_OUT; return self
    def get_all_values(self):
        v = self.seq[min(self.i, len(self.seq)-1)]; self.i += 1; return v
stale_b = [["SHADOW BOARD v1.3.0", "as of 2026-09-11 17:44 Riyadh"], [],
           s1.sb.OUT_HEADER, ["AAA.US"] + [""]*(len(s1.sb.OUT_HEADER)-2) + ["NO"]]
fresh_b = [["SHADOW BOARD v1.3.0", "as of 2026-09-12 17:44 Riyadh"], [],
           s1.sb.OUT_HEADER, ["KRP.US"] + [""]*(len(s1.sb.OUT_HEADER)-2) + ["YES"]]
slept = []
out = s1._board_fresh_retry(Sheet([stale_b, fresh_b]), "2026-09-12",
                            True, stale_b, sleep_fn=slept.append, wait_min=8,
                            poll_s=60)
board, rows, dropped, chal, viol, asof, stale = out
assert asof == "2026-09-12" and stale is False and chal == ["KRP.US"], (asof, chal)
assert len(slept) == 2, slept   # 2 polls: stale re-read, then fresh
out2 = s1._board_fresh_retry(Sheet([stale_b]), "2026-09-12", True, stale_b,
                             sleep_fn=slept.append, wait_min=3, poll_s=60)
assert out2[6] is True and out2[3] == [], "window must close STALE with no YES"
print("K5 PASS  retry recovers the race in 2 polls (chal=[KRP.US]); exhausted window closes STALE honestly")

dig = hashlib.sha256(json.dumps([line.strip(), cases, slept[:2].__len__()]).encode()).hexdigest()[:16]
print("RUN-DIGEST", dig)
