#!/usr/bin/env python3
"""run_shadow_board v1.3.1 L-harness — REAL modules (board + the landed
scorer v1.8.0). L1 selftest battery parity | L2 atomic: ONE update, ZERO
clear, 17x60 rectangle, body cells verbatim at same indices | L3 kill
switch restores clear-then-update with the unpadded body | L4 reader
compat: the real scorer extracts identical challengers + asof from the
padded rectangle. Run x3, identical digest."""
import importlib.util, os, sys, json, hashlib, subprocess, copy
def load(name, path):
    sp = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(sp); sp.loader.exec_module(m); return m
sb = load("sbrev", "scripts/run_shadow_board.py")
s1 = load("s1", "scripts/run_shadow_scorer.py")
assert sb.SCRIPT_VERSION == "1.3.1" and s1.SCRIPT_VERSION == "1.8.0"

r = subprocess.run([sys.executable, "scripts/run_shadow_board.py", "--selftest"],
                   capture_output=True, text=True)
tail = [l for l in r.stdout.splitlines() if "SELFTEST" in l][-1]
assert r.returncode == 0, r.stdout[-400:]
print("L1 PASS  board selftest battery on v1.3.1:", tail.strip())

class WS:
    def __init__(self): self.calls = []
    def clear(self): self.calls.append(("clear",))
    def update(self, values=None, range_name=None):
        self.calls.append(("update", copy.deepcopy(values), range_name))
class SH:
    def __init__(self): self.ws = WS()
    def worksheet(self, name):
        assert name == sb.TAB_OUT; return self.ws

meta = [["SHADOW BOARD v1.3.1", "as of 2026-09-12 17:44 Riyadh", "equity=94,858 SAR"],
        ["authority rows=408 as_of=2026-03-31", "authority_error=-"]]
data = [["KRP.US", "Kimbell"] + [""] * (len(sb.OUT_HEADER) - 3) + ["YES"],
        ["AMG.US", "Affiliated"] + [""] * (len(sb.OUT_HEADER) - 3) + ["NO"]]
legacy_body = meta + [[]] + [sb.OUT_HEADER] + data

os.environ.pop("TFB_SB_ATOMIC_WRITE", None)
sh = SH(); sb.write_board(sh, [list(r) for r in data], [list(r) for r in meta])
calls = sh.ws.calls
assert [c[0] for c in calls] == ["update"], calls
rect = calls[0][1]
assert len(rect) == 60 and all(len(r) == len(sb.OUT_HEADER) for r in rect)
for i, row in enumerate(legacy_body):
    assert rect[i][:len(row)] == [str(x) if False else x for x in row], (i, rect[i], row)
    assert all(c == "" for c in rect[i][len(row):])
assert all(all(c == "" for c in r) for r in rect[len(legacy_body):])
print("L2 PASS  atomic: single update, no clear, 17x60 rectangle, body verbatim + blank pad")

os.environ["TFB_SB_ATOMIC_WRITE"] = "0"
sh2 = SH(); sb.write_board(sh2, [list(r) for r in data], [list(r) for r in meta])
assert [c[0] for c in sh2.ws.calls] == ["clear", "update"]
assert sh2.ws.calls[1][1] == legacy_body, "kill switch payload differs"
os.environ.pop("TFB_SB_ATOMIC_WRITE", None)
print("L3 PASS  kill switch: clear()+update() with the exact v1.3.0 body")

d_pad, drop_pad, chal_pad, viol_pad = s1._board_extract(rect, True)
d_leg, drop_leg, chal_leg, viol_leg = s1._board_extract(legacy_body, True)
assert chal_pad == chal_leg == ["KRP.US"] and viol_pad == viol_leg
assert s1.board_asof_date(rect) == s1.board_asof_date(legacy_body) == "2026-09-12"
assert len(d_pad) == len(d_leg) == 2
print("L4 PASS  landed scorer v1.8.0 reads the rectangle identically: chal=[KRP.US], asof=2026-09-12")

dig = hashlib.sha256(json.dumps([tail.strip(), chal_pad, len(rect)]).encode()).hexdigest()[:16]
print("RUN-DIGEST", dig)
