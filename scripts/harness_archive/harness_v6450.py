import sys, types, json, importlib.util, os
from types import SimpleNamespace as NS

# Stub third-party modules the runner imports but the sandbox lacks.
for m in ("aiohttp", "googleapiclient", "googleapiclient.discovery",
          "googleapiclient.errors", "google", "google.oauth2",
          "google.oauth2.service_account", "google.auth",
          "google.auth.transport", "google.auth.transport.requests",
          "requests", "urllib3"):
    if m not in sys.modules:
        mod = types.ModuleType(m)
        if m == "googleapiclient.errors":
            class HttpError(Exception): ...
            mod.HttpError = HttpError
        sys.modules[m] = mod

spec = importlib.util.spec_from_file_location("rds", "run_dashboard_sync.py")
rds = importlib.util.module_from_spec(spec)
sys.modules["rds"] = rds
spec.loader.exec_module(rds)

# ---- fake Sheets transport (records every call) ----------------------------
class FakeValues:
    def __init__(self, store): self.s = store
    def get(self, spreadsheetId=None, range=None, **k):
        self.s["gets"].append(range); r = range
        return NS(execute=lambda: {"values": self.s["grid_for"](r)})
    def update(self, spreadsheetId=None, range=None, body=None, **k):
        self.s["updates"].append((range, body)); return NS(execute=lambda: {})
    def append(self, spreadsheetId=None, range=None, body=None, **k):
        self.s["appends"].append((range, body)); return NS(execute=lambda: {})
class FakeSvc:
    def __init__(self, store): self.s = store
    def spreadsheets(self): return NS(values=lambda: FakeValues(self.s))
class FakeSheets:
    def __init__(self, store): self.s = store
    def _get_service(self): return FakeSvc(self.s)
    def read_values(self, sid, name, rng):
        return self.s["grid_for"](f"'{name}'!{rng}")

def run_all(label):
    ok = 0; tot = 0
    def check(name, cond):
        nonlocal ok, tot; tot += 1
        print(("PASS " if cond else "FAIL ") + name); ok += int(cond)
        assert cond, name

    HDR = ["Symbol", "Current Price", "Open", "Day High", "Day Low", "Warnings"]
    def rows():
        return [
            ["GOOD.US", 100.0, 100.5, 101.0, 99.0, ""],          # clean
            ["HUFLEAK", 2.75, 310.82, 2.82, 2.74, ""],           # P3 open (HUF donor)
            ["COPPER",  6.59, 6.60, 6.4955, 6.728, ""],          # P1 high<low
            ["MLBAND",  25.24, None, 24.10, 23.90, ""],          # P2 price outside stale band
        ]
    # T1 default classes == v6.44.1 enforce byte-semantics
    os.environ.update({"TFB_SYNC_OHLC_PREWRITE": "1",
                       "TFB_SYNC_OHLC_PREWRITE_MODE": "enforce"})
    os.environ.pop("TFB_SYNC_OHLC_PREWRITE_ENFORCE_CLASSES", None)
    m, st = rds._apply_ohlc_prewrite_guard(HDR, rows(), "T")
    check("T1 counts", st["flagged"] == 3 and st["open"] == 1
          and st["range"] == 1 and st["price_band"] == 1)
    check("T1 open blanked", m[1][2] == "" and m[1][3] == 2.82)
    check("T1 range blanks hi+lo", m[2][3] == "" and m[2][4] == "")
    check("T1 band blanks hi+lo", m[3][3] == "" and m[3][4] == "")
    check("T1 tags", "open:prewrite" in m[1][5] and "range:prewrite" in m[2][5]
          and "price_band:prewrite" in m[3][5])
    # T2 classes=open,range -> price_band measured but NOT mutated
    os.environ["TFB_SYNC_OHLC_PREWRITE_ENFORCE_CLASSES"] = "open,range"
    m2, st2 = rds._apply_ohlc_prewrite_guard(HDR, rows(), "T")
    check("T2 counts unchanged", st2["flagged"] == 3 and st2["price_band"] == 1)
    check("T2 band row untouched", m2[3][3] == 24.10 and m2[3][4] == 23.90
          and m2[3][5] == "")
    check("T2 corruption still enforced", m2[1][2] == "" and m2[2][3] == "")
    os.environ.pop("TFB_SYNC_OHLC_PREWRITE_ENFORCE_CLASSES", None)
    # invalid token fails SAFE to default
    os.environ["TFB_SYNC_OHLC_PREWRITE_ENFORCE_CLASSES"] = "bogus"
    check("T2b invalid->default",
          rds._ohlc_prewrite_enforce_classes() ==
          frozenset({"open", "price_band", "range"}))
    os.environ.pop("TFB_SYNC_OHLC_PREWRITE_ENFORCE_CLASSES", None)

    # T3 SYNC-HOLD publish/clear + invisibility to manual-hold reader
    store = {"gets": [], "updates": [], "appends": [],
             "grid_for": lambda r: [["manual hold until", ""],
                                    ["backend sync hold until", "", ""]]}
    fs = FakeSheets(store)
    os.environ["TFB_SYNC_WRITE_SENTINEL"] = "1"
    rds._SH_STATE.update({"active": False, "row": None})
    rds._sync_hold_publish(fs, "SID", "Global_Markets")
    check("T3 publish row2", store["updates"] and
          store["updates"][0][0].endswith("A2:C2") and
          store["updates"][0][1]["values"][0][0] == "backend sync hold until")
    until = store["updates"][0][1]["values"][0][1]
    check("T3 expiry parses future",
          rds._mh_parse_hold_until(until) is not None)
    check("T3 state active", rds._SH_STATE["active"] and rds._SH_STATE["row"] == 2)
    # the manual-hold reader must NOT see our key as a manual hold
    hold, raw = rds._mh_read_hold(fs, "SID")
    check("T3 invisible to _mh_read_hold", hold is None and raw == "")
    rds._sync_hold_clear(fs, "SID")
    check("T3 clear blanks B", store["updates"][-1][0].endswith("B2:C2")
          and store["updates"][-1][1]["values"][0][0] == ""
          and rds._SH_STATE["active"] is False)
    # gate OFF => zero transport calls
    os.environ["TFB_SYNC_WRITE_SENTINEL"] = "0"
    n_up = len(store["updates"])
    rds._sync_hold_publish(fs, "SID", "X"); rds._sync_hold_clear(fs, "SID")
    check("T3 gate off = no-op", len(store["updates"]) == n_up)

    # T4 READBACK-REPAIR restores payload trio and re-verifies to pw
    os.environ.update({"TFB_SYNC_OHLC_READBACK": "1",
                       "TFB_SYNC_READBACK_REPAIR": "repair",
                       "TFB_SYNC_OHLC_PREWRITE_MODE": "observe"})
    payload = rows()                       # pw flags = 3 on this fixture
    grid_repaired = [HDR] + [list(r) for r in payload]
    st4 = {"gets": [], "updates": [], "appends": [],
           "grid_for": lambda r: grid_repaired}
    fs4 = FakeSheets(st4)
    delta = {"page": "Global_Markets", "prewrite_checked": 4,
             "prewrite_flagged": 3, "readback_checked": 4,
             "readback_flagged": 4, "delta_flagged": 1, "delta_checked": 0,
             "readback_open": 4, "readback_price_band": 0,
             "readback_range": 0, "readback_blank_open": 0,
             "prewrite_blank_open": 1, "matrix_rows": 4, "examples": []}
    info = rds._ohlc_readback_repair(fs4, "SID", "Global_Markets",
                                     HDR, payload, "A5", delta)
    check("T4 ran", info is not None and not info.get("error"))
    check("T4 one contiguous update (C..E rows 6..9)",
          info["updates"] == 1 and st4["updates"][0][0] == "'Global_Markets'!C6:E9")
    body = st4["updates"][0][1]["values"]
    check("T4 payload trio restored + None->''",
          body[1] == [310.82, 2.82, 2.74] and body[3][0] == "")
    check("T4 after==pw", info["after_flagged"] == 3 and info["warn"] is False)
    check("T4 runlog line appended",
          any("_Run_Log" in a[0] for a in st4["appends"]))
    # non-divergent => no-op
    check("T4b matched no-op",
          rds._ohlc_readback_repair(fs4, "SID", "GM", HDR, payload, "A5",
              dict(delta, readback_flagged=3, delta_flagged=0)) is None)
    os.environ["TFB_SYNC_READBACK_REPAIR"] = "off"

    # T5 stamp manifest + data_status
    def mkres(meta, status="success", failed=0):
        return NS(status=status, rows_written=4, rows_failed=failed,
                  warnings=[], error="", start_utc="", end_utc="",
                  dry_run=False, symbols_requested=4, _stamp_meta=meta)
    row = rds._status_stamp_row("Global_Markets", mkres(
        {"payload_sha8": "abcd1234", "pw_flagged": 3, "pw_checked": 4,
         "rb_flagged": 4, "rb_checked": 4, "rb_status": "DIVERGENT",
         "repair_after": 3}), 115)
    check("T5 shape A..J", len(row) == 10 and isinstance(row[9], int))
    check("T5 repaired => COMPLETE", " data=COMPLETE" in row[3]
          and "guard=pw:3/4,rb:4/4,rep:3" in row[3]
          and "sha=abcd1234" in row[3] and "run=" in row[3])
    row2 = rds._status_stamp_row("GM", mkres(
        {"pw_flagged": 3, "pw_checked": 4, "rb_flagged": 9,
         "rb_checked": 4, "rb_status": "DIVERGENT"}), 115)
    check("T5 unrepaired divergent => PARTIAL", " data=PARTIAL" in row2[3])
    row3 = rds._status_stamp_row("GM", mkres({}, status="partial"), 115)
    check("T5 leg partial => PARTIAL", " data=PARTIAL" in row3[3])

    # T6 meta json
    out = json.loads(rds._runlog_meta_json(json.dumps({"a": 1})))
    check("T6 injects run_id+ts", out["a"] == 1 and out["run_id"]
          and out["ts_utc"].endswith("+00:00"))
    check("T6 fail-open", rds._runlog_meta_json("not-json") == "not-json")

    print(f"[{label}] {ok}/{tot} PASS")
    return ok, tot

for i in (1, 2, 3):
    a, b = run_all(f"run{i}")
    assert a == b
print("HARNESS 3/3 CLEAN")
