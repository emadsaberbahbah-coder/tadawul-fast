#!/usr/bin/env python3
"""tests/test_track_force_coverage_p180.py — track_performance v6.40.0
[P-180 FORCED COVERAGE: ACTIVE LOTS FIRST, CLOSED LOTS SCOPED].

REAL module, REAL PerformanceTrackerApp._augment_with_decision_symbols (the
method that decides which decision symbols are force-fetched for
Performance_Log + Signal_History). The only substituted legs are the two
I/O seams: the ledger loaders (module functions, patched per test) and the
backend fetch (a recording fake on app.backend). Dual-tree: the v6.39.0
file is loaded beside the delivered one via TP_BASE (default: the sibling
path given on the commit sheet) and driven through the SAME fixtures.

V1 embedded selftest: base 14/14, delivered 16/16
V2 below the cap (today's 34-symbol ledger): delivered fetch list ==
   base fetch list, same order (byte-identical behaviour)
V3 at the cap (6 active + 40 closed): base drops YUM (alphabetical cut);
   delivered keeps all 6 active and cuts the last closed names
V4 TFB_TRACK_FORCE_SCOPE=active: closed lots are not fetched; extras kept
V5 by-status loader returns None -> delivered == base (legacy path)
V6 covered symbols are never refetched; pinned extras always kept
V7 _extract_costbasis_by_status on the REAL 2026-09-30 ledger export
   (TP_LEDGER_TSV) -> 6 active / 30 closed
Run x3, identical digest.
"""
import re
import asyncio, csv, hashlib, importlib.util, json, logging, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
logging.disable(logging.CRITICAL)

NEW_PATH = os.path.join(os.path.dirname(__file__), "..", "scripts", "track_performance.py")
BASE_PATH = os.environ.get("TP_BASE", "")


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    sys.modules[name] = m                      # @dataclass(slots=True) needs it
    spec.loader.exec_module(m)
    return m


tp = load("tp_new_p180", NEW_PATH)
# (2026-10-06) FLOOR, NOT AN EXACT PIN. This line read
# `== "6.41.0"` and broke the moment track_performance reached 6.42.0
# -- the same stale-pin class as tests/test_tp_target_unit_sentry_p158.py's
# "PASS 14/14". What the harness needs is "at least the version that
# introduced the behaviour under test", so compare version TUPLES and keep a
# floor. A tuple compare also avoids the string trap where "6.9.0" > "6.42.0".
def _ver_tuple(v):
    return tuple(int(x) for x in str(v).strip().split(".")[:3])


assert _ver_tuple(tp.SCRIPT_VERSION) >= (6, 41, 0), tp.SCRIPT_VERSION
tb = load("tp_base_p180", BASE_PATH) if BASE_PATH and os.path.exists(BASE_PATH) else None
if tb is not None:
    assert tb.SCRIPT_VERSION == "6.39.0", tb.SCRIPT_VERSION

out = []
digest_parts = []


def T(name, cond, detail=""):
    out.append(("PASS " if cond else "FAIL ") + name + (" | " + str(detail) if detail else ""))
    if not cond:
        raise AssertionError(name + " " + str(detail))

# (2026-10-06) ALL-OF-THEM, NOT A FIXED COUNT. These assertions pinned the
# embedded self-test's exact case count ("PASS 18/18"), so every release that
# ADDS a self-test case broke this harness: v6.40.0 took it 14 -> 16, v6.41.0
# 16 -> 18 and v6.42.0 18 -> 20, and each time a harness that was never in CI
# went quietly red. What the case actually proves is "the embedded self-test
# ran and every case passed", so assert k == k with k > 0.
def _selftest_all_passed(msg, floor=0):
    m = re.match(r"^PASS (\d+)/(\d+)$", str(msg).strip())
    if not m:
        return False
    done, total = int(m.group(1)), int(m.group(2))
    return done == total and total >= max(1, floor)


class FakeBackend:
    base_url = "https://fake.local"

    def __init__(self):
        self.requests = []

    async def get_rows_for_symbols(self, symbols, page):
        self.requests.append((list(symbols), page))
        return [{"symbol": s, "current_price": 1.0} for s in symbols], {"page": page}


def app_for(mod, active, closed, by_status_ok=True, extras=None, scope=None, cap=None, sheet_id="SHEET"):
    """Build the REAL app, patch the two I/O seams, return (app, backend)."""
    for k in ("TFB_TRACK_FORCE_SCOPE", "TFB_TRACK_PRIORITY_SYMBOLS", "TFB_TRACK_FORCE_MAX",
              "TFB_TRACK_FORCE_DECISION_SYMBOLS", "TRACK_SHEET_ID", "DEFAULT_SPREADSHEET_ID"):
        os.environ.pop(k, None)
    if scope: os.environ["TFB_TRACK_FORCE_SCOPE"] = scope
    if extras: os.environ["TFB_TRACK_PRIORITY_SYMBOLS"] = ",".join(extras)
    if cap: os.environ["TFB_TRACK_FORCE_MAX"] = str(cap)
    args = mod.create_parser().parse_args(["--sheet-id", sheet_id])
    app = mod.PerformanceTrackerApp(args)
    app.spreadsheet_id = sheet_id
    app.backend = FakeBackend()
    allsyms = list(dict.fromkeys(list(active) + list(closed)))
    mod._load_costbasis_symbols = lambda sid: list(allsyms)           # legacy loader
    if hasattr(mod, "_load_costbasis_symbols_by_status"):
        mod._load_costbasis_symbols_by_status = (
            (lambda sid: (list(active), list(closed))) if by_status_ok else (lambda sid: None))
    return app, app.backend


def run(app, prefetched):
    return asyncio.run(app._augment_with_decision_symbols(prefetched))


def fetched(backend):
    return backend.requests[0][0] if backend.requests else []


# ---------------------------------------------------------------- V1 ------ #
a_new = tp.PerformanceTrackerApp(tp.create_parser().parse_args([]))
T("V1 delivered selftest all cases", a_new._track_selftest_() and _selftest_all_passed(tp._TRACK_SELFTEST_MSG, 18), tp._TRACK_SELFTEST_MSG)
if tb is not None:
    a_base = tb.PerformanceTrackerApp(tb.create_parser().parse_args([]))
    T("V1 base selftest all cases", a_base._track_selftest_() and _selftest_all_passed(tb._TRACK_SELFTEST_MSG), tb._TRACK_SELFTEST_MSG)
digest_parts.append(tp._TRACK_SELFTEST_MSG)

# ---------------------------------------------------------------- V2 ------ #
ACTIVE6 = ["5023.SR", "YUM", "DDI.US", "CWBC.US", "AER.US", "KRP.US"]
CLOSED28 = ["FER.US", "RCI.US", "BBD.US", "1050.SR", "7030.SR", "1211.SR", "1180.SR", "4200.SR", "NMM.US",
            "1150.SR", "1831.SR", "2222.SR", "1321.SR", "YUMC", "NTES", "SNX.US", "MRP.US", "5110.SR",
            "ARCO.US", "UVV.US", "PFLT.US", "T82U.SI", "OTIS", "SBAC", "PFS", "SHG.US", "EPRT.US", "HCI.US"]
CLOSED28 += ["VEL.US", "CARE.US"]
CLOSED28 = list(dict.fromkeys(CLOSED28))
PRE = [{"symbol": "PINE.US"}, {"symbol": "NVDA.US"}, {"symbol": "DDI.US"}]     # cockpit rows (DDI covered)
app, be = app_for(tp, ACTIVE6, CLOSED28)
rows = run(app, PRE)
f_new = fetched(be)
T("V2 delivered: 36-symbol ledger below cap 40 -> all missing forced (35 after DDI covered)",
  len(f_new) == len(set(ACTIVE6 + CLOSED28)) - 1 and f_new == sorted(f_new) and "DDI.US" not in f_new, len(f_new))
T("V2 rows carry Decision_Coverage origin and cover every active symbol",
  all(r.get("origin") == "Decision_Coverage" for r in rows[3:]) and {r["symbol"] for r in rows} >= set(ACTIVE6))
if tb is not None:
    appb, beb = app_for(tb, ACTIVE6, CLOSED28)
    run(appb, PRE)
    T("V2 dual-tree: delivered fetch list == base fetch list (same order)", fetched(beb) == f_new)
digest_parts.append(f_new)

# ---------------------------------------------------------------- V3 ------ #
CLOSED40 = ["C%02d.US" % i for i in range(40)]           # all sort before 'YUM'
app, be = app_for(tp, ACTIVE6, CLOSED40, cap=40)
run(app, [])
f3 = fetched(be)
T("V3 delivered at the cap: all 6 active kept incl. YUM, 40 fetched, last closed names cut",
  len(f3) == 40 and all(s in f3 for s in ACTIVE6) and "C39.US" not in f3 and "C38.US" not in f3 and "C33.US" in f3, f3[-3:])
if tb is not None:
    appb, beb = app_for(tb, ACTIVE6, CLOSED40, cap=40)
    run(appb, [])
    f3b = fetched(beb)
    T("V3 base golden-negative: alphabetical cut drops YUM (and KRP.US survives only by letter)",
      len(f3b) == 40 and "YUM" not in f3b, [s for s in ACTIVE6 if s not in f3b])
digest_parts.append([f3[:3], f3[-3:]])

# ---------------------------------------------------------------- V4 ------ #
app, be = app_for(tp, ACTIVE6, CLOSED28, scope="active", extras=["NVDA.US", "PNFP.US"])
run(app, PRE)
f4 = fetched(be)
T("V4 scope=active: only active + pinned extras fetched (closed skipped, covered dropped)",
  sorted(f4) == sorted(set(ACTIVE6 + ["PNFP.US"]) - {"DDI.US"}), f4)
digest_parts.append(f4)

# ---------------------------------------------------------------- V5 ------ #
app, be = app_for(tp, ACTIVE6, CLOSED40, by_status_ok=False, cap=40)
run(app, [])
f5 = fetched(be)
if tb is not None:
    appb, beb = app_for(tb, ACTIVE6, CLOSED40, cap=40)
    run(appb, [])
    T("V5 by-status loader None -> legacy path == base (same alphabetical cut)", f5 == fetched(beb))
else:
    T("V5 by-status loader None -> legacy cut (first 40 sorted)", f5 == sorted(set(ACTIVE6 + CLOSED40))[:40])
digest_parts.append(f5[:3])

# ---------------------------------------------------------------- V6 ------ #
app, be = app_for(tp, ACTIVE6, CLOSED40, extras=["ZZZ.US"], cap=40)
run(app, [{"symbol": s} for s in ACTIVE6])              # every active already covered
f6 = fetched(be)
T("V6 covered actives not refetched; pinned ZZZ.US kept; closed fill the cap",
  "ZZZ.US" in f6 and not any(s in f6 for s in ACTIVE6) and len(f6) == 40, len(f6))
digest_parts.append(len(f6))

# ---------------------------------------------------------------- V7 ------ #
led = os.environ.get("TP_LEDGER_TSV", "")
if led and os.path.exists(led):
    with open(led, encoding="utf-8", errors="replace", newline="") as fh:
        matrix = list(csv.reader(fh, delimiter="\t"))
    act, cls = tp._extract_costbasis_by_status(matrix)
    T("V7 real 2026-09-30 ledger: 6 active / 30 closed, no junk",
      sorted(act) == sorted(ACTIVE6) and len(cls) == 30 and all(tp._valid_symbol_shape(s) for s in act + cls), (act, len(cls)))
    digest_parts.append([sorted(act), len(cls)])
else:
    out.append("SKIP V7 (set TP_LEDGER_TSV=<_Portfolio_CostBasis export>)")

print("\n".join(out))
print("RUN-DIGEST", hashlib.sha256(json.dumps(digest_parts, sort_keys=True, default=str).encode()).hexdigest()[:16])

# The battery above runs at import. Re-enable logging for the rest of the
# process: pytest imports every module before running any test, and a
# process-wide logging.disable() blanked the records that the redaction suites
# (test_redaction_boundaries, test_route_error_redaction) assert on.
logging.disable(logging.NOTSET)


def test_track_force_coverage_harness():
    """pytest entry point: every V-case above passed (T() raises on the first failure)."""
    assert out and all(line.startswith(("PASS ", "SKIP ")) for line in out), out
