#!/usr/bin/env python3
"""v5.130.1 harness v2.2.0 — audit P1-A/F6/F7 remediation.
OLD/NEW equivalence runs in ISOLATED SUBPROCESSES (own cwd, own
PYTHONPATH root, own sys.modules); parent compares canonical JSON and
each child reports its dependency-stack fingerprints.
In-process sections exercise the REAL DataEngineV5 of the NEW tree only;
data-source seams (fetch patch, enrichment pass) are the only patches.

v2.1.0 (2026-08-20) PORTABILITY REBUILD. v2.0 defaulted NEW_TREE to the
literal "/home/claude/build/work" and OLD_TREE to a sibling of it — paths
that existed only inside the session that authored this file. From a bare
`git clone` the harness died on FileNotFoundError before running a single
assertion, so a committed certification artifact could not be executed by
anyone but its author. Now:
    NEW_TREE resolves from THIS FILE (<repo> = parent of scripts/).
    OLD_TREE is optional; absent -> the differential suite is SKIPPED with
    a message, and (unless --require-old) the run still certifies what it
    could actually execute.
Resolution order, both trees: CLI positional -> env -> derived default.

    harness_v5130_1.py [OLD_TREE] [NEW_TREE]
    harness_v5130_1.py --require-old            # differential is mandatory
"""
import asyncio, importlib.util, json, os, subprocess, sys, copy, tempfile

_ARGV = [a for a in sys.argv[1:] if not a.startswith("--")]
_REQUIRE_OLD = "--require-old" in sys.argv

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(_HERE) if os.path.basename(_HERE) == "scripts" else _HERE

OLD_TREE = (_ARGV[0] if len(_ARGV) > 0
            else os.environ.get("TFB_OLD_TREE", ""))
NEW_TREE = (_ARGV[1] if len(_ARGV) > 1
            else os.environ.get("TFB_NEW_TREE", "") or _REPO)


def _tree_ok(t):
    return bool(t) and os.path.isfile(
        os.path.join(t, "core", "data_engine_v2.py"))


HAVE_OLD = _tree_ok(OLD_TREE)
if not _tree_ok(NEW_TREE):
    print("FATAL: NEW_TREE has no core/data_engine_v2.py: %r" % NEW_TREE)
    sys.exit(2)
print("NEW_TREE : %s" % NEW_TREE)
print("OLD_TREE : %s" % (OLD_TREE if HAVE_OLD else
                         "(not supplied — differential suite SKIPPED)"))
if _REQUIRE_OLD and not HAVE_OLD:
    print("  FAIL  CERT: --require-old set but no usable OLD_TREE")
    sys.exit(1)

PASS = FAIL = 0
def check(label, cond, detail=""):
    global PASS, FAIL
    if cond: PASS += 1; print(f"  PASS  {label}")
    else:    FAIL += 1; print(f"  FAIL  {label}  {detail}")

def clear_env():
    for k in ("TFB_ENGINE_OHLC_COHERENCE","TFB_ENGINE_OHLC_COHERENCE_FINAL",
              "TFB_ENGINE_OHLC_COHERENCE_MODE","TFB_ENGINE_BATCH_FPRINT"):
        os.environ.pop(k, None)

ISP = {"symbol":"ISP.MI","requested_symbol":"ISP.MI","name":"Intesa Sanpaolo",
       "current_price":6.881,"price":6.881,"previous_close":6.9,
       "open_price":9.79,"open":9.79,"day_high":6.925,"day_low":6.865,
       "volume":120000.0,"data_provider":"eodhd"}
CLEAN = {"symbol":"AAPL.US","requested_symbol":"AAPL.US","name":"Apple Inc.",
         "current_price":230.0,"price":230.0,"previous_close":229.0,
         "open_price":229.5,"open":229.5,"day_high":231.0,"day_low":228.5,
         "volume":50_000_000.0,"data_provider":"eodhd"}
def fan(sym, o=3942.0, hi=3950.0, lo=3878.0, v=489916.0):
    return {"symbol":sym,"requested_symbol":sym,"name":"FanCo",
            "current_price":3878.0,"price":3878.0,"previous_close":3932.0,
            "open_price":o,"open":o,"day_high":hi,"day_low":lo,"volume":v,
            "data_provider":"eodhd"}
def warns(r):
    w=r.get("warnings")
    return ";".join(str(x) for x in w) if isinstance(w,list) else ("" if w is None else str(w))

# ========================================================================
# CHILD SCRIPT for subprocess T0 (written to temp, run per tree)
# ========================================================================
CHILD = r'''
import asyncio, importlib, importlib.util, json, os, sys, copy
tree = sys.argv[1]
os.chdir(tree); sys.path.insert(0, tree)
spec = importlib.util.spec_from_file_location("core.data_engine_v2", os.path.join(tree,"core","data_engine_v2.py"))
m = importlib.util.module_from_spec(spec); sys.modules["core.data_engine_v2"]=m
spec.loader.exec_module(m)

ISP = {"symbol":"T0SYM.MI","requested_symbol":"T0SYM.MI","name":"T0 Co",
       "current_price":6.881,"price":6.881,"previous_close":6.9,
       "open_price":9.79,"open":9.79,"day_high":6.925,"day_low":6.865,
       "volume":120000.0,"data_provider":"eodhd"}
CLEAN = {"symbol":"T0CLEAN","requested_symbol":"T0CLEAN","name":"Clean Co",
         "current_price":230.0,"price":230.0,"previous_close":229.0,
         "open_price":229.5,"open":229.5,"day_high":231.0,"day_low":228.5,
         "volume":50000000.0,"data_provider":"eodhd"}
def fan(sym):
    return {"symbol":sym,"requested_symbol":sym,"name":"FanCo",
            "current_price":3878.0,"price":3878.0,"previous_close":3932.0,
            "open_price":3942.0,"open":3942.0,"day_high":3950.0,
            "day_low":3878.0,"volume":489916.0,"data_provider":"eodhd"}

NONDET = ("last_updated_utc","last_updated_riyadh","scoring_updated_utc","scoring_updated_riyadh")
def strip(r):
    r=dict(r)
    for k in NONDET: r.pop(k,None)
    return r

async def main():
    eng = m.DataEngineV5(providers=[])
    rows = {"T0SYM.MI": ISP, "T0CLEAN": CLEAN, "FANA": fan("FANA"), "FANB": fan("FANB")}
    async def impl(sym, page=""): return copy.deepcopy(rows[sym])
    eng._get_enriched_quote_impl = impl
    batch = await eng.get_enriched_quotes(list(rows), page="Global_Markets")

    eng2 = m.DataEngineV5(providers=[])
    async def fp(provider, sym, page): return dict(ISP)
    eng2._fetch_patch = fp; eng2._providers_for = lambda p: ["eodhd"]
    frow = await eng2.get_enriched_quote_dict("T0SYM.MI", page="Global_Markets")

    fingerprints = {}
    for name in ("core.scoring","core.reco_normalize","core.surface_action_invariants","core.schema_registry"):
        mod = sys.modules.get(name)
        fingerprints[name] = {"file": getattr(mod,"__file__",None),
                              "version": getattr(mod,"__version__",None)} if mod else None
    print(json.dumps({"engine_version": m.__version__,
                      "stack": fingerprints,
                      "batch": [strip(r) for r in batch],
                      "factory": strip(frow)}, sort_keys=True, default=str))
asyncio.run(main())
'''

print("="*74); print("SECTION A — subprocess-isolated T0: OLD tree vs NEW tree, defaults"); print("="*74)
with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
    f.write(CHILD); child_path = f.name
def run_child(tree):
    env = {k:v for k,v in os.environ.items() if not k.startswith("TFB_ENGINE_OHLC") and k!="TFB_ENGINE_BATCH_FPRINT"}
    env["PYTHONPATH"] = tree
    out = subprocess.run([sys.executable, child_path, tree], capture_output=True, text=True, env=env, timeout=300)
    if out.returncode != 0:
        raise RuntimeError(out.stderr[-800:])
    return json.loads(out.stdout.strip().splitlines()[-1])
# v2.1.0: the differential needs BOTH trees. Announcing a skip is not
# skipping — v2.1.0-pre printed "SKIPPED" and then ran the section anyway,
# dying in run_child(). Guard the body, and still exercise the NEW tree so a
# single-tree run certifies what it can rather than certifying nothing.
if HAVE_OLD:
    o = run_child(OLD_TREE); n = run_child(NEW_TREE)
    check("child versions: OLD=5.129.1 NEW=5.130.1",
          o["engine_version"]=="5.129.1" and n["engine_version"]=="5.130.1",
          f"{o['engine_version']} / {n['engine_version']}")
    for nm in o["stack"]:
        fo, fn = o["stack"][nm], n["stack"][nm]
        ok = (fo is None and fn is None) or (fo and fn and str(fo["file"]).startswith(OLD_TREE) and str(fn["file"]).startswith(NEW_TREE))
        check(f"stack isolation: {nm} rooted per-tree", ok, f"{fo} / {fn}")
    check("T0 batch payload identical", json.dumps(o["batch"],sort_keys=True)==json.dumps(n["batch"],sort_keys=True))
    check("T0 factory payload identical", json.dumps(o["factory"],sort_keys=True)==json.dumps(n["factory"],sort_keys=True))
else:
    print("  ....  SECTION A differential skipped (no OLD_TREE)")
    n = run_child(NEW_TREE)
    # v2.2.0 (pre-merge audit F7): smoke mode accepted ANY non-empty
    # version. Era floor instead: this artifact certifies the 5.130.1
    # change, so the engine under test must be >= 5.130.1 — a moving HEAD
    # (5.130.2, 5.130.3, ...) passes; a pre-fix tree cannot.
    def _vt(v):
        try: return tuple(int(x) for x in str(v).split("."))
        except Exception: return (0,)
    check("NEW tree engine version >= 5.130.1 (era floor)",
          _vt(n.get("engine_version")) >= (5, 130, 1),
          str(n.get("engine_version")))
    check("NEW tree stack rooted in NEW_TREE",
          all((v is None) or str(v["file"]).startswith(NEW_TREE)
              for v in n["stack"].values()), str(n["stack"]))

# ========================================================================
print("="*74); print("SECTION B — NEW tree in-process: real class, expanded matrix"); print("="*74)
sys.path.insert(0, NEW_TREE)
spec = importlib.util.spec_from_file_location("de_new", os.path.join(NEW_TREE,"core","data_engine_v2.py"))
new = importlib.util.module_from_spec(spec); sys.modules["de_new"]=new; spec.loader.exec_module(new)
clear_env()

# B1: cache OFF -> ON transition (audit P1-B repro, must now heal)
async def b1():
    eng = new.DataEngineV5(providers=[], cache_ttl_seconds=3600)
    async def fp(provider, sym, page): return dict(ISP)
    eng._fetch_patch = fp; eng._providers_for = lambda p: ["eodhd"]
    r1 = await eng.get_enriched_quote_dict("ISP.MI", page="Global_Markets")   # master OFF -> cached bad
    pre_open = r1.get("open_price")                                            # snapshot BEFORE heal (shared ref)
    os.environ["TFB_ENGINE_OHLC_COHERENCE"]="1"                                # arm in-process
    r2 = await eng.get_enriched_quote_dict("ISP.MI", page="Global_Markets")   # cache hit
    r3 = await eng.get_enriched_quote_dict("ISP.MI", page="Global_Markets")   # idempotency hit
    clear_env()
    return pre_open, r2, r3
pre_open,r2,r3 = asyncio.run(b1())
check("B1 pre-arm: bad 9.79 open cached (repro precondition)", pre_open==9.79, str(pre_open))
check("B1 cache-hit after arming: open HEALED to None", r2.get("open_price") is None and r2.get("open") is None,
      f"open={r2.get('open_price')}")
check("B1 cache-hit tag ':cache' present", ":cache" in warns(r2), warns(r2)[:140])
check("B1 idempotent: second hit, no duplicate tag",
      warns(r3).count("ohlc_incoherent_dropped")==1, warns(r3)[:140])

# B2: cache observe transition — tag only, value preserved
async def b2():
    eng = new.DataEngineV5(providers=[], cache_ttl_seconds=3600)
    async def fp(provider, sym, page): return dict(ISP)
    eng._fetch_patch = fp; eng._providers_for = lambda p: ["eodhd"]
    await eng.get_enriched_quote_dict("ISP.MI", page="Global_Markets")
    os.environ["TFB_ENGINE_OHLC_COHERENCE"]="1"
    os.environ["TFB_ENGINE_OHLC_COHERENCE_MODE"]="observe"
    r_obs = await eng.get_enriched_quote_dict("ISP.MI", page="Global_Markets")
    obs_open = r_obs.get("open_price"); obs_warns = r_obs.get("warnings")
    obs_warns = ";".join(str(x) for x in obs_warns) if isinstance(obs_warns,list) else str(obs_warns or "")
    os.environ.pop("TFB_ENGINE_OHLC_COHERENCE_MODE")                            # observe -> enforce
    r_enf = await eng.get_enriched_quote_dict("ISP.MI", page="Global_Markets")
    clear_env()
    return obs_open, obs_warns, r_enf
obs_open, obs_warns, re_ = asyncio.run(b2())
check("B2 cache observe: value preserved + ':cache:observe' tag",
      obs_open==9.79 and ":cache:observe" in obs_warns, f"open={obs_open} {obs_warns[:120]}")
enf_tags = {t.strip() for t in warns(re_).split(";")}
check("B2 observe->enforce on next hit: healed + exact ':cache' enforce tag",
      re_.get("open_price") is None and "ohlc_incoherent_dropped:open:engine:cache" in enf_tags,
      warns(re_)[:170])

# B3: final-boundary RANGE contamination (audit F6 gap) via crossed enrichment
async def b3(env):
    clear_env(); os.environ.update(env)
    eng = new.DataEngineV5(providers=[])
    base = {k:v for k,v in ISP.items() if k not in ("open_price","open","day_high","day_low")}
    base.update({"symbol":"TASIX","requested_symbol":"TASIX","current_price":10911.58,
                 "price":10911.58,"previous_close":10704.51})
    async def fp(provider, sym, page): return dict(base)
    eng._fetch_patch = fp; eng._providers_for = lambda p: ["eodhd"]
    async def crossed(merged, sym, page):
        merged=dict(merged); merged["day_high"]=9.54; merged["day_low"]=9.53
        merged["open_price"]=9.53; merged["open"]=9.53
        return merged
    eng._apply_yahoo_enrichment_pass = crossed
    row = await eng.get_enriched_quote_dict("TASIX", page="Commodities_FX")
    clear_env(); return row
row = asyncio.run(b3({"TFB_ENGINE_OHLC_COHERENCE":"1"}))
check("B3 foreign RANGE at final boundary: band+open dropped, close kept",
      row.get("day_high") is None and row.get("day_low") is None
      and row.get("open_price") is None and row.get("current_price")==10911.58,
      f"H={row.get('day_high')} L={row.get('day_low')} O={row.get('open_price')}")
check("B3 tag carries range + ':final'", "range" in warns(row) and ":final" in warns(row), warns(row)[:160])
row = asyncio.run(b3({"TFB_ENGINE_OHLC_COHERENCE":"1","TFB_ENGINE_OHLC_COHERENCE_FINAL":"0"}))
check("B3 FINAL=0 leak reproduction still demonstrable (band survives)",
      row.get("day_high")==9.54, f"H={row.get('day_high')}")

# B4: BD symmetric groups — three members, all tagged, no donor direction
async def b4(rows_by_sym, env):
    clear_env(); os.environ.update(env)
    eng = new.DataEngineV5(providers=[])
    async def impl(sym, page=""): return copy.deepcopy(rows_by_sym[sym])
    eng._get_enriched_quote_impl = impl
    out = await eng.get_enriched_quotes(list(rows_by_sym), page="Global_Markets")
    clear_env(); return {r["symbol"]:r for r in out}
trio = {"AAA": fan("AAA"), "BBB": fan("BBB"), "CLEANC": dict(CLEAN, symbol="CLEANC", requested_symbol="CLEANC"), "CCC": fan("CCC")}
by = asyncio.run(b4(trio, {"TFB_ENGINE_BATCH_FPRINT":"1"}))
check("B4 all three members tagged (incl. FIRST)",
      all("batch_value_collision:with=" in warns(by[s]) for s in ("AAA","BBB","CCC")),
      " / ".join(warns(by[s])[:60] for s in ("AAA","BBB","CCC")))
check("B4 first row names co-members, not itself",
      "BBB" in warns(by["AAA"]) and "CCC" in warns(by["AAA"]) and "with=AAA" not in warns(by["AAA"]))
check("B4 clean row untagged", "batch_value_collision" not in warns(by["CLEANC"]))
check("B4 tag-only: values untouched", by["AAA"]["open_price"]==3942.0)

# B5: near-equal rounding — 6dp collapse tags; beyond 6dp does not
near = {"NEAR1": fan("NEAR1", o=3942.0000001), "NEAR2": fan("NEAR2", o=3942.0000004)}
by = asyncio.run(b4(near, {"TFB_ENGINE_BATCH_FPRINT":"1"}))
check("B5 values equal at 6dp -> tagged (documented normalized semantics)",
      "batch_value_collision" in warns(by["NEAR1"]) and "batch_value_collision" in warns(by["NEAR2"]))
far = {"FARA": fan("FARA", o=3942.00001), "FARB": fan("FARB", o=3942.00002)}
by = asyncio.run(b4(far, {"TFB_ENGINE_BATCH_FPRINT":"1"}))
check("B5 values distinct at 6dp -> NOT tagged",
      "batch_value_collision" not in warns(by["FARA"]) and "batch_value_collision" not in warns(by["FARB"]))

# B6: BD idempotency — rescan same rows, zero new tags
clear_env(); os.environ["TFB_ENGINE_BATCH_FPRINT"]="1"
rows = [fan("IDA"), fan("IDB")]
t1 = new._engine_batch_fprint_scan(rows); t2 = new._engine_batch_fprint_scan(rows)
clear_env()
check("B6 first scan tags 2, rescan tags 0", t1==2 and t2==0, f"{t1}/{t2}")
check("B6 no duplicate tag text", warns(rows[0]).count("batch_value_collision")==1)

# B7: health + banner state exposure
os.environ.update({"TFB_ENGINE_OHLC_COHERENCE":"1","TFB_ENGINE_OHLC_COHERENCE_MODE":"observe",
                   "TFB_ENGINE_BATCH_FPRINT":"1"})
eng = new.DataEngineV5(providers=[])
h = eng.health()
oc = h.get("ohlc_coherence") or {}
check("B7 health exposes master/final/mode/thresholds/batch/bd_errors",
      oc.get("master") is True and oc.get("final_boundary") is True
      and oc.get("mode")=="observe" and oc.get("batch_fprint") is True
      and "range_tol" in oc and "ratio_high" in oc and "bd_scan_errors" in oc, json.dumps(oc))
clear_env()
eng = new.DataEngineV5(providers=[])
h2 = eng.health()
check("B7 defaults in health: master False, mode enforce",
      h2["ohlc_coherence"]["master"] is False and h2["ohlc_coherence"]["mode"]=="enforce")

# B8: early-site enforce tag unchanged from v5.123.0 (no suffix)
os.environ["TFB_ENGINE_OHLC_COHERENCE"]="1"
os.environ["TFB_ENGINE_OHLC_COHERENCE_FINAL"]="0"
r = copy.deepcopy(ISP)
tag = new._engine_bc_boundary_policy(r, "early")
check("B8 early enforce tag exact legacy form",
      tag=="ohlc_incoherent_dropped:open:engine" and r.get("open_price") is None, str(tag))
clear_env()

os.unlink(child_path)
print("="*74)
print(f"RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
