#!/usr/bin/env python3
"""harness_gates_8132.py — v8.13.2 chain proof. The centerpiece is
LIVE-EQUIVALENT: the REAL core.data_engine_v2 module (full import, the same
one production loads) sits in sys.modules while the REAL lifted
_engine_gates_snapshot is driven with an instance-like object that lacks the
attr — exactly the production shape that produced {} on the 12:17 boot."""
import ast, io, os, sys, typing
sys.path.insert(0, "/home/claude/ci2")

SRC = io.open("/home/claude/new_main_8132.py", encoding="utf-8").read()
T = ast.parse(SRC)
G = {"sys": sys, "Any": typing.Any, "Dict": typing.Dict}
for node in ast.walk(T):
    if isinstance(node, ast.FunctionDef) and node.name == "_engine_gates_snapshot":
        exec(compile(ast.Module([node], []), "<l>", "exec"), G)
snap = G["_engine_gates_snapshot"]

P = F = 0
def check(n, c, d=""):
    global P, F
    if c: P += 1; print("  PASS ", n)
    else: F += 1; print("  FAIL ", n, d)

for k in ("TFB_SURFACE_BLOCKED_INVARIANT","TFB_T10_BLOCKED_INVARIANT"):
    os.environ.pop(k, None)

class InstanceNoAttr:          # production shape: app.state.engine today
    pass
class InstanceWithHealth:      # legacy engines 5.128.4..5.132.0
    def health(self): return {"surface_invariants": {"surface_blocked": False,
                                                     "legacy": True}}
class FutureInstance:          # hypothetical instance-level exposure
    def surface_gate_states(self): return {"surface_blocked": True,
                                           "via": "instance"}

print("== H1  LIVE-EQUIVALENT: real module + attr-less instance ==")
import core.data_engine_v2 as REAL   # real full import, into sys.modules
g = snap(InstanceNoAttr())
check("returns the REAL nine-key dict (the 12:17 {} is dead)",
      isinstance(g, dict) and len(g) == 9 and g["surface_blocked"] is False
      and g["sai_version"] == "1.4.1")
os.environ["TFB_SURFACE_BLOCKED_INVARIANT"] = "1"
check("the arming acceptance test flips in the SAME artifact",
      snap(InstanceNoAttr())["surface_blocked"] is True)
os.environ.pop("TFB_SURFACE_BLOCKED_INVARIANT")

print("== H2  chain order + fallbacks (module hidden) ==")
saved = sys.modules.pop("core.data_engine_v2")
try:
    check("no module + bare instance -> {}", snap(InstanceNoAttr()) == {})
    check("no module + future instance attr honored",
          snap(FutureInstance()) == {"surface_blocked": True, "via": "instance"})
    check("no module + legacy health() fallback",
          snap(InstanceWithHealth()) == {"surface_blocked": False, "legacy": True})
    check("engine_obj None -> {}", snap(None) == {})
    class Boom:
        def surface_gate_states(self): raise RuntimeError("x")
        def health(self): raise RuntimeError("x")
    check("raising instance -> {} (fail-open)", snap(Boom()) == {})
finally:
    sys.modules["core.data_engine_v2"] = saved

print("== H3  module wins over instance (single source of truth) ==")
check("module beats FutureInstance",
      snap(FutureInstance()).get("via") is None
      and len(snap(FutureInstance())) == 9)

print("== H4  payload + version contracts ==")
check("exactly one payload site, chain form",
      SRC.count('"engine_gates": _engine_gates_snapshot(engine_obj),') == 1)
check("old getattr expression fully gone",
      'getattr(engine_obj, "surface_gate_states", lambda: {})()' not in SRC)
check("version 8.13.2", 'APP_ENTRY_VERSION = "8.13.2"' in SRC)

print(f"\nRESULT: {P} passed, {F} failed")
sys.exit(1 if F else 0)
