#!/usr/bin/env python3
"""harness_gates_ir096.py — Batch 3.5 (IR-096). REAL engine functions via the
in-tree AST-lift; main.py checked by source contract + a driven payload sim
using the REAL builder expression against fake engine objects."""
import ast, io, logging, math, os, re, sys, typing
sys.path.insert(0, "/home/claude/ci2")
from core.symbols.normalize import normalize_symbol

SRC = io.open("/home/claude/new_eng_51321.py", encoding="utf-8").read()
T = ast.parse(SRC)
LIFT = {"surface_gate_states", "_surface_gate_on", "_mp_blocked_nulls_enabled",
        "_sai_env_combo_state", "_sai_probe",   # combo-state's own dependency
        "_yf_target_fallback_enabled", "_tgt_lkg_enabled"}
G = {"os": os, "re": re, "math": math, "logging": logging,
     "logger": logging.getLogger("x"), "normalize_symbol": normalize_symbol,
     "Any": typing.Any, "Dict": typing.Dict, "List": typing.List,
     "Tuple": typing.Tuple, "Set": typing.Set, "Optional": typing.Optional,
     "Sequence": typing.Sequence, "__version__": "5.132.1"}
for node in T.body:
    if isinstance(node, (ast.Assign, ast.AnnAssign)):
        try: exec(compile(ast.Module([node], []), "<l>", "exec"), G)
        except Exception: pass
got = set()
for node in ast.walk(T):
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in LIFT:
        exec(compile(ast.Module([node], []), "<l>", "exec"), G); got.add(node.name)
assert got == LIFT, LIFT - got

P = F = 0
def check(n, c, d=""):
    global P, F
    if c: P += 1; print("  PASS ", n)
    else: F += 1; print("  FAIL ", n, d)

ENVS = ["TFB_SURFACE_BLOCKED_INVARIANT","TFB_T10_BLOCKED_INVARIANT",
        "TFB_SURFACE_FETCHFAIL_BLOCKED","TFB_SURFACE_WARN_INVEST",
        "TFB_SURFACE_ROW_SANITY","TFB_MP_BLOCKED_NULLS",
        "TFB_YF_TARGET_FALLBACK","TFB_ENGINE_TARGET_KLG"]
def clear():
    for k in ENVS: os.environ.pop(k, None)

print("== H1  all-OFF baseline mirrors today's GUARDS+ line ==")
clear()
st = G["surface_gate_states"]()
check("nine keys", set(st) == {"surface_blocked","surface_fetchfail",
      "surface_warn_invest","surface_row_sanity","mp_blocked_nulls",
      "env_combo","sai_version","yf_target_fallback","engine_target_klg"})
check("all four surface gates False", not any(
      st[k] for k in ("surface_blocked","surface_fetchfail",
                      "surface_warn_invest","surface_row_sanity")))
check("next-arming gates False", st["yf_target_fallback"] is False
      and st["engine_target_klg"] is False)
check("env_combo string ok", st["env_combo"] == "ok")

print("== H2  the arming acceptance test itself ==")
clear(); os.environ["TFB_SURFACE_BLOCKED_INVARIANT"] = "1"
check("primary alias flips surface_blocked True",
      G["surface_gate_states"]()["surface_blocked"] is True)
clear(); os.environ["TFB_T10_BLOCKED_INVARIANT"] = "1"
check("sibling alias flips it too",
      G["surface_gate_states"]()["surface_blocked"] is True)
clear(); os.environ["TFB_SURFACE_FETCHFAIL_BLOCKED"] = "1"
s2 = G["surface_gate_states"]()
check("ladder #2 flips only fetchfail",
      s2["surface_fetchfail"] is True and s2["surface_blocked"] is False)
clear(); os.environ["TFB_YF_TARGET_FALLBACK"] = "1"
check("yf gate visible", G["surface_gate_states"]()["yf_target_fallback"] is True)
clear(); os.environ["TFB_ENGINE_TARGET_KLG"] = "1"
check("klg gate visible", G["surface_gate_states"]()["engine_target_klg"] is True)
clear()

print("== H3  main.py contract + driven payload expression ==")
msrc = io.open("/home/claude/new_main_8131.py", encoding="utf-8").read()
check("exactly one engine_gates PAYLOAD site (comment mentions excluded)",
      msrc.count('"engine_gates": (') == 1)
seg = re.search(r'"engine_gates": \((.*?)\),\n', msrc, re.S).group(1)
check("guarded getattr + None guard",
      "getattr(engine_obj" in seg and "lambda: {}" in seg
      and "engine_obj is not None" in seg)
check("payload key placed after engine_init_error",
      msrc.index('"engine_init_error": engine_init_error')
      < msrc.index('"engine_gates": ('))
expr = compile('(getattr(engine_obj, "surface_gate_states", lambda: {})() '
               'if engine_obj is not None else {})', "<e>", "eval")
class NewEng:  # engine >= 5.132.1
    def surface_gate_states(self): return {"surface_blocked": True}
class OldEng:  # pre-5.132.1 module: attr absent
    pass
check("new engine -> gates dict",
      eval(expr, {"engine_obj": NewEng()}) == {"surface_blocked": True})
check("old engine -> {} (backward-safe)", eval(expr, {"engine_obj": OldEng()}) == {})
check("no engine -> {}", eval(expr, {"engine_obj": None}) == {})
check("version bump 8.13.1", 'APP_ENTRY_VERSION = "8.13.1"' in msrc)

print(f"\nRESULT: {P} passed, {F} failed")
sys.exit(1 if F else 0)
