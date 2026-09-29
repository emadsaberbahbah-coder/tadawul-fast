#!/usr/bin/env python3
"""main.py v8.14.0 [/health truth] dual-tree harness.

Loads the REAL delivered main.py (app created, 6 route modules mounted, no
server) and — when MAIN_BASE points at the v8.13.2 file — the REAL base in a
separate subprocess with the same ENV, then compares the _runtime_meta(app)
payloads: every base key must be present and equal (timestamp masked), and
the delivered payload must add exactly "pf_gates" and "deploy".

H1 versions | H2 payload superset + equality vs base | H3 pf_gates shape:
unset vs explicit values, module versions from sys.modules | H4 deploy
shape: RENDER_* set / unset, pid, boot stamp | H5 fail-open: a raising os.getenv
degrades every value to "unset" (leaf-level), never the payload | H6 strict JSON render (allow_nan=False) accepts the
payload | H7 anonymous reduced view unchanged | H8 AST: 0 functions removed.
Run x3, identical digest. Exit 0 = PASS.
"""
import ast
import hashlib
import importlib.util
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
DELIVERED = os.path.join(ROOT, "main.py")
BASE = os.environ.get("MAIN_BASE") or os.path.join(ROOT, "main_base_v8.13.2.py")

RUNNER = r'''
import importlib.util, json, os, sys
sys.path.insert(0, %r)
os.environ["APP_ENV"] = "production"
sp = importlib.util.spec_from_file_location("main_under_test", %r)
m = importlib.util.module_from_spec(sp); sys.modules["main_under_test"] = m; sp.loader.exec_module(m)
meta = m._runtime_meta(m.app)
print("@@JSON@@" + json.dumps(meta, default=str, sort_keys=True))
'''

FIXED_ENV = {
    "APP_ENV": "production",
    "TFB_PF_CONFIRM_SESSION": "observe",
    "TFB_FORECAST_BASIS": "observe",
    "TFB_PF_DD_EXIT": "off",
    "RENDER_GIT_COMMIT": "5b1ea5e0000000000000000000000000deadbeef",
    "RENDER_GIT_BRANCH": "main",
    "RENDER_SERVICE_NAME": "tadawul-fast-bridge",
}
for k in ("TFB_PF_SWITCH_SCAN", "RENDER_GIT_REPO_SLUG", "RENDER_SERVICE_ID", "RENDER_INSTANCE_ID"):
    os.environ.pop(k, None)


def run_payload(path):
    env = dict(os.environ)
    env.update(FIXED_ENV)
    for k in ("TFB_PF_SWITCH_SCAN", "RENDER_GIT_REPO_SLUG", "RENDER_SERVICE_ID", "RENDER_INSTANCE_ID"):
        env.pop(k, None)
    r = subprocess.run([sys.executable, "-c", RUNNER % (ROOT, path)], capture_output=True, text=True, env=env, timeout=300)
    line = [l for l in r.stdout.splitlines() if l.startswith("@@JSON@@")]
    assert line, (r.stdout[-800:], r.stderr[-1200:])
    return json.loads(line[-1][len("@@JSON@@"):])


def main():
    out, fails = [], 0

    def T(name, cond, detail=""):
        nonlocal fails
        out.append(("PASS " if cond else "FAIL ") + name + ((" | " + str(detail)) if detail else ""))
        if not cond:
            fails += 1

    # in-process delivered module
    for k, v in FIXED_ENV.items():
        os.environ[k] = v
    sp = importlib.util.spec_from_file_location("main_fix", DELIVERED)
    fix = importlib.util.module_from_spec(sp)
    sys.modules["main_fix"] = fix
    sp.loader.exec_module(fix)
    T("H1 entry version", fix.APP_ENTRY_VERSION == "8.14.0" and fix.SERVICE_VERSION == "8.14.0")

    # H2 dual-tree payload comparison
    fx = run_payload(DELIVERED)
    T("H2 delivered payload has the two new keys", "pf_gates" in fx and "deploy" in fx)
    if os.path.exists(BASE):
        bs = run_payload(BASE)
        T("H2 base version", bs.get("entry_version") == "8.13.2")
        missing = [k for k in bs if k not in fx]
        T("H2 every base key present", not missing, missing)
        diff = {k: (bs[k], fx.get(k)) for k in bs if k not in ("timestamp_utc", "entry_version", "service_version", "app_version") and bs[k] != fx.get(k)}
        T("H2 shared keys equal (timestamp/version masked)", not diff, json.dumps(diff)[:300])
        extra = sorted(set(fx) - set(bs))
        T("H2 exactly two additive keys", extra == ["deploy", "pf_gates"], extra)
        T("H2 app_version follows entry (config default)", fx.get("app_version") in ("8.14.0", bs.get("app_version")), fx.get("app_version"))
    else:
        out.append("SKIP H2 base legs (MAIN_BASE not found)")

    # H3 pf_gates shape
    pg = fx["pf_gates"]
    T("H3 pf_gates keys", set(pg) == {"portfolio_actions_version", "opportunity_builder_version", "portfolio_actions", "opportunity_builder"}, sorted(pg))
    T("H3 module versions from sys.modules", pg["portfolio_actions_version"] == "1.13.0" and pg["opportunity_builder_version"] == "1.22.1", (pg["portfolio_actions_version"], pg["opportunity_builder_version"]))
    pa = pg["portfolio_actions"]
    T("H3 explicit values", pa["TFB_PF_CONFIRM_SESSION"] == "observe" and pa["TFB_FORECAST_BASIS"] == "observe" and pa["TFB_PF_DD_EXIT"] == "off")
    T("H3 unset literal", pa["TFB_PF_SWITCH_SCAN"] == "unset" and pg["opportunity_builder"]["TFB_OPP_CASH_FLOOR_SAR"] == "unset")
    T("H3 shared basis in both blocks", pg["opportunity_builder"]["TFB_FORECAST_BASIS"] == "observe")
    T("H3 env list complete", len(pa) == len(fix._PF_GATE_ENVS) == 21 and len(pg["opportunity_builder"]) == len(fix._OB_GATE_ENVS) == 5)
    os.environ["TFB_PF_SWITCH_SCAN"] = "  1  "
    T("H3 call-time read + strip", fix._pf_gates_snapshot()["portfolio_actions"]["TFB_PF_SWITCH_SCAN"] == "1")
    os.environ["TFB_PF_SWITCH_SCAN"] = "   "
    T("H3 whitespace-only reads unset", fix._pf_gates_snapshot()["portfolio_actions"]["TFB_PF_SWITCH_SCAN"] == "unset")
    os.environ.pop("TFB_PF_SWITCH_SCAN", None)

    # H4 deploy shape
    dp = fx["deploy"]
    T("H4 deploy keys", set(dp) == {"render_git_commit", "render_git_branch", "render_git_repo_slug", "render_service_id", "render_service_name", "render_instance_id", "worker_pid", "worker_boot_utc"}, sorted(dp))
    T("H4 commit/branch/name", dp["render_git_commit"] == FIXED_ENV["RENDER_GIT_COMMIT"] and dp["render_git_branch"] == "main" and dp["render_service_name"] == "tadawul-fast-bridge")
    T("H4 unset literals", dp["render_service_id"] == "unset" and dp["render_instance_id"] == "unset" and dp["render_git_repo_slug"] == "unset")
    T("H4 pid int + boot stamp ISO", isinstance(dp["worker_pid"], int) and dp["worker_pid"] > 0 and dp["worker_boot_utc"].endswith("+00:00") and "T" in dp["worker_boot_utc"])
    d1 = fix._deploy_provenance()
    d2 = fix._deploy_provenance()
    T("H4 boot stamp stable within a process", d1["worker_boot_utc"] == d2["worker_boot_utc"] == fix._PROCESS_BOOT_UTC)

    # H5 fail-open
    real_getenv = fix.os.getenv
    def boom(*a, **k):
        raise RuntimeError("env exploded")
    fix.os.getenv = boom
    try:
        pg5 = fix._pf_gates_snapshot()
        T("H5 pf_gates leaf fail-open: full shape, every value unset", set(pg5) == set(pg) and all(v == "unset" for v in pg5["portfolio_actions"].values()) and all(v == "unset" for v in pg5["opportunity_builder"].values()))
        dp5 = fix._deploy_provenance()
        T("H5 deploy leaf fail-open: RENDER_* unset, pid/boot kept", all(dp5[k] == "unset" for k in dp5 if k.startswith("render_")) and dp5["worker_pid"] > 0 and dp5["worker_boot_utc"] == fix._PROCESS_BOOT_UTC)
        T("H5 env_or_unset fail-open", fix._env_or_unset("X") == "unset")
    finally:
        fix.os.getenv = real_getenv
    T("H5 module version absent -> ''", fix._module_version_no_import("core.analysis.does_not_exist", "V") == "")

    # H6 strict JSON render
    try:
        raw = fix._StrictJSONResponse.render(fix._StrictJSONResponse, fix._runtime_meta(fix.app))
        T("H6 strict JSON render ok", b'"pf_gates"' in raw and b'"deploy"' in raw)
    except Exception as e:  # noqa: BLE001
        T("H6 strict JSON render ok", False, repr(e))

    # H7 anonymous reduced view unchanged
    T("H7 public reduced keys unchanged", fix._PUBLIC_STATUS_META_KEYS == {"service", "app_version", "entry_version", "service_version", "env", "timestamp_utc", "python", "routes_mounted", "routes_failed_count", "engine_present", "engine_ready", "engine_version"})

    # H8 AST zero removal
    if os.path.exists(BASE):
        def fns(path):
            t = ast.parse(open(path, encoding="utf-8").read())
            return sorted(n.name for n in ast.walk(t) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)))
        fb, ff = fns(BASE), fns(DELIVERED)
        T("H8 AST zero removal", not (set(fb) - set(ff)) and sorted(set(ff) - set(fb)) == ["_deploy_provenance", "_env_or_unset", "_module_version_no_import", "_pf_gates_snapshot"], (len(fb), len(ff), sorted(set(ff) - set(fb))))

    digest = hashlib.sha256("\n".join(out).encode()).hexdigest()[:12]
    print("\n".join(out))
    print(f"SUMMARY {len(out) - fails}/{len(out)} PASS | digest {digest}")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
