"""
scripts/verify_deployment.py — TFB Deployment Verifier
=======================================================
VERSION 1.0.0  (2026-07-19)  — NEW SCRIPT (operations, deliverable #24)

WHY: every session ends with a run of one-line version checks pasted one at a
time, and the state of the flag set has to be reconstructed from memory each
time. Both are mechanical and both are error-prone by hand — a flag believed
armed but actually absent has already cost this project real debugging time.
This collapses the whole check into ONE command.

WHAT IT REPORTS:
  1. VERSIONS  — every Gen-1/Gen-2 module's live __version__, compared against
     an expected manifest. Drift is flagged as BEHIND / AHEAD / MISSING, never
     silently tolerated.
  2. FLAGS     — every environment switch the platform reads, with its live
     value, its default, and whether it is ARMED. Kill-switches are shown with
     their safe direction so "off" is never mistaken for "broken".
  3. SELFTESTS — optional (--selftests): runs each module's own selftest and
     reports pass/fail counts.
  4. VERDICT   — one line: CLEAN, DRIFT, or FAIL.

HONESTY: a module that cannot be imported is reported as MISSING with the
exception type — never counted as passing. An unknown flag value is shown
verbatim rather than coerced.

USAGE:
  python3 scripts/verify_deployment.py              # versions + flags
  python3 scripts/verify_deployment.py --selftests  # also run selftests
  python3 scripts/verify_deployment.py --json       # machine-readable
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

SCRIPT_VERSION = "1.0.0"

# (import path, version attribute, expected version, label)
MODULES: List[Tuple[str, str, str, str]] = [
    ("core.compliance_gate", "__version__", "1.0.0", "compliance gate"),
    ("core.shariah_authority", "__version__", "1.0.0", "shariah authority"),
    ("core.corporate_actions", "__version__", "1.0.0", "corporate actions"),
    ("core.regime", "__version__", "1.0.0", "regime"),
    ("core.risk_limits", "__version__", "1.0.0", "risk limits"),
    ("core.validation", "__version__", "1.0.0", "validation harness"),
    ("core.regret", "__version__", "1.0.0", "regret ledger"),
    ("core.scoring", "__version__", "5.10.0", "scoring"),
    ("core.enriched_quote", "MODULE_VERSION", "4.10.0", "enriched quote"),
    ("core.analysis.opportunity_builder", "OPPORTUNITY_BUILDER_VERSION",
     "1.2.0", "opportunity builder"),
    ("core.analysis.portfolio_actions", "PORTFOLIO_ACTIONS_VERSION",
     "1.2.1", "portfolio actions"),
    ("core.analysis.top10_selector", "TOP10_SELECTOR_VERSION", "4.23.0",
     "top10 selector"),
    ("core.data_engine_v2", "ENGINE_VERSION", "", "data engine (informational)"),
]

# scripts are checked by SCRIPT_VERSION via a light import
SCRIPTS: List[Tuple[str, str, str]] = [
    ("run_shadow_board", "1.1.3", "shadow board"),
    ("run_weekly_brief", "1.0.2", "weekly brief"),
    ("run_shadow_scorer", "1.1.0", "shadow scorer"),
    ("track_performance", "6.24.0", "track performance"),
]

# (env name, default, meaning when ARMED, is_kill_switch)
FLAGS: List[Tuple[str, str, str, bool]] = [
    ("TFB_COMPLIANCE_GATE_ENABLED", "0", "compliance gate evaluates", False),
    ("TFB_SHADOW_COMPLIANCE", "0", "selector logs [SHADOW-GATE]", False),
    ("TFB_ENRICH_COMPLIANCE", "0", "quotes carry compliance stamp", False),
    ("TFB_OPP_NETEDGE_ANNOTATE", "0", "tickets carry net-edge stamp", False),
    ("TFB_PA_HOLDEDGE_ANNOTATE", "0", "holdings carry hold-edge stamp", False),
    ("TFB_OPP_VENUE_FLOORS", "0", "sub-floor tickets defer (D-8)", False),
    ("TFB_SCORE_ROI_SOFTCAP", "0", "ROI soft-cap ends 35/17.5 saturation", False),
    ("TFB_TOP10_TRADABILITY_GATE", "0", "untradable names excluded (D-10)", False),
    ("TFB_OPP_STOP_VOL_UNITS_FIX", "0", "stop volatility units fixed (D-11)", False),
    ("TFB_PA_PROTECT_SUKUK", "1", "sukuk never a SELL leg (D-9)", True),
    ("TFB_TRACK_CA_LEDGER", "1", "confirmed CA forces verification", True),
    ("TRACK_HORIZONS", "1M,3M", "7D/14D checkpoints recorded", False),
    ("TFB_BACKTEST_KSA_YF", "0", "deep history unblocks hypothesis backtest", False),
    ("TFB_BACKTEST_NONOVERLAP", "0", "non-overlapping windows (honest t-stat)", False),
    ("TFB_SYNC_NAME_DEDUP_MODE", "", "duplicate-name quarantine (D-4)", False),
]

_ARMED = {"1", "true", "yes", "on"}


def check_modules() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for path, attr, expected, label in MODULES:
        rec: Dict[str, Any] = {"module": path, "label": label,
                               "expected": expected or None}
        try:
            mod = importlib.import_module(path)
            live = str(getattr(mod, attr, "") or "")
            if not live:                      # tolerate alternate attribute names
                for alt in ("__version__", "MODULE_VERSION", "SCRIPT_VERSION",
                            "ENGINE_VERSION", "VERSION"):
                    live = str(getattr(mod, alt, "") or "")
                    if live:
                        break
            rec["live"] = live or None
            if not expected:
                rec["status"] = "INFO"
            elif not live:
                rec["status"] = "NO_VERSION"
            elif live == expected:
                rec["status"] = "OK"
            else:
                rec["status"] = ("AHEAD" if _newer(live, expected) else "BEHIND")
        except Exception as exc:  # noqa: BLE001
            rec["live"] = None
            rec["status"] = "MISSING"
            rec["error"] = f"{type(exc).__name__}: {exc}"
        out.append(rec)
    return out


def _newer(a: str, b: str) -> bool:
    def parts(v: str) -> List[int]:
        out = []
        for chunk in str(v).split("."):
            digits = "".join(c for c in chunk if c.isdigit())
            out.append(int(digits) if digits else 0)
        return out
    pa, pb = parts(a), parts(b)
    n = max(len(pa), len(pb))
    pa += [0] * (n - len(pa))
    pb += [0] * (n - len(pb))
    return pa > pb


def check_scripts() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    scripts_dir = os.path.join(_ROOT, "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    for name, expected, label in SCRIPTS:
        rec: Dict[str, Any] = {"module": name, "label": label,
                               "expected": expected}
        try:
            mod = importlib.import_module(name)
            live = str(getattr(mod, "SCRIPT_VERSION", "") or "")
            rec["live"] = live or None
            rec["status"] = ("OK" if live == expected else
                             "NO_VERSION" if not live else
                             "AHEAD" if _newer(live, expected) else "BEHIND")
        except Exception as exc:  # noqa: BLE001
            rec["live"] = None
            rec["status"] = "MISSING"
            rec["error"] = f"{type(exc).__name__}: {exc}"
        out.append(rec)
    return out


def check_flags() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for name, default, meaning, kill in FLAGS:
        raw = os.getenv(name)
        live = raw if raw is not None else default
        if name == "TRACK_HORIZONS":
            armed = bool(raw) and ("1W" in raw or "2W" in raw)
        elif name == "TFB_SYNC_NAME_DEDUP_MODE":
            armed = str(live).strip().lower() == "quarantine"
        else:
            armed = str(live).strip().lower() in _ARMED
        out.append({"flag": name, "value": live, "set": raw is not None,
                    "default": default, "armed": armed, "meaning": meaning,
                    "kill_switch": kill})
    return out


def run_selftests(timeout: int = 120) -> List[Dict[str, Any]]:
    targets = [("core/regime.py", "regime"), ("core/risk_limits.py", "risk limits"),
               ("core/validation.py", "validation"), ("core/regret.py", "regret"),
               ("core/compliance_gate.py", "compliance gate"),
               ("core/shariah_authority.py", "shariah authority")]
    out: List[Dict[str, Any]] = []
    for rel, label in targets:
        path = os.path.join(_ROOT, rel)
        if not os.path.exists(path):
            out.append({"target": rel, "label": label, "status": "MISSING"})
            continue
        try:
            p = subprocess.run([sys.executable, path], capture_output=True,
                               text=True, timeout=timeout, cwd=_ROOT)
            tail = [ln for ln in (p.stdout or "").strip().splitlines()
                    if "SELFTEST" in ln]
            out.append({"target": rel, "label": label,
                        "status": "PASS" if p.returncode == 0 else "FAIL",
                        "summary": (tail[-1] if tail else "").strip()})
        except Exception as exc:  # noqa: BLE001
            out.append({"target": rel, "label": label, "status": "ERROR",
                        "summary": f"{type(exc).__name__}: {exc}"})
    return out


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftests", action="store_true",
                    help="also run each module's selftest")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    mods = check_modules()
    scripts = check_scripts()
    flags = check_flags()
    tests = run_selftests() if args.selftests else []

    drift = [m for m in mods + scripts
             if m["status"] in ("BEHIND", "AHEAD", "NO_VERSION")]
    missing = [m for m in mods + scripts if m["status"] == "MISSING"]
    failed = [t for t in tests if t["status"] != "PASS"]
    verdict = ("FAIL" if (missing or failed)
               else "DRIFT" if drift else "CLEAN")

    if args.json:
        print(json.dumps({"version": SCRIPT_VERSION, "verdict": verdict,
                          "modules": mods, "scripts": scripts,
                          "flags": flags, "selftests": tests}, indent=2))
        return 0 if verdict != "FAIL" else 1

    print(f"TFB DEPLOYMENT VERIFIER v{SCRIPT_VERSION}")
    print("=" * 64)
    print("\nMODULES")
    for m in mods + scripts:
        mark = {"OK": "  ok  ", "INFO": " info ", "BEHIND": "BEHIND",
                "AHEAD": "AHEAD ", "MISSING": "MISS! ",
                "NO_VERSION": "NOVER "}.get(m["status"], "  ?   ")
        exp = f"(expected {m['expected']})" if m.get("expected") and m["status"] != "OK" else ""
        print(f"  [{mark}] {m['label']:<28} {str(m.get('live') or '-'):<10} {exp}")
        if m.get("error"):
            print(f"           -> {m['error']}")

    print("\nFLAGS")
    for f in flags:
        state = "ARMED " if f["armed"] else "  off "
        src = "" if f["set"] else "  (not set — using default)"
        note = "  [kill-switch: off DISABLES protection]" if f["kill_switch"] and not f["armed"] else ""
        print(f"  [{state}] {f['flag']:<32} {str(f['value'])[:14]:<15} {f['meaning']}{src}{note}")

    if tests:
        print("\nSELFTESTS")
        for t in tests:
            print(f"  [{t['status']:<5}] {t['label']:<24} {t.get('summary','')}")

    armed_n = sum(1 for f in flags if f["armed"])
    print("\n" + "=" * 64)
    print(f"VERDICT: {verdict}   modules {len(mods)+len(scripts)} "
          f"({len(drift)} drift, {len(missing)} missing) | "
          f"flags {armed_n}/{len(flags)} armed"
          + (f" | selftests {sum(1 for t in tests if t['status']=='PASS')}"
             f"/{len(tests)}" if tests else ""))
    if drift:
        print("  drift: " + ", ".join(f"{m['label']}={m.get('live')}" for m in drift))
    if missing:
        print("  MISSING: " + ", ".join(m["label"] for m in missing))
    unarmed = [f["flag"] for f in flags if not f["armed"] and not f["kill_switch"]]
    if unarmed:
        print("  not armed: " + ", ".join(unarmed))
    return 0 if verdict != "FAIL" else 1


if __name__ == "__main__":
    sys.exit(main())
