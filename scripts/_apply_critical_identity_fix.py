#!/usr/bin/env python3
"""One-shot fail-closed patch for critical symbol identity integration."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts" / "run_dashboard_sync.py"
RECENT_TESTS = ROOT / "tests" / "test_recent_fixes.py"
SELF = ROOT / "scripts" / "_apply_critical_identity_fix.py"
HELPER = ROOT / ".github" / "workflows" / "_apply_critical_identity_fix.yml"


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new, 1)


def patch_runner() -> None:
    text = RUNNER.read_text(encoding="utf-8")

    text = replace_once(
        text,
        "TADAWUL FAST BRIDGE — DASHBOARD SYNC RUNNER (v6.28.0)",
        "TADAWUL FAST BRIDGE — DASHBOARD SYNC RUNNER (v6.30.0)",
        "runner title version",
    )
    text = replace_once(
        text,
        'SCRIPT_VERSION = "6.29.0"',
        'SCRIPT_VERSION = "6.30.0"',
        "script version",
    )

    import_anchor = "from typing import Any, Dict, List, Optional, Sequence, Tuple\n"
    import_block = import_anchor + '''\ntry:\n    from scripts.critical_symbol_identity import (\n        build_isolated_batches,\n        fail_result_on_identity,\n        quarantine_critical_rows,\n        sanitize_active_universe,\n    )\nexcept ModuleNotFoundError:  # direct ``python scripts/run_dashboard_sync.py``\n    from critical_symbol_identity import (\n        build_isolated_batches,\n        fail_result_on_identity,\n        quarantine_critical_rows,\n        sanitize_active_universe,\n    )\n'''
    text = replace_once(text, import_anchor, import_block, "critical policy import")

    readback_old = '''                    _existing_syms = _clean_syms\n            if _existing_syms:\n                symbols = _existing_syms\n'''
    readback_new = '''                    _existing_syms = _clean_syms\n            if _existing_syms:\n                _existing_syms, _critical_universe_changes = sanitize_active_universe(\n                    _existing_syms\n                )\n                if _critical_universe_changes:\n                    _change_notes = []\n                    for _change in _critical_universe_changes[:20]:\n                        if _change.target_symbol:\n                            _change_notes.append(\n                                f"{_change.source_symbol}->{_change.target_symbol} "\n                                f"({_change.action})"\n                            )\n                        else:\n                            _change_notes.append(\n                                f"{_change.source_symbol} ({_change.action}: "\n                                f"{_change.reason})"\n                            )\n                    _cw = (\n                        "[CRITICAL-IDENTITY v1.0.0] sanitized active universe on "\n                        f"'{task.sheet_name}': " + "; ".join(_change_notes)\n                    )\n                    res.warnings.append(_cw)\n                    logger.warning(_cw)\n            if _existing_syms:\n                symbols = _existing_syms\n'''
    text = replace_once(text, readback_old, readback_new, "active universe sanitation")

    text = replace_once(
        text,
        "    batches = [symbols[i:i + size] for i in range(0, len(symbols), size)]\n",
        "    batches = build_isolated_batches(symbols, size)\n",
        "critical singleton batches",
    )

    klg_anchor = "        # --- Keep-last-good substitution (v6.22.3 L4c) ------------------------\n"
    text = replace_once(
        text,
        klg_anchor,
        "        _critical_identity_failures: list = []\n\n" + klg_anchor,
        "critical failure accumulator",
    )

    runlog_anchor = "        # --- v6.24.0 FW-3: workbook verdict line (best-effort) ------------\n"
    critical_gate = '''        # --- v6.30.0: exact critical Symbol->Issuer firewall -----------------\n        # Page-level anchor thresholds intentionally tolerate one mismatch; these\n        # known collision symbols do not. Purge a poisoned predecessor by writing\n        # a tagged symbol-only stub, then force the page result RED after write.\n        if (task.expects_rows and rows_matrix and headers\n                and task.sheet_name in _RANKED_MARKET_PAGES):\n            rows_matrix, _critical_identity_failures = quarantine_critical_rows(\n                headers, rows_matrix\n            )\n            if _critical_identity_failures:\n                _cf = (\n                    "[CRITICAL-IDENTITY v1.0.0] quarantined "\n                    f"{len(_critical_identity_failures)} exact identity mismatch(es) "\n                    f"on '{task.sheet_name}': "\n                    + "; ".join(\n                        f"{_f.symbol}={_f.seen_name!r} ({_f.reason})"\n                        for _f in _critical_identity_failures[:10]\n                    )\n                    + " — page verdict will be failed even if the stub write succeeds."\n                )\n                res.warnings.append(_cf)\n                logger.error(_cf)\n\n'''
    text = replace_once(text, runlog_anchor, critical_gate + runlog_anchor, "critical row gate")

    status_old = '''            else:\n                res.rows_failed = max(0, len(rows_matrix) - res.rows_written)\n                res.status = "success" if res.rows_failed == 0 else ("partial" if res.rows_written > 0 else "failed")\n        except Exception as e:\n'''
    status_new = '''            else:\n                res.rows_failed = max(0, len(rows_matrix) - res.rows_written)\n                res.status = "success" if res.rows_failed == 0 else ("partial" if res.rows_written > 0 else "failed")\n            if _critical_identity_failures:\n                fail_result_on_identity(res, _critical_identity_failures)\n        except Exception as e:\n'''
    text = replace_once(text, status_old, status_new, "failed page verdict override")

    RUNNER.write_text(text, encoding="utf-8")


def patch_required_ci_tests() -> None:
    text = RECENT_TESTS.read_text(encoding="utf-8")
    marker = "# (e) run_dashboard_sync v6.30.0 — critical symbol identity"
    if marker in text:
        raise SystemExit("required CI tests already contain critical identity block")
    addition = r'''\n\n# --------------------------------------------------------------------------- #\n# (e) run_dashboard_sync v6.30.0 — critical symbol identity\n# --------------------------------------------------------------------------- #\ndef test_rds_critical_identity_policy_is_wired():\n    rds = _rds()\n    assert _ver_at_least(rds.SCRIPT_VERSION, "6.30.0")\n    clean, changes = rds.sanitize_active_universe(\n        ["BK", "BRK-B", "FI", "3001.SR", "8270.SR", "4328.SR"]\n    )\n    assert clean == ["BK.US", "BRK-B.US", "FISV.US"]\n    assert len(changes) == 6\n\n\ndef test_rds_critical_symbols_are_isolated_before_normal_batches():\n    rds = _rds()\n    assert rds.build_isolated_batches(\n        ["AAPL", "BK.US", "MSFT", "BRK-B.US", "FISV.US"], 2\n    ) == [["BK.US"], ["BRK-B.US"], ["FISV.US"], ["AAPL", "MSFT"]]\n\n\ndef test_rds_wrong_critical_issuer_cannot_report_success():\n    rds = _rds()\n    headers = ["Symbol", "Name", "Exchange", "Currency", "Country", "Warnings"]\n    rows = [["BK.US", "Hanwha Aerospace Co., Ltd.", "NYSE", "USD", "USA", ""]]\n    _, failures = rds.quarantine_critical_rows(headers, rows)\n    assert failures and rows[0][1] == ""\n    result = type("Result", (), {"status": "success", "rows_failed": 0, "error": None})()\n    rds.fail_result_on_identity(result, failures)\n    assert result.status == "failed"\n'''
    RECENT_TESTS.write_text(text.rstrip() + addition + "\n", encoding="utf-8")


def main() -> None:
    patch_runner()
    patch_required_ci_tests()
    SELF.unlink(missing_ok=True)
    HELPER.unlink(missing_ok=True)
    print("Applied critical symbol identity integration.")


if __name__ == "__main__":
    main()
