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
    import_block = import_anchor + '''
try:
    from scripts.critical_symbol_identity import (
        build_isolated_batches,
        fail_result_on_identity,
        quarantine_critical_rows,
        sanitize_active_universe,
    )
except ModuleNotFoundError:  # direct ``python scripts/run_dashboard_sync.py``
    from critical_symbol_identity import (
        build_isolated_batches,
        fail_result_on_identity,
        quarantine_critical_rows,
        sanitize_active_universe,
    )
'''
    text = replace_once(text, import_anchor, import_block, "critical policy import")

    readback_old = '''                    _existing_syms = _clean_syms
            if _existing_syms:
                symbols = _existing_syms
'''
    readback_new = '''                    _existing_syms = _clean_syms
            if _existing_syms:
                _existing_syms, _critical_universe_changes = sanitize_active_universe(
                    _existing_syms
                )
                if _critical_universe_changes:
                    _change_notes = []
                    for _change in _critical_universe_changes[:20]:
                        if _change.target_symbol:
                            _change_notes.append(
                                f"{_change.source_symbol}->{_change.target_symbol} "
                                f"({_change.action})"
                            )
                        else:
                            _change_notes.append(
                                f"{_change.source_symbol} ({_change.action}: "
                                f"{_change.reason})"
                            )
                    _cw = (
                        "[CRITICAL-IDENTITY v1.0.0] sanitized active universe on "
                        f"'{task.sheet_name}': " + "; ".join(_change_notes)
                    )
                    res.warnings.append(_cw)
                    logger.warning(_cw)
            if _existing_syms:
                symbols = _existing_syms
'''
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
    critical_gate = '''        # --- v6.30.0: exact critical Symbol->Issuer firewall -----------------
        # Page-level anchor thresholds intentionally tolerate one mismatch; these
        # known collision symbols do not. Purge a poisoned predecessor by writing
        # a tagged symbol-only stub, then force the page result RED after write.
        if (task.expects_rows and rows_matrix and headers
                and task.sheet_name in _RANKED_MARKET_PAGES):
            rows_matrix, _critical_identity_failures = quarantine_critical_rows(
                headers, rows_matrix
            )
            if _critical_identity_failures:
                _cf = (
                    "[CRITICAL-IDENTITY v1.0.0] quarantined "
                    f"{len(_critical_identity_failures)} exact identity mismatch(es) "
                    f"on '{task.sheet_name}': "
                    + "; ".join(
                        f"{_f.symbol}={_f.seen_name!r} ({_f.reason})"
                        for _f in _critical_identity_failures[:10]
                    )
                    + " — page verdict will be failed even if the stub write succeeds."
                )
                res.warnings.append(_cf)
                logger.error(_cf)

'''
    text = replace_once(text, runlog_anchor, critical_gate + runlog_anchor, "critical row gate")

    status_old = '''            else:
                res.rows_failed = max(0, len(rows_matrix) - res.rows_written)
                res.status = "success" if res.rows_failed == 0 else ("partial" if res.rows_written > 0 else "failed")
        except Exception as e:
'''
    status_new = '''            else:
                res.rows_failed = max(0, len(rows_matrix) - res.rows_written)
                res.status = "success" if res.rows_failed == 0 else ("partial" if res.rows_written > 0 else "failed")
            if _critical_identity_failures:
                fail_result_on_identity(res, _critical_identity_failures)
        except Exception as e:
'''
    text = replace_once(text, status_old, status_new, "failed page verdict override")

    RUNNER.write_text(text, encoding="utf-8")


def patch_required_ci_tests() -> None:
    text = RECENT_TESTS.read_text(encoding="utf-8")
    marker = "# (e) run_dashboard_sync v6.30.0 — critical symbol identity"
    if marker in text:
        raise SystemExit("required CI tests already contain critical identity block")
    addition = '''

# --------------------------------------------------------------------------- #
# (e) run_dashboard_sync v6.30.0 — critical symbol identity
# --------------------------------------------------------------------------- #
def test_rds_critical_identity_policy_is_wired():
    rds = _rds()
    assert _ver_at_least(rds.SCRIPT_VERSION, "6.30.0")
    clean, changes = rds.sanitize_active_universe(
        ["BK", "BRK-B", "FI", "3001.SR", "8270.SR", "4328.SR"]
    )
    assert clean == ["BK.US", "BRK-B.US", "FISV.US"]
    assert len(changes) == 6


def test_rds_critical_symbols_are_isolated_before_normal_batches():
    rds = _rds()
    assert rds.build_isolated_batches(
        ["AAPL", "BK.US", "MSFT", "BRK-B.US", "FISV.US"], 2
    ) == [["BK.US"], ["BRK-B.US"], ["FISV.US"], ["AAPL", "MSFT"]]


def test_rds_wrong_critical_issuer_cannot_report_success():
    rds = _rds()
    headers = ["Symbol", "Name", "Exchange", "Currency", "Country", "Warnings"]
    rows = [["BK.US", "Hanwha Aerospace Co., Ltd.", "NYSE", "USD", "USA", ""]]
    _, failures = rds.quarantine_critical_rows(headers, rows)
    assert failures and rows[0][1] == ""
    result = type("Result", (), {"status": "success", "rows_failed": 0, "error": None})()
    rds.fail_result_on_identity(result, failures)
    assert result.status == "failed"
'''
    RECENT_TESTS.write_text(text.rstrip() + addition + "\n", encoding="utf-8")


def main() -> None:
    patch_runner()
    patch_required_ci_tests()
    SELF.unlink(missing_ok=True)
    HELPER.unlink(missing_ok=True)
    print("Applied critical symbol identity integration.")


if __name__ == "__main__":
    main()
