"""Check a declared account capture offline; no broker calls or workbook writes.

Input JSON: holdings, reconciliation_evidence, fx_rates. Stdout contains only
nonidentifying counts/status. --private-report saves conditional quantity
proposals and the certified cash total to an explicitly chosen private path.
Fingerprints and source declarations do not authenticate broker origin.
"""
from __future__ import annotations

import argparse
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.execution_accounting import AccountingError, _timestamp
from core.portfolio_reconciliation import certify_portfolio_inputs, certification_summary

SCRIPT_VERSION = "1.0.0"
MAX_INPUT_BYTES = 2_000_000


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path)
    parser.add_argument("--now", required=True, help="Complete timestamp with timezone for deterministic review")
    parser.add_argument("--private-report", type=Path)
    arguments = parser.parse_args(argv)
    staged = None
    try:
        if not arguments.snapshot.is_file() or arguments.snapshot.stat().st_size > MAX_INPUT_BYTES:
            raise AccountingError("invalid input path or size")
        output = arguments.private_report
        if output is not None:
            if (output.is_symlink() or output.resolve() == arguments.snapshot.resolve()
                    or not output.parent.is_dir() or (output.exists() and not output.is_file())
                    or (output.exists() and os.path.samefile(output, arguments.snapshot))):
                raise AccountingError("invalid private output path")
        raw = arguments.snapshot.read_bytes()
        body = json.loads(raw, parse_float=Decimal)
        if not isinstance(body, dict):
            raise AccountingError("invalid input")
        report = certify_portfolio_inputs(body.get("holdings"), body.get("reconciliation_evidence"), body.get("fx_rates"),
                                         now=_timestamp(arguments.now, "now"), include_proposals=True)
        if output is not None:
            report["input_sha256"] = hashlib.sha256(raw).hexdigest()
            descriptor, name = tempfile.mkstemp(prefix=".portfolio-reconciliation-", dir=output.parent)
            staged = Path(name)
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(report, handle, sort_keys=True, indent=2)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(staged, output)
            staged = None
        print(json.dumps(certification_summary(report), sort_keys=True))
        return 0 if report["funding_eligible"] else 2
    except (AccountingError, OSError, ValueError, TypeError):
        print(json.dumps({"status": "withheld", "error": "portfolio_reconciliation_input_or_output_invalid"}, sort_keys=True))
        return 2
    finally:
        if staged is not None:
            try:
                staged.unlink()
            except OSError:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
