#!/usr/bin/env python3
"""Replay a captured broker JSON file without contacting a broker or workbook.

Usage: python scripts/tfb_import_executions.py snapshot.json --account-id ACCOUNT
       --state replay.json --report reconciliation.json [--now ISO_UTC]

Use --account-map JSON_FILE for trade_id -> account_id context instead of a
global account when the captured feed omits account IDs. Existing per-record
account IDs must agree with the supplied context. Monetary JSON numbers are
parsed exactly as Decimal. Outputs contain financial records; choose private
paths. No input is changed and no order or ledger amendment is applied.
Omitted commission/fee currency follows the captured feed's execution-currency
contract; explicit commission_currency/fee_currency must match that currency.

An exclusive local lock prevents concurrent state writers. Canonical state
replaces atomically; the report is a regenerable sidecar bound to state_digest,
not a multi-file atomic bundle. Invalid inputs leave both files unchanged.
If report replacement fails after state is accepted, exit 3 tells the caller to
replay the same snapshot to regenerate it. Exit 2 means no state was accepted.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.execution_accounting import AccountingError, _timestamp, replay_executions

SCRIPT_VERSION = "1.0.0"
MAX_INPUT_BYTES = 32 * 1024 * 1024


def _read_json(path: Path, *, exact: bool = False):
    if path.stat().st_size > MAX_INPUT_BYTES:
        raise AccountingError("input file exceeds size limit")
    data = path.read_bytes()
    options = {"parse_float": Decimal} if exact else {}
    return json.loads(data, **options), hashlib.sha256(data).hexdigest()


def _paths(source: Path, state: Path, report: Path, mapping: Path | None):
    paths = [source, state, report] + ([mapping] if mapping is not None else [])
    resolved = [path.resolve() for path in paths]
    if len(set(resolved)) != len(resolved):
        raise AccountingError("source, mapping, state and report paths must be distinct")
    if state.is_symlink() or report.is_symlink():
        raise AccountingError("output symlinks are not supported")
    for output in (state, report):
        if not output.parent.is_dir() or (output.exists() and not output.is_file()):
            raise AccountingError("output requires an existing directory and regular-file path")
    lock_path = state.with_name(state.name + ".lock")
    if lock_path.resolve() in resolved or lock_path.is_symlink():
        raise AccountingError("state lock path collides with an input/output")
    # Detect hard-link aliases too; resolve() alone only detects symlinks.
    existing = [path for path in paths if path.exists()]
    if lock_path.exists() and any(os.path.samefile(lock_path, path) for path in existing):
        raise AccountingError("state lock file aliases an input/output inode")
    for i, first in enumerate(existing):
        if any(os.path.samefile(first, other) for other in existing[i + 1:]):
            raise AccountingError("input/output files alias the same inode")
    return lock_path


@contextmanager
def _exclusive_lock(path: Path):
    try:
        import fcntl
    except ImportError:
        raise AccountingError("this offline CLI requires POSIX local file locking") from None
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise AccountingError("replay state is busy; another importer holds its lock") from None
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def _stage(path: Path, value: dict) -> Path:
    temp = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix="." + path.name + ".", suffix=".tmp", delete=False) as output:
            temp = Path(output.name)
            json.dump(value, output, sort_keys=True, indent=2, allow_nan=False)
            output.write("\n")
            if output.tell() > MAX_INPUT_BYTES:
                raise AccountingError("output state/report exceeds size limit")
            output.flush()
            os.fsync(output.fileno())
        return temp
    except BaseException:
        if temp is not None:
            temp.unlink(missing_ok=True)
        raise


def _sync_directory(path: Path):
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path)
    context = parser.add_mutually_exclusive_group()
    context.add_argument("--account-id")
    context.add_argument("--account-map", type=Path)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--now", help="complete timestamp for deterministic offline replay")
    arguments = parser.parse_args(argv)
    staged = []
    accepted = False
    try:
        lock_path = _paths(arguments.snapshot, arguments.state, arguments.report, arguments.account_map)
        snapshot, source_sha = _read_json(arguments.snapshot, exact=True)
        mapping = _read_json(arguments.account_map)[0] if arguments.account_map else None
        now = _timestamp(arguments.now, "--now") if arguments.now else None
        with _exclusive_lock(lock_path):
            previous = _read_json(arguments.state)[0] if arguments.state.exists() else None
            state, report = replay_executions(snapshot, source_ref=str(arguments.snapshot.resolve()),
                source_sha256=source_sha, account_id=arguments.account_id, account_mapping=mapping,
                previous_state=previous, now_utc=now)
            state_temp = _stage(arguments.state, state)
            staged.append(state_temp)
            report_temp = _stage(arguments.report, report)
            staged.append(report_temp)
            os.replace(state_temp, arguments.state)
            accepted = True
            _sync_directory(arguments.state)
            os.replace(report_temp, arguments.report)
            _sync_directory(arguments.report)
        print(json.dumps({"version": SCRIPT_VERSION, "status": "partial",
                          "execution_count": report["execution_count"],
                          "state_digest": state["state_digest"]}, sort_keys=True))
        return 0
    except (AccountingError, OSError, ValueError, TypeError, OverflowError, RecursionError) as error:
        if accepted:
            sys.stderr.write("State accepted; report may be stale. Replay the same snapshot to regenerate it.\n")
            return 3
        # Do not echo account IDs, financial payloads or malformed source tokens.
        message = str(error) if isinstance(error, AccountingError) else "input or output could not be validated"
        sys.stderr.write(f"Import refused; previous state/report retained: {message}\n")
        return 2
    finally:
        for path in staged:
            path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
