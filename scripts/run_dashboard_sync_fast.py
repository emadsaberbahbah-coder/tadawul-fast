#!/usr/bin/env python3
"""Opt-in bounded-concurrency launcher for the production dashboard sync.

Usage mirrors the existing runner:

    python scripts/run_dashboard_sync_fast.py --sheet-id ... --keys ...

The underlying writer, guards, persistence logic and exit-code policy remain in
``scripts/run_dashboard_sync.py``. Only batched provider fetching is replaced.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import run_dashboard_sync as sync  # noqa: E402
from scripts.concurrent_batch_fetch import install  # noqa: E402

install(sync)

if __name__ == "__main__":
    raise SystemExit(sync.main())
