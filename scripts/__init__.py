"""Shared initialization for repository scripts.

Only the sync-integrity installer is started here. It is network-idle, performs
no workbook writes, and patches nothing unless ``run_dashboard_sync`` is loaded
in the same process.
"""
from __future__ import annotations

import logging

_log = logging.getLogger(__name__)

try:
    from scripts.sync_integrity_v13 import start_deferred_install

    start_deferred_install()
except Exception as exc:  # pragma: no cover - startup resilience boundary
    _log.warning(
        "scripts sync-integrity installer unavailable (%s: %s)",
        exc.__class__.__name__,
        exc,
    )
