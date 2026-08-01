"""Shared initialization for repository scripts.

The installers started here are network-idle, perform no workbook writes, and
patch nothing unless ``run_dashboard_sync`` is loaded in the same process.
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

try:
    from scripts.klg_identity_gate_v131 import (
        start_deferred_install as start_klg_identity_install,
    )

    start_klg_identity_install()
except Exception as exc:  # pragma: no cover - startup resilience boundary
    _log.warning(
        "KEEP-LAST-GOOD identity installer unavailable (%s: %s)",
        exc.__class__.__name__,
        exc,
    )
