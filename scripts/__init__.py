"""Network-idle safety initialization for production sync scripts."""
from __future__ import annotations

import logging

_log = logging.getLogger(__name__)

try:
    from scripts.klg_identity_gate_v131 import start_deferred_install

    start_deferred_install()
except Exception as exc:  # pragma: no cover - startup resilience boundary
    _log.warning(
        "KEEP-LAST-GOOD identity installer unavailable (%s: %s)",
        exc.__class__.__name__,
        exc,
    )
