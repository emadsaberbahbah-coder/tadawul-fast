"""Project startup hook for the optional decision-safety profile.

Python imports ``sitecustomize`` automatically when the repository is on
``PYTHONPATH``.  The bootstrap defaults to mode ``off`` and therefore changes no
runtime behaviour until the operator explicitly selects ``shadow`` or
``enforce`` with ``TFB_DECISION_SAFETY_MODE``.
"""
from __future__ import annotations

try:
    from core.runtime_decision_safety import bootstrap_from_environment

    bootstrap_from_environment()
except Exception:
    # Startup safety code must never prevent the application from booting.
    pass
