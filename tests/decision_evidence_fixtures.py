"""Synthetic, attributable price observations for decision contract tests.

The HTTP acquisition boundary has separate tests. These fixtures represent
its successful output without calling a provider or disabling ticket guards.
"""
from datetime import datetime, timezone


def observed_price_fields():
    instant = datetime.now(timezone.utc).isoformat()
    return {"data_provider": "yahoo", "last_updated_utc": instant,
            "acquisition_status": "success", "acquisition_provider": "yahoo",
            "acquisition_acquired_at": instant, "acquisition_quote_asof": instant}
