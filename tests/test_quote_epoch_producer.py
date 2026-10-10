"""Actual EODHD epoch producer and engine receipts remain usable offline."""
import asyncio
import copy
from datetime import datetime, timedelta, timezone

import pytest

from core import data_engine_v2 as engine
from core.analysis import opportunity_builder as ob
from core.data_validity import row_acquisition, source_quote_instant
from core.providers import eodhd_provider as eodhd
from scripts import intraday_quote_refresh as intraday
from tests.decision_evidence_fixtures import observed_portfolio


NOW = datetime.now(timezone.utc).replace(microsecond=0)
QUOTE = NOW - timedelta(minutes=2)
ACQUIRED = NOW - timedelta(seconds=5)


class Clock(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW if tz is None else NOW.astimezone(tz)


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(eodhd, "_utc_iso", lambda: ACQUIRED.isoformat())
    monkeypatch.setattr(eodhd, "_riyadh_iso", lambda: ACQUIRED.astimezone(timezone(timedelta(hours=3))).isoformat())
    monkeypatch.setattr(ob, "datetime", Clock)
    monkeypatch.setattr(ob, "_venue_state", lambda *_args: None)
    monkeypatch.setenv("TFB_OPP_ENABLED", "1")
    monkeypatch.setenv("TFB_TICKET_FRESHNESS_GATE", "1")
    monkeypatch.setenv("TFB_TICKET_MAX_QUOTE_AGE_MIN", "15")
    monkeypatch.setenv("TFB_T10_PRICE_XCHECK", "off")


def produced_row(milliseconds=False):
    """Stub only external HTTP; exercise real producer/cache/canonicalization."""
    timestamp = int(QUOTE.timestamp()) * (1000 if milliseconds else 1)

    async def fetch():
        client = eodhd.EODHDClient.__new__(eodhd.EODHDClient)
        client.quote_cache = eodhd._TTLCache(128, 60)
        client._sf = eodhd._SingleFlight()

        async def request(path, params):
            assert path == "real-time/AAPL.US" and params == {}
            return {"code": "AAPL.US", "close": 100, "timestamp": timestamp,
                    "currency": "USD", "exchange": "NASDAQ"}, None

        client._request_json = request
        return await client.fetch_quote("AAPL.US")

    patch, error = asyncio.run(fetch())
    assert error is None and patch["timestamp"] == str(timestamp)
    row = engine._canonicalize_provider_row(patch, requested_symbol="AAPL.US", provider="eodhd")
    assert row["price_bar_ts"] == patch["timestamp"]
    quote_asof = engine._quote_acquisition_asof(row["price_bar_ts"])
    assert quote_asof == QUOTE.isoformat()
    engine._publish_price_acquisition(row, live_priced=True, fallback_source="",
        acquired_at=row["last_updated_utc"], provider="eodhd", quote_asof=quote_asof)
    return row


@pytest.mark.parametrize("milliseconds", [False, True])
def test_actual_eodhd_integer_epoch_string_agrees_with_minted_quote_receipt(milliseconds):
    row = produced_row(milliseconds)
    before = copy.deepcopy(row)
    proof = row_acquisition(row, NOW, 3600)
    assert proof.successful and proof.quote_asof == QUOTE and proof.acquired_at == ACQUIRED
    assert row == before
    quote = intraday._source_quote(row, "AAPL.US", NOW)
    assert quote is not None and quote.quote_asof == QUOTE and quote.price == 100


@pytest.mark.parametrize("milliseconds", [False, True])
def test_actual_producer_receipt_still_funds_public_builder_and_conflict_cannot(milliseconds):
    row = produced_row(milliseconds)
    row.update({"name": "Synthetic producer fixture", "sector": "Technology",
                "intrinsic_value": 125, "expected_roi_12m": 25,
                "forecast_reliability_score": 85, "data_quality_score": 95,
                "risk_bucket": "Low", "volatility_30d": 4,
                "avg_volume_30d": 2_500_000, "recommendation_detail": "BUY",
                "investability_status": "INVESTABLE"})

    def build(source):
        return ob.build_opportunity_payload([source],
            criteria={"trust_gate_enabled": False, "max_weight_pct": 100, "pf_max_sector_pct": 100},
            portfolio=observed_portfolio({"cash_available_sar": 50_000}, {"USD": 3.75, "SAR": 1}),
            fx_rates={"USD": 3.75, "SAR": 1})

    clean = build(row)
    assert clean["selected"] and clean["meta"]["execution_ready"]
    assert clean["selected"][0]["suggested_shares"] > 0
    row["regularMarketTime"] = str(int(QUOTE.timestamp()) - 1)
    assert row_acquisition(row, NOW, 3600).reason == "quote_timestamp_conflict"
    assert intraday._source_quote(row, "AAPL.US", NOW) is None
    assert build(row)["selected"] == []


@pytest.mark.parametrize("bad", [True, False, 0, -1, float("nan"), float("inf"),
    "0", "-1791622800", "NaN", "Infinity", "1e9", "1791622800.5", "9" * 400,
    "2026-10-10", "2026-10-10T09:00:00", "not-a-clock"])
def test_supplier_epoch_parser_keeps_invalid_and_undeclared_encodings_closed(bad):
    assert source_quote_instant(bad) is None


def test_epoch_string_never_replaces_acquisition_or_missing_quote_receipt():
    row = produced_row()
    row["warnings"] = ""
    proof = row_acquisition(row, NOW, 3600)
    assert proof.successful and proof.quote_asof is None and proof.acquired_at == ACQUIRED
    assert intraday._source_quote(row, "AAPL.US", NOW) is None
    row.update({"acquisition_status": "success", "acquisition_provider": "eodhd",
                "acquisition_acquired_at": str(int(ACQUIRED.timestamp())),
                "acquisition_quote_asof": QUOTE.isoformat()})
    proof = row_acquisition(row, NOW, 3600)
    assert proof.status == "UNKNOWN" and proof.reason == "acquisition_time_unknown"
