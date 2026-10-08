"""Prospective outcome evidence through real fetch and maturation methods.

Only transport, clock and the lean fixture's offline calendar are substituted.
The installed calendar is exercised separately, including holidays/DST/closes.
No provider, workbook or deployment is contacted.
"""
import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
SPEC = importlib.util.spec_from_file_location("tp_outcome_witness", ROOT / "scripts/track_performance.py")
tp = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = tp
SPEC.loader.exec_module(tp)
REAL_CALENDAR = tp._outcome_calendar_schedule
UTC = timezone.utc
NOW = datetime(2026, 10, 8, 21, tzinfo=UTC)
CLOSE = datetime(2026, 10, 8, 20, tzinfo=UTC)


def run(awaitable):
    return asyncio.run(awaitable)


def calendar_fixture(name, day):
    """Known fixture sessions only; real calendar facts are tested below."""
    around = datetime.fromisoformat(day).replace(tzinfo=UTC)
    out = []
    for offset in range(-14, 15):
        date = around + timedelta(days=offset)
        if (date.weekday() in (4, 5) if name == "XSAU" else date.weekday() >= 5):
            continue
        opening = date.replace(hour=7 if name == "XSAU" else 13, minute=0 if name == "XSAU" else 30)
        closing = date.replace(hour=12 if name == "XSAU" else 20, minute=0)
        out.append((date.date().isoformat(), opening, closing))
    return tuple(out)


@pytest.fixture(autouse=True)
def boundaries(monkeypatch):
    monkeypatch.setattr(tp, "_utc_now", lambda: NOW)
    monkeypatch.setattr(tp.RiyadhTime, "now", staticmethod(lambda: NOW))
    monkeypatch.setattr(tp, "_outcome_calendar_schedule", calendar_fixture)
    monkeypatch.setattr(tp, "_price_fallback_enabled", lambda: False)
    monkeypatch.setattr(tp, "_ca_guard_enabled", lambda: False)
    monkeypatch.setattr(tp, "_feed_legacy", lambda: True)
    monkeypatch.setattr(tp, "_fallback_spacing_sec", lambda: 0)
    monkeypatch.setattr(tp, "_fallback_shuffle_enabled", lambda: False)
    monkeypatch.setattr(tp, "_fallback_retry429_enabled", lambda: False)
    monkeypatch.setattr(tp, "urlopen", lambda *a, **k: pytest.fail("unexpected live transport"))
    monkeypatch.setattr(tp.BackendClient, "post_json", AsyncMock(return_value=({}, None, 200)))


def row(symbol="SYNTH.US", price=110.0, quote=CLOSE, acquired=NOW, **changes):
    result = {"symbol": symbol, "current_price": price, "data_provider": "yahoo_chart",
              "acquisition_status": "success", "acquisition_provider": "yahoo_chart",
              "acquisition_acquired_at": acquired.isoformat(),
              "acquisition_quote_asof": quote.isoformat()}
    result.update(changes)
    return result


def record(symbol="SYNTH.US", target=None, entry=100.0):
    target = target or CLOSE - timedelta(hours=4)
    return tp.PerformanceRecord(
        record_id="synthetic-" + symbol, symbol=symbol, horizon=tp.HorizonType.WEEK_1,
        date_recorded=target - timedelta(days=7), entry_price=entry,
        entry_recommendation=tp.RecommendationType.BUY, entry_score=70,
        entry_risk_bucket="MEDIUM", entry_confidence="HIGH", origin_tab="synthetic",
        target_price=110, target_roi=10, target_date=target, status=tp.PerformanceStatus.ACTIVE)


def app_with_response(rows):
    app = tp.PerformanceTrackerApp.__new__(tp.PerformanceTrackerApp)
    app.backend = tp.BackendClient("https://transport.invalid")
    app.backend.post_json = AsyncMock(return_value=({"rows": rows}, None, 200))
    return app


def receipt(rec):
    return json.loads(rec.notes.rsplit(tp._OUTCOME_RECEIPT, 1)[1])


@pytest.mark.parametrize("price,outcome,roi", [(110, "WIN", 10), (90, "LOSS", -10), (100, "BREAKEVEN", 0)])
def test_real_fetch_and_audit_mature_only_witnessed_close(price, outcome, roi):
    app = app_with_response([row(price=price)])
    rec = record()
    result = run(app.audit_active_records([rec]))
    assert result[0] is rec and rec.status == tp.PerformanceStatus.MATURED
    assert rec.realized_roi == pytest.approx(roi) and rec.outcome == outcome
    proof = receipt(rec)
    assert proof["schema"] == "tfb.outcome-price.v1"
    assert proof["quote_asof"] == CLOSE.isoformat() and proof["acquired_at"] == NOW.isoformat()
    assert proof["target_session_close"] == CLOSE.isoformat()
    assert proof["actual_horizon_days"] == pytest.approx(7 + 4 / 24)
    assert app.backend.post_json.await_count == 1


@pytest.mark.parametrize("changes,reason", [
    ({"acquisition_status": "preserved"}, "acquisition_preserved"),
    ({"warnings": "fetch_failed:provider"}, "failed_or_quarantined_or_preserved"),
    ({"warnings": ["price_unverified_live:history"]}, "failed_or_quarantined_or_preserved"),
    ({"warnings": "price_bar_stale:5"}, "failed_or_quarantined_or_preserved"),
    ({"warnings": "kept_last_good:price"}, "failed_or_quarantined_or_preserved"),
    ({"data_provider": "snapshot"}, "nonlive_provider"),
    ({"acquisition_quote_asof": ""}, "quote_timestamp_unknown"),
    ({"acquisition_quote_asof": "2026-10-08"}, "quote_timestamp_unknown"),
    ({"acquisition_quote_asof": "2026-10-08T20:00:00"}, "quote_timestamp_unknown"),
    ({"acquisition_acquired_at": (NOW - timedelta(days=2)).isoformat()}, "acquired_timestamp_stale"),
    ({"acquisition_acquired_at": (NOW + timedelta(hours=2)).isoformat()}, "acquired_timestamp_future"),
    ({"acquisition_quote_asof": (NOW + timedelta(hours=2)).isoformat()}, "quote_after_acquisition"),
    ({"price_bar_ts": (CLOSE - timedelta(minutes=1)).isoformat()}, "quote_timestamp_conflict"),
    ({"Current Price": 120}, "quote_price_invalid_or_conflicting"),
    ({"ticker": "OTHER.US"}, "symbol_alias_conflict"),
])
def test_real_fetch_and_audit_reject_bad_positive_prices(changes, reason):
    app = app_with_response([row(**changes)])
    rec = record()
    rec.unrealized_roi = 42
    run(app.audit_active_records([rec]))
    assert rec.status == tp.PerformanceStatus.ACTIVE
    assert rec.realized_roi is None and rec.outcome is None
    assert receipt(rec)["reason"] == reason
    assert app._last_audit_stats["outcome_skipped_reasons"] == {reason: 1}


@pytest.mark.parametrize("alias", ["current_price", "price", "last", "last_price"])
def test_boolean_price_alias_cannot_mint_witness(alias):
    source = row()
    source.pop("current_price")
    source[alias] = True
    assert tp._outcome_price_from_row(source, "SYNTH.US", NOW)[0] is None


def test_alias_collapse_cannot_hide_raw_timestamp_conflict():
    source = row(price_bar_ts=CLOSE.isoformat())
    source["Price Bar TS"] = (CLOSE - timedelta(hours=1)).isoformat()
    assert tp._outcome_price_from_row(source, "SYNTH.US", NOW)[1] == "quote_timestamp_conflict"


def test_valid_regular_market_time_cannot_hide_bad_explicit_quote_receipt():
    source = row(acquisition_quote_asof="2026-10-08", regularMarketTime=CLOSE.timestamp())
    assert tp._outcome_price_from_row(source, "SYNTH.US", NOW)[1] == "quote_timestamp_unknown"


def test_publication_stamp_and_generic_timestamp_do_not_prove_acquisition_or_quote():
    source = row()
    for field in ("acquisition_status", "acquisition_provider", "acquisition_acquired_at"):
        source.pop(field)
    source["last_updated"] = NOW.isoformat()
    assert tp._outcome_price_from_row(source, "SYNTH.US", NOW)[1] == "acquisition_receipt_missing"
    source = row(acquisition_quote_asof="", timestamp=CLOSE.isoformat())
    assert tp._outcome_price_from_row(source, "SYNTH.US", NOW)[1] == "quote_timestamp_unknown"


@pytest.mark.parametrize("warning_field", ["warnings", "row_warnings"])
def test_warning_only_typed_acquisition_receipt_supported(warning_field):
    source = row()
    tokens = [f"{key}:{source.pop(key)}" for key in list(source) if key.startswith("acquisition_")]
    source[warning_field] = tokens
    proof, reason = tp._outcome_price_from_row(source, "SYNTH.US", NOW)
    assert proof is not None and not reason


@pytest.mark.parametrize("alias", ["rows", "row_objects", "items", "records", "quotes", "results", "data"])
def test_actual_backend_envelopes_preserve_witness(alias):
    client = tp.BackendClient("https://transport.invalid")
    client.post_json = AsyncMock(return_value=({alias: [row()]}, None, 200))
    result = run(client.fetch_prices(["SYNTH.US"]))
    assert result == {"SYNTH.US": 110}
    assert tp._outcome_map_evidence(result, "SYNTH.US", NOW)[0] is not None


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("second", [row(price=120), row(acquisition_status="preserved")])
def test_same_http_envelope_conflicting_duplicates_fail_closed(reverse, second):
    rows = [row(), second]
    if reverse:
        rows.reverse()
    result = run(app_with_response(rows).backend.fetch_prices(["SYNTH.US"]))
    assert not result and result.reasons["SYNTH.US"] == "quote_duplicate_conflict"


def test_identical_duplicate_is_idempotent_and_separate_healthy_secondary_owns_proof():
    client = tp.BackendClient("https://transport.invalid")
    client.post_json = AsyncMock(side_effect=[({"rows": [row(warnings="fetch_failed:primary")]}, None, 200),
                                            ({"data": {"SYNTH.US": row()}}, None, 200)])
    result = run(client.fetch_prices(["SYNTH.US"]))
    assert result == {"SYNTH.US": 110} and not result.reasons
    assert client.post_json.await_count == 2
    assert run(app_with_response([row(), row()]).backend.fetch_prices(["SYNTH.US"])) == result


def test_unexpected_identity_and_keyed_identity_mismatch_fail_closed():
    result = run(app_with_response([row(symbol="OTHER.US")]).backend.fetch_prices(["SYNTH.US"]))
    assert not result and result.reasons["SYNTH.US"] == "quote_identity_mismatch"
    client = tp.BackendClient("https://transport.invalid")
    client.post_json = AsyncMock(return_value=({"data": {"SYNTH.US": row(symbol="OTHER.US")}}, None, 200))
    result = run(client.fetch_prices(["SYNTH.US"]))
    assert not result and result.reasons["SYNTH.US"] == "quote_identity_mismatch"


def test_numeric_dict_cast_and_value_mutation_destroy_outcome_proof():
    prices = run(app_with_response([row()]).backend.fetch_prices(["SYNTH.US"]))
    assert tp._outcome_map_evidence(dict(prices), "SYNTH.US", NOW)[1] == "outcome_price_evidence_missing"
    for value in (120, True, float("nan")):
        prices["SYNTH.US"] = value
        assert tp._outcome_map_evidence(prices, "SYNTH.US", NOW)[1] == "outcome_price_evidence_conflict"


@pytest.mark.parametrize("quote,target,reason", [
    (CLOSE - timedelta(seconds=1), CLOSE - timedelta(hours=4), "quote_before_target_close"),
    (CLOSE - timedelta(hours=2), CLOSE - timedelta(hours=1), "quote_before_target_time"),
    (CLOSE, CLOSE - timedelta(days=7), "quote_outside_target_session"),
    (CLOSE - timedelta(days=1), CLOSE - timedelta(hours=4), "quote_session_stale"),
    (CLOSE + timedelta(minutes=1), CLOSE - timedelta(hours=4), "quote_session_unknown"),
])
def test_inexact_or_late_quote_never_relabels_intended_horizon(quote, target, reason, monkeypatch):
    monkeypatch.setattr(tp, "_mature_grace_days", lambda: 0)
    rec = record(target=target)
    app = app_with_response([row(quote=quote)])
    run(app.audit_active_records([rec]))
    assert rec.status == tp.PerformanceStatus.ACTIVE and rec.realized_roi is None and rec.outcome is None
    assert receipt(rec)["reason"] == reason


def test_current_session_is_not_closed_even_with_future_clock_tolerance(monkeypatch):
    before = CLOSE - timedelta(minutes=1)
    monkeypatch.setattr(tp, "_utc_now", lambda: before)
    monkeypatch.setattr(tp.RiyadhTime, "now", staticmethod(lambda: before))
    rec = record()
    app = app_with_response([row(acquired=before)])
    run(app.audit_active_records([rec]))
    assert rec.status == tp.PerformanceStatus.ACTIVE
    assert receipt(rec)["reason"] == "target_session_not_complete"


def test_regular_market_receipt_can_explain_bounded_after_close_not_after_hours():
    quote = CLOSE + timedelta(seconds=1)
    source = row(quote=quote, regularMarketTime=quote.timestamp())
    app = app_with_response([source])
    rec = record()
    run(app.audit_active_records([rec]))
    assert rec.status == tp.PerformanceStatus.MATURED
    assert receipt(rec)["quote_kind"] == "regular_market"
    source = row(quote=CLOSE + timedelta(minutes=16), regularMarketTime=(CLOSE + timedelta(minutes=16)).timestamp())
    assert tp._outcome_price_from_row(source, "SYNTH.US", NOW)[0] is None


@pytest.mark.parametrize("blank", [None, ""])
def test_blank_regular_market_field_cannot_borrow_after_close_permission(blank):
    rec = record()
    source = row(quote=CLOSE + timedelta(seconds=1), regularMarketTime=blank)
    run(app_with_response([source]).audit_active_records([rec]))
    assert rec.status == tp.PerformanceStatus.ACTIVE and rec.realized_roi is None
    assert receipt(rec)["reason"] == "quote_session_unknown"


def test_missing_target_and_invalid_entry_are_pending_without_zero_outcome():
    rec = record()
    rec.target_date = None
    app = app_with_response([row()])
    run(app.audit_active_records([rec]))
    assert receipt(rec)["reason"] == "outcome_target_time_unknown"
    for price in (0, float("inf"), float("nan")):
        rec = record(entry=price)
        run(app.audit_active_records([rec]))
        assert rec.status == tp.PerformanceStatus.ACTIVE and rec.realized_roi is None
        assert receipt(rec)["reason"] == "entry_price_invalid"


def test_unknown_calendar_and_missing_calendar_dependency_fail_closed(monkeypatch):
    rec = record(symbol="GC=F")
    app = app_with_response([row(symbol="GC=F")])
    run(app.audit_active_records([rec]))
    assert rec.status == tp.PerformanceStatus.ACTIVE
    assert receipt(rec)["reason"] == "outcome_calendar_unknown"
    monkeypatch.setattr(tp, "_outcome_calendar_schedule", lambda *args: (_ for _ in ()).throw(ImportError()))
    assert tp._outcome_price_from_row(row(), "SYNTH.US", NOW)[1] == "outcome_calendar_unknown"


def test_kill_switch_does_not_restore_false_outcomes_and_unpriced_grace_still_expires(monkeypatch):
    rec = record(target=CLOSE - timedelta(days=20))
    rec.unrealized_roi = 77
    app = app_with_response([])
    monkeypatch.setattr(tp, "_mature_fresh_only_enabled", lambda: False)
    run(app.audit_active_records([rec]))
    assert rec.status == tp.PerformanceStatus.ACTIVE and rec.realized_roi is None
    monkeypatch.setattr(tp, "_mature_fresh_only_enabled", lambda: True)
    run(app.audit_active_records([rec]))
    assert rec.status == tp.PerformanceStatus.EXPIRED and rec.outcome == "UNPRICED"


def test_plain_scalar_backend_and_fallback_cannot_mature(monkeypatch):
    app = app_with_response([])
    app.backend.fetch_prices = AsyncMock(return_value={"SYNTH.US": 150})
    monkeypatch.setattr(tp, "_price_fallback_enabled", lambda: True)
    monkeypatch.setattr(tp, "_yahoo_chart_fallback_prices", AsyncMock(return_value={"SYNTH.US": 160}))
    rec = record()
    run(app.audit_active_records([rec]))
    assert rec.status == tp.PerformanceStatus.ACTIVE and rec.realized_roi is None
    assert "outcome_price_evidence_missing" in receipt(rec)["reason"]


def test_actual_backend_failed_http_remains_pending_after_grace():
    app = app_with_response([])
    app.backend.post_json = AsyncMock(return_value=(None, "HTTP failure", 503))
    rec = record(target=CLOSE - timedelta(days=20))
    run(app.audit_active_records([rec]))
    assert rec.status == tp.PerformanceStatus.ACTIVE and rec.realized_roi is None
    assert receipt(rec)["reason"] == "quote_transport_failed"


def chart(symbol="SYNTH", price=110, quote=CLOSE.timestamp(), **extra):
    return {"chart": {"result": [{"meta": {"symbol": symbol, "regularMarketPrice": price,
                                          "regularMarketTime": quote, **extra}}]}}


class HTTPResponse:
    def __init__(self, payload, status=200):
        self.payload, self.status = payload, status
    async def __aenter__(self):
        return self
    async def __aexit__(self, *args):
        pass
    async def read(self):
        return json.dumps(self.payload).encode()


class HTTPSession:
    def __init__(self, responses):
        self.responses, self.urls = responses, []
    def get(self, url):
        self.urls.append(url)
        return self.responses.pop(0)
    async def close(self):
        pass


@pytest.mark.parametrize("payload,reason", [
    (chart(quote=978307200), "quote_timestamp_stale"),
    (chart(quote=None), "quote_timestamp_unknown"),
    (chart(quote="2026-10-08"), "quote_timestamp_unknown"),
    (chart(symbol="OTHER"), "quote_identity_mismatch"),
    (chart(price=None, previousClose=110), "quote_price_invalid_or_conflicting"),
])
def test_real_yahoo_http_rejects_old_unknown_or_wrong_quote(payload, reason):
    session = HTTPSession([HTTPResponse(payload)])
    evidence, status, actual = run(tp._yahoo_fetch_one_outcome_status(session, "SYNTH.US", 2))
    assert evidence is None and actual == reason and status == "other"
    assert "/SYNTH?" in session.urls[0]


def test_actual_yahoo_fallback_and_audit_use_http_quote_receipt(monkeypatch):
    session = HTTPSession([HTTPResponse(chart())])
    monkeypatch.setattr(tp, "ASYNC_HTTP_AVAILABLE", True)
    monkeypatch.setattr(tp, "aiohttp", SimpleNamespace(ClientSession=lambda **kw: session, ClientTimeout=lambda **kw: None))
    monkeypatch.setattr(tp, "_price_fallback_enabled", lambda: True)
    app = app_with_response([])
    rec = record()
    run(app.audit_active_records([rec]))
    assert rec.status == tp.PerformanceStatus.MATURED and rec.realized_roi == pytest.approx(10)
    assert app._last_audit_stats["fallback_filled"] == 1
    assert receipt(rec)["provider"] == "yahoo_chart" and receipt(rec)["quote_kind"] == "regular_market"
    assert len(session.urls) == 1


def test_old_yahoo_fallback_does_not_turn_stale_unrealized_roi_into_win(monkeypatch):
    session = HTTPSession([HTTPResponse(chart(quote=978307200))])
    monkeypatch.setattr(tp, "ASYNC_HTTP_AVAILABLE", True)
    monkeypatch.setattr(tp, "aiohttp", SimpleNamespace(ClientSession=lambda **kw: session, ClientTimeout=lambda **kw: None))
    monkeypatch.setattr(tp, "_price_fallback_enabled", lambda: True)
    rec = record()
    rec.unrealized_roi = 99
    app = app_with_response([])
    run(app.audit_active_records([rec]))
    assert rec.status == tp.PerformanceStatus.ACTIVE and rec.realized_roi is None and rec.outcome is None
    assert receipt(rec)["reason"] == "quote_timestamp_stale"


def test_sync_urllib_http_receipt_not_scalar_parser(monkeypatch):
    payload = chart()
    class Response:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def read(self):
            return json.dumps(payload).encode()
    calls = []
    monkeypatch.setattr(tp, "urlopen", lambda req, **kw: (calls.append(req.full_url) or Response()))
    evidence, status, reason = run(tp._yahoo_fetch_one_outcome_status(None, "SYNTH.US", 2))
    assert evidence.price == 110 and status == "ok" and not reason
    payload = chart(quote=978307200)
    evidence, reason = tp._yahoo_fetch_one_outcome_sync("SYNTH.US", 2)
    assert evidence is None and reason == "quote_timestamp_stale"
    assert len(calls) == 2


@pytest.mark.parametrize("status,reason", [(429, "quote_http_429"), (403, "quote_http_403"), (500, "quote_http_error")])
def test_yahoo_http_failures_are_reasoned_without_using_prices(status, reason):
    evidence, _, actual = run(tp._yahoo_fetch_one_outcome_status(HTTPSession([HTTPResponse(chart(), status)]), "SYNTH.US", 2))
    assert evidence is None and actual == reason


def test_history_untouched_and_notes_receipt_bounded_roundtrip():
    prior = record(symbol="HIST.US")
    prior.status = tp.PerformanceStatus.MATURED
    prior.realized_roi, prior.outcome, prior.notes = 12.5, "WIN", "historical receipt"
    before = deepcopy(prior.to_dict())
    rec = record()
    rec.notes = "operator note | [v6.22.0 CA-ADJUST] original"
    app = app_with_response([row(quote=CLOSE - timedelta(seconds=1))])
    for _ in range(2):
        run(app.audit_active_records([prior, rec]))
    assert prior.to_dict() == before
    assert rec.notes.count(tp._OUTCOME_RECEIPT) == 1
    assert rec.notes.startswith("operator note | [v6.22.0 CA-ADJUST] original | ")
    store = tp.PerformanceStore.__new__(tp.PerformanceStore)
    cells = store._record_to_row(rec)
    assert len(cells) == len(tp.PerformanceStore.HEADERS) == 32
    restored = tp.PerformanceRecord.from_sheet_row(cells, tp.PerformanceStore.HEADERS)
    assert restored.notes == rec.notes and restored.realized_roi is None
    assert json.loads(json.dumps(restored.to_dict()))["notes"] == rec.notes


def test_corporate_action_guard_still_defers_or_adjusts_without_fetching(monkeypatch):
    monkeypatch.setattr(tp, "_ca_guard_enabled", lambda: True)
    monkeypatch.setattr(tp, "_ca_ledger_factor", lambda *a: None)
    monkeypatch.setattr(tp, "_ca_budget_state", lambda *a: "exhausted")
    rec = record()
    app = app_with_response([row(price=160)])
    run(app.audit_active_records([rec]))
    assert rec.status == tp.PerformanceStatus.ACTIVE
    assert receipt(rec)["reason"] == "corporate_action_verification_pending"
    monkeypatch.setattr(tp, "_ca_budget_state", lambda *a: "available")
    monkeypatch.setattr(tp, "_ca_adjusted_roi", lambda *a: 10)
    run(app.audit_active_records([rec]))
    assert rec.status == tp.PerformanceStatus.MATURED and rec.realized_roi == pytest.approx(10)
    assert receipt(rec)["roi_basis"] == "adjust"


def test_nonfinite_return_never_becomes_win_even_with_guard_disabled():
    rec = record(entry=1e-308)
    app = app_with_response([row(price=1e308)])
    run(app.audit_active_records([rec]))
    assert rec.status == tp.PerformanceStatus.ACTIVE and rec.realized_roi is None and rec.outcome is None
    assert rec.unrealized_roi is None
    assert receipt(rec)["reason"] == "outcome_roi_nonfinite"


def test_nonfinite_adjusted_return_is_also_pending(monkeypatch):
    monkeypatch.setattr(tp, "_ca_guard_enabled", lambda: True)
    monkeypatch.setattr(tp, "_ca_ledger_factor", lambda *a: None)
    monkeypatch.setattr(tp, "_ca_budget_state", lambda *a: "available")
    monkeypatch.setattr(tp, "_ca_adjusted_roi", lambda *a: 10)
    monkeypatch.setattr(tp, "_ca_decide_v2", lambda *a: ("adjust", float("inf")))
    rec = record()
    run(app_with_response([row(price=160)]).audit_active_records([rec]))
    assert rec.status == tp.PerformanceStatus.ACTIVE and rec.realized_roi is None
    assert receipt(rec)["reason"] == "outcome_roi_nonfinite"


def test_adjusted_history_exact_target_endpoint_not_latest_bar(monkeypatch):
    bars = [{"date": "2026-10-01", "adjclose": 100},
            {"date": "2026-10-08", "adjclose": 110},
            {"date": "2026-10-09", "adjclose": 250}]
    monkeypatch.setattr(tp, "_yf_deep_history", lambda *a, **kw: bars)
    tp._CA_BARS_CACHE.clear()
    entry = datetime(2026, 10, 1, 16, tzinfo=UTC)
    assert tp._ca_adjusted_roi("SYNTH.US", entry, "2026-10-08") == pytest.approx(10)
    assert tp._ca_adjusted_roi("SYNTH.US", entry) == pytest.approx(150)
    assert tp._ca_adjusted_roi("SYNTH.US", entry, "2026-10-07") is None
    bars.append({"date": "2026-10-08", "adjclose": 120})
    assert tp._ca_adjusted_roi("SYNTH.US", entry, "2026-10-08") is None


def test_real_audit_adjustment_is_bound_to_witnessed_target_while_next_session_open(monkeypatch):
    friday = datetime(2026, 10, 9, 15, tzinfo=UTC)
    monkeypatch.setattr(tp, "_utc_now", lambda: friday)
    monkeypatch.setattr(tp.RiyadhTime, "now", staticmethod(lambda: friday))
    monkeypatch.setattr(tp, "_ca_guard_enabled", lambda: True)
    monkeypatch.setattr(tp, "_ca_ledger_factor", lambda *a: None)
    bars = [{"date": "2026-10-01", "adjclose": 100},
            {"date": "2026-10-08", "adjclose": 110},
            {"date": "2026-10-09", "adjclose": 250}]
    pulls = []
    monkeypatch.setattr(tp, "_yf_deep_history", lambda *a, **kw: (pulls.append(a) or bars))
    rec = record()
    run(app_with_response([row(price=160, acquired=friday)]).audit_active_records([rec]))
    assert rec.status == tp.PerformanceStatus.MATURED and rec.realized_roi == pytest.approx(10)
    assert receipt(rec)["roi_basis"] == "adjust" and receipt(rec)["target_session"] == "2026-10-08"
    assert len(pulls) == 1


@pytest.mark.parametrize("target_bars,reason", [
    ([], "corporate_action_target_close_unavailable"),
    ([{"date": "2026-10-08", "adjclose": 110}, {"date": "2026-10-08", "adjclose": 120}], "corporate_action_target_close_conflict"),
    ([{"date": "2026-10-08", "adjclose": float("inf")}], "corporate_action_target_close_invalid"),
])
def test_real_audit_missing_or_conflicting_ca_target_bar_is_pending(target_bars, reason, monkeypatch):
    monkeypatch.setattr(tp, "_ca_guard_enabled", lambda: True)
    monkeypatch.setattr(tp, "_ca_ledger_factor", lambda *a: None)
    monkeypatch.setattr(tp, "_yf_deep_history", lambda *a, **kw: [{"date": "2026-10-01", "adjclose": 100}] + target_bars)
    rec = record()
    run(app_with_response([row(price=160)]).audit_active_records([rec]))
    assert rec.status == tp.PerformanceStatus.ACTIVE and rec.realized_roi is None and rec.outcome is None
    assert receipt(rec)["reason"] == reason


def test_confirmed_ledger_excludes_actions_effective_after_target(monkeypatch):
    from core import corporate_actions as ca
    index = {"SYNTH.US": [(datetime(2026, 10, 5).date(), 2),
                          (datetime(2026, 10, 9).date(), 3)]}
    monkeypatch.setattr(tp, "_ca_mod", lambda: ca)
    monkeypatch.setattr(tp, "_ca_ledger_index", lambda: index)
    monkeypatch.setattr(tp, "_ca_ledger_enabled", lambda: True)
    entry = datetime(2026, 10, 1, 16, tzinfo=UTC)
    assert tp._ca_ledger_factor("SYNTH.US", entry, "2026-10-08") == 2
    assert tp._ca_ledger_factor("SYNTH.US", entry) == 6
    assert index["SYNTH.US"][-1][1] == 3


def test_real_offline_calendars_dst_holiday_early_close_and_saudi_weekend(monkeypatch):
    pytest.importorskip("exchange_calendars")
    monkeypatch.setattr(tp, "_outcome_calendar_schedule", REAL_CALENDAR)
    def target_check(symbol, target, close, now):
        rec = record(symbol=symbol, target=target)
        evidence, reason = tp._outcome_price_from_row(row(symbol=symbol, quote=close, acquired=now), symbol, now)
        assert evidence is not None and not reason
        context, reason = tp._outcome_target_session(rec, evidence, now)
        assert not reason and context["target_session_close"] == close.isoformat()
    target_check("SYNTH.US", datetime(2026, 10, 8, 14, tzinfo=UTC), CLOSE, NOW)
    # NYSE Thanksgiving closure and following Friday's 13:00 ET early close.
    target_check("SYNTH.US", datetime(2026, 11, 26, 15, tzinfo=UTC),
                 datetime(2026, 11, 27, 18, tzinfo=UTC), datetime(2026, 11, 27, 19, tzinfo=UTC))
    # XSAU Friday/Saturday weekend: first target close is Sunday 12:00 UTC.
    target_check("1234.SR", datetime(2026, 10, 9, 10, tzinfo=UTC),
                 datetime(2026, 10, 11, 12, tzinfo=UTC), datetime(2026, 10, 11, 13, tzinfo=UTC))
    # A closed market weekend may reuse Friday's witnessed close, not an older session.
    friday = datetime(2026, 10, 9, 20, tzinfo=UTC)
    saturday = datetime(2026, 10, 10, 16, tzinfo=UTC)
    evidence, reason = tp._outcome_price_from_row(row(quote=friday, acquired=saturday), "SYNTH.US", saturday)
    assert evidence is not None and not reason
