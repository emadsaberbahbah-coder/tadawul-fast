"""Offline TFB-09 witnesses through the actual ADD gate and funding caller.

Symbols, holdings and cache are synthetic. Calendar boundaries retain the
existing conservative UTC-close table; this suite does not certify MICs or a
complete holiday/half-day calendar. Off/observe retain their legacy policy.
"""
from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
import json
import logging

import pytest

from core.analysis import portfolio_actions as pa
from tests.portfolio_reconciliation_fixtures import build_certified_portfolio_actions, synthetic_quote_receipt


ADD, HOLD = pa.ACTION_ADD, pa.ACTION_HOLD
CTL = {"add_confirm_days": 2, "cash_available_sar": 100000, "target_cash_pct": 10,
       "max_position_pct": 15, "max_sector_pct": 30, "min_reliability_add": 70,
       "min_dq_add": 80, "rebalance_mode": "Advisory Only"}


def at(year, month, day, hour=0, minute=0):
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def isolated_policy(monkeypatch):
    for name, value in {"TFB_PF_ENABLED": "1", "TFB_EXIT_BY_RULE_GATE": "0",
                        "TFB_PF_CONFIRM_SESSION": "enforce", "TFB_PF_CONFIRM_PERSIST": "1",
                        "TFB_PF_ADD_CONFIRM_DAYS": "2", "TFB_PF_ADD_CONFIRM_LEGACY_FAILOPEN": "0"}.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("TFB_PF_SESSION_HOLIDAYS", raising=False)
    live, shadow = copy.deepcopy(pa._ADD_CONFIRM_STORE), copy.deepcopy(pa._ADD_CONFIRM_SESSION_STORE)
    pa._ADD_CONFIRM_STORE.clear()
    pa._ADD_CONFIRM_SESSION_STORE.clear()
    clock = {"now": at(2026, 9, 28, 3, 40)}

    class FrozenClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock["now"].astimezone(tz) if tz else clock["now"].replace(tzinfo=None)

    monkeypatch.setattr(pa, "datetime", FrozenClock)
    cache, traffic = {}, []

    def get(key):
        traffic.append(("get", key))
        return copy.deepcopy(cache.get(key))

    def put(key, value):
        traffic.append(("put", key))
        cache[key] = copy.deepcopy(value)

    def delete(key):
        traffic.append(("delete", key))
        cache.pop(key, None)

    monkeypatch.setattr(pa, "_confirm_redis_get", get)
    monkeypatch.setattr(pa, "_confirm_redis_put", put)
    monkeypatch.setattr(pa, "_confirm_redis_del", delete)
    yield clock, cache, traffic
    pa._ADD_CONFIRM_STORE.clear()
    pa._ADD_CONFIRM_STORE.update(live)
    pa._ADD_CONFIRM_SESSION_STORE.clear()
    pa._ADD_CONFIRM_SESSION_STORE.update(shadow)


def _failure(monkeypatch, clock, kind):
    symbol = "SYNTH.US"
    if kind == "unknown":
        symbol = "SYNTH.UNKNOWN"
        clock["now"] = at(2026, 9, 30, 0, 40)
    elif kind in ("key_fault", "previous_fault"):
        def unavailable(*args, **kwargs):
            raise RuntimeError("synthetic calendar unavailable")
        name = "_confirm_session_key" if kind == "key_fault" else "_confirm_prev_session"
        monkeypatch.setattr(pa, name, unavailable)
    elif kind == "malformed_holiday":
        monkeypatch.setenv("TFB_PF_SESSION_HOLIDAYS", "2026-02-31")
    elif kind in ("key_exhaustion", "previous_exhaustion"):
        clock["now"] = at(2026, 9, 28, 3, 40) if kind == "key_exhaustion" else at(2026, 9, 28, 21)
        closed = [at(2026, 9, 27).date() - timedelta(days=i) for i in range(14)]
        monkeypatch.setenv("TFB_PF_SESSION_HOLIDAYS", ",".join(day.isoformat() for day in closed))
    else:
        raise AssertionError(kind)
    return symbol


@pytest.mark.parametrize("kind", ["unknown", "key_fault", "previous_fault", "malformed_holiday",
                                  "key_exhaustion", "previous_exhaustion"])
@pytest.mark.parametrize("legacy_failopen", ["0", "1"])
@pytest.mark.parametrize("persist", ["0", "1"])
def test_enforced_unavailable_calendar_never_advances_or_reads_writes_clock(
        monkeypatch, isolated_policy, kind, legacy_failopen, persist):
    clock, cache, traffic = isolated_policy
    monkeypatch.setenv("TFB_PF_ADD_CONFIRM_LEGACY_FAILOPEN", legacy_failopen)
    monkeypatch.setenv("TFB_PF_CONFIRM_PERSIST", persist)
    symbol = _failure(monkeypatch, clock, kind)
    original = {"count": 1, "date": "2026-09-27" if kind != "unknown" else "2026-09-28"}
    pa._ADD_CONFIRM_STORE[symbol] = copy.deepcopy(original)
    cache[symbol] = copy.deepcopy(original)
    before = copy.deepcopy(cache)
    result = pa._apply_add_confirmation(symbol, ADD, "synthetic qualified signal", None, CTL)
    assert result[0] == HOLD and result[2] == ADD
    assert "[confirm-failclosed:ConfirmCalendarUnavailable]" in result[1]
    assert "calendar unavailable" in result[1] and "clock is untouched" in result[1]
    assert "ADD confirmed" not in result[1]
    assert pa._ADD_CONFIRM_STORE[symbol] == original
    assert cache == before and not traffic
    assert pa._apply_add_confirmation(symbol, ADD, "synthetic qualified signal", None, CTL) == result
    assert pa._ADD_CONFIRM_STORE[symbol] == original and not traffic


def test_unknown_calendar_does_not_create_a_new_confirmation_chain(isolated_policy):
    _, cache, traffic = isolated_policy
    result = pa._apply_add_confirmation("SYNTH.UNKNOWN", ADD, "q", None, CTL)
    assert result[0] == HOLD and "unknown venue suffix UNKNOWN" in result[1]
    assert not pa._ADD_CONFIRM_STORE and not cache and not traffic


@pytest.mark.parametrize("venue", ["UNKNOWN", "", None])
def test_direct_calendar_helpers_refuse_unknown_venue(venue):
    with pytest.raises(pa.ConfirmCalendarUnavailable):
        pa._confirm_session_key(venue, at(2026, 9, 28, 21))
    with pytest.raises(pa.ConfirmCalendarUnavailable):
        pa._confirm_prev_session(venue, "2026-09-28")
    with pytest.raises(pa.ConfirmCalendarUnavailable):
        pa._confirm_is_session_day(venue, at(2026, 9, 28).date())


def _holding(symbol, *, price=34.5, value=41.4, recommendation="BUY"):
    return {**synthetic_quote_receipt(),
            "Symbol": symbol, "Name": "Synthetic calendar holding", "Sector": "Synthetic",
            "Exchange": "Synthetic", "Currency": "SAR", "Quantity": 100, "Buy Price": 30,
            "Current Price": price, "Intrinsic Value": value, "Expected ROI 12M": 15,
            "Forecast Reliability Score": 83, "Data Quality Score": 92, "Risk Bucket": "Low",
            "Provider/Engine Conflict": "No", "Volatility 30D": 4, "Avg Volume 30D": 1000000,
            "Recommendation Detail": recommendation, "Investability Status": "INVESTABLE",
            "Block Reason": ""}


@pytest.mark.parametrize("kind", ["unknown", "key_fault", "previous_fault"])
def test_actual_build_caller_cannot_fund_calendar_unavailable_add(monkeypatch, isolated_policy, kind):
    clock, cache, traffic = isolated_policy
    symbol = _failure(monkeypatch, clock, kind)
    original = {"count": 1, "date": "2026-09-27" if kind != "unknown" else "2026-09-28"}
    pa._ADD_CONFIRM_STORE[symbol] = copy.deepcopy(original)
    cache[symbol] = copy.deepcopy(original)
    result = build_certified_portfolio_actions(pa, [_holding(symbol)], controls=CTL, fx_rates={"SAR": 1})
    row = result["actions"][0]
    assert result["status"] == "ok" and row["action"] == HOLD
    assert row["detail"]["capped_from"] == ADD and "calendar unavailable" in row["action_reason"]
    assert row["suggested_delta_sar"] in (None, 0)
    assert row["suggested_delta_shares"] in (None, 0)
    assert result["kpis"]["adds_funded_sar"] == 0
    alerts = {alert["type"]: alert["count"] for alert in result["alerts"]}
    assert alerts.get("add_confirmation_gate_error") == 1
    assert "add_confirmation_pending" not in alerts
    assert pa._ADD_CONFIRM_STORE[symbol] == cache[symbol] == original and not traffic


@pytest.mark.parametrize("action", [pa.ACTION_HOLD, pa.ACTION_EXIT, pa.ACTION_TRIM, pa.ACTION_BLOCK])
def test_protective_non_add_actions_never_require_calendar_and_reset_their_chain(isolated_policy, action):
    _, cache, traffic = isolated_policy
    symbol = "SYNTH.UNKNOWN"
    pa._ADD_CONFIRM_STORE[symbol] = {"count": 1, "date": "2026-09-27"}
    cache[symbol] = copy.deepcopy(pa._ADD_CONFIRM_STORE[symbol])
    assert pa._apply_add_confirmation(symbol, action, "protective verdict", None, CTL) == (
        action, "protective verdict", None)
    assert symbol not in pa._ADD_CONFIRM_STORE and symbol not in cache
    assert traffic == [("delete", symbol)]


def test_actual_build_preserves_protective_exit_and_hold_without_calendar(monkeypatch, isolated_policy):
    def unavailable(*args, **kwargs):
        raise AssertionError("protective verdict must not ask for a calendar")
    monkeypatch.setattr(pa, "_confirm_clock", unavailable)
    rows = [_holding("SYNEXIT.UNKNOWN", price=50, value=40),
            _holding("SYNHOLD.UNKNOWN", value=34.5, recommendation="HOLD")]
    result = build_certified_portfolio_actions(pa, rows, controls=CTL, fx_rates={"SAR": 1})
    actions = {row["symbol"]: row for row in result["actions"]}
    assert actions["SYNEXIT.UNKNOWN"]["action"] == pa.ACTION_EXIT
    assert actions["SYNEXIT.UNKNOWN"]["proceeds_sar"] == 5000
    assert actions["SYNHOLD.UNKNOWN"]["action"] == HOLD
    assert result["kpis"]["adds_funded_sar"] == 0


@pytest.mark.parametrize("kind", ["unknown", "key_fault", "previous_fault"])
def test_observe_keeps_legacy_verdict_but_freezes_unavailable_shadow(monkeypatch, isolated_policy, kind):
    clock, cache, traffic = isolated_policy
    monkeypatch.setenv("TFB_PF_CONFIRM_SESSION", "observe")
    symbol = _failure(monkeypatch, clock, kind)
    clock["now"] = at(2026, 9, 28, 3, 40)
    pa._ADD_CONFIRM_STORE[symbol] = {"count": 1, "date": "2026-09-27"}
    old_shadow = {"count": 1, "date": "2026-09-25"}
    pa._ADD_CONFIRM_SESSION_STORE[symbol] = copy.deepcopy(old_shadow)
    cache[pa._CONFIRM_SESSION_SHADOW_NS + symbol] = copy.deepcopy(old_shadow)
    legacy = pa._apply_add_confirmation(symbol, ADD, "q", None, CTL)
    assert legacy[0] == ADD and "ADD confirmed (day 2/2)" in legacy[1]
    tagged = pa._apply_confirm_session_observe({"symbol": symbol}, ADD, *legacy, CTL)
    assert tagged.startswith(legacy[1]) and "session evidence unavailable" in tagged
    assert "legacy clock kept" in tagged and "vs session" not in tagged and "session=" not in tagged
    assert pa._ADD_CONFIRM_SESSION_STORE[symbol] == old_shadow
    assert cache[pa._CONFIRM_SESSION_SHADOW_NS + symbol] == old_shadow
    assert all(key != pa._CONFIRM_SESSION_SHADOW_NS + symbol for _, key in traffic)


def test_explicit_off_retains_legacy_policy_without_resolving_unknown_calendar(monkeypatch, isolated_policy):
    monkeypatch.setenv("TFB_PF_CONFIRM_SESSION", "off")
    def unavailable(*args, **kwargs):
        raise AssertionError("off must not request session evidence")
    monkeypatch.setattr(pa, "_confirm_venue", unavailable)
    pa._ADD_CONFIRM_STORE["SYNTH.UNKNOWN"] = {"count": 1, "date": "2026-09-27"}
    result = pa._apply_add_confirmation("SYNTH.UNKNOWN", ADD, "q", None, CTL)
    assert result == (ADD, "q — ADD confirmed (day 2/2)", None)
    assert pa._ADD_CONFIRM_STORE["SYNTH.UNKNOWN"] == {"count": 2, "date": "2026-09-28"}


@pytest.mark.parametrize("mode", ["enforce", "observe"])
@pytest.mark.parametrize("helper", ["_confirm_session_key", "_confirm_prev_session"])
def test_actual_gate_observe_and_build_redact_calendar_fault_diagnostics(
        monkeypatch, isolated_policy, caplog, mode, helper):
    caplog.set_level(logging.WARNING, logger=pa.logger.name)
    monkeypatch.setenv("TFB_PF_CONFIRM_SESSION", mode)
    monkeypatch.setenv("TFB_PF_ADD_CONFIRM_LEGACY_FAILOPEN", "1")
    secret = "tfb-calendar-synthetic-secret"
    def unavailable(*args, **kwargs):
        raise RuntimeError("synthetic calendar failed: Authorization: Bearer " + secret
                           + " request_id=synthetic-calendar-019 " + "x" * 2500)
    monkeypatch.setattr(pa, helper, unavailable)
    pa._ADD_CONFIRM_STORE["SYNTH.US"] = {"count": 1, "date": "2026-09-27"}
    gate = pa._apply_add_confirmation("SYNTH.US", ADD, "q", None, CTL)
    diagnostic = pa._apply_confirm_session_observe({"symbol": "SYNTH.US"}, ADD, *gate, CTL)
    payload = build_certified_portfolio_actions(pa, [_holding("SYNTH.US")], controls=CTL, fx_rates={"SAR": 1})
    published = json.dumps(payload) + diagnostic + gate[1]
    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert caplog.records
    assert secret not in published and secret not in logged
    assert "[REDACTED]" in published and "[REDACTED]" in logged
    assert "RuntimeError" in diagnostic and "synthetic-calendar-019" in diagnostic
    assert len(diagnostic) < 1200
    assert payload["actions"][0]["action"] == (HOLD if mode == "enforce" else ADD)
    if mode == "enforce":
        assert payload["kpis"]["adds_funded_sar"] == 0
        assert pa._ADD_CONFIRM_STORE["SYNTH.US"] == {"count": 1, "date": "2026-09-27"}
    else:
        assert payload["kpis"]["adds_funded_sar"] > 0
    assert not pa._ADD_CONFIRM_SESSION_STORE


@pytest.mark.parametrize("mode", ["enforce", "observe"])
def test_unknown_suffix_and_calendar_logs_redact_user_supplied_credentials(
        monkeypatch, isolated_policy, caplog, mode):
    caplog.set_level(logging.WARNING, logger=pa.logger.name)
    monkeypatch.setenv("TFB_PF_CONFIRM_SESSION", mode)
    secret = "tfb-calendar-synthetic-secret"
    symbol = "SYNTH.Authorization: Bearer " + secret
    gate = pa._apply_add_confirmation(symbol, ADD, "q", None, CTL)
    diagnostic = pa._apply_confirm_session_observe({"symbol": symbol}, ADD, *gate, CTL)
    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert caplog.records
    assert secret.lower() not in (gate[1] + diagnostic + logged).lower()
    assert "[REDACTED]" in diagnostic and "[REDACTED]" in logged
    assert "unknown venue suffix" in diagnostic
    assert not pa._ADD_CONFIRM_SESSION_STORE


@pytest.mark.parametrize("mode", ["enforce", "observe"])
def test_unprintable_calendar_exception_remains_closed_or_observed_without_raising(
        monkeypatch, isolated_policy, mode):
    monkeypatch.setenv("TFB_PF_CONFIRM_SESSION", mode)
    monkeypatch.setenv("TFB_PF_ADD_CONFIRM_LEGACY_FAILOPEN", "1")
    class UnprintableError(RuntimeError):
        def __str__(self):
            raise RuntimeError("synthetic diagnostic string failure")
    def unavailable(*args, **kwargs):
        raise UnprintableError()
    monkeypatch.setattr(pa, "_confirm_session_key", unavailable)
    result = pa._apply_add_confirmation("SYNTH.US", ADD, "q", None, CTL)
    diagnostic = pa._apply_confirm_session_observe({"symbol": "SYNTH.US"}, ADD, *result, CTL)
    assert result[0] == HOLD
    assert "UnprintableError" in diagnostic and "unprintable" in diagnostic
    assert not pa._ADD_CONFIRM_SESSION_STORE


@pytest.mark.parametrize("symbol,when,current,previous", [
    ("SYNTH.US", at(2026, 9, 27, 6), "2026-09-25", "2026-09-24"),
    ("SYNTH.US", at(2026, 9, 28, 20, 59), "2026-09-25", "2026-09-24"),
    ("SYNTH.US", at(2026, 9, 28, 21), "2026-09-28", "2026-09-25"),
    ("SYNTH.US", at(2026, 9, 8, 3), "2026-09-04", "2026-09-03"),
    # DST and half-day windows keep the existing conservative late close.
    ("SYNTH.US", at(2026, 3, 9, 20, 30), "2026-03-06", "2026-03-05"),
    ("SYNTH.US", at(2026, 11, 24, 20, 59), "2026-11-23", "2026-11-20"),
    ("SYNTH.US", at(2026, 11, 27, 18, 30), "2026-11-25", "2026-11-24"),
    ("SYNTH.US", at(2026, 11, 27, 21), "2026-11-27", "2026-11-25"),
    ("SYNTH.SR", at(2026, 9, 24, 6), "2026-09-22", "2026-09-21"),
    ("SYNTH.SR", at(2026, 9, 27, 13), "2026-09-27", "2026-09-24"),
])
def test_known_calendar_boundaries_and_same_session_counts_remain_unchanged(
        isolated_policy, symbol, when, current, previous):
    clock, cache, _ = isolated_policy
    clock["now"] = when
    assert pa._confirm_clock(symbol) == (current, previous, "session")
    pa._ADD_CONFIRM_STORE[symbol] = {"count": 1, "date": previous}
    first = pa._apply_add_confirmation(symbol, ADD, "q", None, CTL)
    assert first[0] == ADD and "(day 2/2; session %s)" % current in first[1]
    assert pa._ADD_CONFIRM_STORE[symbol] == cache[symbol] == {"count": 2, "date": current}
    assert pa._apply_add_confirmation(symbol, ADD, "q", None, CTL) == first
    assert pa._ADD_CONFIRM_STORE[symbol] == cache[symbol] == {"count": 2, "date": current}


SHARE_CLASS_ALIASES = [form for dotted in ("BRK.B", "BF.B", "HEI.A")
                       for form in (dotted, dotted.replace(".", "-"), dotted + ".US",
                                    dotted.replace(".", "-") + ".US")]


@pytest.mark.parametrize("symbol", SHARE_CLASS_ALIASES)
def test_share_class_actual_gate_advances_only_after_a_new_completed_us_session(isolated_policy, symbol):
    clock, cache, _ = isolated_policy
    clock["now"] = at(2026, 10, 2, 21)
    first = pa._apply_add_confirmation(symbol, ADD, "q", None, CTL)
    assert first[0] == HOLD and "day 1/2; session 2026-10-02" in first[1]
    for unchanged in (at(2026, 10, 2, 21), at(2026, 10, 3, 12), at(2026, 10, 4, 12),
                      at(2026, 10, 5, 3, 40), at(2026, 10, 5, 20, 59)):
        clock["now"] = unchanged
        assert pa._apply_add_confirmation(symbol, ADD, "q", None, CTL) == first
        assert pa._ADD_CONFIRM_STORE[symbol] == cache[symbol] == {"count": 1, "date": "2026-10-02"}
    clock["now"] = at(2026, 10, 5, 21)
    confirmed = pa._apply_add_confirmation(symbol, ADD, "q", None, CTL)
    assert confirmed[0] == ADD and "day 2/2; session 2026-10-05" in confirmed[1]
    assert pa._ADD_CONFIRM_STORE[symbol] == cache[symbol] == {"count": 2, "date": "2026-10-05"}
    assert pa._apply_add_confirmation(symbol, ADD, "q", None, CTL) == confirmed


@pytest.mark.parametrize("symbol", SHARE_CLASS_ALIASES)
def test_share_class_actual_build_waits_preclose_then_confirms_and_funds(isolated_policy, symbol):
    clock, cache, _ = isolated_policy
    original = {"count": 1, "date": "2026-10-06"}
    pa._ADD_CONFIRM_STORE[symbol] = copy.deepcopy(original)
    clock["now"] = at(2026, 10, 7, 20, 59)
    held = build_certified_portfolio_actions(pa, [_holding(symbol)], controls=CTL, fx_rates={"SAR": 1})
    assert held["actions"][0]["action"] == HOLD and held["kpis"]["adds_funded_sar"] == 0
    assert pa._ADD_CONFIRM_STORE[symbol] == cache[symbol] == original
    clock["now"] = at(2026, 10, 7, 21)
    ready = build_certified_portfolio_actions(pa, [_holding(symbol)], controls=CTL, fx_rates={"SAR": 1})
    assert ready["actions"][0]["action"] == ADD and ready["kpis"]["adds_funded_sar"] == 12040
    assert "day 2/2; session 2026-10-07" in ready["actions"][0]["action_reason"]
    assert pa._ADD_CONFIRM_STORE[symbol] == cache[symbol] == {"count": 2, "date": "2026-10-07"}
    repeated = build_certified_portfolio_actions(pa, [_holding(symbol)], controls=CTL, fx_rates={"SAR": 1})
    assert repeated["actions"][0]["action"] == ADD and repeated["kpis"]["adds_funded_sar"] == 12040
    assert pa._ADD_CONFIRM_STORE[symbol] == cache[symbol] == {"count": 2, "date": "2026-10-07"}


@pytest.mark.parametrize("symbol,venue", [("ABC.L", "EU"), ("ABC.T", "ASIA"),
                                        ("ABC.V", "AMER"), ("ABC.SI", "ASIA")])
def test_known_exchange_suffix_precedes_share_class_recognition(symbol, venue):
    assert pa._confirm_venue(symbol) == venue


@pytest.mark.parametrize("symbol", ["ABC.F", "ABC.N", "ABC.OQ", "ABC.NYSE", "ABC.SGX",
                                   "ABC.UNKNOWN", "BRK.B.UNKNOWN", "BRK..B", "BRK. B",
                                   "BRK.B.", "BRK-B.B", "123.B", "ABCDEF.B"])
def test_registered_unsupported_and_malformed_suffixes_remain_closed(isolated_policy, symbol):
    clock, cache, traffic = isolated_policy
    clock["now"] = at(2026, 10, 7, 21)
    original = {"count": 1, "date": "2026-10-06"}
    pa._ADD_CONFIRM_STORE[symbol] = copy.deepcopy(original)
    result = pa._apply_add_confirmation(symbol, ADD, "q", None, CTL)
    assert result[0] == HOLD and "calendar unavailable" in result[1]
    assert pa._ADD_CONFIRM_STORE[symbol] == original and not cache and not traffic


@pytest.mark.parametrize("symbol,build_action", [("ABC.İ", pa.ACTION_BLOCK), ("ABC.K", pa.ACTION_BLOCK),
                                                ("KRK.B", pa.ACTION_BLOCK), ("ABC.ı", HOLD), ("ABC.ſ", HOLD)])
def test_unicode_share_class_shape_is_refused_at_resolver_gate_and_actual_build(isolated_policy, symbol, build_action):
    clock, cache, traffic = isolated_policy
    clock["now"] = at(2026, 10, 7, 21)
    with pytest.raises(pa.ConfirmCalendarUnavailable):
        pa._confirm_venue(symbol)
    original = {"count": 1, "date": "2026-10-06"}
    pa._ADD_CONFIRM_STORE[symbol.upper()] = copy.deepcopy(original)
    result = pa._apply_add_confirmation(symbol, ADD, "q", None, CTL)
    assert result[0] == HOLD and "calendar unavailable" in result[1]
    built = build_certified_portfolio_actions(pa, [_holding(symbol)], controls=CTL, fx_rates={"SAR": 1})
    # This caller preserves the supplied candidate symbol; no claim is made
    # about arbitrary upstream normalizers that already changed its identity.
    assert built["actions"][0]["symbol"] == symbol
    # The acquisition parser rejects non-ASCII canonical identities first.
    # Other Unicode forms uppercase to ASCII there, but remain refused by
    # the calendar resolver using the original supplied candidate symbol.
    assert built["actions"][0]["action"] == build_action and built["kpis"]["adds_funded_sar"] == 0
    if build_action == pa.ACTION_BLOCK:
        assert built["meta"]["input_certification"]["reason_counts"]["holding_quote_unverified"] == 1
    else:
        assert "calendar unavailable" in built["actions"][0]["action_reason"]
    assert pa._ADD_CONFIRM_STORE[symbol.upper()] == original and not cache and not traffic


@pytest.mark.parametrize("symbol", ["BRK.B", "BF.B", "HEI.A"])
def test_share_class_registry_override_cannot_create_a_us_calendar(monkeypatch, isolated_policy, symbol):
    from core.symbols import normalize
    suffix = "." + symbol.rsplit(".", 1)[1]
    normalize.split_symbol_exchange.cache_clear()
    monkeypatch.setitem(normalize.EXCHANGE_SUFFIXES, suffix, "synthetic-unsupported-exchange")
    try:
        with pytest.raises(pa.ConfirmCalendarUnavailable):
            pa._confirm_venue(symbol)
        result = pa._apply_add_confirmation(symbol, ADD, "q", None, CTL)
        assert result[0] == HOLD and "calendar unavailable" in result[1]
    finally:
        # Registry consumers cache splits; no test may leave an override result.
        normalize.split_symbol_exchange.cache_clear()


@pytest.mark.parametrize("mode", ["off", "observe"])
@pytest.mark.parametrize("symbol", ["BRK.B", "BF.B", "HEI.A"])
def test_share_class_off_and_observe_preserve_legacy_verdict_with_valid_shadow(
        monkeypatch, isolated_policy, mode, symbol):
    clock, cache, _ = isolated_policy
    monkeypatch.setenv("TFB_PF_CONFIRM_SESSION", mode)
    clock["now"] = at(2026, 10, 7, 21)
    pa._ADD_CONFIRM_STORE[symbol] = {"count": 1, "date": "2026-10-06"}
    pa._ADD_CONFIRM_SESSION_STORE[symbol] = {"count": 1, "date": "2026-10-06"}
    gate = pa._apply_add_confirmation(symbol, ADD, "q", None, CTL)
    assert gate == (ADD, "q — ADD confirmed (day 2/2)", None)
    reason = pa._apply_confirm_session_observe({"symbol": symbol}, ADD, *gate, CTL)
    if mode == "observe":
        assert "venue=US session=2026-10-07" in reason and "vs session 2/2" in reason
        assert "unavailable" not in reason
        assert cache[pa._CONFIRM_SESSION_SHADOW_NS + symbol] == {"count": 2, "date": "2026-10-07"}
    else:
        assert reason == gate[1] and pa._CONFIRM_SESSION_SHADOW_NS + symbol not in cache


@pytest.mark.parametrize("raw", [" brk.b ", " bF.b ", " hei.A "])
@pytest.mark.parametrize("mode", ["enforce", "observe"])
def test_raw_share_class_classification_preserves_uppercase_live_and_shadow_cache_keys(
        monkeypatch, isolated_policy, raw, mode):
    clock, cache, _ = isolated_policy
    monkeypatch.setenv("TFB_PF_CONFIRM_SESSION", mode)
    clock["now"] = at(2026, 10, 7, 21)
    key = raw.strip().upper()
    shadow_key = pa._CONFIRM_SESSION_SHADOW_NS + key
    previous = {"count": 1, "date": "2026-10-06"}
    pa._ADD_CONFIRM_STORE[key] = copy.deepcopy(previous)
    pa._ADD_CONFIRM_SESSION_STORE[key] = copy.deepcopy(previous)
    cache[key] = cache[shadow_key] = copy.deepcopy(previous)
    result = pa._apply_add_confirmation(raw, ADD, "q", None, CTL)
    assert result[0] == ADD
    assert pa._ADD_CONFIRM_STORE == {key: {"count": 2, "date": "2026-10-07"}}
    assert cache[key] == pa._ADD_CONFIRM_STORE[key]
    tagged = pa._apply_confirm_session_observe({"symbol": raw}, ADD, *result, CTL)
    if mode == "observe":
        assert "venue=US session=2026-10-07" in tagged
        assert pa._ADD_CONFIRM_SESSION_STORE[key] == {"count": 2, "date": "2026-10-07"}
    else:
        assert tagged == result[1] and pa._ADD_CONFIRM_SESSION_STORE[key] == previous
    assert set(pa._ADD_CONFIRM_SESSION_STORE) == {key}
    assert set(cache) == {key, shadow_key} and cache[shadow_key] == pa._ADD_CONFIRM_SESSION_STORE[key]


@pytest.mark.parametrize("symbol", ["BRK.B", "BF.B", "HEI.A"])
def test_share_class_calendar_fault_still_freezes_previous_count_date(monkeypatch, isolated_policy, symbol):
    clock, cache, traffic = isolated_policy
    clock["now"] = at(2026, 10, 7, 21)
    def unavailable(*args, **kwargs):
        raise RuntimeError("synthetic unavailable share-class calendar")
    monkeypatch.setattr(pa, "_confirm_session_key", unavailable)
    original = {"count": 1, "date": "2026-10-06"}
    pa._ADD_CONFIRM_STORE[symbol] = copy.deepcopy(original)
    result = pa._apply_add_confirmation(symbol, ADD, "q", None, CTL)
    assert result[0] == HOLD and "calendar unavailable" in result[1]
    assert pa._ADD_CONFIRM_STORE[symbol] == original and not cache and not traffic
