"""Published calibration needs valid clock, sample and paired MAE evidence."""
from datetime import date, datetime, timedelta, timezone
import json
import math

import pytest

from core.data_validity import MAX_CLOCK_SKEW_SECONDS
from scripts import run_shadow_scorer as scorer


NOW = datetime(2026, 10, 6, 12, tzinfo=timezone.utc)
RIYADH = timezone(timedelta(hours=3))
HEADER = ["As Of (Riyadh)", "State", "N Checkpoints", "Mean Abs Error (pp)",
          "Mean Signed Error (pp)", "Band (pp)", "Min Sample", "By Horizon",
          "Detail", "Writer Version", "Zero MAE (pp)"]


class Worksheet:
    def __init__(self, values):
        self.values = values
        self.reads = 0

    def get_all_values(self):
        self.reads += 1
        if isinstance(self.values, Exception):
            raise self.values
        return self.values


class Sheet:
    def __init__(self, **changes):
        row = ["2026-10-06 14:40:00", "PASS", "41.0", "2", "-0.8", "10",
               "20.0", "1W n=41", "mean |err| 2.00pp | zero_mae=3.00pp",
               "6.42.0", "3"]
        for name, value in changes.items():
            row[HEADER.index(name)] = value
        self.ws = Worksheet([HEADER, row])

    def worksheet(self, name):
        assert name == scorer.TAB_S1_CAL
        return self.ws


@pytest.fixture(autouse=True)
def calibration_policy(monkeypatch):
    monkeypatch.delenv("TFB_S1_CAL_CONSUME", raising=False)
    monkeypatch.delenv("TFB_S1_CAL_MAX_AGE_H", raising=False)


def gate(calibration="PASS", alpha=5.0, ca=True, pit=True):
    return scorer.evaluate_s1(30, [], alpha, calibration, ca, pit,
                              "recorded integrity", "2026-10-01")


def evaluate(base=None, *, mode="enforce", zero=3.0, model=2.0, alpha=5.0):
    return scorer.evaluate_s1_v2(base or gate(), mode, alpha, date(2026, 9, 16),
                                 zero, model, 1)


@pytest.mark.parametrize("state", ["PASS", "FAIL", "PENDING"])
@pytest.mark.parametrize("now", [NOW, NOW.astimezone(RIYADH),
                                  NOW.astimezone(RIYADH).replace(tzinfo=None)])
def test_valid_publication_supports_aware_and_naive_riyadh_clocks(state, now):
    assert scorer.read_s1_calibration(Sheet(State=state), now)[0] == state


def test_default_clock_uses_aware_utc_independent_of_host_timezone(monkeypatch):
    class UTCClock(datetime):
        @classmethod
        def now(cls, tz=None):
            assert tz == timezone.utc
            return NOW

    monkeypatch.setattr(scorer, "datetime", UTCClock)
    assert scorer.read_s1_calibration(Sheet())[0] == "PASS"


@pytest.mark.parametrize("ahead,expected", [
    (MAX_CLOCK_SKEW_SECONDS, "PASS"),
    (MAX_CLOCK_SKEW_SECONDS + 1, "PENDING"),
])
def test_future_evidence_uses_shared_clock_skew_boundary(ahead, expected):
    stamp = (NOW + timedelta(seconds=ahead)).astimezone(RIYADH).isoformat()
    state, detail = scorer.read_s1_calibration(Sheet(**{"As Of (Riyadh)": stamp}), NOW)
    assert state == expected
    if expected == "PENDING":
        assert "timestamp_future" in detail


def test_far_future_pass_cannot_promote():
    state, detail = scorer.read_s1_calibration(
        Sheet(**{"As Of (Riyadh)": "2099-01-01 00:00:00"}), NOW)
    assert state == "PENDING" and "timestamp_future" in detail
    assert gate(calibration=state)["verdict"] == "NOT_DECIDABLE"


@pytest.mark.parametrize("age,expected", [(48 * 3600, "PASS"),
                                           (48 * 3600 + 1, "PENDING")])
def test_full_timezone_offset_is_preserved_at_staleness_boundary(age, expected):
    stamp = (NOW - timedelta(seconds=age)).astimezone(RIYADH).isoformat()
    assert scorer.read_s1_calibration(Sheet(**{"As Of (Riyadh)": stamp}), NOW)[0] == expected


@pytest.mark.parametrize("stamp", ["", "bad", "2026-10-06", "20261006",
                                  "2026-10-06T15", "2026-10-06+03:00",
                                  "2026-10-06 14:40:00garbage"])
def test_unknown_timestamp_or_date_precision_cannot_pass(stamp):
    assert scorer.read_s1_calibration(Sheet(**{"As Of (Riyadh)": stamp}), NOW)[0] == "PENDING"


@pytest.mark.parametrize("policy", ["nan", "inf", "-inf", "-1"])
def test_invalid_max_age_cannot_disable_clock_validation(policy, monkeypatch):
    monkeypatch.setenv("TFB_S1_CAL_MAX_AGE_H", policy)
    state, detail = scorer.read_s1_calibration(Sheet(), NOW)
    assert state == "PENDING" and "timestamp_policy_invalid" in detail


@pytest.mark.parametrize("band", ["nan", "inf", "-1", ""])
def test_invalid_published_band_cannot_justify_pass(band):
    assert scorer.read_s1_calibration(Sheet(**{"Band (pp)": band}), NOW)[0] == "PENDING"


@pytest.mark.parametrize("rows", [[None, []], [HEADER, None]])
def test_malformed_publication_rows_are_pending_without_raising(rows):
    sheet = Sheet()
    sheet.ws.values = rows
    assert scorer.read_s1_calibration(sheet, NOW)[0] == "PENDING"


@pytest.mark.parametrize("count,minimum", [
    ("0", "20"), ("19", "20"), ("-1", "20"), ("20.5", "20"),
    ("nan", "20"), ("inf", "20"), ("", "20"),
    ("20", "0"), ("20", "-1"), ("20", "1.5"),
    ("20", "nan"), ("20", "inf"), ("20", ""),
])
@pytest.mark.parametrize("published", ["PASS", "FAIL"])
def test_untrusted_or_insufficient_sample_cannot_decide(count, minimum, published):
    sheet = Sheet(**{"State": published, "N Checkpoints": count, "Min Sample": minimum})
    assert scorer.read_s1_calibration(sheet, NOW)[0] == "PENDING"


@pytest.mark.parametrize("published,model,expected", [
    ("PASS", "20", "FAIL"), ("PASS", "10", "PASS"), ("PASS", "9", "PASS"),
    ("FAIL", "20", "FAIL"), ("FAIL", "10", "FAIL"), ("FAIL", "9", "FAIL"),
])
def test_sufficient_publication_cannot_pass_above_band_or_promote_failure(published, model, expected):
    sheet = Sheet(**{"State": published, "N Checkpoints": "20", "Min Sample": "20",
                     "Mean Abs Error (pp)": model, "Band (pp)": "10"})
    state, detail = scorer.read_s1_calibration(sheet, NOW)
    assert state == expected
    assert gate(calibration=state)["verdict"] == expected
    if published == "PASS" and expected == "FAIL":
        assert "contradicted" in detail and "20.00pp > band 10.00pp" in detail


@pytest.mark.parametrize("invalid", [
    {"N Checkpoints": "0"},
    {"As Of (Riyadh)": "2099-01-01 00:00:00"},
    {"As Of (Riyadh)": "bad"},
])
def test_untrusted_evidence_remains_pending_before_band_consistency_check(invalid):
    sheet = Sheet(**{"Mean Abs Error (pp)": "20", "Band (pp)": "10", **invalid})
    assert scorer.read_s1_calibration(sheet, NOW)[0] == "PENDING"


def test_legitimate_pending_with_no_outcomes_stays_pending():
    sheet = Sheet(**{"State": "PENDING", "N Checkpoints": "0", "Mean Abs Error (pp)": "",
                     "Detail": "no qualifying checkpoints yet"})
    assert scorer.read_s1_calibration(sheet, NOW) == ("PENDING", "no qualifying checkpoints yet")


@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "-1", "bad"])
@pytest.mark.parametrize("field,parser", [("Zero MAE (pp)", scorer.parse_zero_mae),
                                         ("Mean Abs Error (pp)", scorer.parse_model_mae)])
def test_invalid_explicit_mae_is_not_rescued_by_conflicting_detail(value, field, parser):
    sheet = Sheet(**{field: value})
    assert parser(*sheet.ws.values) is None
    if field == "Mean Abs Error (pp)":
        assert scorer.read_s1_calibration(sheet, NOW)[0] == "PENDING"


def test_valid_mae_column_and_legacy_detail_remain_supported():
    assert scorer.parse_zero_mae(["Zero MAE (pp)"], ["0"]) == 0.0
    assert scorer.parse_model_mae(["Mean Abs Error (pp)"], ["0"]) == 0.0
    assert scorer.parse_zero_mae(["Detail"], ["zero_mae=3.00pp"]) == 3.0
    assert scorer.parse_model_mae(["Detail"], ["mean |err| 2.00pp"]) == 2.0


@pytest.mark.parametrize("value", [None, math.nan, math.inf, -math.inf, -1.0, True])
@pytest.mark.parametrize("field", ["zero", "model"])
def test_pure_evaluator_rejects_invalid_mae_and_exports_json_null(value, field):
    result = evaluate(**{field: value})
    assert result["criteria"][3]["status"] == "PENDING"
    assert result["verdict"] == "NOT_DECIDABLE"
    assert result["v2"][field + "_mae_pp"] is None
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("zero,model,expected", [(3, 2, "PASS"), (3, 3, "FAIL"),
                                                (3, 4, "FAIL"), (0, 0, "FAIL")])
def test_valid_paired_baseline_keeps_existing_strict_threshold(zero, model, expected):
    result = evaluate(zero=zero, model=model)
    assert result["criteria"][3]["status"] == expected
    assert result["verdict"] == expected


@pytest.mark.parametrize("mode", ["off", "observe", "enforce"])
def test_missing_comparison_preserves_rollout_modes(mode):
    base = gate()
    result = evaluate(base, mode=mode, zero=math.inf)
    assert base["criteria"][3]["status"] == "PASS"
    assert result["criteria"][3]["status"] == ("PENDING" if mode == "enforce" else "PASS")
    if mode == "observe":
        assert "would PENDING" in result["criteria"][3]["detail"]


@pytest.mark.parametrize("criterion", [3, 4])
@pytest.mark.parametrize("mode", ["off", "observe", "enforce"])
def test_missing_additional_evidence_does_not_erase_known_failure(criterion, mode):
    base = gate(alpha=-1) if criterion == 3 else gate(calibration="FAIL")
    result = evaluate(base, mode=mode, alpha=None, zero=None)
    assert result["criteria"][criterion - 1]["status"] == "FAIL"
    assert result["verdict"] == "FAIL"


@pytest.mark.parametrize("mode", ["off", "observe", "enforce"])
def test_invalid_calibration_never_erases_independent_ca_pit_failure(mode):
    result = evaluate(gate(ca=None, pit=False), mode=mode, zero=math.inf, model=math.nan)
    assert result["criteria"][4]["status"] == "FAIL"
    assert result["verdict"] == "FAIL"


def test_state_and_maes_use_one_publication_snapshot():
    sheet = Sheet()
    snapshot = scorer._s1_calibration_snapshot(sheet)
    sheet.ws.values = Sheet(**{"State": "PASS", "N Checkpoints": "0",
                               "Zero MAE (pp)": "inf"}).ws.values
    assert scorer.read_s1_calibration(sheet, NOW, _snapshot=snapshot)[0] == "PASS"
    assert scorer.read_s1_calibration_mae(sheet, _snapshot=snapshot) == (3.0, 2.0)
    assert sheet.ws.reads == 1


def test_disabled_consumer_preserves_kill_switch_without_read(monkeypatch):
    monkeypatch.setenv("TFB_S1_CAL_CONSUME", "0")
    sheet = Sheet()
    assert scorer.read_s1_calibration(sheet, NOW)[0] == "PENDING"
    assert sheet.ws.reads == 0
