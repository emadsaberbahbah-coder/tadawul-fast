"""S1 calibration freshness uses the existing clock-skew policy and Riyadh basis."""
from datetime import datetime, timedelta, timezone

import pytest

from core.data_validity import MAX_CLOCK_SKEW_SECONDS
from scripts import run_shadow_scorer as scorer


class Sheet:
    def __init__(self, stamp, state="PASS"):
        self.rows = [["As Of (Riyadh)", "State", "N Checkpoints", "Detail"],
                     [stamp, state, "100", "measured calibration"]]

    def worksheet(self, title):
        assert title == scorer.TAB_S1_CAL
        return self

    def get_all_values(self):
        return self.rows


@pytest.fixture(autouse=True)
def policy(monkeypatch):
    monkeypatch.setenv("TFB_S1_CAL_CONSUME", "1")
    monkeypatch.setenv("TFB_S1_CAL_MAX_AGE_H", "48")


@pytest.mark.parametrize("state", ["PASS", "FAIL", "PENDING"])
def test_recent_riyadh_calibration_state_unchanged(state):
    assert scorer.read_s1_calibration(Sheet("2026-10-14 11:30:00", state), datetime(2026, 10, 14, 12)) == (
        state, "measured calibration")


def test_future_calibration_beyond_existing_skew_cannot_pass():
    now = datetime(2026, 10, 14, 12)
    stamp = (now + timedelta(seconds=MAX_CLOCK_SKEW_SECONDS + 1)).strftime("%Y-%m-%d %H:%M:%S")
    state, reason = scorer.read_s1_calibration(Sheet(stamp), now)
    assert state == "PENDING" and "timestamp_future" in reason
    assert scorer.read_s1_calibration(Sheet("2026-10-16 12:00:00"), now)[0] == "PENDING"


def test_exact_existing_clock_skew_allowance_is_preserved():
    now = datetime(2026, 10, 14, 12)
    stamp = (now + timedelta(seconds=MAX_CLOCK_SKEW_SECONDS)).strftime("%Y-%m-%d %H:%M:%S")
    assert scorer.read_s1_calibration(Sheet(stamp), now)[0] == "PASS"


def test_aware_utc_reference_compares_to_riyadh_publication_clock():
    now = datetime(2026, 10, 14, 9, tzinfo=timezone.utc)
    assert scorer.read_s1_calibration(Sheet("2026-10-14 11:30:00"), now)[0] == "PASS"
    assert scorer.read_s1_calibration(Sheet("2026-10-14 12:16:00"), now)[0] == "PENDING"


def test_stale_and_nonfinite_freshness_policies_cannot_pass(monkeypatch):
    now = datetime(2026, 10, 14, 12)
    state, reason = scorer.read_s1_calibration(Sheet("2026-10-11 23:00:18"), now)
    assert state == "PENDING" and "stale" in reason
    monkeypatch.setenv("TFB_S1_CAL_MAX_AGE_H", "nan")
    state, reason = scorer.read_s1_calibration(Sheet("2026-10-14 11:30:00"), now)
    assert state == "PENDING" and "timestamp_policy_invalid" in reason
