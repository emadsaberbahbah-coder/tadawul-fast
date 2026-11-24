"""v5.163.0 [LABEL-TRUTH] (core.scoring 5.11.5): truth in labels and units - the row shows the horizon it was
scored on, an explicit percent is divided once, the leverage label reads D/E as a ratio, outstanding shares are not
the float, and Country is text on the Data_Dictionary page. TFB_LABEL_TRUTH = off | observe | enforce; unset ->
observe (tags only, values unchanged); off -> no tag and the legacy values.

Each fix has a positive case (the defect the audit of 23 Nov 2026 reproduced) and a negative control (a row the
fix must leave alone).
"""
import copy
import pathlib

import pytest

from tests.test_de_gate_evidence import full_row

_REPO = pathlib.Path(__file__).resolve().parents[1]
BANNED = ("cap", "forecast", "target", "roi", "drop", "reject")      # words the reliability penalty reads


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    monkeypatch.delenv("TFB_LABEL_TRUTH", raising=False)
    yield


def _mode(monkeypatch, mode):
    if mode is None:
        monkeypatch.delenv("TFB_LABEL_TRUTH", raising=False)
    else:
        monkeypatch.setenv("TFB_LABEL_TRUTH", mode)


def scored_row(monkeypatch, mode, **changes):
    """A full equity row through the engine's own scoring, the DD pass and the page backfill."""
    from core import data_engine_v2 as de
    _mode(monkeypatch, mode)
    row = full_row(**changes)
    row.pop("overall_score", None)
    de._compute_scores_canonical_first(row)
    de._apply_phase_dd_enhancements(row)
    return de._apply_page_row_backfill("Global_Markets", row)


def tags(row):
    return [p.strip() for p in str(row.get("warnings") or "").split(";") if "label_truth" in p]


CLOCK = ("scoring_updated_utc", "scoring_updated_riyadh")          # wall-clock stamps of each scoring call


def without_warnings(row):
    return {k: v for k, v in row.items() if k != "warnings" and k not in CLOCK}


# --------------------------------------------------------------------------- the switch
def test_lt1_mode_vocabulary_and_surface(monkeypatch):
    from core import data_engine_v2 as de
    for raw, mode in ((None, "observe"), ("", "observe"), ("off", "off"), ("0", "off"), ("no", "off"),
                      ("enforce", "enforce"), ("on", "enforce"), ("1", "enforce"), ("junk", "observe")):
        _mode(monkeypatch, raw)
        assert de._label_truth_mode() == mode, raw
        assert de.surface_gate_states()["label_truth"] == mode


def test_lt2_off_has_no_tag_and_the_legacy_values(monkeypatch):
    row = scored_row(monkeypatch, "off", debt_to_equity=3.5, roe="250%")
    assert (row["invest_period_label"], row["horizon_days"]) == ("3M", 365)          # the audit's mismatch, kept
    assert row.get("horizon_days_effective") is None
    assert "High leverage" not in str(row.get("top_risks"))
    assert tags(row) == []


def test_lt3_observe_changes_only_the_warnings(monkeypatch):
    off = scored_row(monkeypatch, "off", debt_to_equity=3.5, roe="250%")
    obs = scored_row(monkeypatch, None, debt_to_equity=3.5, roe="250%")
    assert without_warnings(off) == without_warnings(obs)
    assert tags(obs) == ["label_truth_percent:roe:observe", "label_truth_leverage:would_flag:observe",
                         "label_truth_horizon:365d_vs_scored_90d:observe"]


# --------------------------------------------------------------------------- horizon
def test_lt4_enforce_stamps_the_scored_horizon_and_rescoring_keeps_the_weights(monkeypatch):
    from core import scoring
    row = scored_row(monkeypatch, "enforce")
    assert row["horizon_label"] == "month"
    assert (row["invest_period_label"], row["horizon_days"], row["horizon_days_effective"]) == ("3M", 90, 90)
    assert "label_truth_horizon:3M:enforce" in tags(row)
    # the stamped row scores with the same weights again (90 days -> Horizon.MONTH)
    assert scoring.detect_horizon(None, row)[0] == scoring.Horizon.MONTH
    # and the presentation layer shows 3M, not 1Y
    from core import sheet_presentation as sp
    assert sp._horizon_label(row["horizon_days"]) == "3M"
    # idempotent: a second backfill pass changes nothing
    from core import data_engine_v2 as de
    again = de._apply_page_row_backfill("Global_Markets", copy.deepcopy(row))
    assert again == row


@pytest.mark.parametrize("mode", [None, "enforce"])
def test_lt5_horizon_negative_controls(monkeypatch, mode):
    from core import data_engine_v2 as de
    _mode(monkeypatch, mode)
    # a row scored on 365 days already shows 365: nothing to do
    row = {"symbol": "L.US", "horizon_label": "long", "horizon_days_effective": 365, "horizon_days": 365,
           "invest_period_label": "1Y"}
    out = de._apply_page_row_backfill("Global_Markets", copy.deepcopy(row))
    assert (out["invest_period_label"], out["horizon_days"]) == ("1Y", 365) and tags(out) == []
    # a row that was never scored keeps the legacy fill and gets no tag
    out = de._apply_page_row_backfill("Global_Markets", {"symbol": "U.US"})
    assert (out["invest_period_label"], out["horizon_days"]) == ("1Y", 365) and tags(out) == []


@pytest.mark.parametrize("label,days", [("day", 1), ("week", 6), ("month", 90), ("long", 365)])
def test_lt6_scored_labels_round_trip(label, days):
    from core import data_engine_v2 as de
    from core import scoring
    got = de._label_truth_scored_horizon({"horizon_label": label})
    assert got[1] == days and scoring.detect_horizon(None, {"horizon_days": days})[0].value == label
    assert got[0] == scoring.invest_period_label(scoring.Horizon(label), None)    # the scorer's own label


# --------------------------------------------------------------------------- percent
def test_lt7_explicit_percent_divided_once_under_enforce(monkeypatch):
    from core import scoring
    for mode, big in ((None, 0.02), ("observe", 0.02), ("off", 0.02), ("enforce", 2.0)):
        _mode(monkeypatch, mode)
        assert scoring._as_fraction("200%") == pytest.approx(big), mode
        assert scoring._as_fraction("2%") == pytest.approx(0.02)                   # control: a small percent
        assert scoring._as_fraction(25) == pytest.approx(0.25)                     # control: points without '%'
        assert scoring._as_fraction(0.25) == pytest.approx(0.25)                   # control: a fraction
        assert scoring._as_fraction(None) is None


def test_lt8_percent_tag_only_for_big_explicit_strings(monkeypatch):
    from core import data_engine_v2 as de
    _mode(monkeypatch, None)
    row = {"roe": "250%", "roa": "50%", "revenue_growth_yoy": 2.5, "volatility_30d": "-180%", "max_drawdown_1y": "x%"}
    de._label_truth_percent_tags(row)
    assert tags(row) == ["label_truth_percent:roe:observe", "label_truth_percent:volatility_30d:observe"]
    _mode(monkeypatch, "off")
    row = {"roe": "250%"}
    de._label_truth_percent_tags(row)
    assert tags(row) == []


# --------------------------------------------------------------------------- leverage
@pytest.mark.parametrize("de_value,legacy,fixed", [
    (0.47, False, False),        # the median: never flagged
    (1.99, False, False),
    (2.0, False, True),          # ratio units: flagged under enforce
    (35.0, False, True),         # a bank-like ratio
    (150.0, True, False),        # percent points 150 % = 1.5: legacy flags it wrongly
    (250.0, True, True),         # percent points 250 % = 2.5
    (None, False, False),
])
def test_lt9_leverage_reads_the_ratio(monkeypatch, de_value, legacy, fixed):
    from core import data_engine_v2 as de
    assert de._label_truth_high_leverage(de_value, "off") == (legacy, "")
    assert de._label_truth_high_leverage(de_value, "enforce") == (fixed, "")
    flag, tag = de._label_truth_high_leverage(de_value, "observe")
    assert flag == legacy
    assert tag == ("" if legacy == fixed else "label_truth_leverage:%s:observe" % ("would_flag" if fixed else "would_clear"))


def test_lt10_leverage_on_the_row(monkeypatch):
    assert "High leverage" in str(scored_row(monkeypatch, "enforce", debt_to_equity=3.5).get("top_risks"))
    assert "High leverage" not in str(scored_row(monkeypatch, "enforce", debt_to_equity=0.4).get("top_risks"))
    assert "High leverage" in str(scored_row(monkeypatch, "off", debt_to_equity=180.0).get("top_risks"))
    assert "High leverage" not in str(scored_row(monkeypatch, "enforce", debt_to_equity=180.0).get("top_risks"))


# --------------------------------------------------------------------------- float
def test_lt11_outstanding_shares_are_not_the_float(monkeypatch):
    from core import data_engine_v2 as de
    only_out = {"symbol": "X.US", "currentPrice": 10, "sharesOutstanding": 1000}
    both = {"symbol": "X.US", "currentPrice": 10, "floatShares": 800, "sharesOutstanding": 1000}
    for mode, value, tag in (("off", 1000, []), (None, 1000, ["label_truth_float:outstanding:observe"]),
                             ("enforce", None, ["label_truth_float:outstanding:enforce"])):
        _mode(monkeypatch, mode)
        a = de._canonicalize_provider_row(dict(only_out), "X.US", "X.US", "yahoo")
        assert a.get("float_shares") == value and tags(a) == tag, mode
        b = de._canonicalize_provider_row(dict(both), "X.US", "X.US", "yahoo")
        assert b.get("float_shares") == 800 and tags(b) == [], mode                 # control: a real float
    assert de._LT_FLOAT_OUTSTANDING <= set(de._CANONICAL_FIELD_ALIASES["float_shares"])


# --------------------------------------------------------------------------- Data_Dictionary
def test_lt12_country_is_text_under_enforce(monkeypatch):
    from core import data_engine_v2 as de
    for mode, dtype in (("off", "number"), (None, "number"), ("enforce", "string")):
        _mode(monkeypatch, mode)
        assert de._dd_infer_dtype_fmt("country", "Country")[0] == dtype, mode
        assert de._dd_infer_dtype_fmt("news_count", "News Count")[0] == "number"   # control: a real count
        assert de._dd_infer_dtype_fmt("current_price", "Price")[0] == "number"


# --------------------------------------------------------------------------- tags and manifest
def test_lt13_tags_avoid_the_reliability_words(monkeypatch):
    rows = [scored_row(monkeypatch, m, debt_to_equity=3.5, roe="250%", max_drawdown_1y="-200%") for m in (None, "enforce")]
    found = [t for r in rows for t in tags(r)]
    assert found and all(not any(b in t.lower() for b in BANNED) for t in found), found


def test_lt14_version_manifest_why(monkeypatch):
    from core import data_engine_v2 as de
    from core import scoring
    assert de.__version__ == "5.163.0" and scoring.__version__ == "5.11.5"
    vd = (_REPO / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("core.data_engine_v2", "__version__", "5.163.0",' in vd
    assert "(\"core.scoring\", \"__version__\", '5.11.5'," in vd
    src = (_REPO / "core" / "data_engine_v2.py").read_text(encoding="utf-8")
    assert src.count("WHY v5.163.0 - TRUTH IN LABELS AND UNITS") == 1
    assert "Data Engine V2 - GLOBAL-FIRST ORCHESTRATOR - v5.163.0" in src
    assert "WHY v5.11.5" in (_REPO / "core" / "scoring.py").read_text(encoding="utf-8")
