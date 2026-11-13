"""opportunity_builder v1.29.1: the ticket note's annualized figure names the
period it assumes the valuation upside is reached in."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.analysis import opportunity_builder as ob  # noqa: E402

CAND = {"fx_to_sar": 3.75, "stop": 42.35, "tp1": 53.72, "tp2": 61.42, "price": 46.03,
        "roi_pct": 33.4, "ann_roi_pct": 216.7, "valuation_basis": "target_price",
        "rr": 4.18, "reliability": 76.5, "entry_low": 44.65, "entry_high": 46.49}


def _note(pm):
    try:
        return ob._advisor_sentence(dict(CAND), 5006.0, 29, "MEDIUM", "2026-12-12", pm)
    except KeyError:  # _entry_zone may need more fields; fall back to a full cand shape
        raise


def test_version():
    assert ob.OPPORTUNITY_BUILDER_VERSION == "1.29.1"


def test_note_names_period():
    n = _note(3)
    assert "valuation upside 33.4% (ann 216.7% if reached in 3m) via target_price" in n
    assert n.startswith("INVEST — ")


def test_note_without_period_is_unchanged_shape():
    n = _note(None)
    assert "valuation upside 33.4% (ann 216.7%) via target_price" in n


def test_bad_period_falls_back():
    assert "(ann 216.7%) via" in _note("x")
