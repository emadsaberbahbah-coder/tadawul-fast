"""scripts/tfb_acceptance.py v1.0.11: the cockpit's KPI 'Exp. Gain TP1 (SAR)'
(16_Decision_Top10 v1.20.0, Script Property DT10_KPI_PLAN_GAIN=on) is read
into the D10-1c evidence and named as the TP1 plan payoff. The v1.0.10
reading of 'Exp. Gain 12M (SAR)' and every verdict and measured value are
unchanged. Offline; the module's own in-memory source."""
from __future__ import annotations

from scripts import tfb_acceptance as acc

STATUS = ("Last run 2026-11-02 08:49:43 | status: ok | output: EXECUTABLE | route v4.18.2 | "
          "builder vtfb-1.30.0 | funding=final/6exec | stab[strict]: 3 grace | earn 5/10 | 12.0s")
HDR_12M = ["Entered cash (SAR)", "Exp. Gain 12M (SAR)", "Selected", "Blended Reliability", "Blended R/R (TP2)",
           "Scanned", "Passed", "Unallocated (SAR)", "Fundable Now", "By Rotation", "Capital Call (SAR)"]
VALS = ["72,747 SAR", "56,203 SAR", "6 exec + 1 research + 3 grace / 10", "76.0", "3.20", "9,788", "22",
        "3,750 SAR", "6", "0", "0"]
BANNER = ("FEED ACTIONABLE - EXECUTABLE (verdict age 367m) - SELECTED - 6 EXECUTABLE TICKETS + "
          "1 RESEARCH (NO ALLOCATION) + 3 GRACE-HELD (NO PLAN TODAY)")


def _board(label, value):
    hdr = list(HDR_12M)
    vals = list(VALS)
    hdr[1] = label
    vals[1] = value
    rows = [["TOP 10 INVESTMENTS - DECISION"], ["Status:", STATUS], [], ["KPIs"], hdr, vals, [], [BANNER], []]
    return {c.cid: c for c in acc.check_board(acc._MemSource({"Top_10_Investments": rows}), 5)}


def test_version_bumped():
    # merge note (2026-11-02, #808): the relocated audit grid is v1.0.12 over this v1.0.11; a floor.
    assert tuple(int(x) for x in acc.VERSION.split(".")) >= (1, 0, 11)


def test_12m_label_reads_as_before():
    c = _board("Exp. Gain 12M (SAR)", "56,203 SAR")["D10-1c"]
    assert "gain_kpi=56,203 SAR" in c.evidence and "TP1" not in c.evidence, c.row()


def test_tp1_label_is_read_and_named():
    c = _board("Exp. Gain TP1 (SAR)", "11,078 SAR")["D10-1c"]
    assert "gain_kpi=11,078 SAR (TP1 plan payoff)" in c.evidence, c.row()


def test_tp1_dash_and_unknown_label_stay_dashes():
    assert "gain_kpi=" + "\u2014" + " (TP1 plan payoff)" in _board("Exp. Gain TP1 (SAR)", "\u2014")["D10-1c"].evidence
    assert "gain_kpi=-" in _board("Exp. Gain (other)", "11,078 SAR")["D10-1c"].evidence


def test_verdicts_and_measures_unchanged_by_the_label():
    a = _board("Exp. Gain 12M (SAR)", "56,203 SAR")
    b = _board("Exp. Gain TP1 (SAR)", "11,078 SAR")
    assert sorted(a) == sorted(b)
    for cid in a:
        assert (a[cid].verdict, a[cid].measured) == (b[cid].verdict, b[cid].measured), cid
    assert a["D10-1c"].verdict == "PASS" and a["D10-1c"].measured == 0
