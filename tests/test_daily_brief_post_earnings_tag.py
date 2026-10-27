"""16_Decision_Top10 v1.18.0 [POST-EARNINGS] puts
'⚠ post-earnings S/L · EPS x% · ' before a seat's Advisor Note
while the Script Property DT10_POST_EARN is armed. The daily brief lists only
notes that begin with INVEST (optionally after the v1.6.0 forward earnings
tag), so a seat inside its post-earnings quarantine drops out of the brief's
executable picks - interim trading policy rule 3 (program doc section 5).
These cases pin that reading, so a later brief change cannot start listing a
quarantined seat without a deliberate decision. Offline; no network."""
from scripts import run_daily_brief as brief

POST = "⚠ post-earnings 3/10 · EPS -22.2% · "
FWD = "⚠ earnings ≤7d · "
INVEST = "INVEST — 6,000 SAR (400 sh)"


def test_untagged_and_forward_tagged_invest_notes_are_picks():
    assert brief._positive_invest_note(INVEST)
    assert brief._positive_invest_note(FWD + INVEST)


def test_post_earnings_tagged_notes_are_not_picks():
    for note in (POST + INVEST, POST + FWD + INVEST,
                 "⚠ post-earnings 5/5 · EPS n/a · INVEST",
                 "⚠ post-earnings 0/5 · EPS +12.3% · " + INVEST):
        assert not brief._positive_invest_note(note), note


def test_extract_top10_leaves_the_tagged_seat_out():
    hdr = ["Rank", "Symbol", "Name", "Market", "Sector", "Ccy", "Shares", "ROI %", "Rel", "Conf",
           "Stability", "Advisor Note"]
    rows = [["SELECTED (2)"], hdr,
            [1, "FRST.US", "First Bancorp", "US", "Financials", "USD", 174, 18.0, 77, "High",
             "ACTIVE 4d", POST + INVEST],
            [2, "OPBK.US", "OP Bancorp", "US", "Financials", "USD", 400, 19.0, 76, "High",
             "ACTIVE 6d", INVEST]]
    out = brief.extract_top10(rows)
    assert [p["symbol"] for p in out["top"]] == ["OPBK.US"]
