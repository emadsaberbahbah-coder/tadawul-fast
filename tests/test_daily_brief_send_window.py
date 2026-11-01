"""run_daily_brief v1.20.0 [SEND-WINDOW]: truthful send-time label and session hours.

Every brief said "before market open" although the four daily sends
(10:30 / 13:30 / 18:00 / 22:00 Riyadh) all fall after the Tadawul open, and the
US / Europe hours were constants that go wrong when daylight time changes
(US: 1 Nov 2026). _send_window derives both from the IANA zones. Offline.
"""
import datetime as dt

import pytest

from scripts import run_daily_brief as brief


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv("TFB_BRIEF_NEWS_CONTEXT", "0")


@pytest.mark.parametrize("when,label", [
    (dt.datetime(2026, 10, 27, 7, 30), "before Tadawul open"),
    (dt.datetime(2026, 10, 27, 10, 30), "during Tadawul hours"),      # cron 07:30Z
    (dt.datetime(2026, 10, 27, 13, 30), "during Tadawul hours"),      # cron 10:30Z
    (dt.datetime(2026, 10, 27, 15, 30), "after Tadawul close · before US open"),
    (dt.datetime(2026, 10, 27, 18, 0), "during US hours"),            # cron 15:00Z
    (dt.datetime(2026, 10, 27, 22, 56), "during US hours"),           # the 27 Oct copy
    (dt.datetime(2026, 10, 27, 23, 30), "after US close"),
    (dt.datetime(2026, 11, 3, 17, 0), "after Tadawul close · before US open"),  # EST: opens 17:30
    (dt.datetime(2026, 11, 3, 23, 40), "during US hours"),            # EST: closes 00:00
    (dt.datetime(2026, 10, 30, 12, 0), "before US open"),             # Friday
    (dt.datetime(2026, 10, 31, 12, 0), "weekend — Tadawul and US closed"),
    (dt.datetime(2026, 10, 27, 15, 0, tzinfo=dt.timezone.utc), "during US hours"),  # aware -> Riyadh 18:00
])
def test_label(when, label):
    assert brief._send_window(when)["label"] == label


def test_hours_follow_daylight_time():
    edt = brief._send_window(dt.datetime(2026, 10, 27, 10, 30))
    est = brief._send_window(dt.datetime(2026, 11, 3, 10, 30))
    assert edt["us"] == "16:30–23:00" and est["us"] == "17:30–00:00"
    assert brief._send_window(dt.datetime(2026, 7, 1, 10, 30))["europe"] == "~10:00–18:30"   # BST
    assert est["europe"] == "~11:00–19:30"                                                    # GMT


def test_surfaces_carry_the_label_not_the_constant():
    when = dt.datetime(2026, 11, 3, 18, 0)
    model = brief.build_model({})
    html = brief.render_html(model, "Synthetic owner", when)
    text = brief.render_text(model, "Synthetic owner", when)
    assert "before market open" not in html and "16:30–23:00" not in html
    assert "Sent during US hours ·" in html and "17:30–00:00" in html
    assert "exchange holidays are not checked" in html
    assert "Sent during US hours (18:00 Riyadh)." in text and "US 17:30-00:00" in text
    assert "16:30-23:00" not in text
    assert all(ch not in text.split("ACTION SUMMARY")[-1] for ch in "–—")


def test_version():
    assert brief.__version__ == "1.20.0"
