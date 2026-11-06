"""
core/exchange_calendar_ext.py
===========================================================
TADAWUL FAST BRIDGE - EXCHANGE CALENDAR DATA EXTENSION (v1.0.0)
===========================================================

v1.0.0 (2026-11-06) [XCAL-DATA-EXT]
WHY. The pinned exchange_calendars==4.13.2 (still the newest release on PyPI
  on 2026-11-06) records the SGX (XSES) holidays only to the end of 2026, so
  its XSES calendar stops at the 2026-12-31 close:
  - scripts/track_performance.py reads a +/-14-day XSES window for every
    .SI / .SG outcome. From 2026-12-18 that window passes the bound, the
    library raises (DateOutOfBounds) and the record reads
    outcome_calendar_unknown; from 2027-01-01 no Singapore record can
    mature at all.
  - core/analysis/opportunity_builder.py asks the XSES calendar for the last
    close before a .SI quote older than the quote max age. From 17:00 SGT on
    2026-12-31 (09:00Z) previous_close raises (MinuteOutOfBounds), the venue
    state is unknown and such a quote reads "UNVERIFIED_PRICE: ... venue
    calendar unavailable" on Top_10.
EVIDENCE. The 2027 SGX closures are the 2027 Singapore public holidays that
  fall on a weekday, a Sunday holiday moving to the Monday: the rule the
  pinned XSES table follows. The holidays package 0.108 (vacanza/holidays)
  lists them; its Singapore data cites the Ministry of Manpower public
  holiday page (archived 2026-06-18) and marks 2027 a confirmed year for
  the lunar and Islamic dates. Its SGX (XSES) market calendar equals the
  pinned XSES closed weekdays in 19 of the 20 years 2007-2026 (every year
  from 2010; 2009-11-16 is the one difference).
  The same source lists the SGX half days (eves of Christmas, New Year and
  Chinese New Year: "markets close at 12:00 p.m. SGT").
WHAT. XCAL_EXTRA_HOLIDAYS = {MIC: closed weekdays appended to the pinned
  precomputed holiday table}; today only XSES, 2027. XCAL_HALF_DAYS = {MIC:
  (local close, sessions)}: the half days the extension makes reachable,
  closing at 12:00 SGT, the end of continuous trading (the convention of
  the calendar's own 17:00 close, as in track_performance's
  _OUTCOME_SESSION_OVERRIDES). extended_calendar(name) returns a calendar
  whose precomputed holidays are the pinned ones plus those days (so its
  bound moves to 2027-12-31) and whose adhoc special closes add those half
  days, only when TFB_XCAL_DATA_EXT is on / 1 / true / yes, and only while
  the pinned table still ends before the first added day (a newer package
  wins and the extension retires itself). Otherwise None: the caller keeps
  the pinned calendar. Every other session the pinned calendar has keeps
  its open and close. The two 2026 half days (24 and 31 Dec) are inside the
  pinned range, but the tracker only reaches them through windows that pass
  2026-12-31 (which raise on the pinned calendar); the builder reads their
  12:00 SGT close directly once the gate is on.
GATE. TFB_XCAL_DATA_EXT, DEFAULT OFF (read here, so the tracker lane and the
  Render lane read one name).
KNOWN LIMITS. (1) Only the half days from 2026-12-24 on are modelled; the
  earlier ones stay as the pinned calendar has them (17:00). (2) A closure
  SGX adds later in 2027 (a polling day) needs a new row. (3) XSHG and XBOM
  also end at 2026-12-31 in 4.13.2; their 2027 calendars were not official
  on 2026-11-06, so they are not extended. (4) The new bound is 2027-12-31:
  the tracker's +/-14-day window reaches it from 2027-12-18, and the
  builder's previous_close from 2027-12-31 04:00Z, so the 2028 rows are
  needed before then (or a package release that carries them).
ROLLBACK. Unset TFB_XCAL_DATA_EXT (or set off).
"""
from __future__ import annotations

import functools
import importlib
import os
from datetime import time
from typing import Any, Dict, Optional, Tuple

CALENDAR_EXT_VERSION = "1.0.0"

# MIC -> closed weekdays (ISO dates) appended to the pinned precomputed table.
XCAL_EXTRA_HOLIDAYS: Dict[str, Tuple[str, ...]] = {
    # SGX 2027: New Year's Day (Fri 1 Jan); Chinese New Year (Sat 6 and Sun 7
    # Feb -> Mon 8 Feb); Hari Raya Puasa (Wed 10 Mar); Good Friday (26 Mar);
    # Hari Raya Haji (Mon 17 May); Vesak Day (Thu 20 May); National Day (Mon 9
    # Aug); Deepavali (Thu 28 Oct). Labour Day (1 May) and Christmas Day (25
    # Dec) fall on Saturdays.
    "XSES": ("2027-01-01", "2027-02-08", "2027-03-10", "2027-03-26",
             "2027-05-17", "2027-05-20", "2027-08-09", "2027-10-28"),
}

# MIC -> (local close "HH:MM", half-day sessions): the SGX eves of Christmas,
# New Year and Chinese New Year from 2026-12-24 on (Thu 24 and Thu 31 Dec
# 2026; Fri 5 Feb, Fri 24 and Fri 31 Dec 2027), closing at 12:00 SGT.
XCAL_HALF_DAYS: Dict[str, Tuple[str, Tuple[str, ...]]] = {
    "XSES": ("12:00", ("2026-12-24", "2026-12-31", "2027-02-05", "2027-12-24", "2027-12-31")),
}

# MIC -> (module, class) of the pinned precomputed calendar it extends.
_XCAL_BASE_CLASSES: Dict[str, Tuple[str, str]] = {
    "XSES": ("exchange_calendars.exchange_calendar_xses", "XSESExchangeCalendar"),
}


def calendar_ext_enabled() -> bool:
    """v1.0.0: TFB_XCAL_DATA_EXT=1/on/true/yes appends XCAL_EXTRA_HOLIDAYS.
    DEFAULT OFF."""
    return (os.getenv("TFB_XCAL_DATA_EXT") or "off").strip().lower() in {"1", "on", "true", "yes"}


@functools.lru_cache(maxsize=None)
def _extended_calendar(name: str) -> Any:
    """The extended calendar for MIC `name` (one instance per process), or
    None when `name` has no extension or the pinned table already reaches
    the first added day."""
    extra = XCAL_EXTRA_HOLIDAYS.get(name)
    where = _XCAL_BASE_CLASSES.get(name)
    if not extra or not where:
        return None
    import pandas as pd
    base = getattr(importlib.import_module(where[0]), where[1])
    added = pd.DatetimeIndex(sorted(extra))
    if base.bound_max() >= added[0]:
        return None   # a newer package already records these years: it wins

    half = XCAL_HALF_DAYS.get(name)
    half_closes = []
    if half:
        hh, mm = (int(x) for x in half[0].split(":"))
        half_closes = [(time(hh, mm), pd.DatetimeIndex(sorted(half[1])))]

    class _Extended(base):
        @classmethod
        def precomputed_holidays(cls):
            return base.precomputed_holidays().append(added)

        @property
        def special_closes_adhoc(self):
            return list(super().special_closes_adhoc) + half_closes

    _Extended.__name__ = _Extended.__qualname__ = base.__name__ + "DataExt"
    return _Extended(end=_Extended.bound_max())


def extended_calendar(name: str) -> Optional[Any]:
    """v1.0.0 [XCAL-DATA-EXT]: the pinned exchange calendar `name` plus the
    holiday years in XCAL_EXTRA_HOLIDAYS and the half days in
    XCAL_HALF_DAYS, when TFB_XCAL_DATA_EXT is on and
    `name` has an extension; None otherwise, and the caller keeps the pinned
    calendar. Never raises (any error reads None)."""
    if not calendar_ext_enabled():
        return None
    try:
        return _extended_calendar(str(name or "").strip().upper())
    except Exception:
        return None
