#!/usr/bin/env python3
"""
scripts/exit_plan.py - TFB Exit Plan
====================================
VERSION 1.8.0 (2026-11-27) - first written 2026-11-11 (1.0.0)
1.8.0 (27 Nov 2026): THE STOP WITHOUT THE NEWS, RECORDED BESIDE THE STOP [NEWS-SHADOW] (outside engineering audit the
    owner uploaded on 26 Nov 2026, finding C1; owner, 26 Nov 2026 19:13 Riyadh, in Arabic: "if it is ready, build
    it now"). A negative name item of severity H or M and information grade 1-3 tightens the trail to
    TFB_EXIT_TRAIL_TIGHT_ATR x ATR. Grade 3 is a story with few or unconfirmed sources, and the rule has never been
    measured: it may sell holdings on news that did not matter. The rule is NOT changed. Each ledger row now also
    records "Stop Without News": the same stop, from the same entry, ATR, highest close, floors and frozen offers,
    with the negative-news reason taken out (any other reason still tightens it). It follows its own history (the
    highest one recorded for the lot, never down) and is read nowhere: the page, the mail, the actions and the stop
    of record are unchanged. Once enough lots have had news, the two can be compared on what the price did next,
    and the owner decides whether grade 3 keeps tightening. A ledger written before 1.8.0 (19 columns) is read as
    before, its rows' own Stop standing in for the shadow. The run log gives the counts only.
1.7.1 (26 Nov 2026): THE NEWS ROWS ARE READ FROM THE BOTTOM OF THE TAB (found while adding news sources; owner,
    26 Nov 12:42 Riyadh, in Arabic: review what else needs changing so the news reaches the decision). The run read
    _News_Items as "A1:M12000" - the FIRST 12,000 rows - while the news gate appends at the bottom. The tab passed
    12,000 rows on 20 Nov 2026 at 22:42 Riyadh (19:42 UTC) and held 19,526 on 26 Nov, so no headline recorded after
    that hour reached this page: 7,527 rows, 65 of them about a name, among them a negative item of severity M on a
    holding (an 8-K of 24 Nov, tier A) that the rule "negative news tightens the trail" never saw. Nothing failed
    and nothing said so: the page went on showing the news it could see.
    read_newest() reads the columns the rule needs (NEWS_COLS) from the NEWEST NEWS_ROWS (20,000) rows: the header
    row, the length of column A, then one batch of column tails - about the same number of cells as before. The
    run log states which rows were read (numbers only). The Holding Read (1.0.3) reads its items the same way. No
    rule, page layout, ledger column or ENV changes; a holding with a negative item of the last TFB_EXIT_NEWS_DAYS
    days now shows it again, and its trail tightens as the rule always said.
1.7.0 (26 Nov 2026): A HOLDING UNDER A CASH OFFER, AND WHY A PRICE FELL (owner, 26 Nov 01:07 Riyadh, in Arabic, the
    day after a stop filled in the first seconds of a session that closed higher: "is this fall temporary or lasting
    ... is it better to adjust the stop points or keep them ... that is why the stop on the tender holding was
    removed: the fear that the price falls in the wave and a sell order is executed while a purchase offer stands
    at a fixed price, so we lose for no reason. The system must have reason, wisdom and flexibility"; 01:15: "the
    sale there will mostly be a manual decision, waiting for the best price before the final offer"; 01:37: "please
    do the required build now"). Read on the 00:42 run: the tender holding's stop had trailed to 1.8 % under the
    price (its ATR shrinks as the price pins to the offer), its target stood above the offer price, the market-turn
    note asked to sell it with the market, and the Today Action Plan asked to place the stop the owner had removed.
    (a) [OFFER] config/corporate_offers.json (core/corporate_offers.py 1.0.0) lists the cash offers standing on
        listed shares. A holding with a standing offer whose sale is "manual" is read on the offer's own terms: the
        stop of record is FROZEN at its last value (it stays in the ledger and applies again when the offer
        lapses), the target is the offer price, no RAISE STOP, TAKE PROFIT or price-rule SELL is issued and the
        market-turn rule does not apply. Its Why opens with the offer: price, last day, days left, how far the last
        close is under it and what that prices about completion against the close before the offer (a rough
        reading). The action is HOLD, or REVIEW when (i) the price implies under TFB_EXIT_OFFER_DOUBT_PCT (50) that
        it completes - or, with no close before the offer, stands more than TFB_EXIT_OFFER_GAP_PCT (8) under it;
        (ii) the last close is within TFB_EXIT_OFFER_NEAR_PCT (0.5) of the offer or above it (the market pays it
        now); (iii) TFB_EXIT_OFFER_WARN_DAYS (5) or fewer days remain. An offer whose sale is "rule" keeps the
        price rule; its target is capped at the offer and its Why names it. An offer whose last day has passed
        reads "record its outcome" and the price rule is back in force.
        TFB_EXIT_OFFERS = on (default) | off (= 1.6.3 for every holding).
    (b) [STOP-JUDGMENT] (observe only: a note and a ledger column, never an action). For each holding with a price
        rule: its move over TFB_EXIT_SJ_WINDOW (15) sessions beside its market's own reading (core.market_condition:
        SPY, TASI ...). A fall of TFB_EXIT_SJ_DROP_PCT (8) or more is named: WITH ITS MARKET (the market fell
        TFB_EXIT_SJ_MARKET_PCT (5) or more), ON ITS OWN (the market fell TFB_EXIT_SJ_ALONE_PCT (1) or less), or
        mixed. The rule under test - the ladder shown to the owner on 26 Nov - would WAIT instead of selling at the
        stop on a fall with the market, while the stock stays within TFB_EXIT_SJ_MARGIN_PCT (5) points of its market
        and the loss from the entry stays under TFB_EXIT_SJ_HARD_PCT (15); in every other case the stop stands. The
        note shows when there is a fall to judge, the stop is within TFB_EXIT_SJ_NEAR_PCT (3) of the close, or the
        action is SELL. Measured on the archive that night (US shares with bars, Oct 2024 - Nov 2026, one event per
        share in 20 sessions; the SQL of sql/bigquery v_stop_cause_events): fell with the market inside the margin,
        379 cases on 22 days - 73.4 % higher 20 sessions later, mean +6.8 %, but level with the market itself (+0.2
        points) and t 1.0 by day (the gain was the market's rebound, not the share's); fell on their own, 1,767
        cases on 359 days - 51.3 %, +0.7 %; fell 15 % on their own, 112 cases - 39.3 %, -9.7 %. Twenty-two days is
        one kind of market, not proof: the judgment is logged and scored (v_stop_cause_learning) before it may
        control anything (owner's ground rules, 21 Nov).
        TFB_EXIT_STOP_JUDGMENT = observe (default) | off.
    The ledger gains a 19th column, Judgment ("with_market:wait:-9.2:-6.1", "offer:manual"); 1.6.x and 1.5.0
    ledgers are read as before. Nothing here places, changes or cancels an order.
1.6.3 (23 Nov 2026): OPENING THE WORKBOOK WAITS OUT THE QUOTA TOO. The Daily Plan run
    45123566985 (23 Nov 06:3x UTC) ended "cannot open workbook: APIError" at its first call: opening the book reads its
    metadata, which counts against the same per-minute Sheets quota, and it was the one read never retried. It now goes
    through scripts/sheets_quota.call (a quota answer waits 65 s, twice at most), and the error line names the HTTP status
    when there is one (no value of the workbook).
1.6.2 (2026-11-23): A QUOTA REFUSAL OF THE WRITE IS WAITED OUT (owner, 23 Nov
    08:42 Riyadh: "check specially the workflows ... if there is any failing
    ... update and try to improve"). The page jobs share one Google service
    account and its per-minute quota; the reads already wait it out (1.3.1),
    the two page writes did not. Both overwrite their whole range, so a quota
    answer is now waited out and the writes tried again
    (scripts/sheets_quota.call, 65 / 65 s); any other write error still stops
    at "publication unconfirmed". Nothing else changes.
1.6.1 (2026-11-22): THE RUN LOG NAMES NO HOLDING (the owner's standing rule:
    "Repo logs are public: no amounts, quantities, account numbers or
    holdings' symbols in run logs"). Since 1.0.0 every run printed one line
    per holding - its symbol, last close, stop, target and reasons - and the
    plan picks with their levels, into the GitHub Actions log of a public
    repository; the e-mail subject printed there named the holdings with an
    action. Seen on the first 1.6.0 run (22 Nov 06:10 UTC). Now, on GitHub
    Actions, each holding prints as "holding 3 of 8: HOLD (market not calm)",
    each pick as "plan pick 1 of 2: levels computed", and the subject as its
    count of actions; the page, the ledger and the e-mail are unchanged.
    Gate: TFB_EXIT_LOG_DETAIL = auto (default: no detail on GitHub Actions,
    the full lines elsewhere) | on | off.
1.6.0 (2026-11-21): A HOLDING WHOSE MARKET IS NOT CALM IS FLAGGED FOR SALE
    (owner, 21 Nov 22:45 Riyadh: "in the crash phase I think the system had
    better say sell before the loss, and also wait for the better time to
    buy"; 22:51: "consider my approval your choice of the best decision").
    Measured that evening on the matured INVESTABLE Performance_Log records,
    selling at the next close after the first close on which the stock's
    market was not calm (weak, stress, drawdown or crash_day; the labels of
    sql 99_v_market_condition), against holding to the target:
      entered in a calm market (1,166): won 52.2% -> 54.1%, mean +0.49% ->
        +0.61%, the worst 5% -13.6% -> -7.8%;
      entered in a market not calm (1,367): 45.0% -> 47.6%, -1.24% ->
        -0.27%, the worst 5% -16.1% -> -5.2%.
    Selling only on drawdown / crash_day changed little for calm entries.
    On the indices themselves, staying out whenever the market is not calm
    halved the US drawdown over three years (-19% -> -8.7%) but cut its gain
    from +53% to +31%: the rule protects a holding; it is not a reason to
    sit in cash.
    Now each holding of the US, Saudi, European, Japanese or Hong Kong /
    China market (core.market_condition 1.0.0, read live from Yahoo on the
    last completed session) carries its market's label. When the label is in
    TFB_EXIT_MARKET_TURN_LABELS (default weak,stress,drawdown,crash_day):
      observe (default): a note "market not calm (observe): ..." - the
        action, the stop and the target do not change;
      on: the action is SELL ("the market turned: ...").
    Commodities, currencies, indices, bonds, sukuk, crypto and futures carry
    no market label; a lot with no close since its purchase waits for one.
    The ledger gains the column Market (market:label:date of the close
    read), so the rule can be scored on the real book; a ledger written by
    1.5.0 (17 columns) is read as before.
    Gate: TFB_EXIT_MARKET_TURN = observe (default) | on | off (no read, the
    1.5.0 page).
1.5.0 (2026-11-18): THE STOP OF RECORD IS NEVER BELOW THE STOP WORKING AT THE
    BROKER (owner, 18 Nov 10:32 Riyadh: "do the best from your side", on the
    review of that morning). Read on the 09:22 Today Action Plan against the
    broker's orders: on five holdings the plan's stop was under the stop
    working at IBKR - 82.78 against 83.88, 14.99 against 15.22, 47.37 against
    48.69, 79.33 against 80.98, 71.23 against 75.50. The two newest were bought
    two days before with the Daily Plan's stop on the order; this page then
    started its own, lower, initial stop. Nothing was at risk (the broker's
    stop is the tighter one), but the page stated a stop that was not the one
    in the market, its "stop n% under the last close" overstated the room, and
    following its figure would have LOWERED a stop - against the rule above
    ("the stop never moves down").
    Now, when the broker's stop for a holding is known, above the rule's stop
    and under the last close, it IS the stop of record: it is stored in the
    ledger, so it holds from then on whatever happens to the order. Such a row
    never reads RAISE STOP on that account (the broker already carries it);
    its note says "the stop of record is the stop working at IBKR (x), above
    the rule's y". A broker stop under the rule's is the 1.0.0 note, unchanged.
    The floor is taken from the IBKR capture only (its lowest working stop for
    the symbol); the workbook's older broker tabs never set it. A raise the
    broker already carries is not a RAISE STOP on a re-run of the same close
    either (review of #1021).
    Gate: TFB_EXIT_BROKER_FLOOR = on (default) | off (the 1.4.0 levels).
1.4.0 (2026-11-17): DIVIDEND NET OF THE TAX WITHHELD (owner, 17 Nov 10:33
    Riyadh: "go with the best option"). The entry was lowered by the whole
    dividend, as Yahoo's adjusted close does. A Saudi resident does not receive
    the whole dividend of a US company: tax is withheld. On a large special
    dividend the difference is material - the page counted the gain and the
    break-even from a price the owner's cash never reached, and a stop placed
    from that entry allowed a larger loss of capital than the rule states.
    Now, when the dividend log (_Dividend_Log, written by the portfolio ledger)
    carries a tax row for a holding's dividend - a negative Amount for the same
    symbol and ex-date - the bars before that ex-date are rescaled so the step
    is the dividend the owner kept (net_of_tax). The entry, the stop of record
    (rebased through its Basis, as after any dividend), the target and the
    R figures follow. A dividend with no tax row stays gross: nothing is
    guessed from the listing (a foreign company listed in New York may carry
    no US tax at all). Only a kept share from 50% up to 99.5% is applied.
    Gate: TFB_EXIT_DIV_NET = on (default) | off (the 1.3.2 behaviour; the stop
    of record returns to its gross basis because it is stored normalised).
1.3.2 (2026-11-14): ONLY TODAY'S PLAN PICKS. On 14 Nov the Daily Plan was cut
    with pipeline rows and no pick, and this page still listed INSW 18, EFC 180
    and AX 21 - the picks of 13 Nov - as today's plan picks, because the newest
    date was taken among PICK rows only. The plan's date is now the newest date
    of any row (as the Today Action Plan reads it), and its picks are listed
    only when that date is today in Riyadh: before the morning cut, or on a day
    cut with no pick, the section is empty instead of showing older picks.
    ONE BROKER SOURCE. The broker stops came from the _IBKR_Account /
    _IBKR_Orders tabs, last written 17 Oct, so this page said "broker orders
    unknown" while the Today Action Plan read the hourly IBKR capture. The
    stops now come from the same capture (scripts/ibkr_capture.py) when it is
    at most TFB_EXIT_IBKR_MAX_AGE_H hours old (30); the tabs stay the
    fallback. Each stop is the lowest working stop price on the symbol.

1.3.1 (2026-11-13): THE SHEETS QUOTA IS WAITED OUT. Two runs of 13 Nov (16:21
and 17:41 Riyadh) ended red on Sheets HTTP 429: the read was retried twice,
20 s apart, inside the same quota minute. After a quota answer the wait is now
65 s (scripts/sheets_quota.py), so each retry falls in a new quota minute;
every other read error keeps the 20 s wait. Nothing else changes.

WHY (owner, Wed 11 Nov 2026 09:30 Riyadh): fixed stop-loss and take-profit
levels do not fit every stock. Each holding needs its own levels from its own
volatility, the engine's forecast and confidence, and the news; and every day
after the close the stop and the target should be reviewed and raised, so a
stock that has climbed well above its entry does not fall back to the stop set
at the purchase. The recommendation is shown in the workbook and sent by
e-mail every morning.

Evidence for the numbers (project doc TFB_Stop_Loss_and_Take_Profit_Review_
2026-11-11; INVESTABLE records outside Tadawul, next-open entry, daily bars):
  - stop -5% or 2xATR: 43-46% of entries stopped, win rate about 45%;
    stop -8%: 22% stopped, -10%: 15%, -12%: 12% (20 sessions, n=190);
  - a trail of 3xATR after +1R had the best 45-session result (+2.50%, win
    59.3%, n=108); a 2xATR trail ended 66-94% of entries;
  - the engine's 12-month target (about +30%) was never reached in 20
    sessions; +8% was reached by 38% of entries;
  - the real book's broker stops sat 4.7-8.4% under cost; AER, SBAC and DDI
    rose 14-25% after their stops filled.

THE RULE (per holding, recomputed after every close):
  - R (the initial risk) = TFB_EXIT_ATR_INIT x ATR14 at the purchase, kept
    between TFB_EXIT_MIN_PCT and TFB_EXIT_MAX_PCT % of the entry (Tadawul
    names at most TFB_EXIT_SR_MAX_PCT %). Initial stop = entry - R.
  - Once the highest close since the purchase is 1 R above the entry, the
    stop is at least the entry plus TFB_EXIT_BE_PCT % and trails
    TFB_EXIT_TRAIL_ATR x ATR14 (today's) under that highest close.
  - The trail tightens to TFB_EXIT_TRAIL_TIGHT_ATR x ATR14 when any of these
    holds: negative news on the symbol in the last TFB_EXIT_NEWS_DAYS days
    (direction -1, severity H or M); the engine's 1-month forecast is below
    the last close with forecast reliability TFB_EXIT_FC_REL_MIN or more; the
    gain at the high reached 2 R and the last close is under the 20-day
    average. The tightening rules are judgment, not yet measured; they only
    ever raise a stop.
  - The stop never moves down: it is at least yesterday's recommended stop
    for the same lot.
  - Target: the next whole R step above the highest close (the first is
    entry + 1 R, where half the position is sold); each step crossed raises
    the target one step.
  - Actions: SELL (the last close is at or below the stop), TAKE PROFIT (the
    first target was reached for the first time: sell half), RAISE STOP (the
    stop moved up), REVIEW (TFB_EXIT_TIME_SESSIONS sessions without +1 R),
    HOLD. Earnings inside TFB_EXIT_EARN_WARN_DAYS days add a warning: an
    earnings gap can jump any stop.
  - Fixed income (sukuk, bonds, funds) has no stop rule.
  - Prices are dividend-adjusted (Yahoo's adjusted close), and so is the
    entry (the buy price times the adjustment factor on the buy date), so a
    dividend - 10 Nov: SUPN's 8.13 special dividend - neither reads as a loss
    nor leaves a pre-dividend high that would put the stop above the price.
    1.4.0: a dividend whose tax is recorded in _Dividend_Log is counted net of
    that tax (TFB_EXIT_DIV_NET), so the tax does read as the loss it is.
TODAY'S PLAN PICKS: the same initial stop and first target for the Daily
Plan's open picks, beside the plan's fixed stop.

Reads only: _Portfolio_CostBasis, My_Portfolio, Global_Markets,
Market_Leaders, Calendar_Events, Dividend_Events, _Dividend_Log, _News_Items,
_IBKR_Account, _IBKR_Orders, _Daily_Plan and its own ledger. Daily bars from the Yahoo chart
API. Writes only its own two tabs: _Exit_Plan (the ledger) and Exit_Plan (the
page). Sends one e-mail when TFB_EXIT_MAIL=1. Never calls a broker, places,
changes or cancels no order.

1.0.1 (2026-11-11): the HOLD note 'stop X% under the last close' measured the gap
against the stop (first live dry run: SUPN read 18.5% for a stop 15.7% under
28.75); it is now measured against the last close.

1.2.0 (2026-11-11): DIVIDEND TAX (owner, 17:01 Riyadh). A holding whose
ex-dividend falls within TFB_EXIT_DIV_WARN_DAYS days (10) gets a note: the
dividend a share and as a percent of the price, that the price falls by the
full amount, and - on a US listing - the withholding a Saudi resident pays
(TFB_EXIT_WHT_PCT, 30) on the lot: selling before the ex-date avoids it when a
sale is planned anyway. A note only: no stop, target or action changes.

1.1.0 (2026-11-11): HOURLY (owner, 12:52 Riyadh: "the outcome pages should be
updated and refreshed to capture the new data at least each hour and that
should be automatic"). The workflow now runs every hour; the e-mail still goes
once, after the 07:30 run. Three changes make an hourly run safe:
  - completed bars only: while a market is open, Yahoo's last daily bar is the
    session so far; it is set aside (its price shows as Live) and the stop, the
    target and the action are computed on completed closes, so an intraday
    spike cannot ratchet a stop that never moves down;
  - a live price at or under the stop is noted (a broker stop fills intraday;
    the rule acts on the close);
  - an action belongs to the completed close it was computed on (ledger
    'Bars To'), not to the hour of the run: every run on the same close shows
    the same RAISE STOP (with the stop it was before that close) and the same
    TAKE PROFIT, so the 07:30 e-mail carries what the 23:35 run issued on the
    US close; TAKE PROFIT is not repeated on a later close;
  - a lot bought in a session that has not closed yet has no close to judge:
    it reads HOLD with its initial stop and first target until the first
    close after the purchase (Codex P2 on #959).

GATE: TFB_EXIT_PLAN = off (default) | observe (compute and print, write and
send nothing) | on. Rollback: repository variable TFB_EXIT_PLAN=off.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import html
import json
import math
import os
import re
import sys
import time
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.daily_plan import (  # noqa: E402
    _col, _read, cell, dividend_tax, fmt, num, parse_date, read_calendar, riyadh_str, table_index, text, write_grid,
)

__version__ = "1.8.0"

UTC = _dt.timezone.utc
RIYADH = _dt.timezone(_dt.timedelta(hours=3))
LEDGER_TAB = "_Exit_Plan"
PAGE_TAB = "Exit_Plan"
PAGE_WIDTH = 16
MAX_LEDGER_ROWS = 3000
LEDGER_HEADERS = [
    "Date", "Run At (Riyadh)", "Symbol", "Lot", "Entry", "Shares", "Last", "Highest Close", "ATR", "R",
    "Stop", "Target", "TP1 Reached", "Action", "Tighten", "Bars To", "Basis", "Market", "Judgment",
    "Stop Without News",                              # 1.8.0 [NEWS-SHADOW]: recorded, read nowhere
]
LEDGER_HEADERS_V17 = LEDGER_HEADERS[:19]             # 1.8.0: a 1.7.x ledger has no Stop Without News column
LEDGER_HEADERS_V16 = LEDGER_HEADERS[:18]             # 1.7.0: a 1.6.x ledger has no Judgment column
LEDGER_HEADERS_V15 = LEDGER_HEADERS[:17]             # 1.6.0: a 1.5.0 ledger has no Market column
MARKET_TURN_LABELS_DEFAULT = "weak,stress,drawdown,crash_day"
MARKET_TURN_HOLDABLE = ("crash_day", "drawdown", "stress", "weak")      # calm never turns
# instruments no stock-market label describes (data_engine_v2 5.161.0 _MARKET_HOLD_EXEMPT_TOKENS)
MARKET_TURN_EXEMPT = ("commodity", "fx", "currency", "forex", "index", "fixed income", "sukuk", "bond", "crypto",
                      "future")
INFO_COLS = ("Symbol", "Name", "Asset Class", "Current Price", "Forecast Price (1M)", "Forecast Reliability Score",
             "Confidence Score", "Target Price")
YAHOO_HOSTS = ("query1.finance.yahoo.com", "query2.finance.yahoo.com")
YAHOO_PATH = "/v8/finance/chart/{sym}?range=1y&interval=1d"
YAHOO_UA = "Mozilla/5.0 (tfb-exit-plan)"
READ_ATTEMPTS = 3
READ_RETRY_S = 20
_SLEEP = time.sleep
NO_STOP_CLASSES = ("fixed income", "sukuk", "bond", "fund", "money market")


def _status_of(e: BaseException) -> str:
    """1.6.3: the HTTP status of a Sheets error for the run log, e.g. " [429]" (never a value of the workbook)."""
    code = getattr(getattr(e, "response", None), "status_code", None) or getattr(getattr(e, "resp", None), "status", None)
    if code is None:
        m = re.search(r"\[(\d{3})\]", str(e))
        code = m.group(1) if m else None
    return " [%s]" % code if code else ""


def _out(msg: str) -> None:
    print("[exit_plan %s] %s" % (__version__, msg), flush=True)


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def _env_num(name: str, default: float, lo: float, hi: float) -> float:
    try:
        v = float(_env(name, str(default)))
    except ValueError:
        return default
    return min(hi, max(lo, v)) if math.isfinite(v) else default


def mode() -> str:
    v = _env("TFB_EXIT_PLAN", "off").lower()
    return v if v in ("off", "observe", "on") else "off"


def log_detail() -> bool:
    """1.6.1: may the run log name holdings and their levels? TFB_EXIT_LOG_DETAIL = on | off | auto (default:
    not on GitHub Actions, whose logs are public for this repository)."""
    v = _env("TFB_EXIT_LOG_DETAIL", "auto").lower()
    if v in ("on", "1", "true", "yes"):
        return True
    if v in ("off", "0", "false", "no"):
        return False
    return (os.environ.get("GITHUB_ACTIONS") or "").strip().lower() != "true"


def market_turn_mode() -> str:
    """1.6.0: TFB_EXIT_MARKET_TURN = observe (default; also an unknown word) | on | off."""
    v = _env("TFB_EXIT_MARKET_TURN", "observe").lower()
    if v in ("on", "enforce", "1", "true", "yes"):
        return "on"
    if v in ("off", "0", "false", "no"):
        return "off"
    return "observe"


def stop_judgment_mode() -> str:
    """1.7.0: TFB_EXIT_STOP_JUDGMENT = observe (default; also an unknown word) | off. There is no 'on': the judgment
    is a note until its record has been reviewed with the owner."""
    return "off" if _env("TFB_EXIT_STOP_JUDGMENT", "observe").lower() in ("off", "0", "false", "no") else "observe"


def market_turn_labels() -> frozenset:
    """1.6.0: TFB_EXIT_MARKET_TURN_LABELS, the labels that turn a market (calm and unknown words skipped)."""
    raw = os.environ.get("TFB_EXIT_MARKET_TURN_LABELS")
    if raw is None:
        raw = MARKET_TURN_LABELS_DEFAULT
    return frozenset(w for w in (p.strip().lower() for p in raw.split(",")) if w in MARKET_TURN_HOLDABLE)


def knobs() -> Dict[str, Any]:
    k = {
        "atr_init": _env_num("TFB_EXIT_ATR_INIT", 3.0, 1.0, 6.0),
        "min_pct": _env_num("TFB_EXIT_MIN_PCT", 8.0, 2.0, 30.0),
        "max_pct": _env_num("TFB_EXIT_MAX_PCT", 12.0, 2.0, 30.0),
        "sr_max_pct": _env_num("TFB_EXIT_SR_MAX_PCT", 8.0, 2.0, 30.0),
        "trail": _env_num("TFB_EXIT_TRAIL_ATR", 3.0, 1.0, 6.0),
        "trail_tight": _env_num("TFB_EXIT_TRAIL_TIGHT_ATR", 2.0, 1.0, 6.0),
        "be_pct": _env_num("TFB_EXIT_BE_PCT", 0.3, 0.0, 5.0),
        "time_sessions": int(_env_num("TFB_EXIT_TIME_SESSIONS", 45, 5, 250)),
        "news_days": int(_env_num("TFB_EXIT_NEWS_DAYS", 3, 0, 30)),
        "fc_rel_min": _env_num("TFB_EXIT_FC_REL_MIN", 70, 0, 100),
        "earn_warn_days": int(_env_num("TFB_EXIT_EARN_WARN_DAYS", 5, 0, 30)),
        "div_warn_days": int(_env_num("TFB_EXIT_DIV_WARN_DAYS", 10, 0, 60)),
        "wht": _env_num("TFB_EXIT_WHT_PCT", 30.0, 0.0, 100.0),
        "mail": _env("TFB_EXIT_MAIL", "0") in ("1", "true", "on", "yes"),
        "ibkr_max_h": _env_num("TFB_EXIT_IBKR_MAX_AGE_H", 30.0, 1.0, 240.0),      # 1.3.2
        "div_net": _env("TFB_EXIT_DIV_NET", "on").lower() not in ("off", "0", "false", "no"),   # 1.4.0
        "broker_floor": _env("TFB_EXIT_BROKER_FLOOR", "on").lower() not in ("off", "0", "false", "no"),   # 1.5.0
        "market_turn": market_turn_mode(),                                                              # 1.6.0
        "market_turn_labels": market_turn_labels(),
        "offers": _env("TFB_EXIT_OFFERS", "on").lower() not in ("off", "0", "false", "no"),             # 1.7.0
        "offer_doubt_pct": _env_num("TFB_EXIT_OFFER_DOUBT_PCT", 50.0, 0.0, 100.0),
        "offer_gap_pct": _env_num("TFB_EXIT_OFFER_GAP_PCT", 8.0, 0.5, 50.0),
        "offer_near_pct": _env_num("TFB_EXIT_OFFER_NEAR_PCT", 0.5, 0.0, 5.0),
        "offer_warn_days": int(_env_num("TFB_EXIT_OFFER_WARN_DAYS", 5, 0, 60)),
        "stop_judgment": stop_judgment_mode(),                                                          # 1.7.0
        "sj_window": int(_env_num("TFB_EXIT_SJ_WINDOW", 15, 5, 60)),
        "sj_drop": _env_num("TFB_EXIT_SJ_DROP_PCT", 8.0, 1.0, 50.0),
        "sj_market": _env_num("TFB_EXIT_SJ_MARKET_PCT", 5.0, 0.5, 50.0),
        "sj_alone": _env_num("TFB_EXIT_SJ_ALONE_PCT", 1.0, 0.0, 50.0),
        "sj_margin": _env_num("TFB_EXIT_SJ_MARGIN_PCT", 5.0, 0.0, 50.0),
        "sj_hard": _env_num("TFB_EXIT_SJ_HARD_PCT", 15.0, 2.0, 80.0),
        "sj_near": _env_num("TFB_EXIT_SJ_NEAR_PCT", 3.0, 0.0, 30.0),
    }
    k["sj_alone"] = min(k["sj_alone"], k["sj_market"])
    k["min_pct"] = min(k["min_pct"], k["max_pct"])
    k["trail_tight"] = min(k["trail_tight"], k["trail"])
    return k


# ----------------------------------------------------------------------------- #
# Bars and the rule (pure)
# ----------------------------------------------------------------------------- #
Bar = Tuple[_dt.date, float, float, float, float, float]   # date, open, high, low, close, adjustment factor


def parse_chart(payload: Dict[str, Any]) -> List[Bar]:
    """Daily bars from a Yahoo v8 chart payload, dividend-adjusted when the payload carries the adjusted
    close (factor = adjusted close / close, applied to all four prices); a bar without all four prices is
    dropped."""
    try:
        r = payload["chart"]["result"][0]
        ts = r.get("timestamp") or []
        q = r["indicators"]["quote"][0]
        off = int((r.get("meta") or {}).get("gmtoffset") or 0)
    except (KeyError, IndexError, TypeError, ValueError):
        return []
    try:
        adj = r["indicators"]["adjclose"][0]["adjclose"]
    except (KeyError, IndexError, TypeError):
        adj = None
    out: Dict[_dt.date, Bar] = {}
    for i, t in enumerate(ts):
        try:
            o, h, lo, c = (float(q[key][i]) for key in ("open", "high", "low", "close"))
        except (TypeError, ValueError, KeyError, IndexError):
            continue
        if not all(math.isfinite(x) and x > 0 for x in (o, h, lo, c)) or h < lo:
            continue
        f = 1.0
        try:
            a = float(adj[i]) if adj is not None else None
            if a is not None and math.isfinite(a) and a > 0:
                f = a / c
        except (TypeError, ValueError, IndexError):
            f = 1.0
        if not 0.2 <= f <= 1.5:
            f = 1.0                                      # an implausible factor is not applied
        d = _dt.datetime.fromtimestamp(int(t) + off, UTC).date()
        out[d] = (d, o * f, h * f, lo * f, c * f, f)
    return [out[d] for d in sorted(out)]


def split_open_session(payload: Dict[str, Any], bars: List[Bar], now_ts: float) -> Tuple[List[Bar], Optional[float]]:
    """(completed bars, live price). While the venue's regular session is open (Yahoo's currentTradingPeriod), the
    last daily bar is the session so far: it is set aside and its close is the live price."""
    try:
        r = payload["chart"]["result"][0]
        meta = r.get("meta") or {}
        reg = (meta.get("currentTradingPeriod") or {}).get("regular") or {}
        start, end = int(reg["start"]), int(reg["end"])
        off = int(meta.get("gmtoffset") or 0)
    except (KeyError, IndexError, TypeError, ValueError):
        return bars, None
    if bars and start <= now_ts < end and bars[-1][0] == _dt.datetime.fromtimestamp(start + off, UTC).date():
        return bars[:-1], bars[-1][4]
    return bars, None


DIV_KEEP_MIN, DIV_KEEP_MAX = 0.5, 0.995      # 1.4.0: the kept share of a dividend that is applied


def read_dividend_tax(values: Sequence[Sequence[Any]]) -> Dict[str, Dict[_dt.date, float]]:
    """1.4.0 [DIV-NET]: {SYMBOL: {ex-date: kept share}} from _Dividend_Log. Rows with a positive Amount are the
    dividend; rows with a negative Amount are the tax recorded against the same symbol and ex-date. Listed only when
    the kept share (net / gross) lies in [DIV_KEEP_MIN, DIV_KEEP_MAX): a dividend with no tax row stays gross, and an
    implausible ratio (a typing error, a reversal) is not applied."""
    h, col = table_index(values, ("Symbol", "Ex-Date", "Amount"))
    if h is None:
        return {}
    sums: Dict[Tuple[str, _dt.date], List[float]] = {}
    for r in list(values)[h + 1:]:
        sym, ex, amt = text(cell(r, col.get("Symbol"))).upper(), parse_date(cell(r, col.get("Ex-Date"))), num(
            cell(r, col.get("Amount")))
        if not sym or ex is None or amt is None or not math.isfinite(amt) or amt == 0:
            continue
        g = sums.setdefault((sym, ex), [0.0, 0.0])           # [gross, net]
        g[0] += max(0.0, amt)
        g[1] += amt
    out: Dict[str, Dict[_dt.date, float]] = {}
    for (sym, ex), (gross, net) in sums.items():
        if gross > 0 and DIV_KEEP_MIN <= net / gross < DIV_KEEP_MAX:
            out.setdefault(sym, {})[ex] = net / gross
    return out


def net_of_tax(bars: Sequence[Bar], kept: Optional[Dict[_dt.date, float]],
               after: _dt.date) -> Tuple[List[Bar], List[float]]:
    """1.4.0 [DIV-NET]: (the bars with each listed dividend counted net of its tax, [tax percent of each one
    applied]). Across an ex-date Yahoo's factor steps by g = 1 - dividend / previous close; the bars before it are
    rescaled so the step is 1 - kept x dividend / previous close. Only an ex-date after `after` (the purchase)
    counts - the lot received nothing of an earlier dividend - and only where the history shows the step on the
    first bar at or within a week after that date."""
    out = list(bars)
    applied: List[float] = []
    for ex in sorted(kept or {}):
        keep = (kept or {})[ex]
        if ex <= after or not DIV_KEEP_MIN <= keep < DIV_KEEP_MAX:
            continue
        idx = next((i for i, b in enumerate(out) if b[0] >= ex), None)
        if not idx or (out[idx][0] - ex).days > 7 or len(out[idx]) <= 5 or len(out[idx - 1]) <= 5:
            continue
        g = out[idx - 1][5] / out[idx][5]
        if not 0.2 <= g < 1 - 1e-6:
            continue                                          # the history carries no dividend step there
        m = (1 - keep * (1 - g)) / g
        out[:idx] = [(b[0], b[1] * m, b[2] * m, b[3] * m, b[4] * m, b[5] * m) if len(b) > 5 else b for b in out[:idx]]
        applied.append((1 - keep) * 100.0)
    return out, applied


def atr(bars: Sequence[Bar], end: int, n: int = 14) -> Optional[float]:
    """Average true range of the n bars ending at index `end` (inclusive); None with fewer than n/2."""
    trs = []
    for i in range(max(1, end - n + 1), end + 1):
        h, lo = bars[i][2], bars[i][3]
        pc = bars[i - 1][4]
        trs.append(max(h - lo, abs(h - pc), abs(lo - pc)))
    return sum(trs) / len(trs) if len(trs) >= max(2, n // 2) else None


def yahoo_symbol(symbol: str) -> str:
    s = text(symbol).upper()
    if s.endswith(".US"):
        return s[:-3].replace(".", "-")
    return s


def _round(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(x, 4)


def exit_levels(symbol: str, entry: float, entry_date: _dt.date, bars: Sequence[Bar], info: Dict[str, Any],
                prev: Dict[str, Any], k: Dict[str, Any], today: _dt.date) -> Dict[str, Any]:
    """The stop, target and action for one lot. `prev` = the last ledger row for the same lot (or {})."""
    res: Dict[str, Any] = {"symbol": symbol, "entry": entry, "ok": False, "why": [], "tighten": []}
    if not bars or entry <= 0:
        res["action"], res["why"] = "CHECK", ["no daily price history"]
        return res
    idx_e = max((i for i, b in enumerate(bars) if b[0] <= entry_date), default=None)
    f_e = bars[idx_e][5] if idx_e is not None and len(bars[idx_e]) > 5 else 1.0
    buy = entry
    entry = buy * f_e
    res["entry"] = entry
    a_e = atr(bars, idx_e) if idx_e is not None else None
    if a_e is None:
        a_e = atr(bars, min(len(bars) - 1, 14))      # bought before the history starts: the earliest ATR
    a_now = atr(bars, len(bars) - 1)
    if a_e is None or a_now is None:
        res["action"], res["why"] = "CHECK", ["not enough daily bars for an ATR"]
        return res
    hi_pct = k["sr_max_pct"] if symbol.upper().endswith(".SR") else k["max_pct"]
    lo_pct = min(k["min_pct"], hi_pct)
    dist_pct = min(hi_pct, max(lo_pct, k["atr_init"] * a_e / entry * 100.0))
    r = entry * dist_pct / 100.0
    init_stop = entry - r
    since = [b for b in bars if b[0] >= entry_date] or [bars[-1]]
    hc = max(b[4] for b in since)
    after = [b for b in bars if b[0] > entry_date]      # the buy day's high may come before the fill
    hh = max((b[2] for b in after), default=0.0)
    last = bars[-1]
    closes = [b[4] for b in bars[-20:]]
    sma20 = sum(closes) / len(closes)
    fresh = last[0] < entry_date                         # 1.1.0: bought in a session that has not closed yet
    if fresh:
        hc = entry
    r_high = (hc - entry) / r
    news_neg = int(info.get("news_neg") or 0)
    if news_neg:
        res["tighten"].append("negative news (%d item%s in %d days)" % (
            news_neg, "" if news_neg == 1 else "s", k["news_days"]))
    fc, rel = info.get("forecast_1m"), info.get("reliability")
    if fc is not None and rel is not None and rel >= k["fc_rel_min"] and fc < last[4]:
        res["tighten"].append("1-month forecast %s is below the last close (reliability %.0f)" % (fmt(fc), rel))
    if r_high >= 2 and last[4] < sma20:
        res["tighten"].append("close under its 20-day average after +%.1fR" % r_high)
    trail_k = k["trail_tight"] if res["tighten"] else k["trail"]
    stop = init_stop
    if r_high >= 1:
        stop = max(stop, entry * (1 + k["be_pct"] / 100.0), hc - trail_k * a_now)
    # 1.8.0 [NEWS-SHADOW]: the same stop with the negative-news reason taken out (any other reason still tightens)
    news_reasons = [t for t in res["tighten"] if t.startswith("negative news")]
    trail_nn = k["trail_tight"] if len(res["tighten"]) > len(news_reasons) else k["trail"]
    stop_nn = init_stop
    if r_high >= 1:
        stop_nn = max(stop_nn, entry * (1 + k["be_pct"] / 100.0), hc - trail_nn * a_now)
    prev_stop = was_stop = None
    if prev:
        # a dividend paid since the last run re-adjusts the history (and the entry); the stop is rebased with it
        norm = num(prev.get("StopNorm"))
        prev_stop = norm * f_e if norm is not None else num(prev.get("Stop"))
        if "Was" in prev:                                  # 1.1.0: the stop before this close (prior_row)
            wnorm = num(prev.get("WasNorm"))
            was_stop = wnorm * f_e if wnorm is not None else num(prev.get("Was"))
        else:
            was_stop = prev_stop
    if prev_stop is not None and prev_stop > stop:
        stop = prev_stop                                   # the stop never moves down
    if prev:                                               # 1.8.0: the shadow follows its own history
        nn_norm = num(prev.get("StopNNNorm"))
        prev_nn = nn_norm * f_e if nn_norm is not None else num(prev.get("StopNN"))
        prev_nn = prev_stop if prev_nn is None else prev_nn
        if prev_nn is not None and prev_nn > stop_nn:
            stop_nn = prev_nn
    bs = info.get("broker_stop")                           # 1.5.0 [BROKER-FLOOR]
    # The floor is the LOWEST working stop of the IBKR capture ("broker_stop_floor", set by run()); the workbook's
    # older broker tabs keep the highest per symbol and never set it. A direct caller's broker_stop is its floor.
    fl = info.get("broker_stop_floor", bs) if k.get("broker_floor") else None
    rule_stop, floored = stop, False
    if fl is not None and stop * 1.0025 < fl < last[4]:
        stop, floored = fl, True                           # never below the stop working at the broker
    if fl is not None and stop_nn * 1.0025 < fl < last[4]:
        stop_nn = fl                                       # 1.8.0: the shadow takes the same floor
    # the broker already carries the stop of record (on a re-run of the same close the ledger holds the floor, so
    # `floored` is False while the earlier stop is still the one "before this close"): never a RAISE STOP to-do
    carried = floored or (fl is not None and fl >= stop * 0.9975 and fl < last[4])
    step = max(1, int(math.floor(r_high)) + 1)
    target = entry + step * r
    tp1 = hh >= entry + r
    touch = next((b for b in after if b[2] >= entry + r), None)
    tp1_now = last[4] >= entry + r                      # the report runs after the close: the close decides
    sessions = sum(1 for b in bars if b[0] > entry_date)
    offer = info.get("offer") if k.get("offers") and not fresh else None          # 1.7.0 [OFFER]
    held = bool(offer) and offer.get("sale") == "manual"
    if held:
        # the stop of record is frozen at its value before this close: a price pinned to an offer has a shrinking
        # ATR, and a trail that follows it sells on noise
        frozen = was_stop if was_stop is not None else prev_stop
        if frozen is not None:
            stop = frozen
        stop_nn = stop                                     # 1.8.0: a frozen stop has no news to take out
        floored = carried = False
        res["tighten"] = []                              # nothing tightens a stop that is not in force
        target = offer["price"]                          # Codex P2 on #1084: the offer price itself, also above the next R step
    elif offer:
        target = min(target, offer["price"])             # sale = rule: the price rule's target, capped at the offer
    res.update({"ok": True, "basis": f_e, "atr": a_now, "atr_entry": a_e, "atr_pct": a_now / last[4] * 100.0, "r": r,
                "dist_pct": dist_pct, "init_stop": init_stop, "hc": hc, "last": last[4], "bars_to": last[0],
                "r_high": r_high, "stop": stop, "prev_stop": was_stop, "target": target, "tp1": tp1,
                "trail_k": trail_k, "sessions": sessions, "stop_no_news": min(stop_nn, stop),
                "news_tightened": bool(news_reasons) and not held, "pl_pct": (last[4] / entry - 1) * 100.0,
                "stop_gap_pct": (1 - stop / last[4]) * 100.0, "fresh": fresh})
    if fresh:
        live_px = info.get("live")
        res["pl_pct"] = (live_px / entry - 1) * 100.0 if live_px else None
    tp1_before = text(prev.get("TP1 Before", prev.get("TP1 Reached"))).lower() == "yes" if prev else False
    tp1_today = text(prev.get("TP1 Today")).lower() == "yes" if prev else False
    if fresh:                                             # Codex P2 on #959: no close since the purchase yet
        res["action"] = "HOLD"
        res["why"].append("bought on %s: no close since the purchase yet, so the close rules wait for the first one; "
                          "the initial stop %s and the first target %s apply" % (entry_date.isoformat(), fmt(stop),
                                                                                 fmt(entry + r)))
    elif held:
        offer_action(res, offer, bars, k, today)
    elif last[4] <= stop:
        res["action"] = "SELL"
        res["why"].append("last close %s is at or below the stop %s" % (fmt(last[4]), fmt(stop)))
    elif tp1_today:                                       # issued on this close by an earlier run: it stays
        res["action"] = "TAKE PROFIT"
        res["why"].append("first target %s reached (+1R): sell half (issued on the close of %s); the stop rises to "
                          "at least the entry" % (fmt(entry + r), last[0].isoformat()))
    elif tp1 and not tp1_before and tp1_now:
        res["action"] = "TAKE PROFIT"
        res["why"].append("first target %s reached (+1R): sell half; the stop rises to at least the entry"
                          % fmt(entry + r))
    elif was_stop is not None and stop > was_stop * 1.0025 and not carried:
        res["action"] = "RAISE STOP"
        res["why"].append("raise the stop %s -> %s" % (fmt(was_stop), fmt(stop)))
    elif was_stop is None and r_high >= 1 and not carried:
        res["action"] = "RAISE STOP"
        res["why"].append("the stop belongs at %s (+%.1fR at the high)" % (fmt(stop), r_high))
    elif sessions >= k["time_sessions"] and r_high < 1:
        res["action"] = "REVIEW"
        res["why"].append("%d sessions without +1R" % sessions)
    else:
        res["action"] = "HOLD"
        res["why"].append("+%.1fR at the high; stop %.1f%% under the last close" % (
            max(0.0, r_high), res["stop_gap_pct"]))
    if tp1 and not tp1_now and not tp1_before and touch is not None and not held:
        res["why"].append("the first target %s was touched on %s (high %s) and the price is back under it" % (
            fmt(entry + r), touch[0].isoformat(), fmt(touch[2])))
    if offer and not held:                                # 1.7.0: sale = rule - the price rule stays in force
        res["offer"] = _offer_view(offer, bars, last[4], today)
        res["why"].append("%s: the target is capped at the offer price; if its shares were tendered, a sell order "
                          "on them can open a short position" % offer_words(res["offer"]))
    if f_e < 0.995:
        tax = info.get("div_tax_pct") or []                # 1.4.0 [DIV-NET]
        res["why"].append("entry %s is the buy price %s adjusted for the dividends paid since%s" % (
            fmt(entry), fmt(buy), (", net of the %s%% tax withheld (dividend log)" % "/".join(
                "%g" % round(t, 1) for t in tax)) if tax else ""))
    if res["tighten"] and not held:
        res["why"].append("trail tightened to %gxATR: %s" % (trail_k, "; ".join(res["tighten"])))
    ed = info.get("earn_days")
    if ed is not None and 0 <= ed <= k["earn_warn_days"] and not held:
        res["why"].append("earnings %s: a gap can jump the stop - decide to hold through or sell half" % (
            "today" if ed == 0 else "in %d day(s)" % ed))
    live = info.get("live")
    res["live"] = live
    if held:
        return res                                        # no stop is in force: nothing to compare with a stop
    if live is not None and live <= stop:
        res["why"].append("live price %s is at or under the stop %s (a broker stop fills intraday; the rule acts on "
                          "the close)" % (fmt(live), fmt(stop)))
    if floored:                                           # 1.5.0 [BROKER-FLOOR]
        res["why"].append("the stop of record is the stop working at IBKR (%s), above the rule's %s: a stop never "
                          "falls back" % (fmt(fl), fmt(rule_stop)))
    elif carried and was_stop is not None and stop > was_stop * 1.0025:
        res["why"].append("the stop working at IBKR (%s) already carries the stop of record %s: nothing to raise"
                          % (fmt(fl), fmt(stop)))
    if bs is not None and bs < stop * 0.9975:
        res["why"].append("broker stop %s is under the plan's %s" % (fmt(bs), fmt(stop)))
    return res


# 1.7.0 [OFFER] -------------------------------------------------------------------
def _offer_view(offer: Dict[str, Any], bars: Sequence[Bar], last: float, today: _dt.date) -> Dict[str, Any]:
    """The offer with its reading for the page: days left, the gap to the offer and what the price implies about
    completion against the close before it (that close as printed on its day: adjusted close / its factor)."""
    from core import corporate_offers as co  # noqa: PLC0415 - stdlib only
    before = co.close_before(offer, [(b[0], b[4] / (b[5] if len(b) > 5 and b[5] else 1.0)) for b in bars])
    out = dict(offer)
    out.update(co.reading(offer, last, today, before))
    out["label"] = co.label(offer)
    return out


def offer_words(o: Dict[str, Any]) -> str:
    """'cash offer 16.25 a share (cash tender by X, conditional), open until 2026-12-10 - 14 day(s) left ...'."""
    days = o["days_left"]
    out = "cash offer %s a share (%s), open until %s - %s, and the broker's cut-off comes earlier" % (
        fmt(o["price"]), o["label"], o["expires"].isoformat(),
        "the last day" if days == 0 else "%d day%s left" % (days, "" if days == 1 else "s"))
    gap = o.get("gap_pct")
    if gap is not None:
        out += "; the last close is %.1f%% %s it" % (abs(gap), "under" if gap >= 0 else "above")
        if o.get("implied_pct") is not None:
            out += (", which against the close before the offer (%s) prices about %.0f%% that it completes (a rough "
                    "reading)" % (fmt(o["before"]), o["implied_pct"]))
    return out


def offer_action(res: Dict[str, Any], offer: Dict[str, Any], bars: Sequence[Bar], k: Dict[str, Any],
                 today: _dt.date) -> None:
    """HOLD or REVIEW for a holding under a standing cash offer whose sale is the owner's manual decision: the offer's
    own terms decide - the gap to it, what the price implies about completion, the days left - never the price stop."""
    o = _offer_view(offer, bars, res["last"], today)
    res["offer"], res["offer_held"] = o, True
    gap, implied, days = o.get("gap_pct"), o.get("implied_pct"), o["days_left"]
    flags: List[str] = []
    if gap is not None and gap <= k["offer_near_pct"]:
        flags.append("the market pays the offer now (the last close is within %g%% of it or above it): a sale in the "
                     "market takes that price without the wait or the conditions" % k["offer_near_pct"])
    if implied is not None and implied < k["offer_doubt_pct"]:
        flags.append("the price puts the offer in doubt (it prices under %g%% that it completes): read today's news "
                     "on it before anything else" % k["offer_doubt_pct"])
    elif implied is None and gap is not None and gap > k["offer_gap_pct"]:
        flags.append("the price puts the offer in doubt (the last close is more than %g%% under it): read today's "
                     "news on it before anything else" % k["offer_gap_pct"])
    if days <= k["offer_warn_days"]:
        flags.append("%s: decide - sell in the market, tender at the broker, or keep the shares" % (
            "the offer closes today" if days == 0 else "%d day%s to the last day" % (days, "" if days == 1 else "s")))
    res["action"] = "REVIEW" if flags else "HOLD"
    res["why"].append(offer_words(o))
    res["why"] += flags
    res["why"].append("the sale is the owner's manual decision: no stop order is asked for; the stop of record %s is "
                      "frozen while the offer stands and the target is the offer price; the market-turn rule does "
                      "not apply (the price follows the offer, not the market)" % fmt(res["stop"]))


# 1.7.0 [STOP-JUDGMENT] -----------------------------------------------------------
SJ_WORDS = {"with_market": "WITH ITS MARKET", "alone": "ON ITS OWN", "mixed": "partly with its market", "no_fall": ""}
SJ_MARKET_MAX_GAP_DAYS = 4


def stop_judgment(bars: Sequence[Bar], market: Sequence[Tuple[_dt.date, float]], res: Dict[str, Any],
                  k: Dict[str, Any], market_name: str = "") -> Optional[Dict[str, Any]]:
    """{"cause", "verdict", "stock_pct", "market_pct", "code", "show", "text"} - observe only. The holding's move over
    the window beside its market's over the same days; a fall is named with the market, on its own or mixed, and the
    rule under test would wait only on a fall with the market that stays within the margin and above the hard limit.
    None when the holding has no rule result, the history is shorter than the window, or the market reading has no
    close within SJ_MARKET_MAX_GAP_DAYS days of either end."""
    n = k["sj_window"]
    if not res.get("ok") or res.get("fresh") or len(bars) <= n or not market:
        return None
    (d1, c1), (d0, c0) = (bars[-1][0], bars[-1][4]), (bars[-1 - n][0], bars[-1 - n][4])

    def at(day: _dt.date) -> Optional[float]:
        prior = [(d, c) for d, c in market if d <= day and c and c > 0]
        if not prior or (day - prior[-1][0]).days > SJ_MARKET_MAX_GAP_DAYS:
            return None
        return prior[-1][1]
    m1, m0 = at(d1), at(d0)
    if not (c0 and c0 > 0 and c1 and c1 > 0 and m1 and m0):
        return None
    s, m = (c1 / c0 - 1.0) * 100.0, (m1 / m0 - 1.0) * 100.0
    x = s - m
    loss = res.get("pl_pct")
    if s > -k["sj_drop"]:
        cause = "no_fall"
    elif m <= -k["sj_market"]:
        cause = "with_market"
    elif m >= -k["sj_alone"]:
        cause = "alone"
    else:
        cause = "mixed"
    if loss is not None and loss <= -k["sj_hard"]:
        verdict = "hard_limit"
    elif cause == "no_fall":
        verdict = "none"
    elif cause == "with_market" and x >= -k["sj_margin"]:
        verdict = "wait"
    else:
        verdict = "stands"
    near = res.get("stop_gap_pct") is not None and res["stop_gap_pct"] <= k["sj_near"]
    head = "stop judgment (a note under test): over %d sessions the stock %+.1f%% and %s %+.1f%%" % (
        n, s, market_name or "its market", m)
    if verdict == "hard_limit":
        body = ("the loss from the entry (%.1f%%) is past the hard limit of %g%% - the rule under test sells whatever "
                "the cause" % (loss, k["sj_hard"]))
    elif cause == "no_fall":
        body = "no fall of %g%% to judge - the stop is close because it trails the price" % k["sj_drop"]
    elif verdict == "wait":
        body = ("it fell %s (%.1f points %s) - the rule under test would wait rather than sell at the stop, while it "
                "stays within %g points of its market and the loss from the entry stays under %g%%" % (
                    SJ_WORDS[cause], abs(x), "worse than it" if x < 0 else "better than it", k["sj_margin"],
                    k["sj_hard"]))
    elif cause == "with_market":
        body = ("its market fell too, but it fell %.1f points more (the margin is %g) - the rule under test keeps "
                "the stop" % (abs(x), k["sj_margin"]))
    elif cause == "alone":
        body = "it fell %s while its market held - the rule under test keeps the stop" % SJ_WORDS[cause]
    else:
        body = "it fell %s, partly on its own - the rule under test keeps the stop" % SJ_WORDS[cause]
    return {"cause": cause, "verdict": verdict, "stock_pct": s, "market_pct": m,
            "code": "%s:%s:%+.1f:%+.1f" % (cause, verdict, s, m),
            "show": cause != "no_fall" or near or res.get("action") == "SELL" or verdict == "hard_limit",
            "text": "%s: %s. Nothing changes: the stop of record and the stop at the broker stand" % (head, body)}


def read_market_closes(market_id: str, now: _dt.datetime, opener: Callable[..., Any]
                       ) -> List[Tuple[_dt.date, float]]:
    """The completed daily closes of a market's own reading (core.market_condition: SPY, ^TASI.SR ...); [] on any
    fault - then no holding of that market gets a judgment."""
    try:
        from core import market_condition as mc  # noqa: PLC0415 - stdlib only
        return list(mc.series(mc.MARKETS[market_id][0], now.timestamp(), opener, _SLEEP))
    except Exception as e:  # noqa: BLE001
        _out("market closes not read (%s): no stop judgment for that market this run" % type(e).__name__)
        return []


def market_reading_name(market_id: str) -> str:
    try:
        from core import market_condition as mc  # noqa: PLC0415
        return "its market (%s)" % mc.MARKETS[market_id][1]
    except Exception:  # noqa: BLE001
        return "its market"


# 1.6.0 [MARKET-TURN] -------------------------------------------------------------
def read_market(now: _dt.datetime, opener: Callable[..., Any]) -> Dict[str, Dict[str, Any]]:
    """The label of each read market on its last completed session (core.market_condition 1.0.0, the rules of
    sql 99_v_market_condition); {} on any fault - then no holding gets a market note."""
    try:
        from core import market_condition as mc  # noqa: PLC0415 - stdlib only
        return mc.read(now=now, opener=opener, sleep=_SLEEP)
    except Exception as e:  # noqa: BLE001
        _out("market labels not read (%s): no market-turn note this run" % type(e).__name__)
        return {}


def market_of(symbol: str) -> str:
    try:
        from core import market_condition as mc  # noqa: PLC0415
        return mc.market_of(symbol)
    except Exception:  # noqa: BLE001
        return ""


def market_turn(symbol: str, asset: str, labels: Dict[str, Dict[str, Any]], k: Dict[str, Any]
                ) -> Optional[Dict[str, Any]]:
    """{"market", "label", "day", "text", "turned"} for a holding of a read market; None for an instrument no
    stock-market label describes (MARKET_TURN_EXEMPT), a symbol of no read market, or a market not read."""
    if any(tok in (asset or "").lower() for tok in MARKET_TURN_EXEMPT):
        return None
    mid = market_of(symbol)
    info = labels.get(mid) if mid else None
    if not info or not info.get("label"):
        return None
    label = str(info["label"])
    day = info.get("day")
    return {"market": mid, "label": label, "day": day.isoformat() if hasattr(day, "isoformat") else text(day),
            "text": text(info.get("text")) or "%s %s" % (mid, label), "turned": label in k["market_turn_labels"]}


def apply_market_turn(res: Dict[str, Any], mt: Optional[Dict[str, Any]], k: Dict[str, Any]) -> None:
    """res["market"] for the ledger; for a turned market the note (observe) or the SELL (on). A lot with no close
    since its purchase waits for the first one, as the close rules do."""
    if not mt or not res.get("ok"):
        return
    res["market"] = "%s:%s:%s" % (mt["market"], mt["label"], mt["day"])
    if not mt["turned"] or res.get("fresh") or res.get("offer_held"):         # 1.7.0: an offer pins the price
        return
    res["market_turned"] = True
    if k["market_turn"] == "on":
        res["action"] = "SELL"
        res["why"].insert(0, "the market turned: %s - sell (market-turn rule)" % mt["text"])
    else:
        res["why"].append("market not calm (observe): %s - the market-turn rule would sell; nothing changes until "
                          "TFB_EXIT_MARKET_TURN=on" % mt["text"])


def dividend_note(symbol: str, shares: Any, last: Any, ev: Dict[str, Any], k: Dict[str, Any]) -> str:
    """1.2.0 [DIV-TAX]: a holding with an ex-dividend inside TFB_EXIT_DIV_WARN_DAYS days. The price falls by the full
    dividend on the ex-date; a Saudi resident keeps it less the US withholding (TFB_EXIT_WHT_PCT on a US listing).
    '' when there is no known dividend in the window."""
    dt = dividend_tax(symbol, "", last, ev, k["div_warn_days"], k["wht"])   # the listing decides the withholding
    if not dt:
        return ""
    qty = num(shares) or 0.0
    when = "today" if dt["days"] == 0 else "in %d day(s)" % dt["days"]
    msg = "ex-dividend %s: %s a share (%.1f%% of the price) - the price falls by the full dividend" % (
        when, fmt(dt["amount"]), dt["yield_pct"])
    if dt["wht_pct"] > 0:
        msg += ("; you keep %g%% after the %g%% US withholding (%s withheld on %d shares) - if you plan to sell "
                "within weeks, selling before the ex-date avoids it" % (100 - dt["wht_pct"], dt["wht_pct"],
                                                                         fmt(qty * dt["amount"] * dt["wht_pct"] / 100.0),
                                                                         int(qty)))
    return msg


def pick_levels(symbol: str, entry: float, bars: Sequence[Bar], k: Dict[str, Any]) -> Dict[str, Any]:
    """Initial stop and first target for a new pick, from its own ATR."""
    if not bars or entry <= 0:
        return {"ok": False}
    a = atr(bars, len(bars) - 1)
    if a is None:
        return {"ok": False}
    hi_pct = k["sr_max_pct"] if symbol.upper().endswith(".SR") else k["max_pct"]
    dist = min(hi_pct, max(min(k["min_pct"], hi_pct), k["atr_init"] * a / entry * 100.0))
    r = entry * dist / 100.0
    return {"ok": True, "stop": entry - r, "dist_pct": dist, "target": entry + r, "atr_pct": a / entry * 100.0}


# ----------------------------------------------------------------------------- #
# Workbook parsers (pure)
# ----------------------------------------------------------------------------- #
def read_lots(values: Sequence[Sequence[Any]]) -> Dict[str, Dict[str, Any]]:
    """Active lots per symbol: shares, the weighted buy price, the latest buy date."""
    h, col = table_index(values, ("Symbol", "Status", "Buy Date", "Buy Price", "Shares"))
    out: Dict[str, Dict[str, Any]] = {}
    if h is None:
        return out
    for r in list(values)[h + 1:]:
        sym = text(cell(r, col.get("Symbol"))).upper()
        if not sym or text(cell(r, col.get("Status"))).lower() != "active":
            continue
        px, sh = num(cell(r, col.get("Buy Price"))), num(cell(r, col.get("Shares")))
        bd = parse_date(cell(r, col.get("Buy Date")))
        if not px or not sh or px <= 0 or sh <= 0 or bd is None:
            continue
        o = out.setdefault(sym, {"symbol": sym, "name": text(cell(r, col.get("Name"))), "shares": 0.0,
                                 "cost": 0.0, "date": bd, "ccy": text(cell(r, col.get("Ccy"))), "lots": 0})
        o["lots"] += 1
        o["shares"] += sh
        o["cost"] += px * sh
        o["date"] = max(o["date"], bd)
    for o in out.values():
        o["buy"] = o["entry"] = o["cost"] / o["shares"]
    return out


def read_info(grids: Sequence[Sequence[Sequence[Any]]]) -> Dict[str, Dict[str, Any]]:
    """{SYMBOL: name, asset class, forecast 1M, reliability, confidence, target}; the first page listing it wins."""
    out: Dict[str, Dict[str, Any]] = {}
    for g in grids:
        h, col = table_index(g, ("Symbol",))
        if h is None:
            continue
        rel_col = next((c for name, c in col.items() if name.startswith("Forecast Reliability")), None)
        for r in list(g)[h + 1:]:
            sym = text(cell(r, col.get("Symbol"))).upper()
            if not sym or sym in out:
                continue
            out[sym] = {"name": text(cell(r, col.get("Name"))), "asset": text(cell(r, col.get("Asset Class"))),
                        "price": num(cell(r, col.get("Current Price"))),
                        "forecast_1m": num(cell(r, col.get("Forecast Price (1M)"))),
                        "reliability": num(cell(r, rel_col)), "confidence": num(cell(r, col.get("Confidence Score"))),
                        "target": num(cell(r, col.get("Target Price")))}
    return out


NEWS_NOISE_RE = re.compile(
    r"shareholder (alert|update|notice|investigation)|notif(ying|ies) investors|investors? who lost|"
    r"investigations? on behalf|law (firm|offices?)|"
    r"class action (lawsuit has been filed|deadline|reminder|notice|alert)|"
    r"securities (litigation|fraud) (alert|investigation|class action|notice|reminder|deadline)|"
    r"deadline (alert|reminder)|brodsky|rosen law|pomerantz|levi & korsinsky|bragar eagel|bronstein|faruqi|"
    r"hagens berman|glancy|kessler topaz|robbins geller|schall law|johnson fistel", re.I)


def news_is_noise(event_type: Any, headline: Any, grade: Any) -> bool:
    """1.3.0 [NEWS-NOISE]: a law-firm solicitation (they go out on tier-A wires and read 'negative'), or a cluster
    that is rumour-only, denied or social-only (information grade 4-6), never tightens a stop. The legal terms match
    solicitation wording only: 'SEC charges X with securities fraud' or 'X hit with class action' still counts."""
    if text(event_type).lower() == "law_firm_noise" or NEWS_NOISE_RE.search(text(headline)):
        return True
    g = num(grade)
    return g is not None and g >= 4


def read_news(values: Sequence[Sequence[Any]], now: _dt.datetime, days: int) -> Dict[str, Dict[str, Any]]:
    """Negative symbol news (direction -1, severity H or M) published in the last `days` days. 1.3.0: items that
    news_is_noise() are counted under 'ignored' and never under 'n'."""
    h, col = table_index(values, ("Scope", "Direction", "Severity"))
    out: Dict[str, Dict[str, Any]] = {}
    if h is None or days <= 0:
        return out
    since = now - _dt.timedelta(days=days)
    for r in list(values)[h + 1:]:
        scope = text(cell(r, col.get("Scope"))).upper()
        if not scope or scope == "MACRO":
            continue
        if num(cell(r, col.get("Direction"))) != -1 or text(cell(r, col.get("Severity"))).upper() not in ("H", "M"):
            continue
        pub = _parse_ts(cell(r, col.get("Published (UTC)")))
        if pub is None or pub < since:
            continue
        o = out.setdefault(scope, {"n": 0, "headline": "", "at": None, "ignored": 0})
        if news_is_noise(cell(r, col.get("Event Type")), cell(r, col.get("Headline")), cell(r, col.get("Cluster Grade"))):
            o["ignored"] += 1
            continue
        o["n"] += 1
        if o["at"] is None or pub > o["at"]:
            o["at"], o["headline"] = pub, text(cell(r, col.get("Headline")))[:90]
    return out


def _parse_ts(v: Any) -> Optional[_dt.datetime]:
    """'2026-11-10 16:30' / '2026-11-10T16:30...' / '2026-11-10' as UTC; None otherwise."""
    s = text(v)
    for f, n in (("%Y-%m-%d %H:%M", 16), ("%Y-%m-%dT%H:%M", 16), ("%Y-%m-%d", 10)):
        try:
            return _dt.datetime.strptime(s[:n], f).replace(tzinfo=UTC)
        except ValueError:
            continue
    return None


def read_broker_stops(account: Sequence[Sequence[Any]], orders: Sequence[Sequence[Any]],
                      now: _dt.datetime, max_age_h: float = 30.0) -> Tuple[Dict[str, float], str]:
    """Working SELL STOP prices per symbol when the _IBKR_Account read is fresh, else ({}, why)."""
    as_of = None
    for r in account:
        if text(cell(r, 0)).lower() == "as_of":
            s = text(cell(r, 1))
            try:
                as_of = _dt.datetime.strptime(s[:16], "%Y-%m-%d %H:%M").replace(tzinfo=RIYADH)
            except ValueError:
                as_of = None
    if as_of is None:
        return {}, "broker orders unknown (no _IBKR_Account read)"
    age = (now - as_of).total_seconds() / 3600.0
    if age > max_age_h:
        return {}, "broker orders unknown (last read %s, %.0f h old)" % (as_of.strftime("%Y-%m-%d %H:%M"), age)
    h, col = table_index(orders, ("symbol", "side", "type", "price", "status"))
    out: Dict[str, float] = {}
    if h is None:
        return out, "broker orders read %s" % as_of.strftime("%Y-%m-%d %H:%M")
    for r in list(orders)[h + 1:]:
        if text(cell(r, col.get("side"))).upper() != "SELL" or "STOP" not in text(cell(r, col.get("type"))).upper():
            continue
        if text(cell(r, col.get("status"))).upper() in ("CANCELLED", "FILLED", "INACTIVE"):
            continue
        sym = text(cell(r, col.get("symbol"))).upper()
        sym = sym if "." in sym else sym + ".US"
        px = num(cell(r, col.get("price")))
        if px:
            out[sym] = max(px, out.get(sym, 0.0))
    return out, "broker orders read %s" % as_of.strftime("%Y-%m-%d %H:%M")


def capture_stops(cap: Optional[Dict[str, Any]], now: _dt.datetime,
                  max_age_h: float = 30.0) -> Optional[Tuple[Dict[str, float], str]]:
    """1.3.2: the broker stops from a parsed IBKR capture (scripts.ibkr_capture.parse), or None when the capture
    is missing, unusable or older than `max_age_h` (the caller then reads the workbook tabs)."""
    if not cap or not cap.get("ok") or cap.get("as_of") is None:
        return None
    age = (now - cap["as_of"]).total_seconds() / 3600.0
    if age > max_age_h:
        return None
    out: Dict[str, float] = {}
    for sym, rows in (cap.get("stops") or {}).items():
        prices = [px for px, _q in rows if px]
        if prices:
            out[text(sym).upper()] = min(prices)
    return out, "broker orders from the IBKR capture of %s Riyadh" % cap["as_of"].astimezone(RIYADH).strftime(
        "%Y-%m-%d %H:%M")


def read_plan_picks(values: Sequence[Sequence[Any]], today: Optional[_dt.date] = None) -> List[Dict[str, Any]]:
    """The latest plan's PICK rows that are still open (not EXECUTED / INVALIDATED). 1.3.2: the plan's date is the
    newest date of any of its rows, and with `today` its picks count only when that date is today."""
    h, col = table_index(values, ("Plan Date", "Kind", "Symbol", "Entry"))
    if h is None:
        return []
    every = list(values)[h + 1:]
    dates = sorted({text(cell(r, col.get("Plan Date"))) for r in every if parse_date(cell(r, col.get("Plan Date")))})
    if not dates or (today is not None and parse_date(dates[-1]) != today):
        return []
    rows = [r for r in every if text(cell(r, col.get("Kind"))) == "PICK"]
    out = []
    for r in rows:
        if text(cell(r, col.get("Plan Date"))) != dates[-1]:
            continue
        if text(cell(r, col.get("Status"))).upper() in ("EXECUTED", "INVALIDATED"):
            continue
        out.append({"date": dates[-1], "symbol": text(cell(r, col.get("Symbol"))).upper(),
                    "entry": num(cell(r, col.get("Entry"))), "qty": num(cell(r, col.get("Qty"))),
                    "stop": num(cell(r, col.get("Stop"))), "ccy": text(cell(r, col.get("Ccy"))).upper()})
    return out


def tp1_flag(r: Dict[str, Any]) -> str:
    """'yes' once TAKE PROFIT was issued for the lot (on this run or an earlier one): it is issued once. A touch
    that the price has already left does not count, so the action still comes if the price climbs back."""
    return "yes" if r.get("action") == "TAKE PROFIT" or text((r.get("prev") or {}).get("TP1 Reached")).lower() == \
        "yes" else "no"


def parse_ledger(values: Sequence[Sequence[Any]]) -> List[Dict[str, Any]]:
    """1.8.0: the 20-column ledger, a 1.7.x one (19 columns, no Stop Without News), a 1.6.x one (18, no Judgment)
    or a 1.5.0 one (17, no Market): a column the ledger does not have reads as ''."""
    rows = list(values)
    if not rows:
        return []
    head = [text(c) for c in rows[0]]
    if head[:len(LEDGER_HEADERS)] == LEDGER_HEADERS:
        names = LEDGER_HEADERS
    elif head[:len(LEDGER_HEADERS_V17)] == LEDGER_HEADERS_V17:
        names = LEDGER_HEADERS_V17
    elif head[:len(LEDGER_HEADERS_V16)] == LEDGER_HEADERS_V16:
        names = LEDGER_HEADERS_V16
    elif head[:len(LEDGER_HEADERS_V15)] == LEDGER_HEADERS_V15:
        names = LEDGER_HEADERS_V15
    else:
        return []
    out = []
    for r in rows[1:]:
        if not any(text(c) for c in r):
            continue
        d = {h: (r[i] if i < len(r) else "") for i, h in enumerate(names)}
        for h in LEDGER_HEADERS[len(names):]:
            d[h] = ""
        out.append(d)
    return out


def lot_key(symbol: str, lot: Dict[str, Any]) -> str:
    return "%s|%s|%.4f" % (symbol, lot["date"].isoformat(), lot["buy"])


def prior_row(ledger: Sequence[Dict[str, Any]], key: str, today: _dt.date,
              bars_to: Optional[_dt.date] = None) -> Dict[str, Any]:
    """The lot's last ledger row up to today (a same-day re-run keeps what was already published), carrying the
    highest stop ever recommended for it. StopNorm = that stop divided by the dividend basis it was computed on,
    so a later dividend adjustment can rebase it.
    1.1.0: `bars_to` = the last completed close of this run. 'Was'/'WasNorm' = the highest stop recorded on an
    earlier close (the RAISE STOP of this close compares with it, on every run of the close); 'TP1 Today' = TAKE
    PROFIT was issued on this close; 'TP1 Before' = it was issued on an earlier one. Without `bars_to` the
    calendar day of the run stands in for the close."""
    rows = [d for d in ledger if text(d["Lot"]) == key and (parse_date(d["Date"]) or today) <= today]
    if not rows:
        return {}

    def close_of(d: Dict[str, Any]) -> Optional[_dt.date]:
        return parse_date(d.get("Bars To")) if bars_to is not None else (parse_date(d["Date"]) or today)
    mark = bars_to if bars_to is not None else today
    same = [d for d in rows if close_of(d) == mark]
    older = [d for d in rows if (close_of(d) or _dt.date.min) < mark]

    def high(rs: Sequence[Dict[str, Any]]) -> Tuple[Optional[float], Optional[float]]:
        st = [(num(d["Stop"]), num(d.get("Basis")) or 1.0) for d in rs if num(d["Stop"]) is not None]
        return (max(a for a, _ in st), max(a / b for a, b in st)) if st else (None, None)
    last = dict(rows[-1])
    stop, norm = high(rows)
    if stop is not None:
        last["Stop"], last["StopNorm"] = stop, norm
    # 1.8.0 [NEWS-SHADOW]: the highest stop without the news; a row written before 1.8.0 lends its own Stop
    nn = [(num(d.get("Stop Without News")) if num(d.get("Stop Without News")) is not None else num(d["Stop"]),
           num(d.get("Basis")) or 1.0) for d in rows]
    nn = [(a, b) for a, b in nn if a is not None]
    if nn:
        last["StopNN"], last["StopNNNorm"] = max(a for a, _ in nn), max(a / b for a, b in nn)
    last["Was"], last["WasNorm"] = high(older)
    if any(text(d["TP1 Reached"]).lower() == "yes" for d in rows):
        last["TP1 Reached"] = "yes"
    issued = [d for d in same if text(d.get("Action")).upper() == "TAKE PROFIT"]
    last["TP1 Today"] = "yes" if issued else "no"
    last["TP1 Before"] = "yes" if any(text(d["TP1 Reached"]).lower() == "yes" for d in rows
                                      if not any(d is i for i in issued)) else "no"
    return last


# ----------------------------------------------------------------------------- #
# Output
# ----------------------------------------------------------------------------- #
ORDER = {"SELL": 0, "TAKE PROFIT": 1, "RAISE STOP": 2, "REVIEW": 3, "CHECK": 4, "HOLD": 5, "NO RULE": 6}


def market_turn_rule(k: Dict[str, Any]) -> str:
    """1.6.0: the page's words for the market-turn rule ('' while it is off)."""
    if k.get("market_turn", "off") == "off":
        return ""
    labels = ", ".join(w for w in MARKET_TURN_HOLDABLE if w in k["market_turn_labels"]) or "none"
    return (" Market turn (%s): a holding whose market is not calm (%s) %s." % (
        k["market_turn"], labels, "is a SELL" if k["market_turn"] == "on" else "is flagged; its action does not change"))


def offer_rule(k: Dict[str, Any], results: Sequence[Dict[str, Any]]) -> str:
    """1.7.0: the page's words for the offer rule ('' while it is off or no holding has an offer)."""
    if not k.get("offers") or not any(r.get("offer") for r in results):
        return ""
    return (" Cash offer: a holding under a standing cash offer whose sale is manual has no stop in force - its stop "
            "of record is frozen, its target is the offer price, and it reads REVIEW when the price doubts the "
            "offer, reaches it, or %d days or fewer remain." % k["offer_warn_days"])


def stop_judgment_rule(k: Dict[str, Any]) -> str:
    """1.7.0: the page's words for the stop judgment ('' while it is off)."""
    if k.get("stop_judgment", "off") == "off":
        return ""
    return (" Stop judgment, under test: a fall of %g%% or more in %d sessions is named with its market or on its "
            "own; it is a note and changes no action." % (k["sj_drop"], k["sj_window"]))


def page_rows(results: List[Dict[str, Any]], picks: List[Dict[str, Any]], info: Dict[str, Any],
              k: Dict[str, Any]) -> List[List[Any]]:
    w = PAGE_WIDTH
    rows: List[List[Any]] = [
        ["EXIT PLAN - stop and target per holding, reviewed after every close"],
        [info["headline"]],
        ["Rule: initial stop %gxATR at the purchase, kept between %g%% and %g%% of the entry (Tadawul at most %g%%); "
         "after +1R the stop is at least the entry and trails %gxATR under the highest close (%gxATR on negative "
         "news, a falling forecast or a lost 20-day average after +2R); the stop never moves down; target = the "
         "next +1R step (sell half at the first)." % (k["atr_init"], k["min_pct"], k["max_pct"], k["sr_max_pct"],
                                                      k["trail"], k["trail_tight"])
         + market_turn_rule(k) + offer_rule(k, results) + stop_judgment_rule(k)],
        [info["summary"]],
        [],
        ["HOLDINGS (%d)" % len(results)],
        ["Symbol", "Name", "Qty", "Entry", "Last close", "Live", "P/L %", "Highest close", "ATR %",
         "Gain at high (R)", "Stop", "Was", "Target", "Action", "Why", "Broker stop"],
    ]
    for r in sorted(results, key=lambda x: (ORDER.get(x["action"], 9), x["symbol"])):
        if not r.get("ok"):
            rows.append([r["symbol"], r.get("name", ""), fmt(r.get("shares"), 0), fmt(r.get("entry")), "", "", "", "",
                         "", "", "", "", "", r["action"], "; ".join(r["why"]), ""])
            continue
        rows.append([r["symbol"], r.get("name", ""), fmt(r.get("shares"), 0), fmt(r["entry"]), fmt(r["last"]),
                     fmt(r.get("live")) if r.get("live") is not None else "—", "%+.1f" % r["pl_pct"] if r.get("pl_pct") is not None else "—", fmt(r["hc"]), "%.1f" % r["atr_pct"], "%+.1f" % r["r_high"],
                     fmt(r["stop"]), fmt(r["prev_stop"]) if r["prev_stop"] is not None else "—",
                     fmt(r["target"]), r["action"], "; ".join(r["why"]),
                     fmt(r.get("broker_stop")) if r.get("broker_stop") is not None else "unknown"])
    rows += [[], ["TODAY'S PLAN PICKS - per-stock levels (%d)" % len(picks)],
             ["Symbol", "Entry", "Plan stop", "Per-stock stop", "Stop %", "First target", "Target %", "ATR %",
              "Plan qty", "Risk at plan qty", "Note"]]
    for p in picks:
        lv = p.get("levels") or {}
        if not lv.get("ok"):
            rows.append([p["symbol"], fmt(p.get("entry")), fmt(p.get("stop")), "", "", "", "", "", fmt(p.get("qty"), 0),
                         "", "no daily price history"])
            continue
        risk = (p["entry"] - lv["stop"]) * (p.get("qty") or 0.0)
        rows.append([p["symbol"], fmt(p["entry"]), fmt(p.get("stop")), fmt(lv["stop"]), "-%.1f" % lv["dist_pct"],
                     fmt(lv["target"]), "+%.1f" % lv["dist_pct"], "%.1f" % lv["atr_pct"], fmt(p.get("qty"), 0),
                     "%s %s" % (fmt(risk, 0), p.get("ccy", "")), "sell half at the first target"])
    rows += [[], ["Advisory only: nothing here places, changes or cancels an order. Evidence: project doc "
                  "TFB_Stop_Loss_and_Take_Profit_Review_2026-11-11. exit_plan %s" % __version__]]
    return [(list(r) + [""] * w)[:w] for r in rows]


def mail_bodies(results: List[Dict[str, Any]], picks: List[Dict[str, Any]], info: Dict[str, Any]
                ) -> Tuple[str, str, str]:
    acts = [r for r in results if r["action"] in ("SELL", "TAKE PROFIT", "RAISE STOP", "REVIEW", "CHECK")]
    subject = "TFB Exit Plan %s - %s" % (info["date"], ("%d action%s: " % (len(acts), "" if len(acts) == 1 else "s")
                                                       + ", ".join("%s %s" % (r["symbol"], r["action"].lower())
                                                                   for r in sorted(acts, key=lambda x: ORDER.get(
                                                                       x["action"], 9)))) if acts else "hold all")
    lines = [info["headline"], info["summary"], ""]
    trs = []
    for r in sorted(results, key=lambda x: (ORDER.get(x["action"], 9), x["symbol"])):
        stop = fmt(r.get("stop")) if r.get("ok") else ""
        tgt = fmt(r.get("target")) if r.get("ok") else ""
        last = fmt(r.get("last")) if r.get("ok") else ""
        lines.append("%-8s %-11s last %-9s stop %-9s target %-9s %s" % (r["symbol"], r["action"], last, stop, tgt,
                                                                         "; ".join(r["why"])))
        color = {"SELL": "#b42318", "TAKE PROFIT": "#027a48", "RAISE STOP": "#b54708"}.get(r["action"], "#344054")
        trs.append("<tr><td><b>%s</b></td><td style='color:%s'><b>%s</b></td><td>%s</td><td>%s</td><td>%s</td>"
                   "<td>%s</td></tr>" % (html.escape(r["symbol"]), color, html.escape(r["action"]), last, stop, tgt,
                                         html.escape("; ".join(r["why"]))))
    ptrs = []
    for p in picks:
        lv = p.get("levels") or {}
        if lv.get("ok"):
            lines.append("PICK %-8s entry %s stop %s (-%.1f%%) first target %s" % (
                p["symbol"], fmt(p["entry"]), fmt(lv["stop"]), lv["dist_pct"], fmt(lv["target"])))
            ptrs.append("<tr><td><b>%s</b></td><td>%s</td><td>%s (-%.1f%%)</td><td>%s</td><td>%s</td></tr>" % (
                html.escape(p["symbol"]), fmt(p["entry"]), fmt(lv["stop"]), lv["dist_pct"], fmt(lv["target"]),
                fmt(p.get("stop"))))
    lines += ["", "Advisory only: no order is placed or changed."]
    th = "style='text-align:left;padding:4px 8px;border-bottom:1px solid #d0d5dd'"
    body = ("<div style='font-family:Arial,sans-serif;font-size:14px;color:#101828'><h2 style='margin:0 0 6px'>"
            "Exit Plan - %s</h2><p style='margin:0 0 4px'>%s</p><p style='margin:0 0 12px'>%s</p>"
            "<table style='border-collapse:collapse'><tr><th %s>Symbol</th><th %s>Action</th><th %s>Last</th>"
            "<th %s>Stop</th><th %s>Target</th><th %s>Why</th></tr>%s</table>" % (
                html.escape(info["date"]), html.escape(info["headline"]), html.escape(info["summary"]),
                th, th, th, th, th, th, "".join(trs)))
    if ptrs:
        body += ("<h3 style='margin:16px 0 6px'>Today's plan picks - per-stock levels</h3><table style='border-"
                 "collapse:collapse'><tr><th %s>Symbol</th><th %s>Entry</th><th %s>Stop</th><th %s>First target"
                 "</th><th %s>Plan stop</th></tr>%s</table>" % (th, th, th, th, th, "".join(ptrs)))
    body += "<p style='color:#667085;font-size:12px'>Advisory only: no order is placed or changed. exit_plan %s</p>" \
            "</div>" % __version__
    return subject, "\n".join(lines), body


# ----------------------------------------------------------------------------- #
# IO
# ----------------------------------------------------------------------------- #
def _default_opener(req: urllib.request.Request, timeout: float):  # pragma: no cover - network
    return urllib.request.urlopen(req, timeout=timeout)


def fetch_chart(ysym: str, opener: Callable[..., Any], timeout: float = 20.0) -> Dict[str, Any]:
    """The Yahoo v8 chart payload ({} when no host answers with bars; a 404 is final)."""
    for host in YAHOO_HOSTS:
        req = urllib.request.Request("https://" + host + YAHOO_PATH.format(sym=ysym),
                                     headers={"User-Agent": YAHOO_UA, "Accept": "application/json"})
        try:
            with opener(req, timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8", "replace"))
            if parse_chart(payload):
                return payload
        except Exception as exc:  # noqa: BLE001
            if getattr(exc, "code", None) == 404:
                return {}
        _SLEEP(1.0)
    return {}


def fetch_bars(ysym: str, opener: Callable[..., Any], now_ts: Optional[float] = None,
               timeout: float = 20.0) -> Tuple[List[Bar], Optional[float]]:
    """(completed bars, live price) - see split_open_session."""
    payload = fetch_chart(ysym, opener, timeout)
    bars = parse_chart(payload)
    if now_ts is None:
        return bars, None
    return split_open_session(payload, bars, now_ts)


def live_quote(symbol: str, opener: Optional[Callable[..., Any]] = None) -> Optional[Tuple[float, _dt.datetime]]:
    """(last traded price, its time UTC) from the chart's meta, for the Daily Plan's hourly status; None when absent."""
    payload = fetch_chart(yahoo_symbol(symbol), opener or _default_opener)
    try:
        meta = payload["chart"]["result"][0]["meta"]
        px, ts = float(meta["regularMarketPrice"]), int(meta["regularMarketTime"])
    except (KeyError, IndexError, TypeError, ValueError):
        return None
    if not (math.isfinite(px) and px > 0):
        return None
    return px, _dt.datetime.fromtimestamp(ts, UTC)


# 1.7.1: _News_Items is append-only (the news gate adds up to 200 rows a run at the bottom); the newest rows are
# the ones a page needs. 20,000 rows are about a month at the rate of November 2026 (650 a day) and eleven days
# at the gate's ceiling (200 rows x 9 runs a day).
NEWS_ROWS = 20000
NEWS_COLS = ("Published (UTC)", "Scope", "Event Type", "Direction", "Severity", "Cluster Grade", "Headline")


def read_newest(book, tab: str, names: Sequence[str], rows: int = NEWS_ROWS
                ) -> Tuple[List[List[Any]], Optional[Tuple[int, int]]]:
    """1.7.1: (the named columns of the NEWEST `rows` rows of an append-only tab under their header, (first,
    last) sheet row read). The header is row 1 and column A is filled on every row, as the news gate writes it. A
    range fixed from the top ("A1:M12000") stops seeing new rows the day the tab outgrows it, without an error.
    A missing tab, or one without any of the names, reads as [] / None."""
    try:
        ws = book.worksheet(tab)
    except Exception as e:  # noqa: BLE001
        if type(e).__name__ == "WorksheetNotFound":
            return [], None
        raise
    head = ws.get("A1:P1")                                 # the width the pages always read (16 columns)
    header = [text(c) for c in (head[0] if head else [])]
    have = [n for n in names if n in header]
    if not have:
        return [], None
    last = len(ws.batch_get(["A1:A"])[0])                 # column A = the row's id: its length is the last row
    if last < 2:
        return [list(have)], None
    first = max(2, last - int(rows) + 1)
    letters = [_col(header.index(n) + 1) for n in have]
    cols = ws.batch_get(["%s%d:%s" % (c, first, c) for c in letters])
    height = max((len(c) for c in cols), default=0)
    grid = [list(have)] + [[(c[i][0] if i < len(c) and c[i] else "") for c in cols] for i in range(height)]
    return grid, (first, first + height - 1) if height else None


def read_columns(book, tab: str, names: Sequence[str]) -> List[List[Any]]:
    """Only the named columns of a page (one header read, one batch read)."""
    ws = book.worksheet(tab)
    head = ws.get("A1:EZ8")
    h, col = table_index(head, ("Symbol",))
    if h is None:
        return []
    have = [n for n in names if n in col]
    rel = next((n for n in col if n.startswith("Forecast Reliability")), None)
    if rel and rel not in have:
        have.append(rel)
    ranges = ["%s%d:%s" % (_col(col[n] + 1), h + 1, _col(col[n] + 1)) for n in have]
    cols = ws.batch_get(ranges)
    height = max((len(c) for c in cols), default=0)
    return [[(c[i][0] if i < len(c) and c[i] else "") for c in cols] for i in range(height)]


def _mail(subject: str, text_body: str, html_body: str) -> None:  # pragma: no cover - network
    from scripts.run_daily_brief import _mail_config, send_email  # noqa: PLC0415
    send_email(html_body, text_body, subject, _mail_config())


def run(dry_run: bool, send: bool, book=None, now: Optional[_dt.datetime] = None,
        opener: Optional[Callable[..., Any]] = None, mailer: Optional[Callable[[str, str, str], None]] = None,
        capture: Optional[Dict[str, Any]] = None, market: Optional[Dict[str, Dict[str, Any]]] = None,
        offers: Optional[Sequence[Dict[str, Any]]] = None,
        market_closes: Optional[Dict[str, Sequence[Tuple[_dt.date, float]]]] = None) -> int:
    """`capture` (1.3.2): a parsed IBKR capture; left out, the live run (no `book` given) reads the newest one.
    `market` (1.6.0): the market labels ({market id: {"label", "day", "text"}}); left out, they are read live.
    `offers` (1.7.0): the parsed offers registry; left out, the live run reads config/corporate_offers.json.
    `market_closes` (1.7.0): {market id: [(day, close)]} for the stop judgment; left out, each market a holding
    belongs to is read live - unless `market` was given (a caller that supplies the labels supplies the closes)."""
    k = knobs()
    own_book = book is None
    write = mode() == "on" and not dry_run
    now = now or _dt.datetime.now(UTC)
    today = now.astimezone(RIYADH).date()
    stamp = riyadh_str(now)
    opener = opener or _default_opener
    if book is None:
        from scripts.run_calendar_sync import _open_book  # noqa: PLC0415
        try:
            from scripts import sheets_quota  # noqa: PLC0415
            book = sheets_quota.call(_open_book, _SLEEP, _out, "opening the workbook")   # 1.6.3
        except Exception as e:  # noqa: BLE001
            _out("ERROR: cannot open workbook: %s%s" % (type(e).__name__, _status_of(e)))
            return 2
    for attempt in range(1, READ_ATTEMPTS + 1):
        try:
            lots = read_lots(_read(book, "_Portfolio_CostBasis", "A1:Z600"))
            grids = []
            for tab in ("My_Portfolio", "Global_Markets", "Market_Leaders"):
                try:
                    grids.append(read_columns(book, tab, INFO_COLS))
                except Exception as e:  # noqa: BLE001
                    if type(e).__name__ != "WorksheetNotFound":
                        raise
            cal = read_calendar(_read(book, "Calendar_Events", "A1:J4000"), _read(book, "Dividend_Events", "A1:J4000"),
                                today)
            taxes = read_dividend_tax(_read(book, "_Dividend_Log", "A1:J3000")) if k["div_net"] else {}   # 1.4.0
            news_grid, news_span = read_newest(book, "_News_Items", NEWS_COLS)        # 1.7.1: the newest rows
            news = read_news(news_grid, now, k["news_days"])
            stops, broker_note = read_broker_stops(_read(book, "_IBKR_Account", "A1:B40"),
                                                   _read(book, "_IBKR_Orders", "A1:H200"), now)
            picks = read_plan_picks(_read(book, "_Daily_Plan", "A1:AB2001"), today)
            ledger = parse_ledger(_read(book, LEDGER_TAB, "A1:%s%d" % (_col(len(LEDGER_HEADERS)), MAX_LEDGER_ROWS + 1)))
            break
        except Exception as e:  # noqa: BLE001
            if type(e).__name__ == "WorksheetNotFound" or attempt == READ_ATTEMPTS:
                _out("ERROR: cannot read the workbook: %s; nothing written" % type(e).__name__)
                return 2
            from scripts.sheets_quota import wait_for  # noqa: PLC0415
            wait = wait_for(e, attempt) or READ_RETRY_S             # 1.3.1: a quota answer waits into a new minute
            _out("read failed (%s) - attempt %d of %d, retrying in %d s" % (type(e).__name__, attempt,
                                                                          READ_ATTEMPTS, wait))
            _SLEEP(wait)
    if news_span:                                                 # 1.7.1: row numbers only - no name, no headline
        _out("news: rows %d-%d of _News_Items read (the newest %d)" % (news_span[0], news_span[1],
                                                                      news_span[1] - news_span[0] + 1))
    if capture is None and own_book:                              # 1.3.2: the broker's own working stops
        from scripts import ibkr_capture  # noqa: PLC0415
        capture = ibkr_capture.latest(now, out=_out)
    from_capture = capture_stops(capture, now, k["ibkr_max_h"])
    if from_capture is not None:
        stops, broker_note = from_capture
    elif capture:
        _out("IBKR capture not used for stops (%s); the workbook's broker tabs decide" % (
            capture.get("reason") or "older than %.0f h" % k["ibkr_max_h"]))
    info_map = read_info(grids)
    labels: Dict[str, Dict[str, Any]] = {}
    if k["market_turn"] != "off":                                                  # 1.6.0 [MARKET-TURN]
        labels = market if market is not None else read_market(now, opener)
    from core import corporate_offers as _co  # noqa: PLC0415 - stdlib only
    if not k["offers"]:                                                            # 1.7.0 [OFFER]
        offers = []
    elif offers is None:
        offers = _co.load() if own_book else []          # as with `capture`: only the live run reads the registry
    live_closes = market_closes is None and market is None                         # 1.7.0 [STOP-JUDGMENT]
    closes_cache: Dict[str, Sequence[Tuple[_dt.date, float]]] = dict(market_closes or {})

    def closes_of(mid: str) -> Sequence[Tuple[_dt.date, float]]:
        if mid and mid not in closes_cache:
            closes_cache[mid] = read_market_closes(mid, now, opener) if live_closes else []
        return closes_cache.get(mid) or []
    bars_cache: Dict[str, Tuple[List[Bar], Optional[float]]] = {}
    now_ts = now.timestamp()

    def fetched(sym: str) -> Tuple[List[Bar], Optional[float]]:
        if sym not in bars_cache:
            bars_cache[sym] = fetch_bars(yahoo_symbol(sym), opener, now_ts)
        return bars_cache[sym]

    def bars_for(sym: str) -> List[Bar]:
        return fetched(sym)[0]

    results: List[Dict[str, Any]] = []
    for sym, lot in sorted(lots.items()):
        inf = dict(info_map.get(sym) or {})
        if any(c in inf.get("asset", "").lower() for c in NO_STOP_CLASSES):
            results.append({"symbol": sym, "name": inf.get("name") or lot["name"], "shares": lot["shares"],
                            "entry": lot["entry"], "ok": False, "action": "NO RULE",
                            "why": ["%s: no stop rule for fixed income" % inf["asset"]]})
            continue
        inf["news_neg"] = (news.get(sym) or {}).get("n", 0)
        inf["earn_days"] = (cal.get(sym) or {}).get("earn_days")
        inf["broker_stop"] = stops.get(sym)
        inf["broker_stop_floor"] = stops.get(sym) if from_capture is not None else None   # 1.5.0: capture only
        inf["live"] = fetched(sym)[1]
        if offers:                                                                     # 1.7.0 [OFFER]
            inf["offer"] = _co.standing(offers, sym, today)
        sym_bars = bars_for(sym)
        if taxes.get(sym):                                                             # 1.4.0 [DIV-NET]
            sym_bars, inf["div_tax_pct"] = net_of_tax(sym_bars, taxes[sym], lot["date"])
        prev = prior_row(ledger, lot_key(sym, lot), today, sym_bars[-1][0] if sym_bars else None)
        res = exit_levels(sym, lot["entry"], lot["date"], sym_bars, inf, prev, k, today)
        res.update({"name": inf.get("name") or lot["name"], "shares": lot["shares"], "lot": lot_key(sym, lot),
                    "broker_stop": stops.get(sym), "prev": prev})
        if labels:                                                                     # 1.6.0 [MARKET-TURN]
            apply_market_turn(res, market_turn(sym, inf.get("asset", ""), labels, k), k)
        if res.get("offer_held"):                                                      # 1.7.0 [OFFER]
            res["judgment"] = "offer:manual"
        elif k["stop_judgment"] != "off" and res.get("ok") and not any(
                tok in inf.get("asset", "").lower() for tok in MARKET_TURN_EXEMPT):     # 1.7.0 [STOP-JUDGMENT]
            mid = market_of(sym)
            sj = stop_judgment(sym_bars, closes_of(mid), res, k, market_reading_name(mid)) if mid else None
            if sj:
                res["judgment"], res["sj"] = sj["code"], sj
                if sj["show"]:
                    res["why"].append(sj["text"])
        if offers and not inf.get("offer"):                                            # 1.7.0: a passed last day
            gone = _co.lapsed(offers, sym, today)
            if gone:
                res["why"].append("the cash offer at %s on it had its last day on %s: record its outcome in "
                                  "config/corporate_offers.json; the price rule is in force" % (
                                      fmt(gone["price"]), gone["expires"].isoformat()))
        if (news.get(sym) or {}).get("headline"):
            res["why"].append("latest negative item: %s" % news[sym]["headline"])
        if (news.get(sym) or {}).get("ignored"):                                       # 1.3.0 [NEWS-NOISE]
            res["why"].append("%d news item(s) ignored: law-firm notice or unconfirmed" % news[sym]["ignored"])
        if lot.get("lots", 1) > 1:                                                     # 1.3.0: the merged basis, stated
            res["why"].append("entry is the weighted price of %d buys; days are counted from the latest buy"
                              % lot["lots"])
        note = dividend_note(sym, lot["shares"], res.get("last"), cal.get(sym) or {}, k)   # 1.2.0 [DIV-TAX]
        if note:
            res["why"].append(note)
        results.append(res)
    for p in picks:
        p["levels"] = pick_levels(p["symbol"], p["entry"] or 0.0, bars_for(p["symbol"]), k) if p.get("entry") else {}
    counts: Dict[str, int] = {}
    for r in results:
        counts[r["action"]] = counts.get(r["action"], 0) + 1
    bars_to = max((r["bars_to"] for r in results if r.get("bars_to")), default=None)
    info = {"date": today.isoformat(),
            "headline": "As of %s Riyadh · daily bars to %s · %s" % (
                stamp, bars_to.isoformat() if bars_to else "n/a", broker_note),
            "summary": "Today: " + (" · ".join("%s %d" % (a, counts[a]) for a in sorted(counts, key=lambda x: ORDER.get(
                x, 9))) or "no holdings")}
    turned = sum(1 for r in results if r.get("market_turned"))
    if turned and k["market_turn"] == "observe":                                       # 1.6.0 [MARKET-TURN]
        info["summary"] += " · market not calm: %d holding%s (observe)" % (turned, "" if turned == 1 else "s")
    n_offer = sum(1 for r in results if r.get("offer_held"))                           # 1.7.0 [OFFER]
    if n_offer:
        info["summary"] += " · under a cash offer, manual sale: %d" % n_offer
    waits = sum(1 for r in results if (r.get("sj") or {}).get("verdict") == "wait")    # 1.7.0 [STOP-JUDGMENT]
    if waits:
        info["summary"] += " · stop judgment: %d would wait (a note under test)" % waits
    detail = log_detail()                                                              # 1.6.1 [LOG-PRIVACY]
    for i, r in enumerate(results, 1):
        if detail:
            _out("%-9s %-11s last %s stop %s (was %s) target %s | %s" % (
                r["symbol"], r["action"], fmt(r.get("last")), fmt(r.get("stop")), fmt(r.get("prev_stop")),
                fmt(r.get("target")), "; ".join(r["why"])))
        else:
            _out("holding %d of %d: %s%s%s" % (i, len(results), r["action"],
                                               " (market not calm)" if r.get("market_turned") else "",
                                               " (cash offer)" if r.get("offer_held") else ""))
    for i, p in enumerate(picks, 1):
        lv = p.get("levels") or {}
        if detail:
            _out("PICK %-8s entry %s plan stop %s per-stock stop %s target %s" % (
                p["symbol"], fmt(p.get("entry")), fmt(p.get("stop")), fmt(lv.get("stop")), fmt(lv.get("target"))))
        else:
            _out("plan pick %d of %d: levels %s" % (i, len(picks), "computed" if lv.get("ok") else "not computed"))
    page = page_rows(results, picks, info, k)
    subject, text_body, html_body = mail_bodies(results, picks, info)
    n_act = sum(1 for r in results if r["action"] in ("SELL", "TAKE PROFIT", "RAISE STOP", "REVIEW", "CHECK"))
    log_subject = subject if detail else "TFB Exit Plan %s - %d action%s" % (info["date"], n_act,
                                                                           "" if n_act == 1 else "s")
    if not write:
        _out("%s - nothing written, nothing sent | %s" % ("dry run" if dry_run else "observe", log_subject))
        return 0
    day = today.isoformat()
    keep = [d for d in ledger if text(d["Date"]) != day]
    new = [[day, stamp, r["symbol"], r.get("lot", ""), _round(r.get("entry")), r.get("shares"), _round(r.get("last")),
            _round(r.get("hc")), _round(r.get("atr")), _round(r.get("r")), _round(r.get("stop")),
            _round(r.get("target")), tp1_flag(r), r["action"], "; ".join(r.get("tighten") or []),
            r["bars_to"].isoformat() if r.get("bars_to") else "", _round(r.get("basis")), r.get("market", ""),
            r.get("judgment", ""), _round(r.get("stop_no_news"))]
           for r in results if r.get("ok")]
    tight = [r for r in results if r.get("ok") and r.get("news_tightened")]
    _out("news shadow (1.8.0): %d of %d lot(s) tightened by negative news; %d of them with a stop above the stop "
         "without the news" % (len(tight), sum(1 for r in results if r.get("ok")),
                               sum(1 for r in tight if (r.get("stop") or 0) > (r.get("stop_no_news") or 0) * 1.0005)))
    grid = [list(LEDGER_HEADERS)] + ([[d.get(h, "") for h in LEDGER_HEADERS] for d in keep] + new)[-MAX_LEDGER_ROWS:]
    from scripts import sheets_quota  # noqa: PLC0415

    def publish() -> None:                     # both writes overwrite their whole range: safe to repeat (1.6.2)
        write_grid(book, LEDGER_TAB, grid, len(LEDGER_HEADERS), 200)
        write_grid(book, PAGE_TAB, page, PAGE_WIDTH, 60)
    try:
        sheets_quota.call(publish, _SLEEP, _out, "writing the Exit Plan")
    except Exception as e:  # noqa: BLE001
        _out("ERROR: publication unconfirmed: %s; read back before retrying" % type(e).__name__)
        return 3
    _out("wrote %d ledger row(s) for %s and the %s page" % (len(new), day, PAGE_TAB))
    if send and k["mail"]:
        try:
            (mailer or _mail)(subject, text_body, html_body)
            _out("e-mail sent: %s" % log_subject)
        except Exception as e:  # noqa: BLE001
            _out("ERROR: e-mail not sent: %s" % type(e).__name__)
            return 4
    elif send:
        _out("e-mail skipped: TFB_EXIT_MAIL is not 1")
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Review the stop and the target of every holding.")
    ap.add_argument("--dry-run", action="store_true", help="compute and print, write and send nothing")
    ap.add_argument("--send", action="store_true", help="send the e-mail (needs TFB_EXIT_MAIL=1)")
    args = ap.parse_args(list(argv) if argv is not None else None)
    if mode() == "off":
        _out("TFB_EXIT_PLAN=off - nothing to do")
        return 0
    return run(args.dry_run, args.send)


if __name__ == "__main__":
    sys.exit(main())
