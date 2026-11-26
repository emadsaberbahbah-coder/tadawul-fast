# TFB Commit Sheet — 32_Risk_Watch.gs 1.0.0 (an e-mail within minutes when a holding nears its stop or its target, falls sharply, or sits exposed at the open)

Date: 2026-11-26 (Thursday) · Lane: Apps Script, a new file · Gate: none changed — it watches and e-mails · No Render deploy · It places, changes and cancels no order and calls no broker

## Why
Owner, 26 Nov 2026 09:36 Riyadh (in Arabic): is the system studying and adjusting take-profit and stop-loss more precisely, faster and directly, so that what happened yesterday is not repeated — and can an e-mail or alerts be sent at once when that risk exists?

## What happened (IBKR, read 26 Nov)
| | NMM | IBOC |
|---|---|---|
| Sold by its stop order | 20 Nov, 17:30:01 Riyadh | 25 Nov, 17:30:00 Riyadh |
| Stop (plain stop, GTC) | 81.22 | 62.67 |
| Close of the session before | 82.32 (stop 1.3 % under it) | 63.77 (stop 1.7 % under it) |
| After | closed 81.07; 84.23 two sessions later | closed 63.56 |

Both sales happened in the **first second** of the session. For IBOC the broker's own 5-minute chart shows no trade at or below 62.67 that day (first bar low 63.06, day low 62.75 at 17:45); the fill printed at 62.67 on the broker's internal venue. Which quote triggered the order is in the order's own settings, readable only at the broker.

What the system did before these opens:
- **Exit Plan** — recomputes each holding's stop and target after every close and shows them hourly. It reads the close; its one e-mail goes out at 07:30.
- **Stop judgment** (since 26 Nov, observe) — records whether a fall is with the market or the share's own, and what followed. It controls nothing for ten sessions (owner's rule of 21 Nov).
- **Nothing** told the owner that a stop sat inside one ordinary day's range of the last close, and nothing watched a price between two closes.

## Change — `apps_script/32_Risk_Watch.gs` 1.0.0 (new)
`rwTick`, one time trigger every 5 minutes, works on New York weekdays from 75 minutes before the open to 10 minutes after the close and returns at once at any other time. Each run reads the holdings and their levels from the Exit Plan's own page and ledger, one Yahoo chart per US holding and one for the S&P 500 fund, and e-mails the owner **once per holding, rule and day**:

| When | Code | Rule |
|---|---|---|
| Before the open | `OPEN_RISK` | the stop working at the broker is within max(2.5 %, 1 ATR) of the last close — inside one ordinary day's range; with the pre-market trade when there is one |
| | `PRE_BELOW` | a pre-market trade of this morning at or below that stop |
| In the session | `NEAR` | the price is within 1 % above the stop (the broker's when known, else the Exit Plan's) |
| | `CROSSED` | the price is at or below it |
| | `TARGET` | the target of record is reached (the first, +1R, or the next) |
| | `DROP` | the share is down 4 % or more on the day, again at 8 %, 12 % …, with the market's own move beside it |
| | `MARKET` | the S&P 500 fund is down 2 % or more, again at 3 %, 4 % … |
| A cash offer | `OFFER_AWAY` | a holding kept under an offer trades 8 % or more under the offer price, or falls 3 % on the day (the Exit Plan's own offer limits) |
| | `OFFER_AT` | it trades within 0.5 % of the offer price |
| Its own health | `PRICES` | three runs in a row without the market's price |
| | `NO_PRICE` | three runs in a row without **one holding's** price while the market's reads: that holding is not watched until it reads, and the e-mail names it |
| | `LEVELS` | the Exit Plan page is missing, unreadable, read in part (fewer rows than its own HOLDINGS count) or older than 5 days |

**On the two mornings above** it would have sent `OPEN_RISK` at about 16:15 Riyadh, 75 minutes before each open (the harness runs the IBOC morning with its real figures).

### A trading day
- **In the session** it is proven by data: the fund must have traded within 10 minutes, a share's own price must be at most 20 minutes old. On a holiday, or after an early close, nothing is fresh and nothing is sent.
- **Before the open** no trade can prove it yet, so the day counts unless it is a listed New York holiday (`RW_HOLIDAYS_`: the dates of `core/analysis/portfolio_actions.py` and 2027; more in `TFB_RISK_WATCH_HOLIDAYS`). A date missing there costs one early e-mail on a closed day, never a missed one.
- `OPEN_RISK` needs **no price feed**: the last close and the broker's stop are on the Exit Plan page. Yahoo adds only the pre-market trade.

### What it reads, and how that is held
| It reads | Written by | Pinned by |
|---|---|---|
| `Exit_Plan` page, every row: Qty, Entry, Last close, ATR %, Stop, Target, Action, Why, Broker stop; the "As of" time; the broker note; the HOLDINGS count | `scripts/exit_plan.py` `page_rows` | a test builds the page with the real `page_rows` and reads it with the watch's own reader (also a 71-holding page) |
| **Whether a holding is kept under a cash offer**: the page's own Why ("the sale is the owner's manual decision"), written beside the stop | `scripts/exit_plan.py` `offer_action` | a test writes the Why with the real `offer_action` and reads the offer with no ledger at all |
| `_Exit_Plan` ledger, newest day: TP1 Reached; Judgment (`offer:manual`) as a second witness | `scripts/exit_plan.py` | the same test, with `LEDGER_HEADERS` |

Without the ledger the watch loses one thing only: it cannot tell a first target from a later one, says "the target of record", and the `Risk_Watch` tab's first line says "ledger not read".
| Yahoo v8 chart, 5-minute bars with the pre-market | — | parser held to `scripts/pre_open_check.py` `parse_prepost` (only this morning's bars) and to the provider's previous-close order |

### What it writes
`Risk_Watch` (its own tab: every holding, its price, the stop used, the distance to the stop and the target, the state, today's alerts) and `_Risk_Watch_Log` (one row per alert). Nothing else: no decision page, no gate, no broker call.

### What it cannot do
- A stop that sells in the first second of the open is faster than any watch. `OPEN_RISK` is the warning **before** that open; the order's own trigger settings are the owner's, at the broker.
- It does not know a fill. The broker's own trade notification is the instant one; the next IBKR capture updates the pages.
- Listings outside the US are shown on the tab as not watched (Yahoo's Saudi quotes are delayed 15 minutes).
- Prices come from Yahoo. If Yahoo does not answer, `PRICES` says so after three runs; the before-the-open rule still works.

## Settings (Script Properties)
| Property | Meaning |
|---|---|
| `TFB_RISK_WATCH` | `off` (default: one property read and return) · `observe` (rules run, tab and log written, no e-mail) · `on`. The install sets `on` when it is not set. |
| `TFB_RISK_WATCH_TO` | recipient; default `DP2_EXIT_ALERT_TO`, else the script owner |
| `TFB_RISK_WATCH_CFG` | optional JSON of limits, e.g. `{"near_pct":1.5,"drop_pct":5}` |
| `TFB_RISK_WATCH_HOLIDAYS` | extra closed days, comma list |
| `TFB_RISK_WATCH_EVERY_MIN` | 1, 5, 10, 15 or 30 (default 5), read at install |

At most 12 e-mails a day; the twelfth says so, and later alerts go to the tab and the log.

**No new authorization.** The watch uses only Google services the bound project already uses (MailApp and UrlFetchApp in 27_Decision_Pages_v2, triggers in 24 / 29, the lock in 26 / 28); a test holds that. A new service would have stopped every trigger of the project until the owner granted it.

**Cost.** About 100 runs a day inside the hours, a few seconds each (one page read, one batch of charts, one tab write); outside them a run reads one property and the clock.

## Tests
| File | What it pins |
|---|---|
| `tests/test_rw_v100_risk_watch.js` (19 groups) | the real `.gs` in a vm with a real time-zone clock: the gate; idle hours and weekends read nothing; **the IBOC morning of 25 Nov with its real figures** — one e-mail, once; a pre-market trade at or below the stop; a holiday (calendar before the open, data in the session); the rule working with Yahoo down; near then crossed, once each, a new day clean; first and next target; a sharp fall and its steps; the market's fall; a holding under an offer; an old price fires nothing; a listing outside the US; health (prices, second host, levels, an all-cash book); observe; a failed e-mail tried again and logged once; the daily cap; recipient order; the lock; owner functions; summer time; one holding without a price named once; a 70-holding book read whole and a page read in part; an offer read from the page with no ledger; eight mutants, each caught |
| `tests/test_rw_v100_risk_watch.py` (8) | runs the harness under pytest; the page built by the real `page_rows` read by the watch (four broker notes, thousands separators, an all-cash book); the offer Why written by the real `offer_action`; a 71-holding page against its own count; tab names, columns, Yahoo symbol, hosts and offer limits equal to the Exit Plan's; the holiday list holds the repository's own; no new Google service |
| `tfbRiskWatchSelfTest` (in the file) | the pure rules, runnable from the Apps Script editor |

## Review (Codex on #1088, three P2 — all taken)
| Finding | Was | Now |
|---|---|---|
| One holding's price fails while the market's reads | the failure count followed the market's price only, so that holding went unwatched with no word | a count per holding (`miss`, kept in the state); at three runs `NO_PRICE` names it, once a day; counted only while the market's price reads on a trading day; a holding that leaves the book is dropped |
| The page read stopped at 60 rows | a book of more than 53 holdings lost its tail silently | the whole page is read, and the rows read are held against the page's own `HOLDINGS (n)`: a shortfall is `LEVELS` "read in part"; the status tab grows with the book |
| The offer flag came from the ledger alone | a missing or empty ledger made a cash-offer holding a stop-watched one | the offer is read from the page's own Why (the row that carries the stop), the ledger is a second witness; a page without its Why column is `LEVELS` "header not recognised" and nothing is guessed |

## After merge — owner (or Claude in the connected browser, on the owner's word)
1. Apps Script → new file `32_Risk_Watch` → paste `apps_script/32_Risk_Watch.gs`.
2. Run `tfbRiskWatchSelfTest` → must return `risk watch core: ok`.
3. Run `tfbRiskWatchTestMail` → one test e-mail arrives.
4. Run `tfbInstallRiskWatchTrigger` → one trigger every 5 minutes, `TFB_RISK_WATCH = on`.
5. Run `tfbRiskWatchNow` → the `Risk_Watch` tab is filled. `tfbRiskWatchStatus` shows the mode, the trigger and today's alerts.

To look first without e-mail: set `TFB_RISK_WATCH = observe` before step 4.

## Rollback
`TFB_RISK_WATCH = off`, or run `tfbRemoveRiskWatchTrigger`. The two tabs can be deleted; nothing reads them.

## The owner's own decisions (not changed here)
- The trigger settings of the stop orders at the broker (what triggers them, and whether in the first seconds of the open).
- The broker's own trade notifications.

Not investment advice.
