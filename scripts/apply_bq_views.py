#!/usr/bin/env python3
# scripts/apply_bq_views.py
"""
================================================================================
TFB BigQuery Looker views - v1.27.0 (program register C8, Phase 2)
================================================================================
v1.27.0 (24 Nov 2026, the hourly news reading). WHY (owner, 24 Nov 2026
10:10 Riyadh, in Arabic: the news analysis should act on the reading of a
stock's price now, through gathering, comparing and forecasting - not only in
the 8 Dec review; 10:20: "let us start"). scripts/news_now.py 1.0.0 reads the
company news of the holdings and the Top 10 names every hour of the US
session into a new source table, symbol_news_intraday (with its own log,
news_now_log). One new view, research only: v_symbol_news_class_now (128) -
file 97's rules, unchanged, over the hourly table (the test holds that the two
files differ only in the name, the source table and the description). The two
tables are created by the job's first run; this view is applied after it. No
file of 1.26.0 changes.

v1.26.0 (24 Nov 2026, confidence calibration and the shock-drop shadow rule).
WHY (owner, 24 Nov 2026 01:07 Riyadh: "check now we are able to achieve our
target" - the INVESTABLE hit rate above 70 %; 00:58: NVDA fell 39 % on its
CEO's resignation and rose 31 % when he returned - "the stock became
attractive to buy"; 01:11 "ok, no objection"; 01:18 "Ok let's start"). The
check of that night: 47.7 % of the 2,389 scored INVESTABLE records rose, but
58.0 % beat their own market; Saudi HIGH-confidence calls rose 23.8 % of the
time against 53.8 % for Saudi MEDIUM. Three new views, research only:
v_confidence_calibration (125) - the tile's population by market, confidence
and month, the share that rose, the market's own return over the same dates
(SPY, ^TASI.SR, ACWI), the share that beat it, and whether HIGH did better
than MEDIUM; v_shock_drop_events (126) - the 100 largest US companies with
bars falling 30 % within 15 sessions, entered at the first close of the fall
(A) or after the first confirmed turn (B), scored against SPY over 5, 10 and
20 sessions; v_shock_drop_learning (127) - that rule's record with a fixed
reading (history is in-sample; forward from 24 Nov 2026). No file of 1.25.1
changes.

v1.25.1 (23 Nov 2026, the news understanding kept lean). After the 1.25.0
deploy, BigQuery refused to plan a read of v_news_understanding ("too many
subqueries or query is too complex"): it joined v_news_trend_learning and
v_projection_context, which expanded v_news_trend_daily a second time beside
the market views. v_news_understanding (124) now reads v_news_trend_daily and
v_universe_latest only; each state's record (v_news_trend_learning) and the
projection (v_projection_context) are read beside it, as the strategist's
price_drivers tool does. No other file changes.

v1.25.0 (23 Nov 2026, news and trend read together). WHY (owner, 23 Nov
2026 12:34 Riyadh): "deep understanding of information ... analysis news ...
using data and information and in same time use strategy and trend". A
headline alone carried no edge in TFB's record (v_company_news_eval) and a
price alone is a trend; research reads them together (Chan 2003: a move with
news continues, a large move without news reverses; Tetlock 2011 and
Lopez-Lira and Tang 2023: news the price has not answered drifts its way).
Three new views, research only: v_news_trend_daily (122) - per US stock TFB
follows the company news of and per session from 1 Oct 2026, the company
news score from both feeds, the price answer against SPY over two sessions
and its z, the trend before, a state fixed from that research before the
data was read, its expected direction and fit with the trend, and the
returns against SPY over the next 1 / 5 / 10 / 20 sessions; v_news_trend_
learning (123) - each state's record by period, trend fit and horizon, with
an overlap-adjusted t over dates and a fixed reading (only sessions from 24
Nov 2026 count as evidence); v_news_understanding (124) - per stock today:
the latest state, headlines, trend, the expected move and its record, TFB's
own view, the agreement between them and the projection context, in one
line. No file of 1.24.0 changes.

v1.24.0 (23 Nov 2026, the projection context). WHY (owner, 23 Nov 2026 10:03
Riyadh): "make more deep analysis ... link the last updates ... news, future
options, financial derivatives, any other related data and information that
can support the projection accuracy". The review of that day found, on the
2,569 scored INVESTABLE buy calls, that the promised return and the result
were uncorrelated (r = -0.04), that a one-sigma range from realised
volatility held about 57-59 % of the results instead of 68 %, and that the
count of adverse US market states at the call (VIX 20 or more, VIX curve
inverted, VVIX high, MOVE high, credit weakening, 10-year yield jumping)
sorted the US calls (0 -> 62 % hit, 3 or more -> 30 %; in-sample). Two new
views, research only: v_projection_context (120) - per stock the promise
against its own one-sigma move (Cboe implied volatility when read, else
realised), the one-month range, an observe-only shadow projection capped at
0.7 sigma and the adverse states now; v_projection_scorecard (121) - five
rules fixed on 23 Nov (B0 baseline, P1 at most 1 adverse state, P2 promise
within 0.7 sigma, P3 both, P4 3 or more adverse states) on the scored US
calls, in-sample and forward from 24 Nov, with a within-day lift for the
stock rule, a between-days lift for the market rules and a kept-against-
left-out lift clustered by call day for P3, every t overlap-adjusted (Codex
P2 x2 on #1064). No file of 1.23.0 changes.

v1.23.0 (22 Nov 2026, the price drivers). WHY (owner, 22 Nov 2026 19:53
Riyadh): "add how future options and other options affect the current price
and recommendations ... add this effect to our score and analysis, also check
other factors that effect current price and add them to our project". Five new
views, research only, and one new source table, options_daily (scripts/
archive_options_reading.py v1.0.0: Cboe's delayed chain and FINRA's daily
short volume, read once a night for the names TFB holds, rates and the largest
US names). v_options_reading (115): per name and day the implied volatility,
its 1-year percentile and term ratio, the implied move to the front expiry and
whether an event is priced, 25-delta put skew, call-minus-put IV, put/call
volume and open interest, short volume and its z against 20 days, and an
options tilt from the cross-section. v_market_drivers (116): the US market's
forces per day from market_series_daily - the VIX curve (VIX9D, VIX3M, VIX6M),
VVIX, SKEW, MOVE, index futures against cash, the 10y-3m curve, the dollar,
credit (HYG/IEF), breadth (RSP/SPY), small caps - each with a state, and SPY's
next 5 and 20 sessions. v_market_driver_learning (117): each driver state's
record against SPY over about three years, in-sample. v_price_drivers (118):
every stock on TFB's pages with its options reading, implied over realised
volatility, the week against its sector ETF, volume against average, analysts'
monthly change, days to earnings and the market's state, plus an observe-only
score with drivers beside TFB's overall score. v_driver_learning (119): each
stock-level driver's daily rank IC and quintile spread against the next week's
excess return, read 'too few weeks' under 8 weeks. Nothing decides, gates or
counts from them. No file of 1.22.0 changes.

v1.22.0 (22 Nov 2026, the learning scorecard). WHY (owner, 22 Nov 2026): "check
the system is getting learning day over day and how we can improve and
accelerate the learning ... how system can learn from the previous loss". The
full review of that day found that the hit rate follows the market's
condition, that stops caused most of the real loss, and that no rule is ever
re-tested by itself: every finding so far was a one-off query. One new view,
research only: v_learning_scorecard (114) - a list of ten rule variants fixed
on 22 Nov before any forward data (the armed market hold list, the same
without 'weak', without Tadawul, the selection gate, both gates, stops at 5 /
8 / 10 % on daily closes, and the sell side against the buy side), each
against its baseline on the scored calls of v_outcome_engine, the market
label of v_market_condition at the last close BEFORE the call and the signal
v_signal_history recorded on the day of the call. Per period ('in-sample,
not evidence' / 'forward from 2026-11-23'), horizon and arm: calls, call
days, hit rate, mean result, worst 5 %, the baseline's, the mean daily lift
and its t; a forward row is read only at 20 call days and 40 calls (t >= 2
BETTER, t <= -2 WORSE). A rule needs about 20 call days to be judged and that
clock cannot be shortened; ten variants now run on it at once. No file of
1.21.0 changes.

v1.21.0 (21 Nov 2026, the forward event scenarios). WHY (owner, 21 Nov 2026
11:03 Riyadh, item 5 of the build list): "News: a news reading per market;
forward event scenarios; momentum/surprise; influential figures of 19 Nov;
score every news signal"; the design of 16 Nov 2026: each scenario carries
its probability, is logged before the outcome and scored after it (Brier).
Five new views, observe only, over econ_calendar (archive_econ_calendar
1.0.0, a new source table): v_event_calendar (109) - the Fed, ECB, Bank of
Japan and OPEC+ meetings on their official dates (2023-2027) and the tracked
releases of the US, the euro area, Japan, China and Saudi Arabia, each figure
against its forecast in units of the family's earlier misses (above / in line
/ below); v_event_reactions (110) - each market's move from the close before
an event to the first close after it, at each market's own close time, over 5
closes, and after the event close, against its normal daily move;
v_event_scenarios (111) - for each event, held or ahead, what the earlier
events of its family did: the band of the event-day moves, the chance of a
big move, the median move after a figure above / below its forecast, and the
chances of the figure itself; v_event_trial (112) - each scenario against
what came ('forward' from 23 Nov 2026); v_event_trial_summary (113) - the
scorecard: the band against 80 %, the big-move chance against any day's, the
direction and the drift after a figure against a coin (each event counted
once over its markets), the figure chances of the releases against equal
thirds. A forward event is scored on the scenario logged last before it
(event_scenario_log, appended by archive_econ_calendar 1.1.0 after each
calendar run), never on one rebuilt later (Codex P2 on #1050); the backtest
is rebuilt from today's calendar. One event per release: EODHD lists some releases twice ("Oct" and
"Oct. 2026") and a first estimate with its revisions (US GDP, a flash and its
final) under the same period, so a family's same period within 120 days is
one release. First read (backtest, the history loaded on 21 Nov 2026, run
44914578389: 27,886 releases of six economies since November 2023; 544
events with 5,805 event-market rows): the band of the earlier moves held
72.9 % of 4,342 rows (a true band 80 %: a band read off 8 to 35 earlier
events is narrower than the truth); event days moved a normal day or more
25.7 % of the time against 25.2 % for any day, and the families' chance of
it did not beat any day's (skill -0.08); the direction after a figure held
52.1 % of 1,502 rows over 148 events (z 1.38, not proven), the drift after
the close 52.4 % (z 1.59); the figure chances scored -0.01 against equal
thirds over 342 figures. No family reaches 20 events scored for direction
yet. Research views: the live system never reads BigQuery.

v1.20.0 (21 Nov 2026, the news by market and by person). WHY (owner, 21 Nov
2026 11:03 Riyadh, item 5 of the build list): "News: a news reading per
market; forward event scenarios; momentum/surprise; influential figures of
19 Nov; score every news signal." Four new views, observe only, over the
three sources archive_news_themes 1.1.0 merges into its new table
news_entity_daily (kept apart from news_theme_daily, whose every feature the
news loop of 19 Nov 2026 reads): v_market_news (105) - each
news day, each market's place (the US, Saudi Arabia, the war zone around the
Gulf for gold and oil, Europe, Japan, China / Hong Kong) against its own
previous 250 news days: the share and tone of the news naming it and the
fighting located there, a 5-day score signed for the market (favourable /
neutral / unfavourable), its momentum and a surprise flag; v_people_news
(106) - the same for 17 people who move markets, with a spike flag and the
markets each is expected to move; v_news_trial (107) - each US session, each
market's reading of the day before and what its fund did over the next 5,
10 and 20 closes (v_market_trial; 'forward' from 23 Nov 2026); and
v_news_trial_summary (108) - the scorecard: each reading against its
signal's own mean, an overlap-adjusted t, and the signal's rank correlation
with the markets' next changes. First read (backtest, about 690 sessions
from Feb 2024; the windows overlap): no news reading is proven - favourable
news was followed by -0.15 / -0.40 / -0.87 pp against the signal's mean over
5 / 10 / 20 closes (t -0.77 / -1.04 / -1.51), unfavourable news by +0.01 /
+0.33 / +0.74, the rank correlation -0.02 / -0.06 / -0.11 (t -0.79 / -1.50 /
-1.86); improving news +0.18 pp over 5 closes (t 1.09); an unfavourable
surge +0.47 / +0.68 / +1.17 pp; no person's spike reached a t of 1.1 over 10
closes. If anything, bad news has been followed by rebounds; the forward
sessions decide. Research views: the live system never reads BigQuery.

v1.19.0 (21 Nov 2026, the ready list). WHY (owner, 21 Nov 2026 11:03 Riyadh,
items 3 and 4 of the build list): "A ready list: each day the 2-3 best
qualified names from the top-ranked markets, each labelled with what it is
waiting for (cash / calmer market / switch)" and "The Daily Plan's hold then
works market by market. It follows the market an asset tracks, not the
currency it trades in." Two new views, observe only: v_ready_list (103) - on
each archived day every qualified name (INVESTABLE, INVEST, the panel floors,
not held that day; the day's last copy first, then the floors) with the
market it tracks (the market funds by name, US REITs, Tadawul REITs,
Hong Kong / China, Tokyo, Europe; anything else not read yet), that market's
state that day, the ready order (open markets first, two names per market,
then the market's rank and the name's 1-month expected return; the top 3 are
the ready list), what it waits for, and the live US rule beside it;
v_ready_trial (104) - what those names did over the next week and two weeks
(v_forward_universe), by the state of their market, on and off the ready list,
and held or not by each rule. First read (names of 31 Oct - 14 Nov, 1 week,
a falling stretch; windows overlap): the names the live US rule held moved
-1.11 % against +0.69 % for the others (720 and 428 name-days); the market
rule's held names -0.84 % against +0.38 % (768 and 380); the ready list's 43
name-days -1.73 % against -0.39 % for the other qualified names - the list
is not proven. The worst 5 % is exact (PERCENTILE_CONT). The switch check of
item 4 is v_hold_vs_switch (1.9.0), shown beside them by markets_board 1.1.0.
Research views: the live system never reads BigQuery.

v1.18.0 (21 Nov 2026, a condition reading for every market). WHY (owner, 21
Nov 2026 11:03 Riyadh, the build list after the voice call): "A condition
reading for every market, with the same tests as v_market_regime ... A daily
ranking of the markets by trend and risk ... When every market is weak, cash
stays cash ... Accuracy: log and score everything. Observe for two weeks (10
sessions), then review with the owner." Four new views, observe only:
v_market_condition (99) - the five v_market_regime labels for eleven markets
(US, Saudi Arabia, Europe, Japan, China / Hong Kong, gold, oil, bonds, sukuk,
US REITs, Saudi REITs), the limits scaled to each market's normal volatility
(k = normal yearly volatility / 16; the US unchanged: its labels equal
v_market_regime's on all 755 sessions of the archive), what each is waiting
for, and a trend score; v_market_rank (100) - each calendar day, each
market's latest condition, open (calm) or hold, the day's order and the
verdict (ALL ON HOLD - cash stays cash when none is open); v_market_trial
(101) - each US session, each market's state and what its fund did over the
next 5 and 10 closes from the first close after the day ('forward' from 23
Nov 2026, 'backtest' before it); v_market_trial_summary (102) - the scorecard
the 10-session review reads. File numbers may now have three digits
(NNN_<object>.sql) and the files are applied in number order (99 before 100);
the two-digit files keep their order. Research views: the live system never
reads BigQuery; nothing in a score, a pick, a gate or an order changes.

v1.17.0 (21 Nov 2026, the company news against its peers). WHY (owner, 21 Nov
2026 02:20 Riyadh, in Arabic: "let us also review the news analysis and review
the updates for 6 hours"). The review found every class of v_company_news_eval
beating the S&P 500 - neutral as much as positive - because the US names of the
archive beat the index in a falling market, not because of their news: the
comparison with SPY was measuring the names, not the news. v_company_news_eval
now also sets each name-day against the same-day average of every US name in
symbol_bars_daily over the same 1 and 5 sessions from the same close (about 490
names; new columns mean_1d_peer_pp, mean_5d_peer_pp, matured_5d_peer,
beat_5d_peer_pct, corr_5d_peer and peers, added after the old ones), and the
reading is taken from the peer columns (THIN from the peer matured count); a day
with no SPY bar now keeps its peer figures instead of being dropped (none was,
since 15 Sep). First peer read (news days 2 Oct - 18 Nov): positive +0.69 pp
on 52 matured days, neutral +0.67 pp on 186, negative +2.55 pp on 5, correlation
0.019 - still NO EDGE, and the index had added about half a point to every
class. The SPY columns stay for continuity. scripts/news_board.py 1.3.0 shows
the peer figures, with SPY beside them. Nothing in a score, a pick or an order
changes. Research view: the live system never reads BigQuery.

v1.16.0 (20 Nov 2026, the company news re-read). WHY (owner, 20 Nov 2026 12:54
Riyadh, in Arabic: "let us do the expected improvement to the news
classification, improve it and re-classify it"). The news scoreboard showed the
provider's sentiment says "positive" on 97 % of name-days and carries nothing;
reading the 1,306 archived headlines found that four in ten of those filed
under a symbol are about another company. New v_symbol_news_class: every
headline re-read from its own text - relevance (company / actor / roundup /
noise / other), event type, direction, weight and score - with the provider
polarity beside it; the same rules written in Python gave the same reading on
all 1,306 headlines (a plain US ticker never counts right after a colon:
"XTRA:CBK" is Commerzbank, not CBK.US - found by the worked examples in the
tests; and the company counts as the one speaking about others only for a
rating, target or view - never for its own dividend, guidance or results:
"Star Bulk Carriers raises dividend" is company news - review P2 on PR 1038).
New v_company_news_eval: the scoreboard of the new classes
(entry the first close after the news day, 1 and 5 sessions against the S&P
500, US names), the provider's sentiment scored the same way beside it. First
read: the classes are balanced at last (positive 21 %, neutral 77 %, negative
2 %, against the provider's 98 % positive) but carry no edge yet (correlation
0.02; reading NO EDGE). scripts/news_board.py 1.2.0 shows each holding's
re-read news in a new section 7. Nothing in a score, a pick or an order
changes. Research views: the live system never reads BigQuery.

v1.15.0 (19 Nov 2026, the trial labels on the pages). WHY (owner, 19 Nov 2026
18:46 Riyadh, verbatim: "This new updat for news is linked with the
recommdadtions pages like protofilo or investment pages"; 18:50, in Arabic:
"do the best and most suitable for the project"). The two open trials - C-2 and
the news overlay - lived only in BigQuery and on News_Board, away from the
pages where the owner decides. New v_c2_calls: every buy call with what C-2
does with it (kept or not, its upside rank in the day, the conditions it
fails), built on v_trial_c2's own WITH block, character for character, so a
label cannot disagree with the trial. scripts/trial_labels.py reads it for the
observe-only Trial_Labels tab. Research view: the live system never reads
BigQuery; no decision depends on it.

v1.14.0 (19 Nov 2026, the news learning loop). WHY (owner, 19 Nov 2026: world
news - wars, politics, the economy, AI fear, elections, summits - must be
classified, weighted by kind, tested on the past and able to correct itself;
then 13:06 Riyadh, verbatim: "Let's Start build the new news and it's effect in
the file by the best way that you propse and before doing that make more final
test then apply the changes by the best possible and effective way"). The final
test of that day (three years of GDELT against the US sector ETFs, no
look-ahead, 75 scored weeks) found no news weight that beats chance yet: the
favoured-minus-avoided sector spread was 0.15 - 0.17 points a week with a t of
0.5 - 0.7, and the same method fed news 8 to 40 weeks old did as well. So the
news is built to learn and to be measured, not to move a recommendation. New
source table news_theme_daily (scripts/archive_news_themes.py). New
v_news_theme_weekly (weekly themes and their surprise), v_news_sector_weights
(walk-forward weight of every theme for every US sector, switching itself off
when its own record turns bad), v_news_sector_tilt (favoured / avoided
sectors per news week, two arms), v_news_overlay_list (the investable list
beside its news version, a shadow) and v_trial_news_overlay (the trial, forward
evidence from 20 Nov 2026, promotion rule fixed now). Nothing on a page, in a
score or in a pick changes. Research views: the live system never reads
BigQuery. APPLY ORDER: the news table must exist before these views are
created - run the BigQuery News Themes workflow (write) first.

v1.13.0 (19 Nov 2026, the standing lost-opportunity comparison). WHY (owner,
19 Nov 2026 10:04 Riyadh, verbatim: "make better compsion between the ROI as
the investable recommdadtion and the best avalaible in the market as actual
result and what we are missed to be consider in the future as well"). The
review of that morning had to rebuild the comparison by hand. New
v_forward_universe: every liquid market-page equity and archived day with its
forward 1W / 2W price return, status, call, sector and volatility tercile. New
v_investable_vs_market: the investable list and the buy calls beside the
market median, the best tenth, sectors and volatility terciles, per day. New
v_missed_winners: the day's 20 best risers with the cause each was not a pick.
In-sample read (31 Oct - 19 Nov): the investable list 0.18 points under the
market median at one week (48.5% above it), the low-volatility tercile 0.37
over and the high one 1.51 under, the best tenth 11.6 over. Nothing on a page,
in a score or in a pick changes. Research views: the live system never reads
BigQuery.

v1.12.0 (19 Nov 2026, the second trial and outcomes by sector). WHY (owner,
19 Nov 2026 01:18 Riyadh: full review - compare the calls with what the market
did, how many investable names gained or lost, how to improve forecasting and
recommendations, check the data for alignment; then 01:49, verbatim: "start
building the plan that helps outperform and make sure the recommendations
beat the market by a much bigger margin"). The review found the champion (all
buy calls) level with its markets, HIGH-confidence buy calls losing 5.3% in a
month, Tadawul buy calls losing to TASI at every horizon, the analyst target
upside the only input ranking the next month the right way, and no view able
to read outcomes by sector because Performance_Log records carry no sector.
New v_symbol_sector: one GICS sector per symbol (the market pages' latest,
else the Top 10 board's; the six provider spellings folded as in
core/sectors.py). New v_outcome_sector: buy calls by market group, sector and
horizon against the market benchmark, with a fixed reading (100-call floor).
New v_trial_c2: C-2 keeps a buy call that is not HIGH confidence or
reliability 80+, not a Tadawul call, and in the top half of its day by analyst
target upside; it is measured against the champion like C-1, with the
overlap-adjusted t of v_trial_shadow_v2. Rule and promotion test fixed on
19 Nov before any forward day; only calls from 20 Nov 2026 are evidence.
C-2 membership is decided over every buy call of the day, scored or not, and
only scored calls enter the statistics (Codex P2 on #1026). In-sample (not
evidence): one month, C-2 +3.8 points a call against the market (215 calls,
76% ahead), the champion +0.7 (980 calls); overlap-adjusted t 0.35. Nothing on
a page, in a score or in a pick changes. Research views: the live system never
reads BigQuery.

v1.11.0 (18 Nov 2026, the shadow score). WHY (owner, 18 Nov 2026, after two
weeks of market shocks: do the risk and confidence parameters or the Scoring
need adjusting; forecast "on a scientific basis with continuous learning";
then "let us start building"). On 18 Nov v_signal_edge read overall_score,
confidence and reliability as carrying no edge in any market at any horizon;
the analyst target upside and the engine's 1M forecast ROI were the two
signals ranking the next month in the right direction (both still 'thin').
v_signal_edge_daily gains one feature, shadow_v2 = 0.6 x day rank of the
target upside + 0.4 x day rank of the 1M engine ROI, ranked within the signal
day and market group among the names carrying both inputs, so it is measured
every day beside overall_score by the same method. New v_trial_shadow_v2: the
daily rank-IC difference shadow - current with its overlap-adjusted t, each
score's top-third excess return and a reading. Both sides are taken over the
SAME names (the features shadow_v2_matched / overall_score_matched keep each
score only on the names carrying both; Codex P2 on #1023: the plain
overall_score also covers names with no analyst target). The rule and the
promotion test were fixed on 18 Nov before any forward day; only signal days
from 19 Nov 2026 are evidence (a reading needs the larger of 20 days and two
horizon lengths). In-sample read on 18 Nov (US, 1M, 34 days, 23.6 names a
day): shadow IC 0.32, every day positive, top third +3.9 pp; overall_score
on the same names -0.22, top third -2.1 pp - one six-week stretch, the days
the weights were chosen on, not evidence.
Nothing on a page, in a score or in a pick changes. Research views: the live
system never reads BigQuery.

v1.10.0 (16 Nov 2026, the news layer's scoreboard). WHY (owner, 16 Nov 2026:
read all news, measure every recommendation against what happened, learn and
correct itself; design TFB_News_Scenarios_and_Continuous_Learning_Design,
phase 0). New v_news_signal_eval: the News & Macro Gate verdict against the
next 1 and 5 SPY sessions, and each name's news sentiment against its own next
5 sessions (total return), with a reading per signal: NOT MEASURABLE (one
class), BIASED (one class > 90 %), NO EDGE, THIN, or CANDIDATE (forward test).
Read on 16 Nov: gate NO EDGE (NO_NEW_ENTRY 14 days, CAUTION 3, GO none);
sentiment BIASED (98 % positive), correlation -0.10. No news signal moves a
decision until this view shows it adds something. Research view: the live
system never reads BigQuery.

v1.9.1 (15 Nov 2026, review of #991). v_hold_vs_switch scaled the 1M forecast
to one and two weeks by 0.16 and 0.27, the ratio of two averages taken over
different record sets. The performance tracker records its 1W and 2W
forecasts as the 1M price forecast x days/30 (track_performance
_derive_checkpoint_target), and v_switch_calibration measured what a gap is
worth in those units. The view now uses x 7/30 and x 14/30. Read on 15 Nov:
no verdict changes (no horizon is proven; every holding reads KEEP).

v1.9.0 (15 Nov 2026, hold or switch). WHY (owner, 15 Nov 2026: a holding
should be kept only while it is better than the best available opportunity
over the same period - 1 week, 2 weeks, 1 month or more - after the
transaction cost of both trades, "accurate and intelligent"). Two research
views. v_switch_calibration: per market group and horizon, what one point of
forecast gap has been worth in outcome points on the matured records (pooled
within-day slope), the spread of outcomes across names, and the fixed
v_signal_edge verdict on the daily forecast/outcome correlation; k_used is 0
unless the verdict is 'proven'. v_hold_vs_switch: each holding against the
best INVESTABLE candidate of its market per horizon - forecast gap, what such
a gap has been worth, the cost of both trades (0.10 commission + 0.15 spread
per side; the commission measured on the owner's own fills), the net, the
chance the switch ends ahead, and a verdict that reads SWITCH CANDIDATE only
when the signal is proven, the net is positive and the chance is 60% or more.
On 15 Nov no horizon is proven in any market (US: 1W and 2W 'no edge', 1M
and 3M 'thin'), so every holding reads KEEP with indicative figures.
Research views: the live system never reads BigQuery; never an order.
v1.8.0 (14 Nov 2026, the first controlled trial). WHY (owner, 14 Nov 2026:
"make full review ... which area still least improved ... let's work to
improve it"; the full review of that day found the project's HIGH
confidence, reliability 80+ and Tadawul buy calls losing to their market).
New v_trial_c1: challenger C-1, its rule fixed on 14 Nov before any forward
data - keep a buy call unless its confidence is HIGH or its reliability is
80+, or it is a Tadawul call whose own 1-month return did not beat TASI's -
against the champion (every buy call), per horizon, with the calls left out,
the mean daily difference and its t, and a reading. Only calls recorded from
15 Nov 2026 count as evidence (period 'forward'); the earlier calls are shown
as in-sample, which is why the rule was chosen. Promotion rule fixed now: 20
call days, 40 matured C-1 calls, t >= 2; the owner decides even then.
Research view: the live system never reads BigQuery; no decision, gate,
count or broker action depends on it.
v1.7.0 (8 Nov 2026, total return, benchmarks and regimes). WHY (owner, 8
Nov 2026, after the week of 2-6 Nov: "how we can avoid that in the future";
"let's do the requirement and implement it"): three lessons from that week
belong in the learning loop. (1) Supernus went ex an $8.13 special dividend
on 3 Nov (22 % of the price) the day after the owner bought it; the engine
scored raw closes, so it would have learned a -22 % loss that was mostly a
dividend, and the stale 41.92 pre-merger SUPN quote recorded on 3 Nov would
have scored -37 %. v_outcome_engine 1.6.0: every return is a total return
(adj_close restated in entry-day terms, raw close when adj is missing),
engine_roi_raw_pp / dividend_adj_pp disclose the difference, a record's
bars come from its alias (INDV.US -> SUPN.US) and an entry price more than
35 % from the base bar's close is entry_price_mismatch, never scored (226
of 25,715 records on 8 Nov: splits, stale quotes, another security's
price). (2) The 5 Nov -4.4 % day took three stops; an absolute win rate
reads that as five bad picks. New v_outcome_excess: each engine-scored
forecast beside its market benchmark (SPY / TASI / HSI / FTSE / Euro Stoxx /
Nikkei / ACWI by market) with an excess return and verdict at the target
and on the ladder. (3) Nothing said what kind of day a seat was taken on.
New v_market_regime (SPY changes, 50-day and 250-day distance, 20-day
volatility, VIX, one label crash_day / drawdown / stress / weak / calm),
v_seat_regime (every board seat tagged with the regime at the last US close
before it) and v_regime_outcomes (seat outcomes per regime and currency
group with sample sizes). Research views: the live system never reads
BigQuery; no decision, gate, count or broker action depends on them.
v1.6.0 (8 Nov 2026, the signal edge). WHY (owner, 8 Nov 2026: "make full
review for the Project and need to improve the outcome and analysis"): the
October review and the 8 Nov archive study (8,230 symbol-days, 28 Jun - 8
Nov) found that the engine's ROI forecast does not rank the next weeks
(its top fifth trails the middle), that forecast reliability and confidence
carry no edge, and that overall_score is the one signal with a stable
positive rank correlation with the next week (US names). Those were one-off
queries; owner rules, the ranking challenger and the fundable-currency choice
need the same test kept current. New: v_signal_edge_daily (per signal day,
market group ALL / US / Tadawul / Other markets, feature and horizon: the
Spearman rank IC between each live signal - engine ROI forecasts 1W / 2W / 1M
/ 3M, reliability, data quality, confidence, recommendation, investability,
stance, overall / risk score, target upside - and the realized 1W / 2W / 1M /
3M return, from the day's first recording time and with the scoring signals
taken AS OF the forecast, plus the top / bottom-third excess return) and
v_signal_edge (days, mean IC, the overlap-adjusted t, first- and second-half
IC, share of positive days and a fixed verdict: thin / edge / inverse / no
edge / unstable). Research views: the live system never reads BigQuery; no
decision, gate, count or broker action depends on them.
v1.5.0 (2 Nov 2026, the learning loop from the archive). WHY (owner, 2 Nov
2026: "learning is slow ... improve today, results tomorrow"; "layers for
recommendations so the system still issues them when one is available"):
the tracker closes a forecast only when an audit holds a live quote of
exactly the target session, so 5,759 overdue records waited on 1 Nov while
the bar archive already held those sessions' closes. New: v_outcome_engine
(every Performance_Log record scored from the archived daily close of the
first session on/after its target - engine_state, engine ROI and verdict,
best / worst excursion, touched target, a 1W / 2W / 1M / 3M ladder, the
first engine-scored record per key as the cohort, and the sheet's own
outcome beside it with agree / disagree), v_outcome_calibration (engine win
rate, realized ROI, ROI capture, excursions and the sheet's rate by
confidence, reliability tier, data-quality tier, sector, market, horizon,
origin, risk, recommendation and week, with a calibration flag) and
v_board_archive (recommendation layer 2: the Top-10 panel's default gates
re-run over the archived universe, research only, executable = FALSE).
Changed: v_outcome_coverage adds the engine's scored / wins / rate and
backlog states per horizon (file renumbered 69 -> 73, after the engine it
reads). Research views: the live system never reads BigQuery; no decision,
gate, count or broker action depends on them.
v1.4.1 (1 Nov 2026, review follow-up on #886): v_outcome_coverage counts
scored calls and wins over the sheet's outcome cohort only (in_outcome_cohort:
the first valid matured record per stable key, as track_performance's
_summary_outcome_cohort counts them) and publishes matured_cohort and
matured_excluded beside the raw matured count, so a key archived twice can
never count twice in the win rate; v_portfolio_insight publishes
weight_sar_pct only when every holding has a riyal value (NULL for all rows
otherwise, with holdings_without_value saying how many), so a holding with no
rate can no longer leave the others sharing 100 % of a partial book. No other
object changes.
v1.4.0 (1 Nov 2026, full review: data accuracy and honest presentation on the
Looker pages). New: v_fx_sar (currency -> riyal from the Commodities_FX page)
and v_outcome_coverage (how much of the Performance_Log has matured, per
horizon, so a win rate always sits next to its denominator). Changed:
v_symbol_rating adds rating_gate / tfb_rating_display (the engine's verdict
beside every rating: blocked, do not invest, watch only, below the floors,
none - a Positive rating never overrides it) and the analyst / news sources;
v_top10_insight adds seat_kind and allocation (a row without a ticket reads
'none - research board', never an order); v_portfolio_insight gives riyal
value, cost, P&L and weight for every holding (the decision page's figures
first, else the sheet's values times v_fx_sar) with value_source. WHY (owner
request 1 Nov 2026): "avoid any data presentation interpretation ...
continuous presenting recommendation, avoid misleading".
v1.3.1 (1 Nov 2026, the one-symbol page): v_symbol_candles also gives
last_close (the symbol's newest close) and open / high / low /
close_vs_last_pct, every candle in percent points against that close. The
Looker candlestick chart always draws its axis from zero, so candles in
prices sit squeezed in a thin band at the top; against the newest close
they straddle zero and fill the chart. No other object changes.
v1.3.0 (31 Oct 2026, owner request: candles, news and an IBKR-like rating
in My Portfolio and on the one-symbol page): the three tables of
scripts/archive_symbol_deep.py (symbol_bars_daily, symbol_analyst_daily,
symbol_news) are source tables, for v_symbol_candles, v_symbol_analyst,
v_symbol_news, v_symbol_rating (the TFB rating) and the symbol-picker views
v_dive_signals / v_dive_board / v_dive_events; v_top10_insight and
v_portfolio_insight (now files 67 / 68, after the rating) carry the rating
and an 'analyst consensus below Hold' caution, and the portfolio's
bearish-call flag reads the market pages' STRONG_SELL spelling too.
v1.2.0 (31 Oct 2026, Looker pages 3-6): the seven tabs archive_to_bigquery
v1.1.0 adds (four market pages, three news gate tabs) are source tables, for
v_universe / v_universe_latest / f_universe_return (World Top-25 ROI),
v_news_items / v_econ_events / v_news_gate / v_calendar_events, the
v_top10_insight and v_portfolio_insight evidence views and f_symbol_pick
(the one-symbol page). Two helpers, f_col and f_at, read a wide tab by
position after one header lookup per snapshot; header_names_used() reports
the headers a view looks up either way (f_cell or f_col). WHY (owner request
31 Oct 2026): "the top companies that have best ROI world wide ... the page
of my portfolio and the page of top 10 ... insights ... that will make the
decision more reliable ... deep analysis for one selected symbol".
v1.1.0 (29 Oct 2026, global markets page): table functions (CREATE OR REPLACE
TABLE FUNCTION - the Looker date range reaches them through a custom query)
and market_series_daily (scripts/archive_market_series.py) as a source
table, for v_market_daily, f_market_window and f_market_summary. Scalar
functions still come before every view and table function.
WHY (owner request 29 Oct 2026, "can we also build Google Looker Studio"):
the nightly archive (scripts/archive_to_bigquery.py, C8 Phase 1) keeps each
learning tab as raw rows - cells[] plus record_json - one partition per day.
Looker Studio needs typed columns, one row per record and stated units. The
SQL in sql/bigquery/ turns the raw rows into those columns (three helper
functions, views, table functions); this script is how that SQL reaches BigQuery and
how anyone can prove the live objects are the reviewed ones.

WHAT:
  --dry-run (default)  checks every file (one CREATE OR REPLACE VIEW, FUNCTION
                       or TABLE FUNCTION, the {{DS}} placeholder, no data-
                       changing statement, scalar functions before views,
                       every referenced object defined earlier or a source
                       table) and prints
                       the plan with each object's body hash. When the
                       service-account key is configured it also asks
                       BigQuery to dry-run each statement (a broken key
                       exits 2); without one the checks stay offline.
                       Nothing is created.
  --write              creates or replaces every object in file order.
  --verify             reads the live definitions (INFORMATION_SCHEMA VIEWS
                       and ROUTINES, table functions included) and compares
                       their SHA-256 with the files;
                       exit 1 on any missing or different object.
Views and functions only: no table is created, changed or deleted, nothing
is written to the workbook, and the live system never reads BigQuery.

ENV: TFB_BQ_PROJECT (default project-2e3530e6-2311-4aa3-b36), TFB_BQ_DATASET
(default tfb_archive), TFB_BQ_LOCATION (default US), GOOGLE_SHEETS_CREDENTIALS
(the archive's service account: BigQuery Data Editor + Job User).
Exit: 0 done / everything matches, 1 a check, statement or comparison failed,
2 configuration missing (credentials for --write / --verify).

Not a licensed financial adviser; the owner decides and executes every trade.
================================================================================
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

__version__ = "1.27.0"

PLACEHOLDER = "{{DS}}"
DEFAULT_PROJECT = "project-2e3530e6-2311-4aa3-b36"
DEFAULT_DATASET = "tfb_archive"
DEFAULT_LOCATION = "US"
SQL_DIR = Path(__file__).resolve().parent.parent / "sql" / "bigquery"
BQ_API = "https://bigquery.googleapis.com/bigquery/v2"
# the tables scripts/archive_to_bigquery.py v1.1.0 creates (TABS -> table_name)
ARCHIVE_TABLES = frozenset({
    "top_10_investments", "selection_log", "shadow_rank_log", "board_scorecard",
    "calendar_events", "signal_history", "performance_log", "my_portfolio",
    "portfolio_decision", "run_log",
    # v1.2.0: the market pages and the news gate tabs (archive v1.1.0)
    "market_leaders", "global_markets", "mutual_funds", "commodities_fx",
    "news_items", "news_gate", "econ_events",
})
# the table scripts/archive_market_series.py v1.0.0 loads (global markets page)
MARKET_TABLES = frozenset({"market_series_daily"})
# the tables scripts/archive_symbol_deep.py v1.0.0 loads (v1.3.0)
DEEP_TABLES = frozenset({"symbol_bars_daily", "symbol_analyst_daily", "symbol_news"})
# the table scripts/archive_news_themes.py merges (v1.14.0, the news loop; its 1.1.0 sources since v1.20.0)
NEWS_TABLES = frozenset({"news_theme_daily", "news_entity_daily"})   # v1.20.0: the places and the people apart
# the change log scripts/archive_econ_calendar.py v1.0.0 appends, and the scenarios it logs before their events
# from v1.1.0 (v1.21.0, the event scenarios)
ECON_TABLES = frozenset({"econ_calendar", "event_scenario_log"})
# the table scripts/archive_options_reading.py v1.0.0 loads (v1.23.0, the price drivers)
OPTIONS_TABLES = frozenset({"options_daily"})
# the tables scripts/news_now.py v1.0.0 creates and loads (v1.27.0, the hourly news reading)
NOW_TABLES = frozenset({"symbol_news_intraday", "news_now_log"})
SOURCE_TABLES = (ARCHIVE_TABLES | MARKET_TABLES | DEEP_TABLES | NEWS_TABLES | ECON_TABLES
                 | OPTIONS_TABLES | NOW_TABLES)
_CREATE = re.compile(
    r"CREATE OR REPLACE (VIEW|TABLE FUNCTION|FUNCTION) `\{\{DS\}\}\.([a-z][a-z0-9_]*)`")
_REF = re.compile(r"`\{\{DS\}\}\.([a-z][a-z0-9_]*)`")
_FORBIDDEN = re.compile(
    r"\b(INSERT\s+INTO|UPDATE\s+`|DELETE\s+FROM|MERGE\s+`|TRUNCATE\s+TABLE|"
    r"DROP\s+(?:TABLE|VIEW|SCHEMA|FUNCTION)|ALTER\s+(?:TABLE|VIEW|SCHEMA)|"
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE(?!\s+FUNCTION)|SCHEMA|MATERIALIZED)|GRANT|REVOKE|"
    r"EXPORT\s+DATA|LOAD\s+DATA|CALL)\b", re.IGNORECASE)


class SqlObject:
    __slots__ = ("path", "kind", "name", "template")

    def __init__(self, path: Path, kind: str, name: str, template: str):
        self.path, self.kind, self.name, self.template = path, kind, name, template

    def render(self, project: str, dataset: str) -> str:
        return render(self.template, project, dataset)

    def body(self, project: str, dataset: str) -> str:
        return object_body(self.render(project, dataset), self.kind)


def _out(msg: str) -> None:
    sys.stderr.write(f"[bq_views v{__version__}] {msg}\n")


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _has_credentials() -> bool:
    """The archive's service-account key is configured (raw JSON or base64)."""
    return bool(_env("GOOGLE_SHEETS_CREDENTIALS") or _env("GOOGLE_CREDENTIALS"))


def _code(sql: str) -> str:
    """The SQL without its comment lines (checks must not read the prose)."""
    return "\n".join(ln for ln in sql.splitlines() if not ln.lstrip().startswith("--"))


def render(template: str, project: str, dataset: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9\-]{4,61}[a-z0-9]", project or ""):
        raise ValueError(f"bad project id {project!r}")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,1023}", dataset or ""):
        raise ValueError(f"bad dataset id {dataset!r}")
    return template.replace(PLACEHOLDER, f"{project}.{dataset}")


def object_body(rendered: str, kind: str) -> str:
    """The text BigQuery keeps as the definition: for a view or a table
    function the query after the line 'AS' (INFORMATION_SCHEMA.VIEWS
    .view_definition / ROUTINES.routine_definition), for a scalar SQL
    function the expression inside 'AS ( ... )' before OPTIONS, stripped
    (INFORMATION_SCHEMA.ROUTINES.routine_definition)."""
    if kind in ("VIEW", "TABLE FUNCTION"):
        if "\nAS\n" not in rendered:
            raise ValueError(f"{kind.lower()} without an 'AS' line")
        body = rendered.split("\nAS\n", 1)[1].rstrip()
        return body[:-1] if body.endswith(";") else body
    start = rendered.index("\nAS (") + len("\nAS (")
    end = rendered.rindex(")\nOPTIONS")
    return rendered[start:end].strip()


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def file_order(path: Path) -> Tuple[int, str]:
    """1.18.0: the files are applied in the order of their number (99 before
    100), then by name; a name without a leading number sorts last (and fails
    check_objects)."""
    m = re.match(r"(\d+)_", path.name)
    return (int(m.group(1)) if m else 10 ** 9, path.name)


def load_objects(sql_dir: Path = SQL_DIR) -> List[SqlObject]:
    objs: List[SqlObject] = []
    for path in sorted(sql_dir.glob("*.sql"), key=file_order):
        text = path.read_text(encoding="utf-8")
        found = _CREATE.findall(_code(text))
        if len(found) != 1:
            raise ValueError(f"{path.name}: expected exactly one CREATE OR REPLACE "
                             f"VIEW|FUNCTION|TABLE FUNCTION, found {len(found)}")
        kind, name = found[0]
        objs.append(SqlObject(path, kind, name, text))
    return objs


def check_objects(objs: Sequence[SqlObject]) -> List[str]:
    """Static checks; [] when every file is safe to apply in this order."""
    problems: List[str] = []
    defined: Dict[str, str] = {}
    seen_view = False
    for o in objs:
        code = _code(o.template)
        label = o.path.name
        if not re.fullmatch(r"\d{2,3}_[a-z0-9_]+\.sql", label):
            problems.append(f"{label}: file name must be NN_<object>.sql or NNN_<object>.sql")
        elif label.split("_", 1)[1][:-4] != o.name:
            problems.append(f"{label}: defines {o.name}, file name says {label.split('_', 1)[1][:-4]}")
        if PLACEHOLDER not in code:
            problems.append(f"{label}: no {PLACEHOLDER} placeholder")
        if re.search(r"project-[0-9a-f]{8}-|tfb_archive\.", code):
            problems.append(f"{label}: hard-coded project or dataset (use {PLACEHOLDER})")
        bad = _FORBIDDEN.search(code)
        if bad:
            problems.append(f"{label}: forbidden statement {bad.group(1)!r}")
        if code.count(";") != 1 or not code.rstrip().endswith(";"):
            problems.append(f"{label}: exactly one statement ending in ';'")
        if o.kind == "FUNCTION" and seen_view:
            problems.append(f"{label}: functions must come before every view and table function")
        if o.kind in ("VIEW", "TABLE FUNCTION"):
            seen_view = True
            if "\nAS\n" not in o.template:
                problems.append(f"{label}: the {o.kind.lower()} body must follow a line 'AS'")
        else:
            if "\nAS (" not in o.template or ")\nOPTIONS" not in o.template:
                problems.append(f"{label}: function layout must be 'AS (' ... ')' then 'OPTIONS'")
        if o.name in defined:
            problems.append(f"{label}: {o.name} already defined in {defined[o.name]}")
        for ref in _REF.findall(code):
            if ref == o.name:
                continue
            if ref not in SOURCE_TABLES and ref not in defined:
                problems.append(f"{label}: references {ref} before it is defined")
        defined[o.name] = label
    return problems


def header_names_used(template: str) -> List[str]:
    """Every header literal a view looks up - with f_cell, or once per
    snapshot with f_col (v1.2.0) - for the header tests."""
    return (re.findall(r"\.f_cell`\(h, (?:s\.cells|c), '([^']+)'\)", template)
            + re.findall(r"\.f_col`\(h, '([^']+)'\)", template))


# --------------------------------------------------------------------- BigQuery
def _archive_module():  # pragma: no cover - thin loader
    path = Path(__file__).resolve().parent / "archive_to_bigquery.py"
    spec = importlib.util.spec_from_file_location("_tfb_archive_to_bigquery", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def make_session():  # pragma: no cover - needs google-auth and a real key
    return _archive_module().make_session()


def _err(resp: Any) -> str:
    try:
        body = resp.json()
        msg = ((body or {}).get("error") or {}).get("message") or ""
    except Exception:
        msg = ""
    return f"HTTP {getattr(resp, 'status_code', '?')}" + (f": {msg[:300]}" if msg else "")


def run_query(session: Any, project: str, location: str, sql: str,
              dry_run: bool = False) -> Dict[str, Any]:
    payload = {"query": sql, "useLegacySql": False, "location": location,
               "dryRun": bool(dry_run), "timeoutMs": 60000,
               "labels": {"tfb": "looker-views"}}
    resp = session.post(f"{BQ_API}/projects/{project}/queries", json=payload, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(_err(resp))
    body = resp.json() or {}
    errs = body.get("errors") or []
    if errs:
        raise RuntimeError(str(errs[0].get("message") or errs[0])[:300])
    return body


def live_definitions(session: Any, project: str, dataset: str,
                     location: str) -> Dict[str, str]:
    """{object name: sha256 of its live definition} for views and routines."""
    sql = (f"SELECT table_name AS name, TO_HEX(SHA256(view_definition)) AS sha "
           f"FROM `{project}.{dataset}.INFORMATION_SCHEMA.VIEWS` "
           f"UNION ALL SELECT routine_name, TO_HEX(SHA256(routine_definition)) "
           f"FROM `{project}.{dataset}.INFORMATION_SCHEMA.ROUTINES`")
    body = run_query(session, project, location, sql)
    out: Dict[str, str] = {}
    for row in body.get("rows") or []:
        f = row.get("f") or []
        if len(f) >= 2:
            out[str(f[0].get("v"))] = str(f[1].get("v"))
    return out


def compare(objs: Sequence[SqlObject], project: str, dataset: str,
            live: Dict[str, str]) -> List[Tuple[str, str]]:
    """[(name, 'match' | 'missing' | 'different')] for every file."""
    rows = []
    for o in objs:
        want = sha256(o.body(project, dataset))
        got = live.get(o.name)
        rows.append((o.name, "match" if got == want else ("missing" if got is None else "different")))
    return rows


def main(argv: Optional[Sequence[str]] = None, session: Any = None,
         sql_dir: Optional[Path] = None) -> int:
    ap = argparse.ArgumentParser(description="TFB BigQuery Looker views")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="check and print the plan (default)")
    mode.add_argument("--write", action="store_true", help="create or replace every object")
    mode.add_argument("--verify", action="store_true", help="compare live definitions with the files")
    args = ap.parse_args(list(argv) if argv is not None else None)

    project = _env("TFB_BQ_PROJECT", DEFAULT_PROJECT)
    dataset = _env("TFB_BQ_DATASET", DEFAULT_DATASET)
    location = _env("TFB_BQ_LOCATION", DEFAULT_LOCATION)
    try:
        objs = load_objects(sql_dir or SQL_DIR)
        problems = check_objects(objs)
        for o in objs:
            o.render(project, dataset)
    except (OSError, ValueError) as exc:
        _out(f"FAIL {exc}")
        return 1
    if problems:
        for p in problems:
            _out(f"FAIL {p}")
        return 1
    for o in objs:
        _out(f"plan {o.kind.lower():14s} {o.name:22s} sha {sha256(o.body(project, dataset))[:12]}")

    if args.write or args.verify:
        if session is None:
            try:
                session = make_session()
            except Exception as exc:  # noqa: BLE001
                _out(f"CONFIG {exc}")
                return 2
        if args.write:
            for o in objs:
                try:
                    run_query(session, project, location, o.render(project, dataset))
                except Exception as exc:  # noqa: BLE001
                    _out(f"FAIL {o.name}: {exc}")
                    return 1
                _out(f"applied {o.name}")
        try:
            live = live_definitions(session, project, dataset, location)
        except Exception as exc:  # noqa: BLE001
            _out(f"FAIL reading live definitions: {exc}")
            return 1
        rows = compare(objs, project, dataset, live)
        for name, state in rows:
            _out(f"verify {name:22s} {state}")
        bad = [r for r in rows if r[1] != "match"]
        print(f"[bq_views v{__version__}] {len(rows) - len(bad)}/{len(rows)} objects match "
              f"{project}.{dataset}" + ("" if not bad else f"; mismatch: {', '.join(n for n, _ in bad)}"))
        return 1 if bad else 0

    if session is None and _has_credentials():
        try:
            session = make_session()
        except Exception as exc:  # noqa: BLE001
            _out(f"CONFIG {exc}")
            return 2
    if session is not None:
        for o in objs:
            try:
                run_query(session, project, location, o.render(project, dataset), dry_run=True)
            except Exception as exc:  # noqa: BLE001
                _out(f"FAIL dry-run {o.name}: {exc}")
                return 1
    checked = "static checks and BigQuery dry-run" if session is not None else "static checks only (no credentials)"
    print(f"[bq_views v{__version__}] dry run: {len(objs)} objects, {checked}, for "
          f"{project}.{dataset}; nothing created")
    return 0


if __name__ == "__main__":
    sys.exit(main())
