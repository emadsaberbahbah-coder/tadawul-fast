/**
 * ============================================================================
 * 16_Decision_Top10.gs — Top_10_Investments DECISION page (frontend renderer)
 * Version: 1.19.1 (see DT10_VERSION; header kept in lockstep — restored
 *                  again at v1.6.6 after drifting to 1.6.4 while
 *                  DT10_VERSION read 1.6.5)
 * Runtime: ES5 ONLY (V8 exceptions are 01_Menu.gs / 03_Schema.gs only).
 * ============================================================================
 *
 * v1.19.1 (2026-10-14) -- HELD SEATS KEPT IN THE WRITTEN AUDIT
 * (Built 2026-10-13 as an unreleased v1.13.9 draft in PR #770; renumbered
 * after #774 shipped v1.13.9, again on 2026-10-21 after #810 shipped a
 * different v1.13.10, on 2026-10-22 after #815 shipped a different
 * v1.13.11 (bulk render formatting), on 2026-10-25 after #833 and
 * #838 shipped v1.14.0 (confirm basis) and v1.15.0 (pending grace), and
 * on 2026-10-27 after #844, #849 and #852 shipped v1.16.0 (funding
 * evidence), v1.17.0 (shadow rank log) and v1.17.1 (shadow momentum),
 * on 2026-10-28 after #855 shipped v1.18.0 (post-earnings), and on
 * 2026-11-01 after #887 shipped v1.19.0 (qualified hold, seat live fill,
 * owner rule). ALL v1.19.0 logic carried verbatim. No behaviour changed
 * in the renumbering; the base named below is now v1.19.0, the one it
 * sits on.)
 * WHY (Top_10 export 2026-10-13 20:37 Riyadh): six stability grace-held
 * seats (AED.BR, BBOX.L, INDV.US, CRC.US, MGY.US, JMSB.US) rendered blank
 * Rel / DQ / Conf / Engine ROI. None was in the backend's written audit
 * (TFB_OPP_AUDIT_ROWS_MAX keeps selected, unfunded, near-miss and INVEST
 * rows only), so dt10StabIdFallback_ built an identity-only shell: the
 * backend never learns which incumbents this cockpit holds.
 * WHAT: Script Property DT10_AUDIT_KEEP. Exactly 'on' (trimmed, any case)
 * arms it; absent or any other word is off. OFF IS BYTE-IDENTICAL to
 * v1.19.0 in every request and render (one memoized property read is
 * added). Armed, body.criteria gains audit_keep_symbols = the sorted
 * stability members (seated and grace-held, fast-track included; at most
 * 60) read from DT10_STAB_STATE_V1, the blob dt10StabCore_ already uses.
 * The allocation replay deep-copies body, so it carries the same list.
 * opportunity_builder 1.27.0 writes those rows into the capped audit only
 * when its own TFB_OPP_AUDIT_KEEP=1 is set: both switches are needed and
 * either alone is inert. The list is display context, never a decision
 * input: it moves no gate, score, seat or ticket. A property or state read
 * failure sends nothing (the v1.19.0 body).
 * Functions added (0 removed): dt10AuditKeepOn_, dt10AuditKeepSymbols_;
 * vars added: DT10_AUDIT_KEEP_PROP, DT10_AUDIT_KEEP_MAX. refreshDecisionTop10
 * makes one guarded call before the POST. Harness:
 * tests/js/dt10_audit_keep.test.js.
 * Rollback: delete the property (the next run is v1.19.0 behaviour) or
 * re-paste v1.19.0.
 *
 * v1.19.0 (2026-11-01) [QUALIFIED-HOLD / SEAT-LIVE-FILL / OWNER-RULE] -- THREE GATES, DEFAULT OFF
 * WHY (recommendation pages review, 31 Oct 2026, items R2.0 / R3.1 / R4.1):
 * of the 258 board episodes since 1 Oct, 248 had exited, 225 of them within
 * 3 days; 46 exits happened while the source row still read INVESTABLE /
 * BUY with reliability >= 70 and DQ >= 80. Mechanism on this file: a seat
 * is labelled GRACE whenever it is absent from the backend's sized
 * `selected` list (`!rawSet[sym] || co > 0`), even when the audited grid
 * still rates it INVEST inside the rank buffer -- the v1.14.0 qualified
 * day that already keeps its miss clock at 0. GRACE suspends sizing and
 * dt10BoardEligible_ excludes every GRACE seat, so a top-ranked qualified
 * name (DDI.US, qualified #2, 'GRACE (0/3 missed)') was shown with no plan
 * and never offered to the allocation replay. Grace seats absent from the
 * audited grid rendered blank Rel / DQ / FX and a stale stash price (ZTO.US
 * 19.88 while Global_Markets read 20.15, INVESTABLE / BUY, Rel 71.5, DQ
 * 100). And the owner's interim buy rule (Confidence High AND Engine ROI
 * 12M < 34 %) was visible nowhere on the board.
 * WHAT -- three Script Properties, each DEFAULT OFF; absent, blank or any
 * other word = the v1.18.0 run byte for byte (cells, status line, _Run_Log
 * and Logger lines, _Selection_Log rows, state blob, knobs object):
 * (1) DT10_QUALIFIED_HOLD = off | observe | enforce (R2.0). Read ONCE in
 *   dt10ApplyStability_ (dt10StabQualifiedHoldMode_, the
 *   dt10StabConfirmBasis_ pattern) into knobs.qualified_hold, set ONLY when
 *   armed. In dt10StabCore_ a seat is HOLDABLE when it is a confirmed
 *   member (ft !== true), absent from rawSet, co === 0, not outage-paused,
 *   the basis is 'qualified' and dt10StabQualifiedWithin_ rates it INVEST
 *   inside the rank buffer (the very test the counter pass applied).
 *   observe: the stab note gains ', qhold: would hold N SYM ...' (up to 3
 *   names, board order) and audit.qualified_held lists them; nothing else
 *   moves. enforce: such a seat keeps 'ACTIVE (day n)' instead of 'GRACE
 *   (0/n missed)', leaves the grace ledger ('N grace', held_by_grace), and
 *   its ticket shell (dt10StabQualifiedHoldSeat_: the audited-row metrics
 *   of the ghost, no _grace_hold, a countable _qhold marker, no plan) is
 *   board-eligible, so dt10ReallocateBoard_ names it in
 *   board_funding_symbols and the EXISTING replay decides; the note token
 *   reads ', qhold: held N SYM ...'. Nothing here recomputes a gate, score
 *   or verdict (section 8), lowers a threshold, adds a seat or sizes
 *   anything: an unsized held seat finalizes as 'Research seat -- no
 *   executable allocation today. ACTIVE (day n)' like any other unfunded
 *   seat. Unconfirmed fast-track seats are never held (a fast-track seat
 *   must not carry executable sizing under strict, v1.8.10). KNOWN LIMIT:
 *   the backend's frozen snapshot carries only the research picks; a seat
 *   the research pass deferred (sector / market cap, lot or floor, held
 *   target, issuer dedup, price cross-check) is offered but cannot be sized
 *   by the replay until the backend widens the snapshot.
 * (2) DT10_SEAT_LIVE_FILL = off | on (R3.1). Read once in
 *   dt10RefreshDecisionTop10Run_; when on, payload._dt10_live carries a
 *   PURE index of the sheet pool rows the cockpit already collected
 *   (dt10SeatLiveIndex_: Symbol -> Current Price, Forecast Reliability
 *   Score, Data Quality Score, Currency, Investability Status) and the FX
 *   map already read for the POST body; nothing new is fetched. After the
 *   render's finalization dt10SeatLiveFillCore_ fills, on GRACE /
 *   FAST-TRACK / held seats only, a blank Rel, DQ and FX (SAR = 1, else the
 *   lookup rate through dt10HoldingFxRate_), replaces a ghost's stash price
 *   with today's row price (audited grid row first, else the pool row) and
 *   fills a blank or replaced Price SAR (grid price_sar, else price x FX at
 *   2 dp); when the source row is investable (grid verdict INVEST, or
 *   Investability Status INVESTABLE) the Advisor Note gains ' Source row
 *   today: investable (...)'. Status, sizing, eligibility, clocks and the
 *   state blob are untouched. Status token ' | live-fill k/n (m investable
 *   at source)'. A NOT_ACTIONABLE feed still replaces the note cell with
 *   the withhold text (v1.10.0); the numbers stay.
 * (3) DT10_OWNER_RULE_MARK = off | on (R4.1). Read once in
 *   dt10RefreshDecisionTop10Run_; when on, payload._dt10_rule is set and
 *   the render appends ONE column after the seat block (after the v1.13.4
 *   vintage columns when those are enforced; the same insertColumnsAfter
 *   path), header 'Owner Rule', cell dt10OwnerRuleCell_(ticket): 'rule:
 *   yes' when confidence_band is High (any case) AND engine_roi_pct <
 *   DT10_OWNER_RULE_ROI_MAX (34), else 'rule: no (<band or conf unknown>;
 *   ROI <x.x> or ROI unknown)' naming only the failing parts. Display only;
 *   the withhold indices (<= 29), the LAYOUT_PRESERVE panel logic and every
 *   existing column are untouched. Status token ' | owner-rule k/n yes'.
 * Functions added (0 removed): dt10StabQualifiedHoldSeat_ (pure),
 * dt10StabQualifiedHoldMode_, dt10SeatLiveFillOn_, dt10SeatLiveIndex_
 * (pure), dt10SeatLiveFillCore_ (pure), dt10SeatLiveFill_,
 * dt10OwnerRuleMarkOn_, dt10OwnerRuleCell_ (pure), dt10OwnerRuleNote_
 * (pure). Changed: dt10StabCore_ (hold list, enforce branch, note token,
 * audit key), dt10ApplyStability_ (one read), dt10RefreshDecisionTop10Run_
 * (two reads, two status tokens), dt10RenderPayload_ (fill after
 * finalization; the extra column), dt10SelfTest (three property lines,
 * three 'core: ok' lines). Vars added: DT10_QUALIFIED_HOLD_PROP,
 * DT10_SEAT_LIVE_FILL_PROP, DT10_OWNER_RULE_PROP, DT10_OWNER_RULE_ROI_MAX,
 * DT10_OWNER_RULE_HEADERS.
 * Harness: tests/test_dt10_v1190_hold_live_rule.js (REAL file; golden
 * negative on the v1.18.0 base rebuilt by reversing these edits, SHA-256
 * pinned, or a base file given as the second argument; kill identity with
 * the three properties absent / off / junk / faulted through the pure
 * core, dt10ApplyStability_, the REAL render and the REAL refresh;
 * ES5/ASCII lint; inventory).
 * Rollback: set the three properties off (or delete them) with no
 * re-paste; or re-paste v1.18.0.
 * ES5; ASCII; all prior WHYs verbatim; DT10_VERSION and the header bumped
 * in lockstep. Operator order: observe the hold for several evidence runs
 * and compare the 'would hold' names with the owner's judgement before
 * enforce; one property per evidence run.
 *
 * v1.18.0 (2026-10-27) [POST-EARNINGS] -- THE BOARD SAYS WHEN A NAME HAS JUST REPORTED
 * WHY (program register A4 part 2; FRST post-mortem P1): the first funded
 * ticket (FRST.US, 26 Oct 2026) was bought two sessions after a 22% EPS miss
 * (Q3 reported 22 Oct after the close; -4.1% and -3.7% on heavy volume the
 * next sessions). Every calendar signal on this board looks FORWARD (v1.6.0
 * '\u26a0 earnings \u2264Nd'); nothing showed that a seat had just reported,
 * let alone missed. Calendar_Events now carries the last report
 * (calendar_sync v1.2.0 with TFB_CAL_LAST_REPORT, calendar_provider v1.3.1
 * and v1.3.2): Last Earnings Date | Days Since Earnings | EPS Surprise %.
 * WHAT (only when Script Property DT10_POST_EARN is observe or on; absent,
 * blank or anything else = off = v1.17.1 byte for byte): every SELECTED
 * ticket whose symbol reported inside its quarantine gets
 *   '\u26a0 post-earnings S/L \u00b7 EPS x% \u00b7 '
 * on its Advisor Note - after the v1.6.0 forward tag when the note carries
 * one (that tagger looks for its own prefix at position 0), else at the
 * start - where S = trading sessions after the report date up to today
 * (Riyadh calendar day; Mon-Fri, Sun-Thu for KSA symbols; exchange
 * holidays not known) = the session an entry today would be, and L = 5, or
 * 10 when the EPS surprise is negative (interim trading policy rule 3,
 * program doc section 5). The report is the latest PAST date of Last
 * Earnings Date and Next Earnings Date: the 08:07 morning render runs before
 * the 08:15 calendar sync, so a name that reported after the previous close
 * still shows its report as the Next Earnings Date (independent review of
 * PR #855) - that case counts with its surprise unknown. A blank surprise is
 * unknown -> 5; text surprises accept '%' and ','. Reports older than 31
 * days are ignored (the session walk is bounded). One Calendar_Events read
 * per render (A1 to the last column, at most 16), only when armed; any read
 * fault -> no tag and the token 'post-earn n/a'. The status line gains
 * 'post-earn \u26a0N/M' (tagged / selected) after the forward 'earn' token.
 * A withheld row (feed NOT_ACTIONABLE) keeps both research prefixes, forward
 * then post-earnings (dt10UvWithholdRow_; with no post-earnings tag on the
 * note the kept prefix is the v1.17.1 one).
 * OBSERVE ONLY on this board: nothing gates, blocks, resizes or re-ranks.
 * Downstream, the daily brief (run_daily_brief v1.19.0) holds a tagged seat
 * back from its executable picks and says so in one line, like its earnings
 * line - interim rule 3; the v2 decision pages show the tag once 01_Menu
 * reads it (register A12). Interim rule 3 stays a hand check at every
 * GO/NO-GO until the enforce step (no new money for a tagged seat) ships
 * after a week of observed tags. KSA names carry no last report today
 * (EODHD has no Saudi coverage); only a past Next Earnings Date can tag one.
 * Functions added (0 removed): dt10PostEarnMode_, dt10WeekdaysBetween_
 * (pure), dt10PostEarnMapFromValues_ (pure), dt10PostEarnMap_,
 * dt10ApplyPostEarnTags_ (pure), dt10PostEarnAnnotate_. Changed:
 * dt10RenderPayload_ (one call after the forward tag), dt10UvWithholdRow_
 * (keeps the new prefix), dt10SelfTest (new 'post-earnings core' line).
 * Vars added: DT10_POST_EARN_PROP, DT10_POST_EARN_SESSIONS,
 * DT10_POST_EARN_SESSIONS_NEG, DT10_POST_EARN_MAX_DAYS.
 * Harness: tests/test_dt10_post_earnings.js (REAL file; golden negative on
 * the v1.17.1 base rebuilt by reversing these edits, SHA-256 pinned; kill
 * identity with the property absent / off / junk / faulted; ES5/ASCII lint;
 * inventory).
 * Rollback: DT10_POST_EARN off (or delete it) with no re-paste; or re-paste
 * v1.17.1.
 * ES5; ASCII; all prior WHYs verbatim; DT10_VERSION and the header bumped in
 * lockstep.
 *
 * v1.17.1 (2026-10-27) [SHADOW-MOMENTUM] -- THE CHALLENGER GETS ITS MOMENTUM INPUT
 * WHY (first shadow day, 27 Oct 2026): the pre-registered challenger order
 * (opportunity_builder v1.28.0, program register A2) breaks reliability-band
 * ties by sector-relative momentum, read from momentum_score on each pool
 * row. DT10_POOL_FIELDS never sent the 'Momentum Score' column, so the
 * backend logged momentum=0/26 on every call that day and the first
 * _Shadow_Rank_Log event (10:48 Riyadh) shows the challenger as the exact
 * reverse of the champion (smaller engine ROI first inside one reliability
 * band) -- not the order that was pre-registered. The column is filled on
 * the pages (Global_Markets 6,605 of 6,611 rows, column BK). Same defect
 * class as v1.6.1, v1.8.1 and v1.11.12: an input starved by the pool
 * contract.
 * WHAT: var DT10_SHADOW_POOL_FIELDS = [Momentum Score]. The per-execution
 * pool projection (dt10PoolFieldsActive_ with no argument, memoized) appends
 * it ONLY while DT10_SHADOW_RANK_LOG is on or all (dt10PoolFieldsWithShadow_;
 * a send already in the list is never added twice). Off -> the very list
 * object of v1.17.0: every header map, pool row and POST body is byte for
 * byte unchanged. Under TFB_OPP_RANK_MODE=legacy the builder never reads the
 * field; under shadow it feeds only the challenger's second key, so the live
 * order, gates, verdicts, sizing and funding are unchanged either way. The
 * mode is read ONCE per execution (memo 'shadow_log_mode') and the log
 * written after the POST (dt10AppendShadowRankLog_) reuses it, so a property
 * flipped mid-run cannot log a day whose rows went without momentum. The
 * send follows the LOG switch, not TFB_OPP_RANK_MODE: with the log off and
 * the backend in shadow the challenger runs without momentum (harmless while
 * enforce is reserved); the promotion PR must send it whenever the backend
 * ranks by it. (Independent review before merge, 27 Oct: mode read once,
 * self-test memo swap in try/finally, field-count-proof self-test check.)
 * SECTION 8 KEPT: the value is the engine's own sheet column, sent as is.
 * Functions added (0 removed): dt10PoolFieldsWithShadow_. Changed:
 * dt10PoolFieldsActive_ (the memoized no-argument branch only),
 * dt10AppendShadowRankLog_ (the memoized mode when the projection ran),
 * dt10SelfTest ('property memo core' counts the armed field; new 'shadow
 * pool fields core'). Var added: DT10_SHADOW_POOL_FIELDS.
 * Harness: tests/test_dt10_shadow_momentum.js (REAL file; golden negative on
 * the v1.17.0 base rebuilt by reversing these edits, SHA-256 pinned; kill
 * identity with the property absent / off / junk; the builder alias read from
 * core/analysis/opportunity_builder.py; ES5/ASCII lint; inventory).
 * Rollback: DT10_SHADOW_RANK_LOG off stops the send (and the log) with no
 * re-paste; or re-paste v1.17.0.
 * ES5; ASCII; all prior WHYs verbatim; DT10_VERSION and the header bumped in
 * lockstep. Days logged without momentum (27 Oct) are not counted by the
 * nightly scorecard (score_selection_log v1.2.1).
 *
 * v1.17.0 (2026-10-27) [SHADOW-RANK-LOG] -- THE RANKING CHALLENGER, LOGGED
 * WHY (October review + first nightly scorecard, 27 Oct 2026): the live
 * order of the INVEST pool does not predict outcomes (engine ROI vs excess
 * Spearman -0.38 in October, -0.17 since 1 Sep; names at the ROI cap beat
 * their sector/venue reference 34-35% of the time vs 53-61% below it).
 * opportunity_builder v1.28.0 (TFB_OPP_RANK_MODE=shadow, program register A2)
 * computes a second, pre-registered order of the same pool and publishes it
 * in meta.rank_challenger (champion and challenger top 10) plus shadow_rank
 * on the INVEST audit rows and tickets. Nothing in the workbook recorded it,
 * so the nightly scorecard could not compare the two orders on the same days.
 * WHAT: Script Property DT10_SHADOW_RANK_LOG = off (DEFAULT: one property
 * read and nothing else -- no tab, no write, no status token; every render is
 * v1.16.0 byte for byte) | on | all. When armed and the payload carries
 * meta.rank_challenger without an error, dt10AppendShadowRankLog_ appends one
 * event to the hidden tab _Shadow_Rank_Log (created on first use): CHAMPION
 * rows (the backend's live order, top 10), CHALLENGER rows (the shadow order,
 * top 10) and BOARD rows (the seats this render shows: board rank, seat class,
 * and the shadow rank when the name is in today's INVEST pool). Columns:
 * Logged At, Day (UTC key), Cohort (source run), Order, Rank, Symbol, Market,
 * Sector, Seat, Live Rank, Shadow Rank, Engine ROI %, Rel, Conf, Momentum,
 * Strength, Strength Ref, Cap, Floor OK, Mode, Rule, Builder, Pool, Overlap
 * Top5 (Rule and Builder written as 'v1.0' / 'v1.28.0' so Sheets cannot turn
 * them into numbers or dates). Cadence: 'on' = the first event of each UTC
 * day only -- exactly what the nightly scorecard reads, about 30 rows a day;
 * 'all' = also whenever an order changes (audit trail, about 180 rows a day;
 * the workbook's cell budget is why it is not the default). State
 * DT10_SHADOW_LOG_STATE_V1. Row 1 must be the header: it is written whenever
 * row 1 is blank (new, emptied or cleared tab; a first run cut short after
 * the tab was created) and a tab whose row 1 carries another layout is never
 * written ('ERROR header mismatch at column N'). Rows are added before a write
 * that would pass the grid. Creating the tab drops its two unused columns,
 * hides it and returns the view to Top_10_Investments. The status line gains
 * ' | ShadowLog: +N' / 'logged today' / 'unchanged' / 'none (<reason>)' /
 * 'ERROR ...' only when armed. The log holds ranks, symbols and the backend's
 * own figures -- no ticket, price or amount -- so it is research evidence,
 * never an order sheet. (Review before merge, 27 Oct: header on an empty tab,
 * cadence default, text-safe version cells, view and grid handling.)
 * SECTION 8 KEPT: nothing is ranked, scored or gated here; both orders are the
 * backend's. _Selection_Log is untouched (schema and cadence).
 * Functions added (0 removed): dt10ShadowLogMode_, dt10ShadowLogRows_,
 * dt10AppendShadowRankLog_. Changed: dt10RefreshDecisionTop10Run_ (the
 * refreshDecisionTop10 body: one call after the exit log; shadowNote in the
 * final status line), dt10SelfTest ('shadow rank log core'). Vars added:
 * DT10_SHADOW_LOG_PROP, DT10_SHADOW_LOG_SHEET,
 * DT10_SHADOW_LOG_STATE_PROP, DT10_SHADOW_LOG_HEADERS.
 * Harness: tests/test_dt10_shadow_rank_log.js (REAL file; golden negative on
 * the v1.16.0 base rebuilt by reversing these edits, SHA-256 pinned; kill
 * identity with the property absent; ES5/ASCII lint; inventory).
 * Rollback: delete DT10_SHADOW_RANK_LOG (no re-paste) or re-paste v1.16.0;
 * 'all' -> 'on' stops the on-change rows.
 * ES5; ASCII; all prior WHYs verbatim; DT10_VERSION and the header bumped in
 * lockstep. Arm after the backend runs TFB_OPP_RANK_MODE=shadow.
 *
 * v1.16.0 (2026-10-26) [FUNDING-EVIDENCE] -- THE BROKER PACKET CERTIFIES THE REQUEST
 * WHY (Monday 26 Oct 2026, the first 3/3 board): FRST.US, ZTO.US and CRC.US
 * confirmed ACTIVE (day 3) and the board rendered them UNSIZED -- Ticket 0
 * SAR, Shares 0, Entry/Stop/TP '--', 'Research seat -- no executable
 * allocation today', funding=final/0exec, EXECUTABLE 0 of 10. Trace: the
 * allocate replay (dt10ReallocateBoard_) POSTs the same body as the first
 * request; the backend's allocate stage (opportunity_builder
 * _portfolio_input_assessment -> core/portfolio_reconciliation
 * certify_portfolio_inputs) withholds executable funding unless
 * portfolio.reconciliation_evidence is a fresh (<= 900 s) broker packet
 * whose certified cash equals cash_available_sar to the cent, whose FX
 * proofs equal the request's fx_rates, whose positions link to every
 * holding row sent, and holdings_input_incomplete is false. This file
 * forwarded the document property TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1
 * (v1.11.x) but nothing wrote it ('Nothing writes it today', 14 Oct), the
 * cash came from the Top_10 panel (82,819.20, before the day's BUY
 * reservation), fx_rates.USD came from TFB_FX_LOOKUP (3.7448, not the 3.75
 * peg the proof declares) and the Sukuk row 5023.SR (Tadawul custody) can
 * never link to an IBKR position. The executable funding path had never
 * been exercisable in production; it surfaced on the first 3/3 day.
 * WHAT: Script Property DT10_FUNDING_EVIDENCE = off (DEFAULT: the request
 * body, the status line and every render are v1.15.0 byte for byte) | on.
 * When on, after the holdings and cash-source blocks build the body,
 * dt10FundingApply_ reads the packet 10_My_Portfolio v1.5.16 publishes
 * (PD_IBKR_EVIDENCE_PUBLISH=on, from the read-only IBKR capture the
 * 08:30-10:30 watcher consumes) and the PURE dt10FundingApplyCore_ applies
 * it IN PLACE when, and only when, every check holds: schema 1, captured_at
 * with an explicit offset, age 0..840 s (the backend's 900 s less a 60 s
 * request margin), holdings sent (DT10_SEND_HOLDINGS) and read complete,
 * FX proofs readable at 2 decimals. Then: rows whose Symbol is in
 * PD_EXECUTION_EXCLUDE (the same property 10_My_Portfolio uses; 5023.SR)
 * leave the funding cohort; holding_links are rebuilt for THIS request's
 * rows (native symbol + currency, each positive packet position must be
 * represented -- the backend decides, the note discloses); cash_available_sar
 * becomes the packet's certified cash (settled - reserved) x proven rate,
 * integer cents, round half-even exactly as the backend compares it;
 * pending_proceeds_sar = 0; fx_rates gains the proven rates (USD 3.75) and a
 * kept row with a proven currency, a quantity and a price has value_sar
 * recomputed at that rate (the backend's price x fx x quantity check). The
 * allocate replay deep-copies the applied body, so one apply covers both
 * stages. Any failed check leaves the body untouched and the status line
 * says ' | funding evidence NOT APPLIED (<reason>)'; success says
 * ' | funding evidence APPLIED (capture age Nm; cash X SAR; linked a/b;
 * excluded ...; USD 3.75)'. The packet is never logged or rendered.
 * ORDER: the backend's 900 s window means Top_10 must render within ~14 min
 * of the capture: 10_My_Portfolio v1.5.16's watcher arms a one-off
 * follow-up render after it publishes (chain-style guards). SECTION 8 KEPT:
 * no gate, score or verdict is recomputed here; sizing stays the backend's.
 * Functions added (0 removed): dt10FundingMode_, dt10FundingExcludeList_,
 * dt10FundingSym_, dt10FundingUnits_, dt10FundingDiv_, dt10FundingAge_,
 * dt10FundingNative_, dt10FundingRates_, dt10FundingCashCents_,
 * dt10FundingLinks_, dt10FundingDecimal_, dt10FundingValueSar_,
 * dt10FundingApplyCore_,
 * dt10FundingNote_, dt10FundingApply_. Changed: refreshDecisionTop10 (one
 * call after the cash block; fundingNote in the status line), dt10SelfTest
 * ('funding evidence core'). Vars added: DT10_FUNDING_PROP,
 * DT10_FUNDING_EXCLUDE_PROP, DT10_FUNDING_MAX_AGE_MS, DT10_FUNDING_MAX_ITEMS,
 * DT10_FUNDING_SAFE. Harness: tests/test_dt10_funding_evidence.js (REAL
 * file; golden negative on the v1.15.0 base; kill identity with the property
 * absent; ES5/ASCII lint). Rollback: delete DT10_FUNDING_EVIDENCE (no
 * re-paste) or re-paste v1.15.0. ES5; ASCII; all prior WHYs verbatim;
 * DT10_VERSION and the header bumped in lockstep. Arming is an owner step
 * AFTER the first funded ticket of 26 Oct 2026 is recorded.
 *
 * v1.15.0 (2026-10-25) [PENDING-GRACE] -- A SOFT MISS PAUSES A PENDING COUNT
 * WHY (owner, 24-25 Oct 2026): a challenger confirmed on days 1-2 that fails
 * ONE criteria day (still on the audited grid, no structural block, not a
 * hard-out verdict, inside the rank buffer, but DO_NOT_INVEST because a
 * floor was missed) had its count reset to 0 by dt10StabCore_'s
 * `else { st.ci = 0; ... }` -- the same branch a hard exit uses. Live case,
 * 25 Oct 08:10 board (first run on the 2026-10-25 epoch): PHP.L sat at 1/3
 * on 24 Oct with reliability 71.5 / DQ 100; on 25 Oct -- a weekend, no LSE
 * session, price unchanged at 92.05 -- its Expected ROI 3M read 8.13% (<12%)
 * and the name fell to GRACE with its count gone. Members already enjoy this
 * tolerance ('plain DO_NOT_INVEST criteria-misses ARE grace-holdable', exit
 * clock, v1.2.0); challengers did not. The owner asked (24 Oct 19:32) for a
 * rule that does not restart a positive name's count on one such day.
 * WHAT: Script Property TFB_T10_PENDING_GRACE_DAYS (integer 0..5; default
 * absent/0/junk = v1.14.0 clocks byte for byte). When > 0, on a day-advance
 * a NON-member with ci > 0 that is absent from raw and not qualifiedWithin
 * but is a SOFT MISS (dt10StabSoftMiss_: audited row present, verdict not
 * INVEST, structural_block !== true, not hardOut, global rank inside the
 * rank buffer) keeps ci and increments st.cp (consecutive pending-grace
 * days); the day's score point is still recorded. Once cp would exceed the
 * grace, the v1.14.0 reset applies (ci 0, cp cleared). Any raw or qualified
 * day clears cp. Members, hard exits, outage pauses, seating (still needs a
 * raw ticket), fast-track, displacement, capacity and sizing are unchanged.
 * SECTION 8 KEPT: no gate, score or verdict is recomputed -- the soft miss is
 * read from the backend's own audited row (verdict + structural_block + rank).
 * SURFACE: status note gains ', pgrace=<n>' and ', pheld <k> SYM(ci/n) ...'
 * (up to 3) only when the property is armed; audit gains `pending_held`
 * only when armed; the state blob writes `cp` only while > 0 (a blob that
 * never paused is byte-identical). dt10SelfTest gains 'pending grace core'.
 * Functions added (0 removed): dt10StabSoftMiss_, dt10StabPendingGrace_;
 * var added: DT10_T10_PENDING_GRACE_PROP.
 * Harness: tests/test_dt10_pending_grace.js (REAL file; golden negative on
 * the v1.14.0 base rebuilt by reversing these edits, SHA-256 pinned; kill
 * identity with the property absent/0; ES5/ASCII lint; inventory).
 * Rollback: delete the property (no re-paste) or re-paste v1.14.0.
 * ES5; ASCII; all prior WHYs verbatim; DT10_VERSION and the header bumped
 * in lockstep. Arming is an owner step AFTER the first funded ticket of
 * 26 Oct 2026 (the Monday path runs on v1.14.0 clocks).
 *
 * v1.14.0 (2026-10-24) [CONFIRM-BASIS] -- QUALIFIED DAYS COUNT TOWARD CONFIRMATION
 * WHY (owner, 24 Oct 2026): a stock that qualifies on day 1 and day 2 is
 * pushed out of the backend's raw top-10 on day 3 by a higher-ranked
 * newcomer. It is still INVEST on the audited grid, only ranked lower, yet
 * its confirmation count fell to 0 and it had to start over. Live trail:
 * the 22 Oct board seated XIOR.BR, GBCI.US and PINE.US at FAST-TRACK 1/3;
 * on the 24 Oct advance none of them was in the raw set and every count
 * read 0 again (those three had also left INVEST, so the rule below would
 * not have kept them; it keeps a name that stays INVEST inside the
 * buffer). Mechanism, pinned on v1.13.11: the dt10StabCore_ counter pass
 * advances ci only for a symbol present in rawSet (payload.selected, the
 * sized top-N); any other day-advance sets ci = 0, and for a member only
 * the exit clock honours the rank buffer (pause). Confirmation was keyed
 * to the sized list, not to qualification.
 * WHAT: Script Property TFB_T10_CONFIRM_BASIS, read ONCE in
 *   dt10ApplyStability_ (dt10StabConfirmBasis_, the dt10HardVerdictStrict_
 *   pattern: the service read stays outside the pure core) into
 *   knobs.confirm_basis. 'qualified' (DEFAULT, owner decision 2026-10-24)
 *   | 'raw' (legacy). Anything else, blank or a service fault reads as
 *   'qualified'. Under 'raw' every clock, label, note and state byte is
 *   v1.13.11.
 * (a) new PURE dt10StabQualifiedWithin_(sym, candBySym, globalRank, knobs,
 *   hardOut): the symbol has an audited row whose verdict is exactly
 *   INVEST (any case), no structural_block, not hardOut, and its
 *   opportunity-score rank across the deduped audited grid is <= 'T10:
 *   Stability Rank Buffer' (> 0) -- the SAME window the exit-clock pause
 *   has used since v1.3.0 (rank <= buffer, default 15), so the panel knob
 *   keeps one meaning.
 * (b) counter pass, day-advance, not outage-paused: when the symbol is NOT
 *   in rawSet and knobs.confirm_basis === 'qualified' and (a) holds, the
 *   day counts exactly like a raw sighting: ci + 1, co = 0, score point
 *   recorded. Otherwise the v1.13.11 branch runs unchanged (ci = 0; a
 *   member pauses inside the buffer or takes a miss). Members benefit the
 *   same way: a GRACE or FAST-TRACK seat still INVEST inside the buffer
 *   has co reset and ci advanced instead of a pause, so a fast-track seat
 *   can graduate on a qualified day (its label that day stays GRACE, ghost
 *   path, sizing suspended -- no raw ticket, no sizing).
 * (c) NOT changed: seating. Entry as 'NEW (confirmed n/n)' still requires
 *   presence in rawSyms (a sized ticket); fast-track fills only from
 *   rawSyms; displacement, capacity, hard/soft exit and every label are
 *   v1.13.11. The rule applies to symbols already tracked in the state
 *   blob (members and past raw sightings inside the 14-day prune window);
 *   a symbol never seen in raw is not tracked and accrues nothing.
 * (d) Read-back: under 'qualified' the stab note gains ', confirm=qualified'
 *   and, when a non-raw challenger inside the buffer has ci > 0, ', near
 *   <k> SYM(ci/confirm_days) ...' (up to 3, the 'pending' style). Under
 *   'raw' no token is added (byte-identical note).
 * (e) dt10SelfTest gains 'confirm basis (TFB_T10_CONFIRM_BASIS): ...' and
 *   the pure battery 'confirm basis core: ok' (raw replay identity; day-1/2
 *   raw, day-3 qualified at rank 12 -> ci 3, day-4 seated NEW 3/3 with the
 *   raw ticket's sizing; WATCH -> 0; rank buffer + 1 -> 0; buffer 0 ->
 *   legacy; GRACE member -> co 0, ci advanced).
 * Functions added (0 removed): dt10StabQualifiedWithin_,
 * dt10StabConfirmBasis_; var added: DT10_T10_CONFIRM_BASIS_PROP.
 * Harness: tests/test_dt10_confirm_basis.js (REAL file; golden negative on
 * the v1.13.11 base rebuilt by reversing these edits, SHA-256 pinned; kill
 * deep-equal; ES5/ASCII lint; inventory). Rollback: Script Property
 * TFB_T10_CONFIRM_BASIS = raw (no re-paste) or re-paste v1.13.11.
 * ES5; ASCII; all prior WHYs verbatim; DT10_VERSION and the header bumped
 * in lockstep.
 *
 * v1.13.11 (2026-10-22) -- BULK RENDER FORMATTING (6-MINUTE LIMIT)
 * WHY (21 Oct 2026, Apps Script execution log): the automatic Top-10
 * refresh that followed the first decision-feed release logged 'upstream
 * verdict: EXECUTABLE; final board: HELD' at 23:48:42 Riyadh and was
 * killed at 23:50:59 ('Exceeded maximum execution time') before it wrote
 * its status line. B2 stayed 'running', the next scheduled release check
 * read T10_STATUS_NOT_OK and withdrew the release (decision_failed). The
 * 19:40 run was killed the same way; completed runs took 127-232 s. The
 * render painted formats cell by cell: one setBackground per zebra row in
 * dt10WriteTable_, and setBackground + setFontColor + setFontWeight per
 * coloured cell in dt10ColorVerdicts_, dt10ColorQualified_ and
 * dt10ColorStability_ -- about 1,750 range calls for the 500-row CANDIDATES
 * grid alone, each one slower while other scripts write the workbook.
 * WHAT: the same formats are written in bulk. dt10WriteTable_ reads the
 * body backgrounds once, sets the zebra rows and writes them back once.
 * The three colourers build one token per row and call the new
 * dt10BulkPaint_, which reads backgrounds, font colours and font weights
 * once, sets tok.bg / tok.fg / bold on the token rows and writes each
 * attribute once (nothing is read or written when no row has a token).
 * Rows the old loops left alone keep their current format (read back and
 * written unchanged), so the painted board is the same. Values, verdicts,
 * withholding, funding, stability, receipts and the status line are
 * unchanged. Harness: tests/js/dt10_bulk_format.test.js (cell-model
 * equivalence against the v1.13.10 code, bounded call counts).
 *
 * v1.13.10 (2026-10-20) -- HOLDING SUBUNIT FX, NO NATIVE-AS-SAR, FAILED STATE
 * (1) dt10HoldingFromRow_ upper-cased the currency before the FX lookup, so
 * a GBp (pence) holding converted at the GBP rate (100x too large). The new
 * dt10HoldingFxRate_ mirrors opportunity_builder._resolve_fx (USX gate off):
 * GBX/GBp, ZAC/ZAc, ILA/ILa are detected on the RAW token and use an exact
 * raw FX key, else parent / 100. USX keeps needing its own 'USX' FX row.
 * (2) A currency with no FX rate kept its NATIVE value labelled value_sar.
 * It now sends value_sar null and marks holdings_input_incomplete, so
 * funding stays withheld; the builder never sums a native amount as SAR.
 * (3) refreshDecisionTop10 wrote 'running...' and a throw left it there
 * (20 Oct: cockpit stuck RUNNING, no grid, validation failed). The body is
 * now dt10RefreshDecisionTop10Run_; on a throw dt10RefreshFailed_ writes
 * 'status: FAILED | failed_at_utc=<ISO Z> | <error>', upserts _Status ERROR,
 * clears the P-195 marker this run stored (a throw before the marker check
 * leaves a previous run's marker for ABORTED_PREV) and rethrows. The
 * freshness audit reads FAILED as T10_STATUS_NOT_OK. LIMIT: an Apps Script
 * hard timeout (6 min) kills the script without running catch; that case
 * still shows RUNNING and is reported by the next run's P-195 ABORTED_PREV
 * row.
 *
 * v1.13.9 (2026-10-14) -- ENTERED CASH LABEL AND HOLDING UTC PROVENANCE
 * The cash KPI echoes an entered amount and cannot certify custody or
 * executable funding. It now always says Entered cash (SAR), including
 * existing layouts. Cash values, inputs, allocation and feed gates are
 * unchanged. Holding reads accept the canonical Last Updated (UTC) header
 * and retain its UTC field name and original instant. Multiple accepted
 * timestamp columns make the input incomplete and omit timestamp proof;
 * no first-column choice can conceal contradictory source timestamps.
 *
 * v1.13.8 (2026-10-13) -- FEED RUN BOUND TO THE SOURCE COHORT (P0-1 PREREQ 2)
 * WHY (2026-10-13): the audited release (scripts/release_decision_feed.py,
 * PR #763) writes 'EXECUTABLE | run=<SOURCE> | <anchor> | ...' into the
 * _Status 'TFB Decision Feed' cell. dt10BoardVerdict_ honoured an
 * EXECUTABLE feed on its state and age alone (dt10UvParse_) and never
 * compared the feed's run= with this board's own source cohort (the one
 * shared run= of the four market _Status rows, dt10SourceReadinessCore_).
 * A release for run A could therefore render EXECUTABLE over a newer,
 * unaudited cohort B written by a writer the writer-start supersede step
 * does not cover (a stale workflow definition, a writer outside those
 * workflows, a disarmed gate, a run-listing race). Arming prerequisite 2
 * of docs/evidence/TFB_Commit_Sheet_release_decision_feed_v1.0.0_2026-10-13.md
 * asks for exactly this check, as defence in depth.
 * WHAT: Script Property DT10_FEED_COHORT_BIND. Exactly 'on' (trimmed, any
 * case) arms it; absent or any other word is off. OFF IS BYTE-IDENTICAL to
 * v1.13.7 in every function apart from the version string and two
 * diagnostic dt10SelfTest lines (one memoized property read is added, and
 * only while the feed reads EXECUTABLE). A property-service failure reads
 * as on (fail-closed; the check can only withhold). Armed, and only when
 * the parsed feed state is EXECUTABLE after source readiness passed, the
 * raw feed value must carry exactly one run=<digits> token equal to the
 * readiness cohort. Tokens are counted with the Python consumers' widest
 * rule (SOURCE_RUN_RE: 'run' '=' not preceded by a letter, digit or
 * underscore, any case), so superseded_by_run= is not a run token. A miss
 * makes the verdict NOT_ACTIONABLE with reason 'feed run X != cohort Y' or
 * 'feed run token missing/ambiguous' ('cockpit source cohort unverified'
 * or 'feed cohort check failed' on a fault). Nothing is ever upgraded to
 * EXECUTABLE; a source-blocked verdict keeps its source reason; the B2
 * receipt 'source readiness VERIFIED (run N)' is untouched, so the release
 * still bootstraps from it. DT10_UPSTREAM_VERDICT=off stays the existing
 * cockpit BYPASS: the feed is not read, so this check is bypassed too.
 * Never use it as a kill switch.
 * Functions added (0 removed): dt10FeedCohortBindOn_, dt10FeedCohortCheck_;
 * var added: DT10_FEED_COHORT_BIND_PROP. dt10BoardVerdict_ keeps the raw
 * feed value it already read and makes one guarded call. dt10SelfTest
 * gains 'feed cohort bind (DT10_FEED_COHORT_BIND): off|on' and 'feed
 * cohort core: ok'. Harness: tests/js/dt10_feed_cohort_binding.test.js.
 * Rollback: delete the property (the next run is v1.13.7 behaviour) or
 * re-paste v1.13.7.
 *
 * v1.13.7 (2026-10-12) -- PROVED CROSS-PAGE POOL EXCLUSIONS
 * Five approved fund-tab rows legitimately repeat earlier source pages.
 * The exact collected physical grids now emit individual symbol/owner
 * exclusion receipts, physical and unique counts, shared cohort and SHA256
 * bindings. Source readiness verifies every exclusion against freshly read
 * complete columns before allowing its physical-to-unique count difference.
 * Missing/extra/duplicate/changed identities, unexplained omissions, a capped
 * scan or an unproved cohort still withhold execution. B2 distinguishes
 * included/physical counts and carries the canonical pool_reconciliation_v1
 * receipt; no submitted count alone proves consumption.
 *
 * v1.13.6 (2026-10-12) -- OWNER-APPROVED EXACT SOURCE SCOPE
 * Dated approval 2026-10-12.current-workbook.v1 replaces the staged 1,025
 * Saudi-leader and 4,496 fund floors with the approved 255/2,474 memberships.
 * Every readiness boundary reads each complete physical Symbol column and
 * verifies one sorted-symbol SHA256, without alias mapping or filtering bad
 * rows. A count alone cannot certify the scope. Missing, duplicate, extra or
 * substituted symbols withhold execution; membership is bound to the source
 * capture. Global/commodities floors, freshness, cohort and funding guards
 * remain in force. No raw approved roster is embedded in this source.
 *
 * v1.13.5 (2026-10-11) -- POST-REVIEW ACTIVATION REPAIRS
 * Owned KPIs become inactive when a mode changes or properties fail. Strict
 * partial policy references retain seed comparisons for undeclared controls.
 * News uses backend gate receipts separately from display-label coverage;
 * undated analyst targets remain unknown. Candidate publication stages and
 * verifies complete values, including zero rows, then commits a run/digest
 * receipt last. Readers reject stale, partial or changed results. All gates
 * remain default off; decision/funding controls are unchanged.
 *
 * v1.13.4 (2026-10-11) [WP-8 / DQ-14, 47, 48, 49, 50, 52; P-196] -- COCKPIT INPUT TRUTH
 * WHY (workbook export 2026-10-10, GitHub run 38071484709; Top_10 run
 * 2026-10-10 19:28:22, req d2c065e9fd85, route v4.18.1, builder v1.25.0):
 *  (a) DQ-48: the candidate audit (500 rows, cockpit rows 77-576) carries
 *      News Unknown 440/500 (88 %), Sector Trend Unknown 500/500 (100 %),
 *      Conflict Yes 150/500. The 60 News labels equal the TFB_NEWS_DISPLAY
 *      cap (routes v4.10.0 [NEWS-DISPLAY]: display-only; the builder gates
 *      still see 'Unknown', which passes by design). The panel reads
 *      'Allow Negative News/Sector = No' while neither gate can fire, and
 *      the KPI strip (11 cells) carried no gate-evaluability cell.
 *  (b) DQ-47: panel 'T10: Max Per Sector' 2 vs _Lists_Config seed 2 vs the
 *      GAS built-in 3; 'PF: Max Sector %' 30 vs seed 30; 'PF: Max Position %'
 *      20 vs seed 15. The repo holds a dated operator DECISION ('3 names /
 *      40 %', 2026-09-10, v1.11.12 WHY block) that neither the live panel
 *      nor the seed adopted, while docs/reviews/project-review-20261008.md
 *      retained 20 %/30 %/two -- there is NO adopted policy artifact. The
 *      line therefore reports drift against DECLARED references (seed,
 *      built-in, decision-0910, optional Script Property DT10_POLICY_REF)
 *      and never calls 3/40 'policy' until DT10_POLICY_REF is set.
 *  (c) Cockpit <= 80 rows: the 500-row audit grid sits inside the cockpit
 *      (section header 75, column header 76, data 77-576, footer 578; grid
 *      732 x 118). 75 rows precede the grid; a header + pointer line puts
 *      the footer at 79 (= 75 + alerts.length on this shape).
 *  (d) Quote as-of / target vintage: SELECTED has 30 columns and no as-of;
 *      pool rows carry Warnings tokens 'acquisition_quote_asof:<ISO>' (on
 *      6,389/6,609 Global_Markets rows) and 'analyst_lkg:<N>h' (2,555 rows;
 *      103/170 INVESTABLE). On the 10 selected: as-of 10/10, lkg 7/10.
 *  (e) DQ-49: Calendar_Events covers 67/170 GM INVESTABLE; of the board,
 *      8/10 selected and 10/13 qualified have a DATED upcoming earnings row
 *      (the keep-rule of dt10EarningsMapFromValues_: 2286.SR has a blank
 *      date, MGY.US / COSO.US have no row).
 *  (f) DQ-14/50: TFB_FX_LOOKUP (55 rows) has no USX row -> missing_fx 13
 *      (findings usx-subunit: operator row now; backend subunit fix separate).
 * WHAT: three Script Property gates, each off (default) | observe | enforce,
 * fail-closed to off on junk/absent/service error, read once per execution
 * through dt10PropMemoGet_ (P-202). DEFAULT OFF IS BYTE-IDENTICAL to v1.13.3
 * on every sheet write, the status line, _Status, _Run_Log, _Selection_Log
 * and the payload object (tests/js/decision_input_truth.test.js proves it
 * against the base file's write log); off adds three memoized property
 * reads and nothing else.
 *  DT10_INPUT_TRUTH  observe: B2 tokens 'gates: ...', 'panel: ...',
 *      'cal(earn-dated): ...' + ONE _Run_Log INPUT-TRUTH row (details JSON
 *      carries the counts, the reference items and the countable alert
 *      objects). enforce: + KPI cells L15:M16 'News Gate' / 'Sector Gate'.
 *      No ALERTS rows are added (the cockpit stays <= 80 rows); panel cells
 *      are never written. DT10_POLICY_REF (optional JSON) is read only when
 *      this gate is armed.
 *  DT10_CAND_TAB  observe: token 'cands N in cockpit (footer row R; target
 *      <= 80)'. enforce: the audit grid moves to the hidden tab
 *      _T10_Candidates (row 1 run stamp incl. request id, row 2 headers,
 *      rows 3.. data, verdict colouring kept, active sheet restored); the
 *      cockpit keeps the section header + column header + one pointer line.
 *      A write failure falls back to today's in-cockpit grid with a 'write
 *      FAILED' token. Arm enforce only after TFB_IQR_SYMBOL_PAGES lists
 *      _T10_Candidates (separate ENV item) and the readers know the tab.
 *  DT10_VINTAGE_COLS  observe: token 'vintage: as-of k/N lkg m/N'.
 *      enforce: SELECTED gains columns 31 'Quote As-Of (UTC)' and 32
 *      'Target Vintage' (text; 'retrieved <Last Updated>' is labelled as a
 *      retrieval clock, never as quote time); withhold indices untouched.
 * Functions added (0 removed): dt10ModeProp_, dt10InputTruthMode_,
 * dt10CandTabMode_, dt10VintageColsMode_, dt10PolicyRefParse_, dt10PolicyRef_,
 * dt10GateEvaluabilityCore_, dt10GateEvaluabilityNote_,
 * dt10GateEvaluabilityAlerts_, dt10GateKpiCells_, dt10PanelRefCore_,
 * dt10PanelRefNote_, dt10PanelRefAlerts_, dt10CalCoverageCore_,
 * dt10CandPointerText_, dt10CandTabStampLine_, dt10PoolVintageIndex_,
 * dt10VintageFmt_, dt10VintageCells_, dt10VintageNote_, dt10LabelScan_,
 * dt10PfPanelRead_, dt10CandTabWrite_, dt10InputTruthRunLog_.
 * dt10EarningsAnnotate_ returns the additive field 'map' (callers of
 * earn.note unchanged). dt10SelfTest gains the mode line, 'fx USX row' and
 * seven pure 'core: ok' lines. Rollback: set the properties to off (the
 * next run is byte-identical) or re-paste v1.13.3 (sha256 ce3ef1ea...).
 * EXPECT after paste + DT10_INPUT_TRUTH=observe on a 10-10-shaped run: B2
 * 'gates: news 60/500 evaluable (88% unknown; display-only labels, gate
 * input decoupled) NOT EVALUABLE | sector 0/500 evaluable (100% unknown)
 * NOT EVALUABLE | conflict yes 150/no 350 [observe]' (middle dots in the
 * live cell) and one _Run_Log INPUT-TRUTH row.
 *
 * v1.13.3 (2026-10-10) -- SOURCE READINESS AND PUBLICATION COHORT
 * Validate factual acquisition/data receipts and approved source row floors
 * in _Status A:J in addition to the composite feed token. Bind the source
 * generation and shared producer run before reading the sheet pool; recheck
 * before allocation and final rendering. A changed/partial/stale source withholds the order
 * surface. Available sheet rows are never described as a full approved
 * universe. Empty Sheets input stays an explicit research input. Source
 * failures freeze stability writes; no roster, floor or quote is invented.
 * Installed source and a successful live readback still require attestation.
 *
 * v1.13.2 (2026-10-10) -- HELD SEAT DISPLAY CONSISTENCY
 * The Selected KPI separates fast-track suspension, research and grace.
 * Qualified and audit rows explain visible held research seats while their
 * Selected flag remains executable-only; backend deferrals stay unchanged.
 * Earnings annotations run after the final render-time finalization so
 * held-seat note replacement cannot erase the per-ticket calendar warning.
 * Feed withholding retains only that calendar prefix and replaces the
 * order narrative and levels with the existing withheld explanation.
 * No qualification, stability, funding, policy or reset behavior changes.
 *
 * v1.13.1 (2026-10-10) -- QUOTE HANDOFF AND FINAL BANNER TRUTH
 * Preserve actual provider, typed acquisition/quote witnesses and their
 * source aliases through the sheet request. Duplicate or contradictory
 * evidence is retained for backend validation; no missing proof is invented.
 * Retrieval timestamps retain their declared UTC/Riyadh basis. The selected
 * banner derives EXECUTABLE/HELD/NO_TRADE from the finalized board rather
 * than copying an upstream EXECUTABLE verdict onto an empty or grace board.
 * Repository version only: installed source and live request/readback still
 * require native attestation before claiming production repair.
 *
 * v1.12.2 (2026-10-07) -- FINAL CASH-FLOOR OBSERVATION DENOMINATOR
 * Cash-floor sizing/loss observations use only final executable displayed
 * tickets, including a zero-eligible board or late render-time withholding.
 * Existing cash policy context, reserve, budgets and allocation are unchanged.
 * Observe alerts retain their backend policy head and pre/post budgets while
 * their seat/loss tail follows the final board. No alert or floor is invented.
 *
 * v1.12.1 (2026-10-07) -- FINAL BOARD FUNDING AND REPLAY HARDENING
 * Qualify research without reserving cash, advance stability once, then fund
 * only eligible final seats through the signed frozen backend replay. Grace,
 * research and withheld seats carry zero money; KPIs and logs follow final
 * actions. Builder 1.24.1 binds execution policy, active auth and Render SHA.
 * DT10_BOARD_FUNDING_MODE=research is the reversible zero-allocation brake;
 * unknown values or property-read failures also withhold allocation.
 * Install this complete file, run dt10SelfTest(), then refreshDecisionTop10()
 * and verify the live footer plus HELD zero-execution money fields.
 *
 * ============================================================================
 * v1.11.13 (2026-10-05) [P-202] -- HOTFIX: ONE PROPERTY READ PER EXECUTION
 * ----------------------------------------------------------
 * WHY (Monitoring Sheet #8, export 2026-10-05 09:10):
 *   Four consecutive refreshDecisionTop10 runs left no completion (00:30
 *   manual, 01:31, 05:35, 08:07 morning trigger); each was judged
 *   ABORTED_PREV by the next (the v1.11.12 P-195 marker working as built).
 *   Alive at +5 min (05:40 'document lock busy'), dead by +9 min (01:40
 *   lock free) = the 6-minute Apps Script execution ceiling. ROOT CAUSE on
 *   this file: v1.11.12 E5 made dt10PoolRowFromSheetRow_ (called once per
 *   sheet row) call dt10PoolFieldsActive_(), which calls
 *   dt10W52FieldsLegacy_(), which calls PropertiesService.getScriptProperties()
 *   .getProperty('DT10_P181B_W52_LEGACY') -- ONE SERVICE CALL PER POOL ROW:
 *   255 + 6,609 + 453 + 2,474 = 9,791 rows -> ~9,800 service calls per run
 *   (+4 in dt10MapHeaderCols_), i.e. +100..+300 s on a run whose GAS side
 *   already took 55-185 s (7-day median 84 s). The only v1.11.12 run that
 *   completed took 301.9 s (21:30 on 10-04, backend 25.3 s); the next four
 *   crossed 360 s. Side effect: ~59k property reads/day against the 50k/day
 *   quota (the read is inside try/catch, so quota errors degrade to
 *   legacy=false -- slowly). The 10-04 harness stubbed PropertiesService, so
 *   the per-row service cost was invisible to a functional dual-tree.
 *   DT10_P181B_W52_LEGACY='1' does NOT help: it changes the value returned,
 *   not the number of reads.
 * WHAT:
 *   (a) DT10_PROP_MEMO_ (per-execution object) + dt10PropMemoGet_(key,
 *       reader) / dt10PropMemoReset_(). Apps Script gives every execution a
 *       fresh global scope, so the memo lives exactly one run.
 *   (b) dt10W52FieldsLegacy_ reads its property ONCE per execution;
 *       dt10PoolFieldsActive_() (no-arg form) returns ONE cached list per
 *       execution; dt10SeatTruthOn_ reads its property once per execution
 *       (it ran once per qualified row). Explicit-argument calls
 *       dt10PoolFieldsActive_(true|false) stay pure and unmemoized.
 *   (c) refreshDecisionTop10 resets the memo at entry (fresh reads per run,
 *       no cross-run state even if the runtime ever reused a scope).
 *   (d) dt10SelfTest gains 'property memo core: ok (...)'.
 *   Zero behavioural change for any property value: the same flags produce
 *   the byte-identical pool, body, board and KPIs; only the service-call
 *   count changes (harness: base reads == rows + 4, delivered reads == 1).
 *   No ENV, no YAML, no arming. Rollback = re-paste v1.11.12 (f30bddd3...)
 *   or v1.11.11 (0112efbd...).
 * EXPECT after paste: dt10SelfTest prints the new line; one manual cockpit
 *   refresh completes with a 'decision-cockpit refresh completed' row whose
 *   durationMs is back in the 60-200 s band; no ABORTED_PREV row at the
 *   next auto run.
 * ============================================================================
 *
 * ============================================================================
 * v1.11.12 (2026-10-04) [P-181b / P-149 / P-195 / SYNC-INFLIGHT] -- COCKPIT TRUTH
 * ----------------------------------------------------------
 * WHY (Monitoring Sheets #6/#7, exports 2026-10-02 .. 10-04):
 *   (a) P-181b -- opportunity_builder v1.23.0's two-sided 52W timing gate
 *       (TFB_T10_W52_TIMING, observe since 10-01) printed
 *       '[w52-observe] n/a (no 52W / change fields)' on 500/500 audited
 *       rows while the SAME names carry 52W High / 52W Low / 52W Position %
 *       / Percent Change on Global_Markets (RDN 6.6 %, NVDA 92.1 %, DDI
 *       98.3 % of range). The pool contract DT10_POOL_FIELDS never sent the
 *       four fields -- the v1.6.1 (Last Updated) / v1.8.1 (Forecast Source)
 *       class exactly: a gate starved of its input by this projection.
 *   (b) P-149 (GAS half) -- ALL QUALIFIED 'Why Not Selected' read 'ranked
 *       below the Max Selected cut' for AER.US / KRP.US (held names,
 *       structural_block=true, failure_reason 'Portfolio: held ...') on the
 *       10-04 board. The v1.8.8 G-b branch that prints the real reason sat
 *       behind DT10_V188_SEAT_TRUTH = false since 2026-08-11 (never flipped);
 *       the same toggle gates G-a, the seat-truth KPI cell ('E exec + P pend
 *       + G grace / M') that would have read '0 exec + 0 pend + 2 grace / 10'
 *       instead of 'Selected 4 / 10' beside 0 executable tickets.
 *   (c) P-195 -- the 2026-10-03 17:06:29 cockpit run wrote 'kicking off' and
 *       never a completion or error row (the 21:10 run became the frozen
 *       weekly board). A GAS execution killed at the 6-minute limit leaves
 *       no trace in _Run_Log; nothing on the sheet says a run was lost.
 *   (d) SYNC-INFLIGHT -- the 2026-10-03 13:05 cockpit ran while daily_sync
 *       run 37114669145 was still writing (Global_Markets completed 13:48:28,
 *       76 s after the cockpit's verdict read); the board was built on a
 *       mixed epoch (Market_Leaders / Commodities_FX new, Mutual_Funds /
 *       Global_Markets old) and withheld 'aged:GM'. The _Sync_Control write
 *       lease covers only the 3-minute page write, not the 40-minute leg.
 * WHAT:
 *   (a) DT10_POOL_FIELDS gains '52W High', '52W Low', '52W Position %' and
 *       'Percent Change' (builder aliases w52_high / w52_low /
 *       w52_position_pct / pct_change_1d, v1.23.0 _FIELD_ALIASES). Kill:
 *       Script Property DT10_P181B_W52_LEGACY = '1' restores the v1.11.11
 *       projection (dt10PoolFieldsActive_ filters the four sends). EXPECT on
 *       the first refresh after paste: '[w52-observe] pos N% | 1d +x%' on
 *       every row and meta.timing_gate.evaluated > 0 -- the gate seeing.
 *   (b) DT10_V188_SEAT_TRUTH flips to true behind dt10SeatTruthOn_(); kill:
 *       Script Property DT10_SEAT_TRUTH_LEGACY = '1' (v1.8.7 rendering).
 *       Display only: selection, gates, tickets, clocks untouched.
 *   (c) Run marker: refreshDecisionTop10 stores Script Property
 *       DT10_RUN_MARKER = {started, v} at entry and clears it on every
 *       return path. If a marker older than DT10_P195_LIMIT_MIN (7 min, the
 *       GAS limit + skew) is still present at the next entry, ONE _Run_Log
 *       row 'ABORTED_PREV' names the lost run (P-195). A marker younger
 *       than the limit (a concurrent run) is overwritten silently. Kill:
 *       DT10_P195_MARKER_LEGACY = '1'. Log-only; no decision changes.
 *   (d) Sync in-flight detector: PURE dt10SyncInflightCore_ compares the
 *       _Status 'TFB Feed Market_Leaders' (first leg) and 'TFB Feed
 *       Global_Markets' (last leg) stamps: different run ids AND the first
 *       leg newer than the last AND the first leg younger than
 *       DT10_INFLIGHT_MAX_MIN (90) => a sync run is between its legs.
 *       Mode (Script Property DT10_SYNC_INFLIGHT): 'off' | 'observe'
 *       (DEFAULT -- status token 'inflight=run <id> ML..GM pending Nm' only
 *       when active, run proceeds byte-identically otherwise) | 'enforce'
 *       (status 'HELD', _Status 'HELD', one _Run_Log row, no POST, no clock
 *       advance -- the board keeps its last render). A failed run leaves
 *       the first leg newer for hours: the 90-minute bound ends the hold;
 *       P-144 keeps the clocks frozen on the stale epoch regardless.
 *       Weekly-skipped pages (MF/CFX) never enter the test (first/last
 *       legs only).
 *   (e) 'T10: Max Per Sector' built-in default 2 -> 3 (operator decision
 *       2026-09-10: 3 names / 40 %). The LIVE panel cell is an operator
 *       input preserved by P-76 -- type 3 into it; the default only governs
 *       re-seeds. 15_Lists_Config's TFB_PANEL_DEFAULTS may still carry 2.
 *   dt10SelfTest gains 'w52 projection core', 'sync inflight core', 'run
 *   marker core', 'seat truth' and a live 'sync in-flight now' line.
 * ES5. ZERO functions removed; TWELVE added (dt10SeatTruthOn_,
 * dt10W52FieldsLegacy_, dt10PoolFieldsActive_, dt10FeedStampParse_,
 * dt10SyncInflightCore_, dt10SyncInflightMode_, dt10SyncInflight_,
 * dt10RunMarkerCore_, dt10RunMarkerLegacy_, dt10RunMarkerCheck_,
 * dt10RunMarkerClear_, dt10RunLogRow_); two loops
 * re-pointed to dt10PoolFieldsActive_; two DT10_V188_SEAT_TRUTH reads
 * routed through dt10SeatTruthOn_; five return paths clear the marker.
 * DT10_VERSION and the header bumped in lockstep.
 * ============================================================================
 *
 * ============================================================================
 * v1.11.11 (2026-09-26) [P-144] -- EPOCH-KEYED STABILITY CLOCKS
 * ----------------------------------------------------------
 * WHY (runs 36199188352 / 36231358321, 2026-09-26): GitHub fired the 20Z
 * sync slot at 22:59Z (3 h late) and the 04Z slot at 09:00Z (5 h late).
 * The stability clocks are keyed on the WALL-CLOCK UTC date, so the first
 * cockpit run after 03:00 Riyadh advanced every clock on a Global_Markets
 * epoch that was 9 hours old, while the board itself was withheld as
 * `aged:GM` -- the P-144 mixed-epoch class (2026-09-14/15/16/22/23 boards)
 * now happening EVERY morning. A day-advance on stale evidence is not an
 * observation; the clocks were counting it as one.
 * WHAT: the day-key handed to dt10StabCore_ becomes the UTC date of the
 *   latest Global_Markets FEED STAMP (`TFB Feed Global_Markets` in
 *   _Status L:M, written by run_dashboard_sync after each leg) instead of
 *   the wall-clock date. Pure dt10StabResolveEpoch_(feedRaw, stateDate,
 *   todayKey) -> { key, src, held }:
 *   - feed state OK / PARTIAL / PARTIAL_FRESH / STALE_COV (rows were
 *     written) and a readable stamp => key = the stamp's UTC date (an
 *     explicit offset is honoured; a naive stamp is read as Riyadh, the
 *     producer's convention), src 'feed';
 *   - feed SKIPPED / FAILED / missing / unreadable => no new evidence:
 *     key = the state's own date (clocks frozen), src 'held';
 *   - never backwards (key < state.date => state.date) and never in the
 *     future (key > wall-clock => wall-clock);
 *   - dt10ApplyStability_ reads the key with the existing _Status L:M
 *     reader (new keyed variant dt10UvReadKeyed_), FAIL-OPEN to the
 *     wall-clock date on any service error, and appends a countable
 *     `epoch=<key>/<src>` token to the status note ('/held' = frozen).
 *   Effect: an aged-feed morning run FREEZES every clock (the same-day
 *   re-run path, byte-identical), the first run on the fresh epoch
 *   advances once, three legs in one UTC day still advance once, and a
 *   day with no refreshed epoch (quota exhausted, legs skipped) records
 *   no observation. Composes with v1.11.10: poisoned rows on a FRESH
 *   epoch are paused per symbol; a stale epoch is frozen for all.
 *   dt10SelfTest gains 'epoch key core: ok'.
 * DEFAULT ON (the OFF state IS the defect -- P-168/P-145 precedent).
 * KILL: Script Property DT10_P144_EPOCH_KEY_LEGACY = '1' restores the
 * wall-clock key byte-for-byte (dt10EpochKeyLegacy_, the
 * dt10OutagePauseLegacy_ property pattern, never throws).
 * SCOPE CUT (stated): the trigger time itself (08:10/08:30 Riyadh) and the
 * feed-age withholding are untouched; the release-epoch stamp on the
 * payload (P-144 full form) remains a backend item.
 * Zero rows/columns/schema changes; ES5; all prior WHYs verbatim;
 * DT10_VERSION and the header bumped in lockstep.
 * ============================================================================
 *
 * ============================================================================
 * v1.11.10 (2026-09-26) [P-168] -- OUTAGE-EPOCH CLOCK PAUSE
 * ----------------------------------------------------------
 * WHY (live boards 2026-09-26, 02:36 vs 06:41 Riyadh; six-gate audit and
 * the external audit adjudicated the same morning): the 20Z sync's
 * recovery replay ran into an EXHAUSTED EODHD quota and wrote 6,302
 * Global_Markets rows tagged `fetch_failed:HTTP 402` (DQ capped 55, row
 * BLOCKED). The day-advance cockpit run (UTC date key) then read that
 * pool: none of the four seats qualified and none sat inside T10
 * rank_buffer (they were not even in the audited 500), so every seat
 * took a miss -- ITRN 0->1/3, NVDA 0->1/3, PINFRA 1->2/3, 2222.SR
 * 1->2/3 -- and one more poisoned epoch would have soft-exited two of
 * them. The 2026-09-24 02:01 board did exit ITRN/CRC/PINFRA/ADAM on the
 * same storm class. A data outage is not a market verdict; the clocks
 * were consuming it as one.
 * WHAT: (a) pure dt10OutageMapFromPool_(rows): the sheet pool rows the
 *   cockpit already POSTs carry the engine's own 'Warnings' cell; a row
 *   matching DT10_P168_OUTAGE_RE (/fetch_failed/i) marks its symbol as
 *   an OUTAGE sighting. (b) dt10StabCore_ gains an optional 7th arg
 *   `outage` (plain map; the core stays pure): on a day-advance, a
 *   symbol with a clock (member, or challenger with ci>0), ABSENT from
 *   raw and marked outage, is PAUSED -- ci/co untouched, no score point
 *   pushed, hardOut/soft-exit skipped, state.op (consecutive paused
 *   day-advances) +1; a non-outage day-advance resets op to 0. Bounded:
 *   after DT10_P168_MAX_PAUSE_DAYS (5) consecutive pauses the miss
 *   counts again, so a permanently dead symbol (404 forever) still
 *   leaves the board. Same-day re-runs stay frozen as before.
 * (c) Disclosure: the GRACE label gains ' - outage pause n/5' (paint and
 *   the SelLog marker key on the 'GRACE' prefix; additive), ghost notes
 *   say why the clock did not advance, a countable _p168_paused marker
 *   rides the ticket, the status note gains 'N outage-paused',
 *   audit.outage_paused lists them, held_by_grace entries carry
 *   outage_pause. (d) dt10ApplyStability_ takes the map as an optional
 *   3rd arg; refreshDecisionTop10 builds it ONCE from the collected pool
 *   (null when the pool is not sheet-sourced -> v1.11.9-identical).
 * (e) dt10StabParseState_ reads `op` (written only when > 0, so an
 *   unused blob stays byte-identical). (f) dt10SelfTest gains
 *   'outage pause core: ok'.
 * SCOPE CUT (stated): the ADD-confirmation counter half of P-168 lives
 *   in portfolio_actions (Python) and is not touched here. Feed-level
 *   (aged/withheld) epochs are P-144's scope, deliberately not folded
 *   in: pausing every clock on every raced morning would stall the
 *   board's hysteresis.
 * DEFAULT ON (the OFF state IS the defect -- P-145/P-142 precedent).
 * KILL: Script Property DT10_P168_OUTAGE_PAUSE_LEGACY = '1' restores the
 * v1.11.9 clocks byte-for-byte (dt10OutagePauseLegacy_, the
 * dt10GraceSizingLegacy_ property pattern, never throws).
 * Zero rows/columns/schema changes; ES5; all prior WHYs verbatim;
 * DT10_VERSION and the header bumped in lockstep.
 * ============================================================================
 *
 * ============================================================================
 * v1.11.9 (2026-09-16) [P-145] -- GRACE SIZING SUSPENSION (SRC-PRESENT LEG)
 * ----------------------------------------------------------
 * WHY (live 08:07 board, 2026-09-16; independent external audit, same
 * day, adjudicated): TSM.US rendered Stability 'GRACE (1/3 missed)' AND
 * a full 13,976 SAR / 9-sh INVEST ticket at rank 3. Mechanism, pinned
 * on this source: the GRACE status branch in dt10StabCore_ fires for
 * `!rawSet[sym] || co>0`, and its src-PRESENT case (the seat qualifies
 * TODAY while a prior day's miss counter is still >0) had NO suspension
 * leg -- the full builder plan copied at the ticket seam rode through.
 * Ghosts (absent seats) were always blank by construction; both
 * FAST-TRACK branches suspend explicitly; only this leg leaked.
 * Reachable exactly as on 2026-09-16: a day-advance run misses the name
 * inside T10 rank_buffer (exit clock PAUSES, co stays 1), then a
 * same-day re-run (clocks FROZEN) sees it re-qualify -- label says
 * GRACE, plan says INVEST. Downstream the two count bases split:
 * heldByGrace said '5 grace' while the banner said
 * '1 EXECUTABLE + 4 GRACE-HELD'.
 * WHAT: (a) new pure dt10GraceSuspend_(tk, status) -- the
 *   dt10FastTrackSuspend_ dash vocabulary on the seven sizing fields +
 *   funds_from copy-on-write (P-77 class), PLUS _grace_hold = true so
 *   every existing grace surface applies unchanged (D-4 rank dash,
 *   paint, the seat classifier's grace count, SelLog seated marker; the
 *   _Selection_Log row keeps its numeric rank, D-4). A countable
 *   _p145_suspended marker is stamped for read-back.
 * (b) the GRACE branch calls it when src exists. (c) dt10SelfTest gains
 *   'grace sizing core: ok'.
 * DEFAULT ON (the OFF state IS the defect -- P-142/P-127 precedent).
 * KILL: Script Property DT10_P145_GRACE_SIZING_LEGACY = '1' restores
 * v1.11.8 behavior byte-for-byte (dt10GraceSizingLegacy_, the
 * dt10FundingWithholdLegacy_ property pattern, never throws).
 * Doctrine restored, not invented: v1.6.7 'A GRACE-HOLD IS NOT AN
 * EXECUTABLE TICKET'. Zero rows/columns/schema changes; ES5; all prior
 * WHYs verbatim; DT10_VERSION and the header bumped in lockstep.
 * ============================================================================
 *
 * ============================================================================
 * v1.11.8 (2026-09-15) [P-142] — FUNDING-NARRATIVE CONTAINMENT (WITHHELD)
 * ----------------------------------------------------------
 * WHY (live 08:08 board, 2026-09-15; independent external review, same
 * day): with 'FEED NOT ACTIONABLE — SIZING WITHHELD' on screen, the
 * ALERTS table still solicited funding ('capital_call: Deposit >= 37,650
 * SAR to take the top-3 qualified ticket(s)...', 'unfunded_candidates:
 * ... increase Cash Available') and the NEAR MISS Funding rows still
 * printed 'CAPITAL_CALL: deposit >= 5,000 SAR for a 5,000 SAR ticket
 * (cash 50 SAR)' — figures the backend's internal seat-funding pass
 * computed against allocations the same gate had suppressed. The
 * v1.11.5/6 withheld-truth pass covers the KPI strip only; funding
 * NARRATIVES leaked around it.
 * WHAT: one render-side containment gate at the alerts/near-miss seam.
 * When dt10OutputStatus_(payload) === 'WITHHELD':
 *   (a) alerts of type capital_call / unfunded_candidates are dropped;
 *   (b) NEAR MISS rows whose failed gate is Funding (or whose text
 *       carries a CAPITAL_CALL/deposit solicitation) keep symbol /
 *       failed gate / verdict, and current / required / improve_note
 *       are replaced by withheld-honest text;
 *   (c) ONE disclosure alert 'funding_withheld' with the suppressed
 *       count is appended — the countable read-back; absent on days
 *       with nothing to suppress.
 * EVERY other state (EXECUTABLE / HELD / QUALIFIED_UNFUNDED / EMPTY /
 * UNKNOWN) renders byte-identically — HELD and QUALIFIED_UNFUNDED are
 * actionable-feed states where funding information is legitimate
 * operator input (deliberate scope cut, recorded on the commit sheet).
 * The payload is never mutated (pure copies only); the backend, the
 * Selection Log and the status line are untouched — compute-level
 * containment of the seat-funding pass itself is F02a/F15 scope.
 * Idempotence hardened: an already-transformed row (required starts
 * 'WITHHELD —') passes through uncounted, and the disclosure row is
 * never duplicated.
 * DEFAULT ON, kill switch: Script Property
 * DT10_FUNDING_WITHHOLD_LEGACY = '1' restores v1.11.7 rendering
 * verbatim (the OFF state IS the defect — P-127/P-130 default-ON
 * precedent; operator veto preserved). Pure core
 * dt10ContainFundingCore_ is self-tested (dt10SelfTest line
 * 'funding containment core'). Zero rows/columns/schema changes.
 * ============================================================================
 * v1.11.7 (2026-09-12) [P-134] — COCKPIT CASH SOURCE-OF-TRUTH (gated OFF)
 * ----------------------------------------------------------
 * WHY (live, 2026-09-12): the cockpit sized its CAPITAL_CALL against the
 *   panel's operator-entered 'Cash Available (SAR)' = 9,026 while the
 *   Portfolio_Decision page carried the fresh 23,242.50 the SAME day —
 *   a 14,216 SAR divergence between two operator-maintained cells. The
 *   v1.11.3 P-76 preserve fix works exactly as designed: it faithfully
 *   preserves a STALE entry across layout rebuilds. Nothing reconciles
 *   the two surfaces, and the cockpit's number is the one that sizes
 *   tickets and issues deposit calls.
 * FIX: Script Property DT10_CASH_SOURCE = panel (absent/default, byte-
 *   identical) | observe | portfolio. observe: read the PF page's
 *   'PF: Cash Available SAR' cell (label-scan, value in the cell to the
 *   right, top 15x30 block, fail-open) and DISCLOSE the delta in the
 *   status line while still sending the panel value. portfolio: send the
 *   PF value (panel becomes fallback when the PF cell is unreadable),
 *   delta still disclosed. Pure core dt10CashScan_ + dt10CashChoose_ are
 *   self-tested against the LIVE exported PF row shape ('PF: Cash
 *   Available SAR' | '23,242.50'). ES5. Added: dt10CashSourceMode_,
 *   dt10CashScan_, dt10CashFromPortfolio_, dt10CashChoose_; changed
 *   additively: the refresh payload block, the status suffix,
 *   dt10SelfTest. Removed: 0.
 * ============================================================================
 * v1.11.6 (2026-09-08) [K-1b] — WITHHELD-TRUTH KPI PASS MOVED TO THE
 *                       VERDICT STASH POINT (one-site relocation; ALL
 *                       v1.11.5 logic carried verbatim; [K-2] unchanged)
 * ----------------------------------------------------------
 * WHY (live S6, 2026-09-08 00:28 board): v1.11.5 [K-1] evaluated
 * dt10OutputStatus_ at the KPI write, which executes BEFORE the render
 * reads the upstream verdict -- payload._dt10_uv was not yet stashed and
 * meta.upstream_verdict is a sheet-side key the backend never sends, so
 * the classifier fell through the regex fallback to HELD and the raw
 * strip rendered under the withheld banner (Exp. Gain 864 / Selected
 * 1 / 10 / Fundable 1). The v1.11.5 harness proved the pure functions
 * but fed the verdict through the fallback path, so it validated state
 * COMPUTATION, not state AVAILABILITY at the write site -- the exact gap
 * a live observe cycle exists to catch. [K-2] (FX/Price-SAR withhold)
 * was confirmed live on the same board: all ten withheld seats,
 * including the GLNG-class real ticket, render em dashes.
 * WHAT: the first KPI write reverts to the v1.11.4 raw line; ONE
 * corrective truth pass now sits immediately after
 * payload._dt10_uv = dt10Uv (kpiRange is same-function scope), gated on
 * dt10Uv.state !== EXECUTABLE, DT10_KPI_TRUTH (default ON, fail-open)
 * and dt10OutputStatus_(payload) === WITHHELD -- which at this point
 * reads the stashed authoritative verdict. EXECUTABLE feeds never take
 * the second write; withheld boards briefly hold the raw strip for the
 * milliseconds between the two setValues, then state the truth.
 * ES5. ZERO functions added or removed; ONE call site reverted, ONE
 * inserted; DT10_VERSION and the header bumped in lockstep.
 *
 * v1.11.5 (2026-09-07) [K-1/K-2] — WITHHELD-TRUTH KPI STRIP + PRICE-SAR/FX
 *                       JOIN THE WITHHOLD SET (two coupled render fixes;
 *                       ALL v1.11.4 logic carried verbatim, zero payload
 *                       or gate changes, S8 honored)
 * ----------------------------------------------------------
 * WHY (2026-09-07 six-gate audit + external red-team, both adjudicated on
 * the live export): under the banner 'FEED NOT ACTIONABLE ... 0 EXECUTABLE
 * ... SIZING WITHHELD' the KPI strip still printed funded claims --
 * Selected 1 / 10, Exp. Gain 12M 1,854 SAR, Fundable Now 1, Capital Call
 * 59,377 -- and one withheld seat (GLNG.US, a real sized ticket this run,
 * unlike the grace GHOSTS whose payload already carries dashes) leaked
 * Price SAR 195.60 while the eight ghost rows showed an em dash: the R3
 * withhold index list starts at Entry Zone (9) and never covered FX (6) /
 * Price SAR (8), so only rows that arrived pre-populated could leak.
 * WHAT:
 * [K-1] dt10KpiTruthEnabled_ (ScriptProperty DT10_KPI_TRUTH, default ON,
 *       fail-open) + pure dt10KpiWithheldValues_; the KPI write becomes
 *       state-aware via the EXISTING dt10OutputStatus_ classifier. On
 *       WITHHELD: Exp. Gain / Unallocated / Fundable Now / By Rotation /
 *       Capital Call render an em dash and Selected renders '0 / N
 *       (withheld)'; Deployable, Blended Reliability, Blended R/R,
 *       Scanned, Passed stay real (pool/research facts, not order
 *       claims). EXECUTABLE / HELD / QUALIFIED_UNFUNDED / EMPTY boards
 *       render byte-identically to v1.11.4. payload.kpis is NEVER
 *       mutated: SEAT-CHECK keeps comparing raw backend kpis vs the
 *       board, so the kpi-vs-exec discrepancy detector stays alive.
 *       v1.8.8 seat-truth (when enabled) still overwrites the Selected
 *       cell AFTER the mask -- seat truth is truth; ordering unchanged.
 * [K-2] DT10_UV_BOARD_WITHHOLD_IDX gains 6 (FX->SAR) and 8 (Price SAR);
 *       DT10_UV_LOG_WITHHOLD_IDX gains the +3-offset twins 9 and 11. The
 *       withholder is already idempotent (ghost dashes re-dash); the only
 *       visible ghost change is FX '' -> em dash, which makes every
 *       withheld row read identically. Price (7) stays: instrument
 *       identity, not sizing.
 * ADJUDICATED OUT (Register, backend): the near-miss 'Current 1.2%' unit
 * leak is a payload string (this file applies NO number format to that
 * table and the zone is clear()ed each run) -- opportunity_builder item,
 * not a cockpit edit.
 * ES5. ZERO functions removed; TWO added (dt10KpiTruthEnabled_,
 * dt10KpiWithheldValues_); ONE call site edited; TWO index literals
 * extended; DT10_VERSION and the header bumped in lockstep.
 *
 * v1.11.4 (2026-09-05) — P-78 / P-80: HOLDINGS REACH THE PORTFOLIO GATE IN
 *                        SAR; THE STAB NOTE COUNTS LIVE CARRIES ONLY
 * ----------------------------------------------------------------
 * EVIDENCE (Top_10 alerts 14:36 and 15:03): "RPC.US is fundable by exit of
 *   PFS 119 SAR" — the PFS position is USD 118.60 = SAR 445. My_Portfolio
 *   carries no SAR-labelled value column, so dt10CollectHoldings_ (v1.5.0
 *   Fix H1) fell back to the raw Position Value and sent it as value_sar:
 *   every USD holding reached the builder's Portfolio gate at 1/3.75 of its
 *   SAR value (HCI 5,629 instead of 21,126), skewing the sector-cap context,
 *   the rotation proceeds and the proposal text. The exclusion itself
 *   (symbol-only) was never affected.
 * FIX (P-78, pure, ES5): dt10CollectHoldings_ also locates a Currency column
 *   and — ONLY when the value column is native (no SAR-labelled column) —
 *   hands dt10HoldingFromRow_ the cockpit's own FX map (dt10FxRates_, the
 *   TFB_FX_LOOKUP named range the ticket sizer already uses). A native value
 *   in a non-SAR currency is multiplied by SAR-per-unit; SAR rows, rows
 *   without a currency, and currencies missing from the map stay as before
 *   (counted as unconverted in the Logger line). Payload schema unchanged.
 * FIX (P-80, cosmetic): the stab note's "N ft-carried" counts only seats
 *   that are ft=true AND live in today's raw list with co=0; a grace-held
 *   ft seat is a GRACE seat in the note (the audit list keeps them all).
 * TOGGLE: DT10_V1114_HOLDINGS_FX (default ON; false => v1.11.3 payload).
 * Changed additively: dt10HoldingFromRow_ (optional extra.iCcy/extra.fx),
 *   dt10CollectHoldings_, dt10StabCore_ (one predicate), dt10SelfTest;
 *   removed: 0.
 *
 * ============================================================================
 * v1.11.3 (2026-09-05) — P-76 / P-77: THE LAYOUT BUILDER KEEPS OPERATOR
 *                        INPUTS; A SUSPENDED SEAT DISCLOSES NO FUNDING
 * ----------------------------------------------------------------
 * EVIDENCE (live, 2026-09-05 14:32–14:36): buildDecisionTop10Layout() was
 *   run once ("layout built"); it sheet.clear()s and re-seeds EVERY panel
 *   cell from _Lists_Config / the built-in defaults, so 'Cash Available
 *   (SAR)' went from the operator's 3,825 to the built-in 100,000. The next
 *   two refreshes sized against phantom capital: Deployable 100,000,
 *   Selected 5/10, Capital Call 34,007, four ~19.5k tickets seated as
 *   fast-track fills, AEFES.IS pushed to GRACE. Same board: the four
 *   suspended seats showed Ticket/Shares '—' but 'Funds From: Cash 19,544
 *   SAR' — dt10FastTrackSuspend_ (v1.8.10) never blanked detail.funds_from,
 *   the BC-4 leak class the Python selector closed in v4.28.0/v4.29.0.
 * FIX (P-76): dt10PanelPreserve_ snapshots every NON-BLANK operator value
 *   BEFORE the clear; dt10PanelSeedValue_ (pure) picks preserved > defaults
 *   > built-in per cell; the builder logs how many inputs it kept. A fresh
 *   sheet (no panel yet) seeds exactly as before.
 * FIX (P-77): dt10FastTrackSuspend_ blanks detail.funds_from on a COPY of
 *   detail (the raw payload ticket and its shared detail stay untouched);
 *   the board row and the _Selection_Log row both read the copy.
 * TOGGLES (kill-switches, default ON): DT10_V1113_LAYOUT_PRESERVE,
 *   DT10_V1113_SUSPEND_FUNDS. Added: dt10PanelPreserve_, dt10PanelSeedValue_;
 *   changed additively: dt10FastTrackSuspend_, buildDecisionTop10Layout,
 *   dt10SelfTest; removed: 0.
 *
 * ============================================================================
 * v1.11.2 (2026-09-05) — P-61 / P-62 / P-67: A FAST-TRACK SEAT STAYS
 *                        WITHHELD UNTIL CONFIRMED; EVERY OUTPUT-STATE
 *                        CHANGE IS LOGGED; THE MORNING TRIGGER LOGS A RUN
 * ----------------------------------------------------------------
 * EVIDENCE (2026-09-05 export, _Selection_Log): AEFES.IS 03:40
 *   'FAST-TRACK (day 1) — sizing suspended under strict until confirmed
 *   (... not an executable ticket today)' (output HELD) -> 07:46
 *   'ACTIVE (day 1)' (WITHHELD only because the GM feed was aged) -> 08:08
 *   EXECUTABLE 3,824 SAR / 2,693 sh with 'T10: Stability Confirm Days' = 3
 *   and ci = 1; the 08:08 transition read 'SelLog: unchanged'.
 * ROOT CAUSE (dt10StabCore_): the fast-track fill seats the symbol as a
 *   FULL member (member=true, since=today) with no memory that it was
 *   fast-tracked; the 'FAST-TRACK (day 1)' label and the v1.8.10 sizing
 *   suspension key on THIS run's fastTracked list only. Every later run
 *   (same day or next) finds member=true, seats it as a survivor, labels
 *   it 'ACTIVE (day n)' although ci < confirm_days, and copies the
 *   builder's FULL plan -> EXECUTABLE on an unconfirmed seat. (The same
 *   defect existed in core/analysis/top10_selector.py's twin layer, fixed
 *   there as v4.31.0 [BC-6]; the cockpit path is THIS file.)
 * FIX (P-61, pure, ES5): per-symbol state gains `ft` (true while the seat
 *   is an unconfirmed fast-track). Set at seating time to (symbol was
 *   fast-track filled this run); cleared the first run on which
 *   ci >= confirm_days (graduation). A persisted, unconfirmed seat reads
 *   'FAST-TRACK (day n, ci/confirm confirmed)' — same prefix, so
 *   dt10SelLogSignature_ / dt10TicketClasses_ / output truth see it as
 *   before — and, under strict, receives the SAME dt10FastTrackSuspend_
 *   it had on day 1 (label passed through; default text byte-identical).
 *   GRACE keeps precedence; ACTIVE only after graduation. Membership,
 *   order, clocks, hist, exits, displacement: UNTOUCHED. Audit gains
 *   fast_track_unconfirmed; the stab note gains 'N ft-carried'.
 * FIX (P-62): dt10SelLogSignature_(tickets, outputState) — the signature
 *   also carries the board's OUTPUT state (dt10OutputStatus_), so a
 *   HELD -> WITHHELD -> EXECUTABLE change is logged even when membership
 *   and per-seat tokens are unchanged (the tickets' suggested_shares never
 *   reflect the feed withholding; the rendered rows do).
 * FIX (P-67): tfbMorningCockpitRefresh writes ONE _Run_Log row
 *   (INFO|tfbMorningCockpitRefresh|Top_10_Investments) via
 *   dt10MorningRunLog_ (10-column house format; never throws; ERROR row on
 *   failure) — the 08:07 run was invisible outside Logger.
 * TOGGLES (kill-switches, default ON): DT10_V1112_FASTTRACK_PERSIST,
 *   DT10_V1112_SELLOG_OUTPUT_SIG, DT10_V1112_MORNING_RUNLOG. State blobs
 *   without `ft` parse as false (pre-existing members keep their labels).
 * Added: dt10MorningRunLog_; changed additively: dt10StabParseState_,
 *   dt10FastTrackSuspend_ (optional label), dt10StabCore_,
 *   dt10SelLogSignature_ (optional outputState), dt10AppendSelectionLog_
 *   (optional outputState), refreshDecisionTop10 (passes it),
 *   tfbMorningCockpitRefresh, dt10SelfTest; removed: 0.
 *
 * ============================================================================
 * v1.10.1 (2026-08-25) — DECLARE THE FEED STATE ON EVERY RENDER
 * ----------------------------------------------------------------
 * Re-run #3607 went red the FIRST time the feed turned EXECUTABLE:
 * validate_dashboard's decision.feed_banner_present (exit-2 authority)
 * demands a FEED verdict line above the grid, and v1.9.x/v1.10.0 only
 * wrote one when the feed was BLOCKED — the healthy state rendered
 * silently. Fix: the EXECUTABLE branch now prefixes the section title
 * with '\u2705 FEED ACTIONABLE \u2014 EXECUTABLE (verdict age Nm) \u2014 ...',
 * carrying both finder tokens (FEED + ACTIONABLE) and never the blocked
 * phrase 'NOT ACTIONABLE'. Wording only — gate, parse, withholding and
 * the blocked branch are byte-identical. Residual (Register): with the
 * verdict gate property OFF the render is still bannerless.
 *
 * ============================================================================
 * v1.10.0 (2026-08-25) — R3: A WITHHELD BOARD MUST DISCLOSE NO ORDER
 * ----------------------------------------------------------------
 * WHY (two independent 24-Aug reviews + the six-gate export audit, all
 * confirmed on the 2026-08-24 board): under the v1.9.x blocked-feed gate
 * the banner said SIZING WITHHELD and Ticket/Shares read '—', yet the
 * SAME rows still published Entry Zone, Stop, TP1/TP2, TP1-basis and
 * annualized ROI, Gain 12M, Funds From ('Cash 6,251 SAR'-class) and an
 * Advisor Note that spoke the full order aloud — and _Selection_Log
 * appended every one of those fields verbatim (25 rows on 08-24).
 * Price + TP1-ROI reconstructs TP1 exactly, so ROI %(TP1) and Ann ROI
 * are part of the order surface, not analytics.
 * FIX (pure, ES5, fail-closed):
 *  (1) dt10UvWithholdRow_ — ONE pure withholder + two 0-based index
 *      maps: board [9,10,11,12,13,14,15,17,18,22] + Advisor Note 29;
 *      log [12,13,14,15,16,17,18,20,21,25] + Advisor Note 27. Engine
 *      ROI %, Rel, DQ, Conf, prices, identity and the stability block
 *      stay REAL (v1.8.10 doctrine). Idempotent over grace-ghost '—'.
 *  (2) dt10RenderPayload_ blocked branch withholds the FULL set
 *      (supersedes the two-cell v1.9.0 blanking; banner, gate and
 *      parse are byte-identical).
 *  (3) dt10AppendSelectionLog_ does its own fail-closed verdict read
 *      (same dt10UvOn_/Read_/Parse_ pattern) and withholds appended
 *      rows under any non-EXECUTABLE state — the log remains an audit
 *      trail of WHO was selected, never an order sheet.
 *  (4) dt10SelfTest gains a pure withhold check (board + log).
 * Cross-checked: validate_dashboard v1.3.0 P0-5 reads '—' sizing
 * cells — a superset of '—' is compatible; its banner finder keys on
 * FEED+ACTIONABLE tokens, untouched. The Verdict column stays engine
 * truth (precedence is W4 scope). The backend payload still carries
 * sizing in transport — sheet and log no longer disclose it; builder-
 * side suppression is queued in the Register.
 *
 * ============================================================================
 * v1.9.1 (2026-08-23) — IR-089: HONEST COUNT UNDER A BLOCKED FEED
 * ----------------------------------------------------------------
 * The 2026-08-23 independent review (Q1) caught the v1.9.0 banner
 * contradicting itself: '⛔ FEED NOT ACTIONABLE — ... — SELECTED —
 * 1 EXECUTABLE TICKET — SIZING WITHHELD'. Under a blocked feed NOTHING
 * is executable by definition; the count that matters is qualified
 * PLANS. v1.9.1 recounts the embedded title in the blocked branch only:
 *   'SELECTED — 0 EXECUTABLE / N QUALIFIED PLAN(S) [+ G GRACE-HELD
 *   (NO PLAN TODAY)]', wrapped as before by the ⛔ prefix and SIZING
 * WITHHELD suffix. Wording only — the gate, the '—' sizing blanking,
 * the fail-closed parse, and the EXECUTABLE branch are byte-identical.
 * Cross-checked against validate_dashboard v1.3.0's decision surface:
 * its banner finder keys on FEED+ACTIONABLE tokens (still present) and
 * its P0-5 tripwire reads the '—' sizing cells (unchanged).
 *
 * v1.9.0 (2026-08-21) — W1A-4a CONSUMER: UPSTREAM-FEED VERDICT GATE
 * --------------------------------------------------------------------------
 * WHY (six-gate audit P0-3, 2026-08-21): Global_Markets was 246 min stale
 *   at board consumption — past the 240-min W1A-4a window — and the board
 *   rendered normally with live ticket sizing. The sync side (v6.40.0)
 *   composes the verdict and publishes it to _Status L:M under the key
 *   'TFB Decision Feed', but NOTHING in this cockpit ever read it: the
 *   consumer half of W1A-4a was specified and never delivered. This build
 *   is that consumer, verbatim to the producer contract:
 *     value = '{EXECUTABLE|NOT_ACTIONABLE(reason)} | run=.. | ts | summary'
 *     ts    = '%Y-%m-%d %H:%M:%S' stamped on the UTC runner.
 *   BEHAVIOR (fail-closed at every branch):
 *   - dt10UvRead_ pulls _Status!L1:M60 once per render; dt10UvParse_ is
 *     PURE (harness-tested): missing key, unreadable timestamp, stale
 *     verdict (> DT10_UV_MAX_AGE_MIN = 480m) and NOT_ACTIONABLE(..) all
 *     resolve to NOT ACTIONABLE with a stated reason.
 *   - On NOT ACTIONABLE: the SELECTED section title is prefixed
 *     '\u26d4 FEED NOT ACTIONABLE \u2014 <reason>' and every selected row's
 *     Ticket SAR + Shares cells are withheld ('\u2014') so no sizing can be
 *     executed off a stale or unverified feed. Grace tint, stability
 *     colors, KPI strip, near-miss/alerts/candidates all unchanged.
 *   - Producer OFF => key absent => this gate reports 'no verdict
 *     published' and withholds sizing — arming order therefore matters:
 *     set repo Variable TFB_SYNC_UPSTREAM_VERDICT=1 in the SAME sitting
 *     as pasting this file (yml already maps it; Variable was never set).
 *   KILL-SWITCH: script property DT10_UPSTREAM_VERDICT='off' restores the
 *   v1.8.10 render path byte-identically. ES5 ONLY. Fail-open on internal
 *   errors is DELIBERATELY INVERTED here: a read/parse failure withholds
 *   sizing (fail-CLOSED) — that is the entire point of W1A-4.
 *
 * ============================================================================
 * v1.8.10 (2026-08-19) — FAST-TRACK SIZING SUSPENSION UNDER STRICT
 *                          + WARNINGS TRAVELS IN THE POOL POST
 * --------------------------------------------------------------------------
 * WHY (IR-094, 2026-08-17 board + external audit adjudicated 2026-08-19):
 * 151/151 fast-track fills carried full executable day-1 sizing —
 * including 19k-SAR tickets on 1050.SR/1060.SR that soft-exited days
 * later, and two tickets written by the 00:42 off-cadence run. The
 * stability doctrine already withholds sizing from GRACE ghosts
 * ("no plan today"); a day-1 unconfirmed fill under STRICT deserves the
 * same honesty. FIX: dt10FastTrackSuspend_ (new, pure) blanks the
 * executable plan ('—' vocabulary, v1.3.1 operator decision) on a
 * FAST-TRACK ticket when DT10_V1810_FASTTRACK_SIZING_SUSPEND (default
 * ON — protective class) AND knobs.hard_strict are both true; identity,
 * prices, ROI/RR, Rel/DQ stay real; Advisor Note states the withholding.
 * Ghost path untouched; strict disarmed => v1.8.9 byte-identical.
 * ALSO (TRUST-001 witness): DT10_POOL_FIELDS gains
 * { send:'Warnings', match:['warnings'] } — without it the source
 * engine's low_data_trust verdict never reaches opportunity_builder
 * v1.13.0's trust-lineage defense (counters would read 0 forever).
 * ONE function added (dt10FastTrackSuspend_), ONE var added, ONE branch
 * edited (fast-track status), ONE send-map entry appended; ZERO
 * functions removed.
 * --------------------------------------------------------------------------
 * v1.8.9 (2026-08-12) — SELLOG SIGNATURE SEES SEAT TRUTH, NOT JUST NAMES
 * --------------------------------------------------------------------------
 * FORENSIC WHY (2026-08-11): the day's two biggest board churns — runs
 * 12:53 (req eabc8e962bdb) and 14:24 (req a10e7cb52282), where funding,
 * executable seats and ranks flipped violently — both returned
 * 'SelLog: unchanged (logged today)'. Root cause: dt10SelLogSignature_
 * hashed the sorted MEMBER SYMBOLS only, and the stability layer
 * deliberately keeps membership sticky (grace holds members through
 * misses). So by construction the signature was blind to exactly the
 * churn an audit trail exists to record: WHO IS FUNDED and WHO IS
 * EXECUTABLE. (19:15 logged only because four hard-exits finally changed
 * membership; the morning run logged for the same reason.)
 * FIX: the signature now tokenizes each member as
 *   symbol ~ S|- (seated: numeric rank, not a grace ghost)
 *          ~ F|- (funded: suggested shares > 0)
 *          ~ STABILITY-CLASS first word (ACTIVE/FAST-TRACK/GRACE/…)
 * so any change in funding, executability or seat class logs a CHANGE
 * entry, while price/score wiggles still do NOT (no tick-spam). Same
 * once-per-day baseline, same 'always'/'off' modes, same 33-col rows.
 * ONE-TIME EFFECT: the first run after this paste logs one extra CHANGE
 * entry (old-format state vs new-format signature) — expected, harmless.
 * ZERO functions removed or added; ONE function body edited
 * (dt10SelLogSignature_) + version lockstep.
 * --------------------------------------------------------------------------
 * v1.8.8 — SEAT-TRUTH DISPLAY (G-a KPI cell + G-b structural reason)
 *          (2026-08-11) — TOGGLE-GATED, DEFAULT OFF
 * ------------------------------------------------------------------------------
 * OPERATOR FINDING (2026-08-11 board, run 09:18:48, req 23ee5d199004):
 *   (a) KPI cell 3 read 'Selected 6 / 10' while the board delivered
 *       3 executable tickets + 7 grace ghosts and 3 names sat stability-
 *       pending (1/3). Both numbers are true of DIFFERENT sets; the v1.8.0
 *       SEAT-CHECK names the gap in the STATUS line, but the KPI cell — the
 *       first thing read — still asserts the funded count as if deployable
 *       today. The operator asked "is the refresh broken?"; the machinery
 *       was fine, the presentation was not.
 *   (b) ARCO.US (qualified rank 4, structural_block=true, failure_reason
 *       'Portfolio: held vs exclude holdings') showed Why Not Selected =
 *       'ranked below the Max Selected cut'. Same class as BE-1/D-2: the
 *       mapper consulted deferral and the pending map but never the
 *       structural verdict the audit grid already carries, so a rank cut
 *       was asserted that never happened for that name.
 * WHAT (both additive, both behind ONE toggle):
 *   G-a  dt10SeatTruthKpi_(payload) — PURE; renders KPI cell 3 as
 *        'E exec + P pend + G grace / M' from payload.selected._grace_hold
 *        (the D-1 annotation, present at render time), the BE-1 pending
 *        source (meta.stability.audit.pending), and kpis.max_selected.
 *        Applied AFTER dt10KpiValues_ writes the strip; '' => no override.
 *        The backend funded count stays visible via SEAT-CHECK (S-5/S-6
 *        doctrine: verify, never rewrite, the backend number — this cell
 *        is cockpit presentation, not payload data).
 *   G-b  dt10QualToRow_ reason precedence gains a STRUCTURAL branch between
 *        deferral and stability-pending: structural_block === true =>
 *        failure_reason (fallback 'structural: <first_fail.gate>'). The
 *        rank-cut text can no longer be asserted for a structurally
 *        excluded name.
 * TOGGLE: var DT10_V188_SEAT_TRUTH — **false** by default; false =>
 *        v1.8.7 rendering byte-for-byte (S-1 window law: no silent display
 *        change; the operator flips it deliberately in the editor).
 * VERIFIED, NOT CHANGED: the status footer's 'route v4.16.0' is payload
 *        truth (meta.route.version = routes/advanced_analysis.py
 *        ADVANCED_ANALYSIS_VERSION '4.16.0' at HEAD) — an earlier
 *        suspicion that it was a stale tag is WITHDRAWN; no edit.
 * ES5. ZERO functions removed; ONE function added (dt10SeatTruthKpi_),
 * one var added (DT10_V188_SEAT_TRUTH), two insertion sites
 * (dt10RenderPayload_ KPI strip; dt10QualToRow_ reason chain).
 * ============================================================================
 * v1.8.0 — HARD VERDICTS CAN BEAT GRACE + GRACE ROWS UNMISTAKABLE (2026-08-08)
 * ------------------------------------------------------------------------------
 * OPERATOR EVIDENCE (2026-08-07 04:16 board; trades executed that morning):
 * ranks 6 and 9 were GRACE-held incumbents whose CURRENT audited verdicts
 * read DO_NOT_INVEST (UVV.US — also '⚠ earnings ≤0d' — and PFLT.US '≤4d');
 * the operator bought both. The v1.3.0 doctrine that plain DO_NOT_INVEST is
 * grace-holdable STAYS THE DEFAULT (that jitter absorption is the layer's
 * whole point); the strict mode is an explicit opt-in. Three seams:
 *   BE-2 STRICT HARD-EXIT (kill-switched, DEFAULT OFF): Script Property
 *        DT10_HARD_VERDICT_STRICT = 1/true/on/yes arms it; hardOut() then
 *        ALSO hard-exits an incumbent whose audited verdict is exactly
 *        DO_NOT_INVEST or BLOCKED. Mirrors backend top10_selector v4.26.0
 *        [BC-2] so the GAS-side stability layer (the LIVE one) and a
 *        future backend consolidation agree. The service read stays in
 *        dt10ApplyStability_ (knobs.hard_strict) so the pure core remains
 *        node-testable; the status note reads 'stab[strict]:' when armed.
 *   D-4  A GRACE RANK IS NOT A RANK: dt10TicketToRow_ renders '—' in the
 *        Rank cell of a _grace_hold row (_Selection_Log keeps the numeric
 *        rank — history unchanged); NEW dt10TintGraceRows_ paints the whole
 *        grace row in the WATCH amber tint before the Stability-cell
 *        coloring, so a grace row can never again read as pick #6.
 *   S-6  SEAT-CHECK TOKEN: NEW pure dt10SeatCheckNote_ compares
 *        kpis.selected_count (the backend's FUNDED picks) against the
 *        board's executable count and appends 'SEAT-CHECK kpi N funded vs
 *        board E exec (+G grace)' when they differ — the 2026-08-07 board
 *        said 'Selected 6/10' while rendering 1 executable + 9 grace, and
 *        nothing on the page reconciled the two sets.
 * ES5. ZERO functions removed; THREE added (dt10HardVerdictStrict_,
 * dt10TintGraceRows_, dt10SeatCheckNote_); TWO bodies extended (hardOut
 * strict branch, dt10TicketToRow_ rank cell) plus the orchestrator knob,
 * one render hook, one status token, and dt10SelfTest checks. With the
 * property unset the board is byte-identical to v1.7.1 except the grace
 * Rank '—' and row tint (display honesty, deliberately always-on — the
 * v1.6.7 D-1 precedent).
 *
 * ============================================================================
 * v1.7.1 — HONEST AUDIT HEADER (2026-08-05, display string only)
 * ------------------------------------------------------------------------------
 * The grid header read 'FULL AUDIT (N scanned)' where N is the WRITTEN row
 * count (TFB_OPP_AUDIT_ROWS_MAX cap, 300), not the scan size — the operator
 * read '300 scanned' twice today and reasonably concluded the system samples
 * a fraction of the universe. The backend scans kpis.scanned candidates
 * (2,000 on the 15:12 board) and writes the top-N audit slice (every
 * selected / qualified / near-miss row always kept; only the low-score tail
 * is trimmed — see opportunity_builder AUDIT-CAP). The header now says so:
 * 'FULL AUDIT — top N rows written (every selected / qualified /
 * near-miss row included; the low-score tail is trimmed — full scan
 * size is the Scanned KPI)'. Self-contained: no KPI object is in scope
 * in this renderer, so the header references the KPI by name instead
 * of embedding the number.
 * One display string; no data, ordering or selection change.
 *
 * ============================================================================
 * v1.7.0 — GRACE-ROW IDENTITY PERSISTENCE (2026-08-05)
 * ------------------------------------------------------------------------------
 * OPERATOR AUDIT (2026-08-05, 13:17 board): the five 12:29 executables were
 * grace-held after 62 eligible names were positionally cut by the backend's
 * TFB_OPP_MAX_CANDIDATES=1000 clamp (pregate 9824->elig 1062 kept 1000), and
 * FOUR of the five rendered with Name/Market/Sector/Ccy/Price completely
 * blank. Root cause is line-level: the ghost builder's fallback is
 * `candBySym[osym] || { symbol: osym }` — a symbol absent from today's
 * candidates_rows has no identity source at all, so the board shows a bare
 * symbol and the operator reads 'many missing information'. UVV.US kept
 * partial data only because it DID scan that run (sector-capped).
 *
 * FIX (display-only, three seams):
 *   1. Per-run state update: when today's audited grid carries the symbol,
 *      stash a tiny identity snapshot on its state entry —
 *      st.id = { n:name<=60, mk:market<=24, se:sector<=24, cy:ccy<=8,
 *      px:price } (~70 bytes/symbol; ~10-20 tracked symbols is <1.5 KB
 *      against the 9 KB ScriptProperties budget; the existing drop-oldest
 *      compactor in dt10StabSave_ already governs overflow).
 *   2. dt10StabParseState_ sanitizes st.id through the round-trip (each
 *      field re-coerced + re-truncated; malformed id -> null, never throws).
 *   3. The ghost fallback becomes candBySym[osym] ||
 *      dt10StabIdFallback_(ost, osym) || { symbol: osym }: identity fields
 *      render from the stash; ALL metric fields ('' -> blank) and plan
 *      fields ('—') stay honest — no stale ROI/reliability is ever shown
 *      as current. A never-scanned symbol still falls through to the bare
 *      object exactly as v1.6.9.
 * Selection, tickets, sizing, stability counters: byte-untouched.
 *
 * ============================================================================
 * v1.6.9 — 'T10: Require Investable' DEFAULT FLIPPED Yes -> No (2026-08-05)
 * ------------------------------------------------------------------------------
 * OPERATOR AUDIT (2026-08-05, 12:11 board): the Investability MAJOR gate was
 * the top blocker at 230 of 300 candidates (76.7%) — SON.LS score 79.5,
 * 4503.T, 6960.T, SIDO.JK all benched on WATCHLIST alone. Render had
 * TFB_OPP_INVESTABILITY_GATE=0 (verified in shell), yet the gate fired:
 * this panel's 'Yes' seed was the arming source, POSTed each run as
 * criteria.investability_gate_enabled=true, overriding the env default by
 * design (request criteria win).
 *
 * The backend retired this gate on 2026-07-24: opportunity_builder v1.7.0's
 * header records it "benched 9 legitimate names for every 1 it was right
 * about" and replaced it with the narrow Sell-Class gate
 * (TFB_OPP_SELL_CLASS_GATE, DEFAULT ON — verified), which MAJOR-fails only
 * an explicit engine sell-tier verdict. This file's 'Yes' seed predates
 * that decision (v1.2.7, 2026-07-05) and silently re-armed the retired
 * gate on every panel rebuild.
 *
 * CHANGES (v1.6.9): (1) DT10_VERSION 1.6.8 -> 1.6.9; (2) the panel seed
 * def flips 'Yes' -> 'No'; (3) the blank-cell fallback in the criteria
 * mapper flips 'Yes' -> 'No' so an unbuilt/blank cell no longer arms the
 * gate. The mapping logic itself is untouched: an operator typing Yes
 * still arms it (explicit opt-in preserved), leading 'n'/'N' disarms, and
 * the choice is still captured in the logged panel snapshot. Sell-Class
 * protection is unaffected. An EXISTING panel cell keeps its current
 * value — deploying this file does not edit the sheet; flip the cell (or
 * clear it) once.
 *
 * ============================================================================
 * v1.6.8 (Fix D-3) — 'ROI % (TP1)' COLUMN IS NOW ACTUALLY TP1-BASIS (2026-08-03)
 * ------------------------------------------------------------------------------
 * OPERATOR AUDIT (2026-08-03 board): the ALL QUALIFIED table printed 35.0%
 * for TRMD.US and 23.8% for HOPE.US under 'ROI % (TP1)' while their live
 * tickets computed 17.5% and 11.9% (TP1 ÷ entry − 1). Root cause: the row
 * mapper emitted the CANDIDATE payload's engine-basis roi_pct under a
 * TP1-labelled header. Fix: dt10QualToRow_ gains an optional ticketRoiMap
 * (normalized symbol → the ticket's own roi_pct); ticketed rows now show the
 * ticket's TP1 ROI, and unticketed qualifiers show '—' — no ticket means no
 * TP1 ROI exists, and the engine-basis figure remains one column to the
 * right ('Engine ROI % (12M)'). ES5; the extra arg is optional so any other
 * caller renders '—' safely rather than a wrong number.
 * AUDIT RETRACTION, for the record: the same review initially flagged a
 * TRMD risk-label mismatch (High vs Medium). The SELECTED table has no Risk
 * column — 'High' was the Conf band; qualified's 'Medium' is risk_level.
 * Both were correct; the finding is withdrawn. ZERO functions removed.
 * v1.6.7 SECTION HONESTY — A GRACE-HOLD IS NOT AN EXECUTABLE TICKET
 * ------------------------------------------------------------------------------
 * OPERATOR EVIDENCE (2026-07-27 morning board): the section header read
 * "SELECTED - EXECUTABLE TICKETS (5)" while the KPI strip on the SAME
 * board read Selected "1 / 10". Only MRP.US carried an entry zone, ticket,
 * stop and TP ladder; EXE.US, SNX.US, 5110.SR and 2914.T were grace-held
 * ghosts from dt10StabGhost_ - correct rows, correctly stamped with em-dash
 * placeholders and a "sizing suspended" advisor note, but counted and
 * titled as executable tickets. A hand-built brief prepared from that board
 * the same morning read the header, reported "3 cleared for new money",
 * and put a name in front of the operator that the gate had not cleared.
 * The mechanism was right; the LABEL was wrong, and the label is what a
 * reader acts on.
 * FIX (display only - the stability state machine, membership hysteresis,
 * grace clocks, ghost rows and every gate are BYTE-UNTOUCHED):
 *   D-1 dt10StabGhost_ stamps _grace_hold = true. The section header then
 *       counts the two classes separately and names them:
 *       "SELECTED - 1 EXECUTABLE TICKET + 4 GRACE-HELD (NO PLAN TODAY)".
 *       An all-executable board keeps the v1.6.6 wording exactly, so a
 *       normal day is visually unchanged.
 *   D-2 dt10QualToRow_ last-resort reason no longer asserts "no seat was
 *       taken this run" when seats WERE taken - it reports the true seat
 *       state (filled/limit) and says the name is absent from this run's
 *       ticket set. 1831.SR carried the false string on the 2026-07-27
 *       board while five seats were occupied.
 *   D-3 KPI label "Blended R/R" -> "Blended R/R (TP2)". The ticket ROI is
 *       TP1-basis and the R/R is TP2-basis; on the 2026-07-27 board that
 *       printed 16.6% beside 2.89, and on one consistent basis the same
 *       ticket is 1.44. Naming the basis costs one word. Label count is
 *       unchanged (8), so the KPI strip layout and formats are untouched.
 * ES5. No payload field is required; _grace_hold is additive and any board
 * built by an older core simply reports every row as executable, which is
 * the v1.6.6 behaviour.
 *
 * v1.6.6 LOUD STABILITY STATE + NAMED EXITS + KPI CROSS-CHECK (2026-07-25/26)
 * ------------------------------------------------------------------------------
 * OPERATOR EVIDENCE (2026-07-25 board): 5110.SR was ACTIVE (day 2) at the
 * 05:00 run and simply ABSENT from the 17:07 board, whose status note read
 * only "stab: 2 fast-track" — no exit token of ANY class. That line is
 * exactly what a bootstrap-from-empty prints, and both state paths can go
 * dark silently: dt10StabLoad_ swallows a ScriptProperties read failure,
 * and dt10StabSave_ logs a write failure only to Logger, which nobody
 * reads. Separately the same board's KPI strip asserted Blended R/R 1.25
 * beside two rendered tickets at 2.89 and 2.10 (the KPI arrives from
 * payload.kpis — a backend number this cockpit reprinted unverified),
 * and hardOut() classed mere ABSENCE from today's audited candidates as a
 * safety hard-exit, which guts the grace layer for the commonest jitter
 * (a name that failed one intraday gate pass). Five cuts, all ES5:
 *   S-1 STATE TELEMETRY: dt10StabLoadEx_ reports WHY the state is empty
 *       (STATE-EMPTY after a recorded prior success / STATE-CORRUPT /
 *       STATE-READ-FAIL) and dt10StabSave_ verifies its write by reading
 *       it back, returning a STATE-SAVE-FAIL token on any mismatch —
 *       both tokens land in the status line, so a reset can never again
 *       masquerade as a calm "fast-track" morning.
 *   S-2 ABSENCE IS NOT A SAFETY VERDICT: hardOut() no longer treats a
 *       symbol missing from candBySym as SELL/AVOID/EXIT; absence flows
 *       to the day-keyed miss clock (grace), exactly the jitter this
 *       layer exists to absorb. The safety regex itself is unchanged.
 *   S-3 NAMED, RECONCILED EXITS: capacity exits gain a status token (they
 *       had NONE), displaced exits are named not counted, and a
 *       reconciler compares the loaded membership against final-plus-
 *       every-exit-list — any leftover is reported as LOST <syms>
 *       (a tripwire that should never fire) and audited as exited_lost.
 *   S-4 EXIT ROWS IN _Selection_Log: every departure (hard / soft /
 *       capacity / displaced / lost) appends one Outcome-stamped row via
 *       dt10SelLogExitRows_ + dt10AppendExitLog_, so membership history
 *       is reconstructible from the log alone. Respects the 'off' mode;
 *       never dedupe-suppressed (exits are rare and decision-critical).
 *   S-5 KPI CROSS-CHECK: dt10KpiCheckNote_ recomputes Blended R/R
 *       (TP2 basis: (TP2−price)/(price−stop)) and Blended
 *       Reliability from the RENDERED tickets and appends a
 *       'KPI-CHECK …' token when the backend figure disagrees
 *       (>0.05 rr / >0.5 rel). The backend number is still displayed —
 *       this cockpit verifies, it does not silently rewrite money math.
 * DISPLAY: table headers now name their basis — 'ROI % (TP1)',
 * 'Engine ROI % (12M)', and QUAL 'R/R (TP2)'. Formats/widths are
 * index-based; _Selection_Log schema untouched.
 * ZERO functions removed; added: dt10StabLoadEx_, dt10KpiCheckNote_,
 * dt10SelLogExitRows_, dt10AppendExitLog_.
 * ============================================================================
 *
 * v1.5.0 HOLDINGS SENT TO THE PORTFOLIO GATE (Fix H1, 2026-07-14)
 * ------------------------------------------------------------------------------
 * OPERATOR FINDING (2026-07-14 morning board, export v40): SELECTED —
 * EXECUTABLE TICKETS ranked 1050.SR at #1 while the operator HELD 1050.SR
 * (19.1% of book, on the same morning's TRIM list) and the panel said
 * "T10: Include Portfolio Holdings = No".
 * FORENSICS: the backend chain has been READY since route v4.7.0 — the
 * opportunity-candidates endpoint documents portfolio.holdings[{symbol,
 * sector, market, value_sar}]; opportunity_builder v1.0.23 defaults
 * include_portfolio_holdings=False, coerces the panel's "No" correctly,
 * matches bare/.US variants (its v1.0.21 Fix #4), and applies a STRUCTURAL
 * Portfolio gate ("exclude holdings (Include Portfolio Holdings = No)").
 * But held = {h.symbol for h in portfolio.holdings} — and THIS cockpit sent
 * portfolio:{cash_available_sar, pending_proceeds_sar} with NO holdings
 * array. Empty held-set => the gate passed every held name. The defect was
 * never in the builder or top10_selector: the cockpit withheld the one
 * input the gate needs.
 * FIX H1: dt10CollectHoldings_(ss) reads My_Portfolio (header auto-scan via
 * the existing dt10FindHeaderRow_/dt10NormToken_; Symbol mandatory; Sector
 * and a value column best-effort — an explicitly-SAR-labelled value column
 * is preferred, else raw Position Value; value/sector feed only the
 * builder's sector-cap context, the EXCLUSION needs symbol alone; cap 500
 * rows; per-symbol dedupe) and the request body gains portfolio.holdings.
 * The status line gains "| held=<n> sent" so every run DISCLOSES what the
 * gate saw — including held=0 when the page is unreadable (a blind gate is
 * never silent). KILL SWITCH: Script Property DT10_SEND_HOLDINGS =
 * 0/false/off/no restores the exact v1.4.0 request body (holdings key
 * absent, no note). Fail-open: any read fault logs, sends [], and the
 * refresh proceeds. ES5. ZERO functions removed; additions:
 * dt10SendHoldingsEnabled_, dt10HoldingFromRow_, dt10CollectHoldings_.
 * dt10SelfTest extended (kill-switch state + mapper check + live count).
 *
 * v1.4.0 COCKPIT-OWNED _STATUS UPSERT (2026-07-13)
 * ------------------------------------------------------------------------------
 * OPERATOR FINDING (2026-07-13 morning workbook audit): the _Status row for
 * Top_10_Investments froze at its LAST validatePage_ write — 2026-06-12,
 * WARN "width mismatch: expected 118, actual 95" — while the cockpit itself
 * rendered cleanly every day (in-page banner: run 2026-07-13 08:50, steady).
 * Every audit that reads _Status saw a 31-day-old false WARN.
 *
 * ROOT CAUSE (ownership seam, evidenced): the day the cockpit took rendering
 * ownership, every legacy writer of that _Status row was correctly BLOCKED
 * (05_Refresh v1.12.0 skips decision-owned pages in validatePage_ and both
 * refresh cores; 13_AutoRefresh v1.7.0+ routes the trigger straight to
 * refreshDecisionTop10) — but this file never picked up the reporting
 * duty. Nothing could update OR clear the row: a permanently stale WARN.
 *
 * FIX — the cockpit now OWNS its _Status row exactly as it owns its
 * selection log (v1.3.1 pattern): NEW dt10WritePageStatus_ upserts through
 * the SAME writePageStatus_ helper (02_Core) every other page uses —
 * found-in-place overwrite by page name, so the 2026-06-12 row is RECLAIMED,
 * never duplicated — on ALL THREE run outcomes of refreshDecisionTop10:
 *   - success        -> OK    (message = the exact banner status line,
 *                              rows = SELECTED board count, http 200, ms)
 *   - network error  -> ERROR (message carries the exception text)
 *   - HTTP != 200    -> ERROR (message carries code + response head)
 * RULES (mirrors dt10AppendSelectionLog_): NEVER throws — the render is
 * untouchable; typeof-guarded — a workbook without 02_Core degrades to a
 * silent no-op; rows/httpCode/durationMs are stringified so an honest zero
 * survives writePageStatus_'s falsy-coercion (0 || '').
 * KILL-SWITCH: Script Property DT10_PAGE_STATUS, default ON; set
 * 0/false/off/no/disabled to restore the exact v1.3.1 no-report
 * behavior (banner + SelLog untouched either way).
 * NO layout change, NO panel change, NO schema change, NO new sheet.
 *
 * v1.3.1 SELECTION-AUDIT-LOG OWNERSHIP + ghost "—" fill (2026-07-09)
 * ------------------------------------------------------------------------------
 * OPERATOR FINDING (workbook audit, 2026-07-09 evening export): _Selection_Log
 * silent since 2026-06-27 00:27 while the board refreshed daily — the
 * stability gate of the standing review protocol was blind for 12 days.
 *
 * ROOT CAUSE (evidenced, not guessed):
 *   - This file (and v1.2.x before it) contains ZERO references to
 *     _Selection_Log; logging lived only in 17_Selection_Log.gs, a
 *     sheet-SCRAPER keyed to panel row offsets (SL_T10_PANEL), invoked on
 *     the operator's MANUAL runs.
 *   - The scheduled DECISION-COCKPIT auto-refresh (first fired 2026-07-02
 *     02:29 via 13_AutoRefresh -> refreshDecisionTop10) never carried a
 *     logging call. The operator's last manual session was 2026-06-27 00:27
 *     — exactly the last log rows. Manual stopped => log stopped.
 *   - The v1.2.7 (panel 7->8) and v1.3.0 (8->9) layout shifts ADDITIONALLY
 *     invalidated 17's offsets, but the silence predates both.
 *
 * FIX — the cockpit now OWNS its audit trail: after a successful render,
 * refreshDecisionTop10 appends the board it just drew to _Selection_Log from
 * the IN-MEMORY payload (no sheet scraping, immune to every future layout
 * shift). 17_Selection_Log.gs is retired to optional manual back-fill;
 * delete any trigger it holds.
 *
 * RULES:
 *   - DEFAULT cadence: one snapshot per UTC day PLUS an extra snapshot on
 *     any run whose MEMBERSHIP SET changed (intraday hard-exits and
 *     fast-track fills are captured; score-only intraday re-runs are not
 *     spammed). Day-keying reuses dt10StabToday_ — the stability clock.
 *   - Mode / kill-switch: ScriptProperty DT10_SELECTION_LOG = 'off' (never
 *     log) | 'always' (every run) | unset (default above). No panel change,
 *     no layout shift, nothing for 17 to re-pair with.
 *   - SCHEMA: the existing 31 columns VERBATIM (append-compatible with the
 *     2026-06 rows; Panel Snapshot carries the JSON panel) + TWO appended
 *     columns 'Stability', 'Days' (cols 32-33; header self-heals, old rows
 *     keep blanks there). The sheet is created with the full header if
 *     missing.
 *   - NEVER throws: every failure degrades to a 'SelLog: ERROR …' tail on
 *     the status line; the render itself is untouchable.
 *
 * GHOST TICKETS (operator decision 2026-07-09 — "keep + tag, no fake
 * plans"): dt10StabGhost_ ticket cells (Entry Zone / Ticket / Shares /
 * Stop / TP1 / TP2 / Gain 12M) now carry '—' instead of silent blanks —
 * the 2026-07-09 board showed 5 grace-held rows whose empty cells read as
 * data loss. The advisor note already names the reason ("Stability grace
 * hold (n/m missed)"); retry-per-run is inherent — the moment the backend
 * re-selects the symbol, the full ticket returns.
 *
 * SURFACE CHANGES: status line gains a ' | SelLog: …' tail. SIX functions
 * added (dt10SelLogSignature_, dt10SelLogRowFromTicket_,
 * dt10SelLogLoadState_, dt10SelLogSaveState_, dt10SelLogSheet_,
 * dt10AppendSelectionLog_); all 49 v1.3.0 top-level functions carried
 * verbatim; ONE function body changed (dt10StabGhost_ dash fill); ONE hook
 * added in refreshDecisionTop10; ONE dt10SelfTest block appended. No KPI,
 * zone, POST-contract, or panel change.
 *
 * v1.3.0 SELECTION-STABILITY LAYER — membership hysteresis (2026-07-07)
 * ------------------------------------------------------------------------------
 * OPERATOR FINDING (Emad): the Top-10 reshuffles intraday and day to day; a
 * churning list cannot be traded without constant sell/buy round-trips. The
 * backend selection (route /sheet-rows/opportunity-candidates -> core/analysis/
 * opportunity_builder) is MEMORYLESS per run, and scores cluster near the
 * cutoff, so rank jitter flips seats 8-14 constantly.
 *
 * FIX: this file now applies a DAY-KEYED MEMBERSHIP-HYSTERESIS layer between
 * the backend response and the render — the backend's list stays the honest
 * "raw" answer; the BOARD (what the operator trades) gains memory. State is a
 * compact JSON blob in ScriptProperties (DT10_STAB_STATE_V1) — the SAME shape
 * as core/analysis/top10_selector.py v4.21.0's `stability_state`, so a later
 * backend consolidation (moving these rules into opportunity_builder) can
 * adopt the blob unchanged and this layer then thins to persist+pass-through.
 *
 * RULES (knobs are panel inputs; defaults 3/3/15/5; §8 note: this layer
 * SELECTS AMONG the backend's own audited candidates — it never recomputes a
 * gate, score, or verdict):
 *   - ENTRY CONFIRMATION: a challenger must appear in the backend's selected
 *     list `Confirm Days` consecutive days before taking a seat; until then
 *     it shows in the status note as pending (e.g. "XNEW(2/3)").
 *   - EXIT GRACE: an incumbent leaves only after `Exit Days` consecutive
 *     missed days ("GRACE n/m missed" while holding; sizing suspended).
 *   - RANK-JITTER IMMUNITY: a missed day while still ranking <= `Rank
 *     Buffer` across ALL audited candidates does not count (clock PAUSES).
 *   - DISPLACEMENT: seats full — a CONFIRMED challenger replaces the weakest
 *     incumbent only when its SMOOTHED score is higher. Noise never
 *     displaces; fast-track fills never displace.
 *   - HARD EXIT: absent from the audit grid, structural_block, or a
 *     SELL/AVOID/EXIT-class verdict exits IMMEDIATELY (safety verdicts are
 *     never grace-held). Plain DO_NOT_INVEST criteria-misses ARE
 *     grace-holdable — that is exactly the jitter this layer absorbs.
 *   - FAST-TRACK: empty seats (bootstrap / mass exits) fill from today's raw
 *     order so the board never under-fills; flagged, never displacing.
 *   - SMOOTHING & ORDER: board order = mean of the last `Smooth Days` daily
 *     opportunity-score points; Rank is re-stamped 1..N in that order.
 *   - DAY-KEYED: counters advance at most once per UTC date; intraday
 *     re-runs refresh the day's score point and re-evaluate seating but
 *     cannot advance confirmation/exit clocks — intraday churn is
 *     structurally removed (hard exits and seat-fills stay live, by design).
 *
 * SURFACE CHANGES:
 *   1. DT10_PANEL +5 (APPENDED, positions preserved): 'T10: Stability
 *      Enabled' (Yes), 'T10: Stability Confirm Days' (3), 'T10: Stability
 *      Exit Days' (3), 'T10: Stability Rank Buffer' (15), 'T10: Stability
 *      Smooth Days' (5). 27 inputs / 3 per row => DT10_PANEL_ROWS 8 -> 9,
 *      so the KPI band and zones shift DOWN ONE ROW (KPI head 13->14,
 *      labels 14->15, values 15->16, zones 17->18). refreshDecisionTop10 now
 *      LAYOUT-GUARDS on the KPI head cell ('KPIs' at the NEW row) and
 *      auto-rebuilds a v1.2.x sheet on first refresh — no manual step.
 *   2. SELECTED gains 5 columns BEFORE 'Advisor Note': Stability, Days,
 *      Since, Sm Score, Trend (DT10_LAST_COL 25 -> 30; the Advisor-Note
 *      wrap + 360px width follow DT10_LAST_COL unchanged). Stability cell
 *      colored via §3.4 tokens (ACTIVE/NEW green, GRACE/FAST-TRACK amber).
 *   3. Grace-held incumbents missing from today's tickets render from their
 *      audited candidate row with sizing suspended (blank Ticket/Shares/
 *      Stop/TPs) and an explanatory Advisor Note — honest: no stale sizing.
 *   4. dt10CriteriaFromPanel_ ALSO emits a real `stability` mapping
 *      ({enabled, confirm_days, exit_days, rank_buffer, smooth_days}) —
 *      criteria-snapshot readable today, and the exact knob shape
 *      top10_selector v4.21.0 / a future opportunity_builder port consumes
 *      (v1.2.7 Require-Investable key-mapping precedent; §8 honored).
 *   5. dt10StabilityReset() (editor-run) clears the state blob; the status
 *      note and dt10SelfTest surface the live state summary. Panel
 *      'T10: Stability Enabled' = No bypasses the layer (state frozen, not
 *      cleared) — the board then mirrors the raw backend list exactly.
 *   6. State blob self-prunes (non-members idle > 14 days; ~8.5KB guard
 *      under the 9KB ScriptProperties value limit).
 *   7. dt10SelfTest expected T10 defaults text 18 -> 23 (18 is normal until
 *      15_Lists_Config reseeds; built-in defaults cover the gap). PAIRS
 *      WITH: 17_Selection_Log.gs SL_T10_PANEL rows 8 -> 9 (optional,
 *      audit-snapshot completeness only).
 * All 37 v1.2.7 functions carried verbatim; TEN pure functions added
 * (node-parity-tested against the v4.21.0 Python simulation); TWO function
 * bodies extended (dt10CriteriaFromPanel_, dt10TicketToRow_) plus the
 * refresh hook and layout guard. No KPI change; QUAL/NEAR-MISS/ALERTS/GAPS/
 * CANDIDATES zones untouched (the audit grid stays the backend's raw truth).
 *
 * v1.2.7 SHEET-SIDE "REQUIRE INVESTABLE" SWITCH (2026-07-05 incident fix)
 * ------------------------------------------------------------------------------
 * INCIDENT: the 2026-07-05 14:15 run selected 7 tickets of which FIVE
 * (8053.T #1, 4578.T, 1150.SR, 9433.T, ACA.PA) were WATCHLIST rows the engine
 * had explicitly benched ("Conservative gate: overall < 68") on their own
 * universe pages — yet they rendered under "ALL QUALIFIED — INVEST" and were
 * emailed by the daily brief as best investments. Root cause is NOT here (§8:
 * this file renders, never gates): opportunity_builder v1.0.7 already HAS an
 * Investability MAJOR gate (fails WATCHLIST/BLOCKED; blank passes) wired to
 * criteria key `investability_gate_enabled` / env TFB_OPP_INVESTABILITY_GATE —
 * shipped DEFAULT OFF by its v1.0.8 policy note, whose rationale (the builder
 * fed a surface SEPARATE from the selector-owned Top_10 page) is OBSOLETE
 * since this file's §5 pool-source amendment made THIS cockpit the
 * Top_10_Investments page. v1.2.7 gives the operator the switch on the sheet:
 *
 *   1. DT10_PANEL gains one yesno input, 'T10: Require Investable',
 *      DEFAULT 'Yes' — APPENDED as the 22nd item so every EXISTING panel
 *      cell keeps its position (no rebuild needed to keep reading correctly;
 *      the new cell reads blank → default 'Yes' until the layout is rebuilt,
 *      which paints the label + blue cell at A12/B12).
 *   2. DT10_PANEL_ROWS 7 -> 8 (22 inputs / 3 per row = grid rows 5–12; the
 *      KPI band at rows 13–15 is untouched — row 12 was an empty spacer).
 *   3. dt10CriteriaFromPanel_ now ALSO emits the builder's REAL criteria key
 *      `investability_gate_enabled` as a TRUE BOOLEAN (Yes/blank => true;
 *      only an explicit leading 'n'/'N' => false). WHY a boolean and not the
 *      label alone: the route's _OPP_CRITERIA_ALIASES has no alias for this
 *      label, so it snake-cases to `require_investable`, which the builder
 *      ignores; and a raw 'No' STRING under the real key would be TRUTHY in
 *      the builder's `criteria.get("investability_gate_enabled")` check. The
 *      friendly T10 label is still sent verbatim too (harmless; keeps the
 *      criteria snapshot human-readable). §8 HONORED: this is key MAPPING,
 *      not gating — the backend's existing Investability gate decides, and
 *      its verdict/first_fail flow through the already-built DATA GAPS and
 *      CANDIDATES zones with zero renderer changes.
 *   4. dt10SelfTest expected T10 defaults text 17 -> 18 (with a note: 17 is
 *      normal until 15_Lists_Config's TFB_PANEL_DEFAULTS is reseeded — the
 *      built-in 'Yes' default covers the gap, so reseeding is optional).
 *
 * PRECEDENCE (unchanged chain, new item included): sheet panel cell ->
 * TFB_PANEL_DEFAULTS['T10: Require Investable'] (optional 15_Lists seed) ->
 * built-in 'Yes'. Because the key is now ALWAYS sent, the backend env
 * TFB_OPP_INVESTABILITY_GATE no longer matters on this path — the panel is
 * authoritative. Setting the cell to 'No' deliberately restores the
 * pre-incident WATCHLIST-permissive selection (operator override, visible in
 * the logged panel snapshot).
 *
 * EXPECTED FIRST RUN AFTER PASTE (with the 2026-07-05 data): SELECTED drops
 * to 2 honest tickets (4503.T, 0016.HK — ten is a CEILING, no forced fill),
 * DATA GAPS gains a "Investability" blocking-gate row (~5 names), and the
 * next daily brief carries the clean list automatically.
 *
 * PAIRS WITH: 17_Selection_Log.gs v1.0.2 (SL_T10_PANEL rows 7 -> 8 so the
 * audit panel-snapshot captures row 12). All 37 v1.2.6 functions carried
 * verbatim — none added, none removed; ONE function body extended
 * (dt10CriteriaFromPanel_). No payload zone, column contract, or KPI change.
 *
 * v1.2.6 PANEL-DEFAULT RECALIBRATION (2026-07-02): Max Selected def 3 -> 10,
 * Max Per Market def 4 -> 10 in DT10_PANEL.
 * ------------------------------------------------------------------------------
 * WHY: the owner's standing instruction is a TEN-pick Selected list. DT10_PANEL
 * is the FINAL fallback in the panel precedence chain (sheet panel cell ->
 * _Lists_Config TFB_PANEL_DEFAULTS -> DT10_PANEL def) AND the seed source when
 * the layout builder (re)writes the rows 1-15 panel — so a stale 3/4 here could
 * silently reintroduce the old caps on a rebuild or if the named range breaks.
 * Now aligned with 15_Lists_Config.gs v1.2.0 (seeds 10/10) and
 * core/analysis/opportunity_builder.py v1.0.19 (DEFAULT_CRITERIA 10/10) — all
 * four layers agree. 'T10: Max Per Sector' stays def 2 (diversification guard).
 * Two literals changed; no function added or removed; rendering, zones, POST
 * contract, and every other default byte-identical to v1.2.5. Ten is a CEILING
 * (no forced fill) — qualification/funding gates unchanged in the builder.
 *
 * v1.2.5 ENGINE ROI % COLUMN on ALL QUALIFIED + CANDIDATES (additive; 2026-06-25)
 * ------------------------------------------------------------------------------
 * Adds an "Engine ROI %" column (after "ROI %") to the ALL QUALIFIED and the
 * CANDIDATES — FULL AUDIT grids, reading the new per-row engine_roi_pct that
 * opportunity_builder v1.0.12 now emits on every candidate (previously the
 * engine's 12M forecast was visible only on the 8 SELECTED tickets). This makes
 * the target-vs-forecast divergence — e.g. valuation "ROI %" 35% next to an
 * Engine ROI % of 3.5% — auditable on every row, and lets the Forecast-gate
 * floor (TFB_OPP_MIN_ENGINE_ROI_PCT) be set from the visible distribution
 * rather than a guess. Derived/display only: §8 honored — no payload zone, no
 * gate/score/selection touched; pure read of candidates_rows. Two grids gain
 * one column each; index-based color/format maps shifted accordingly. No
 * function added or removed. Matches the existing SELECTED-ticket convention
 * (roi_pct, engine_roi_pct, ann_roi_pct) and field name. Requires the live
 * backend to be on opportunity_builder >= v1.0.12; on an older backend the
 * column simply renders blank (engine_roi_pct absent), never errors.
 *
 * v1.2.4 DATA GAPS — FAILURE BREAKDOWN (additive, derived view; 2026-06-25)
 * ------------------------------------------------------------------------------
 * NEW rendered section "DATA GAPS — WHY CANDIDATES DIDN'T QUALIFY", placed
 * between ALERTS and CANDIDATES. It aggregates the audited candidates by the
 * gate each FAILED FIRST (the headline blocker the builder already assigned in
 * first_fail.gate) and shows, sorted by count: Blocking Gate | Candidates
 * Failed | Share % | Example Symbols, with a headline "(K of N passed all
 * gates; top blocker: GATE, J names)". This answers the operator's real
 * question — "why is my qualified set small, and which criterion do I relax to
 * grow it?" (e.g. most failures on Reliability => Min Reliability is the
 * binding constraint; most on Risk => relax Max Risk Level). §8 HONORED: a
 * PURE read/aggregate of the candidates_rows the page already receives — no
 * gate/score/verdict is recomputed, no backend change, no new endpoint, and no
 * payload/schema/column-contract change to any existing zone (DATA GAPS is a
 * 4-column derived view via the existing section/table renderers). NOTE: the
 * breakdown reflects the AUDITED candidates_rows (capped by
 * TFB_OPP_AUDIT_ROWS_MAX); pool-level attrition counts (missing FX / valuation,
 * etc.) remain in ALERTS above. NEW pure helpers dt10FailureBreakdown_ +
 * dt10GapToRow_ and constant DT10_GAP_HEADERS; all 35 v1.2.3 functions carried
 * verbatim (none removed); 2 added (37 total).
 *
 * v1.2.3 FULL-UNIVERSE POOL — raise hidden GAS cap; blank = scan all (2026-06-24)
 * ------------------------------------------------------------------------------
 * Root cause of "raising Pool Limit still truncates": DT10_POOL_HARD_CAP (2000)
 * SILENTLY clamped the panel's Pool Limit inside dt10CollectPoolRows_ —
 *   cap = Math.max(1, Math.min(limit, DT10_POOL_HARD_CAP));
 * so even Pool Limit = 50000 was capped at 2000, BELOW the ~3,905-row deduped
 * universe (Global_Markets alone ~3,002). The route's body_rows path is NOT
 * truncated, and opportunity_builder v1.0.10 caps ONLY the written audit grid
 * (selected / ALL-QUALIFIED / near-miss always kept), so this GAS hard cap was
 * the last real ceiling on scan depth. Changes (frontend only; no schema /
 * payload / column / KPI change; §8 honored):
 *   1. DT10_POOL_HARD_CAP 2000 -> 50000 — far above any universe at this scale;
 *      auto-adapts as symbols are added (effectively "scan all").
 *   2. Pool Limit semantics: a BLANK / 0 / non-positive cell now means "scan
 *      the ENTIRE available universe" (bounded only by the hard cap) — no
 *      number to maintain. A positive value still caps deliberately.
 *      dt10CollectPoolRows_ maps non-positive limit -> hard cap; the
 *      refreshDecisionTop10 fallback resolves blanks to the hard cap too.
 *   3. Pool Limit panel DEFAULT 1000 -> 50000 (fresh / blank installs scan all).
 *   4. dt10SelfTest pool probe uses DT10_POOL_HARD_CAP.
 * All 35 v1.2.2 functions carried verbatim (none added, none removed). Pairs
 * with opportunity_builder v1.0.10 (TFB_OPP_AUDIT_ROWS_MAX), which keeps the
 * write-back inside the Sheets limit no matter how deep the scan goes.
 *
 * v1.2.2 POOL SYMBOL DE-DUPLICATION + GHOST GUARD (adopted 2026-06-24)
 * --------------------------------------------------------------------
 * Root cause of "one share repeated / selection not the best": the source
 * sheets contain the SAME symbol on multiple rows (Market_Leaders had 406 rows
 * for only 203 unique symbols — every name 2-3x; Global_Markets had a handful
 * of dups + 1 ghost "SYMBOL" row). v1.2.0 fairly pooled ALL of those rows, so
 * the backend saw e.g. 1831.SR twice and issued it as TWO executable tickets —
 * which also consumed both Saudi-Market sector-cap slots, blocking genuinely
 * different names (4030.SR, 2320.SR) from selection. So the duplicate was
 * actively degrading selection quality, not just cosmetics.
 * Fixes, all at the POOL source so the backend only ever sees one row/symbol:
 *   1. dt10CollectPoolRows_ now keeps a GLOBAL seen-set across all four pages
 *      during phase-1 collection and skips any row whose normalized symbol was
 *      already collected (first occurrence wins). Tracks duplicatesSkipped and
 *      surfaces it in the status line for transparency.
 *   2. dt10PoolRowFromSheetRow_ skips ghost/header rows whose Symbol cell is a
 *      literal header token ("SYMBOL"/"TICKER") — these are never real tickers.
 *   3. dt10QualifiedFromCands_ tie-break now prefers the selected=true row on
 *      equal scores, so the ALL QUALIFIED "Selected" column is accurate even if
 *      a dup ever slips through (belt-and-suspenders; moot once pool is deduped).
 * NEW helper dt10NormSym_ (trim+uppercase). All v1.2.1 logic, formats, and the
 * 34 functions carried verbatim; 1 helper added (35 total). No payload/schema
 * change — only which rows are placed into body.rows.
 *
 * v1.2.1 QUALIFIED-SET COLOR CODING (design pass — adopted 2026-06-24)
 * --------------------------------------------------------------------
 * A review of the v1.2.0 ALL QUALIFIED section found it rendered as flat text
 * with NO color coding — inconsistent with every other table (which color
 * their verdict column) and missing the whole point of the section, which is
 * to see at a glance WHY each qualified name is or isn't an executable ticket.
 * Fixed: the "Selected" column is now status-color-coded —
 *   GREEN  (VERDICT_POSITIVE) = already selected (a ticket in SELECTED above);
 *   AMBER  (VERDICT_WATCH)    = qualified but DEFERRED by a diversification
 *                              cap (the actionable "relax Max Per Sector /
 *                              Max Per Market to add this" signal);
 *   GREY   (VERDICT_BLOCK)    = qualified but ranked below the Max Selected
 *                              cut.
 * NEW helper dt10ColorQualified_ reads each row's selected/deferral status
 * directly from the qualified array (same single-column pattern as
 * dt10ColorVerdicts_, so the palette stays consistent and §3.4 tokens drive
 * the colors). All v1.2.0 logic, number formats, and the 33 functions are
 * carried verbatim; 1 helper added (34 total). No payload/schema/column-count
 * change — the QUALIFIED grid is still a derived view of candidates_rows.
 *
 * v1.2.0 FAIR MULTI-SHEET POOLING + ALL-QUALIFIED SET (adopted 2026-06-24)
 * -----------------------------------------------------------------------
 * THE BUG THIS FIXES (a genuine logic defect, not a data issue):
 *   v1.1.0 dt10CollectPoolRows_ walked the four source pages IN ORDER under a
 *   single shared row cap:
 *       for (p = 0; p < PAGES.length && rows.length < cap; p++) { ... }
 *   Market_Leaders alone holds >= cap rows, so it filled the entire 250-row
 *   budget on the FIRST page and the loop exited before Global_Markets /
 *   Commodities_FX / Mutual_Funds were ever read. The live status proved it:
 *   `pool 250 rows ({"Market_Leaders":250})` — the other three pages were not
 *   even visited (their perPage keys never got set). Result: an all-Saudi
 *   universe, and the Max-Per-Sector=2 cap then choked selection to 2 names.
 *
 * THE FIX (frontend-only; the backend already supports it):
 *   Verified against the live route (advanced_analysis v4.9.0): when the page
 *   POSTs `rows` (the body_rows path) the backend ingests EVERY row with NO
 *   truncation — `pool_rows = [dict(r) for r in explicit_rows]`. The
 *   pool_limit/ceiling-500 clamp applies ONLY to the backend-selector path,
 *   which body_rows bypasses; opportunity_builder's TFB_OPP_MAX_CANDIDATES
 *   defaults to 0 (unlimited). So the GAS-side cap was the only limiter.
 *   dt10CollectPoolRows_ is rewritten to:
 *     (1) collect EVERY valid row from EACH sheet independently first (no
 *         sheet can starve the others);
 *     (2) if the four-sheet total fits within the cap, send it ALL (full
 *         multi-sheet universe);
 *     (3) if it exceeds the cap, ROUND-ROBIN across the sheets so each is
 *         fairly represented instead of the first draining the budget.
 *   DT10_POOL_HARD_CAP 500 -> 2000 (body_rows is not clamped at 500, so 500
 *   was the wrong guard; 2000 safely covers the four-sheet universe). The
 *   Pool Limit panel DEFAULT 250 -> 1000 so fresh installs scan everything.
 *   OPERATOR ACTION: an EXISTING sheet still has whatever Pool Limit you typed
 *   (e.g. 250). Raise that blue cell (e.g. to 1000) to scan the full universe;
 *   even left at 250 the fairness fix now spreads the 250 across all four
 *   sheets instead of all-Saudi.
 *
 * NEW: ALL QUALIFIED — INVEST opportunity set (answers "show me every name I
 *   COULD invest in, not just the auto-selected tickets"). A new rendered
 *   section lists every candidate whose verdict is INVEST — including names
 *   the selector deferred by a diversification cap — de-duplicated by symbol
 *   (highest score wins), ranked by score, with a "Why Not Selected" column
 *   (selected / cap-deferred / ranked-below-cut). This is a PURE FILTER of the
 *   candidates_rows payload the page already receives — §8 honored, nothing is
 *   recomputed, no backend change, no new endpoint.
 *
 * CHANGES (v1.2.0): (1) DT10_VERSION 1.1.0 -> 1.2.0; (2) DT10_POOL_HARD_CAP
 * 500 -> 2000; (3) DT10_PANEL 'Pool Limit' def 250 -> 1000; (4)
 * dt10CollectPoolRows_ rewritten for fair multi-sheet pooling (returns
 * available/total/truncated for transparency); (5) refreshDecisionTop10
 * pool note now reports included/available per sheet + truncation, so empty/
 * stale sheets are visible at a glance; (6) NEW pure helpers
 * dt10QualifiedFromCands_ + dt10QualToRow_ + DT10_QUAL_HEADERS; (7)
 * dt10RenderPayload_ renders the ALL QUALIFIED section right after SELECTED.
 * All 31 v1.1.0 functions are carried verbatim (none removed); 2 added (33
 * total). No schema/column-contract change to SELECTED or CANDIDATES; the
 * QUALIFIED grid is a derived view, not a payload zone. Everything else is
 * byte-identical to v1.1.0.
 *
 * v1.1.0 PROPOSAL RESKIN (render-only — adopted 2026-06-20)
 * --------------------------------------------------------
 * The cockpit adopts the APPROVED "TFB_Final_Proposal" palette verbatim so
 * Top_10_Investments matches the mockup the owner signed off on. This is a
 * PURE PRESENTATION change: §8 is honored — no schema, payload, sizing,
 * gate, verdict, sort, column-count, or KPI logic is touched, the 25-column
 * SELECTED table and 24-column CANDIDATES grid are unchanged, and all 31
 * functions are carried verbatim (none added, none removed). Only fill /
 * font / border colors and three small style touches change.
 *
 * PALETTE (exact hex from the proposal's "Top 10 Opportunities" tab):
 *   - Brand navy  #1F3A5F  → title banner, CONTROL PANEL / KPI / section
 *                            banners, and every column-header row (white text)
 *   - Light tint  #EEF2F7  → KPI tiles + section context strips (navy text)
 *   - Body zebra  #F5F7FA  → alternate data rows (was #FAFAFA)
 *   - Border      #D6DEE8  → table gridlines (was #E0E0E0)
 *   - Ink         #1A2433  → body text (was sheet default)
 *   - Muted       #6B7280  → status / footer / labels (was #5F6368)
 *   - Gold        #C8A04B  → a thin title bottom-rule accent (the proposal's
 *                            signal-group accent; reserved as constants, NOT
 *                            mapped to any data column so the audited schema
 *                            contract is never reopened)
 *   - Verdicts:   INVEST green #146C43 on #E6F4EA; WATCH amber #9A6700 on
 *                 #FFF3CD; DO_NOT_INVEST red #B3261E on pink #FBE4E2; other
 *                 muted #5B6470 on #ECEEF1
 *
 * WHY THIS IS LIVE THE MOMENT YOU PASTE IT (no schema reopen):
 *   dt10Tokens_ overlays the TFB_DESIGN_TOKENS named range onto this file's
 *   fallback map, but that named range publishes COLOR_* keys while the
 *   renderers read VERDICT_* / HEADER / OPERATOR_INPUT keys — so the named
 *   range never overwrites them. Editing the fallback map below therefore
 *   reskins the cockpit immediately (Option-1 SURGICAL: tokens, not a new
 *   column contract). The new DT10_NAVY/TINT/GOLD constants drive the three
 *   style touches in buildDecisionTop10Layout / dt10WriteSection_.
 *
 * OPERATIONAL (read this): after pasting + saving, run the menu items
 * "🧭 Build Decision Cockpit Layout" ONCE (this lays down the title / panel /
 * KPI fixed-layout styling — buildLayout only runs on a build/repair), THEN
 * "🚀 Refresh Decision Top 10" (the dynamic SELECTED / ALL QUALIFIED / NEAR
 * MISS / ALERTS / CANDIDATES zones restyle on every refresh). A plain refresh
 * alone will restyle the zones but not the fixed header band until rebuilt.
 *
 * WHY THIS FILE EXISTS
 * --------------------
 * Plan v5.0 §3.1/§5: Top_10_Investments stops being a 118-column ranked dump
 * and becomes a decision cockpit — operator control panel, KPI strip, sized
 * executable tickets in SAR, near-miss ladder, alerts, and a full candidates
 * audit grid. The intelligence lives in core/analysis/opportunity_builder.py
 * (P2) behind POST /sheet-rows/opportunity-candidates
 * (routes/advanced_analysis.py P3). This file ONLY collects inputs, calls the
 * endpoint, and renders the FROZEN §5 payload zones. Qualification,
 * scoring and sizing stay in the backend (§8); native source-readiness
 * checks can withhold the displayed order surface.
 *
 * §5 POOL-SOURCE AMENDMENT (adopted 2026-06-12)
 * ---------------------------------------------
 * The backend selector path builds source pages under internal budgets
 * (page_sec 30 / total_sec 70) which free-tier engine builds (50–175s/page)
 * always exceed → pool starvation (3 snapshot rows, coverage 0.0). Therefore
 * the PRIMARY pool source is the spreadsheet itself: this file reads the
 * already-refreshed Market_Leaders / Global_Markets / Commodities_FX /
 * Mutual_Funds rows and POSTs them as `rows` (the route's body_rows path,
 * live-tested). The backend selector is used only when Pool Source =
 * "Backend". Empty Sheets input produces a local empty research display
 * without a provider fallback or allocation request. Source receipts and
 * approved row floors must be ready before any order surface is shown.
 *
 * v1.0.1 ENGINE ROI % COLUMN (additive — adopted 2026-06-15)
 * ----------------------------------------------------------
 * The SELECTED tickets table gains one column, 'Engine ROI %', placed
 * immediately after the existing 'ROI %'. The two are DIFFERENT measures and
 * are shown side by side, never substituted:
 *   - 'ROI %' is the ticket's VALUATION upside = (reference - price)/price,
 *     the basis the sizing/verdict logic already uses.
 *   - 'Engine ROI %' is the engine's honest 12-month forecast return, emitted
 *     per selected ticket by opportunity_builder as engine_roi_pct (already in
 *     percent units, same scale as ROI %, so it renders with DT10_FMT_PCT).
 * SINGLE MASTER SWITCH: the builder only populates engine_roi_pct when the
 * backend env flag TFB_OPP_ENGINE_ROI_DISPLAY=1 (opportunity_builder v1.0.5+).
 * When that flag is OFF the field is absent from the payload and dt10Cell_
 * renders the cell blank — so this column is inert (header shows, cells empty)
 * until the flag is set on Render. There is deliberately NO separate GAS flag.
 * CHANGES (renderer-only; §8 honored — nothing is recomputed here):
 *   1. DT10_SELECTED_HEADERS: 'Engine ROI %' inserted after 'ROI %' (24 -> 25).
 *   2. dt10TicketToRow_: dt10Cell_(t.engine_roi_pct) emitted after t.roi_pct.
 *   3. DT10_LAST_COL 24 -> 25; the SELECTED format map shifts the post-ROI%
 *      columns by one (col 17 Engine ROI % = DT10_FMT_PCT). All DT10_LAST_COL-
 *      driven merges, column widths, and the advisor-note wrap auto-follow.
 * The CANDIDATES audit grid is intentionally unchanged. Every other byte is
 * carried forward verbatim from v1.0.0.
 *
 * DESIGN RULES HONORED
 * --------------------
 * L13 — honest empties: zero selected tickets renders as an explicit
 *       "no qualifying opportunities" line, never padded.
 * L14 — operator-editable cells use the OPERATOR_INPUT token (blue on
 *       #E8F0FE); everything else is renderer-owned.
 * §3.4 — colors/formats come from TFB_DESIGN_TOKENS (15_Lists_Config.gs
 *       v1.0.0) with byte-identical hardcoded fallbacks.
 * §4.1 — the "T10: *" panel labels match TFB_PANEL_DEFAULTS verbatim;
 *       blanks re-seed from that named range, then from built-ins.
 *       (v1.2.7 amendment: an 18th T10 label, 'T10: Require Investable',
 *       joins the set; its named-range seed is optional — built-in 'Yes'.)
 * §8  — verdicts/scores are rendered, never recomputed.
 *
 * LAYOUT (fixed rows 1–15; zones dynamic from row 17)
 * ---------------------------------------------------
 *   1   Title bar
 *   2   Status line (run timestamp, payload status, versions, pool, timing)
 *   4   CONTROL PANEL header
 *   5–12 Panel grid: 22 inputs as label/value pairs in cols A/B, D/E, G/H
 *        (v1.2.7: was 5–11 / 21 inputs)
 *   13  KPI header     14 KPI labels     15 KPI values  (8 KPIs, cols A–H)
 *   17+ SELECTED → ALL QUALIFIED → NEAR MISS → ALERTS → DATA GAPS →
 *       CANDIDATES → meta footer (dynamic; cleared from row 17 down on refresh)
 *
 * ENTRY POINTS (wire into 01_Menu.gs; no onOpen here to avoid collisions)
 *   buildDecisionTop10Layout() — one-time/repair: panel + formats
 *   refreshDecisionTop10()     — main run: collect → POST → render
 *   dt10SelfTest()             — config/named-range/mapping dry checks
 *
 * BACKEND RESOLUTION (defensive — 00_Config.gs v1.12.1 getters preferred):
 *   typeof-guarded getBackendUrl_()/getAppToken_() → ScriptProperties
 *   BACKEND_URL|TFB_BACKEND_URL|BACKEND_BASE_URL / APP_TOKEN|TFB_APP_TOKEN
 *   → DT10_DEFAULT_BACKEND constant.
 *
 * LOGGER TAGS: headline [DT10 v1.2.7]; internal tags stable per convention.
 * ============================================================================
 */
/**
 * v1.6.2 HONEST STALE BANNER + FUNNEL TELEMETRY (2026-07-24)
 * ------------------------------------------------------------------------------
 * WHY (evidence: 2026-07-24 11:06 run, HTTP 502 on
 * /sheet-rows/opportunity-candidates): on a failed refresh v1.6.1 correctly
 * LEAVES the rendered board intact (both failure branches return before
 * dt10RenderPayload_) — but the banner is overwritten with the raw error, so
 * the operator sees "status: HTTP 502 | <!DOCTYPE html>..." above a full
 * board and cannot tell WHEN that board was produced, or that it was not
 * refreshed at all. Today that read as "the page is stuck / the refresh does
 * nothing". Silence about staleness is the defect, not the preserved board.
 * FIX (display-only, zero decision logic):
 *   1. On every SUCCESSFUL render, remember the run stamp + selected count in
 *      Script Property DT10_LAST_SUCCESS (fail-open: a properties fault never
 *      breaks a refresh).
 *   2. Both failure branches (network + non-200) append
 *      " | BOARD NOT REFRESHED — content is from <stamp> (N selected)" so the
 *      banner tells the whole truth: attempt time, failure, and data age.
 *   3. The success banner surfaces the builder's own pre-gate funnel when the
 *      payload carries kpis.pregate — "pregate 10311->elig 412 kept 300" —
 *      which is the standing answer to "why were only 300 scanned and why did
 *      none pass". Absent key => banner byte-identical to v1.6.1.
 * ES5 only. No pool fields, no gates, no render changes. Additions:
 * dt10RememberSuccess_, dt10StaleNote_, dt10FunnelNote_.
 */
/**
 * v1.6.1 POOL CONTRACT CARRIES THE QUOTE TIMESTAMP (defect fix, 2026-07-21)
 * ------------------------------------------------------------------------------
 * ROOT CAUSE (proven by elimination on 2026-07-21): the backend's Quote
 * Freshness gate (opportunity_builder v1.4.x) and the older 168h Data-Trust
 * ceiling both skip any candidate whose row carries no timestamp — the
 * correct "fail on PROVEN staleness only" philosophy. This cockpit's
 * DT10_POOL_FIELDS never sent `Last Updated (UTC)`, so on the body_rows
 * path EVERY candidate arrived timestamp-less and BOTH gates skipped 100%
 * of the pool, silently, since the day each shipped. The 07:22 board sized
 * PAM.US/MRP.US on pre-close quotes and 4503.T mid-session on a 6h quote;
 * a Render shell diagnostic proved the venue calendar itself healthy —
 * eliminating every backend suspect and leaving only the missing field.
 * FIX: ONE pool-field entry — send `Last Updated (UTC)` (source headers:
 * lastupdatedutc / lastupdated / asof). The builder's alias map lands it in
 * engine_gate.last_updated and both gates engage for real. EXPECT on the
 * first refresh after paste: a WAVE of `STALE_PRICE` deferrals across the
 * stale global tail — that is the gate finally seeing, not a regression.
 * §8 honored: a data field travels; nothing is gated here.
 */
/**
 * v1.6.0 EARNINGS PROXIMITY TAG (W-3, Execution Plan v2.1, 2026-07-21)
 * ------------------------------------------------------------------------------
 * OPERATOR FINDING (evening audit 2026-07-20, export __39_): RCI.US was
 * trimmed 2 days before its print and EXE.US bought 8 days before its own —
 * with Calendar_Events LIVE in the same workbook (updated daily 11:18) and
 * the cockpit blind to it. Tickets carried no event awareness at all.
 * FIX: after stability stamping and BEFORE render, every SELECTED ticket
 * whose symbol shows 0 <= Days To Earnings <= DT10_EARNINGS_TAG_DAYS
 * (default 14) gets its Advisor Note PREFIXED with:  ⚠ earnings ≤Nd ·
 * The prefix survives note truncation in narrow cells, applies to GRACE
 * rows too (their stability note is composed first), and is idempotent —
 * a re-tag on the same objects never doubles.
 * ANNOTATION-ONLY, BY DESIGN: nothing gates, blocks, resizes, or re-ranks —
 * the Gen-2 rule that entries avoid the pre-earnings window lives in the
 * backend risk engine (§7), not here; the cockpit's job is to make the date
 * IMPOSSIBLE TO MISS. The status banner gains "earn ⚠N/M" and one Logger
 * line reports the tag count.
 * FAIL-SAFETY: any Calendar_Events read failure => empty map => zero tags,
 * render untouched. KILL-SWITCH: Script Property DT10_EARNINGS_TAG =
 * 0/false/off/no restores v1.5.0 output byte-for-byte (fail-open ON, the
 * dt10PageStatusEnabled_ pattern). Threshold: Script Property
 * DT10_EARNINGS_TAG_DAYS, clamped 1..60 via dt10StabInt_.
 * NEW FUNCTIONS (5): dt10EarningsTagEnabled_, dt10EarningsTagDays_,
 * dt10EarningsMapFromValues_ (pure), dt10EarningsMap_,
 * dt10ApplyEarningsTags_ (pure), dt10EarningsAnnotate_ — zero removals.
 * ES5 throughout, per the file's own runtime rule.
 */
/**
 * v1.6.3 DEGRADED PAYLOAD IS NOT A REFRESH (2026-07-24)
 * ------------------------------------------------------------------------------
 * WHY: v1.6.2 preserves the board only when the HTTP call FAILS. But the
 * backend's fail-soft paths answer 200 with an EMPTY envelope — status
 * "degraded"/"error"/"unavailable" and zero rows (route v4.12.0's
 * builder_timeout, the builder-unbound path, selector collapse). v1.6.2 would
 * treat that as a good render and WIPE a valid board with an empty one, and
 * the stale note would never fire because the code was 200. Arming the new
 * off-loop budget would therefore have traded "502 + board kept" for
 * "200 + board erased" — a strictly worse screen.
 * FIX: dt10PayloadDegraded_() classifies the envelope BEFORE rendering.
 * Degraded => do not render, keep the last good board, and say so:
 *   status: DEGRADED (builder_timeout) | BOARD NOT REFRESHED — content is
 *   from 2026-07-24 00:31 (7 selected)
 * "ok" and the legitimate empty state "no_candidates" render exactly as
 * before — an honest empty board is a real result and must still be shown.
 * ES5 only; display-only; addition: dt10PayloadDegraded_.
 */
/**
 * v1.6.4 (2026-07-24, review catches): (a) the file HEADER said "Version:
 * 1.5.0" while the runtime constant was 1.6.3 — four releases of doc rot on
 * the one artifact a human reads first; header and constant are now locked
 * together. (b) The degraded classifier gains a CONTRADICTION test: a payload
 * that reports a non-empty scan (kpis.scanned > 0) yet returns no candidate
 * rows at all is a processing failure wearing an empty-result costume, so the
 * board is preserved instead of wiped. A genuine empty scan (scanned = 0 /
 * status "no_candidates") still renders exactly as before.
 */
var DT10_VERSION = '1.19.1';

/** A reversible rollout brake with no fallback to the old funding bug.
 * DT10_BOARD_FUNDING_MODE=research keeps research/stability but allocates zero.
 * Unset/enabled permits the signed replay. Unknown/read-failed is research. */
function dt10BoardFundingMode_() {
  try {
    var mode = String(PropertiesService.getScriptProperties()
        .getProperty('DT10_BOARD_FUNDING_MODE') || '').trim().toLowerCase();
    return !mode || mode === 'enabled' ? 'enabled' : 'research';
  } catch (e) { return 'research'; }
}

/** Funding follows the final stability board, using one frozen backend replay. */
function dt10BoardEligible_(t) {
  return !!t && t._grace_hold !== true && t._ft_suspended !== true &&
      t._p145_suspended !== true && !/^GRACE\b/.test(String(t._stab_status || ''));
}
function dt10BoardFundingAlert_(a) {
  return dt10IsFundingAlert_(a) || String((a && a.type) || '') === 'rotation_proposal';
}
function dt10BoardVerdict_(ss, sourceContext) {
  var out, feedRaw = null;
  if (!dt10UvOn_()) out = { state: 'EXECUTABLE', reason: '', ageMin: null };
  else {
    try { feedRaw = dt10UvRead_(ss); out = dt10UvParse_(feedRaw, Date.now()); }
    catch (e) { out = { state: 'NOT_ACTIONABLE', reason: 'verdict read failed', ageMin: null }; }
  }
  // The rollout token cannot override factual incomplete source evidence.
  var source = dt10SourceReadiness_(ss, sourceContext);
  out.source_readiness = source;
  if (!source.ready) {
    out.state = 'NOT_ACTIONABLE'; out.source_blocked = true;
    out.reason = 'source readiness: ' + source.reason;
  }
  // v1.13.8: DT10_FEED_COHORT_BIND=on binds an EXECUTABLE feed to this
  // board's source cohort. Downgrade only. DT10_UPSTREAM_VERDICT=off never
  // reads the feed (feedRaw stays null), so that bypass skips it as well.
  if (out.state === 'EXECUTABLE' && feedRaw !== null) {
    try {
      if (dt10FeedCohortBindOn_()) {
        var bind = dt10FeedCohortCheck_(feedRaw, source.cohort);
        out.feed_cohort = bind;
        if (bind.ok !== true) {
          out.state = 'NOT_ACTIONABLE'; out.feed_cohort_blocked = true;
          out.reason = bind.reason || 'feed cohort check failed';
        }
      }
    } catch (eBind) {
      out.state = 'NOT_ACTIONABLE'; out.feed_cohort_blocked = true;
      out.reason = 'feed cohort check failed';
    }
  }
  return out;
}
/** v1.13.8: Script Property DT10_FEED_COHORT_BIND. Exactly 'on' (trimmed,
 * any case) arms the feed/cohort binding; absent or any other word is off.
 * A property-service failure reads as ON (fail-closed): the binding can
 * only withhold an EXECUTABLE feed, never permit one. Memoized (P-202). */
var DT10_FEED_COHORT_BIND_PROP = 'DT10_FEED_COHORT_BIND';
function dt10FeedCohortBindOn_() {
  return dt10PropMemoGet_('feed_cohort_bind', function () {
    try {
      return String(PropertiesService.getScriptProperties()
          .getProperty(DT10_FEED_COHORT_BIND_PROP) || '')
          .replace(/^\s+|\s+$/g, '').toLowerCase() === 'on';
    } catch (eProp) { return true; }
  });
}
/** v1.13.8 PURE: does the raw feed value name exactly this cohort?
 * Run tokens follow the Python consumers' widest rule (SOURCE_RUN_RE in
 * scripts/audit_decision_surface_freshness.py): 'run', optional spaces,
 * '=', optional spaces, value up to whitespace ; | or comma, not preceded
 * by a letter, digit or underscore, any case. superseded_by_run= is
 * therefore not a run token. ES5 has no lookbehind, so the preceding
 * character is matched instead; the classes spell out Python's str
 * whitespace (JS \s lacks U+001C-U+001F and U+0085 and adds U+FEFF) and the
 * four non-ASCII letters Python's case-insensitive [a-z] also matches, so
 * the count equals release_decision_feed._run_tokens. Exactly one token, a
 * positive integer equal to the positive-integer cohort, is ok; anything
 * else is a reason. */
function dt10FeedCohortCheck_(raw, cohort) {
  var out = { ok: false, reason: '', run: '', cohort: '' };
  try {
    var text = String(raw === null || raw === undefined ? '' : raw);
    out.cohort = String(cohort === null || cohort === undefined ? '' : cohort);
    var ws = '\\t\\n\\v\\f\\r\\x1c-\\x20\\x85\\xa0\\u1680\\u2000-\\u200a' +
        '\\u2028\\u2029\\u202f\\u205f\\u3000';
    var re = new RegExp('(^|[^a-z0-9_\\u0130\\u0131\\u017f\\u212a])run[' + ws +
        ']*=[' + ws + ']*([^' + ws + ';|,]*)', 'gi'), runs = [], m;
    while ((m = re.exec(text)) !== null) runs.push(m[2]);
    if (!/^[1-9]\d*$/.test(out.cohort)) {
      out.reason = 'cockpit source cohort unverified'; return out;
    }
    if (runs.length !== 1 || !/^[1-9]\d*$/.test(runs[0])) {
      out.reason = 'feed run token missing/ambiguous'; return out;
    }
    out.run = runs[0];
    if (out.run !== out.cohort) {
      out.reason = 'feed run ' + out.run + ' != cohort ' + out.cohort; return out;
    }
    out.ok = true;
  } catch (eCheck) {
    out.ok = false; out.reason = 'feed cohort check failed';
  }
  return out;
}
// ML/MF counts are effective only with the exact dated membership receipt.
// Legacy staged-expansion properties do not override this explicit approval.
// Unscoped Global/commodities properties may still tighten their floors.
var DT10_SOURCE_MIN_ROWS = {Market_Leaders: 255, Global_Markets: 6512,
                          Commodities_FX: 453, Mutual_Funds: 2474};
var DT10_SOURCE_APPROVED_SCOPE = {
  policy_revision: '2026-10-12.current-workbook.v1', approved_on: '2026-10-12',
  pages: {
    Market_Leaders: {approved_count: 255, supersedes_min_rows: 1025,
      symbol_set_sha256: '9c9949e0952b3b6c7ac4de2cdefd7fdc084e0649ecffef48e3e27cdaab703c66'},
    Mutual_Funds: {approved_count: 2474, supersedes_min_rows: 4496,
      symbol_set_sha256: 'd9a3fcb8aefc9de60de28c1e4d2c89464be2dbaf0907e11edd904bd48214bc02'}
  }
};
var DT10_SOURCE_STATUS_MAX_ROWS = 1000;
function dt10SourcePolicy_() {
  return dt10PropMemoGet_('source_readiness_policy', function () {
    var policy = {floors: {}, freshMin: 95, maxAgeMs: 30 * 3600000,
                  approved_scope: DT10_SOURCE_APPROVED_SCOPE, superseded_properties: {}};
    var props;
    try { props = PropertiesService.getScriptProperties(); } catch (eP) {}
    DT10_POOL_PAGES.forEach(function (page) {
      var floor = DT10_SOURCE_MIN_ROWS[page], value;
      try { value = Number(props.getProperty('TFB_EXPECTED_MIN_ROWS_' + page.toUpperCase())); } catch (eF) {}
      if (Object.prototype.hasOwnProperty.call(DT10_SOURCE_APPROVED_SCOPE.pages, page)) {
        floor = DT10_SOURCE_APPROVED_SCOPE.pages[page].approved_count;
        if (isFinite(value) && value > 0 && Math.floor(value) === value) policy.superseded_properties[page] = value;
      } else if (isFinite(value) && value > floor && Math.floor(value) === value) floor = value;
      policy.floors[page] = floor;
    });
    return policy;
  });
}
/** _Status clocks use their declared offset, or established Riyadh wall time.
 * Date-only, malformed and impossible dates never acquire an invented age. */
function dt10SourceStampMs_(value) {
  if (Object.prototype.toString.call(value) === '[object Date]') {
    var dateMs = value.getTime(); return isFinite(dateMs) ? dateMs : null;
  }
  var m = String(value || '').trim().match(
      /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,3}))?(Z|[+-]\d{2}:?\d{2})?$/);
  if (!m) return null;
  var year = +m[1], month = +m[2], day = +m[3], hour = +m[4], minute = +m[5], second = +m[6];
  if (year < 1900 || month < 1 || month > 12 || day < 1 || day > 31 ||
      hour > 23 || minute > 59 || second > 59) return null;
  var utc = Date.UTC(year, month - 1, day, hour, minute, second,
      m[7] ? Number((m[7] + '00').slice(0, 3)) : 0);
  var check = new Date(utc);
  if (check.getUTCFullYear() !== year || check.getUTCMonth() !== month - 1 ||
      check.getUTCDate() !== day) return null;
  var offset = 180; // _Status naive Last Updated is Riyadh, never UTC.
  if (m[8] === 'Z') offset = 0;
  else if (m[8]) {
    var parts = m[8].match(/^([+-])(\d{2}):?(\d{2})$/);
    if (+parts[2] > 23 || +parts[3] > 59) return null;
    offset = (+parts[2] * 60 + +parts[3]) * (parts[1] === '-' ? -1 : 1);
  }
  return utc - offset * 60000;
}
function dt10SourceToken_(message, name) {
  var re = new RegExp('(?:^|[\\s|])' + name + '=([^\\s|]+)', 'gi');
  var first = re.exec(String(message || ''));
  return first && !re.exec(String(message || '')) ? first[1] : '';
}
function dt10SourceCount_(value) {
  if (typeof value === 'boolean' || value === null || value === undefined || String(value).trim() === '') return null;
  var n = Number(String(value).replace(/,/g, ''));
  return isFinite(n) && n >= 0 && n <= 9007199254740991 && Math.floor(n) === n ? n : null;
}
/** One digest of the complete physical column. Trim+upper spelling is the
 * only normalization: aliases are distinct, duplicate/bad rows are failures,
 * and the digest serializes sorted names plus one trailing newline. */
function dt10SourceScopeCore_(rawSymbols, spec, approval) {
  var out = {ready: false, policy_revision: approval.policy_revision,
      approved_on: approval.approved_on, expected_count: spec.approved_count,
      expected_symbol_set_sha256: spec.symbol_set_sha256,
      supersedes_min_rows: spec.supersedes_min_rows, actual_count: rawSymbols.length,
      actual_unique_count: 0, duplicate_count: 0, bad_rows: 0,
      observed_symbol_set_sha256: '', reason: ''};
  var symbols = [], seen = {};
  for (var i = 0; i < rawSymbols.length; i++) {
    var raw = rawSymbols[i], symbol = typeof raw === 'string' ? raw.trim().toUpperCase() : '';
    if (!symbol || symbol.length > 256 || /[\s\x00-\x1f\x7f-\x9f\u200b-\u200f\u2028-\u202e\u2060-\u206f]/.test(symbol)) {
      out.bad_rows++; continue;
    }
    var key = '$' + symbol;
    if (Object.prototype.hasOwnProperty.call(seen, key)) out.duplicate_count++;
    else { seen[key] = true; symbols.push(symbol); }
  }
  out.actual_unique_count = symbols.length;
  var errors = [];
  if (out.bad_rows) errors.push('invalid Symbol rows ' + out.bad_rows);
  if (out.duplicate_count) errors.push('duplicate Symbol rows ' + out.duplicate_count);
  if (out.actual_count !== spec.approved_count) errors.push('physical count ' + out.actual_count + ' differs from approved ' + spec.approved_count);
  if (!out.bad_rows && !out.duplicate_count) {
    try {
      var bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256,
          symbols.sort().join('\n') + '\n', Utilities.Charset.UTF_8), hex = '';
      for (var b = 0; b < bytes.length; b++) hex += ('0' + ((bytes[b] + 256) % 256).toString(16)).slice(-2);
      if (!/^[0-9a-f]{64}$/.test(hex)) throw new Error('digest unavailable');
      out.observed_symbol_set_sha256 = hex;
      if (!(spec.binding_only === true && spec.symbol_set_sha256 === null) && hex !== spec.symbol_set_sha256) {
        errors.push(spec.binding_only ? 'collected pool membership mismatch' : 'exact approved membership mismatch');
      }
    } catch (eDigest) { errors.push('membership digest unavailable'); }
  }
  out.reason = errors.join('; '); out.ready = !errors.length;
  return out;
}
/** Read full source columns, independently of pool caps/deduplication and
 * before trusting _Status. Header-only, missing, torn or oversized reads are
 * unverified; no row may disappear through filtering or projection. */
function dt10SourceScopeRead_(ss, policy, sourceContext) {
  var approval = policy.approved_scope, receipts = {_physical_columns: {}};
  var frame = sourceContext && sourceContext.pool_reconciliation;
  (frame ? DT10_POOL_PAGES : ['Market_Leaders', 'Mutual_Funds']).forEach(function (page) {
    try {
      var spec = approval.pages[page], sheet = ss.getSheetByName(page);
      if (!spec) {
        if (!frame.pages || !frame.pages[page] || !frame.physical_symbol_sha256) throw new Error('physical pool binding missing');
        spec = {approved_count: frame.pages[page].physical,
          symbol_set_sha256: frame.physical_symbol_sha256[page],
          supersedes_min_rows: policy.floors[page], binding_only: true};
      }
      if (!sheet) throw new Error('source sheet missing');
      var rows = sheet.getLastRow(), cols = sheet.getLastColumn();
      if (!(rows >= 2) || rows > DT10_POOL_HARD_CAP + DT10_HEADER_SCAN_ROWS ||
          !(cols >= 1) || cols > 512) throw new Error('source dimensions invalid');
      var head = sheet.getRange(1, 1, Math.min(rows, DT10_HEADER_SCAN_ROWS), cols).getValues();
      var headerRow = -1, symbolCol = -1;
      for (var r = 0; r < head.length; r++) {
        for (var c = 0; c < head[r].length; c++) {
          if (typeof head[r][c] !== 'string' || head[r][c].trim().toUpperCase() !== 'SYMBOL') continue;
          if (headerRow !== -1) throw new Error('ambiguous Symbol header');
          headerRow = r + 1; symbolCol = c + 1;
        }
      }
      if (headerRow < 0 || rows <= headerRow) throw new Error('Symbol header/data missing');
      var grid = sheet.getRange(headerRow + 1, symbolCol, rows - headerRow, 1).getValues();
      if (grid.length !== rows - headerRow) throw new Error('incomplete Symbol read');
      var rawSymbols = [];
      for (var g = 0; g < grid.length; g++) {
        if (!Array.isArray(grid[g]) || grid[g].length !== 1) throw new Error('invalid Symbol column shape');
        rawSymbols.push(grid[g][0]);
      }
      if (sheet.getLastRow() !== rows || sheet.getLastColumn() !== cols) throw new Error('source dimensions changed during membership read');
      receipts[page] = dt10SourceScopeCore_(rawSymbols, spec, approval);
      receipts._physical_columns[page] = rawSymbols;
    } catch (eScope) {
      receipts[page] = {ready: false, policy_revision: approval.policy_revision,
          reason: String(eScope && eScope.message || eScope)};
    }
  });
  return receipts;
}
/** Hash exactly the fixed-order JSON preceding the sha256 field. */
function dt10PoolFrameSha_(frame) {
  var bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256,
      JSON.stringify(frame), Utilities.Charset.UTF_8), hex = '';
  for (var i = 0; i < bytes.length; i++) hex += ('0' + ((bytes[i] + 256) % 256).toString(16)).slice(-2);
  if (!/^[0-9a-f]{64}$/.test(hex)) throw new Error('pool receipt checksum unavailable');
  return hex;
}
function dt10PoolExclusionOrder_(a, b) {
  var ak = DT10_POOL_PAGES.indexOf(a.page) + '\n' + a.symbol + '\n' + a.owner;
  var bk = DT10_POOL_PAGES.indexOf(b.page) + '\n' + b.symbol + '\n' + b.owner;
  return ak < bk ? -1 : (ak > bk ? 1 : 0);
}
/** Receipt uses actual collected columns and actual inclusion/exclusion
 * events. An absent initial source proof never acquires an invented run. */
function dt10PoolFrameBuild_(observed, unique, included, excluded, sourceContext) {
  try {
    if (!sourceContext || sourceContext.initial_ready !== true ||
        typeof sourceContext.cohort !== 'string' || !/^[1-9]\d*$/.test(sourceContext.cohort)) return null;
    var pages = {}, hashes = {};
    for (var i = 0; i < DT10_POOL_PAGES.length; i++) {
      var page = DT10_POOL_PAGES[i], receipt = observed[page];
      if (!receipt || receipt.ready !== true) return null;
      pages[page] = {physical: receipt.actual_count, unique: unique[page], included: included[page]};
      hashes[page] = receipt.observed_symbol_set_sha256;
    }
    var frame = {version: 1, run: sourceContext.cohort, pages: pages,
      excluded: excluded.slice().sort(dt10PoolExclusionOrder_), physical_symbol_sha256: hashes};
    frame.sha256 = dt10PoolFrameSha_(frame);
    if (JSON.stringify(frame).length > 30000) return null;
    return frame;
  } catch (eFrame) { return null; }
}
/** Recompute first-owner exclusions from independent current full columns,
 * validate canonical bytes/counts/hash/cohort and reject capped scans. */
function dt10PoolFrameValidate_(frame, observed, sourceContext) {
  var out = {ready: false, reason: ''};
  try {
    if (!frame || frame.version !== 1 || typeof frame.run !== 'string' ||
        !/^[1-9]\d*$/.test(frame.run) || frame.run !== sourceContext.cohort) throw new Error('pool receipt cohort unverified');
    var pages = {}, hashes = {}, seen = {}, excluded = [], sent = 0;
    var columns = observed && observed._physical_columns;
    for (var i = 0; i < DT10_POOL_PAGES.length; i++) {
      var page = DT10_POOL_PAGES[i], receipt = observed && observed[page];
      var rows = columns && columns[page], declared = frame.pages && frame.pages[page];
      if (!receipt || receipt.ready !== true || !Array.isArray(rows) || !declared ||
          !frame.physical_symbol_sha256 || receipt.observed_symbol_set_sha256 !== frame.physical_symbol_sha256[page]) {
        throw new Error(page + ' physical pool membership unverified');
      }
      var unique = 0;
      for (var r = 0; r < rows.length; r++) {
        var symbol = dt10NormSym_(rows[r]), key = '$' + symbol;
        if (Object.prototype.hasOwnProperty.call(seen, key)) {
          if (seen[key] === page) throw new Error(page + ' duplicate physical Symbol');
          excluded.push({symbol: symbol, page: page, owner: seen[key]});
        } else { seen[key] = page; unique++; }
      }
      if (declared.physical !== rows.length || declared.unique !== unique ||
          dt10SourceCount_(declared.included) === null || declared.included !== unique ||
          !sourceContext.pool_counts || sourceContext.pool_counts[page] !== unique ||
          !sourceContext.pool_included_counts || sourceContext.pool_included_counts[page] !== declared.included) {
        throw new Error(page + ' pool counts incomplete, capped or unexplained');
      }
      pages[page] = {physical: rows.length, unique: unique, included: declared.included};
      hashes[page] = receipt.observed_symbol_set_sha256; sent += declared.included;
    }
    if (sourceContext.pool_sent_total !== sent) throw new Error('pool sent total mismatch');
    var canonical = {version: 1, run: frame.run, pages: pages,
      excluded: excluded.sort(dt10PoolExclusionOrder_), physical_symbol_sha256: hashes};
    canonical.sha256 = dt10PoolFrameSha_(canonical);
    if (JSON.stringify(canonical) !== JSON.stringify(frame)) throw new Error('pool exclusion records/checksum mismatch');
    out.ready = true; out.sha256 = frame.sha256; out.run = frame.run;
    out.excluded = excluded; out.pages = pages;
  } catch (ePool) { out.reason = String(ePool && ePool.message || ePool); }
  return out;
}
/** PURE readiness facts; sourceContext binds the exact pre-pool generation. */
function dt10SourceReadinessCore_(grid, nowMs, sourceContext, policy, scopeReceipts) {
  var out = {ready: false, reason: '', issues: [], generation: {}, cohort: '', approved_scope: {}};
  var rows = {}, index = {}, header = (grid || [])[0] || [];
  policy = policy || {floors: DT10_SOURCE_MIN_ROWS, freshMin: 95, maxAgeMs: 30 * 3600000,
      approved_scope: DT10_SOURCE_APPROVED_SCOPE, superseded_properties: {}};
  function issue(text) { out.issues.push(text); }
  var poolProof = null;
  for (var h = 0; h < header.length; h++) {
    var token = dt10NormToken_(header[h]);
    if (token && !Object.prototype.hasOwnProperty.call(index, token)) index[token] = h;
    else if (token) issue('duplicate _Status header ' + token);
  }
  ['page', 'lastupdated', 'status', 'message', 'rows'].forEach(function (key) {
    if (!Object.prototype.hasOwnProperty.call(index, key)) issue('_Status missing ' + key);
  });
  if (typeof nowMs !== 'number' || !isFinite(nowMs)) issue('decision clock invalid');
  if (out.issues.length) { out.reason = out.issues.join('; '); return out; }
  if (sourceContext && sourceContext.pool_counts) {
    poolProof = dt10PoolFrameValidate_(sourceContext.pool_reconciliation, scopeReceipts, sourceContext);
    out.pool_reconciliation = poolProof;
    if (!poolProof.ready) issue('collected pool reconciliation unverified: ' + poolProof.reason);
  }
  for (var r = 1; r < grid.length; r++) {
    var row = grid[r] || [], page = String(row[index.page] || '').trim();
    if (!Object.prototype.hasOwnProperty.call(policy.floors, page)) continue;
    if (Object.prototype.hasOwnProperty.call(rows, page)) { issue(page + ' duplicate source status'); continue; }
    rows[page] = row;
  }
  DT10_POOL_PAGES.forEach(function (page) {
    var row = rows[page], floor = policy.floors[page];
    if (!row) { issue(page + ' source status missing'); return; }
    var state = String(row[index.status] || '').trim().toUpperCase();
    var message = String(row[index.message] || '');
    var stamp = dt10SourceStampMs_(row[index.lastupdated]);
    var count = dt10SourceCount_(row[index.rows]);
    var scope = policy.approved_scope.pages[page], receipt = scopeReceipts && scopeReceipts[page];
    var scopeGeneration = null;
    if (scope) {
      if (!receipt || receipt.ready !== true || receipt.policy_revision !== policy.approved_scope.policy_revision ||
          receipt.actual_count !== scope.approved_count || receipt.actual_unique_count !== scope.approved_count ||
          receipt.observed_symbol_set_sha256 !== scope.symbol_set_sha256) {
        issue(page + ' approved scope unverified: ' + ((receipt && receipt.reason) || 'membership receipt unavailable'));
      }
      out.approved_scope[page] = receipt || {ready: false, reason: 'membership receipt unavailable'};
      if (count !== scope.approved_count) issue(page + ' status count differs from approved scope ' + scope.approved_count);
      scopeGeneration = [policy.approved_scope.policy_revision, receipt ? receipt.actual_count : null,
          receipt ? receipt.observed_symbol_set_sha256 : null, receipt ? receipt.ready : false];
    }
    if (poolProof && poolProof.ready && count !== poolProof.pages[page].physical) {
      issue(page + ' source status differs from collected physical count');
    }
    out.generation[page] = JSON.stringify([String(row[index.lastupdated]), state, message, count, scopeGeneration]);
    // Fresh independent page stamps cannot certify one publication cycle.
    // The scheduled producer publishes one positive GitHub run ID per row;
    // a missing/duplicate/reusable "local" value proves no shared cohort.
    var run = dt10SourceToken_(message, 'run');
    if (!/^[1-9]\d*$/.test(run)) issue(page + ' source publication run unverified');
    else if (!out.cohort) out.cohort = run;
    else if (out.cohort !== run) issue(page + ' source run ' + run + ' differs from cohort ' + out.cohort);
    if (!/^(OK|SUCCESS|VALID|PASS|COMPLETE)$/.test(state)) issue(page + ' status=' + (state || 'unknown'));
    if (stamp === null) issue(page + ' source timestamp unverified');
    else if (stamp > nowMs) issue(page + ' source publication is in the future');
    else if (nowMs - stamp > policy.maxAgeMs) issue(page + ' source stale (>30h)');
    if (count === null || count < floor) issue(page + ' rows ' + (count === null ? 'unknown' : count) + ' below floor ' + floor);
    var acquired = dt10SourceToken_(message, 'acquired').match(/^(\d+)\/(\d+)$/);
    var fresh = acquired ? Number(acquired[1]) : null, requested = acquired ? Number(acquired[2]) : null;
    if (fresh === null || requested === null || !isFinite(fresh) || !isFinite(requested) ||
        requested > 9007199254740991 || requested <= 0 || fresh > requested) issue(page + ' acquisition receipt unknown/invalid');
    else {
      if (scope && requested !== scope.approved_count) issue(page + ' acquisition denominator differs from approved scope ' + scope.approved_count);
      if (poolProof && poolProof.ready && requested !== poolProof.pages[page].physical) {
        issue(page + ' acquisition denominator differs from collected physical count');
      }
      if (requested < floor) issue(page + ' acquired denominator ' + requested + ' below floor ' + floor);
      if (fresh * 100 < policy.freshMin * requested) issue(page + ' acquired ' + fresh + '/' + requested + ' below ' + policy.freshMin + '%');
    }
    if (dt10SourceToken_(message, 'acquisition').toUpperCase() !== 'COMPLETE') issue(page + ' acquisition not COMPLETE');
    if (dt10SourceToken_(message, 'data').toUpperCase() !== 'COMPLETE') issue(page + ' data not COMPLETE');
    var effective = dt10SourceToken_(message, 'fetchfail_effective').toLowerCase();
    if (effective === 'error') issue(page + ' acquisition policy error');
    else if (!/^(off|observe|enforce)$/.test(effective)) issue(page + ' acquisition policy unknown/ambiguous');
    if (sourceContext) {
      if (!sourceContext.generation || sourceContext.generation[page] !== out.generation[page]) issue(page + ' changed after decision source capture');
      if (stamp !== null && stamp > sourceContext.captured_at_ms) issue(page + ' source newer than decision capture');
      if (sourceContext.pool_counts) {
        var poolN = dt10SourceCount_(sourceContext.pool_counts[page]);
        if (poolN === null || poolN < floor) {
          var explained = poolProof && poolProof.ready && poolProof.pages[page].unique === poolN &&
              poolProof.pages[page].physical >= floor;
          if (!explained) issue(page + ' available pool ' + (poolN === null ? 'unknown' : poolN) + ' below floor ' + floor);
        }
        if (scope) {
          var used = sourceContext.pool_scope_receipts && sourceContext.pool_scope_receipts[page];
          if (!used || used.ready !== true || used.policy_revision !== policy.approved_scope.policy_revision ||
              used.actual_count !== scope.approved_count || used.actual_unique_count !== scope.approved_count ||
              used.observed_symbol_set_sha256 !== scope.symbol_set_sha256) {
            issue(page + ' collected physical pool scope unverified');
          }
        }
      }
    }
  });
  if (sourceContext && (sourceContext.initial_ready !== true || typeof sourceContext.captured_at_ms !== 'number' ||
      !isFinite(sourceContext.captured_at_ms))) {
    issue('decision source capture was not ready');
  }
  if (poolProof && poolProof.ready && poolProof.run !== out.cohort) issue('collected pool cohort differs from current sources');
  out.ready = out.issues.length === 0; out.reason = out.issues.join('; ');
  return out;
}
/** Bounded read, once per readiness boundary; no row or sheet mutations. */
function dt10SourceReadiness_(ss, sourceContext) {
  try {
    var policy = dt10SourcePolicy_();
    var scopeReceipts = dt10SourceScopeRead_(ss, policy, sourceContext);
    var sheet = ss.getSheetByName('_Status');
    if (!sheet) throw new Error('_Status missing');
    var rows = sheet.getLastRow(), cols = sheet.getLastColumn();
    if (!(rows >= 2) || rows > DT10_SOURCE_STATUS_MAX_ROWS || !(cols >= 7)) throw new Error('_Status dimensions invalid');
    var values = sheet.getRange(1, 1, rows, Math.min(cols, 10)).getValues();
    var result = dt10SourceReadinessCore_(values, Date.now(), sourceContext, policy, scopeReceipts);
    result.scope_policy_revision = policy.approved_scope.policy_revision;
    result.superseded_scope_properties = policy.superseded_properties;
    return result;
  } catch (eSource) {
    return {ready: false, reason: 'source status unavailable', issues: ['source status unavailable'], generation: {}};
  }
}
function dt10SourceCapture_(ss) {
  var captured = Date.now(), source = dt10SourceReadiness_(ss);
  return {captured_at_ms: captured, initial_ready: source.ready, generation: source.generation, cohort: source.cohort || ''};
}
/** Counts describe available sheet input, never approved-universe membership. */
function dt10PoolNote_(pool, source) {
  var frame = pool.pool_reconciliation;
  var parts = DT10_POOL_PAGES.map(function (page) {
    return page + ' ' + (pool.perPage[page] || 0) + '/' +
        (frame ? frame.pages[page].physical : (pool.available[page] || 0));
  });
  return 'sheets pool ' + pool.rows.length + ' rows [' + parts.join(', ') + '] ' +
      (pool.duplicatesSkipped ? '(' + pool.duplicatesSkipped + ' duplicate symbols removed) ' : '') +
      (pool.truncated ? '(TRUNCATED to cap ' + pool.cap + ' of ' + pool.total + ' available)' : '(all available sheet rows)') +
      (frame ? ' | pool_reconciliation_v1=' + JSON.stringify(frame) : '') +
      ' | source readiness ' + (source.ready ? 'VERIFIED (run ' + source.cohort + ')' : 'WITHHELD: ' + source.reason);
}
/** Local display result only; an empty Sheets pool never invokes providers
 * or fabricates backend acquisition/allocation proof. */
function dt10EmptySheetPayload_() {
  return {version: 'native-empty-sheet-pool', status: 'ok', selected: [],
      candidates_rows: [], alerts: [], near_miss: [],
      kpis: {scanned: 0, passed: 0, selected_count: 0},
      meta: {pool_source: 'sheets_empty', board_funding: {contract_version: 1,
          stage: 'research', finalized: true, eligible_symbols: [],
          reason: 'empty sheet pool; no allocation requested'}}};
}
function dt10BoardFundingFail_(payload, reason) {
  payload.meta = payload.meta || {};
  payload.meta.board_funding = payload.meta.board_funding || {};
  payload.meta.board_funding.finalized = false;
  payload.meta.board_funding.reason = reason;
  delete payload.meta.board_funding.snapshot;
  (payload.selected || []).forEach(function (t) { t._board_unfunded = true; });
  payload.alerts = (payload.alerts || []).filter(function (a) {
    return a.type !== 'board_funding_unavailable';
  });
  payload.alerts.push({type: 'board_funding_unavailable', count: 1,
      required_action: 'Final board allocation unavailable: ' + reason +
          '. Research seats reserve no cash; retry after the backend and cockpit versions agree.'});
}
function dt10ReallocateBoard_(payload, body) {
  var meta = payload.meta || {}, funding = meta.board_funding || {};
  if (dt10BoardFundingMode_() !== 'enabled') {
    dt10BoardFundingFail_(payload, 'research-only mode; allocation disabled');
    payload.meta.board_funding.mode = 'research';
    return;
  }
  var snapshot = funding.snapshot;
  if (funding.contract_version !== 1 || funding.stage !== 'research' ||
      !funding.snapshot_available || !snapshot ||
      snapshot.builder_version !== payload.version ||
      snapshot.snapshot_id !== funding.snapshot_id || !Array.isArray(snapshot.rows)) {
    dt10BoardFundingFail_(payload, funding.reason || 'frozen funding contract unsupported');
    return;
  }
  var eligible = (payload.selected || []).filter(dt10BoardEligible_).map(function (t) {
    return dt10NormSym_(t.symbol);
  });
  funding.eligible_symbols = eligible;
  // No allocation request can promote a seat or advance stability clocks.
  if (!eligible.length || (payload._dt10_uv && payload._dt10_uv.state !== 'EXECUTABLE')) {
    funding.finalized = true;
    funding.eligible_symbols = [];
    delete funding.snapshot;
    return;
  }
  try {
    var replay = JSON.parse(JSON.stringify(body));
    replay.rows = snapshot.rows;
    replay.criteria.board_funding_stage = 'allocate';
    replay.criteria.board_funding_symbols = eligible;
    replay.criteria.board_funding_snapshot = snapshot;
    var response = dt10Post_(replay), allocated = response.json;
    var af = allocated && allocated.meta && allocated.meta.board_funding;
    if (response.code !== 200 || !allocated || dt10PayloadDegraded_(allocated) ||
        allocated.status !== 'ok' || allocated.version !== payload.version || !af ||
        af.contract_version !== 1 || af.stage !== 'allocate' ||
        af.snapshot_id !== snapshot.snapshot_id) {
      throw new Error((allocated && allocated.message) || 'allocation response/version/snapshot mismatch');
    }
    var bySymbol = {};
    (allocated.selected || []).forEach(function (t) {
      var sym = dt10NormSym_(t.symbol);
      if (eligible.indexOf(sym) < 0 || bySymbol[sym]) throw new Error('unexpected allocation symbol');
      bySymbol[sym] = t;
    });
    payload.selected = (payload.selected || []).map(function (seat, index) {
      var ticket = bySymbol[dt10NormSym_(seat.symbol)];
      if (ticket && dt10BoardEligible_(seat)) {
        var out = {};
        Object.keys(seat).forEach(function (key) { out[key] = seat[key]; });
        Object.keys(ticket).forEach(function (key) { out[key] = ticket[key]; });
        delete out._board_research;
        delete out._board_unfunded;
        out.rank = index + 1;
        return out;
      }
      seat._board_unfunded = true;
      return seat;
    });
    if (payload._dt10_stability_disabled === true) payload.selected = allocated.selected || [];
    var deferrals = {};
    (allocated.candidates_rows || []).forEach(function (c) {
      if (eligible.indexOf(dt10NormSym_(c.symbol)) >= 0) deferrals[dt10NormSym_(c.symbol)] = c.deferral || '';
    });
    (payload.candidates_rows || []).forEach(function (c) {
      if (deferrals.hasOwnProperty(dt10NormSym_(c.symbol))) c.deferral = deferrals[dt10NormSym_(c.symbol)];
    });
    payload.near_miss = (payload.near_miss || []).filter(function (n) {
      return !dt10IsFundingNearMiss_(n);
    }).concat((allocated.near_miss || []).filter(function (n) {
      return eligible.indexOf(dt10NormSym_(n.symbol)) >= 0;
    }));
    payload.alerts = (payload.alerts || []).filter(function (a) {
      return !dt10BoardFundingAlert_(a) && a.type !== 'cash_floor';
    }).concat((allocated.alerts || []).filter(function (a) {
      return dt10BoardFundingAlert_(a) || a.type === 'cash_floor';
    }));
    if (allocated.meta.cash_floor) payload.meta.cash_floor = allocated.meta.cash_floor;
    ['fundable_by_rotation', 'capital_call', 'capital_call_topn_sar', 'rotation_insufficient'].forEach(function (key) {
      if (allocated.kpis.hasOwnProperty(key)) payload.kpis[key] = allocated.kpis[key];
    });
    funding.finalized = true;
    funding.stage = 'allocate';
    funding.allocation_version = allocated.version;
    delete funding.snapshot; // never carry raw snapshot rows into logs/briefs
  } catch (e) {
    dt10BoardFundingFail_(payload, String(e).slice(0, 160));
    delete funding.snapshot;
  }
}
/** Python round(value, 0) parity for the displayed-ticket observation basis. */
function dt10CashFloorRound_(value) {
  var base = Math.floor(value);
  if (value - base === 0.5) return base % 2 === 0 ? base : base + 1;
  return Math.round(value);
}
/** Preserve the backend policy head and pre/post wording; refresh only its
 * observation tail. Unsupported wording gets an explicit final-board tail. */
function dt10CashFloorObserveText_(original, floor) {
  var shortSar = Number(floor.would_unfund_sar || 0).toFixed(0)
      .replace(/\B(?=(\d{3})+(?!\d))/g, ',') + ' SAR';
  var tail = floor.would_unfund_seats + ' of ' + floor.seats_sized +
      ' sized seat(s) would lose funding (' + shortSar + '). No ticket changed.';
  var text = String(original || ''), marker = text.indexOf(' (observe):');
  var separator = marker >= 0 ? text.indexOf(';', marker) : -1;
  if (separator >= 0) return text.slice(0, separator) + '; ' + tail;
  var head = marker >= 0 ? text.slice(0, marker) : 'Cash floor';
  return head + ' (observe): final executable board; ' + tail;
}
/** Observation only: mirror backend _cash_floor_finalize on final tickets.
 * No sizing, policy context, reserves or budget fields are changed. */
function dt10FinalizeCashFloor_(payload, executable) {
  var floor = payload.meta && payload.meta.cash_floor;
  if (!floor || typeof floor !== 'object' || Array.isArray(floor)) return payload;
  floor.seats_sized = executable.length;
  if (floor.mode !== 'observe') return payload;
  var post = Number(floor.deployable_post_sar || 0), cumulative = 0, seats = 0, short = 0;
  executable.forEach(function (t) {
    var size = dt10CashFloorRound_(Number(t.suggested_sar || 0));
    cumulative += size;
    if (size > 0 && cumulative > post + 0.5) {
      seats += 1;
      short += Math.min(size, cumulative - post);
    }
  });
  floor.would_unfund_seats = seats;
  floor.would_unfund_sar = dt10CashFloorRound_(short);
  if (Array.isArray(payload.alerts)) payload.alerts = payload.alerts.map(function (alert) {
    if (!alert || alert.type !== 'cash_floor') return alert;
    var out = {};
    Object.keys(alert).forEach(function (key) { out[key] = alert[key]; });
    out.count = seats;
    out.required_action = dt10CashFloorObserveText_(alert.required_action, floor);
    return out;
  });
  return payload;
}
/** Idempotent: every monetary aggregate and narrative uses final actions. */
function dt10FinalizeBoard_(payload) {
  payload.kpis = payload.kpis || {};
  payload.meta = payload.meta || {};
  // Check again at render/finalization: the rollout brake may have changed
  // during the HTTP call, and direct render callers must obey it too.
  if (dt10BoardFundingMode_() !== 'enabled') {
    dt10BoardFundingFail_(payload, 'research-only mode; allocation disabled');
    payload.meta.board_funding.mode = 'research';
  }
  var k = payload.kpis, funding = payload.meta.board_funding || {};
  delete funding.snapshot;
  var feedOk = !payload._dt10_uv || payload._dt10_uv.state === 'EXECUTABLE';
  var allocatedSafe = funding.finalized === true && funding.stage === 'allocate' &&
      funding.contract_version === 1 && funding.allocation_version === payload.version;
  var executable = [], selectedSymbols = {}, heldSeatReasons = {};
  (payload.selected || []).forEach(function (t) {
    var executableNow = feedOk && allocatedSafe &&
        (funding.eligible_symbols || []).indexOf(dt10NormSym_(t.symbol)) >= 0 &&
        dt10BoardEligible_(t) && t._board_research !== true &&
        t._board_unfunded !== true && Number(t.suggested_shares) > 0 && Number(t.suggested_sar) > 0;
    if (executableNow) {
      executable.push(t); selectedSymbols[dt10NormSym_(t.symbol)] = true;
    } else {
      t.suggested_sar = 0; t.suggested_shares = 0; t.exp_gain_12m_sar = 0;
      ['engine_exp_gain_12m_sar', 'valuation_exp_gain_12m_sar'].forEach(function (key) {
        if (t.hasOwnProperty(key)) t[key] = 0;
      });
      // Research identity/forecast/stability stays visible; an order surface
      // never accompanies an unfunded final seat, including replay failure.
      t.entry_zone = '\u2014'; t.stop_sar = '\u2014'; t.tp1_sar = '\u2014'; t.tp2_sar = '\u2014';
      t.detail = t.detail || {}; t.detail.funds_from = 'Research — no allocation';
      if (!allocatedSafe || t._board_research === true || t._board_unfunded === true || !feedOk) {
        t.advisor_note = 'Research seat — no executable allocation today. ' + String(t._stab_status || '');
      }
      var heldKey = dt10NormSym_(t.symbol);
      if (heldKey && !Object.prototype.hasOwnProperty.call(heldSeatReasons, heldKey)) {
        heldSeatReasons[heldKey] = dt10HeldSeatReason_(t);
      }
    }
  });
  function sum(key) {
    return Math.round(executable.reduce(function (total, t) { return total + (Number(t[key]) || 0); }, 0));
  }
  function blend(getValue, digits) {
    var weight = 0, total = 0;
    executable.forEach(function (t) {
      var v = getValue(t), w = Number(t.suggested_sar);
      if (v !== null && v !== undefined && isFinite(Number(v))) { total += Number(v) * w; weight += w; }
    });
    return weight ? Math.round(total / weight * Math.pow(10, digits)) / Math.pow(10, digits) : null;
  }
  k.selected_count = executable.length; k.fundable_now = executable.length;
  k.total_suggested_sar = sum('suggested_sar'); k.expected_gain_12m_sar = sum('exp_gain_12m_sar');
  k.capital_unallocated_sar = Math.round((Number(k.deployable_sar) || 0) - k.total_suggested_sar);
  ['engine_expected_gain_12m_sar', 'valuation_expected_gain_12m_sar'].forEach(function (key) {
    if (k.hasOwnProperty(key)) {
      var field = key.replace('expected', 'exp');
      var known = executable.some(function (t) { return t[field] !== null && t[field] !== undefined; });
      k[key] = executable.length && !known ? null : sum(field);
    }
  });
  k.blended_reliability = blend(function (t) { return t.reliability; }, 1);
  var rrBasis = (payload.meta.board_execution_basis || {}).blended_rr || 'tp2';
  k.blended_rr = blend(function (t) { return (t.detail || {})[rrBasis === 'plan' ? 'rr' : 'rr_tp2']; }, 2);
  var plansAllowed = feedOk && allocatedSafe &&
      (funding.eligible_symbols || []).length > 0;
  if (!plansAllowed) {
    ['fundable_by_rotation', 'capital_call', 'capital_call_topn_sar', 'rotation_insufficient'].forEach(function (key) {
      if (k.hasOwnProperty(key)) k[key] = 0;
    });
    payload.alerts = (payload.alerts || []).filter(function (a) { return !dt10BoardFundingAlert_(a); });
    payload.near_miss = (payload.near_miss || []).map(function (n) {
      if (!dt10IsFundingNearMiss_(n)) return n;
      var out = {}; Object.keys(n).forEach(function (key) { out[key] = n[key]; });
      out.required = 'Research — no executable funding action';
      out.current = 'No cash reserved'; out.improve_note = 'Recheck qualification and confirmation before funding.';
      return out;
    });
  }
  (payload.candidates_rows || []).forEach(function (c) {
    var key = dt10NormSym_(c.symbol);
    c.selected = !!selectedSymbols[key];
    // Display annotation only: the executable flag and backend deferral
    // retain their own meanings. Re-finalization clears stale membership.
    delete c._dt10_held_seat_reason;
    if (!c.selected && Object.prototype.hasOwnProperty.call(heldSeatReasons, key)) {
      c._dt10_held_seat_reason = heldSeatReasons[key];
    }
  });
  payload.meta.board_execution = {executable_count: executable.length,
      spend_sar: k.total_suggested_sar, gain_sar: k.expected_gain_12m_sar,
      allocation_finalized: funding.finalized === true};
  dt10FinalizeCashFloor_(payload, executable);
  return payload;
}
/* v1.11.1 (2026-09-03) — MORNING TRIGGER TARGET RESTORED + OUTPUT TRUTH
 *  (1) tfbMorningCockpitRefresh(): the 08:07 time-driven trigger pointed at
 *      a function this file no longer defined ("Script function not found",
 *      100% error rate). Restored as a UI-free wrapper around
 *      refreshDecisionTop10() that logs the feed verdict it saw and reports
 *      any throw through the cockpit-owned _Status row. Trigger untouched.
 *  (2) OUTPUT TRUTH. dt10OutputStatus_ read a payload key the backend never
 *      sends (meta.upstream_verdict), so 'output: EXECUTABLE' printed on a
 *      NOT_ACTIONABLE feed (2026-09-03 06:55) and on a board whose only seat
 *      was a FAST-TRACK ticket with sizing suspended (13:09). Now: the render
 *      stashes the parsed verdict on the payload (payload._dt10_uv); an
 *      EXECUTABLE output needs >= 1 ticket that is neither a grace ghost nor
 *      fast-track-suspended AND carries funded shares; a board of holds
 *      reads 'HELD'. The SELECTED title names suspended seats separately
 *      ('... + 1 FAST-TRACK (SIZING SUSPENDED) ...'); dt10SeatCheckNote_ /
 *      dt10SeatTruthKpi_ no longer count them as exec; expected-gain on zero
 *      executable tickets appends 'gain kpi N SAR vs board 0 exec'. Backend
 *      numbers are still displayed, never rewritten (S-5/S-6 doctrine).
 *      validate_dashboard's banner finder (FEED + ACTIONABLE) and the
 *      withhold surface are byte-identical. ES5. Functions added: 2
 *      (dt10TicketClasses_, tfbMorningCockpitRefresh); removed: 0. */
/* v1.11.0 (2026-09-01) — THREE SEAMS FROM THE 10-DAY PROGRAM (Day 5/7)
 *  (1) EMPTY_BOARD IS AN EVENT, NOT A SILENCE. dt10AppendSelectionLog_
 *      returned 'SelLog: empty board (not logged)' — the most important
 *      transition (the board reaching zero) had to be inferred from other
 *      tabs (audit finding 2026-08-31 §5). Now one EMPTY_BOARD row per day
 *      (Symbol='EMPTY_BOARD', Advisor Note = reason snapshot: feed state,
 *      scanned/passed, top blocking gates) is appended under the same
 *      once-per-day signature mechanism. Mode 'off' still silences all.
 *  (2) route_status vs output_status. The status line said 'status: ok' on
 *      a withheld/empty board ("Request completed" and "decision-ready" were
 *      conflated). dt10StatusLine_ gains ' | output: <EXECUTABLE|WITHHELD|
 *      EMPTY|QUALIFIED_UNFUNDED>' derived from the payload.
 *  (3) HOLDINGS PAYLOAD carries market, buy_date and tp1_sar (when a
 *      TP1 / Take Profit / Target Price column exists on My_Portfolio) so
 *      the builder's rotation rule (v1.18.x) can honour "held >= 5 days" and
 *      "not within 3% of TP1". Missing columns -> keys omitted (fail-open).
 *  (4) KPI strip appends 'Fundable Now', 'By Rotation', 'Capital Call (SAR)'
 *      from the builder's v1.18.0 additive kpis (blank when absent).
 *  Nothing else moves. Kill-switch DT10_EMPTY_BOARD_LOG=0 restores (1). */
/* v1.10.3 (2026-08-28) - HONOR EXPLICIT UTC OFFSETS IN THE FEED TIMESTAMP.
 * FORENSIC WHY: v1.10.2's dual-clock min() is provably wrong for any naive
 * verdict older than 180m (measured 173m shown vs 353m true on 2026-08-27;
 * effective staleness window 660m vs declared 480m). The producer
 * (run_dashboard_sync v6.46.0, live since run 33121085275) now writes
 * '2026-08-28 02:07:12+03:00'. This parser: (1) captures an optional
 * [+-]HH:MM suffix in the same regex; (2) when present, computes ONE exact
 * age from it - no guessing; (3) when absent (legacy rows), keeps v1.10.2's
 * dual-clock behavior BYTE-IDENTICAL. Functions added: 0. Removed: 0. */
// v1.8.8 master toggle — DEFAULT false => v1.8.7 rendering byte-for-byte.
// true arms BOTH v1.8.8 display-truth changes (G-a seat-truth KPI cell,
// G-b structural Why-Not-Selected). Operator-armed, deliberately, like an
// ENV kill-switch (S-1 window law).
var DT10_V188_SEAT_TRUTH = true;   // v1.11.12 [P-149]: ON (was false since v1.8.8)
/** v1.11.12 [P-149]: seat-truth rendering (G-a KPI cell + G-b structural
 * reason) -- ON unless Script Property DT10_SEAT_TRUTH_LEGACY = '1'.
 * Never throws. */
function dt10SeatTruthOn_() {
  if (!DT10_V188_SEAT_TRUTH) return false;
  return dt10PropMemoGet_('DT10_SEAT_TRUTH_LEGACY', function () {   // v1.11.13 [P-202]
    try {
      return String(PropertiesService.getScriptProperties()
          .getProperty('DT10_SEAT_TRUTH_LEGACY') || '') !== '1';
    } catch (eSt) {}
    return true;
  });
}
// v1.8.10 [IR-094 FAST-TRACK SIZING SUSPENSION] — **true** by default
// (protective class: ships ON, unlike display toggles). true => a
// FAST-TRACK (day 1) fill under STRICT hard-exit renders like a grace
// ghost: identity/ROI real, sizing suspended ('—') + honest note,
// until the name confirms. false => v1.8.9 behaviour byte-for-byte
// (fast-track copies full builder sizing on day 1 — the 2026-08-17
// board's 151/151 day-1 executable tickets, incl. two written by the
// 00:42 off-cadence run). Suspension keys on dt10HardVerdictStrict_():
// with strict disarmed the v1.3.0 grace doctrine (and day-1 sizing)
// stands untouched.
var DT10_V1810_FASTTRACK_SIZING_SUSPEND = true;
/* v1.11.2 [P-61]: remember that a seat was fast-track filled until it has
 * `confirm_days` qualifying days; false => v1.11.1 labels/sizing verbatim
 * (the `ft` key is still carried in the state blob so an off/on cycle
 * keeps its memory). */
var DT10_V1112_FASTTRACK_PERSIST = true;
/* v1.11.2 [P-62]: the SelLog signature also keys on the OUTPUT state
 * (HELD / WITHHELD / EXECUTABLE ...). false => v1.11.1 signature. */
var DT10_V1112_SELLOG_OUTPUT_SIG = true;
/* v1.11.2 [P-67]: the time-driven morning refresh writes one _Run_Log
 * row. false => Logger.log only (v1.11.1). */
var DT10_V1112_MORNING_RUNLOG = true;
/* v1.11.3 [P-76]: buildDecisionTop10Layout keeps non-blank operator inputs
 * across the rebuild. false => v1.11.2 (defaults overwrite everything). */
var DT10_V1113_LAYOUT_PRESERVE = true;
/* v1.11.3 [P-77]: a fast-track sizing suspension also blanks Funds From.
 * false => v1.11.2 (Funds From leaks the plan on a suspended seat). */
var DT10_V1113_SUSPEND_FUNDS = true;
/* v1.11.4 [P-78]: holdings reach the Portfolio gate in SAR (native value x
 * TFB_FX_LOOKUP). false => v1.11.3 payload (native value sent as value_sar). */
var DT10_V1114_HOLDINGS_FX = true;
var DT10_SHEET = 'Top_10_Investments';
var DT10_ENDPOINT = '/sheet-rows/opportunity-candidates';
var DT10_DEFAULT_BACKEND = 'https://tadawul-fast-bridge.onrender.com';
var DT10_POOL_PAGES = ['Market_Leaders', 'Global_Markets',
                       'Commodities_FX', 'Mutual_Funds'];
var DT10_HEADER_SCAN_ROWS = 12;   // header row search depth on source pages
// v1.2.3: raised 2000 -> 50000. The body_rows path is NOT truncated by the
// route's pool_limit, and opportunity_builder v1.0.10 caps only the WRITTEN
// audit grid, so this GAS cap was the last real ceiling on scan depth. 50000
// is far above any universe at this scale (~3,905 deduped today) and adapts as
// symbols are added — effectively "scan all". A blank / 0 Pool Limit cell maps
// to this value (see dt10CollectPoolRows_), so there is no number to maintain.
var DT10_POOL_HARD_CAP = 50000;
// v1.3.0 — SELECTION STABILITY LAYER constants. State blob lives in
// ScriptProperties under DT10_STAB_PROP; shape is IDENTICAL to
// core/analysis/top10_selector.py v4.21.0 stability_state (v/date/symbols→
// {ci,co,member,since,ls,hist}) so a backend consolidation adopts it as-is.
var DT10_STAB_PROP = 'DT10_STAB_STATE_V1';
var DT10_STAB_DEF_CONFIRM = 3;    // consecutive qualifying days before ENTRY
var DT10_STAB_DEF_EXIT = 3;       // consecutive missed days before soft EXIT
var DT10_STAB_DEF_BUFFER = 15;    // audit-grid rank <= buffer pauses exit clock
var DT10_STAB_DEF_SMOOTH = 5;     // score-history window (days)
var DT10_STAB_TREND_EPS = 2.0;    // |first-last| below this = 'steady'
var DT10_STAB_PRUNE_DAYS = 14;    // drop non-member entries unseen this long
// v1.14.0 [CONFIRM-BASIS]: which days count toward entry confirmation --
// see the v1.14.0 header block. 'qualified' (default) | 'raw' (v1.13.11).
var DT10_T10_CONFIRM_BASIS_PROP = 'TFB_T10_CONFIRM_BASIS';
// v1.15.0 [PENDING-GRACE]: consecutive soft-miss day-advances a NON-member
// challenger with ci > 0 may absorb without its count resetting -- see the
// v1.15.0 header block. Absent/0/junk = v1.14.0 clocks; capped at 5.
var DT10_T10_PENDING_GRACE_PROP = 'TFB_T10_PENDING_GRACE_DAYS';
var DT10_T10_PENDING_GRACE_MAX = 5;
// v1.19.0 [QUALIFIED-HOLD]: a confirmed member absent from the sized list
// that the audited grid still rates INVEST inside the rank buffer -- see the
// v1.19.0 header block. off (default) | observe | enforce.
var DT10_QUALIFIED_HOLD_PROP = 'DT10_QUALIFIED_HOLD';
// v1.19.0 [SEAT-LIVE-FILL]: live price / Rel / DQ / FX on grace, fast-track
// and held seats from today's pool or grid row. off (default) | on.
var DT10_SEAT_LIVE_FILL_PROP = 'DT10_SEAT_LIVE_FILL';
// v1.19.0 [OWNER-RULE]: the owner's interim buy rule shown per seat
// (Confidence High AND Engine ROI 12M < DT10_OWNER_RULE_ROI_MAX). Display
// only; off (default) | on. The column rides after the seat block.
var DT10_OWNER_RULE_PROP = 'DT10_OWNER_RULE_MARK';
var DT10_OWNER_RULE_ROI_MAX = 34;
var DT10_OWNER_RULE_HEADERS = ['Owner Rule'];
// v1.11.10 [P-168]: outage pause -- see the v1.11.10 header block.
var DT10_P168_MAX_PAUSE_DAYS = 5;         // consecutive paused day-advances
var DT10_P168_OUTAGE_RE = /fetch_failed/i; // engine tag family on 'Warnings'
// v1.11.11 [P-144]: the per-page feed key whose stamp dates the epoch.
var DT10_P144_FEED_KEY = 'TFB Feed Global_Markets';
var DT10_P144_EPOCH_STATES = /^(OK|PARTIAL|PARTIAL_FRESH|STALE_COV)$/;
// v1.11.12 [P-195]: run marker (Script Property) -- see the v1.11.12 block.
var DT10_P195_MARKER_PROP = 'DT10_RUN_MARKER';
var DT10_P195_LIMIT_MIN = 7;          // GAS 6-minute limit + skew
// v1.11.12 [SYNC-INFLIGHT]: first/last legs of a daily_sync run and the bound.
var DT10_INFLIGHT_PROP = 'DT10_SYNC_INFLIGHT';   // off | observe (default) | enforce
var DT10_INFLIGHT_FIRST_KEY = 'TFB Feed Market_Leaders';
var DT10_INFLIGHT_LAST_KEY = 'TFB Feed Global_Markets';
var DT10_INFLIGHT_MAX_MIN = 90;

// v1.13.4 [WP-8]: COCKPIT INPUT TRUTH -- three Script Property gates, each
// off (default) | observe | enforce, read once per execution (dt10ModeProp_).
var DT10_IT_PROP = 'DT10_INPUT_TRUTH';          // off (default) | observe | enforce
var DT10_IT_POLICY_PROP = 'DT10_POLICY_REF';    // optional JSON {label: number}
var DT10_IT_UNKNOWN_MAX_PCT = 20;               // DQ-48 pass rule: Unknown <= 20 %
var DT10_IT_REF_LABELS = ['T10: Max Per Sector', 'PF: Max Sector %',
                          'PF: Max Position %'];
// Dated operator DECISION (2026-09-10: '3 names / 40 %', quoted in the
// v1.11.12 WHY block) -- a declared reference, NOT an adopted policy.
var DT10_IT_DECISION_REF = { 'T10: Max Per Sector': 3, 'PF: Max Sector %': 40 };
var DT10_CANDTAB_PROP = 'DT10_CAND_TAB';        // off (default) | observe | enforce
var DT10_CANDTAB_SHEET = '_T10_Candidates';
var DT10_CANDTAB_STAGE_SHEET = '_T10_Candidates_Staging';
var DT10_CANDTAB_COCKPIT_TARGET_ROWS = 80;     // audit acceptance: cockpit <= 80 rows
var DT10_VINTAGE_PROP = 'DT10_VINTAGE_COLS';    // off (default) | observe | enforce
var DT10_VINTAGE_HEADERS = ['Quote As-Of (UTC)', 'Target Vintage'];
var DT10_VINT_ASOF_RE = /(?:^|[;\s|,])acquisition_quote_asof:([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.+\-Z]+)/i;
var DT10_VINT_LKG_RE = /(?:^|[;\s|,])analyst_lkg:(\d+)h/i;
var DT10_VINT_PUBLISHED_RE = /(?:^|[;\s|,])analyst_target_(?:published_at|asof):([0-9]{4}-[0-9]{2}-[0-9]{2}(?:T[0-9:.+\-Z]+)?)/i;
var DT10_STAB_PROP_SOFT_MAX = 8500; // chars; 9KB ScriptProperties value limit
// Safety-class verdicts hard-exit immediately (never grace-held). Plain
// DO_NOT_INVEST criteria-misses stay grace-holdable — that IS the jitter
// this layer absorbs.
var DT10_STAB_HARD_VERDICT_RE = /SELL|AVOID|EXIT/i;
// v1.3.1 — SELECTION AUDIT LOG. The cockpit appends its rendered board to
// _Selection_Log (schema below = the sheet's existing 31 headers verbatim +
// 2 appended). Mode property: 'off' | 'always' | unset (daily + on
// membership change). State blob {date, sig} dedupes the default cadence.
var DT10_SELLOG_SHEET = '_Selection_Log';
var DT10_SELLOG_STATE_PROP = 'DT10_SELLOG_STATE_V1';
var DT10_SELLOG_MODE_PROP = 'DT10_SELECTION_LOG';
var DT10_SELLOG_HEADERS = ['Logged At', 'Run Info', 'Source Page', 'Rank',
  'Symbol', 'Name', 'Market', 'Sector', 'Ccy', 'FX\u2192SAR', 'Price',
  'Price SAR', 'Entry Zone', 'Ticket SAR', 'Shares', 'Stop SAR', 'TP1 SAR',
  'TP2 SAR', 'ROI %', 'Engine ROI %', 'Ann ROI %', 'Gain 12M SAR', 'Rel',
  'DQ', 'Conf', 'Funds From', 'Review By', 'Advisor Note', 'Panel Snapshot',
  'Outcome', 'Review Notes',
  // v1.3.1 appended (pre-existing rows keep blanks here):
  'Stability', 'Days'];
// Fixed layout anchors
var DT10_ROW_TITLE = 1;
var DT10_ROW_STATUS = 2;
var DT10_ROW_PANEL_HEAD = 4;
var DT10_ROW_PANEL_FIRST = 5;
var DT10_PANEL_ROWS = 9;          // v1.3.0: 27 inputs / 3 per row (rows 5–13)
var DT10_ROW_KPI_HEAD = 14;       // v1.3.0: +1 (panel grew to rows 5–13)
var DT10_ROW_KPI_LABELS = 15;     // v1.3.0: +1
var DT10_ROW_KPI_VALUES = 16;     // v1.3.0: +1
var DT10_ZONES_START = 18;        // v1.3.0: +1
var DT10_LAST_COL = 30;           // widest table (SELECTED, +v1.3.0 Stability block)
// §3.4 token fallbacks — RESKINNED to the approved proposal palette (v1.1.0).
// dt10Tokens_ overlays TFB_DESIGN_TOKENS (COLOR_* keys) onto this map, but the
// renderers read these VERDICT_*/HEADER/OPERATOR_INPUT keys, which the named
// range never publishes — so editing the map below reskins the cockpit live.
var DT10_FALLBACK_TOKENS = {
  VERDICT_POSITIVE: { fg: '#146C43', bg: '#E6F4EA' },
  VERDICT_WATCH:    { fg: '#9A6700', bg: '#FFF3CD' },
  VERDICT_NEGATIVE: { fg: '#B3261E', bg: '#FBE4E2' },
  VERDICT_BLOCK:    { fg: '#5B6470', bg: '#ECEEF1' },
  HEADER:           { fg: '#FFFFFF', bg: '#1F3A5F' },
  OPERATOR_INPUT:   { fg: '#1A57C2', bg: '#E8F0FE' }
};
// v1.1.0 proposal-palette constants (presentation only; gold is a reserved
// accent used for the title bottom-rule, never mapped to a data column).
var DT10_NAVY = '#1F3A5F';        // brand banners + column-header rows
var DT10_NAVY_SUB = '#2C4A70';    // sub-banner (reserved)
var DT10_TINT = '#EEF2F7';        // KPI tiles + section context strips
var DT10_ACCENT_GOLD = '#C8A04B'; // title bottom-rule accent
var DT10_GOLD_TEXT = '#4A3A12';   // reserved
var DT10_GOLD_FILL = '#F3F1EC';   // reserved
var DT10_INK = '#1A2433';         // body text
var DT10_BORDER = '#D6DEE8';      // table gridlines (was #E0E0E0)
var DT10_ZEBRA = '#F5F7FA';       // alternate data rows (was #FAFAFA)
var DT10_MUTED = '#6B7280';       // status / footer / labels (was #5F6368)
var DT10_FMT_SAR = '#,##0 "SAR"';
var DT10_FMT_PRICE = '#,##0.00';
var DT10_FMT_PCT = '0.0"%"';
var DT10_FMT_NUM2 = '0.00';
var DT10_FMT_INT = '#,##0';
/**
 * Panel definition: label shown on sheet (and sent verbatim as criteria key
 * for "T10: *" entries — the route's _OPP_CRITERIA_ALIASES strips the prefix
 * and maps to builder keys), built-in default, and kind for cell formatting.
 * kind: 'int' | 'num' | 'text' | 'yesno' | 'sar'
 * The original 17 "T10: *" labels are §4.1-verbatim (TFB_PANEL_DEFAULTS);
 * v1.2.7 appends an 18th (seed optional — built-in default covers it).
 */
var DT10_PANEL = [
  { label: 'T10: Universe Scope', def: 'All Main Sheets', kind: 'text' },
  { label: 'T10: Max Selected', def: 10, kind: 'int' },   // v1.2.6: 3 -> 10
  { label: 'T10: Period (Months)', def: 12, kind: 'int' },
  { label: 'T10: Required ROI %', def: 12, kind: 'num' },
  { label: 'T10: Required Annualized ROI %', def: 10, kind: 'num' },
  { label: 'T10: Risk Profile', def: 'Moderate', kind: 'text' },
  { label: 'T10: Min Reliability', def: 70, kind: 'num' },
  { label: 'T10: Min Data Quality', def: 80, kind: 'num' },
  { label: 'T10: Min Risk/Reward', def: 2, kind: 'num' },
  { label: 'T10: Max Risk Level', def: 'Medium', kind: 'text' },
  { label: 'T10: Allow Conflict', def: 'No', kind: 'yesno' },
  { label: 'T10: Allow Negative News', def: 'No', kind: 'yesno' },
  { label: 'T10: Allow Negative Sector', def: 'No', kind: 'yesno' },
  { label: 'T10: Max Per Sector', def: 3, kind: 'int' },   // v1.11.12: 2 -> 3 (2026-09-10 decision)
  { label: 'T10: Max Per Market', def: 10, kind: 'int' },  // v1.2.6: 4 -> 10
  { label: 'T10: Include Portfolio Holdings', def: 'No', kind: 'yesno' },
  { label: 'T10: Base Currency', def: 'SAR', kind: 'text' },
  { label: 'Cash Available (SAR)', def: 100000, kind: 'sar' },
  { label: 'Pending Proceeds (SAR)', def: 0, kind: 'sar' },
  { label: 'Pool Source', def: 'Sheets', kind: 'text' },
  // v1.2.3: default raised 1000 -> 50000 and a BLANK / 0 cell now means "scan
  // the entire available universe" (bounded only by DT10_POOL_HARD_CAP). Clear
  // this cell to scan everything, or type a positive number to cap on purpose.
  { label: 'Pool Limit', def: 50000, kind: 'int' },
  // v1.2.7: sheet-side switch for the backend's Investability MAJOR gate
  // (opportunity_builder v1.0.7+, criteria key investability_gate_enabled).
  // APPENDED so every existing panel cell keeps its position.
  // v1.6.9: default flipped 'Yes' -> 'No'. The backend retired this
  // all-or-nothing gate on 2026-07-24 (opportunity_builder v1.7.0: "benched
  // 9 legitimate names for every 1 it was right about") in favour of the
  // always-on Sell-Class gate; the old 'Yes' seed re-armed the retired gate
  // on every rebuild and made it the top funnel blocker (230/300 on the
  // 2026-08-05 board). A blank cell now reads 'No'; typing Yes remains an
  // explicit operator opt-in, captured in the logged panel snapshot.
  { label: 'T10: Require Investable', def: 'No', kind: 'yesno' },
  // v1.3.0: SELECTION-STABILITY knobs — APPENDED (items 23-27) so every
  // existing panel cell keeps its position. New cells read blank (=> built-in
  // defaults below) until the layout is rebuilt; the v1.3.0 layout guard in
  // refreshDecisionTop10 rebuilds a v1.2.x sheet automatically on first run.
  { label: 'T10: Stability Enabled', def: 'Yes', kind: 'yesno' },
  { label: 'T10: Stability Confirm Days', def: DT10_STAB_DEF_CONFIRM, kind: 'int' },
  { label: 'T10: Stability Exit Days', def: DT10_STAB_DEF_EXIT, kind: 'int' },
  { label: 'T10: Stability Rank Buffer', def: DT10_STAB_DEF_BUFFER, kind: 'int' },
  { label: 'T10: Stability Smooth Days', def: DT10_STAB_DEF_SMOOTH, kind: 'int' }
];
/**
 * Pool field specs (§5 amendment): `send` is the key POSTed per row —
 * chosen to hit opportunity_builder v1.0.1 _FIELD_ALIASES after its
 * lowercase/strip-non-alnum normalization (verified against source).
 * `match` lists normalized source-page header tokens accepted for the field.
 * Analyst target intentionally excludes engine "Forecast Price 12M" (L5:
 * engine forecast ROI travels separately via Expected ROI 12M; mapping the
 * engine forecast price into target_price would double-count it as the
 * valuation basis).
 */
var DT10_POOL_FIELDS = [
  { send: 'Symbol', match: ['symbol', 'ticker'] },
  { send: 'Name', match: ['name', 'companyname', 'instrumentname'] },
  { send: 'Sector', match: ['sector', 'gicssector'] },
  { send: 'Exchange', match: ['exchange', 'market', 'marketregion'] },
  { send: 'Currency', match: ['currency', 'tradingcurrency', 'currencycode'] },
  { send: 'Current Price', match: ['currentprice', 'price', 'lastprice'] },
  // TFB-01: a retrieval clock alone never proves an executable quote.
  // Send only actual source evidence; backend row_acquisition owns validation.
  { send: 'Data Provider',
    match: ['dataprovider', 'provider', 'datasource', 'source', 'primaryprovider'] },
  { send: 'acquisition_status', match: ['acquisitionstatus'] },
  { send: 'acquisition_provider', match: ['acquisitionprovider'] },
  { send: 'acquisition_acquired_at', match: ['acquisitionacquiredat'] },
  { send: 'acquisition_quote_asof', match: ['acquisitionquoteasof'] },
  { send: 'Instrument ID', match: ['instrumentid'] },
  { send: 'Provider Symbol', match: ['providersymbol'] },
  { send: 'Price Basis', match: ['pricebasis'] },
  { send: 'Quote Currency', match: ['quotecurrency'] },
  { send: 'Quote Session', match: ['quotesession'] },
  { send: 'Target Price',
    match: ['pricetarget', 'targetprice', 'analysttarget'] },
  { send: 'Intrinsic Value',
    match: ['intrinsicvalue', 'fairvalue', 'fairprice'] },
  { send: 'Expected ROI 12M',
    match: ['expectedroi12m', 'expectedroi', 'forecastroi12m'] },
  { send: 'Forecast Reliability Score',
    match: ['forecastreliabilityscore', 'reliabilityscore', 'reliability'] },
  { send: 'Data Quality Score',
    match: ['dataqualityscore', 'dataquality', 'dqscore'] },
  { send: 'Risk Bucket', match: ['riskbucket', 'risklevel', 'riskband'] },
  { send: 'Provider/Engine Conflict',
    match: ['providerengineconflict', 'providerconflict', 'conflictflag',
            'conflict'] },
  { send: 'Volatility 30D', match: ['volatility30d', 'vol30d'] },
  { send: 'Avg Volume 30D',
    match: ['avgvolume30d', 'averagevolume30d', 'avgvol30d'] },
  { send: 'Recommendation Detail',
    match: ['recommendationdetail', 'recommendationdetailed'] },
  { send: 'Investability Status',
    match: ['investabilitystatus', 'investability'] },
  { send: 'Block Reason', match: ['blockreason', 'blockreasons'] },
  // v1.8.10 [TRUST-001 witness]: the source engine's own trust verdict
  // (low_data_trust / rank_skipped_low_trust) lives in Warnings; without
  // it the opportunity_builder v1.13.0 trust-lineage defense reads a
  // permanently-empty witness. Verbatim passthrough; builder alias
  // 'warnings' (v1.13.0).
  { send: 'Warnings', match: ['warnings'] },
  // Retrieval and quote clocks are separate. A generic/Riyadh retrieval
  // stamp must never be relabeled UTC, nor an AsOf treated as acquisition.
  { send: 'Last Updated (UTC)',
    match: ['lastupdatedutc'] },
  { send: 'Last Updated (Riyadh)', match: ['lastupdatedriyadh'] },
  { send: 'Last Updated', match: ['lastupdated'] },
  // v1.8.1: forecast PROVENANCE must travel with the row — opportunity_builder
  // v1.10.0's B4 gate (env TFB_T10_EXCLUDE_DEFAULT_CONF, armed 2026-08-08)
  // matches forecast_source == 'phase_ii_synthetic', and the builder's
  // _FIELD_ALIASES has carried the 'Forecast Source' -> forecast_source
  // mapping since v1.9.1 — but this contract never sent the field, so on
  // the body_rows path the armed gate read blank on every candidate and
  // passed all of them (the 2026-08-08 19:13 board's zero-synthetic result
  // was the other gates plus ranking luck, verified by TSV join, not B4).
  // Exactly the v1.6.1 timestamp defect class: a gate starved of its input
  // by the pool contract. EXPECT on the first refresh after paste: the
  // Passed KPI SHRINKS as sheet-synthetic candidates (42.6% of
  // Global_Markets on the 2026-08-08 export) are finally excluded — that
  // is the gate seeing, not a regression.
  { send: 'Forecast Source',
    match: ['forecastsource'] },
  // v1.11.12 [P-181b]: the 52W timing gate's inputs (opportunity_builder
  // v1.23.0 TFB_T10_W52_TIMING, aliases w52_high / w52_low /
  // w52_position_pct / pct_change_1d). Observe read-back 2026-10-02..04:
  // 500/500 rows '[w52-observe] n/a (no 52W / change fields)' because this
  // contract never sent them. Kill: DT10_P181B_W52_LEGACY = '1'.
  { send: '52W High', match: ['52whigh', 'week52high', 'high52w'] },
  { send: '52W Low', match: ['52wlow', 'week52low', 'low52w'] },
  { send: '52W Position %',
    match: ['52wposition', '52wpositionpct', 'week52positionpct'] },
  { send: 'Percent Change',
    match: ['percentchange', 'pctchange', 'changepct'] }
];
/** TFB-01: retain original evidence headers as well as canonical sends.
 * First-column selection must not erase an invalid provider, contradictory
 * alias, duplicate timestamp or alternate instrument/price/currency witness.
 * No values are parsed, reconciled or fabricated at this adapter boundary. */
var DT10_POOL_EVIDENCE_TOKENS = {
  symbol: true, ticker: true,
  dataprovider: true, provider: true, datasource: true, source: true,
  primaryprovider: true,
  currentprice: true, price: true, lastprice: true,
  currency: true, tradingcurrency: true, currencycode: true, ccy: true,
  exchange: true, market: true, marketregion: true,
  warnings: true, warning: true, rowwarnings: true, flags: true,
  error: true, errors: true, errormessage: true,
  lastupdatedutc: true, lastupdatedriyadh: true, lastupdated: true,
  asof: true, timestamp: true,
  acquisitionstatus: true, acquisitionprovider: true,
  acquisitionacquiredat: true, acquisitionquoteasof: true,
  instrumentid: true, providersymbol: true, isin: true,
  pricebasis: true, quotecurrency: true, quotesession: true,
  quoteasof: true, quotetimestamp: true, pricebarts: true, regularmarkettime: true
};
/** v1.11.12 [P-181b]: the four sends the kill switch removes. */
var DT10_P181B_SENDS = { '52W High': true, '52W Low': true,
                         '52W Position %': true, 'Percent Change': true };
/** v1.17.1 [SHADOW-MOMENTUM]: the ranking challenger's inputs that ride on
 * the pool row ONLY while the shadow rank log is armed (DT10_SHADOW_RANK_LOG
 * on/all; dt10PoolFieldsWithShadow_). opportunity_builder v1.28.0 reads
 * momentum_score (alias 'momentumscore') only under TFB_OPP_RANK_MODE
 * shadow/enforce, for the challenger's second key; the live order never
 * reads it. */
var DT10_SHADOW_POOL_FIELDS = [
  { send: 'Momentum Score', match: ['momentumscore'] }
];
/** v1.11.13 [P-202]: per-execution memo for Script Property reads that sit
 * on per-row paths. Apps Script gives each execution a fresh global scope,
 * so this object lives exactly one run; refreshDecisionTop10 also resets it
 * at entry. reader() runs once per key per execution. Never throws beyond
 * what reader() throws. */
var DT10_PROP_MEMO_ = {};
function dt10PropMemoGet_(key, reader) {
  if (!Object.prototype.hasOwnProperty.call(DT10_PROP_MEMO_, key)) {
    DT10_PROP_MEMO_[key] = reader();
  }
  return DT10_PROP_MEMO_[key];
}
function dt10PropMemoReset_() {
  DT10_PROP_MEMO_ = {};
}
/** v1.11.12 [P-181b]: Script Property DT10_P181B_W52_LEGACY = '1' restores
 * the v1.11.11 projection. Absent/junk/no service (node) => fields ON.
 * Never throws. v1.11.13 [P-202]: read ONCE per execution (was once per
 * pool row -- ~9,800 service calls per run, the 6-minute kill). */
function dt10W52FieldsLegacy_() {
  return dt10PropMemoGet_('DT10_P181B_W52_LEGACY', function () {
    try {
      return String(PropertiesService.getScriptProperties()
          .getProperty('DT10_P181B_W52_LEGACY') || '') === '1';
    } catch (eW52) {}
    return false;
  });
}
/** v1.11.12 [P-181b]: the projection in force this run (PURE given the
 * legacy flag; the flag read is the only service touch). */
function dt10PoolFieldsActive_(legacy) {
  if (legacy === undefined) {   // v1.11.13 [P-202]: ONE list per execution
    return dt10PropMemoGet_('pool_fields_active', function () {
      // v1.17.1 [SHADOW-MOMENTUM]: + the challenger's inputs while the shadow
      // rank log is armed; off -> the v1.17.0 list (the same object). The
      // mode is read once and kept for this execution: the log written after
      // the POST (dt10AppendShadowRankLog_) uses the same value.
      var shadowMode = dt10PropMemoGet_('shadow_log_mode', dt10ShadowLogMode_);
      return dt10PoolFieldsWithShadow_(
          dt10PoolFieldsActive_(dt10W52FieldsLegacy_()),
          shadowMode !== 'off');
    });
  }
  var leg = !!legacy;
  if (!leg) return DT10_POOL_FIELDS;
  var out = [];
  for (var i = 0; i < DT10_POOL_FIELDS.length; i++) {
    if (!DT10_P181B_SENDS[DT10_POOL_FIELDS[i].send]) out.push(DT10_POOL_FIELDS[i]);
  }
  return out;
}
// ---------------------------------------------------------------------------
// Pure helpers (node-testable: no Apps Script services touched)
// ---------------------------------------------------------------------------
/** Lowercase, strip non-alphanumerics — mirrors builder _norm_token. */
function dt10NormToken_(s) {
  return String(s === null || s === undefined ? '' : s)
      .toLowerCase().replace(/[^a-z0-9]/g, '');
}
/** Find header row index (0-based) in a 2-D values grid; -1 if absent. */
function dt10FindHeaderRow_(values, scanRows) {
  var limit = Math.min(values.length, scanRows || DT10_HEADER_SCAN_ROWS);
  for (var r = 0; r < limit; r++) {
    var hasSymbol = false, hasSecond = false;
    for (var c = 0; c < values[r].length; c++) {
      var t = dt10NormToken_(values[r][c]);
      if (t === 'symbol' || t === 'ticker') hasSymbol = true;
      if (t === 'currentprice' || t === 'name' || t === 'companyname') {
        hasSecond = true;
      }
    }
    if (hasSymbol && hasSecond) return r;
  }
  return -1;
}
/** v1.17.1 [SHADOW-MOMENTUM] PURE: `fields` plus DT10_SHADOW_POOL_FIELDS
 * when `armed`; `fields` itself (the same array) when not, so an unarmed
 * projection is the v1.17.0 list object. A send already present is not
 * added again. Never mutates `fields`. */
function dt10PoolFieldsWithShadow_(fields, armed) {
  if (!armed) return fields;
  var out = fields.slice();
  var have = {};
  for (var i = 0; i < out.length; i++) have[out[i].send] = true;
  for (var j = 0; j < DT10_SHADOW_POOL_FIELDS.length; j++) {
    if (!have[DT10_SHADOW_POOL_FIELDS[j].send]) {
      out.push(DT10_SHADOW_POOL_FIELDS[j]);
      have[DT10_SHADOW_POOL_FIELDS[j].send] = true;
    }
  }
  return out;
}
/** Map DT10_POOL_FIELDS → column index from a header row array; null=absent */
function dt10MapHeaderCols_(headerRow) {
  var byToken = {};
  var aliases = {};
  var evidenceCols = [];
  for (var c = 0; c < headerRow.length; c++) {
    var t = dt10NormToken_(headerRow[c]);
    if (t && byToken[t] === undefined) byToken[t] = c;
    if (t) {
      if (!aliases[t]) aliases[t] = [];
      aliases[t].push(c);
      if (DT10_POOL_EVIDENCE_TOKENS[t]) {
        evidenceCols.push({ key: String(headerRow[c]).trim(), col: c });
      }
    }
  }
  var map = { _dt10_alias_cols: {}, _dt10_evidence_cols: evidenceCols };
  var fields = dt10PoolFieldsActive_();   // v1.11.12 [P-181b]
  for (var i = 0; i < fields.length; i++) {
    var spec = fields[i];
    var col = null;
    var cols = [];
    for (var m = 0; m < spec.match.length; m++) {
      if (byToken[spec.match[m]] !== undefined) {
        if (col === null) col = byToken[spec.match[m]];
        cols = cols.concat(aliases[spec.match[m]]);
      }
    }
    map[spec.send] = col;
    map._dt10_alias_cols[spec.send] = cols;
  }
  return map;
}
/** First meaningful source value for a canonical field, without discarding
 * the other evidence values (dt10PoolRowFromSheetRow_ forwards those below). */
function dt10PoolValue_(row, colMap, send) {
  var cols = colMap._dt10_alias_cols && colMap._dt10_alias_cols[send];
  if (!cols) cols = [colMap[send]];
  for (var i = 0; i < cols.length; i++) {
    var col = cols[i];
    if (col !== null && col !== undefined && dt10HasValue_(row[col])) return row[col];
  }
  return undefined;
}
/** True if a cell value is a meaningful (non-placeholder) payload value. */
function dt10HasValue_(v) {
  if (v === null || v === undefined) return false;
  if (typeof v === 'number') return isFinite(v);
  if (typeof v === 'boolean') return true;
  var s = String(v).trim();
  if (!s) return false;
  var low = s.toLowerCase();
  return !(low === '-' || low === '—' || low === 'n/a' || low === 'na' ||
           low === 'none' || low === 'null' || low === 'nan' ||
           low === '#n/a' || low === 'no data');
}
/** Build one pool row object from a sheet data row; null if not sendable. */
function dt10PoolRowFromSheetRow_(row, colMap, pageName) {
  var symCol = colMap['Symbol'];
  if (symCol === null || symCol === undefined) return null;
  var sym = dt10PoolValue_(row, colMap, 'Symbol');
  if (!dt10HasValue_(sym)) return null;
  var s = String(sym).trim();
  if (s.charAt(0) === '—' || s.toLowerCase().indexOf('no data') === 0) {
    return null;
  }
  // v1.2.2: skip ghost/header rows whose Symbol cell is a literal header token.
  var su = s.toUpperCase();
  if (su === 'SYMBOL' || su === 'TICKER') return null;
  var out = {};
  var fields = dt10PoolFieldsActive_();   // v1.11.12 [P-181b]
  for (var i = 0; i < fields.length; i++) {
    var send = fields[i].send;
    var col = colMap[send];
    if (col === null || col === undefined) continue;
    var v = dt10PoolValue_(row, colMap, send);
    if (dt10HasValue_(v)) out[send] = v;
  }
  var evidenceCols = colMap._dt10_evidence_cols || [];
  for (var e = 0; e < evidenceCols.length; e++) {
    var witness = evidenceCols[e], value = row[witness.col];
    // Unlike display placeholders, explicit nonlive/unknown evidence such as
    // provider="none" must survive so a second valid alias cannot hide it.
    if (value === null || value === undefined || String(value).trim() === '') continue;
    var key = witness.key;
    if (Object.prototype.hasOwnProperty.call(out, key)) {
      if (String(out[key]) === String(value)) continue;
      // JSON objects cannot hold identical headers twice. Punctuation-only
      // suffixes preserve both values under the SAME backend normalized key.
      while (Object.prototype.hasOwnProperty.call(out, key)) key += '_';
    }
    out[key] = value;
  }
  // Sheet membership is context, not an observed exchange. Preserve even an
  // explicit unknown Exchange value; source_page is a separate builder alias.
  if (!dt10HasValue_(out['Exchange'])) out['Source Page'] = pageName;
  return out;
}
/** v1.2.2 — normalize a symbol for de-duplication (trim + uppercase). */
function dt10NormSym_(v) {
  return String(v === null || v === undefined ? '' : v).trim().toUpperCase();
}
/** v1.13.10 PURE: SAR per unit of a holding's native currency cell, mirroring
 * opportunity_builder._resolve_fx with TFB_OPP_FX_USX_SUBUNIT off (default).
 * Subunits are detected on the RAW token BEFORE case folding ('GBp' would
 * otherwise fold into pounds): GBX/GBp, ZAC/ZAc, ILA/ILa (and gbx/zac/ila
 * spellings) are 1/100 of GBP/ZAR/ILS. An exact raw key in the map wins
 * (already per subunit); otherwise the parent rate / 100. USX is not folded
 * here (its gate is backend-only and DEFAULT OFF): it needs its own 'USX'
 * row, as before. Returns null for SAR/blank (no conversion), NaN when no
 * rate exists, else the positive rate. */
var DT10_FX_SUBUNIT_PARENT = { GBp: 'GBP', GBX: 'GBP', ZAC: 'ZAR', ZAc: 'ZAR',
                               ILA: 'ILS', ILa: 'ILS' };
function dt10HoldingFxRate_(cell, fx) {
  var raw = String(cell === null || cell === undefined ? '' : cell).trim();
  var up = raw.toUpperCase();
  if (!raw || up === 'SAR') return null;
  fx = fx || {};
  var exact = Number(fx[raw]);
  if (isFinite(exact) && exact > 0) return exact;
  var parent = Object.prototype.hasOwnProperty.call(DT10_FX_SUBUNIT_PARENT, raw) ?
      DT10_FX_SUBUNIT_PARENT[raw] :
      (up === 'GBX' || up === 'ZAC' || up === 'ILA') ? DT10_FX_SUBUNIT_PARENT[up] : '';
  var rate = Number(fx[parent || up]);
  if (!(isFinite(rate) && rate > 0)) return NaN;
  return parent ? rate / 100 : rate;
}
/** v1.5.0 (Fix H1) PURE: one My_Portfolio sheet row -> holdings entry for
 * the builder's Portfolio gate, or null. Exclusion needs symbol only;
 * sector/value are best-effort context for the builder's sector-cap math.
 * Indices may be -1 (column absent). Node-testable (no Apps Script
 * services). */
function dt10HoldingFromRow_(row, iSym, iSec, iVal, extra) {
  if (!row || iSym === null || iSym === undefined || iSym < 0) return null;
  var sym = dt10NormSym_(row[iSym]);
  if (!sym || sym === 'SYMBOL') return null;
  var sector = '';
  if (iSec !== null && iSec !== undefined && iSec >= 0) {
    sector = String(row[iSec] === null || row[iSec] === undefined ?
                    '' : row[iSec]).trim();
  }
  var val = 0;
  if (iVal !== null && iVal !== undefined && iVal >= 0) {
    var n = Number(row[iVal]);
    if (isFinite(n) && n > 0) val = n;
  }
  extra = extra || {};
  // v1.11.4 [P-78]: a NATIVE value converts to SAR when the caller supplies a
  // currency column and an FX map (SAR per unit). SAR rows and rows without a
  // currency keep the native value. v1.13.10: subunits resolve on the RAW
  // token and a currency missing from the map sends value_sar null and
  // marks the row _input_incomplete instead of a native amount labelled SAR.
  if (DT10_V1114_HOLDINGS_FX && val > 0 && extra.fx && extra.iCcy >= 0) {
    var rate = dt10HoldingFxRate_(row[extra.iCcy], extra.fx);
    if (rate === null) {
      // SAR or blank currency: the value is already SAR (unchanged).
    } else if (isFinite(rate) && rate > 0) {
      val = val * rate;
      extra._converted = (extra._converted || 0) + 1;
    } else {
      val = null;
      extra._unconverted = (extra._unconverted || 0) + 1;
    }
  }
  var h = { symbol: sym, sector: sector || 'Unknown', value_sar: val };
  // Reconciliation never invents quantity/currency or a fresh quote timestamp.
  // Preserve minor-unit spelling so the backend can reject ambiguous units.
  if (extra.iCcy >= 0 && dt10HasValue_(row[extra.iCcy])) {
    h.currency = String(row[extra.iCcy]).trim();
  }
  if (extra.iQuantity >= 0 && dt10HasValue_(row[extra.iQuantity]) &&
      typeof row[extra.iQuantity] !== 'boolean') {
    var qty = Number(row[extra.iQuantity]);
    if (isFinite(qty) && qty >= 0) h.quantity = qty;
  }
  if (extra.iPrice >= 0 && dt10HasValue_(row[extra.iPrice]) &&
      typeof row[extra.iPrice] !== 'boolean') {
    var price = Number(row[extra.iPrice]);
    if (isFinite(price) && price > 0) h.current_price = price;
  }
  var proofFields = [['iProvider', 'data_provider'], ['iWarnings', 'warnings'],
                     ['iUpdated', extra.updatedKey || 'last_updated']];
  for (var pf = 0; pf < proofFields.length; pf++) {
    var col = extra[proofFields[pf][0]];
    if (col >= 0 && dt10HasValue_(row[col])) {
      var original = row[col];
      h[proofFields[pf][1]] = original instanceof Date && !isNaN(original.getTime()) ?
          original.toISOString() : String(original).trim();
    }
  }
  // v1.11.0 (3): optional fields for the rotation rule — omitted when absent.
  if (extra.iMarket >= 0) {
    var mk = String(row[extra.iMarket] === null || row[extra.iMarket] === undefined ?
                    '' : row[extra.iMarket]).trim();
    if (mk) h.market = mk;
  }
  if (extra.iBuyDate >= 0) {
    var bd = row[extra.iBuyDate];
    var bdS = '';
    if (bd instanceof Date && !isNaN(bd.getTime())) {
      bdS = Utilities.formatDate(bd, 'Asia/Riyadh', 'yyyy-MM-dd');
    } else if (bd !== null && bd !== undefined) {
      bdS = String(bd).trim().slice(0, 10);
    }
    if (/^\d{4}-\d{2}-\d{2}$/.test(bdS)) h.buy_date = bdS;
  }
  if (extra.iTp1 >= 0) {
    var tp = Number(row[extra.iTp1]);
    if (isFinite(tp) && tp > 0) h.tp1_sar = tp;
  }
  return h;
}
/** v1.5.0 (Fix H1): read the operator's holdings from My_Portfolio for the
 * request body. Header auto-scan (dt10FindHeaderRow_); Symbol mandatory;
 * an explicitly-SAR-labelled value column is preferred over raw Position
 * Value; cap 500 rows; per-symbol dedupe. Missing, duplicate, oversized or
 * unreadable inputs carry _input_incomplete so funding stays withheld. */
function dt10CollectHoldings_(ss) {
  var out = [];
  function unavailable() { out._input_incomplete = true; return out; }
  try {
    var sheet = ss.getSheetByName('My_Portfolio');
    if (!sheet) return unavailable();
    var lastRow = sheet.getLastRow(), lastCol = sheet.getLastColumn();
    var readRows = Math.min(lastRow, DT10_HEADER_SCAN_ROWS + 501);
    var readCols = Math.min(lastCol, 250);
    if (!(readRows > 0) || !(readCols > 0)) return unavailable();
    var values = sheet.getRange(1, 1, readRows, readCols).getValues();
    if (lastRow > readRows || lastCol > readCols) out._input_incomplete = true;
    if (!values || !values.length) return unavailable();
    var hIdx = dt10FindHeaderRow_(values, DT10_HEADER_SCAN_ROWS);
    if (hIdx < 0) return unavailable();
    var hdr = values[hIdx];
    var iSym = -1, iSec = -1, iValSar = -1, iVal = -1;
    var extra = { iMarket: -1, iBuyDate: -1, iTp1: -1,
                  iCcy: -1, iQuantity: -1, iPrice: -1, iProvider: -1,
                  iWarnings: -1, iUpdated: -1, updatedKey: 'last_updated', fx: null };
    var proofSeen = {};
    for (var c = 0; c < hdr.length; c++) {
      var t = dt10NormToken_(hdr[c]);
      var proofKey = t === 'symbol' || t === 'ticker' ? 'symbol' :
          t === 'currency' || t === 'ccy' ? 'currency' :
          t === 'quantity' || t === 'positionqty' || t === 'shares' ? 'quantity' :
          t === 'currentprice' || t === 'price' ? 'price' :
          t === 'lastupdated' || t === 'lastupdatedutc' || t === 'sourcelastupdated' ? 'updated' : '';
      if (proofKey && proofSeen[proofKey]) out._input_incomplete = true;
      if (proofKey) proofSeen[proofKey] = (proofSeen[proofKey] || 0) + 1;
      if (iSym < 0 && (t === 'symbol' || t === 'ticker')) {
        iSym = c;
      } else if (iSec < 0 && t === 'sector') {
        iSec = c;
      } else if (iValSar < 0 && (t === 'positionvaluesar' ||
                                 t === 'marketvaluesar' || t === 'mvsar')) {
        iValSar = c;
      } else if (iVal < 0 && (t === 'positionvalue' ||
                              t === 'marketvalue')) {
        iVal = c;
      } else if (extra.iCcy < 0 && (t === 'currency' || t === 'ccy')) {
        extra.iCcy = c;                                     // v1.11.4 [P-78]
      } else if (extra.iQuantity < 0 && (t === 'quantity' || t === 'positionqty' || t === 'shares')) {
        extra.iQuantity = c;
      } else if (extra.iPrice < 0 && (t === 'currentprice' || t === 'price')) {
        extra.iPrice = c;
      } else if (extra.iProvider < 0 && t === 'dataprovider') {
        extra.iProvider = c;
      } else if (extra.iWarnings < 0 && (t === 'warnings' || t === 'datawarnings')) {
        extra.iWarnings = c;
      } else if (extra.iUpdated < 0 && (t === 'lastupdated' || t === 'lastupdatedutc' || t === 'sourcelastupdated')) {
        extra.iUpdated = c;
        extra.updatedKey = t === 'lastupdatedutc' ? 'last_updated_utc' : 'last_updated';
      } else if (extra.iMarket < 0 && (t === 'market' || t === 'exchange')) {
        extra.iMarket = c;
      } else if (extra.iBuyDate < 0 && (t === 'buydate' || t === 'purchasedate' ||
                                        t === 'entrydate')) {
        extra.iBuyDate = c;
      } else if (extra.iTp1 < 0 && (t === 'tp1' || t === 'tp1sar' ||
                                    t === 'takeprofit' || t === 'takeprofit1' ||
                                    t === 'targetprice' || t === 'targetpricesar')) {
        extra.iTp1 = c;
      }
    }
    if (iSym < 0) return unavailable();
    if (proofSeen.updated > 1) extra.iUpdated = -1;
    var useVal = iValSar >= 0 ? iValSar : iVal;
    // v1.11.4 [P-78]: only a NATIVE value column needs the FX map.
    if (DT10_V1114_HOLDINGS_FX && iValSar < 0 && iVal >= 0 && extra.iCcy >= 0) {
      extra.fx = dt10FxRates_(ss);
    }
    var seen = {};
    for (var r = hIdx + 1; r < values.length; r++) {
      var h = dt10HoldingFromRow_(values[r], iSym, iSec, useVal, extra);
      if (!h) continue;
      if (h.value_sar === null) out._input_incomplete = true;   // v1.13.10: no FX
      if (seen[h.symbol]) { out._input_incomplete = true; continue; }
      if (out.length >= 500) { out._input_incomplete = true; continue; }
      seen[h.symbol] = true;
      out.push(h);
    }
    if (extra.fx) {                                           // v1.11.4 [P-78]
      try {
        Logger.log('[DT10 v' + DT10_VERSION + '] holdings -> gate: ' + out.length +
                   ' row(s), ' + (extra._converted || 0) + ' converted to SAR, ' +
                   (extra._unconverted || 0) + ' unconverted (no FX rate)');
      } catch (eHl) {}
    }
    return out;
  } catch (eCh) {
    try {
      Logger.log('[DT10 v' + DT10_VERSION + '] holdings read unavailable');
    } catch (eLog) {}
    return unavailable();
  }
}

/** Private declared evidence, populated separately by an authorized owner.
 * No workbook cash/date or position row is promoted into broker evidence.
 * Document scope prevents a capture from silently crossing workbooks; missing,
 * unavailable, malformed or oversized properties produce no evidence. */
function dt10ReconciliationEvidence_() {
  try {
    var props = PropertiesService.getDocumentProperties();
    if (!props) return null;
    var raw = props.getProperty('TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1');
    if (typeof raw !== 'string' || !raw || raw.length > 9000) return null;
    var packet = JSON.parse(raw);
    if (!packet || Object.prototype.toString.call(packet) !== '[object Object]' ||
        packet.schema_version !== 1) return null;
    return packet;
  } catch (eEvidence) { return null; }
}

/** Actual request seam, also used by full-source offline transport tests. */
function dt10PortfolioInputs_(ss, panel) {
  var portfolio = {
    cash_available_sar: Number(panel['Cash Available (SAR)']) || 0,
    pending_proceeds_sar: Number(panel['Pending Proceeds (SAR)']) || 0,
    holdings_input_incomplete: true
  };
  var packet = dt10ReconciliationEvidence_();
  if (packet) portfolio.reconciliation_evidence = packet;
  if (dt10SendHoldingsEnabled_()) {
    var holdings = dt10CollectHoldings_(ss);
    portfolio.holdings = holdings;
    portfolio.holdings_input_incomplete = holdings._input_incomplete === true;
  }
  return portfolio;
}
// ---------------------------------------------------------------------------
// v1.16.0 [FUNDING-EVIDENCE] -- apply the published broker packet to THIS
// request (PURE core + thin orchestrator). Script Property
// DT10_FUNDING_EVIDENCE = off (DEFAULT: the request body is v1.15.0 byte for
// byte) | on. The packet is the document property 10_My_Portfolio publishes
// from a read-only IBKR capture (PD_IBKR_EVIDENCE_PUBLISH=on); this file only
// reads it. Nothing here invents a position, a quantity, a quote or a cash
// figure: every number sent is read from the packet or from My_Portfolio.
// ---------------------------------------------------------------------------
var DT10_FUNDING_PROP = 'DT10_FUNDING_EVIDENCE';        // off (default) | on
var DT10_FUNDING_EXCLUDE_PROP = 'PD_EXECUTION_EXCLUDE';  // shared with 10_My_Portfolio
var DT10_FUNDING_MAX_AGE_MS = 840000;   // backend 900 s less a 60 s request margin
var DT10_FUNDING_MAX_ITEMS = 500;
var DT10_FUNDING_SAFE = 9007199254740991;

function dt10FundingMode_() {
  try {
    var raw = PropertiesService.getScriptProperties().getProperty(DT10_FUNDING_PROP);
    return String(raw === null || raw === undefined ? '' : raw)
        .replace(/^\s+|\s+$/g, '').toLowerCase() === 'on' ? 'on' : 'off';
  } catch (eFm) { return 'off'; }
}

/** PURE: 'a, B;c' -> ['A', 'B', 'C'] (trimmed, upper, unique). */
function dt10FundingExcludeList_(raw) {
  var out = [], seen = {};
  var parts = String(raw === null || raw === undefined ? '' : raw).split(/[,;\s]+/);
  for (var i = 0; i < parts.length; i++) {
    var s = parts[i].toUpperCase();
    if (s && !seen.hasOwnProperty(s)) { seen[s] = true; out.push(s); }
  }
  return out;
}

function dt10FundingSym_(v) {
  return String(v === null || v === undefined ? '' : v)
      .replace(/^\s+|\s+$/g, '').toUpperCase();
}

/** PURE: a plain decimal (text exactly; a number via toFixed(scale)) ->
 * integer units of 10^-scale, or null (port of 10_My_Portfolio pdIbkrUnits_). */
function dt10FundingUnits_(value, scale) {
  if (typeof value === 'boolean' || value === null || value === undefined) return null;
  var text;
  if (typeof value === 'number') {
    if (!isFinite(value) || Math.abs(value) >= 1e15) return null;
    text = value.toFixed(scale);
  } else {
    text = String(value).replace(/^\s+|\s+$/g, '');
  }
  var m = text.match(/^(-)?(\d+)(?:\.(\d+))?$/);
  if (!m) return null;
  var frac = m[3] || '';
  if (frac.length > scale) {
    if (/[^0]/.test(frac.slice(scale))) return null;
    frac = frac.slice(0, scale);
  }
  while (frac.length < scale) frac += '0';
  var digits = (m[2] + frac).replace(/^0+(?=\d)/, '');
  if (digits.length > 15) return null;
  var n = Number(digits);
  return (m[1] && n !== 0) ? -n : n;
}

/** PURE: exact integer division of safe integers; 'even' = round half to
 * even (Python Decimal default, the backend's rounding). */
function dt10FundingDiv_(num, div, mode) {
  var q = Math.floor(num / div);
  var r = num - q * div;
  while (r < 0) { q -= 1; r += div; }
  while (r >= div) { q += 1; r -= div; }
  if (r === 0) return q;
  if (mode === 'up') return q + 1;
  if (r * 2 > div) return q + 1;
  if (r * 2 < div) return q;
  return q % 2 === 0 ? q : q + 1;
}

function dt10FundingAge_(ms) {
  var m = Math.max(0, Math.round(ms / 60000));
  return m < 60 ? m + 'm' : Math.floor(m / 60) + 'h' + (m % 60) + 'm';
}

/** PURE: the decision Symbol an IBKR position stands for (same rule as
 * 10_My_Portfolio pdIbkrNative_ for the packet, which carries USD stocks
 * only): 'BRK B'/USD -> 'BRK-B.US'; anything else links nothing. */
function dt10FundingNative_(symbol, currency) {
  if (dt10FundingSym_(currency) !== 'USD') return '';
  var s = dt10FundingSym_(symbol).replace(/\s+/g, '-');
  return /^[A-Z0-9][A-Z0-9.\-]{0,15}$/.test(s) ? s + '.US' : '';
}

/** PURE: {CCY: rate x100} from the packet's FX proofs (SAR fixed at 100).
 * A proof finer than 2 decimals, repeated or unreadable -> null. */
function dt10FundingRates_(packet) {
  var rates = {SAR: 100};
  var proofs = packet && Object.prototype.toString.call(packet.fx_rates) === '[object Array]'
      ? packet.fx_rates : [];
  if (proofs.length > DT10_FUNDING_MAX_ITEMS) return null;
  for (var i = 0; i < proofs.length; i++) {
    var p = proofs[i] || {};
    var ccy = dt10FundingSym_(p.currency);
    var x100 = dt10FundingUnits_(p.rate_to_sar, 2);
    if (!/^[A-Z]{3}$/.test(ccy) || x100 === null || x100 <= 0) return null;
    if (rates.hasOwnProperty(ccy)) return null;
    rates[ccy] = x100;
  }
  return rates;
}

/** PURE: certified cash in cents from the funding accounts' settled cash
 * less reservations, at the proven rates. {cents|null, reason}. */
function dt10FundingCashCents_(packet, rates) {
  var out = {cents: null, reason: ''};
  function fail(why) { out.cents = null; out.reason = why; return out; }
  var funding = packet && Object.prototype.toString.call(packet.funding_accounts) === '[object Array]'
      ? packet.funding_accounts : [];
  var accounts = packet && Object.prototype.toString.call(packet.accounts) === '[object Array]'
      ? packet.accounts : [];
  if (!funding.length || accounts.length > DT10_FUNDING_MAX_ITEMS) return fail('funding accounts missing');
  var total = 0, rows = 0;
  for (var f = 0; f < funding.length; f++) {
    var aid = String(funding[f]);
    var acc = null;
    for (var a = 0; a < accounts.length; a++) {
      if (accounts[a] && String(accounts[a].account_id) === aid) { acc = accounts[a]; break; }
    }
    if (!acc) return fail('funding account unknown');
    var cash = Object.prototype.toString.call(acc.cash) === '[object Array]' ? acc.cash : [];
    if (!cash.length || cash.length > DT10_FUNDING_MAX_ITEMS) return fail('cash rows missing');
    for (var c = 0; c < cash.length; c++) {
      var row = cash[c] || {};
      var ccy = dt10FundingSym_(row.currency);
      var settled = dt10FundingUnits_(row.settled_cash, 6);
      var reserved = dt10FundingUnits_(row.reserved_cash, 6);
      if (settled === null || reserved === null) return fail('cash unreadable (' + ccy + ')');
      if (row.reservations_complete !== true) return fail('reservations incomplete (' + ccy + ')');
      var net = settled - reserved;
      if (net < 0) return fail('cash over-reserved (' + ccy + ')');
      if (!rates.hasOwnProperty(ccy)) return fail('cash currency without FX proof (' + ccy + ')');
      var rate = rates[ccy];
      if (net > Math.floor(DT10_FUNDING_SAFE / rate) || total > DT10_FUNDING_SAFE - net * rate) {
        return fail('cash too large for an exact check');
      }
      total += net * rate;             /* SAR at 10^-8 */
      rows++;
    }
  }
  if (!rows) return fail('cash rows missing');
  out.cents = dt10FundingDiv_(total, 1000000, 'even');
  return out;
}

/** PURE: links for THIS request's rows (row_index = position in holdings),
 * matched on the native symbol + currency; every positive packet position
 * must be represented. {links, unlinked, unrepresented, reason}. */
function dt10FundingLinks_(holdings, packet) {
  var out = {links: [], unlinked: [], unrepresented: [], reason: ''};
  var accounts = packet && Object.prototype.toString.call(packet.accounts) === '[object Array]'
      ? packet.accounts : [];
  var positions = [];
  for (var a = 0; a < accounts.length; a++) {
    var acc = accounts[a] || {};
    var list = Object.prototype.toString.call(acc.positions) === '[object Array]' ? acc.positions : [];
    if (list.length > DT10_FUNDING_MAX_ITEMS) { out.reason = 'positions oversized'; return out; }
    for (var p = 0; p < list.length; p++) {
      var pos = list[p] || {};
      var qty = dt10FundingUnits_(pos.quantity, 6);
      positions.push({account_id: String(acc.account_id), instrument_id: String(pos.instrument_id),
                      symbol: dt10FundingSym_(pos.symbol), currency: dt10FundingSym_(pos.currency),
                      native: dt10FundingNative_(pos.symbol, pos.currency),
                      positive: qty !== null && qty > 0});
    }
  }
  var used = {};
  for (var i = 0; i < holdings.length; i++) {
    var h = holdings[i] || {};
    var s = dt10FundingSym_(h.symbol), c = dt10FundingSym_(h.currency);
    var hit = null, hits = 0;
    for (var q = 0; q < positions.length; q++) {
      if (positions[q].native && positions[q].native === s && positions[q].currency === c) {
        hit = positions[q]; hits++;
      }
    }
    var key = hit ? hit.account_id + '|' + hit.instrument_id : '';
    if (hits === 1 && !used.hasOwnProperty(key)) {
      used[key] = true;
      out.links.push({row_index: i, account_id: hit.account_id, instrument_id: hit.instrument_id,
                      position_symbol: hit.symbol, symbol: s, currency: c});
    } else {
      out.unlinked.push(s || '?');
    }
  }
  for (var u = 0; u < positions.length; u++) {
    var k2 = positions[u].account_id + '|' + positions[u].instrument_id;
    if (positions[u].positive && !used.hasOwnProperty(k2)) out.unrepresented.push(positions[u].symbol);
  }
  return out;
}

/** PURE: a non-negative plain decimal at its own minimal scale (<= 6):
 * 15.2 -> {units: 152, scale: 1}; 174 -> {units: 174, scale: 0}; else null. */
function dt10FundingDecimal_(value) {
  var u = dt10FundingUnits_(value, 6);
  if (u === null || u < 0) return null;
  var scale = 6;
  while (scale > 0 && u % 10 === 0) { u = u / 10; scale--; }
  return {units: u, scale: scale};
}

/** PURE: price x quantity x proven rate, rounded half-even to the cent, or
 * null when any input is not a plain non-negative number or the exact
 * product would leave the safe-integer range (the backend then decides). */
function dt10FundingValueSar_(price, quantity, rateX100) {
  var p = dt10FundingDecimal_(price), q = dt10FundingDecimal_(quantity);
  if (!p || !q || !(rateX100 > 0)) return null;
  if (p.units > 0 && q.units > Math.floor(DT10_FUNDING_SAFE / p.units)) return null;
  var pq = p.units * q.units;
  if (pq > Math.floor(DT10_FUNDING_SAFE / rateX100)) return null;
  var div = Math.pow(10, p.scale + q.scale);     /* -> cents */
  return dt10FundingDiv_(pq * rateX100, div, 'even') / 100;
}

/** PURE core. Mutates body IN PLACE only when it returns applied=true.
 * exclude = symbols outside the broker's custody (e.g. 5023.SR). */
function dt10FundingApplyCore_(body, packet, exclude, nowMs) {
  var out = {applied: false, reason: '', ageMs: null, cashCents: null, rows: 0,
             linked: 0, unlinked: [], unrepresented: [], excluded: [], rates: null};
  function fail(why) { out.applied = false; out.reason = why; return out; }
  if (!body || !body.portfolio || typeof body.portfolio !== 'object') return fail('request body unavailable');
  if (!packet || Object.prototype.toString.call(packet) !== '[object Object]' ||
      packet.schema_version !== 1) return fail('no evidence packet');
  var capMs = dt10SourceStampMs_(packet.captured_at);
  if (capMs === null || typeof packet.captured_at !== 'string' ||
      !/(Z|[+-]\d{2}:?\d{2})$/.test(packet.captured_at)) return fail('captured_at unreadable');
  out.ageMs = nowMs - capMs;
  if (out.ageMs < 0) return fail('capture time ahead of the clock');
  if (out.ageMs > DT10_FUNDING_MAX_AGE_MS) return fail('capture too old (' + dt10FundingAge_(out.ageMs) + ')');
  var holdings = body.portfolio.holdings;
  if (Object.prototype.toString.call(holdings) !== '[object Array]') return fail('holdings not sent (DT10_SEND_HOLDINGS)');
  if (body.portfolio.holdings_input_incomplete !== false) return fail('holdings read incomplete');
  if (holdings.length > DT10_FUNDING_MAX_ITEMS) return fail('holdings oversized');
  var rates = dt10FundingRates_(packet);
  if (!rates) return fail('fx proofs unreadable');
  var set = {};
  for (var e = 0; e < (exclude || []).length; e++) set[exclude[e]] = true;
  var kept = [];
  for (var r = 0; r < holdings.length; r++) {
    var s = dt10FundingSym_(holdings[r] ? holdings[r].symbol : '');
    if (s && set.hasOwnProperty(s)) out.excluded.push(s); else kept.push(holdings[r]);
  }
  var cash = dt10FundingCashCents_(packet, rates);
  if (cash.cents === null) return fail(cash.reason);
  var linkRes = dt10FundingLinks_(kept, packet);
  if (linkRes.reason) return fail(linkRes.reason);
  var evidence;
  try { evidence = JSON.parse(JSON.stringify(packet)); } catch (eCp) { return fail('packet not serializable'); }
  evidence.holding_links = linkRes.links;
  var fx = {};
  for (var k in body.fx_rates) { if (body.fx_rates.hasOwnProperty(k)) fx[k] = body.fx_rates[k]; }
  for (var ccy in rates) {
    if (rates.hasOwnProperty(ccy) && ccy !== 'SAR') fx[ccy] = rates[ccy] / 100;
  }
  for (var v = 0; v < kept.length; v++) {
    var h = kept[v];
    var hc = dt10FundingSym_(h.currency);
    if (hc && hc !== 'SAR' && rates.hasOwnProperty(hc) &&
        typeof h.quantity === 'number' && typeof h.current_price === 'number') {
      var val = dt10FundingValueSar_(h.current_price, h.quantity, rates[hc]);
      if (val !== null) h.value_sar = val;
    }
  }
  body.portfolio.holdings = kept;
  body.portfolio.reconciliation_evidence = evidence;
  body.portfolio.cash_available_sar = cash.cents / 100;
  body.portfolio.pending_proceeds_sar = 0;
  body.fx_rates = fx;
  out.applied = true;
  out.cashCents = cash.cents;
  out.rows = kept.length;
  out.linked = linkRes.links.length;
  out.unlinked = linkRes.unlinked;
  out.unrepresented = linkRes.unrepresented;
  out.rates = rates;
  return out;
}

/** PURE: the status-line fragment (never the packet). */
function dt10FundingNote_(res) {
  if (!res) return '';
  if (!res.applied) return ' | funding evidence NOT APPLIED (' + String(res.reason || '?').slice(0, 80) + ')';
  var cents = res.cashCents, a = Math.abs(cents), c = a % 100;
  var cashText = (cents < 0 ? '-' : '') + Math.floor(a / 100) + '.' + (c < 10 ? '0' : '') + c;
  var fxText = [];
  for (var k in res.rates) {
    if (res.rates.hasOwnProperty(k) && k !== 'SAR') fxText.push(k + ' ' + (res.rates[k] / 100));
  }
  return ' | funding evidence APPLIED (capture age ' + dt10FundingAge_(res.ageMs) +
      '; cash ' + cashText + ' SAR; linked ' + res.linked + '/' + res.rows +
      (res.unlinked.length ? '; unlinked ' + res.unlinked.slice(0, 3).join(',') : '') +
      (res.unrepresented.length ? '; unrepresented ' + res.unrepresented.slice(0, 3).join(',') : '') +
      (res.excluded.length ? '; excluded ' + res.excluded.slice(0, 3).join(',') : '') +
      (fxText.length ? '; ' + fxText.join(' ') : '') + ')';
}

/** Orchestrator: DT10_FUNDING_EVIDENCE=on only. Never throws; a fault leaves
 * the body untouched and says so in the note. */
function dt10FundingApply_(body, nowMs) {
  try {
    if (dt10FundingMode_() !== 'on') return '';
    var exclude = dt10FundingExcludeList_(
        PropertiesService.getScriptProperties().getProperty(DT10_FUNDING_EXCLUDE_PROP));
    var res = dt10FundingApplyCore_(body, dt10ReconciliationEvidence_(), exclude, nowMs);
    try {
      Logger.log('[DT10 v' + DT10_VERSION + '] funding evidence ' +
                 (res.applied ? 'applied' : 'not applied: ' + res.reason));
    } catch (eLg) {}
    return dt10FundingNote_(res);
  } catch (eFa) {
    return ' | funding evidence ERROR (' + String(eFa && eFa.message ? eFa.message : eFa).slice(0, 80) + ')';
  }
}
// ---------------------------------------------------------------------------
// v1.3.0 — SELECTION STABILITY LAYER (pure core; node-parity-tested against
// core/analysis/top10_selector.py v4.21.0's simulation suite). No Apps Script
// services below except in the thin load/save/apply orchestrators further
// down; everything here takes `today` as a parameter.
// ---------------------------------------------------------------------------
/** Clamp an int knob. */
function dt10StabInt_(v, def, lo, hi) {
  var n = parseInt(v, 10);
  if (isNaN(n)) n = def;
  if (n < lo) n = lo;
  if (n > hi) n = hi;
  return n;
}
/** Panel → knob mapping (blank cells fall to built-in defaults). */
function dt10StabKnobs_(panel) {
  panel = panel || {};
  var en = String(panel['T10: Stability Enabled'] === null ||
                  panel['T10: Stability Enabled'] === undefined ?
                  'Yes' : panel['T10: Stability Enabled'])
      .replace(/^\s+|\s+$/g, '');
  return {
    enabled: !(en.charAt(0) === 'n' || en.charAt(0) === 'N'),
    confirm_days: dt10StabInt_(panel['T10: Stability Confirm Days'],
                               DT10_STAB_DEF_CONFIRM, 1, 30),
    exit_days: dt10StabInt_(panel['T10: Stability Exit Days'],
                            DT10_STAB_DEF_EXIT, 1, 30),
    rank_buffer: dt10StabInt_(panel['T10: Stability Rank Buffer'],
                              DT10_STAB_DEF_BUFFER, 0, 500),
    smooth_days: dt10StabInt_(panel['T10: Stability Smooth Days'],
                              DT10_STAB_DEF_SMOOTH, 1, 30)
  };
}
/** Whole days from ISO date a to ISO date b (floor 0; never throws). */
function dt10StabDays_(a, b) {
  function p(x) {
    var m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(x || ''));
    return m ? Date.UTC(+m[1], +m[2] - 1, +m[3]) : null;
  }
  var da = p(a), db = p(b);
  if (da === null || db === null) return 0;
  var d = Math.floor((db - da) / 86400000);
  return d > 0 ? d : 0;
}
/** Sanitize a state blob (JSON string or object) — never throws. */
function dt10StabParseState_(raw) {
  var obj = raw;
  if (typeof raw === 'string') {
    obj = null;
    var t = raw.replace(/^\s+|\s+$/g, '');
    if (t) {
      try { obj = JSON.parse(t); } catch (e) { obj = null; }
    }
  }
  if (!obj || typeof obj !== 'object') obj = {};
  var out = { v: 1, date: String(obj.date || ''), symbols: {} };
  var syms = obj.symbols;
  if (syms && typeof syms === 'object') {
    for (var k in syms) {
      if (!syms.hasOwnProperty(k)) continue;
      var sym = dt10NormSym_(k);
      var v = syms[k];
      if (!sym || !v || typeof v !== 'object') continue;
      var hist = [];
      if (v.hist && v.hist.length) {
        for (var i = 0; i < v.hist.length && hist.length < 30; i++) {
          var f = Number(v.hist[i]);
          if (isFinite(f)) hist.push(Math.round(f * 10) / 10);
        }
      }
      // v1.7.0: sanitized identity stash (display-only; null when absent
      // or malformed — parse must never throw on a hand-edited blob).
      var vid = null;
      if (v.id && typeof v.id === 'object') {
        var vpx = Number(v.id.px);
        vid = {
          n: String(v.id.n || '').slice(0, 60),
          mk: String(v.id.mk || '').slice(0, 24),
          se: String(v.id.se || '').slice(0, 24),
          cy: String(v.id.cy || '').slice(0, 8),
          px: isFinite(vpx) && vpx > 0 ? vpx : ''
        };
        if (!vid.n && !vid.mk && !vid.cy) vid = null;
      }
      out.symbols[sym] = {
        ci: Math.max(0, parseInt(v.ci, 10) || 0),
        co: Math.max(0, parseInt(v.co, 10) || 0),
        member: v.member === true,
        since: String(v.since || ''),
        ls: String(v.ls || ''),
        hist: hist,
        id: vid,
        ft: v.ft === true   // v1.11.2 [P-61]: absent (v1.11.1 blob) => false
      };
      // v1.11.10 [P-168]: consecutive outage-paused day-advances. Absent in
      // older blobs => 0; written only when > 0 so a blob that never paused
      // stays byte-identical to v1.11.9.
      var vop = Math.max(0, parseInt(v.op, 10) || 0);
      if (vop > 0) out.symbols[sym].op = vop;
      // v1.15.0 [PENDING-GRACE]: consecutive soft-miss day-advances absorbed
      // by a pending challenger. Absent in older blobs => 0; written only
      // when > 0 so a blob that never paused stays byte-identical to v1.14.0.
      var vcp = Math.max(0, parseInt(v.cp, 10) || 0);
      if (vcp > 0) out.symbols[sym].cp = vcp;
    }
  }
  return out;
}
/** v1.7.0: identity fallback for a grace-held symbol ABSENT from today's
 *  audited grid — rebuilds a display-only candidate shell from the state
 *  stash. Metric fields are '' (render blank; never stale numbers shown
 *  as current). Returns null when no stash exists, so the caller's final
 *  `|| { symbol: osym }` fallback keeps v1.6.9 behavior for symbols the
 *  stash has never seen. */
function dt10StabIdFallback_(ost, osym) {
  var oid = ost && ost.id;
  if (!oid || (!oid.n && !oid.mk && !oid.cy)) return null;
  return {
    symbol: osym,
    name: oid.n || '', market: oid.mk || '', sector: oid.se || '',
    currency: oid.cy || '', price: (oid.px === '' ? '' : oid.px),
    price_sar: '', roi_pct: '', engine_roi_pct: '', ann_roi_pct: '',
    reliability: '', dq: '', confidence_band: ''
  };
}
/** Ghost ticket for a grace-held incumbent absent from today's tickets:
 *  audited-candidate metrics, sizing SUSPENDED (honest — no stale sizing). */
function dt10StabGhost_(c, co, exitDays) {
  return {
    // v1.6.7 (D-1): explicit class marker so the SELECTED section can count
    // and name executables vs grace-holds without sniffing placeholders.
    _grace_hold: true,
    rank: '', symbol: c.symbol, name: c.name, market: c.market,
    sector: c.sector, currency: c.currency, fx_to_sar: '',
    // v1.3.1: '—' instead of silent blanks — operator decision
    // 2026-07-09 (grace-held rows without a plan must READ as "no plan
    // today", not as missing data). ROI columns stay real (grid values).
    price: c.price, price_sar: c.price_sar, entry_zone: '—',
    suggested_sar: '—', suggested_shares: '—', stop_sar: '—',
    tp1_sar: '—', tp2_sar: '—', roi_pct: c.roi_pct,
    engine_roi_pct: c.engine_roi_pct, ann_roi_pct: c.ann_roi_pct,
    exp_gain_12m_sar: '—', reliability: c.reliability, dq: c.dq,
    confidence_band: c.confidence_band, detail: {},
    advisor_note: 'Stability grace hold (' + co + '/' + exitDays +
        ' missed) — sizing suspended until it re-qualifies.'
  };
}
/** v1.8.10 [IR-094]: suspend sizing on a FAST-TRACK day-1 ticket under
 * strict. Mirrors dt10StabGhost_'s operator-decided '—' vocabulary
 * (2026-07-09: a row without a plan must READ as "no plan today") but on
 * a REAL src-copied ticket: identity, prices, ROI/RR context stay live;
 * only the executable plan (entry/size/stops/TPs/gain) is withheld.
 * Pure + node-testable; mutates and returns tk. */
function dt10FastTrackSuspend_(tk, label) {
  // v1.11.2 [P-61]: optional label — a persisted unconfirmed seat passes its
  // day-n status; the default keeps the v1.8.10 text byte-identical.
  var lbl = label || 'FAST-TRACK (day 1)';
  tk._ft_suspended = true;
  tk.entry_zone = '—';
  tk.suggested_sar = '—';
  tk.suggested_shares = '—';
  tk.stop_sar = '—';
  tk.tp1_sar = '—';
  tk.tp2_sar = '—';
  tk.exp_gain_12m_sar = '—';
  // v1.11.3 [P-77]: Funds From is a sizing disclosure too (BC-4 class) —
  // blank it on a COPY so the raw payload ticket's shared detail is untouched.
  if (DT10_V1113_SUSPEND_FUNDS && tk.detail && typeof tk.detail === 'object') {
    var dcp = {};
    for (var dk in tk.detail) {
      if (tk.detail.hasOwnProperty(dk)) dcp[dk] = tk.detail[dk];
    }
    dcp.funds_from = '—';
    tk.detail = dcp;
  }
  tk.advisor_note = lbl + ' — sizing suspended under ' +
      'strict until confirmed (' +
      'seat filled to avoid under-fill; not an executable ticket today).';
  return tk;
}
/** v1.11.9 [P-145]: suspend sizing on a GRACE-labeled seat that is
 * PRESENT in today's raw set (the src-copied full plan). Absent seats
 * were always blank (ghost path); this closes the src-present leg under
 * the same operator-decided dash vocabulary. Sets _grace_hold = true so
 * every existing grace surface applies unchanged (D-4 rank dash, paint,
 * the seat classifier, SelLog seated marker) and stamps a countable
 * _p145_suspended read-back marker. Pure + node-testable; mutates and
 * returns tk; funds_from blanked on a COPY (P-77 class) so the raw
 * payload ticket's shared detail is untouched. */
function dt10GraceSuspend_(tk, label) {
  var lbl = label || 'GRACE';
  tk._grace_hold = true;
  tk._p145_suspended = true;
  tk.entry_zone = '\u2014';
  tk.suggested_sar = '\u2014';
  tk.suggested_shares = '\u2014';
  tk.stop_sar = '\u2014';
  tk.tp1_sar = '\u2014';
  tk.tp2_sar = '\u2014';
  tk.exp_gain_12m_sar = '\u2014';
  if (DT10_V1113_SUSPEND_FUNDS && tk.detail && typeof tk.detail === 'object') {
    var gcp = {};
    for (var gk in tk.detail) {
      if (tk.detail.hasOwnProperty(gk)) gcp[gk] = tk.detail[gk];
    }
    gcp.funds_from = '\u2014';
    tk.detail = gcp;
  }
  tk.advisor_note = lbl + ' \u2014 re-qualifying seat; sizing suspended ' +
      'until the day-advance clock clears (a grace hold is not an ' +
      'executable ticket).';
  return tk;
}
/** v1.19.0 [QUALIFIED-HOLD] PURE: the ticket shell of a seat HELD under
 * DT10_QUALIFIED_HOLD = enforce. `tk` is the dt10StabGhost_ shell built
 * from the seat's audited row (no raw ticket exists, so no plan exists):
 * the grace marker goes (no _grace_hold, so dt10BoardEligible_ offers the
 * seat to the allocation replay and the D-4 rank dash, the grace tint and
 * the grace counts no longer apply), a countable _qhold marker rides for
 * read-back and the note says what the hold is. Sizing keeps the dash
 * vocabulary until the backend's replay funds it; nothing here sizes.
 * Mutates and returns tk; never throws. */
function dt10StabQualifiedHoldSeat_(tk) {
  if (!tk || typeof tk !== 'object') return tk;
  if (Object.prototype.hasOwnProperty.call(tk, '_grace_hold')) delete tk._grace_hold;
  tk._qhold = true;
  tk.advisor_note = 'Qualified hold: INVEST on the audited grid inside the ' +
      'rank buffer, absent from the sized list today; offered to the ' +
      'allocation replay (a plan appears only if the backend funds it).';
  return tk;
}
/** v1.11.10 [P-168] PURE: outage map from the sheet pool rows the cockpit
 * POSTs (dt10CollectPoolRows_ output; keys are the source headers). A row
 * whose 'Warnings' cell carries the engine's own fetch_failed tag (the
 * HTTP 402 quota-storm / 404 / timeout class: DQ capped, row BLOCKED) is
 * an OUTAGE sighting, not a market verdict. Returns { SYM: true }; never
 * throws; non-array or empty input => {}. */
function dt10OutageMapFromPool_(rows) {
  var map = {};
  if (!rows || !rows.length) return map;
  for (var i = 0; i < rows.length; i++) {
    var r = rows[i];
    if (!r || typeof r !== 'object') continue;
    var sym = dt10NormSym_(r['Symbol']);
    if (!sym) continue;
    if (DT10_P168_OUTAGE_RE.test(String(r['Warnings'] || ''))) map[sym] = true;
  }
  return map;
}
/** v1.14.0 [CONFIRM-BASIS] PURE: is `sym` INVEST-qualified inside the rank
 * buffer on this run? True when the deduped audited grid (candBySym) has a
 * row for it whose verdict is exactly INVEST (any case), structural_block is
 * not true, hardOut(sym) is false, knobs.rank_buffer > 0 and its
 * opportunity-score rank (globalRank, 1 = best) is <= knobs.rank_buffer --
 * the same window the exit-clock pause has used since v1.3.0. Never throws;
 * a symbol absent from the grid is false (absence is jitter, not a
 * qualification). hardOut is the caller's closure over candBySym + knobs. */
function dt10StabQualifiedWithin_(sym, candBySym, globalRank, knobs, hardOut) {
  var c = candBySym ? candBySym[sym] : null;
  if (!c || typeof c !== 'object') return false;
  if (String(c.verdict || '').toUpperCase() !== 'INVEST') return false;
  if (c.structural_block === true) return false;
  if (typeof hardOut === 'function' && hardOut(sym)) return false;
  var buf = knobs ? Number(knobs.rank_buffer) : 0;
  if (!(buf > 0)) return false;
  var r = globalRank ? Number(globalRank[sym]) : 0;
  return r > 0 && r <= buf;
}
/** v1.15.0 [PENDING-GRACE] PURE: is today's miss for `sym` a SOFT miss?
 * True when the deduped audited grid (candBySym) has a row for it whose
 * verdict is NOT INVEST (a criteria miss the backend itself reports),
 * structural_block is not true, hardOut(sym) is false and its all-pool rank
 * is inside the rank buffer. Never recomputes a gate, score or verdict
 * (section 8): the row, its verdict and its rank are the backend's own.
 * A row absent from the grid, a structural block, a hard-out verdict or a
 * rank outside the buffer is a HARD miss (v1.14.0 reset). Never throws. */
function dt10StabSoftMiss_(sym, candBySym, globalRank, knobs, hardOut) {
  var c = candBySym ? candBySym[sym] : null;
  if (!c || typeof c !== 'object') return false;
  if (String(c.verdict || '').toUpperCase() === 'INVEST') return false;
  if (c.structural_block === true) return false;
  if (typeof hardOut === 'function' && hardOut(sym)) return false;
  var buf = knobs ? Number(knobs.rank_buffer) : 0;
  if (!(buf > 0)) return false;
  var r = globalRank ? Number(globalRank[sym]) : 0;
  return r > 0 && r <= buf;
}
/**
 * Membership-hysteresis core. Inputs:
 *   rawTickets  payload.selected (backend's memoryless answer, best-first)
 *   cands       payload.candidates_rows (the audited grid = the "pools")
 *   state       sanitized blob (dt10StabParseState_)
 *   knobs       dt10StabKnobs_ output
 *   limit       seats (panel 'T10: Max Selected')
 *   today       'yyyy-MM-dd' UTC key
 *   outage      v1.11.10 [P-168] OPTIONAL { SYM: true } from
 *               dt10OutageMapFromPool_; absent/null => v1.11.9 clocks.
 * Returns { tickets, state, audit, note }. §8: selects AMONG the backend's
 * audited candidates only — never recomputes a gate, score, or verdict.
 */
function dt10StabCore_(rawTickets, cands, state, knobs, limit, today, outage) {
  rawTickets = rawTickets || [];
  cands = cands || [];
  limit = Math.max(1, parseInt(limit, 10) || 10);
  var symbols = state.symbols;
  var dayAdvance = state.date !== today;
  // v1.11.10 [P-168]: outage sightings (plain data, so the core stays pure)
  // and the symbols PAUSED on this run (day-advance only; see counter pass).
  var outageMap = (outage && typeof outage === 'object') ? outage : {};
  function outageOf(sym) { return outageMap[sym] === true; }
  var pausedSyms = [];
  // v1.6.6 (S-3): membership snapshot BEFORE any mutation — the
  // reconciler proves every departure is accounted for.
  var prevMembers = [];
  for (var pmk in symbols) {
    if (symbols.hasOwnProperty(pmk) && symbols[pmk].member) prevMembers.push(pmk);
  }
  // Audited-candidate index: dedupe by symbol (highest opportunity_score
  // wins), then rank the deduped set by score desc = the all-pool rank.
  var candBySym = {};
  var ci2;
  for (ci2 = 0; ci2 < cands.length; ci2++) {
    var c = cands[ci2] || {};
    var cs = dt10NormSym_(c.symbol);
    if (!cs) continue;
    var sc0 = Number(c.opportunity_score);
    if (!candBySym[cs] ||
        (isFinite(sc0) && sc0 > (Number(candBySym[cs].opportunity_score) || -1e9))) {
      candBySym[cs] = c;
    }
  }
  var rankedSyms = [];
  for (var rs in candBySym) {
    if (candBySym.hasOwnProperty(rs)) rankedSyms.push(rs);
  }
  rankedSyms.sort(function (a, b) {
    var sa = Number(candBySym[a].opportunity_score);
    var sb = Number(candBySym[b].opportunity_score);
    sa = isFinite(sa) ? sa : -1e9;
    sb = isFinite(sb) ? sb : -1e9;
    if (sb !== sa) return sb - sa;
    return a < b ? -1 : (a > b ? 1 : 0);
  });
  var globalRank = {};
  for (var gr = 0; gr < rankedSyms.length; gr++) {
    globalRank[rankedSyms[gr]] = gr + 1;
  }
  function hardOut(sym) {
    var c = candBySym[sym];
    // v1.6.6 (S-2): ABSENCE from today's audited candidates is jitter,
    // not a safety verdict — it flows to the day-keyed miss clock
    // (grace) below. Only structural blocks and the safety regex hard-exit.
    if (!c) return false;
    if (c.structural_block === true) return true;
    // v1.8.0 (BE-2, explicit opt-in): with knobs.hard_strict armed, a
    // present incumbent whose audited verdict is exactly DO_NOT_INVEST or
    // BLOCKED hard-exits instead of riding grace. Default OFF preserves
    // the v1.3.0 doctrine (plain criteria-misses ARE the jitter to absorb).
    if (knobs && knobs.hard_strict === true) {
      var vhs = String(c.verdict || '').toUpperCase();
      if (vhs === 'DO_NOT_INVEST' || vhs === 'BLOCKED') return true;
    }
    if (DT10_STAB_HARD_VERDICT_RE.test(String(c.verdict || ''))) return true;
    return false;
  }
  var rawSyms = [];
  var rawBySym = {};
  for (var ri = 0; ri < rawTickets.length; ri++) {
    var t = rawTickets[ri] || {};
    var ts = dt10NormSym_(t.symbol);
    if (ts && !rawBySym[ts]) { rawSyms.push(ts); rawBySym[ts] = t; }
  }
  var rawSet = {};
  for (var rsi = 0; rsi < rawSyms.length; rsi++) rawSet[rawSyms[rsi]] = true;
  function scoreOf(sym) {
    var c = candBySym[sym];
    if (!c) return null;
    var v = Number(c.opportunity_score);
    return isFinite(v) ? v : null;
  }
  // v1.14.0 [CONFIRM-BASIS]: the basis rides as a plain knob (service read in
  // dt10ApplyStability_). Only the exact string 'qualified' arms the
  // qualified branch; an absent knob (older callers, the self-test replays)
  // is the v1.13.11 raw basis.
  var qualifiedBasis = knobs.confirm_basis === 'qualified';
  function qualifiedWithin(sym) {
    return dt10StabQualifiedWithin_(sym, candBySym, globalRank, knobs, hardOut);
  }
  // v1.15.0 [PENDING-GRACE]: the grace rides as a plain knob (service read in
  // dt10ApplyStability_); absent/0 (older callers, the self-test replays,
  // the property unset) is the v1.14.0 clock byte for byte.
  var pgDays = Math.max(0, Math.min(DT10_T10_PENDING_GRACE_MAX,
                                    parseInt(knobs.pending_grace, 10) || 0));
  var pendingHeld = [];
  function softMiss(sym) {
    return dt10StabSoftMiss_(sym, candBySym, globalRank, knobs, hardOut);
  }
  // v1.19.0 [QUALIFIED-HOLD]: the hold mode rides as a plain knob (service
  // read in dt10ApplyStability_); absent / anything else (older callers, the
  // self-test replays, the property off) is the v1.18.0 board byte for byte.
  var qhMode = (knobs.qualified_hold === 'observe' ||
                knobs.qualified_hold === 'enforce') ? knobs.qualified_hold : 'off';
  var qualifiedHeld = [];
  // ---- counter pass (day-keyed) -------------------------------------------
  var uni = {};
  var us;
  for (us in symbols) { if (symbols.hasOwnProperty(us)) uni[us] = true; }
  for (var ur = 0; ur < rawSyms.length; ur++) uni[rawSyms[ur]] = true;
  for (var sym in uni) {
    if (!uni.hasOwnProperty(sym)) continue;
    var st = symbols[sym];
    if (!st) {
      st = { ci: 0, co: 0, member: false, since: '', ls: '', hist: [],
             ft: false };   // v1.11.2 [P-61]
      symbols[sym] = st;
    }
    var sc = scoreOf(sym);
    // v1.7.0: refresh the identity stash from today's audited grid on
    // EVERY run (day-advance or not) — the freshest sighting wins. Never
    // clears an existing stash when the symbol is absent today.
    var idc = candBySym[sym];
    if (idc) {
      var idpx = Number(idc.price);
      st.id = {
        n: String(idc.name || '').slice(0, 60),
        mk: String(idc.market || '').slice(0, 24),
        se: String(idc.sector || '').slice(0, 24),
        cy: String(idc.currency || '').slice(0, 8),
        px: isFinite(idpx) && idpx > 0 ? idpx : ''
      };
    }
    if (dayAdvance) {
      // v1.11.10 [P-168]: OUTAGE PAUSE. A day-advance sighting whose pool
      // row is fetch-failed is a data failure, not a market read: for a
      // symbol WITH a clock (member, or challenger with ci>0) that is
      // absent from raw, neither clock moves and no score point is
      // recorded. Bounded by DT10_P168_MAX_PAUSE_DAYS consecutive pauses;
      // a non-outage day-advance resets the pause counter. A symbol
      // present in raw qualified today and is never paused.
      var opPrev = st.op || 0;
      var opClock = st.member === true || (st.ci || 0) > 0;
      var opNow = opClock && !rawSet[sym] && outageOf(sym);
      if (opNow) st.op = opPrev + 1; else if (st.op) delete st.op;
      if (opNow && opPrev < DT10_P168_MAX_PAUSE_DAYS) {
        pausedSyms.push(sym);
      } else {
        if (rawSet[sym]) {
          st.ci = (st.ci || 0) + 1;
          st.co = 0;
          if (st.cp) delete st.cp;                       // v1.15.0 [PENDING-GRACE]
        } else if (qualifiedBasis && qualifiedWithin(sym)) {
          // v1.14.0 [CONFIRM-BASIS]: absent from the sized top-N but INVEST
          // on the audited grid inside the rank buffer -- the day counts
          // exactly like a raw sighting (ci + 1, co = 0; the score point
          // below is recorded as for raw). The 'raw' basis never gets here.
          st.ci = (st.ci || 0) + 1;
          st.co = 0;
          if (st.cp) delete st.cp;                       // v1.15.0 [PENDING-GRACE]
        } else if (pgDays > 0 && !st.member && (st.ci || 0) > 0 &&
                   (st.cp || 0) < pgDays && softMiss(sym)) {
          // v1.15.0 [PENDING-GRACE]: a NON-member challenger with a live count
          // misses on a SOFT criteria day (audited row present, not INVEST, no
          // structural block, not hard-out, inside the rank buffer): the count
          // PAUSES (ci kept) and the grace counter advances; the score point
          // below is still recorded. Members never enter this branch (their
          // exit clock already pauses inside the buffer); a hard miss, or a
          // grace already spent, falls through to the v1.14.0 reset.
          st.cp = (st.cp || 0) + 1;
          pendingHeld.push(sym);
        } else {
          st.ci = 0;
          if (st.cp) delete st.cp;                       // v1.15.0 [PENDING-GRACE]
          if (st.member) {
            var r = globalRank[sym];
            var within = knobs.rank_buffer > 0 && r && r <= knobs.rank_buffer;
            if (!within) st.co = (st.co || 0) + 1;
            // within buffer: rank-jitter immunity — the exit clock PAUSES
            // for the day (it does not reset).
          }
        }
        if (sc !== null) st.hist.push(Math.round(sc * 10) / 10);
      }
    } else if (sc !== null) {
      // Same-day re-run: clocks frozen; refresh the day's score point.
      if (st.hist.length) st.hist[st.hist.length - 1] = Math.round(sc * 10) / 10;
      else st.hist.push(Math.round(sc * 10) / 10);
    }
    if (st.hist.length > knobs.smooth_days) {
      st.hist = st.hist.slice(st.hist.length - knobs.smooth_days);
    }
    if (candBySym[sym]) st.ls = today;
  }
  // ---- membership pass ------------------------------------------------------
  var exitedHard = [], exitedSoft = [], exitedCapacity = [],
      exitedDisplaced = [], survivors = [];
  for (var ms in symbols) {
    if (!symbols.hasOwnProperty(ms) || !symbols[ms].member) continue;
    var stm = symbols[ms];
    // v1.11.10 [P-168]: an outage-paused member keeps its seat untouched --
    // today's absence/verdict is a data failure, not a market read.
    if (dt10InList_(pausedSyms, ms)) { survivors.push(ms); continue; }
    if (hardOut(ms)) {
      stm.member = false; stm.ci = 0; stm.co = 0;
      exitedHard.push(ms);
    } else if ((stm.co || 0) >= knobs.exit_days) {
      stm.member = false; stm.ci = 0; stm.co = 0;
      exitedSoft.push(ms);
    } else {
      survivors.push(ms);
    }
  }
  var rawIndex = {};
  for (var rix = 0; rix < rawSyms.length; rix++) rawIndex[rawSyms[rix]] = rix;
  function smoothed(sym) {
    var h = (symbols[sym] && symbols[sym].hist) || [];
    if (h.length) {
      var sum = 0;
      for (var hi2 = 0; hi2 < h.length; hi2++) sum += h[hi2];
      return sum / h.length;
    }
    var sc2 = scoreOf(sym);
    return sc2 === null ? 0 : sc2;
  }
  function boardSort(a, b) {
    var d = smoothed(b) - smoothed(a);
    if (d) return d;
    var ia = rawIndex.hasOwnProperty(a) ? rawIndex[a] : 1e9;
    var ib = rawIndex.hasOwnProperty(b) ? rawIndex[b] : 1e9;
    if (ia !== ib) return ia - ib;
    return (globalRank[a] || 1e9) - (globalRank[b] || 1e9);
  }
  survivors.sort(boardSort);
  if (survivors.length > limit) {
    for (var xc = limit; xc < survivors.length; xc++) {
      var stc = symbols[survivors[xc]];
      stc.member = false; stc.ci = 0; stc.co = 0;
      exitedCapacity.push(survivors[xc]);
    }
    survivors = survivors.slice(0, limit);
  }
  var finalSyms = survivors.slice();
  var entered = [], fastTracked = [];
  function inFinal(sym) {
    for (var fi = 0; fi < finalSyms.length; fi++) {
      if (finalSyms[fi] === sym) return true;
    }
    return false;
  }
  for (var ei = 0; ei < rawSyms.length; ei++) {   // confirmed challengers
    var es = rawSyms[ei];
    if (inFinal(es)) continue;
    if ((symbols[es].ci || 0) < knobs.confirm_days) continue;
    if (finalSyms.length < limit) {
      entered.push(es);
      finalSyms.push(es);
      continue;
    }
    // DISPLACEMENT: a CONFIRMED challenger replaces the weakest incumbent
    // only when its SMOOTHED score is higher. Fast-track never displaces.
    var weakest = finalSyms[finalSyms.length - 1];
    if (weakest && smoothed(es) > smoothed(weakest)) {
      finalSyms.pop();
      var stw = symbols[weakest];
      stw.member = false; stw.ci = 0; stw.co = 0;
      exitedDisplaced.push(weakest);
      entered.push(es);
      finalSyms.push(es);
      finalSyms.sort(boardSort);
    }
  }
  for (var ff = 0; ff < rawSyms.length && finalSyms.length < limit; ff++) {
    var fs = rawSyms[ff];                          // fast-track fill
    if (!inFinal(fs)) { fastTracked.push(fs); finalSyms.push(fs); }
  }
  var pending = [];
  for (var pi = 0; pi < rawSyms.length; pi++) {
    var ps = rawSyms[pi];
    if (!inFinal(ps) && !symbols[ps].member) {
      pending.push({ symbol: ps, confirmed_days: symbols[ps].ci || 0,
                     required_days: knobs.confirm_days });
    }
  }
  var heldByGrace = [];
  for (var mi = 0; mi < finalSyms.length; mi++) {
    var msym = finalSyms[mi];
    var mst = symbols[msym];
    if (!mst.member) {
      mst.member = true;
      mst.since = today;
      mst.co = 0;
      mst.ft = dt10InList_(fastTracked, msym);          // v1.11.2 [P-61]
    } else if (!mst.since) {
      mst.since = today;
    }
    if (mst.ft === true && (mst.ci || 0) >= knobs.confirm_days) {
      mst.ft = false;                                    // v1.11.2 [P-61] graduated
    }
    // v1.19.0 [QUALIFIED-HOLD]: a confirmed member absent from the sized list
    // with a clean miss clock that the audited grid still rates INVEST inside
    // the rank buffer (the v1.14.0 qualified day) is HOLDABLE. observe only
    // lists it; enforce keeps it out of the grace ledger here and gives it
    // the ACTIVE label in the output pass. Unconfirmed fast-track seats
    // never qualify (v1.8.10: no executable sizing under strict).
    if (qhMode !== 'off' && qualifiedBasis && !rawSet[msym] &&
        (mst.co || 0) === 0 && mst.ft !== true &&
        !dt10InList_(pausedSyms, msym) && qualifiedWithin(msym)) {
      qualifiedHeld.push(msym);
      if (qhMode === 'enforce') continue;
    }
    if (!rawSet[msym] || (mst.co || 0) > 0) {
      heldByGrace.push({ symbol: msym, missed_days: mst.co || 0,
                         exit_days: knobs.exit_days,
                         // v1.11.10 [P-168]: 0 unless paused on this run
                         outage_pause: dt10InList_(pausedSyms, msym) ?
                                       (mst.op || 0) : 0 });
    }
  }
  // ---- output tickets (stability block stamped; Rank re-stamped 1..N) ------
  function inList(arr, sym) {
    for (var li = 0; li < arr.length; li++) { if (arr[li] === sym) return true; }
    return false;
  }
  var tickets = [];
  for (var oi = 0; oi < finalSyms.length; oi++) {
    var osym = finalSyms[oi];
    var src = rawBySym[osym];
    var ost = symbols[osym];
    var tk;
    if (src) {
      tk = {};
      for (var kk in src) { if (src.hasOwnProperty(kk)) tk[kk] = src[kk]; }
    } else {
      tk = dt10StabGhost_(candBySym[osym] ||
                          dt10StabIdFallback_(ost, osym) ||
                          { symbol: osym },
                          ost.co || 0, knobs.exit_days);
    }
    var h2 = ost.hist || [];
    var sm = null;
    if (h2.length) {
      var s2 = 0;
      for (var h3 = 0; h3 < h2.length; h3++) s2 += h2[h3];
      sm = Math.round((s2 / h2.length) * 10) / 10;
    }
    var trend = 'n/a';
    if (h2.length >= 2) {
      var dl = h2[h2.length - 1] - h2[0];
      trend = dl >= DT10_STAB_TREND_EPS ? 'improving' :
              (dl <= -DT10_STAB_TREND_EPS ? 'declining' : 'steady');
    }
    var since = ost.since || today;
    var daysIn = dt10StabDays_(since, today) + 1;
    var status;
    if (inList(fastTracked, osym)) {
      status = 'FAST-TRACK (day 1)';
      // v1.8.10 [IR-094]: under strict, a day-1 fast-track fill must not
      // carry executable sizing (src copies the builder's FULL plan).
      // Toggle-gated; strict-gated; ghost path (no src) already blank.
      if (DT10_V1810_FASTTRACK_SIZING_SUSPEND && src &&
          knobs && knobs.hard_strict === true) {
        dt10FastTrackSuspend_(tk);
      }
    }
    else if (inList(entered, osym)) {
      status = 'NEW (confirmed ' + knobs.confirm_days + '/' +
               knobs.confirm_days + ')';
    } else if (qhMode === 'enforce' && inList(qualifiedHeld, osym)) {
      // v1.19.0 [QUALIFIED-HOLD] enforce: the seat keeps the status the grid
      // gives it. tk is the ghost shell (no raw ticket): strip its grace
      // marker so the allocation replay is offered the seat; no sizing here.
      status = 'ACTIVE (day ' + daysIn + ')';
      dt10StabQualifiedHoldSeat_(tk);
    } else if (!rawSet[osym] || (ost.co || 0) > 0) {
      status = 'GRACE (' + (ost.co || 0) + '/' + knobs.exit_days + ' missed)';
      // v1.11.10 [P-168]: disclose the pause on the label (paint and the
      // SelLog marker key on the 'GRACE' prefix -- the suffix is additive)
      // and on the ghost note; countable _p168_paused marker for read-back.
      if (dt10InList_(pausedSyms, osym)) {
        status += ' - outage pause ' + (ost.op || 0) + '/' +
                  DT10_P168_MAX_PAUSE_DAYS;
        tk._p168_paused = true;
        if (!src) {
          tk.advisor_note = String(tk.advisor_note || '') +
              ' Outage pause: the pool row is fetch-failed today, so the ' +
              'miss clock did not advance (' + (ost.op || 0) + '/' +
              DT10_P168_MAX_PAUSE_DAYS + ').';
        }
      }
      // v1.11.9 [P-145]: the src-PRESENT case previously kept the
      // builder's full sized plan here (label GRACE, plan INVEST -- the
      // 2026-09-16 TSM board). Ghosts (no src) are blank by
      // construction; close the present leg under the v1.6.7 doctrine.
      // Kill: DT10_P145_GRACE_SIZING_LEGACY = '1'.
      if (src && !dt10GraceSizingLegacy_()) {
        dt10GraceSuspend_(tk, status);
      }
    } else if (DT10_V1112_FASTTRACK_PERSIST && ost.ft === true) {
      // v1.11.2 [P-61]: persisted, still-unconfirmed fast-track seat — same
      // prefix (same withholding downstream) and, under strict, the same
      // sizing suspension it carried on day 1.
      status = 'FAST-TRACK (day ' + daysIn + ', ' + (ost.ci || 0) + '/' +
               knobs.confirm_days + ' confirmed)';
      if (DT10_V1810_FASTTRACK_SIZING_SUSPEND && src &&
          knobs && knobs.hard_strict === true) {
        dt10FastTrackSuspend_(tk, status);
      }
    } else {
      status = 'ACTIVE (day ' + daysIn + ')';
    }
    tk.rank = oi + 1;
    tk._stab_status = status;
    tk._stab_days = daysIn;
    tk._stab_since = since;
    tk._stab_smoothed = sm;
    tk._stab_trend = trend;
    tickets.push(tk);
  }
  // ---- state hygiene ---------------------------------------------------------
  var pruned = 0;
  for (var pk in symbols) {
    if (!symbols.hasOwnProperty(pk) || symbols[pk].member) continue;
    var pls = symbols[pk].ls || '';
    if (!pls || dt10StabDays_(pls, today) > DT10_STAB_PRUNE_DAYS) {
      delete symbols[pk];
      pruned++;
    }
  }
  state.date = today;
  state.v = 1;
  // v1.6.6 (S-3): reconcile — every previous member must be in the
  // final board or in exactly one exit list; leftovers are LOST (tripwire).
  var exitedLost = [];
  for (var rlk = 0; rlk < prevMembers.length; rlk++) {
    var rsym = prevMembers[rlk];
    if (inFinal(rsym)) continue;
    if (dt10InList_(exitedHard, rsym) || dt10InList_(exitedSoft, rsym) ||
        dt10InList_(exitedCapacity, rsym) ||
        dt10InList_(exitedDisplaced, rsym)) continue;
    exitedLost.push(rsym);
  }
  var noteBits = [];
  if (entered.length) noteBits.push(entered.length + ' new');
  if (exitedDisplaced.length) noteBits.push('displaced ' + dt10JoinCap_(exitedDisplaced, 4));
  if (exitedCapacity.length) noteBits.push('capacity-exit ' + dt10JoinCap_(exitedCapacity, 4));
  if (exitedHard.length) noteBits.push('hard-exit ' + exitedHard.join('/'));
  if (exitedSoft.length) noteBits.push('exit ' + exitedSoft.join('/'));
  if (exitedLost.length) noteBits.push('LOST ' + exitedLost.join('/'));
  if (heldByGrace.length) noteBits.push(heldByGrace.length + ' grace');
  // v1.11.10 [P-168]: read-back token (count of clocks paused this run).
  if (pausedSyms.length) noteBits.push(pausedSyms.length + ' outage-paused');
  if (fastTracked.length) noteBits.push(fastTracked.length + ' fast-track');
  // v1.11.2 [P-61]: persisted unconfirmed fast-track seats (read-back).
  var ftUnconfirmed = [], ftCarried = [];
  for (var fu = 0; fu < finalSyms.length; fu++) {
    if (symbols[finalSyms[fu]] && symbols[finalSyms[fu]].ft === true) {
      ftUnconfirmed.push(finalSyms[fu]);
      if (!inList(fastTracked, finalSyms[fu]) && rawSet[finalSyms[fu]] &&
          (symbols[finalSyms[fu]].co || 0) === 0) {          // v1.11.4 [P-80]
        ftCarried.push(finalSyms[fu]);
      }
    }
  }
  if (ftCarried.length) noteBits.push(ftCarried.length + ' ft-carried');
  if (pending.length) {
    var pparts = [];
    for (var pp2 = 0; pp2 < pending.length && pparts.length < 3; pp2++) {
      pparts.push(pending[pp2].symbol + '(' + pending[pp2].confirmed_days +
                  '/' + pending[pp2].required_days + ')');
    }
    noteBits.push('pending ' + pparts.join(' '));
  }
  var note = 'stab: ' + (noteBits.length ? noteBits.join(', ') :
                         'steady — no membership change');
  // v1.14.0 [CONFIRM-BASIS]: read-back tokens under the qualified basis only
  // ('raw' leaves the note byte-identical to v1.13.11): the armed basis and
  // the non-raw challengers inside the buffer whose count is alive.
  if (qualifiedBasis) {
    var nearSyms = [];
    for (var nqk in symbols) {
      if (!symbols.hasOwnProperty(nqk) || symbols[nqk].member || rawSet[nqk]) continue;
      if ((symbols[nqk].ci || 0) > 0 && qualifiedWithin(nqk)) nearSyms.push(nqk);
    }
    nearSyms.sort(function (a, b) {
      var dci = (symbols[b].ci || 0) - (symbols[a].ci || 0);
      if (dci) return dci;
      var drk = (globalRank[a] || 1e9) - (globalRank[b] || 1e9);
      if (drk) return drk;
      return a < b ? -1 : (a > b ? 1 : 0);
    });
    note += ', confirm=qualified';
    if (nearSyms.length) {
      var nqParts = [];
      for (var nqi = 0; nqi < nearSyms.length && nqParts.length < 3; nqi++) {
        nqParts.push(nearSyms[nqi] + '(' + (symbols[nearSyms[nqi]].ci || 0) +
                     '/' + knobs.confirm_days + ')');
      }
      note += ', near ' + nearSyms.length + ' ' + nqParts.join(' ');
    }
  }
  // v1.15.0 [PENDING-GRACE]: read-back tokens only while the grace is armed
  // (0 leaves the note and the audit byte-identical to v1.14.0): the armed
  // grace and the challengers whose count was paused on this day-advance.
  if (pgDays > 0) {
    pendingHeld.sort(function (a, b) {
      var dci = (symbols[b].ci || 0) - (symbols[a].ci || 0);
      if (dci) return dci;
      var drk = (globalRank[a] || 1e9) - (globalRank[b] || 1e9);
      if (drk) return drk;
      return a < b ? -1 : (a > b ? 1 : 0);
    });
    note += ', pgrace=' + pgDays;
    if (pendingHeld.length) {
      var phParts = [];
      for (var phi = 0; phi < pendingHeld.length && phParts.length < 3; phi++) {
        phParts.push(pendingHeld[phi] + '(' + (symbols[pendingHeld[phi]].ci || 0) +
                     '/' + knobs.confirm_days + ')');
      }
      note += ', pheld ' + pendingHeld.length + ' ' + phParts.join(' ');
    }
  }
  // v1.19.0 [QUALIFIED-HOLD]: read-back token only while armed (off leaves
  // the note and the audit byte-identical to v1.18.0): the seats held, or
  // that would be held, in board order, up to 3 names.
  if (qhMode !== 'off') {
    var qhParts = [];
    for (var qhi = 0; qhi < qualifiedHeld.length && qhParts.length < 3; qhi++) {
      qhParts.push(qualifiedHeld[qhi]);
    }
    note += ', qhold: ' + (qhMode === 'enforce' ? 'held ' : 'would hold ') +
            qualifiedHeld.length + (qhParts.length ? ' ' + qhParts.join(' ') : '');
  }
  var coreOut = {
    tickets: tickets,
    state: state,
    audit: { entered: entered, fast_tracked: fastTracked,
             exited_hard: exitedHard, exited_soft: exitedSoft,
             exited_capacity: exitedCapacity,
             exited_displaced: exitedDisplaced,
             exited_lost: exitedLost,
             held_by_grace: heldByGrace, pending: pending,
             fast_track_unconfirmed: ftUnconfirmed,   // v1.11.2 [P-61]
             outage_paused: pausedSyms,               // v1.11.10 [P-168]
             state_pruned: pruned },
    note: note
  };
  if (pgDays > 0) coreOut.audit.pending_held = pendingHeld;   // v1.15.0
  if (qhMode !== 'off') coreOut.audit.qualified_held = qualifiedHeld;   // v1.19.0
  return coreOut;
}
// ---------------------------------------------------------------------------
// Payload → render-row mappers (pure; verified by node exec-test)
// ---------------------------------------------------------------------------
var DT10_SELECTED_HEADERS = ['Rank', 'Symbol', 'Name', 'Market', 'Sector',
  'Ccy', 'FX\u2192SAR', 'Price', 'Price SAR', 'Entry Zone', 'Ticket SAR',
  'Shares', 'Stop SAR', 'TP1 SAR', 'TP2 SAR', 'ROI % (TP1)', 'Engine ROI % (12M)', 'Ann ROI %',
  'Gain 12M SAR', 'Rel', 'DQ', 'Conf', 'Funds From', 'Review By',
  // v1.3.0: Stability block (cols 25-29); Advisor Note stays last (=30).
  'Stability', 'Days', 'Since', 'Sm Score', 'Trend',
  'Advisor Note'];
function dt10TicketToRow_(t) {
  var d = t.detail || {};
  // v1.8.0 (D-4): a grace-held ghost carries no actionable rank — render
  // '—' on the board (the _Selection_Log row keeps the numeric rank).
  return [(t._grace_hold === true ? '\u2014' : dt10Cell_(t.rank)),
          dt10Cell_(t.symbol), dt10Cell_(t.name),
          dt10Cell_(t.market), dt10Cell_(t.sector), dt10Cell_(t.currency),
          dt10Cell_(t.fx_to_sar), dt10Cell_(t.price), dt10Cell_(t.price_sar),
          dt10Cell_(t.entry_zone), dt10Cell_(t.suggested_sar),
          dt10Cell_(t.suggested_shares), dt10Cell_(t.stop_sar),
          dt10Cell_(t.tp1_sar), dt10Cell_(t.tp2_sar), dt10Cell_(t.roi_pct),
          dt10Cell_(t.engine_roi_pct), dt10Cell_(t.ann_roi_pct),
          dt10Cell_(t.exp_gain_12m_sar),
          dt10Cell_(t.reliability), dt10Cell_(t.dq),
          dt10Cell_(t.confidence_band), dt10Cell_(d.funds_from),
          dt10Cell_(d.review_date),
          // v1.3.0: stability block, stamped by dt10StabCore_.
          dt10Cell_(t._stab_status), dt10Cell_(t._stab_days),
          dt10Cell_(t._stab_since), dt10Cell_(t._stab_smoothed),
          dt10Cell_(t._stab_trend),
          dt10Cell_(t.advisor_note)];
}
/* v1.10.0 (R3): withheld-order surface — index maps + ONE pure withholder.
 * Board = dt10TicketToRow_ order; Log = dt10SelLogRowFromTicket_ order.
 * Everything an order needs turns '\u2014' and the Advisor Note states the
 * withholding (no numbers); analytics and identity stay real. Pure and
 * idempotent (grace ghosts already carry '\u2014'). */
var DT10_UV_BOARD_WITHHOLD_IDX = [6, 8, 9, 10, 11, 12, 13, 14, 15, 17, 18, 22];
var DT10_UV_BOARD_NOTE_IDX = 29;   /* Advisor Note (board, 0-based) */
var DT10_UV_LOG_WITHHOLD_IDX = [9, 11, 12, 13, 14, 15, 16, 17, 18, 20, 21, 25];
var DT10_UV_LOG_NOTE_IDX = 27;     /* Advisor Note (log, 0-based) */
function dt10UvWithholdNote_(reason) {
  return 'SIZING WITHHELD \u2014 feed NOT_ACTIONABLE (' +
      (reason ? String(reason) : 'unknown') +
      '); levels, funds & gains withheld';
}
function dt10UvWithholdRow_(row, idxList, noteIdx, reason) {
  if (!row || !row.length) return row;
  for (var wi = 0; wi < idxList.length; wi++) {
    if (idxList[wi] < row.length) row[idxList[wi]] = '\u2014';
  }
  if (noteIdx < row.length) {
    // Calendar proximity is research context. Keep only the generated
    // prefix; never preserve the previous order narrative or numbers.
    // v1.18.0 [POST-EARNINGS]: the post-earnings tag is research context too
    // (it follows a forward tag when there is one). It can only exist while
    // DT10_POST_EARN is armed, so without it the kept prefix is the v1.17.1 one.
    var earnings = String(row[noteIdx] || '').match(
        /^((?:\u26a0 earnings \u2264\d+d \u00b7 )?(?:\u26a0 post-earnings \d+\/\d+ \u00b7 EPS (?:n\/a|[+-]?\d+\.\d%) \u00b7 )?)/);
    row[noteIdx] = (earnings ? earnings[1] : '') + dt10UvWithholdNote_(reason);
  }
  return row;
}
var DT10_NEARMISS_HEADERS = ['Symbol', 'Failed Gate', 'Current', 'Required',
  'Verdict', 'How To Qualify'];
function dt10NearMissToRow_(n) {
  return [dt10Cell_(n.symbol), dt10Cell_(n.failed_gate), dt10Cell_(n.current),
          dt10Cell_(n.required), dt10Cell_(n.verdict),
          dt10Cell_(n.improve_note)];
}
var DT10_ALERT_HEADERS = ['Type', 'Count', 'Required Action'];
function dt10AlertToRow_(a) {
  return [dt10Cell_(a.type), dt10Cell_(a.count), dt10Cell_(a.required_action)];
}

/**
 * v1.11.8 [P-142]: funding-narrative containment — Script Property kill
 * switch. Absent/junk => containment ON; DT10_FUNDING_WITHHOLD_LEGACY =
 * '1' => legacy (v1.11.7) rendering. Mirrors the dt10CashSourceMode_
 * property pattern; never throws.
 */
function dt10FundingWithholdLegacy_() {
  try {
    return String(PropertiesService.getScriptProperties()
        .getProperty('DT10_FUNDING_WITHHOLD_LEGACY') || '') === '1';
  } catch (eFw) {}
  return false;
}

/**
 * v1.11.9 [P-145]: grace sizing suspension -- Script Property kill
 * switch. Absent/junk => suspension ON; DT10_P145_GRACE_SIZING_LEGACY =
 * '1' => legacy (v1.11.8) rendering: a src-present GRACE seat keeps the
 * builder's full sized plan. Mirrors dt10FundingWithholdLegacy_;
 * never throws.
 */
function dt10GraceSizingLegacy_() {
  try {
    return String(PropertiesService.getScriptProperties()
        .getProperty('DT10_P145_GRACE_SIZING_LEGACY') || '') === '1';
  } catch (eGs) {}
  return false;
}

/**
 * v1.11.10 [P-168]: outage-epoch clock pause -- Script Property kill
 * switch. Absent/junk => pause ON; DT10_P168_OUTAGE_PAUSE_LEGACY = '1'
 * => legacy (v1.11.9) clocks: every day-advance miss counts. Mirrors
 * dt10GraceSizingLegacy_; never throws.
 */
function dt10OutagePauseLegacy_() {
  try {
    return String(PropertiesService.getScriptProperties()
        .getProperty('DT10_P168_OUTAGE_PAUSE_LEGACY') || '') === '1';
  } catch (eOp) {}
  return false;
}

/**
 * v1.11.11 [P-144]: epoch-keyed clocks -- Script Property kill switch.
 * Absent/junk => epoch key ON; DT10_P144_EPOCH_KEY_LEGACY = '1' => legacy
 * (v1.11.10) wall-clock UTC day key. Mirrors dt10OutagePauseLegacy_;
 * never throws.
 */
function dt10EpochKeyLegacy_() {
  try {
    return String(PropertiesService.getScriptProperties()
        .getProperty('DT10_P144_EPOCH_KEY_LEGACY') || '') === '1';
  } catch (eEk) {}
  return false;
}

/**
 * v1.14.0 [CONFIRM-BASIS]: confirmation basis -- Script Property
 * TFB_T10_CONFIRM_BASIS (DT10_T10_CONFIRM_BASIS_PROP). Exactly 'raw'
 * (trimmed, any case) selects the v1.13.11 clocks; anything else, a blank
 * or a PropertiesService fault reads as 'qualified' (the owner's default of
 * 2026-10-24). Mirrors dt10HardVerdictStrict_ (service read outside the
 * pure core); never throws.
 */
function dt10StabConfirmBasis_() {
  try {
    var v = String(PropertiesService.getScriptProperties()
        .getProperty(DT10_T10_CONFIRM_BASIS_PROP) || '')
        .replace(/^\s+|\s+$/g, '').toLowerCase();
    return v === 'raw' ? 'raw' : 'qualified';
  } catch (eCb) {}
  return 'qualified';
}

/**
 * v1.15.0 [PENDING-GRACE]: pending grace days -- Script Property
 * TFB_T10_PENDING_GRACE_DAYS (DT10_T10_PENDING_GRACE_PROP). An integer
 * 1..DT10_T10_PENDING_GRACE_MAX arms the grace (larger values clamp to the
 * max); absent, blank, 0, negative, junk or a PropertiesService fault read
 * as 0 = the v1.14.0 clocks. Mirrors dt10StabConfirmBasis_ (service read
 * outside the pure core); never throws.
 */
function dt10StabPendingGrace_() {
  try {
    var raw = String(PropertiesService.getScriptProperties()
        .getProperty(DT10_T10_PENDING_GRACE_PROP) || '')
        .replace(/^\s+|\s+$/g, '');
    if (!/^\d+$/.test(raw)) return 0;
    var n = parseInt(raw, 10);
    if (!(n > 0)) return 0;
    return Math.min(DT10_T10_PENDING_GRACE_MAX, n);
  } catch (ePg) {}
  return 0;
}

/**
 * v1.19.0 [QUALIFIED-HOLD]: hold mode -- Script Property DT10_QUALIFIED_HOLD
 * (DT10_QUALIFIED_HOLD_PROP). Exactly 'observe' or 'enforce' (trimmed, any
 * case) arms it; absent, blank, 'off', junk or a PropertiesService fault
 * read as 'off' = the v1.18.0 board. Mirrors dt10StabPendingGrace_ (service
 * read outside the pure core); never throws.
 */
function dt10StabQualifiedHoldMode_() {
  try {
    var v = String(PropertiesService.getScriptProperties()
        .getProperty(DT10_QUALIFIED_HOLD_PROP) || '')
        .replace(/^\s+|\s+$/g, '').toLowerCase();
    if (v === 'observe' || v === 'enforce') return v;
  } catch (eQh) {}
  return 'off';
}

/** v1.11.11 [P-144] PURE: UTC date key ('yyyy-MM-dd') of a feed-stamp
 * string. An explicit offset ("+03:00" / "-0500") is honoured; a naive
 * stamp is read as Riyadh (UTC+3, the producer's convention). '' when
 * unreadable. Never throws. */
function dt10StabStampUtcDate_(s) {
  var ts = String(s || '').match(
      /(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:([+-])(\d{2}):?(\d{2})|(Z))?/);
  if (!ts) return '';
  var utc = Date.UTC(+ts[1], +ts[2] - 1, +ts[3], +ts[4], +ts[5], +ts[6]);
  var offMin = 180;
  if (ts[10]) {
    offMin = 0;
  } else if (ts[7]) {
    offMin = (+ts[8]) * 60 + (+ts[9]);
    if (ts[7] === '-') offMin = -offMin;
  }
  var d = new Date(utc - offMin * 60000);
  if (isNaN(d.getTime())) return '';
  function p2(n) { return (n < 10 ? '0' : '') + n; }
  return d.getUTCFullYear() + '-' + p2(d.getUTCMonth() + 1) + '-' +
         p2(d.getUTCDate());
}

/** v1.11.11 [P-144] PURE: resolve the stability day-key for this run.
 *   feedRaw    the 'TFB Feed Global_Markets' value ('' when missing)
 *   stateDate  the persisted blob's date ('' on a fresh bootstrap)
 *   todayKey   wall-clock UTC date (the v1.11.10 key)
 * Returns { key, src: 'feed'|'held'|'clock', held: bool } -- held is true
 * whenever the run will NOT advance (key equals the state's date).
 *   feed state OK/PARTIAL/PARTIAL_FRESH/STALE_COV + readable stamp =>
 *     the stamp's UTC date ('feed');
 *   SKIPPED/FAILED/missing/unreadable => no new evidence: the state's own
 *     date ('held'; a fresh bootstrap with no date falls to the clock);
 *   never backwards (key < stateDate => stateDate, 'held');
 *   never in the future (key > todayKey => todayKey). Never throws. */
function dt10StabResolveEpoch_(feedRaw, stateDate, todayKey) {
  var sd = String(stateDate || '');
  var tk = String(todayKey || '');
  var out = { key: tk, src: 'clock', held: false };
  try {
    var s = String(feedRaw || '').replace(/^\s+|\s+$/g, '');
    var st = s ? s.split('|')[0].replace(/^\s+|\s+$/g, '').toUpperCase()
               : '';
    var fk = (st && DT10_P144_EPOCH_STATES.test(st)) ?
             dt10StabStampUtcDate_(s) : '';
    if (fk) {
      out.key = fk; out.src = 'feed';
    } else if (sd) {
      out.key = sd; out.src = 'held';
    }
    if (sd && out.key < sd) { out.key = sd; out.src = 'held'; }
    if (tk && out.key > tk) { out.key = tk; }
    if (!out.key) { out.key = tk; out.src = 'clock'; }
    out.held = !!(sd && out.key === sd);   // this run will NOT advance
  } catch (eRe) {
    out = { key: tk, src: 'clock', held: false };
  }
  return out;
}

/** v1.11.12 [SYNC-INFLIGHT] PURE: parse one 'TFB Feed <page>' value ->
 * { state, run, ms } (state = first '|' token upper-cased, run = the
 * 'run=<digits>' token or '', ms = stamp epoch ms or NaN; a naive stamp is
 * Riyadh UTC+3, the producer's convention -- same reading as
 * dt10StabStampUtcDate_). Never throws. */
function dt10FeedStampParse_(raw) {
  var out = { state: '', run: '', ms: NaN };
  try {
    var s = String(raw || '').replace(/^\s+|\s+$/g, '');
    if (!s) return out;
    out.state = s.split('|')[0].replace(/^\s+|\s+$/g, '').toUpperCase();
    var rm = s.match(/run=(\d+)/);
    if (rm) out.run = rm[1];
    var ts = s.match(
        /(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:([+-])(\d{2}):?(\d{2})|(Z))?/);
    if (!ts) return out;
    var utc = Date.UTC(+ts[1], +ts[2] - 1, +ts[3], +ts[4], +ts[5], +ts[6]);
    var offMin = 180;
    if (ts[10]) {
      offMin = 0;
    } else if (ts[7]) {
      offMin = (+ts[8]) * 60 + (+ts[9]);
      if (ts[7] === '-') offMin = -offMin;
    }
    out.ms = utc - offMin * 60000;
  } catch (eFs) {
    out = { state: '', run: '', ms: NaN };
  }
  return out;
}
/** v1.11.12 [SYNC-INFLIGHT] PURE: is a daily_sync run between its first and
 * last legs? firstRaw / lastRaw are the 'TFB Feed Market_Leaders' /
 * 'TFB Feed Global_Markets' values. Active iff both stamps parse, the run
 * ids differ, the first leg is NEWER than the last, and the first leg is at
 * most maxMin old. Returns { active, run, firstAgeMin, lastRun, detail }.
 * Unreadable input => inactive (fail-open). Never throws. */
function dt10SyncInflightCore_(firstRaw, lastRaw, nowMs, maxMin) {
  var out = { active: false, run: '', firstAgeMin: null, lastRun: '',
              detail: '' };
  try {
    var f = dt10FeedStampParse_(firstRaw);
    var l = dt10FeedStampParse_(lastRaw);
    out.run = f.run; out.lastRun = l.run;
    if (!f.run || !l.run || isNaN(f.ms) || isNaN(l.ms)) {
      out.detail = 'stamps unreadable'; return out;
    }
    out.firstAgeMin = Math.round((nowMs - f.ms) / 60000);
    if (f.run === l.run) { out.detail = 'run ' + f.run + ' complete'; return out; }
    if (f.ms <= l.ms) { out.detail = 'last leg newer'; return out; }
    var lim = (maxMin > 0) ? maxMin : DT10_INFLIGHT_MAX_MIN;
    if (out.firstAgeMin > lim) {
      out.detail = 'run ' + f.run + ' stale (' + out.firstAgeMin + 'm)';
      return out;
    }
    out.active = true;
    out.detail = 'run ' + f.run + ' ML..GM pending ' + out.firstAgeMin +
                 'm (GM still run ' + l.run + ')';
  } catch (eIf) {
    out = { active: false, run: '', firstAgeMin: null, lastRun: '',
            detail: 'error' };
  }
  return out;
}
/** v1.11.12 [SYNC-INFLIGHT]: 'off' | 'observe' | 'enforce' from Script
 * Property DT10_SYNC_INFLIGHT; absent/junk/no service => 'observe'. */
function dt10SyncInflightMode_() {
  try {
    var v = String(PropertiesService.getScriptProperties()
        .getProperty(DT10_INFLIGHT_PROP) || '').toLowerCase()
        .replace(/^\s+|\s+$/g, '');
    if (v === 'off' || v === 'enforce') return v;
  } catch (eIm) {}
  return 'observe';
}
/** v1.13.4 [WP-8]: generic off|observe|enforce reader, memoized per
 * execution (P-202). Absent / junk / service error => 'off' (fail-closed:
 * a property outage can never arm a gate). */
function dt10ModeProp_(prop, memoKey) {
  return dt10PropMemoGet_(memoKey, function () {
    try {
      var v = String(PropertiesService.getScriptProperties()
          .getProperty(prop) || '').toLowerCase()
          .replace(/^\s+|\s+$/g, '');
      if (v === 'observe' || v === 'enforce') return v;
    } catch (eMp) {}
    return 'off';
  });
}
function dt10InputTruthMode_() {
  return dt10ModeProp_(DT10_IT_PROP, 'input_truth_mode');
}
function dt10CandTabMode_() {
  return dt10ModeProp_(DT10_CANDTAB_PROP, 'cand_tab_mode');
}
function dt10VintageColsMode_() {
  return dt10ModeProp_(DT10_VINTAGE_PROP, 'vintage_mode');
}
/** PURE: validate exact sizing-control keys and values. Partial references
 * remain explicit: controls not declared by policy still compare to seed.
 * Percent limits are (0,100]; sector counts are positive whole numbers,
 * bounded by the existing pool ceiling. No null/boolean coercion. */
function dt10PolicyRefInspect_(raw) {
  var out = { values: null, status: 'none', errors: [], missing: [] };
  try {
    var obj = raw;
    if (typeof raw === 'string' || raw === null || raw === undefined) {
      var s = String(raw === null || raw === undefined ? '' : raw)
          .replace(/^\s+|\s+$/g, '');
      if (!s) return out;
      obj = JSON.parse(s);
    }
    out.status = 'invalid';
    if (!obj || typeof obj !== 'object' || Array.isArray(obj)) {
      out.errors.push('policy must be an object'); return out;
    }
    var vals = {}, any = false;
    for (var k in obj) {
      if (!Object.prototype.hasOwnProperty.call(obj, k)) continue;
      if (DT10_IT_REF_LABELS.indexOf(k) === -1) {
        out.errors.push('unknown control: ' + k); continue;
      }
      var v = obj[k], text = typeof v === 'string' ?
          v.replace(/^\s+|\s+$/g, '') : '';
      var n = (typeof v === 'number' ||
               (text && /^[+]?(?:\d+(?:\.\d*)?|\.\d+)$/.test(text))) ? Number(v) : NaN;
      var count = k === 'T10: Max Per Sector';
      if (!isFinite(n) || n <= 0 || n > (count ? DT10_POOL_HARD_CAP : 100) ||
          (count && Math.floor(n) !== n)) {
        out.errors.push('invalid range/type: ' + k); continue;
      }
      vals[k] = n; any = true;
    }
    for (var i = 0; i < DT10_IT_REF_LABELS.length; i++) {
      if (!Object.prototype.hasOwnProperty.call(vals, DT10_IT_REF_LABELS[i])) {
        out.missing.push(DT10_IT_REF_LABELS[i]);
      }
    }
    if (!any) out.errors.push('zero recognized controls');
    if (out.errors.length) return out;
    out.values = vals;
    out.status = out.missing.length ? 'partial' : 'complete';
    return out;
  } catch (ePr) {
    out.status = 'invalid'; out.errors.push('invalid JSON'); return out;
  }
}
function dt10PolicyRefParse_(raw) { return dt10PolicyRefInspect_(raw).values; }
/** v1.13.4 [WP-8]: the optional policy reference; read only when
 * DT10_INPUT_TRUTH is armed (callers guard), memoized per execution. */
function dt10PolicyRefState_() {
  return dt10PropMemoGet_('policy_ref', function () {
    try {
      return dt10PolicyRefInspect_(PropertiesService.getScriptProperties()
          .getProperty(DT10_IT_POLICY_PROP));
    } catch (ePf) {}
    return { values: null, status: 'unavailable', errors: ['property read failed'], missing: [] };
  });
}
function dt10PolicyRef_() { return dt10PolicyRefState_().values; }
/** v1.11.12 [SYNC-INFLIGHT] I/O: read both legs from _Status and evaluate.
 * Returns the core result plus mode; mode 'off' short-circuits the read. */
function dt10SyncInflight_(ss) {
  var mode = dt10SyncInflightMode_();
  var res = { active: false, run: '', firstAgeMin: null, lastRun: '',
              detail: 'off', mode: mode };
  if (mode === 'off') return res;
  try {
    var core = dt10SyncInflightCore_(
        dt10UvReadKeyed_(ss, DT10_INFLIGHT_FIRST_KEY),
        dt10UvReadKeyed_(ss, DT10_INFLIGHT_LAST_KEY),
        Date.now(), DT10_INFLIGHT_MAX_MIN);
    core.mode = mode;
    return core;
  } catch (eIo) {
    res.detail = 'read failed';
    return res;
  }
}
/** v1.11.12 [P-195] PURE: judge the previous run marker. prevRaw is the
 * stored JSON ('' when none). Returns { aborted, startedIso, ageMin }:
 * aborted is true only when a marker exists and is older than limitMin
 * (a run that never cleared it); a younger marker is a concurrent run.
 * Never throws. */
function dt10RunMarkerCore_(prevRaw, nowMs, limitMin) {
  var out = { aborted: false, startedIso: '', ageMin: null };
  try {
    var s = String(prevRaw || '').replace(/^\s+|\s+$/g, '');
    if (!s) return out;
    var obj = null;
    try { obj = JSON.parse(s); } catch (eJ) { obj = null; }
    var started = obj && obj.started ? String(obj.started) : s;
    var ms = Date.parse(started);
    if (!isFinite(ms)) return out;
    out.startedIso = started;
    out.ageMin = Math.round((nowMs - ms) / 60000);
    var lim = (limitMin > 0) ? limitMin : DT10_P195_LIMIT_MIN;
    out.aborted = out.ageMin > lim;
  } catch (eRm) {
    out = { aborted: false, startedIso: '', ageMin: null };
  }
  return out;
}
/** v1.11.12 [P-195]: Script Property DT10_P195_MARKER_LEGACY = '1' disables
 * the marker; absent/junk/no service => marker ON. Never throws. */
function dt10RunMarkerLegacy_() {
  try {
    return String(PropertiesService.getScriptProperties()
        .getProperty('DT10_P195_MARKER_LEGACY') || '') === '1';
  } catch (eRl) {}
  return false;
}
/** v1.11.12 [P-195] I/O: at entry -- log an ABORTED_PREV row when the
 * previous marker is stale, then store this run's marker. Never throws;
 * returns the core judgement (or null when the marker is off). */
function dt10RunMarkerCheck_(entry) {
  if (dt10RunMarkerLegacy_()) return null;
  var judged = null;
  try {
    var props = PropertiesService.getScriptProperties();
    var prev = props.getProperty(DT10_P195_MARKER_PROP) || '';
    judged = dt10RunMarkerCore_(prev, Date.now(), DT10_P195_LIMIT_MIN);
    if (judged.aborted) {
      dt10RunLogRow_('WARN', 'ABORTED_PREV',
          'P-195: previous cockpit run started ' + judged.startedIso + ' (' +
          judged.ageMin + 'm ago) left no completion -- aborted or timed ' +
          'out before clearing its marker; this run proceeds',
          { version: DT10_VERSION, started: judged.startedIso,
            age_min: judged.ageMin });
    }
    props.setProperty(DT10_P195_MARKER_PROP, JSON.stringify({
      started: new Date().toISOString(), v: DT10_VERSION,
      entry: String(entry || 'refreshDecisionTop10') }));
  } catch (eRc) {}
  return judged;
}
/** v1.11.12 [P-195] I/O: clear this run's marker (every return path). */
function dt10RunMarkerClear_() {
  if (dt10RunMarkerLegacy_()) return;
  try {
    PropertiesService.getScriptProperties().deleteProperty(DT10_P195_MARKER_PROP);
  } catch (eRx) {}
}
/** v1.11.12: ONE _Run_Log row in the 10-column house format for the
 * cockpit itself (Action 'refreshDecisionTop10'); mirrors
 * dt10MorningRunLog_. Never throws; true only when a row was written. */
function dt10RunLogRow_(level, status, message, details) {
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var sh = ss.getSheetByName('_Run_Log');
    if (!sh) return false;
    var dj = '{}';
    try { dj = JSON.stringify(details || {}); } catch (eJ) {}
    sh.appendRow([new Date(), String(level || 'INFO'), 'refreshDecisionTop10',
                  DT10_SHEET, String(status || ''),
                  String(message || '').slice(0, 400), '', '', '', dj]);
    return true;
  } catch (eRr) {
    return false;
  }
}
/**
 * v1.11.8 [P-142] PURE: true when an alert row is a funding
 * SOLICITATION (suppressed on a WITHHELD board). Informational alerts
 * (missing_fx, missing_valuation, ...) always pass through, and so does
 * the containment's own 'funding_withheld' disclosure row.
 */
function dt10IsFundingAlert_(a) {
  var t = String((a && a.type) || '').toLowerCase();
  return t === 'capital_call' || t === 'unfunded_candidates';
}

/**
 * v1.11.8 [P-142] PURE: true when a near-miss row carries a funding
 * solicitation — failed_gate is Funding, or the row text itself asks
 * for a deposit / CAPITAL_CALL (belt for backend wording drift).
 */
function dt10IsFundingNearMiss_(n) {
  var g = String((n && n.failed_gate) || '').toLowerCase();
  if (g.indexOf('fund') !== -1) return true;
  var txt = (String((n && n.required) || '') + ' ' +
             String((n && n.improve_note) || '') + ' ' +
             String((n && n.current) || '')).toLowerCase();
  return txt.indexOf('capital_call') !== -1 ||
         txt.indexOf('deposit') !== -1;
}

/**
 * v1.11.8 [P-142] PURE CORE: the containment transform. Inputs are
 * NEVER mutated (shallow copies only). Only state === 'WITHHELD' with
 * legacy === false changes anything; every other combination returns
 * the ORIGINAL array references untouched (byte-identity by identity).
 * Idempotent: already-transformed rows and an existing disclosure row
 * are recognized and never double-counted or duplicated.
 * Returns { nearMiss, alerts, suppressed }.
 */
function dt10ContainFundingCore_(nearMiss, alerts, state, legacy) {
  var nmIn = nearMiss || [];
  var alIn = alerts || [];
  if (legacy || state !== 'WITHHELD') {
    return { nearMiss: nmIn, alerts: alIn, suppressed: 0 };
  }
  var suppressed = 0;
  var hasDisclosure = false;
  var alOut = [];
  for (var i = 0; i < alIn.length; i++) {
    var a = alIn[i];
    if (String((a && a.type) || '').toLowerCase() === 'funding_withheld') {
      hasDisclosure = true;
      alOut.push(a);
      continue;
    }
    if (dt10IsFundingAlert_(a)) { suppressed++; continue; }
    alOut.push(a);
  }
  var nmOut = [];
  for (var j = 0; j < nmIn.length; j++) {
    var n = nmIn[j];
    if (String((n && n.required) || '').indexOf('WITHHELD \u2014') === 0) {
      nmOut.push(n);
      continue;
    }
    if (!dt10IsFundingNearMiss_(n)) { nmOut.push(n); continue; }
    suppressed++;
    var c = {};
    for (var k in n) { if (n.hasOwnProperty(k)) c[k] = n[k]; }
    c.current = '\u2014';
    c.required = 'WITHHELD \u2014 funding not evaluated ' +
                 '(feed not actionable)';
    c.improve_note = 'Re-evaluated when the feed is actionable';
    nmOut.push(c);
  }
  if (suppressed > 0 && !hasDisclosure) {
    alOut.push({ type: 'funding_withheld', count: suppressed,
      required_action: 'Funding prompts suppressed \u2014 sizing withheld ' +
                       'while the feed is not actionable [P-142]' });
  }
  return { nearMiss: nmOut, alerts: alOut, suppressed: suppressed };
}

/** v1.11.8 [P-142]: property-reading wrapper around the pure core. */
function dt10ContainFunding_(nearMiss, alerts, state) {
  return dt10ContainFundingCore_(nearMiss, alerts, state,
                                 dt10FundingWithholdLegacy_());
}
var DT10_CAND_HEADERS = ['Symbol', 'Name', 'Market', 'Sector', 'Ccy', 'Price',
  'Price SAR', 'ROI % (TP1)', 'Engine ROI % (12M)', 'Ann ROI %', 'R/R (TP2)', 'Rel', 'DQ',
  'Risk', 'News',
  'Sector Trend', 'Conflict', 'Verdict', 'Conf', 'Score', 'First Fail',
  'Failure Reason', 'Structural', 'Selected', 'Deferral'];
function dt10CandToRow_(c) {
  var ff = c.first_fail || null;
  var deferral = c.deferral;
  if (!dt10HasValue_(deferral) && dt10SeatTruthOn_()) deferral = c._dt10_held_seat_reason;
  return [dt10Cell_(c.symbol), dt10Cell_(c.name), dt10Cell_(c.market),
          dt10Cell_(c.sector), dt10Cell_(c.currency), dt10Cell_(c.price),
          dt10Cell_(c.price_sar), dt10Cell_(c.roi_pct),
          dt10Cell_(c.engine_roi_pct),
          dt10Cell_(c.ann_roi_pct), dt10Cell_(c.rr), dt10Cell_(c.reliability),
          dt10Cell_(c.dq), dt10Cell_(c.risk_level), dt10Cell_(c.news_trend),
          dt10Cell_(c.sector_trend),
          c.conflict === true ? 'Yes' : (c.conflict === false ? 'No' : ''),
          dt10Cell_(c.verdict), dt10Cell_(c.confidence_band),
          dt10Cell_(c.opportunity_score),
          dt10Cell_(ff ? ff.gate : null), dt10Cell_(c.failure_reason),
          c.structural_block === true ? 'Yes' : 'No',
          c.selected === true ? 'Yes' : 'No', dt10Cell_(deferral)];
}
// v1.2.0 — ALL QUALIFIED (full INVEST opportunity set). Derived view, not a
// payload zone; purely filters candidates_rows the page already receives.
var DT10_QUAL_HEADERS = ['Rank', 'Symbol', 'Name', 'Market', 'Sector',
  'ROI % (TP1)', 'Engine ROI % (12M)', 'Ann ROI %', 'R/R (TP2)', 'Rel', 'DQ', 'Risk', 'Score', 'Selected',
  'Why Not Selected'];
/**
 * Every candidate whose verdict is INVEST (passed all gates) — INCLUDING names
 * the selector deferred by a diversification cap — de-duplicated by symbol
 * (highest opportunity_score wins) and sorted by score descending. §8: this is
 * a pure read/filter of the payload; no gate or score is recomputed here.
 */
function dt10QualifiedFromCands_(cands) {
  cands = cands || [];
  var bySym = {};
  for (var i = 0; i < cands.length; i++) {
    var c = cands[i];
    if (!c || String(c.verdict || '') !== 'INVEST') continue;
    var sym = String(c.symbol || '');
    if (!sym) continue;
    var score = (typeof c.opportunity_score === 'number') ?
        c.opportunity_score : -1;
    var cur = bySym[sym];
    // v1.2.2: on equal scores prefer the selected=true row so the "Selected"
    // column is accurate even if a duplicate symbol ever reaches candidates.
    var better = !cur || score > cur._score ||
        (score === cur._score && c.selected === true &&
         cur.row.selected !== true);
    if (better) {
      bySym[sym] = { row: c, _score: score };
    }
  }
  var list = [];
  for (var k in bySym) {
    if (bySym.hasOwnProperty(k)) list.push(bySym[k]);
  }
  list.sort(function (a, b) { return b._score - a._score; });
  var out = [];
  for (var j = 0; j < list.length; j++) out.push(list[j].row);
  return out;
}
/**
 * v1.6.5 (Fix BE-1) — {normalisedSymbol: "n/m"} from the stability audit that
 * dt10ApplyStability_ published onto payload.meta.stability.audit.pending.
 * Never throws: any missing/odd shape yields {} and the caller falls through
 * to its previous behaviour, so a stability-off run renders exactly as v1.6.4.
 */
function dt10StabPendingMap_(payload) {
  var map = {};
  try {
    var pend = payload && payload.meta && payload.meta.stability &&
               payload.meta.stability.audit &&
               payload.meta.stability.audit.pending;
    if (!pend || !pend.length) return map;
    for (var i = 0; i < pend.length; i++) {
      var p = pend[i];
      if (!p || !p.symbol) continue;
      var k = dt10NormToken_(p.symbol);
      if (!k) continue;
      var cd = (p.confirmed_days === null || p.confirmed_days === undefined)
          ? 0 : p.confirmed_days;
      var rd = (p.required_days === null || p.required_days === undefined)
          ? '' : p.required_days;
      map[k] = String(cd) + '/' + String(rd);
    }
  } catch (e) {
    return {};
  }
  return map;
}
/**
 * v1.6.5 (Fix BE-1) — stability-aware "why not selected".
 *
 * OPERATOR FINDING (2026-07-25 board): ALL QUALIFIED listed exactly ONE name
 * (1831.SR) with reason "ranked below the Max Selected cut" while the panel
 * read T10: Max Selected = 10 and SELECTED was 0/10. One qualifier cannot be
 * ranked below a cut of ten, and nothing was cut: the seat was withheld by the
 * v1.3.0 selection-stability layer, which requires confirm_days consecutive
 * days before a new name may take a seat. dt10StabCore_ already computes that
 * set and publishes it (payload.meta.stability.audit.pending, each entry
 * carrying confirmed_days / required_days) and the STATUS NOTE already prints
 * it as "XNEW(2/3)" — but this mapper only ever consulted c.deferral, so the
 * one place the operator reads a per-name explanation asserted a rank cut that
 * had not happened. The gate was correct; the explanation was not, and an
 * operator told "ranked below the cut" reasonably concludes the name lost on
 * merit rather than that it is one day from a seat.
 *
 * BE-1: reason precedence becomes selected -> backend deferral -> STABILITY
 * PENDING (with the day count) -> rank cut, and the rank-cut branch now fires
 * ONLY when seats were actually filled and the qualified set exceeded them.
 * When neither applies the reason says so plainly instead of guessing.
 * ES5. Extra args are optional, so any caller passing (c, rank) still works.
 */
function dt10QualToRow_(c, rank, pendingMap, qualifiedCount, seatsFilled, ticketRoiMap) {
  var why;
  var pk = dt10NormToken_(c && c.symbol);
  var pend = (pendingMap && pk && pendingMap[pk]) ? pendingMap[pk] : null;
  if (c.selected === true) why = '— selected';
  else if (dt10HasValue_(c.deferral)) why = String(c.deferral);
  else if (dt10SeatTruthOn_() && c.structural_block === true &&
           (dt10HasValue_(c.failure_reason) ||
            (c.first_fail && dt10HasValue_(c.first_fail.gate)))) {
    // v1.8.8 (G-b): a structurally excluded qualifier names its REAL
    // reason (the audit grid already carries it) — never the rank cut.
    why = dt10HasValue_(c.failure_reason)
        ? String(c.failure_reason)
        : ('structural: ' + String(c.first_fail.gate));
  }
  else if (dt10SeatTruthOn_() && dt10HasValue_(c._dt10_held_seat_reason)) {
    why = String(c._dt10_held_seat_reason);
  }
  else if (pend) {
    why = 'stability: awaiting confirmation (' + pend + ' days) — ' +
          'qualified on merit, seat withheld until confirmed';
  } else if (seatsFilled > 0 && qualifiedCount > seatsFilled) {
    why = 'ranked below the Max Selected cut';
  } else if (seatsFilled > 0) {
    // v1.6.7 (D-2): seats WERE taken - do not claim otherwise.
    why = 'not selected — qualified, but absent from this run\'s ticket set (' +
          seatsFilled + ' seat(s) taken); see Status line';
  } else {
    why = 'not selected — no seat was taken this run; see Status line';
  }
  // v1.6.8 (Fix D-3): TP1-basis truth — ticketed rows show the ticket's own
  // TP1 ROI; no ticket => '—' (engine-basis stays in the next column).
  var _tp1Roi = (ticketRoiMap && pk &&
                 Object.prototype.hasOwnProperty.call(ticketRoiMap, pk))
    ? ticketRoiMap[pk] : '\u2014';
  return [rank, dt10Cell_(c.symbol), dt10Cell_(c.name), dt10Cell_(c.market),
          dt10Cell_(c.sector), dt10Cell_(_tp1Roi),
          dt10Cell_(c.engine_roi_pct), dt10Cell_(c.ann_roi_pct),
          dt10Cell_(c.rr), dt10Cell_(c.reliability), dt10Cell_(c.dq),
          dt10Cell_(c.risk_level), dt10Cell_(c.opportunity_score),
          c.selected === true ? 'Yes' : 'No', why];
}
// v1.2.4 — DATA GAPS / FAILURE BREAKDOWN. Derived view (not a payload zone):
// aggregates the audited candidates by the gate each FAILED FIRST. Headers.
var DT10_GAP_HEADERS = ['Blocking Gate', 'Candidates Failed', 'Share %',
  'Example Symbols'];
/**
 * v1.2.4 — group the audited candidates by the gate each FAILED FIRST (the
 * headline blocker the builder already assigned in first_fail.gate), so the
 * operator can see which criteria are the binding constraints on the qualified
 * set and what to relax to grow it. §8: a PURE read/aggregate of
 * candidates_rows — no gate, score, or verdict is recomputed here. INVEST rows
 * (which passed every gate) are counted but never bucketed. Returns
 *   { rows: [{ gate, count, examples[] }] sorted by count desc, total, invest }
 * where total is the number of audited candidates and invest is how many
 * passed all gates.
 */
function dt10FailureBreakdown_(cands) {
  cands = cands || [];
  var byGate = {};
  var order = [];
  var total = 0;
  var invest = 0;
  for (var i = 0; i < cands.length; i++) {
    var c = cands[i];
    if (!c) continue;
    total++;
    if (String(c.verdict || '') === 'INVEST') { invest++; continue; }
    var ff = c.first_fail || null;
    var gate = (ff && ff.gate) ? String(ff.gate) : 'Other / unscored';
    if (!byGate[gate]) {
      byGate[gate] = { gate: gate, count: 0, examples: [] };
      order.push(gate);
    }
    byGate[gate].count++;
    if (byGate[gate].examples.length < 4 && dt10HasValue_(c.symbol)) {
      byGate[gate].examples.push(String(c.symbol));
    }
  }
  var list = [];
  for (var j = 0; j < order.length; j++) list.push(byGate[order[j]]);
  list.sort(function (a, b) { return b.count - a.count; });
  return { rows: list, total: total, invest: invest };
}
/** One DATA GAPS row: gate, candidates failed, share % of audited, examples. */
function dt10GapToRow_(g, total) {
  var pct = (total > 0) ? (g.count * 100.0 / total) : 0;
  return [dt10Cell_(g.gate), dt10Cell_(g.count), pct, g.examples.join(', ')];
}
// ---------------------------------------------------------------------------
// v1.13.4 [WP-8] COCKPIT INPUT TRUTH -- pure cores (no I/O, never throw).
// ---------------------------------------------------------------------------
/** PURE: the backend's per-row gates are evaluation receipts. A display
 * label cannot attest which input the News gate actually consumed. */
function dt10GateEvaluabilityCore_(cands, newsMeta) {
  cands = cands || [];
  function unknown(v) {
    if (typeof v !== 'string') return true;
    return !/^(positive|neutral|negative)$/i.test(v.replace(/^\s+|\s+$/g, ''));
  }
  function newsReceipt(c) {
    var gates = c.gates;
    if (!Array.isArray(gates)) return null;
    var found = null;
    for (var j = 0; j < gates.length; j++) {
      var g = gates[j];
      if (!g || g.gate !== 'News') continue;
      // Duplicate/contradictory receipts are ambiguous, never evaluable.
      if (found || typeof g.passed !== 'boolean' || unknown(g.current)) return null;
      found = g;
    }
    return found;
  }
  var total = 0, newsUnk = 0, newsLabels = 0, secUnk = 0, cYes = 0, cNo = 0, cUnk = 0;
  for (var i = 0; i < cands.length; i++) {
    var c = cands[i];
    if (!c) continue;
    total++;
    if (!unknown(c.news_trend)) newsLabels++;
    if (!newsReceipt(c)) newsUnk++;
    if (unknown(c.sector_trend)) secUnk++;
    if (c.conflict === true) cYes++;
    else if (c.conflict === false) cNo++;
    else cUnk++;
  }
  function gate(u) {
    var p = total ? Math.round(u * 1000 / total) / 10 : null;
    return { unknown: u, evaluable: total - u, pct_unknown: p,
             not_evaluable: total > 0 && p > DT10_IT_UNKNOWN_MAX_PCT };
  }
  var news = gate(newsUnk), sector = gate(secUnk);
  news.display_evaluable = newsLabels;
  news.display_unknown = total - newsLabels;
  news.receipt_required = true;
  news.display_only = !!(newsMeta && newsMeta.enabled === true);
  news.cap = (newsMeta && newsMeta.cap !== undefined) ? newsMeta.cap : null;
  news.labeled = (newsMeta && newsMeta.labeled !== undefined) ?
      newsMeta.labeled : null;
  return { total: total, news: news, sector: sector,
           conflict: { yes: cYes, no: cNo, unknown: cUnk } };
}
/** PURE: the 'gates: ...' status token (never '' once the gate is armed). */
function dt10GateEvaluabilityNote_(ev, mode) {
  var tail = mode === 'observe' ? ' [observe]' : '';
  if (!ev || !ev.total) return 'gates: no candidates' + tail;
  var n = ev.news, s = ev.sector, c = ev.conflict;
  return 'gates: news ' + n.evaluable + '/' + ev.total + ' evaluable (' +
      n.pct_unknown + '% unknown' +
      '; display labels ' + n.display_evaluable + '/' + ev.total +
      (n.display_only ? ', display-only' : '') + '; backend gate receipts required' +
      ')' + (n.not_evaluable ? ' NOT EVALUABLE' : '') +
      ' \u00b7 sector ' + s.evaluable + '/' + ev.total + ' evaluable (' +
      s.pct_unknown + '% unknown)' + (s.not_evaluable ? ' NOT EVALUABLE' : '') +
      ' \u00b7 conflict yes ' + c.yes + '/no ' + c.no +
      (c.unknown ? '/unk ' + c.unknown : '') + tail;
}
/** PURE: countable alert objects, one per gate that is not evaluable. They
 * feed the _Run_Log INPUT-TRUTH details and the self-test; the cockpit
 * ALERTS table is NOT extended (cockpit <= 80 rows) and payload.alerts is
 * never mutated (P-142 doctrine). */
function dt10GateEvaluabilityAlerts_(ev) {
  var out = [];
  if (!ev || !ev.total) return out;
  var gates = [['News', ev.news], ['Sector Trend', ev.sector]];
  for (var i = 0; i < gates.length; i++) {
    var g = gates[i][1];
    if (!g.not_evaluable) continue;
    out.push({ type: 'gate_not_evaluable', count: g.unknown,
      required_action: gates[i][0] + ' gate not evaluable: ' + g.unknown +
        '/' + ev.total + ' candidates without usable ' +
        (gates[i][0] === 'News' ? 'backend gate receipts' : 'inputs') +
        ' (' + g.pct_unknown + '% > ' +
        DT10_IT_UNKNOWN_MAX_PCT + '%); the gate passes Unknown by design ' +
        '(opportunity_builder) \u2014 wire the input or read the gate as ' +
        'not applied' +
        (gates[i][0] === 'News' && g.display_only ?
         '; News labels are display-only (route NEWS-DISPLAY cap ' + g.cap +
         ')' : '') });
  }
  return out;
}
/** PURE: KPI strip cells L15:M16 -> [[labels], [values]]; total 0 -> 'n/a'. */
function dt10GateKpiCells_(ev) {
  function txt(g) {
    if (!ev || !ev.total || !g) return 'n/a';
    return g.not_evaluable ?
        'NOT EVALUABLE \u2014 ' + g.unknown + '/' + ev.total + ' Unknown' :
        'evaluable ' + g.evaluable + '/' + ev.total;
  }
  return [['News Gate', 'Sector Gate'],
          [txt(ev && ev.news), txt(ev && ev.sector)]];
}
/** Clear stale feature-owned values on observe/off/property-failure paths.
 * A never-activated sheet gets no write; unrelated panel cells are kept. */
function dt10InputTruthKpiState_(sheet, mode, ev) {
  var col = DT10_KPI_LABELS.length + 1;
  if (mode === 'enforce') {
    sheet.getRange(DT10_ROW_KPI_LABELS, col, 2, 2).setValues(dt10GateKpiCells_(ev));
    return;
  }
  var labels = sheet.getRange(DT10_ROW_KPI_LABELS, col, 1, 2).getValues()[0];
  if (labels[0] === 'News Gate' && labels[1] === 'Sector Gate') {
    sheet.getRange(DT10_ROW_KPI_LABELS + 1, col, 1, 2)
        .setValues([['INACTIVE \u2014 ' + mode, 'INACTIVE \u2014 ' + mode]]);
  }
}
/** PURE: the three sizing parameters against their DECLARED references --
 * _Lists_Config seed (TFB_PANEL_DEFAULTS, label-keyed), the GAS built-in
 * (DT10_PANEL def; T10 labels only), the dated operator decision
 * (DT10_IT_DECISION_REF) and the optional DT10_POLICY_REF. pfRead =
 * {found, values, err} from dt10PfPanelRead_ (PF labels live on
 * Portfolio_Decision). Nothing is called 'policy' unless policyRef is set. */
function dt10PanelRefCore_(panel, defaults, pfRead, policyRef) {
  panel = panel || {};
  defaults = defaults || {};
  var policyState = policyRef && typeof policyRef.status === 'string' &&
      Object.prototype.hasOwnProperty.call(policyRef, 'values') ? policyRef :
      dt10PolicyRefInspect_(policyRef);
  policyRef = policyState.values;
  function numv(x) {
    if (x === null || x === undefined || x === '') return null;
    var n = Number(String(x).replace(/,/g, '').replace(/^\s+|\s+$/g, ''));
    return isFinite(n) ? n : null;
  }
  function drift(a, b) {
    return a !== null && b !== null && Math.abs(a - b) > 1e-9;
  }
  var items = [], driftSeed = 0, driftPolicy = 0, driftDecision = 0;
  var policyCompared = 0, compared = 0, driftActive = 0;
  for (var i = 0; i < DT10_IT_REF_LABELS.length; i++) {
    var label = DT10_IT_REF_LABELS[i];
    var isT10 = label.indexOf('T10: ') === 0;
    var panelVal = isT10 ? panel[label] :
        ((pfRead && pfRead.found && pfRead.values) ? pfRead.values[label] :
         undefined);
    var builtin = null;
    if (isT10) {
      for (var p = 0; p < DT10_PANEL.length; p++) {
        if (DT10_PANEL[p].label === label) {
          builtin = numv(DT10_PANEL[p].def);
          break;
        }
      }
    }
    var it = { label: label, panel: numv(panelVal), seed: numv(defaults[label]),
               builtin: builtin,
               decision: Object.prototype.hasOwnProperty.call(
                   DT10_IT_DECISION_REF, label) ?
                   numv(DT10_IT_DECISION_REF[label]) : null,
               policy: (policyRef && Object.prototype.hasOwnProperty.call(
                   policyRef, label)) ? numv(policyRef[label]) : null };
    it.drift_seed = drift(it.panel, it.seed);
    it.drift_builtin = drift(it.panel, it.builtin);
    it.drift_decision = drift(it.panel, it.decision);
    it.drift_policy = drift(it.panel, it.policy);
    it.reference = it.policy !== null ? 'policy' : 'seed';
    it.reference_value = it.policy !== null ? it.policy : it.seed;
    it.compared = it.panel !== null && it.reference_value !== null;
    it.drift_active = drift(it.panel, it.reference_value);
    if (it.panel !== null && it.policy !== null) policyCompared++;
    if (it.compared) compared++;
    if (it.drift_active) driftActive++;
    if (it.drift_seed) driftSeed++;
    if (it.drift_decision) driftDecision++;
    if (it.drift_policy) driftPolicy++;
    items.push(it);
  }
  return { items: items, drift_seed: driftSeed, drift_decision: driftDecision,
           drift_policy: policyCompared ? driftPolicy : null,
           policy_status: policyState.status, policy_errors: policyState.errors,
           policy_missing: policyState.missing, policy_comparisons: policyCompared,
           comparisons: compared, drift_active: driftActive,
           not_evaluable: compared === 0 ||
               (policyState.status !== 'none' && policyCompared === 0),
           ref: policyRef ? (policyState.status === 'partial' ? 'mixed' : 'policy') : 'seed',
           pf_err: (pfRead && !pfRead.found) ? (pfRead.err || 'unreadable') : '' };
}
/** PURE: the ' | panel: ...' status token. Prints 'policy \u2014' per
 * parameter and 'policy ref none' until DT10_POLICY_REF is set. */
function dt10PanelRefNote_(ref, mode) {
  if (!ref) return '';
  function short(label) {
    return label.replace(/^T10: /, '').replace(/^PF: /, 'PF ');
  }
  var dash = '\u2014', dot = ' \u00b7 ', parts = [];
  for (var i = 0; i < ref.items.length; i++) {
    var it = ref.items[i];
    var refs = ['seed ' + (it.seed === null ? dash : it.seed)];
    if (it.builtin !== null) refs.push('built-in ' + it.builtin);
    if (it.decision !== null) refs.push('decision-0910 ' + it.decision);
    refs.push('policy ' + (it.policy === null ? dash : it.policy));
    var shown = it.panel === null ?
        (it.label.indexOf('PF: ') === 0 && ref.pf_err ? 'n/a' : dash) : it.panel;
    parts.push(short(it.label) + ' ' + shown + ' (' + refs.join(dot) + ')');
  }
  return ' | panel: ' + parts.join(dot) +
      (ref.pf_err ? dot + 'PF n/a (' + ref.pf_err + ')' : '') +
      dot + 'ref=' + ref.ref + ' drift ' +
      ref.drift_active +
      dot + 'policy ref ' + (ref.policy_status === 'complete' ? 'set' : ref.policy_status) +
      (ref.policy_status === 'partial' ? ' (undeclared controls use seed)' : '') +
      (ref.not_evaluable ? ' NOT EVALUABLE (zero policy comparisons)' : '') +
      (ref.policy_errors.length ? ' (' + ref.policy_errors.join('; ') + ')' : '') +
      (mode === 'observe' ? ' [observe]' : '');
}
/** PURE: one countable 'panel_reference_drift' object when the panel differs
 * from the active reference (policy when set, else seed); else []. Feeds
 * the _Run_Log details and the self-test; never the cockpit ALERTS table. */
function dt10PanelRefAlerts_(ref) {
  var out = [];
  if (!ref) return out;
  if (ref.policy_status === 'invalid' || ref.policy_status === 'unavailable' ||
      ref.not_evaluable) {
    out.push({ type: 'policy_reference_not_evaluable', count: ref.policy_comparisons,
      required_action: 'Policy reference NOT EVALUABLE: ' +
          (ref.policy_errors.join('; ') || 'zero usable comparisons') +
          '; undeclared controls still compare to seed.' });
  }
  var parts = [];
  for (var i = 0; i < ref.items.length; i++) {
    var it = ref.items[i];
    if (it.drift_active) {
      parts.push(it.label + ' ' + it.panel + ' vs ' + it.reference + ' ' +
                 it.reference_value);
    }
  }
  if (!parts.length) return out;
  out.push({ type: 'panel_reference_drift', count: parts.length,
    required_action: 'Panel differs from the ' + ref.ref + ' reference on: ' +
      parts.join('; ') + '. Operator decision: keep the panel value and ' +
      'update _Lists_Config TFB_PANEL_DEFAULTS' +
      (ref.policy_comparisons ? ' / DT10_POLICY_REF' : '') +
      ', or change the panel cell. The cockpit never writes panel cells.' });
  return out;
}
/** PURE: board symbols with a DATED upcoming earnings row -- the same
 * keep-rule as dt10EarningsMapFromValues_ (date parses and is >= today).
 * calMap is the earn.map dt10EarningsAnnotate_ already built: no second
 * Calendar_Events read. null map (tagging OFF / read failed) => 'n/a'. */
function dt10CalCoverageCore_(payload, calMap) {
  var p = payload || {};
  var map = (calMap && typeof calMap === 'object') ? calMap : null;
  function covered(list) {
    var n = 0;
    for (var i = 0; i < list.length; i++) {
      var sym = dt10NormSym_(list[i] && list[i].symbol);
      if (sym && map && Object.prototype.hasOwnProperty.call(map, sym)) n++;
    }
    return n;
  }
  var sel = p.selected || [];
  var qual = dt10QualifiedFromCands_(p.candidates_rows || []);
  var keys = 0;
  for (var k in map) { if (Object.prototype.hasOwnProperty.call(map, k)) keys++; }
  var out = { available: !!map, map_keys: keys,
              sel_covered: covered(sel), sel_total: sel.length,
              qual_covered: covered(qual), qual_total: qual.length };
  out.note = map ?
      ' | cal(earn-dated): sel ' + out.sel_covered + '/' + out.sel_total +
      ' \u00b7 qual ' + out.qual_covered + '/' + out.qual_total +
      (keys ? '' : ' (map empty)') :
      ' | cal(earn-dated): n/a';
  return out;
}
/** PURE: the one cockpit line that replaces the moved audit grid. */
function dt10CandPointerText_(n, stamp) {
  return 'Full audit (' + n + ' rows) written to hidden tab ' +
      DT10_CANDTAB_SHEET + ' \u2014 ' + stamp +
      ' \u2014 View \u25b8 Hidden sheets to open';
}
/** PURE: row 1 of _T10_Candidates; nowDate is injected (harness clock). */
function dt10CandTabStampLine_(meta, n, nowDate) {
  var iso = '';
  try { iso = nowDate.toISOString(); } catch (eIso) { iso = String(nowDate); }
  return 'run ' + iso + ' | ' + dt10MetaLine_(meta) + ' | rows ' + n +
      ' | DT10 v' + DT10_VERSION;
}
/** PURE: {SYMBOL: {quote_asof, retrieved, lkg_h, has_target}} from the pool
 * rows the cockpit already collected (keys per dt10PoolRowFromSheetRow_:
 * 'Symbol', 'Warnings', 'Last Updated (UTC)', 'Target Price', optional
 * 'acquisition_quote_asof'). Empty / symbol-less rows are skipped. */
function dt10PoolVintageIndex_(poolRows) {
  var out = {};
  var rows = poolRows || [];
  function iso(v) {
    if (Object.prototype.toString.call(v) === '[object Date]') {
      try { return v.toISOString(); } catch (eD) { return String(v); }
    }
    return String(v);
  }
  for (var i = 0; i < rows.length; i++) {
    var r = rows[i];
    if (!r) continue;
    var sym = dt10NormSym_(r['Symbol']);
    if (!sym) continue;
    var w = String(r['Warnings'] === null || r['Warnings'] === undefined ?
                   '' : r['Warnings']);
    var m1 = DT10_VINT_ASOF_RE.exec(w), m2 = DT10_VINT_LKG_RE.exec(w);
    var mp = DT10_VINT_PUBLISHED_RE.exec(w);
    var published = r['Analyst Target Published At'] || r['Target Publication Date'] ||
        r['analyst_target_published_at'] || r['analyst_target_asof'] || (mp ? mp[1] : null);
    if (published) {
      published = iso(published);
      if (!/^\d{4}-\d{2}-\d{2}(?:$|T\d{2}:\d{2}.*(?:Z|[+-]\d{2}:\d{2})$)/.test(published) ||
          !isFinite(Date.parse(published)) || Date.parse(published) > Date.now() + 300000) published = null;
      if (published) {
        var calendarDate = published.slice(0, 10);
        if (new Date(calendarDate + 'T00:00:00Z').toISOString().slice(0, 10) !== calendarDate) published = null;
      }
    }
    out[sym] = {
      quote_asof: dt10HasValue_(r['acquisition_quote_asof']) ?
          iso(r['acquisition_quote_asof']) : (m1 ? m1[1] : null),
      retrieved: dt10HasValue_(r['Last Updated (UTC)']) ?
          iso(r['Last Updated (UTC)']) : null,
      lkg_h: m2 ? parseInt(m2[1], 10) : null,
      analyst_published: published,
      has_target: dt10HasValue_(r['Target Price'])
    };
  }
  return out;
}
/** PURE: 'YYYY-MM-DDTHH:MM[:SS[.ffffff]](+00:00|Z|+hh:mm)' -> 'YYYY-MM-DD
 * HH:MMZ'. Explicit offsets are converted to UTC; a naive timestamp is
 * labelled timezone unknown rather than silently presented as UTC. */
function dt10VintageFmt_(iso) {
  var s = String(iso === null || iso === undefined ? '' : iso);
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(s)) return s.slice(0, 25);
  if (/(Z|[+-]\d\d:\d\d)$/.test(s) && isFinite(Date.parse(s))) {
    return new Date(Date.parse(s)).toISOString().slice(0, 16).replace('T', ' ') + 'Z';
  }
  return s.slice(0, 10) + ' ' + s.slice(11, 16) + ' (timezone unknown)';
}
/** PURE: ['Quote As-Of (UTC)', 'Target Vintage'] cells for one ticket.
 * 'retrieved <clock>' names the retrieval clock (never quote time);
 * A target's presence or retrieval clock never proves its analyst vintage. */
function dt10VintageCells_(ticket, idx) {
  var e = (idx && ticket) ? idx[dt10NormSym_(ticket.symbol)] : null;
  var asOf = '\u2014', vint = '\u2014';
  if (e) {
    if (e.quote_asof) asOf = dt10VintageFmt_(e.quote_asof);
    else if (e.retrieved) asOf = 'retrieved ' + dt10VintageFmt_(e.retrieved);
    if (e.lkg_h !== null && e.lkg_h !== undefined) vint = 'lkg ' + e.lkg_h + 'h';
    else if (e.has_target && e.analyst_published) vint = 'analyst ' + dt10VintageFmt_(e.analyst_published);
    else if (e.has_target) vint = 'unknown (analyst date unavailable)';
  }
  return [asOf, vint];
}
/** PURE: the 'vintage: as-of k/N lkg m/N' status token. */
function dt10VintageNote_(selected, idx, mode) {
  var sel = selected || [], asOf = 0, lkg = 0;
  for (var i = 0; i < sel.length; i++) {
    var e = idx ? idx[dt10NormSym_(sel[i] && sel[i].symbol)] : null;
    if (e && e.quote_asof) asOf++;
    if (e && e.lkg_h !== null && e.lkg_h !== undefined) lkg++;
  }
  return 'vintage: as-of ' + asOf + '/' + sel.length + ' \u00b7 lkg ' + lkg +
      '/' + sel.length + (idx ? '' : ' (no sheet pool)') +
      (mode === 'observe' ? ' [observe]' : '');
}
/** v1.19.0 [SEAT-LIVE-FILL]: Script Property DT10_SEAT_LIVE_FILL. Exactly
 * 'on' (trimmed, any case) arms the fill; absent, blank, 'off', junk or a
 * PropertiesService fault read as off (the v1.18.0 render). Never throws. */
function dt10SeatLiveFillOn_() {
  try {
    return String(PropertiesService.getScriptProperties()
        .getProperty(DT10_SEAT_LIVE_FILL_PROP) || '')
        .replace(/^\s+|\s+$/g, '').toLowerCase() === 'on';
  } catch (eLf) {}
  return false;
}
/** v1.19.0 [SEAT-LIVE-FILL] PURE: {SYM: {price, rel, dq, ccy, status}} from
 * the sheet pool rows the cockpit already POSTs (dt10CollectPoolRows_
 * output; keys are the DT10_POOL_FIELDS sends). The first row per symbol
 * wins (the collector already de-duplicates). Non-array or empty input
 * => {}; never throws. */
function dt10SeatLiveIndex_(rows) {
  var idx = {};
  if (!rows || !rows.length) return idx;
  for (var i = 0; i < rows.length; i++) {
    var r = rows[i];
    if (!r || typeof r !== 'object') continue;
    var sym = dt10NormSym_(r['Symbol']);
    if (!sym || Object.prototype.hasOwnProperty.call(idx, sym)) continue;
    idx[sym] = { price: r['Current Price'],
                 rel: r['Forecast Reliability Score'],
                 dq: r['Data Quality Score'],
                 ccy: r['Currency'],
                 status: r['Investability Status'] };
  }
  return idx;
}
/** v1.19.0 [SEAT-LIVE-FILL] PURE: fills, on the GRACE / FAST-TRACK / held
 * seats of `selected` only, the live numbers a seat without a raw ticket
 * lacks. Source row: the audited grid row (cands, deduped by symbol, the
 * highest opportunity_score wins -- the dt10StabCore_ rule) first, else
 * the pool index row (dt10SeatLiveIndex_). Per seat: a blank reliability
 * / dq is filled from the row; a blank fx_to_sar from `fx` (SAR = 1, else
 * dt10HoldingFxRate_ on the seat's currency, else the row's); a ghost's
 * price (no raw ticket: _grace_hold without _p145_suspended, or _qhold) is
 * replaced by the row's positive price, and a blank or replaced Price SAR
 * becomes the grid price_sar, else price x FX at 2 dp. When the row is
 * investable (grid verdict INVEST, or Investability Status INVESTABLE) the
 * note gains ' Source row today: investable (...)' once. Status, sizing,
 * eligibility and every other field are untouched; a second pass changes
 * nothing. Returns { seats, filled, investable }. */
function dt10SeatLiveFillCore_(selected, cands, idx, fx) {
  var out = { seats: 0, filled: 0, investable: 0 };
  selected = selected || [];
  cands = cands || [];
  idx = idx || {};
  fx = fx || {};
  var grid = {};
  for (var gi = 0; gi < cands.length; gi++) {
    var gc = cands[gi];
    if (!gc || typeof gc !== 'object') continue;
    var gs = dt10NormSym_(gc.symbol);
    if (!gs) continue;
    var gsc = Number(gc.opportunity_score);
    if (!grid[gs] ||
        (isFinite(gsc) && gsc > (Number(grid[gs].opportunity_score) || -1e9))) {
      grid[gs] = gc;
    }
  }
  for (var i = 0; i < selected.length; i++) {
    var t = selected[i];
    if (!t || typeof t !== 'object') continue;
    var seat = t._grace_hold === true || t._ft_suspended === true ||
        t._qhold === true ||
        /^(GRACE|FAST-TRACK)\b/.test(String(t._stab_status || ''));
    if (!seat) continue;
    out.seats++;
    var sym = dt10NormSym_(t.symbol);
    var g = sym && grid[sym] ? grid[sym] : null;
    var p = sym && Object.prototype.hasOwnProperty.call(idx, sym) ? idx[sym] : null;
    if (!g && !p) continue;
    var ghost = (t._grace_hold === true && t._p145_suspended !== true) ||
        t._qhold === true;
    var touched = false, priceSet = false;
    var rowPrice = Number(g ? g.price : p.price);
    var rowRel = g ? g.reliability : p.rel;
    var rowDq = g ? g.dq : p.dq;
    var rowCcy = String((g ? g.currency : p.ccy) || '');
    var investable = g ? String(g.verdict || '').toUpperCase() === 'INVEST' :
        String(p.status || '').replace(/^\s+|\s+$/g, '').toUpperCase() === 'INVESTABLE';
    if (ghost && isFinite(rowPrice) && rowPrice > 0 && t.price !== rowPrice) {
      t.price = rowPrice;
      priceSet = true;
      touched = true;
    }
    if (!dt10HasValue_(t.reliability) && dt10HasValue_(rowRel) &&
        isFinite(Number(rowRel))) {
      t.reliability = Number(rowRel);
      touched = true;
    }
    if (!dt10HasValue_(t.dq) && dt10HasValue_(rowDq) && isFinite(Number(rowDq))) {
      t.dq = Number(rowDq);
      touched = true;
    }
    var ccy = String(t.currency || rowCcy || '').replace(/^\s+|\s+$/g, '');
    if (!dt10HasValue_(t.fx_to_sar) && ccy) {
      var rate = ccy.toUpperCase() === 'SAR' ? 1 : dt10HoldingFxRate_(ccy, fx);
      if (typeof rate === 'number' && isFinite(rate) && rate > 0) {
        t.fx_to_sar = rate;
        touched = true;
      }
    }
    if (priceSet || !dt10HasValue_(t.price_sar)) {
      var gps = g ? Number(g.price_sar) : NaN;
      var fxv = Number(t.fx_to_sar), pxv = Number(t.price);
      if (g && isFinite(gps) && gps > 0) {
        t.price_sar = gps;
        touched = true;
      } else if (isFinite(fxv) && fxv > 0 && isFinite(pxv) && pxv > 0) {
        t.price_sar = Math.round(pxv * fxv * 100) / 100;
        touched = true;
      }
    }
    if (investable) {
      var note = String(t.advisor_note || '');
      if (note.indexOf(' Source row today: investable') < 0) {
        t.advisor_note = note + ' Source row today: investable (' +
            (g ? 'INVEST on the audited grid' : 'Investability Status INVESTABLE') + ').';
        out.investable++;
      }
    }
    if (touched) out.filled++;
  }
  return out;
}
/** v1.19.0 [SEAT-LIVE-FILL]: render-side orchestrator. '' when the refresh
 * attached no payload._dt10_live (gate off -> nothing touched); else fills
 * the seats in place and returns the status token
 * 'live-fill k/n (m investable at source)'. Guarded end to end. */
function dt10SeatLiveFill_(payload) {
  var lv = payload && payload._dt10_live;
  if (!lv || typeof lv !== 'object') return '';
  try {
    var r = dt10SeatLiveFillCore_(payload.selected || [],
        payload.candidates_rows || [], lv.idx || {}, lv.fx || {});
    return 'live-fill ' + r.filled + '/' + r.seats +
        (r.investable ? ' (' + r.investable + ' investable at source)' : '') +
        (lv.idx ? '' : ' (no sheet pool)');
  } catch (eLv) {
    return 'live-fill n/a';
  }
}
/** v1.19.0 [OWNER-RULE]: Script Property DT10_OWNER_RULE_MARK. Exactly 'on'
 * (trimmed, any case) arms the per-seat marker; absent, blank, 'off', junk
 * or a PropertiesService fault read as off (the v1.18.0 render). Never
 * throws. */
function dt10OwnerRuleMarkOn_() {
  try {
    return String(PropertiesService.getScriptProperties()
        .getProperty(DT10_OWNER_RULE_PROP) || '')
        .replace(/^\s+|\s+$/g, '').toLowerCase() === 'on';
  } catch (eOr) {}
  return false;
}
/** v1.19.0 [OWNER-RULE] PURE: the 'Owner Rule' cell of one seat. The
 * owner's interim buy rule (31 Oct 2026): Confidence band High AND Engine
 * ROI 12M < DT10_OWNER_RULE_ROI_MAX. 'rule: yes' when both hold; else
 * 'rule: no (<parts>)' naming only the failing parts: the band as printed
 * ('Medium', 'Low'; 'conf unknown' when blank) and 'ROI <x.x>' ('ROI
 * unknown' when not a number). Display only; never throws. */
function dt10OwnerRuleCell_(t) {
  t = t || {};
  var band = String(t.confidence_band === null || t.confidence_band === undefined ?
      '' : t.confidence_band).replace(/^\s+|\s+$/g, '');
  var roi = Number(t.engine_roi_pct);
  var roiKnown = dt10HasValue_(t.engine_roi_pct) && isFinite(roi);
  var parts = [];
  if (band.toUpperCase() !== 'HIGH') parts.push(band ? band : 'conf unknown');
  if (!(roiKnown && roi < DT10_OWNER_RULE_ROI_MAX)) {
    parts.push(roiKnown ? 'ROI ' + roi.toFixed(1) : 'ROI unknown');
  }
  return parts.length ? 'rule: no (' + parts.join('; ') + ')' : 'rule: yes';
}
/** v1.19.0 [OWNER-RULE] PURE: the status token 'owner-rule k/n yes'. */
function dt10OwnerRuleNote_(selected) {
  var sel = selected || [], yes = 0;
  for (var i = 0; i < sel.length; i++) {
    if (dt10OwnerRuleCell_(sel[i]) === 'rule: yes') yes++;
  }
  return 'owner-rule ' + yes + '/' + sel.length + ' yes';
}
var DT10_KPI_LABELS = ['Entered cash (SAR)', 'Exp. Gain 12M (SAR)', 'Selected',
  'Blended Reliability', 'Blended R/R (TP2)', 'Scanned', 'Passed',
  'Unallocated (SAR)',
  // v1.11.0 (4): funding-state layer (opportunity_builder v1.18.0 kpis)
  'Fundable Now', 'By Rotation', 'Capital Call (SAR)'];
/* v1.11.5 [K-1] kill-switch for the withheld-truth KPI strip. Default ON;
 * 0/false/off/no/disabled restores the v1.11.4 raw-kpis strip byte-
 * identically. Fail-open to ON (same doctrine as dt10PageStatusEnabled_):
 * a PropertiesService error must not let a withheld board print funded
 * KPI claims. */
function dt10KpiTruthEnabled_() {
  try {
    var v = String(PropertiesService.getScriptProperties()
        .getProperty('DT10_KPI_TRUTH') || '').toLowerCase();
    return !(v === '0' || v === 'false' || v === 'off' ||
             v === 'no' || v === 'disabled');
  } catch (ePs) {
    return true;
  }
}
/* v1.11.5 [K-1] pure: the KPI strip a WITHHELD board is allowed to show.
 * Derived from dt10KpiValues_ (one source of truth for slot order), then
 * the EXECUTION/FUNDING claims are withheld: Exp. Gain 12M, Unallocated,
 * Fundable Now, By Rotation, Capital Call -> em dash; Selected -> '0 / N
 * (withheld)'. Pool/research stats stay real: Entered cash (the operator's
 * own cash echo), Blended Reliability, Blended R/R, Scanned, Passed.
 * payload.kpis is NEVER mutated -- SEAT-CHECK still compares the raw
 * backend kpis against the board and keeps reporting the discrepancy. */
function dt10KpiWithheldValues_(k) {
  k = k || {};
  var out = dt10KpiValues_(k).slice();
  var sel = '0';
  if (k.max_selected !== null && k.max_selected !== undefined) {
    sel = '0 / ' + String(k.max_selected);
  }
  out[1] = '\u2014';
  out[2] = sel + ' (withheld)';
  out[7] = '\u2014';
  out[8] = '\u2014';
  out[9] = '\u2014';
  out[10] = '\u2014';
  return out;
}
function dt10KpiValues_(k) {
  k = k || {};
  var selected = dt10Cell_(k.selected_count);
  if (k.max_selected !== null && k.max_selected !== undefined) {
    selected = String(dt10Cell_(k.selected_count)) + ' / ' +
               String(k.max_selected);
  }
  return [dt10Cell_(k.deployable_sar), dt10Cell_(k.expected_gain_12m_sar),
          selected, dt10Cell_(k.blended_reliability),
          dt10Cell_(k.blended_rr), dt10Cell_(k.scanned), dt10Cell_(k.passed),
          dt10Cell_(k.capital_unallocated_sar),
          dt10Cell_(k.fundable_now), dt10Cell_(k.fundable_by_rotation),
          dt10Cell_(k.capital_call_topn_sar)];
}
function dt10Cell_(v) {
  return (v === null || v === undefined) ? '' : v;
}
/** One-line meta footer from payload.meta (route/pool/coverage/budget). */
function dt10MetaLine_(meta) {
  meta = meta || {};
  var route = meta.route || {};
  var pool = route.pool || {};
  var budget = meta.budget || {};
  var versions = meta.versions || {};
  var parts = [];
  parts.push('route v' + dt10Cell_(route.version));
  parts.push('builder v' + dt10Cell_(versions.opportunity_builder ||
             route.opportunity_builder_version));
  parts.push('pool=' + dt10Cell_(pool.source) + '/' + dt10Cell_(pool.count));
  if (meta.board_execution) {
    parts.push('funding=' + (meta.board_execution.allocation_finalized ? 'final' : 'research') +
        '/' + meta.board_execution.executable_count + 'exec');
  }
  if (budget.exhausted === true) parts.push('budget_exhausted');
  if (budget.universe_starved === true) parts.push('universe_starved');
  if (route.duration_ms !== undefined && route.duration_ms !== null) {
    parts.push(Math.round(route.duration_ms) + 'ms');
  }
  if (route.request_id) parts.push('req ' + route.request_id);
  return parts.join(' | ');
}
/** Status line for row 2. */
/**
 * v1.6.2 — remember the last SUCCESSFUL render so a later failure can say
 * how old the board on screen is. Never throws.
 */
function dt10RememberSuccess_(stamp, selectedCount) {
  try {
    PropertiesService.getScriptProperties().setProperty(
        'DT10_LAST_SUCCESS',
        String(stamp) + '|' + String(selectedCount == null ? '?'
                                                           : selectedCount));
  } catch (eRem) {
    // fail-open: telemetry must never break a good refresh
  }
}
/**
 * v1.6.2 — the stale-board note appended to any FAILED refresh banner.
 * Returns '' when no success has ever been recorded (nothing to claim).
 */
function dt10StaleNote_() {
  var raw = '';
  try {
    raw = String(PropertiesService.getScriptProperties()
        .getProperty('DT10_LAST_SUCCESS') || '');
  } catch (eSt) {
    raw = '';
  }
  if (!raw) {
    return ' | BOARD NOT REFRESHED \u2014 content age unknown';
  }
  var parts = raw.split('|');
  var when = parts[0] || '?';
  var n = parts.length > 1 ? parts[1] : '?';
  return ' | BOARD NOT REFRESHED \u2014 content is from ' + when +
         ' (' + n + ' selected)';
}
/**
 * v1.6.2 — surface the builder's pre-gate funnel when present, so the
 * banner answers "why only N scanned / why did none pass" by itself.
 */
function dt10FunnelNote_(payload) {
  try {
    var k = payload && payload.kpis ? payload.kpis.pregate : null;
    if (!k) { return ''; }
    return ' | pregate ' + k.pool + '\u2192elig ' + k.eligible +
           ' kept ' + k.kept;
  } catch (eFn) {
    return '';
  }
}
/**
 * v1.6.3 — is this 200 actually a refresh? Returns '' when the payload is a
 * real result (including the honest empty "no_candidates"), else a short
 * reason token. Unknown/absent status is treated as GOOD (proven-degraded
 * only) so a future status word cannot freeze the board silently.
 */
function dt10PayloadDegraded_(payload) {
  if (!payload) { return 'empty response'; }
  var st = String(payload.status || '').toLowerCase();
  if (st === 'degraded' || st === 'error' || st === 'unavailable' ||
      st === 'failed') {
    return String(payload.error || payload.message || st).slice(0, 60);
  }
  // v1.6.4: contradiction — the run says it scanned a pool but produced no
  // candidate rows at all. That is not an honest empty result.
  try {
    var k = payload.kpis || {};
    var scanned = Number(k.scanned || 0);
    var rows = payload.candidates_rows || payload.audit || [];
    var sel = payload.selected || [];
    if (scanned > 0 && rows.length === 0 && sel.length === 0 &&
        (payload.near_miss || []).length === 0) {
      return 'scanned ' + scanned + ' but produced no rows';
    }
  } catch (eK) {
    // classification must never throw — fall through to "good"
  }
  return '';
}
/** v1.11.1 PURE: count SELECTED tickets by class. exec = neither a grace
 * ghost nor fast-track-suspended AND funded (numeric suggested_shares > 0);
 * a seat with no shares is 'suspended' (no plan today). Node-testable. */
function dt10TicketClasses_(selected) {
  var out = { exec: 0, grace: 0, suspended: 0, total: 0 };
  selected = selected || [];
  for (var i = 0; i < selected.length; i++) {
    var t = selected[i];
    if (!t) continue;
    out.total++;
    if (t._grace_hold === true) { out.grace++; continue; }
    if (t._ft_suspended === true) { out.suspended++; continue; }
    var sh = Number(t.suggested_shares);
    if (isFinite(sh) && sh > 0) out.exec++;
    else out.suspended++;
  }
  return out;
}
/** v1.13.2 PURE: visible held membership is separate from executable
 * selection. Preserve the actual stability label and its confirmation or
 * miss count; this text does not assign money or replace backend deferrals. */
function dt10HeldSeatReason_(ticket) {
  var t = ticket || {};
  var kind = t._grace_hold === true ? 'GRACE-HELD' :
      (t._ft_suspended === true ? 'FAST-TRACK (SIZING SUSPENDED)' :
       'RESEARCH (NO ALLOCATION)');
  var stability = String(t._stab_status || '');
  if (!stability && dt10HasValue_(t._stab_days)) stability = 'day ' + String(t._stab_days);
  return 'Held research seat — ' + kind +
      (stability ? '; stability: ' + stability : '') +
      '; no executable allocation today';
}
/** v1.11.0 (2) / v1.11.1: the OUTPUT state of the board, separate from the
 * route status. EXECUTABLE = >= 1 funded, non-held ticket on an actionable
 * feed; WITHHELD = seats or qualified names exist but the feed is not
 * actionable; HELD = seats exist and the feed is actionable, but every seat
 * is a grace ghost or a fast-track suspension (no plan today);
 * QUALIFIED_UNFUNDED = qualified names, no seat; EMPTY = nothing passed.
 * The feed verdict is the parse the render stashed (payload._dt10_uv); the
 * legacy meta regex stays as a fallback for callers that never rendered. */
function dt10OutputStatus_(payload) {
  try {
    var p = payload || {};
    var k = p.kpis || {};
    var cls = dt10TicketClasses_(p.selected || []);
    var passed = Number(k.passed);
    if (!isFinite(passed)) passed = (p.qualified || []).length;
    var feedOk;
    if (p._dt10_uv && p._dt10_uv.state) {
      feedOk = (p._dt10_uv.state === 'EXECUTABLE');
    } else {
      var uv = '';
      try { uv = String((p.meta && p.meta.upstream_verdict) || p.upstream_verdict || ''); } catch (e1) {}
      feedOk = !/NOT_ACTIONABLE|WITHHELD|STALE|AGED/i.test(uv);
    }
    if (!feedOk && (cls.total > 0 || passed > 0 ||
        (p._dt10_uv && p._dt10_uv.source_blocked === true))) return 'WITHHELD';
    if (cls.exec > 0) return 'EXECUTABLE';
    var funding = p.meta && p.meta.board_funding;
    if (funding && funding.finalized === true && funding.stage === 'allocate' &&
        (funding.eligible_symbols || []).length > 0) return 'QUALIFIED_UNFUNDED';
    if (cls.total > 0) return 'HELD';
    if (passed > 0) return 'QUALIFIED_UNFUNDED';
    return 'EMPTY';
  } catch (e) {
    return 'UNKNOWN';
  }
}

/** TFB-02 narrow renderer fix: an upstream-ready feed is input evidence,
 * while only the finalized board may claim executable opportunities.
 * Pure given the payload and display flag; call after dt10FinalizeBoard_. */
function dt10SelectedTitle_(payload, showFeed) {
  var p = payload || {}, selected = p.selected || [];
  var cls = dt10TicketClasses_(selected);
  var output = dt10OutputStatus_(p);
  var execN = output === 'EXECUTABLE' ? cls.exec : 0;
  var fastN = selected.filter(function (t) {
    return t && t._ft_suspended === true && t._grace_hold !== true;
  }).length;
  var researchN = Math.max(0, cls.suspended - fastN);
  var title = 'SELECTED — ' + execN + ' EXECUTABLE TICKET' + (execN === 1 ? '' : 'S') +
      (fastN > 0 ? ' + ' + fastN + ' FAST-TRACK (SIZING SUSPENDED)' : '') +
      (researchN > 0 ? ' + ' + researchN + ' RESEARCH (NO ALLOCATION)' : '') +
      (cls.grace > 0 ? ' + ' + cls.grace + ' GRACE-HELD (NO PLAN TODAY)' : '');
  var uv = p._dt10_uv || {};
  if (showFeed && uv.state !== 'EXECUTABLE') {
    var qualN = cls.exec + cls.suspended;
    return '\u26d4 FEED NOT ACTIONABLE \u2014 ' + String(uv.reason || 'verdict unknown') +
        ' \u2014 SELECTED \u2014 0 EXECUTABLE / ' + qualN + ' QUALIFIED PLAN' +
        (qualN === 1 ? '' : 'S') +
        (cls.grace > 0 ? ' + ' + cls.grace + ' GRACE-HELD (NO PLAN TODAY)' : '') +
        ' \u2014 SIZING WITHHELD';
  }
  var finalState = output === 'EXECUTABLE' ? 'EXECUTABLE' :
      (cls.total > 0 || output === 'QUALIFIED_UNFUNDED' ? 'HELD' : 'NO_TRADE');
  if (!showFeed) return finalState + ' \u2014 ' + title;
  return (finalState === 'EXECUTABLE' ? '\u2705' : '\u2139') +
      ' FEED ACTIONABLE \u2014 ' + finalState +
      (uv.ageMin != null ? ' (verdict age ' + Math.round(uv.ageMin) + 'm)' : '') +
      ' \u2014 ' + title;
}

function dt10StatusLine_(status, extra, output) {
  var stamp = '';
  try {
    stamp = Utilities.formatDate(new Date(),
        Session.getScriptTimeZone() || 'Asia/Riyadh', 'yyyy-MM-dd HH:mm:ss');
  } catch (e) {
    stamp = new Date().toString();
  }
  return 'Last run ' + stamp + ' | status: ' + status +
         (output ? ' | output: ' + output : '') +
         (extra ? ' | ' + extra : '');
}
/**
 * v1.4.0 — Cockpit-owned _Status upsert. See header WHY block.
 * NEVER throws; typeof-guarded on writePageStatus_ (02_Core); zero values
 * stringified to survive the status writer's || '' falsy coercion.
 */
/**
 * v1.4.0 — kill-switch reader. Default ON; any of 0/false/off/no/
 * disabled turns the _Status upsert off (banner + SelLog unaffected).
 * Fail-open to ON: a PropertiesService error must not silence reporting.
 */
function dt10PageStatusEnabled_() {
  try {
    var v = String(PropertiesService.getScriptProperties()
        .getProperty('DT10_PAGE_STATUS') || '').toLowerCase();
    return !(v === '0' || v === 'false' || v === 'off' ||
             v === 'no' || v === 'disabled');
  } catch (ePs) {
    return true;
  }
}
/** v1.5.0 (Fix H1): send My_Portfolio holdings with every refresh so the
 * builder's Portfolio gate can enforce "Include Portfolio Holdings = No".
 * Default ON. Disabling transport keeps holdings_input_incomplete=true;
 * it cannot bypass the account-scoped funding contract. */
function dt10SendHoldingsEnabled_() {
  try {
    var v = String(PropertiesService.getScriptProperties()
        .getProperty('DT10_SEND_HOLDINGS') || '').toLowerCase();
    return !(v === '0' || v === 'false' || v === 'off' ||
             v === 'no' || v === 'disabled');
  } catch (eHs) {
    return true;
  }
}
/** v1.19.1: Script Property DT10_AUDIT_KEEP. Exactly 'on' (trimmed, any
 * case) arms the held-seat hand-off; absent or any other word is off. A
 * property-service failure reads as OFF: the list is display context only,
 * so failing open keeps the unarmed request. Memoized (P-202). */
var DT10_AUDIT_KEEP_PROP = 'DT10_AUDIT_KEEP';
var DT10_AUDIT_KEEP_MAX = 60;   // mirrors opportunity_builder _AUDIT_KEEP_MAX
function dt10AuditKeepOn_() {
  return dt10PropMemoGet_('audit_keep', function () {
    try {
      return String(PropertiesService.getScriptProperties()
          .getProperty(DT10_AUDIT_KEEP_PROP) || '')
          .replace(/^\s+|\s+$/g, '').toLowerCase() === 'on';
    } catch (eProp) { return false; }
  });
}
/** v1.19.1 PURE: the stability members (member === true: seated, grace-held
 * and fast-track seats) of a parsed DT10_STAB_STATE_V1 state, sorted, at
 * most DT10_AUDIT_KEEP_MAX. [] for a missing or malformed state. */
function dt10AuditKeepSymbols_(state) {
  var out = [];
  var syms = state && state.symbols;
  if (!syms || typeof syms !== 'object') return out;
  for (var k in syms) {
    if (syms.hasOwnProperty(k) && k && syms[k] && syms[k].member === true) {
      out.push(String(k));
    }
  }
  out.sort();
  return out.slice(0, DT10_AUDIT_KEEP_MAX);
}

/**
 * v1.11.7 [P-134]: cockpit cash source-of-truth mode.
 * panel (default) | observe | portfolio. Absent/junk => panel.
 */
function dt10CashSourceMode_() {
  try {
    var v = String(PropertiesService.getScriptProperties()
        .getProperty('DT10_CASH_SOURCE') || '').toLowerCase();
    if (v === 'observe' || v === 'portfolio') return v;
  } catch (eCm) {}
  return 'panel';
}

/**
 * v1.11.7 [P-134] PURE: find the PF cash cell in a values block.
 * Label cell contains 'cash available' (normalized, so 'PF: Cash
 * Available SAR' matches); the VALUE is the cell immediately to the
 * right ('23,242.50' string commas tolerated). Scan capped 15x30.
 * Returns {found, value, label, row, col} (1-based).
 */
function dt10CashScan_(values) {
  var rMax = Math.min((values || []).length, 15);
  for (var r = 0; r < rMax; r++) {
    var row = values[r] || [];
    var cMax = Math.min(row.length, 30);
    for (var c = 0; c < cMax; c++) {
      var t = String(row[c] == null ? '' : row[c])
          .toLowerCase().replace(/[^a-z]/g, '');
      if (t.indexOf('cashavailable') !== -1) {
        var raw = (c + 1 < row.length) ? row[c + 1] : null;
        // v1.11.7 harness catch: a BLANK neighbor must not read as 0 —
        // portfolio mode would size against phantom-zero cash. Blank =>
        // keep scanning; only a non-blank finite number is a hit.
        var rawS = String(raw == null ? '' : raw)
            .replace(/,/g, '').replace(/\s/g, '');
        var num = rawS === '' ? NaN : Number(rawS);
        if (isFinite(num) && num >= 0) {
          return { found: true, value: num,
                   label: String(row[c]), row: r + 1, col: c + 1 };
        }
      }
    }
  }
  return { found: false, value: 0, label: '', row: 0, col: 0 };
}

/**
 * v1.11.7 [P-134]: read Portfolio_Decision's cash cell. Fail-open:
 * any fault returns {found:false, err} and the refresh proceeds on
 * the panel value.
 */
function dt10CashFromPortfolio_(ss) {
  try {
    var sh = ss.getSheetByName('Portfolio_Decision');
    if (!sh) return { found: false, value: 0, err: 'sheet-missing' };
    var vals = sh.getRange(1, 1,
        Math.min(15, Math.max(1, sh.getLastRow())),
        Math.min(30, Math.max(1, sh.getLastColumn()))).getValues();
    var hit = dt10CashScan_(vals);
    if (!hit.found) hit.err = 'label-not-found';
    return hit;
  } catch (eCf) {
    return { found: false, value: 0,
             err: String(eCf && eCf.message || eCf) };
  }
}

/** v1.13.4 [WP-8] PURE: exact (trimmed) label match on any cell of a values
 * block -> the cell immediately to its right. Scan capped 15x30 like
 * dt10CashScan_. Returns {found, value, row, col} (1-based). */
function dt10LabelScan_(values, label) {
  var want = String(label || '').replace(/^\s+|\s+$/g, '');
  var rMax = Math.min((values || []).length, 15);
  for (var r = 0; r < rMax; r++) {
    var row = values[r] || [];
    var cMax = Math.min(row.length, 30);
    for (var c = 0; c < cMax; c++) {
      var t = String(row[c] === null || row[c] === undefined ? '' : row[c])
          .replace(/^\s+|\s+$/g, '');
      if (want && t === want) {
        return { found: true, value: (c + 1 < row.length) ? row[c + 1] : null,
                 row: r + 1, col: c + 1 };
      }
    }
  }
  return { found: false, value: null, row: 0, col: 0 };
}
/** v1.13.4 [WP-8] I/O: ONE 15x30 read of Portfolio_Decision (the same
 * block dt10CashFromPortfolio_ reads) -> {found, values: {label: value},
 * err}. Fail-open: any fault => found:false + err; nothing is written. */
function dt10PfPanelRead_(ss, labels) {
  try {
    var sh = ss.getSheetByName('Portfolio_Decision');
    if (!sh) return { found: false, values: {}, err: 'sheet-missing' };
    var vals = sh.getRange(1, 1,
        Math.min(15, Math.max(1, sh.getLastRow())),
        Math.min(30, Math.max(1, sh.getLastColumn()))).getValues();
    var out = {}, any = false;
    for (var i = 0; i < (labels || []).length; i++) {
      var hit = dt10LabelScan_(vals, labels[i]);
      if (hit.found) { out[labels[i]] = hit.value; any = true; }
    }
    return { found: any, values: out, err: any ? '' : 'labels-not-found' };
  } catch (ePf) {
    return { found: false, values: {},
             err: String(ePf && ePf.message || ePf) };
  }
}

/**
 * v1.11.7 [P-134] PURE: choose the cash to SEND + the status note.
 * mode 'panel' => byte-identical legacy (panel value, empty note).
 */
function dt10CashChoose_(mode, panelCash, pfHit) {
  var p = Number(panelCash) || 0;
  if (mode !== 'observe' && mode !== 'portfolio') {
    return { sent: p, note: '' };
  }
  if (!pfHit || !pfHit.found) {
    return { sent: p, note: ' | cash=panel ' + Math.round(p) +
             ' (PF unreadable: ' + String((pfHit && pfHit.err) || '?') +
             ', mode=' + mode + ')' };
  }
  var f = Number(pfHit.value) || 0;
  var d = Math.round(f - p);
  if (mode === 'portfolio') {
    return { sent: f, note: ' | cash=PF ' + Math.round(f) +
             ' (panel ' + Math.round(p) +
             (d ? ', \u0394' + d : '') + ')' };
  }
  return { sent: p, note: ' | cash=panel ' + Math.round(p) +
           ' (PF ' + Math.round(f) +
           (d ? ', \u0394' + d : '') + ' observe)' };
}
/** v1.6.0 (W-3): earnings-tag kill-switch. Script Property
 * DT10_EARNINGS_TAG = 0/false/off/no/disabled => OFF. Fail-open ON. */
function dt10EarningsTagEnabled_() {
  try {
    var v = String(PropertiesService.getScriptProperties()
        .getProperty('DT10_EARNINGS_TAG') || '').toLowerCase();
    return !(v === '0' || v === 'false' || v === 'off' ||
             v === 'no' || v === 'disabled');
  } catch (eEt) {
    return true;
  }
}
/** v1.6.0 (W-3): tag horizon in days. Script Property
 * DT10_EARNINGS_TAG_DAYS, clamped 1..60, default 14. */
function dt10EarningsTagDays_() {
  var raw = '';
  try {
    raw = PropertiesService.getScriptProperties()
        .getProperty('DT10_EARNINGS_TAG_DAYS') || '';
  } catch (eEd) {}
  return dt10StabInt_(raw, 14, 1, 60);
}
/** v1.8.0 (BE-2): strict hard-exit opt-in. Script Property
 * DT10_HARD_VERDICT_STRICT = 1/true/on/yes => ON; anything else — or a
 * PropertiesService fault — => OFF, so the v1.3.0 grace doctrine stays
 * the default (fail-safe OFF, unlike the fail-open display switches). */
function dt10HardVerdictStrict_() {
  // v1.8.6 [OPERATOR-DIRECTED DEFAULT FLIP]: strict hard-exit is now the
  // DEFAULT. Evidence trail of 2026-08-09: the operator attempted to arm
  // this five separate times (property panel x4 -> raw len=0 every time;
  // then dt10ArmStrict at 13:18:14 succeeded and dt10zzDisarmStrict was
  // click-storm-run 19s later, wiping it). Five explicit arming attempts
  // in one day IS the operator's setting — the code now honors it. The
  // property inverts to an explicit DISARM switch: only a value of
  // 0 / off / false / no disables; anything else (including absence,
  // whitespace, or '1') leaves strict ON.
  // v1.8.7: the 13:26:03 click-storm disarmed even the default (property=0
  // written 16s after arming). FINAL FORM — no function in this file can
  // touch strict any more (Arm/Disarm deleted); the ONLY disarm is a value
  // no ritual could ever store: 'disarm-strict-confirm', hand-typed in the
  // property. The stranded '0' from today is deliberately INERT.
  try {
    var v = String(PropertiesService.getScriptProperties()
        .getProperty('DT10_HARD_VERDICT_STRICT') || '').trim().toLowerCase();
    return v !== 'disarm-strict-confirm';
  } catch (eHv) {
    return true;
  }
}
// Calendar_Events is a bounded facts table, not a cockpit-sized sample.
var DT10_CALENDAR_MAX_ROWS = 5000; // body rows; overflow produces no annotation
/** Calendar day as an integer UTC day. Dates represent spreadsheet instants
 * in Riyadh (UTC+3); ISO date-only strings represent literal calendar dates.
 * Reject rollover dates, timestamps in text cells, and numeric sheet serials.
 * A missing observation is never interpreted as an event occurring today. */
function dt10CalendarDay_(value) {
  var text, shifted;
  if (Object.prototype.toString.call(value) === '[object Date]') {
    if (!isFinite(value.getTime())) return null;
    shifted = new Date(value.getTime() + 3 * 60 * 60 * 1000);
    text = shifted.toISOString().slice(0, 10);
  } else if (typeof value === 'string') {
    text = value.trim();
  } else {
    return null;
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text) || text.slice(0, 4) === '0000') return null;
  var parsed = new Date(text + 'T00:00:00.000Z');
  if (!isFinite(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== text) {
    return null;
  }
  return Math.floor(parsed.getTime() / 86400000);
}
/** PURE — {SYMBOL: daysToEarnings} from dated Calendar_Events facts. The
 * consumer derives days for its current Riyadh date, so a yesterday counter
 * cannot survive midnight. Blank/invalid dates never fall back to a counter. */
function dt10EarningsMapFromValues_(values, asOfDate) {
  var map = {};
  if (!values || !values.length) return map;
  var today = dt10CalendarDay_(asOfDate === undefined ? new Date() : asOfDate);
  if (today === null) return map;
  var iSym = -1, iDate = -1, r, c, cells, tok, rowSym, rowDate;
  for (r = 0; r < values.length && iSym < 0; r++) {
    cells = values[r] || [];
    rowSym = -1; rowDate = -1;
    for (c = 0; c < cells.length; c++) {
      tok = dt10NormToken_(cells[c]);
      if (tok === 'symbol') rowSym = c;
      else if (tok === 'nextearningsdate' || tok === 'earningsdate') rowDate = c;
    }
    if (rowSym >= 0 && rowDate >= 0) { iSym = rowSym; iDate = rowDate; }
  }
  if (iSym < 0 || iDate < 0) return map;
  for (; r < values.length; r++) {
    cells = values[r] || [];
    var sym = dt10NormSym_(cells[iSym]);
    if (!sym || sym === 'SYMBOL' || map.hasOwnProperty(sym)) continue;
    var eventDay = dt10CalendarDay_(cells[iDate]);
    if (eventDay !== null && eventDay >= today) map[sym] = eventDay - today;
  }
  return map;
}
/** v1.6.0 (W-3): live Calendar_Events read. ANY failure => empty map =>
 * zero tags — the annotation must never be able to break the render. */
function dt10EarningsMap_(ss, asOfDate) {
  try {
    var sh = ss.getSheetByName('Calendar_Events');
    if (!sh) return {};
    var rows = sh.getLastRow(), cols = Math.min(sh.getLastColumn(), 7);
    if (!isFinite(rows) || !isFinite(cols) || rows < 2 || cols < 2 ||
        rows > DT10_CALENDAR_MAX_ROWS + 1) return {};
    return dt10EarningsMapFromValues_(
        sh.getRange(1, 1, rows, cols).getValues(), asOfDate);
  } catch (eEm) {
    return {};
  }
}
/** v1.6.0 (W-3): PURE core — prefixes '⚠ earnings ≤Nd · ' onto
 * advisor_note for tickets within the horizon. Idempotent (a note already
 * carrying the prefix is never doubled). Returns the tag count. */
function dt10ApplyEarningsTags_(tickets, map, maxDays) {
  var n = 0;
  if (!tickets || !tickets.length || !map) return n;
  for (var i = 0; i < tickets.length; i++) {
    var t = tickets[i];
    if (!t) continue;
    var sym = dt10NormSym_(t.symbol);
    if (!sym || !map.hasOwnProperty(sym)) continue;
    var d = map[sym];
    if (!(d >= 0 && d <= maxDays)) continue;
    var note = String(t.advisor_note || '');
    if (note.indexOf('\u26a0 earnings') === 0) continue;
    t.advisor_note = '\u26a0 earnings \u2264' + d + 'd \u00b7 ' + note;
    n++;
  }
  return n;
}
/** v1.6.0 (W-3): orchestrator — gathers switch + horizon + map, tags the
 * SELECTED board in place, returns {note} for the status banner. Guarded
 * end-to-end: on any surprise the render proceeds untagged. */
function dt10EarningsAnnotate_(payload, ss) {
  try {
    if (!dt10EarningsTagEnabled_()) return { note: '' };
    var days = dt10EarningsTagDays_();
    var map = dt10EarningsMap_(ss);
    var sel = (payload && payload.selected) || [];
    var n = dt10ApplyEarningsTags_(sel, map, days);
    Logger.log('[DT10 v' + DT10_VERSION + '] earnings tags: ' + n + '/' +
               sel.length + ' tickets (\u2264' + days + 'd; calendar rows ' +
               (function (m) { var k = 0, q; for (q in m) {
                 if (m.hasOwnProperty(q)) k++; } return k; })(map) + ')');
    // v1.13.4 [WP-8]: 'map' is additive (dt10CalCoverageCore_ reads it;
    // every earn.note consumer is unchanged; the catch path keeps {note}).
    return { note: n ? 'earn \u26a0' + n + '/' + sel.length : '', map: map };
  } catch (eEa) {
    return { note: '' };
  }
}
/** v1.18.0 [POST-EARNINGS]: Script Property that arms the post-earnings
 * advisor tag (observe | on; anything else = off), the quarantine lengths of
 * the interim trading policy (rule 3) - sessions after any report and after
 * a report with a negative EPS surprise - and how far back a report can
 * matter at all (calendar days; no quarantine reaches further). */
var DT10_POST_EARN_PROP = 'DT10_POST_EARN';
var DT10_POST_EARN_SESSIONS = 5;
var DT10_POST_EARN_SESSIONS_NEG = 10;
var DT10_POST_EARN_MAX_DAYS = 31;
/** v1.18.0 [POST-EARNINGS]: 'observe' when DT10_POST_EARN is observe or on
 * (any case, surrounding blanks ignored); else 'off'. Never throws. */
function dt10PostEarnMode_() {
  var raw = '';
  try {
    raw = String(PropertiesService.getScriptProperties()
        .getProperty(DT10_POST_EARN_PROP) || '');
  } catch (ePe) {
    raw = '';
  }
  raw = raw.replace(/^\s+|\s+$/g, '').toLowerCase();
  return (raw === 'observe' || raw === 'on') ? 'observe' : 'off';
}
/** v1.18.0 [POST-EARNINGS] PURE: trading sessions after `fromDay` up to and
 * including `toDay` (integer UTC days from dt10CalendarDay_): Mon-Fri, or
 * Sun-Thu when `sunThu` is true (Tadawul). Exchange holidays are not known
 * here. The walk is bounded to the last 366 days (callers pass at most
 * DT10_POST_EARN_MAX_DAYS). */
function dt10WeekdaysBetween_(fromDay, toDay, sunThu) {
  var n = 0;
  if (typeof fromDay !== 'number' || typeof toDay !== 'number') return n;
  if (toDay - fromDay > 366) fromDay = toDay - 366;
  for (var d = fromDay + 1; d <= toDay; d++) {
    var dow = ((d % 7) + 11) % 7;   // day 0 (1970-01-01) was a Thursday; 0 = Sunday
    if (sunThu === true ? dow <= 4 : (dow >= 1 && dow <= 5)) n++;
  }
  return n;
}
/** v1.18.0 [POST-EARNINGS] PURE: {SYMBOL: {date, sessions, surprise}} from a
 * Calendar_Events grid as of `asOfDate` (a Riyadh calendar day; default
 * now). The header row is the first one carrying 'Symbol' and a 'Last
 * Earnings Date' or 'Next Earnings Date' column ('EPS Surprise %'
 * optional). The report is the latest PAST date of the two: a Next Earnings
 * Date already behind today is a report the calendar has not caught up with
 * yet (the morning render runs before the calendar sync), its surprise
 * unknown. Reports older than DT10_POST_EARN_MAX_DAYS are left out.
 * surprise = the cell's number (text accepted with '%' / ',' stripped,
 * clamped to +-999999), or null. KSA symbols count Sun-Thu sessions. */
function dt10PostEarnMapFromValues_(values, asOfDate) {
  var map = {};
  if (!values || !values.length) return map;
  var today = dt10CalendarDay_(asOfDate === undefined ? new Date() : asOfDate);
  if (today === null) return map;
  var iSym = -1, iLast = -1, iNext = -1, iSur = -1;
  var r, c, cells, tok, rs, rl, rn, rp;
  for (r = 0; r < values.length && iSym < 0; r++) {
    cells = values[r] || [];
    rs = -1; rl = -1; rn = -1; rp = -1;
    for (c = 0; c < cells.length; c++) {
      tok = dt10NormToken_(cells[c]);
      if (tok === 'symbol') rs = c;
      else if (tok === 'lastearningsdate') rl = c;
      else if (tok === 'nextearningsdate' || tok === 'earningsdate') rn = c;
      else if (tok === 'epssurprise') rp = c;
    }
    if (rs >= 0 && (rl >= 0 || rn >= 0)) {
      iSym = rs; iLast = rl; iNext = rn; iSur = rp;
    }
  }
  if (iSym < 0) return map;
  for (; r < values.length; r++) {
    cells = values[r] || [];
    var sym = dt10NormSym_(cells[iSym]);
    if (!sym || sym === 'SYMBOL' ||
        Object.prototype.hasOwnProperty.call(map, sym)) continue;
    var last = iLast >= 0 ? dt10CalendarDay_(cells[iLast]) : null;
    var next = iNext >= 0 ? dt10CalendarDay_(cells[iNext]) : null;
    if (last !== null && last >= today) last = null;
    if (next !== null && next >= today) next = null;
    var day = last, fromNext = false;
    if (next !== null && (day === null || next > day)) {
      day = next;
      fromNext = true;
    }
    if (day === null || today - day > DT10_POST_EARN_MAX_DAYS) continue;
    var sur = null;
    if (!fromNext && iSur >= 0) {
      var raw = cells[iSur];
      if (typeof raw === 'string') raw = raw.replace(/[,%\s]/g, '');
      if (typeof raw === 'number' && isFinite(raw)) {
        sur = raw;
      } else if (typeof raw === 'string' && /^[+-]?\d+(\.\d+)?$/.test(raw)) {
        sur = parseFloat(raw);
      }
      if (sur !== null && Math.abs(sur) > 999999) sur = sur > 0 ? 999999 : -999999;
    }
    map[sym] = { date: new Date(day * 86400000).toISOString().slice(0, 10),
                 sessions: dt10WeekdaysBetween_(day, today,
                     /\.SR$/.test(sym) || /^\d{4}$/.test(sym)),
                 surprise: sur };
  }
  return map;
}
/** v1.18.0 [POST-EARNINGS]: live Calendar_Events read (A1 to the last
 * column, at most 16). ANY failure => empty map => zero tags. */
function dt10PostEarnMap_(ss, asOfDate) {
  try {
    var sh = ss.getSheetByName('Calendar_Events');
    if (!sh) return {};
    var rows = sh.getLastRow(), cols = Math.min(sh.getLastColumn(), 16);
    if (!isFinite(rows) || !isFinite(cols) || rows < 2 || cols < 2 ||
        rows > DT10_CALENDAR_MAX_ROWS + 1) return {};
    return dt10PostEarnMapFromValues_(
        sh.getRange(1, 1, rows, cols).getValues(), asOfDate);
  } catch (ePm) {
    return {};
  }
}
/** v1.18.0 [POST-EARNINGS] PURE: puts
 * '\u26a0 post-earnings S/L \u00b7 EPS x% \u00b7 ' on advisor_note for the
 * tickets inside their quarantine (S sessions since the report, L = 5, or
 * 10 after a negative EPS surprise) - after a leading v1.6.0 forward tag
 * when there is one (that tagger checks for its own prefix at position 0),
 * else at the start. Idempotent: a note already carrying the tag is never
 * tagged again. Returns the tag count. */
function dt10ApplyPostEarnTags_(tickets, map) {
  var n = 0;
  if (!tickets || !tickets.length || !map) return n;
  for (var i = 0; i < tickets.length; i++) {
    var t = tickets[i];
    if (!t) continue;
    var sym = dt10NormSym_(t.symbol);
    if (!sym || !Object.prototype.hasOwnProperty.call(map, sym)) continue;
    var e = map[sym];
    var lim = (e.surprise !== null && e.surprise < 0) ?
        DT10_POST_EARN_SESSIONS_NEG : DT10_POST_EARN_SESSIONS;
    if (!(e.sessions >= 0 && e.sessions <= lim)) continue;
    var note = String(t.advisor_note || '');
    if (note.indexOf('\u26a0 post-earnings ') >= 0) continue;
    var eps = e.surprise === null ? 'n/a' :
        (e.surprise > 0 ? '+' : '') + e.surprise.toFixed(1) + '%';
    var tag = '\u26a0 post-earnings ' + e.sessions + '/' + lim +
        ' \u00b7 EPS ' + eps + ' \u00b7 ';
    var fwd = note.match(/^\u26a0 earnings \u2264\d+d \u00b7 /);
    t.advisor_note = fwd ? fwd[0] + tag + note.slice(fwd[0].length) : tag + note;
    n++;
  }
  return n;
}
/** v1.18.0 [POST-EARNINGS]: orchestrator - '' when DT10_POST_EARN is off
 * (no sheet read, nothing touched); else tags the SELECTED board in place
 * and returns the status token 'post-earn \u26a0N/M' ('post-earn 0/M' when
 * no seat is inside a quarantine, 'post-earn n/a' when the calendar holds no
 * recent report or the read failed). Guarded end to end. */
function dt10PostEarnAnnotate_(payload, ss) {
  if (dt10PostEarnMode_() === 'off') return '';
  try {
    var map = dt10PostEarnMap_(ss);
    var sel = (payload && payload.selected) || [];
    var n = dt10ApplyPostEarnTags_(sel, map);
    var k = 0, q;
    for (q in map) {
      if (Object.prototype.hasOwnProperty.call(map, q)) k++;
    }
    Logger.log('[DT10 v' + DT10_VERSION + '] post-earnings tags: ' + n + '/' +
               sel.length + ' tickets (recent reports ' + k + '; quarantine ' +
               DT10_POST_EARN_SESSIONS + '/' + DT10_POST_EARN_SESSIONS_NEG +
               ' sessions)');
    if (!k) return 'post-earn n/a';
    return 'post-earn ' + (n ? '\u26a0' : '') + n + '/' + sel.length;
  } catch (ePa) {
    return 'post-earn n/a';
  }
}
function dt10WritePageStatus_(status, message, httpCode, rows, durationMs,
                              warnings) {
  try {
    if (!dt10PageStatusEnabled_()) return;
    if (typeof writePageStatus_ !== 'function') return;
    writePageStatus_(DT10_SHEET, {
      status: String(status || ''),
      message: String(message || ''),
      endpoint: DT10_ENDPOINT,
      httpCode: (httpCode === 0 || httpCode) ? String(httpCode) : '',
      rows: (rows === 0 || rows) ? String(rows) : '',
      columns: '',
      durationMs: (durationMs === 0 || durationMs) ? String(durationMs) : '',
      warnings: warnings || []
    });
  } catch (eStatus) {
    try {
      Logger.log('[DT10 v' + DT10_VERSION + '] _Status upsert failed: ' +
                 eStatus);
    } catch (eLog) {}
  }
}
// ---------------------------------------------------------------------------
// Config / token / FX / defaults readers
// ---------------------------------------------------------------------------
function dt10BackendUrl_() {
  try {
    if (typeof getBackendUrl_ === 'function') {
      var u = getBackendUrl_();
      if (u) return String(u).replace(/\/+$/, '');
    }
  } catch (e1) {}
  try {
    var props = PropertiesService.getScriptProperties();
    var keys = ['BACKEND_URL', 'TFB_BACKEND_URL', 'BACKEND_BASE_URL'];
    for (var i = 0; i < keys.length; i++) {
      var v = props.getProperty(keys[i]);
      if (v) return String(v).replace(/\/+$/, '');
    }
  } catch (e2) {}
  return DT10_DEFAULT_BACKEND;
}
function dt10AppToken_() {
  try {
    if (typeof getAppToken_ === 'function') {
      var t = getAppToken_();
      if (t) return String(t);
    }
  } catch (e1) {}
  try {
    var props = PropertiesService.getScriptProperties();
    var keys = ['APP_TOKEN', 'TFB_APP_TOKEN', 'X_APP_TOKEN'];
    for (var i = 0; i < keys.length; i++) {
      var v = props.getProperty(keys[i]);
      if (v) return String(v);
    }
  } catch (e2) {}
  return '';
}
/** TFB_DESIGN_TOKENS → {TOKEN: {fg,bg}}; falls back to §3.4 constants. */
function dt10Tokens_(ss) {
  var out = {};
  for (var k in DT10_FALLBACK_TOKENS) {
    if (DT10_FALLBACK_TOKENS.hasOwnProperty(k)) {
      out[k] = { fg: DT10_FALLBACK_TOKENS[k].fg,
                 bg: DT10_FALLBACK_TOKENS[k].bg };
    }
  }
  try {
    var rng = ss.getRangeByName('TFB_DESIGN_TOKENS');
    if (rng) {
      var vals = rng.getValues();
      for (var i = 0; i < vals.length; i++) {
        var name = String(vals[i][0] || '').trim();
        if (!name) continue;
        out[name] = { fg: String(vals[i][1] || '') || (out[name] || {}).fg,
                      bg: String(vals[i][2] || '') || (out[name] || {}).bg };
      }
    }
  } catch (e) {}
  return out;
}
/** TFB_FX_LOOKUP (CCY|Live|Static|Effective) → {CCY: effective_rate}. */
function dt10FxRates_(ss) {
  var out = {};
  try {
    var rng = ss.getRangeByName('TFB_FX_LOOKUP');
    if (!rng) return out;
    var vals = rng.getValues();
    for (var i = 0; i < vals.length; i++) {
      var ccy = String(vals[i][0] || '').trim();
      var eff = vals[i][3];
      if (ccy && typeof eff === 'number' && isFinite(eff) && eff > 0) {
        out[ccy] = eff;
      }
    }
  } catch (e) {}
  return out;
}
/** TFB_PANEL_DEFAULTS (Setting|Default) → {label: default}. */
function dt10PanelDefaults_(ss) {
  var out = {};
  try {
    var rng = ss.getRangeByName('TFB_PANEL_DEFAULTS');
    if (!rng) return out;
    var vals = rng.getValues();
    for (var i = 0; i < vals.length; i++) {
      var label = String(vals[i][0] || '').trim();
      if (label) out[label] = vals[i][1];
    }
  } catch (e) {}
  return out;
}
// ---------------------------------------------------------------------------
// Panel read/write
// ---------------------------------------------------------------------------
/** (row, labelCol, valueCol) for panel item index i in the 3-per-row grid. */
function dt10PanelPos_(i) {
  var row = DT10_ROW_PANEL_FIRST + Math.floor(i / 3);
  var slot = i % 3;                      // 0→A/B, 1→D/E, 2→G/H
  var labelCol = 1 + slot * 3;
  return { row: row, labelCol: labelCol, valueCol: labelCol + 1 };
}
/** Read the panel into {label: value}; blanks resolved via defaults chain. */
/** v1.11.3 [P-76]: snapshot of the panel's NON-BLANK operator values,
 *  read raw (no defaults) BEFORE a layout rebuild clears the sheet. A sheet
 *  without a panel yet returns {}. Never throws. */
function dt10PanelPreserve_(sheet) {
  var out = {};
  if (!DT10_V1113_LAYOUT_PRESERVE) return out;
  try {
    if (!sheet || sheet.getLastRow() < DT10_ROW_PANEL_FIRST) return out;
    for (var i = 0; i < DT10_PANEL.length; i++) {
      var item = DT10_PANEL[i];
      var pos = dt10PanelPos_(i);
      var lbl = sheet.getRange(pos.row, pos.labelCol).getValue();
      if (String(lbl || '') !== item.label) continue;   // layout drifted
      var v = sheet.getRange(pos.row, pos.valueCol).getValue();
      if (dt10HasValue_(v)) out[item.label] = v;
    }
  } catch (e) {}
  return out;
}
/** v1.11.3 [P-76] PURE: the value a rebuilt panel cell receives —
 *  preserved operator input > _Lists_Config default > built-in default. */
function dt10PanelSeedValue_(item, defaults, preserved) {
  if (DT10_V1113_LAYOUT_PRESERVE && preserved &&
      preserved.hasOwnProperty(item.label) &&
      dt10HasValue_(preserved[item.label])) {
    return preserved[item.label];
  }
  return defaults && defaults.hasOwnProperty(item.label) &&
         dt10HasValue_(defaults[item.label]) ? defaults[item.label]
                                             : item.def;
}
function dt10ReadPanel_(sheet, ss) {
  var defaults = dt10PanelDefaults_(ss);
  var out = {};
  for (var i = 0; i < DT10_PANEL.length; i++) {
    var item = DT10_PANEL[i];
    var pos = dt10PanelPos_(i);
    var v = sheet.getRange(pos.row, pos.valueCol).getValue();
    if (!dt10HasValue_(v)) {
      v = defaults.hasOwnProperty(item.label) &&
          dt10HasValue_(defaults[item.label]) ? defaults[item.label]
                                              : item.def;
    }
    out[item.label] = v;
  }
  return out;
}
/** Criteria object for the POST body: the "T10: *" labels verbatim. */
function dt10CriteriaFromPanel_(panel) {
  var crit = {};
  for (var i = 0; i < DT10_PANEL.length; i++) {
    var label = DT10_PANEL[i].label;
    if (label.indexOf('T10: ') === 0) crit[label] = panel[label];
  }
  // v1.2.7: the Require-Investable switch must reach the builder as its REAL
  // criteria key (investability_gate_enabled) carrying a TRUE BOOLEAN — the
  // route has no alias for this label (it snake-cases to require_investable,
  // which the builder ignores), and a raw 'No' STRING under the real key
  // would be truthy in the builder's criteria.get(...) check.
  // v1.6.9: blank-cell fallback flipped 'Yes' -> 'No' — a missing/unbuilt
  // cell no longer arms the retired gate (see the v1.6.9 header WHY).
  // Logic unchanged: only a leading 'n'/'N' => false, so an operator's
  // explicit Yes still arms it and the choice lands in the panel snapshot.
  // §8 honored: this is key MAPPING, not gating — the backend's existing
  // Investability MAJOR gate (opportunity_builder v1.0.7+) makes the call.
  var reqInvRaw = panel['T10: Require Investable'];
  var reqInv = String(reqInvRaw === null || reqInvRaw === undefined ?
      'No' : reqInvRaw).replace(/^\s+|\s+$/g, '');
  crit['investability_gate_enabled'] =
      !(reqInv.charAt(0) === 'n' || reqInv.charAt(0) === 'N');
  // v1.3.0: emit the REAL stability knob mapping (same shape consumed by
  // top10_selector v4.21.0 and a future opportunity_builder port). Today the
  // opportunity route ignores it — harmless — but the criteria snapshot
  // documents the knobs, and backend consolidation needs zero GAS change.
  crit['stability'] = dt10StabKnobs_(panel);
  return crit;
}
// ---------------------------------------------------------------------------
// Pool collection (§5 amendment: sheets are the primary pool source)
// ---------------------------------------------------------------------------
/**
 * v1.2.0 FAIR MULTI-SHEET POOLING (fixes the v1.1.0 single-sheet-drain bug).
 * Phase 1 collects EVERY valid row from EACH page independently (no shared cap
 * during collection), so a large Market_Leaders can never exhaust the budget
 * before the other pages are read. Phase 2 then: if the four-sheet total fits
 * within the cap, sends all available input; otherwise round-robins across
 * the pages so each is fairly represented. Returns per-page included AND
 * available counts plus total/truncated for an honest status line.
 */
function dt10CollectPoolRows_(ss, limit, sourceContext) {
  // v1.2.3: a non-positive / blank limit means "scan the ENTIRE available
  // universe" (bounded only by DT10_POOL_HARD_CAP); a positive number caps
  // deliberately. This is what makes a blank Pool Limit cell == scan-all.
  var lim = (typeof limit === 'number' && isFinite(limit) && limit > 0) ?
      limit : DT10_POOL_HARD_CAP;
  var cap = Math.max(1, Math.min(lim, DT10_POOL_HARD_CAP));
  // Phase 1 — collect every valid row from every page, independently.
  // v1.2.2: a GLOBAL seen-set de-duplicates by symbol across all pages, so the
  // backend never receives the same stock twice (source sheets repeat symbols).
  var perPageRows = {};
  var available = {};
  var order = [];
  var seen = {};
  var duplicatesSkipped = 0;
  var scopeReceipts = {};
  var physicalObserved = {}, excluded = [];
  for (var p = 0; p < DT10_POOL_PAGES.length; p++) {
    var pageName = DT10_POOL_PAGES[p];
    var scope = DT10_SOURCE_APPROVED_SCOPE.pages[pageName];
    physicalObserved[pageName] = {ready: false, reason: 'physical source grid unavailable'};
    if (scope) scopeReceipts[pageName] = {ready: false, reason: 'physical pool scope unavailable'};
    perPageRows[pageName] = [];
    available[pageName] = 0;
    var sheet = ss.getSheetByName(pageName);
    if (!sheet) continue;
    var values;
    try {
      values = sheet.getDataRange().getValues();
    } catch (e) {
      continue;
    }
    if (!values || !values.length) continue;
    var hIdx = dt10FindHeaderRow_(values, DT10_HEADER_SCAN_ROWS);
    if (hIdx < 0) continue;
    try {
        var symbolColumn = -1;
        for (var hc = 0; hc < values[hIdx].length; hc++) {
          if (typeof values[hIdx][hc] === 'string' && values[hIdx][hc].trim().toUpperCase() === 'SYMBOL') {
            if (symbolColumn !== -1) throw new Error('ambiguous pool Symbol header');
            symbolColumn = hc;
          }
        }
        if (symbolColumn === -1) throw new Error('pool Symbol header missing');
        var physicalSymbols = [];
        for (var pr = hIdx + 1; pr < values.length; pr++) physicalSymbols.push(values[pr][symbolColumn]);
        var observed = dt10SourceScopeCore_(physicalSymbols, scope || {
          approved_count: physicalSymbols.length, symbol_set_sha256: null,
          supersedes_min_rows: DT10_SOURCE_MIN_ROWS[pageName], binding_only: true
        }, DT10_SOURCE_APPROVED_SCOPE);
        physicalObserved[pageName] = observed;
        if (scope) scopeReceipts[pageName] = observed;
    } catch (ePoolScope) {
      physicalObserved[pageName] = {ready: false, reason: String(ePoolScope && ePoolScope.message || ePoolScope)};
      if (scope) scopeReceipts[pageName] = physicalObserved[pageName];
    }
    var colMap = dt10MapHeaderCols_(values[hIdx]);
    for (var r = hIdx + 1; r < values.length; r++) {
      var obj = dt10PoolRowFromSheetRow_(values[r], colMap, pageName);
      if (!obj) continue;
      var key = dt10NormSym_(obj['Symbol']);
      if (key && seen[key]) {
        duplicatesSkipped++;
        excluded.push({symbol: key, page: pageName, owner: seen[key]});
        continue;
      }
      if (key) seen[key] = pageName;
      perPageRows[pageName].push(obj);
    }
    available[pageName] = perPageRows[pageName].length;
    if (available[pageName] > 0) order.push(pageName);
  }
  var total = 0;
  for (var t = 0; t < DT10_POOL_PAGES.length; t++) {
    total += available[DT10_POOL_PAGES[t]];
  }
  // Phase 2 — assemble up to cap.
  var rows = [];
  var perPage = {};
  for (var z = 0; z < DT10_POOL_PAGES.length; z++) {
    perPage[DT10_POOL_PAGES[z]] = 0;
  }
  if (total <= cap) {
    // Everything fits — send the full multi-sheet universe.
    for (var o = 0; o < order.length; o++) {
      var pg0 = order[o];
      var list0 = perPageRows[pg0];
      for (var k0 = 0; k0 < list0.length; k0++) {
        rows.push(list0[k0]);
        perPage[pg0]++;
      }
    }
  } else {
    // Over cap — round-robin so each page is fairly represented.
    var idx = {};
    for (var i2 = 0; i2 < order.length; i2++) idx[order[i2]] = 0;
    var progressed = true;
    while (rows.length < cap && progressed) {
      progressed = false;
      for (var q = 0; q < order.length && rows.length < cap; q++) {
        var pg = order[q];
        if (idx[pg] < perPageRows[pg].length) {
          rows.push(perPageRows[pg][idx[pg]]);
          idx[pg]++;
          perPage[pg]++;
          progressed = true;
        }
      }
    }
  }
  return { rows: rows, perPage: perPage, available: available,
           cap: cap, total: total, truncated: (total > cap),
           duplicatesSkipped: duplicatesSkipped, approved_scope: scopeReceipts,
           pool_reconciliation: dt10PoolFrameBuild_(physicalObserved, available, perPage, excluded, sourceContext) };
}
// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------
function dt10Post_(body) {
  var url = dt10BackendUrl_() + DT10_ENDPOINT;
  var options = {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(body),
    muteHttpExceptions: true,
    followRedirects: true,
    headers: {}
  };
  var token = dt10AppToken_();
  if (token) options.headers['X-APP-TOKEN'] = token;
  var resp = UrlFetchApp.fetch(url, options);
  var code = resp.getResponseCode();
  var text = resp.getContentText() || '';
  var json = null;
  try {
    json = JSON.parse(text);
  } catch (e) {}
  return { code: code, json: json, text: text, url: url };
}
// ---------------------------------------------------------------------------
// v1.3.0 — stability orchestration (the only stability code touching Apps
// Script services: ScriptProperties + UTC date). Core stays pure above.
// ---------------------------------------------------------------------------
/** v1.6.6 (S-3) PURE: ES5 membership test (no indexOf-on-array reliance). */
function dt10InList_(list, sym) {
  for (var i = 0; i < (list ? list.length : 0); i++) {
    if (list[i] === sym) return true;
  }
  return false;
}
/** v1.6.6 (S-3) PURE: join up to n symbols, '+K' the remainder. */
function dt10JoinCap_(list, n) {
  list = list || [];
  if (list.length <= n) return list.join('/');
  return list.slice(0, n).join('/') + '+' + (list.length - n);
}
function dt10StabToday_() {
  return Utilities.formatDate(new Date(), 'Etc/UTC', 'yyyy-MM-dd');
}
function dt10StabLoad_() {
  return dt10StabLoadEx_().state;
}
/**
 * v1.6.6 (S-1): load WITH telemetry. Returns { state, note } where note is
 * '' on a healthy load and otherwise names the failure class so the status
 * line — not just Logger — carries it:
 *   STATE-READ-FAIL(...)  ScriptProperties.getProperty threw;
 *   STATE-CORRUPT         blob present but unparsable (bootstrapped);
 *   STATE-EMPTY(bootstrap after prior success)  blob missing although a
 *       prior successful render is on record — the 5110.SR class.
 * A genuinely first-ever run (no prior success recorded) stays silent.
 */
function dt10StabLoadEx_() {
  var raw = '';
  var note = '';
  try {
    raw = PropertiesService.getScriptProperties()
        .getProperty(DT10_STAB_PROP) || '';
  } catch (e) {
    note = 'STATE-READ-FAIL(' + String(e).slice(0, 40) + ')';
    return { state: dt10StabParseState_(''), note: note };
  }
  var st = dt10StabParseState_(raw);
  if (raw) {
    var hasSyms = false;
    for (var k in st.symbols) {
      if (st.symbols.hasOwnProperty(k)) { hasSyms = true; break; }
    }
    if (!hasSyms && raw.length > 2) note = 'STATE-CORRUPT';
  } else {
    var prior = '';
    try {
      prior = String(PropertiesService.getScriptProperties()
          .getProperty('DT10_LAST_SUCCESS') || '');
    } catch (e2) {}
    if (prior) note = 'STATE-EMPTY(bootstrap after prior success)';
  }
  return { state: st, note: note };
}
/** Persist the blob under the 9KB property limit: drop oldest-seen
 *  non-members until it fits (members are never dropped). */
function dt10StabSave_(state) {
  var blob = JSON.stringify(state);
  if (blob.length > DT10_STAB_PROP_SOFT_MAX) {
    var order = [];
    for (var k in state.symbols) {
      if (state.symbols.hasOwnProperty(k) && !state.symbols[k].member) {
        order.push(k);
      }
    }
    order.sort(function (a, b) {
      var la = state.symbols[a].ls || '', lb = state.symbols[b].ls || '';
      return la < lb ? -1 : (la > lb ? 1 : (a < b ? -1 : 1));
    });
    while (blob.length > DT10_STAB_PROP_SOFT_MAX && order.length) {
      delete state.symbols[order.shift()];
      blob = JSON.stringify(state);
    }
  }
  try {
    PropertiesService.getScriptProperties().setProperty(DT10_STAB_PROP, blob);
    // v1.6.6 (S-1): verify the write — a silent save failure is how a
    // member vanishes with no exit token. Read-back must match length.
    var back = '';
    try {
      back = String(PropertiesService.getScriptProperties()
          .getProperty(DT10_STAB_PROP) || '');
    } catch (eV) { back = ''; }
    if (back.length !== blob.length) {
      Logger.log('[DT10 v' + DT10_VERSION + '] stability state verify ' +
                 'mismatch: wrote ' + blob.length + ' read ' + back.length);
      return 'STATE-SAVE-FAIL(verify ' + blob.length + '/' + back.length + ')';
    }
    return '';
  } catch (e2) {
    Logger.log('[DT10 v' + DT10_VERSION + '] stability state save failed: ' +
               e2);
    return 'STATE-SAVE-FAIL(' + String(e2).slice(0, 40) + ')';
  }
}
/**
 * Apply the stability layer to a live payload (mutates payload.selected).
 * Panel switch 'T10: Stability Enabled' = No bypasses (state frozen, not
 * cleared) so the board mirrors the raw backend list exactly.
 */
function dt10ApplyStability_(payload, panel, outage) {
  var knobs = dt10StabKnobs_(panel);
  if (!knobs.enabled) {
    payload._dt10_stability_disabled = true;
    return { note: 'stab: OFF (panel switch)' };
  }
  // v1.8.0 (BE-2): the service read lives HERE so dt10StabCore_ stays
  // pure/node-testable; the core consumes the choice as a plain knob.
  knobs.hard_strict = dt10HardVerdictStrict_();
  // v1.14.0 [CONFIRM-BASIS]: same pattern -- one service read here, the
  // core consumes the basis as a plain knob ('qualified' default | 'raw').
  knobs.confirm_basis = dt10StabConfirmBasis_();
  // v1.15.0 [PENDING-GRACE]: same pattern -- one service read here; the knob
  // is set ONLY when armed (> 0) so an unarmed run publishes the v1.14.0
  // knobs object byte for byte.
  var pgRead = dt10StabPendingGrace_();
  if (pgRead > 0) knobs.pending_grace = pgRead;
  // v1.19.0 [QUALIFIED-HOLD]: same pattern -- one service read here; the knob
  // is set ONLY when armed so an unarmed run publishes the v1.18.0 knobs
  // object byte for byte.
  var qhRead = dt10StabQualifiedHoldMode_();
  if (qhRead !== 'off') knobs.qualified_hold = qhRead;
  // v1.11.10 [P-168]: the outage map (from the pool rows) rides as a plain
  // 7th core argument; null under the kill property or when the pool was
  // not sheet-sourced this run => v1.11.9-identical clocks.
  var outageMap = (outage && typeof outage === 'object' &&
                   !dt10OutagePauseLegacy_()) ? outage : null;
  var limit = parseInt(panel['T10: Max Selected'], 10);
  if (!(limit > 0)) limit = 10;
  var loaded = dt10StabLoadEx_();
  var state = loaded.state;
  // v1.11.11 [P-144]: the day-key is the UTC date of the latest
  // Global_Markets feed stamp (no fresh epoch => clocks frozen), read
  // here so the core stays pure; FAIL-OPEN to the wall-clock key.
  var todayKey = dt10StabToday_();
  var epoch = { key: todayKey, src: 'clock', held: false };
  if (!dt10EpochKeyLegacy_()) {
    try {
      epoch = dt10StabResolveEpoch_(
          dt10UvReadKeyed_(SpreadsheetApp.getActiveSpreadsheet(),
                           DT10_P144_FEED_KEY),
          state.date, todayKey);
    } catch (eEp) {
      epoch = { key: todayKey, src: 'clock', held: false };
    }
  }
  var out = dt10StabCore_(payload.selected || [],
                          payload.candidates_rows || [],
                          state, knobs, limit, epoch.key, outageMap);
  if (!dt10EpochKeyLegacy_()) {
    out.note += ' | epoch=' + epoch.key + '/' + epoch.src +
                (epoch.held ? '(frozen)' : '');
  }
  var saveNote = dt10StabSave_(out.state);
  if (loaded.note) out.note += ' | ' + loaded.note;
  if (saveNote) out.note += ' | ' + saveNote;
  // v1.8.0 (BE-2): make the armed mode visible on every banner.
  if (knobs.hard_strict === true && out.note.indexOf('stab: ') === 0) {
    out.note = 'stab[strict]: ' + out.note.slice(6);
  }
  payload.selected = out.tickets;
  if (payload.meta && typeof payload.meta === 'object') {
    payload.meta.stability = { knobs: knobs, audit: out.audit };
  }
  Logger.log('[DT10 v' + DT10_VERSION + '] ' + out.note);
  return out;
}
/** Editor-run: wipe the stability memory (next refresh re-bootstraps). */
function dt10StabilityReset() {
  // v1.8.7 TWO-STEP CONFIRM: this ran six times today inside function-bar
  // ritual passes, wiping grace clocks each time. A single click now only
  // ARMS a 2-minute confirmation window; the state clears ONLY on a second
  // deliberate run inside that window. A top-to-bottom ritual pass can
  // never trip it.
  var props = PropertiesService.getScriptProperties();
  var now = Date.now();
  var pend = 0;
  try { pend = Number(props.getProperty('DT10_RESET_PENDING') || 0); } catch (e0) {}
  if (pend && (now - pend) < 120000) {
    try { props.deleteProperty('DT10_RESET_PENDING'); } catch (e1) {}
    try { props.deleteProperty(DT10_STAB_PROP); } catch (e) {}
    var msg = '[DT10 v' + DT10_VERSION + '] stability state cleared — ' +
        'next refresh fast-tracks a fresh board (day 1).';
    Logger.log(msg);
    try { SpreadsheetApp.getActiveSpreadsheet().toast(msg, 'TFB', 8); } catch (e2) {}
    return msg;
  }
  try { props.setProperty('DT10_RESET_PENDING', String(now)); } catch (e3) {}
  var ask = '[DT10 v' + DT10_VERSION + '] reset NOT executed — run ' +
      'dt10StabilityReset AGAIN within 2 minutes to confirm clearing the board state.';
  Logger.log(ask);
  try { SpreadsheetApp.getActiveSpreadsheet().toast(ask, 'TFB', 8); } catch (e4) {}
  return ask;
}
// ---------------------------------------------------------------------------
// Selection audit log (v1.3.1) — cockpit-owned _Selection_Log appender
// ---------------------------------------------------------------------------
/** Pure: stable membership signature — sorted symbols joined by '|'. */
function dt10SelLogSignature_(tickets, outputState) {
  /* v1.8.9: seat-truth signature — see header WHY. Tokens change when
   * funding / executability / stability class change; they do NOT change
   * on price, score or rank-order wiggles alone.
   * v1.11.2 [P-62]: optional outputState (dt10OutputStatus_) is one more
   * token, so an authorization change (HELD -> WITHHELD -> EXECUTABLE)
   * is logged even when every seat token is unchanged. */
  tickets = tickets || [];
  var toks = [];
  if (DT10_V1112_SELLOG_OUTPUT_SIG && outputState) {
    toks.push('OUT~' + String(outputState).toUpperCase());
  }
  for (var i = 0; i < tickets.length; i++) {
    var t = tickets[i] || {};
    var sym = t.symbol;
    if (sym === null || sym === undefined || String(sym) === '') { continue; }
    var seated = (t._grace_hold === true) ? '-' : 'S';
    var shares = Number(t.suggested_shares);
    var funded = (isFinite(shares) && shares > 0) ? 'F' : '-';
    var stab = '';
    try {
      stab = String(t._stab_status || '').split(' ')[0].split('(')[0]
          .toUpperCase();
    } catch (eS) { stab = ''; }
    toks.push(String(sym) + '~' + seated + funded + '~' + stab);
  }
  toks.sort();
  return toks.join('|');
}
/** Pure: one row (DT10_SELLOG_HEADERS width) from a rendered ticket.
 *  Mirrors dt10TicketToRow_ field-for-field, in the LOG schema order. */
function dt10SelLogRowFromTicket_(t, loggedAt, runInfo, panelJson) {
  t = t || {};
  var d = t.detail || {};
  return [loggedAt, runInfo, DT10_SHEET, dt10Cell_(t.rank),
          dt10Cell_(t.symbol), dt10Cell_(t.name), dt10Cell_(t.market),
          dt10Cell_(t.sector), dt10Cell_(t.currency), dt10Cell_(t.fx_to_sar),
          dt10Cell_(t.price), dt10Cell_(t.price_sar), dt10Cell_(t.entry_zone),
          dt10Cell_(t.suggested_sar), dt10Cell_(t.suggested_shares),
          dt10Cell_(t.stop_sar), dt10Cell_(t.tp1_sar), dt10Cell_(t.tp2_sar),
          dt10Cell_(t.roi_pct), dt10Cell_(t.engine_roi_pct),
          dt10Cell_(t.ann_roi_pct), dt10Cell_(t.exp_gain_12m_sar),
          dt10Cell_(t.reliability), dt10Cell_(t.dq),
          dt10Cell_(t.confidence_band), dt10Cell_(d.funds_from),
          dt10Cell_(d.review_date), dt10Cell_(t.advisor_note),
          panelJson, '', '',
          dt10Cell_(t._stab_status), dt10Cell_(t._stab_days)];
}
function dt10SelLogLoadState_() {
  var raw = '';
  try {
    raw = PropertiesService.getScriptProperties()
        .getProperty(DT10_SELLOG_STATE_PROP) || '';
  } catch (e) {}
  var st = { date: '', sig: '' };
  if (raw) {
    try {
      var parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object') {
        st.date = String(parsed.date || '');
        st.sig = String(parsed.sig || '');
      }
    } catch (e2) {}
  }
  return st;
}
function dt10SelLogSaveState_(dateKey, sig) {
  try {
    PropertiesService.getScriptProperties().setProperty(
        DT10_SELLOG_STATE_PROP, JSON.stringify({ date: dateKey, sig: sig }));
  } catch (e) {}
}
/** Resolve/create the log sheet; heal the v1.3.1 header (cols 32-33). */
function dt10SelLogSheet_(ss) {
  var want = DT10_SELLOG_HEADERS.length;
  var sh = ss.getSheetByName(DT10_SELLOG_SHEET);
  if (!sh) {
    sh = ss.insertSheet(DT10_SELLOG_SHEET);
    sh.getRange(1, 1, 1, want).setValues([DT10_SELLOG_HEADERS])
        .setFontWeight('bold');
    sh.setFrozenRows(1);
    return sh;
  }
  if (sh.getMaxColumns() < want) {
    sh.insertColumnsAfter(sh.getMaxColumns(), want - sh.getMaxColumns());
  }
  if (String(sh.getRange(1, 32).getValue() || '') === '') {
    sh.getRange(1, 32, 1, 2).setValues([['Stability', 'Days']])
        .setFontWeight('bold');
  }
  return sh;
}
/** Canonical digest covers exactly the header and values readers consume. */
function dt10CandTabChecksum_(headers, rows) {
  var bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256,
      JSON.stringify([headers, rows]), Utilities.Charset.UTF_8);
  var hex = '';
  for (var i = 0; i < bytes.length; i++) hex += ('0' + ((bytes[i] + 256) % 256).toString(16)).slice(-2);
  if (hex.length !== 64) throw new Error('candidate checksum unavailable');
  return hex;
}
function dt10CandRunId_(meta) {
  var m = meta || {}, route = m.route || {};
  return String(route.request_id || m.run_id || m.request_id || '').replace(/^\s+|\s+$/g, '');
}
/** A current reader requires the final COMMITTED receipt, exact run ID,
 * exact dimensions and digest of a fresh values read. Old/partial grids
 * never authorize a current read, including a current empty result. */
function dt10CandTabRead_(ss, runId) {
  try {
    var sh = ss.getSheetByName(DT10_CANDTAB_SHEET);
    if (!sh || !runId) throw new Error('current candidate run missing');
    var receiptRaw = String(sh.getRange(1, 2).getValue() || '');
    var receipt = JSON.parse(receiptRaw);
    if (receipt.state !== 'COMMITTED' || receipt.run_id !== String(runId) ||
        typeof receipt.rows !== 'number' || receipt.rows < 0 ||
        Math.floor(receipt.rows) !== receipt.rows || receipt.cols !== DT10_CAND_HEADERS.length ||
        sh.getLastRow() !== receipt.rows + 2) throw new Error('candidate run not committed/current');
    var header = sh.getRange(2, 1, 1, receipt.cols).getValues()[0];
    var data = receipt.rows ? sh.getRange(3, 1, receipt.rows, receipt.cols).getValues() : [];
    if (JSON.stringify(header) !== JSON.stringify(DT10_CAND_HEADERS) ||
        dt10CandTabChecksum_(header, data) !== receipt.checksum) throw new Error('candidate readback mismatch');
    if (String(sh.getRange(1, 2).getValue() || '') !== receiptRaw ||
        sh.getLastRow() !== receipt.rows + 2) throw new Error('candidate commit changed during read');
    return { ok: true, rows: receipt.rows, run_id: receipt.run_id,
             checksum: receipt.checksum, data: data };
  } catch (eCr) {
    return { ok: false, rows: 0, err: String(eCr && eCr.message || eCr) };
  }
}
/** Stage, read back and verify the complete grid before touching the public
 * tab. Invalidate its receipt before mutation; COMMITTED is the final write.
 * A fault falls back to today's cockpit grid and leaves no trusted partial
 * publication. The same protocol commits zero rows, replacing old results. */
function dt10CandTabWrite_(ss, cRows, stampLine, tokens, runId) {
  var lock = null, locked = false;
  try {
    lock = LockService.getScriptLock();
    locked = lock.tryLock(5000);
    if (!locked) throw new Error('candidate publication already running');
    var n = cRows.length, cols = DT10_CAND_HEADERS.length;
    if (!runId) throw new Error('current candidate run ID missing');
    for (var ri = 0; ri < n; ri++) {
      if (!Array.isArray(cRows[ri]) || cRows[ri].length !== cols) throw new Error('candidate row shape invalid');
    }
    var checksum = dt10CandTabChecksum_(DT10_CAND_HEADERS, cRows);
    var receipt = { state: 'PENDING', run_id: String(runId), rows: n, cols: cols, checksum: checksum };
    var stamp = [stampLine, JSON.stringify(receipt)];
    for (var k = 2; k < cols; k++) stamp.push('');
    var grid = [stamp, DT10_CAND_HEADERS].concat(cRows);
    var stage = ss.getSheetByName(DT10_CANDTAB_STAGE_SHEET);
    if (!stage) stage = ss.insertSheet(DT10_CANDTAB_STAGE_SHEET);
    function capacity(sh) {
      if (sh.getMaxColumns() < cols) sh.insertColumnsAfter(sh.getMaxColumns(), cols - sh.getMaxColumns());
      if (sh.getMaxRows() < n + 2) sh.insertRowsAfter(sh.getMaxRows(), n + 2 - sh.getMaxRows());
    }
    capacity(stage);
    stage.clear();
    stage.getRange(1, 1, n + 2, cols).setValues(grid);
    SpreadsheetApp.flush();
    var stagedHeader = stage.getRange(2, 1, 1, cols).getValues()[0];
    var stagedData = n ? stage.getRange(3, 1, n, cols).getValues() : [];
    if (stage.getLastRow() !== n + 2 ||
        dt10CandTabChecksum_(stagedHeader, stagedData) !== checksum) throw new Error('candidate staging readback mismatch');
    if (!stage.isSheetHidden()) stage.hideSheet();
    var sh = ss.getSheetByName(DT10_CANDTAB_SHEET);
    if (!sh) sh = ss.insertSheet(DT10_CANDTAB_SHEET);
    capacity(sh);
    // Invalidate before clear/copy, so even a torn multi-cell write cannot
    // retain an old COMMITTED marker beside current-stamped partial rows.
    sh.getRange(1, 2).setValue(JSON.stringify(receipt));
    sh.clear();
    sh.getRange(1, 1, n + 2, cols).setValues(grid);
    sh.getRange(2, 1, 1, cols).setFontWeight('bold');
    if (n) {
      sh.getRange(3, 1, n, cols).setValues(cRows);
      dt10ApplyColFormats_(sh, 3, n, {
        6: DT10_FMT_PRICE, 7: DT10_FMT_PRICE, 8: DT10_FMT_PCT, 9: DT10_FMT_PCT,
        10: DT10_FMT_PCT, 11: DT10_FMT_NUM2, 12: '0.0', 13: '0.0', 20: '0.0'
      });
      if (tokens) dt10ColorVerdicts_(sh, 3, n, 18, tokens);
    }
    sh.setFrozenRows(2);
    if (!sh.isSheetHidden()) sh.hideSheet();
    SpreadsheetApp.flush();
    var writtenHeader = sh.getRange(2, 1, 1, cols).getValues()[0];
    var writtenData = n ? sh.getRange(3, 1, n, cols).getValues() : [];
    if (sh.getLastRow() !== n + 2 ||
        dt10CandTabChecksum_(writtenHeader, writtenData) !== checksum) throw new Error('candidate final readback mismatch');
    receipt.state = 'COMMITTED';
    sh.getRange(1, 2).setValue(JSON.stringify(receipt));
    SpreadsheetApp.flush();
    var committed = dt10CandTabRead_(ss, runId);
    if (!committed.ok) throw new Error(committed.err);
    return { ok: true, rows: n, run_id: String(runId), checksum: checksum, state: 'COMMITTED' };
  } catch (eCt) {
    return { ok: false, rows: 0, err: String(eCt && eCt.message || eCt) };
  } finally {
    if (locked) { try { lock.releaseLock(); } catch (eLock) {} }
    try { ss.setActiveSheet(ss.getSheetByName(DT10_SHEET)); } catch (eFocus) {}
  }
}
/** v1.13.4 [WP-8]: ONE _Run_Log INPUT-TRUTH row per run when any gate is
 * armed; off everywhere => no row (returns false). details JSON carries the
 * evaluability facts, the reference items, calendar coverage, the tab
 * result and the countable alert objects (no portfolio figures). */
function dt10InputTruthRunLog_(itMode, candMode, vintMode, payload, itRef,
                               notes) {
  if (itMode === 'off' && candMode === 'off' && vintMode === 'off') return false;
  var p = payload || {};
  var ev = p._dt10_it_ev || null;
  var alerts = [];
  try {
    if (ev) alerts = alerts.concat(dt10GateEvaluabilityAlerts_(ev));
    if (itRef) alerts = alerts.concat(dt10PanelRefAlerts_(itRef));
  } catch (eAl) {}
  var msg = [];
  for (var i = 0; i < (notes || []).length; i++) if (notes[i]) msg.push(notes[i]);
  return dt10RunLogRow_('INFO', 'INPUT-TRUTH', msg.join(' | '), {
    version: DT10_VERSION, input_truth: itMode, cand_tab: candMode,
    vintage_cols: vintMode, ev: ev, ref: itRef, cal: p._dt10_cal || null,
    cand_rows: (p.candidates_rows || []).length,
    cand_tab_result: p._dt10_cand_result || null,
    notes: notes || [], alerts: alerts });
}
/**
 * v1.6.6 (S-5) PURE: recompute Blended R/R (TP2 basis) and Blended
 * Reliability from the RENDERED tickets and compare against payload.kpis.
 * Returns '' when they agree (or cannot be computed) and a
 * 'KPI-CHECK ...' token when the backend figure disagrees beyond
 * tolerance (0.05 rr / 0.5 rel). Verifies — never rewrites — the
 * backend number. Never throws.
 */
function dt10KpiCheckNote_(payload) {
  try {
    var sel = (payload && payload.selected) || [];
    var kpis = (payload && payload.kpis) || {};
    if (!sel.length) return '';
    var rrSum = 0, rrN = 0, relSum = 0, relN = 0;
    var rrBasis = ((payload.meta || {}).board_execution_basis || {}).blended_rr || 'tp2';
    for (var i = 0; i < sel.length; i++) {
      var t = sel[i] || {};
      var weight = Number(t.suggested_sar), rel = Number(t.reliability);
      if (!dt10BoardEligible_(t) || !(weight > 0) || !(Number(t.suggested_shares) > 0)) continue;
      var rrRaw = (t.detail || {})[rrBasis === 'plan' ? 'rr' : 'rr_tp2'];
      var rr = Number(rrRaw);
      if (rrRaw !== null && rrRaw !== undefined && isFinite(rr)) {
        rrSum += rr * weight;
        rrN += weight;
      }
      if (isFinite(rel) && rel > 0) { relSum += rel * weight; relN += weight; }
    }
    var bits = [];
    var brr = Number(kpis.blended_rr);
    if (rrN && isFinite(brr)) {
      var rrBoard = Math.round((rrSum / rrN) * 100) / 100;
      if (Math.abs(rrBoard - brr) > 0.05) {
        bits.push('rr board\u2248' + rrBoard + ' vs kpi ' + brr);
      }
    }
    var brel = Number(kpis.blended_reliability);
    if (relN && isFinite(brel)) {
      var relBoard = Math.round((relSum / relN) * 10) / 10;
      if (Math.abs(relBoard - brel) > 0.5) {
        bits.push('rel board\u2248' + relBoard + ' vs kpi ' + brel);
      }
    }
    return bits.length ? ('KPI-CHECK ' + bits.join('; ')) : '';
  } catch (eKc) {
    return '';
  }
}
/**
 * v1.8.0 (S-6) PURE: reconcile the backend's FUNDED pick count
 * (kpis.selected_count) against the BOARD's executable count. On the
 * 2026-08-07 board the KPI said 'Selected 6 / 10' while the table rendered
 * 1 executable + 9 grace — both true of DIFFERENT sets, with nothing on
 * the page naming the gap. Returns '' when they agree or cannot be read;
 * never throws. Verifies — never rewrites — the backend number (the S-5
 * doctrine).
 */
function dt10SeatCheckNote_(payload) {
  try {
    var sel = (payload && payload.selected) || [];
    var k = (payload && payload.kpis) || {};
    var funded = Number(k.selected_count);
    if (!isFinite(funded)) return '';
    var cls = dt10TicketClasses_(sel);            /* v1.11.1 */
    var execN = cls.exec, graceN = cls.grace, suspN = cls.suspended;
    var gain = Number(k.expected_gain_12m_sar);
    var gainBit = (execN === 0 && isFinite(gain) && gain > 0) ?
        ('gain kpi ' + Math.round(gain) + ' SAR vs board 0 exec') : '';
    if (execN === funded) return gainBit ? ('KPI-CHECK ' + gainBit) : '';
    return 'SEAT-CHECK kpi ' + funded + ' funded vs board ' + execN +
           ' exec' + (suspN ? ' +' + suspN + ' suspended' : '') +
           (graceN ? ' +' + graceN + ' grace' : '') +
           (gainBit ? '; ' + gainBit : '');
  } catch (eSc) {
    return '';
  }
}
/**
 * v1.8.8 (G-a) PURE: the seat-truth text for KPI cell 3 —
 * 'E exec + F fast-track suspended + R research + P pend + G grace / M'.
 * Sources: final ticket classification, payload.selected._grace_hold
 * (the D-1 annotation), the BE-1 pending source
 * (payload.meta.stability.audit.pending, via dt10StabPendingMap_), and
 * kpis.max_selected. Returns '' when there is nothing to say or on ANY
 * error — the v1.8.7 cell then stands untouched (fail-open).
 */
function dt10SeatTruthKpi_(payload) {
  try {
    var sel = (payload && payload.selected) || [];
    var cls = dt10TicketClasses_(sel);            /* v1.11.1 */
    var execN = cls.exec, graceN = cls.grace;
    var fastN = sel.filter(function (t) {
      return t && t._ft_suspended === true && t._grace_hold !== true;
    }).length;
    var researchN = Math.max(0, cls.suspended - fastN);
    var pendMap = dt10StabPendingMap_(payload);
    var pendN = 0, pk, seatKeys = {};
    sel.forEach(function (t) {
      var key = dt10NormToken_(t && t.symbol);
      if (key) seatKeys[key] = true;
    });
    for (pk in pendMap) {
      if (Object.prototype.hasOwnProperty.call(pendMap, pk) && !seatKeys[pk]) pendN++;
    }
    if (cls.total === 0 && pendN === 0) return '';
    var bits = [execN + ' exec'];
    if (fastN) bits.push(fastN + ' fast-track suspended');
    if (researchN) bits.push(researchN + ' research');
    if (pendN) bits.push(pendN + ' pend');
    if (graceN) bits.push(graceN + ' grace');
    var txt = bits.join(' + ');
    var k = (payload && payload.kpis) || {};
    if (k.max_selected !== null && k.max_selected !== undefined) {
      txt += ' / ' + String(k.max_selected);
    }
    return txt;
  } catch (eSt) {
    return '';
  }
}
/**
 * v1.6.6 (S-4) PURE: one _Selection_Log row per departed symbol, Outcome
 * column stamped 'EXIT: <kind>'. Row width = DT10_SELLOG_HEADERS; only
 * Logged At / Run Info / Source Page / Symbol / Outcome are filled.
 */
function dt10SelLogExitRows_(audit, stamp, runInfo) {
  var rows = [];
  audit = audit || {};
  var want = DT10_SELLOG_HEADERS.length;
  var iSym = -1, iOut = -1, h;
  for (h = 0; h < want; h++) {
    if (DT10_SELLOG_HEADERS[h] === 'Symbol') iSym = h;
    if (DT10_SELLOG_HEADERS[h] === 'Outcome') iOut = h;
  }
  if (iSym < 0 || iOut < 0) return rows;
  var kinds = [['hard', audit.exited_hard], ['soft', audit.exited_soft],
               ['capacity', audit.exited_capacity],
               ['displaced', audit.exited_displaced],
               ['LOST', audit.exited_lost]];
  for (var k = 0; k < kinds.length; k++) {
    var list = kinds[k][1] || [];
    for (var j = 0; j < list.length; j++) {
      var row = [];
      for (var c = 0; c < want; c++) row.push('');
      row[0] = stamp;
      row[1] = String(runInfo || '') + ' [membership exit]';
      row[2] = DT10_SHEET;
      row[iSym] = list[j];
      row[iOut] = 'EXIT: ' + kinds[k][0];
      rows.push(row);
    }
  }
  return rows;
}
/**
 * v1.6.6 (S-4): append the exit rows. Respects DT10_SELECTION_LOG='off';
 * NOT subject to the daily/signature dedupe — exits are rare and
 * decision-critical. NEVER throws; returns a short status token or ''.
 */
function dt10AppendExitLog_(ss, audit, runInfo) {
  try {
    var mode = '';
    try {
      mode = String(PropertiesService.getScriptProperties()
          .getProperty(DT10_SELLOG_MODE_PROP) || '').toLowerCase();
    } catch (ePx) {}
    if (mode === 'off') return '';
    var stamp = '';
    try {
      stamp = Utilities.formatDate(new Date(),
          Session.getScriptTimeZone() || 'Asia/Riyadh',
          'yyyy-MM-dd HH:mm:ss');
    } catch (eTx) { stamp = String(new Date()); }
    var rows = dt10SelLogExitRows_(audit, stamp, runInfo);
    if (!rows.length) return '';
    var sh = dt10SelLogSheet_(ss);
    sh.getRange(sh.getLastRow() + 1, 1, rows.length,
                DT10_SELLOG_HEADERS.length).setValues(rows);
    return 'ExitLog: +' + rows.length;
  } catch (eEl) {
    try {
      Logger.log('[DT10 v' + DT10_VERSION + '] ExitLog ERROR: ' + eEl);
    } catch (eE2) {}
    return 'ExitLog: ERROR';
  }
}
// ---------------------------------------------------------------------------
// v1.17.0 [SHADOW-RANK-LOG] -- the backend's ranking challenger, logged.
// Script Property DT10_SHADOW_RANK_LOG = off (DEFAULT) | on | all; see the
// v1.17.0 header block. Research evidence only: ranks, symbols and the backend's own
// figures, never a ticket, price or amount.
// ---------------------------------------------------------------------------
var DT10_SHADOW_LOG_PROP = 'DT10_SHADOW_RANK_LOG';
var DT10_SHADOW_LOG_SHEET = '_Shadow_Rank_Log';
var DT10_SHADOW_LOG_STATE_PROP = 'DT10_SHADOW_LOG_STATE_V1';
var DT10_SHADOW_LOG_HEADERS = ['Logged At', 'Day', 'Cohort', 'Order', 'Rank',
  'Symbol', 'Market', 'Sector', 'Seat', 'Live Rank', 'Shadow Rank',
  'Engine ROI %', 'Rel', 'Conf', 'Momentum', 'Strength', 'Strength Ref',
  'Cap', 'Floor OK', 'Mode', 'Rule', 'Builder', 'Pool', 'Overlap Top5'];
/** v1.17.0: 'on' (first event of each UTC day) or 'all' (also on change)
 * for those exact words (trimmed, any case), else 'off'. Never throws. */
function dt10ShadowLogMode_() {
  var raw = '';
  try {
    raw = String(PropertiesService.getScriptProperties()
        .getProperty(DT10_SHADOW_LOG_PROP) || '');
  } catch (e) {
    raw = '';
  }
  raw = raw.replace(/^\s+|\s+$/g, '').toLowerCase();
  return (raw === 'on' || raw === 'all') ? raw : 'off';
}
/** v1.17.0 PURE: the rows of one logging event and its signature, from
 * payload.meta.rank_challenger (opportunity_builder v1.28.0, shadow) and the
 * seats this render shows (payload.selected). Returns { rows, sig, reason };
 * rows is empty -- nothing to log -- when the block is absent (backend at
 * legacy), carries an error, or both lists are empty. Orders: CHAMPION (the
 * backend's live order; Rank = live rank), CHALLENGER (the shadow order;
 * Rank = shadow rank), BOARD (Rank = board rank; Shadow Rank from the seat,
 * else from the INVEST audit row; blank when the name is not in today's
 * INVEST pool, e.g. a grace hold). Every row is DT10_SHADOW_LOG_HEADERS wide. */
function dt10ShadowLogRows_(payload, stamp, day, cohort) {
  var meta = (payload && payload.meta) || {};
  var rc = meta.rank_challenger;
  if (!rc || typeof rc !== 'object') {
    return { rows: [], sig: '', reason: 'no challenger block' };
  }
  if (rc.error) {
    return { rows: [], sig: '', reason: 'challenger error ' + String(rc.error) };
  }
  var champ = rc.champion || [];
  var chal = rc.challenger || [];
  if (!champ.length && !chal.length) {
    return { rows: [], sig: '', reason: 'empty challenger lists' };
  }
  var detail = {};
  var i, r, s;
  var lists = [chal, champ];
  for (var li = 0; li < lists.length; li++) {
    for (i = 0; i < lists[li].length; i++) {
      r = lists[li][i] || {};
      s = dt10NormSym_(r.symbol);
      if (s && !detail.hasOwnProperty(s)) detail[s] = r;
    }
  }
  var auditRank = {};
  var cands = (payload && payload.candidates_rows) || [];
  for (i = 0; i < cands.length; i++) {
    r = cands[i] || {};
    s = dt10NormSym_(r.symbol);
    if (s && r.shadow_rank !== undefined && r.shadow_rank !== null &&
        !auditRank.hasOwnProperty(s)) {
      auditRank[s] = r.shadow_rank;
    }
  }
  function vtag(v) {
    var c = dt10Cell_(v);
    return c === '' ? '' : 'v' + String(c);
  }
  var tail = [dt10Cell_(rc.mode), vtag(rc.version), vtag(rc.builder),
              dt10Cell_(rc.pool), dt10Cell_(rc.overlap_top5)];
  function flag(v) {
    return v === true ? 'Y' : (v === false ? 'N' : '');
  }
  function line(order, rank, sym, x, seat, live, shadow, eng, rel, conf) {
    return [stamp, day, cohort, order, dt10Cell_(rank), dt10Cell_(sym),
            dt10Cell_(x.market), dt10Cell_(x.sector), seat, dt10Cell_(live),
            dt10Cell_(shadow), dt10Cell_(eng), dt10Cell_(rel), dt10Cell_(conf),
            dt10Cell_(x.momentum), dt10Cell_(x.strength),
            dt10Cell_(x.strength_ref), flag(x.cap), flag(x.floor_ok)]
        .concat(tail);
  }
  var rows = [], cSyms = [], sSyms = [], bToks = [];
  for (i = 0; i < champ.length; i++) {
    r = champ[i] || {};
    rows.push(line('CHAMPION', r.live_rank, r.symbol, r, '', r.live_rank,
                   r.shadow_rank, r.engine_roi_pct, r.rel, r.conf));
    cSyms.push(dt10NormSym_(r.symbol));
  }
  for (i = 0; i < chal.length; i++) {
    r = chal[i] || {};
    rows.push(line('CHALLENGER', r.shadow_rank, r.symbol, r, '', r.live_rank,
                   r.shadow_rank, r.engine_roi_pct, r.rel, r.conf));
    sSyms.push(dt10NormSym_(r.symbol));
  }
  var seats = (payload && payload.selected) || [];
  for (i = 0; i < seats.length; i++) {
    var t = seats[i] || {};
    s = dt10NormSym_(t.symbol);
    if (!s) continue;
    var x = detail.hasOwnProperty(s) ? detail[s] : {};
    var seat = '';
    try {
      seat = String(t._stab_status || '').split(' ')[0].split('(')[0]
          .toUpperCase();
    } catch (eS) {
      seat = '';
    }
    var shadow = (t.shadow_rank !== undefined && t.shadow_rank !== null) ?
        t.shadow_rank : (auditRank.hasOwnProperty(s) ? auditRank[s] : '');
    var bRank = (t.rank !== undefined && t.rank !== null && t.rank !== '') ?
        t.rank : i + 1;
    rows.push(line('BOARD', bRank, t.symbol,
                   { market: t.market, sector: t.sector,
                     momentum: x.momentum, strength: x.strength,
                     strength_ref: x.strength_ref, cap: x.cap,
                     floor_ok: x.floor_ok },
                   seat, x.live_rank, shadow, t.engine_roi_pct, t.reliability,
                   t.confidence_band));
    bToks.push(bRank + ':' + s);
  }
  return { rows: rows, reason: '',
           sig: day + '|C:' + cSyms.join(',') + '|S:' + sSyms.join(',') +
               '|B:' + bToks.join(',') };
}
/** v1.17.0: append one event to _Shadow_Rank_Log when DT10_SHADOW_RANK_LOG
 * is on or all. '' when off (no further read, no tab, no write: the v1.16.0
 * status line). Cadence: 'on' = the first event of each UTC day (what the
 * nightly scorecard reads); 'all' = also whenever an order changes (state
 * DT10_SHADOW_LOG_STATE_V1). The tab is created hidden on first use; row 1
 * must be the header (written when blank; another layout is never written).
 * Returns the status token. NEVER throws. */
function dt10AppendShadowRankLog_(ss, payload, cohort) {
  // v1.17.1 [SHADOW-MOMENTUM]: the mode the pool projection used in this
  // execution when it ran (the logged day and the sent rows agree); else the
  // property itself, as in v1.17.0.
  var mode = (typeof DT10_PROP_MEMO_ === 'object' && DT10_PROP_MEMO_ &&
      Object.prototype.hasOwnProperty.call(DT10_PROP_MEMO_, 'shadow_log_mode')) ?
      DT10_PROP_MEMO_.shadow_log_mode : dt10ShadowLogMode_();
  if (mode === 'off') return '';
  try {
    var stamp = '';
    try {
      stamp = Utilities.formatDate(new Date(),
          Session.getScriptTimeZone() || 'Asia/Riyadh',
          'yyyy-MM-dd HH:mm:ss');
    } catch (eT) {
      stamp = String(new Date());
    }
    var day = dt10StabToday_();
    var built = dt10ShadowLogRows_(payload, stamp, day,
        String(cohort === null || cohort === undefined ? '' : cohort));
    if (!built.rows.length) return 'ShadowLog: none (' + built.reason + ')';
    var props = PropertiesService.getScriptProperties();
    var prev = {};
    try {
      prev = JSON.parse(props.getProperty(DT10_SHADOW_LOG_STATE_PROP) ||
                        '{}') || {};
    } catch (eP) {
      prev = {};
    }
    if (prev.date === day && (mode === 'on' || prev.sig === built.sig)) {
      return mode === 'on' ? 'ShadowLog: logged today' :
          'ShadowLog: unchanged';
    }
    var want = DT10_SHADOW_LOG_HEADERS.length;
    var sh = ss.getSheetByName(DT10_SHADOW_LOG_SHEET);
    if (!sh) {
      sh = ss.insertSheet(DT10_SHADOW_LOG_SHEET);
      try {
        if (sh.getMaxColumns() > want) {
          sh.deleteColumns(want + 1, sh.getMaxColumns() - want);
        }
      } catch (eC) {}
      try {
        sh.hideSheet();
      } catch (eH) {}
      try {
        ss.setActiveSheet(ss.getSheetByName(DT10_SHEET));
      } catch (eF) {}
    }
    if (sh.getMaxColumns() < want) {
      sh.insertColumnsAfter(sh.getMaxColumns(), want - sh.getMaxColumns());
    }
    var head = sh.getLastRow() > 0 ?
        (sh.getRange(1, 1, 1, want).getValues()[0] || []) : [];
    var blank = false;
    for (var hi = 0; hi < want; hi++) {
      var hv = String(head[hi] === null || head[hi] === undefined ?
          '' : head[hi]).replace(/^\s+|\s+$/g, '');
      if (hv === '') {
        blank = true;
      } else if (hv !== DT10_SHADOW_LOG_HEADERS[hi]) {
        return 'ShadowLog: ERROR header mismatch at column ' + (hi + 1);
      }
    }
    if (blank) {
      sh.getRange(1, 1, 1, want).setValues([DT10_SHADOW_LOG_HEADERS])
          .setFontWeight('bold');
      sh.setFrozenRows(1);
    }
    var first = Math.max(sh.getLastRow(), 1) + 1;
    var need = first + built.rows.length - 1;
    if (sh.getMaxRows() < need) {
      sh.insertRowsAfter(sh.getMaxRows(), need - sh.getMaxRows());
    }
    sh.getRange(first, 1, built.rows.length, want).setValues(built.rows);
    try {
      props.setProperty(DT10_SHADOW_LOG_STATE_PROP,
          JSON.stringify({ date: day, sig: built.sig }));
    } catch (eS) {}
    return 'ShadowLog: +' + built.rows.length;
  } catch (e) {
    try {
      Logger.log('[DT10 v' + DT10_VERSION + '] ShadowLog ERROR: ' + e);
    } catch (e2) {}
    return 'ShadowLog: ERROR ' + String(e).slice(0, 60);
  }
}
/**
 * Append the rendered board to _Selection_Log. Modes (ScriptProperty
 * DT10_SELECTION_LOG): 'off' kill-switch; 'always' every run; unset =
 * DEFAULT: once per UTC day PLUS any run whose membership set changed.
 * NEVER throws — every failure degrades to the returned status note.
 * v1.10.0: under a non-EXECUTABLE upstream verdict the appended rows are
 * withheld (same surface as the board) via a self-contained fail-closed
 * verdict read — the log is an audit trail, never an order sheet.
 */
/** v1.11.0 (1): one EMPTY_BOARD row per day in _Selection_Log with the
 * reason snapshot in Advisor Note. Uses the same date+signature state as
 * ticket rows (signature 'EMPTY_BOARD'). Kill-switch DT10_EMPTY_BOARD_LOG=0
 * restores the v1.10.x silence. NEVER throws. */
function dt10AppendEmptyBoardRow_(ss, runInfo, panel, mode) {
  try {
    var flag = '';
    try {
      flag = String(PropertiesService.getScriptProperties()
          .getProperty('DT10_EMPTY_BOARD_LOG') || '').toLowerCase();
    } catch (eF) {}
    if (flag === '0' || flag === 'false' || flag === 'off' || flag === 'no') {
      return 'SelLog: empty board (not logged)';
    }
    var today = dt10StabToday_();
    var sig = 'EMPTY_BOARD';
    if (mode !== 'always') {
      var prev = dt10SelLogLoadState_();
      if (prev.date === today && prev.sig === sig) {
        return 'SelLog: empty board (logged today)';
      }
    }
    var sh = dt10SelLogSheet_(ss);
    if (!sh) return 'SelLog: sheet unavailable';
    var loggedAt = Utilities.formatDate(new Date(),
        Session.getScriptTimeZone() || 'Asia/Riyadh', 'yyyy-MM-dd HH:mm:ss');
    var reason = String(runInfo || '');
    var panelJson = '';
    try { panelJson = JSON.stringify(panel || {}); } catch (eJ) {}
    var t = { rank: '', symbol: 'EMPTY_BOARD', name: 'no qualifying opportunities',
              advisor_note: 'EMPTY_BOARD: ' + reason.slice(0, 900) };
    var row = dt10SelLogRowFromTicket_(t, loggedAt, runInfo, panelJson);
    sh.getRange(sh.getLastRow() + 1, 1, 1, DT10_SELLOG_HEADERS.length)
      .setValues([row]);
    dt10SelLogSaveState_(today, sig);
    return 'SelLog: EMPTY_BOARD logged';
  } catch (e) {
    return 'SelLog: empty board (log failed: ' + String(e).slice(0, 60) + ')';
  }
}

function dt10AppendSelectionLog_(ss, tickets, runInfo, panel, outputState, sourceContext) {
  try {
    var mode = '';
    try {
      mode = String(PropertiesService.getScriptProperties()
          .getProperty(DT10_SELLOG_MODE_PROP) || '').toLowerCase();
    } catch (eP) {}
    if (mode === 'off') return 'SelLog: off';
    tickets = tickets || [];
    if (!tickets.length) {
      // v1.11.0 (1): the board reaching zero is logged as an event, once/day.
      return dt10AppendEmptyBoardRow_(ss, runInfo, panel, mode);
    }
    var today = dt10StabToday_();
    var sig = dt10SelLogSignature_(tickets, outputState);   // v1.11.2 [P-62]
    if (mode !== 'always') {
      var prev = dt10SelLogLoadState_();
      if (prev.date === today && prev.sig === sig) {
        return 'SelLog: unchanged (logged today)';
      }
    }
    var stamp = '';
    try {
      stamp = Utilities.formatDate(new Date(),
          Session.getScriptTimeZone() || 'Asia/Riyadh',
          'yyyy-MM-dd HH:mm:ss');
    } catch (eT) {
      stamp = String(new Date());
    }
    var panelJson = '';
    try { panelJson = JSON.stringify(panel || {}); } catch (eJ) {}
    var rows = [];
    for (var i = 0; i < tickets.length; i++) {
      rows.push(dt10SelLogRowFromTicket_(tickets[i], stamp,
                                         String(runInfo || ''), panelJson));
    }
    /* v1.10.0 (R3): fail-closed verdict read (mirrors dt10RenderPayload_);
     * read trouble withholds. */
    if (dt10UvOn_() || sourceContext) {
      var slUv;
      try {
        slUv = dt10BoardVerdict_(ss, sourceContext);
      } catch (eUv) {
        slUv = { state: 'NOT_ACTIONABLE', reason: 'verdict read failed' };
      }
      if (slUv.state !== 'EXECUTABLE') {
        for (var wI = 0; wI < rows.length; wI++) {
          dt10UvWithholdRow_(rows[wI], DT10_UV_LOG_WITHHOLD_IDX,
                             DT10_UV_LOG_NOTE_IDX, slUv.reason);
        }
      }
    }
    var sh = dt10SelLogSheet_(ss);
    sh.getRange(sh.getLastRow() + 1, 1, rows.length,
                DT10_SELLOG_HEADERS.length).setValues(rows);
    dt10SelLogSaveState_(today, sig);
    return 'SelLog: +' + rows.length;
  } catch (e) {
    try {
      Logger.log('[DT10 v' + DT10_VERSION + '] SelLog ERROR: ' + e);
    } catch (e2) {}
    return 'SelLog: ERROR ' + String(e).slice(0, 80);
  }
}
// ---------------------------------------------------------------------------
// Layout builder (one-time / repair)
// ---------------------------------------------------------------------------
function buildDecisionTop10Layout() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(DT10_SHEET);
  if (!sheet) sheet = ss.insertSheet(DT10_SHEET);
  var tokens = dt10Tokens_(ss);
  var header = tokens.HEADER || DT10_FALLBACK_TOKENS.HEADER;
  var op = tokens.OPERATOR_INPUT || DT10_FALLBACK_TOKENS.OPERATOR_INPUT;
  var preserved = dt10PanelPreserve_(sheet);        // v1.11.3 [P-76]
  sheet.clear();
  // Stale merges from a previous layout make merge()/setValues throw.
  sheet.getRange(1, 1, sheet.getMaxRows(), sheet.getMaxColumns())
      .breakApart();
  if (sheet.getMaxColumns() < DT10_LAST_COL) {
    sheet.insertColumnsAfter(sheet.getMaxColumns(),
        DT10_LAST_COL - sheet.getMaxColumns());
  }
  // Title + status
  var title = sheet.getRange(DT10_ROW_TITLE, 1, 1, DT10_LAST_COL);
  title.merge().setValue('TOP 10 INVESTMENTS — DECISION')
      .setBackground(header.bg).setFontColor(header.fg)
      .setFontWeight('bold').setFontSize(13)
      .setHorizontalAlignment('left');
  // v1.1.0 reskin: thin gold rule beneath the title (proposal accent).
  title.setBorder(null, null, true, null, null, null,
      DT10_ACCENT_GOLD, SpreadsheetApp.BorderStyle.SOLID_THICK);
  sheet.getRange(DT10_ROW_STATUS, 1).setValue('Status:')
      .setFontWeight('bold').setFontColor(DT10_MUTED);
  sheet.getRange(DT10_ROW_STATUS, 2, 1, DT10_LAST_COL - 1).merge()
      .setValue('Never run — use menu: Refresh Decision Top 10')
      .setFontColor(DT10_MUTED);
  // Control panel
  sheet.getRange(DT10_ROW_PANEL_HEAD, 1, 1, DT10_LAST_COL).merge()
      .setValue('CONTROL PANEL — blue cells are operator inputs (L14). ' +
                'Blanks re-seed from _Lists_Config defaults on run.')
      .setBackground(header.bg).setFontColor(header.fg)
      .setFontWeight('bold');
  var defaults = dt10PanelDefaults_(ss);
  for (var i = 0; i < DT10_PANEL.length; i++) {
    var item = DT10_PANEL[i];
    var pos = dt10PanelPos_(i);
    sheet.getRange(pos.row, pos.labelCol).setValue(item.label)
        .setFontWeight('bold').setFontSize(9).setWrap(true)
        .setBackground(DT10_TINT).setFontColor(DT10_NAVY);
    var seed = dt10PanelSeedValue_(item, defaults, preserved);   // v1.11.3
    var cell = sheet.getRange(pos.row, pos.valueCol);
    cell.setValue(seed).setBackground(op.bg).setFontColor(op.fg)
        .setFontWeight('bold');
    if (item.kind === 'sar') cell.setNumberFormat(DT10_FMT_SAR);
    else if (item.kind === 'num') cell.setNumberFormat('0.0#');
    else if (item.kind === 'int') cell.setNumberFormat('0');
  }
  sheet.getRange(DT10_ROW_PANEL_FIRST, 1, DT10_PANEL_ROWS, 8)
      .setBorder(true, true, true, true, true, true, DT10_BORDER,
                 SpreadsheetApp.BorderStyle.SOLID);
  try {                                             // v1.11.3 [P-76]
    var kept = [];
    for (var pk in preserved) { if (preserved.hasOwnProperty(pk)) kept.push(pk); }
    Logger.log('[DT10 v' + DT10_VERSION + '] layout: preserved ' + kept.length +
               ' operator input(s)' + (kept.length ? ': ' + kept.join(' | ') : ''));
  } catch (eK) {}
  // KPI strip
  sheet.getRange(DT10_ROW_KPI_HEAD, 1, 1, DT10_LAST_COL).merge()
      .setValue('KPIs').setBackground(header.bg).setFontColor(header.fg)
      .setFontWeight('bold');
  sheet.getRange(DT10_ROW_KPI_LABELS, 1, 1, DT10_KPI_LABELS.length)
      .setValues([DT10_KPI_LABELS]).setFontWeight('bold').setFontSize(9)
      .setWrap(true).setBackground(DT10_TINT).setFontColor(DT10_MUTED);
  var kpiVals = sheet.getRange(DT10_ROW_KPI_VALUES, 1, 1,
                               DT10_KPI_LABELS.length);
  kpiVals.setValue('');
  kpiVals.setFontWeight('bold').setFontSize(11)
      .setBackground(DT10_TINT).setFontColor(DT10_NAVY);
  sheet.getRange(DT10_ROW_KPI_LABELS, 1, 2, DT10_KPI_LABELS.length)
      .setBorder(true, true, true, true, true, true, DT10_BORDER,
                 SpreadsheetApp.BorderStyle.SOLID);
  sheet.setFrozenRows(DT10_ROW_KPI_VALUES);
  sheet.setColumnWidths(1, DT10_LAST_COL, 95);
  sheet.setColumnWidth(2, 110);  // labels / symbol
  sheet.setColumnWidth(3, 160);  // name
  sheet.setColumnWidth(DT10_LAST_COL, 360);  // advisor note
  Logger.log('[DT10 v' + DT10_VERSION + '] layout built');
  return sheet;
}
// ---------------------------------------------------------------------------
// Zone renderers
// ---------------------------------------------------------------------------
function dt10WriteSection_(sheet, row, title, tokens) {
  // v1.1.0 reskin: section dividers use the light navy tint with navy bold
  // text, sitting one tier above the navy/white column-header rows below.
  // `tokens` is kept in the signature for caller compatibility (unused here).
  sheet.getRange(row, 1, 1, DT10_LAST_COL).merge().setValue(title)
      .setBackground(DT10_TINT).setFontColor(DT10_NAVY)
      .setFontWeight('bold').setFontSize(11);
  return row + 1;
}
function dt10WriteTable_(sheet, row, headers, dataRows, emptyText) {
  // v1.1.0 reskin: column-header row is the brand navy band with white text.
  sheet.getRange(row, 1, 1, headers.length).setValues([headers])
      .setFontWeight('bold').setFontSize(9).setWrap(true)
      .setBackground(DT10_NAVY).setFontColor('#FFFFFF');
  row++;
  if (!dataRows.length) {
    sheet.getRange(row, 1, 1, DT10_LAST_COL).merge().setValue(emptyText)
        .setFontStyle('italic').setFontColor(DT10_MUTED);
    return { next: row + 1, firstDataRow: -1, count: 0 };
  }
  sheet.getRange(row, 1, dataRows.length, headers.length).setValues(dataRows);
  // Body text in proposal ink; alternate rows in the proposal zebra tint.
  var body = sheet.getRange(row, 1, dataRows.length, headers.length);
  body.setFontColor(DT10_INK);
  // v1.13.11: one read + one write instead of one call per zebra row; the
  // other rows keep the background they already had.
  var zebra = body.getBackgrounds();
  for (var i = 0; i < dataRows.length; i += 2) {
    for (var zc = 0; zc < headers.length; zc++) zebra[i][zc] = DT10_ZEBRA;
  }
  body.setBackgrounds(zebra);
  sheet.getRange(row - 1, 1, dataRows.length + 1, headers.length)
      .setBorder(true, true, true, true, true, true, DT10_BORDER,
                 SpreadsheetApp.BorderStyle.SOLID);
  return { next: row + dataRows.length, firstDataRow: row,
           count: dataRows.length };
}
/** v1.8.0 (D-4): paint each _grace_hold row of the SELECTED table in the
 *  WATCH amber tint, whole-row, so a grace-held ghost can never read as an
 *  executable pick at a glance. Runs BEFORE dt10ColorStability_, which then
 *  re-asserts the bold Stability cell on top. Display only; never throws. */
function dt10TintGraceRows_(sheet, firstDataRow, count, selected, tokens) {
  if (firstDataRow < 0 || !count || !selected) return;
  var tok = (tokens && tokens.VERDICT_WATCH) ||
      DT10_FALLBACK_TOKENS.VERDICT_WATCH;
  for (var i = 0; i < count && i < selected.length; i++) {
    if (selected[i] && selected[i]._grace_hold === true) {
      sheet.getRange(firstDataRow + i, 1, 1, DT10_LAST_COL)
          .setBackground(tok.bg);
    }
  }
}
/** v1.3.0 — color the SELECTED Stability column by state (§3.4 tokens):
 *  ACTIVE/NEW -> positive green; GRACE/FAST-TRACK -> watch amber. */
function dt10ColorStability_(sheet, firstDataRow, count, col, tokens) {
  if (firstDataRow < 0 || !count) return;
  var rng = sheet.getRange(firstDataRow, col, count, 1);
  var vals = rng.getValues(), toks = [];
  for (var i = 0; i < count; i++) {
    var v = String(vals[i][0] || '');
    var tok = null;
    if (v.indexOf('ACTIVE') === 0 || v.indexOf('NEW') === 0) {
      tok = tokens.VERDICT_POSITIVE;
    } else if (v.indexOf('GRACE') === 0 || v.indexOf('FAST-TRACK') === 0) {
      tok = tokens.VERDICT_WATCH;
    }
    toks.push(tok || null);
  }
  dt10BulkPaint_(rng, toks);   // v1.13.11
}
/** v1.13.11: paint a one-column range from per-row tokens in bulk. toks[i]
 * is a {bg, fg} token or null. A token row gets tok.bg, tok.fg and bold,
 * exactly what the per-cell setBackground/setFontColor/setFontWeight calls
 * wrote; a null row keeps its current background, font colour and weight
 * (read back and written unchanged). One read and one write per attribute;
 * nothing is read or written when no row has a token. */
function dt10BulkPaint_(rng, toks) {
  var any = false;
  for (var a = 0; a < toks.length; a++) {
    if (toks[a]) { any = true; break; }
  }
  if (!any) return;
  var bg = rng.getBackgrounds(), fg = rng.getFontColors(),
      fw = rng.getFontWeights();
  for (var i = 0; i < toks.length; i++) {
    var tok = toks[i];
    if (!tok) continue;
    bg[i][0] = tok.bg === undefined ? null : tok.bg;
    fg[i][0] = tok.fg === undefined ? null : tok.fg;
    fw[i][0] = 'bold';
  }
  rng.setBackgrounds(bg);
  rng.setFontColors(fg);
  rng.setFontWeights(fw);
}
/** Verdict/conf cell coloring via §3.4 tokens on a written table column. */
function dt10ColorVerdicts_(sheet, firstDataRow, count, col, tokens) {
  if (firstDataRow < 0 || !count) return;
  var rng = sheet.getRange(firstDataRow, col, count, 1);
  var vals = rng.getValues(), toks = [];
  for (var i = 0; i < count; i++) {
    var v = String(vals[i][0] || '');
    var tok = null;
    if (v === 'INVEST') tok = tokens.VERDICT_POSITIVE;
    else if (v === 'WATCH') tok = tokens.VERDICT_WATCH;
    else if (v === 'DO_NOT_INVEST') tok = tokens.VERDICT_NEGATIVE;
    else if (v) tok = tokens.VERDICT_BLOCK;
    toks.push(tok || null);
  }
  dt10BulkPaint_(rng, toks);   // v1.13.11
}
function dt10ApplyColFormats_(sheet, firstDataRow, count, fmtByCol) {
  if (firstDataRow < 0 || !count) return;
  for (var col in fmtByCol) {
    if (fmtByCol.hasOwnProperty(col)) {
      sheet.getRange(firstDataRow, parseInt(col, 10), count, 1)
          .setNumberFormat(fmtByCol[col]);
    }
  }
}
/**
 * v1.2.1 — color the ALL QUALIFIED "Selected" column by status. Reads each
 * row's selected/deferral straight from the qualified array (in render order),
 * so colors are driven by the data, not a re-parse of the cell. §3.4 tokens:
 *   selected -> VERDICT_POSITIVE (green); cap-deferred -> VERDICT_WATCH
 *   (amber); ranked-below-cut -> VERDICT_BLOCK (grey).
 */
function dt10ColorQualified_(sheet, firstDataRow, count, col, qualified,
                             tokens) {
  if (firstDataRow < 0 || !count) return;
  var toks = [];
  for (var i = 0; i < count; i++) {
    var c = qualified[i] || {};
    var tok;
    if (c.selected === true) tok = tokens.VERDICT_POSITIVE;
    else if (dt10HasValue_(c.deferral)) tok = tokens.VERDICT_WATCH;
    else tok = tokens.VERDICT_BLOCK;
    toks.push(tok || null);
  }
  dt10BulkPaint_(sheet.getRange(firstDataRow, col, count, 1), toks);   // v1.13.11
}
// ---------------------------------------------------------------------------
// Main refresh
// ---------------------------------------------------------------------------
function refreshDecisionTop10() {
  dt10PropMemoReset_();   // v1.11.13 [P-202]: fresh property reads per run
  var run = { statusCell: null, markerOwned: false };   // v1.13.10: a throw ends in FAILED
  try {
    dt10RefreshDecisionTop10Run_(run);
  } catch (eRun) {
    dt10RefreshFailed_(run, eRun);
    throw eRun;
  }
}
/** v1.13.10: a refresh that THROWS must not leave the banner at 'running...'
 * (live 20 Oct: cockpit stuck RUNNING with no grid). Writes a terminal
 * 'status: FAILED' line (the Riyadh 'Last run' stamp the audit parses, plus
 * failed_at_utc and a short error), upserts cockpit _Status ERROR and clears
 * the P-195 marker THIS run stored (a reported failure is not an abort); a
 * throw before the marker check leaves a previous run's stale marker for the
 * next check to report as ABORTED_PREV. Never throws; the caller rethrows. A
 * hard Apps Script timeout cannot be caught: that case still leaves RUNNING
 * and is reported by the next run's P-195 check. */
function dt10RefreshFailed_(run, err) {
  var msg = String(err && err.message ? err.message : err)
      .replace(/\s+/g, ' ').slice(0, 200);
  var utc = '';
  try { utc = new Date().toISOString().replace(/\.\d{3}Z$/, 'Z'); } catch (eIso) {}
  try {
    var cell = run && run.statusCell;
    if (!cell) {
      var sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(DT10_SHEET);
      cell = sh ? sh.getRange(DT10_ROW_STATUS, 2) : null;
    }
    if (cell) {
      cell.setValue(dt10StatusLine_('FAILED',
          'failed_at_utc=' + utc + ' | ' + msg + dt10StaleNote_()));
    }
  } catch (eCell) {}
  try {
    dt10WritePageStatus_('ERROR', 'Decision cockpit: FAILED at ' + utc + ' \u2014 ' + msg,
        '', '', '', []);
  } catch (eSt) {}
  if (run && run.markerOwned) {   // only the marker this run stored
    try { dt10RunMarkerClear_(); } catch (eMk) {}
  }
  try { SpreadsheetApp.flush(); } catch (eFl) {}
}
function dt10RefreshDecisionTop10Run_(run) {
  var t0 = new Date().getTime();
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(DT10_SHEET);
  // v1.3.0 layout guard: the panel grew a row (KPI head moved 13 -> 14), so a
  // v1.2.x sheet would take KPI/zone writes one row high. 'KPIs' at the NEW
  // head row is the layout fingerprint; anything else auto-rebuilds once.
  if (!sheet || sheet.getRange(DT10_ROW_PANEL_HEAD, 1).getValue() === '' ||
      sheet.getRange(DT10_ROW_KPI_HEAD, 1).getValue() !== 'KPIs') {
    sheet = buildDecisionTop10Layout();
  }
  var tokens = dt10Tokens_(ss);
  var statusCell = sheet.getRange(DT10_ROW_STATUS, 2);
  run.statusCell = statusCell;                              // v1.13.10
  statusCell.setValue(dt10StatusLine_('running\u2026', ''));
  SpreadsheetApp.flush();
  var panel = dt10ReadPanel_(sheet, ss);
  // v1.13.4 [WP-8]: input-truth gates -- three memoized property reads;
  // off (default) => no feature input reads; a narrow owned-KPI read
  // prevents prior enforce cells surviving an off/property-failure run.
  var itMode = dt10InputTruthMode_();
  var candMode = dt10CandTabMode_();
  var vintMode = dt10VintageColsMode_();
  // v1.19.0 [SEAT-LIVE-FILL] / [OWNER-RULE]: one read each; off (default)
  // => nothing is attached to the payload and the render is v1.18.0.
  var liveOn = dt10SeatLiveFillOn_();
  var ruleOn = dt10OwnerRuleMarkOn_();
  if (itMode !== 'enforce') dt10InputTruthKpiState_(sheet, itMode, null);
  var itRef = null;
  if (itMode !== 'off') {
    itRef = dt10PanelRefCore_(panel, dt10PanelDefaults_(ss),
        dt10PfPanelRead_(ss, DT10_IT_REF_LABELS), dt10PolicyRefState_());
  }
  // v1.11.12 [P-195]: judge the previous run's marker, then store ours.
  dt10RunMarkerCheck_('refreshDecisionTop10');
  run.markerOwned = true;   // v1.13.10: a later throw clears this run's marker
  // v1.11.12 [SYNC-INFLIGHT]: observe => token only; enforce => HELD.
  var inflight = dt10SyncInflight_(ss);
  var inflightNote = '';
  if (inflight && inflight.active) {
    inflightNote = 'inflight=' + inflight.detail +
                   (inflight.mode === 'enforce' ? '' : ' (observe)');
    if (inflight.mode === 'enforce') {
      var heldLine = dt10StatusLine_('HELD',
          'sync in flight -- ' + inflight.detail +
          ' | board kept from the last render; re-run after the ' +
          'Global_Markets leg lands' + dt10StaleNote_(), 'HELD');
      statusCell.setValue(heldLine);
      dt10WritePageStatus_('HELD', 'Decision cockpit: ' + heldLine, '', '',
          String(new Date().getTime() - t0), []);
      dt10RunLogRow_('INFO', 'HELD',
          'SYNC-INFLIGHT: cockpit deferred -- ' + inflight.detail,
          { version: DT10_VERSION, run: inflight.run, mode: inflight.mode });
      dt10RunMarkerClear_();
      return;
    }
  }
  var poolLimit = parseInt(panel['Pool Limit'], 10);
  // v1.2.3: blank / 0 / non-positive Pool Limit == scan all available input.
  if (!(poolLimit > 0)) poolLimit = DT10_POOL_HARD_CAP;
  var useSheets = String(panel['Pool Source'] || 'Sheets')
      .toLowerCase().indexOf('backend') === -1;
  var body = {
    criteria: dt10CriteriaFromPanel_(panel),
    fx_rates: dt10FxRates_(ss),
    portfolio: dt10PortfolioInputs_(ss, panel),
    pool_limit: poolLimit
  };
  // v1.5.0 (Fix H1): give the builder's Portfolio gate the held symbols it
  // needs to enforce "Include Portfolio Holdings = No". Kill switch
  // DT10_SEND_HOLDINGS=0 restores the exact v1.4.0 body (key absent).
  var heldNote = '';
  if (dt10SendHoldingsEnabled_()) {
    var heldRows = body.portfolio.holdings || [];
    if (heldRows.length) {
      body.portfolio.holdings = heldRows;
      heldNote = ' | held=' + heldRows.length + ' sent';
    } else {
      heldNote = body.portfolio.holdings_input_incomplete ?
          ' | held=unknown (My_Portfolio incomplete/unreadable; funding withheld)' :
          ' | held=0 (empty source; reconciliation evidence required)';
    }
  }
  // v1.11.7 [P-134]: cash source-of-truth. Default mode 'panel' leaves the
  // sent value and the status line byte-identical.
  var dt10CashMode = dt10CashSourceMode_();
  var cashNote = '';
  if (dt10CashMode !== 'panel') {
    var dt10PfCash = dt10CashFromPortfolio_(ss);
    var dt10Chosen = dt10CashChoose_(dt10CashMode,
        body.portfolio.cash_available_sar, dt10PfCash);
    body.portfolio.cash_available_sar = dt10Chosen.sent;
    cashNote = dt10Chosen.note;
  }
  // v1.16.0 [FUNDING-EVIDENCE]: the published broker packet certifies THIS
  // request (cash after reservations, links, proven FX). Default off = the
  // body above byte for byte; the replay deep-copies the applied body.
  var fundingNote = dt10FundingApply_(body, Date.now());
  var poolNote = 'backend selector';
  var dt10Outage = null;   // v1.11.10 [P-168]: sheet-pool outage sightings
  // Capture source generations BEFORE reading pool rows. A late source
  // publication cannot relabel this older in-memory board as current.
  var sourceContext = dt10SourceCapture_(ss);
  var pool = null;
  if (useSheets) {
    pool = dt10CollectPoolRows_(ss, poolLimit, sourceContext);
    sourceContext.pool_counts = pool.available;
    sourceContext.pool_scope_receipts = pool.approved_scope || {};
    sourceContext.pool_included_counts = pool.perPage;
    sourceContext.pool_sent_total = pool.rows.length;
    sourceContext.pool_reconciliation = pool.pool_reconciliation;
    // Empty sheet input is explicit; never silently switch to another pool.
    body.rows = pool.rows;
    if (pool.rows.length) {
      dt10Outage = dt10OutageMapFromPool_(pool.rows);   // v1.11.10 [P-168]
    }
    poolNote = dt10PoolNote_(pool, dt10SourceReadiness_(ss, sourceContext));
  }
  Logger.log('[DT10 v' + DT10_VERSION + '] POST ' + DT10_ENDPOINT + ' | ' +
             poolNote);
  body.criteria.board_funding_stage = 'research';
  // v1.19.1: held seats kept in the backend's written audit (display only).
  if (dt10AuditKeepOn_()) {
    try {
      var akSyms = dt10AuditKeepSymbols_(dt10StabLoad_());
      if (akSyms.length) body.criteria.audit_keep_symbols = akSyms;
    } catch (eAk) { /* send nothing: the unarmed body */ }
  }
  var resp;
  try {
    resp = pool && !pool.rows.length ?
        {code: 200, json: dt10EmptySheetPayload_(), local: true} : dt10Post_(body);
    if (resp.local && candMode === 'enforce') {
      // This identifies a local empty publication only; it does not attest
      // any backend/provider acquisition. Each refresh gets a distinct run.
      resp.json.meta.run_id = 'native-empty-sheet-pool:' + new Date().toISOString() +
          ':' + Utilities.getUuid();
    }
  } catch (eNet) {
    statusCell.setValue(dt10StatusLine_('NETWORK ERROR',
        String(eNet) + dt10StaleNote_()));
    // v1.4.0: cockpit-owned _Status — failure is reported, never silent.
    dt10WritePageStatus_('ERROR',
        'Decision cockpit: NETWORK ERROR — ' + String(eNet).slice(0, 300),
        '', '', '', []);
    dt10RunMarkerClear_();   // v1.11.12 [P-195]
    return;
  }
  if (resp.code !== 200 || !resp.json) {
    statusCell.setValue(dt10StatusLine_('HTTP ' + resp.code,
        (resp.text || '').slice(0, 180) + dt10StaleNote_()));
    // v1.4.0: cockpit-owned _Status — failure is reported, never silent.
    dt10WritePageStatus_('ERROR',
        'Decision cockpit: HTTP ' + resp.code + ' — ' +
        (resp.text || '').slice(0, 180),
        resp.code, '', '', []);
    dt10RunMarkerClear_();   // v1.11.12 [P-195]
    return;
  }
  var payload = resp.json;
  // v1.6.3: a fail-soft 200 is NOT a refresh — never let it erase a good board.
  var degraded = dt10PayloadDegraded_(payload);
  if (degraded) {
    statusCell.setValue(dt10StatusLine_('DEGRADED',
        degraded + dt10StaleNote_()));
    dt10WritePageStatus_('WARN',
        'Decision cockpit: degraded payload (' + degraded +
        ') — board preserved, not refreshed', resp.code, '', '', []);
    dt10RunMarkerClear_();   // v1.11.12 [P-195]
    return;
  }
  // v1.3.0: membership hysteresis between response and render. The audit
  // grid / qualified / near-miss zones keep the backend's RAW truth; only
  // the SELECTED board gains memory.
  payload._dt10_source_context = sourceContext;
  // v1.13.4 [WP-8]: private render inputs, attached ONLY when a gate is
  // armed (off => the payload object is byte-identical to v1.13.3).
  if (itMode !== 'off') payload._dt10_it = { mode: itMode, ref: itRef };
  if (candMode !== 'off') payload._dt10_cand_mode = candMode;
  if (vintMode !== 'off') {
    payload._dt10_vint = { mode: vintMode,
                           idx: pool ? dt10PoolVintageIndex_(pool.rows) : null };
  }
  // v1.19.0 [SEAT-LIVE-FILL]: the live index of the sheet pool rows already
  // collected and the FX map already read for the body; attached only when
  // armed (off => the payload object is byte-identical).
  if (liveOn) {
    payload._dt10_live = { idx: pool ? dt10SeatLiveIndex_(pool.rows) : null,
                           fx: body.fx_rates || {} };
  }
  // v1.19.0 [OWNER-RULE]: the marker column rides only when armed.
  if (ruleOn) payload._dt10_rule = { mode: 'on' };
  payload._dt10_uv = dt10BoardVerdict_(ss, sourceContext);
  // Incomplete source evidence must not advance confirmation/miss memory.
  var stab = payload._dt10_uv.source_blocked === true ?
      {note: 'stab: frozen (source readiness withheld)'} :
      dt10ApplyStability_(payload, panel, dt10Outage);   // v1.11.10 [P-168]
  payload._dt10_uv = dt10BoardVerdict_(ss, sourceContext);
  if (resp.local !== true) dt10ReallocateBoard_(payload, body);
  dt10FinalizeBoard_(payload);
  // v1.6.6 (S-5): board-vs-backend KPI verification — token only.
  var kpiNote;
  // v1.8.0 (S-6): reconcile funded-pick KPI vs the board's executable set.
  var seatNote;
  var earn = dt10RenderPayload_(sheet, payload, tokens);
  // v1.13.4 [WP-8]: status tokens the render stored ('' when a gate is off).
  var itNote = payload._dt10_it_note || '';
  if (itMode !== 'off') {
    var itCal = dt10CalCoverageCore_(payload, earn && earn.map);
    payload._dt10_cal = itCal;
    itNote += itCal.note;
  }
  var candNote = payload._dt10_cand_note || '';
  var vintNote = payload._dt10_vint_note || '';
  // v1.19.0: status tokens the render stored ('' when a gate is off).
  var liveNote = payload._dt10_live_note || '';
  var ruleNote = payload._dt10_rule_note || '';
  var finalSource = payload._dt10_uv.source_readiness;
  if (pool) poolNote = dt10PoolNote_(pool, finalSource);
  else poolNote += ' | source readiness ' + (finalSource.ready ? 'VERIFIED (run ' + finalSource.cohort + ')' : 'WITHHELD: ' + finalSource.reason);
  if (resp.local === true) poolNote += ' | empty Sheets input; no backend request or allocation';
  kpiNote = dt10KpiCheckNote_(payload);
  seatNote = dt10SeatCheckNote_(payload);
  var secs = Math.round((new Date().getTime() - t0) / 100) / 10;
  var statusLine = dt10StatusLine_(payload._dt10_uv.source_blocked === true ? 'WITHHELD' : String(payload.status || '?'),
      poolNote + heldNote + cashNote + fundingNote + ' | ' + dt10MetaLine_(payload.meta) +
      (stab && stab.note ? ' | ' + stab.note : '') +
      (earn && earn.note ? ' | ' + earn.note : '') +
      (kpiNote ? ' | ' + kpiNote : '') +
      (seatNote ? ' | ' + seatNote : '') +
      (inflightNote ? ' | ' + inflightNote : '') +   // v1.11.12
      (itNote ? ' | ' + itNote : '') +               // v1.13.4 [WP-8]
      (candNote ? ' | ' + candNote : '') +
      (vintNote ? ' | ' + vintNote : '') +
      (liveNote ? ' | ' + liveNote : '') +               // v1.19.0 [SEAT-LIVE-FILL]
      (ruleNote ? ' | ' + ruleNote : '') +               // v1.19.0 [OWNER-RULE]
      dt10FunnelNote_(payload) + ' | ' + secs + 's',
      dt10OutputStatus_(payload));   // v1.11.0 (2)
  // v1.3.1: the cockpit OWNS the selection audit trail — log the board it
  // just rendered (in-memory; 17_Selection_Log.gs scraping retired). The
  // Run Info column carries this exact status line, pre-'SelLog' tail.
  var selLogNote = dt10AppendSelectionLog_(ss, payload.selected || [],
                                           statusLine, panel,
                                           dt10OutputStatus_(payload), sourceContext);  // v1.11.2 [P-62]
  // v1.6.6 (S-4): membership departures logged with an Outcome stamp.
  var exitNote = dt10AppendExitLog_(ss,
      (stab && stab.audit) ? stab.audit : {}, statusLine);
  // v1.17.0 [SHADOW-RANK-LOG]: the backend's ranking challenger, logged
  // ('' when DT10_SHADOW_RANK_LOG is off -> the v1.16.0 status line).
  var shadowNote = dt10AppendShadowRankLog_(ss, payload,
      finalSource && finalSource.ready ? finalSource.cohort : '');
  var finalStatusLine = selLogNote ? statusLine + ' | ' + selLogNote
                                   : statusLine;
  if (exitNote) finalStatusLine += ' | ' + exitNote;
  if (shadowNote) finalStatusLine += ' | ' + shadowNote;   // v1.17.0
  statusCell.setValue(finalStatusLine);
  // v1.13.4 [WP-8]: ONE _Run_Log INPUT-TRUTH row when any gate is armed.
  dt10InputTruthRunLog_(itMode, candMode, vintMode, payload, itRef,
                        [itNote, candNote, vintNote]);
  // v1.6.2: remember this good render so a later 502 can date the board.
  dt10RememberSuccess_(
      Utilities.formatDate(new Date(),
          Session.getScriptTimeZone() || 'Asia/Riyadh', 'yyyy-MM-dd HH:mm'),
      (payload.selected || []).length);
  // v1.4.0: cockpit-owned _Status — same text as the banner, one truth.
  dt10WritePageStatus_(payload._dt10_uv.source_blocked === true ? 'WITHHELD' : 'OK', finalStatusLine, resp.local === true ? '' : resp.code,
      (payload.selected || []).length, Math.round(secs * 1000), []);
  Logger.log('[DT10 v' + DT10_VERSION + '] done status=' + payload.status +
             ' in ' + secs + 's');
  dt10RunMarkerClear_();   // v1.11.12 [P-195]: normal completion
}
/** v1.11.1: MORNING COCKPIT REFRESH — time-driven entry point (the 08:07
 * Riyadh trigger). UI-free: no getUi()/alert/toast; the render's banner and
 * the cockpit-owned _Status upsert carry the outcome. Logs the feed verdict
 * it saw so the morning log shows whether the GM leg had landed. Any throw
 * is logged and reported through _Status; never rethrown. */
function tfbMorningCockpitRefresh() {
  var t0 = new Date().getTime();
  try {
    var seen = '';
    try {
      var uv = dt10UvParse_(dt10UvRead_(SpreadsheetApp.getActiveSpreadsheet()),
                            Date.now());
      seen = uv.state + (uv.reason ? '(' + uv.reason + ')' : '') +
             (uv.ageMin != null ? ' age ' + Math.round(uv.ageMin) + 'm' : '');
    } catch (eUv) { seen = 'verdict read failed'; }
    Logger.log('[DT10 v' + DT10_VERSION + '] morning cockpit refresh ' +
               '(time-driven) | feed: ' + seen);
    refreshDecisionTop10();
    // v1.11.2 [P-67]: the morning run leaves a _Run_Log row like the
    // 4-hourly wrapper does (cockpit _Status already carries the payload).
    dt10MorningRunLog_('OK',
        'Morning cockpit refresh (time-driven) completed | feed: ' + seen,
        new Date().getTime() - t0, { version: DT10_VERSION, feed: seen });
  } catch (eMc) {
    Logger.log('[DT10 v' + DT10_VERSION + '] morning cockpit refresh ' +
               'FAILED: ' + eMc);
    dt10WritePageStatus_('ERROR',
        'Morning cockpit refresh failed: ' + String(eMc).slice(0, 300),
        '', '', String(new Date().getTime() - t0), []);
    dt10MorningRunLog_('ERROR',
        'Morning cockpit refresh failed: ' + String(eMc).slice(0, 300),
        new Date().getTime() - t0, { version: DT10_VERSION });   // v1.11.2
    dt10RunMarkerClear_();   // v1.11.12 [P-195]: a logged failure is not an abort
  }
}
/** v1.11.2 [P-67]: ONE _Run_Log row for the time-driven morning refresh
 *  (13_AutoRefresh.gs logs its own 4-hourly runs; this trigger logged only
 *  to Logger). 10-column house format (Timestamp | Level | Action | Page |
 *  Status | Message | Endpoint | HTTP Code | Duration ms | Details JSON).
 *  Toggle-gated; never throws; returns true only when a row was written. */
function dt10MorningRunLog_(status, message, durationMs, details) {
  if (!DT10_V1112_MORNING_RUNLOG) return false;
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var sh = ss.getSheetByName('_Run_Log');
    if (!sh) return false;
    var dj = '{}';
    try { dj = JSON.stringify(details || {}); } catch (eJ) {}
    sh.appendRow([new Date(), (status === 'OK' ? 'INFO' : 'ERROR'),
                  'tfbMorningCockpitRefresh', DT10_SHEET, String(status || ''),
                  String(message || '').slice(0, 400), '', '',
                  String(durationMs || ''), dj]);
    return true;
  } catch (e) {
    return false;
  }
}
/* ---- v1.9.0 W1A-4a consumer: upstream-feed verdict gate ---- */
var DT10_PROP_UV = 'DT10_UPSTREAM_VERDICT';   /* 'off' => v1.8.10 path */
var DT10_UV_KEY = 'TFB Decision Feed';
/* ---------------------------------------------------------------------------
 * v1.10.2 (2026-08-26) — VERDICT AGE: DUAL-CLOCK RESOLUTION + FUTURE FAIL-CLOSED
 * EVIDENCE: the live 2026-08-26 10:04 board rendered
 *   '\u2705 FEED ACTIONABLE \u2014 EXECUTABLE (verdict age -162m)'.
 * The producer stamps the verdict in NAIVE RIYADH wall-clock while
 * dt10UvParse_ read it as UTC (Date.UTC), shifting every age by -180m; and
 * the only age check was '> 480m', so a NEGATIVE (future) age had no guard
 * at all and sailed to EXECUTABLE. Fix, fail-closed on every ambiguity:
 *   1. The naive timestamp is interpreted under BOTH clocks (UTC and
 *      Riyadh UTC+3); the plausible (>= -skew) interpretation with the
 *      smallest age wins — a Riyadh-stamped verdict reads its true age
 *      (+18m, not -162m), and a future UTC stamp can no longer hide.
 *   2. If NO interpretation is plausible, the verdict is from the future:
 *      NOT_ACTIONABLE('verdict timestamp Nm in the future — clock/timezone
 *      mismatch; fail-closed').
 * Stale (>480m) and every other branch byte-identical. ES5. ZERO functions
 * added or removed; ONE constant added (DT10_UV_FUTURE_SKEW_MIN).
 * ------------------------------------------------------------------------- */
var DT10_UV_MAX_AGE_MIN = 480;   /* verdict older than this is itself stale */
var DT10_UV_FUTURE_SKEW_MIN = 15; /* v1.10.2: tolerated clock skew (minutes) */

function dt10UvOn_() {
  try {
    var v = String(PropertiesService.getScriptProperties()
        .getProperty(DT10_PROP_UV) || '').toLowerCase();
    return v !== 'off';
  } catch (e) { return true; }   /* prop failure never disables the gate */
}

/** v1.11.11 [P-144]: read ANY L:M key's value from _Status (the composite
 *  reader below is byte-identical; this variant takes the key). '' on miss. */
function dt10UvReadKeyed_(ss, key) {
  try {
    var sh = ss.getSheetByName('_Status');
    if (!sh) { return ''; }
    var want = String(key || '').replace(/^\s+|\s+$/g, '').toLowerCase();
    if (!want) { return ''; }
    var vals = sh.getRange('L1:M60').getValues();
    for (var i = 0; i < vals.length; i++) {
      var k = String(vals[i][0] || '').replace(/^\s+|\s+$/g, '');
      if (k.toLowerCase() === want) {
        return String(vals[i][1] || '');
      }
    }
    return '';
  } catch (e) { return ''; }
}
/** Read the composite verdict value from _Status L1:M60. '' on any miss. */
function dt10UvRead_(ss) {
  try {
    var sh = ss.getSheetByName('_Status');
    if (!sh) { return ''; }
    var vals = sh.getRange('L1:M60').getValues();
    for (var i = 0; i < vals.length; i++) {
      var k = String(vals[i][0] || '').replace(/^\s+|\s+$/g, '');
      if (k.toLowerCase() === DT10_UV_KEY.toLowerCase()) {
        return String(vals[i][1] || '');
      }
    }
    return '';
  } catch (e) { return ''; }
}

/** PURE parser — fail-closed on every ambiguity. Returns
 *  { state:'EXECUTABLE'|'NOT_ACTIONABLE', reason:'', ageMin:null|Number }. */
function dt10UvParse_(raw, nowMs) {
  var out = { state: 'NOT_ACTIONABLE', reason: '', ageMin: null };
  var s = String(raw || '').replace(/^\s+|\s+$/g, '');
  if (!s) {
    out.reason = 'no verdict published (arm TFB_SYNC_UPSTREAM_VERDICT)';
    return out;
  }
  var token = s.split('|')[0].replace(/^\s+|\s+$/g, '');
  var m = token.match(/^NOT_ACTIONABLE\((.*)\)$/);
  var ts = s.match(/(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})(?:([+-])(\d{2}):?(\d{2}))?/);
  if (!ts) {
    out.reason = 'verdict timestamp unreadable';
    return out;
  }
  var utc = Date.UTC(+ts[1], +ts[2] - 1, +ts[3], +ts[4], +ts[5], +ts[6]);
  if (ts[7]) {
    /* v1.10.3: explicit offset present - ONE exact interpretation. */
    var offMin = (+ts[8]) * 60 + (+ts[9]);
    if (ts[7] === '-') { offMin = -offMin; }
    out.ageMin = (nowMs - (utc - offMin * 60000)) / 60000;
    if (out.ageMin < -DT10_UV_FUTURE_SKEW_MIN) {
      out.reason = 'verdict timestamp ' + Math.round(-out.ageMin) +
          'm in the future \u2014 clock mismatch; fail-closed';
      return out;
    }
    if (out.ageMin > DT10_UV_MAX_AGE_MIN) {
      out.reason = 'verdict stale (' + Math.round(out.ageMin) + 'm old)';
      return out;
    }
    if (token === 'EXECUTABLE') { out.state = 'EXECUTABLE'; return out; }
    out.reason = m ? m[1] : token;
    return out;
  }
  /* v1.10.2: the stamp is NAIVE - resolve it under both producer clocks
   * and keep the smallest plausible (>= -skew) age; fail closed when the
   * verdict is from the future under every interpretation. */
  var ageAsUtc = (nowMs - utc) / 60000;
  var ageAsRiyadh = ageAsUtc + 180;      /* naive treated as UTC+3 */
  var candidates = [];
  if (ageAsRiyadh >= -DT10_UV_FUTURE_SKEW_MIN) { candidates.push(ageAsRiyadh); }
  if (ageAsUtc >= -DT10_UV_FUTURE_SKEW_MIN) { candidates.push(ageAsUtc); }
  out.ageMin = candidates.length ? Math.min.apply(null, candidates)
                                 : Math.max(ageAsRiyadh, ageAsUtc);
  if (out.ageMin < -DT10_UV_FUTURE_SKEW_MIN) {
    out.reason = 'verdict timestamp ' + Math.round(-out.ageMin) +
        'm in the future \u2014 clock/timezone mismatch; fail-closed';
    return out;
  }
  if (out.ageMin > DT10_UV_MAX_AGE_MIN) {
    out.reason = 'verdict stale (' + Math.round(out.ageMin) + 'm old)';
    return out;
  }
  if (token === 'EXECUTABLE') {
    out.state = 'EXECUTABLE';
    return out;
  }
  out.reason = m ? m[1] : token;
  return out;
}

function dt10RenderPayload_(sheet, payload, tokens) {
  // Clear dynamic zones (breakApart first: section headers and empty-state
  // lines are merged ranges; writing over stale merges throws in GAS).
  var lastRow = sheet.getMaxRows();
  if (lastRow >= DT10_ZONES_START) {
    var zone = sheet.getRange(DT10_ZONES_START, 1,
                              lastRow - DT10_ZONES_START + 1,
                              sheet.getMaxColumns());
    zone.breakApart();
    zone.clear();
  }
  // Recheck after sheet preparation and before any financial values are
  // painted. The pre-pool generation must still match the latest sources.
  payload._dt10_uv = dt10BoardVerdict_(sheet.getParent(), payload._dt10_source_context || {});
  dt10FinalizeBoard_(payload);
  // v1.19.0 [SEAT-LIVE-FILL]: live numbers on grace / fast-track / held
  // seats after the last note-replacing finalization ('' when the refresh
  // attached no payload._dt10_live -> nothing touched, no key stored).
  var liveFillNote = dt10SeatLiveFill_(payload);
  if (liveFillNote) payload._dt10_live_note = liveFillNote;
  // v1.13.2: annotate after the last note-replacing finalization, before
  // painting the board. The refresh status and selection log use this result.
  var earn = dt10EarningsAnnotate_(payload, sheet.getParent());
  // v1.18.0 [POST-EARNINGS]: the post-earnings advisor tag ('' when
  // DT10_POST_EARN is off -> no sheet read, earn and every note untouched).
  var postEarn = dt10PostEarnAnnotate_(payload, sheet.getParent());
  if (postEarn) earn.note = earn.note ? earn.note + ' | ' + postEarn : postEarn;
  // KPI strip
  sheet.getRange(DT10_ROW_KPI_LABELS, 1).setValue(DT10_KPI_LABELS[0]);
  var kpiRange = sheet.getRange(DT10_ROW_KPI_VALUES, 1, 1,
                                DT10_KPI_LABELS.length);
  kpiRange.setValues([dt10KpiValues_(payload.kpis)]);
  // v1.8.8 (G-a): seat-truth Selected cell — toggle-gated, fail-open
  // ('' => the v1.8.7 cell stands).
  if (dt10SeatTruthOn_()) {   // v1.11.12 [P-149]
    var _stKpi = dt10SeatTruthKpi_(payload);
    if (_stKpi) sheet.getRange(DT10_ROW_KPI_VALUES, 3).setValue(_stKpi);
  }
  var kpiFmts = [DT10_FMT_SAR, DT10_FMT_SAR, '@', '0.0', '0.00',
                 DT10_FMT_INT, DT10_FMT_INT, DT10_FMT_SAR];
  for (var kf = 0; kf < kpiFmts.length; kf++) {
    sheet.getRange(DT10_ROW_KPI_VALUES, kf + 1).setNumberFormat(kpiFmts[kf]);
  }
  var row = DT10_ZONES_START;
  var selected = payload.selected || [];
  var nearMiss = payload.near_miss || [];
  var alerts = payload.alerts || [];
  // v1.11.8 [P-142]: funding-narrative containment — a WITHHELD board
  // renders NO funding solicitation (capital_call / unfunded_candidates
  // alerts dropped; NEAR MISS funding text replaced; one countable
  // 'funding_withheld' disclosure row appended). Pure copies — payload
  // untouched. Kill: Script Property DT10_FUNDING_WITHHOLD_LEGACY='1'.
  var _p142 = dt10ContainFunding_(nearMiss, alerts,
                                  dt10OutputStatus_(payload));
  nearMiss = _p142.nearMiss;
  alerts = _p142.alerts;
  var cands = payload.candidates_rows || [];
  // v1.13.4 [WP-8] DT10_INPUT_TRUTH: evaluability facts + KPI cells L15:M16
  // (enforce only). Pure reads of the audit rows; payload.alerts and the
  // ALERTS table are untouched; off only checks for previously owned KPIs.
  var _it = payload._dt10_it || { mode: 'off' };
  if (_it.mode !== 'off') {
    var _ev = dt10GateEvaluabilityCore_(cands,
        (payload.meta && payload.meta.route) ?
            payload.meta.route.news_display : null);
    payload._dt10_it_ev = _ev;
    payload._dt10_it_note = dt10GateEvaluabilityNote_(_ev, _it.mode) +
                            dt10PanelRefNote_(_it.ref, _it.mode);
  }
  dt10InputTruthKpiState_(sheet, _it.mode, payload._dt10_it_ev || null);
  // SELECTED: finalization above already applied funding and feed controls.
  // The upstream token may label input readiness but never invent a ticket.
  var dt10Uv = payload._dt10_uv;
  var dt10SelTitle = dt10SelectedTitle_(payload, dt10UvOn_() || dt10Uv.source_blocked === true);
  if (dt10UvOn_()) {
    Logger.log('[DT10 v' + DT10_VERSION + '] upstream verdict: ' +
        (dt10Uv.state === 'EXECUTABLE' ? 'EXECUTABLE' : dt10Uv.reason) +
        '; final board: ' + dt10OutputStatus_(payload));
  }
  payload._dt10_uv = dt10Uv;   /* v1.11.1: the verdict the render acted on */
  /* v1.11.6 [K-1b]: the withheld-truth KPI pass runs HERE, where the
   * verdict is authoritative -- the v1.11.5 site ran before dt10Uv was
   * read, so _dt10_uv was absent and the classifier fell through to HELD
   * (caught live, 2026-09-08 00:28 board). One corrective setValues on
   * the same kpiRange; EXECUTABLE feeds skip it entirely. */
  if (dt10Uv.state !== 'EXECUTABLE' && dt10KpiTruthEnabled_() &&
      dt10OutputStatus_(payload) === 'WITHHELD') {
    kpiRange.setValues([dt10KpiWithheldValues_(payload.kpis)]);
  }
  row = dt10WriteSection_(sheet, row, dt10SelTitle, tokens);
  // v1.13.4 [WP-8] DT10_VINTAGE_COLS: enforce appends two research-context
  // cells AFTER Advisor Note (idx 30/31); withhold indices (<= 29) and
  // DT10_UV_BOARD_NOTE_IDX are untouched. off => 30 columns as before.
  var _vt = payload._dt10_vint || { mode: 'off' };
  var selHeaders = DT10_SELECTED_HEADERS;
  if (_vt.mode === 'enforce') {
    selHeaders = DT10_SELECTED_HEADERS.concat(DT10_VINTAGE_HEADERS);
    if (sheet.getMaxColumns() < selHeaders.length) {
      sheet.insertColumnsAfter(sheet.getMaxColumns(),
                               selHeaders.length - sheet.getMaxColumns());
    }
  }
  // v1.19.0 [OWNER-RULE]: one extra column after the seat block (after the
  // vintage columns when those are enforced); off => the headers above and
  // no key stored on the payload.
  var _or = !!(payload._dt10_rule && payload._dt10_rule.mode === 'on');
  if (_or) {
    selHeaders = selHeaders.concat(DT10_OWNER_RULE_HEADERS);
    if (sheet.getMaxColumns() < selHeaders.length) {
      sheet.insertColumnsAfter(sheet.getMaxColumns(),
                               selHeaders.length - sheet.getMaxColumns());
    }
    payload._dt10_rule_note = dt10OwnerRuleNote_(selected);
  }
  var selRows = [];
  for (var s = 0; s < selected.length; s++) {
    selRows.push(dt10TicketToRow_(selected[s]));
    if (_vt.mode === 'enforce') {
      var _vc = dt10VintageCells_(selected[s], _vt.idx);
      selRows[selRows.length - 1].push(_vc[0], _vc[1]);
    }
    if (_or) selRows[selRows.length - 1].push(dt10OwnerRuleCell_(selected[s]));   // v1.19.0
  }
  if (dt10Uv.state !== 'EXECUTABLE') {          /* v1.9.0 W1A-4a */
    for (var uvI = 0; uvI < selRows.length; uvI++) {
      /* v1.10.0 (R3): withhold the FULL order surface — entry, stop,
       * TPs, TP1/Ann ROI, gain and funds reconstruct an order, and the
       * advisor note said the numbers out loud. Engine ROI, Rel, DQ and
       * prices stay real (v1.8.10 doctrine). */
      dt10UvWithholdRow_(selRows[uvI], DT10_UV_BOARD_WITHHOLD_IDX,
                         DT10_UV_BOARD_NOTE_IDX, dt10Uv.reason);
    }
  }
  var selT = dt10WriteTable_(sheet, row, selHeaders, selRows,   // v1.13.4 [WP-8]
      'No qualifying opportunities under the current criteria — ' +
      'an honest empty result (L13). See All Qualified / Near Miss for what ' +
      'almost qualified.');
  dt10ApplyColFormats_(sheet, selT.firstDataRow, selT.count, {
    7: '0.0000', 8: DT10_FMT_PRICE, 9: DT10_FMT_PRICE, 11: DT10_FMT_SAR,
    12: DT10_FMT_INT, 13: DT10_FMT_PRICE, 14: DT10_FMT_PRICE,
    15: DT10_FMT_PRICE, 16: DT10_FMT_PCT, 17: DT10_FMT_PCT, 18: DT10_FMT_PCT,
    19: DT10_FMT_SAR, 20: '0.0', 21: '0.0',
    26: DT10_FMT_INT, 28: '0.0'  // v1.3.0: Days, Sm Score
  });
  if (_vt.mode === 'enforce' && selT.firstDataRow > 0) {   // v1.13.4 [WP-8]
    sheet.getRange(selT.firstDataRow, DT10_SELECTED_HEADERS.length + 1,
                   selT.count, DT10_VINTAGE_HEADERS.length).setNumberFormat('@');
  }
  if (_or && selT.firstDataRow > 0) {   // v1.19.0 [OWNER-RULE]: text cells
    sheet.getRange(selT.firstDataRow, selHeaders.length, selT.count, 1)
        .setNumberFormat('@');
  }
  if (_vt.mode !== 'off') {
    payload._dt10_vint_note = dt10VintageNote_(selected, _vt.idx, _vt.mode);
  }
  // v1.8.0 (D-4): grace rows tinted whole-row FIRST; the Stability-cell
  // coloring below then re-asserts its bold cell on top.
  dt10TintGraceRows_(sheet, selT.firstDataRow, selT.count, selected, tokens);
  // v1.3.0: color the Stability column (25) by state.
  dt10ColorStability_(sheet, selT.firstDataRow, selT.count, 25, tokens);
  if (selT.firstDataRow > 0) {
    sheet.getRange(selT.firstDataRow, DT10_LAST_COL, selT.count, 1)
        .setWrap(true).setFontSize(9);
  }
  row = selT.next + 1;
  // ALL QUALIFIED — full INVEST opportunity set (v1.2.0). Every name that
  // passed all gates, including those deferred by a diversification cap, so
  // the operator can pick beyond the auto-selected tickets.
  var qualified = dt10QualifiedFromCands_(cands);
  row = dt10WriteSection_(sheet, row,
      'ALL QUALIFIED — INVEST opportunity set (' + qualified.length + ')',
      tokens);
  // v1.6.5 (Fix BE-1): hand the mapper the stability-pending set and the real
  // seat count so it can name the actual reason instead of assuming a cut.
  var qPending = dt10StabPendingMap_(payload);
  var qSeats = (payload.selected || []).length;
  // v1.6.8 (Fix D-3): normalized symbol -> the ticket's OWN TP1-basis ROI,
  // so the QUAL table's 'ROI % (TP1)' column can never again print the
  // engine-basis figure under a TP1 label (2026-08-03 board: 35.0%/23.8%
  // where the tickets computed 17.5%/11.9%).
  var qTicketRoi = {};
  var _selT = payload.selected || [];
  for (var qt = 0; qt < _selT.length; qt++) {
    var _tk = dt10NormToken_(_selT[qt] && _selT[qt].symbol);
    if (_tk) qTicketRoi[_tk] = _selT[qt].roi_pct;
  }
  var qRows = [];
  for (var q2 = 0; q2 < qualified.length; q2++) {
    qRows.push(dt10QualToRow_(qualified[q2], q2 + 1, qPending,
                              qualified.length, qSeats, qTicketRoi));
  }
  var qT = dt10WriteTable_(sheet, row, DT10_QUAL_HEADERS, qRows,
      'None — no candidate passed every gate (INVEST) under the current ' +
      'criteria. See Near Miss and the full audit below for the closest.');
  dt10ApplyColFormats_(sheet, qT.firstDataRow, qT.count, {
    6: DT10_FMT_PCT, 7: DT10_FMT_PCT, 8: DT10_FMT_PCT, 9: DT10_FMT_NUM2,
    10: '0.0', 11: '0.0', 13: '0.0'
  });
  if (qT.firstDataRow > 0) {
    sheet.getRange(qT.firstDataRow, DT10_QUAL_HEADERS.length, qT.count, 1)
        .setWrap(true).setFontSize(9);
  }
  // v1.2.1: color the Selected column by status (green/amber/grey).
  dt10ColorQualified_(sheet, qT.firstDataRow, qT.count, 14, qualified, tokens);
  row = qT.next + 1;
  // NEAR MISS
  row = dt10WriteSection_(sheet, row,
      'NEAR MISS — closest to qualifying (' + nearMiss.length + ')',
      tokens);
  var nmRows = [];
  for (var n = 0; n < nearMiss.length; n++) {
    nmRows.push(dt10NearMissToRow_(nearMiss[n]));
  }
  var nmT = dt10WriteTable_(sheet, row, DT10_NEARMISS_HEADERS, nmRows,
      'None — no candidates were close to qualifying.');
  dt10ColorVerdicts_(sheet, nmT.firstDataRow, nmT.count, 5, tokens);
  row = nmT.next + 1;
  // ALERTS
  row = dt10WriteSection_(sheet, row, 'ALERTS (' + alerts.length + ')',
                          tokens);
  var alRows = [];
  for (var a = 0; a < alerts.length; a++) {
    alRows.push(dt10AlertToRow_(alerts[a]));
  }
  var alT = dt10WriteTable_(sheet, row, DT10_ALERT_HEADERS, alRows,
      'None — no data or budget alerts raised.');
  row = alT.next + 1;
  // DATA GAPS — failure breakdown (v1.2.4). Aggregates the audited candidates
  // by the gate each failed first, so the operator sees the binding
  // constraints and what to relax to grow the qualified set. Derived view of
  // candidates_rows; no payload/schema/column-contract change (§8). Pool-level
  // attrition (missing FX / valuation) stays in ALERTS above; this breakdown
  // is scoped to the AUDITED candidates (capped by TFB_OPP_AUDIT_ROWS_MAX).
  var gap = dt10FailureBreakdown_(cands);
  var gapHead = 'DATA GAPS — WHY CANDIDATES DIDN\u2019T QUALIFY';
  if (gap.total > 0) {
    gapHead += ' (' + gap.invest + ' of ' + gap.total +
               ' passed all gates' +
               (gap.rows.length ? '; top blocker: ' + gap.rows[0].gate +
                ', ' + gap.rows[0].count + ' names' : '') + ')';
  }
  row = dt10WriteSection_(sheet, row, gapHead, tokens);
  var gapRows = [];
  for (var g2 = 0; g2 < gap.rows.length; g2++) {
    gapRows.push(dt10GapToRow_(gap.rows[g2], gap.total));
  }
  var gapT = dt10WriteTable_(sheet, row, DT10_GAP_HEADERS, gapRows,
      'None — every audited candidate passed all gates, or the pool was ' +
      'empty (see Alerts and the Status line).');
  dt10ApplyColFormats_(sheet, gapT.firstDataRow, gapT.count, {
    2: DT10_FMT_INT, 3: DT10_FMT_PCT
  });
  if (gapT.firstDataRow > 0) {
    sheet.getRange(gapT.firstDataRow, DT10_GAP_HEADERS.length, gapT.count, 1)
        .setWrap(true).setFontSize(9);
  }
  row = gapT.next + 1;
  // CANDIDATES audit grid
  row = dt10WriteSection_(sheet, row,
      'CANDIDATES — FULL AUDIT — top ' + cands.length + ' rows written ' +
      '(every selected / qualified / near-miss row included; the low-' +
      'score tail is trimmed — full scan size is the Scanned KPI)',
      tokens);
  var cRows = [];
  for (var c = 0; c < cands.length; c++) {
    cRows.push(dt10CandToRow_(cands[c]));
  }
  // v1.13.4 [WP-8] DT10_CAND_TAB: enforce moves the grid to the hidden tab
  // (fail-open to today's in-cockpit grid on any write fault); off =>
  // identical calls in identical order (the else branch IS v1.13.3).
  var _cm = payload._dt10_cand_mode || 'off';
  var _tab = null;
  if (_cm === 'enforce') {
    _tab = dt10CandTabWrite_(sheet.getParent(), cRows,
        dt10CandTabStampLine_(payload.meta, cRows.length, new Date()), tokens,
        dt10CandRunId_(payload.meta));
    payload._dt10_cand_result = _tab;
  }
  var cT;
  if (_tab && _tab.ok) {
    cT = dt10WriteTable_(sheet, row, DT10_CAND_HEADERS, [],
        dt10CandPointerText_(cRows.length, dt10MetaLine_(payload.meta)));
  } else {
    cT = dt10WriteTable_(sheet, row, DT10_CAND_HEADERS, cRows,
        'No candidates — pool was empty (see Status line and Alerts).');
    dt10ApplyColFormats_(sheet, cT.firstDataRow, cT.count, {
      6: DT10_FMT_PRICE, 7: DT10_FMT_PRICE, 8: DT10_FMT_PCT, 9: DT10_FMT_PCT,
      10: DT10_FMT_PCT, 11: DT10_FMT_NUM2, 12: '0.0', 13: '0.0', 20: '0.0'
    });
    dt10ColorVerdicts_(sheet, cT.firstDataRow, cT.count, 18, tokens);
  }
  row = cT.next + 1;
  if (_cm !== 'off') {   // row is now the meta-footer row
    if (_tab && _tab.ok) {
      payload._dt10_cand_note = 'cands ' + cRows.length + ' -> ' +
          DT10_CANDTAB_SHEET + ' (cockpit footer row ' + row + ')';
    } else if (_cm === 'enforce') {
      payload._dt10_cand_note = 'cands ' + DT10_CANDTAB_SHEET + ' write ' +
          'FAILED (' + ((_tab && _tab.err) || 'unknown') + ')' +
          ' \u2014 grid kept in cockpit (footer row ' + row + ')';
    } else {
      payload._dt10_cand_note = 'cands ' + cRows.length + ' in cockpit (footer row ' +
          row + '; target <= ' + DT10_CANDTAB_COCKPIT_TARGET_ROWS + ') [observe]';
    }
    if (_cm === 'enforce' && row > DT10_CANDTAB_COCKPIT_TARGET_ROWS) {
      payload._dt10_cand_note += ' | layout OVERFLOW: ' + row + ' > ' +
          DT10_CANDTAB_COCKPIT_TARGET_ROWS + ' rows; all decision evidence retained';
    }
  }
  // Meta footer
  sheet.getRange(row, 1, 1, DT10_LAST_COL).merge()
      .setValue(dt10MetaLine_(payload.meta))
      .setFontColor(DT10_MUTED).setFontSize(8).setFontStyle('italic');
  return earn;
}
// ---------------------------------------------------------------------------
// Self-test
// ---------------------------------------------------------------------------
function dt10SelfTest() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var report = [];
  report.push('[DT10 v' + DT10_VERSION + '] self-test');
  report.push('backend: ' + dt10BackendUrl_());
  report.push('token set: ' + (dt10AppToken_() ? 'yes' : 'NO'));
  var fx = dt10FxRates_(ss);
  var fxCount = 0;
  for (var k in fx) {
    if (fx.hasOwnProperty(k)) fxCount++;
  }
  report.push('fx rates from TFB_FX_LOOKUP: ' + fxCount);
  // v1.13.4 [WP-8 / DQ-14, DQ-50]: US-cent quoted rows need a USX FX row.
  report.push('fx USX row: ' +
      (Object.prototype.hasOwnProperty.call(fx, 'USX') && fx.USD > 0 &&
       Math.abs(fx.USX - fx.USD / 100) < 1e-9 ?
       'valid USD/100 (' + fx.USX + ')' :
       'ABSENT \u2014 US-cent quoted Commodities_FX rows raise missing_fx; ' +
       'operator adds "USX | Subunit Of USD" to LC_FX_Table now (resolves ' +
       'via raw-key lookup); backend USX subunit fix tracked separately ' +
       '(finding usx-subunit)'));
  // v1.5.0 (Fix H1): holdings-to-Portfolio-gate wiring.
  report.push('send holdings to Portfolio gate: ' +
              (dt10SendHoldingsEnabled_() ? 'ON' :
               'OFF (DT10_SEND_HOLDINGS)'));
  var hMap = dt10HoldingFromRow_(['NMM.US', 'Navios', 'Industrials', 566],
                                 0, 2, 3);
  report.push('holdings row mapper: ' +
              ((hMap && hMap.symbol === 'NMM.US' &&
                hMap.sector === 'Industrials' && hMap.value_sar === 566) ?
               'ok' : 'FAIL'));
  var hLive = dt10CollectHoldings_(ss);
  report.push('My_Portfolio holdings visible to gate: ' + hLive.length);
  var defs = dt10PanelDefaults_(ss);
  var t10Defs = 0;
  for (var d in defs) {
    if (defs.hasOwnProperty(d) && d.indexOf('T10: ') === 0) t10Defs++;
  }
  report.push('T10 panel defaults found: ' + t10Defs +
              ' (expect 23; 18 is normal until 15_Lists_Config reseeds ' +
              'TFB_PANEL_DEFAULTS — built-in defaults cover the ' +
              'v1.3.0 stability knobs)');
  // v1.3.0: stability state summary.
  var stabState = dt10StabLoad_();
  var stabSyms = 0, stabMembers = 0;
  for (var sk in stabState.symbols) {
    if (stabState.symbols.hasOwnProperty(sk)) {
      stabSyms++;
      if (stabState.symbols[sk].member) stabMembers++;
    }
  }
  report.push('stability state: date=' + (stabState.date || '(none)') +
              ' members=' + stabMembers + ' tracked=' + stabSyms +
              ' (dt10StabilityReset() clears)');
  var pool = dt10CollectPoolRows_(ss, DT10_POOL_HARD_CAP);
  report.push('pool scan (available sheet rows): ' + pool.rows.length + ' of ' +
              pool.total + ' rows, per-sheet incl/avail ' +
              JSON.stringify(pool.perPage) + ' / ' +
              JSON.stringify(pool.available) +
              (pool.truncated ? ' (TRUNCATED)' : ' (full)'));
  var sourceLive = dt10SourceReadiness_(ss);
  report.push('source readiness: ' + (sourceLive.ready ? 'VERIFIED' : 'WITHHELD: ' + sourceLive.reason));
  var sourceClockTest = dt10SourceStampMs_('2026-10-10 12:00:00+03:00');
  report.push('source clock core: ' + ((sourceClockTest === Date.UTC(2026, 9, 10, 9) &&
      dt10SourceStampMs_('2026-10-10') === null &&
      dt10SourceStampMs_('2026-02-30 12:00:00+03:00') === null) ? 'ok' : 'FAIL'));
  if (pool.rows.length) {
    report.push('first pool row keys: ' +
                JSON.stringify(pool.rows[0]).slice(0, 300));
  }
  // v1.3.1: selection-log dry checks (pure — nothing is written).
  var slRow = dt10SelLogRowFromTicket_(
      { symbol: 'TEST', rank: 1, detail: {} }, 'stamp', 'run', '{}');
  var slMode = '';
  try {
    slMode = String(PropertiesService.getScriptProperties()
        .getProperty(DT10_SELLOG_MODE_PROP) || '(default: daily+change)');
  } catch (eSl) {}
  report.push('selection log: headers=' + DT10_SELLOG_HEADERS.length +
              ' rowCells=' + slRow.length + ' mode=' + slMode +
              ' sig(B,A)=' +
              dt10SelLogSignature_([{ symbol: 'B' }, { symbol: 'A' }]));
  // v1.17.0 [SHADOW-RANK-LOG]: pure rows core (nothing is written).
  var srlBuilt = dt10ShadowLogRows_({ meta: { rank_challenger: {
      version: '1.0', mode: 'shadow', builder: '1.28.0', pool: 2,
      overlap_top5: 2,
      champion: [{ symbol: 'A.US', live_rank: 1, shadow_rank: 2 },
                 { symbol: 'B.US', live_rank: 2, shadow_rank: 1 }],
      challenger: [{ symbol: 'B.US', live_rank: 2, shadow_rank: 1 },
                   { symbol: 'A.US', live_rank: 1, shadow_rank: 2 }] } },
      selected: [{ symbol: 'A.US', rank: 1, shadow_rank: 2 }] },
      'stamp', 'day', 'run');
  report.push('shadow rank log (' + DT10_SHADOW_LOG_PROP + '): ' +
              dt10ShadowLogMode_());
  report.push('shadow rank log core: ' +
      ((srlBuilt.rows.length === 5 &&
        srlBuilt.rows[0].length === DT10_SHADOW_LOG_HEADERS.length &&
        srlBuilt.sig === 'day|C:A.US,B.US|S:B.US,A.US|B:1:A.US' &&
        srlBuilt.rows[0][20] === 'v1.0' && srlBuilt.rows[0][21] === 'v1.28.0' &&
        dt10ShadowLogRows_({}, 's', 'd', 'c').rows.length === 0) ?
       'ok' : 'FAIL'));
  // v1.4.0: page-status dry check (pure — nothing is written).
  report.push('page-status upsert: writePageStatus_ ' +
              (typeof writePageStatus_ === 'function' ?
               'available' : 'ABSENT (deploy 02_Core.gs)') +
              ' | DT10_PAGE_STATUS=' +
              (dt10PageStatusEnabled_() ? 'on' : 'OFF'));
  // v1.6.0 (W-3): earnings-tag dry checks (pure — nothing is written).
  var eaFix = [['Control', ''],
               ['Symbol', 'Next Earnings Date', 'Days To Earnings'],
               ['EXE.US', '2026-07-28', 7],
               ['MRP.US', '2026-08-04', 14],
               ['FAR.US', '2026-08-05', 15],
               ['JUNK.X', '', ''],
               ['OLD.US', '2026-07-01', -3]];
  var eaMap = dt10EarningsMapFromValues_(eaFix, '2026-07-21');
  var eaTk = [{ symbol: 'EXE.US', advisor_note: 'INVEST' },
              { symbol: 'MRP.US', advisor_note: 'grace' },
              { symbol: 'FAR.US', advisor_note: 'x' },
              { symbol: 'NONE.US', advisor_note: 'y' }];
  var eaN = dt10ApplyEarningsTags_(eaTk, eaMap, 14);
  var eaAgain = dt10ApplyEarningsTags_(eaTk, eaMap, 14);
  report.push('earnings tag core: ' +
              ((eaMap['EXE.US'] === 7 && eaMap['MRP.US'] === 14 &&
                !eaMap.hasOwnProperty('OLD.US') &&
                !eaMap.hasOwnProperty('JUNK.X') &&
                eaN === 2 && eaAgain === 0 &&
                eaTk[0].advisor_note.indexOf('\u26a0 earnings \u22647d') === 0 &&
                eaTk[1].advisor_note.indexOf('\u26a0 earnings \u226414d') === 0 &&
                eaTk[2].advisor_note === 'x') ?
               'ok (2 tagged, 15d excluded, idempotent)' : 'FAIL'));
  var eaLive = dt10EarningsMap_(ss);
  var eaLiveN = 0;
  for (var eaK in eaLive) {
    if (eaLive.hasOwnProperty(eaK)) eaLiveN++;
  }
  report.push('earnings tag: ' +
              (dt10EarningsTagEnabled_() ? 'ON' :
               'OFF (DT10_EARNINGS_TAG)') +
              ' | horizon \u2264' + dt10EarningsTagDays_() +
              'd | Calendar_Events rows visible: ' + eaLiveN);
  // v1.8.0: strict hard-exit + seat-check + grace-rank dry checks (pure).
  var v182raw = '';
  try {
    v182raw = String(PropertiesService.getScriptProperties()
        .getProperty('DT10_HARD_VERDICT_STRICT') || '');
  } catch (e182) {}
  report.push('strict hard-exit (DT10_HARD_VERDICT_STRICT): ' +
              (dt10HardVerdictStrict_() ? 'ON' : 'OFF (default)') +
              ' [raw len=' + v182raw.length + ']');
  var v18sc = dt10SeatCheckNote_({ selected: [{ symbol: 'A', suggested_shares: 5 },
      { symbol: 'B', _grace_hold: true }], kpis: { selected_count: 3 } });
  var v18gr = dt10TicketToRow_({ _grace_hold: true, rank: 6, symbol: 'G',
                                 detail: {} });
  report.push('seat/grace core: ' +
              ((v18sc.indexOf('SEAT-CHECK kpi 3 funded vs board 1 exec') ===
                0 && v18gr[0] === '\u2014') ? 'ok' : 'FAIL'));
  /* v1.11.7 [P-134]: cash source-of-truth pure cores, incl. the LIVE
     exported PF row shape. */
  var v117m = dt10CashScan_([
      ['x'], [],
      ['PF: Cash Available SAR', '23,242.50', '',
       'PF: Target Cash %', '10.0']]);
  var v117n = dt10CashScan_([['no such label here', 123]]);
  var v117a = dt10CashChoose_('panel', 9026, { found: true, value: 23242.5 });
  var v117b = dt10CashChoose_('observe', 9026, { found: true, value: 23242.5 });
  var v117c = dt10CashChoose_('portfolio', 9026, { found: true, value: 23242.5 });
  var v117d = dt10CashChoose_('portfolio', 9026, { found: false, err: 'x' });
  report.push('cash source core: ' +
              ((v117m.found && v117m.value === 23242.5 && v117m.row === 3 &&
                !v117n.found &&
                v117a.sent === 9026 && v117a.note === '' &&
                v117b.sent === 9026 &&
                v117b.note.indexOf('observe') !== -1 &&
                v117c.sent === 23242.5 &&
                v117c.note.indexOf('cash=PF 23243') !== -1 &&
                v117d.sent === 9026 &&
                v117d.note.indexOf('PF unreadable') !== -1)
               ? 'ok' : 'FAIL'));
  /* v1.11.8 [P-142]: funding-narrative containment core (pure). */
  var v118al = [
    { type: 'missing_fx', count: 1, required_action: 'Add FX rate(s)' },
    { type: 'capital_call', count: 3,
      required_action: 'Deposit >= 37,650 SAR to take the top-3 ' +
                       'qualified ticket(s): IGG.L, GBCI.US, VNOM.US.' },
    { type: 'unfunded_candidates', count: 3,
      required_action: 'increase Cash Available' }];
  var v118nm = [
    { symbol: 'GBCI.US', failed_gate: 'Funding',
      current: '0 SAR (capital exhausted)',
      required: 'CAPITAL_CALL: deposit >= 5,000 SAR for a 5,000 SAR ' +
                'ticket (cash 50 SAR)',
      verdict: 'WATCH',
      improve_note: 'deployable capital to fund >= 1 lot' },
    { symbol: 'HBAN.US', failed_gate: 'Diversification',
      current: 'sector cap 2/2 (Financials)', required: 'within caps',
      verdict: 'INVEST', improve_note: 'Qualified (INVEST) - deferred' }];
  var v118w = dt10ContainFundingCore_(v118nm, v118al, 'WITHHELD', false);
  var v118e = dt10ContainFundingCore_(v118nm, v118al, 'EXECUTABLE', false);
  var v118h = dt10ContainFundingCore_(v118nm, v118al, 'HELD', false);
  var v118k = dt10ContainFundingCore_(v118nm, v118al, 'WITHHELD', true);
  var v118w2 = dt10ContainFundingCore_(v118w.nearMiss, v118w.alerts,
                                       'WITHHELD', false);
  report.push('funding containment core: ' +
              ((v118w.suppressed === 3 &&
                v118w.alerts.length === 2 &&
                v118w.alerts[0].type === 'missing_fx' &&
                v118w.alerts[1].type === 'funding_withheld' &&
                v118w.alerts[1].count === 3 &&
                v118w.nearMiss.length === 2 &&
                v118w.nearMiss[0].symbol === 'GBCI.US' &&
                v118w.nearMiss[0].required.indexOf('WITHHELD') === 0 &&
                v118w.nearMiss[0].verdict === 'WATCH' &&
                v118w.nearMiss[1].required === 'within caps' &&
                v118e.suppressed === 0 &&
                v118e.alerts === v118al && v118e.nearMiss === v118nm &&
                v118h.alerts === v118al && v118h.nearMiss === v118nm &&
                v118k.suppressed === 0 && v118k.alerts === v118al &&
                v118w2.suppressed === 0 &&
                v118w2.alerts.length === 2 &&
                v118w2.nearMiss[0].required === v118w.nearMiss[0].required)
               ? 'ok' : 'FAIL'));
  /* v1.11.9 [P-145]: pure grace-sizing suspension checks. */
  var v119t = { rank: 3, symbol: 'TSM.US',
    entry_zone: '1,506.25-1,568.36 SAR', suggested_sar: 13976,
    suggested_shares: 9, stop_sar: 1371.28, tp1_sar: 1804.25,
    tp2_sar: 2055.67, exp_gain_12m_sar: 11488,
    advisor_note: 'INVEST - 13,976 SAR (9 sh)',
    detail: { funds_from: 'Cash 13,976 SAR', review_date: '2026-10-16' } };
  var v119c = {};
  for (var v9k in v119t) {
    if (v119t.hasOwnProperty(v9k)) v119c[v9k] = v119t[v9k];
  }
  var v119r = dt10GraceSuspend_(v119c, 'GRACE (1/3 missed)');
  var v119r2 = dt10GraceSuspend_(v119r, 'GRACE (1/3 missed)');
  report.push('grace sizing core: ' +
              ((v119r === v119c &&
                v119r._grace_hold === true &&
                v119r._p145_suspended === true &&
                v119r.entry_zone === '\u2014' &&
                v119r.suggested_sar === '\u2014' &&
                v119r.suggested_shares === '\u2014' &&
                v119r.stop_sar === '\u2014' &&
                v119r.tp1_sar === '\u2014' &&
                v119r.tp2_sar === '\u2014' &&
                v119r.exp_gain_12m_sar === '\u2014' &&
                v119r.detail.funds_from === '\u2014' &&
                v119t.detail.funds_from === 'Cash 13,976 SAR' &&
                v119r.advisor_note.indexOf('GRACE (1/3 missed)') === 0 &&
                v119r.advisor_note.indexOf('sizing suspended') > 0 &&
                v119r2.detail.funds_from === '\u2014' &&
                v119r2._grace_hold === true)
               ? 'ok' : 'FAIL'));
  /* v1.10.0: pure withhold checks (board + log schemas). */
  var v110t = { rank: 1, symbol: 'W1', name: 'W', market: 'M', sector: 'S',
    currency: 'USD', fx_to_sar: 3.75, price: 10, price_sar: 37.5,
    entry_zone: '9.8-10.1', suggested_sar: 6251, suggested_shares: 22,
    stop_sar: 34.9, tp1_sar: 41.2, tp2_sar: 44.0, roi_pct: 9.4,
    engine_roi_pct: 18.8, ann_roi_pct: 12.1, exp_gain_12m_sar: 588,
    reliability: 61, dq: 72, confidence_band: 'B',
    advisor_note: 'BUY 22 sh = 6,251 SAR',
    detail: { funds_from: 'Cash 6,251 SAR', review_date: '2026-09-01' } };
  var v110b = dt10UvWithholdRow_(dt10TicketToRow_(v110t),
      DT10_UV_BOARD_WITHHOLD_IDX, DT10_UV_BOARD_NOTE_IDX, 'test');
  var v110l = dt10UvWithholdRow_(
      dt10SelLogRowFromTicket_(v110t, 'stamp', 'run', '{}'),
      DT10_UV_LOG_WITHHOLD_IDX, DT10_UV_LOG_NOTE_IDX, 'test');
  report.push('uv withhold core: ' +
      ((v110b[10] === '\u2014' && v110b[22] === '\u2014' &&
        v110b[15] === '\u2014' && v110b[16] === 18.8 &&
        String(v110b[29]).indexOf('SIZING WITHHELD') === 0 &&
        v110l[13] === '\u2014' && v110l[25] === '\u2014' &&
        v110l[19] === 18.8 &&
        String(v110l[27]).indexOf('SIZING WITHHELD') === 0)
       ? 'ok' : 'FAIL'));
  /* v1.11.1: morning trigger target + output-truth pure checks. */
  report.push('morning cockpit trigger target (tfbMorningCockpitRefresh): ' +
              (typeof tfbMorningCockpitRefresh === 'function' ?
               'present' : 'MISSING'));
  var v1111a = dt10OutputStatus_({ _dt10_uv: { state: 'EXECUTABLE' },
      kpis: { passed: 74 }, selected: [{ symbol: 'G', _grace_hold: true },
      { symbol: 'F', _ft_suspended: true, suggested_shares: '\u2014' }] });
  var v1111b = dt10OutputStatus_({ _dt10_uv: { state: 'EXECUTABLE' },
      kpis: { passed: 74 }, selected: [{ symbol: 'E', suggested_shares: 22 }] });
  var v1111c = dt10OutputStatus_({ _dt10_uv: { state: 'NOT_ACTIONABLE',
      reason: 'partial:GM' }, kpis: { passed: 74 },
      selected: [{ symbol: 'E', suggested_shares: 22 }] });
  var v1111d = dt10OutputStatus_({ _dt10_uv: { state: 'EXECUTABLE' },
      kpis: { passed: 0 }, selected: [] });
  var v1111s = dt10SeatCheckNote_({ selected: [{ symbol: 'F',
      _ft_suspended: true }], kpis: { selected_count: 1,
      expected_gain_12m_sar: 558 } });
  report.push('output truth core: ' +
              ((v1111a === 'HELD' && v1111b === 'EXECUTABLE' &&
                v1111c === 'WITHHELD' && v1111d === 'EMPTY' &&
                v1111s.indexOf('SEAT-CHECK kpi 1 funded vs board 0 exec') === 0 &&
                v1111s.indexOf('gain kpi 558') > 0)
               ? 'ok (HELD / EXECUTABLE / WITHHELD / EMPTY; seat-check gain)'
               : 'FAIL [' + v1111a + '/' + v1111b + '/' + v1111c + '/' +
                 v1111d + '/' + v1111s + ']'));
  /* v1.11.2: fast-track persistence, output-keyed signature, run-log helper
   * (pure — nothing is written). Replays the 2026-09-05 03:40 -> 07:46 case. */
  var ftKnobs = { confirm_days: 3, exit_days: 3, rank_buffer: 0,
                  smooth_days: 5, hard_strict: true };
  var ftTk = function () {
    return { symbol: 'AEFES.IS', name: 'Anadolu Efes', opportunity_score: 71.8,
             suggested_sar: 3824, suggested_shares: 2693, entry_zone: '1.38-1.43',
             stop_sar: 1.21, tp1_sar: 1.67, tp2_sar: 1.92, exp_gain_12m_sar: 669,
             advisor_note: 'INVEST — 3,824 SAR' };
  };
  var ftCand = [{ symbol: 'AEFES.IS', name: 'Anadolu Efes', opportunity_score: 71.8,
                  verdict: 'INVEST' }];
  var ftS1 = dt10StabCore_([ftTk()], ftCand, dt10StabParseState_({}),
                           ftKnobs, 10, '2026-09-05');
  var ftS2 = dt10StabCore_([ftTk()], ftCand, dt10StabParseState_(ftS1.state),
                           ftKnobs, 10, '2026-09-05');
  var ftS3 = dt10StabCore_([ftTk()], ftCand, dt10StabParseState_(ftS2.state),
                           ftKnobs, 10, '2026-09-06');
  var ftS4 = dt10StabCore_([ftTk()], ftCand, dt10StabParseState_(ftS3.state),
                           ftKnobs, 10, '2026-09-07');
  var ftSaved = DT10_V1112_FASTTRACK_PERSIST;
  DT10_V1112_FASTTRACK_PERSIST = false;
  var ftS2k = dt10StabCore_([ftTk()], ftCand, dt10StabParseState_(ftS1.state),
                            ftKnobs, 10, '2026-09-05');
  DT10_V1112_FASTTRACK_PERSIST = ftSaved;
  report.push('fast-track persistence core: ' +
      ((ftS1.tickets[0]._stab_status === 'FAST-TRACK (day 1)' &&
        ftS1.tickets[0]._ft_suspended === true &&
        ftS1.state.symbols['AEFES.IS'].ft === true &&
        ftS2.tickets[0]._stab_status === 'FAST-TRACK (day 1, 1/3 confirmed)' &&
        ftS2.tickets[0]._ft_suspended === true &&
        ftS2.tickets[0].suggested_shares === '\u2014' &&
        String(ftS2.tickets[0].advisor_note).indexOf(
            'FAST-TRACK (day 1, 1/3 confirmed) \u2014 sizing suspended') === 0 &&
        ftS2.audit.fast_track_unconfirmed.length === 1 &&
        ftS2.note.indexOf('1 ft-carried') > 0 &&
        ftS3.tickets[0]._stab_status === 'FAST-TRACK (day 2, 2/3 confirmed)' &&
        ftS4.tickets[0]._stab_status === 'ACTIVE (day 3)' &&
        ftS4.tickets[0]._ft_suspended !== true &&
        ftS4.state.symbols['AEFES.IS'].ft === false &&
        ftS2k.tickets[0]._stab_status === 'ACTIVE (day 1)')
       ? 'ok (day1 FAST-TRACK / rerun FAST-TRACK 1/3 suspended / day2 2/3 / day3 ACTIVE; kill-switch ACTIVE)'
       : 'FAIL [' + ftS1.tickets[0]._stab_status + ' / ' +
         ftS2.tickets[0]._stab_status + ' / ' + ftS3.tickets[0]._stab_status +
         ' / ' + ftS4.tickets[0]._stab_status + ' / ' +
         ftS2k.tickets[0]._stab_status + ']'));
  var sigA = dt10SelLogSignature_([{ symbol: 'A', suggested_shares: 5 }]);
  var sigE = dt10SelLogSignature_([{ symbol: 'A', suggested_shares: 5 }], 'EXECUTABLE');
  var sigW = dt10SelLogSignature_([{ symbol: 'A', suggested_shares: 5 }], 'WITHHELD');
  report.push('sellog output-keyed signature: ' +
      ((sigA === 'A~SF~' && sigE !== sigW && sigE.indexOf('OUT~EXECUTABLE') >= 0 &&
        sigW.indexOf('OUT~WITHHELD') >= 0) ? 'ok' : 'FAIL [' + sigA + ' | ' + sigE + ' | ' + sigW + ']'));
  /* v1.11.3: suspension blanks Funds From on a copy; panel seed order. */
  var sfRaw = { symbol: 'X', suggested_shares: 9,
                detail: { funds_from: 'Cash 19,544 SAR', review_date: '2026-10-05' } };
  var sfTk = {};
  for (var sfk in sfRaw) { if (sfRaw.hasOwnProperty(sfk)) sfTk[sfk] = sfRaw[sfk]; }
  dt10FastTrackSuspend_(sfTk);
  report.push('suspension blanks Funds From (copy-on-write): ' +
      ((sfTk.detail.funds_from === '\u2014' &&
        sfTk.detail.review_date === '2026-10-05' &&
        sfRaw.detail.funds_from === 'Cash 19,544 SAR' &&
        sfTk.suggested_shares === '\u2014') ? 'ok' : 'FAIL'));
  var psItem = { label: 'Cash Available (SAR)', def: 100000, kind: 'sar' };
  var psA = dt10PanelSeedValue_(psItem, {}, { 'Cash Available (SAR)': 3825 });
  var psB = dt10PanelSeedValue_(psItem, { 'Cash Available (SAR)': 50000 }, {});
  var psC = dt10PanelSeedValue_(psItem, {}, {});
  var psD = dt10PanelSeedValue_(psItem, { 'Cash Available (SAR)': 50000 },
                                { 'Cash Available (SAR)': 3825 });
  report.push('layout panel seed (preserved > default > built-in): ' +
      ((psA === 3825 && psB === 50000 && psC === 100000 && psD === 3825)
       ? 'ok' : 'FAIL [' + psA + '/' + psB + '/' + psC + '/' + psD + ']'));
  /* v1.11.4: holdings FX (pure mapper) and the live-only ft-carried note. */
  var hxA = dt10HoldingFromRow_(['PFS', 'Financials', 118.6, 'USD'], 0, 1, 2,
                                { iCcy: 3, fx: { USD: 3.7528 } });
  var hxB = dt10HoldingFromRow_(['5023.SR', 'Sukuk', 10250, 'SAR'], 0, 1, 2,
                                { iCcy: 3, fx: { USD: 3.7528 } });
  var hxC = dt10HoldingFromRow_(['T82U.SI', 'REIT', 5852, 'SGD'], 0, 1, 2,
                                { iCcy: 3, fx: { USD: 3.7528 } });
  var hxD = dt10HoldingFromRow_(['PFS', 'Financials', 445.08, 'USD'], 0, 1, 2,
                                { iCcy: 3, fx: null });
  var hxE = dt10HoldingFromRow_(['VOD.L', 'Telecom', 1000, 'GBp'], 0, 1, 2,
                                { iCcy: 3, fx: { GBP: 5 } });   // v1.13.10
  report.push('holdings value_sar via FX (native USD 118.6 -> 445.08; GBp 1000 -> 50; ' +
      'SAR/SAR-column untouched; unknown -> null): ' +
      ((Math.abs(hxA.value_sar - 445.08) < 0.01 && hxB.value_sar === 10250 &&
        hxC.value_sar === null && hxD.value_sar === 445.08 &&
        Math.abs(hxE.value_sar - 50) < 1e-9) ? 'ok' :
       'FAIL [' + hxA.value_sar + '/' + hxB.value_sar + '/' + hxC.value_sar + '/' +
       hxD.value_sar + '/' + hxE.value_sar + ']'));
  var gcS = dt10StabParseState_({ v: 1, date: '2026-09-05', symbols: {
      'GHOST.US': { ci: 0, co: 1, member: true, since: '2026-09-05', ls: '', hist: [70], ft: true } } });
  var gcR = dt10StabCore_([], [], gcS, ftKnobs, 10, '2026-09-05');
  report.push('stab note: grace-held ft seat is not "ft-carried": ' +
      ((gcR.note.indexOf('ft-carried') < 0 && gcR.note.indexOf('1 grace') >= 0 &&
        gcR.audit.fast_track_unconfirmed.length === 1) ? 'ok' : 'FAIL [' + gcR.note + ']'));
  report.push('morning run-log helper (dt10MorningRunLog_): ' +
      (typeof dt10MorningRunLog_ === 'function' ? 'present' : 'MISSING') +
      ' | DT10_V1112_MORNING_RUNLOG=' + (DT10_V1112_MORNING_RUNLOG ? 'on' : 'OFF'));
  /* v1.11.10 [P-168]: outage pause core (pure; no services). */
  var opK = { enabled: true, confirm_days: 3, exit_days: 3, rank_buffer: 15,
              smooth_days: 5, hard_strict: true };
  var opSeed = { v: 1, date: '2026-09-25', symbols: {
      'PAUSE.US': { ci: 3, co: 1, member: true, since: '2026-09-20',
                    ls: '2026-09-25', hist: [74], ft: false },
      'MISS.US': { ci: 3, co: 1, member: true, since: '2026-09-20',
                   ls: '2026-09-25', hist: [70], ft: false },
      'CHAL.US': { ci: 2, co: 0, member: false, since: '',
                   ls: '2026-09-25', hist: [69], ft: false } } };
  var opMap = dt10OutageMapFromPool_([
      { 'Symbol': ' pause.us ', 'Warnings': 'fetch_failed:HTTP 402; ' +
        'dq_capped:coherence:fetch_failed' },
      { 'Symbol': 'CHAL.US', 'Warnings': 'fetch_failed:HTTP 404 not_found' },
      { 'Symbol': 'MISS.US', 'Warnings': 'yahoo_enrichment_applied' },
      { 'Symbol': '' }, null ]);
  var opR = dt10StabCore_([], [], dt10StabParseState_(opSeed), opK, 10,
                          '2026-09-26', opMap);
  var opL = dt10StabCore_([], [], dt10StabParseState_(opSeed), opK, 10,
                          '2026-09-26');
  var opSeed5 = { v: 1, date: '2026-09-25', symbols: {
      'PAUSE.US': { ci: 3, co: 1, member: true, since: '2026-09-20',
                    ls: '2026-09-25', hist: [74], ft: false, op: 5 } } };
  var opC = dt10StabCore_([], [], dt10StabParseState_(opSeed5), opK, 10,
                          '2026-09-26', opMap);
  var opF = dt10StabCore_([], [], dt10StabParseState_(opR.state), opK, 10,
                          '2026-09-26', opMap);   // same-day re-run: frozen
  function opTk(res, sym) {
    for (var oti = 0; oti < res.tickets.length; oti++) {
      if (res.tickets[oti].symbol === sym) return res.tickets[oti];
    }
    return null;
  }
  var opTp = opTk(opR, 'PAUSE.US'), opTm = opTk(opR, 'MISS.US');
  report.push('outage pause core: ' +
      ((opMap['PAUSE.US'] === true && opMap['CHAL.US'] === true &&
        opMap['MISS.US'] !== true && opMap[''] !== true &&
        opR.state.symbols['PAUSE.US'].co === 1 &&
        opR.state.symbols['PAUSE.US'].op === 1 &&
        opR.state.symbols['PAUSE.US'].member === true &&
        opR.state.symbols['MISS.US'].co === 2 &&
        !opR.state.symbols['MISS.US'].op &&
        opR.state.symbols['CHAL.US'].ci === 2 &&
        opR.state.symbols['CHAL.US'].op === 1 &&
        opR.audit.outage_paused.length === 2 &&
        opR.note.indexOf('2 outage-paused') >= 0 &&
        opR.note.indexOf('2 grace') >= 0 &&
        opTp && opTp._p168_paused === true &&
        opTp._stab_status === 'GRACE (1/3 missed) - outage pause 1/5' &&
        opTp.advisor_note.indexOf('Outage pause') > 0 &&
        opTm && opTm._p168_paused !== true &&
        opTm._stab_status === 'GRACE (2/3 missed)' &&
        opL.state.symbols['PAUSE.US'].co === 2 &&
        !opL.state.symbols['PAUSE.US'].op &&
        opL.state.symbols['CHAL.US'].ci === 0 &&
        opL.audit.outage_paused.length === 0 &&
        opL.note.indexOf('outage-paused') < 0 &&
        opC.state.symbols['PAUSE.US'].co === 2 &&
        opC.state.symbols['PAUSE.US'].op === 6 &&
        opC.audit.outage_paused.length === 0 &&
        opF.state.symbols['PAUSE.US'].co === 1 &&
        opF.state.symbols['PAUSE.US'].op === 1 &&
        opF.audit.outage_paused.length === 0)
       ? 'ok' : 'FAIL [' + opR.note + ' | ' + (opTp ? opTp._stab_status : 'no PAUSE ticket') + ']'));
  /* v1.11.11 [P-144]: epoch key core (pure; no services). */
  var ek1 = dt10StabResolveEpoch_(
      'OK | cov=97.5 | run=36199188352 | 2026-09-26 04:18:18+03:00',
      '2026-09-25', '2026-09-26');
  var ek2 = dt10StabResolveEpoch_(
      'OK | cov=100.0 | run=36199188352 | 2026-09-26 01:59:00+03:00',
      '2026-09-25', '2026-09-26');                       // 22:59Z 09-25: aged
  var ek3 = dt10StabResolveEpoch_(
      'STALE_COV | cov=89.0 | run=1 | 2026-09-26 01:30:00',
      '2026-09-25', '2026-09-26');                       // naive => Riyadh
  var ek4 = dt10StabResolveEpoch_(
      'SKIPPED | run=2 | 2026-09-26 12:09:00+03:00', '2026-09-25', '2026-09-26');
  var ek5 = dt10StabResolveEpoch_('', '2026-09-25', '2026-09-26');
  var ek6 = dt10StabResolveEpoch_('', '', '2026-09-26');
  var ek7 = dt10StabResolveEpoch_(
      'OK | cov=97.5 | run=3 | 2026-09-24 10:00:00+03:00', '2026-09-26', '2026-09-26');
  var ek8 = dt10StabResolveEpoch_(
      'OK | cov=97.5 | run=4 | 2026-09-27 03:30:00+03:00', '2026-09-25', '2026-09-26');
  var ek9 = dt10StabResolveEpoch_('garbage', '2026-09-25', '2026-09-26');
  var ekS = { v: 1, date: '2026-09-25', symbols: {
      'E.US': { ci: 3, co: 1, member: true, since: '2026-09-20', ls: '2026-09-25',
                hist: [70], ft: false } } };
  var ekA = dt10StabCore_([], [], dt10StabParseState_(ekS), opK, 10, ek2.key);
  var ekB = dt10StabCore_([], [], dt10StabParseState_(ekS), opK, 10, ek1.key);
  report.push('epoch key core: ' +
      ((ek1.key === '2026-09-26' && ek1.src === 'feed' && !ek1.held &&
        ek2.key === '2026-09-25' && ek2.src === 'feed' && ek2.held &&
        ek3.key === '2026-09-25' && ek3.src === 'feed' && ek3.held &&
        ek4.key === '2026-09-25' && ek4.src === 'held' && ek4.held &&
        ek5.key === '2026-09-25' && ek5.src === 'held' && ek5.held &&
        ek6.key === '2026-09-26' && ek6.src === 'clock' && !ek6.held &&
        ek7.key === '2026-09-26' && ek7.src === 'held' && ek7.held &&
        ek8.key === '2026-09-26' && ek8.src === 'feed' && !ek8.held &&
        ek9.key === '2026-09-25' && ek9.src === 'held' && ek9.held &&
        dt10StabStampUtcDate_('2026-09-26T00:17:04Z') === '2026-09-26' &&
        dt10StabStampUtcDate_('no stamp here') === '' &&
        dt10StabStampUtcDate_('2026-09-26 03:00:00+03:00') === '2026-09-26' &&
        dt10StabStampUtcDate_('2026-09-26 02:59:59+03:00') === '2026-09-25' &&
        ekA.state.symbols['E.US'].co === 1 && ekA.state.date === '2026-09-25' &&
        ekB.state.symbols['E.US'].co === 2 && ekB.state.date === '2026-09-26')
       ? 'ok' : 'FAIL [' + ek1.key + '/' + ek1.src + ' ' + ek2.key + '/' + ek2.src +
         ' ' + ek3.key + ' ' + ek4.key + ' ' + ek7.key + ' ' + ek8.key + ']'));
  /* v1.13.1 [TFB-01/02]: synthetic adapter and final-state smoke checks.
     They do not attest provider validity or the installed request/board. */
  var qpHeader = ['Symbol', 'Current Price', 'Data Provider', 'provider',
                  'acquisition_quote_asof', 'Last Updated (Riyadh)'];
  var qpRow = dt10PoolRowFromSheetRow_(
      ['SYNTH.US', 25, 'eodhd', 'yahoo_chart', '2026-10-09T20:00:00Z',
       '2026-10-10 09:00:00'], dt10MapHeaderCols_(qpHeader), 'Global_Markets');
  report.push('quote projection core: ' +
      ((qpRow && qpRow['Data Provider'] === 'eodhd' && qpRow.provider === 'yahoo_chart' &&
        qpRow.acquisition_quote_asof === '2026-10-09T20:00:00Z' &&
        qpRow['Last Updated (Riyadh)'] === '2026-10-10 09:00:00' &&
        !Object.prototype.hasOwnProperty.call(qpRow, 'Last Updated (UTC)'))
       ? 'ok (provider/witness aliases retained; clock basis unchanged)' : 'FAIL'));
  var btHeld = dt10SelectedTitle_({selected: [{_grace_hold: true, suggested_shares: 0}],
      kpis: {passed: 0}, _dt10_uv: {state: 'EXECUTABLE'}}, true);
  var btEmpty = dt10SelectedTitle_({selected: [], kpis: {passed: 0},
      _dt10_uv: {state: 'EXECUTABLE'}}, true);
  report.push('final banner core: ' +
      ((btHeld.indexOf('FEED ACTIONABLE — HELD') >= 0 &&
        btEmpty.indexOf('FEED ACTIONABLE — NO_TRADE') >= 0)
       ? 'ok (grace HELD; empty NO_TRADE)' : 'FAIL'));
  /* v1.13.2: synthetic display-only held seats; no calendar/broker writes. */
  var hsMixed = {selected: [{symbol: 'E', suggested_shares: 1},
      {symbol: 'F', _ft_suspended: true, suggested_shares: 0},
      {symbol: 'G', _grace_hold: true, _ft_suspended: true, suggested_shares: 0},
      {symbol: 'R', suggested_shares: 0}], kpis: {max_selected: 10}};
  var hsPayload = {version: 'synthetic', selected: [{symbol: 'F',
      _ft_suspended: true, _stab_status: 'FAST-TRACK (day 1, 1/3 confirmed)'},
      {symbol: 'G', _grace_hold: true, _stab_status: 'GRACE (2/3 missed)'}],
      candidates_rows: [{symbol: 'f', deferral: ''}, {symbol: 'G', deferral: ''}],
      kpis: {max_selected: 10}, meta: {}};
  dt10FinalizeBoard_(hsPayload);
  var hsQual = dt10QualToRow_(hsPayload.candidates_rows[0], 1, {}, 2, 2, {});
  var hsTruth = dt10SeatTruthOn_();
  report.push('held seat labels core: ' +
      ((dt10SeatTruthKpi_(hsMixed) ===
          '1 exec + 1 fast-track suspended + 1 research + 1 grace / 10' &&
        dt10SeatTruthKpi_(hsPayload) === '0 exec + 1 fast-track suspended + 1 grace / 10' &&
        hsPayload.candidates_rows[0].selected === false && hsQual[13] === 'No' &&
        hsPayload.candidates_rows[0].deferral === '' &&
        (!hsTruth || (hsQual[14].indexOf('Held research seat') === 0 &&
          hsQual[14].indexOf('1/3 confirmed') > 0 &&
          dt10CandToRow_(hsPayload.candidates_rows[0])[24] === hsQual[14])))
       ? 'ok (exec / fast-track suspended / research / grace; held qualifier stays No)' : 'FAIL'));
  var hsEarnMap = {F: 4, G: 15};
  dt10ApplyEarningsTags_(hsPayload.selected, hsEarnMap, 14);
  dt10FinalizeBoard_(hsPayload); // render-time finalization can replace held notes
  var hsEarnCount = dt10ApplyEarningsTags_(hsPayload.selected, hsEarnMap, 14);
  var hsEarnAgain = dt10ApplyEarningsTags_(hsPayload.selected, hsEarnMap, 14);
  var hsEarnWithheld = dt10UvWithholdRow_(dt10TicketToRow_(hsPayload.selected[0]),
      DT10_UV_BOARD_WITHHOLD_IDX, DT10_UV_BOARD_NOTE_IDX, 'synthetic');
  report.push('final earnings annotation core: ' +
      ((hsEarnCount === 1 && hsEarnAgain === 0 &&
        hsPayload.selected[0].advisor_note.indexOf('\u26a0 earnings \u22644d \u00b7 ') === 0 &&
        hsPayload.selected[1].advisor_note.indexOf('\u26a0 earnings') < 0 &&
        hsEarnWithheld[DT10_UV_BOARD_NOTE_IDX].indexOf(
            '\u26a0 earnings \u22644d \u00b7 SIZING WITHHELD') === 0 &&
        hsEarnWithheld[10] === '\u2014' && hsEarnWithheld[12] === '\u2014')
       ? 'ok (post-finalization; 15d excluded; idempotent; withheld prefix only)' : 'FAIL'));
  /* v1.11.12 [P-181b]: the four timing-gate fields project from a GM-shaped
     header and travel on the pool row; the legacy filter drops exactly them. */
  var w52Hdr = ['Symbol', 'Name', 'Current Price', '52W High', '52W Low',
                '52W Position %', 'Percent Change', 'Forecast Source'];
  var w52Map = dt10MapHeaderCols_(w52Hdr);
  var w52Row = dt10PoolRowFromSheetRow_(
      ['RDN.US', 'Radian', 32.57, 41.05, 30.53, 19.39, 0.0432, 'provider'],
      w52Map, 'Global_Markets');
  var w52Leg = dt10PoolFieldsActive_(true);
  var w52LegHas = false;
  for (var w52i = 0; w52i < w52Leg.length; w52i++) {
    if (DT10_P181B_SENDS[w52Leg[w52i].send]) w52LegHas = true;
  }
  report.push('w52 projection core: ' +
      ((w52Map['52W High'] === 3 && w52Map['52W Low'] === 4 &&
        w52Map['52W Position %'] === 5 && w52Map['Percent Change'] === 6 &&
        w52Row && w52Row['52W High'] === 41.05 && w52Row['52W Low'] === 30.53 &&
        w52Row['52W Position %'] === 19.39 &&
        w52Row['Percent Change'] === 0.0432 &&
        !w52LegHas && w52Leg.length === DT10_POOL_FIELDS.length - 4 &&
        dt10PoolFieldsActive_(false).length === DT10_POOL_FIELDS.length)
       ? 'ok (4 fields; legacy filter drops 4)' : 'FAIL') +
      ' | DT10_P181B_W52_LEGACY=' + (dt10W52FieldsLegacy_() ? '1 (OFF)' : 'unset (ON)'));
  /* v1.11.13 [P-202]: property memo -- the no-arg projection is ONE list
     per execution and the legacy flag ONE read (v1.11.12 read it per pool
     row: ~9,800 service calls, the 6-minute kill). */
  dt10PropMemoReset_();
  var pmA = dt10PoolFieldsActive_();
  var pmB = dt10PoolFieldsActive_();
  var pmC = dt10PoolFieldsActive_();
  var pmKeys = 0;
  for (var pmK in DT10_PROP_MEMO_) {
    if (Object.prototype.hasOwnProperty.call(DT10_PROP_MEMO_, pmK)) pmKeys++;
  }
  report.push('property memo core: ' +
      ((pmA === pmB && pmB === pmC && pmKeys >= 2 &&
        pmA.length === (dt10W52FieldsLegacy_() ? DT10_POOL_FIELDS.length - 4
                                               : DT10_POOL_FIELDS.length) +
            (DT10_PROP_MEMO_.shadow_log_mode !== 'off' ?
             DT10_SHADOW_POOL_FIELDS.length : 0))   // v1.17.1
       ? 'ok (pool fields memoized; keys=' + pmKeys + '; reads=1/execution)'
       : 'FAIL'));
  /* v1.17.1 [SHADOW-MOMENTUM]: the challenger's momentum input travels on
     the pool row only while the shadow rank log is armed. Pure: the memo is
     swapped for the two projections and restored (finally). */
  var spOff = dt10PoolFieldsWithShadow_(DT10_POOL_FIELDS, false);
  var spOn = dt10PoolFieldsWithShadow_(DT10_POOL_FIELDS, true);
  var spTwice = dt10PoolFieldsWithShadow_(spOn, true);
  var spTail = spOn.slice(DT10_POOL_FIELDS.length);
  var spTailOk = spTail.length === DT10_SHADOW_POOL_FIELDS.length;
  for (var spi = 0; spTailOk && spi < spTail.length; spi++) {
    if (spTail[spi] !== DT10_SHADOW_POOL_FIELDS[spi]) spTailOk = false;
  }
  var spHdr = ['Symbol', 'Name', 'Momentum Score'];
  var spMemo = DT10_PROP_MEMO_;
  var spMap = null, spRow = null, spBlank = null, spOffMap = null;
  var spOffRow = null;
  try {
    DT10_PROP_MEMO_ = { pool_fields_active: spOn };
    spMap = dt10MapHeaderCols_(spHdr);
    spRow = dt10PoolRowFromSheetRow_(['OPBK.US', 'OP Bancorp', 57.03],
        spMap, 'Global_Markets');
    spBlank = dt10PoolRowFromSheetRow_(['WPC.US', 'W. P. Carey', ''],
        spMap, 'Global_Markets');
    DT10_PROP_MEMO_ = { pool_fields_active: spOff };
    spOffMap = dt10MapHeaderCols_(spHdr);
    spOffRow = dt10PoolRowFromSheetRow_(['OPBK.US', 'OP Bancorp', 57.03],
        spOffMap, 'Global_Markets');
  } finally {
    DT10_PROP_MEMO_ = spMemo;
  }
  report.push('shadow pool fields core: ' +
      ((spOff === DT10_POOL_FIELDS && spOn !== DT10_POOL_FIELDS && spTailOk &&
        spTwice.length === spOn.length &&
        spMap && spMap['Momentum Score'] === 2 && spRow !== null &&
        spRow['Momentum Score'] === 57.03 && spBlank !== null &&
        spBlank['Momentum Score'] === undefined &&
        spOffMap && spOffMap['Momentum Score'] === undefined &&
        spOffRow !== null && spOffRow['Momentum Score'] === undefined) ?
       'ok (Momentum Score sent only while the shadow log is armed)' :
       'FAIL') +
      ' | sent now: ' + (dt10ShadowLogMode_() !== 'off' ? 'yes' : 'no'));
  /* v1.18.0 [POST-EARNINGS]: pure core on the FRST case (reported Thu 22 Oct
     after the close, EPS -22.2%; read on Tue 27 Oct = 3 sessions later) and
     a report the calendar has not caught up with (Next Earnings Date Mon 26
     Oct, already past: 1 session, surprise unknown). */
  var peVals = [['Symbol', 'Next Earnings Date', 'Days To Earnings',
      'Next Ex-Div Date', 'Days To ExDiv', 'Updated At (Riyadh)', 'Source',
      'Last Earnings Date', 'Days Since Earnings', 'EPS Surprise %'],
    ['FRST.US', '2027-01-28', 93, '', '', '', 's', '2026-10-22', 5, -22.2],
    ['OPBK.US', '', '', '', '', '', 's', '2026-10-13', 14, 3.1],
    ['WTFC.US', '', '', '', '', '', 's', '2026-10-20', 7, ''],
    ['AER.US', '2026-10-28', 1, '', '', '', 's', '', '', ''],
    ['WPC.US', '2026-10-26', 0, '', '', '', 's', '', '', '']];
  var peMap = dt10PostEarnMapFromValues_(peVals, '2026-10-27');
  var peT = [{ symbol: 'FRST.US', advisor_note: 'n1' },
             { symbol: 'OPBK.US', advisor_note: 'n2' },
             { symbol: 'WTFC.US', advisor_note: 'n3' },
             { symbol: 'AER.US', advisor_note: 'n4' },
             { symbol: 'WPC.US', advisor_note: '\u26a0 earnings \u22649d \u00b7 n5' }];
  var peN = dt10ApplyPostEarnTags_(peT, peMap);
  var peN2 = dt10ApplyPostEarnTags_(peT, peMap);
  var peKept = dt10UvWithholdRow_(['x', peT[4].advisor_note], [], 1, 'r');
  report.push('post-earnings core: ' +
      ((peN === 3 && peN2 === 0 && peMap['OPBK.US'].sessions === 10 &&
        peT[0].advisor_note ===
            '\u26a0 post-earnings 3/10 \u00b7 EPS -22.2% \u00b7 n1' &&
        peT[1].advisor_note === 'n2' &&
        peT[2].advisor_note === '\u26a0 post-earnings 5/5 \u00b7 EPS n/a \u00b7 n3' &&
        peT[3].advisor_note === 'n4' &&
        peT[4].advisor_note === '\u26a0 earnings \u22649d \u00b7 ' +
            '\u26a0 post-earnings 1/5 \u00b7 EPS n/a \u00b7 n5' &&
        peKept[1].indexOf('\u26a0 earnings \u22649d \u00b7 \u26a0 post-earnings 1/5 ' +
                          '\u00b7 EPS n/a \u00b7 SIZING WITHHELD') === 0) ?
       'ok (FRST 3/10; +EPS at 10 sessions untagged; blank EPS 5/5; past next date 1/5)' :
       'FAIL') +
      ' | mode now: ' + dt10PostEarnMode_());
  /* v1.11.12 [SYNC-INFLIGHT]: pure core on the 2026-10-03 13:05 race
     (ML run 37114669145 at 12:59, GM still run 37079471147 at 03:43) and
     the clean 10-04 case (both legs run 37160795404). */
  var ifNow = Date.UTC(2026, 9, 3, 10, 5, 0);   // 13:05 Riyadh
  var ifA = dt10SyncInflightCore_(
      'OK | cov=100.0 | run=37114669145 | 2026-10-03 12:59:12+03:00',
      'OK | cov=99.1 | run=37079471147 | 2026-10-03 03:43:37+03:00', ifNow, 90);
  var ifB = dt10SyncInflightCore_(
      'OK | cov=100.0 | run=37160795404 | 2026-10-04 02:12:58+03:00',
      'OK | cov=99.8 | run=37160795404 | 2026-10-04 03:07:34+03:00',
      Date.UTC(2026, 9, 4, 5, 9, 0), 90);
  var ifC = dt10SyncInflightCore_(
      'OK | cov=100.0 | run=37114669145 | 2026-10-03 12:59:12+03:00',
      'OK | cov=99.1 | run=37079471147 | 2026-10-03 03:43:37+03:00',
      Date.UTC(2026, 9, 3, 12, 0, 0), 90);   // 15:00 Riyadh: 121m old
  var ifD = dt10SyncInflightCore_('', 'OK | run=1 | 2026-10-03 03:43:37+03:00',
                                  ifNow, 90);
  report.push('sync inflight core: ' +
      ((ifA.active && ifA.run === '37114669145' && ifA.firstAgeMin === 6 &&
        ifA.lastRun === '37079471147' && !ifB.active &&
        ifB.detail === 'run 37160795404 complete' && !ifC.active &&
        ifC.detail.indexOf('stale') !== -1 &&
        !ifD.active && ifD.detail === 'stamps unreadable')
       ? 'ok (race=active, clean=inactive, 121m=stale, blank=inactive)'
       : 'FAIL [' + ifA.detail + ' / ' + ifB.detail + ' / ' + ifC.detail + ']') +
      ' | DT10_SYNC_INFLIGHT=' + dt10SyncInflightMode_());
  var ifLive = dt10SyncInflight_(ss);
  report.push('sync in-flight now: ' + (ifLive.active ? 'YES' : 'no') +
              ' (' + ifLive.detail + ')');
  /* v1.11.12 [P-195]: marker judgement -- stale marker = aborted; fresh =
     concurrent (not aborted); blank/junk = nothing. */
  var rmNow = Date.UTC(2026, 9, 3, 18, 10, 9);   // 21:10:09 Riyadh
  var rmA = dt10RunMarkerCore_(
      '{"started":"2026-10-03T14:06:29.000Z","v":"1.11.12"}', rmNow, 7);
  var rmB = dt10RunMarkerCore_(
      '{"started":"2026-10-03T18:08:00.000Z","v":"1.11.12"}', rmNow, 7);
  var rmC = dt10RunMarkerCore_('', rmNow, 7);
  var rmD = dt10RunMarkerCore_('junk', rmNow, 7);
  report.push('run marker core: ' +
      ((rmA.aborted && rmA.ageMin === 244 && !rmB.aborted && rmB.ageMin === 2 &&
        !rmC.aborted && rmC.ageMin === null && !rmD.aborted)
       ? 'ok (17:06 run = aborted 244m; 2m = concurrent)' : 'FAIL') +
      ' | DT10_P195_MARKER_LEGACY=' + (dt10RunMarkerLegacy_() ? '1 (OFF)' : 'unset (ON)'));
  report.push('seat truth (P-149 / KPI cell): ' +
              (dt10SeatTruthOn_() ? 'ON' : 'OFF (DT10_SEAT_TRUTH_LEGACY)'));
  // v1.13.4 [WP-8]: input-truth gates -- modes (first property read of
  // this execution) + pure cores; every line must read 'ok'.
  report.push('input truth (DT10_INPUT_TRUTH): ' + dt10InputTruthMode_() +
              ' | cand tab (DT10_CAND_TAB): ' + dt10CandTabMode_() +
              ' | vintage cols (DT10_VINTAGE_COLS): ' + dt10VintageColsMode_() +
              ' | policy ref: ' + (dt10PolicyRef_() ? 'set' : 'none'));
  var itEv = dt10GateEvaluabilityCore_([
      { news_trend: 'Unknown', sector_trend: 'Unknown', conflict: true },
      { news_trend: 'Neutral', sector_trend: 'Unknown', conflict: false },
      { news_trend: 'Unknown', sector_trend: 'Unknown', conflict: true },
      { news_trend: 'Positive', sector_trend: 'Unknown', conflict: null },
      { news_trend: null, sector_trend: 'Unknown', conflict: false }],
      { enabled: true, labeled: 2, cap: 60 });
  var itAl = dt10GateEvaluabilityAlerts_(itEv);
  var itKpi = dt10GateKpiCells_(itEv);
  var itEv0 = dt10GateEvaluabilityCore_([], null);
  report.push('evaluability core: ' +
      ((itEv.total === 5 && itEv.news.unknown === 5 && itEv.news.evaluable === 0 &&
        itEv.news.display_evaluable === 2 && itEv.news.pct_unknown === 100 && itEv.news.not_evaluable &&
        itEv.news.display_only && itEv.news.cap === 60 &&
        itEv.sector.unknown === 5 && itEv.sector.not_evaluable &&
        itEv.conflict.yes === 2 && itEv.conflict.no === 2 &&
        itEv.conflict.unknown === 1 &&
        itAl.length === 2 && itAl[0].count === 5 && itAl[1].count === 5 &&
        itKpi[0][0] === 'News Gate' && itKpi[0][1] === 'Sector Gate' &&
        itKpi[1][0].indexOf('NOT EVALUABLE') === 0 &&
        itEv0.total === 0 && !itEv0.news.not_evaluable &&
        dt10GateEvaluabilityNote_(itEv0, 'observe') === 'gates: no candidates [observe]' &&
        dt10GateEvaluabilityAlerts_(itEv0).length === 0 &&
        dt10GateKpiCells_(itEv0)[1][0] === 'n/a') ?
       'ok (display labels are not gate receipts; empty -> n/a)' : 'FAIL'));
  var itDefs = { 'T10: Max Per Sector': 2, 'PF: Max Sector %': 30,
                 'PF: Max Position %': 15 };
  var itPf = { found: true, values: { 'PF: Max Sector %': 30,
                                      'PF: Max Position %': 20 } };
  var itRefA = dt10PanelRefCore_({ 'T10: Max Per Sector': 2 }, itDefs, itPf, null);
  var itRefB = dt10PanelRefCore_({ 'T10: Max Per Sector': 2 }, itDefs, itPf,
      { 'T10: Max Per Sector': 3, 'PF: Max Sector %': 40, 'PF: Max Position %': 10 });
  var itRefC = dt10PanelRefCore_({ 'T10: Max Per Sector': 2 }, {},
      { found: false, values: {}, err: 'sheet-missing' }, null);
  var itNoteA = dt10PanelRefNote_(itRefA, 'observe');
  var itAlA = dt10PanelRefAlerts_(itRefA), itAlB = dt10PanelRefAlerts_(itRefB);
  report.push('panel reference core: ' +
      ((itRefA.items[0].builtin === 3 && itRefA.items[0].decision === 3 &&
        itRefA.items[0].panel === 2 && itRefA.items[0].seed === 2 &&
        itRefA.items[1].panel === 30 && itRefA.items[1].builtin === null &&
        itRefA.items[2].panel === 20 && itRefA.items[2].seed === 15 &&
        itRefA.drift_seed === 1 && itRefA.drift_policy === null &&
        itRefA.ref === 'seed' && itAlA.length === 1 && itAlA[0].count === 1 &&
        itNoteA.indexOf('policy \u2014') !== -1 &&
        itNoteA.indexOf('policy ref none') !== -1 &&
        itNoteA.indexOf('built-in 3') !== -1 &&
        itNoteA.indexOf('decision-0910 3') !== -1 &&
        itNoteA.indexOf('[observe]') !== -1 &&
        itRefB.ref === 'policy' && itRefB.drift_policy === 3 &&
        itAlB.length === 1 && itAlB[0].count === 3 &&
        dt10PanelRefNote_(itRefB, 'enforce').indexOf('policy 3') !== -1 &&
        itRefC.pf_err === 'sheet-missing' && itRefC.items[1].panel === null &&
        itRefC.items[0].panel === 2 &&
        dt10PanelRefNote_(itRefC, 'observe').indexOf('PF n/a (sheet-missing)') !== -1) ?
       'ok (seed drift 1; policy ref drift 3; PF n/a path)' : 'FAIL'));
  var itScan = dt10LabelScan_([['PF: Max Sector %', 30, '',
      'PF: Min Reliability to Add', 70, '', 'PF: Min DQ to Add', 80]],
      'PF: Max Sector %');
  report.push('label scan core: ' +
      ((itScan.found && itScan.value === 30 && itScan.row === 1 && itScan.col === 1 &&
        !dt10LabelScan_([['PF: Max Sector']], 'PF: Max Sector %').found &&
        !dt10LabelScan_([], 'PF: Max Sector %').found) ?
       'ok (live PD row 6 shape)' : 'FAIL'));
  var itIdx = dt10PoolVintageIndex_([
      { Symbol: 'INDV.US', Warnings: 'x; analyst_lkg:45h; acquisition_quote_asof:2026-10-09T20:00:00+00:00', 'Target Price': 50 },
      { Symbol: 'RET.US', Warnings: 'quote_attempt:eodhd:unpriced',
        'Last Updated (UTC)': '2026-10-10T17:48:33.119276+00:00', 'Target Price': 12 },
      { Symbol: 'BARE.US', Warnings: '' }, null, {}]);
  var itVc1 = dt10VintageCells_({ symbol: 'indv.us' }, itIdx);
  var itVc2 = dt10VintageCells_({ symbol: 'RET.US' }, itIdx);
  var itVc3 = dt10VintageCells_({ symbol: 'BARE.US' }, itIdx);
  var itVc4 = dt10VintageCells_({ symbol: 'NONE.US' }, itIdx);
  report.push('vintage core: ' +
      ((itIdx['INDV.US'].lkg_h === 45 &&
        itIdx['INDV.US'].quote_asof === '2026-10-09T20:00:00+00:00' &&
        itVc1[0] === '2026-10-09 20:00Z' && itVc1[1] === 'lkg 45h' &&
        itVc2[0] === 'retrieved 2026-10-10 17:48Z' && itVc2[1] === 'unknown (analyst date unavailable)' &&
        itVc3[0] === '\u2014' && itVc3[1] === '\u2014' &&
        itVc4[0] === '\u2014' && itVc4[1] === '\u2014' &&
        dt10VintageCells_({ symbol: 'INDV.US' }, null)[0] === '\u2014' &&
        dt10VintageNote_([{ symbol: 'INDV.US' }, { symbol: 'RET.US' }], itIdx,
                         'observe') === 'vintage: as-of 1/2 \u00b7 lkg 1/2 [observe]') ?
       'ok (as-of / lkg / retrieved / em-dash)' : 'FAIL'));
  var itPtr = dt10CandPointerText_(500, 'req x');
  var itStamp = dt10CandTabStampLine_(
      { route: { version: '4.18.1', request_id: 'abc' } }, 500,
      new Date(Date.UTC(2026, 9, 10, 19, 28, 22)));
  report.push('cand pointer core: ' +
      ((itPtr.indexOf(DT10_CANDTAB_SHEET) !== -1 && itPtr.indexOf('500') !== -1 &&
        itStamp.indexOf('run 2026-10-10T19:28:22.000Z') === 0 &&
        itStamp.indexOf('req abc') !== -1 && itStamp.indexOf('rows 500') !== -1 &&
        itStamp.indexOf('DT10 v' + DT10_VERSION) !== -1) ? 'ok' : 'FAIL'));
  var itCal = dt10CalCoverageCore_({
      selected: [{ symbol: 'A.US' }, { symbol: 'B.US' }],
      candidates_rows: [{ symbol: 'A.US', verdict: 'INVEST' },
                        { symbol: 'C.US', verdict: 'INVEST' },
                        { symbol: 'D.US', verdict: 'WATCH' }] },
      { 'A.US': 3, 'C.US': 10 });
  report.push('cal coverage core: ' +
      ((itCal.sel_covered === 1 && itCal.sel_total === 2 &&
        itCal.qual_covered === 2 && itCal.qual_total === 2 &&
        itCal.note === ' | cal(earn-dated): sel 1/2 \u00b7 qual 2/2' &&
        dt10CalCoverageCore_({}, null).note === ' | cal(earn-dated): n/a') ?
       'ok' : 'FAIL'));
  report.push('policy ref parse core: ' +
      ((dt10PolicyRefParse_('junk') === null &&
        dt10PolicyRefParse_('[1,2]') === null && dt10PolicyRefParse_('') === null &&
        dt10PolicyRefParse_('{"a":"x"}') === null &&
        dt10PolicyRefParse_('{"T10: Max Per Sector": "3"}')['T10: Max Per Sector'] === 3) ?
       'ok' : 'FAIL'));
  // v1.13.8: feed/cohort binding -- mode line + pure core (no feed read).
  report.push('feed cohort bind (DT10_FEED_COHORT_BIND): ' +
      (dt10FeedCohortBindOn_() ? 'on' : 'off'));
  report.push('feed cohort core: ' +
      ((dt10FeedCohortCheck_('EXECUTABLE | run=7 | x', '7').ok === true &&
        dt10FeedCohortCheck_('EXECUTABLE | run=6 | x', '7').reason === 'feed run 6 != cohort 7' &&
        dt10FeedCohortCheck_('EXECUTABLE | run=7 run=7', '7').reason ===
            'feed run token missing/ambiguous' &&
        dt10FeedCohortCheck_('x | superseded_by_run=7.1', '7').ok === false &&
        dt10FeedCohortCheck_('EXECUTABLE | run=7', '').ok === false) ? 'ok' : 'FAIL'));
  /* v1.14.0 [CONFIRM-BASIS]: basis line (one property read) + pure battery
     (no services). Board of 2 seats: A (ACTIVE) and B (GRACE 1/3, INVEST at
     rank 15 = the buffer boundary); challenger CHAL in raw on days 1-2,
     displaced from raw by NEWC on day 3 while INVEST at rank 12, back on
     day 4. Grid ranks: NEWC 97 (1), A 95 (2), F1..F12 94.5..83.5, CHAL 86
     (12), B 84 (15). */
  report.push('confirm basis (TFB_T10_CONFIRM_BASIS): ' + dt10StabConfirmBasis_() +
              ' (default qualified; raw = v1.13.11 clocks)');
  function cbKnobs(basis, buf) {
    return { enabled: true, confirm_days: 3, exit_days: 3,
             rank_buffer: (buf === undefined ? 15 : buf), smooth_days: 5,
             hard_strict: true, confirm_basis: basis };
  }
  function cbCand(sym, sc, verdict) {
    return { symbol: sym, name: sym, opportunity_score: sc,
             verdict: (verdict === undefined ? 'INVEST' : verdict), price: 10 };
  }
  function cbTk(sym, sc) {
    return { symbol: sym, name: sym, opportunity_score: sc, suggested_sar: 5000,
             suggested_shares: 50, entry_zone: '9-11', stop_sar: 8, tp1_sar: 12,
             tp2_sar: 14, exp_gain_12m_sar: 900, advisor_note: 'INVEST',
             detail: { funds_from: 'Cash 5,000 SAR' } };
  }
  function cbGrid(chalScore, chalVerdict) {
    var g = [cbCand('NEWC.US', 97), cbCand('A.US', 95)];
    for (var cf = 1; cf <= 12; cf++) g.push(cbCand('F' + cf + '.US', 95.5 - cf));
    g.push(cbCand('B.US', 84));
    g.push(cbCand('CHAL.US', chalScore, chalVerdict));
    return g;
  }
  var cbSeed = { v: 1, date: '2026-10-20', symbols: {
      'A.US': { ci: 3, co: 0, member: true, since: '2026-10-15',
                ls: '2026-10-20', hist: [95], ft: false },
      'B.US': { ci: 3, co: 1, member: true, since: '2026-10-15',
                ls: '2026-10-20', hist: [84], ft: false } } };
  var cbRaw = [[cbTk('A.US', 95), cbTk('CHAL.US', 86)],
               [cbTk('A.US', 95), cbTk('CHAL.US', 86)],
               [cbTk('A.US', 95), cbTk('NEWC.US', 97)],
               [cbTk('A.US', 95), cbTk('CHAL.US', 86)]];
  var cbDays = ['2026-10-21', '2026-10-22', '2026-10-23', '2026-10-24'];
  function cbRun(knobs, day3Score, day3Verdict) {
    var st = dt10StabParseState_(cbSeed);
    var outs = [];
    for (var cd = 0; cd < 4; cd++) {
      var grid = (cd === 2) ? cbGrid(day3Score, day3Verdict) : cbGrid(86);
      var o = dt10StabCore_(cbRaw[cd], grid, st, knobs, 2, cbDays[cd]);
      st = dt10StabParseState_(o.state);
      outs.push(o);
    }
    return outs;
  }
  function cbTk4(outs, sym) {
    for (var ct = 0; ct < outs[3].tickets.length; ct++) {
      if (outs[3].tickets[ct].symbol === sym) return outs[3].tickets[ct];
    }
    return null;
  }
  function cbClocks(outs) {   // everything but the armed-basis token
    var cc = [];
    for (var ck = 0; ck < outs.length; ck++) {
      cc.push({ t: outs[ck].tickets, s: outs[ck].state, a: outs[ck].audit,
                n: outs[ck].note.replace(', confirm=qualified', '') });
    }
    return JSON.stringify(cc);
  }
  var cbQ = cbRun(cbKnobs('qualified'), 86);
  var cbR = cbRun(cbKnobs('raw'), 86);
  var cbN = cbRun(cbKnobs(undefined), 86);               // knob absent
  var cbW = cbRun(cbKnobs('qualified'), 86, 'WATCH');   // (c)
  var cbO = cbRun(cbKnobs('qualified'), 83);            // (d) rank 16
  var cbE = cbRun(cbKnobs('qualified'), 84.2);          // rank 15 boundary
  var cbZ = cbRun(cbKnobs('qualified', 0), 86);         // (e)
  var cbZr = cbRun(cbKnobs('raw', 0), 86);
  var opKraw = { enabled: true, confirm_days: 3, exit_days: 3, rank_buffer: 15,
                 smooth_days: 5, hard_strict: true, confirm_basis: 'raw' };
  var opRraw = dt10StabCore_([], [], dt10StabParseState_(opSeed), opKraw, 10,
                             '2026-09-26', opMap);       // (a) P-168 replay
  var cbChal4 = cbTk4(cbQ, 'CHAL.US');
  report.push('confirm basis core: ' +
      ((JSON.stringify(opRraw) === JSON.stringify(opR) &&
        JSON.stringify(cbR) === JSON.stringify(cbN) &&
        cbR[2].state.symbols['CHAL.US'].ci === 0 &&
        cbR[3].state.symbols['CHAL.US'].ci === 1 &&
        cbR[3].tickets.length === 2 && cbTk4(cbR, 'CHAL.US') === null &&
        cbR[2].note.indexOf('confirm=') < 0 && cbR[2].note.indexOf('near') < 0 &&
        cbR[0].state.symbols['B.US'].co === 1 && cbR[0].state.symbols['B.US'].ci === 0 &&
        cbQ[1].state.symbols['CHAL.US'].ci === 2 &&
        cbQ[2].state.symbols['CHAL.US'].ci === 3 &&
        cbQ[2].state.symbols['CHAL.US'].member === false &&
        cbQ[2].tickets.length === 2 &&
        cbQ[2].note.indexOf('pending NEWC.US(1/3)') > 0 &&
        cbQ[2].note.indexOf('confirm=qualified') > 0 &&
        cbQ[2].note.indexOf('near 1 CHAL.US(3/3)') > 0 &&
        cbChal4 !== null && cbChal4._stab_status === 'NEW (confirmed 3/3)' &&
        cbChal4.suggested_sar === 5000 && cbChal4.suggested_shares === 50 &&
        cbChal4._grace_hold !== true && cbChal4._ft_suspended !== true &&
        cbQ[3].audit.entered.length === 1 &&
        cbQ[3].audit.exited_displaced.length === 1 &&
        cbQ[3].audit.exited_displaced[0] === 'B.US' &&
        cbQ[3].note.indexOf('near 1 NEWC.US(2/3)') > 0 &&
        cbQ[0].state.symbols['B.US'].co === 0 && cbQ[0].state.symbols['B.US'].ci === 4 &&
        cbQ[0].state.symbols['A.US'].ci === 4 && cbR[0].state.symbols['A.US'].ci === 4 &&
        cbW[2].state.symbols['CHAL.US'].ci === 0 &&
        cbO[2].state.symbols['CHAL.US'].ci === 0 &&
        cbE[2].state.symbols['CHAL.US'].ci === 3 &&
        cbClocks(cbZ) === cbClocks(cbZr))
       ? 'ok (raw = v1.13.11 clocks; qualified: day-3 rank 12 -> 3/3, day-4 NEW 3/3 sized, ' +
         'WATCH -> 0, rank 16 -> 0, rank 15 -> 3, buffer 0 = legacy, GRACE member co 0)'
       : 'FAIL [' + cbQ[2].note + ' | ' + cbQ[3].note + ' | ' + cbR[3].note + ']'));
  /* v1.15.0 [PENDING-GRACE]: property line (one read) + pure battery on the
     same board. CHAL confirmed days 1-2 (raw), day 3 a SOFT miss (absent from
     raw, audited row verdict WATCH at rank 12, no block), day 4 raw again.
     grace 1: day 3 pauses (ci 2, cp 1, 'pgrace=1', 'pheld 1 CHAL.US(2/3)'),
     day 4 NEW 3/3 sized. grace absent/0: identical to the v1.14.0 cbW replay.
     Two consecutive soft misses with grace 1: day 4 resets. Hard miss
     (structural_block) with grace 1: reset. Member B untouched either way. */
  report.push('pending grace (TFB_T10_PENDING_GRACE_DAYS): ' + dt10StabPendingGrace_() +
              ' (0 = v1.14.0 clocks; 1..' + DT10_T10_PENDING_GRACE_MAX + ' arms)');
  function pgKnobs(pg) {
    var k = cbKnobs('qualified');
    if (pg !== undefined) k.pending_grace = pg;
    return k;
  }
  function pgRun(knobs, grids, raw) {
    var st = dt10StabParseState_(cbSeed);
    var outs = [];
    for (var pd = 0; pd < 4; pd++) {
      var o = dt10StabCore_(raw[pd], grids[pd], st, knobs, 2, cbDays[pd]);
      st = dt10StabParseState_(o.state);
      outs.push(o);
    }
    return outs;
  }
  var pgG = cbGrid(86);
  var pgSoft = cbGrid(86, 'WATCH');
  var pgBlk = cbGrid(86);
  pgBlk[pgBlk.length - 1].structural_block = true;
  var pgRaw2 = [[cbTk('A.US', 95), cbTk('CHAL.US', 86)],
                [cbTk('A.US', 95), cbTk('CHAL.US', 86)],
                [cbTk('A.US', 95), cbTk('NEWC.US', 97)],
                [cbTk('A.US', 95), cbTk('NEWC.US', 97)]];
  var pg1 = pgRun(pgKnobs(1), [pgG, pgG, pgSoft, pgG], cbRaw);
  var pg0 = pgRun(pgKnobs(0), [pgG, pgG, pgSoft, pgG], cbRaw);
  var pgA = pgRun(pgKnobs(undefined), [pgG, pgG, pgSoft, pgG], cbRaw);
  var pg1x2 = pgRun(pgKnobs(1), [pgG, pgG, pgSoft, pgSoft], pgRaw2);
  var pg1h = pgRun(pgKnobs(1), [pgG, pgG, pgBlk, pgG], cbRaw);
  var pgChal4 = cbTk4(pg1, 'CHAL.US');
  var pgBlob2 = JSON.stringify(pg1[2].state.symbols['CHAL.US']);
  // v1.16.0 [FUNDING-EVIDENCE]: pure core dry run (nothing is sent).
  var fundProbe = {fx_rates: {USD: 3.7448}, portfolio: {cash_available_sar: 1, pending_proceeds_sar: 0,
      holdings_input_incomplete: false, holdings: [{symbol: '5023.SR', currency: 'SAR', value_sar: 9390}]}};
  var fundPacket = {schema_version: 1, source_ref: 'selftest', captured_at: '2026-10-26T05:40:00Z',
      accounts: [{account_id: 'ibkr-primary', positions_asof: '2026-10-26T05:40:00Z', positions_complete: true,
                  positions: [], cash_asof: '2026-10-26T05:40:00Z', cash_complete: true,
                  cash: [{currency: 'USD', settled_cash: '22085.12', reserved_cash: '2644.8', reservations_complete: true}]}],
      holding_links: [], funding_accounts: ['ibkr-primary'],
      fx_rates: [{currency: 'USD', rate_to_sar: '3.75', asof: '2026-10-26T05:40:00Z', source_ref: 'policy://sama-usd-peg-3.75'}]};
  var fundRes = dt10FundingApplyCore_(fundProbe, fundPacket, ['5023.SR'], Date.UTC(2026, 9, 26, 5, 45));
  report.push('funding evidence core: ' + ((fundRes.applied && fundRes.cashCents === 7290120 &&
      fundProbe.portfolio.cash_available_sar === 72901.2 && fundProbe.fx_rates.USD === 3.75 &&
      fundProbe.portfolio.holdings.length === 0 && fundRes.excluded.length === 1 &&
      dt10FundingApplyCore_({fx_rates: {}, portfolio: {holdings: [], holdings_input_incomplete: false}},
          fundPacket, [], Date.UTC(2026, 9, 26, 6)).reason.indexOf('capture too old') === 0) ? 'ok' : 'FAIL') +
      ' (mode ' + dt10FundingMode_() + ', packet ' + (dt10ReconciliationEvidence_() ? 'present' : 'absent') + ')');
  report.push('pending grace core: ' +
      ((JSON.stringify(pg0) === JSON.stringify(cbW) &&
        JSON.stringify(pgA) === JSON.stringify(cbW) &&
        pg1[1].state.symbols['CHAL.US'].ci === 2 &&
        pg1[2].state.symbols['CHAL.US'].ci === 2 &&
        pg1[2].state.symbols['CHAL.US'].cp === 1 &&
        pg1[2].state.symbols['CHAL.US'].member === false &&
        pgBlob2.indexOf('"cp":1') > 0 &&
        pg1[2].note.indexOf('pgrace=1') > 0 &&
        pg1[2].note.indexOf('pheld 1 CHAL.US(2/3)') > 0 &&
        pg1[2].audit.pending_held.length === 1 &&
        pg1[3].state.symbols['CHAL.US'].ci === 3 &&
        pg1[3].state.symbols['CHAL.US'].cp === undefined &&
        pg1[3].note.indexOf('pheld') < 0 &&
        pgChal4 !== null && pgChal4._stab_status === 'NEW (confirmed 3/3)' &&
        pgChal4.suggested_sar === 5000 &&
        pg1x2[2].state.symbols['CHAL.US'].ci === 2 &&
        pg1x2[3].state.symbols['CHAL.US'].ci === 0 &&
        pg1x2[3].state.symbols['CHAL.US'].cp === undefined &&
        pg1h[2].state.symbols['CHAL.US'].ci === 0 &&
        pg1h[2].note.indexOf('pheld') < 0 &&
        JSON.stringify(pg1[2].state.symbols['B.US']) ===
            JSON.stringify(cbW[2].state.symbols['B.US']) &&
        cbW[2].audit.pending_held === undefined &&
        cbW[2].note.indexOf('pgrace') < 0)
       ? 'ok (0/absent = v1.14.0 clocks; grace 1: soft miss pauses 2/3 -> NEW 3/3 next raw day, ' +
         'two soft misses -> reset, structural block -> reset, member untouched)'
       : 'FAIL [' + pg1[2].note + ' | ' + pg1[3].note + ' | ' + pg1x2[3].note + ']'));
  /* v1.19.0 [QUALIFIED-HOLD]: property line (one read) + pure battery on the
     confirm-basis board, day 1. Member B (GRACE 1/3) is INVEST at rank 15 =
     the buffer boundary: off / junk = the v1.14.0 qualified replay byte for
     byte; observe = the same board plus ', qhold: would hold 1 B.US' and
     audit.qualified_held; enforce = B 'ACTIVE (day 7)' with no _grace_hold,
     a _qhold marker, out of the grace ledger, board-eligible, still unsized,
     state identical; B WATCH on the grid = 'GRACE (1/3 missed)' as before. */
  report.push('qualified hold (DT10_QUALIFIED_HOLD): ' + dt10StabQualifiedHoldMode_() +
              ' (off = v1.18.0 board; observe lists; enforce holds)');
  function qhKnobs(mode) {
    var k = cbKnobs('qualified');
    if (mode !== undefined) k.qualified_hold = mode;
    return k;
  }
  function qhRun(knobs, grid) {
    return dt10StabCore_(cbRaw[0], grid, dt10StabParseState_(cbSeed), knobs, 2,
                         cbDays[0]);
  }
  function qhSeat(o, sym) {
    for (var qs = 0; qs < o.tickets.length; qs++) {
      if (o.tickets[qs].symbol === sym) return o.tickets[qs];
    }
    return null;
  }
  function qhStrip(o) {   // everything but the armed token / audit key
    var qa = {};
    for (var qk in o.audit) {
      if (o.audit.hasOwnProperty(qk) && qk !== 'qualified_held') qa[qk] = o.audit[qk];
    }
    return JSON.stringify({ t: o.tickets, s: o.state, a: qa,
                            n: o.note.replace(/, qhold: [^,]*$/, '') });
  }
  var qhGridW = cbGrid(86);
  qhGridW[14].verdict = 'WATCH';
  var qhBase = cbQ[0];
  var qhBaseStr = JSON.stringify({ t: qhBase.tickets, s: qhBase.state,
                                   a: qhBase.audit, n: qhBase.note });
  var qhOff = qhRun(qhKnobs(undefined), cbGrid(86));
  var qhOffX = qhRun(qhKnobs('junk'), cbGrid(86));
  var qhObs = qhRun(qhKnobs('observe'), cbGrid(86));
  var qhEnf = qhRun(qhKnobs('enforce'), cbGrid(86));
  var qhEnfW = qhRun(qhKnobs('enforce'), qhGridW);
  var qhB = qhSeat(qhEnf, 'B.US'), qhBo = qhSeat(qhObs, 'B.US');
  var qhBw = qhSeat(qhEnfW, 'B.US');
  report.push('qualified hold core: ' +
      ((JSON.stringify(qhOff) === JSON.stringify(qhBase) &&
        JSON.stringify(qhOffX) === JSON.stringify(qhBase) &&
        qhStrip(qhObs) === qhBaseStr &&
        qhObs.note.indexOf(', qhold: would hold 1 B.US') > 0 &&
        qhObs.audit.qualified_held.length === 1 &&
        qhObs.audit.qualified_held[0] === 'B.US' &&
        qhBo !== null && qhBo._stab_status === 'GRACE (0/3 missed)' &&
        qhBo._grace_hold === true && !dt10BoardEligible_(qhBo) &&
        qhB !== null && qhB._stab_status === 'ACTIVE (day 7)' &&
        qhB._grace_hold !== true && qhB._qhold === true && qhB.rank === 2 &&
        qhB.suggested_sar === '\u2014' && dt10BoardEligible_(qhB) &&
        qhEnf.state.symbols['B.US'].co === 0 &&
        qhEnf.state.symbols['B.US'].member === true &&
        qhEnf.audit.held_by_grace.length === 0 &&
        qhEnf.note.indexOf('grace') < 0 &&
        qhEnf.note.indexOf(', qhold: held 1 B.US') > 0 &&
        JSON.stringify(qhEnf.state) === JSON.stringify(qhBase.state) &&
        JSON.stringify(qhSeat(qhEnf, 'A.US')) === JSON.stringify(qhSeat(qhBase, 'A.US')) &&
        qhBw !== null && qhBw._stab_status === 'GRACE (1/3 missed)' &&
        qhBw._grace_hold === true && qhEnfW.note.indexOf(', qhold: held 0') > 0)
       ? 'ok (off/junk = v1.18.0 replay; observe adds the token only; enforce: B ACTIVE (day 7), ' +
         'eligible, unsized, out of the grace ledger, state identical; B WATCH -> GRACE 1/3)'
       : 'FAIL [' + qhObs.note + ' | ' + qhEnf.note + ' | ' + qhEnfW.note + ']'));
  /* v1.19.0 [SEAT-LIVE-FILL]: property line (one read) + pure core. A stash
     ghost (blank Rel / DQ / FX, stale price 19.88) with a pool row (20.15,
     Rel 71.5, DQ 100, INVESTABLE) is filled and noted; a grid ghost takes
     the grid price, Price SAR and FX and is noted (INVEST); a WATCHLIST pool
     row fills but is not noted; a ghost without any row stays blank; an
     ACTIVE seat and a fast-track seat with no row are untouched; a second
     pass changes nothing; no _dt10_live gives ''. */
  report.push('seat live fill (DT10_SEAT_LIVE_FILL): ' +
              (dt10SeatLiveFillOn_() ? 'on' : 'off') + ' (off = v1.18.0 render)');
  var lfIdx = dt10SeatLiveIndex_([
      { 'Symbol': 'ZTO.US', 'Current Price': 20.15, 'Forecast Reliability Score': 71.5,
        'Data Quality Score': 100, 'Currency': 'USD', 'Investability Status': 'INVESTABLE' },
      { 'Symbol': 'ZTO.US', 'Current Price': 1 },
      { 'Symbol': 'BHF.US', 'Current Price': 50.1, 'Forecast Reliability Score': 65,
        'Data Quality Score': 90, 'Currency': 'USD', 'Investability Status': 'WATCHLIST' },
      { 'Symbol': '' }, null]);
  var lfSel = [
      { symbol: 'ZTO.US', currency: 'USD', price: 19.88, price_sar: '', fx_to_sar: '',
        reliability: '', dq: '', _grace_hold: true, _stab_status: 'GRACE (2/3 missed)',
        advisor_note: 'n1' },
      { symbol: 'DDI.US', currency: 'USD', price: 13.2, price_sar: 49.5, fx_to_sar: '',
        reliability: 71.5, dq: 100, _grace_hold: true, _stab_status: 'GRACE (0/3 missed)',
        advisor_note: 'n2' },
      { symbol: 'BHF.US', currency: 'USD', price: 49, price_sar: '', fx_to_sar: '',
        reliability: '', dq: '', _grace_hold: true, _stab_status: 'GRACE (2/3 missed)',
        advisor_note: 'n3' },
      { symbol: 'NONE.US', currency: 'USD', price: 5, price_sar: '', fx_to_sar: '',
        reliability: '', dq: '', _grace_hold: true, _stab_status: 'GRACE (1/3 missed)',
        advisor_note: 'n4' },
      { symbol: 'ACT.US', currency: 'USD', price: 7, price_sar: 26.25, fx_to_sar: 3.75,
        reliability: 80, dq: 95, _stab_status: 'ACTIVE (day 3)', advisor_note: 'n5' },
      { symbol: 'FT.US', currency: 'USD', price: 9, price_sar: 33.75, fx_to_sar: 3.75,
        reliability: 77, dq: 90, _ft_suspended: true, _stab_status: 'FAST-TRACK (day 1)',
        advisor_note: 'n6' }];
  var lfCands = [{ symbol: 'DDI.US', verdict: 'INVEST', price: 13.4, price_sar: 50.25,
                   reliability: 71.5, dq: 100, currency: 'USD', opportunity_score: 80 },
                 { symbol: 'ACT.US', verdict: 'INVEST', price: 7.5, price_sar: 28.13,
                   reliability: 80, dq: 95, currency: 'USD', opportunity_score: 70 }];
  var lfBefore = JSON.stringify([lfSel[4], lfSel[5]]);
  var lfRes = dt10SeatLiveFillCore_(lfSel, lfCands, lfIdx, { USD: 3.75 });
  var lfOnce = JSON.stringify(lfSel);
  var lfRes2 = dt10SeatLiveFillCore_(lfSel, lfCands, lfIdx, { USD: 3.75 });
  report.push('seat live fill core: ' +
      ((lfRes.seats === 5 && lfRes.filled === 3 && lfRes.investable === 2 &&
        lfSel[0].price === 20.15 && lfSel[0].reliability === 71.5 && lfSel[0].dq === 100 &&
        lfSel[0].fx_to_sar === 3.75 && lfSel[0].price_sar === 75.56 &&
        lfSel[0].advisor_note ===
            'n1 Source row today: investable (Investability Status INVESTABLE).' &&
        lfSel[0]._stab_status === 'GRACE (2/3 missed)' && lfSel[0]._grace_hold === true &&
        lfSel[1].price === 13.4 && lfSel[1].price_sar === 50.25 &&
        lfSel[1].fx_to_sar === 3.75 &&
        lfSel[1].advisor_note === 'n2 Source row today: investable (INVEST on the audited grid).' &&
        lfSel[2].price === 50.1 && lfSel[2].reliability === 65 && lfSel[2].dq === 90 &&
        lfSel[2].advisor_note === 'n3' &&
        lfSel[3].price === 5 && lfSel[3].reliability === '' && lfSel[3].fx_to_sar === '' &&
        JSON.stringify([lfSel[4], lfSel[5]]) === lfBefore &&
        lfRes2.filled === 0 && lfRes2.investable === 0 && JSON.stringify(lfSel) === lfOnce &&
        lfIdx['ZTO.US'].price === 20.15 && dt10SeatLiveIndex_(null)['X'] === undefined &&
        dt10SeatLiveFill_({ selected: lfSel }) === '')
       ? 'ok (stash ghost filled from the pool row and noted; grid ghost from the grid; ' +
         'WATCHLIST row not noted; no row = blank; ACTIVE / fast-track untouched; idempotent)'
       : 'FAIL [' + JSON.stringify(lfRes) + ' ' + JSON.stringify(lfSel[0]) + ']'));
  /* v1.19.0 [OWNER-RULE]: property line (one read) + pure cell / note. */
  report.push('owner rule mark (DT10_OWNER_RULE_MARK): ' +
              (dt10OwnerRuleMarkOn_() ? 'on' : 'off') +
              ' (High + Engine ROI 12M < ' + DT10_OWNER_RULE_ROI_MAX + '; display only)');
  var orSel = [{ confidence_band: 'High', engine_roi_pct: 31.6 },
               { confidence_band: 'Medium', engine_roi_pct: 34.8 },
               { confidence_band: 'High', engine_roi_pct: 34 },
               { confidence_band: 'Medium', engine_roi_pct: 20 },
               { confidence_band: '', engine_roi_pct: '' },
               { confidence_band: 'high', engine_roi_pct: '33.9' },
               { confidence_band: 'Low' }];
  var orCells = [];
  for (var ori = 0; ori < orSel.length; ori++) orCells.push(dt10OwnerRuleCell_(orSel[ori]));
  report.push('owner rule core: ' +
      ((orCells.join('|') === 'rule: yes|rule: no (Medium; ROI 34.8)|rule: no (ROI 34.0)|' +
            'rule: no (Medium)|rule: no (conf unknown; ROI unknown)|rule: yes|' +
            'rule: no (Low; ROI unknown)' &&
        dt10OwnerRuleNote_(orSel) === 'owner-rule 2/7 yes' &&
        dt10OwnerRuleNote_([]) === 'owner-rule 0/0 yes' &&
        dt10OwnerRuleCell_(null) === 'rule: no (conf unknown; ROI unknown)')
       ? 'ok (High + ROI < ' + DT10_OWNER_RULE_ROI_MAX + ' = yes; the failing part is named)'
       : 'FAIL [' + orCells.join('|') + ']'));
  var msg = report.join('\n');
  Logger.log(msg);
  try {
    SpreadsheetApp.getUi().alert(msg);
  } catch (e) {}
  return msg;
}
