/**
 * ============================================================================
 * 16_Decision_Top10.gs — Top_10_Investments DECISION page (frontend renderer)
 * Version: 1.10.1  (see DT10_VERSION; header kept in lockstep — restored
 *                  again at v1.6.6 after drifting to 1.6.4 while
 *                  DT10_VERSION read 1.6.5)
 * Runtime: ES5 ONLY (V8 exceptions are 01_Menu.gs / 03_Schema.gs only).
 * ============================================================================
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
 * endpoint, and renders the FROZEN §5 payload zones verbatim. No gating,
 * scoring, sizing, or verdict logic is re-implemented here (§8).
 *
 * §5 POOL-SOURCE AMENDMENT (adopted 2026-06-12)
 * ---------------------------------------------
 * The backend selector path builds source pages under internal budgets
 * (page_sec 30 / total_sec 70) which free-tier engine builds (50–175s/page)
 * always exceed → pool starvation (3 snapshot rows, coverage 0.0). Therefore
 * the PRIMARY pool source is the spreadsheet itself: this file reads the
 * already-refreshed Market_Leaders / Global_Markets / Commodities_FX /
 * Mutual_Funds rows and POSTs them as `rows` (the route's body_rows path,
 * live-tested). The backend selector remains the automatic fallback when
 * Pool Source = "Backend" or when the sheets yield no rows. Bonus: decisions
 * are computed from exactly the data visible on the pages (transparency).
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
var DT10_VERSION = '1.10.1';
// v1.8.8 master toggle — DEFAULT false => v1.8.7 rendering byte-for-byte.
// true arms BOTH v1.8.8 display-truth changes (G-a seat-truth KPI cell,
// G-b structural Why-Not-Selected). Operator-armed, deliberately, like an
// ENV kill-switch (S-1 window law).
var DT10_V188_SEAT_TRUTH = false;
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
  { label: 'T10: Max Per Sector', def: 2, kind: 'int' },
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
  // v1.6.1: the quote's own timestamp MUST travel with the row — without it
  // the backend's freshness + data-trust gates skip every body_rows
  // candidate by design (proven-staleness-only). Source pages publish
  // 'Last Updated (UTC)'; builder alias: lastupdatedutc.
  { send: 'Last Updated (UTC)',
    match: ['lastupdatedutc', 'lastupdated', 'asof'] },
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
    match: ['forecastsource'] }
];
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
/** Map DT10_POOL_FIELDS → column index from a header row array; null=absent */
function dt10MapHeaderCols_(headerRow) {
  var byToken = {};
  for (var c = 0; c < headerRow.length; c++) {
    var t = dt10NormToken_(headerRow[c]);
    if (t && byToken[t] === undefined) byToken[t] = c;
  }
  var map = {};
  for (var i = 0; i < DT10_POOL_FIELDS.length; i++) {
    var spec = DT10_POOL_FIELDS[i];
    var col = null;
    for (var m = 0; m < spec.match.length; m++) {
      if (byToken[spec.match[m]] !== undefined) {
        col = byToken[spec.match[m]];
        break;
      }
    }
    map[spec.send] = col;
  }
  return map;
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
  var sym = row[symCol];
  if (!dt10HasValue_(sym)) return null;
  var s = String(sym).trim();
  if (s.charAt(0) === '—' || s.toLowerCase().indexOf('no data') === 0) {
    return null;
  }
  // v1.2.2: skip ghost/header rows whose Symbol cell is a literal header token.
  var su = s.toUpperCase();
  if (su === 'SYMBOL' || su === 'TICKER') return null;
  var out = {};
  for (var i = 0; i < DT10_POOL_FIELDS.length; i++) {
    var send = DT10_POOL_FIELDS[i].send;
    var col = colMap[send];
    if (col === null || col === undefined) continue;
    var v = row[col];
    if (dt10HasValue_(v)) out[send] = v;
  }
  if (!dt10HasValue_(out['Exchange'])) out['Exchange'] = pageName;
  return out;
}
/** v1.2.2 — normalize a symbol for de-duplication (trim + uppercase). */
function dt10NormSym_(v) {
  return String(v === null || v === undefined ? '' : v).trim().toUpperCase();
}
/** v1.5.0 (Fix H1) PURE: one My_Portfolio sheet row -> holdings entry for
 * the builder's Portfolio gate, or null. Exclusion needs symbol only;
 * sector/value are best-effort context for the builder's sector-cap math.
 * Indices may be -1 (column absent). Node-testable (no Apps Script
 * services). */
function dt10HoldingFromRow_(row, iSym, iSec, iVal) {
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
  return { symbol: sym, sector: sector || 'Unknown', value_sar: val };
}
/** v1.5.0 (Fix H1): read the operator's holdings from My_Portfolio for the
 * request body. Header auto-scan (dt10FindHeaderRow_); Symbol mandatory;
 * an explicitly-SAR-labelled value column is preferred over raw Position
 * Value; cap 500 rows; per-symbol dedupe. Fail-open: any fault logs and
 * returns [] — a broken portfolio page can never break the Top_10
 * refresh (the status line then discloses held=0). */
function dt10CollectHoldings_(ss) {
  try {
    var sheet = ss.getSheetByName('My_Portfolio');
    if (!sheet) return [];
    var values = sheet.getDataRange().getValues();
    if (!values || !values.length) return [];
    var hIdx = dt10FindHeaderRow_(values, DT10_HEADER_SCAN_ROWS);
    if (hIdx < 0) return [];
    var hdr = values[hIdx];
    var iSym = -1, iSec = -1, iValSar = -1, iVal = -1;
    for (var c = 0; c < hdr.length; c++) {
      var t = dt10NormToken_(hdr[c]);
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
      }
    }
    if (iSym < 0) return [];
    var useVal = iValSar >= 0 ? iValSar : iVal;
    var out = [];
    var seen = {};
    for (var r = hIdx + 1; r < values.length && out.length < 500; r++) {
      var h = dt10HoldingFromRow_(values[r], iSym, iSec, useVal);
      if (!h) continue;
      if (seen[h.symbol]) continue;
      seen[h.symbol] = true;
      out.push(h);
    }
    return out;
  } catch (eCh) {
    try {
      Logger.log('[DT10 v' + DT10_VERSION + '] holdings read failed: ' + eCh);
    } catch (eLog) {}
    return [];
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
        id: vid
      };
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
function dt10FastTrackSuspend_(tk) {
  tk._ft_suspended = true;
  tk.entry_zone = '—';
  tk.suggested_sar = '—';
  tk.suggested_shares = '—';
  tk.stop_sar = '—';
  tk.tp1_sar = '—';
  tk.tp2_sar = '—';
  tk.exp_gain_12m_sar = '—';
  tk.advisor_note = 'FAST-TRACK (day 1) — sizing suspended under ' +
      'strict until confirmed (' +
      'seat filled to avoid under-fill; not an executable ticket today).';
  return tk;
}
/**
 * Membership-hysteresis core. Inputs:
 *   rawTickets  payload.selected (backend's memoryless answer, best-first)
 *   cands       payload.candidates_rows (the audited grid = the "pools")
 *   state       sanitized blob (dt10StabParseState_)
 *   knobs       dt10StabKnobs_ output
 *   limit       seats (panel 'T10: Max Selected')
 *   today       'yyyy-MM-dd' UTC key
 * Returns { tickets, state, audit, note }. §8: selects AMONG the backend's
 * audited candidates only — never recomputes a gate, score, or verdict.
 */
function dt10StabCore_(rawTickets, cands, state, knobs, limit, today) {
  rawTickets = rawTickets || [];
  cands = cands || [];
  limit = Math.max(1, parseInt(limit, 10) || 10);
  var symbols = state.symbols;
  var dayAdvance = state.date !== today;
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
  // ---- counter pass (day-keyed) -------------------------------------------
  var uni = {};
  var us;
  for (us in symbols) { if (symbols.hasOwnProperty(us)) uni[us] = true; }
  for (var ur = 0; ur < rawSyms.length; ur++) uni[rawSyms[ur]] = true;
  for (var sym in uni) {
    if (!uni.hasOwnProperty(sym)) continue;
    var st = symbols[sym];
    if (!st) {
      st = { ci: 0, co: 0, member: false, since: '', ls: '', hist: [] };
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
      if (rawSet[sym]) {
        st.ci = (st.ci || 0) + 1;
        st.co = 0;
      } else {
        st.ci = 0;
        if (st.member) {
          var r = globalRank[sym];
          var within = knobs.rank_buffer > 0 && r && r <= knobs.rank_buffer;
          if (!within) st.co = (st.co || 0) + 1;
          // within buffer: rank-jitter immunity — the exit clock PAUSES
          // for the day (it does not reset).
        }
      }
      if (sc !== null) st.hist.push(Math.round(sc * 10) / 10);
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
    } else if (!mst.since) {
      mst.since = today;
    }
    if (!rawSet[msym] || (mst.co || 0) > 0) {
      heldByGrace.push({ symbol: msym, missed_days: mst.co || 0,
                         exit_days: knobs.exit_days });
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
    } else if (!rawSet[osym] || (ost.co || 0) > 0) {
      status = 'GRACE (' + (ost.co || 0) + '/' + knobs.exit_days + ' missed)';
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
  if (fastTracked.length) noteBits.push(fastTracked.length + ' fast-track');
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
  return {
    tickets: tickets,
    state: state,
    audit: { entered: entered, fast_tracked: fastTracked,
             exited_hard: exitedHard, exited_soft: exitedSoft,
             exited_capacity: exitedCapacity,
             exited_displaced: exitedDisplaced,
             exited_lost: exitedLost,
             held_by_grace: heldByGrace, pending: pending,
             state_pruned: pruned },
    note: note
  };
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
var DT10_UV_BOARD_WITHHOLD_IDX = [9, 10, 11, 12, 13, 14, 15, 17, 18, 22];
var DT10_UV_BOARD_NOTE_IDX = 29;   /* Advisor Note (board, 0-based) */
var DT10_UV_LOG_WITHHOLD_IDX = [12, 13, 14, 15, 16, 17, 18, 20, 21, 25];
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
  if (noteIdx < row.length) row[noteIdx] = dt10UvWithholdNote_(reason);
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
var DT10_CAND_HEADERS = ['Symbol', 'Name', 'Market', 'Sector', 'Ccy', 'Price',
  'Price SAR', 'ROI % (TP1)', 'Engine ROI % (12M)', 'Ann ROI %', 'R/R (TP2)', 'Rel', 'DQ',
  'Risk', 'News',
  'Sector Trend', 'Conflict', 'Verdict', 'Conf', 'Score', 'First Fail',
  'Failure Reason', 'Structural', 'Selected', 'Deferral'];
function dt10CandToRow_(c) {
  var ff = c.first_fail || null;
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
          c.selected === true ? 'Yes' : 'No', dt10Cell_(c.deferral)];
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
  else if (DT10_V188_SEAT_TRUTH && c.structural_block === true &&
           (dt10HasValue_(c.failure_reason) ||
            (c.first_fail && dt10HasValue_(c.first_fail.gate)))) {
    // v1.8.8 (G-b): a structurally excluded qualifier names its REAL
    // reason (the audit grid already carries it) — never the rank cut.
    why = dt10HasValue_(c.failure_reason)
        ? String(c.failure_reason)
        : ('structural: ' + String(c.first_fail.gate));
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
var DT10_KPI_LABELS = ['Deployable (SAR)', 'Exp. Gain 12M (SAR)', 'Selected',
  'Blended Reliability', 'Blended R/R (TP2)', 'Scanned', 'Passed',
  'Unallocated (SAR)'];
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
          dt10Cell_(k.capital_unallocated_sar)];
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
function dt10StatusLine_(status, extra) {
  var stamp = '';
  try {
    stamp = Utilities.formatDate(new Date(),
        Session.getScriptTimeZone() || 'Asia/Riyadh', 'yyyy-MM-dd HH:mm:ss');
  } catch (e) {
    stamp = new Date().toString();
  }
  return 'Last run ' + stamp + ' | status: ' + status +
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
 * Default ON; Script Property DT10_SEND_HOLDINGS = 0/false/off/no restores
 * the exact v1.4.0 request body. Fail-open ON. */
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
/** v1.6.0 (W-3): PURE — {SYMBOL: daysToEarnings} from a raw Calendar_Events
 * matrix. Header row = the one whose cells contain 'symbol'; the days come
 * from the 'Days To Earnings' column; only finite values >= 0 are kept
 * (blank, junk, and past dates never tag). */
function dt10EarningsMapFromValues_(values) {
  var map = {};
  if (!values || !values.length) return map;
  var iSym = -1, iDays = -1, r, c, cells, tok;
  for (r = 0; r < values.length && iSym < 0; r++) {
    cells = values[r] || [];
    for (c = 0; c < cells.length; c++) {
      tok = dt10NormToken_(cells[c]);
      if (tok === 'symbol') iSym = c;
      else if (tok === 'daystoearnings') iDays = c;
    }
    if (iSym >= 0 && iDays < 0) iSym = -1; // need both on the same row
  }
  if (iSym < 0 || iDays < 0) return map;
  for (; r < values.length; r++) {
    cells = values[r] || [];
    var sym = dt10NormSym_(cells[iSym]);
    if (!sym || map.hasOwnProperty(sym)) continue;
    // v1.8.3: a BLANK days cell must not coerce to 0 ("earnings today").
    // Number('') === 0 passed the >=0 gate, so every Calendar_Events row
    // with an empty Days-To-Earnings cell false-tagged its symbol with a
    // same-day \u26a0 on the board — the exact case the v1.6.0 self-test
    // fixture (JUNK.X) was written to catch, and did: 'earnings tag
    // core: FAIL' on 2026-08-09 led straight here.
    if (String(cells[iDays] === null || cells[iDays] === undefined ?
               '' : cells[iDays]).trim() === '') continue;
    var d = Number(cells[iDays]);
    if (isFinite(d) && d >= 0) map[sym] = Math.floor(d);
  }
  return map;
}
/** v1.6.0 (W-3): live Calendar_Events read. ANY failure => empty map =>
 * zero tags — the annotation must never be able to break the render. */
function dt10EarningsMap_(ss) {
  try {
    var sh = ss.getSheetByName('Calendar_Events');
    if (!sh) return {};
    return dt10EarningsMapFromValues_(sh.getRange('A1:H200').getValues());
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
    return { note: n ? 'earn \u26a0' + n + '/' + sel.length : '' };
  } catch (eEa) {
    return { note: '' };
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
 * within the cap, sends it ALL (full universe); otherwise round-robins across
 * the pages so each is fairly represented. Returns per-page included AND
 * available counts plus total/truncated for an honest status line.
 */
function dt10CollectPoolRows_(ss, limit) {
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
  for (var p = 0; p < DT10_POOL_PAGES.length; p++) {
    var pageName = DT10_POOL_PAGES[p];
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
    var colMap = dt10MapHeaderCols_(values[hIdx]);
    for (var r = hIdx + 1; r < values.length; r++) {
      var obj = dt10PoolRowFromSheetRow_(values[r], colMap, pageName);
      if (!obj) continue;
      var key = dt10NormSym_(obj['Symbol']);
      if (key && seen[key]) { duplicatesSkipped++; continue; }
      if (key) seen[key] = true;
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
           duplicatesSkipped: duplicatesSkipped };
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
function dt10ApplyStability_(payload, panel) {
  var knobs = dt10StabKnobs_(panel);
  if (!knobs.enabled) return { note: 'stab: OFF (panel switch)' };
  // v1.8.0 (BE-2): the service read lives HERE so dt10StabCore_ stays
  // pure/node-testable; the core consumes the choice as a plain knob.
  knobs.hard_strict = dt10HardVerdictStrict_();
  var limit = parseInt(panel['T10: Max Selected'], 10);
  if (!(limit > 0)) limit = 10;
  var loaded = dt10StabLoadEx_();
  var state = loaded.state;
  var out = dt10StabCore_(payload.selected || [],
                          payload.candidates_rows || [],
                          state, knobs, limit, dt10StabToday_());
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
function dt10SelLogSignature_(tickets) {
  /* v1.8.9: seat-truth signature — see header WHY. Tokens change when
   * funding / executability / stability class change; they do NOT change
   * on price, score or rank-order wiggles alone. */
  tickets = tickets || [];
  var toks = [];
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
    for (var i = 0; i < sel.length; i++) {
      var t = sel[i] || {};
      var px = Number(t.price_sar), st = Number(t.stop_sar),
          tp2 = Number(t.tp2_sar), rel = Number(t.reliability);
      if (isFinite(px) && isFinite(st) && isFinite(tp2) &&
          px > 0 && px > st && tp2 > px) {
        rrSum += (tp2 - px) / (px - st);
        rrN++;
      }
      if (isFinite(rel) && rel > 0) { relSum += rel; relN++; }
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
    var execN = 0, graceN = 0;
    for (var i = 0; i < sel.length; i++) {
      if (sel[i] && sel[i]._grace_hold === true) graceN++;
      else execN++;
    }
    if (execN === funded) return '';
    return 'SEAT-CHECK kpi ' + funded + ' funded vs board ' + execN +
           ' exec' + (graceN ? ' +' + graceN + ' grace' : '');
  } catch (eSc) {
    return '';
  }
}
/**
 * v1.8.8 (G-a) PURE: the seat-truth text for KPI cell 3 —
 * 'E exec + P pend + G grace / M'. Sources: payload.selected._grace_hold
 * (the D-1 annotation), the BE-1 pending source
 * (payload.meta.stability.audit.pending, via dt10StabPendingMap_), and
 * kpis.max_selected. Returns '' when there is nothing to say or on ANY
 * error — the v1.8.7 cell then stands untouched (fail-open).
 */
function dt10SeatTruthKpi_(payload) {
  try {
    var sel = (payload && payload.selected) || [];
    var execN = 0, graceN = 0;
    for (var i = 0; i < sel.length; i++) {
      if (sel[i] && sel[i]._grace_hold === true) graceN++;
      else if (sel[i]) execN++;
    }
    var pendMap = dt10StabPendingMap_(payload);
    var pendN = 0, pk;
    for (pk in pendMap) {
      if (Object.prototype.hasOwnProperty.call(pendMap, pk)) pendN++;
    }
    if (execN === 0 && graceN === 0 && pendN === 0) return '';
    var bits = [execN + ' exec'];
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
/**
 * Append the rendered board to _Selection_Log. Modes (ScriptProperty
 * DT10_SELECTION_LOG): 'off' kill-switch; 'always' every run; unset =
 * DEFAULT: once per UTC day PLUS any run whose membership set changed.
 * NEVER throws — every failure degrades to the returned status note.
 * v1.10.0: under a non-EXECUTABLE upstream verdict the appended rows are
 * withheld (same surface as the board) via a self-contained fail-closed
 * verdict read — the log is an audit trail, never an order sheet.
 */
function dt10AppendSelectionLog_(ss, tickets, runInfo, panel) {
  try {
    var mode = '';
    try {
      mode = String(PropertiesService.getScriptProperties()
          .getProperty(DT10_SELLOG_MODE_PROP) || '').toLowerCase();
    } catch (eP) {}
    if (mode === 'off') return 'SelLog: off';
    tickets = tickets || [];
    if (!tickets.length) return 'SelLog: empty board (not logged)';
    var today = dt10StabToday_();
    var sig = dt10SelLogSignature_(tickets);
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
    if (dt10UvOn_()) {
      var slUv;
      try {
        slUv = dt10UvParse_(dt10UvRead_(ss), Date.now());
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
    var seed = defaults.hasOwnProperty(item.label) &&
               dt10HasValue_(defaults[item.label]) ? defaults[item.label]
                                                   : item.def;
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
  sheet.getRange(row, 1, dataRows.length, headers.length)
      .setFontColor(DT10_INK);
  for (var i = 0; i < dataRows.length; i += 2) {
    sheet.getRange(row + i, 1, 1, headers.length).setBackground(DT10_ZEBRA);
  }
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
  var vals = sheet.getRange(firstDataRow, col, count, 1).getValues();
  for (var i = 0; i < count; i++) {
    var v = String(vals[i][0] || '');
    var tok = null;
    if (v.indexOf('ACTIVE') === 0 || v.indexOf('NEW') === 0) {
      tok = tokens.VERDICT_POSITIVE;
    } else if (v.indexOf('GRACE') === 0 || v.indexOf('FAST-TRACK') === 0) {
      tok = tokens.VERDICT_WATCH;
    }
    if (tok) {
      sheet.getRange(firstDataRow + i, col).setBackground(tok.bg)
          .setFontColor(tok.fg).setFontWeight('bold');
    }
  }
}
/** Verdict/conf cell coloring via §3.4 tokens on a written table column. */
function dt10ColorVerdicts_(sheet, firstDataRow, count, col, tokens) {
  if (firstDataRow < 0 || !count) return;
  var rng = sheet.getRange(firstDataRow, col, count, 1);
  var vals = rng.getValues();
  for (var i = 0; i < count; i++) {
    var v = String(vals[i][0] || '');
    var tok = null;
    if (v === 'INVEST') tok = tokens.VERDICT_POSITIVE;
    else if (v === 'WATCH') tok = tokens.VERDICT_WATCH;
    else if (v === 'DO_NOT_INVEST') tok = tokens.VERDICT_NEGATIVE;
    else if (v) tok = tokens.VERDICT_BLOCK;
    if (tok) {
      sheet.getRange(firstDataRow + i, col).setBackground(tok.bg)
          .setFontColor(tok.fg).setFontWeight('bold');
    }
  }
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
  for (var i = 0; i < count; i++) {
    var c = qualified[i] || {};
    var tok;
    if (c.selected === true) tok = tokens.VERDICT_POSITIVE;
    else if (dt10HasValue_(c.deferral)) tok = tokens.VERDICT_WATCH;
    else tok = tokens.VERDICT_BLOCK;
    if (tok) {
      sheet.getRange(firstDataRow + i, col).setBackground(tok.bg)
          .setFontColor(tok.fg).setFontWeight('bold');
    }
  }
}
// ---------------------------------------------------------------------------
// Main refresh
// ---------------------------------------------------------------------------
function refreshDecisionTop10() {
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
  statusCell.setValue(dt10StatusLine_('running\u2026', ''));
  SpreadsheetApp.flush();
  var panel = dt10ReadPanel_(sheet, ss);
  var poolLimit = parseInt(panel['Pool Limit'], 10);
  // v1.2.3: blank / 0 / non-positive Pool Limit == scan the full universe.
  if (!(poolLimit > 0)) poolLimit = DT10_POOL_HARD_CAP;
  var useSheets = String(panel['Pool Source'] || 'Sheets')
      .toLowerCase().indexOf('backend') === -1;
  var body = {
    criteria: dt10CriteriaFromPanel_(panel),
    fx_rates: dt10FxRates_(ss),
    portfolio: {
      cash_available_sar: Number(panel['Cash Available (SAR)']) || 0,
      pending_proceeds_sar: Number(panel['Pending Proceeds (SAR)']) || 0
    },
    pool_limit: poolLimit
  };
  // v1.5.0 (Fix H1): give the builder's Portfolio gate the held symbols it
  // needs to enforce "Include Portfolio Holdings = No". Kill switch
  // DT10_SEND_HOLDINGS=0 restores the exact v1.4.0 body (key absent).
  var heldNote = '';
  if (dt10SendHoldingsEnabled_()) {
    var heldRows = dt10CollectHoldings_(ss);
    if (heldRows.length) {
      body.portfolio.holdings = heldRows;
      heldNote = ' | held=' + heldRows.length + ' sent';
    } else {
      heldNote = ' | held=0 (My_Portfolio empty/unreadable \u2014 gate blind)';
    }
  }
  var poolNote = 'backend selector';
  if (useSheets) {
    var pool = dt10CollectPoolRows_(ss, poolLimit);
    if (pool.rows.length) {
      body.rows = pool.rows;
      // v1.2.0: included/available per sheet so empty/stale pages are visible.
      var parts = [];
      for (var pp = 0; pp < DT10_POOL_PAGES.length; pp++) {
        var pn = DT10_POOL_PAGES[pp];
        parts.push(pn + ' ' + (pool.perPage[pn] || 0) + '/' +
                   (pool.available[pn] || 0));
      }
      poolNote = 'sheets pool ' + pool.rows.length + ' rows [' +
                 parts.join(', ') + '] ' +
                 (pool.duplicatesSkipped ?
                  '(' + pool.duplicatesSkipped + ' duplicate symbols removed) ' :
                  '') +
                 (pool.truncated ? '(TRUNCATED to cap ' + pool.cap + ' of ' +
                  pool.total + ' available — raise Pool Limit to scan ' +
                  'all)' : '(full universe)');
    } else {
      poolNote = 'sheets pool EMPTY \u2192 backend selector fallback';
    }
  }
  Logger.log('[DT10 v' + DT10_VERSION + '] POST ' + DT10_ENDPOINT + ' | ' +
             poolNote);
  var resp;
  try {
    resp = dt10Post_(body);
  } catch (eNet) {
    statusCell.setValue(dt10StatusLine_('NETWORK ERROR',
        String(eNet) + dt10StaleNote_()));
    // v1.4.0: cockpit-owned _Status — failure is reported, never silent.
    dt10WritePageStatus_('ERROR',
        'Decision cockpit: NETWORK ERROR — ' + String(eNet).slice(0, 300),
        '', '', '', []);
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
    return;
  }
  // v1.3.0: membership hysteresis between response and render. The audit
  // grid / qualified / near-miss zones keep the backend's RAW truth; only
  // the SELECTED board gains memory.
  var stab = dt10ApplyStability_(payload, panel);
  // v1.6.0 (W-3): earnings proximity tag — annotation-only, never gates.
  var earn = dt10EarningsAnnotate_(payload, ss);
  // v1.6.6 (S-5): board-vs-backend KPI verification — token only.
  var kpiNote = dt10KpiCheckNote_(payload);
  // v1.8.0 (S-6): reconcile funded-pick KPI vs the board's executable set.
  var seatNote = dt10SeatCheckNote_(payload);
  dt10RenderPayload_(sheet, payload, tokens);
  var secs = Math.round((new Date().getTime() - t0) / 100) / 10;
  var statusLine = dt10StatusLine_(String(payload.status || '?'),
      poolNote + heldNote + ' | ' + dt10MetaLine_(payload.meta) +
      (stab && stab.note ? ' | ' + stab.note : '') +
      (earn && earn.note ? ' | ' + earn.note : '') +
      (kpiNote ? ' | ' + kpiNote : '') +
      (seatNote ? ' | ' + seatNote : '') +
      dt10FunnelNote_(payload) + ' | ' + secs + 's');
  // v1.3.1: the cockpit OWNS the selection audit trail — log the board it
  // just rendered (in-memory; 17_Selection_Log.gs scraping retired). The
  // Run Info column carries this exact status line, pre-'SelLog' tail.
  var selLogNote = dt10AppendSelectionLog_(ss, payload.selected || [],
                                           statusLine, panel);
  // v1.6.6 (S-4): membership departures logged with an Outcome stamp.
  var exitNote = dt10AppendExitLog_(ss,
      (stab && stab.audit) ? stab.audit : {}, statusLine);
  var finalStatusLine = selLogNote ? statusLine + ' | ' + selLogNote
                                   : statusLine;
  if (exitNote) finalStatusLine += ' | ' + exitNote;
  statusCell.setValue(finalStatusLine);
  // v1.6.2: remember this good render so a later 502 can date the board.
  dt10RememberSuccess_(
      Utilities.formatDate(new Date(),
          Session.getScriptTimeZone() || 'Asia/Riyadh', 'yyyy-MM-dd HH:mm'),
      (payload.selected || []).length);
  // v1.4.0: cockpit-owned _Status — same text as the banner, one truth.
  dt10WritePageStatus_('OK', finalStatusLine, resp.code,
      (payload.selected || []).length, Math.round(secs * 1000), []);
  Logger.log('[DT10 v' + DT10_VERSION + '] done status=' + payload.status +
             ' in ' + secs + 's');
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
  var ts = s.match(/(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})/);
  if (!ts) {
    out.reason = 'verdict timestamp unreadable';
    return out;
  }
  var utc = Date.UTC(+ts[1], +ts[2] - 1, +ts[3], +ts[4], +ts[5], +ts[6]);
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
  // KPI strip
  var kpiRange = sheet.getRange(DT10_ROW_KPI_VALUES, 1, 1,
                                DT10_KPI_LABELS.length);
  kpiRange.setValues([dt10KpiValues_(payload.kpis)]);
  // v1.8.8 (G-a): seat-truth Selected cell — toggle-gated, fail-open
  // ('' => the v1.8.7 cell stands).
  if (DT10_V188_SEAT_TRUTH) {
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
  var cands = payload.candidates_rows || [];
  // SELECTED
  // v1.6.7 (D-1): separate the two classes in the TITLE. A grace-held ghost
  // has no entry/ticket/stop/TP - calling it an executable ticket is what
  // misled a reader on 2026-07-27.
  var dt10ExecN = 0, dt10GraceN = 0;
  for (var gh = 0; gh < selected.length; gh++) {
    if (selected[gh] && selected[gh]._grace_hold === true) dt10GraceN++;
    else dt10ExecN++;
  }
  var dt10SelTitle;
  if (dt10GraceN > 0) {
    dt10SelTitle = 'SELECTED — ' + dt10ExecN + ' EXECUTABLE TICKET' +
        (dt10ExecN === 1 ? '' : 'S') + ' + ' + dt10GraceN +
        ' GRACE-HELD (NO PLAN TODAY)';
  } else {
    dt10SelTitle = 'SELECTED — EXECUTABLE TICKETS (' + selected.length + ')';
  }
  /* v1.9.0 W1A-4a: consume the upstream verdict BEFORE any sizing is
   * shown. Fail-closed: read/parse trouble withholds sizing. */
  var dt10Uv = { state: 'EXECUTABLE', reason: '', ageMin: null };
  if (dt10UvOn_()) {
    try {
      dt10Uv = dt10UvParse_(dt10UvRead_(sheet.getParent()), Date.now());
    } catch (e) {
      dt10Uv = { state: 'NOT_ACTIONABLE',
                 reason: 'verdict read failed', ageMin: null };
    }
    if (dt10Uv.state !== 'EXECUTABLE') {
      /* v1.9.1 (IR-089): under a blocked feed nothing is executable —
       * recount the embedded title as qualified PLANS (review Q1).
       * Wording only; gate + sizing blanking below are untouched. */
      var dt10QualN = dt10ExecN;
      dt10SelTitle = 'SELECTED \u2014 0 EXECUTABLE / ' + dt10QualN +
          ' QUALIFIED PLAN' + (dt10QualN === 1 ? '' : 'S') +
          (dt10GraceN > 0 ? ' + ' + dt10GraceN +
              ' GRACE-HELD (NO PLAN TODAY)' : '');
      dt10SelTitle = '\u26d4 FEED NOT ACTIONABLE \u2014 ' + dt10Uv.reason +
          ' \u2014 ' + dt10SelTitle + ' \u2014 SIZING WITHHELD';
      Logger.log('[DT10 v' + DT10_VERSION + '] \u26d4 upstream verdict: ' +
          dt10Uv.reason);
    } else {
      /* v1.10.1: DECLARE the feed state on EVERY render, not only when
       * blocked — decision.feed_banner_present has exit-2 authority and
       * the healthy state must not render silently. Both finder tokens
       * (FEED + ACTIONABLE), never the blocked phrase. */
      dt10SelTitle = '\u2705 FEED ACTIONABLE \u2014 EXECUTABLE' +
          (dt10Uv.ageMin != null ? ' (verdict age ' +
              Math.round(dt10Uv.ageMin) + 'm)' : '') +
          ' \u2014 ' + dt10SelTitle;
      Logger.log('[DT10 v' + DT10_VERSION +
          '] \u2705 upstream verdict: EXECUTABLE');
    }
  }
  row = dt10WriteSection_(sheet, row, dt10SelTitle, tokens);
  var selRows = [];
  for (var s = 0; s < selected.length; s++) {
    selRows.push(dt10TicketToRow_(selected[s]));
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
  var selT = dt10WriteTable_(sheet, row, DT10_SELECTED_HEADERS, selRows,
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
  var cT = dt10WriteTable_(sheet, row, DT10_CAND_HEADERS, cRows,
      'No candidates — pool was empty (see Status line and Alerts).');
  dt10ApplyColFormats_(sheet, cT.firstDataRow, cT.count, {
    6: DT10_FMT_PRICE, 7: DT10_FMT_PRICE, 8: DT10_FMT_PCT, 9: DT10_FMT_PCT,
    10: DT10_FMT_PCT, 11: DT10_FMT_NUM2, 12: '0.0', 13: '0.0', 20: '0.0'
  });
  dt10ColorVerdicts_(sheet, cT.firstDataRow, cT.count, 18, tokens);
  row = cT.next + 1;
  // Meta footer
  sheet.getRange(row, 1, 1, DT10_LAST_COL).merge()
      .setValue(dt10MetaLine_(payload.meta))
      .setFontColor(DT10_MUTED).setFontSize(8).setFontStyle('italic');
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
  report.push('pool scan (full universe): ' + pool.rows.length + ' of ' +
              pool.total + ' rows, per-sheet incl/avail ' +
              JSON.stringify(pool.perPage) + ' / ' +
              JSON.stringify(pool.available) +
              (pool.truncated ? ' (TRUNCATED)' : ' (full)'));
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
               ['FAR.US', '2026-09-01', 15],
               ['JUNK.X', '', ''],
               ['OLD.US', '2026-07-01', -3]];
  var eaMap = dt10EarningsMapFromValues_(eaFix);
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
  var v18sc = dt10SeatCheckNote_({ selected: [{ symbol: 'A' },
      { symbol: 'B', _grace_hold: true }], kpis: { selected_count: 3 } });
  var v18gr = dt10TicketToRow_({ _grace_hold: true, rank: 6, symbol: 'G',
                                 detail: {} });
  report.push('seat/grace core: ' +
              ((v18sc.indexOf('SEAT-CHECK kpi 3 funded vs board 1 exec') ===
                0 && v18gr[0] === '\u2014') ? 'ok' : 'FAIL'));
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
  var msg = report.join('\n');
  Logger.log(msg);
  try {
    SpreadsheetApp.getUi().alert(msg);
  } catch (e) {}
  return msg;
}
