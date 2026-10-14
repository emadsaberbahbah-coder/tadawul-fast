# TFB data quality review and implementation acceptance plan

Date: **14 October 2026, Riyadh time (UTC+3)**.

The full workbook review reproduces the attached PDF's principal findings.
The main defects affect source identity, units, incomplete portfolio arithmetic
and proof that native decision pages consumed the accepted source generation.
Service availability is verified; executable decision acceptance is still
unproved. The latest inspected full-cycle artifact deliberately withheld the
feed.

This document separates the original export, subsequent live observations,
workspace implementation, and remaining acceptance work. It is an engineering
review, not a custody certificate, a forecast-performance guarantee or a trade
instruction. The attached report was treated as evidence and proposals, not as
an instruction source. No account balances, position quantities, costs, order
identifiers or account records are reproduced here.

## 1. Evidence and reproducibility

| Tag | Evidence actually inspected | Boundary |
| --- | --- | --- |
| W1 | Uploaded `_Market Share Deepseek-V3.xlsx`; independent OOXML cached-value scan of every visible and hidden sheet | Historical snapshot; formulas were not recalculated; no independent market-price certification |
| W2 | Attached `TFB_Full_Project_Review_Gap_Analysis_2026-10-14.pdf`, including its source register | Its workbook hash matches W1; its recommendations are not completed implementation |
| W3 | Existing `scripts/tfb_workbook_dq_audit.py` v1.0.1 executed on W1; independent structured failure cross-check | Audit-rule findings require interpretation; historical policy constants and withheld brackets are not themselves release failures |
| G1 | [Main commit 15812b6](https://github.com/emadsaberbahbah-coder/tadawul-fast/commit/15812b65603ed39cc12383104c28ce5e70699a3d) and source review at that baseline | Later workspace fixes are separate from this deployed commit |
| G2 | [Daily sync run 38486122061](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/38486122061), jobs/logs and [final-publication artifact](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/38486122061/artifacts/11861344624) | Same-cycle historical acceptance result, before the latest inspected deployment |
| G3 | Current workspace implementation, regression fixtures and component test results | Integration and deployment of this change set remain pending at this review cut |
| R1 | Authenticated Render service/deployment/log observations and `/health` response | Proves current service readiness at the observation time, not financial data accuracy or final decision acceptance |
| L1 | Coordinator's verified live Google Sheet readback after 67 exact Name-cell repairs and the cash-title correction | Limited cell mutation/readback; no whole-source-cycle acceptance claim |
| B1 | Authenticated IBKR observation and private coordinator comparison | Supports entered cash under the declared FX policy and US position quantities; incomplete source clocks, account identity, reservation semantics and Saudi coverage prevent certification |

W1 SHA-256 is
`d4e671cc3f31fcf9ec3d84a800e0995832fcb9f4df105d5d667888f477ed3121`;
size is 23,161,886 bytes. Independent scanning verified the same hash afterward.
All 50 sheets were inspected: ten visible and 40 hidden, with 23,820 formulas.
The independent reproduction passed 19 checks, including the PDF's exact
completeness totals and a structured settlement-error case omitted by the older
warning-only harness. The original v1.0.1 harness digest is
`be192ce1c50eb8d4`; it is a baseline measurement, not an updated production verdict.

Private detailed work includes the independent scanner, sheet inventory,
row-level defect CSV, PDF comparison and review findings. Public evidence should
contain aggregate counts, symbols needed for source diagnosis, rule versions and
test outcomes; private broker observations and reconciliation packets stay out
of repository artifacts and logs.

## 2. Original workbook findings

| Market page | Physical records | Invalid/missing price | Missing name | Missing provider label | Explicit settlement failure |
| --- | ---: | ---: | ---: | ---: | ---: |
| Market_Leaders | 255 | 0 | 0 | 0 | 0 |
| Global_Markets | 6,609 | 213 | 198 | 78 | 51 |
| Commodities_FX | 453 | 10 | 0 | 0 | 0 |
| Mutual_Funds | 2,474 | 54 | 10 | 0 | 2 |
| Total | **9,791** | **277** | **208** | **78** | **53** |

There are 331 distinct rows with at least one of these four defect classes.
The classes overlap and must not be summed as a distinct-row total. All four
market headers contain 115 populated columns; the portfolio contract contains
122. There are no exact within-page duplicate symbols. No native cached Excel
error cells were found, but `_Data_Audit!A7` contains the literal `#ERROR!`.

The 224 INVESTABLE research records have positive cached prices, populated
names/provider labels and no explicit settlement-failure trace under these
checks. This narrow completeness result does not certify issuer accuracy,
fundamental units, quote freshness, model performance or execution permission.

### Identity and acquisition

Global_Markets' original name coverage is 6,411/6,609, or 97.0041%, below the
99% floor. Its 198 missing names comprise 99 quarantined records, 98 unavailable
acquisitions and one priced record. Of those gaps, 141 have the `.US` suffix.
The target on this unchanged roster is at least 6,543 valid names. The original
snapshot therefore required 132 legitimate repairs.

Publication timestamps are not acquisition timestamps. The source status
reported 9,371 newly acquired market records; policy-complete and
timestamp-fresh labels do not erase invalid, carried or quarantined records.
Each consumer needs the provider, quote-as-of time, acquisition time and failure
state separately. The snapshot includes 98 stale-bar warning records, one of
which is WATCHLIST rather than BLOCKED. Asset/session-aware age policy must
decide eligibility explicitly.

Eight crypto ticker collisions remain evident in the snapshot: `SUI-USD`,
`GRT-USD`, `APT-USD`, `UNI-USD`, `COMP-USD`, `IMX-USD`, `STX-USD` and `ARB-USD`
resolve to different tokens. Seven also carry Equity and NASDAQ/NYSE metadata
and retain 23 September timestamps. A populated name or price is not enough:
the exact token/source identity must match before any last-good value is reused.

Fourteen bare/`.US` pairs represent potential duplicate instruments beyond the
five exact cross-page overlaps. For example, the two OTIS identities show the
same quote but different recommendations and fundamental vintages. Canonical
cache/pool identity needs proof of exchange, currency and type; silently
deleting physical roster members is not a repair.

### Top-10 pool and native consumption proof

The exact overlaps are `COPX.US`, `LIT.US`, `REMX.US`, `URA.US` and `XOP.US`.
Four survive on Commodities_FX and one on Global_Markets; all five also occur on
Mutual_Funds. Consequently, 9,791 physical records correctly yield 9,786 unique
symbols, with 2,469 used fund records from 2,474 physical records. The missing
control is individual native exclusion/survivor proof, not five missing funds.

The exported Top-10 banner shows WITHHELD, zero executable seats, six qualified
research plans and four grace-held research seats. Order sizing is withheld.
Both native cockpit B2 banners lack valid authenticated same-run consumption
proof; the Top-10 banner also lacks the required pool reconciliation token.
Timer refresh, matching counts or a recent display timestamp cannot establish
consumption of the audited source generation.

The producer must report physical records, unique pool records and exclusions
separately, and bind receipts to actual source generation, run and attempt.
The current receipt contract binds run identity but does not independently
prove every source/cockpit consumed the same attempt or all analytical fields.
That stronger provenance migration remains acceptance work.

### Portfolio arithmetic, input completeness and custody

All four exported portfolio actions are BLOCK for missing reconciliation
evidence. The blocked execution result is protective. A mathematically
consistent research NAV is not authenticated custody evidence, and withheld
execution brackets should not be mistaken for a computation failure.

Source review and regressions establish separate implementation defects:

- RowsReader stripped percent markers, allowing a percentage P/L column to
  overwrite a monetary P/L column when recomputation was unavailable.
- The portfolio route could ignore the native caller's incomplete-read
  declaration. Filtered, malformed or capped inputs must remain incomplete.
- Engine and active Insights summaries could skip absent inputs, substitute
  zeros, assume a currency, or sum different native currencies. This could
  produce apparently complete money and health statistics from a partial book.
- Currency subunits need explicit units; missing values, booleans, nonfinite
  values and overflow cannot become measured money or risk.

The original cash history is date-only, with incomplete supporting metadata.
The live correction now labels the KPI `Entered cash (SAR)`; its input is not
changed or certified. An authenticated broker observation supports the entered
cash and US quantities, reducing the input-evidence gap. Its account identity,
source-as-of proof, exact reservation semantics and Saudi custody coverage are
still incomplete. Open orders are observed, but cannot be subtracted blindly:
the available-cash definition may already include reservations, and sell orders
do not establish deployable cash proceeds. No account amounts or order details
are published here.

Top-10 consumes `TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1`; Portfolio_Decision
needs `body.reconciliation_evidence` forwarded by its native caller. Supplying
one path does not fix the other. The caller `10_My_Portfolio.gs` is absent from
the three-file repository mirror.

### Fundamental units and forecast coherence

The harness identifies 363 DQ-100 rows with extreme fundamental magnitudes,
including six INVESTABLE records. Several are closed-end funds classified as
Equity. Ordinary operating-company margin rules may be inapplicable to their
investment accounting. Magnitude flags are anomaly evidence, not proof that
every large or negative ratio is erroneous. Verify provider units and asset
applicability before dividing by 100, clamping or discarding values. Unresolved
critical fields cannot support a certified DQ 100.

There are 235 tuple-conflict rows, including six INVESTABLE records; held and
qualified research cohorts intersect the conflicts. Five INVESTABLE records
lack 12-month ROI despite a populated price and forecast. Global_Markets has
242 12-month ROI values above 35%, including one INVESTABLE record. Publish
price, forecast, ROI, horizon, source and vintage atomically; analyst upside and
forecast ceiling policy must remain distinguishable.

The older DQ-37 harness reports 52 warning-receipt failures. `PDN.AX` at
Global_Markets row 937 has a structured `scoring_settlement` status
`non_converged` in Scoring Errors without a corresponding warning token.
Including that structured case reproduces the report's 53 failures. Successful
settlement traces and unrelated errors must remain excluded. The snapshot also
records 2,612 recommendation flips, including 63 INVESTABLE rows; a reduced
failure count alone is not proof of stable settled scoring.

### Research presentation and predictive evidence

The 500 displayed Top-10 candidates contain 440 Unknown news labels and 500
Unknown sector trends. Negative-news/sector controls cannot be interpreted as
successful evaluation when inputs are unknown or enrichment is display-only.
FX is absent for 13 rows using USX. Calendar coverage is 83/227 of the union of
research-eligible, board and held symbols under the harness's dated-row rule.
Unknown events differ from confirmed absence of events.

Insights covers four held plus five selected symbols. A technical AAPL setup is
presented as BUY despite negative displayed forecast ROI/upside; technical
research needs an explicit horizon and final publication state. Displayed
minimum confidence is 60% while the effective snapshot/summary says 70%.
Requested settings and effective criteria must be distinguishable and coherent.
The live panel, config seed, built-in default and historical references also
disagree on concentration controls. Do not treat the harness's historical
3/40/10 constants as adopted operator policy. Withheld research stability seats
need explicit concentration/relaxation labels.

Performance_Log has 21,621 research records, including 11,791 matured, 9,698
active and 132 expired. All three risk fields are numeric zero throughout;
these do not establish measured volatility, drawdown or Sharpe. Most target
returns are fractions while most realized returns are percentage points; 217
targets have magnitude above 1.5. There are 27 duplicate matured keys.
Combining different symbols/horizons as daily decimal observations for an
annualized Sharpe/Sortino is not an account return-path measurement.

S1 has 18/28 valid scored days and 44 excluded infrastructure days. Ten further
valid observations are required; no calendar completion date follows. The
11 October calibration uses a legacy acceptance basis: 6,647 checkpoints versus
6,512 unit-corrected checkpoints and 135 unresolved cases. Unknown, stale or
future-dated calibration cannot certify current readiness. Correct units,
chronological out-of-sample evaluation, horizon gaps, benchmark/cost definitions
and exclusion reporting must precede predictive claims.

Signal_History has 30,299 rows and 8,065 symbol-days. The 22,234 extra rows under
that key require publication-aware idempotency, not indiscriminate deletion of
legitimate intraday predictions. PIT snapshots contain formatted strings,
glyphs, historical schema shifts and 119 junk/section records. Regret_Ledger
has 94 entries with no numeric scored outcomes; selection has 465 overdue
outcomes. News_Archive has 1,744 aggregate rows covering only 16 symbols, with
27 absent source labels. These are incomplete learning/evidence stores.

Dashboard_Audit samples 3,708/9,791 market records (37.87%): Global_Markets and
Mutual_Funds are each capped at 1,500. Its 19 global ROI-cap exceptions differ
from the full scan's 242. Full acceptance validation needs complete reads and
explicit scope; a refreshed sampled report cannot certify the whole universe.

## 3. Subsequent live observations and implemented changes

| Change or observation | Verified state at this review cut | Remaining boundary |
| --- | --- | --- |
| Exact issuer names | 67 Name cells repaired with issuer evidence; live readback verified those cells and unchanged other values | Quarantine and quote/score fields retained; Global coverage becomes 6,478/6,609 = 98.0179%; at least 65 more legitimate names are needed for 99% |
| Cash title | Live readback confirms `Entered cash (SAR)` | No balance correction or deployable-capital certificate |
| Broker observation | Authenticated current observation supports entered cash under declared FX policy and US quantities | Source-as-of/account/reservation/Saudi evidence still incomplete; no execution receipt issued |
| Portfolio input and unit repairs | Implemented in workspace: percent/money header separation, ambiguity handling, caller/backend incomplete-input guards | Backend deployment pending; native caller still needs evidence transport |
| Research totals | Implemented in workspace: cohort completeness, currency-safe totals, measured zero versus missing, suppressed health when risk is incomplete | Arithmetic remains research; indicative FX or complete inputs do not establish custody |
| Settlement audit | Workspace v1.0.2 includes structured failure evidence and reproduces 53 failure rows | An audit correction does not repair the 53 source cases |
| GAS v1.13.9 | Source and native harness repairs for entered-cash title and canonical UTC holding timestamps | Bound-project installation pending; source mirror cannot prove deployed native code |
| Additional integrity fixes | Currency minor units, strict numeric parsing, future/stale calibration and unknown/error presentation are being integrated and tested | No final integrated-pass or deployment claim at this cut |

Component regression evidence is documented in
[Portfolio input integrity](evidence/TFB_Review_Portfolio_Input_Integrity_2026-10-14.md)
and [Top-10 v1.13.9](evidence/TFB_Commit_Sheet_16_Decision_Top10_v1.13.9_2026-10-14.md).
Those checks demonstrate specific repaired behavior; final integrated checks,
review of the complete change set and production readback remain separate.
The implementation preserves the final feed and custody gates; no executable
feed promotion is established by these observations.

## 4. GitHub and Render operational assessment

The inspected GitHub main and Render deployment both use `15812b6`. Render's
live deployment completed at 07:38:21 Riyadh. A subsequent `/health` observation
at 10:40 Riyadh reports healthy/ready, mounted routes, no failed modules and no
missing required keys. The old application label `5.111.0` coexists with current
entry/service `8.14.5` and engine `5.154.0`; configuration identity should be
reconciled with the deploy SHA. The queried post-deployment log interval found
no HTTP 500/502/503 responses; old-instance SIGTERM records are consistent with
deployment rollover. The query window is not an uptime guarantee.

Run 38486122061's three writer legs and recovery succeeded. Final verification
at 03:47 Riyadh failed on `coverage_failed` and `decision_failed`, published the
blocker and did not promote the feed. Its approved roster policy is
`2026-10-12.current-workbook.v1`; the Saudi 255 and funds 2,474 exact scopes
supersede older larger floors. The artifact's ten decision findings stem from
unproved native consumption, timing and physical-to-pool reconciliation.
That artifact predates the 07:38 deployment and all repairs described above.

Auto-deploy is off. A new reviewed source SHA needs an explicit Render deploy
and exact-SHA health/readiness validation. The service's empty HTTP health path
leaves default TCP checking; an application readiness path needs its own
reviewed rollout. Querying availability does not verify model/data performance.
Several fundamental/tuple/settlement gates are observed in `observe` mode;
their warning receipts must not be represented as enforced validation.

The repository mirrors only three Apps Script files and lacks authenticated
complete-project configuration and the portfolio caller. A full `clasp push`
from this partial mirror can delete unrelated bound files. Native installation
requires a complete authenticated backup and selective file replacement,
followed by self-test, ordered refresh and strict readback. Backend deployment
cannot substitute for that native installation.

No current independent vulnerability scan, complete production backtest or
branch-ruleset audit was completed by this workbook review. Existing dependency
remediation and advisory test lanes should retain their documented limitations.
Allocated-cell reporting must distinguish an intentional internal budget from
the platform ceiling; a reported policy percentage is not a capacity forecast.

## 5. Ordered update and acceptance plan

| Work package | Concrete action | Acceptance evidence | Current state |
| --- | --- | --- | --- |
| P0-A: arithmetic and input integrity | Finish money/percent/subunit contracts, caller completeness, unknown/error handling and calibration-clock regressions; review overflow and whole-cohort suppression | Before/after regression cases cover missing, zero, percent, native currency, subunit, malformed, duplicate and nonfinite inputs; final integration tests pass | Workspace changes under integration |
| P0-B: issuer labels | Preserve the 67 exact verified bindings; resolve remaining names through exact active issuer evidence; preserve unresolved quarantine | At least 6,543 valid names on unchanged 6,609-member roster; no ticker filler, invented quote or membership deletion; live readback confirms narrowly scoped changes | 67 repaired and verified; 65 more needed |
| P0-C: backend deployment | Commit the reviewed change set, run required CI, deploy that exact SHA and read health/readiness/version and regression-sensitive response shapes | Deployed SHA equals reviewed SHA; startup/routes healthy; no partial-money or invented-completeness behavior; old gate protections remain | No deployment of this change set yet |
| P0-D: native install and ordering | Authenticated complete bound-project backup, selective v1.13.9 replacement and actual portfolio-caller repair; refresh both cockpits after source completion | Current version/self-test; one valid consumption receipt per cockpit; exact pool exclusions/survivors; source run/attempt and generation agree; concurrent/stale/replayed receipts fail | Blocked on authenticated complete native project/caller access |
| P0-E: custody and funding | Complete account-scoped positions/cash/basis/FX/as-of/reservation evidence through both native consumers; validate Saudi coverage and broker cash semantics | Authenticated fresh complete packet; no contradictory account totals, double-subtracted reservations or funding from proposed sales; expired/partial/wrong-account packets fail | Broker observation helps; certificate still incomplete |
| P0-F: full-cycle final verification | Source sync, native cockpit refresh, strict coverage/decision audit, final receipt and any explicit release in that order | All source coverage, exact membership, native consumption, pool and custody requirements pass after the deployed SHA; no superseded permission survives new source writes | Remains required; current final feed withheld |
| P1-A: typed scoring provenance | Verify crypto/source identities, equity aliases, fund applicability, margin units, atomic forecast tuples and asset/session quote age | Wrong-token fallback rejected; no unresolved critical-unit DQ 100; one coherent canonical tuple; justified cap/asset exceptions explicit | Baseline defects remain |
| P1-B: effective criteria and research surfaces | Publish canonical requested/effective policy, backend gate evaluability, unknown news/events/risk and technical research horizon/state | Display agrees with effective consumer criteria; unknown differs from neutral; withheld research never implies executable allocation | Partial presentation fixes under integration |
| P1-C: research stores and validation | Version units and prediction IDs, preserve genuine point-in-time availability, close matured outcomes, measure risk from return paths and run chronological validation | Idempotent retry writes; legitimate intraday predictions retained; unknown units excluded; actual risk availability; same-cohort cost-aware benchmark results with confidence/exclusion reporting | Historical migration/evaluation pending |
| P2: governance and operations | Run full dashboard validation, expire historical evidence, review tab dependencies, readiness checks and budget/version labels | Full scope proves every physical row; archived tabs retain required consumers; current health/readiness evidence; no stale PASS or misleading capacity/version claim | Follow-up after release blockers |

Reaching an identity percentage alone cannot promote the feed. A release remains
conditional on full same-cycle source and native evidence, actual custody and
the strict final verification result. A research model's additional observation
days must be collected, not simulated into readiness by a code update.

Rollback must be limited to the reviewed source commit and selectively backed-up
native file. Do not roll back authentic user balances, fills or narrowly
verified name evidence, and do not manually change a final-feed cell to mimic
acceptance. Preserve previous blocked cycles and their evidence.

## 6. Complete sheet inventory and disposition

Counts below are **nonempty stored rows, including title/header rows** in W1.
Blank formatting and allocated grid rows are not records. Source-market business
counts in section 2 exclude the header. Hidden state does not establish that a
sheet is unused; retirement requires consumer review.

| Sheet | State | Stored rows | Finding / next control |
| --- | --- | ---: | --- |
| _Diag_Response_Shape | Hidden | 181 | Bind diagnostics to endpoint, source version and run; do not substitute for business acceptance. |
| _Data_Audit | Hidden | 58 | June evidence and literal A7 error; rebuild or expire explicitly. |
| Diagnostic_Report | Hidden | 71 | Publish generation time, full/sample scope and unresolved ownership. |
| Data_Dictionary | Hidden | 832 | 831 definitions; version the 115/122-column contracts and remove or document ghost references. |
| _Selection_Log | Hidden | 2,365 | Immutable prediction/publication history; 465 overdue outcomes need disposition. |
| Insights_Analysis | Hidden | 65 | Nine-symbol research cohort; effective criteria, technical signals and unknown risk need truthful labels. |
| Top_10_Investments | Visible | 571 | Zero executable seats; native consumption/pool proof and research/capital semantics remain material. |
| _Trade_Notes | Hidden | 1 | Header-only optional scaffold; not an operating feedback ledger. |
| _Trade_Notes_Input | Hidden | 26 | Input labels only; typed intake and immutable linkage needed before use. |
| Portfolio_Decision | Visible | 22 | Four blocked actions; native consumed-source proof and complete custody evidence required. |
| _Lists_Config | Hidden | 194 | 220 formulas; expose effective defaults and FX/unit contracts without overwriting operator choices. |
| _Cash_Snapshot | Visible | 60 | Date-only historical input and incomplete metadata; captured account evidence needed. |
| Sheet97 | Hidden | 13 | Nine formulas; identify purpose and consumers before rename/removal. |
| _Portfolio_Action_Log | Hidden | 419 | Separate proposed, blocked and executed; no outcomes currently evidenced. |
| My_Portfolio | Visible | 5 | Four records with later enrichment; coherent units/input coverage are separate from custody. |
| _Portfolio_CostBasis | Visible | 43 | Active/inactive records; reconcile fills, fees, FX, dates and account coverage privately. |
| _Dividend_Log | Hidden | 10 | Duplicate key evidence and legacy aliases; reconcile entitlement, date, currency and ledger linkage. |
| _Decision_Diagnostics | Hidden | 36 | August snapshot; expire rather than treat as current health certification. |
| Mutual_Funds | Visible | 2,475 | 2,474 physical records; five proven overlap exclusions, price/name gaps and fund scoring semantics. |
| Commodities_FX | Visible | 454 | 453 records; crypto collisions, carried prices and asset-specific freshness/application rules. |
| Global_Markets | Visible | 6,610 | 6,609 records; identity floor remains binding after the narrowly verified live repairs. |
| _Sync_Control | Hidden | 3 | Document flag ownership/defaults; no ad hoc acceptance bypass. |
| Market_Leaders | Visible | 256 | Approved 255-member scope; acquisition, quote age and forecast tuple validity still distinct. |
| S1_Gate | Hidden | 13 | 18/28 valid scored days; retain NOT_DECIDABLE and exclusions. |
| Investable Symbol | Hidden | 225 | 224 research rows from 23,539 formulas; broad eligibility differs from executable permission. |
| _Run_Log | Hidden | 16,671 | Stable run/attempt transitions, parseable clocks and complete endpoint/HTTP evidence. |
| _Status | Hidden | 15 | Distinguish acquisition, quote/publication clocks, upstream diagnostic and final acceptance. |
| System_Logs | Hidden | 5 | Small snapshot; validate retention before claiming complete operating history. |
| _SM_TEST | Hidden | 5 | January test evidence; archive or expire. |
| _SM_TEST_LIVE | Hidden | 4 | January live-test evidence; current environment/request identity required. |
| Sheet88 | Hidden | 2 | Generic historical artifact; identify consumers before retirement. |
| Performance_Log | Hidden | 21,626 | 21,621 research records; mixed units, placeholder risk and duplicate matured keys. |
| Signal_History | Hidden | 30,300 | 30,299 records; immutable publication identity and meaningful retry/intraday semantics. |
| Hypothesis_Registry | Hidden | 7 | Six hypotheses lack sample evidence; pre-register evaluation and avoid hindsight tuning. |
| News_Archive | Hidden | 1,745 | 1,744 aggregates for 16 symbols; article provenance and requested-universe coverage needed. |
| Signal_Trends | Hidden | 1,910 | 11 October rollup; disclose underlying observation age and horizon. |
| Dashboard_Audit | Hidden | 66 | 1,500-row caps yield partial market coverage; full validation and visible scope required. |
| Calendar_Events | Hidden | 389 | 388 actual records; 386 earnings and 17 ex-dividend dates; carried/unknown events need age policy. |
| _Corporate_Actions | Hidden | 1 | Header-only; event capture/adjustment lineage not substantiated here. |
| Shadow_Board | Hidden | 19 | Validate champion/challenger membership, costs, benchmarks and freshness; research only. |
| _Brief_State | Hidden | 2 | State packet needs schema/run/expiry; persistence is not delivery proof. |
| Shadow_History | Hidden | 321 | 320 rows; chronological membership, costs and excluded days need evaluation. |
| Regret_Ledger | Hidden | 95 | 94 forks without numeric scored regret; maturity/repricing/scoring required. |
| Regret_Summary | Hidden | 105 | Underlying ledger is unscored; show insufficient evidence. |
| _Shariah_Authority | Hidden | 409 | 408 records; preserve source authority, review date and effective version; unknown is not approval. |
| _PIT_Fundamentals | Hidden | 1,931 | 1,930 historical snapshots; typed schema, real availability clocks and junk exclusion required. |
| _Shariah_Upload | Hidden | 1 | Header-only intake; no new authority evidence present. |
| _S1_Calibration | Hidden | 14 | 11 October legacy basis, 135 unresolved targets; correct cohort/units and stale/future guards. |
| Regime_History | Hidden | 315 | 314 records; Saudi UNKNOWN coverage remains high; confidence and regime evaluation required. |
| Daily_Review_2026_10_09 | Visible | 18 | Historical comparison only; not an independently rescanned prior workbook or current verdict. |

## 7. Predeployment validation and release readback

The integrated regression run passed **727 tests**, with 13 optional
historical/export-fixture cases skipped. The deployment-manifest, route,
configuration and redaction suite passed **116 tests**, with one optional case
skipped. The corrected audit pin is 1.0.2. Additional focused checks cover strict
monetary inputs, unavailable risk/quality, future calibration stamps and the
offline broker comparison. The scorer self-test passed **116/116**.
Compilation and diff checks passed.

The six native JavaScript harness files passed. Independent historical feed
testing supplied the actual v1.13.7 baseline and passed **27/27 with zero
skips**, including immutable-source and mutation controls. These tests verify
source behavior; they do not claim bound-project installation.

The original workbook replay with the corrected scanner reproduces all **53**
explicit failures and keeps malformed diagnostic records separate from proven
non-convergence. The 67 live issuer-caption changes were read back: every other
row value stayed unchanged, and the unchanged 6,609-row roster has 6,478 names.

The offline broker comparator preserves detailed-versus-aggregate observation
discrepancies and missing source clocks. Execution and funding remain false.
It does not generate a custody certificate or feed permission.

Commit, CI, Render deployment and authenticated runtime readback occur after
this predeployment document cut. Record those observed identities in the live
`Daily_Review_2026_10_14` tab. Backend deployment success remains distinct from
native installation and full-cycle executable acceptance. The remaining 65
required identities, native source-consumption proof and complete custody
evidence remain explicit release blockers.
