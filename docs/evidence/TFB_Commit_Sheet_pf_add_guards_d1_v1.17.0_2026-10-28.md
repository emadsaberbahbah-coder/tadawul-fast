# Commit Sheet - Portfolio_Decision ADD guards: post-earnings window (D1b) and open-ticket cap (D1c) - 2026-10-28

**Scope:** weekly-review defect D1 (28 Oct), parts (b) and (c). Part (a), `TFB_PF_ADD_LOSER_VETO=enforce`, is live since the 07:54Z deploy. Both new guards are default off and byte-identical when off. No sizing rule, funding pass, confirmation clock, gate threshold or other page is touched.

**Files:**
| File | Version | Change |
|---|---|---|
| `core/analysis/portfolio_actions.py` | 1.16.1 -> 1.17.0 | two seams after P-183, four alerts, two meta blocks; WHY block above the constant |
| `routes/advanced_analysis.py` | 4.18.2 -> 4.19.0 | forwards `body.post_earnings` (a mapping) as `upstream_meta["post_earnings"]` |
| `apps_script/10_My_Portfolio.gs` | 1.5.16 -> 1.5.17 | `PD_POST_EARN=on` sends Calendar_Events report dates for the held symbols; meta-line read-back tokens |
| `main.py` | 8.14.7 -> 8.14.8 | `/health` pf_gates lists the five new env names |
| `scripts/verify_deployment.py` | pins | portfolio_actions 1.17.0, advanced_analysis 4.19.0, main 8.14.8 |
| `docs/render_env_contract.md` | regenerated | five new rows |
| `.github/workflows/ci.yml` | 1.1.53 -> 1.1.54 | new pytest file in the isolated protective step (+ base fetch by SHA), new node harness |
| `tests/test_pf_add_guards_d1.py` | new | 37 cases |
| `tests/test_pd_post_earn_inputs.js` | new | 62 checks (+3 with `PD_BASE`) |
| `tests/test_auth_wall_scope_path.py`, `tests/test_auth_wall_downstream_errors.py`, `tests/test_main_health_pf_gates_v8140.py`, `tests/test_pf_allcash_ok.py`, `tests/test_pd_evidence_publish.js`, `tests/js/gas_ledger_fx_failed_state.test.js` | pins | stacked version pins moved (main 8.14.8, route 4.19.0, PD 1.5.17, +5 health keys, +4 GAS functions, +259 lines); every other assertion kept |

## S1 - Evidence
- `Portfolio_Decision` 28 Oct 08:46: `ADD FRST.US 48 sh (~2,671 SAR, cash) - Upside 23.0 %` on a lot at -3.9 % (174 @ 15.4363), five sessions after the 22 Oct report (`Calendar_Events`: Last Earnings Date 2026-10-22, EPS Surprise -22.2 %).
- Interim trading policy (27 Oct): one open ticket; no entry within 5 sessions after earnings (10 after a negative surprise). The Top-10 tags its seats from the same calendar (16_Decision_Top10 v1.18.0, `post-earn 2/7` on 28 Oct); Portfolio_Decision never saw it (no earnings column among the 122 My_Portfolio columns; the backend reads no sheet).
- P-183 holds this ADD only because the lot is a loser; a profitable lot would pass, and nothing counts tickets.

## S2 - WHAT
**(b) post-earnings window - `_apply_add_post_earn`, per row after P-183**
- Inputs: `body.post_earnings` (schema `tfb.pd-post-earnings/v1`) from 10_My_Portfolio v1.5.17: `status` ok | unavailable | stale, `asof` (Riyadh date), `updated`, `reports`, and for held symbols only `{date, sessions, surprise}` from the Top-10's reader `dt10PostEarnMapFromValues_`, so both pages count the same sessions (Mon-Fri, Sun-Thu for KSA) inside the same 31-day window. A held name whose Last or Next Earnings Date is today enters at 0 sessions (that reader keeps strictly past reports only; review fix below). The route forwards it; `_build` pops it like the evidence packet, so `meta.upstream` never echoes it.
- `_post_earn_inputs` validates: wrong schema, malformed map (> 200 symbols), `asof` more than one day from the backend's Riyadh date -> `invalid`; malformed entries are dropped.
- Rule: an ADD whose holding reported within `TFB_PF_POST_EARN_SESSIONS` (5) sessions, or `TFB_PF_POST_EARN_SESSIONS_NEG` (10) after a negative EPS surprise, narrows to HOLD. Both lengths accept 0..14 only: Calendar_Events keeps a last report for 21 calendar days, and a report at most 14 sessions old is at most 20 days old (review fix 2). Under enforce, inputs that are absent, unavailable, stale or invalid also hold (the window cannot be verified; an ADD is never urgent). A holding without a report in the window passes.

**(c) open-ticket cap - `_apply_ticket_cap`, one pass after the per-row loop, before funding**
- Open tickets = held non-sukuk positions (quantity > 0) in this run. ADDs take the free slots (`TFB_PF_MAX_OPEN_TICKETS`, default 1, minus open tickets) in the funding pass's own order (annual ROI, reliability, symbol); every other ADD narrows to HOLD. With one ticket open and cap 1, no ADD passes.
- An ADD already held by (b) does not use a slot.

**Both:** a sukuk (D-9) is exempt and never counts as a ticket. Gates read at call time; any other value, blank included, is off.
- observe: actions, sizing and KPIs identical to off; one `[postearn-observe] ...` / `[ticketcap-observe] ...` tag per ADD row (action_reason and advisor_note); alerts `add_post_earn_observe` / `add_ticket_cap_observe`; `meta.post_earn_guard` / `meta.ticket_cap`.
- enforce: `ADD held [POST-EARN]: reported 2026-10-22, 4/10 sessions since the report (EPS -22.2%) - no adding inside the post-earnings window (was ADD: ...)` or `ADD held [TICKET-CAP]: 1 open ticket at the cap of 1 - no new ticket until one closes (was ADD: ...)`; proceeds 0, `capped_from=ADD`; alerts `add_post_earn_hold` / `add_ticket_cap_hold`, both excluded from `low_confidence_capped`.
- Read-back: the Portfolio_Decision status line gains ` | post-earn <mode> (<input status>; held N)` and ` | tickets <open>/<cap> <mode> (held N)` only when the response carries those meta blocks; `/health` pf_gates shows the five env values.

**Limits:** the EODHD calendar does not cover KSA names (a KSA holding passes (b)); a working entry order that has not filled is not counted as a ticket (the evidence packet carries positions only); the Top-10 board does not apply the ticket cap (follow-up).

## S3 - Tests
| Test | Proves |
|---|---|
| `test_mode_readers`, `test_number_readers` | off unless exactly observe/enforce; sessions 0..60 (else 5/10); cap 1..100 (else 1) |
| `test_inputs_validation`, `test_window_eval` | schema, statuses, asof window, entry cleaning; 5/10 session limits |
| `test_off_is_identical_with_or_without_inputs` | off: same payload with or without `post_earnings`; no new marker anywhere |
| `test_off_is_byte_identical_to_v1161_base` | off = the untouched v1.16.1 module at `22658e0f` (versions masked) |
| post-earnings observe / enforce / limits / env limits / unverifiable inputs / sukuk | tags only in observe; HOLD only for the name inside its window; KPIs and alerts consistent; absent, unavailable, stale, invalid inputs hold under enforce only |
| ticket cap at the cap / free slots / observe + sukuk / interplay / determinism | FRST-shaped one-ticket book holds every ADD; slots in funding order; sukuk not counted; a post-earnings hold frees the slot |
| `test_route_forwards_only_a_mapping` | the real route forwards a mapping only; absent, list or null -> no key |
| `tests/test_pd_post_earn_inputs.js` E1-E10 | GAS inputs equal the Top-10 reader on a realistic grid (FRST 4 sessions, EPS -22.2); unavailable/stale paths; Date vs text cells; bounded read; mode; meta line unchanged without blocks; gated wiring; ES5/ASCII; 4 functions added, 0 removed |

Golden negatives: `tests/test_pf_add_guards_d1.py` on `22658e0f`: 32 of 35 fail (the three that pass are the off-identity cases). `tests/test_pd_post_earn_inputs.js` on the v1.5.16 file fails at the first added var; with `PD_BASE=<v1.5.16>` E10 proves the base has none of the additions.

Local runs: the new files, every 10_My_Portfolio harness (7 node, 4 pytest), the version-pin tests, `render_env_contract.py --check`, and the whole suite (see the PR).

### Review fix (Codex, PR #869, head `502b7400`)
| Finding | Fix | Golden negative |
|---|---|---|
| P1: the reused Top-10 reader drops reports dated today (`last >= today`, `next >= today`), so a held name on its report day was absent from the inputs (status still `ok`) and its ADD passed under enforce. | `pdPostEarnCore_` adds a held name whose Last or Next Earnings Date is today at 0 sessions (surprise only from a Last Earnings Date of today, parsed as the Top-10 reader parses it); the Top-10 reader itself is unchanged. The backend names it `report day <date> (0/<limit> sessions, EPS ...)`. New E1b (reported today, reports today, tomorrow and not-held stay out, Date cells, 23:59 vs 00:00 Riyadh) and `test_report_day_is_inside_every_window`. | The harness fails at E1b on the `502b7400` file. |
| P2 (head `c12a754e`): the session readers accepted up to 60 sessions while the inputs hold reports for at most 31 days (Top-10 reader) - in practice 21 (calendar_sync keeps a Last Earnings Date for `TFB_CALENDAR_DAYS_BACK` = 21 days); a configured 30-session window passed a 25-session-old report as "no report". | `_POST_EARN_MAX_SESSIONS = 14`: both readers accept 0..14 (else the default 5/10). A report at most 14 sessions old is at most 20 calendar days old (Mon-Fri and Sun-Thu); 15 sessions can be 23 days. | `test_session_cap_is_the_longest_window_the_calendar_can_prove` and the 15/30 reader cases fail on `c12a754e`. |

## S4 - Owner steps (after merge)
1. Apps Script: replace `10_My_Portfolio.gs` with the merged repo copy -> Ctrl+S. Project Settings -> Script Properties -> add `PD_POST_EARN` = `on`. Run `pdSelfTest` once: the line `post-earnings inputs (PD_POST_EARN=on): ok; ...` should list `FRST.US 2026-10-22 4 sess EPS -22.2` (sessions grow by one per weekday). The live backend (4.18.2) ignores the new field, so this step is safe before the deploy.
2. Render -> Environment: add `TFB_PF_ADD_POST_EARN` = `enforce` and `TFB_PF_MAX_OPEN_TICKETS_MODE` = `enforce` (the cap `TFB_PF_MAX_OPEN_TICKETS` defaults to 1, the interim policy) -> Save, rebuild, and deploy (latest main), outside a sync. One deploy also carries #865/#867 if they are not live yet.
3. Read-back: `/health` pf_gates shows the five keys; on the next Portfolio_Decision render FRST's ADD is HOLD with `[POST-EARN]` (or `[P-183]`) in the note and the status line ends ` | post-earn enforce (ok; held ...) | tickets 1/1 enforce (held ...)`.

Order matters: arming step 2 before step 1 holds every ADD as `earnings calendar absent` (safe, but noisy).

## S5 - Rollback
Unset `TFB_PF_ADD_POST_EARN` and `TFB_PF_MAX_OPEN_TICKETS_MODE` (= v1.16.1 output). GAS: `PD_POST_EARN=off` (request = v1.5.16). Or revert the PR.

Not a licensed financial adviser; the owner decides and executes every trade.
