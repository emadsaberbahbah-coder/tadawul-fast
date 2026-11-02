# Commit Sheet - `core/providers/calendar_provider.py` v1.3.2 [VENUE BATCHES + EODHD EXCHANGE CODES] + `calendar_sync.yml` + `ci.yml` 1.1.46 - 2026-10-27

**Scope:** program register **A4** (post-earnings quarantine data lane), the follow-up found on the first armed run of v1.3.1. US codes can no longer be starved by foreign codes EODHD rejects, and the calendar is asked with EODHD's own codes for four venues it rejected. Two gates, `TFB_CALENDAR_VENUE_BATCHES` and `TFB_CALENDAR_SUFFIX_MAP`: default off in the module (the v1.3.1 requests, results and log lines byte for byte), armed in the calendar workflow.
**Files:** `core/providers/calendar_provider.py` (1.3.1 -> 1.3.2; six helpers added, zero functions removed), `tests/test_calendar_venue_batches.py` (new, 37 cases), `tests/test_redaction_boundaries.py` (the calendar harness extracts the new helpers and reads the two gates; new leg: the exchange-code path never logs the key), `tests/test_calendar_split_404.py` + `tests/test_calendar_last_report_a4.py` (version pins moved), `scripts/verify_deployment.py` (manifest pin 1.3.1 -> 1.3.2), `.github/workflows/calendar_sync.yml` (arms both gates), `.github/workflows/ci.yml` 1.1.46, this sheet.

## S1 - WHY (measured 27 Oct)
- calendar_sync run `40498374271` (14:44 Riyadh, first run with v1.3.1's split armed): `Calendar_Events` last-report rows went from **3 to 34 of 327** (`last_report: with=34 fresh=31 carried=3 window=21d`; FRST.US 2026-10-22, -22.2 %; OPBK.US 2026-10-22, -1.9 %).
- The same step log: `[calendar_provider v1.3.1] last-earnings: 404 batches split 99 time(s), 200 request(s); EODHD rejected 33 code(s): 1015.KL, 1605.T, ... SON.LS.US; 42 code(s) dropped at the request cap 200`.
- Cause: the codes are batched in sorted order, so every batch mixed venues. The rejected codes - 13 `.T` (Tokyo), 12 `.L` (London), 2 `.KL` (Bursa Malaysia), `IG.MI`, and `AEFES.IS.US`, `KCHOL.IS.US`, `EDP.LS.US`, `NOS.LS.US`, `SON.LS.US` (the shared normalizer does not list `.IS` / `.LS`, so it appends the default `.US`) - spent the whole budget on splits, and the 42 codes left were never requested: **27 US names from TAK.US to ZTO.US** (TSM, TDY, TGS, UMBF, UNTY, VICI, WPC, WTFC ...).
- An offline replay of the real 327-symbol universe through the v1.3.1 module reproduces the live line exactly (99 / 200 / 33 named / 42 dropped) with `T82U.SI` (Singapore) as the one further unknown code; `TUPRS.IS.US` sits in the dropped tail.

## S2 - WHAT
- `TFB_CALENDAR_VENUE_BATCHES` (on/1/true/yes): `_earnings_batches` builds the `/calendar/earnings` batches per venue - the code's exchange suffix; a code with more than one dot (`X.IS.US`, `BRK.B.US`) is a venue of its own - **US first**, then the other venues in code order, multi-dot venues last; each venue sliced by `TFB_CALENDAR_EARNINGS_CHUNK`. A 404 split can no longer touch a US batch, and the request cap can only fall on foreign codes. Off: the v1.3.1 slices.
- `TFB_CALENDAR_SUFFIX_MAP` (on/1/true/yes): `_calendar_code` asks the earnings calendar with EODHD's exchange codes for the venues it rejected: `.L -> .LSE` (EODHD exchanges API: London Exchange, code LSE), `.KL -> .KLSE` (EODHD exchange page KLSE), `X.IS.US -> X.IS`, `X.LS.US -> X.LS` (EODHD exchange page LS). Answers map back to the ORIGINAL symbol; an answer under the untranslated code lands too. `.T`, `.SI` and `IG.MI` are left as they are: their EODHD codes are not established, so they keep being named in the WARNING line.
- The WARNING line (unchanged otherwise) gains `; sent as EODHD exchange codes: IS.US->IS x3, KL->KLSE x2, L->LSE x12, LS.US->LS x3` when the map translated anything, so the next run shows both what was translated and what EODHD still rejects. With the map off the line is v1.3.1's.
- Both batched earnings paths (`fetch_earnings_map`, `fetch_last_earnings_map`). The shared `to_eodhd_symbol` (prices, fundamentals), the per-symbol ex-dividend path, the Yahoo fallbacks, KSA handling and the request cap are unchanged.
- Expected on the 27 Oct universe (replay): **59 requests, 15 codes rejected (13 `.T`, `T82U.SI`, `IG.MI`), nothing dropped**; if EODHD does not know the translated codes either: 91 requests, 35 named, still nothing dropped.

## S3 - Gate / arming
| Env | Where | Value | Effect |
|---|---|---|---|
| `TFB_CALENDAR_VENUE_BATCHES` | module default | off | v1.3.1 batches |
| | `calendar_sync.yml` step env | `"on"` | one venue per batch, US first |
| `TFB_CALENDAR_SUFFIX_MAP` | module default | off | v1.3.1 codes |
| | `calendar_sync.yml` step env | `"on"` | EODHD codes for `.L` / `.KL` / `.IS` / `.LS` |

No owner arming step: merge is enough (Render never calls this module). Kill: set either workflow value to `"off"`, independently.

## S4 - Tests
- `tests/test_calendar_venue_batches.py` (37 cases), on the real 327-symbol universe of 27 Oct: **V1** both gates off (absent / off / 0 / blank / junk): requests, results and log lines equal the untouched v1.3.1 (`git show e8337a16`) on both paths, and the replay reproduces the live WARNING line exactly - 42 dropped, 27 US, TSM / TDY / WTFC / VICI missing (golden negative); **V2** venue batches: every request one venue, the first five the 207 US codes (50/50/50/50/7), nothing dropped, 91 requests, 35 named; **V3** the translation table (17 cases incl. share classes, already-EODHD codes, empty bases), `_earnings_codes` off = the same dict object, on = the summary `IS.US->IS x3, KL->KLSE x2, L->LSE x12, LS.US->LS x3`, London / Malaysia / Lisbon reports land on `AZN.L` / `1015.KL` / `EDP.LS`, an answer under the untranslated spelling lands too; **V4** both on: 59 requests, the exact WARNING line, ten reports incl. the late-alphabet US names; pessimistic EODHD: 91 requests, nothing dropped, the translated codes named; **V5** forward path US first, KSA never sent, earliest upcoming kept; ex-dividend requests identical with every gate on or off; **V6** gate spellings, pure helpers, the workflow arms both gates.
- `tests/test_redaction_boundaries.py`: new leg - armed, the request carries `SYNTHETIC.LSE` and the WARNING line `EODHD rejected 1 code(s): SYNTHETIC.LSE ... L->LSE x1`, never the URL or the key.
- Calendar suites + redaction + manifest pins: 298 passed, 1 skipped. Contract lane list (ci.yml 1.1.46): 427 passed, 10 skipped. Workflow regressions + manifest pins: 338 passed; `scripts/audit_repository_workflows.py` errors 0, warnings 0.

## S5 - Rollback
`TFB_CALENDAR_VENUE_BATCHES: "off"` and / or `TFB_CALENDAR_SUFFIX_MAP: "off"` in the workflow (v1.3.1 behaviour on the next run), or revert.

## S6 - Known limits / next
- `.T` (13 Tokyo codes), `.SI` and `IG.MI` stay rejected until their EODHD codes are established from evidence; they cost about 2n requests per run inside their own venue batches and are named every run.
- The shared normalizer still builds `X.IS.US` / `X.LS.US` and passes `.L` / `.KL` for prices and fundamentals; that lane is out of scope here.
- No Yahoo fallback for past reports (v1.3.0 limit, unchanged).

## S7 - Operator steps
1. Merge. Nothing to arm.
2. Read the next `📅 Calendar Sync` run: the step log's last-earnings line should read about `59 request(s); EODHD rejected 15 code(s): IG.MI, T82U.SI, 1605.T ...; sent as EODHD exchange codes: ...` with no `dropped`, and `last_report: with=` should rise above 34 (TSM.US, TDY.US, WTFC.US carry their October reports).

Not a licensed financial adviser; the owner decides and executes every trade.
