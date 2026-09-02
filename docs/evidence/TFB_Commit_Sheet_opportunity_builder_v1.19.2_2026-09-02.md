# Commit sheet — core/analysis/opportunity_builder.py v1.19.2 [ELIGIBILITY (VENUE)]

**Base (live `main`):** v1.19.1 · SHA256 `44721dd6…` · **Delivered:** v1.19.2 · SHA256 `a12d0d5b08e8506e…` · 5,205 lines (was 5,147)
**Repo path:** `core/analysis/opportunity_builder.py` · **Project copy of this sheet:** `TFB_Commit_Sheet_opportunity_builder_v1.19.2_2026-09-02.md` · **Repo copy:** `docs/evidence/` same name

## What it does
New gate **"Eligibility (Venue)"** (FAIL_MAJOR, in GATE_ORDER right after Eligibility (KSA)): a candidate passes iff its market name OR its symbol suffix is in `TFB_T10_VENUE_ALLOWLIST` (CSV, case-insensitive; bare ticker = suffix US). Fail-open: unknown market with no suffix passes as "venue unknown".
**Default inert:** allow-list unset/empty → gate not appended → v1.19.1 gate list, verdicts, selection, KPIs, alerts, audit and near-miss **byte-identical** (harness A; only `generated_at_utc` differs).

## Evidence
11:10 board: Selected = HDFCBANK.NS (NSE), qualified set carried 2317.TW / TRAN.BA / BMRI.JK — venues a foreign retail IBKR/Derayah account generally cannot trade. "Passed" counted names that can never become a ticket.

## Arming (Render, your hands, after reading IBKR Trading Permissions)
`TFB_T10_VENUE_ALLOWLIST` = e.g. `US,NYSE,NASDAQ,AMEX,SR,TADAWUL` (+ whatever your permissions list shows: L, PA, DE, SW, T, HK, TO, AX …). Kill-switch: delete the variable. Recommendation-touching (declared version break) — arm as its own evidence run.

## Protocol
Live-fetch + SHA · 5 anchored edits (count==1) · py_compile · AST zero-removal (added `_env_venue_allowlist`, `_venue_eligibility`) · harness on REAL `build_opportunity_payload` original vs patched ×3 (inert byte-identical; NSE/TWSE fail at the venue gate; SR/US/bare pass; NSE-only allow-list fails NVDA; gate order verified) · repo tests **42 passed**.

## Deferred to v1.19.3 (deliberately not rushed into the ticket path today)
STALE_PRICE as a ticket-stage deferral (name stays in ALL QUALIFIED with "live quote required for GO", new funding state QUOTE_STALE) — needs the selection-loop/funding-layer read first. Mitigation until then: refresh the board 08:00–09:59 Riyadh.
