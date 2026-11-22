# Commit sheet: universe_maintenance 1.1.0 (22 Nov 2026)

## The request

Owner, 22 Nov 2026: new IPOs should be added automatically, the best missing companies should be added, and bad symbols should be removed, without manual work.

## What v1.0.0 could not do

| Gap | Evidence on the 22 Nov page (6,602 rows) |
| --- | --- |
| A new listing arrives only once it ranks in the US top 1,000 by market value | The additions lane reads the screener's top 1,000 and nothing else |
| A row that is refreshed every day and still cannot be used is never watched | 41 rows blocked as "Stale price bar (provider data N session(s) old)", 18 of them 20 sessions or more, one 98; 26 rows blocked as "Identity unverified" |
| Nothing can leave except a `.US` row the sync cannot read | The red predicate looks only at Warnings and Last Updated |

## What v1.1.0 adds

**New listings (`TFB_UNIVERSE_IPO`, default observe).** In the weekly additions run, EODHD's IPO calendar is read for the last 180 days. A priced US listing that began trading at least 30 days ago, is missing from the page and is worth at least USD 1.0 billion passes through every v1.0.0 addition filter. At most 5 a week, inside the same weekly cap (25) and row cap (6,750), ahead of the top-1,000 additions. A calendar that cannot be read is a note; the top-1,000 lane still runs.

**Quality watch and park (`TFB_UNIVERSE_PARK`, default observe).** A row that reads well but is blocked for a stale price bar or an unverified identity joins `_Universe_Watch` with a `quality:` reason. A stale bar counts its own sessions. A `.US` quality row absent from the active US list is retired by the v1.0.0 rule. Any other quality row, and a non-US row the sync cannot read, is parked at 20 trading days: appended to `_Retired_Symbols` with a reason that starts `parked`. Never parked: a held symbol, an unreadable `.US` row still on the active list, anything past the shared per-run cap (25) or below the row floor (6,512).

**Safety of the rollout.** A lane in observe writes only the report, even when `TFB_UNIVERSE_MAINT` is enforce, and leaves the top-1,000 lane exactly as v1.0.0 had it. A lane writes only when both switches are enforce. `off` on both is v1.0.0 exactly (tested, also with a v1.0.0 configuration and a page without the Block Reason column). The JSON artifact no longer names a held symbol.

## What it does not do

- No additions outside the US: the row cap leaves 148 seats, the account funds US dollars only, and Market_Leaders is an owner-approved roster with a membership fingerprint.
- No removal for low traded value: 477 US-dollar rows trade under USD 100,000 a day, and the row floor leaves room for 90 removals.
- A parked row does not return by itself: delete its `_Retired_Symbols` line and paste the symbol back, as for a retired row.
- The IPO calendar call is new and has not been run against the live provider from here. In observe it can only produce a report section or a note.

## Evidence

- `python scripts/universe_maintenance.py --selftest`: PASS (6 new checks).
- `tests/test_universe_maintenance.py`: 88 passed (25 new: U14 to U20).
- `python scripts/audit_repository_workflows.py`: errors=0 warnings=0.

## What the owner does

1. Merge. Nothing changes on the page: the keeper is in observe, and both new lanes default to observe.
2. Read `_Universe_Report` after the next Sunday run: the sections WOULD ADD NEW LISTING and WOULD PARK OR RETIRE FOR QUALITY.
3. To arm, in this order: `TFB_UNIVERSE_MAINT = enforce` (with `TFB_SYNC_RETIRE_TAB = enforce` for removals), then `TFB_UNIVERSE_PARK = enforce`, then `TFB_UNIVERSE_IPO = enforce`.
