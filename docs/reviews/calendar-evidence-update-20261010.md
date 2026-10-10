# Calendar evidence update — 10 October 2026

This follows merged PR #748 at `616f60605b6113dfdda362c3b0b9d689507d65ee`.
The remaining calendar defect was reproduced in the actual producer and
consumers: Yahoo fallback dates were displayed as EODHD, per-event provenance
was lost on consumption, the tracker read only the first 2,000 rows, and the
brief could interpret a stale countdown as a current event.

## Resulting contract

`Calendar_Events` keeps its first seven columns in their original order. Six
appended columns identify the source, UTC observation time and evidence status
of earnings and ex-dividend separately. The observation is an acquisition
clock; `Updated At (Riyadh)` remains a summary/publication field.

| Status | Meaning |
| --- | --- |
| `reported` | EODHD supplied a date, or Yahoo supplied an ex-dividend date. |
| `estimated` | Yahoo supplied an earnings estimate. |
| `unknown` | Missing, legacy, incomplete, invalid or unsupported provenance. |

No vendor report establishes issuer confirmation. Free-form Source notes,
publication clocks and unsupported confirmation labels do not become typed
evidence. Invalid/future observation clocks downgrade provenance to unknown.
A valid dated legacy event can still produce a conservative warning.

The new provider evidence APIs preserve source and observation per event;
the existing date-only APIs retain their original return shape. The writer
calls the provider's supported Yahoo-only path when the EODHD key is absent.
Missing fields remain unknown. Partial refreshes carry each surviving event's
original evidence; repeated publication does not refresh its acquisition time.

Publication remains one RAW rectangular update containing header, body and
blank tail. Existing layouts and newly claimed columns must be unambiguous
before migration. Preparation may grow the grid without deleting old cells;
unknown layouts or pre-existing unheaded extension data refuse replacement.
A lost write acknowledgement remains unconfirmed and requires readback before
retry, as documented in the preceding publication update.

## Consumers and historical compatibility

The tracker and brief read the complete supported calendar, up to 5,000 body
rows, independently of market-page sampling caps. Oversized or unreadable
tables supply no event context. Conflicting duplicate witnesses are not resolved
by selecting whichever row appears first.

Warnings and the brief's presentation holdback use valid dates and the current
Riyadh day. Static sheet/note countdowns cannot manufacture an upcoming event.
Native Top10 retains its numeric date-map API and labels estimates, reports and
unknown evidence. Financial withholding retains only the bounded calendar
warning; old order text and monetary claims remain withheld.

`Signal_History` appends eight fields for both event dates and their evidence.
Its original 18 columns and historical rows remain in place. Migration requires
recognized headers and empty newly claimed columns; a missing header is
initialized only after confirming the whole allocated body is empty. Legacy
rows retain unknown provenance rather than acquiring modern receipts.
`Performance_Log` outcome policy, backend qualification and funding are unchanged.

Versions: calendar evidence **1.0.0**, provider **1.3.0**, calendar sync **1.2.0**,
tracker **6.44.0**, brief **1.17.2**, native Top10 **1.13.4**, verifier **1.0.45**.
Focused producer/consumer/native regressions are enrolled in blocking CI.

Local verification at the candidate tree: **409 Python tests**, **163 native
checks**, and calendar self-test **9/9** passed. Compilation and diff checks
passed. Provider and Sheets boundaries used synthetic offline fixtures; these
checks did not write a workbook or send a brief.

## Release acceptance and remaining improvements

Source tests do not establish installation in the bound workbook. After merge,
install Top10 1.13.4, run the calendar writer once, and read back the full table
and dated warnings at the same release. Confirm legacy migration and individual
event observation clocks, including an event beyond row 2,000. Do not rerun an
unconfirmed write without first checking its actual result.

The approved Market_Leaders/Mutual_Funds membership manifests, issuer identity
coverage, broker account capture and strict decision-publication acceptance
remain separate requirements. Their coverage floors and execution guards stay
in place; calendar evidence does not clear them. A verified issuer integration
and precedence contract remain needed before any issuer-confirmed promotion.

Independent review reconfirmed the next research improvements: chronological
evaluation with purged label windows and training-only baselines, followed by
truthful news acquisition/archive recovery. Compatible dependency maintenance
also remains open. These are subsequent scoped changes, with no calibration
mode or news-effect activation in this update.
