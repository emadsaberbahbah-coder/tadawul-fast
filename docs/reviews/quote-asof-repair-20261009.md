# Executable quote evidence repair

The 9 October review found that the ticket quote-freshness gate consumed the
engine retrieval timestamp. A recently refreshed row could therefore pass
with an old market price, and absent timestamps passed as a skipped check.

Opportunity builder 1.25.0 retains the acquisition verdict from the original
row and assesses the actual `acquisition_quote_asof`. Successful acquisitions
with a recent market instant pass; older observations require a confirmed
latest venue close. Missing, conflicting, preserved, failed and unverified
proof cannot authorize new capital, including when the age policy is disabled.
Future market instants are withheld rather than clamped to zero age. Unknown
calendars cannot certify an old close. Bare US share classes use the existing
symbol grammar before falling back to an unknown venue.

The existing research/allocation signature binds the original rows, portfolio,
FX, effective policy and release. This protects replay consistency; it does
not independently authenticate a provider observation or broker cash source.
The change preserves research/audit rows and does not restore any roster,
alter thresholds, activate observational forecast controls or place orders.

Validation: actual normalization, quote assessment and allocation tests cover
retrieval versus market time, failed/preserved/history/stale rows, absent and
conflicting receipts, future dates, closed/open/unknown sessions, disabled age
policy, semicolon serialization and nonfinite policy input. Successful legacy
algorithm fixtures now carry complete synthetic acquisition observations;
their assertions and production guards remain active.
