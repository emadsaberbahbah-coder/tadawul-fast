# Share-class confirmation review follow-up — 8 October 2026

The completed [Codex review on PR #738](https://github.com/emadsaberbahbah-coder/tadawul-fast/pull/738#discussion_r4218426484)
identified a real compatibility regression in venue resolution. The reviewed
head `33b2fa655e4f054cd17fbb95ee42f799775ff905` and merged release
`0c847c85bb27ee0f50de1b23c28926b2e22c3800` contain the same code.

`BRK.B`, `BF.B` and `HEI.A` are US share-class forms recognized by the existing
symbol-normalization convention. The tightened confirmation resolver interpreted
their class letters as unknown exchange suffixes. Under session enforcement,
these symbols could never advance their confirmation clocks. In observe mode,
their legacy action remained unchanged but the shadow diagnostic incorrectly
reported unavailable session evidence.

An independent offline replay used a synthetic qualifying holding, a frozen
2026-10-07 21:00 UTC clock and prior count 1 dated October 6. The pre-PR-738
source produced ADD with SAR 3,881 funding and count 2 dated October 7. The
merged source produced HOLD with zero funding, retaining count 1 and October 6.
Dash-form `BRK-B` and explicit `BRK.B.US` retained their valid ADD behavior.
This is a controlled compatibility witness, not a live trading recommendation.

## Bounded repair

Portfolio actions **1.14.2** recognizes the existing strict ASCII share-class
shape only after mapped exchange suffixes have been resolved and the shared
exchange registry confirms that the final letter is not an exchange suffix.
The shared normalizer remains unchanged. Known venues retain their existing
calendars; registry-only unsupported exchanges and malformed or unknown explicit
suffixes continue to fail closed. Unknown-calendar state preservation, diagnostic
redaction, protective actions and off/observe policy remain required regressions.

Actual confirmation and funding tests cover the reviewed dot forms, dash and
explicit-US aliases, completed-session progression, same-session/pre-close
behavior, weekends and exchange/Unicode/malformed counterexamples. Existing
required CI collects the calendar regression file. Legacy protective harnesses
remain in their isolated required processes. The deployment verifier **1.0.38**
and harness release pins match portfolio actions **1.14.2**.

## Deployment and acceptance limits

Render deployment `dep-db3o187avr4c73aijn4g` brought the existing merge
`0c847c8` live at **14:39:50 Riyadh**. Readiness at **15:06:33 Riyadh** returned
HTTP 200, engine **5.151.6**, portfolio actions **1.14.1** and confirmation
mode **observe**. This follow-up is a separate source candidate until its own
review, merge and deployment are verified.

The repaired spelling convention does not establish a complete instrument/MIC
registry or a versioned holiday/half-day calendar. Confirmation, settlement and
margin enforcement activation, installed Apps Script parity, approved membership,
coherent publication, durable history and broker charge classification retain
their separate acceptance requirements in the
[audit status register](finalization-status-20261008.md).
