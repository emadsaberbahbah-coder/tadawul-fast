# Commit sheet — data_engine_v2 5.165.0 [HOLD-PROSE]: a HOLD row no longer says "scale in"

## Why
Owner's external audit of 25 Nov 2026, P1 analysis: My_Portfolio rows GLPI, AX and IBOC read HOLD / WATCH,
while their reason said "Scale in / accumulate ... supports a gradual position" and their sizing hint said
"Scale in gradually / partial position".

Root cause: scoring writes the ACCUMULATE prose. The investability gate and the 5.102.0 coherence pass then
demote the row to HOLD. `_reconcile_recommendation_family` rewrote only the reason's prefix ("HOLD:"), and the
5.77.23 hint rule treated every hint as "flexible" on HOLD.

## Change (TFB_HOLD_PROSE_POST_GATE, default on)
On a final HOLD:
- A hint with add-capital language and no hold/trim language becomes the HOLD hint, "Maintain or trim".
- A reason whose text after the prefix opens with add-capital language becomes
  `HOLD: no new capital now; pre-gate view: <original text>`, so the pre-gate rationale stays visible
  and is labelled as pre-gate.

Both rewrites are idempotent. The change is presentation only: no score, verdict, rank, final_action or column
changes. BUY-family and sell-family rows are unchanged.

## Rollback
Set the Render ENV `TFB_HOLD_PROSE_POST_GATE=off`. The output is then byte-identical to 5.164.0.

## Tests
- `tests/test_de_hold_prose_post_gate.py` (new, 13 tests): demoted HOLD, idempotence, the coherence-pass
  path, BUY-family kept, HOLD-language hints kept, off = 5.164.0, sell family unchanged.
- Version pins moved to 5.165.0 in six tests and in `verify_deployment.py`.

## Deploy
Needs a Render deploy (the owner's ask). Read-back after the next My_Portfolio refresh: GLPI, AX and IBOC
should show hint "Maintain or trim" and a reason starting "HOLD: no new capital now; pre-gate view:".
