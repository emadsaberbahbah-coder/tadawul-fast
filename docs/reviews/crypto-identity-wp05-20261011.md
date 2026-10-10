# WP-05 crypto identity review

The October 10 handoff identified Yahoo short-ticker collisions repeatedly kept
alive by last-good. This change supplies three independent, default-off controls:

- `TFB_SYNC_KLG_CRYPTO_NAME_GATE=off|observe|enforce`: refuse a definite curated
  name mismatch as a predecessor, and screen the final matrix after every
  restoration pass. Observe leaves cells unchanged; enforce writes an honest
  Symbol-only stub tagged `identity_quarantined:crypto_name:v6.65.0`. Both primary
  and recovery Actions lanes map the flag with `off` as their default.
- `TFB_EODHD_CRYPTO_MAP=1`: explicit curated `BASE-USD` pairs map to EODHD
  `BASE-USD.CC`. The existing cached normalizer requires a restart after arming.
  Other instrument forms and unarmed outputs preserve the previous mapping.
  Native `.CC` forms are idempotent through both the engine and EODHD provider
  normalization steps; the provider must never append `.US` on its second pass.
- `TFB_CRYPTO_NAME_GATE=off|observe|enforce`: screen the real Yahoo fundamentals
  channel through the identity guard's shared adapter. Enforce discards the
  whole conflicting fundamentals patch, so its numeric fields cannot survive
  after only the display name is removed. Observe logs and preserves the patch.

`core/symbols/crypto_identity.py` extracts the audit vocabulary without changing
its semantics; the auditor and runner import that same dictionary. Unknown roots
are not judged. Data-free stubs retain their original tags and are idempotent.
The final screen runs after second-chance persistence, preventing restoration
from undoing its decision. The operator quarantine remains available for a
one-run poison reset and does not require these new controls to be armed.

A curated substring match is only plausibility evidence: the existing `COMP`
entry also accepts “Compound Coin.” The name guard therefore cannot replace
verified provider identifiers. This ambiguity is explicitly tested and preserved
rather than silently rewriting the curator's policy. CMC-suffixed Yahoo identifiers
and coin universe membership remain owner decisions. EODHD coverage has not been
confirmed against a live provider in this task; arm only after a provider read-back.

Offline regressions use actual runner and engine modules, a Sheets read-boundary
double and a Yahoo provider boundary double. They cover defaults, modes, healthy
and unknown names, other asset classes, no-read fast paths, final stubbing,
idempotence and native routing. No broker, Sheet or Render calls are made.
