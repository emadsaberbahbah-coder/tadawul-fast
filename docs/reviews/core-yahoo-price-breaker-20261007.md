# Yahoo quote transport isolation

The deployed quote provider used one circuit breaker for every symbol and both
public chart and authenticated yfinance transports. Repeated not-found/empty
instruments could open it, preventing an unrelated valid instrument from even
reaching its configured provider. A failing authenticated fallback could also
deny a healthy public chart transport.

Yahoo chart provider 8.15.1 separates the two transport breakers. Each retains
the configured failure threshold, cooldown and recovery threshold. Actual
authentication, rate-limit, network and server failures protect the affected
transport; a successful quote through the other transport neither repairs nor
disables that protection. The existing host ladder retains outage evidence
when a later host reports a symbol-local 404.

Known symbol-local 404/not-found and valid empty responses remain unpriced.
They use bounded backoff for that symbol through the existing cache, capped by
the existing quote TTL and circuit cooldown. A breaker-denied request creates
no false symbol-miss cache entry. Only a finite positive quote can advance the
corresponding transport's recovery. Existing response identity, currency,
coherence and timestamp handling remains binding.

The existing yfinance history call now requests `raise_errors=True`, supported
by the pinned 0.2.66 library. Suppressed fallback errors, including the library's
wrapped status/error descriptions and metadata getter failures, are classified
privately and stripped before enrichment. A usable priced fallback remains
accepted despite an optional information error. No retry or symbol alias is
added, and no policy threshold, membership list or production setting changes.

The engine release marker is 5.151.3; deployment verifier pins match both source
versions. The 6.64.5 portfolio census repair remains included from main.

Offline producer evidence uses the real quote method, host ladder, parser,
executor, cache and circuit state with transport seams replaced in memory:

- Beyond-threshold synthetic not-found/empty instruments no longer prevent a
  later valid native future or FX quote from reaching transport.
- Real outages, mixed host failures, half-open recovery and independently
  healthy transports retain correct failure accounting.
- The pinned yfinance `PriceHistory` implementation suppresses synthetic
  timeout/503 failures by default; the delivered sync path surfaces them with
  one history attempt and protects the fallback transport.
- Valid quotes retain their identity, currency and source quote timestamp;
  crossed identities and invalid prices cannot earn successful recovery.

The new suite is blocking in lean CI and the daily-sync full-requirements CI.
The two actual-library cases skip only in the lean environment. Focused proof:
67 passed, 2 skipped in Python 3.11 lean; 69 passed in the pinned full image.
The inherited loopguard and manifest regressions also pass independently.

A separate local lifecycle repair shields subscribers from cancelling the
shared acquisition. Cancelling its owner cancels the shared result, wakes all
subscribers and propagates cancellation. Ordinary owner errors still reach
every subscriber and flight cleanup retains its existing identity guard.
The real batch method explicitly propagates a gathered cancellation rather
than unpacking it as a quote or publishing a successful partial/empty batch.
Six direct concurrency and real-provider/batch regressions verify these
paths without inventing a price, cache entry or circuit failure.

Final combined required validation used the checked-in workflow commands and
Python 3.11 lean, production-pinned contract and full-requirements images:
all 14 steps passed. Lean: 657 passed, 5 optional skips and 13 subtests;
contract: 45 passed, 2 optional skips; full-source Apps Script: 18 passed;
daily-sync full image: 150 passed plus the deterministic 84/84 harness;
policy: 18 passed; workflow audit: 7 passed and zero scanner errors. Existing
test-return warnings and 27 action-major audit warnings remain unchanged.

This is local acquisition-path evidence. A deployed exact-SHA refresh and the
shared acquisition census must verify the resulting live coverage. Retrieval
freshness remains distinct from source quote age, liquidity and tradability.
Upstream symbol support and historical roster approval are separate evidence
requirements; no threshold is relaxed to make a failing cohort pass.
