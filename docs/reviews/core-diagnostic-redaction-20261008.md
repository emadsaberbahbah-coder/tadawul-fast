# Diagnostic credential redaction — 2026-10-08

Current main `4592677ae458f03bc1dcc6afcfb05c941868ae36` still echoed synthetic
credentials through BackendClient HTTP bodies and exceptions, application
messages/tracebacks, calendar HTTP error URLs, EODHD 401/403 row diagnostics,
caught route JSON error envelopes, and sync task reports. These are credential
exposure defects in diagnostic output. The offline witnesses establish code
behavior; they do not claim that a production credential was exposed.

The repair introduces a dependency-free diagnostic redactor and uses it only
at those output boundaries. Caller-known credentials protect bare echoes;
explicit runtime credential aliases protect configured values, including
supported token lists and Google JSON/base64 forms, including base64 accepted
under the primary Google credential variable names. Context patterns cover
unknown credential fields, authorization headers, URL userinfo, and private-key
blocks. Redaction precedes truncation. Malformed and deeply nested diagnostic
JSON cannot bypass redaction by breaking the formatter. Credential files are
never opened.

Existing logging formatters are delegated and their rendered messages,
tracebacks, and extras are redacted. Handler routing, levels, JSON contracts,
request IDs, HTTP statuses, exception classes, and retry counts are retained.
Root/main/Gunicorn/uvicorn handlers present at setup are wrapped idempotently;
handlers installed later need the same wrapper or another installation call.

BackendClient request authentication, POST backoff, and successful response
payloads are unchanged. EODHD classifies the same original first 200 raw body
characters before sanitizing exported text, preserving quota/plan/IP/auth
precedence and health accounting. Routes sanitize their explicit diagnostic
strings without walking successful business payloads. TaskResult sanitizes
only serialized `error` and string `warnings`; original state, financial and
acquisition facts, guard decisions, result counters, and no-write outcomes are
unchanged. No broker, policy, provider routing, threshold, or production
configuration changes belong to this patch.

## Reproduction and validation

Exact-baseline source copies and count-only proof artifacts were stored
privately under scratch work, without credential values. The same actual
transport/handler/runner regression suites run against baseline and repaired
source. There are no external HTTP calls.

| Suite | Exact baseline | Repaired |
| --- | --- | --- |
| HTTP client, main logging/error handler, calendar | 17 failed / 3 passed | 20 passed |
| EODHD request → quote/enriched diagnostics | 6 failed / 2 passed | 8 passed |
| Caught route envelopes, engine error shells, single quotes | 13 failed / 6 passed | 19 passed |
| Sync task read/write/guard failures and serialized reports | 3 failed | 3 passed |
| Shared leaf redactor | New suite | 77 passed |

Baseline failures retain synthetic credentials; positive controls verify
unchanged auth/status/retries, healthy rows, successful quote payloads,
HTTPException propagation, and earliest future calendar dates. Integration
tests use actual httpx MockTransport, mounted FastAPI/ASGI handlers and route
envelopes, real EODHD request/quote methods, and the real sync task runner.
The guard witness verifies a skipped task performs no clear/write and that
serialization does not mutate the original warning or error state.

Focused checks on the final source:

- Exact Python 3.11 lean: redactor/task/manifest plus existing sync outcome and
  successful-acquisition suites — **198 passed, 2 subtests passed**.
- Pinned Python 3.11 contract stack: all five security suites, manifest, existing
  main health modes and board funding route — **150 passed**.
- `git diff --check` — clean.

The production blocking workflow must execute the actual httpx/FastAPI suites;
their optional dependency skips in a lean environment are supplementary only.
The parent release combines independent fixes and runs the full required
checked-in workflow commands and remote CI on the combined SHA.

## Runtime versions

| Component | Version |
| --- | --- |
| Main entry | 8.14.2 |
| Dashboard sync | 6.64.9 |
| EODHD provider | 4.18.1 |
| Calendar provider | 1.2.1 |
| Enriched quote route | 8.5.1 |
| Analysis sheet rows route | 4.8.1 |

Dashboard sync's existing deployment pin is updated; the two changed provider
pins and their AST file mappings are added. Engine and opportunity-builder
versions remain 5.151.3 / 1.24.2 in this isolated patch; the parent owns final
pins for separately integrated runtime changes. Historical logs/artifacts are
not rewritten by a prospective diagnostic-output repair.
