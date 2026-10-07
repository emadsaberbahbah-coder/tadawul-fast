# TFB commit sheet — provider routing and acquisition truth — 2026-10-07

Status: code review candidate, based on `26fe4960e7e4f03ab3066ed3031e91eeb4c8a3b4`. All fixtures in this sheet are synthetic. No live workbook write, production pipeline refresh, environment change, deployment, workflow dispatch or trade was performed for this implementation.

## Problem and behavior

The generic quote/history order sends Yahoo-native futures and indices to unsupported EODHD paths, and `.MI`/`.NZ` instruments do not have a verified EODHD mapping in this code. A failed unpriced primary response can also supply misleading provider, currency, timestamp and terminal-failure facts to a later successful quote. Timestamp-only freshness then overstates coverage after failed acquisition or last-good preservation.

The shared instrument capability rule filters the configured quote and history provider lists. Symbols ending `=F`, starting `^`, or ending `.MI`/`.NZ` use a configured Yahoo provider without guessing an EODHD exchange or changing symbol identity. If Yahoo is absent from that configured list, routing does not enable it. Ordinary instrument provider order and KSA exceptions remain intact. Existing Yahoo enrichment and identity rescue have separate controls; this change does not claim the configured quote/history list disables every independent Yahoo enrichment request.

An unpriced provider attempt is retained as a scoped `quote_attempt:<provider>:unpriced` diagnostic. It cannot donate quote facts to a later provider's positive price. A priced provider response with terminal `fetch_failed` evidence remains invalid, including a raw singular error field that canonicalization would otherwise omit. When every source fails, the terminal failure and unavailable acquisition remain visible. Finnhub price entry points remain unchanged and disabled.

## Acquisition and timestamp contract

Engine version `5.151.1` records retrieval acquisition provenance in the existing Warnings column; synchronizer version `6.64.3` and both freshness audits use one pure classifier. Canonical sheet width remains 115 columns.

Successful acquisition requires a finite positive price, compatible actual provider, bounded precise retrieval timestamp and no failure, quarantine, last-good, stale or conflicting provenance. Optional price-bar time is retained only when the price source supplies a precise timestamp. `Last Updated` and `acquisition_acquired_at` describe retrieval/publication, not the market quote's time. Missing actual quote-as-of remains unknown; it does not invent quote age or introduce a mandatory quote-time field for legacy rows.

Cached reads retain their original acquisition evidence. Snapshot/history/last-good recovery cannot become successful acquisition solely because publication is recent. Failed origins are captured before preservation replacements, and excluded symbol sets are combined once. Contradictory aliases, duplicate headers and repeated status facts cannot hide a known negative fact behind a later value.

Actual PV1, KLG, firewall-keep and PV2 restorations carry `acquisition_status:preserved` into their published warning cell, including a prior row with an otherwise recent clean timestamp. The marker supersedes only the old typed acquisition status; price, provider, original times and other warnings remain intact. Real-runner PV1-missing and KLG-failed fixtures verify that PAGE and full-row audits both report 3/4 successful acquisitions while all four retrieval stamps are timely; the fresh provider row is untouched. The deterministic daily harness now distinguishes legacy policy coverage from unknown factual acquisition rather than inventing proof from row counters.

Factual acquisition census and audits are independent of rollout mode. Existing feed eligibility still follows configured off/observe/enforce behavior; observe is not silently converted to enforcement. Floors, fees, risk flags, identity/currency checks and policy settings are retained. No physical alias-twin removal or universe shrink is claimed by this repair.

## Candidate validation

New provider-spy tests exercise actual engine orchestration without network access: all four page contexts, configured provider lists, direct-call routing guards, intact identity/currency, positive secondary fallback, all-source outage, raw priced error, missing quote time, cached reads, and history/snapshot preservation. The provider suite passed 56 cases in the production-CI pinned Python 3.11 environment. A supplementary numpy/pytest-only Python 3.12 environment passed 55 with one skip for importing the unchanged Finnhub module's optional HTTP dependency; that run demonstrates dependency-light behavior, not the exact CI Python version.

Focused validation before the final restoration-marker repair passed 211 tests plus 9 subtests in the pinned contract environment (affected acquisition/audits/sync, recovery, engine and deployment manifest). The affected acquisition/audit/sync plus manifest subset in the supplementary minimal Python 3.12 environment passed 160 tests plus 9 subtests, with the same optional Finnhub import skip. Existing return-not-none harness warnings were disclosed (three pinned, two minimal). Changed Python files compile and `git diff --check` passes. Final repair validation and complete workflow commands in exact Python 3.11 lean and pinned contract environments remain pending the root review runner and will be appended before publication.

The new provider/acquisition and existing affected audit/fetch-failure regression files are wired into the blocking lean CI job. Runtime version pins are updated in `scripts/verify_deployment.py`.

An independent anonymous Yahoo chart canary for `GC=F`, `^TASI.SR`, `ENI.MI` and `AIR.NZ` returned HTTP 429 on 2026-10-07 and was stopped without retries or a host bypass. That path does not establish production provider behavior. Offline routing tests cannot demonstrate restored live CFX coverage or guarantee a 95% acquisition threshold; acceptance requires a later successful pipeline read-back.

## Coordinated release verification and rollback

Final local workflow validation used Python 3.11.9: the exact checked-in lean command passed 442 tests, with 3 optional skips, 2 pre-existing return-value warnings and 9 passing subtests; the production-pinned framework contract command passed 25 tests with 2 schema-capability skips. Both compile steps passed. Investment policy passed 18 tests and the deterministic smoke assertions; repository workflow audit passed 7 tests and reported zero errors with 27 existing maintenance warnings. The daily CI test command passed 75 tests with 2 schema-capability skips, followed by the deterministic harness at 84/84 (one external-export suite skipped by design). The local daily command used the pinned framework environment; the remote daily job additionally installs the complete repository requirements. These counts include repeated coverage across jobs and are not a unique-test total. Remote checks must be assessed on the published commit.

This sheet is evidence for code review, not a deployment instruction or proof of live readiness. A future coordinated release must verify the backend engine version, synchronizer version and both audit versions together, then compare requested count, successful acquisition count, timestamp count, failed-origin and preserved rows in the same pipeline run. Read-back must retain failed/quarantined rows and report factual shortfall even when rollout policy is observe. Quote-as-of availability should be reported separately from retrieval freshness.

Rollback is a code revert of this candidate and its deployment pins, followed by coordinated backend/synchronizer version verification. No environment arming or worksheet schema migration is part of this candidate.
