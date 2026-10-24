# Dependency security review — 24 October 2026

Baseline: `561db60c74c1d56d866d4b72756f8ef9a9df57d5`. This is a separate
dependency revision; the workflow-health branch is unchanged. No deployment,
new schedule, advisory exclusion, trading action or financial gate change is
part of this revision.

## Confirmed changes

| Component | Baseline | Candidate | Reason |
| --- | --- | --- | --- |
| aiohttp | 3.14.3 | 3.14.4 | Eight confirmed advisories fixed in the patch release. |
| FastAPI / Starlette | 0.115.14 / 0.41.3 | 0.140.13 / 1.3.1 | Close seven Starlette advisories with a supported framework pair. |
| OpenTelemetry API, SDK, OTLP exporter | 1.28.0 | 1.45.1 | Fix unbounded tracestate propagation; retain the coherent family. |
| OpenTelemetry instrumentation, ASGI, FastAPI | 0.49b0 | 0.66b1 | Match the SDK's exact semantic-conventions/instrumentation family. |
| NLTK | Unused reserved pin 3.10.3 | Retired | No fixed upstream release; no implemented runtime API or reverse dependency uses it. |
| CI-only pypdf | 6.20.0 | 6.21.2 | Close nine additional findings discovered in the full installed scan; production dependencies are unchanged. |

Protobuf remains 5.29.6 and grpcio/grpcio-status remain 1.64.3. The full
resolver and `pip check` accept this combination. Exact framework pins in the
required CI contract lane follow the production file.

NLTK retirement is supported by a complete source/import search and installed
reverse-dependency inspection, followed by actual RSS-to-news-result tests.
Eight fixed scenarios return identical results when the capability probe is
present or absent, with real imports forbidden and checked after execution.
The health response truthfully changes only its NLTK availability field. The
reserved tokenizer smoke is replaced by explicit production-pin/installed
absence guards; it is not silently skipped. No implemented sentiment or
prediction path is removed.

## Audit scope and evidence

The original advisory job audits exact direct pins with `pip-audit 2.9.0
--no-deps --disable-pip`. It does not resolve transitives and skips the direct
httpcore wheel URL. The same command reports **17 advisories in four packages
before, zero after**, with that skipped URL still explicitly reported.

A separate scan of the complete installed environments includes the
bootstrap installer and the CI-only test packages. Its initial comparison is
**32 advisories in 144 distributions before, 15 in 139 distributions after**.
Those remaining findings were six in bootstrap pip 25.0.1 and nine in the
CI-only pypdf 6.20.0. They remain in the original receipts. Applying the
required heavy lane's existing pip-upgrade policy to both isolated venvs
reduces the baseline to 26; the candidate with pip 26.2 and test-only pypdf
6.21.2 reports **zero advisories across all 139 installed distributions, with
zero skipped entries**. The advisory whole-suite lane now upgrades pip before
installing dependencies too. No production transitive finding or NLTK
reinstallation was observed in the candidate scan.

The nine `docs/evidence/dependency-health-*-20261024.json` receipts retain the
direct and installed audit results and full resolver package/version/Requires-
Dist/artifact-hash closure. Provenance records tool/Python versions, time,
requirements digest and raw report digest. Resolver package descriptions and
registry URLs are omitted; no dependency identity or artifact hash is removed.
Resolver comparisons include the same named CI test extras; the final
candidate deliberately changes only the test-only pypdf version. Initial,
pip-aligned baseline and final candidate stages remain distinct. The existing full
environment was never modified; baseline and candidate use isolated venvs.

The advisory workflow now always retains its JSON artifact and prints the
same report's findings/skipped count or UNKNOWN if unavailable/inconsistent.
Its original audit exit, advisory job policy and schedule remain unchanged.
The summary explicitly distinguishes direct pins from resolved dependencies.

## Behavioral verification

All local comparisons use Python 3.12.14. They do not establish Python 3.11
CI acceptance or deployment by themselves.

- Original dependency stack: 151 tests passed, 11 skipped across security
  libraries, auth wall, route/schema contracts and news contracts/lifecycle.
- Candidate dependency/security suite: 48 passed, two expected optional
  golden-negative skips. Required smoke imports fail if unavailable.
- Final candidate security and daily-brief suite: 91 passed, two expected
  golden-negative skips. All 43 daily-brief blocked-input cases, including
  generated-PDF readback, also pass on the original stack; blocked/feed-veto
  evidence remains present after the PDF parser update.
- The new framework security cases reproduce nine baseline failures and two
  valid cases, then all eleven pass on the candidate. They exercise library
  URL construction and form limits; they do not claim production endpoints
  expose every affected library feature.
- Oversized combined tracestate is discarded by the candidate's actual
  TraceContext propagator while the valid traceparent is retained; baseline
  accepts it. Ordinary propagation still succeeds. The real ASGI-to-SDK-to-
  OTLP gRPC and HTTP protobuf encoders preserve every span ID through local
  in-memory collectors on both stacks.
- Existing HTTP/2, aiohttp POST/chunked-gzip, multipart, Google signing,
  protobuf consumers and network-blocked `main:app` smoke checks remain active.

The first complete candidate run recorded 5,810 passed, 43 skipped, 274
subtests and three failures confined to framework-compatibility harness
assumptions: URL rendering of authority-free synthetic scopes and flat
inspection of lazy included routers. The harness now keeps universal
independent scope-path/auth assertions, retains URL parity with real authority,
and uses the existing effective-route traversal. All scopes, token/root-path
tripwires, HTTP checks and exact route ownership/methods remain required.
These adapted harnesses pass identically on both stacks: 45 passed, six
skipped, 31 subtests. The broader candidate contract battery passes 143 tests,
nine skips and 31 subtests. The final complete candidate suite passes
**5,813 tests and 274 subtests**, with 43 intentional/optional skips and three
preexisting test-return warnings, in 405.37 seconds. Independent final review
has no outstanding findings; its combined batch passes 136 tests and 31
subtests with eight intentional/optional skips. Exact-head CI and any later
deployment acceptance remain required; this revision has not been deployed.

Primary references: [aiohttp 3.14.4](https://github.com/aio-libs/aiohttp/releases/tag/v3.14.4),
[OpenTelemetry advisory](https://github.com/open-telemetry/opentelemetry-python/security/advisories/GHSA-xcq7-p3pf-p88h),
[SDK metadata](https://pypi.org/pypi/opentelemetry-sdk/1.45.1/json),
[protobuf compatibility](https://pypi.org/pypi/opentelemetry-proto/1.45.1/json),
[NLTK no-fix advisory](https://github.com/nltk/nltk/security/advisories/GHSA-8mgp-746c-j5xp).
[pypdf security fixes](https://pypdf.readthedocs.io/en/6.21.2/meta/CHANGELOG.html).
