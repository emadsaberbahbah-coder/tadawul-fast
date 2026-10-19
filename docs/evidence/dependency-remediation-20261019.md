# Dependency security remediation — 2026-10-19

The revised production requirements installed successfully on CPython 3.11.17,
passed `pip check`, and passed a strict audit of every installed distribution:
**132 packages, zero known vulnerabilities, zero skipped packages**. The audit
includes the resolved `httpcore==1.0.9` distribution from the retained wheel URL.
These results describe the candidate dependency set and the advisory data read
on 2026-10-19; they are not a deployment receipt.

## Scope and version decision

The starting repository commit was
`9aac4356e7d540b330af7083a9850b5166592536`. A comparison of non-comment requirement
entries confirms exactly these changes; the other **69 entries are unchanged**.

| Dependency | Previous requirement | Candidate requirement | Reason |
| --- | --- | --- | --- |
| FastAPI | `0.115.14` | `0.133.0` | First release supporting Starlette 1.x; required to install the fixed Starlette version. |
| Starlette | `0.41.3` | `1.3.1` | Highest minimum fixed version among the seven reported Starlette advisories. |
| NLTK | `3.10.3` | Removed | No patched release for the reported advisory, and no production imports or calls. |

FastAPI's [0.133.0 release notes](https://fastapi.tiangolo.com/release-notes/#01330)
record Starlette 1.x support. Public PyPI metadata confirms that
[0.132.1](https://pypi.org/pypi/fastapi/0.132.1/json) requires
`starlette>=0.40.0,<1.0.0`, while
[0.133.0](https://pypi.org/pypi/fastapi/0.133.0/json) requires
`starlette>=0.40.0`. Both require Python 3.10 or later. The exact Starlette pin
keeps the selected framework pair bounded.

[Starlette 1.3.1 metadata](https://pypi.org/pypi/starlette/1.3.1/json) requires
Python 3.10 or later and `anyio>=3.6.2,<5`; its optional full dependencies permit
the retained `httpx==0.27.2`. The candidate retains the existing AnyIO,
Pydantic, Pydantic Core, HTTPX, HTTPcore, and all other production pins.

## Actual failure addressed

The [dependency job from run 39163216738](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39163216738/job/117780701910)
reported eight vulnerabilities in two packages. Its requirements-only audit
also skipped the direct-wheel HTTPcore entry. The aliases and fixed versions
below agree with the retained earlier audit evidence and the current successful
installed-package audit.

| Package | Advisory | Minimum fixed version reported |
| --- | --- | --- |
| Starlette | [GHSA-86qp-5c8j-p5mr](https://github.com/advisories/GHSA-86qp-5c8j-p5mr) / PYSEC-2026-161 | `1.0.1` |
| Starlette | [GHSA-82w8-qh3p-5jfq](https://github.com/advisories/GHSA-82w8-qh3p-5jfq) / PYSEC-2026-249 | `1.3.1` |
| Starlette | [GHSA-jp82-jpqv-5vv3](https://github.com/advisories/GHSA-jp82-jpqv-5vv3) / PYSEC-2026-248 | `1.3.0` |
| Starlette | [GHSA-7f5h-v6xp-fcq8](https://github.com/advisories/GHSA-7f5h-v6xp-fcq8) / PYSEC-2026-1942 | `0.49.1` |
| Starlette | [GHSA-2c2j-9gv5-cj73](https://github.com/advisories/GHSA-2c2j-9gv5-cj73) / PYSEC-2026-1941 | `0.47.2` |
| Starlette | [GHSA-wqp7-x3pw-xc5r](https://github.com/advisories/GHSA-wqp7-x3pw-xc5r) / PYSEC-2026-2281 | `1.1.0` |
| Starlette | [GHSA-x746-7m8f-x49c](https://github.com/advisories/GHSA-x746-7m8f-x49c) / PYSEC-2026-2280 | `1.1.0` |
| NLTK | [GHSA-8mgp-746c-j5xp](https://github.com/advisories/GHSA-8mgp-746c-j5xp) | None |

The NLTK advisory lists versions through 3.10.3 as affected and no patched
version; [PyPI's NLTK metadata](https://pypi.org/pypi/nltk/json) still reports
3.10.3 as the latest release at review time. A case-insensitive scan of every
production Python file found only two references, both in
`core/news_intelligence.py`: the optional `_module_exists("nltk")` capability
probe and the corresponding health-report field. The existing tokenizer uses
standard-library regular expressions. Removing NLTK therefore makes the
capability field false while preserving the implemented tokenizer and news
pipeline. No production Python source was changed for this dependency repair.

## Complete installed audit

The candidate was installed from `requirements.txt` into a separate CPython
3.11.17 environment. A second environment contains `pip-audit==2.9.0`, so audit
tool dependencies do not enter the application inventory. The successful
invocation was:

```sh
/tmp/tfb-dependency-review-20261019-auditor312/bin/python -m pip_audit \
  --path /tmp/tfb-dependency-review-20261019-candidate/lib/python3.11/site-packages \
  --strict --format json \
  --output /tmp/tfb-dependency-review-20261019-resolved-audit.json \
  --progress-spinner off
```

The command exited **0** and printed `No known vulnerabilities found`. The
complete artifacts were preserved before installing any test tools:

- [Audit JSON](dependency-audit-resolved-20261019.json): all 132 distribution
  records, including FastAPI 0.133.0, Starlette 1.3.1, and HTTPcore 1.0.9; every
  `vulns` list is empty and no distribution has a skip reason.
- [Installed inventory](dependency-installed-20261019.json): the corresponding
  `pip list --format=json` output. Normalized name/version pairs match the audit
  exactly, with no missing, extra, or duplicate records.

The 132 records include the seeded environment's pip, setuptools, and wheel.
They exclude pytest, PDF/spreadsheet test dependencies, Ruff, and the separately
installed auditor. An unseeded production environment can therefore contain
fewer bootstrap distributions; its own inventory must still match its own
audit exactly. The evidence files contain package names, versions, and audit
results, with no credentials or request URLs.

Requirements snapshot SHA-256:
`68c79207dcd8ccc7df7bd7aa1923c70eb5bbee4f72f059ac5269b95c86a57f80`.

The local auditor ran under Python 3.12.14 while reading the already-resolved
Python 3.11.17 target. The same auditor under the local Python 3.11 runtime
encountered a proxy `426 Upgrade Required` response before completing a query.
The successful 3.12 invocation used the same audit version, strict flags,
target, and vulnerability service. No advisory was ignored and no package was
excluded. This local transport distinction does not change the application's
Python runtime or resolved dependencies.

## Compatibility and regression proof

The tests run with the installed candidate requirements. Test tools were added
only after the production inventory and audit were captured; the final test
environment uses pytest 9.1.1, OpenPyXL 3.1.5, ReportLab 5.0.1, PyPDF 6.20.0, and
Ruff 0.16.8. `pip check` remained clean after adding those tools.

The revised security floor test rejects explicit mutations to FastAPI 0.132.1,
Starlette 1.3.0, or reintroduced NLTK 3.10.3. The strict library smoke requires
the installed framework versions to equal the production pins, requires NLTK
to be absent, and exercises the actual English/Arabic news tokenizer. It also
checks form parsing, file upload, HTTP/2, local aiohttp traffic, application
import, mounted routes, and OpenAPI generation. Mocked news tests exercise the
existing news pipeline with provider traffic disabled.

The first complete focused run produced 332 passed, 10 skipped, and two failed
assertions in authentication cases W8/W15. Both unchanged cases passed on the
old framework. The failure was isolated to an upstream URL-formatting change:
the fixed Starlette validates Host values and falls back to `scope.server`.
For handcrafted requests with no server and a rejected Host, it instead builds
a relative URL. Thus scope path `//health` can produce URL path `""`, and
`//x/health` can produce URL path `/health`. The real scope-based auth helper
continues to return the complete protected path.

Only the auth test harness changed. W8/W15 now compare historical behavior
against an explicit frozen Starlette 0.41.3 URL reference and independently
require the helper to return the literal ASGI path. Every original corpus
entry, malformed-Host/decoded-path control, allow/deny decision, and router
`get_route_path` comparison remains. The corpus additionally includes
`//x/health` with no server. The frozen reference matched the installed old
framework in **all 34,185 original and extended W8 scopes**.

W20 replaces the real global helper with `request.url.path` only inside an
isolated test child. The test requires the existing W3 security check to reject
that mutation, and also requires W8 to reject it when current URL formatting
diverges from the scope. The complete corrected auth file passed on both
framework stacks:

| Environment | Complete auth harness result |
| --- | --- |
| Python 3.11.17, FastAPI 0.115.14, Starlette 0.41.3 | 14 passed, 6 existing optional skips, 29.49 seconds |
| Python 3.11.17, FastAPI 0.133.0, Starlette 1.3.1 | 14 passed, 6 existing optional skips, 30.43 seconds |

The final combined compatibility run completed with **335 passed, 10 existing
optional skips, zero failures in 95.67 seconds**. This covers the security and
library guards, mocked news pipeline, and the complete selected CI contract
group, including the corrected auth harness. The command, run with the
candidate environment's Python, was:

```sh
TFB_LIBSMOKE_REQUIRE_PINNED=1 python -m pytest -q -o faulthandler_timeout=30 \
  tests/test_security_pins_20261013.py \
  tests/test_security_pins_libsmoke_20261013.py \
  tests/test_news_intelligence_contracts.py \
  tests/test_schema_alignment.py \
  tests/test_current_schema_contract.py \
  tests/test_opportunity_blocked_invariant.py \
  tests/test_board_funding_route.py \
  tests/test_insights_cohort_route.py \
  tests/test_portfolio_reconciliation_route.py \
  tests/test_calendar_completeness.py \
  tests/test_calendar_partial_fill.py \
  tests/test_calendar_london_requests.py \
  tests/test_quote_epoch_producer.py \
  tests/test_redaction_boundaries.py \
  tests/test_route_error_redaction.py \
  tests/test_eodhd_error_redaction.py \
  tests/test_margin_unit_cache_contract.py \
  tests/test_analysis_fallback_publication.py \
  tests/test_analysis_sheet_rows_rank_withheld.py \
  tests/test_auth_wall_scope_path.py \
  tests/test_route_mount_compat.py
```

The default local sandbox cannot bind the loopback fixture and also stalls an
AnyIO test-client portal. The unchanged loopback/form tests passed 2/2 in 0.55
seconds with local socket permission, and the subsequent compatibility runs
used that permission. Fixtures remained offline/in-memory or local-loopback;
no production requests, auth source changes, financial gates, or test skips
were introduced to address that environment limitation.
