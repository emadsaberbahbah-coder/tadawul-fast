# News workflow health — 24 October 2026

Baseline: `561db60c74c1d56d866d4b72756f8ef9a9df57d5`.
Owner files: `.github/workflows/news_archive.yml` and
`scripts/news_archive_collector.py` (1.3.1 → 1.3.2).

## Actual failure classification

All five failed news runs in the bounded main-branch review window had a
successful selected news acquisition and accepted archive appends. Their
nonzero exit correctly disclosed incomplete adaptive identity discovery;
it was not a news-provider exception or a dependency-install failure.

| Run | New rows accepted | Existing rows skipped | Source read complete | Identity discovery complete |
| --- | ---: | ---: | --- | --- |
| [40066451707](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/40066451707) | 27 | 33 | true | false |
| [40035277798](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/40035277798) | 60 | 0 | true | false |
| [39999648493](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39999648493) | 31 | 29 | false | false |
| [39906549787](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39906549787) | 21 | 39 | true | false |
| [39870284821](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39870284821) | 60 | 0 | true | false |

Decoded failed-job logs retain `complete=false`, selected acquisition true,
`APPEND_ACCEPTED`, and OK/EMPTY per-symbol collection results. No failure
is relabelled as successful discovery. The latest artifact
`news-acquisition-40066451707` (ID `12549660925`) supplies the detailed source
read and identity evidence.

The latest green run before those failures was
[39831374656](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39831374656),
22 October at 21:26 Riyadh. Its decoded job log used collector 1.1.0,
fetched 16/16 static symbols and wrote 16 rows. The later adaptive runs
selected 60 symbols and checked source identity coverage. These are different
scopes; the red discovery result does not mean their selected feed stopped
working or that the old 16-symbol result certified full discovery.

## Repaired defect and diagnostics

The source-identity parser rejected the canonical Mexican ticker
`PE&OLES.MX` because its symbol grammar excluded ampersands. This was one
of the latest artifact's seven invalid Global_Markets rows but was omitted
from its six-name unknown-symbol example list. Both the
[Mexican exchange issuer record](https://www.bmv.com.mx/en/issuers/financialinformation/PE%26OLES-5608-CGEN_CAPIT)
and the [issuer's stock page](https://www3.penoles.com.mx/en/investors/stock.html)
identify the ticker as `PE&OLES`. The parser now permits one internal
ampersand in `.MX` identities, while retaining the exact symbol, name
requirement, namespace and length limit. Other venues, malformed delimiters,
empty company names and ticker aliases remain unverified.

The collector now records bounded safe `failure_codes` in JSON/stdout and
an Actions step summary. It distinguishes failed source reads, unknown
identities, selected-acquisition failure, capped priority selection,
response-validation failure and uncertain archive writes. It exposes no
portfolio-role identity examples in that summary. The existing exit-code
decision, append protocol, deduplication and data-quality gates are unchanged.

Artifact upload uses the current v7 action on the existing `always()` path.
The single shared archive-writer concurrency group remains uncancelled.

## Validation and remaining work

- News workflow health battery: 14 passed; baseline: 6 failed, 8 passed.
- Combined news collection, adaptive selection, lifecycle, contract and
  cancellation suites: 174 passed.
- Fatal Ruff checks and whitespace checks passed.
- Positive and adverse tests verify exact ampersand identity handling,
  unknown company names, non-disclosure, and incomplete-discovery exit 1
  even after successful acquisition/archive append. A subsequent complete
  discovery returns success without duplicating an existing daily row.

The latest source still has 17 unproven current identities. This revision
does not guess names or map renamed/terminated instruments into successors.
The archive workflow can therefore remain red until genuine source identity
and publication gaps are resolved. These changes improve parser correctness
and diagnostic actionability; they do not certify full-market news coverage,
prediction accuracy, investment eligibility or a new live collection.

No data replay, external write, workflow rerun, commit or deployment was
performed by the news repair worker. Source adoption and post-merge readback
must be recorded separately by the owning revision.
