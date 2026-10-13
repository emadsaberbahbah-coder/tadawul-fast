# TFB Commit Sheet - security pin bumps in requirements.txt (v1.1.3 -> v1.1.4)

Date: 2026-10-13. Base: origin/main dda1211 (requirements.txt v1.1.3, ci.yml v1.1.12).
Commits: fe557e1 (pin bumps, lean harness, first version of this sheet) and a review follow-up commit (library behaviour smoke harness, heavy-lane enrolment, comment-only requirements.txt notes, the rollout and read-back parts of this sheet). Both belong to this one PR, so the versions stay requirements.txt v1.1.4 and ci.yml v1.1.13.
Files: requirements.txt (v1.1.4), .github/workflows/ci.yml (v1.1.13), tests/test_security_pins_20261013.py (NEW, v1.0.0), tests/test_security_pins_libsmoke_20261013.py (NEW, v1.0.0), this sheet (NEW).
Not edited: scripts/verify_deployment.py, every production module, every workflow file other than ci.yml, every cron time and concurrency group, Render settings, every data provider. This change does not write to the live workbook itself. It does change the libraries that every workflow installing requirements.txt runs on, and several of those write to the live workbook, from their first run after merge (see "Where this lands").

## Why

The advisory dependency job (.github/workflows/dependency_typing_review.yml, job "dependencies") runs `pip-audit==2.9.0 --requirement requirements.txt --no-deps --disable-pip`. On main it reports 89 known advisories in 13 pinned packages. Production Render installs exactly these pins (the owner's 13 Oct build log), so every one of those advisories ships to production on every build. This change moves each affected pin to the minimal version that closes its reported advisories, provided that version resolves cleanly with every other pin on Python 3.11. Two packages are deliberately left where they are; the reasons are below.

## Evidence (before)

I re-ran the CI audit exactly, in a fresh Python 3.11 venv (`uv venv -p 3.11`, `uv pip install pip-audit==2.9.0`):

```
pip-audit --requirement requirements.txt --no-deps --disable-pip
Found 89 known vulnerabilities in 13 packages
```

Full table on base dda1211:

```
Name              Version ID                  Fix Versions
----------------- ------- ------------------- --------------------
starlette         0.41.3  PYSEC-2026-161      1.0.1
starlette         0.41.3  PYSEC-2026-249      1.3.1
starlette         0.41.3  PYSEC-2026-248      1.3.0
starlette         0.41.3  PYSEC-2026-1942     0.49.1
starlette         0.41.3  PYSEC-2026-1941     0.47.2
starlette         0.41.3  PYSEC-2026-2281     1.1.0
starlette         0.41.3  PYSEC-2026-2280     1.1.0
anyio             4.13.0  PYSEC-2026-4025     4.14.2
anyio             4.13.0  PYSEC-2026-4024     4.14.2
python-multipart  0.0.12  PYSEC-2026-1852     0.0.22
python-multipart  0.0.12  PYSEC-2026-1851     0.0.18
python-multipart  0.0.12  PYSEC-2026-3038     0.0.26
python-multipart  0.0.12  PYSEC-2026-3037     0.0.30
python-multipart  0.0.12  PYSEC-2026-3036     0.0.30
python-multipart  0.0.12  PYSEC-2026-3040     0.0.31
python-multipart  0.0.12  PYSEC-2026-3039     0.0.27
pydantic-settings 2.13.1  GHSA-4xgf-cpjx-pc3j 2.14.2
cryptography      43.0.3  PYSEC-2026-35       46.0.6
cryptography      43.0.3  PYSEC-2026-1284     44.0.1
cryptography      43.0.3  PYSEC-2026-2141     46.0.5
cryptography      43.0.3  PYSEC-2026-3553     49.0.0
cryptography      43.0.3  PYSEC-2026-3554     49.0.0
cryptography      43.0.3  GHSA-537c-gmf6-5ccf 48.0.1
pyopenssl         24.3.0  PYSEC-2026-2269     26.0.0
pyopenssl         24.3.0  PYSEC-2026-2268     26.0.0
h2                4.3.0   PYSEC-2026-3628     4.4.1
aiohttp           3.13.5  PYSEC-2026-237      3.14.1
aiohttp           3.13.5  PYSEC-2026-2104     3.14.0
aiohttp           3.13.5  PYSEC-2026-2112     3.14.1
aiohttp           3.13.5  PYSEC-2026-2111     3.14.1
aiohttp           3.13.5  PYSEC-2026-2113     3.14.1
aiohttp           3.13.5  PYSEC-2026-2109     3.14.1
aiohttp           3.13.5  PYSEC-2026-2110     3.14.1
aiohttp           3.13.5  PYSEC-2026-2108     3.14.1
aiohttp           3.13.5  PYSEC-2026-2105     3.14.0
aiohttp           3.13.5  PYSEC-2026-2106     3.14.0
aiohttp           3.13.5  PYSEC-2026-2107     3.14.1
aiohttp           3.13.5  PYSEC-2026-3545     3.14.3
aiohttp           3.13.5  PYSEC-2026-3546     3.14.2
aiohttp           3.13.5  PYSEC-2026-3547     3.14.2
protobuf          5.26.1  PYSEC-2026-1806     4.25.8,5.29.5,6.31.1
protobuf          5.26.1  PYSEC-2026-1805     5.29.6,6.33.5
python-dotenv     1.0.1   PYSEC-2026-2270     1.2.2
pyjwt             2.12.1  PYSEC-2026-179      2.13.0
pyjwt             2.12.1  PYSEC-2026-175      2.13.0
pyjwt             2.12.1  PYSEC-2026-177      2.13.0
pyjwt             2.12.1  PYSEC-2026-178      2.13.0
pyjwt             2.12.1  PYSEC-2026-4145     2.14.0
pyjwt             2.12.1  PYSEC-2026-4146
pyjwt             2.12.1  PYSEC-2026-4149     2.14.0
pyjwt             2.12.1  PYSEC-2026-4152     2.14.0
pyjwt             2.12.1  PYSEC-2026-4144     2.14.0
pyjwt             2.12.1  PYSEC-2026-4148     2.14.0
pyjwt             2.12.1  PYSEC-2026-4140     2.14.0
pyjwt             2.12.1  PYSEC-2026-4141     2.15.0
pyjwt             2.12.1  PYSEC-2026-4147     2.14.0
pyjwt             2.12.1  PYSEC-2026-4183     2.15.0
pyjwt             2.12.1  GHSA-jq35-7prp-9v3f 2.13.0
lxml              5.4.0   PYSEC-2026-87       6.1.0
nltk              3.9.4   PYSEC-2026-597
nltk              3.9.4   PYSEC-2026-3583     3.10.0
nltk              3.9.4   PYSEC-2026-3582     3.10.0
nltk              3.9.4   PYSEC-2026-3584     3.10.0
nltk              3.9.4   PYSEC-2026-3723     3.10.0
nltk              3.9.4   PYSEC-2026-3722     3.10.0
nltk              3.9.4   PYSEC-2026-3728     3.10.0
nltk              3.9.4   PYSEC-2026-3734     3.10.0
nltk              3.9.4   PYSEC-2026-3736     3.10.3
nltk              3.9.4   PYSEC-2026-3739     3.10.3
nltk              3.9.4   PYSEC-2026-3738     3.10.3
nltk              3.9.4   PYSEC-2026-3735     3.10.3
nltk              3.9.4   PYSEC-2026-3737     3.10.3
nltk              3.9.4   PYSEC-2026-3730     3.10.0
nltk              3.9.4   PYSEC-2026-3741     3.10.3
nltk              3.9.4   PYSEC-2026-3740     3.10.3
nltk              3.9.4   PYSEC-2026-3733     3.10.3
nltk              3.9.4   PYSEC-2026-3731     3.10.0
nltk              3.9.4   PYSEC-2026-3748     3.10.3
nltk              3.9.4   PYSEC-2026-3750     3.10.0
nltk              3.9.4   PYSEC-2026-3751     3.10.3
nltk              3.9.4   PYSEC-2026-3749     3.10.3
nltk              3.9.4   PYSEC-2026-3752     3.10.3
nltk              3.9.4   PYSEC-2026-3790     3.10.3
nltk              3.9.4   PYSEC-2026-3868     3.10.3
nltk              3.9.4   PYSEC-2026-3871     3.10.3
nltk              3.9.4   PYSEC-2026-3870     3.10.1
nltk              3.9.4   PYSEC-2026-3869     3.10.3
nltk              3.9.4   PYSEC-2026-3955
nltk              3.9.4   PYSEC-2026-3954     3.10.0
Name     Skip Reason
-------- ---------------------------------------------------------------
httpcore URL requirements cannot be pinned to a specific package version
```

Per package: nltk 30, pyjwt 15, aiohttp 14, starlette 7, python-multipart 7, cryptography 6, pyopenssl 2, protobuf 2, anyio 2, and 1 each for python-dotenv, pydantic-settings, lxml and h2 (89 in total).

Why httpcore is skipped: since v1.1.3, httpcore is pinned by direct wheel URL (`httpcore @ https://files.pythonhosted.org/.../httpcore-1.0.9-py3-none-any.whl`) to work around a Render CDN index-page outage. With `--no-deps`, pip-audit cannot map a URL requirement to a version, so it skips the line. I audited that version on its own (`echo httpcore==1.0.9 > f.txt; pip-audit -r f.txt --no-deps --disable-pip`): "No known vulnerabilities found". 1.0.9 is also the newest httpcore release, so nothing needs to move. The pin is unchanged.

## What changed

requirements.txt v1.1.3 -> v1.1.4 (+59/-14 lines; the 14 removed lines are the 12 replaced pin lines, the replaced version heading, and the old "WHY this revision" label, which now reads "WHY v1.1.3 (2026-06-11)"). Twelve pins move:

- aiohttp 3.13.5 -> 3.14.3
- anyio 4.13.0 -> 4.14.2
- cryptography 43.0.3 -> 50.0.0
- h2 4.3.0 -> 4.4.1
- lxml 5.4.0 -> 6.1.0
- nltk 3.9.4 -> 3.10.3 (now pulls in defusedxml 0.7.1, a pure-Python wheel; this is the only change in the resolved transitive set)
- protobuf 5.26.1 -> 5.29.6 (stays on 5.x)
- pydantic-settings 2.13.1 -> 2.14.2
- PyJWT 2.12.1 -> 2.15.0
- pyOpenSSL 24.3.0 -> 26.4.0
- python-dotenv 1.0.1 -> 1.2.2
- python-multipart 0.0.12 -> 0.0.31

The comment style is kept. The header gains a dated "WHY v1.1.4 (2026-10-13)" block, and short "v1.1.4:" notes sit next to the starlette, cryptography/pyOpenSSL, httpcore and protobuf lines. Every other pin is carried forward verbatim.

Two pins differ from the fix versions in the brief, on purpose:

1. cryptography goes to 50.0.0, not 49.0.0. Auditing the 49.0.0 candidate returned "Found 9 known vulnerabilities in 3 packages", including `cryptography 49.0.0 PYSEC-2026-3552 50.0.0` (CVE-2026-69247, GHSA-g6cj-pr64-35w5: a PKCS7 decrypt Bleichenbacher oracle, "Introduced in 44.0.0. Fixed in 50.0.0."). 43.0.3 did not carry it, so pinning 49.0.0 would have closed three advisories and opened a new one. 50.0.0 is the minimal version with no known advisory (`pip-audit` on `cryptography==50.0.0`: "No known vulnerabilities found"). It was uploaded 2026-07-31.
2. pyOpenSSL goes to 26.4.0, not 26.0.0. The minimal fix, 26.0.0, declares `cryptography<47,>=46.0.0`, which cannot coexist with the cryptography fix. uv on Python 3.11: "Because pyopenssl==26.0.0 depends on cryptography>=46.0.0,<47 and you require cryptography==50.0.0, we can conclude that your requirements and pyopenssl==26.0.0 are incompatible." The cryptography ranges published on PyPI for pyOpenSSL are: 26.1.0 needs <48, 26.2.0 needs <49, 26.3.0 needs >=49,<50, and 26.4.0 needs >=49,<51. So 26.4.0 (uploaded 2026-08-01) is the minimal pyOpenSSL that accepts cryptography 50. It audits clean. Nothing in the repo imports OpenSSL; it is a reserved pin.

.github/workflows/ci.yml v1.1.12 -> v1.1.13 (+10/-2):

- A new VERSION heading, plus a dated v1.1.13 WHY paragraph.
- tests/test_security_pins_20261013.py is added to the blocking lean-unit list, right after tests/test_verify_manifest_pins.py.
- The contract job's `"anyio==4.13.0"` becomes `"anyio==4.14.2"`. That job's own v1.0.1 rule is that contract dependencies match the production pins in requirements.txt exactly; leaving 4.13.0 there would test a framework stack Render no longer runs.

Nothing is removed from ci.yml. Every other job, step, pin and verdict is byte-identical.

tests/test_security_pins_20261013.py (new, 270 lines, stdlib only) runs as a script ("PASS k/k") and under pytest:

- S1: each of the 12 bumped packages is pinned exactly once with == and is at or above its minimal fixed version. The floors are the audit fix versions; cryptography's floor is 50.0.0, pyOpenSSL's is 26.0.0.
- S2: protobuf stays on 5.x.
- S3: pyOpenSSL and cryptography form a compatible pair, checked against a recorded table of published ranges. It skips, rather than fails, on a pyOpenSSL line it has no window for, because pip in the heavy lane still refuses an incompatible pair.
- S4: while fastapi is 0.115.x, starlette stays below 0.47.0.
- S5: every "pkg==ver" in ci.yml's contract job equals requirements.txt.
- S6: the httpcore URL pin still names the version audited on its own (at or above 1.0.9).
- S7: the requirements header carries the dated WHY.
- G1 (golden negative): with TFB_SECPIN_BASE pointing at the old requirements.txt, it evaluates the S1 floors against that file and FAILS. When the variable is unset it SKIPS.

Review follow-up commit (same PR, same versions):

- tests/test_security_pins_libsmoke_20261013.py (new, v1.0.0). It imports only the standard library at module level; each check imports its library inside the test and SKIPS when it is absent. L1-L15, I1 and G1 are listed in its docstring and measured in point 6 below.
- .github/workflows/ci.yml (+11/-0 over fe557e1): the heavy job gains one step, "Security pin library behaviour smoke (pinned production stack)", which runs the new harness with TFB_LIBSMOKE_REQUIRE_PINNED=1. The v1.1.13 WHY paragraph gains a sentence for it. Nothing is removed; every other step, pin and verdict is byte-identical, and the heavy job still has no continue-on-error (tests/test_ci_required_verdict.py passes).
- requirements.txt (+13/-3 over fe557e1, comment lines only): the header's Guard and NOT-default-off notes now name the new harness and the workflows that install this file, and a 3-line v1.1.4 note above `h2==4.4.1` records the ENABLE_PUSH behaviour change. The three removed lines are this PR's own fe557e1 header lines, rewritten. No pin moves.

## Default OFF identity

This change is NOT default-off and cannot be gated. A dependency pin cannot be read from an ENV flag or Script Property: Render runs `pip install -r requirements.txt` at build time, before any application code reads configuration. It takes effect in two places, independently of each other: the next Render build after merge, and the first run after merge of every GitHub Actions workflow that installs requirements.txt from main (next section). The proofs offered instead of byte-identity are below: the same whole-suite results, the same app surface, and the same library behaviour on both stacks in the committed smoke harness, with one disclosed exception: h2 4.4 rejects a server SETTINGS frame carrying ENABLE_PUSH=1, which h2 4.3.0 accepted (Known limits). The other production-visible differences are the library versions themselves and the new transitive package defusedxml 0.7.1. The ci.yml changes affect CI only.

## Where this lands

Render is not the only consumer of these pins. Each workflow below runs `pip install -r requirements.txt` from the checked-out main on every run. It therefore adopts the new pins at its first run after merge, whether or not Render has rebuilt, and a rollback by revert also reaches it at its next run. Times are UTC (Riyadh is UTC+3). Listing command, run on this branch: `for f in .github/workflows/*.yml; do grep -n 'install -r requirements.txt' "$f"; grep -n 'cron:' "$f"; done`.

Scheduled:

- daily_sync.yml ("Tadawul Dashboard Advanced Sync"): cron `17 4,12,20 * * *`; installs at lines 160, 702, 1823 and 2031. It runs scripts/run_dashboard_sync.py (httpx calls to the Render backend; Google Sheets writes through google-auth, which signs with cryptography), validate_dashboard.py, track_performance.py, inline page recovery and finalize_decision_publication.py. It writes the live workbook. The provider fetches the backend performs (eodhd, finnhub, tadawul, all httpx with http2=True) run on Render's libraries, so those change with the Render build rather than with this job.
- track_performance.yml: cron `47 11 * * *` and `47 15 * * *`; installs at line 183. It runs scripts/track_performance.py, including the aiohttp.ClientSession paths at :3289 (Yahoo price fallback) and :5291 (backend POST). It writes the tracker tabs.
- calendar_sync.yml: cron `15 5 * * *`; installs at line 75. Scheduled runs always pass --write. Path: scripts/run_calendar_sync.py -> core/providers/calendar_provider.py:502 `Ticker.get_earnings_dates` -> yfinance 0.2.66 base.py:808 `pd.read_html`, which parses with lxml (6.1.0 after merge). It replaces the Calendar_Events tab.
- news_archive.yml: cron `0 14 * * *`; installs at line 79. scripts/news_archive_collector.py appends to News_Archive (gspread, google-auth).
- daily_brief.yml: cron `30 7`, `30 10`, `0 15` and `0 19` on days 0-4 (Sunday to Thursday); installs at line 92. It reads the workbook only.
- backtest_harness.yml: cron `0 13 * * 0` (Sunday); installs at line 79. track_performance.py --register-hypotheses --backtest; it writes Hypothesis_Registry.
- investment_policy_shadow.yml: cron `15 2 * * *`; installs at line 40. No workbook reference.
- provider_target_coverage.yml: cron `20 6 * * *`; installs at line 80. Read-only audit.

Triggered by another run (workflow_run when Advanced Sync completes): decision_surface_freshness.yml (line 134) and full_refresh_coverage.yml (line 142).

Manual or path-triggered only: page_refresh_recovery.yml (line 243; it runs run_dashboard_sync.py and can write) and workflow_repair_tests.yml (line 111).

CI only: ci.yml whole-suite (line 324) and heavy (line 656), line numbers as of this branch.

Not affected: board_scorecard.yml (cron `23 22 * * 1-5`) and workbook_backup.yml (cron `30 23 * * *`) install their own pins (gspread 6.2.1 and google-auth 2.49.2, plus requests 2.33.1 and openpyxl 3.1.5 for the backup) and leave cryptography unpinned, so pip picks the newest cryptography on every run. `uv pip compile -p 3.11 --python-platform x86_64-manylinux_2_28` of the backup's pins resolves cryptography 50.0.2 today. The Sheets service-account signing path therefore already runs nightly on a cryptography 50.x release, independently of this change.

## Measured results

Environment: CPython 3.11.17 (/usr/bin/python3.11; Render's runtime.txt says python-3.11.9, the same minor version), uv 0.11.32, Node v22.22.0. Separate fresh venvs throughout: OLD = base requirements.txt; NEW = this requirements.txt. Both whole-suite venvs also get pytest 9.1.1, openpyxl 3.1.5, reportlab 5.0.1 and pypdf 6.20.0, exactly as ci.yml's whole-suite lane installs them.

What the whole suite proves, and what it does not. On base, no file under tests/ imports any of the twelve bumped libraries directly, and none drives httpx with http2=True. Command: `for lib in aiohttp anyio cryptography lxml jwt OpenSSL google.protobuf h2 dotenv pydantic_settings python_multipart multipart nltk; do grep -lE "^\s*(import|from)\s+${lib//./\\.}\b" tests/*.py | wc -l; done` prints 0 for each, and `grep -lE 'http2\s*=\s*True' tests/*.py | wc -l` prints 0. Whole-suite parity (point 4) therefore guards the application logic on the new stack, not library behaviour. Library behaviour is covered by the committed harness tests/test_security_pins_libsmoke_20261013.py (point 6), which replaces the scratch-only smoke scripts the first version of this sheet described in prose.

1. Resolver, no conflict. Each command was run once with the old file and once with the new:
   - `uv pip compile -p 3.11 --python-platform x86_64-manylinux_2_28 requirements.txt`: exit 0 for both.
   - `uv pip install -r requirements.txt pytest openpyxl==3.1.5 reportlab==5.0.1 pypdf==6.20.0`: exit 0 for both.
   - `python -m pip check`: "No broken requirements found." for both.
   - pip's own resolver (Render uses pip): `pip install --dry-run --ignore-installed --report r.json -r requirements.txt` exits 0 and resolves 134 packages, with cryptography 50.0.0 and pyopenssl 26.4.0.
   - The `pip freeze` diff between OLD and NEW is exactly the 12 bumps plus the new defusedxml 0.7.1. Nothing else moved.
2. The constraints the brief named, read from installed metadata:
   - grpcio-status 1.64.3: "protobuf <6.0dev,>=5.26.1". opentelemetry-proto 1.28.0: "protobuf<6.0,>=5.0". proto-plus 1.24.0: "<6.0.0dev". googleapis-common-protos 1.64.0: "<6.0.0.dev0". All are satisfied by 5.29.6.
   - google-auth 2.49.2: "cryptography>=38.0.3" (pyopenssl only via extras).
   - fastapi 0.115.14: "starlette<0.47.0,>=0.40.0" and "python-multipart>=0.0.18" (extra).
   - starlette 0.41.3: "anyio<5,>=3.4.0".
   - pydantic-settings 2.14.2: "pydantic>=2.7.0" and "python-dotenv>=0.21.0".
   - beautifulsoup4 4.14.3: lxml only as an extra, with no bound. yfinance 0.2.66 does not depend on lxml.
   - redis 5.3.1: "PyJWT>=2.9.0".
3. Wheels for Render: every bumped pin has a cp311 manylinux2014 (glibc 2.17+) wheel, so no source build is needed on any current image. Command: `pip download --no-deps --only-binary=:all: --platform manylinux2014_x86_64 --python-version 3.11 --implementation cp --abi cp311 --abi abi3 --abi none -r bumped12.txt`, exit 0, with 12 wheels including `cryptography-50.0.0-cp311-abi3-manylinux2014_x86_64.manylinux_2_17_x86_64.whl` and `lxml-6.1.0-cp311-cp311-manylinux2014_x86_64.manylinux_2_17_x86_64.whl`.
4. Whole suite, `python -m pytest -q tests`. Zero new failures:
   - OLD requirements, base tree: `4768 passed, 9 skipped, 3 warnings, 212 subtests passed in 317.07s`, exit 0.
   - NEW requirements, base tree (isolates the library change): `4768 passed, 9 skipped, 3 warnings, 212 subtests passed in 319.04s`, exit 0. Identical counts; the 3 warnings are the same PytestReturnNotNoneWarning lines.
   - NEW requirements, commit tree (adds the harness): `4775 passed, 10 skipped, 3 warnings, 212 subtests passed in 310.50s`, exit 0. That is +7 passed and +1 skipped, which is exactly the new harness with G1 skipping.
   - NEW requirements, follow-up tree (adds the library smoke harness): `4791 passed, 11 skipped, 3 warnings, 212 subtests passed in 320.63s`, exit 0. That is +16 passed and +1 skipped over the previous line: L1-L15 and I1 pass, G1 skips. The 3 warnings are the same three PytestReturnNotNoneWarning lines (test_sync_fetchfail_truth_p162.py t3/t6, test_sync_klg_fetchfail_p162b.py t8).
5. ci.yml lanes, executed as written: each job's run-blocks were extracted from ci.yml with PyYAML and run with bash -e, skipping only the steps named Install (runner source and command in Appendix B; the follow-up runs use that version, which also applies each step's own `env:`).
   - lean-unit (numpy 1.26.4, pytest 9.1.1, PyYAML 6.0.3 only):
     - Base tree: decision-layer step `3431 passed, 10 skipped, 2 warnings, 131 subtests passed`.
     - Commit tree: `3438 passed, 11 skipped, 2 warnings, 131 subtests passed` (+7/+1 = the harness).
     - The other lean steps are identical on both trees: 275 passed, 99 passed, 14+14 passed, the node suites (64 board funding, 73 source readiness, 16 reconciliation inputs, 25/25 calendar, 20 passed / 3 skipped input truth), and scripts/test_maturation.py.
     - Overall RC 0 on both.
     - Follow-up tree: identical to the commit tree (decision-layer `3438 passed, 11 skipped, 2 warnings, 131 subtests passed`; every other lean step unchanged); overall RC 0. The library smoke harness is not in the lean lane; in a lean-only venv it gives `PASS 0/0  (skipped 17)` and pytest `17 skipped`.
   - contract:
     - Base pins (anyio 4.13.0): `239 passed, 2 skipped`.
     - New pins (anyio 4.14.2): `239 passed, 2 skipped`, RC 0 (commit tree and follow-up tree).
   - heavy (NEW requirements): 39, 116, 43, 74, 12 passed + 1 skipped, 77, 95, 196 and 1 passed; overall RC 0. Follow-up tree: the same nine steps with the same counts, plus the new step "Security pin library behaviour smoke (pinned production stack)" (TFB_LIBSMOKE_REQUIRE_PINNED=1): `16 passed, 1 skipped`; overall RC 0.
   - compile:
     - `python -m compileall -q main.py core routes scripts integrations`: exit 0.
     - `python -m compileall -q tests`: exit 0.
     - `ruff==0.16.8 check --select E9,F63,F7,F82 --output-format concise .`: "All checks passed!".
     - All three re-run on the follow-up tree with the same results.
   - tests/test_verify_manifest_pins.py and tests/test_ci_required_verdict.py pass inside the lean step.
6. Library behaviour, committed as tests/test_security_pins_libsmoke_20261013.py (v1.0.0; it replaces the scratch scripts lib_smoke.py, import_smoke.py, gauth_smoke.py and form_smoke.py that the first version of this sheet described in prose). Each check drives a bumped library in the call shape production uses, offline: L10 and L11 feed scripted HTTP/2 frames through httpx.AsyncHTTPTransport(http2=True) -> httpcore -> h2 with no socket, L12 uses a 127.0.0.1 server on an ephemeral port, and L15 imports main:app in a child process with every outbound connect and getaddrinfo replaced by a raiser. The docstring lists L1-L15, I1 and G1. Results from the same follow-up tree on two fresh venvs:
   - NEW: `TFB_LIBSMOKE_REQUIRE_PINNED=1 python tests/test_security_pins_libsmoke_20261013.py` gives `PASS 16/16  (skipped 1)`, exit 0 (G1 skips; I1 prints `missing=0 differ=0 strict=True`).
   - OLD: `python tests/test_security_pins_libsmoke_20261013.py` gives `PASS 15/15  (skipped 2)`, exit 0. I1 skips with "installed stack is not requirements.txt (missing=0 differ=12)", and G1 skips. Every L check passes on both stacks.
   - pytest: NEW with TFB_LIBSMOKE_REQUIRE_PINNED=1 `16 passed, 1 skipped`; OLD `15 passed, 2 skipped`; the shared /home/user/tfb-venv (an old-stack venv) script `PASS 15/15  (skipped 2)`.
   - Printed lines, NEW then OLD:

```
L1 cryptography 50.0.0 Fernet ok
L2 google-auth 2.49.2 cryptography 50.0.0 signing ok
L3 pyOpenSSL 26.4.0 context ok
L4 PyJWT 2.15.0 HS256 ok
L5 protobuf 5.29.6 ok; consumers imported 6/6
L6 lxml 6.1.0 ok: lxml, bs4, pandas.read_html
L7 nltk 3.10.3 ok
L8 python-dotenv 1.2.2 ok
L9 pydantic-settings 2.14.2 ok
L10 h2 4.4.1 httpx 0.27.2 httpcore 1.0.9: 200 HTTP/2 40000 bytes
L11 h2 4.4.1 server ENABLE_PUSH=1 -> LocalProtocolError: Setting 2 has invalid value 1
L12 aiohttp 3.14.3 POST json + GET chunked gzip ok
L13 anyio 4.14.2 ok
L14 python-multipart 0.0.31 starlette 0.41.3 form+upload ok
L15 main:app {"app_type": "FastAPI", "blocked": 0, "openapi_paths": 50, "routes": 100}; Routes mounted at app creation: mounted=6 duplicate_skips=0 partial_duplicate_skips=0 missing=0 failed=0 strategy=main.controlled_priority_plan route_signatures=108

L1 cryptography 43.0.3 Fernet ok
L2 google-auth 2.49.2 cryptography 43.0.3 signing ok
L3 pyOpenSSL 24.3.0 context ok
L4 PyJWT 2.12.1 HS256 ok
L5 protobuf 5.26.1 ok; consumers imported 6/6
L6 lxml 5.4.0 ok: lxml, bs4, pandas.read_html
L7 nltk 3.9.4 ok
L8 python-dotenv 1.0.1 ok
L9 pydantic-settings 2.13.1 ok
L10 h2 4.3.0 httpx 0.27.2 httpcore 1.0.9: 200 HTTP/2 40000 bytes
L11 h2 4.3.0 server ENABLE_PUSH=1 -> accepted 200
L12 aiohttp 3.13.5 POST json + GET chunked gzip ok
L13 anyio 4.13.0 ok
L14 python-multipart 0.0.12 starlette 0.41.3 form+upload ok
L15 main:app {"app_type": "FastAPI", "blocked": 0, "openapi_paths": 50, "routes": 100}; Routes mounted at app creation: mounted=6 duplicate_skips=0 partial_duplicate_skips=0 missing=0 failed=0 strategy=main.controlled_priority_plan route_signatures=108
```

   - Golden negative: `TFB_LIBSMOKE_BASE_PYTHON=<OLD venv>/bin/python python tests/test_security_pins_libsmoke_20261013.py` (run with the NEW venv) prints `G1 base=... violations=12`, one line per package (for example "cryptography 43.0.3 installed, below its fixed version 50.0.0"), then `FAIL test_g1_golden_negative_base_stack_fails_the_floors` and `PASS 16/17`, exit 1. pytest with the same variable: `1 failed, 16 passed`. Positive control with the NEW venv's own python as the "base": `violations=0`, `PASS 17/17`. With TFB_LIBSMOKE_REQUIRE_PINNED=1 on the OLD venv, I1 fails and lists the 12 "installed X, requirements.txt Y" lines (`PASS 15/16  (skipped 1)`, exit 1).
   - L14 notes: python-multipart 0.0.12 ships only the legacy `multipart` module and 0.0.31 ships `python_multipart`; starlette 0.41.3 imports `python_multipart` first, so the deprecated shim is not loaded on NEW. The app itself has no Form/File/UploadFile route.
   - The first version of this sheet also reported a pyOpenSSL DeprecationWarning for PKey.generate_key on NEW. That call was in the scratch smoke only; the repo never calls it, and L3 does not either.
7. h2 ENABLE_PUSH (review finding, re-verified). Source diff 4.3.0 -> 4.4.1 (h2/settings.py): new `Settings.validate_received_setting`, and `_validate_setting(..., client=...)` now returns PROTOCOL_ERROR when `client is False and value != 0` for ENABLE_PUSH, quoting RFC 9113 section 6.5.2 ("A client MUST treat receipt of a SETTINGS frame with SETTINGS_ENABLE_PUSH set to 1 as a connection error"). L11 above shows the split in memory. A loopback TLS reproduction of the review's experiment (h2 server side, ALPN h2, httpx.AsyncClient(http2=True), 6 concurrent GETs with 300 KB bodies; source in Appendix C; command `python -I h2_push_tls.py` in each venv) printed:

```
h2 4.3.0 httpx 0.27.2 httpcore 1.0.9
server ENABLE_PUSH=0 -> 6/6 ok []
server ENABLE_PUSH=1 -> 6/6 ok []
h2 4.4.1 httpx 0.27.2 httpcore 1.0.9
server ENABLE_PUSH=0 -> 6/6 ok []
server ENABLE_PUSH=1 -> 0/6 ok ['LocalProtocolError: Invalid input ConnectionInputs.SEND_HEADERS in state ConnectionState.CLOSED', 'LocalProtocolError: Setting 2 has invalid value 1']
```

   ("ENABLE_PUSH=0" there means the server did not send the setting at all.) See Known limits and operator step 3.
8. Resolution unchanged by the follow-up: its requirements.txt edits are comment lines only. The non-comment lines equal fe557e1's, and `uv pip compile -p 3.11 --python-platform x86_64-manylinux_2_28 --no-header requirements.txt` resolves the same 134-line set as before.
9. Starlette Host-header probe (pre-existing, not changed here): source and command in Appendix A. Output, identical on OLD and NEW:

```
starlette 0.41.3
plain /secret          -> 401
crafted Host /secret   -> 200
```

10. Audit after, same command as CI:

```
Found 8 known vulnerabilities in 2 packages
Name      Version ID                  Fix Versions
--------- ------- ------------------- ------------
starlette 0.41.3  PYSEC-2026-161      1.0.1
starlette 0.41.3  PYSEC-2026-249      1.3.1
starlette 0.41.3  PYSEC-2026-248      1.3.0
starlette 0.41.3  PYSEC-2026-1942     0.49.1
starlette 0.41.3  PYSEC-2026-1941     0.47.2
starlette 0.41.3  PYSEC-2026-2281     1.1.0
starlette 0.41.3  PYSEC-2026-2280     1.1.0
nltk      3.10.3  GHSA-8mgp-746c-j5xp
Name     Skip Reason
-------- ---------------------------------------------------------------
httpcore URL requirements cannot be pinned to a specific package version
```

   Golden negative (counts): 89 advisories in 13 packages before, 8 in 2 packages after. As a cross-check I also audited the fully resolved transitive lock: `uv pip compile` output with httpcore written as ==1.0.9, 134 pins. It gives 89 before and 8 after, with the same rows, so no unpinned transitive dependency carries a known advisory today.

11. Harness:
    - `python tests/test_security_pins_20261013.py`: `PASS 7/7  (skipped 1)`, exit 0.
    - `python -m pytest -q tests/test_security_pins_20261013.py`: `7 passed, 1 skipped`.
    - With `TFB_SECPIN_BASE=<copy of base requirements.txt>`, the script prints `G1 base=... violations=12`, one line per package (for example "cryptography==43.0.3 is below its fixed version 50.0.0"), then `FAIL test_g1_golden_negative_base_requirements_fail_the_floors` and `PASS 7/8`, exit 1.
    - With the same variable, pytest gives `1 failed, 7 passed`.
    - Re-run on the follow-up tree: the same four results. The base copy used for TFB_SECPIN_BASE is byte-identical to `git show origin/main:requirements.txt` (cmp exit 0).

## Exclusions and advisories left open

- starlette 0.41.3, 7 advisories open: PYSEC-2026-161, -249, -248, -1942, -1941, -2281 and -2280. Every published fix (0.47.2, 0.49.1, 1.0.1, 1.1.0, 1.3.0, 1.3.1) is outside fastapi 0.115.14's declared "starlette<0.47.0". Fixing any of them means a FastAPI framework upgrade, which is a major API move beyond starlette itself. That makes it an owner decision and a separate item. Feature exposure, from a grep of main.py, core, routes, integrations and scripts:
  - No StaticFiles, FileResponse, HTTPEndpoint, request.form() or Form/File/UploadFile routes. That covers -1942, -1941, -2281, -2280 and -249.
  - main.py does read request.url.path, including at main.py:1351, where GlobalAuthEnforcementMiddleware decides whether a request is exempt from the global token wall. That is the class described by PYSEC-2026-161 (Host header reconstruction) and PYSEC-2026-248.
  - Offline probe on starlette 0.41.3, run identically on the OLD and NEW stacks (pre-existing, not changed here): a BaseHTTPMiddleware with the same decision shape (exempt if request.url.path is "/health") returns 401 for GET /secret, but 200 for GET /secret with header `Host: testserver/health?x=`. request.url.path reads "/health" while routing uses the real path. Source and command: Appendix A.
  - Mitigating factors: the wall is active only when REQUIRE_AUTH resolves true, OPEN_MODE is off and TFB_GLOBAL_AUTH_ENFORCE is not falsy. Per-route auth in the route modules is unaffected. Whether Render's edge forwards such a Host header to the service was not tested.
  - See Owner decisions.
- nltk GHSA-8mgp-746c-j5xp (CVE-2026-81726, alias PYSEC-2026-3740), 1 advisory open. The PYSEC record lists 3.10.3 as the fix, but the GHSA record says "Patched versions: Not yet patched", and 3.10.3 is the newest nltk on PyPI, so pip-audit still matches it. The affected APIs (TransitionParser, AveragedPerceptron and PerceptronTagger save/load with attacker-chosen paths) are not used. nltk is a reserved pin whose only use is an existence probe (core/news_intelligence.py HAS_NLTK).
- httpcore: not bumped. It is skipped by the audit as a URL requirement, and 1.0.9 audits clean on its own.
- Nothing else was excluded. Every other advisory row on base is closed.

## Known limits

- Not default-off (see above). Libraries change at the next Render build and, independently, at each workflow's first run after merge (see "Where this lands"). Several of those workflows write to the live workbook.
- All measurements are offline; the only sockets are 127.0.0.1 loopback (L12 and the Appendix C reproduction). Live provider HTTP, live Google Sheets/Secret Manager calls, gRPC/OTLP export and Redis were not exercised against real endpoints. The checks cover imports, construction, local signing, in-memory HTTP/2 exchanges, a loopback aiohttp exchange, a loopback TLS HTTP/2 exchange and the full offline suite.
- h2 4.4 behaviour change, disclosed. A client on h2 4.4.x treats a server SETTINGS frame with ENABLE_PUSH=1 as a fatal protocol error (RFC 9113 section 6.5.2); h2 4.3.0 accepted it. All three live providers build `httpx.AsyncClient(..., http2=True)`: core/providers/eodhd_provider.py:2296, finnhub_provider.py:777 and tadawul_provider.py:1083. Against a server that negotiates h2 by ALPN and sends that value, every request on the connection now fails with httpx.LocalProtocolError "Setting 2 has invalid value 1". The providers catch it as httpx.RequestError and record `network_error:LocalProtocolError` (eodhd, finnhub; finnhub also counts a circuit-breaker failure) or `network_error_LocalProtocolError` (tadawul), then retry and fail. Measured: point 6 L11 (in memory) and point 7 (loopback TLS, 6/6 on h2 4.3.0 vs 0/6 on h2 4.4.1). RFC-compliant servers do not send this value, and servers that do not negotiate h2 are unaffected, but whether the eodhd, finnhub or tadawul endpoints send it cannot be checked offline. No code change is made here; operator step 3 reads it back and owner decision 7 covers the fallback. Keeping h2 at 4.3.0 instead would leave PYSEC-2026-3628 open.
- The library smoke harness checks the call shapes the code uses; it does not replay every production call. It guards against a library breaking those shapes and does not replace the read-backs in Operator steps.
- Measured on CPython 3.11.17; Render pins 3.11.9. Every bumped package ships cp311/abi3/py3 manylinux2014 wheels (point 3), so the patch-level difference is not expected to matter.
- Transitive dependencies remain unpinned and resolve at Render build time, as before. Today's resolution carries no known advisory.
- The advisory database moves. A re-run on a later date can report new rows. The audit job stays advisory (continue-on-error).
- pyOpenSSL 26.x deprecates key generation helpers (DeprecationWarning). The repo does not call them.
- This branch is independent of the stacked PRs. Each of them also rewrites ci.yml's single VERSION heading line (main is v1.1.12; this branch writes v1.1.13), so a trivial header conflict is expected on whichever lands second. Resolve it by keeping both dated paragraphs and taking the next free version number. The lean-list insertion point (after tests/test_verify_manifest_pins.py) and the contract anyio line are unlikely to collide.

## Operator steps

1. Review and merge. There are no ENV, Script Property, cron or Render settings to change. Suggested timing (owner decision 6): merge shortly after the 04:17 UTC Advanced Sync slot has finished (07:17 Riyadh). The first runs on the new libraries then fall in Riyadh working hours: calendar_sync 05:15, provider_target_coverage 06:20, daily_brief 07:30 and 10:30 (Sunday to Thursday), track_performance 11:47, and Advanced Sync 12:17 (all UTC), which can be watched live.
2. Render. The next build installs the new pins. Read-back in the build log: "Successfully installed ... cryptography-50.0.0 ... pyOpenSSL-26.4.0 ... protobuf-5.29.6 ... python-multipart-0.0.31 ..." with no "Building wheel for" line for any of the 12 packages. After deploy, the startup log shows the same "Routes mounted at app creation: mounted=6 ... failed=0" line, and /health answers as before.
3. Workflows, at the first run of each after merge (Actions tab). For every workflow listed in "Where this lands", the dependency install step's "Successfully installed" line shows the new versions (for example cryptography-50.0.0, h2-4.4.1, lxml-6.1.0, aiohttp-3.14.3), and the job ends green. Then, per workflow:
   - Advanced Sync (daily_sync.yml): every leg is green, and the per-page written and failed counts in the run summary are in line with the same slot the day before. Search the job logs, and the Render service log for the hours since the Render build, for "LocalProtocolError" and "has invalid value". Expect zero hits. A hit names the provider, through its `network_error:LocalProtocolError` or `network_error_LocalProtocolError` reason; in that case see step 5. Also compare the number of "network_error" reasons with the previous day's same slot.
   - track_performance.yml: the "[v6.21.0 PRICE-FALLBACK] priced N/M symbol(s) ... stats={...}" line has a priced share and ok/429/403/timeout/other counters in line with the previous day, with no new "pass error:" warning (this is the aiohttp path).
   - calendar_sync.yml: "[calendar_sync v...] rows: N | with at least one date: M" and "wrote N rows -> Calendar_Events", with N and M in line with the previous day, and no "WARN: provider failed" line (the earnings tier parses with pandas.read_html on lxml 6.1.0).
   - news_archive.yml: green, with rows appended to News_Archive in line with the previous day.
   - backtest_harness.yml (Sunday 13:00 UTC): green, with Hypothesis_Registry written as before.
   - daily_brief.yml, investment_policy_shadow.yml, provider_target_coverage.yml, and the two workflow_run audits (decision_surface_freshness.yml, full_refresh_coverage.yml): green, with the same verdicts as the previous day.
4. Optional: run the "Dependency and typed-module review (advisory)" workflow by hand. Its dependencies job should report "Found 8 known vulnerabilities in 2 packages".
5. If one provider fails on HTTP/2 only (LocalProtocolError hits naming it), nothing in this PR needs to change in place. The narrow fix is http2=False for that provider, as its own versioned item (owner decision 7). The broad fix is the rollback in step 6.
6. Rollback: revert this PR's commits (`git revert <follow-up sha> fe557e1`, or revert the PR's merge or squash commit). Every pin returns to v1.1.3 verbatim, and ci.yml returns to v1.1.12 (contract anyio 4.13.0, neither harness enrolled). The rollback reaches each consumer at its next install: Render at its next build, and each workflow in "Where this lands" at its next run, because each one installs from requirements.txt on every run. The setup-python pip caches keyed on requirements.txt (investment_policy_shadow.yml, workflow_repair_tests.yml) cache downloads only; the pins still decide what is installed. No data, workbook or configuration state depends on these versions. Rows that workflows wrote while on the new libraries stay as written.

## Owner decisions

1. starlette/FastAPI: decide whether to schedule a FastAPI upgrade to a release that accepts starlette 1.3.1 or later, which closes all 7 remaining starlette advisories. The ci.yml framework-drift lane already observes latest FastAPI/Starlette on Python 3.13. Independently of that upgrade, consider a small main.py hardening, as its own versioned item with a verify_deployment pin bump: decide the global-auth exemption on request.scope["path"] (the path starlette routes on) rather than request.url.path. The offline probe above shows the two can disagree on 0.41.3.
2. nltk: it is reserved and unused (existence probe only). Decide whether to keep tracking the unpatched GHSA or drop the pin in a later cleanup. This change removes nothing.
3. httpcore URL pin: the v1.1.3 header says both forms are stable. Restoring "httpcore==1.0.9" would let the CI audit see it directly. Keep the URL form only if the Render CDN index issue still matters.
4. pyOpenSSL is a reserved pin that nothing imports. Decide whether to keep it, given that it couples the cryptography version (any future cryptography security bump has to move pyOpenSSL with it).
5. Whether to promote the dependency audit from advisory to blocking once the starlette item is settled.
6. Merge timing. The suggested window is in operator step 1. Any other time works if someone can watch the first Advanced Sync, track_performance and calendar_sync runs after merge.
7. h2 fallback. If a provider is seen failing with LocalProtocolError after merge, choose between http2=False for that provider (a code change in that provider module, with its own version bump and verify_deployment pin) and holding h2 at 4.3.0 (which re-opens PYSEC-2026-3628).

## Appendix A - starlette Host-header probe

Command, in each venv, from a scratch directory: `python -I host_probe.py`. Output is in Measured results point 9. Source:

```python
"""Offline: does request.url.path disagree with the routed path (starlette)?"""
from importlib.metadata import version
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from starlette.middleware.base import BaseHTTPMiddleware
EXEMPT = {"/health"}
class Wall(BaseHTTPMiddleware):  # same decision shape as main.py:1351
    async def dispatch(self, request: Request, call_next):
        if str(request.url.path or "") in EXEMPT:
            return await call_next(request)
        return JSONResponse({"error": "unauthorized"}, status_code=401)
app = FastAPI(); app.add_middleware(Wall)
@app.get("/health")
def h(): return {"ok": True}
@app.get("/secret")
def s(): return {"secret": True}
c = TestClient(app)
print("starlette", version("starlette"))
print("plain /secret          ->", c.get("/secret").status_code)
print("crafted Host /secret   ->", c.get("/secret", headers={"host": "testserver/health?x="}).status_code)
```

## Appendix B - ci.yml job runner

Commands, from a copy of the tree (`git ls-files -co --exclude-standard -z | tar --null -T - -cf - | tar -xf - -C <copy>`), each with the venv that the job's own install step builds:

- `PATH=<lean venv>/bin:/opt/node22/bin:/usr/bin:/bin python run_job.py .github/workflows/ci.yml lean-unit` (lean venv: numpy 1.26.4, pytest 9.1.1, PyYAML 6.0.3)
- `PATH=<contract venv>/bin:/opt/node22/bin:/usr/bin:/bin python run_job.py .github/workflows/ci.yml contract` (contract venv: the job's pin list)
- `PATH=<NEW venv>/bin:/opt/node22/bin:/usr/bin:/bin python run_job.py .github/workflows/ci.yml heavy`

Source:

```python
"""Execute one ci.yml job's run-blocks as written (skip steps whose name has the word Install).

Usage: python run_job.py .github/workflows/ci.yml <job-id>
Each step's own `env:` mapping is applied for that step only.
"""
import os, re, subprocess, sys, yaml
wf, job = sys.argv[1], sys.argv[2]
steps = yaml.safe_load(open(wf, encoding="utf-8"))["jobs"][job]["steps"]
rc_all = 0
for st in steps:
    run = st.get("run"); name = st.get("name", "")
    if not run or re.search(r"\bInstall\b", name):
        continue
    env = dict(os.environ)
    env.update({str(k): str(v) for k, v in (st.get("env") or {}).items()})
    print("=== STEP:", name.encode("ascii", "replace").decode(), flush=True)
    rc = subprocess.call(["bash", "-e", "-c", run], env=env)
    print("=== RC:", rc, flush=True)
    rc_all = rc_all or rc
print("JOB", job, "OVERALL RC", rc_all)
sys.exit(rc_all)
```

## Appendix C - h2 ENABLE_PUSH loopback TLS reproduction

Command, in each venv, from a scratch directory: `python -I h2_push_tls.py`. Output is in Measured results point 7. Source:

```python
"""Loopback TLS HTTP/2 server (h2, server side) + httpx.AsyncClient(http2=True).
6 concurrent GETs with 300 KB bodies; optional server SETTINGS ENABLE_PUSH=1."""
import asyncio, datetime, os, ssl, sys, tempfile
from importlib.metadata import version
import h2.config, h2.connection, h2.events, h2.settings, httpx
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
import ipaddress

def make_cert(d):
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
            .serial_number(1).not_valid_before(now - datetime.timedelta(minutes=5)).not_valid_after(now + datetime.timedelta(days=1))
            .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), critical=False)
            .sign(key, hashes.SHA256()))
    c, k = os.path.join(d, "c.pem"), os.path.join(d, "k.pem")
    open(c, "wb").write(cert.public_bytes(serialization.Encoding.PEM))
    open(k, "wb").write(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    return c, k

BODY = b"x" * 300_000

async def serve(reader, writer, push):
    conn = h2.connection.H2Connection(h2.config.H2Configuration(client_side=False))
    conn.initiate_connection()
    if push:
        conn.update_settings({h2.settings.SettingCodes.ENABLE_PUSH: 1})
    writer.write(conn.data_to_send()); await writer.drain()
    pending = {}  # stream_id -> remaining bytes
    try:
        while True:
            data = await reader.read(65535)
            if not data:
                break
            try:
                events = conn.receive_data(data)
            except Exception:
                break
            for ev in events:
                if isinstance(ev, h2.events.RequestReceived):
                    conn.send_headers(ev.stream_id, [(":status", "200"), ("content-length", str(len(BODY)))])
                    pending[ev.stream_id] = BODY
                elif isinstance(ev, h2.events.DataReceived):
                    conn.acknowledge_received_data(ev.flow_controlled_length, ev.stream_id)
            for sid in list(pending):
                buf = pending[sid]
                while buf:
                    n = min(conn.local_flow_control_window(sid), conn.max_outbound_frame_size, len(buf))
                    if n <= 0:
                        break
                    conn.send_data(sid, buf[:n], end_stream=(n == len(buf)))
                    buf = buf[n:]
                if buf:
                    pending[sid] = buf
                else:
                    del pending[sid]
            writer.write(conn.data_to_send()); await writer.drain()
    finally:
        writer.close()

async def main(push):
    d = tempfile.mkdtemp(); c, k = make_cert(d)
    sctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH); sctx.load_cert_chain(c, k); sctx.set_alpn_protocols(["h2"])
    server = await asyncio.start_server(lambda r, w: serve(r, w, push), "127.0.0.1", 0, ssl=sctx)
    port = server.sockets[0].getsockname()[1]
    cctx = ssl.create_default_context(cafile=c)
    ok, errs = 0, []
    async with httpx.AsyncClient(http2=True, verify=cctx, timeout=10) as cl:
        async def one():
            r = await cl.get("https://127.0.0.1:%d/q" % port)
            return r.http_version, len(r.content)
        res = await asyncio.gather(*[one() for _ in range(6)], return_exceptions=True)
    for r in res:
        if isinstance(r, Exception):
            errs.append("%s: %s" % (type(r).__name__, r))
        elif r == ("HTTP/2", len(BODY)):
            ok += 1
    server.close(); await server.wait_closed()
    return ok, sorted(set(errs))

print("h2", version("h2"), "httpx", version("httpx"), "httpcore", version("httpcore"))
for push in (False, True):
    ok, errs = asyncio.run(main(push))
    print("server ENABLE_PUSH=%d -> %d/6 ok %s" % (int(push), ok, errs))
```

## Appendix D - review findings and dispositions (2026-10-13)

1. Major, this sheet: production libraries were said to change only at the next Render build. Confirmed: 8 scheduled workflows (plus 2 workflow_run audits and 2 manual workflows) install requirements.txt from main on every run, and several write to the live workbook. Fixed: "Where this lands", the rewritten Default OFF identity and Known limits bullets, per-workflow read-backs in operator step 3, rollback reach in step 6, and the merge-timing advice in step 1 and owner decision 6.
2. Minor, requirements.txt h2==4.4.1: h2 4.4 rejects a server SETTINGS ENABLE_PUSH=1. Confirmed from the source diff, in memory (L11) and over loopback TLS (Appendix C). Fixed as advised, with no code change: a Known limits bullet, a comment above the pin, operator steps 3 and 5, and owner decision 7.
3. Minor, this sheet: whole-suite parity was offered as proof of library equivalence, while no test imports the bumped libraries and the smoke scripts were scratch-only. Confirmed with the grep commands quoted at the top of Measured results. Fixed: the suite's scope is stated plainly; the library smoke is committed as tests/test_security_pins_libsmoke_20261013.py and enrolled in the blocking heavy lane (TFB_LIBSMOKE_REQUIRE_PINNED=1; kept out of the lean lane, where it would only skip); and the scripts that are not tests (host probe, job runner, TLS reproduction) are pasted above with their commands.
