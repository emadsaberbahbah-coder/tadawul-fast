# TFB Commit Sheet - security pin bumps in requirements.txt (v1.1.3 -> v1.1.4)

Date: 2026-10-13. Base: origin/main dda1211 (requirements.txt v1.1.3, ci.yml v1.1.12).
Files: requirements.txt (v1.1.4), .github/workflows/ci.yml (v1.1.13), tests/test_security_pins_20261013.py (NEW, v1.0.0), this sheet (NEW).
Not touched: scripts/verify_deployment.py, every production module, every cron time and concurrency group, the live workbook, Render, every data provider.

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

## Default OFF identity

This change is NOT default-off and cannot be gated. A dependency pin cannot be read from an ENV flag or Script Property: Render runs `pip install -r requirements.txt` at build time, before any application code reads configuration. The next Render build after merge installs the 12 new library versions in production. The proofs offered instead of byte-identity are below: the same test results, the same app surface, and the same library behaviour on both stacks. The only production-visible differences are the library versions themselves and the new transitive package defusedxml 0.7.1. The CI changes affect CI only.

## Measured results

Environment: CPython 3.11.17 (/usr/bin/python3.11; Render's runtime.txt says python-3.11.9, the same minor version), uv 0.11.32, Node v22.22.0. Separate fresh venvs throughout: OLD = base requirements.txt; NEW = this requirements.txt. Both whole-suite venvs also get pytest 9.1.1, openpyxl 3.1.5, reportlab 5.0.1 and pypdf 6.20.0, exactly as ci.yml's whole-suite lane installs them.

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
5. ci.yml lanes, executed as written: each job's run-blocks were extracted from ci.yml with PyYAML and run with bash -e, skipping only the install steps.
   - lean-unit (numpy 1.26.4, pytest 9.1.1, PyYAML 6.0.3 only):
     - Base tree: decision-layer step `3431 passed, 10 skipped, 2 warnings, 131 subtests passed`.
     - Commit tree: `3438 passed, 11 skipped, 2 warnings, 131 subtests passed` (+7/+1 = the harness).
     - The other lean steps are identical on both trees: 275 passed, 99 passed, 14+14 passed, the node suites (64 board funding, 73 source readiness, 16 reconciliation inputs, 25/25 calendar, 20 passed / 3 skipped input truth), and scripts/test_maturation.py.
     - Overall RC 0 on both.
   - contract:
     - Base pins (anyio 4.13.0): `239 passed, 2 skipped`.
     - New pins (anyio 4.14.2): `239 passed, 2 skipped`, RC 0.
   - heavy (NEW requirements): 39, 116, 43, 74, 12 passed + 1 skipped, 77, 95, 196 and 1 passed; overall RC 0.
   - compile:
     - `python -m compileall -q main.py core routes scripts integrations`: exit 0.
     - `python -m compileall -q tests`: exit 0.
     - `ruff==0.16.8 check --select E9,F63,F7,F82 --output-format concise .`: "All checks passed!".
   - tests/test_verify_manifest_pins.py and tests/test_ci_required_verdict.py pass inside the lean step.
6. Import smoke of main:app, with no server and no network. Every socket connect and getaddrinfo is replaced by a raiser, then `import main; app = main.app; app.openapi()` runs under `env -i`. Both stacks print "Routes mounted at app creation: mounted=6 duplicate_skips=0 partial_duplicate_skips=0 missing=0 failed=0 strategy=main.controlled_priority_plan route_signatures=108" and `{"app_type": "FastAPI", "blocked_socket_attempts": 0, "openapi_paths": 50, "routes": 100, ...}`, ending "IMPORT SMOKE OK". Stderr is empty on both.
7. Library behaviour smoke (offline), OLD vs NEW, `LIB SMOKE 11/11 ok` on both:
   - Fernet round-trip (core/config.py's only cryptography use).
   - pyOpenSSL key and context.
   - PyJWT HS256 encode/decode.
   - protobuf Struct round-trip, plus imports of grpc, grpc_status, google.api_core, google.cloud.secretmanager, the OTLP gRPC exporter and googleapiclient.
   - lxml parse, BeautifulSoup "lxml" and pandas.read_html(flavor="lxml").
   - nltk tokenize.
   - dotenv_values.
   - pydantic-settings env binding.
   - httpx.AsyncClient(http2=True) and an h2 connection preamble (the providers use http2=True).
   - aiohttp ClientSession.
   - anyio task group.

   The only new warning on NEW is pyOpenSSL's DeprecationWarning for PKey.generate_key, an API the repo never calls.
8. FastAPI form and upload parsing on starlette 0.41.3 (python-multipart 0.0.12 -> 0.0.31; starlette and FastAPI import `python_multipart` first, so the deprecated `multipart` shim is never loaded): urlencoded Form gives 200 {'a': 'x y', 'b': 7}; multipart UploadFile of 70000 bytes gives 200 {'name': 'z', 'fn': 't.bin', 'n': 70000}. Identical on both. The app itself has no Form/File/UploadFile route.
9. google-auth service-account signing (the Sheets auth path) with a throwaway RSA key, offline: sign and verify, plus google.auth.jwt encode and decode. "GAUTH SMOKE OK" with google-auth 2.49.2 on both cryptography 43.0.3 and 50.0.0.
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

## Exclusions and advisories left open

- starlette 0.41.3, 7 advisories open: PYSEC-2026-161, -249, -248, -1942, -1941, -2281 and -2280. Every published fix (0.47.2, 0.49.1, 1.0.1, 1.1.0, 1.3.0, 1.3.1) is outside fastapi 0.115.14's declared "starlette<0.47.0". Fixing any of them means a FastAPI framework upgrade, which is a major API move beyond starlette itself. That makes it an owner decision and a separate item. Feature exposure, from a grep of main.py, core, routes, integrations and scripts:
  - No StaticFiles, FileResponse, HTTPEndpoint, request.form() or Form/File/UploadFile routes. That covers -1942, -1941, -2281, -2280 and -249.
  - main.py does read request.url.path, including at main.py:1351, where GlobalAuthEnforcementMiddleware decides whether a request is exempt from the global token wall. That is the class described by PYSEC-2026-161 (Host header reconstruction) and PYSEC-2026-248.
  - Offline probe on starlette 0.41.3, run identically on the OLD and NEW stacks (pre-existing, not changed here): a BaseHTTPMiddleware with the same decision shape (exempt if request.url.path is "/health") returns 401 for GET /secret, but 200 for GET /secret with header `Host: testserver/health?x=`. request.url.path reads "/health" while routing uses the real path.
  - Mitigating factors: the wall is active only when REQUIRE_AUTH resolves true, OPEN_MODE is off and TFB_GLOBAL_AUTH_ENFORCE is not falsy. Per-route auth in the route modules is unaffected. Whether Render's edge forwards such a Host header to the service was not tested.
  - See Owner decisions.
- nltk GHSA-8mgp-746c-j5xp (CVE-2026-81726, alias PYSEC-2026-3740), 1 advisory open. The PYSEC record lists 3.10.3 as the fix, but the GHSA record says "Patched versions: Not yet patched", and 3.10.3 is the newest nltk on PyPI, so pip-audit still matches it. The affected APIs (TransitionParser, AveragedPerceptron and PerceptronTagger save/load with attacker-chosen paths) are not used. nltk is a reserved pin whose only use is an existence probe (core/news_intelligence.py HAS_NLTK).
- httpcore: not bumped. It is skipped by the audit as a URL requirement, and 1.0.9 audits clean on its own.
- Nothing else was excluded. Every other advisory row on base is closed.

## Known limits

- Not default-off (see above). Production libraries change on the next Render build.
- All measurements are offline. Live provider HTTP (aiohttp, httpx with HTTP/2), live Google Sheets/Secret Manager calls, gRPC/OTLP export and Redis were not exercised against real endpoints. The checks cover imports, construction, local signing and the full offline suite.
- Measured on CPython 3.11.17; Render pins 3.11.9. Every bumped package ships cp311/abi3/py3 manylinux2014 wheels (point 3), so the patch-level difference is not expected to matter.
- Transitive dependencies remain unpinned and resolve at Render build time, as before. Today's resolution carries no known advisory.
- The advisory database moves. A re-run on a later date can report new rows. The audit job stays advisory (continue-on-error).
- pyOpenSSL 26.x deprecates key generation helpers (DeprecationWarning). The repo does not call them.
- This branch is independent of the stacked PRs. Each of them also rewrites ci.yml's single VERSION heading line (main is v1.1.12; this branch writes v1.1.13), so a trivial header conflict is expected on whichever lands second. Resolve it by keeping both dated paragraphs and taking the next free version number. The lean-list insertion point (after tests/test_verify_manifest_pins.py) and the contract anyio line are unlikely to collide.

## Operator steps

1. Review and merge. There are no ENV, Script Property, cron or Render settings to change.
2. The next Render build installs the new pins. Read-back in the Render build log: "Successfully installed ... cryptography-50.0.0 ... pyOpenSSL-26.4.0 ... protobuf-5.29.6 ... python-multipart-0.0.31 ..." with no "Building wheel for" line for any of the 12 packages.
3. After deploy: the app's startup log should show the same "Routes mounted at app creation: mounted=6 ... failed=0" line, and /health should answer as before.
4. Optional: run the "Dependency and typed-module review (advisory)" workflow by hand. Its dependencies job should report "Found 8 known vulnerabilities in 2 packages".
5. Rollback: revert this commit (`git revert <sha>`). Every pin returns to v1.1.3 verbatim, ci.yml returns to v1.1.12 (contract anyio 4.13.0, harness not enrolled), and the next Render build reinstalls the old libraries. No data, workbook or configuration state depends on these versions.

## Owner decisions

1. starlette/FastAPI: decide whether to schedule a FastAPI upgrade to a release that accepts starlette 1.3.1 or later, which closes all 7 remaining starlette advisories. The ci.yml framework-drift lane already observes latest FastAPI/Starlette on Python 3.13. Independently of that upgrade, consider a small main.py hardening, as its own versioned item with a verify_deployment pin bump: decide the global-auth exemption on request.scope["path"] (the path starlette routes on) rather than request.url.path. The offline probe above shows the two can disagree on 0.41.3.
2. nltk: it is reserved and unused (existence probe only). Decide whether to keep tracking the unpatched GHSA or drop the pin in a later cleanup. This change removes nothing.
3. httpcore URL pin: the v1.1.3 header says both forms are stable. Restoring "httpcore==1.0.9" would let the CI audit see it directly. Keep the URL form only if the Render CDN index issue still matters.
4. pyOpenSSL is a reserved pin that nothing imports. Decide whether to keep it, given that it couples the cryptography version (any future cryptography security bump has to move pyOpenSSL with it).
5. Whether to promote the dependency audit from advisory to blocking once the starlette item is settled.
