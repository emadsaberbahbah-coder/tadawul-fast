# Workbook backup v1.5.3 — resumable upload recovery

## Observed failure

The scheduled backup [run 39185293314, job 117846330450](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39185293314/job/117846330450) exported the workbook successfully, then failed at 2026-10-19 01:26:19 UTC during the Drive resumable content upload. The exception was `requests.exceptions.SSLError`, caused by `SSLEOFError`; the job exited with code 3. The old upload error also included a resumable session URI and suggested folder permissions for this transport failure. No session URI, workbook rows, or credentials are reproduced here.

The upload helper in the reviewed `9aac4356` source still issued one session-creation POST and one content PUT. It had no way to recover the upload's acknowledged position or recognize a completed upload whose response was lost.

## Change

`scripts/backup_workbook.py` is now version **1.5.3**. Drive and OAuth Drive destinations use the same recovery logic; the GCS upload implementation remains unchanged.

- A transient connection, TLS, timeout, incomplete response, HTTP 5xx, HTTP 429, or recognized Drive rate-limit error leads to an empty status PUT on the **same resumable session**.
- A `308` response supplies the acknowledged byte range. Only the remaining bytes are sent. Invalid, out-of-bounds, or regressing ranges stop the upload.
- A lost final response can be recovered from a completed status response, without another file-creation request or another data upload.
- Recovery permits at most **eight content/status PUT requests**. Backoff grows from one second to a maximum of sixteen seconds. Request timeouts are capped by the remaining **600-second recovery budget**, starting after session creation.
- That budget is checked between requests. Requests uses an inactivity timeout, so it is not a strict ten-minute wall-clock deadline; the existing workflow's twenty-minute job timeout supplies the outer limit.
- HTTP `200` or `201` is accepted only with a matching file identity, filename, byte count, and MD5 content checksum. Those fields are explicitly requested during session creation. The existing SHA-256 audit digest is unchanged.
- An incomplete `308`, malformed completion receipt, permanent HTTP error, expired session, or exhausted recovery budget cannot authorize monthly copy or retention. The backup returns code **3** and records a failure.
- Upload diagnostics contain controlled failure messages, HTTP status, and recognized API reason codes. They do not include raw transport exception text, session URLs, or API response messages.

The session-creation POST is not blindly retried. An expired or unconfirmed session does not trigger another file creation in this run. Export validation, optional local copy, dry-run behavior, monthly-copy failure policy, and dated retention rules are preserved.

## Verification

All checks used synthetic workbook bytes and fake HTTP/storage boundaries. Socket connections were denied during the regression tests.

| Check | Result |
|---|---|
| New TLS interruption regression against the original `9aac4356` helper | **1 expected failure**, reproducing the unrecovered `SSLError` |
| `tests/test_workbook_backup_upload_recovery.py` plus existing validation, retention, and openpyxl compatibility suites | **120 passed in 1.15 seconds** |
| Python interpreter | **CPython 3.11.17** |
| Dependency-free `--selftest` before HTTP test dependencies were installed | **7/7 passed** |
| Python compilation and whitespace validation | Passed |

Recovery tests cover partial acknowledgement, a lost completion response, transient failures while querying status, missing ranges, rejected ranges, permanent errors, bounded exhaustion, deadline expiry, invalid completion metadata, safe diagnostics, and the real `run_backup` boundary before copy/pruning.

No live backup, workflow rerun, Drive write, service configuration change, or deployment was performed for this verification. This evidence establishes the local protocol and failure behavior; it does not claim a new remote backup has been created.

## Protocol references

- [Google Drive: resumable uploads and recovery](https://developers.google.com/workspace/drive/api/guides/manage-uploads) — status PUT, `308` ranges, completion responses, and interrupted upload handling.
- [Google Drive: File resource](https://developers.google.com/workspace/drive/api/reference/rest/v3/files) — file identity, binary size, and `md5Checksum` fields.
- [Google Drive: response fields](https://developers.google.com/workspace/drive/api/guides/fields-parameter) — explicitly requesting completion metadata.

References checked on 2026-10-19.
