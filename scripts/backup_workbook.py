#!/usr/bin/env python3
# scripts/backup_workbook.py
"""
================================================================================
Workbook Backup — v1.0.0 (W-4, Execution Plan v2.1; Gate & Evidence Register §8)
================================================================================
NEW script (window whitelist W-4, scheduled Jul 22–24; adopted 2026-07-20).

WHY
    The production classifier ("Market Share Deepseek-V3") is a single point
    of failure: every decision surface, ledger, and evidence tab lives in one
    Google Sheet with no independent copy. Register §8 sets the contract this
    script implements: RPO ≤ 24h · RTO ≤ 60min via runbook · retention
    30 daily + 12 monthly · restorer = operator · scope = WORKBOOK DATA ONLY.

WHAT IT DOES (nightly via .github/workflows/workbook_backup.yml)
    1. Exports the workbook as full-size xlsx through the Docs export URL
       (https://docs.google.com/spreadsheets/d/{id}/export?format=xlsx).
       DELIBERATE: the Drive API files.export endpoint caps at 10 MB and this
       workbook is ~18 MB — the Docs URL streams the same bytes Google's own
       File▸Download produces, with no cap, authorized by the service
       account's bearer token.
    2. Uploads it to the operator's backup folder as
       DAILY_TFB_Workbook_YYYY-MM-DD.xlsx using a RESUMABLE upload
       (multipart caps at 5 MB — also too small).
    3. On the first run of each month, server-side files.copy of the fresh
       daily into MONTHLY_TFB_Workbook_YYYY-MM.xlsx (no second upload).
    4. Prunes: newest 30 DAILY_ kept, newest 12 MONTHLY_ kept.
    5. Appends an audit line to _Run_Log — [BACKUP v1.0.0] name/size/sha/
       kept-counts on success, an ERROR row on ANY failure — so a silent
       failure is impossible: the seven-gate audit reads _Run_Log daily, and
       the workflow itself goes red (GitHub failure e-mail).

SECURITY SCOPE (Register §8, explicit)
    Workbook DATA only. Credentials, tokens, and ENV configuration are
    EXCLUDED BY DESIGN — secrets live in GitHub/Render and never enter a
    backup artifact; a credential-bearing backup would be a security
    regression, not a safety net. Configuration is recoverable from the repo
    plus the Gate & Evidence Register.

RESTORE RUNBOOK (RTO ≤ 60 min, restorer = operator)
    1. Open the Drive backup folder → download the wanted DAILY_/MONTHLY_
       xlsx (2 min).
    2. sheets.new → File ▸ Import ▸ Upload → "Insert new sheet(s)" into a NEW
       spreadsheet (never overwrite production blind) (5–10 min).
    3. Compare _Status / _Run_Log tails against the incident window; copy the
       needed tabs back into production, or repoint DEFAULT_SPREADSHEET_ID at
       the restored copy if production is unrecoverable (remaining budget).
    4. Verify with the standard shell/version checks + a manual cockpit
       refresh. Post-restore, dispatch this workflow with restore_test=true
       to re-prove the chain.

ONE-TIME OPERATOR SETUP (flagged — GitHub side, NOT Render)
    a. Drive → New folder (e.g. "TFB_Backups").
    b. Share it, role EDITOR, with the service-account e-mail (this script
       PRINTS that e-mail on a folder-access failure so it is never a hunt).
    c. Repo → Settings → Secrets and variables → Actions → Variables →
       New variable: TFB_BACKUP_FOLDER_ID = the folder id from its URL.

ENV
    DEFAULT_SPREADSHEET_ID / SPREADSHEET_ID   production workbook id
    TFB_BACKUP_FOLDER_ID                      Drive folder id (repo Variable)
    GOOGLE_SHEETS_CREDENTIALS(_B64) | GOOGLE_APPLICATION_CREDENTIALS
    TFB_BACKUP_MIN_BYTES     default 1000000  (sanity floor on the export)
    TFB_BACKUP_KEEP_DAILY    default 30
    TFB_BACKUP_KEEP_MONTHLY  default 12

USAGE
    python scripts/backup_workbook.py                 # nightly backup (CI)
    python scripts/backup_workbook.py --dry-run       # export+verify, no upload
    python scripts/backup_workbook.py --restore-test  # acceptance: download
                                                      #   newest daily, open it,
                                                      #   assert core tabs, log
    python scripts/backup_workbook.py --selftest      # offline logic harness
    python scripts/backup_workbook.py --attach dq_scorecard.md dq_report.json
                                                      # v1.6.0: upload-only;
                                                      #   small reports beside
                                                      #   the backups, dated
================================================================================
"""
from __future__ import annotations

import argparse
import base64
import datetime as _dt
import hashlib
import io
import json
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

__version__ = "1.6.0"
# v1.6.0 (2026-10-15) — --attach: PUBLISH SMALL REPORTS BESIDE THE BACKUPS.
# WHY: the nightly DQ audit (tfb_workbook_dq_audit.py, DQ-01..DQ-75) writes
# dq_scorecard.md / dq_report.json / dq_activation_receipt.json, but they only
# reach a 30-day GitHub artifact and the step summary, both behind a GitHub
# sign-in, so the morning data-quality review is still done by hand from an
# exported workbook. The same job already writes the workbook to the
# operator's PRIVATE backup folder; the scorecard belongs next to it. It is
# NOT committed to the repository: the repo is public and the scorecard names
# symbols, counts and portfolio-state checks.
# WHAT: `--attach PATH [PATH ...]` is an UPLOAD-ONLY mode (no export, no
# workbook upload, no monthly copy). It resolves the destination exactly as
# run_backup does (TFB_BACKUP_DEST / TFB_BACKUP_FOLDER_ID / the OAuth app
# folder, via _backup_dest, _credentials, _oauth_session,
# drive_ensure_app_folder — the run_restore_test composition) and uploads each
# file as DAILY_TFB_<Label>_<YYYY-MM-DD><ext> (Riyadh date, the daily_name
# convention), e.g. dq_scorecard.md -> DAILY_TFB_DQ_Scorecard_2026-10-15.md.
# A same-name file from an earlier run that day is replaced, and each report
# kind is pruned with plan_pruning to TFB_BACKUP_KEEP_DAILY (default 30).
# Missing paths are skipped with a NOTE and never fail the run. GUARDS: only
# .md / .json / .txt; never a zip/xlsx payload, the TFB_BACKUP_LOCAL_COPY
# export, the GOOGLE_APPLICATION_CREDENTIALS file, or anything carrying a
# private key / client secret / refresh token; 20 MB cap. Exit: 0 = done
# (including nothing to upload), 2 = setup failure or a blocked path,
# 3 = an upload failed. One _Run_Log row "[BACKUP-ATTACH vX]" (worded
# "blocked", never "refus...", which tfb_export_audit counts as an
# identity-guard refusal). Errors are logged without URLs (a resumable
# session URL in a public log could be used to finish the upload). Two
# inputs with one destination name: the second is skipped. The date is the
# TFB_BACKUP_LOCAL_COPY export's mtime in Riyadh when present.
# ADDITIVE: drive_upload_resumable / gcs_upload_resumable / _store_upload
# gain an optional `mime` (default the xlsx type, so the workbook upload is
# byte-identical) and plan_pruning gains optional name patterns (default the
# workbook patterns). Without --attach every mode, line and exit code is
# unchanged. ROLLBACK: revert this file (the workflow step is
# continue-on-error and skips cleanly).
#
# v1.5.0 (2026-10-11) [WP-0] — ENV-GATED LOCAL COPY OF THE EXPORTED BYTES.
# WHY: the WP-0 DQ harness (scripts/tfb_workbook_dq_audit.py) must measure
# the SAME bytes this job just exported, in the SAME job, without a second
# Drive export and without ever turning the xlsx into a GitHub artifact (the
# repository is public; the export carries portfolio quantities). Until now
# `data` lived only in memory: export -> min-bytes check -> dry-run return or
# upload; nothing was ever written to disk. v1.5.0 adds ONE additive helper,
# _write_local_copy(data, sha), called after the TFB_BACKUP_MIN_BYTES check and
# before the dry-run return (so dry runs also produce the copy when asked).
# GATE: TFB_BACKUP_LOCAL_COPY = absolute file path (the workflow derives it
# from the repo Variable TFB_DQ_AUDIT_MODE and places it under $RUNNER_TEMP).
# UNSET / EMPTY (the default) -> the helper returns before any I/O or log line,
# so v1.5.0 is byte-identical in behaviour to v1.4.0: export, upload,
# retention, _Run_Log rows and every exit code are untouched. SET -> the
# bytes are written with mode 0600 and one stderr line `local copy: <path>
# (<bytes> bytes sha256=<12hex>)` is logged; ANY exception is logged as
# `NOTE: local copy failed: ...` and never changes the return code. The
# always() cleanup step of the workflow removes the file. Evidence that made
# this necessary: 2026-10-10 export 22,487,265 bytes, 50 tabs (10 visible),
# 9,791 live rows (255 / 6,609 / 453 / 2,474) measured by WP-0 — 275 rows
# without a price, 153 ROI-12M soft-cap breaches, 2,618 F-7 flips, 21,670
# duplicate Signal_History keys — none of which any workflow could see
# because no step ever held the exported file. ROLLBACK: revert this file.
#
# v1.4.0 (2026-07-26) — OAUTH USER-DELEGATED DRIVE DESTINATION ("oauth").
# WHY: the service account cannot own Drive files (storageQuotaExceeded,
# run #8) and GCS needs billing, which is unavailable for individual
# onboarding in this region. Remaining free path: authenticate the UPLOAD
# as the OPERATOR, so backups consume the operator's own 15 GB and land
# in the operator's own Drive — openable and restorable by hand, with no
# tooling, which is what a disaster-recovery artefact should be.
#
# TWO IDENTITIES, DELIBERATELY. This is the load-bearing design point:
#   SERVICE ACCOUNT  keeps exporting the workbook and writing _Run_Log.
#                    It already has read access to the workbook (proven:
#                    18.5 MB exported cleanly on runs #6-#8).
#   OAUTH USER       performs ONLY the destination operations (upload,
#                    list, copy, delete, download).
# They are NOT interchangeable. The OAuth grant uses the NARROW
# drive.file scope — access strictly limited to files THIS APP CREATES —
# so it can neither read the workbook nor touch anything else in the
# operator's Drive. Trying to serve both roles from one credential would
# have meant requesting full Drive scope, i.e. handing a CI job standing
# read/write over the operator's entire Drive to solve a backup problem.
# Refused. The split costs one extra session object and nothing else.
#
# FOLDER, WITHOUT OVER-REACHING. drive.file cannot write into a folder the
# app did not create — supplying an arbitrary parent id returns 404. So
# the app CREATES its own folder (default "TFB Workbook Backups") on first
# run and FINDS IT AGAIN on later runs, because files.list under
# drive.file returns exactly the app-created set. Self-healing: delete the
# folder and the next run rebuilds it. TFB_BACKUP_FOLDER_ID is NOT used on
# this path and must not be — that id names a folder the app did not
# create.
#
# TOKEN LONGEVITY: a refresh token for an app left in "Testing" publishing
# status expires after 7 DAYS, which would make this job die silently each
# week — worse than no backup, because the dashboard would look healthy.
# The operator app is published ("In production"), so the token persists.
# drive.file is non-sensitive, so publishing needs no Google review.
# Failure of the refresh exchange is reported explicitly, never as a
# generic auth error, so an expired grant is diagnosable in one glance.
#
# DEFAULT UNCHANGED: TFB_BACKUP_DEST unset => "drive" => v1.3.0 verbatim.
# Every drive_* and gcs_* helper is byte-identical; "oauth" reuses the
# drive_* helpers wholesale, differing ONLY in which session they receive.
# v1.3.0 (2026-07-26) — PLUGGABLE DESTINATION + GCS BACKEND.
# WHY: run #8 proved the Drive path is architecturally impossible for this
# credential. Google's own words: "Service Accounts do not have storage
# quota." A file a service account creates in a My Drive folder has no
# owner Drive will accept, so NO sharing change, folder id or permission
# grant can ever make it work. The destination had to change, not the
# configuration. Options weighed: a Shared Drive (clean, but needs a paid
# Workspace subscription); OAuth user-delegation (free, but a refresh
# token for an app in "Testing" publishing status EXPIRES EVERY 7 DAYS —
# a disaster-recovery job that silently dies weekly is worse than none);
# and Google Cloud Storage, where the bucket belongs to the PROJECT, so
# the service account writes against project-owned storage and the quota
# problem does not exist. GCS chosen: same credentials, no token to
# expire, ~555 MB for 30 dailies + 12 monthlies — inside the free tier.
# DESIGN: destination is a SWITCH, not a replacement. TFB_BACKUP_DEST
# defaults to "drive", so this build is byte-identical in behaviour until
# the operator sets "gcs" — and if a Shared Drive becomes available later,
# flipping back needs no code change. Every drive_* helper is untouched.
# The retention engine is destination-agnostic: it reasons over NAMES, and
# the GCS helpers return the same {"id", "name"} shape the Drive ones do,
# so plan_pruning / monthly_needed / _DAILY_RE / _MONTHLY_RE and the
# 30-daily + 12-monthly policy are shared verbatim by both backends.
# SCOPE ADDED: devstorage.read_write. Harmless to the Drive and Sheets
# paths — a service account only ever gets what IAM already grants it.
# OPERATOR SETUP for gcs: enable billing on the GCP project, create a
# bucket, grant the service account Storage Object Admin on it, then set
# TFB_BACKUP_DEST=gcs and TFB_BACKUP_GCS_BUCKET=<name>.
# v1.2.0 (2026-07-26) — SURFACE GOOGLE'S OWN REASON. Still NO change to
# export, upload, retention or exit codes.
# WHY, from run #7: v1.1.0 named the account and normalised the folder id,
# and the failure moved 404 -> 403. That is progress with a wall behind
# it: 404 meant "cannot see the folder", 403 means "sees it, refused the
# write" — and TWO very different causes both return 403:
#   insufficientFilePermissions  the account has Viewer/Commenter, not
#                                Editor  -> fix by changing the role
#   storageQuotaExceeded         a bare GCP service account has NO Drive
#                                storage of its own. A file it creates in
#                                a normal (My Drive) folder would be owned
#                                by the service account, so Google refuses
#                                -> re-sharing as Editor will NEVER fix
#                                this; the folder must live on a SHARED
#                                DRIVE (service account added as Content
#                                Manager), which owns the storage itself.
# requests' raise_for_status() discards the response BODY, which is where
# Google states which one it is — so the operator was left guessing
# between a 30-second fix and an architectural one. _api_reason() now
# extracts error.errors[].reason and error.message from the body and the
# failure message names the reason AND the matching remedy.
# drive_upload_resumable is deliberately NOT touched: the body is read
# from the exception's attached response in the caller, so the upload path
# stays byte-identical.
# v1.1.0 (2026-07-26) — SELF-DIAGNOSING AUTH + FOLDER ID. NO backup logic,
# retention rule, export path, upload call or exit code changed.
# WHY, from a real failed run (Actions #6, 2026-07-26): the export
# SUCCEEDED (18,547,634 bytes) and the upload then returned
#   "404 ... — is the folder shared EDITOR with ??"
# Two defects made a one-line problem into a guessing game:
#  [A] THE EMAIL PRINTED AS "??". _credentials() stamps
#      _tfb_client_email ONLY on the inline-JSON branch. Every workflow in
#      this repo materializes the secret to a FILE and exports
#      GOOGLE_APPLICATION_CREDENTIALS, so the file branch is the one that
#      ALWAYS runs in CI — and it never stamped the address. The single
#      fact needed to fix the failure was the one fact the error could not
#      state. Now read from the file and stamped on both branches.
#  [B] A PASTED FOLDER ID IS SILENTLY WRONG. Drive's "Copy link" yields
#      .../folders/<ID>?usp=drive_link; pasting that whole tail into the
#      variable sends "<ID>?usp=drive_link" to the API, which answers 404
#      — indistinguishable from a genuine permission problem. _drive_id()
#      now extracts the id from a full URL, strips any query/fragment, and
#      SAYS SO when it had to alter the operator's value.
# Also: a preflight line names the account, the folder id and the sheet id
# BEFORE the 13-second export, so a misconfigured run is obvious at the top
# of the log; and a failed upload/list re-states the account and folder
# with the exact sharing instruction. 404 is called what it is — the
# folder is invisible to THIS account, which is Drive's answer for both
# "not shared" and "wrong id".
_RIYADH = ZoneInfo("Asia/Riyadh")

_XLSX_MIME = ("application/vnd.openxmlformats-officedocument"
              ".spreadsheetml.sheet")
_DAILY_RE = re.compile(r"^DAILY_TFB_Workbook_(\d{4}-\d{2}-\d{2})\.xlsx$")
_MONTHLY_RE = re.compile(r"^MONTHLY_TFB_Workbook_(\d{4}-\d{2})\.xlsx$")
_CORE_TABS = {"_Status", "_Run_Log", "Top_10_Investments",
              "_Portfolio_CostBasis", "My_Portfolio", "Shadow_History"}
# v1.6.0 --attach: small text reports only.
_ATTACH_MIME = {".md": "text/markdown", ".json": "application/json",
                ".txt": "text/plain"}
_ATTACH_MAX_BYTES = 20_000_000
_ATTACH_SECRET_MARKERS = (b'"private_key"', b'"client_secret"',
                          b'"refresh_token"', b"-----BEGIN ")
_ATTACH_UPPER = {"dq", "pf", "tfb", "kpi", "roi", "s1"}


def _out(msg: str) -> None:
    sys.stderr.write(f"[workbook_backup v{__version__}] {msg}\n")


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _env_int(name: str, default: int) -> int:
    try:
        v = int(float(_env(name, str(default))))
        return v if v > 0 else default
    except (TypeError, ValueError):
        return default


def _now_riyadh() -> _dt.datetime:
    return _dt.datetime.now(_RIYADH)


def _write_local_copy(data: bytes, sha: str) -> None:
    """v1.5.0 [WP-0]: write the exported bytes to TFB_BACKUP_LOCAL_COPY when set.

    Unset or empty (the default) -> no I/O, no log line, nothing changes.
    Set -> the file is written with mode 0600 for the same-job DQ audit step;
    any failure is logged as a NOTE and never alters the backup's return code.
    """
    path = _env("TFB_BACKUP_LOCAL_COPY")
    if not path:
        return
    try:
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            os.write(fd, data)
        finally:
            os.close(fd)
        _out(f"local copy: {path} ({len(data):,} bytes sha256={sha})")
    except Exception as exc:  # noqa: BLE001 — a local copy must never fail the backup
        _out(f"NOTE: local copy failed: {type(exc).__name__}: {exc}")


# --------------------------------------------------------------------------- #
# Pure helpers (offline selftest)                                             #
# --------------------------------------------------------------------------- #
def daily_name(d: _dt.date) -> str:
    return f"DAILY_TFB_Workbook_{d.isoformat()}.xlsx"


def monthly_name(d: _dt.date) -> str:
    return f"MONTHLY_TFB_Workbook_{d.strftime('%Y-%m')}.xlsx"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def plan_pruning(names: List[str], keep_daily: int, keep_monthly: int,
                 daily_re=None, monthly_re=None
                 ) -> Tuple[List[str], List[str]]:
    """(keep, delete) over folder file NAMES. Newest-by-embedded-date wins;
    anything not matching either pattern is LEFT ALONE (never delete a file
    this script did not name). v1.6.0: the patterns default to the workbook's
    (unchanged behaviour); --attach passes one pair per report kind."""
    daily_re = daily_re or _DAILY_RE
    monthly_re = monthly_re or _MONTHLY_RE
    dailies = sorted((n for n in names if daily_re.match(n)), reverse=True)
    monthlies = sorted((n for n in names if monthly_re.match(n)), reverse=True)
    keep = dailies[:keep_daily] + monthlies[:keep_monthly]
    delete = dailies[keep_daily:] + monthlies[keep_monthly:]
    return keep, delete


def monthly_needed(today: _dt.date, existing_names: List[str]) -> bool:
    """A MONTHLY_ copy is due when none exists yet for today's month."""
    want = monthly_name(today)
    return want not in set(existing_names)


def attach_label(path: str) -> str:
    """v1.6.0: 'dq_activation_receipt.json' -> 'DQ_Activation_Receipt'."""
    stem = os.path.splitext(os.path.basename(path))[0]
    parts = [p for p in re.split(r"[^A-Za-z0-9]+", stem) if p]
    return "_".join(p.upper() if p.lower() in _ATTACH_UPPER else p.capitalize()
                    for p in parts) or "Report"


def attach_name(path: str, d: _dt.date) -> str:
    """v1.6.0: the daily_name convention for a report, e.g.
    DAILY_TFB_DQ_Scorecard_2026-10-15.md (Riyadh date, sorts with the xlsx)."""
    return (f"DAILY_TFB_{attach_label(path)}_{d.isoformat()}"
            f"{os.path.splitext(path)[1].lower()}")


def attach_patterns(path: str):
    """v1.6.0: (daily_re, monthly_re) naming exactly one report kind, for
    plan_pruning. No MONTHLY_ copies are made for reports; the pattern exists
    so plan_pruning's policy applies unchanged."""
    label = re.escape(attach_label(path))
    ext = re.escape(os.path.splitext(path)[1].lower())
    return (re.compile(rf"^DAILY_TFB_{label}_(\d{{4}}-\d{{2}}-\d{{2}}){ext}$"),
            re.compile(rf"^MONTHLY_TFB_{label}_(\d{{4}}-\d{{2}}){ext}$"))


def attach_refusal(path: str, data: bytes) -> str:
    """v1.6.0: why `path` must NOT be uploaded by --attach ('' = allowed).
    Never the workbook export, never a credential."""
    ext = os.path.splitext(path)[1].lower()
    if ext not in _ATTACH_MIME:
        return f"extension {ext or '(none)'} not in {sorted(_ATTACH_MIME)}"
    real = os.path.realpath(path)
    for var in ("TFB_BACKUP_LOCAL_COPY", "GOOGLE_APPLICATION_CREDENTIALS"):
        other = _env(var)
        if other and os.path.realpath(other) == real:
            return f"is the {var} file"
    if data[:4] == b"PK\x03\x04":
        return "zip/xlsx payload"
    if len(data) > _ATTACH_MAX_BYTES:
        return f"{len(data):,} bytes > {_ATTACH_MAX_BYTES:,} cap"
    hits = [m.decode() for m in _ATTACH_SECRET_MARKERS if m in data]
    if hits:
        return "looks like a credential (" + ", ".join(hits) + ")"
    return ""


# --------------------------------------------------------------------------- #
# Google auth + HTTP (CI-only imports)                                        #
# --------------------------------------------------------------------------- #
def _credentials():
    from google.oauth2 import service_account  # local import: CI only
    scopes = ["https://www.googleapis.com/auth/drive",
              "https://www.googleapis.com/auth/spreadsheets",
              # v1.3.0: GCS backend. Requesting a scope grants nothing on
              # its own — IAM still decides — so this is inert for the
              # drive destination.
              "https://www.googleapis.com/auth/devstorage.read_write"]
    path = _env("GOOGLE_APPLICATION_CREDENTIALS")
    if path and os.path.exists(path):
        creds = service_account.Credentials.from_service_account_file(
            path, scopes=scopes)
        # v1.1.0 [A]: the branch CI actually takes must name its identity.
        # service_account_email is the library's own attribute; the file
        # read is a belt-and-braces fallback and never fatal.
        email = getattr(creds, "service_account_email", "") or ""
        if not email:
            try:
                with open(path, "r", encoding="utf-8") as fh:
                    email = json.load(fh).get("client_email", "") or ""
            except Exception:
                email = ""
        creds._tfb_client_email = email or "?"
        return creds
    raw = (_env("GOOGLE_SHEETS_CREDENTIALS")
           or _env("GOOGLE_SHEETS_CREDENTIALS_B64")
           or _env("GOOGLE_CREDENTIALS"))
    if not raw:
        raise RuntimeError("no Google credentials in env")
    s = raw
    if not s.startswith("{"):
        try:
            dec = base64.b64decode(s).decode("utf-8", errors="replace").strip()
            if dec.startswith("{"):
                s = dec
        except Exception:
            pass
    info = json.loads(s)
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=scopes)
    creds._tfb_client_email = info.get("client_email", "?")  # for error help
    return creds


def _creds_email(creds) -> str:
    """v1.1.0: the account this run authenticates as, whatever branch
    _credentials() took. Never raises."""
    return (getattr(creds, "_tfb_client_email", "")
            or getattr(creds, "service_account_email", "")
            or "?")


def _backup_dest() -> str:
    """"drive" (default, service account) | "gcs" | "oauth" (v1.4.0,
    operator-delegated Drive). Anything unrecognised falls back to the
    v1.2.0 default rather than guessing."""
    v = _env("TFB_BACKUP_DEST", "drive").lower()
    if v in ("gcs", "gcloud", "cloud-storage", "storage"):
        return "gcs"
    if v in ("oauth", "user", "drive_oauth", "drive-oauth"):
        return "oauth"
    return "drive"


def _oauth_folder_name() -> str:
    return _env("TFB_BACKUP_DRIVE_FOLDER_NAME", "TFB Workbook Backups")


def _oauth_credentials():
    """v1.4.0: operator-delegated user credentials, drive.file scope only.
    Raises with a NAMED cause — an expired or revoked grant must never
    surface as an anonymous auth error."""
    from google.oauth2.credentials import Credentials as UserCredentials
    cid = _env("TFB_BACKUP_OAUTH_CLIENT_ID")
    csec = _env("TFB_BACKUP_OAUTH_CLIENT_SECRET")
    rtok = _env("TFB_BACKUP_OAUTH_REFRESH_TOKEN")
    missing = [n for n, v in (("TFB_BACKUP_OAUTH_CLIENT_ID", cid),
                              ("TFB_BACKUP_OAUTH_CLIENT_SECRET", csec),
                              ("TFB_BACKUP_OAUTH_REFRESH_TOKEN", rtok))
               if not v]
    if missing:
        raise RuntimeError("oauth secrets missing: " + ", ".join(missing))
    return UserCredentials(
        None, refresh_token=rtok,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=cid, client_secret=csec,
        scopes=["https://www.googleapis.com/auth/drive.file"])


def _oauth_session():
    """A session that has already PROVED its refresh token works, so a
    dead grant is reported before the 20-second export rather than after."""
    from google.auth.transport.requests import Request, AuthorizedSession
    creds = _oauth_credentials()
    try:
        creds.refresh(Request())
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(
            "oauth refresh failed (" + type(e).__name__ + ": " + str(e)
            + ") — the refresh token is expired or revoked. Re-run the "
            "OAuth Playground consent for scope drive.file and update "
            "TFB_BACKUP_OAUTH_REFRESH_TOKEN. Confirm the OAuth app is "
            "published (In production): a Testing-status app expires "
            "refresh tokens after 7 days.")
    return AuthorizedSession(creds)


def drive_ensure_app_folder(sess, name: str) -> str:
    """v1.4.0: find-or-create the app's OWN backup folder and return its
    id. Under drive.file, files.list returns only app-created items, so a
    name match here cannot collide with anything else in the Drive."""
    q = ("mimeType='application/vnd.google-apps.folder' and trashed=false "
         "and name='" + name.replace("'", "\\'") + "'")
    r = sess.get("https://www.googleapis.com/drive/v3/files",
                 params={"q": q, "fields": "files(id,name)", "pageSize": 10},
                 timeout=60)
    r.raise_for_status()
    hits = r.json().get("files", [])
    if hits:
        return hits[0]["id"]
    r2 = sess.post("https://www.googleapis.com/drive/v3/files",
                   json={"name": name,
                         "mimeType": "application/vnd.google-apps.folder"},
                   params={"fields": "id"}, timeout=60)
    r2.raise_for_status()
    return r2.json()["id"]


def _gcs_bucket() -> str:
    return _env("TFB_BACKUP_GCS_BUCKET")


def _gcs_prefix() -> str:
    """Object-name prefix. Normalised to end with exactly one slash, or be
    empty — so object names are never accidentally concatenated."""
    raw = os.environ.get("TFB_BACKUP_GCS_PREFIX")
    # Present-but-empty is a DELIBERATE choice (objects at the bucket root);
    # only an absent variable falls back to the default.
    v = ("tfb-backups/" if raw is None else raw).strip().lstrip("/")
    if v and not v.endswith("/"):
        v += "/"
    return v


def _api_reason(exc) -> str:
    """v1.2.0: Google's own machine reason + human message from the failed
    response attached to a requests HTTPError. Returns "" when there is no
    body to read. Never raises — a diagnostic must not become the fault."""
    resp = getattr(exc, "response", None)
    if resp is None:
        return ""
    reason = message = ""
    try:
        err = (resp.json() or {}).get("error") or {}
        message = str(err.get("message") or "")
        errs = err.get("errors") or []
        if errs and isinstance(errs, list) and isinstance(errs[0], dict):
            reason = str(errs[0].get("reason") or "")
        if not reason:
            reason = str(err.get("status") or "")
    except Exception:
        try:
            message = (resp.text or "")[:300]
        except Exception:
            message = ""
    out = ":".join(x for x in (reason, message) if x)
    return out[:400]


def _403_remedy(reason_blob: str, email: str, folder_id: str) -> str:
    """v1.2.0: map Google's reason to the ONE action that fixes it."""
    r = (reason_blob or "").lower()
    if "storagequota" in r or "storage quota" in r:
        return ("service accounts have NO Drive storage of their own, so a "
                "file created in a My Drive folder cannot be owned — "
                "re-sharing will NOT fix this. Move the backup folder to a "
                "SHARED DRIVE and add " + email + " as Content Manager, or "
                "point TFB_BACKUP_FOLDER_ID at a Shared Drive folder.")
    if "insufficientfilepermissions" in r or "permission" in r:
        return ("the folder is visible to " + email + " but not writable — "
                "open the folder's Share dialog and set that account to "
                "EDITOR (Viewer/Commenter cannot upload).")
    return ("folder " + repr(folder_id) + " is visible to " + email + " but "
            "refused the write — check the share role is EDITOR; if it "
            "already is, this is the service-account storage-quota case "
            "and the folder must live on a Shared Drive.")


def _drive_id(raw: str) -> str:
    """v1.1.0 [B]: accept what an operator can realistically paste and
    return the bare Drive id.

    Handles a full folder URL, a "Copy link" tail (?usp=drive_link), a
    trailing slash, surrounding quotes or whitespace. Pure and total: an
    unrecognised value is returned trimmed rather than rejected, so this
    can never turn a working configuration into a failure."""
    v = (raw or "").strip().strip('"').strip("'")
    if not v:
        return ""
    for marker in ("/folders/", "/d/"):
        if marker in v:
            v = v.split(marker, 1)[1]
            break
    for cut in ("?", "#", "/"):
        if cut in v:
            v = v.split(cut, 1)[0]
    return v.strip()


def _session(creds):
    from google.auth.transport.requests import AuthorizedSession
    return AuthorizedSession(creds)


def export_xlsx(sess, sheet_id: str) -> bytes:
    """Full-size xlsx via the Docs export URL (files.export caps at 10 MB)."""
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
    r = sess.get(url, timeout=300)
    r.raise_for_status()
    return r.content


def drive_list(sess, folder_id: str) -> List[Dict[str, Any]]:
    files: List[Dict[str, Any]] = []
    token = None
    while True:
        params = {"q": f"'{folder_id}' in parents and trashed=false",
                  "fields": "nextPageToken,files(id,name,size,createdTime)",
                  "pageSize": 200,
                  "supportsAllDrives": "true",
                  "includeItemsFromAllDrives": "true"}
        if token:
            params["pageToken"] = token
        r = sess.get("https://www.googleapis.com/drive/v3/files",
                     params=params, timeout=60)
        r.raise_for_status()
        j = r.json()
        files += j.get("files", [])
        token = j.get("nextPageToken")
        if not token:
            return files


def drive_upload_resumable(sess, folder_id: str, name: str,
                           data: bytes, mime: str = _XLSX_MIME
                           ) -> Dict[str, Any]:
    meta = {"name": name, "parents": [folder_id]}
    r = sess.post(
        "https://www.googleapis.com/upload/drive/v3/files"
        "?uploadType=resumable&supportsAllDrives=true",
        json=meta,
        headers={"X-Upload-Content-Type": mime,
                 "X-Upload-Content-Length": str(len(data))},
        timeout=60)
    r.raise_for_status()
    loc = r.headers.get("Location")
    if not loc:
        raise RuntimeError("resumable upload: no session Location header")
    r2 = sess.put(loc, data=data,
                  headers={"Content-Type": mime}, timeout=600)
    r2.raise_for_status()
    return r2.json()


def drive_copy(sess, file_id: str, folder_id: str, new_name: str
               ) -> Dict[str, Any]:
    r = sess.post(
        f"https://www.googleapis.com/drive/v3/files/{file_id}/copy"
        "?supportsAllDrives=true",
        json={"name": new_name, "parents": [folder_id]}, timeout=120)
    r.raise_for_status()
    return r.json()


def drive_delete(sess, file_id: str) -> None:
    r = sess.delete(
        f"https://www.googleapis.com/drive/v3/files/{file_id}"
        "?supportsAllDrives=true", timeout=60)
    if r.status_code not in (200, 204):
        raise RuntimeError(f"delete {file_id}: HTTP {r.status_code}")


def drive_download(sess, file_id: str) -> bytes:
    r = sess.get(
        f"https://www.googleapis.com/drive/v3/files/{file_id}"
        "?alt=media&supportsAllDrives=true", timeout=600)
    r.raise_for_status()
    return r.content


# --------------------------------------------------------------------------- #
# v1.3.0 Google Cloud Storage backend                                         #
# --------------------------------------------------------------------------- #
# Contract: every function below returns the SAME shape as its drive_*
# counterpart — {"id": <full object name>, "name": <bare file name>} — so
# the retention engine, which reasons purely over names, is shared verbatim.
# For GCS the object name IS the identifier, so id carries the prefixed
# path while name carries the basename the regexes match on.
def _gcs_obj_path(name: str) -> str:
    return _gcs_prefix() + name


def _gcs_quote(obj: str) -> str:
    """Object names contain slashes; they MUST be percent-encoded when they
    sit in the URL PATH, or the API reads them as resource separators."""
    from urllib.parse import quote
    return quote(obj, safe="")


def gcs_list(sess, bucket: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    token = None
    prefix = _gcs_prefix()
    while True:
        params: Dict[str, Any] = {"maxResults": 200,
                                  "fields": "nextPageToken,items(name,size,timeCreated)"}
        if prefix:
            params["prefix"] = prefix
        if token:
            params["pageToken"] = token
        r = sess.get(f"https://storage.googleapis.com/storage/v1/b/{bucket}/o",
                     params=params, timeout=60)
        r.raise_for_status()
        j = r.json()
        for it in j.get("items", []):
            full = it.get("name", "")
            out.append({"id": full, "name": full.rsplit("/", 1)[-1],
                        "size": it.get("size"),
                        "createdTime": it.get("timeCreated")})
        token = j.get("nextPageToken")
        if not token:
            return out


def gcs_upload_resumable(sess, bucket: str, name: str,
                         data: bytes, mime: str = _XLSX_MIME
                         ) -> Dict[str, Any]:
    obj = _gcs_obj_path(name)
    r = sess.post(
        f"https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o",
        params={"uploadType": "resumable", "name": obj},
        json={"name": obj, "contentType": mime},
        headers={"X-Upload-Content-Type": mime,
                 "X-Upload-Content-Length": str(len(data))},
        timeout=60)
    r.raise_for_status()
    loc = r.headers.get("Location")
    if not loc:
        raise RuntimeError("gcs resumable upload: no session Location header")
    r2 = sess.put(loc, data=data,
                  headers={"Content-Type": mime}, timeout=600)
    r2.raise_for_status()
    j = r2.json()
    full = j.get("name", obj)
    return {"id": full, "name": full.rsplit("/", 1)[-1]}


def gcs_copy(sess, bucket: str, src_obj: str, new_name: str
             ) -> Dict[str, Any]:
    dst = _gcs_obj_path(new_name)
    r = sess.post(
        f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/"
        f"{_gcs_quote(src_obj)}/copyTo/b/{bucket}/o/{_gcs_quote(dst)}",
        json={}, timeout=300)
    r.raise_for_status()
    j = r.json().get("resource") or r.json()
    full = j.get("name", dst)
    return {"id": full, "name": full.rsplit("/", 1)[-1]}


def gcs_delete(sess, bucket: str, obj: str) -> None:
    r = sess.delete(
        f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/"
        f"{_gcs_quote(obj)}", timeout=60)
    if r.status_code not in (200, 204):
        raise RuntimeError(f"gcs delete {obj}: HTTP {r.status_code}")


def gcs_download(sess, bucket: str, obj: str) -> bytes:
    r = sess.get(
        f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/"
        f"{_gcs_quote(obj)}", params={"alt": "media"}, timeout=600)
    r.raise_for_status()
    return r.content


# --------------------------------------------------------------------------- #
# v1.3.0 destination dispatch — the ONE place the two backends meet          #
# --------------------------------------------------------------------------- #
# v1.4.0: "oauth" is the Drive API with a different IDENTITY, not a
# different protocol — it reuses every drive_* helper unchanged.
def _store_list(sess, dest: str, container: str) -> List[Dict[str, Any]]:
    return (gcs_list(sess, container) if dest == "gcs"
            else drive_list(sess, container))


def _store_upload(sess, dest: str, container: str, name: str,
                  data: bytes, mime: str = _XLSX_MIME) -> Dict[str, Any]:
    return (gcs_upload_resumable(sess, container, name, data, mime)
            if dest == "gcs"
            else drive_upload_resumable(sess, container, name, data, mime))


def _store_copy(sess, dest: str, container: str, src_id: str,
                new_name: str) -> Dict[str, Any]:
    return (gcs_copy(sess, container, src_id, new_name) if dest == "gcs"
            else drive_copy(sess, src_id, container, new_name))


def _store_delete(sess, dest: str, container: str, fid: str) -> None:
    if dest == "gcs":
        gcs_delete(sess, container, fid)
    else:
        drive_delete(sess, fid)


def _store_download(sess, dest: str, container: str, fid: str) -> bytes:
    return (gcs_download(sess, container, fid) if dest == "gcs"
            else drive_download(sess, fid))


# --------------------------------------------------------------------------- #
# _Run_Log audit line (guarded — logging must never mask the real outcome)    #
# --------------------------------------------------------------------------- #
def append_run_log(status: str, message: str, details: Dict[str, Any]) -> None:
    try:
        import gspread
        creds = _credentials()
        gc = gspread.authorize(creds)
        sid = _env("DEFAULT_SPREADSHEET_ID") or _env("SPREADSHEET_ID")
        ws = gc.open_by_key(sid).worksheet("_Run_Log")
        ws.append_row(
            [_dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
             "INFO" if status == "OK" else "ERROR", "workbook_backup",
             "_Backup", status, message, "", "", "",
             json.dumps(details)[:900]],
            value_input_option="RAW")
    except Exception as e:  # noqa: BLE001
        _out(f"WARN: _Run_Log append failed (outcome unaffected): {e}")


# --------------------------------------------------------------------------- #
# Modes                                                                       #
# --------------------------------------------------------------------------- #
def run_backup(dry_run: bool) -> int:
    _sheet_raw = _env("DEFAULT_SPREADSHEET_ID") or _env("SPREADSHEET_ID")
    _folder_raw = _env("TFB_BACKUP_FOLDER_ID")
    sheet_id = _drive_id(_sheet_raw)          # v1.1.0 [B]
    folder_id = _drive_id(_folder_raw)        # v1.1.0 [B]
    dest = _backup_dest()                     # v1.3.0 / v1.4.0
    container = _gcs_bucket() if dest == "gcs" else folder_id
    if not sheet_id:
        _out("ERROR: DEFAULT_SPREADSHEET_ID not set")
        return 2
    creds = _credentials()
    sess = _session(creds)          # service account: EXPORT + _Run_Log
    dsess = sess                    # destination session (may differ)
    if dest == "oauth":
        # v1.4.0: the operator identity owns the storage. Resolved BEFORE
        # the export so a dead grant costs 2 seconds, not 25.
        try:
            dsess = _oauth_session()
            container = drive_ensure_app_folder(dsess, _oauth_folder_name())
        except Exception as e:  # noqa: BLE001
            msg = f"[BACKUP v{__version__}] FAIL oauth setup: {e}"
            _out("ERROR: " + msg)
            append_run_log("FAIL", msg, {"stage": "oauth"})
            return 2
    # v1.1.0: preflight BEFORE the multi-second export, so a misconfigured
    # run is visible at the top of the log rather than 13 seconds in.
    _out(f"preflight: export_account={_creds_email(creds)} dest={dest} "
         f"sheet_id={sheet_id} "
         + (f"bucket={container or '(unset)'} prefix={_gcs_prefix()!r} "
            if dest == "gcs"
            else f"folder={_oauth_folder_name()!r} folder_id={container} "
            if dest == "oauth"
            else f"folder_id={container or '(unset)'} ")
         + f"dry_run={bool(dry_run)}")
    if folder_id != _folder_raw.strip():
        _out(f"NOTE: TFB_BACKUP_FOLDER_ID normalised from "
             f"{_folder_raw.strip()!r} to {folder_id!r} — set the bare id "
             f"in the repo Variable to remove this step")
    if sheet_id != _sheet_raw.strip():
        _out(f"NOTE: spreadsheet id normalised from {_sheet_raw.strip()!r} "
             f"to {sheet_id!r}")

    t0 = time.monotonic()
    data = export_xlsx(sess, sheet_id)
    sha = sha256_hex(data)[:12]
    min_bytes = _env_int("TFB_BACKUP_MIN_BYTES", 1_000_000)
    _out(f"export: {len(data):,} bytes in {time.monotonic()-t0:.1f}s "
         f"sha256={sha}")
    if len(data) < min_bytes:
        msg = (f"[BACKUP v{__version__}] FAIL export too small: "
               f"{len(data)} < {min_bytes} bytes")
        _out("ERROR: " + msg)
        append_run_log("FAIL", msg, {"bytes": len(data), "sha": sha})
        return 3

    _write_local_copy(data, sha)      # v1.5.0 [WP-0]: no-op unless TFB_BACKUP_LOCAL_COPY is set

    if dry_run:
        _out("dry-run — export verified, nothing uploaded")
        return 0
    if dest == "drive" and not folder_id:      # v1.3.0: destination-aware
        email = getattr(creds, "_tfb_client_email", "?")
        msg = (f"[BACKUP v{__version__}] FAIL TFB_BACKUP_FOLDER_ID not set — "
               f"create a Drive folder, share EDITOR with {email}, add the "
               f"repo Variable")
        _out("ERROR: " + msg)
        append_run_log("FAIL", msg, {"setup": "folder_id_missing"})
        return 2
    if dest == "gcs" and not container:                    # v1.3.0
        msg = (f"[BACKUP v{__version__}] FAIL TFB_BACKUP_DEST=gcs but "
               f"TFB_BACKUP_GCS_BUCKET not set")
        _out("ERROR: " + msg)
        append_run_log("FAIL", msg, {"setup": "gcs_bucket_missing"})
        return 2

    today = _now_riyadh().date()
    try:
        up = _store_upload(dsess, dest, container, daily_name(today), data)
    except Exception as e:  # noqa: BLE001
        email = _creds_email(creds)
        reason = _api_reason(e)          # v1.2.0: Google's own words
        _err = str(e)
        if "404" in _err:
            # Drive answers 404 for a parent this account cannot SEE,
            # which covers BOTH "not shared" and "wrong id" — say both.
            _hint = (("bucket " + repr(container) + " not found for "
                      + email + " — check the name and that the bucket "
                      "exists in this project")
                     if dest == "gcs" else
                     ("app folder " + repr(container) + " vanished between "
                      "resolution and upload — re-run; the folder is "
                      "recreated automatically")
                     if dest == "oauth" else
                     "folder_id " + repr(folder_id) + " is not visible to "
                     + email + " — either share that Drive folder with it "
                     "as EDITOR, or the id is wrong (it must be the bare "
                     "id from drive.google.com/drive/folders/<ID>, no "
                     "?usp=... tail)")
        elif "403" in _err:
            _hint = (("bucket " + repr(container) + " refused the write for "
                  + email + " — grant it the Storage Object Admin role "
                  "(roles/storage.objectAdmin) on that bucket")
                 if dest == "gcs" else
                 ("the operator grant refused the write — re-run consent "
                  "for scope drive.file and refresh "
                  "TFB_BACKUP_OAUTH_REFRESH_TOKEN")
                 if dest == "oauth"
                 else _403_remedy(reason, email, folder_id))
        else:
            _hint = "is the folder shared EDITOR with " + email + "?"
        msg = (f"[BACKUP v{__version__}] FAIL upload: {e}"
               + (f" [api_reason={reason}]" if reason else "")
               + f" — {_hint}")
        _out("ERROR: " + msg)
        append_run_log("FAIL", msg, {"stage": "upload"})
        return 3
    _out(f"uploaded {up.get('name')} id={up.get('id')}")

    listing = _store_list(dsess, dest, container)
    names = [f.get("name", "") for f in listing]

    monthly_done = ""
    if monthly_needed(today, names):
        mn = monthly_name(today)
        try:
            _store_copy(dsess, dest, container, up["id"], mn)
            monthly_done = mn
            names.append(mn)
            _out(f"monthly copy created: {mn}")
        except Exception as e:  # noqa: BLE001
            _out(f"WARN: monthly copy failed ({e}) — daily backup stands")

    keep_d = _env_int("TFB_BACKUP_KEEP_DAILY", 30)
    keep_m = _env_int("TFB_BACKUP_KEEP_MONTHLY", 12)
    _keep, delete = plan_pruning(names, keep_d, keep_m)
    by_name = {f.get("name"): f.get("id") for f in listing}
    deleted = 0
    for n in delete:
        fid = by_name.get(n)
        if not fid:
            continue
        try:
            _store_delete(dsess, dest, container, fid)
            deleted += 1
        except Exception as e:  # noqa: BLE001
            _out(f"WARN: prune failed for {n}: {e}")

    d_count = sum(1 for n in names if _DAILY_RE.match(n)) - deleted \
        if deleted else sum(1 for n in names if _DAILY_RE.match(n))
    m_count = sum(1 for n in names if _MONTHLY_RE.match(n))
    msg = (f"[BACKUP v{__version__}] name={daily_name(today)} "
           f"bytes={len(data)} sha={sha} monthly={monthly_done or '-'} "
           f"pruned={deleted} daily~{min(d_count, keep_d)}/{keep_d} "
           f"monthly~{min(m_count, keep_m)}/{keep_m}")
    _out(msg)
    append_run_log("OK", msg, {"bytes": len(data), "sha": sha,
                               "monthly": monthly_done, "pruned": deleted})
    return 0


def run_restore_test() -> int:
    """Acceptance criterion (Register §8): a backup never restored is a hope.
    Downloads the newest DAILY_, opens it, asserts the core tabs, logs."""
    folder_id = _drive_id(_env("TFB_BACKUP_FOLDER_ID"))   # v1.1.0 [B]
    dest = _backup_dest()                                 # v1.3.0 / v1.4.0
    container = _gcs_bucket() if dest == "gcs" else folder_id
    _creds = _credentials()
    sess = _session(_creds)
    if dest == "oauth":                                   # v1.4.0
        try:
            sess = _oauth_session()
            container = drive_ensure_app_folder(sess, _oauth_folder_name())
        except Exception as e:  # noqa: BLE001
            msg = f"[RESTORE-TEST v{__version__}] FAIL oauth setup: {e}"
            _out("ERROR: " + msg)
            append_run_log("FAIL", msg, {"stage": "oauth"})
            return 2
    if not container:
        _out("ERROR: " + ("TFB_BACKUP_GCS_BUCKET not set" if dest == "gcs"
                          else "TFB_BACKUP_FOLDER_ID not set"))
        return 2
    _out(f"preflight: dest={dest} container={container} mode=restore-test")
    try:
        listing = _store_list(sess, dest, container)
    except Exception as e:  # noqa: BLE001  v1.1.0: name the cause
        _r = _api_reason(e)              # v1.2.0
        msg = (f"[RESTORE-TEST v{__version__}] FAIL list: {e}"
               + (f" [api_reason={_r}]" if _r else "")
               + f" — {dest} container {container!r} not readable by "
               + _creds_email(_creds))
        _out("ERROR: " + msg)
        append_run_log("FAIL", msg, {"stage": "list"})
        return 3
    dailies = sorted((f for f in listing
                      if _DAILY_RE.match(f.get("name", ""))),
                     key=lambda f: f.get("name", ""), reverse=True)
    if not dailies:
        msg = f"[RESTORE-TEST v{__version__}] FAIL no DAILY_ backup found"
        _out("ERROR: " + msg)
        append_run_log("FAIL", msg, {})
        return 3
    target = dailies[0]
    t0 = time.monotonic()
    data = _store_download(sess, dest, container, target["id"])
    from openpyxl import load_workbook  # CI-only import
    wb = load_workbook(io.BytesIO(data), read_only=True)
    tabs = set(wb.sheetnames)
    missing = sorted(_CORE_TABS - tabs)
    dur = time.monotonic() - t0
    if missing:
        msg = (f"[RESTORE-TEST v{__version__}] FAIL {target['name']} "
               f"missing tabs: {missing}")
        _out("ERROR: " + msg)
        append_run_log("FAIL", msg, {"tabs": len(tabs), "missing": missing})
        return 3
    msg = (f"[RESTORE-TEST v{__version__}] PASS {target['name']} "
           f"bytes={len(data)} tabs={len(tabs)} core_tabs=all "
           f"open_time={dur:.1f}s")
    _out(msg)
    append_run_log("OK", msg, {"bytes": len(data), "tabs": len(tabs),
                               "seconds": round(dur, 1)})
    return 0


def _attach_error(exc) -> str:
    """v1.6.0: an error for a PUBLIC log - type, HTTP status and Google's own
    reason, never str(exc) of a library error: a failed resumable PUT renders
    its session URL (upload_id), which would let a log reader finish the
    upload. This module's own RuntimeError messages (exact type, no HTTP
    response) carry no URL and name the fix - an expired OAuth grant, a
    missing secret - and run_backup already prints them, so they are kept."""
    resp = getattr(exc, "response", None)
    code = getattr(resp, "status_code", None)
    reason = _api_reason(exc)
    own = f": {exc}" if type(exc) is RuntimeError and resp is None else ""
    return (type(exc).__name__ + own + (f" HTTP {code}" if code else "")
            + (f" [api_reason={reason}]" if reason else ""))


def _attach_date() -> _dt.date:
    """v1.6.0: the date of the export these reports describe - the
    TFB_BACKUP_LOCAL_COPY file's mtime in Riyadh when it exists (written just
    before the workbook upload names its DAILY_ file), else today in Riyadh."""
    path = _env("TFB_BACKUP_LOCAL_COPY")
    if path:
        try:
            return _dt.datetime.fromtimestamp(os.path.getmtime(path), _RIYADH).date()
        except OSError:
            pass
    return _now_riyadh().date()


def run_attach(paths: List[str]) -> int:
    """v1.6.0: upload-only. Each existing, allowed PATH goes to the backup
    destination as DAILY_TFB_<Label>_<date><ext>; no export, no workbook
    upload. Missing paths are skipped (exit 0); see the v1.6.0 header."""
    tag = f"[BACKUP-ATTACH v{__version__}]"
    # "blocked", never "refused": tfb_export_audit counts _Run_Log rows that
    # contain REFUS as identity-guard refusals.
    day = _attach_date()
    files: List[Tuple[str, bytes]] = []
    skipped: List[str] = []
    blocked: List[str] = []
    names_seen = set()
    for path in paths:
        if not os.path.isfile(path):
            _out(f"NOTE: attach: {path} not found — skipped")
            skipped.append(path)
            continue
        with open(path, "rb") as fh:
            data = fh.read()
        why = attach_refusal(path, data)
        if why:
            _out(f"WARN: attach: {path} blocked — {why}")
            blocked.append(path)
            continue
        name = attach_name(path, day)
        if name in names_seen:
            # Two inputs with one destination name would replace each other.
            _out(f"NOTE: attach: {path} -> {name} duplicates an earlier input — skipped")
            skipped.append(path)
            continue
        names_seen.add(name)
        files.append((path, data))
    if not files:
        _out(f"attach: nothing to upload (skipped={len(skipped)} "
             f"blocked={len(blocked)})")
        return 2 if blocked else 0

    # Destination: the run_restore_test composition of the run_backup helpers.
    # A setup failure is reported under the attach tag, never as an
    # "[BACKUP v...] FAIL unhandled" row that reads like a failed backup.
    folder_id = _drive_id(_env("TFB_BACKUP_FOLDER_ID"))
    dest = _backup_dest()
    container = _gcs_bucket() if dest == "gcs" else folder_id
    stage = "credentials"
    try:
        sess = _session(_credentials())
        if dest == "oauth":
            stage = "oauth"
            sess = _oauth_session()
            container = drive_ensure_app_folder(sess, _oauth_folder_name())
    except Exception as e:  # noqa: BLE001
        msg = f"{tag} FAIL {stage} setup: {_attach_error(e)}"
        _out("ERROR: " + msg)
        append_run_log("FAIL", msg, {"stage": "attach-" + stage})
        return 2
    if not container:
        msg = (f"{tag} FAIL " + ("TFB_BACKUP_GCS_BUCKET not set" if dest == "gcs"
                                 else "TFB_BACKUP_FOLDER_ID not set"))
        _out("ERROR: " + msg)
        append_run_log("FAIL", msg, {"stage": "attach-setup"})
        return 2
    _out(f"attach preflight: dest={dest} container={container} date={day} "
         f"files={len(files)} skipped={len(skipped)} blocked={len(blocked)}")

    uploaded: List[Tuple[str, Dict[str, Any]]] = []
    failed: List[str] = []
    for path, data in files:
        name = attach_name(path, day)
        mime = _ATTACH_MIME[os.path.splitext(path)[1].lower()]
        try:
            up = _store_upload(sess, dest, container, name, data, mime)
        except Exception as e:  # noqa: BLE001
            _out(f"ERROR: attach upload {path} -> {name}: {_attach_error(e)}")
            failed.append(path)
            continue
        uploaded.append((path, up))
        _out(f"attached {path} -> {up.get('name', name)} id={up.get('id')} "
             f"({len(data):,} bytes)")

    pruned = 0
    if uploaded:
        try:
            listing = _store_list(sess, dest, container)
        except Exception as e:  # noqa: BLE001
            listing = None
            _out(f"WARN: attach list failed ({_attach_error(e)}) — pruning skipped")
        if listing is not None:
            keep_d = _env_int("TFB_BACKUP_KEEP_DAILY", 30)
            keep_m = _env_int("TFB_BACKUP_KEEP_MONTHLY", 12)
            mine = {up.get("id") for _, up in uploaded}     # never prune this run's uploads
            gone = set()
            for path, up in uploaded:
                name = attach_name(path, day)
                # Same-day re-run: the new upload replaces the older copy.
                stale = [f for f in listing if f.get("name") == name
                         and f.get("id") not in mine]
                daily_re, monthly_re = attach_patterns(path)
                names = [f.get("name", "") for f in listing
                         if f.get("id") not in gone and f not in stale]
                _keep, delete = plan_pruning(names, keep_d, keep_m,
                                             daily_re, monthly_re)
                victims = stale + [f for f in listing if f.get("name") in set(delete)
                                   and f.get("id") not in mine]
                for f in victims:
                    if f.get("id") in gone:
                        continue
                    try:
                        _store_delete(sess, dest, container, f.get("id"))
                        gone.add(f.get("id"))
                        pruned += 1
                    except Exception as e:  # noqa: BLE001
                        _out(f"WARN: attach prune failed for {f.get('name')}: "
                             f"{_attach_error(e)}")

    msg = (f"{tag} uploaded={len(uploaded)}/{len(paths)} "
           f"skipped={len(skipped)} blocked={len(blocked)} failed={len(failed)} "
           f"pruned={pruned} names="
           + ",".join(attach_name(p, day) for p, _ in uploaded))
    _out(msg)
    append_run_log("FAIL" if failed or blocked else "OK", msg,
                   {"uploaded": len(uploaded), "skipped": skipped,
                    "blocked": blocked, "failed": failed, "pruned": pruned})
    if failed:
        return 3
    return 2 if blocked else 0


# --------------------------------------------------------------------------- #
# Offline selftest                                                            #
# --------------------------------------------------------------------------- #
def _selftest() -> int:
    checks: List[Tuple[str, bool]] = []
    d = _dt.date(2026, 7, 22)
    checks.append(("names deterministic",
                   daily_name(d) == "DAILY_TFB_Workbook_2026-07-22.xlsx"
                   and monthly_name(d) == "MONTHLY_TFB_Workbook_2026-07.xlsx"))
    names = ([daily_name(_dt.date(2026, 7, 1) + _dt.timedelta(days=i))
              for i in range(35)]
             + [f"MONTHLY_TFB_Workbook_2025-{m:02d}.xlsx"
                for m in range(1, 13)]
             + ["MONTHLY_TFB_Workbook_2026-06.xlsx",
                "MONTHLY_TFB_Workbook_2026-07.xlsx",
                "operator_notes.txt"])
    keep, delete = plan_pruning(names, 30, 12)
    checks.append(("prune: 30 newest dailies kept",
                   sum(1 for n in keep if _DAILY_RE.match(n)) == 30
                   and daily_name(_dt.date(2026, 8, 4)) in keep
                   and daily_name(_dt.date(2026, 7, 1)) in delete))
    checks.append(("prune: 12 newest monthlies kept, oldest dropped",
                   sum(1 for n in keep if _MONTHLY_RE.match(n)) == 12
                   and "MONTHLY_TFB_Workbook_2025-01.xlsx" in delete
                   and "MONTHLY_TFB_Workbook_2025-02.xlsx" in delete
                   and "MONTHLY_TFB_Workbook_2026-07.xlsx" in keep))
    checks.append(("prune: foreign files untouched",
                   "operator_notes.txt" not in keep + delete))
    checks.append(("monthly_needed: due once per month",
                   monthly_needed(d, ["MONTHLY_TFB_Workbook_2026-06.xlsx"])
                   and not monthly_needed(
                       d, ["MONTHLY_TFB_Workbook_2026-07.xlsx"])))
    h1, h2 = sha256_hex(b"tfb"), sha256_hex(b"tfb")
    checks.append(("sha stable 64-hex", h1 == h2 and len(h1) == 64))
    checks.append(("env int guard", _env_int("TFB_NOPE_X", 30) == 30))
    # v1.6.0 --attach
    checks.append(("attach names follow daily_name",
                   attach_name("out/dq_scorecard.md", d)
                   == "DAILY_TFB_DQ_Scorecard_2026-07-22.md"
                   and attach_name("dq_activation_receipt.json", d)
                   == "DAILY_TFB_DQ_Activation_Receipt_2026-07-22.json"))
    a_daily, a_monthly = attach_patterns("dq_report.json")
    a_names = ([attach_name("dq_report.json", d - _dt.timedelta(days=i))
                for i in range(33)]
               + [attach_name("dq_scorecard.md", d), daily_name(d)])
    a_keep, a_delete = plan_pruning(a_names, 30, 12, a_daily, a_monthly)
    checks.append(("attach prune: per kind, workbook and other kinds untouched",
                   len(a_keep) == 30 and len(a_delete) == 3
                   and attach_name("dq_report.json", d) in a_keep
                   and daily_name(d) not in a_keep + a_delete
                   and attach_name("dq_scorecard.md", d) not in a_keep + a_delete))
    checks.append(("plan_pruning default patterns unchanged",
                   plan_pruning(names, 30, 12)
                   == plan_pruning(names, 30, 12, _DAILY_RE, _MONTHLY_RE)))
    checks.append(("attach refuses xlsx/zip, credentials, unknown types",
                   attach_refusal("a.json", b"PK\x03\x04...") != ""
                   and attach_refusal("a.xlsx", b"x") != ""
                   and attach_refusal("k.json", b'{"private_key": "x"}') != ""
                   and attach_refusal("ok.md", b"# DQ scorecard") == ""))
    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"[workbook_backup v{__version__}] SELFTEST {passed}/{len(checks)}")
    return 0 if passed == len(checks) else 1


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="TFB nightly workbook backup")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--restore-test", action="store_true")
    g.add_argument("--selftest", action="store_true")
    g.add_argument("--attach", nargs="+", metavar="PATH",
                   help="v1.6.0: upload-only — put these small reports "
                        "beside the backups (no export, no workbook upload)")
    args = ap.parse_args(argv)
    if args.selftest:
        return _selftest()
    try:
        if args.attach:
            return run_attach(args.attach)
        if args.restore_test:
            return run_restore_test()
        return run_backup(dry_run=bool(args.dry_run))
    except Exception as e:  # noqa: BLE001
        msg = f"[BACKUP v{__version__}] FAIL unhandled: {type(e).__name__}: {e}"
        _out("ERROR: " + msg)
        append_run_log("FAIL", msg, {"stage": "unhandled"})
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
