#!/usr/bin/env python3
"""
scripts/sheets_backoff.py - every request of a page job waits out the Sheets quota
==================================================================================
VERSION 1.0.0 (2026-11-25) - NEW SCRIPT
WHY (full audit of 25 Nov 2026): in the 36 hours before, five hourly page runs
ended red on Sheets HTTP 429 "Read requests per minute per user":
  Evidence Board  24 Nov 13:06 UTC - one second after start: the workbook OPEN
                  (spreadsheet metadata) was never retried by any job;
  Trial Labels    24 Nov 14:42 - both of its 65 s retries met the quota again;
  News Now        24 Nov 13:42 and 16:42;
  Holding Read    23 Nov 21:42.
They all start in the same minute after the hourly Exit Plan, share one
service account, and retried on the same 65 s beat, so they met each other
again a minute later.
WHAT: quota_http_client() returns a gspread HTTP client class that waits out a
quota answer on EVERY request it sends - the open included - CLIENT_WAITS
(65, 65, 65) seconds plus a random 0-20 s, so jobs that collided spread apart.
Only the quota answer (sheets_quota.is_quota: HTTP 429) is retried: Google
refuses such a request before carrying it out, so sending it again cannot
write twice. Every other answer, a server fault (5xx) included, is raised
unchanged - a write is never repeated on a guess.
Used by scripts/run_calendar_sync.py 1.3.1 _open_book, the opener of 15 page
jobs. Rollback: TFB_SHEETS_BACKOFF=off in a job's env (the plain client), or
revert. scripts/sheets_quota.py (pure, no client) is unchanged.
"""
from __future__ import annotations

import os
import random
import sys
import time
from typing import Any, Callable

from scripts.sheets_quota import is_quota

__version__ = "1.0.0"

CLIENT_WAITS = (65.0, 65.0, 65.0)          # seconds before each further try; the quota is counted per minute
CLIENT_JITTER = 20.0                       # plus up to this many seconds, so jobs that collided spread apart
_SLEEP: Callable[[float], None] = time.sleep
_RANDOM: Callable[[], float] = random.random


def _say(msg: str) -> None:
    sys.stderr.write("[sheets_backoff v%s] %s\n" % (__version__, msg))


def backoff_on() -> bool:
    """TFB_SHEETS_BACKOFF = off / 0 / false / no restores the plain gspread client."""
    return os.environ.get("TFB_SHEETS_BACKOFF", "").strip().lower() not in ("off", "0", "false", "no")


def quota_http_client() -> Any:
    """A gspread HTTPClient subclass that waits out the per-minute quota (HTTP 429) on every request and tries
    again, at most len(CLIENT_WAITS) more times; every other answer is raised at once, unchanged."""
    from gspread.http_client import HTTPClient  # local import: CI only

    class QuotaBackOffHTTPClient(HTTPClient):
        def request(self, *args: Any, **kwargs: Any) -> Any:
            n = 0
            while True:
                try:
                    return super().request(*args, **kwargs)
                except Exception as e:  # noqa: BLE001 - re-raised unless it is the quota answer and tries remain
                    if not is_quota(e) or n >= len(CLIENT_WAITS):
                        raise
                    wait = CLIENT_WAITS[n] + CLIENT_JITTER * _RANDOM()
                    n += 1
                    _say("Sheets quota reached: waiting %d s, then try %d of %d" % (wait, n, len(CLIENT_WAITS)))
                    _SLEEP(wait)

    return QuotaBackOffHTTPClient
