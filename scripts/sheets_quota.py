#!/usr/bin/env python3
"""
scripts/sheets_quota.py - wait out the Google Sheets per-minute read quota
==========================================================================
VERSION 1.0.1 (2026-11-14) - THE GOOGLE API CLIENT'S 429 IS THE QUOTA TOO
The Provider Target Coverage Alert reads through the Google API client, whose
quota answer is "<HttpError 429 when requesting ...>" with the status on
e.resp.status, and 1.0.0 did not recognise it (the monitor failed red on
11, 12 and 13 Nov). is_quota() now also reads e.resp.status and that text.

VERSION 1.0.0 (2026-11-13) - NEW SCRIPT

WHY (owner, Fri 13 Nov 2026 19:12 Riyadh: "make full check ... another
improvement"; the same day's project review listed the Sheets read-quota
failures). In the hours before, three scheduled runs ended red on Sheets
HTTP 429 ("Read requests per minute per user"):
  Exit Plan       16:21 and 17:41 Riyadh - it retried twice, 20 s apart, so
                  all three tries fell inside the same quota minute;
  Evidence Board  17:42 Riyadh - it did not retry at all.
Several page jobs start within the same minutes and share one service
account, so the quota is reached whenever they overlap.

WHAT: two small pure helpers, used by the jobs that read the workbook.
  is_quota(e)   True for the per-minute quota answer (HTTP 429).
  call(fn, ...) run fn; after a quota answer wait WAITS[n] seconds (65, 65 -
                each wait crosses into a new quota minute) and run it again,
                at most `retries` more times. Every other error is raised at
                once, unchanged.
It reads nothing and writes nothing by itself.
"""
from __future__ import annotations

from typing import Any, Callable, Optional

__version__ = "1.0.1"

WAITS = (65.0, 65.0)                       # seconds; the quota is counted per minute
RETRIES = 2


def is_quota(e: BaseException) -> bool:
    """True for the Sheets per-minute quota answer (HTTP 429)."""
    code = getattr(getattr(e, "response", None), "status_code", None)
    status = getattr(getattr(e, "resp", None), "status", None)          # 1.0.1: googleapiclient HttpError
    s = "%s %s" % (type(e).__name__, e)
    return (code == 429 or str(status) == "429" or "[429]" in s or "HttpError 429" in s or "Quota exceeded" in s
            or "RESOURCE_EXHAUSTED" in s)


def wait_for(e: BaseException, attempt: int) -> Optional[float]:
    """Seconds to wait after the `attempt`-th failed try (1-based) when it was the quota answer, else None."""
    if not is_quota(e):
        return None
    return WAITS[min(max(attempt, 1), len(WAITS)) - 1]


def call(fn: Callable[[], Any], sleep: Callable[[float], None], say: Callable[[str], None], what: str,
         retries: int = RETRIES) -> Any:
    """Run fn; wait out a quota answer and run it again, at most `retries` more times."""
    n = 0
    while True:
        try:
            return fn()
        except Exception as e:  # noqa: BLE001 - re-raised unless it is the quota answer and tries remain
            wait = wait_for(e, n + 1)
            if wait is None or n >= retries:
                raise
            n += 1
            say("Sheets quota reached while %s: waiting %d s, then try %d of %d" % (what, wait, n, retries))
            sleep(wait)
