# Manual Refresh Priority v1

Status: **Reviewed source / not deployed to the bound Google Apps Script project**  
Owner activation: **Source approved; live deployment still requires bound-script integration, simulation evidence, and a second deployment review**

## Problem

The workbook's automatic refresh/resume activity can hold the refresh lock almost
continuously. A menu-triggered manual refresh then receives `lock busy`, queues
behind repeated automatic continuation runs, or never obtains a clean window.

The required behavior is:

1. user requests Manual Refresh;
2. automatic refresh pauses/yields at the next safe page or batch boundary;
3. manual refresh runs alone;
4. automatic refresh resumes after manual completion;
5. no duplicate time-driven triggers are created;
6. an error never leaves automation paused forever;
7. an old cleanup can never erase a newer manual-priority request.

## Platform limitation

Google Apps Script cannot forcibly terminate an execution that is already running.
The safe implementation is cooperative. The automatic loop must check a pause flag
between completed pages/batches and exit before starting the next destructive or
long-running step.

## Added source

`apps_script/11_Manual_Refresh_Coordinator.gs`

The coordinator is inert until the existing bound Apps Script entrypoints call it.
The repository currently does not contain the live bound Apps Script sources, so
this PR cannot by itself change the Google Sheet menu or triggers.

### Coordinator locking and lease model — v1.1.0

Two independent lock domains are used deliberately:

- `ScriptLock` protects the long-running automatic or manual refresh execution;
- `DocumentLock` protects only short coordinator transactions: pause ownership,
  compare-and-delete expiry cleanup, deferred-trigger lookup/creation, tracked
  trigger-ID persistence, and one-shot resume deduplication.

Each manual request receives an ownership ID. A finishing or failed execution may
clear only the same request ID. If a newer manual request replaced it, the old
cleanup records the mismatch and leaves the newer request intact.

Deferred-trigger lookup, creation, and saved trigger-ID update occur inside one
`DocumentLock` transaction. Two simultaneous manual clicks therefore cannot both
observe “no trigger” and create duplicate one-shot triggers.

## Integration into the live bound Apps Script

### 1. Add the coordinator file

Copy `apps_script/11_Manual_Refresh_Coordinator.gs` into the spreadsheet's bound
Apps Script project.

### 2. Make the manual handler checkpoint-capable

Rename the current manual refresh body to a private core function and accept the
owner-bound lease context supplied by the coordinator. JavaScript functions that
ignore the extra argument remain compatible, but an hours-long refresh must either:

- call `lease.renew(label)` only after a completed write, verification, and saved
  checkpoint; or
- return the canonical persisted-partial shape shown below so the coordinator
  renews the lease and queues its owned one-shot continuation.

```javascript
function refreshAllDataCore_(lease) {
  var result = refreshPageInBatchesCore_(/* existing arguments */);

  // The canonical batch helper returns this only after the batch is written,
  // verified, and checkpointed. Return it unchanged to activate continuation.
  if (result && result.partial === true && result.paused === true) {
    return result; // includes nextIndex and totalSymbols
  }

  // A custom multi-step loop may instead renew at its own proven safe boundary:
  // lease.renew('after-persisted-batch:' + completedBatchNumber);
  return result;
}
```

Do not renew after clearing a page and before completing its write. Do not return
`partial/paused` before the resume cursor or checkpoint has been saved.

Set this Script Property:

```text
TFB_MANUAL_REFRESH_HANDLER = refreshAllDataCore_
```

Change the menu item to call:

```text
tfbManualRefresh
```

Do not configure `TFB_MANUAL_REFRESH_HANDLER=tfbManualRefresh`; the coordinator
rejects that recursive configuration. Handler existence is validated before any
pause state is claimed.

### 3. Wrap every automatic entrypoint

Example for the scheduled entrypoint:

```javascript
function tfbAutoRefresh() {
  return tfbRunAutomaticRefresh_(function (shouldYield) {
    return tfbAutoRefreshCore_(shouldYield);
  }, 'scheduled');
}
```

The old body becomes:

```javascript
function tfbAutoRefreshCore_(shouldYield) {
  for (var i = 0; i < pages.length; i++) {
    if (shouldYield('before-page:' + pages[i])) {
      saveResumeCursor_(i);
      return {status: 'yielded_manual_priority', nextIndex: i};
    }

    refreshOnePage_(pages[i]);

    if (shouldYield('after-page:' + pages[i])) {
      saveResumeCursor_(i + 1);
      return {status: 'yielded_manual_priority', nextIndex: i + 1};
    }
  }
}
```

Apply the same wrapper to automatic continuation/resume handlers, including the
live function currently named `tfbAutoResumeOnOpen` if it can launch refresh work.

### 4. Safe yield placement

A yield point belongs:

- after a completed page write;
- after a completed batch and saved cursor;
- before beginning a new fetch/clear/write transaction.

Never yield:

- after clearing a page but before writing;
- while a batch write is partially complete;
- before saving the resume cursor;
- while manual operator values are temporarily staged.

### 5. Manual continuation and automatic resume

When the configured manual handler returns a persisted partial result containing
`partial=true` or `paused=true` plus valid `nextIndex < totalSymbols`, the
coordinator:

1. renews the same request-owned idle lease;
2. queues one coordinator-owned `tfbManualRefreshDeferred_` trigger;
3. retains the pause through cleanup;
4. consumes that exact trigger ID before the next batch; and
5. stops rather than reviving the request if the lease expired or hit its hard
   ceiling.

Normal time-driven automatic triggers remain installed. They fire as usual but
skip while the manual pause is active. When the manual refresh finally completes,
its pause is cleared in isolated `finally` cleanup, so the next scheduled trigger
continues automatically.

Optionally set:

```text
TFB_AUTO_RESUME_HANDLER = <existing safe one-shot resume handler>
```

The coordinator then creates at most one one-shot resume trigger after manual
completion. Leave the property blank when the normal scheduled trigger is enough.

Do not point this property to `tfbManualRefresh` or
`tfbManualRefreshDeferred_`.

## Why triggers are not deleted

Deleting every automatic trigger and recreating it is unsafe because Apps Script
does not expose enough scheduling detail to reproduce every existing trigger
exactly. It also risks duplicates, missed schedules and authorization failures.
The coordinator therefore keeps triggers installed and uses a TTL-backed pause.

The only trigger it deletes is its own deduplicated deferred-manual one-shot
trigger, identified by its saved unique ID. Trigger enumeration, creation, tracked
ID persistence, and deletion are serialized by the coordinator `DocumentLock`.

## Failure handling

- If an automatic run still owns `ScriptLock`, manual refresh waits up to 25
  seconds and then queues one deferred attempt.
- Simultaneous queue attempts share one atomically created deferred trigger.
- The pause is a renewable 20-minute idle lease, capped at six hours from the
  immutable original request time; an abandoned or stuck request therefore
  self-expires and cannot be renewed forever.
- Expiry cleanup uses compare-and-delete: it clears only the exact stored expiry
  value and request ID that were read as stale.
- Final cleanup clears only the request ID owned by the finishing execution.
- Pause clearing occurs before fallible trigger cleanup or resume scheduling.
- `tfbClearStaleManualRefreshPause()` is the owner-only force reset.
- `tfbManualRefreshStatus()` reports the current pause request ID, reason,
  remaining TTL, deferred trigger ID, and last coordinator event.

## Required verification before deployment

1. Manual click during idle: manual starts immediately; next automatic cycle runs.
2. Manual click during automatic page 1: automatic completes the safe page,
   records cursor and yields; manual runs; automation resumes from cursor.
3. Two simultaneous manual clicks while automatic holds `ScriptLock`: one deferred
   trigger exists, no concurrent manual executions occur, and repeated clicks are
   reported as already queued/running.
4. Expired-pause race: replace an expired request with a new request while an
   automatic read is cleaning the old value; the new request remains present.
5. Newer-request race: create a second request before the first manual execution
   exits; first-execution cleanup must not clear the newer request ID.
6. Manual handler throws: the owned pause clears and the automatic schedule remains
   active.
7. Deferred-trigger enumeration or deletion throws: pause clearing has already run
   and automation is not suppressed until TTL expiry.
8. Checkpoint continuation: a handler returns a persisted partial result,
   the lease is renewed, exactly one owned continuation trigger is queued, the
   next invocation resumes, and final completion clears the owned pause.
9. Expired continuation: after idle expiry or the six-hour ceiling, the deferred
   trigger does not create a new request and performs no write.
10. Automatic trigger fires during manual: it records
   `skipped_manual_priority` and performs no write.
11. No clear-without-write state under forced yield testing.
12. Existing trigger count is unchanged except for one temporary
    coordinator-owned deferred trigger when needed.

## Production acceptance

- three successful manual-during-auto simulations;
- zero lost rows;
- zero duplicate triggers under simultaneous-click testing;
- zero newer request IDs deleted by stale or final cleanup;
- zero `lockBusySkips` for the manual request after cooperative integration;
- `_Run_Log` distinguishes manual request, automatic yield, manual completion and
  automatic resume;
- owner and second reviewer approve the live deployment.
