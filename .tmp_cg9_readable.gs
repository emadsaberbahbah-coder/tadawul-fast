// TADAWUL FAST BRIDGE — MANUAL REFRESH PRIORITY COORDINATOR v1.1.0
//
// v1.1.0 — WHY: RENEWABLE MANUAL-PAUSE IDLE LEASE (CG-9, 2026-08-01)
// -----------------------------------------------------------------------------
// Root cause / live evidence: Global_Markets manual refreshes can run for hours;
// one live run remained at 1400/6190 while MANUAL_PAUSE_TTL_MS was a one-time
// 20-minute expiry. The pause could therefore lapse while the manual runner was
// still active, allowing an automatic execution to collide with the same work.
//
// Fix: the 20-minute pause is now a renewable idle lease. A long-running manual
// page/batch loop renews only between fully completed, safely persisted batches.
// The original request start is immutable and caps total pause time at six hours,
// so a crashed runner cannot keep automation paused forever.
//
// Blast radius: coordinator pause-state and diagnostics only. No provider, score,
// ranking, recommendation, portfolio, Sheet-write implementation, trigger
// installation, or live Apps Script deployment changes.
//
// Reversibility: stop using the lease context / checkpoint-continuation path to
// retain the prior fixed-idle-TTL behavior. Emergency clear, unique-trigger
// ownership, cleanup ordering, and automatic yield points remain unchanged.
//
// P1 REVIEW HARDENING: the configured handler now receives an owner-bound lease
// context. When the handler returns a persisted partial/checkpoint result, this
// coordinator renews the lease and queues the next owned one-shot invocation.
// The renewal therefore executes at a proven safe boundary rather than existing
// only as documentation. JavaScript handlers that ignore extra arguments remain
// compatible; checkpointed handlers gain automatic continuation.
// Historical race-hardened assignment: TFB_REFRESH_COORDINATOR_VERSION = '1.0.2'.
//
// Purpose
// -------
// Manual refresh must take priority over scheduled/automatic refresh without
// deleting unrelated triggers, creating duplicate deferred triggers, or leaving
// automation paused.
//
// IMPORTANT: Google Apps Script cannot forcibly kill an execution that is already
// running. The safe design is cooperative:
//   1) validate the manual handler before claiming priority;
//   2) serialize pause ownership and one-shot trigger creation with DocumentLock;
//   3) let an automatic run yield only at a completed page/batch boundary;
//   4) run manual work alone under ScriptLock;
//   5) clear only the pause request owned by the finishing execution;
//   6) isolate cleanup so trigger failure cannot prevent pause clearing.
//
// This file is inert until the existing bound Apps Script entrypoints call the
// wrapper functions documented in docs/MANUAL_REFRESH_PRIORITY_V1.md.

var TFB_REFRESH_COORDINATOR_VERSION = '1.1.0';

var TFB_REFRESH_COORDINATOR_ = Object.freeze({
  PROP_MANUAL_UNTIL_MS: 'TFB_MANUAL_REFRESH_UNTIL_MS',
  PROP_MANUAL_REQUEST_ID: 'TFB_MANUAL_REFRESH_REQUEST_ID',
  PROP_MANUAL_REQUESTED_AT: 'TFB_MANUAL_REFRESH_REQUESTED_AT',
  PROP_MANUAL_REQUESTED_BY: 'TFB_MANUAL_REFRESH_REQUESTED_BY',
  PROP_MANUAL_REASON: 'TFB_MANUAL_REFRESH_REASON',
  PROP_MANUAL_HANDLER: 'TFB_MANUAL_REFRESH_HANDLER',
  PROP_AUTO_RESUME_HANDLER: 'TFB_AUTO_RESUME_HANDLER',
  PROP_DEFERRED_TRIGGER_ID: 'TFB_MANUAL_DEFERRED_TRIGGER_ID',
  PROP_LAST_EVENT: 'TFB_REFRESH_COORDINATOR_LAST_EVENT',
  MANUAL_PAUSE_TTL_MS: 20 * 60 * 1000,
  MANUAL_PAUSE_MAX_TOTAL_MS: 6 * 60 * 60 * 1000,
  MANUAL_LOCK_WAIT_MS: 25 * 1000,
  AUTO_LOCK_WAIT_MS: 1000,
  COORDINATOR_LOCK_WAIT_MS: 5000,
  DEFERRED_DELAY_MS: 60 * 1000,
  AUTO_RESUME_DELAY_MS: 60 * 1000
});

function tfbNowMs_() {
  return Date.now();
}

function tfbScriptProperties_() {
  return PropertiesService.getScriptProperties();
}

function tfbNewManualRequestId_() {
  try {
    return Utilities.getUuid();
  } catch (err) {
    return String(tfbNowMs_()) + '-' + String(Math.random()).slice(2);
  }
}

function tfbSafeUserEmail_() {
  try {
    return Session.getActiveUser().getEmail() || 'unknown';
  } catch (err) {
    return 'unknown';
  }
}

/**
 * DocumentLock is deliberately separate from ScriptLock. Automatic/manual work
 * may hold ScriptLock for a long time; the coordinator lock protects only short
 * property and trigger ownership transactions.
 */
function tfbWithCoordinatorLock_(label, callback) {
  if (typeof callback !== 'function') {
    throw new Error('tfbWithCoordinatorLock_ requires a callback');
  }
  var lock = LockService.getDocumentLock();
  if (!lock) {
    throw new Error('DocumentLock unavailable; coordinator requires a bound script');
  }
  if (!lock.tryLock(TFB_REFRESH_COORDINATOR_.COORDINATOR_LOCK_WAIT_MS)) {
    throw new Error('Coordinator lock busy: ' + String(label || 'unknown'));
  }
  try {
    return callback();
  } finally {
    lock.releaseLock();
  }
}

function tfbRecordRefreshEvent_(eventName, detail) {
  var payload = {
    version: TFB_REFRESH_COORDINATOR_VERSION,
    event: String(eventName || 'unknown'),
    detail: String(detail || ''),
    at: new Date().toISOString()
  };
  try {
    tfbScriptProperties_().setProperty(
      TFB_REFRESH_COORDINATOR_.PROP_LAST_EVENT,
      JSON.stringify(payload)
    );
  } catch (err) {
    console.warn('[REFRESH-COORDINATOR] unable to record event: ' + err);
  }
  console.log('[REFRESH-COORDINATOR] ' + JSON.stringify(payload));
}

function tfbInactivePause_() {
  return {
    active: false,
    requestId: '',
    untilMs: 0,
    remainingMs: 0,
    startedAtMs: 0,
    maxUntilMs: 0,
    ceilingRemainingMs: 0,
    ceilingReached: false,
    reason: '',
    requestedBy: ''
  };
}

function tfbDeleteManualPauseUnlocked_(props) {
  props.deleteProperty(TFB_REFRESH_COORDINATOR_.PROP_MANUAL_UNTIL_MS);
  props.deleteProperty(TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUEST_ID);
  props.deleteProperty(TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUESTED_AT);
  props.deleteProperty(TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUESTED_BY);
  props.deleteProperty(TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REASON);
}

/**
 * Resolve the immutable start of the current request while the caller owns the
 * coordinator DocumentLock. Existing v1.0.2 states already carry the ISO
 * PROP_MANUAL_REQUESTED_AT value, so they migrate without a new property.
 */
function tfbManualPauseStartMsUnlocked_(props, requestId, nowMs) {
  var requestedId = String(requestId || '');
  var currentId = props.getProperty(
    TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUEST_ID
  ) || '';
  var rawStartedAt = props.getProperty(
    TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUESTED_AT
  ) || '';
  var parsedStartedAt = Date.parse(rawStartedAt);
  if (requestedId && currentId === requestedId && isFinite(parsedStartedAt)) {
    return parsedStartedAt;
  }
  return Number(nowMs || tfbNowMs_());
}

function tfbWriteManualPauseUnlocked_(props, reason, requestId) {
  var nowMs = tfbNowMs_();
  var requestIdText = String(requestId || '');
  var previousId = props.getProperty(
    TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUEST_ID
  ) || '';
  var startedAtMs = tfbManualPauseStartMsUnlocked_(
    props,
    requestIdText,
    nowMs
  );
  var maxUntilMs = startedAtMs +
    TFB_REFRESH_COORDINATOR_.MANUAL_PAUSE_MAX_TOTAL_MS;
  var untilMs = Math.min(
    nowMs + TFB_REFRESH_COORDINATOR_.MANUAL_PAUSE_TTL_MS,
    maxUntilMs
  );

  if (untilMs <= nowMs) {
    if (previousId === requestIdText) {
      tfbDeleteManualPauseUnlocked_(props);
    }
    return {
      requestId: requestIdText,
      untilMs: nowMs,
      startedAtMs: startedAtMs,
      maxUntilMs: maxUntilMs,
      ceilingRemainingMs: 0,
      ceilingReached: true,
      expired: true
    };
  }

  var requestedBy = previousId === requestIdText
    ? (props.getProperty(TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUESTED_BY) || '')
    : '';
  var values = {};
  values[TFB_REFRESH_COORDINATOR_.PROP_MANUAL_UNTIL_MS] = String(untilMs);
  values[TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUEST_ID] = requestIdText;
  values[TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUESTED_AT] =
    new Date(startedAtMs).toISOString();
  values[TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUESTED_BY] =
    requestedBy || tfbSafeUserEmail_();
  values[TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REASON] =
    String(reason || 'manual-refresh');
  props.setProperties(values, false);
  return {
    requestId: requestIdText,
    untilMs: untilMs,
    startedAtMs: startedAtMs,
    maxUntilMs: maxUntilMs,
    ceilingRemainingMs: Math.max(0, maxUntilMs - nowMs),
    ceilingReached: untilMs >= maxUntilMs,
    expired: false
  };
}

/** Clear an expired pause only when the stored instance is still the one read. */
function tfbClearExpiredManualPauseIfMatch_(expectedRaw, expectedRequestId) {
  var cleared = tfbWithCoordinatorLock_('clear-expired-pause', function() {
    var props = tfbScriptProperties_();
    var currentRaw = props.getProperty(TFB_REFRESH_COORDINATOR_.PROP_MANUAL_UNTIL_MS) || '';
    var currentRequestId = props.getProperty(
      TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUEST_ID
    ) || '';

    if (currentRaw !== String(expectedRaw || '') ||
        currentRequestId !== String(expectedRequestId || '')) {
      return false;
    }

    var nowMs = tfbNowMs_();
    var currentUntilMs = Number(currentRaw || 0);
    var startedAtMs = tfbManualPauseStartMsUnlocked_(
      props,
      currentRequestId,
      nowMs
    );
    var maxUntilMs = startedAtMs +
      TFB_REFRESH_COORDINATOR_.MANUAL_PAUSE_MAX_TOTAL_MS;
    var effectiveUntilMs = Math.min(currentUntilMs, maxUntilMs);
    if (isFinite(effectiveUntilMs) && effectiveUntilMs > nowMs) {
      return false;
    }

    tfbDeleteManualPauseUnlocked_(props);
    return true;
  });

  if (cleared) {
    tfbRecordRefreshEvent_(
      'manual-pause-expired',
      'requestId=' + String(expectedRequestId || '')
    );
  }
  return cleared;
}

function tfbReadManualPause_() {
  // Re-read when compare-and-delete reports that another invocation replaced the
  // stale instance. This prevents an old read from clearing or masking a new
  // manual request.
  for (var attempt = 0; attempt < 3; attempt++) {
    var props = tfbScriptProperties_();
    var raw = props.getProperty(TFB_REFRESH_COORDINATOR_.PROP_MANUAL_UNTIL_MS) || '';
    var requestId = props.getProperty(
      TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUEST_ID
    ) || '';
    var untilMs = Number(raw || 0);
    var nowMs = tfbNowMs_();
    var startedAtMs = tfbManualPauseStartMsUnlocked_(props, requestId, nowMs);
    var maxUntilMs = startedAtMs +
      TFB_REFRESH_COORDINATOR_.MANUAL_PAUSE_MAX_TOTAL_MS;
    var effectiveUntilMs = Math.min(untilMs, maxUntilMs);

    if (isFinite(effectiveUntilMs) && effectiveUntilMs > nowMs) {
      return {
        active: true,
        requestId: requestId,
        untilMs: effectiveUntilMs,
        remainingMs: Math.max(0, effectiveUntilMs - nowMs),
        startedAtMs: startedAtMs,
        maxUntilMs: maxUntilMs,
        ceilingRemainingMs: Math.max(0, maxUntilMs - nowMs),
        ceilingReached: effectiveUntilMs >= maxUntilMs,
        reason: props.getProperty(TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REASON) || '',
        requestedBy: props.getProperty(
          TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUESTED_BY
        ) || ''
      };
    }

    if (!raw) {
      return tfbInactivePause_();
    }

    if (tfbClearExpiredManualPauseIfMatch_(raw, requestId)) {
      return tfbInactivePause_();
    }
    // State changed while the stale instance was being checked; loop and read it.
  }

  // Conservative final read: if a valid request is now active, block automatic
  // work. Otherwise leave any rapidly changing state untouched and report idle.
  var latestProps = tfbScriptProperties_();
  var latestRaw = latestProps.getProperty(
    TFB_REFRESH_COORDINATOR_.PROP_MANUAL_UNTIL_MS
  ) || '';
  var latestRequestId = latestProps.getProperty(
    TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUEST_ID
  ) || '';
  var latestNowMs = tfbNowMs_();
  var latestUntilMs = Number(latestRaw || 0);
  var latestStartedAtMs = tfbManualPauseStartMsUnlocked_(
    latestProps,
    latestRequestId,
    latestNowMs
  );
  var latestMaxUntilMs = latestStartedAtMs +
    TFB_REFRESH_COORDINATOR_.MANUAL_PAUSE_MAX_TOTAL_MS;
  var latestEffectiveUntilMs = Math.min(latestUntilMs, latestMaxUntilMs);
  if (isFinite(latestEffectiveUntilMs) && latestEffectiveUntilMs > latestNowMs) {
    return {
      active: true,
      requestId: latestRequestId,
      untilMs: latestEffectiveUntilMs,
      remainingMs: Math.max(0, latestEffectiveUntilMs - latestNowMs),
      startedAtMs: latestStartedAtMs,
      maxUntilMs: latestMaxUntilMs,
      ceilingRemainingMs: Math.max(0, latestMaxUntilMs - latestNowMs),
      ceilingReached: latestEffectiveUntilMs >= latestMaxUntilMs,
      reason: latestProps.getProperty(TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REASON) || '',
      requestedBy: latestProps.getProperty(
        TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUESTED_BY
      ) || ''
    };
  }
  return tfbInactivePause_();
}

/** Atomically create one manual-priority request; repeated clicks are deduped. */
function tfbClaimManualPause_(reason) {
  var claim = tfbWithCoordinatorLock_('claim-manual-pause', function() {
    var props = tfbScriptProperties_();
    var nowMs = tfbNowMs_();
    var currentId = props.getProperty(
      TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUEST_ID
    ) || '';
    var currentUntilMs = Number(
      props.getProperty(TFB_REFRESH_COORDINATOR_.PROP_MANUAL_UNTIL_MS) || 0
    );
    var currentStartedAtMs = tfbManualPauseStartMsUnlocked_(
      props,
      currentId,
      nowMs
    );
    var currentMaxUntilMs = currentStartedAtMs +
      TFB_REFRESH_COORDINATOR_.MANUAL_PAUSE_MAX_TOTAL_MS;
    var currentEffectiveUntilMs = Math.min(currentUntilMs, currentMaxUntilMs);
    if (isFinite(currentEffectiveUntilMs) && currentEffectiveUntilMs > nowMs) {
      return {
        claimed: false,
        requestId: currentId,
        untilMs: currentEffectiveUntilMs,
        startedAtMs: currentStartedAtMs,
        maxUntilMs: currentMaxUntilMs,
        reason: props.getProperty(TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REASON) || ''
      };
    }

    tfbDeleteManualPauseUnlocked_(props);
    var requestId = tfbNewManualRequestId_();
    var state = tfbWriteManualPauseUnlocked_(props, reason, requestId);
    state.claimed = true;
    state.reason = String(reason || 'manual-refresh');
    return state;
  });

  tfbRecordRefreshEvent_(
    claim.claimed ? 'manual-pause-claimed' : 'manual-pause-deduplicated',
    'requestId=' + String(claim.requestId || '') +
      ' reason=' + String(claim.reason || '')
  );
  return claim;
}

/** Extend only the currently owned request; never overwrite a newer request. */
function tfbExtendManualPause_(reason, requestId) {
  var extended = tfbWithCoordinatorLock_('extend-manual-pause', function() {
    var props = tfbScriptProperties_();
    var currentId = props.getProperty(
      TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUEST_ID
    ) || '';
    if (currentId && currentId !== String(requestId || '')) {
      return false;
    }
    var state = tfbWriteManualPauseUnlocked_(props, reason, requestId);
    return !state.expired;
  });

  tfbRecordRefreshEvent_(
    extended ? 'manual-pause-extended' : 'manual-pause-extension-skipped',
    'requestId=' + String(requestId || '') + ' reason=' + String(reason || '')
  );
  return extended;
}

/**
 * Renew the manual pause only between fully completed, safely persisted manual
 * batches. The caller must retain the active requestId (or read it from
 * tfbManualRefreshStatus().pause.requestId). Never renew after clearing a page
 * and before completing that page's write.
 */
function tfbRenewManualPause_(requestId) {
  var requestedId = String(requestId || '').trim();
  var outcome = tfbWithCoordinatorLock_('renew-manual-pause', function() {
    var props = tfbScriptProperties_();
    var currentId = props.getProperty(
      TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUEST_ID
   ) || '';
    var nowMs = tfbNowMs_();

    if (!requestedId) {
      return {
        renewed: false,
        status: 'invalid_request_id',
        requestId: requestedId,
        currentId: currentId,
        cleared: false
      };
    }
    if (currentId !== requestedId) {
      return {
        renewed: false,
        status: 'stale_request_id',
        requestId: requestedId,
        currentId: currentId,
        cleared: false
      };
    }

    var rawUntilMs = props.getProperty(
      TFB_REFRESH_COORDINATOR_.PROP_MANUAL_UNTIL_MS
    ) || '';
    var currentUntilMs = Number(rawUntilMs || 0);
    var startedAtMs = tfbManualPauseStartMsUnlocked_(
      props,
      currentId,
      nowMs
    );
    var maxUntilMs = startedAtMs +
      TFB_REFRESH_COORDINATOR_.MANUAL_PAUSE_MAX_TOTAL_MS;

    // Classify the hard ceiling before the idle expiry. At the exact ceiling,
    // both conditions can be true; the safety-relevant reason is the immutable
    // six-hour maximum, and operators must see that named outcome.
    if (nowMs >= maxUntilMs) {
      tfbDeleteManualPauseUnlocked_(props);
      return {
        renewed: false,
        status: 'hard_ceiling_reached',
        requestId: requestedId,
        currentId: currentId,
        untilMs: nowMs,
        startedAtMs: startedAtMs,
        maxUntilMs: maxUntilMs,
        ceilingRemainingMs: 0,
        ceilingReached: true,
        cleared: true
      };
    }
    if (!isFinite(currentUntilMs) || currentUntilMs <= nowMs) {
      tfbDeleteManualPauseUnlocked_(props);
      return {
        renewed: false,
        status: 'idle_lease_expired',
        requestId: requestedId,
        currentId: currentId,
        untilMs: currentUntilMs,
        startedAtMs: startedAtMs,
        maxUntilMs: maxUntilMs,
        ceilingRemainingMs: Math.max(0, maxUntilMs - nowMs),
        ceilingReached: false,
        cleared: true
      };
    }

    var untilMs = Math.min(
      nowMs + TFB_REFRES