// TADAWUL FAST BRIDGE — MANUAL REFRESH PRIORITY COORDINATOR v1.0.2
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

var TFB_REFRESH_COORDINATOR_VERSION = '1.0.2';

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

function tfbWriteManualPauseUnlocked_(props, reason, requestId) {
  var nowMs = tfbNowMs_();
  var untilMs = nowMs + TFB_REFRESH_COORDINATOR_.MANUAL_PAUSE_TTL_MS;
  var values = {};
  values[TFB_REFRESH_COORDINATOR_.PROP_MANUAL_UNTIL_MS] = String(untilMs);
  values[TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUEST_ID] = String(requestId || '');
  values[TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUESTED_AT] = new Date(nowMs).toISOString();
  values[TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUESTED_BY] = tfbSafeUserEmail_();
  values[TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REASON] = String(reason || 'manual-refresh');
  props.setProperties(values, false);
  return {requestId: String(requestId || ''), untilMs: untilMs};
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

    var currentUntilMs = Number(currentRaw || 0);
    if (isFinite(currentUntilMs) && currentUntilMs > tfbNowMs_()) {
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

    if (isFinite(untilMs) && untilMs > nowMs) {
      return {
        active: true,
        requestId: requestId,
        untilMs: untilMs,
        remainingMs: Math.max(0, untilMs - nowMs),
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
  var latestUntilMs = Number(latestRaw || 0);
  if (isFinite(latestUntilMs) && latestUntilMs > tfbNowMs_()) {
    return {
      active: true,
      requestId: latestProps.getProperty(
        TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUEST_ID
      ) || '',
      untilMs: latestUntilMs,
      remainingMs: Math.max(0, latestUntilMs - tfbNowMs_()),
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
    var currentUntilMs = Number(
      props.getProperty(TFB_REFRESH_COORDINATOR_.PROP_MANUAL_UNTIL_MS) || 0
    );
    if (isFinite(currentUntilMs) && currentUntilMs > tfbNowMs_()) {
      return {
        claimed: false,
        requestId: props.getProperty(
          TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUEST_ID
        ) || '',
        untilMs: currentUntilMs,
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
    tfbWriteManualPauseUnlocked_(props, reason, requestId);
    return true;
  });

  tfbRecordRefreshEvent_(
    extended ? 'manual-pause-extended' : 'manual-pause-extension-skipped',
    'requestId=' + String(requestId || '') + ' reason=' + String(reason || '')
  );
  return extended;
}

/** Clear only the owned request unless force=true for the owner emergency reset. */
function tfbClearManualPause_(reason, expectedRequestId, force) {
  var outcome = tfbWithCoordinatorLock_('clear-manual-pause', function() {
    var props = tfbScriptProperties_();
    var currentId = props.getProperty(
      TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUEST_ID
    ) || '';
    if (!force && currentId !== String(expectedRequestId || '')) {
      return {cleared: false, currentId: currentId};
    }
    tfbDeleteManualPauseUnlocked_(props);
    return {cleared: true, currentId: currentId};
  });

  tfbRecordRefreshEvent_(
    outcome.cleared ? 'manual-pause-cleared' : 'manual-pause-clear-skipped',
    String(reason || '') +
      ' expected=' + String(expectedRequestId || '') +
      ' current=' + String(outcome.currentId || '')
  );
  return outcome.cleared;
}

/** Automatic entrypoints call this before doing any work. */
function tfbAutomaticRefreshAllowed_(label) {
  var pause = tfbReadManualPause_();
  if (!pause.active) {
    return true;
  }
  tfbRecordRefreshEvent_(
    'automatic-skipped-for-manual',
    String(label || '') + ' requestId=' + pause.requestId +
      ' remainingMs=' + pause.remainingMs
  );
  return false;
}

/** Automatic loops call this only between completed, safely persisted steps. */
function tfbAutomaticYieldPoint_(label) {
  var pause = tfbReadManualPause_();
  if (!pause.active) {
    return false;
  }
  tfbRecordRefreshEvent_(
    'automatic-yielded-for-manual',
    String(label || '') + ' requestId=' + pause.requestId +
      ' remainingMs=' + pause.remainingMs
  );
  return true;
}

/** Wrap every scheduled/time-driven automatic entrypoint with this function. */
function tfbRunAutomaticRefresh_(callback, label) {
  if (typeof callback !== 'function') {
    throw new Error('tfbRunAutomaticRefresh_ requires a callback function');
  }
  if (!tfbAutomaticRefreshAllowed_(label)) {
    return {status: 'skipped_manual_priority', label: String(label || '')};
  }

  var lock = LockService.getScriptLock();
  if (!lock.tryLock(TFB_REFRESH_COORDINATOR_.AUTO_LOCK_WAIT_MS)) {
    tfbRecordRefreshEvent_('automatic-lock-busy', String(label || ''));
    return {status: 'skipped_lock_busy', label: String(label || '')};
  }

  try {
    // Recheck after ScriptLock acquisition: a manual request may have arrived
    // while this automatic execution was waiting.
    if (!tfbAutomaticRefreshAllowed_(label)) {
      return {status: 'skipped_manual_priority', label: String(label || '')};
    }
    tfbRecordRefreshEvent_('automatic-started', String(label || ''));
    var result = callback(function(boundaryLabel) {
      return tfbAutomaticYieldPoint_(boundaryLabel || label);
    });
    tfbRecordRefreshEvent_('automatic-finished', String(label || ''));
    return result;
  } finally {
    lock.releaseLock();
  }
}

function tfbConfiguredFunction_(propertyName, forbiddenName) {
  var handlerName = String(
    tfbScriptProperties_().getProperty(propertyName) || ''
  ).trim();
  if (!handlerName) {
    throw new Error('Missing Script Property: ' + propertyName);
  }
  if (handlerName === forbiddenName) {
    throw new Error('Recursive refresh handler is not allowed: ' + handlerName);
  }
  var fn = globalThis[handlerName];
  if (typeof fn !== 'function') {
    throw new Error('Configured handler does not exist: ' + handlerName);
  }
  return {name: handlerName, fn: fn};
}

function tfbConfiguredManualHandler_() {
  return tfbConfiguredFunction_(
    TFB_REFRESH_COORDINATOR_.PROP_MANUAL_HANDLER,
    'tfbManualRefresh'
  );
}

function tfbToast_(message, title, seconds) {
  try {
    SpreadsheetApp.getActiveSpreadsheet().toast(
      String(message || ''),
      String(title || 'Tadawul Fast'),
      Number(seconds || 5)
    );
  } catch (err) {
    console.log('[REFRESH-COORDINATOR] toast unavailable: ' + err);
  }
}

function tfbFindTriggerByHandler_(handlerName) {
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    if (triggers[i].getHandlerFunction() === handlerName) {
      return triggers[i];
    }
  }
  return null;
}

/** Caller must hold the coordinator DocumentLock. */
function tfbEnsureOneShotTrigger_(handlerName, delayMs) {
  var existing = tfbFindTriggerByHandler_(handlerName);
  if (existing) {
    return {created: false, triggerId: existing.getUniqueId()};
  }
  var trigger = ScriptApp.newTrigger(handlerName)
    .timeBased()
    .after(Math.max(60 * 1000, Number(delayMs || 0)))
    .create();
  return {created: true, triggerId: trigger.getUniqueId()};
}

/** Serialize lookup, creation, and trigger-ID persistence as one transaction. */
function tfbScheduleDeferredManual_() {
  var scheduled = tfbWithCoordinatorLock_('schedule-deferred-manual', function() {
    var result = tfbEnsureOneShotTrigger_(
      'tfbManualRefreshDeferred_',
      TFB_REFRESH_COORDINATOR_.DEFERRED_DELAY_MS
    );
    tfbScriptProperties_().setProperty(
      TFB_REFRESH_COORDINATOR_.PROP_DEFERRED_TRIGGER_ID,
      String(result.triggerId || '')
    );
    return result;
  });

  tfbRecordRefreshEvent_(
    'manual-deferred',
    'created=' + scheduled.created + ' triggerId=' + scheduled.triggerId
  );
  return scheduled;
}

function tfbRemoveOwnDeferredTrigger_() {
  var removed = tfbWithCoordinatorLock_('remove-deferred-manual', function() {
    var props = tfbScriptProperties_();
    var expectedId = props.getProperty(
      TFB_REFRESH_COORDINATOR_.PROP_DEFERRED_TRIGGER_ID
    ) || '';
    if (!expectedId) {
      return false;
    }
    var triggers = ScriptApp.getProjectTriggers();
    for (var i = 0; i < triggers.length; i++) {
      if (triggers[i].getUniqueId() === expectedId) {
        ScriptApp.deleteTrigger(triggers[i]);
        break;
      }
    }
    props.deleteProperty(TFB_REFRESH_COORDINATOR_.PROP_DEFERRED_TRIGGER_ID);
    return true;
  });
  return removed;
}

function tfbScheduleAutomaticResume_() {
  var handlerName = String(
    tfbScriptProperties_().getProperty(
      TFB_REFRESH_COORDINATOR_.PROP_AUTO_RESUME_HANDLER
    ) || ''
  ).trim();

  // No handler means normal scheduled triggers continue naturally.
  if (!handlerName) {
    tfbRecordRefreshEvent_('automatic-resume-next-schedule', 'no one-shot handler');
    return {created: false, reason: 'next_scheduled_trigger'};
  }
  if (handlerName === 'tfbManualRefresh' ||
      handlerName === 'tfbManualRefreshDeferred_') {
    throw new Error('Invalid automatic resume handler: ' + handlerName);
  }

  var scheduled = tfbWithCoordinatorLock_('schedule-automatic-resume', function() {
    return tfbEnsureOneShotTrigger_(
      handlerName,
      TFB_REFRESH_COORDINATOR_.AUTO_RESUME_DELAY_MS
    );
  });
  tfbRecordRefreshEvent_(
    'automatic-resume-scheduled',
    'handler=' + handlerName + ' created=' + scheduled.created
  );
  return scheduled;
}

function tfbLogCleanupFailure_(step, err) {
  console.error(
    '[REFRESH-COORDINATOR] cleanup step failed: ' +
    String(step || 'unknown') + ' | ' + String(err && err.stack || err)
  );
}

function tfbExecuteManualHandler_(sourceLabel, configured, requestId) {
  configured = configured || tfbConfiguredManualHandler_();
  requestId = String(requestId || tfbNewManualRequestId_());
  var lock = LockService.getScriptLock();

  if (!lock.tryLock(TFB_REFRESH_COORDINATOR_.MANUAL_LOCK_WAIT_MS)) {
    try {
      tfbScheduleDeferredManual_();
    } catch (scheduleErr) {
      // A request that cannot be queued may clear only its own pause instance.
      try {
        tfbClearManualPause_('deferred-schedule-failed', requestId, false);
      } catch (clearErr) {
        tfbLogCleanupFailure_('clear-after-deferred-schedule-failure', clearErr);
      }
      throw scheduleErr;
    }
    tfbToast_(
      'Automatic refresh is finishing a safe step. Manual refresh has been queued.',
      'Manual refresh queued',
      8
    );
    return {status: 'queued', handler: configured.name, requestId: requestId};
  }

  var pauseCleared = false;
  try {
    // Adopt the newest queued request before extending the TTL. This lets one
    // manual execution safely satisfy repeated clicks without overwriting a
    // newer owner token.
    var latestPause = tfbReadManualPause_();
    if (latestPause.active && latestPause.requestId) {
      requestId = latestPause.requestId;
    }
    tfbExtendManualPause_(String(sourceLabel || 'manual') + ':running', requestId);

    tfbRecordRefreshEvent_('manual-started', configured.name + ' requestId=' + requestId);
    tfbToast_('Automatic refresh paused. Manual refresh started.', 'Manual refresh', 5);
    var result = configured.fn();
    SpreadsheetApp.flush();
    tfbRecordRefreshEvent_('manual-finished', configured.name + ' requestId=' + requestId);
    tfbToast_('Manual refresh completed. Automatic refresh resumed.', 'Refresh complete', 7);
    return result;
  } catch (err) {
    tfbRecordRefreshEvent_('manual-failed', String(err && err.stack || err));
    tfbToast_('Manual refresh failed: ' + err, 'Refresh error', 10);
    throw err;
  } finally {
    // Every cleanup step is isolated. Pause clearing runs before any fallible
    // trigger enumeration/deletion and is conditional on the owned request ID.
    try {
      lock.releaseLock();
    } catch (releaseErr) {
      tfbLogCleanupFailure_('release-lock', releaseErr);
    }
    try {
      pauseCleared = tfbClearManualPause_('manual-finally', requestId, false);
    } catch (clearErr) {
      tfbLogCleanupFailure_('clear-manual-pause', clearErr);
    }

    if (pauseCleared) {
      try {
        tfbRemoveOwnDeferredTrigger_();
      } catch (triggerErr) {
        tfbLogCleanupFailure_('remove-deferred-trigger', triggerErr);
      }
      try {
        tfbScheduleAutomaticResume_();
      } catch (resumeErr) {
        tfbLogCleanupFailure_('schedule-automatic-resume', resumeErr);
      }
    } else {
      tfbRecordRefreshEvent_(
        'manual-cleanup-preserved-newer-request',
        'requestId=' + requestId
      );
    }
  }
}

/**
 * Public menu function. Configure Script Property TFB_MANUAL_REFRESH_HANDLER to
 * the existing core refresh function (for example refreshAllDataCore_).
 */
function tfbManualRefresh() {
  // Handler validation occurs before any pause state is created.
  var configured = tfbConfiguredManualHandler_();
  var claim = tfbClaimManualPause_('menu-request');
  if (!claim.claimed) {
    tfbToast_(
      'A manual refresh is already queued or running.',
      'Manual refresh already requested',
      6
    );
    return {
      status: String(claim.reason || '').indexOf(':running') >= 0
        ? 'already_running'
        : 'already_queued',
      requestId: claim.requestId
    };
  }
  return tfbExecuteManualHandler_('menu', configured, claim.requestId);
}

/** One-shot retry used only when an automatic run still held ScriptLock. */
function tfbManualRefreshDeferred_() {
  // The deferred trigger can outlive a configuration change. Validate first.
  var configured = tfbConfiguredManualHandler_();
  var pause = tfbReadManualPause_();
  var requestId = pause.active && pause.requestId
    ? pause.requestId
    : tfbClaimManualPause_('deferred-request').requestId;
  tfbExtendManualPause_('deferred-request', requestId);
  return tfbExecuteManualHandler_('deferred', configured, requestId);
}

/** Read-only diagnostic callable from the Apps Script editor. */
function tfbManualRefreshStatus() {
  var pause = tfbReadManualPause_();
  var props = tfbScriptProperties_();
  var status = {
    version: TFB_REFRESH_COORDINATOR_VERSION,
    pause: pause,
    manualHandler: props.getProperty(
      TFB_REFRESH_COORDINATOR_.PROP_MANUAL_HANDLER
    ) || '',
    autoResumeHandler: props.getProperty(
      TFB_REFRESH_COORDINATOR_.PROP_AUTO_RESUME_HANDLER
    ) || '',
    deferredTriggerId: props.getProperty(
      TFB_REFRESH_COORDINATOR_.PROP_DEFERRED_TRIGGER_ID
    ) || '',
    lastEvent: props.getProperty(
      TFB_REFRESH_COORDINATOR_.PROP_LAST_EVENT
    ) || ''
  };
  console.log('[REFRESH-COORDINATOR-STATUS] ' + JSON.stringify(status));
  return status;
}

/** Emergency owner control: force-clears only this coordinator's state. */
function tfbClearStaleManualRefreshPause() {
  try {
    tfbClearManualPause_('owner-emergency-clear', '', true);
  } finally {
    try {
      tfbRemoveOwnDeferredTrigger_();
    } catch (err) {
      tfbLogCleanupFailure_('emergency-remove-deferred-trigger', err);
    }
  }
  tfbToast_(
    'Manual refresh pause cleared. Automatic refresh may continue.',
    'Refresh control',
    6
  );
}
