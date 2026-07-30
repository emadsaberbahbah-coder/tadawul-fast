// TADAWUL FAST BRIDGE — MANUAL REFRESH PRIORITY COORDINATOR v1.0.0
//
// Purpose
// -------
// Manual refresh must take priority over scheduled/automatic refresh without
// deleting triggers, creating duplicate triggers, or leaving automation paused.
//
// IMPORTANT: Google Apps Script cannot forcibly kill an execution that is already
// running. The safe design is cooperative:
//   1) manual refresh records a pause request immediately;
//   2) automatic refresh checks the request at startup and between safe page/batch
//      boundaries, then exits cleanly;
//   3) manual refresh acquires the ScriptLock and runs alone;
//   4) the pause is cleared in finally and automation continues on its next trigger
//      (or via one configured, deduplicated one-shot resume trigger).
//
// This file is inert until the existing manual and automatic entrypoints call the
// wrapper functions documented in docs/MANUAL_REFRESH_PRIORITY_V1.md.

var TFB_REFRESH_COORDINATOR_VERSION = '1.0.0';

var TFB_REFRESH_COORDINATOR_ = Object.freeze({
  PROP_MANUAL_UNTIL_MS: 'TFB_MANUAL_REFRESH_UNTIL_MS',
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
  DEFERRED_DELAY_MS: 60 * 1000,
  AUTO_RESUME_DELAY_MS: 60 * 1000
});

function tfbNowMs_() {
  return Date.now();
}

function tfbScriptProperties_() {
  return PropertiesService.getScriptProperties();
}

function tfbSafeUserEmail_() {
  try {
    return Session.getActiveUser().getEmail() || 'unknown';
  } catch (err) {
    return 'unknown';
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

function tfbReadManualPause_() {
  var props = tfbScriptProperties_();
  var raw = props.getProperty(TFB_REFRESH_COORDINATOR_.PROP_MANUAL_UNTIL_MS);
  var untilMs = Number(raw || 0);
  var nowMs = tfbNowMs_();

  if (!isFinite(untilMs) || untilMs <= nowMs) {
    if (raw) {
      tfbClearManualPause_('expired');
    }
    return {
      active: false,
      untilMs: 0,
      remainingMs: 0,
      reason: '',
      requestedBy: ''
    };
  }

  return {
    active: true,
    untilMs: untilMs,
    remainingMs: Math.max(0, untilMs - nowMs),
    reason: props.getProperty(TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REASON) || '',
    requestedBy: props.getProperty(
      TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUESTED_BY
    ) || ''
  };
}

function tfbRequestManualPause_(reason) {
  var props = tfbScriptProperties_();
  var nowMs = tfbNowMs_();
  var untilMs = nowMs + TFB_REFRESH_COORDINATOR_.MANUAL_PAUSE_TTL_MS;

  props.setProperties({
    TFB_MANUAL_REFRESH_UNTIL_MS: String(untilMs),
    TFB_MANUAL_REFRESH_REQUESTED_AT: new Date(nowMs).toISOString(),
    TFB_MANUAL_REFRESH_REQUESTED_BY: tfbSafeUserEmail_(),
    TFB_MANUAL_REFRESH_REASON: String(reason || 'manual-refresh')
  }, false);

  tfbRecordRefreshEvent_('manual-pause-requested', String(reason || ''));
  return untilMs;
}

function tfbClearManualPause_(reason) {
  var props = tfbScriptProperties_();
  props.deleteProperty(TFB_REFRESH_COORDINATOR_.PROP_MANUAL_UNTIL_MS);
  props.deleteProperty(TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUESTED_AT);
  props.deleteProperty(TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REQUESTED_BY);
  props.deleteProperty(TFB_REFRESH_COORDINATOR_.PROP_MANUAL_REASON);
  tfbRecordRefreshEvent_('manual-pause-cleared', String(reason || ''));
}

/**
 * Automatic entrypoints call this before doing any work.
 * Returns false while a manual refresh is pending/running.
 */
function tfbAutomaticRefreshAllowed_(label) {
  var pause = tfbReadManualPause_();
  if (!pause.active) {
    return true;
  }
  tfbRecordRefreshEvent_(
    'automatic-skipped-for-manual',
    String(label || '') + ' remainingMs=' + pause.remainingMs
  );
  return false;
}

/**
 * Automatic loops call this only at safe boundaries (after a completed page or
 * batch and before the next clear/write/fetch cycle). Returns true when the
 * automatic run should save its cursor and exit cleanly for the pending manual.
 */
function tfbAutomaticYieldPoint_(label) {
  var pause = tfbReadManualPause_();
  if (!pause.active) {
    return false;
  }
  tfbRecordRefreshEvent_(
    'automatic-yielded-for-manual',
    String(label || '') + ' remainingMs=' + pause.remainingMs
  );
  return true;
}

/**
 * Wrap every scheduled/time-driven automatic entrypoint with this function.
 * The callback may accept a shouldYield function and should check it between
 * safe pages/batches.
 */
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
    // Recheck after lock acquisition: manual may have requested priority while
    // this automatic execution was waiting.
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

function tfbRemoveOwnDeferredTrigger_() {
  var props = tfbScriptProperties_();
  var expectedId = props.getProperty(
    TFB_REFRESH_COORDINATOR_.PROP_DEFERRED_TRIGGER_ID
  );
  if (!expectedId) {
    return;
  }
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    if (triggers[i].getUniqueId() === expectedId) {
      ScriptApp.deleteTrigger(triggers[i]);
      break;
    }
  }
  props.deleteProperty(TFB_REFRESH_COORDINATOR_.PROP_DEFERRED_TRIGGER_ID);
}

function tfbScheduleDeferredManual_() {
  var scheduled = tfbEnsureOneShotTrigger_(
    'tfbManualRefreshDeferred_',
    TFB_REFRESH_COORDINATOR_.DEFERRED_DELAY_MS
  );
  tfbScriptProperties_().setProperty(
    TFB_REFRESH_COORDINATOR_.PROP_DEFERRED_TRIGGER_ID,
    String(scheduled.triggerId || '')
  );
  tfbRecordRefreshEvent_(
    'manual-deferred',
    'created=' + scheduled.created + ' triggerId=' + scheduled.triggerId
  );
  return scheduled;
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

  var scheduled = tfbEnsureOneShotTrigger_(
    handlerName,
    TFB_REFRESH_COORDINATOR_.AUTO_RESUME_DELAY_MS
  );
  tfbRecordRefreshEvent_(
    'automatic-resume-scheduled',
    'handler=' + handlerName + ' created=' + scheduled.created
  );
  return scheduled;
}

function tfbExecuteManualHandler_(sourceLabel) {
  var configured = tfbConfiguredFunction_(
    TFB_REFRESH_COORDINATOR_.PROP_MANUAL_HANDLER,
    'tfbManualRefresh'
  );
  var lock = LockService.getScriptLock();

  if (!lock.tryLock(TFB_REFRESH_COORDINATOR_.MANUAL_LOCK_WAIT_MS)) {
    tfbScheduleDeferredManual_();
    tfbToast_(
      'Automatic refresh is finishing a safe step. Manual refresh has been queued.',
      'Manual refresh queued',
      8
    );
    return {status: 'queued', handler: configured.name};
  }

  try {
    // Extend the TTL after the lock is ours so long manual runs are not
    // accidentally overlapped by an automatic trigger.
    tfbRequestManualPause_(String(sourceLabel || 'manual') + ':running');
    tfbRecordRefreshEvent_('manual-started', configured.name);
    tfbToast_('Automatic refresh paused. Manual refresh started.', 'Manual refresh', 5);
    var result = configured.fn();
    SpreadsheetApp.flush();
    tfbRecordRefreshEvent_('manual-finished', configured.name);
    tfbToast_('Manual refresh completed. Automatic refresh resumed.', 'Refresh complete', 7);
    return result;
  } catch (err) {
    tfbRecordRefreshEvent_('manual-failed', String(err && err.stack || err));
    tfbToast_('Manual refresh failed: ' + err, 'Refresh error', 10);
    throw err;
  } finally {
    lock.releaseLock();
    tfbRemoveOwnDeferredTrigger_();
    tfbClearManualPause_('manual-finally');
    tfbScheduleAutomaticResume_();
  }
}

/**
 * Public menu function. Configure Script Property TFB_MANUAL_REFRESH_HANDLER to
 * the existing core refresh function (for example refreshAllDataCore_).
 */
function tfbManualRefresh() {
  tfbRequestManualPause_('menu-request');
  return tfbExecuteManualHandler_('menu');
}

/** One-shot retry used only when an automatic run still held the ScriptLock. */
function tfbManualRefreshDeferred_() {
  tfbRequestManualPause_('deferred-request');
  return tfbExecuteManualHandler_('deferred');
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
    lastEvent: props.getProperty(
      TFB_REFRESH_COORDINATOR_.PROP_LAST_EVENT
    ) || ''
  };
  console.log('[REFRESH-COORDINATOR-STATUS] ' + JSON.stringify(status));
  return status;
}

/** Emergency owner control: clears only this coordinator's pause state. */
function tfbClearStaleManualRefreshPause() {
  tfbRemoveOwnDeferredTrigger_();
  tfbClearManualPause_('owner-emergency-clear');
  tfbToast_('Manual refresh pause cleared. Automatic refresh may continue.', 'Refresh control', 6);
}
