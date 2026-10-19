/* Preserve the immutable v1.13.8 source without duplicating a 438 KB fixture.
 * Every v1.13.9/v1.13.11/v1.13.12 edit is reversed exactly once; the historical digest
 * must match. Any unrelated edit or code hidden in an approved region fails.
 * Historical D1-D5 use this hash-proven source; current behavior tests use
 * the delivered source. This helper is a test fixture, never runtime code. */
'use strict';
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const HISTORY_SHA256 = '9d11832acae738be2299faa463ca6aa71b8e9a77ab31c05d4bf8d5e55bcd506c';
const COMPLETION_12_EDITS = [
  [
    [
      "/**",
      " * ============================================================================",
      " * 16_Decision_Top10.gs \u2014 Top_10_Investments DECISION page (frontend renderer)",
      " * Version: 1.13.12 (see DT10_VERSION; header kept in lockstep \u2014 restored",
      " *                  again at v1.6.6 after drifting to 1.6.4 while",
      " *                  DT10_VERSION read 1.6.5)",
      " * Runtime: ES5 ONLY (V8 exceptions are 01_Menu.gs / 03_Schema.gs only).",
      " * ============================================================================",
      " *",
      " * v1.13.12 (2026-10-17) -- TYPED COMPLETION AND PRICE DISPLAY",
      " * WHY: a normal SYNC-INFLIGHT HELD return was acknowledged as a completed",
      " * cohort by the proposed decision chain. Return completed/deferred facts;",
      " * only a finally verified, actually collected Sheets pool proves source_run.",
      " * The morning wrapper records deferral without an OK or script-failure claim.",
      " * Backend meta.price_precision='on' selects seven-significant-digit price",
      " * formats from in-memory rows; absent/off keeps legacy writes. Money, ROI,",
      " * quantities and all financial/source gates are unchanged. Base formats reset",
      " * an earlier armed render before the bounded per-column precision overrides.",
      " * Also integrate PR #770/#778's default-off DT10_AUDIT_KEEP handoff (reserved",
      " * v1.13.10): held members are display context only; its backend companion is",
      " * still required. No property is armed, source grant invented or retry added.",
      " * Candidate publication inherits a caller-owned ScriptLock and releases only",
      " * a lock it acquired itself, preserving the coordinator's writer boundary.",
      " * A completion-contract caller must use Sheets: a Backend selector cannot",
      " * prove its consumed cohort, so that opt-in path defers before any POST.",
      " * Typed completion also carries the latest consumed four-page source stamp",
      " * from its verified frame, so a prior same-run audit cannot certify a rerun.",
      " *",
      " * v1.13.11 (2026-10-17) -- FAILED REFRESHES CANNOT ACKNOWLEDGE COMPLETION",
      " * WHY: network, HTTP and degraded-response paths preserved the previous",
      ""
    ].join('\n'),
    [
      "/**",
      " * ============================================================================",
      " * 16_Decision_Top10.gs \u2014 Top_10_Investments DECISION page (frontend renderer)",
      " * Version: 1.13.11 (see DT10_VERSION; header kept in lockstep \u2014 restored",
      " *                  again at v1.6.6 after drifting to 1.6.4 while",
      " *                  DT10_VERSION read 1.6.5)",
      " * Runtime: ES5 ONLY (V8 exceptions are 01_Menu.gs / 03_Schema.gs only).",
      " * ============================================================================",
      " *",
      " * v1.13.11 (2026-10-17) -- FAILED REFRESHES CANNOT ACKNOWLEDGE COMPLETION",
      " * WHY: network, HTTP and degraded-response paths preserved the previous",
      ""
    ].join('\n')
  ],
  [
    [
      " * board is preserved instead of wiped. A genuine empty scan (scanned = 0 /",
      " * status \"no_candidates\") still renders exactly as before.",
      " */",
      "var DT10_VERSION = '1.13.12';",
      "",
      "/** Completion is an operational fact; a withheld render is still a render. */",
      "function dt10RefreshCompletion_(state, sourceRun, reason, outputState, sourceLatestMs) {",
      "  var out = {contract: 'tfb_native_refresh_v1', page: DT10_SHEET,",
      "    state: state, source_run: String(sourceRun || ''),",
      "    reason: String(reason || ''), output_state: String(outputState || '')};",
      "  if (state === 'completed' && sourceRun && typeof sourceLatestMs === 'number' &&",
      "      isFinite(sourceLatestMs) && sourceLatestMs >= 0) out.source_latest_ms = sourceLatestMs;",
      "  return out;",
      "}",
      "/** Pure projection of the exact validated generations; no second source read. */",
      "function dt10SourceLatestMs_(source) {",
      "  if (!source || source.ready !== true || !source.generation) return null;",
      "  var latest = null;",
      "  try {",
      "    for (var i = 0; i < DT10_POOL_PAGES.length; i++) {",
      "      var page = DT10_POOL_PAGES[i];",
      "      if (!Object.prototype.hasOwnProperty.call(source.generation, page)) return null;",
      "      var generation = JSON.parse(source.generation[page]);",
      "      if (!Array.isArray(generation) || generation.length < 4) return null;",
      "      var stamp = dt10SourceStampMs_(generation[0]);",
      "      if (typeof stamp !== 'number' || !isFinite(stamp)) return null;",
      "      latest = latest === null ? stamp : Math.max(latest, stamp);",
      "    }",
      "  } catch (eStamp) { return null; }",
      "  return latest;",
      "}",
      "",
      "/** A reversible rollout brake with no fallback to the old funding bug.",
      " * DT10_BOARD_FUNDING_MODE=research keeps research/stability but allocates zero.",
      ""
    ].join('\n'),
    [
      " * board is preserved instead of wiped. A genuine empty scan (scanned = 0 /",
      " * status \"no_candidates\") still renders exactly as before.",
      " */",
      "var DT10_VERSION = '1.13.11';",
      "",
      "/** A reversible rollout brake with no fallback to the old funding bug.",
      " * DT10_BOARD_FUNDING_MODE=research keeps research/stability but allocates zero.",
      ""
    ].join('\n')
  ],
  [
    [
      "var DT10_MUTED = '#6B7280';       // status / footer / labels (was #5F6368)",
      "var DT10_FMT_SAR = '#,##0 \"SAR\"';",
      "var DT10_FMT_PRICE = '#,##0.00';",
      "/** Display only: match the backend's armed seven-significant-digit boundary. */",
      "function dt10PriceFormat_(value) {",
      "  if (typeof value !== 'number' || !isFinite(value) || value === 0 || Math.abs(value) >= 10) {",
      "    return DT10_FMT_PRICE;",
      "  }",
      "  var magnitude = Math.abs(value);",
      "  var exponent = Math.floor(Math.log(magnitude) / Math.LN10);",
      "  if (magnitude < Math.pow(10, exponent)) exponent--;",
      "  else if (magnitude >= Math.pow(10, exponent + 1)) exponent++;",
      "  var digits = Math.max(2, 6 - exponent);",
      "  return digits > 20 ? '0.000000E+00' : '#,##0.' + new Array(digits + 1).join('0');",
      "}",
      "/** Bounded format matrices; no value rereads, per-cell RPC loop or value edits. */",
      "function dt10ApplyPriceFormats_(sheet, firstRow, rows, columns, mode) {",
      "  if (mode !== 'on' || firstRow < 1 || !rows || !rows.length) return;",
      "  columns.forEach(function (column) {",
      "    var formats = rows.map(function (row) { return [dt10PriceFormat_(row[column - 1])]; });",
      "    sheet.getRange(firstRow, column, rows.length, 1).setNumberFormats(formats);",
      "  });",
      "}",
      "var DT10_FMT_PCT = '0.0\"%\"';",
      "var DT10_FMT_NUM2 = '0.00';",
      "var DT10_FMT_INT = '#,##0';",
      ""
    ].join('\n'),
    [
      "var DT10_MUTED = '#6B7280';       // status / footer / labels (was #5F6368)",
      "var DT10_FMT_SAR = '#,##0 \"SAR\"';",
      "var DT10_FMT_PRICE = '#,##0.00';",
      "var DT10_FMT_PCT = '0.0\"%\"';",
      "var DT10_FMT_NUM2 = '0.00';",
      "var DT10_FMT_INT = '#,##0';",
      ""
    ].join('\n')
  ],
  [
    [
      "    return true;",
      "  }",
      "}",
      "/** PR #770/#778 native handoff: display context only, default off. */",
      "var DT10_AUDIT_KEEP_PROP = 'DT10_AUDIT_KEEP';",
      "var DT10_AUDIT_KEEP_MAX = 60;",
      "function dt10AuditKeepOn_() {",
      "  return dt10PropMemoGet_('audit_keep', function () {",
      "    try {",
      "      return String(PropertiesService.getScriptProperties()",
      "          .getProperty(DT10_AUDIT_KEEP_PROP) || '')",
      "          .replace(/^\\s+|\\s+$/g, '').toLowerCase() === 'on';",
      "    } catch (eProp) { return false; }",
      "  });",
      "}",
      "function dt10AuditKeepSymbols_(state) {",
      "  var out = [], syms = state && state.symbols;",
      "  if (!syms || typeof syms !== 'object') return out;",
      "  for (var key in syms) {",
      "    if (syms.hasOwnProperty(key) && key && syms[key] && syms[key].member === true) {",
      "      out.push(String(key));",
      "    }",
      "  }",
      "  out.sort();",
      "  return out.slice(0, DT10_AUDIT_KEEP_MAX);",
      "}",
      "",
      "/**",
      " * v1.11.7 [P-134]: cockpit cash source-of-truth mode.",
      ""
    ].join('\n'),
    [
      "    return true;",
      "  }",
      "}",
      "",
      "/**",
      " * v1.11.7 [P-134]: cockpit cash source-of-truth mode.",
      ""
    ].join('\n')
  ],
  [
    [
      " * tab. Invalidate its receipt before mutation; COMMITTED is the final write.",
      " * A fault falls back to today's cockpit grid and leaves no trusted partial",
      " * publication. The same protocol commits zero rows, replacing old results. */",
      "function dt10CandTabWrite_(ss, cRows, stampLine, tokens, runId, pricePrecision) {",
      "  var lock = null, locked = false;",
      "  try {",
      "    lock = LockService.getScriptLock();",
      "    var inherited = typeof lock.hasLock === 'function' && lock.hasLock();",
      "    if (!inherited) {",
      "      locked = lock.tryLock(5000);",
      "      if (!locked) throw new Error('candidate publication already running');",
      "    }",
      "    var n = cRows.length, cols = DT10_CAND_HEADERS.length;",
      "    if (!runId) throw new Error('current candidate run ID missing');",
      "    for (var ri = 0; ri < n; ri++) {",
      ""
    ].join('\n'),
    [
      " * tab. Invalidate its receipt before mutation; COMMITTED is the final write.",
      " * A fault falls back to today's cockpit grid and leaves no trusted partial",
      " * publication. The same protocol commits zero rows, replacing old results. */",
      "function dt10CandTabWrite_(ss, cRows, stampLine, tokens, runId) {",
      "  var lock = null, locked = false;",
      "  try {",
      "    lock = LockService.getScriptLock();",
      "    locked = lock.tryLock(5000);",
      "    if (!locked) throw new Error('candidate publication already running');",
      "    var n = cRows.length, cols = DT10_CAND_HEADERS.length;",
      "    if (!runId) throw new Error('current candidate run ID missing');",
      "    for (var ri = 0; ri < n; ri++) {",
      ""
    ].join('\n')
  ],
  [
    [
      "        6: DT10_FMT_PRICE, 7: DT10_FMT_PRICE, 8: DT10_FMT_PCT, 9: DT10_FMT_PCT,",
      "        10: DT10_FMT_PCT, 11: DT10_FMT_NUM2, 12: '0.0', 13: '0.0', 20: '0.0'",
      "      });",
      "      dt10ApplyPriceFormats_(sh, 3, cRows, [6, 7], pricePrecision);",
      "      if (tokens) dt10ColorVerdicts_(sh, 3, n, 18, tokens);",
      "    }",
      "    sh.setFrozenRows(2);",
      ""
    ].join('\n'),
    [
      "        6: DT10_FMT_PRICE, 7: DT10_FMT_PRICE, 8: DT10_FMT_PCT, 9: DT10_FMT_PCT,",
      "        10: DT10_FMT_PCT, 11: DT10_FMT_NUM2, 12: '0.0', 13: '0.0', 20: '0.0'",
      "      });",
      "      if (tokens) dt10ColorVerdicts_(sh, 3, n, 18, tokens);",
      "    }",
      "    sh.setFrozenRows(2);",
      ""
    ].join('\n')
  ],
  [
    [
      "// ---------------------------------------------------------------------------",
      "// Main refresh",
      "// ---------------------------------------------------------------------------",
      "function refreshDecisionTop10(options) {",
      "  var t0 = new Date().getTime();",
      "  dt10PropMemoReset_();   // v1.11.13 [P-202]: fresh property reads per run",
      "  var ss = SpreadsheetApp.getActiveSpreadsheet();",
      ""
    ].join('\n'),
    [
      "// ---------------------------------------------------------------------------",
      "// Main refresh",
      "// ---------------------------------------------------------------------------",
      "function refreshDecisionTop10() {",
      "  var t0 = new Date().getTime();",
      "  dt10PropMemoReset_();   // v1.11.13 [P-202]: fresh property reads per run",
      "  var ss = SpreadsheetApp.getActiveSpreadsheet();",
      ""
    ].join('\n')
  ],
  [
    [
      "          'SYNC-INFLIGHT: cockpit deferred -- ' + inflight.detail,",
      "          { version: DT10_VERSION, run: inflight.run, mode: inflight.mode });",
      "      dt10RunMarkerClear_();",
      "      return dt10RefreshCompletion_('deferred', '', 'sync_inflight', 'HELD');",
      "    }",
      "  }",
      "  var poolLimit = parseInt(panel['Pool Limit'], 10);",
      ""
    ].join('\n'),
    [
      "          'SYNC-INFLIGHT: cockpit deferred -- ' + inflight.detail,",
      "          { version: DT10_VERSION, run: inflight.run, mode: inflight.mode });",
      "      dt10RunMarkerClear_();",
      "      return;",
      "    }",
      "  }",
      "  var poolLimit = parseInt(panel['Pool Limit'], 10);",
      ""
    ].join('\n')
  ],
  [
    [
      "  if (!(poolLimit > 0)) poolLimit = DT10_POOL_HARD_CAP;",
      "  var useSheets = String(panel['Pool Source'] || 'Sheets')",
      "      .toLowerCase().indexOf('backend') === -1;",
      "  if (options && options.completionContract === true && !useSheets) {",
      "    var sourceHeld = dt10StatusLine_('HELD',",
      "        'completion contract requires Sheets pool source; board kept from the last render', 'HELD');",
      "    statusCell.setValue(sourceHeld);",
      "    dt10WritePageStatus_('HELD', 'Decision cockpit: ' + sourceHeld,",
      "        '', '', String(new Date().getTime() - t0), []);",
      "    dt10RunLogRow_('INFO', 'HELD', 'Completion contract deferred: Sheets pool source required',",
      "        {version: DT10_VERSION});",
      "    dt10RunMarkerClear_();",
      "    return dt10RefreshCompletion_('deferred', '', 'sheets_pool_required', 'HELD');",
      "  }",
      "  var body = {",
      "    criteria: dt10CriteriaFromPanel_(panel),",
      "    fx_rates: dt10FxRates_(ss),",
      ""
    ].join('\n'),
    [
      "  if (!(poolLimit > 0)) poolLimit = DT10_POOL_HARD_CAP;",
      "  var useSheets = String(panel['Pool Source'] || 'Sheets')",
      "      .toLowerCase().indexOf('backend') === -1;",
      "  var body = {",
      "    criteria: dt10CriteriaFromPanel_(panel),",
      "    fx_rates: dt10FxRates_(ss),",
      ""
    ].join('\n')
  ],
  [
    [
      "  Logger.log('[DT10 v' + DT10_VERSION + '] POST ' + DT10_ENDPOINT + ' | ' +",
      "             poolNote);",
      "  body.criteria.board_funding_stage = 'research';",
      "  // PR #770/#778: both this native switch and its backend companion are needed.",
      "  if (dt10AuditKeepOn_()) {",
      "    try {",
      "      var auditKeep = dt10AuditKeepSymbols_(dt10StabLoad_());",
      "      if (auditKeep.length) body.criteria.audit_keep_symbols = auditKeep;",
      "    } catch (eAuditKeep) { /* display context unavailable; send nothing */ }",
      "  }",
      "  var resp;",
      "  try {",
      "    resp = pool && !pool.rows.length ?",
      ""
    ].join('\n'),
    [
      "  Logger.log('[DT10 v' + DT10_VERSION + '] POST ' + DT10_ENDPOINT + ' | ' +",
      "             poolNote);",
      "  body.criteria.board_funding_stage = 'research';",
      "  var resp;",
      "  try {",
      "    resp = pool && !pool.rows.length ?",
      ""
    ].join('\n')
  ],
  [
    [
      "  Logger.log('[DT10 v' + DT10_VERSION + '] done status=' + payload.status +",
      "             ' in ' + secs + 's');",
      "  dt10RunMarkerClear_();   // v1.11.12 [P-195]: normal completion",
      "  // Fresh _Status alone does not prove which inputs a backend selector used.",
      "  var consumedRun = finalSource.ready === true && finalSource.pool_reconciliation &&",
      "      finalSource.pool_reconciliation.ready === true ? finalSource.cohort : '';",
      "  return dt10RefreshCompletion_('completed', consumedRun,",
      "      consumedRun ? '' : 'consumed_source_unverified', dt10OutputStatus_(payload),",
      "      consumedRun ? dt10SourceLatestMs_(finalSource) : null);",
      "}",
      "/** v1.11.1: MORNING COCKPIT REFRESH \u2014 time-driven entry point (the 08:07",
      " * Riyadh trigger). UI-free: no getUi()/alert/toast; the render's banner and",
      ""
    ].join('\n'),
    [
      "  Logger.log('[DT10 v' + DT10_VERSION + '] done status=' + payload.status +",
      "             ' in ' + secs + 's');",
      "  dt10RunMarkerClear_();   // v1.11.12 [P-195]: normal completion",
      "}",
      "/** v1.11.1: MORNING COCKPIT REFRESH \u2014 time-driven entry point (the 08:07",
      " * Riyadh trigger). UI-free: no getUi()/alert/toast; the render's banner and",
      ""
    ].join('\n')
  ],
  [
    [
      "    } catch (eUv) { seen = 'verdict read failed'; }",
      "    Logger.log('[DT10 v' + DT10_VERSION + '] morning cockpit refresh ' +",
      "               '(time-driven) | feed: ' + seen);",
      "    var completed = refreshDecisionTop10();",
      "    if (completed && completed.state === 'deferred') {",
      "      dt10MorningRunLog_('DEFERRED',",
      "          'Morning cockpit refresh deferred (' + completed.reason + ') | feed: ' + seen,",
      "          new Date().getTime() - t0, {version: DT10_VERSION, feed: seen,",
      "            reason: completed.reason});",
      "      return completed;",
      "    }",
      "    // v1.11.2 [P-67]: the morning run leaves a _Run_Log row like the",
      "    // 4-hourly wrapper does (cockpit _Status already carries the payload).",
      "    dt10MorningRunLog_('OK',",
      "        'Morning cockpit refresh (time-driven) completed | feed: ' + seen,",
      "        new Date().getTime() - t0, { version: DT10_VERSION, feed: seen });",
      "    return completed;",
      "  } catch (eMc) {",
      "    Logger.log('[DT10 v' + DT10_VERSION + '] morning cockpit refresh ' +",
      "               'FAILED: ' + eMc);",
      ""
    ].join('\n'),
    [
      "    } catch (eUv) { seen = 'verdict read failed'; }",
      "    Logger.log('[DT10 v' + DT10_VERSION + '] morning cockpit refresh ' +",
      "               '(time-driven) | feed: ' + seen);",
      "    refreshDecisionTop10();",
      "    // v1.11.2 [P-67]: the morning run leaves a _Run_Log row like the",
      "    // 4-hourly wrapper does (cockpit _Status already carries the payload).",
      "    dt10MorningRunLog_('OK',",
      "        'Morning cockpit refresh (time-driven) completed | feed: ' + seen,",
      "        new Date().getTime() - t0, { version: DT10_VERSION, feed: seen });",
      "  } catch (eMc) {",
      "    Logger.log('[DT10 v' + DT10_VERSION + '] morning cockpit refresh ' +",
      "               'FAILED: ' + eMc);",
      ""
    ].join('\n')
  ],
  [
    [
      "    if (!sh) return false;",
      "    var dj = '{}';",
      "    try { dj = JSON.stringify(details || {}); } catch (eJ) {}",
      "    sh.appendRow([new Date(), (status === 'OK' ? 'INFO' :",
      "                  (status === 'DEFERRED' ? 'WARN' : 'ERROR')),",
      "                  'tfbMorningCockpitRefresh', DT10_SHEET, String(status || ''),",
      "                  String(message || '').slice(0, 400), '', '',",
      "                  String(durationMs || ''), dj]);",
      ""
    ].join('\n'),
    [
      "    if (!sh) return false;",
      "    var dj = '{}';",
      "    try { dj = JSON.stringify(details || {}); } catch (eJ) {}",
      "    sh.appendRow([new Date(), (status === 'OK' ? 'INFO' : 'ERROR'),",
      "                  'tfbMorningCockpitRefresh', DT10_SHEET, String(status || ''),",
      "                  String(message || '').slice(0, 400), '', '',",
      "                  String(durationMs || ''), dj]);",
      ""
    ].join('\n')
  ],
  [
    [
      "    19: DT10_FMT_SAR, 20: '0.0', 21: '0.0',",
      "    26: DT10_FMT_INT, 28: '0.0'  // v1.3.0: Days, Sm Score",
      "  });",
      "  dt10ApplyPriceFormats_(sheet, selT.firstDataRow, selRows, [8, 9, 13, 14, 15],",
      "      payload.meta && payload.meta.price_precision);",
      "  if (_vt.mode === 'enforce' && selT.firstDataRow > 0) {   // v1.13.4 [WP-8]",
      "    sheet.getRange(selT.firstDataRow, DT10_SELECTED_HEADERS.length + 1,",
      "                   selT.count, DT10_VINTAGE_HEADERS.length).setNumberFormat('@');",
      ""
    ].join('\n'),
    [
      "    19: DT10_FMT_SAR, 20: '0.0', 21: '0.0',",
      "    26: DT10_FMT_INT, 28: '0.0'  // v1.3.0: Days, Sm Score",
      "  });",
      "  if (_vt.mode === 'enforce' && selT.firstDataRow > 0) {   // v1.13.4 [WP-8]",
      "    sheet.getRange(selT.firstDataRow, DT10_SELECTED_HEADERS.length + 1,",
      "                   selT.count, DT10_VINTAGE_HEADERS.length).setNumberFormat('@');",
      ""
    ].join('\n')
  ],
  [
    [
      "  if (_cm === 'enforce') {",
      "    _tab = dt10CandTabWrite_(sheet.getParent(), cRows,",
      "        dt10CandTabStampLine_(payload.meta, cRows.length, new Date()), tokens,",
      "        dt10CandRunId_(payload.meta), payload.meta && payload.meta.price_precision);",
      "    payload._dt10_cand_result = _tab;",
      "  }",
      "  var cT;",
      ""
    ].join('\n'),
    [
      "  if (_cm === 'enforce') {",
      "    _tab = dt10CandTabWrite_(sheet.getParent(), cRows,",
      "        dt10CandTabStampLine_(payload.meta, cRows.length, new Date()), tokens,",
      "        dt10CandRunId_(payload.meta));",
      "    payload._dt10_cand_result = _tab;",
      "  }",
      "  var cT;",
      ""
    ].join('\n')
  ],
  [
    [
      "      6: DT10_FMT_PRICE, 7: DT10_FMT_PRICE, 8: DT10_FMT_PCT, 9: DT10_FMT_PCT,",
      "      10: DT10_FMT_PCT, 11: DT10_FMT_NUM2, 12: '0.0', 13: '0.0', 20: '0.0'",
      "    });",
      "    dt10ApplyPriceFormats_(sheet, cT.firstDataRow, cRows, [6, 7],",
      "        payload.meta && payload.meta.price_precision);",
      "    dt10ColorVerdicts_(sheet, cT.firstDataRow, cT.count, 18, tokens);",
      "  }",
      "  row = cT.next + 1;",
      ""
    ].join('\n'),
    [
      "      6: DT10_FMT_PRICE, 7: DT10_FMT_PRICE, 8: DT10_FMT_PCT, 9: DT10_FMT_PCT,",
      "      10: DT10_FMT_PCT, 11: DT10_FMT_NUM2, 12: '0.0', 13: '0.0', 20: '0.0'",
      "    });",
      "    dt10ColorVerdicts_(sheet, cT.firstDataRow, cT.count, 18, tokens);",
      "  }",
      "  row = cT.next + 1;",
      ""
    ].join('\n')
  ]
];
const COMPLETION_WHY = [
  ' * v1.13.11 (2026-10-17) -- FAILED REFRESHES CANNOT ACKNOWLEDGE COMPLETION',
  ' * WHY: network, HTTP and degraded-response paths preserved the previous',
  ' * board but returned normally. The morning wrapper then logged OK, and the',
  ' * proposed decision-chain caller marked that cohort refreshed and stopped',
  ' * retrying. After the existing failure status and marker cleanup, these',
  ' * paths now throw a bounded operational error. Both callers already catch',
  ' * exceptions. Malformed HTTP-200 renderer inputs are refused before any',
  ' * stability/render work; a missing result is not a verified empty board.',
  ' * Successful empty, WITHHELD and NO_TRADE renders are unchanged;',
  ' * no source, funding, stability or financial gate changes. Version 1.13.10',
  ' * remains reserved for the separate pending audit-keep change in PR #778.',
  ' *',
  ''
].join('\n');
const RESPONSE_HELPER = [
  "function dt10ResponseProblem_(payload) {",
  "  var object = function (value) {",
  "    return Object.prototype.toString.call(value) === '[object Object]';",
  "  };",
  "  var scalar = function (value) {",
  "    return value === null || value === undefined || typeof value === 'string' ||",
  "        typeof value === 'boolean' || (typeof value === 'number' && isFinite(value));",
  "  };",
  "  var cells = function (row) {",
  "    for (var i = 0; i < row.length; i++) if (!scalar(row[i])) return false;",
  "    return true;",
  "  };",
  "  if (!object(payload)) return 'malformed response object';",
  "  if (typeof payload.status !== 'string' || !payload.status.trim()) {",
  "    return 'missing response status';",
  "  }",
  "  if (!object(payload.kpis)) return 'malformed kpis';",
  "  if (!cells(dt10KpiValues_(payload.kpis)) || !scalar(payload.kpis.selected_count) ||",
  "      !scalar(payload.kpis.max_selected)) return 'malformed kpi cells';",
  "  var tables = ['selected', 'candidates_rows', 'near_miss', 'alerts'];",
  "  if (payload.qualified !== undefined) tables.push('qualified');",
  "  // These are only fields projected by the candidate/qualified row mappers.",
  "  // Do not invoke their property-reading display fallback during validation.",
  "  var candidateFields = ['symbol', 'name', 'market', 'sector', 'currency', 'price',",
  "      'price_sar', 'roi_pct', 'engine_roi_pct', 'ann_roi_pct', 'rr', 'reliability',",
  "      'dq', 'risk_level', 'news_trend', 'sector_trend', 'verdict', 'confidence_band',",
  "      'opportunity_score', 'failure_reason', 'deferral', '_dt10_held_seat_reason'];",
  "  for (var t = 0; t < tables.length; t++) {",
  "    var rows = payload[tables[t]];",
  "    if (!Array.isArray(rows)) return 'malformed ' + tables[t];",
  "    for (var r = 0; r < rows.length; r++) {",
  "      var item = rows[r];",
  "      if (!object(item)) return 'malformed ' + tables[t] + ' entry';",
  "      if (tables[t] !== 'alerts' &&",
  "          (typeof item.symbol !== 'string' || !item.symbol.trim())) {",
  "        return 'missing ' + tables[t] + ' identity';",
  "      }",
  "      var projected;",
  "      if (tables[t] === 'selected') projected = dt10TicketToRow_(item);",
  "      else if (tables[t] === 'near_miss') projected = dt10NearMissToRow_(item);",
  "      else if (tables[t] === 'alerts') projected = dt10AlertToRow_(item);",
  "      else {",
  "        projected = candidateFields.map(function (field) { return item[field]; });",
  "        projected.push(item.first_fail ? item.first_fail.gate : null);",
  "      }",
  "      if (!cells(projected)) return 'malformed ' + tables[t] + ' cells';",
  "    }",
  "  }",
  "  if (payload.meta !== null && payload.meta !== undefined && !object(payload.meta)) {",
  "    return 'malformed meta';",
  "  }",
  "  return '';",
  "}",
  ""
].join('\n');
const COMPLETION_EDITS = [
  [' * Version: 1.13.11 ', ' * Version: 1.13.9 '],
  [COMPLETION_WHY, ''],
  ["var DT10_VERSION = '1.13.11';", "var DT10_VERSION = '1.13.9';"],
  [RESPONSE_HELPER, ''],
  [' * reason token. Nonempty future status words remain supported when the\n * renderer containers are valid; absent inputs cannot certify an empty board.',
    ' * reason token. Unknown/absent status is treated as GOOD (proven-degraded\n * only) so a future status word cannot freeze the board silently.'],
  ['  var malformed = dt10ResponseProblem_(payload);\n  if (malformed) return malformed;',
    "  if (!payload) { return 'empty response'; }"],
  ["    throw new Error('DT10_REFRESH_FAILED: network request unavailable');", '    return;'],
  ["    throw new Error('DT10_REFRESH_FAILED: HTTP response unavailable');", '    return;'],
  ["    throw new Error('DT10_REFRESH_FAILED: backend result incomplete');", '    return;']
];
const WHY = [
  ' * v1.13.9 (2026-10-14) -- ENTERED CASH LABEL AND HOLDING UTC PROVENANCE',
  ' * The cash KPI echoes an entered amount and cannot certify custody or',
  ' * executable funding. It now always says Entered cash (SAR), including',
  ' * existing layouts. Cash values, inputs, allocation and feed gates are',
  ' * unchanged. Holding reads accept the canonical Last Updated (UTC) header',
  ' * and retain its UTC field name and original instant. Multiple accepted',
  ' * timestamp columns make the input incomplete and omit timestamp proof;',
  ' * no first-column choice can conceal contradictory source timestamps.',
  ' *',
  ''
].join('\n');
const EDITS = [
  [' * Version: 1.13.9 ', ' * Version: 1.13.8 '],
  [WHY, ''],
  ["var DT10_VERSION = '1.13.9';", "var DT10_VERSION = '1.13.8';"],
  ["                     ['iUpdated', extra.updatedKey || 'last_updated']];",
    "                     ['iUpdated', 'last_updated']];"],
  ["                  iWarnings: -1, iUpdated: -1, updatedKey: 'last_updated', fx: null };",
    '                  iWarnings: -1, iUpdated: -1, fx: null };'],
  ["          t === 'lastupdated' || t === 'lastupdatedutc' || t === 'sourcelastupdated' ? 'updated' : '';",
    "          t === 'lastupdated' || t === 'sourcelastupdated' ? 'updated' : '';"],
  ['      if (proofKey) proofSeen[proofKey] = (proofSeen[proofKey] || 0) + 1;',
    '      if (proofKey) proofSeen[proofKey] = true;'],
  ["      } else if (extra.iUpdated < 0 && (t === 'lastupdated' || t === 'lastupdatedutc' || t === 'sourcelastupdated')) {",
    "      } else if (extra.iUpdated < 0 && (t === 'lastupdated' || t === 'sourcelastupdated')) {"],
  ["        extra.updatedKey = t === 'lastupdatedutc' ? 'last_updated_utc' : 'last_updated';\n", ''],
  ['    if (proofSeen.updated > 1) extra.iUpdated = -1;\n', ''],
  ["var DT10_KPI_LABELS = ['Entered cash (SAR)', 'Exp. Gain 12M (SAR)', 'Selected',",
    "var DT10_KPI_LABELS = ['Deployable (SAR)', 'Exp. Gain 12M (SAR)', 'Selected',"],
  [" * (withheld)'. Pool/research stats stay real: Entered cash (the operator's",
    " * (withheld)'. Pool/research stats stay real: Deployable (the operator's"],
  ['  sheet.getRange(DT10_ROW_KPI_LABELS, 1).setValue(DT10_KPI_LABELS[0]);\n', '']
];
function feedHistorySource(source) {
  let historical = source;
  for (const [current, prior] of [...COMPLETION_12_EDITS, ...COMPLETION_EDITS, ...EDITS]) {
    assert.equal(historical.split(current).length - 1, 1,
      'exactly one approved later-release edit: ' + current.split('\n')[0]);
    historical = historical.replace(current, prior);
  }
  assert.equal(crypto.createHash('sha256').update(historical).digest('hex'), HISTORY_SHA256,
    'later versions may change only the enumerated exact edits');
  return historical;
}
module.exports = {feedHistorySource, HISTORY_SHA256};
