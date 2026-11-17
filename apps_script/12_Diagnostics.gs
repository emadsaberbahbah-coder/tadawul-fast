/**
 * 12_Diagnostics.gs
 * Tadawul Tracker — Diagnostics, Health Tests, API Tests, CSV Export,
 * Log Viewer, Full Diagnostic Sweep
 * Version: 1.7.0
 *
 * Aligned with current deployment baseline:
 * - 00_Config.gs          v1.12.1  main pages = 115, Top10 = 118
 * - 03_Schema.gs          v2.5.0   INVESTABILITY_GATE_V578 + no Layout-A OK
 * - 05_Refresh.gs         v1.11.1  final column derived from getFallbackHeaders
 * - 07_Global_Markets.gs  v1.7.1   Global_Markets full 115-column gate page
 * - 11_SpecialPages.gs    v1.6.0   strict Top_10_Investments shortlist
 * - 99_Data_Audit.gs      v1.3.0   115/118 + investability-gate audit
 *
 * v1.7.0 MAIN CHANGES
 * -------------------
 * 1. Removes old 106/109 and Layout A assumptions from the diagnostic sweep.
 * 2. Updates schema-contract handling for 03_Schema.gs v2.5.0 result shape:
 *      { ok, pageCount, statusCounts, pages[] }
 * 3. Adds a Global_Markets investment-gate runner and sweep support.
 * 4. Keeps backward-compatible blank-cells runners for older page modules.
 * 5. Adds Data Audit runner integration for 99_Data_Audit.gs v1.3.0.
 * 6. Keeps Diagnostic_Report sheet schema at 7 columns for compatibility.
 * 7. Uses v1.7.0 RUNNER / SWEEP internal tags.
 */

'use strict';

/* =========================================================
 * Recommendation accuracy thresholds
 * ========================================================= */

var _RECO_ACCURACY_PASS_THRESHOLD_ = 95.0;
var _RECO_ACCURACY_WARN_THRESHOLD_ = 80.0;

/* =========================================================
 * Public editor runners — no trailing underscore
 * ========================================================= */

function runFullDiagnosticSweep() {
  Logger.log('[v1.7.0 RUNNER] runFullDiagnosticSweep invoked');
  return runFullDiagnosticSweep_();
}

function runFullDiagnosticSweep_NoReport() {
  Logger.log('[v1.7.0 RUNNER] runFullDiagnosticSweep_NoReport invoked');
  return runFullDiagnosticSweep_({ writeReport: false });
}

function runFullDiagnosticSweep_AAPL() {
  Logger.log('[v1.7.0 RUNNER] runFullDiagnosticSweep_AAPL invoked');
  return runFullDiagnosticSweep_({ blankCellsSymbol: 'AAPL' });
}

function testHealth() {
  Logger.log('[v1.7.0 RUNNER] testHealth invoked');
  return testHealth_();
}

function testMeta() {
  Logger.log('[v1.7.0 RUNNER] testMeta invoked');
  return testMeta_();
}

function testOpenApi() {
  Logger.log('[v1.7.0 RUNNER] testOpenApi invoked');
  return testOpenApi_();
}

function testMetaAndOpenApi() {
  Logger.log('[v1.7.0 RUNNER] testMetaAndOpenApi invoked');
  return testMetaAndOpenApi_();
}

function testProviderHealth() {
  Logger.log('[v1.7.0 RUNNER] testProviderHealth invoked');
  return testProviderHealth_();
}

function testRecommendationAccuracy_GlobalMarkets() {
  Logger.log('[v1.7.0 RUNNER] testRecommendationAccuracy_GlobalMarkets invoked');
  return testRecommendationAccuracy_(SHEET_NAMES.GLOBAL_MARKETS);
}

function testRecommendationAccuracy_Top10() {
  Logger.log('[v1.7.0 RUNNER] testRecommendationAccuracy_Top10 invoked');
  return testRecommendationAccuracy_(SHEET_NAMES.TOP_10_INVESTMENTS);
}

function testCurrentPageApi() {
  Logger.log('[v1.7.0 RUNNER] testCurrentPageApi invoked');
  return testCurrentPageApi_();
}

function exportCurrentPageCsv() {
  Logger.log('[v1.7.0 RUNNER] exportCurrentPageCsv invoked');
  return exportCurrentPageCsv_();
}

function viewLastRunLog() {
  Logger.log('[v1.7.0 RUNNER] viewLastRunLog invoked');
  return viewLastRunLog_();
}

function aboutTadawulTracker() {
  Logger.log('[v1.7.0 RUNNER] aboutTadawulTracker invoked');
  return aboutTadawulTracker_();
}

function debugGlobalMarketsInvestmentGate_AAPL() {
  Logger.log('[v1.7.0 RUNNER] debugGlobalMarketsInvestmentGate_AAPL invoked');

  if (typeof debugGlobalMarketsInvestmentGate_ === 'function') {
    return _runAndLogDiagnosticFunction_('debugGlobalMarketsInvestmentGate_', function () {
      return debugGlobalMarketsInvestmentGate_('AAPL');
    });
  }

  if (typeof debugGlobalMarketsBlankCells_ === 'function') {
    return _runAndLogDiagnosticFunction_('debugGlobalMarketsBlankCells_', function () {
      return debugGlobalMarketsBlankCells_('AAPL');
    });
  }

  return _showMissingHelper_('debugGlobalMarketsInvestmentGate_', 'Update 07_Global_Markets.gs to v1.7.1 or later.');
}

function debugGlobalMarketsBlankCells_AAPL() {
  Logger.log('[v1.7.0 RUNNER] debugGlobalMarketsBlankCells_AAPL invoked');
  return debugGlobalMarketsInvestmentGate_AAPL();
}

function debugMarketLeadersBlankCells_AAPL() {
  Logger.log('[v1.7.0 RUNNER] debugMarketLeadersBlankCells_AAPL invoked');

  if (typeof debugMarketLeadersBlankCells_ !== 'function') {
    return _showMissingHelper_('debugMarketLeadersBlankCells_', 'Update 06_Market_Leaders.gs to the latest version.');
  }

  return _runAndLogDiagnosticFunction_('debugMarketLeadersBlankCells_', function () {
    return debugMarketLeadersBlankCells_('AAPL');
  });
}

function debugMyPortfolioPreserve() {
  Logger.log('[v1.7.0 RUNNER] debugMyPortfolioPreserve invoked');

  if (typeof debugMyPortfolioPreserve_ !== 'function') {
    return _showMissingHelper_('debugMyPortfolioPreserve_', 'Update 10_My_Portfolio.gs to the latest version.');
  }

  return _runAndLogDiagnosticFunction_('debugMyPortfolioPreserve_', function () {
    return debugMyPortfolioPreserve_();
  });
}

function verifyConfigVersion() {
  Logger.log('[v1.7.0 RUNNER] verifyConfigVersion invoked');

  if (typeof debugVerifyConfigVersion_ !== 'function') {
    return _showMissingHelper_('debugVerifyConfigVersion_', 'Update 00_Config.gs to v1.12.1 or later.');
  }

  return _runAndLogDiagnosticFunction_('debugVerifyConfigVersion_', function () {
    return debugVerifyConfigVersion_();
  });
}

function verifySchemaPropagation() {
  Logger.log('[v1.7.0 RUNNER] verifySchemaPropagation invoked');

  if (typeof debugV28SchemaPropagation_ !== 'function') {
    return _showMissingHelper_('debugV28SchemaPropagation_', 'Update 03_Schema.gs to v2.5.0 or later.');
  }

  return _runAndLogDiagnosticFunction_('debugV28SchemaPropagation_', function () {
    return debugV28SchemaPropagation_(SHEET_NAMES.MARKET_LEADERS);
  });
}

function diagnoseSchemaContractStatus() {
  Logger.log('[v1.7.0 RUNNER] diagnoseSchemaContractStatus invoked');

  if (typeof diagnoseSchemaContractStatus_ !== 'function') {
    return _showMissingHelper_('diagnoseSchemaContractStatus_', 'Update 03_Schema.gs to v2.5.0 or later.');
  }

  return _runAndLogDiagnosticFunction_('diagnoseSchemaContractStatus_', function () {
    return diagnoseSchemaContractStatus_();
  });
}

function runDataAuditAllPages() {
  Logger.log('[v1.7.0 RUNNER] runDataAuditAllPages invoked');

  if (typeof auditAllPages !== 'function') {
    return _showMissingHelper_('auditAllPages', 'Update 99_Data_Audit.gs to v1.3.0 or later.');
  }

  return _runAndLogDiagnosticFunction_('auditAllPages', function () {
    return auditAllPages();
  });
}

function runDataAuditGlobalMarkets() {
  Logger.log('[v1.7.0 RUNNER] runDataAuditGlobalMarkets invoked');

  if (typeof auditGlobalMarkets !== 'function') {
    return _showMissingHelper_('auditGlobalMarkets', 'Update 99_Data_Audit.gs to v1.3.0 or later.');
  }

  return _runAndLogDiagnosticFunction_('auditGlobalMarkets', function () {
    return auditGlobalMarkets();
  });
}

function runDataAuditTop10() {
  Logger.log('[v1.7.0 RUNNER] runDataAuditTop10 invoked');

  if (typeof auditTop10Investments === 'function') {
    return _runAndLogDiagnosticFunction_('auditTop10Investments', function () {
      return auditTop10Investments();
    });
  }

  if (typeof auditPage === 'function') {
    return _runAndLogDiagnosticFunction_('auditPage Top_10_Investments', function () {
      return auditPage(SHEET_NAMES.TOP_10_INVESTMENTS);
    });
  }

  return _showMissingHelper_('auditTop10Investments/auditPage', 'Update 99_Data_Audit.gs to v1.3.0 or later.');
}

/* =========================================================
 * Backend diagnostic functions
 * ========================================================= */

function testHealth_() {
  var endpointCandidates = [ENDPOINTS.HEALTH, '/readyz', ENDPOINTS.META];
  var best = null;

  for (var i = 0; i < endpointCandidates.length; i++) {
    var path = endpointCandidates[i];
    var resp = apiGet_(path, {}, { maxRetries: 2 });
    var summary = summarizeDiagnosticResponse_('GET', path, resp);

    if (!best) best = summary;
    if (resp.ok && resp.json) {
      best = summary;
      break;
    }
  }

  logRun_(best && best.ok ? 'INFO' : 'WARN', 'testHealth', '', best && best.ok ? 'Health test passed.' : 'Health test returned warnings.', {
    status: best && best.ok ? 'PASS' : 'WARN',
    endpoint: best ? best.path : '',
    httpCode: best ? best.statusCode : '',
    durationMs: best ? best.durationMs : '',
    details: best || {}
  });

  writeGlobalStatus_({
    healthStatus: best && best.ok ? 'HEALTH_OK' : 'HEALTH_WARN',
    version: best && best.version ? best.version : '',
    message: best && best.message ? best.message : 'Health test executed'
  });

  showDiagnosticResultDialog_('Health Test', [
    'OK: ' + ((best && best.ok) ? 'Yes' : 'No'),
    'Path: ' + (best ? best.path : ''),
    'HTTP: ' + (best ? best.statusCode : ''),
    'Status: ' + (best ? best.statusText : ''),
    'Service: ' + (best ? best.service : ''),
    'Version: ' + (best ? best.version : ''),
    'Engine: ' + (best ? best.engine : ''),
    'Message: ' + (best ? best.message : '')
  ]);

  return best;
}

function testMeta_() {
  var resp = apiGet_(ENDPOINTS.META, {}, { maxRetries: 2 });
  var summary = summarizeDiagnosticResponse_('GET', ENDPOINTS.META, resp);

  logRun_(summary.ok ? 'INFO' : 'WARN', 'testMeta', '', summary.ok ? 'Meta test passed.' : 'Meta test returned warnings.', {
    status: summary.ok ? 'PASS' : 'WARN',
    endpoint: summary.path,
    httpCode: summary.statusCode,
    durationMs: summary.durationMs,
    details: summary
  });

  showDiagnosticResultDialog_('Meta Test', [
    'OK: ' + (summary.ok ? 'Yes' : 'No'),
    'HTTP: ' + summary.statusCode,
    'Status: ' + summary.statusText,
    'Service: ' + summary.service,
    'Version: ' + summary.version,
    'Engine: ' + summary.engine,
    'Routes Mounted: ' + summary.routesMounted,
    'Message: ' + summary.message
  ]);

  return summary;
}

function testOpenApi_() {
  var resp = apiGet_(ENDPOINTS.OPENAPI, {}, { maxRetries: 2 });
  var json = resp.json || {};
  var pathCount = 0;

  if (json && json.paths && typeof json.paths === 'object' && !Array.isArray(json.paths)) {
    pathCount = Object.keys(json.paths).length;
  }

  var summary = {
    ok: !!resp.ok,
    method: 'GET',
    path: ENDPOINTS.OPENAPI,
    statusCode: resp.statusCode || 0,
    durationMs: resp.durationMs || 0,
    title: json && json.info ? String(json.info.title || '') : '',
    version: json && json.info ? String(json.info.version || '') : '',
    pathCount: pathCount,
    message: resp.message || ''
  };

  logRun_(summary.ok ? 'INFO' : 'WARN', 'testOpenApi', '', summary.ok ? 'OpenAPI test passed.' : 'OpenAPI test returned warnings.', {
    status: summary.ok ? 'PASS' : 'WARN',
    endpoint: summary.path,
    httpCode: summary.statusCode,
    durationMs: summary.durationMs,
    details: summary
  });

  showDiagnosticResultDialog_('OpenAPI Test', [
    'OK: ' + (summary.ok ? 'Yes' : 'No'),
    'HTTP: ' + summary.statusCode,
    'Title: ' + summary.title,
    'Version: ' + summary.version,
    'Path Count: ' + summary.pathCount,
    'Message: ' + summary.message
  ]);

  return summary;
}

function testMetaAndOpenApi_() {
  var meta = testMeta_();
  var openapi = testOpenApi_();
  return { ok: !!(meta && meta.ok && openapi && openapi.ok), meta: meta, openapi: openapi };
}

function testProviderHealth_() {
  var path = '/v1/schema/provider-health';
  if (typeof ENDPOINTS !== 'undefined' && ENDPOINTS && ENDPOINTS.PROVIDER_HEALTH) {
    path = ENDPOINTS.PROVIDER_HEALTH;
  }

  var resp = apiGet_(path, {}, { maxRetries: 2 });
  var json = resp.json || {};
  var providerHealth = json && json.provider_health ? json.provider_health : {};
  var markers = providerHealth && providerHealth.unhealthy_markers ? providerHealth.unhealthy_markers : {};
  var active = markers && Array.isArray(markers.active) ? markers.active : [];
  var activeCount = markers && typeof markers.active_count === 'number' ? markers.active_count : active.length;
  var responseStatus = json && json.status ? String(json.status) : '';
  var engineVersion = String((json && json.engine_version) || (providerHealth && providerHealth.engine_version) || '');
  var routeVersion = String((json && json.version) || '');
  var engineSource = String((json && json.engine_source) || '');
  var reason = providerHealth && providerHealth.reason ? String(providerHealth.reason) : '';

  var verdict;
  var verdictMessage;
  if (!resp.ok) {
    verdict = 'WARN';
    verdictMessage = 'Provider health endpoint unreachable (HTTP ' + (resp.statusCode || 0) + ').';
  } else if (responseStatus === 'unavailable') {
    verdict = 'WARN';
    verdictMessage = 'Provider health surface unavailable (reason=' + (reason || 'unknown') + ').';
  } else if (responseStatus === 'success' && activeCount === 0) {
    verdict = 'PASS';
    verdictMessage = 'All upstream providers healthy.';
  } else if (responseStatus === 'success' && activeCount > 0) {
    verdict = 'WARN';
    verdictMessage = activeCount + ' provider(s) unhealthy; engine routing around.';
  } else {
    verdict = 'WARN';
    verdictMessage = 'Unexpected response shape (status=' + (responseStatus || '?') + ').';
  }

  var summary = {
    ok: verdict === 'PASS',
    method: 'GET',
    path: path,
    statusCode: resp.statusCode || 0,
    durationMs: resp.durationMs || 0,
    responseStatus: responseStatus,
    routeVersion: routeVersion,
    engineVersion: engineVersion,
    engineSource: engineSource,
    activeCount: activeCount,
    activeProviders: active,
    reason: reason,
    verdict: verdict,
    verdictMessage: verdictMessage,
    message: resp.message || ''
  };

  logRun_(verdict === 'PASS' ? 'INFO' : 'WARN', 'testProviderHealth', '', verdictMessage, {
    status: verdict,
    endpoint: summary.path,
    httpCode: summary.statusCode,
    durationMs: summary.durationMs,
    details: summary
  });

  showDiagnosticResultDialog_('Provider Health Test', [
    'OK: ' + (summary.ok ? 'Yes' : 'No'),
    'Verdict: ' + verdict,
    'HTTP: ' + summary.statusCode,
    'Path: ' + summary.path,
    'Response Status: ' + (responseStatus || '(unknown)'),
    'Route Version: ' + (routeVersion || '(unknown)'),
    'Engine Version: ' + (engineVersion || '(unknown)'),
    'Engine Source: ' + (engineSource || '(unknown)'),
    'Unhealthy Provider Count: ' + activeCount,
    '',
    verdictMessage
  ]);

  return summary;
}

function testRecommendationAccuracy_(pageName) {
  var targetPage = pageName || SHEET_NAMES.GLOBAL_MARKETS;

  if (typeof diagnoseRecommendationAccuracy_ !== 'function') {
    var skipSummary = {
      ok: false,
      verdict: 'WARN',
      pageName: targetPage,
      message: 'diagnoseRecommendationAccuracy_ is not available. Deploy 11_SpecialPages.gs v1.6.0 or later.',
      result: null
    };
    showDiagnosticResultDialog_('Recommendation Accuracy Test', ['OK: No', 'Verdict: WARN', 'Page: ' + targetPage, '', skipSummary.message]);
    return skipSummary;
  }

  var startedAt = new Date().getTime();
  var result;

  try {
    result = diagnoseRecommendationAccuracy_(targetPage);
  } catch (e) {
    var errMsg = e && e.message ? e.message : String(e);
    showDiagnosticResultDialog_('Recommendation Accuracy Test', ['OK: No', 'Verdict: FAIL', 'Page: ' + targetPage, '', 'Helper threw: ' + errMsg]);
    return { ok: false, verdict: 'FAIL', pageName: targetPage, message: 'Helper threw: ' + errMsg, result: null };
  }

  var durationMs = new Date().getTime() - startedAt;
  if (!result || result.skipped) {
    var skipMsg = result && result.message ? result.message : 'Page does not carry Recommendation/Overall Score columns.';
    showDiagnosticResultDialog_('Recommendation Accuracy Test - ' + targetPage, ['OK: Yes', 'Verdict: PASS', 'Page: ' + targetPage, 'Duration: ' + durationMs + 'ms', '', 'Skipped: ' + skipMsg]);
    return { ok: true, verdict: 'PASS', pageName: targetPage, message: skipMsg + ' (skipped)', durationMs: durationMs, result: result || null };
  }

  var accuracyPct = typeof result.accuracyPct === 'number' ? result.accuracyPct : 0;
  var rowsScanned = typeof result.rowsScanned === 'number' ? result.rowsScanned : 0;
  var rowsMatching = typeof result.rowsMatching === 'number' ? result.rowsMatching : 0;
  var rowsDiffering = typeof result.rowsDiffering === 'number' ? result.rowsDiffering : 0;
  var rowsUnscored = typeof result.rowsUnscored === 'number' ? result.rowsUnscored : 0;

  var verdict = rowsScanned === 0 || accuracyPct >= _RECO_ACCURACY_PASS_THRESHOLD_
    ? 'PASS'
    : (accuracyPct >= _RECO_ACCURACY_WARN_THRESHOLD_ ? 'WARN' : 'FAIL');

  var summary = {
    ok: verdict === 'PASS',
    verdict: verdict,
    pageName: targetPage,
    accuracyPct: accuracyPct,
    rowsScanned: rowsScanned,
    rowsMatching: rowsMatching,
    rowsDiffering: rowsDiffering,
    rowsUnscored: rowsUnscored,
    durationMs: durationMs,
    result: result
  };

  logRun_(verdict === 'PASS' ? 'INFO' : 'WARN', 'testRecommendationAccuracy', targetPage, 'Recommendation accuracy: ' + accuracyPct + '% (' + verdict + ')', {
    status: verdict,
    durationMs: durationMs,
    details: summary
  });

  showDiagnosticResultDialog_('Recommendation Accuracy Test - ' + targetPage, [
    'OK: ' + (summary.ok ? 'Yes' : 'No'),
    'Verdict: ' + verdict,
    'Page: ' + targetPage,
    'Duration: ' + durationMs + 'ms',
    '',
    'Accuracy: ' + accuracyPct + '%',
    'Rows Scanned: ' + rowsScanned,
    'Rows Matching: ' + rowsMatching,
    'Rows Differing: ' + rowsDiffering,
    'Rows Unscored: ' + rowsUnscored
  ]);

  return summary;
}

function testCurrentPageApi_(pageName) {
  var normalized = normalizePageName_(pageName || getActivePageName_());
  var page = getPageConfig(normalized);
  var testRequest = buildDiagnosticPageRequest_(page.pageName);
  var resp = executeEndpointRequest_(testRequest);
  var parsed = normalizeEndpointResponse_(page, testRequest.endpoint, resp);

  var headersCount = parsed.normalized && parsed.normalized.headers ? parsed.normalized.headers.length : 0;
  var keysCount = parsed.normalized && parsed.normalized.keys ? parsed.normalized.keys.length : 0;
  var rowCount = parsed.normalized && parsed.normalized.rows2d ? parsed.normalized.rows2d.length : 0;

  var summary = {
    ok: !!parsed.ok,
    pageName: page.pageName,
    endpoint: parsed.endpointUsed || testRequest.endpoint,
    method: testRequest.method,
    statusCode: parsed.statusCode || 0,
    durationMs: parsed.durationMs || 0,
    payloadStatus: parsed.payloadStatus || '',
    headersCount: headersCount,
    keysCount: keysCount,
    rowCount: rowCount,
    warnings: parsed.warnings || [],
    message: parsed.message || ''
  };

  logRun_(summary.ok ? 'INFO' : 'WARN', 'testCurrentPageApi', page.pageName, summary.ok ? 'Current page API test passed.' : 'Current page API test returned warnings.', {
    status: summary.ok ? 'PASS' : 'WARN',
    endpoint: summary.endpoint,
    httpCode: summary.statusCode,
    durationMs: summary.durationMs,
    details: summary
  });

  showDiagnosticResultDialog_('Current Page API Test - ' + page.pageName, [
    'OK: ' + (summary.ok ? 'Yes' : 'No'),
    'Method: ' + summary.method,
    'Endpoint: ' + summary.endpoint,
    'HTTP: ' + summary.statusCode,
    'Payload Status: ' + summary.payloadStatus,
    'Headers Count: ' + summary.headersCount,
    'Keys Count: ' + summary.keysCount,
    'Rows Count: ' + summary.rowCount,
    'Warnings: ' + (summary.warnings.length ? summary.warnings.join(' | ') : 'None'),
    'Message: ' + summary.message
  ]);

  return summary;
}

function exportCurrentPageCsv_(pageName) {
  var normalized = normalizePageName_(pageName || getActivePageName_());
  var sheet = ensureSheetExists_(normalized);
  var data = sheet.getDataRange().getValues();

  if (!data || !data.length) {
    throw new Error('No data found to export for page: ' + normalized);
  }

  var csv = convertRangeValuesToCsv_(data);
  var tz = _resolveTimezone_();
  var timestamp = Utilities.formatDate(new Date(), tz, 'yyyyMMdd_HHmmss');
  var fileName = normalized + '_' + timestamp + '.csv';
  var file = DriveApp.createFile(fileName, csv, MimeType.CSV);

  logRun_('INFO', 'exportCurrentPageCsv', normalized, 'CSV exported successfully.', {
    status: 'OK', fileName: fileName, fileId: file.getId()
  });

  showDiagnosticResultDialog_('CSV Export', [
    'Page: ' + normalized,
    'File Name: ' + fileName,
    'Drive File ID: ' + file.getId(),
    'Rows Exported: ' + data.length
  ]);

  return { ok: true, pageName: normalized, fileId: file.getId(), fileName: fileName, rowCount: data.length };
}

function viewLastRunLog_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var logSheet = getLogSheet_();
  ss.setActiveSheet(logSheet);

  var lastRow = logSheet.getLastRow();
  var lastCol = logSheet.getLastColumn();
  if (lastRow > 2 && lastCol > 0) {
    try {
      logSheet.getRange(2, 1, lastRow - 1, lastCol).sort({ column: 1, ascending: false });
    } catch (e) {}
  }

  return { ok: true, sheetName: logSheet.getName(), rowCount: Math.max(0, lastRow - 1) };
}

function aboutTadawulTracker_() {
  var summary = getConfigSummary_();
  var rootTitle = typeof ROOT_MENU_TITLE_ !== 'undefined' ? ROOT_MENU_TITLE_ : 'Tadawul Tracker';

  var message = [
    rootTitle,
    '',
    'App Name: ' + summary.appName,
    'Version: ' + summary.version,
    'Backend: ' + summary.backendName,
    'Base URL: ' + (summary.baseUrl || '(not set)'),
    'Token Loaded: ' + (summary.tokenLoaded ? 'Yes' : 'No'),
    'Default Timeout: ' + summary.timeoutSec + ' sec',
    'Default Limit: ' + summary.defaultLimit,
    'Default Mode: ' + summary.defaultMode,
    'Supported Pages: ' + (summary.supportedPages ? summary.supportedPages.length : 0),
    'Log Sheet: ' + summary.logSheetName,
    'Status Sheet: ' + summary.statusSheetName,
    'Insights Keep Last Good: ' + (summary.insightsKeepLastGood ? 'Yes' : 'No'),
    'Diagnostics: 12_Diagnostics.gs v1.7.0'
  ].join('\n');

  SpreadsheetApp.getUi().alert('About Tadawul Tracker', message, SpreadsheetApp.getUi().ButtonSet.OK);
  return { ok: true, summary: summary };
}

/* =========================================================
 * Full diagnostic sweep
 * ========================================================= */

function runFullDiagnosticSweep_(options) {
  options = options || {};
  var writeReport = options.writeReport !== false;
  var blankCellsSymbol = options.blankCellsSymbol || '';
  var sweepId = _generateSweepId_();
  var startedAt = new Date();
  var steps = [];

  Logger.log('[v1.7.0 SWEEP] Starting full diagnostic sweep id=' + sweepId);
  Logger.log('[v1.7.0 SWEEP] writeReport=' + writeReport + ' blankCellsSymbol=' + (blankCellsSymbol || '(default)'));

  steps.push(_sweepStep_('health', 'Backend health probe', function () {
    var resp = apiGet_(ENDPOINTS.HEALTH, {}, { maxRetries: 1 });
    var summary = summarizeDiagnosticResponse_('GET', ENDPOINTS.HEALTH, resp);
    return { ok: summary.ok, summary: 'HTTP ' + summary.statusCode + ' service=' + summary.service + ' version=' + summary.version, details: summary };
  }));

  steps.push(_sweepStep_('meta', 'Backend meta probe', function () {
    var resp = apiGet_(ENDPOINTS.META, {}, { maxRetries: 1 });
    var summary = summarizeDiagnosticResponse_('GET', ENDPOINTS.META, resp);
    return { ok: summary.ok, summary: 'HTTP ' + summary.statusCode + ' routes=' + summary.routesMounted, details: summary };
  }));

  steps.push(_sweepStep_('openapi', 'OpenAPI schema probe', function () {
    var resp = apiGet_(ENDPOINTS.OPENAPI, {}, { maxRetries: 1 });
    var json = resp.json || {};
    var pathCount = json && json.paths && typeof json.paths === 'object' ? Object.keys(json.paths).length : 0;
    return { ok: !!resp.ok && pathCount > 0, summary: 'HTTP ' + (resp.statusCode || 0) + ' paths=' + pathCount, details: { statusCode: resp.statusCode, pathCount: pathCount } };
  }));

  steps.push(_sweepStep_('config_version', 'Config version verify (00_Config v1.12.1)', function () {
    if (typeof debugVerifyConfigVersion_ !== 'function') {
      return { ok: true, summary: 'helper not present (skipped)', details: { skipped: true } };
    }
    var result = debugVerifyConfigVersion_();
    return { ok: result && result.ok !== false, summary: 'Config version check executed', details: result || {} };
  }));

  steps.push(_sweepStep_('schema_gate_propagation', 'Schema propagation verify (115/118 + investability gate)', function () {
    if (typeof debugV28SchemaPropagation_ !== 'function') {
      return { ok: true, summary: 'helper not present (skipped)', details: { skipped: true, reason: '03_Schema v2.5.0 not deployed' } };
    }
    var probePage = SHEET_NAMES.MARKET_LEADERS;
    var result = debugV28SchemaPropagation_(probePage);
    var missingConfig = _extractNumber_(result, ['trackedMissingInConfigTotal', 'v28MissingInConfigTotal'], 0);
    var missingSheet = _extractNumber_(result, ['trackedMissingOnSheetTotal'], 0);
    var widthOk = result && result.widthOk !== false;
    return {
      ok: !!(result && result.ok && missingConfig === 0 && missingSheet === 0 && widthOk),
      summary: 'page=' + probePage + ' missingConfig=' + missingConfig + ' missingSheet=' + missingSheet + ' widthOk=' + widthOk,
      details: result || {}
    };
  }));

  steps.push(_sweepStep_('schema_contract_status', 'Schema contract status (115/118 current contract)', function () {
    if (typeof diagnoseSchemaContractStatus_ !== 'function') {
      return { ok: true, summary: 'helper not present (skipped)', details: { skipped: true, reason: '03_Schema v2.5.0 not deployed' } };
    }
    var result = diagnoseSchemaContractStatus_();
    var counts = result && result.statusCounts ? result.statusCounts : {};
    var aligned = counts.aligned || 0;
    var total = result && typeof result.pageCount === 'number' ? result.pageCount : (result && result.pages ? result.pages.length : 0);
    var nonAligned = Math.max(0, total - aligned);
    return { ok: !!(result && result.ok && nonAligned === 0), summary: 'aligned=' + aligned + '/' + total + ' nonAligned=' + nonAligned, details: result || {} };
  }));

  steps.push(_sweepStep_('backend_paths', 'Backend GET vs POST verify', function () {
    if (typeof debugCheckBackendPaths_ !== 'function') {
      return { ok: true, summary: 'helper not present (skipped)', details: { skipped: true } };
    }
    var result = debugCheckBackendPaths_();
    return { ok: result && result.ok !== false, summary: 'Backend paths check executed', details: result || {} };
  }));

  steps.push(_sweepStep_('provider_health', 'Provider health probe', function () {
    var result = testProviderHealth_();
    return { ok: result && result.ok, summary: result ? result.verdictMessage : '', details: result || {} };
  }));

  steps.push(_sweepStepForRecommendationAccuracy_(SHEET_NAMES.GLOBAL_MARKETS, 'reco_accuracy_global_markets', 'Reco accuracy Global_Markets'));
  steps.push(_sweepStepForRecommendationAccuracy_(SHEET_NAMES.TOP_10_INVESTMENTS, 'reco_accuracy_top10', 'Reco accuracy Top_10_Investments'));

  var pageDiagnostics = [
    { page: SHEET_NAMES.MARKET_LEADERS, fn: 'diagnoseMarketLeaders_' },
    { page: SHEET_NAMES.GLOBAL_MARKETS, fn: 'diagnoseGlobalMarkets_' },
    { page: SHEET_NAMES.COMMODITIES_FX, fn: 'diagnoseCommoditiesFx_' },
    { page: SHEET_NAMES.MUTUAL_FUNDS, fn: 'diagnoseMutualFunds_' },
    { page: SHEET_NAMES.MY_PORTFOLIO, fn: 'diagnoseMyPortfolio_' },
    { page: SHEET_NAMES.INSIGHTS_ANALYSIS, fn: 'diagnoseInsightsAnalysis_' },
    { page: SHEET_NAMES.TOP_10_INVESTMENTS, fn: 'diagnoseTop10Investments_' },
    { page: SHEET_NAMES.DATA_DICTIONARY, fn: 'diagnoseDataDictionary_' }
  ];

  for (var d = 0; d < pageDiagnostics.length; d++) {
    steps.push(_sweepStepForDiagnose_(pageDiagnostics[d].page, pageDiagnostics[d].fn));
  }

  steps.push(_sweepStep_('criteria_visibility', 'Criteria visibility / hard filters', function () {
    if (typeof diagnoseV310CriteriaVisibility_ !== 'function') {
      return { ok: true, summary: 'helper not present (skipped)', details: { skipped: true } };
    }
    var result = diagnoseV310CriteriaVisibility_();
    var fieldsDetected = _extractNumber_(result, ['hardFilterFieldsDetected', 'fieldsPresent'], result && result.hardFilterFields ? result.hardFilterFields.length : 0);
    return { ok: !!(result && result.ok), summary: 'hardFilterFields=' + fieldsDetected, details: result || {} };
  }));

  steps.push(_sweepStep_('insights_dq_alerts', 'Insights Data Quality Alerts', function () {
    if (typeof diagnoseDataQualityAlerts_ !== 'function') {
      return { ok: true, summary: 'helper not present (skipped)', details: { skipped: true } };
    }
    var result = diagnoseDataQualityAlerts_();
    var alertRows = _extractNumber_(result, ['alertRowCount', 'alertsFound'], result && result.alerts ? result.alerts.length : 0);
    return { ok: !!(result && result.ok), summary: 'sectionPresent=' + (result && result.sectionPresent ? 'yes' : 'no') + ' alertRows=' + alertRows, details: result || {} };
  }));

  steps.push(_sweepStepForBlankCells_(SHEET_NAMES.MARKET_LEADERS, 'debugMarketLeadersBlankCells_', blankCellsSymbol));
  steps.push(_sweepStepForGlobalMarketsGate_(blankCellsSymbol));

  steps.push(_sweepStep_('portfolio_preserve', 'My_Portfolio preserve column inspection', function () {
    if (typeof debugMyPortfolioPreserve_ !== 'function') {
      return { ok: true, summary: 'helper not present (skipped)', details: { skipped: true } };
    }
    var result = debugMyPortfolioPreserve_(blankCellsSymbol || undefined);
    var preserveCount = result && result.preserveColumns ? result.preserveColumns.length : 0;
    return { ok: !!(result && result.ok), summary: 'symbol=' + (result && result.symbol ? result.symbol : '(none)') + ' preserveColumns=' + preserveCount, details: result || {} };
  }));

  steps.push(_sweepStep_('data_audit_all_pages', 'Data audit all pages (99_Data_Audit v1.3.0)', function () {
    if (typeof auditAllPages !== 'function') {
      return { ok: true, summary: 'auditAllPages not present (skipped)', details: { skipped: true, reason: '99_Data_Audit v1.3.0 not deployed' } };
    }
    var result = auditAllPages();
    return { ok: result && result.ok !== false, summary: 'pagesAudited=' + (result && result.pagesAudited !== undefined ? result.pagesAudited : '?'), details: _compactAuditDetails_(result || {}) };
  }));

  var overallVerdict = _deriveOverallVerdict_(steps);
  var finishedAt = new Date();
  var durationMs = finishedAt.getTime() - startedAt.getTime();

  Logger.log('[v1.7.0 SWEEP] Completed sweepId=' + sweepId + ' verdict=' + overallVerdict + ' steps=' + steps.length + ' durationMs=' + durationMs);

  logRun_(overallVerdict === 'PASS' ? 'INFO' : 'WARN', 'runFullDiagnosticSweep', '', 'Full diagnostic sweep completed: ' + overallVerdict, {
    status: overallVerdict,
    durationMs: durationMs,
    details: { sweepId: sweepId, stepCount: steps.length, verdict: overallVerdict, passCount: _countByVerdict_(steps, 'PASS'), warnCount: _countByVerdict_(steps, 'WARN'), failCount: _countByVerdict_(steps, 'FAIL') }
  });

  if (writeReport) {
    try {
      _writeDiagnosticReport_(sweepId, overallVerdict, steps, startedAt);
    } catch (writeErr) {
      Logger.log('[v1.7.0 SWEEP] Failed to write Diagnostic_Report sheet: ' + (writeErr.message || writeErr));
    }
  }

  var dialogLines = [
    'Sweep ID: ' + sweepId,
    'Verdict: ' + overallVerdict,
    'Duration: ' + durationMs + 'ms',
    'Steps: ' + steps.length,
    'Pass: ' + _countByVerdict_(steps, 'PASS'),
    'Warn: ' + _countByVerdict_(steps, 'WARN'),
    'Fail: ' + _countByVerdict_(steps, 'FAIL'),
    ''
  ];

  for (var x = 0; x < steps.length; x++) {
    dialogLines.push(steps[x].verdict + ' — ' + steps[x].name + ': ' + (steps[x].summary || ''));
  }

  if (writeReport) {
    dialogLines.push('');
    dialogLines.push('Full report written to "Diagnostic_Report" sheet.');
  }

  showDiagnosticResultDialog_('Full Diagnostic Sweep — ' + overallVerdict, dialogLines);

  return { sweepId: sweepId, verdict: overallVerdict, durationMs: durationMs, steps: steps };
}

/* =========================================================
 * Request builders and summarizers
 * ========================================================= */

function buildDiagnosticPageRequest_(pageName) {
  var page = getPageConfig(pageName);
  var method = page.defaultMethod || 'POST';

  if (page.pageName === SHEET_NAMES.DATA_DICTIONARY) {
    return { method: 'GET', endpoint: ENDPOINTS.SCHEMA_DATA_DICTIONARY, params: {}, payload: null };
  }

  var endpoint = page.primaryEndpoint;
  var payload = buildBasePayload(page.pageName, { limit: 10 });

  return { method: method, endpoint: endpoint, params: method === 'GET' ? payload : {}, payload: method === 'GET' ? null : payload };
}

function summarizeDiagnosticResponse_(method, path, resp) {
  var safeResp = resp || {};
  var json = safeResp.json || {};
  var payload = safeResp.payload || unwrapPayload_(json) || json || {};
  var statusText = '';

  if (typeof derivePayloadStatus_ === 'function') {
    statusText = derivePayloadStatus_(json, payload);
  }

  return {
    ok: !!safeResp.ok,
    method: method,
    path: path,
    statusCode: safeResp.statusCode || 0,
    durationMs: safeResp.durationMs || 0,
    statusText: statusText,
    service: String((json && (json.service || json.app_name || json.name)) || (payload && (payload.service || payload.app_name || payload.name)) || ''),
    version: String((json && (json.entry_version || json.version || json.app_version)) || (payload && (payload.entry_version || payload.version || payload.app_version)) || ''),
    engine: String((json && json.engine_source) || (payload && payload.engine_source) || ''),
    routesMounted: String((json && json.routes_mounted) || (payload && payload.routes_mounted) || ''),
    message: safeResp.message || ''
  };
}

function showDiagnosticResultDialog_(title, lines) {
  var safeLines = Array.isArray(lines) ? lines : [];
  SpreadsheetApp.getUi().alert(title, safeLines.join('\n'), SpreadsheetApp.getUi().ButtonSet.OK);
}

/* =========================================================
 * CSV helpers
 * ========================================================= */

function convertRangeValuesToCsv_(values) {
  var safeValues = Array.isArray(values) ? values : [];
  var rows = [];

  for (var i = 0; i < safeValues.length; i++) {
    var row = Array.isArray(safeValues[i]) ? safeValues[i] : [];
    var cells = [];

    for (var j = 0; j < row.length; j++) {
      cells.push(escapeCsvCell_(row[j]));
    }

    rows.push(cells.join(','));
  }

  return rows.join('\r\n');
}

function escapeCsvCell_(value) {
  if (value === null || value === undefined) return '';

  var text;
  if (value instanceof Date) {
    text = Utilities.formatDate(value, _resolveTimezone_(), 'yyyy-MM-dd HH:mm:ss');
  } else {
    text = String(value);
  }

  if (text.indexOf('"') > -1) text = text.replace(/"/g, '""');
  if (/[",\n\r]/.test(text)) text = '"' + text + '"';
  return text;
}

/* =========================================================
 * Sweep helpers
 * ========================================================= */

function _sweepStep_(stepKey, stepName, fn) {
  Logger.log('[v1.7.0 SWEEP] step=' + stepKey + ' starting...');

  try {
    var result = fn();
    var verdict;

    if (result && result.details && result.details.skipped) verdict = 'PASS';
    else if (result && result.ok) verdict = 'PASS';
    else if (result && result.verdict === 'FAIL') verdict = 'FAIL';
    else verdict = 'WARN';

    var summary = result && result.summary ? result.summary : '';
    Logger.log('[v1.7.0 SWEEP] step=' + stepKey + ' verdict=' + verdict + ' summary=' + summary);

    return { name: stepName, key: stepKey, verdict: verdict, summary: summary, details: result && result.details ? result.details : {} };
  } catch (e) {
    var errMsg = e && e.message ? e.message : String(e);
    Logger.log('[v1.7.0 SWEEP] step=' + stepKey + ' THREW: ' + errMsg);
    return { name: stepName, key: stepKey, verdict: 'FAIL', summary: 'threw: ' + errMsg, details: { error: errMsg } };
  }
}

function _sweepStepForRecommendationAccuracy_(pageName, key, name) {
  return _sweepStep_(key, name, function () {
    if (typeof diagnoseRecommendationAccuracy_ !== 'function') {
      return { ok: true, summary: 'helper not present (skipped)', details: { skipped: true, reason: '11_SpecialPages v1.6.0 not deployed' } };
    }

    var result = diagnoseRecommendationAccuracy_(pageName);
    if (!result || result.skipped) {
      return { ok: true, summary: 'skipped: ' + ((result && result.message) || 'no-op'), details: result || { skipped: true } };
    }

    var accuracyPct = typeof result.accuracyPct === 'number' ? result.accuracyPct : 0;
    var rowsScanned = typeof result.rowsScanned === 'number' ? result.rowsScanned : 0;
    var rowsMatching = typeof result.rowsMatching === 'number' ? result.rowsMatching : 0;
    var rowsDiffering = typeof result.rowsDiffering === 'number' ? result.rowsDiffering : 0;
    var ok = rowsScanned > 0 && accuracyPct >= _RECO_ACCURACY_PASS_THRESHOLD_;

    return { ok: ok, summary: 'accuracy=' + accuracyPct + '% scanned=' + rowsScanned + ' matching=' + rowsMatching + ' differing=' + rowsDiffering, details: result };
  });
}

function _sweepStepForDiagnose_(pageName, fnName) {
  return _sweepStep_('diagnose_' + pageName, 'Diagnose ' + pageName, function () {
    var fn = _lookupGlobalFunction_(fnName);
    if (typeof fn !== 'function') {
      return { ok: true, summary: fnName + ' not present (skipped)', details: { skipped: true, reason: 'function not found' } };
    }

    var result = fn();
    var headerCount = _extractNumber_(result, ['headerCount', 'actualHeadersCount', 'headersCount'], 0);
    var rowCount = _extractNumber_(result, ['rowCount', 'rows'], 0);
    var missing = 0;
    if (result && result.missingBusinessHeaders) missing += result.missingBusinessHeaders.length;
    if (result && result.missingCoreHeaders) missing += result.missingCoreHeaders.length;
    if (result && result.missingGateHeaders) missing += result.missingGateHeaders.length;

    return { ok: !!(result && result.ok), summary: 'headers=' + headerCount + ' rows=' + rowCount + (missing ? ' missing=' + missing : ''), details: result || {} };
  });
}

function _sweepStepForBlankCells_(pageName, fnName, optSymbol) {
  return _sweepStep_('blankcells_' + pageName, 'Blank-cells diag ' + pageName, function () {
    var fn = _lookupGlobalFunction_(fnName);
    if (typeof fn !== 'function') {
      return { ok: true, summary: fnName + ' not present (skipped)', details: { skipped: true } };
    }

    var result = optSymbol ? fn(optSymbol) : fn();
    var populated = _extractNumber_(result, ['populatedCount'], 0);
    var total = _extractNumber_(result, ['totalViewFields'], 0);

    if (result && Array.isArray(result.blankFields)) {
      return { ok: !!result.ok, summary: 'symbol=' + (result.symbol || '(none)') + ' blankFields=' + result.blankFields.length + ' missingColumns=' + ((result.missingColumns || []).length), details: result };
    }

    return { ok: !!(result && result.ok && (total === 0 || populated > 0)), summary: 'symbol=' + (result && result.symbol ? result.symbol : '(none)') + ' populated=' + populated + '/' + total + ' refreshPath=' + (result && result.refreshPath ? result.refreshPath : 'n/a'), details: result || {} };
  });
}

function _sweepStepForGlobalMarketsGate_(optSymbol) {
  return _sweepStep_('global_markets_investment_gate', 'Global_Markets investment-gate diagnostic', function () {
    var fn = null;
    var fnName = '';

    if (typeof debugGlobalMarketsInvestmentGate_ === 'function') {
      fn = debugGlobalMarketsInvestmentGate_;
      fnName = 'debugGlobalMarketsInvestmentGate_';
    } else if (typeof debugGlobalMarketsBlankCells_ === 'function') {
      fn = debugGlobalMarketsBlankCells_;
      fnName = 'debugGlobalMarketsBlankCells_';
    }

    if (typeof fn !== 'function') {
      return { ok: true, summary: 'helper not present (skipped)', details: { skipped: true } };
    }

    var result = optSymbol ? fn(optSymbol) : fn();
    var blankFields = result && result.blankFields ? result.blankFields.length : 0;
    var missingColumns = result && result.missingColumns ? result.missingColumns.length : 0;
    var populated = _extractNumber_(result, ['populatedCount'], 0);
    var total = _extractNumber_(result, ['totalViewFields'], 0);

    var summary;
    if (blankFields || missingColumns || result && result.fields) {
      summary = fnName + ' symbol=' + (result && result.symbol ? result.symbol : '(none)') + ' blankFields=' + blankFields + ' missingColumns=' + missingColumns;
    } else {
      summary = fnName + ' symbol=' + (result && result.symbol ? result.symbol : '(none)') + ' populated=' + populated + '/' + total;
    }

    return { ok: !!(result && result.ok), summary: summary, details: result || {} };
  });
}

function _deriveOverallVerdict_(steps) {
  var anyWarn = false;

  for (var i = 0; i < steps.length; i++) {
    if (steps[i].verdict === 'FAIL') return 'FAIL';
    if (steps[i].verdict === 'WARN') anyWarn = true;
  }

  return anyWarn ? 'WARN' : 'PASS';
}

function _countByVerdict_(steps, verdict) {
  var n = 0;
  for (var i = 0; i < steps.length; i++) {
    if (steps[i].verdict === verdict) n++;
  }
  return n;
}

function _generateSweepId_() {
  var rand = Math.floor(Math.random() * 0xFFFFFF).toString(16);
  while (rand.length < 6) rand = '0' + rand;
  return rand.toUpperCase();
}

function _writeDiagnosticReport_(sweepId, overallVerdict, steps, startedAt) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName('Diagnostic_Report');

  if (!sheet) {
    sheet = ss.insertSheet('Diagnostic_Report');
    sheet.getRange(1, 1, 1, 7).setValues([[
      'Timestamp (Riyadh)',
      'Sweep ID',
      'Overall Verdict',
      'Step Name',
      'Step Verdict',
      'Endpoint / Page',
      'Details (JSON)'
    ]]);
    sheet.getRange(1, 1, 1, 7).setFontWeight('bold');
    sheet.setFrozenRows(1);
  }

  var timestamp = Utilities.formatDate(startedAt, _resolveTimezone_(), 'yyyy-MM-dd HH:mm:ss');
  var rowsToWrite = [];

  for (var i = 0; i < steps.length; i++) {
    var step = steps[i];
    var detailsJson;

    try {
      detailsJson = JSON.stringify(step.details || {});
      if (detailsJson.length > 4000) detailsJson = detailsJson.substring(0, 4000) + '... (truncated)';
    } catch (jsonErr) {
      detailsJson = '(unstringifiable details)';
    }

    rowsToWrite.push([timestamp, sweepId, overallVerdict, step.name || step.key || '', step.verdict || '', step.summary || '', detailsJson]);
  }

  if (rowsToWrite.length) {
    sheet.getRange(sheet.getLastRow() + 1, 1, rowsToWrite.length, 7).setValues(rowsToWrite);
  }
}

/* =========================================================
 * Utility helpers
 * ========================================================= */

function _resolveTimezone_() {
  if (typeof APP_INFO !== 'undefined' && APP_INFO && APP_INFO.TIMEZONE) return APP_INFO.TIMEZONE;
  return Session.getScriptTimeZone();
}

function _lookupGlobalFunction_(fnName) {
  var fn = null;

  if (typeof globalThis !== 'undefined') {
    fn = globalThis[fnName];
    if (typeof fn === 'function') return fn;
  }

  try {
    fn = this[fnName];
    if (typeof fn === 'function') return fn;
  } catch (e1) {}

  try {
    fn = new Function('return ' + fnName + ';')();
    if (typeof fn === 'function') return fn;
  } catch (e2) {}

  return null;
}

function _runAndLogDiagnosticFunction_(name, fn) {
  var result = fn();
  try {
    Logger.log(JSON.stringify(result, null, 2));
  } catch (e) {
    Logger.log('[v1.7.0 RUNNER] ' + name + ' result not stringifiable');
  }
  return result;
}

function _showMissingHelper_(helperName, message) {
  var msg = helperName + ' is not deployed. ' + (message || '');
  try {
    SpreadsheetApp.getUi().alert('Function not available', msg, SpreadsheetApp.getUi().ButtonSet.OK);
  } catch (e) {}
  Logger.log('[v1.7.0 RUNNER] ' + msg);
  return { ok: false, skipped: true, helper: helperName, message: msg };
}

function _extractNumber_(obj, keys, fallback) {
  if (!obj) return fallback;
  for (var i = 0; i < keys.length; i++) {
    var k = keys[i];
    if (typeof obj[k] === 'number' && !isNaN(obj[k])) return obj[k];
  }
  return fallback;
}

function _compactAuditDetails_(auditResult) {
  var out = {
    ok: auditResult.ok,
    pagesAudited: auditResult.pagesAudited
  };

  if (Array.isArray(auditResult.reports)) {
    out.reports = auditResult.reports.map(function (r) {
      return {
        pageName: r.pageName,
        ok: r.ok,
        rows: r.rows,
        headerCount: r.headers ? r.headers.length : 0,
        critical: r.rowFindings ? r.rowFindings.filter(function (f) { return f.severity === 'CRITICAL'; }).length : 0,
        warn: r.rowFindings ? r.rowFindings.filter(function (f) { return f.severity === 'WARN'; }).length : 0
      };
    });
  }

  return out;
}
