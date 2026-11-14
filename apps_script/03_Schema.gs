/**
 * 03_Schema.gs
 * Tadawul Tracker — Schema, Header Enforcement, Sheet Setup
 * Version: 2.7.0
 *
 * Aligned with:
 * - 00_Config.gs v1.12.6 (decision-layer wiring)
 * - 02_Core.gs v2.8.0
 * - 04_Format.gs v2.12.0
 * - 05_Refresh.gs v1.11.2
 * - Main investment pages: 115 columns
 * - Top_10_Investments: 118 columns
 * - Global_Markets: full 115-column investment page, NOT old 46-column Layout A

 * v2.7.0 DECISION-LAYER WIRING (lockstep with 00_Config v1.12.6 + 01_Menu v2.10.0)
 * ----------------------------------------------------------------------------------
 * Top_10_Investments is now RENDERED BY 16_Decision_Top10.gs (cockpit layout:
 * control panel, KPI strip, SELECTED / NEAR MISS / ALERTS / CANDIDATES zones).
 * Writing the 118-column engine header row into row 1 of that sheet — which is
 * exactly what this file's header-enforcement paths do — destroys the cockpit
 * title row and slaps a sheet filter over the layout. TWO ADDITIVE GUARDS
 * (every v2.6.1 function carried verbatim):
 *
 * 1. _schemaIsDecisionOwnedPage_() consults isDecisionOwnedPage_ from
 *    00_Config v1.12.6 via typeof guard (deploy order cannot break this
 *    file). Both header WRITE choke points — applyHeadersFromSchema_ and
 *    mergeBackendHeadersIntoSheet_ — now skip decision-owned pages and
 *    return action='skipped_decision_owned'. options.force does NOT
 *    override: the cockpit must never receive engine headers, and the
 *    skip-not-throw contract keeps ensureAllSheetsAndHeaders_ (which loops
 *    every supported page through applyHeadersFromSchema_) running cleanly
 *    across the remaining pages. This closes the "Ensure All Sheets &
 *    Headers" bypass that the 01_Menu v2.10.0 page-action guard could not
 *    reach (it calls this file directly, not via menuPageAction_).
 *
 * 2. diagnoseSchemaContractStatus_ classifies decision-owned pages with
 *    their own 'decision-owned' status (no schema fetch, no width
 *    comparison) instead of reporting perpetual sheet-behind /
 *    header-mismatch noise for a sheet that is intentionally not on the
 *    118-column contract. 'decision-owned' counts as acceptable for the
 *    overall ok rollup.
 *
 * Read-only diagnostics (validateHeaderOrder_, debugV28SchemaPropagation_,
 * validateHeaderMapping_) are deliberately NOT guarded — they never write.
 *
 * v2.6.1 INSIGHTS HEADER FIX
 * --------------------------
 * 1. Fixes the exact-width / wrong-header case found on Insights_Analysis:
 *      sheet width = 7, but row 1 contained old headers such as Notes,
 *      Source, and Sort Order instead of Section, Item, Symbol, Metric,
 *      Value, Signal, Last Updated (Riyadh).
 *
 * 2. When 00_Config has an authoritative fallback header list with the
 *    expected width, schema promotion now also occurs when backend/schema
 *    headers have the right width but do not match the approved Config
 *    fallback headers. This prevents a false aligned status.
 *
 * 3. diagnoseSchemaContractStatus_ now reports header-mismatch when sheet
 *    width is correct but required/config headers are wrong.
 *
 * 4. mergeBackendHeadersIntoSheet_ rewrites the header row instead of
 *    appending columns when the sheet has the expected width but wrong
 *    header names. This prevents 7-column Insights_Analysis from becoming
 *    14 columns.
 *
 * 5. Adds public runner:
 *      repairInsightsAnalysisHeaders()
 *    to force the approved 7 headers onto Insights_Analysis.
 *
 *
 * v2.6.0 ALIGNMENT FIXES
 * ----------------------
 * 1. Preserves the v2.5.0 115/118 schema contract and INVESTABILITY_GATE_V578
 *    diagnostics.
 *
 * 2. Fixes fallback/promoted schema key handling. When the backend schema is
 *    behind and 00_Config fallback headers are promoted, the effective schema
 *    now derives API keys through getHeaderToKey/coreResolveHeaderKey_ instead
 *    of treating display headers as API keys. This keeps propagation diagnostics
 *    accurate for SCORING_V574 and INVESTABILITY_GATE_V578.
 *
 * 3. Uses var for top-level constants to avoid Apps Script flattened-scope
 *    const redeclaration failures if backup tabs exist.
 *
 * 4. Adds public runner wrappers for Apps Script dropdown use:
 *      verifySchemaVersion()
 *      diagnoseSchemaContractStatus()
 *      debugSchemaPropagationGlobalMarkets()
 *
 * v2.5.0 MAIN FIXES
 * -----------------
 * 1. Adds INVESTABILITY_GATE_V578 diagnostic group:
 *      forecast_source
 *      data_quality_score
 *      forecast_reliability_score
 *      provider_engine_conflict
 *      conflict_type
 *      final_decision_basis
 *      investability_status
 *      final_action
 *      block_reason
 *
 * 2. Updates schema diagnostics to current 115 / 118 contract.
 *
 * 3. If backend schema is still behind expected width but 00_Config has a
 *    correct fallback header set, this file promotes the effective schema to
 *    the Config fallback headers. This protects the sheet from staying stuck
 *    at old 46 / 106 / 109 layouts.
 *
 * 4. Global_Markets Layout A / 46 is no longer considered OK.
 *
 * 5. Contract-status OK now requires pages to be aligned to expected width.
 *
 * Required after deployment:
 * - Run debugV28SchemaPropagation_('Global_Markets')
 * - Run diagnoseSchemaContractStatus_()
 * - Run Update Current Headers for all main pages and Top_10_Investments
 */

'use strict';

/* =========================================================
 * Canonical multi-wave column groups
 * ========================================================= */

var _V28_COLUMN_GROUPS_ = Object.freeze({
  INSIGHTS_WAVE_3: Object.freeze([
    'sector_relative_score',
    'conviction_score',
    'top_factors',
    'top_risks',
    'position_size_hint'
  ]),

  DECISION_MATRIX_V28: Object.freeze([
    'recommendation_detailed',
    'recommendation_priority'
  ]),

  CANDLESTICK_V28: Object.freeze([
    'candlestick_pattern',
    'candlestick_signal',
    'candlestick_strength',
    'candlestick_confidence',
    'candlestick_patterns_recent'
  ]),

  CANONICAL_PROVENANCE_V53: Object.freeze([
    'recommendation_priority_band',
    'recommendation_source'
  ]),

  SCORING_V574: Object.freeze([
    'provider_rating',
    'scoring_recommendation_source',
    'scoring_schema_version',
    'scoring_errors',
    'opportunity_source',
    'overall_score_raw',
    'overall_penalty_factor'
  ]),

  INVESTABILITY_GATE_V578: Object.freeze([
    'forecast_source',
    'data_quality_score',
    'forecast_reliability_score',
    'provider_engine_conflict',
    'conflict_type',
    'final_decision_basis',
    'investability_status',
    'final_action',
    'block_reason'
  ]),

  TOP10_ONLY: Object.freeze([
    'top10_rank',
    'selection_reason',
    'criteria_snapshot'
  ])
});

/* =========================================================
 * v2.7.0 — decision-owned page guard
 * ========================================================= */

function _schemaIsDecisionOwnedPage_(pageName) {
  try {
    if (typeof isDecisionOwnedPage_ === 'function') {
      return isDecisionOwnedPage_(pageName) === true;
    }
  } catch (ownErr) {}
  return false;
}

function _schemaSkipDecisionOwned_(actionName, pageName) {
  var msg = '"' + pageName + '" is rendered by the Decision cockpit (16_Decision_Top10.gs). ' +
    'Engine header enforcement is skipped to protect the cockpit layout. ' +
    'Use the Decision menu (Build Top 10 Cockpit Layout) to rebuild this sheet.';

  try {
    logRun_('WARN', actionName, pageName, msg, {
      status: 'SKIPPED_DECISION_OWNED'
    });
  } catch (logErr) {}

  return {
    ok: true,
    pageName: pageName,
    action: 'skipped_decision_owned',
    additions: [],
    message: msg
  };
}

/* =========================================================
 * Public schema entry points
 * ========================================================= */

function fetchSchemaPages_() {
  var candidates = [ENDPOINTS.SCHEMA_PAGES];

  for (var ci = 0; ci < candidates.length; ci++) {
    var path = candidates[ci];
    var resp = apiGet_(path, {}, { maxRetries: 2 });

    if (!resp.ok || !resp.json) continue;

    var pages = extractSchemaPages_(resp.json);
    if (pages.length) {
      return {
        ok: true,
        pages: pages,
        source: path,
        response: resp
      };
    }
  }

  return {
    ok: false,
    pages: [],
    source: '',
    response: null
  };
}

/**
 * Fetch page schema from backend. If backend returns a schema narrower than
 * the expected config width and config has a correct fallback header list,
 * promote the effective schema to the config fallback headers.
 */
function fetchSheetSpec_(pageName) {
  var page = getPageConfig(pageName);
  var candidates = getSchemaEndpointCandidates(page.pageName);
  var queryAliases = ['page', 'sheet', 'sheet_name', 'name', 'tab'];

  for (var pi = 0; pi < candidates.length; pi++) {
    var path = candidates[pi];

    for (var ai = 0; ai < queryAliases.length; ai++) {
      var alias = queryAliases[ai];
      var getResp = apiGet_(path, buildPageQueryAliases_(alias, page.pageName), { maxRetries: 2 });
      var getSpec = extractSheetSpecForPage_(getResp.json, page.pageName);

      if (getResp.ok && getSpec && (getSpec.headers.length || getSpec.keys.length)) {
        return _promoteSchemaIfBehindConfig_(page, {
          ok: true,
          pageName: page.pageName,
          headers: getSpec.headers || [],
          keys: getSpec.keys || [],
          columns: getSpec.keys && getSpec.keys.length ? getSpec.keys.slice() : (getSpec.headers || []).slice(),
          source: path + ' [GET]',
          response: getResp,
          usedFallback: false
        });
      }

      var postBody = {};
      postBody[alias] = page.pageName;

      var postResp = apiPost_(path, postBody, { maxRetries: 2 });
      var postSpec = extractSheetSpecForPage_(postResp.json, page.pageName);

      if (postResp.ok && postSpec && (postSpec.headers.length || postSpec.keys.length)) {
        return _promoteSchemaIfBehindConfig_(page, {
          ok: true,
          pageName: page.pageName,
          headers: postSpec.headers || [],
          keys: postSpec.keys || [],
          columns: postSpec.keys && postSpec.keys.length ? postSpec.keys.slice() : (postSpec.headers || []).slice(),
          source: path + ' [POST]',
          response: postResp,
          usedFallback: false
        });
      }
    }
  }

  var fallbackHeaders = getFallbackHeaders(page.pageName);
  if (fallbackHeaders.length) {
    var fallbackKeys = _resolveSchemaKeysFromHeaders_(page.pageName, fallbackHeaders);
    return {
      ok: true,
      pageName: page.pageName,
      headers: fallbackHeaders.slice(),
      keys: fallbackKeys.slice(),
      columns: fallbackKeys.slice(),
      source: 'fallbackHeaders',
      response: null,
      usedFallback: true,
      promotedFromBackend: false,
      backendWidth: 0,
      expectedColumnCount: Number(page.expectedColumnCount || 0)
    };
  }

  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(page.pageName);
  if (sheet) {
    var currentHeaders = readCurrentSheetHeaders_(sheet);
    if (currentHeaders.length) {
      var currentKeys = _resolveSchemaKeysFromHeaders_(page.pageName, currentHeaders);
      return {
        ok: true,
        pageName: page.pageName,
        headers: currentHeaders.slice(),
        keys: currentKeys.slice(),
        columns: currentKeys.slice(),
        source: 'existingSheetHeaders',
        response: null,
        usedFallback: true,
        promotedFromBackend: false,
        backendWidth: 0,
        expectedColumnCount: Number(page.expectedColumnCount || 0)
      };
    }
  }

  return {
    ok: false,
    pageName: page.pageName,
    headers: [],
    keys: [],
    columns: [],
    source: '',
    response: null,
    usedFallback: true,
    promotedFromBackend: false,
    backendWidth: 0,
    expectedColumnCount: Number(page.expectedColumnCount || 0)
  };
}

function fetchDataDictionarySchema_() {
  var primary = apiGet_(ENDPOINTS.SCHEMA_DATA_DICTIONARY, {}, { maxRetries: 2 });

  if (primary.ok && primary.json) {
    var directSpec = extractSheetSpecForPage_(primary.json, SHEET_NAMES.DATA_DICTIONARY);
    if (directSpec && (directSpec.headers.length || directSpec.keys.length)) {
      return {
        ok: true,
        pageName: SHEET_NAMES.DATA_DICTIONARY,
        headers: directSpec.headers || [],
        keys: directSpec.keys || [],
        columns: directSpec.keys && directSpec.keys.length ? directSpec.keys.slice() : (directSpec.headers || []).slice(),
        source: ENDPOINTS.SCHEMA_DATA_DICTIONARY,
        response: primary,
        usedFallback: false
      };
    }

    var payload = unwrapPayload_(primary.json) || primary.json;
    var headers = extractHeaders_(payload);
    var keys = extractKeys_(payload);

    if (headers.length || keys.length) {
      return {
        ok: true,
        pageName: SHEET_NAMES.DATA_DICTIONARY,
        headers: headers,
        keys: keys,
        columns: keys.length ? keys.slice() : headers.slice(),
        source: ENDPOINTS.SCHEMA_DATA_DICTIONARY,
        response: primary,
        usedFallback: false
      };
    }
  }

  return fetchSheetSpec_(SHEET_NAMES.DATA_DICTIONARY);
}

/* =========================================================
 * Schema promotion helper
 * ========================================================= */

function _promoteSchemaIfBehindConfig_(page, schema) {
  if (!page || !schema || !schema.ok) return schema;

  var expected = Number(page.expectedColumnCount || 0);
  var headers = Array.isArray(schema.headers) ? schema.headers : [];
  var fallbackHeaders = getFallbackHeaders(page.pageName);
  var fallbackIsAuthoritative = expected > 0 && fallbackHeaders.length === expected;

  // v2.6.1: promote not only when backend is narrower, but also when
  // backend/schema returns the correct width with wrong display headers.
  // This is the exact failure found on Insights_Analysis: width=7 but the
  // sheet/schema carried old labels such as Notes / Source / Sort Order
  // instead of Section / Item / Symbol / Metric / Value / Signal /
  // Last Updated (Riyadh). The writer requires Symbol, so width-only
  // alignment is not sufficient.
  if (
    fallbackIsAuthoritative &&
    headers.length > 0 &&
    (
      headers.length < expected ||
      (headers.length === expected && !_schemaHeadersExactlyMatch_(headers, fallbackHeaders))
    )
  ) {
    var fallbackKeys = _resolveSchemaKeysFromHeaders_(page.pageName, fallbackHeaders);
    var reason = headers.length < expected
      ? 'Backend schema was behind expected width'
      : 'Backend/schema width matched expected but header names/order differed from Config fallback';

    return {
      ok: true,
      pageName: page.pageName,
      headers: fallbackHeaders.slice(),
      keys: fallbackKeys.slice(),
      columns: fallbackKeys.slice(),
      source: schema.source + ' + configFallbackHeaders',
      response: schema.response || null,
      usedFallback: true,
      promotedFromBackend: true,
      backendWidth: headers.length,
      expectedColumnCount: expected,
      headerMismatchPromoted: headers.length === expected,
      originalHeaders: headers.slice(),
      warning: reason + '; config fallback headers promoted and API keys resolved from 00_Config.'
    };
  }

  if (schema.headers && schema.headers.length && (!schema.keys || !schema.keys.length || schema.keys.length !== schema.headers.length)) {
    var resolvedKeys = _resolveSchemaKeysFromHeaders_(page.pageName, schema.headers);
    schema.keys = resolvedKeys.slice();
    schema.columns = resolvedKeys.slice();
  }

  return schema;
}

function _schemaHeaderForCompare_(value) {
  var s = String(value == null ? '' : value);

  try {
    if (typeof _normalizeHeaderText_ === 'function') {
      return _normalizeHeaderText_(s);
    }
  } catch (e) {}

  s = s.replace(/[\u200B\u200C\u200D\uFEFF]/g, '');
  s = s.replace(/[\s\u00A0\u1680\u2000-\u200A\u2028\u2029\u202F\u205F\u3000]+/g, ' ');
  return s.trim();
}

function _schemaHeadersExactlyMatch_(actualHeaders, expectedHeaders) {
  var actual = Array.isArray(actualHeaders) ? actualHeaders : [];
  var expected = Array.isArray(expectedHeaders) ? expectedHeaders : [];

  if (actual.length !== expected.length) return false;

  for (var i = 0; i < expected.length; i++) {
    if (_schemaHeaderForCompare_(actual[i]) !== _schemaHeaderForCompare_(expected[i])) {
      return false;
    }
  }

  return true;
}

function _schemaHeaderDiff_(actualHeaders, expectedHeaders) {
  var actual = Array.isArray(actualHeaders) ? actualHeaders : [];
  var expected = Array.isArray(expectedHeaders) ? expectedHeaders : [];
  var actualSet = {};
  var expectedSet = {};
  var missing = [];
  var extra = [];

  for (var i = 0; i < actual.length; i++) {
    actualSet[_schemaHeaderForCompare_(actual[i])] = true;
  }

  for (var j = 0; j < expected.length; j++) {
    var eh = _schemaHeaderForCompare_(expected[j]);
    expectedSet[eh] = true;
    if (!actualSet[eh]) missing.push(expected[j]);
  }

  for (var k = 0; k < actual.length; k++) {
    var ah = _schemaHeaderForCompare_(actual[k]);
    if (!expectedSet[ah]) extra.push(actual[k]);
  }

  return {
    exact: _schemaHeadersExactlyMatch_(actual, expected),
    missing: missing,
    extra: extra
  };
}

function _resolveSchemaKeysFromHeaders_(pageName, headers) {
  var safeHeaders = Array.isArray(headers) ? headers : [];
  var headerToKey = {};

  try {
    if (typeof getHeaderToKey === 'function') {
      headerToKey = getHeaderToKey(pageName) || {};
    }
  } catch (e) {
    headerToKey = {};
  }

  return safeHeaders.map(function (h) {
    var header = String(h || '');
    var key = null;

    try {
      if (typeof coreResolveHeaderKey_ === 'function') {
        key = coreResolveHeaderKey_(header, headerToKey);
      }
    } catch (e2) {
      key = null;
    }

    if (!key && headerToKey && Object.prototype.hasOwnProperty.call(headerToKey, header)) {
      key = headerToKey[header];
    }

    return key || header;
  });
}

/* =========================================================
 * Sheet creation / header enforcement
 * ========================================================= */

function ensureSheetExists_(pageName) {
  var page = getPageConfig(pageName);
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(page.sheetName);

  if (!sheet) {
    sheet = ss.insertSheet(page.sheetName);
  }

  if (page.formatting && page.formatting.tabColor) {
    try {
      sheet.setTabColor(page.formatting.tabColor);
    } catch (e) {}
  }

  return sheet;
}

function ensureAllSheets_() {
  var pages = getSupportedPages();
  var created = pages.map(function (pageName) {
    return ensureSheetExists_(pageName).getName();
  });

  return {
    ok: true,
    sheets: created
  };
}

/**
 * Historical Layout A guard retained for compatibility only.
 * Under 00_Config v1.12.1, Global_Markets expected width is 115, so old
 * 46-column Layout A should not be protected as final production output.
 */
function _shouldGuardLayoutA_(page, sheet, schemaHeaders, options) {
  if (options && options.force) return false;
  if (!page || !sheet || !Array.isArray(schemaHeaders) || !schemaHeaders.length) return false;

  var expected = Number(page.expectedColumnCount || 0);
  if (!expected || expected <= 0) return false;

  var currentHeaders = readCurrentSheetHeaders_(sheet);

  if (page.pageName === SHEET_NAMES.GLOBAL_MARKETS && expected >= 115) {
    return false;
  }

  if (currentHeaders.length !== expected) return false;
  if (schemaHeaders.length === expected) return false;

  return true;
}

function applyHeadersFromSchema_(pageName, options) {
  options = options || {};

  return withScriptLock_(function () {
    var page = getPageConfig(pageName);

    /* v2.7.0: decision-owned pages never receive engine headers — not even
     * with options.force. Skip (not throw) so batch callers continue. */
    if (_schemaIsDecisionOwnedPage_(page.pageName)) {
      return _schemaSkipDecisionOwned_('applyHeadersFromSchema', page.pageName);
    }

    var sheet = ensureSheetExists_(page.pageName);

    var schema = page.pageName === SHEET_NAMES.DATA_DICTIONARY
      ? fetchDataDictionarySchema_()
      : fetchSheetSpec_(page.pageName);

    if (!schema.ok || !schema.headers.length) {
      throw new Error('No schema headers available for page: ' + page.pageName);
    }

    if (_shouldGuardLayoutA_(page, sheet, schema.headers, options)) {
      var expected = Number(page.expectedColumnCount || 0);
      var guardMsg =
        'Layout protection: sheet width (' + expected + ') matches expectedColumnCount; ' +
        'schema would rewrite to ' + schema.headers.length + ' columns. ' +
        'Pass options.force=true to override.';

      logRun_('WARN', 'applyHeadersFromSchema', page.pageName, guardMsg, {
        status: 'SKIPPED_LAYOUT_PROTECTED',
        endpoint: schema.source,
        existingWidth: expected,
        backendWidth: schema.headers.length,
        expectedColumnCount: expected,
        schemaSource: schema.source
      });

      return {
        ok: true,
        pageName: page.pageName,
        sheetName: sheet.getName(),
        action: 'skipped_layout_protected',
        existingWidth: expected,
        backendWidth: schema.headers.length,
        message: guardMsg,
        source: schema.source,
        usedFallback: schema.usedFallback
      };
    }

    writeHeaderRowExact_(sheet, schema.headers);
    applyHeaderSheetSetup_(sheet, page);

    var coverageDetail = buildMappingCoverageDetail_(page, schema.headers);
    var logLevel = schema.usedFallback ? 'WARN' : 'INFO';
    var logMessage = schema.usedFallback
      ? 'Headers applied from fallback/promoted source (' + schema.source + ').'
      : 'Headers applied from backend schema.';

    logRun_(
      logLevel,
      'applyHeadersFromSchema',
      page.pageName,
      logMessage,
      Object.assign(
        {
          status: schema.usedFallback ? 'WARN' : 'OK',
          endpoint: schema.source,
          httpCode: schema.response && schema.response.statusCode ? schema.response.statusCode : '',
          columns: schema.headers.length,
          schemaSource: schema.source,
          usedFallback: !!schema.usedFallback,
          promotedFromBackend: !!schema.promotedFromBackend,
          backendWidth: schema.backendWidth || '',
          expectedColumnCount: schema.expectedColumnCount || page.expectedColumnCount,
          forced: !!options.force
        },
        coverageDetail
      )
    );

    writePageStatus_(page.pageName, {
      status: schema.usedFallback ? 'SCHEMA_FALLBACK' : 'SCHEMA_OK',
      message: schema.usedFallback
        ? 'Headers applied using fallback/promoted schema source'
        : 'Headers applied from schema',
      endpoint: schema.source,
      httpCode: schema.response && schema.response.statusCode ? schema.response.statusCode : '',
      rows: 0,
      columns: schema.headers.length,
      durationMs: schema.response && schema.response.durationMs ? schema.response.durationMs : '',
      warnings: schema.usedFallback ? ['Used fallback/promoted schema source'] : []
    });

    return {
      ok: true,
      pageName: page.pageName,
      sheetName: sheet.getName(),
      action: 'applied',
      headers: schema.headers,
      keys: schema.keys,
      source: schema.source,
      usedFallback: schema.usedFallback,
      promotedFromBackend: !!schema.promotedFromBackend
    };
  });
}

function updateHeadersOnly_(pageName, options) {
  return applyHeadersFromSchema_(pageName, options);
}

function validateHeaderOrder_(pageName) {
  var page = getPageConfig(pageName);
  var sheet = ensureSheetExists_(page.pageName);

  var expectedHeaders = getExpectedHeaders_(page.pageName);
  if (!expectedHeaders.length) {
    throw new Error('Expected headers are unavailable for page: ' + page.pageName);
  }

  var actualHeaders = readCurrentSheetHeaders_(sheet);
  var missingHeaders = expectedHeaders.filter(function (h) {
    return actualHeaders.indexOf(h) === -1;
  });
  var extraHeaders = actualHeaders.filter(function (h) {
    return expectedHeaders.indexOf(h) === -1;
  });
  var orderMatches = arraysEqual_(expectedHeaders, actualHeaders);

  var ok = missingHeaders.length === 0 && extraHeaders.length === 0 && orderMatches;

  return {
    ok: ok,
    pageName: page.pageName,
    expectedHeaders: expectedHeaders,
    actualHeaders: actualHeaders,
    missingHeaders: missingHeaders,
    extraHeaders: extraHeaders,
    orderMatches: orderMatches
  };
}

/* =========================================================
 * Expected schema getters
 * ========================================================= */

function getExpectedHeaders_(pageName) {
  var schema = pageName === SHEET_NAMES.DATA_DICTIONARY
    ? fetchDataDictionarySchema_()
    : fetchSheetSpec_(pageName);

  return schema && schema.headers ? schema.headers.slice() : [];
}

function getExpectedKeys_(pageName) {
  var schema = pageName === SHEET_NAMES.DATA_DICTIONARY
    ? fetchDataDictionarySchema_()
    : fetchSheetSpec_(pageName);

  return schema && schema.keys ? schema.keys.slice() : [];
}

function getExpectedColumns_(pageName) {
  var keys = getExpectedKeys_(pageName);
  return keys.length ? keys : getExpectedHeaders_(pageName);
}

/* =========================================================
 * Internal schema extraction
 * ========================================================= */

function extractSchemaPages_(json) {
  if (!json) return [];

  if (Array.isArray(json)) {
    return json.map(String);
  }

  if (Array.isArray(json.pages)) {
    return json.pages.map(function (x) {
      return String(extractPageName_(x));
    }).filter(Boolean);
  }

  if (json.data && Array.isArray(json.data.pages)) {
    return json.data.pages.map(function (x) {
      return String(extractPageName_(x));
    }).filter(Boolean);
  }

  if (Array.isArray(json.data)) {
    return json.data.map(function (x) {
      return String(extractPageName_(x));
    }).filter(Boolean);
  }

  if (Array.isArray(json.items)) {
    return json.items.map(function (x) {
      return String(extractPageName_(x));
    }).filter(Boolean);
  }

  return [];
}

function extractPageName_(value) {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string') return value;

  if (isPlainObject_(value)) {
    return value.page || value.sheet || value.name || value.id || '';
  }

  return '';
}

function extractSheetSpecForPage_(json, pageName) {
  if (!json) return null;

  var direct = normalizeSchemaSpec_(json);
  if (direct && (direct.headers.length || direct.keys.length)) {
    return direct;
  }

  var root = json;
  var payload = unwrapPayload_(json) || json;

  var directPayload = normalizeSchemaSpec_(payload);
  if (directPayload && (directPayload.headers.length || directPayload.keys.length)) {
    return directPayload;
  }

  var directCandidates = [payload, root];

  for (var dc = 0; dc < directCandidates.length; dc++) {
    var candidate = directCandidates[dc];
    if (!candidate || !isPlainObject_(candidate)) continue;

    if (isPlainObject_(candidate[pageName])) {
      return normalizeSchemaSpec_(candidate[pageName]);
    }

    if (candidate.data && isPlainObject_(candidate.data[pageName])) {
      return normalizeSchemaSpec_(candidate.data[pageName]);
    }

    if (candidate.payload && isPlainObject_(candidate.payload[pageName])) {
      return normalizeSchemaSpec_(candidate.payload[pageName]);
    }

    if (candidate.result && isPlainObject_(candidate.result[pageName])) {
      return normalizeSchemaSpec_(candidate.result[pageName]);
    }

    if (Array.isArray(candidate.items)) {
      for (var ii = 0; ii < candidate.items.length; ii++) {
        var item = candidate.items[ii];
        if (
          isPlainObject_(item) &&
          normalizePageName_(item.page || item.sheet || item.name) === normalizePageName_(pageName)
        ) {
          return normalizeSchemaSpec_(item);
        }
      }
    }

    if (Array.isArray(candidate.data)) {
      for (var si = 0; si < candidate.data.length; si++) {
        var spec = candidate.data[si];
        if (
          isPlainObject_(spec) &&
          normalizePageName_(spec.page || spec.sheet || spec.name) === normalizePageName_(pageName)
        ) {
          return normalizeSchemaSpec_(spec);
        }
      }
    }
  }

  return null;
}

function normalizeSchemaSpec_(spec) {
  if (!spec || !isPlainObject_(spec)) return null;

  var headers = [];
  var keys = [];

  if (Array.isArray(spec.headers)) {
    headers = spec.headers.map(String);
  }

  if (Array.isArray(spec.keys)) {
    keys = spec.keys.map(String);
  }

  if ((!headers.length || !keys.length) && Array.isArray(spec.columns)) {
    var colHeaders = [];
    var colKeys = [];

    for (var ci2 = 0; ci2 < spec.columns.length; ci2++) {
      var col = spec.columns[ci2];
      if (typeof col === 'string') {
        colHeaders.push(String(col));
        colKeys.push(String(col));
      } else if (isPlainObject_(col)) {
        colHeaders.push(String(col.header || col.name || col.key || ''));
        colKeys.push(String(col.key || col.field || col.header || ''));
      }
    }

    if (!headers.length) headers = colHeaders.filter(Boolean);
    if (!keys.length) keys = colKeys.filter(Boolean);
  }

  if (!keys.length && Array.isArray(spec.fields)) {
    keys = spec.fields.map(String);
  }

  if (!headers.length && Array.isArray(spec.display_headers)) {
    headers = spec.display_headers.map(String);
  }

  if (!headers.length && keys.length) headers = keys.slice();
  if (!keys.length && headers.length) keys = headers.slice();

  if (!headers.length && !keys.length) {
    return null;
  }

  return {
    headers: headers,
    keys: keys
  };
}

/* =========================================================
 * Sheet/header utilities
 * ========================================================= */

function writeHeaderRowExact_(sheet, headers) {
  headers = headers || [];

  if (!headers.length) {
    throw new Error('Cannot write empty header row.');
  }

  var maxCols = sheet.getMaxColumns();
  if (maxCols < headers.length) {
    sheet.insertColumnsAfter(maxCols, headers.length - maxCols);
  }

  var clearCols = Math.max(sheet.getLastColumn(), headers.length);
  sheet.getRange(1, 1, 1, clearCols).clearContent();
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
}

function readCurrentSheetHeaders_(sheet) {
  var lastCol = Math.max(sheet.getLastColumn(), 1);
  var values = sheet.getRange(1, 1, 1, lastCol).getValues()[0];

  return values.map(String).map(function (v) {
    return v.trim();
  }).filter(Boolean);
}

function applyHeaderSheetSetup_(sheet, page) {
  try {
    sheet.setFrozenRows(1);
  } catch (e) {}

  try {
    var lastCol = Math.max(sheet.getLastColumn(), 1);
    var existingFilter = sheet.getFilter();
    if (existingFilter) existingFilter.remove();
    if (lastCol > 0) {
      sheet.getRange(1, 1, Math.max(sheet.getMaxRows(), 2), lastCol).createFilter();
    }
  } catch (e2) {}

  if (page && page.formatting && page.formatting.tabColor) {
    try {
      sheet.setTabColor(page.formatting.tabColor);
    } catch (e3) {}
  }
}

/* =========================================================
 * Batch setup helpers
 * ========================================================= */

function ensureAllSheetsAndHeaders_(options) {
  options = options || {};

  return withScriptLock_(function () {
    var pages = getSupportedPages();
    var results = pages.map(function (pageName) {
      return applyHeadersFromSchema_(pageName, options);
    });

    return {
      ok: true,
      count: results.length,
      results: results
    };
  });
}

/* =========================================================
 * Validation helpers
 * ========================================================= */

function arraysEqual_(a, b) {
  a = a || [];
  b = b || [];

  if (!Array.isArray(a) || !Array.isArray(b)) return false;
  if (a.length !== b.length) return false;

  return a.every(function (val, i) {
    return String(val) === String(b[i]);
  });
}

function buildPageQueryAliases_(alias, pageName) {
  var out = {};
  out[alias] = pageName;
  return out;
}

function buildMappingCoverageDetail_(page, sheetHeaders) {
  if (typeof getHeaderToKey !== 'function') return {};

  try {
    var headerToKey = getHeaderToKey(page.pageName);
    var mapped = 0;
    var unmapped = 0;

    (sheetHeaders || []).forEach(function (h) {
      var key = (typeof coreResolveHeaderKey_ === 'function')
        ? coreResolveHeaderKey_(h, headerToKey)
        : (headerToKey[h] || null);

      if (key) mapped++;
      else unmapped++;
    });

    var mode = (typeof getSchemaCheckMode === 'function')
      ? getSchemaCheckMode(page.pageName)
      : 'min';

    return {
      schemaCheckMode: mode,
      mappedCount: mapped,
      unmappedCount: unmapped
    };
  } catch (e) {
    return {};
  }
}

function validateHeaderMapping_(pageName) {
  var page = getPageConfig(pageName);
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(page.sheetName);

  var mode = (typeof getSchemaCheckMode === 'function')
    ? getSchemaCheckMode(page.pageName)
    : 'min';

  if (!sheet) {
    return {
      ok: false,
      reason: 'sheet-not-found',
      pageName: page.pageName,
      sheetHeaders: [],
      mapped: [],
      unmapped: [],
      userOwned: [],
      preserveOnEmpty: [],
      backendNew: [],
      mode: mode,
      counts: {
        total: 0,
        mapped: 0,
        unmapped: 0,
        userOwned: 0,
        backendNew: 0
      }
    };
  }

  var sheetHeaders = readCurrentSheetHeaders_(sheet);
  var headerToKey = (typeof getHeaderToKey === 'function')
    ? getHeaderToKey(page.pageName)
    : {};

  var manualCols = (typeof getPreserveManualColumns === 'function')
    ? getPreserveManualColumns(page.pageName) || []
    : [];

  var onEmptyCols = (typeof getPreserveOnEmptyColumns === 'function')
    ? getPreserveOnEmptyColumns(page.pageName) || []
    : [];

  var manualSet = new Set(manualCols.map(String));
  var onEmptySet = new Set(onEmptyCols.map(String));

  var mapped = [];
  var unmapped = [];
  var userOwned = [];

  sheetHeaders.forEach(function (h) {
    if (manualSet.has(h)) userOwned.push(h);

    var key = (typeof coreResolveHeaderKey_ === 'function')
      ? coreResolveHeaderKey_(h, headerToKey)
      : (headerToKey[h] || null);

    if (key) mapped.push({ header: h, apiKey: key });
    else unmapped.push(h);
  });

  var backendNew = [];

  try {
    var schema = page.pageName === SHEET_NAMES.DATA_DICTIONARY
      ? fetchDataDictionarySchema_()
      : fetchSheetSpec_(page.pageName);

    if (schema && schema.ok && Array.isArray(schema.keys) && schema.keys.length > 0) {
      var sheetKeySet = new Set(mapped.map(function (m) {
        return m.apiKey;
      }));

      var keyToSuggestedHeader = {};
      Object.keys(headerToKey).forEach(function (h) {
        var k = headerToKey[h];
        if (k && keyToSuggestedHeader[k] === undefined) keyToSuggestedHeader[k] = h;
      });

      backendNew = schema.keys
        .filter(function (k) {
          return k && !sheetKeySet.has(k);
        })
        .map(function (k) {
          return {
            apiKey: k,
            suggestedHeader: keyToSuggestedHeader[k] || null
          };
        });
    }
  } catch (e) {}

  var counts = {
    total: sheetHeaders.length,
    mapped: mapped.length,
    unmapped: unmapped.length,
    userOwned: userOwned.length,
    backendNew: backendNew.length
  };

  return {
    ok: counts.unmapped === 0,
    pageName: page.pageName,
    sheetHeaders: sheetHeaders,
    mapped: mapped,
    unmapped: unmapped,
    userOwned: userOwned,
    preserveOnEmpty: Array.from(onEmptySet),
    backendNew: backendNew,
    mode: mode,
    counts: counts
  };
}

function mergeBackendHeadersIntoSheet_(pageName, options) {
  options = options || {};

  return withScriptLock_(function () {
    var page = getPageConfig(pageName);

    /* v2.7.0: same decision-owned skip as applyHeadersFromSchema_. */
    if (_schemaIsDecisionOwnedPage_(page.pageName)) {
      return _schemaSkipDecisionOwned_('mergeBackendHeadersIntoSheet', page.pageName);
    }

    var sheet = ensureSheetExists_(page.pageName);

    var schema = page.pageName === SHEET_NAMES.DATA_DICTIONARY
      ? fetchDataDictionarySchema_()
      : fetchSheetSpec_(page.pageName);

    if (!schema.ok || !schema.headers.length) {
      throw new Error('No schema headers available for page: ' + page.pageName);
    }

    if (_shouldGuardLayoutA_(page, sheet, schema.headers, options)) {
      var expected = Number(page.expectedColumnCount || 0);
      var guardMsg =
        'Layout protection: sheet width (' + expected + ') matches expectedColumnCount; ' +
        'schema has ' + schema.headers.length + ' columns. ' +
        'Pass options.force=true to override.';

      logRun_('WARN', 'mergeBackendHeadersIntoSheet', page.pageName, guardMsg, {
        status: 'SKIPPED_LAYOUT_PROTECTED',
        endpoint: schema.source,
        existingWidth: expected,
        backendWidth: schema.headers.length,
        expectedColumnCount: expected,
        schemaSource: schema.source
      });

      return {
        ok: true,
        pageName: page.pageName,
        action: 'skipped_layout_protected',
        additions: [],
        newWidth: expected,
        existing: expected,
        backend: schema.headers.length,
        message: guardMsg
      };
    }

    var existing = readCurrentSheetHeaders_(sheet);

    // v2.6.1: if the sheet already has the expected width but the header
    // names/order are wrong, rewrite the header row instead of appending
    // missing headers. This fixes Insights_Analysis width=7 with old labels
    // (Notes / Source / Sort Order) and prevents it from becoming 14 columns.
    var expectedWidthForMerge = Number(page.expectedColumnCount || 0);
    if (
      expectedWidthForMerge > 0 &&
      existing.length === expectedWidthForMerge &&
      schema.headers.length === expectedWidthForMerge &&
      !_schemaHeadersExactlyMatch_(existing, schema.headers)
    ) {
      var mergeDiff = _schemaHeaderDiff_(existing, schema.headers);

      writeHeaderRowExact_(sheet, schema.headers);
      applyHeaderSheetSetup_(sheet, page);

      logRun_('WARN', 'mergeBackendHeadersIntoSheet', page.pageName,
        'Rewrote header row because sheet had expected width but wrong headers.',
        {
          status: 'REWRITTEN_HEADER_MISMATCH',
          existing: existing.length,
          backend: schema.headers.length,
          expectedColumnCount: expectedWidthForMerge,
          schemaSource: schema.source,
          missingExpectedHeaders: mergeDiff.missing.join(', '),
          extraUnexpectedHeaders: mergeDiff.extra.join(', '),
          forced: !!options.force
        });

      return {
        ok: true,
        pageName: page.pageName,
        action: 'rewritten_header_mismatch',
        additions: [],
        newWidth: schema.headers.length,
        existing: existing.length,
        backend: schema.headers.length,
        missingExpectedHeaders: mergeDiff.missing,
        extraUnexpectedHeaders: mergeDiff.extra,
        message: 'Header row rewritten to approved schema because width matched but names/order were wrong.'
      };
    }

    var existingSet = new Set(existing.map(String));

    var headerToKey = (typeof getHeaderToKey === 'function')
      ? getHeaderToKey(page.pageName)
      : {};

    var keyToAllAliases = {};
    Object.keys(headerToKey).forEach(function (h) {
      var k = headerToKey[h];
      if (!k) return;
      if (!keyToAllAliases[k]) keyToAllAliases[k] = [];
      keyToAllAliases[k].push(h);
    });

    var sheetCoversKey = function (key) {
      var aliases = keyToAllAliases[key] || [];
      return aliases.some(function (alias) {
        return existingSet.has(alias);
      });
    };

    var additions = [];

    schema.headers.forEach(function (bh, idx) {
      if (existingSet.has(bh)) return;

      var apiKey = (schema.keys && schema.keys[idx])
        ? schema.keys[idx]
        : ((typeof coreResolveHeaderKey_ === 'function')
          ? coreResolveHeaderKey_(bh, headerToKey)
          : null);

      if (apiKey && sheetCoversKey(apiKey)) return;

      additions.push(bh);
    });

    if (additions.length === 0) {
      logRun_('INFO', 'mergeBackendHeadersIntoSheet', page.pageName,
        'No additions — sheet already covers all schema headers.',
        {
          status: 'OK',
          existing: existing.length,
          backend: schema.headers.length,
          additions: 0,
          schemaSource: schema.source,
          forced: !!options.force
        });

      return {
        ok: true,
        pageName: page.pageName,
        action: 'no_additions',
        additions: [],
        newWidth: existing.length,
        existing: existing.length,
        backend: schema.headers.length
      };
    }

    var newWidth = existing.length + additions.length;
    if (sheet.getMaxColumns() < newWidth) {
      sheet.insertColumnsAfter(sheet.getMaxColumns(), newWidth - sheet.getMaxColumns());
    }

    sheet.getRange(1, existing.length + 1, 1, additions.length).setValues([additions]);

    logRun_('INFO', 'mergeBackendHeadersIntoSheet', page.pageName,
      'Appended ' + additions.length + ' new column(s) without disturbing existing layout.',
      {
        status: 'OK',
        existing: existing.length,
        backend: schema.headers.length,
        additions: additions.length,
        newWidth: newWidth,
        schemaSource: schema.source,
        addedHeaders: additions.join(', '),
        forced: !!options.force
      });

    return {
      ok: true,
      pageName: page.pageName,
      action: 'appended',
      additions: additions,
      newWidth: newWidth,
      existing: existing.length,
      backend: schema.headers.length
    };
  });
}

function proposeHeaderUpgrades_(pageName) {
  var validation = validateHeaderMapping_(pageName);
  return {
    ok: true,
    pageName: validation.pageName,
    suggestions: validation.backendNew || [],
    counts: validation.counts || {},
    mode: validation.mode
  };
}

function getMappedHeaderCoverage_(pageName) {
  var v = validateHeaderMapping_(pageName);

  return {
    pageName: v.pageName,
    totalSheetHeaders: v.counts.total,
    mappedToApi: v.counts.mapped,
    unmapped: v.counts.unmapped,
    backendKeysNotInSheet: v.counts.backendNew,
    schemaCheckMode: v.mode
  };
}

function debugPageSchema_(pageName) {
  var page = getPageConfig(pageName);
  var schema = page.pageName === SHEET_NAMES.DATA_DICTIONARY
    ? fetchDataDictionarySchema_()
    : fetchSheetSpec_(page.pageName);

  var expected = Number(page.expectedColumnCount || 0);
  var mode = (typeof getSchemaCheckMode === 'function')
    ? getSchemaCheckMode(page.pageName)
    : 'min';

  var summary = {
    pageName: page.pageName,
    ok: !!(schema && schema.ok),
    source: (schema && schema.source) || '',
    usedFallback: !!(schema && schema.usedFallback),
    promotedFromBackend: !!(schema && schema.promotedFromBackend),
    backendWidth: (schema && schema.backendWidth) || '',
    headersCount: (schema && schema.headers) ? schema.headers.length : 0,
    keysCount: (schema && schema.keys) ? schema.keys.length : 0,
    firstHeaders: (schema && schema.headers) ? schema.headers.slice(0, 10) : [],
    firstKeys: (schema && schema.keys) ? schema.keys.slice(0, 10) : [],
    schemaCheckMode: mode,
    expectedColumnCount: expected,
    matchesExpected: expected > 0
      ? ((schema && schema.headers) ? schema.headers.length === expected : false)
      : null
  };

  try {
    Logger.log('[v2.6.1 SCHEMA-DIAG][' + page.pageName + '] ' +
      'ok=' + summary.ok +
      ' source=' + summary.source +
      ' usedFallback=' + summary.usedFallback +
      ' promotedFromBackend=' + summary.promotedFromBackend +
      ' backendWidth=' + summary.backendWidth +
      ' headers=' + summary.headersCount +
      ' keys=' + summary.keysCount +
      ' expectedColumnCount=' + expected +
      ' matchesExpected=' + summary.matchesExpected +
      ' mode=' + mode);

    Logger.log('[v2.6.1 SCHEMA-DIAG][' + page.pageName + '] firstHeaders=' +
      JSON.stringify(summary.firstHeaders));

    Logger.log('[v2.6.1 SCHEMA-DIAG][' + page.pageName + '] firstKeys=' +
      JSON.stringify(summary.firstKeys));
  } catch (e) {}

  return summary;
}

/* =========================================================
 * Multi-wave backend group propagation diagnostic
 * ========================================================= */

function debugV28SchemaPropagation_(pageName) {
  var page = getPageConfig(pageName);
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(page.sheetName);

  var schema = page.pageName === SHEET_NAMES.DATA_DICTIONARY
    ? fetchDataDictionarySchema_()
    : fetchSheetSpec_(page.pageName);

  var backendKeys = (schema && Array.isArray(schema.keys)) ? schema.keys : [];
  var backendKeySet = new Set(backendKeys.map(String));

  var headerToKey = (typeof getHeaderToKey === 'function')
    ? getHeaderToKey(page.pageName)
    : {};

  var keyToHeaders = {};
  if (headerToKey && typeof headerToKey === 'object') {
    Object.keys(headerToKey).forEach(function (h) {
      var k = headerToKey[h];
      if (!k) return;
      if (!keyToHeaders[k]) keyToHeaders[k] = [];
      keyToHeaders[k].push(h);
    });
  }

  var sheetHeaders = sheet ? readCurrentSheetHeaders_(sheet) : [];
  var sheetHeaderSet = new Set(sheetHeaders.map(String));

  var classifyGroup = function (groupKeys) {
    var result = {
      emitted: 0,
      mapped: 0,
      onSheet: 0,
      missingInConfig: [],
      missingOnSheet: [],
      keys: groupKeys.slice()
    };

    groupKeys.forEach(function (k) {
      if (!backendKeySet.has(k)) return;

      result.emitted++;

      var aliases = keyToHeaders[k] || [];
      if (aliases.length > 0) {
        result.mapped++;

        var onSheetForKey = aliases.some(function (a) {
          return sheetHeaderSet.has(a);
        });

        if (onSheetForKey) result.onSheet++;
        else result.missingOnSheet.push(k);
      } else {
        result.missingInConfig.push(k);
      }
    });

    return result;
  };

  var groups = {
    INSIGHTS_WAVE_3: classifyGroup(_V28_COLUMN_GROUPS_.INSIGHTS_WAVE_3),
    DECISION_MATRIX_V28: classifyGroup(_V28_COLUMN_GROUPS_.DECISION_MATRIX_V28),
    CANDLESTICK_V28: classifyGroup(_V28_COLUMN_GROUPS_.CANDLESTICK_V28),
    CANONICAL_PROVENANCE_V53: classifyGroup(_V28_COLUMN_GROUPS_.CANONICAL_PROVENANCE_V53),
    SCORING_V574: classifyGroup(_V28_COLUMN_GROUPS_.SCORING_V574),
    INVESTABILITY_GATE_V578: classifyGroup(_V28_COLUMN_GROUPS_.INVESTABILITY_GATE_V578),
    TOP10_ONLY: classifyGroup(_V28_COLUMN_GROUPS_.TOP10_ONLY)
  };

  var knownGroupKeys = new Set();
  Object.keys(_V28_COLUMN_GROUPS_).forEach(function (groupName) {
    _V28_COLUMN_GROUPS_[groupName].forEach(function (k) {
      knownGroupKeys.add(k);
    });
  });

  var legacyKeys = backendKeys.filter(function (k) {
    return !knownGroupKeys.has(k);
  });

  groups.legacy = (function () {
    var emitted = 0;
    var mapped = 0;
    var onSheet = 0;
    var missingInConfig = [];
    var missingOnSheet = [];

    legacyKeys.forEach(function (k) {
      emitted++;

      var aliases = keyToHeaders[k] || [];
      if (aliases.length > 0) {
        mapped++;

        var onSheetForKey = aliases.some(function (a) {
          return sheetHeaderSet.has(a);
        });

        if (onSheetForKey) onSheet++;
        else missingOnSheet.push(k);
      } else {
        missingInConfig.push(k);
      }
    });

    return {
      emitted: emitted,
      mapped: mapped,
      onSheet: onSheet,
      missingInConfig: missingInConfig,
      missingOnSheet: missingOnSheet,
      keys: legacyKeys
    };
  })();

  var trackedGroupNames = [
    'INSIGHTS_WAVE_3',
    'DECISION_MATRIX_V28',
    'CANDLESTICK_V28',
    'CANONICAL_PROVENANCE_V53',
    'SCORING_V574',
    'INVESTABILITY_GATE_V578',
    'TOP10_ONLY'
  ];

  var isTop10 = page.pageName === SHEET_NAMES.TOP_10_INVESTMENTS;

  var trackedMissingInConfigTotal = trackedGroupNames.reduce(function (acc, g) {
    if (g === 'TOP10_ONLY' && !isTop10) return acc;
    return acc + (groups[g].missingInConfig.length || 0);
  }, 0);

  var trackedMissingOnSheetTotal = trackedGroupNames.reduce(function (acc, g) {
    if (g === 'TOP10_ONLY' && !isTop10) return acc;
    return acc + (groups[g].missingOnSheet.length || 0);
  }, 0);

  var expected = Number(page.expectedColumnCount || 0);
  var widthOk = expected > 0 ? sheetHeaders.length === expected : true;

  var ok = !!(schema && schema.ok) &&
    trackedMissingInConfigTotal === 0 &&
    trackedMissingOnSheetTotal === 0 &&
    widthOk;

  var report = {
    ok: ok,
    pageName: page.pageName,
    source: (schema && schema.source) || '',
    usedFallback: !!(schema && schema.usedFallback),
    promotedFromBackend: !!(schema && schema.promotedFromBackend),
    backendWidthOriginal: (schema && schema.backendWidth) || '',
    backendKeysCount: backendKeys.length,
    sheetHeadersCount: sheetHeaders.length,
    expectedColumnCount: expected,
    widthOk: widthOk,
    schemaCheckMode: (typeof getSchemaCheckMode === 'function')
      ? getSchemaCheckMode(page.pageName)
      : 'min',
    groups: groups,
    trackedMissingInConfigTotal: trackedMissingInConfigTotal,
    trackedMissingOnSheetTotal: trackedMissingOnSheetTotal,
    v28MissingInConfigTotal: trackedMissingInConfigTotal
  };

  try {
    var groupSummary = trackedGroupNames.map(function (g) {
      var grp = groups[g];
      return g + '=' + grp.emitted + '/' + grp.mapped + '/' + grp.onSheet;
    }).join(' ');

    Logger.log('[v2.6.1 SCHEMA-PROPAGATION][' + page.pageName + '] ' +
      'ok=' + ok +
      ' backendKeys=' + backendKeys.length +
      ' sheetHeaders=' + sheetHeaders.length +
      ' expected=' + expected +
      ' widthOk=' + widthOk +
      ' usedFallback=' + report.usedFallback +
      ' promotedFromBackend=' + report.promotedFromBackend +
      ' trackedMissingConfig=' + trackedMissingInConfigTotal +
      ' trackedMissingSheet=' + trackedMissingOnSheetTotal +
      ' groups[emitted/mapped/onSheet]: ' + groupSummary +
      ' legacy[emitted/mapped/onSheet]=' +
      groups.legacy.emitted + '/' + groups.legacy.mapped + '/' + groups.legacy.onSheet);

    trackedGroupNames.forEach(function (g) {
      if (g === 'TOP10_ONLY' && !isTop10) return;

      var grp = groups[g];

      if (grp.missingInConfig.length > 0) {
        Logger.log('[v2.6.1 SCHEMA-PROPAGATION][' + page.pageName + '] ' +
          g + ' missingInConfig=' + JSON.stringify(grp.missingInConfig));
      }

      if (grp.missingOnSheet.length > 0) {
        Logger.log('[v2.6.1 SCHEMA-PROPAGATION][' + page.pageName + '] ' +
          g + ' missingOnSheet=' + JSON.stringify(grp.missingOnSheet));
      }
    });
  } catch (e) {}

  return report;
}

/* =========================================================
 * Cross-page schema contract status
 * ========================================================= */

function diagnoseSchemaContractStatus_() {
  var pages = (typeof getSupportedPages === 'function')
    ? getSupportedPages()
    : [];

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var results = [];

  var statusCounts = {
    aligned: 0,
    'decision-owned': 0,
    'header-mismatch': 0,
    'sheet-behind': 0,
    'backend-behind-promoted': 0,
    'backend-behind': 0,
    'sheet-ahead': 0,
    'no-sheet': 0,
    'fetch-failed': 0,
    unknown: 0
  };

  pages.forEach(function (pageName) {
    var page = getPageConfig(pageName);

    /* v2.7.0: decision-owned pages are intentionally NOT on the engine
     * column contract — the cockpit renders its own layout. Classify and
     * skip the schema fetch + width comparison entirely. */
    if (_schemaIsDecisionOwnedPage_(page.pageName)) {
      statusCounts['decision-owned'] = (statusCounts['decision-owned'] || 0) + 1;

      var ownedEntry = {
        pageName: page.pageName,
        status: 'decision-owned',
        expected: Number(page.expectedColumnCount || 0),
        sheetWidth: '',
        schemaWidth: '',
        backendOriginalWidth: '',
        drift: 0,
        headerNamesOk: true,
        missingExpectedHeaders: [],
        extraUnexpectedHeaders: [],
        usedFallback: false,
        promotedFromBackend: false,
        source: 'decision_cockpit'
      };

      results.push(ownedEntry);

      try {
        Logger.log('[v2.6.1 CONTRACT-STATUS][' + page.pageName + '] ' +
          'status=decision-owned (cockpit-rendered; engine contract not applicable)');
      } catch (eOwned) {}

      return;
    }

    var expected = Number(page.expectedColumnCount || 0);
    var sheet = ss.getSheetByName(page.sheetName);
    var sheetHeaders = sheet ? readCurrentSheetHeaders_(sheet) : [];
    var sheetWidth = sheetHeaders.length;
    var fallbackHeaders = getFallbackHeaders(page.pageName);
    var fallbackHeaderDiff = fallbackHeaders.length
      ? _schemaHeaderDiff_(sheetHeaders, fallbackHeaders)
      : { exact: true, missing: [], extra: [] };
    var headerNamesOk = fallbackHeaders.length
      ? fallbackHeaderDiff.exact
      : true;

    var schemaWidth = 0;
    var backendOriginalWidth = '';
    var source = '';
    var usedFallback = false;
    var promotedFromBackend = false;
    var fetchOk = false;

    try {
      var schema = page.pageName === SHEET_NAMES.DATA_DICTIONARY
        ? fetchDataDictionarySchema_()
        : fetchSheetSpec_(page.pageName);

      if (schema && schema.ok) {
        fetchOk = true;
        schemaWidth = (schema.headers || []).length;
        source = schema.source || '';
        usedFallback = !!schema.usedFallback;
        promotedFromBackend = !!schema.promotedFromBackend;
        backendOriginalWidth = schema.backendWidth || '';
      }
    } catch (e) {
      fetchOk = false;
    }

    var status = 'unknown';

    if (!sheet) {
      status = 'no-sheet';
    } else if (!fetchOk) {
      status = 'fetch-failed';
    } else if (expected > 0 && sheetWidth === expected && schemaWidth === expected && !headerNamesOk) {
      status = 'header-mismatch';
    } else if (expected > 0 && sheetWidth === expected && schemaWidth === expected && headerNamesOk) {
      status = 'aligned';
    } else if (promotedFromBackend && schemaWidth === expected && sheetWidth < expected) {
      status = 'sheet-behind';
    } else if (promotedFromBackend && schemaWidth === expected && sheetWidth === expected && !headerNamesOk) {
      status = 'header-mismatch';
    } else if (promotedFromBackend && schemaWidth === expected && sheetWidth === expected) {
      status = 'aligned';
    } else if (expected > 0 && schemaWidth < expected) {
      status = promotedFromBackend ? 'backend-behind-promoted' : 'backend-behind';
    } else if (expected > 0 && sheetWidth < expected) {
      status = 'sheet-behind';
    } else if (expected > 0 && sheetWidth > expected) {
      status = 'sheet-ahead';
    } else if (sheetWidth === schemaWidth && (expected === 0 || sheetWidth === expected) && headerNamesOk) {
      status = 'aligned';
    } else if (sheetWidth === schemaWidth && expected > 0 && !headerNamesOk) {
      status = 'header-mismatch';
    }

    statusCounts[status] = (statusCounts[status] || 0) + 1;

    var entry = {
      pageName: page.pageName,
      status: status,
      expected: expected,
      sheetWidth: sheetWidth,
      schemaWidth: schemaWidth,
      backendOriginalWidth: backendOriginalWidth,
      drift: schemaWidth - sheetWidth,
      headerNamesOk: headerNamesOk,
      missingExpectedHeaders: fallbackHeaderDiff.missing,
      extraUnexpectedHeaders: fallbackHeaderDiff.extra,
      usedFallback: usedFallback,
      promotedFromBackend: promotedFromBackend,
      source: source
    };

    results.push(entry);

    try {
      Logger.log('[v2.6.1 CONTRACT-STATUS][' + page.pageName + '] ' +
        'status=' + status +
        ' expected=' + expected +
        ' sheet=' + sheetWidth +
        ' schema=' + schemaWidth +
        ' backendOriginal=' + backendOriginalWidth +
        ' drift=' + entry.drift +
        ' headerNamesOk=' + headerNamesOk +
        (headerNamesOk ? '' : ' missingExpected=' + JSON.stringify(fallbackHeaderDiff.missing) + ' extraUnexpected=' + JSON.stringify(fallbackHeaderDiff.extra)) +
        ' source=' + (source || '(none)') +
        ' usedFallback=' + usedFallback +
        ' promotedFromBackend=' + promotedFromBackend);
    } catch (e) {}
  });

  var allOk = results.every(function (r) {
    return r.status === 'aligned' || r.status === 'decision-owned';
  });

  try {
    var summaryParts = Object.keys(statusCounts)
      .filter(function (k) {
        return statusCounts[k] > 0;
      })
      .map(function (k) {
        return k + '=' + statusCounts[k];
      });

    Logger.log('[v2.6.1 CONTRACT-STATUS][summary] ' +
      'pages=' + results.length +
      ' ok=' + allOk +
      ' ' + summaryParts.join(' '));
  } catch (e) {}

  return {
    ok: allOk,
    pageCount: results.length,
    statusCounts: statusCounts,
    pages: results
  };
}

/* =========================================================
 * Public runner wrappers for Apps Script dropdown
 * ========================================================= */

function verifySchemaVersion() {
  var report = {
    ok: true,
    version: '2.7.0',
    expectedMainColumns: 115,
    expectedTop10Columns: 118,
    hasInvestabilityGate: !!(_V28_COLUMN_GROUPS_ && _V28_COLUMN_GROUPS_.INVESTABILITY_GATE_V578 && _V28_COLUMN_GROUPS_.INVESTABILITY_GATE_V578.length === 9),
    hasTop10Only: !!(_V28_COLUMN_GROUPS_ && _V28_COLUMN_GROUPS_.TOP10_ONLY && _V28_COLUMN_GROUPS_.TOP10_ONLY.length === 3),
    hasDecisionOwnedGuard: typeof _schemaIsDecisionOwnedPage_ === 'function',
    decisionOwnedWired: _schemaIsDecisionOwnedPage_('Top_10_Investments') === true,
    message: '03_Schema.gs v2.7.0 loaded.'
  };

  try {
    Logger.log('[v2.6.1 SCHEMA-VERSION] ' + JSON.stringify(report));
  } catch (e) {}

  return report;
}

function diagnoseSchemaContractStatus() {
  return diagnoseSchemaContractStatus_();
}

function debugSchemaPropagationGlobalMarkets() {
  return debugV28SchemaPropagation_(SHEET_NAMES.GLOBAL_MARKETS);
}

function repairInsightsAnalysisHeaders() {
  return applyHeadersFromSchema_(SHEET_NAMES.INSIGHTS_ANALYSIS, { force: true });
}

function repairCurrentSheetHeadersFromSchema() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss ? ss.getActiveSheet() : null;

  if (!sheet) {
    return {
      ok: false,
      message: 'No active sheet.'
    };
  }

  return applyHeadersFromSchema_(sheet.getName(), { force: true });
}
