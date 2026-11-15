/**
 * 06_Market_Leaders.gs
 * Tadawul Tracker — Market Leaders Page Module
 * Version: 1.6.0
 *
 * Aligned with — current 115/118 investment reliability contract
 * - 00_Config.gs        v1.12.1
 * - 01_Menu.gs          v2.9.0
 * - 03_Schema.gs        v2.5.0
 * - 04_Format.gs        v2.12.0
 * - 05_Refresh.gs       v1.11.2
 * - 07_Global_Markets.gs v1.7.1
 * - 11_SpecialPages.gs  v1.6.0
 * - 99_Data_Audit.gs    v1.3.0
 * - 99_Diag.gs          v2.3.0
 *
 * v1.6.0 MAIN CHANGES
 * -------------------
 * 1. Updates Market_Leaders from the old v1.5.1 dependency chain
 *    to the current 115-column main-page contract.
 *
 * 2. Adds explicit investability-gate header tracking:
 *      Forecast Source
 *      Data Quality Score
 *      Forecast Reliability Score
 *      Provider/Engine Conflict
 *      Conflict Type
 *      Final Decision Basis
 *      Investability Status
 *      Final Action
 *      Block Reason
 *
 * 3. Uses the new 05_Refresh validators when available:
 *      validateExpectedRefreshWidth_
 *      validatePostRefreshIntegrity_
 *    This keeps Market_Leaders aligned with the same validation layer
 *    as Global_Markets and Top_10_Investments.
 *
 * 4. Removes the duplicate shared dispatcher manageSymbolsForPage_().
 *    That dispatcher must live only in the central menu/special-pages
 *    layer to avoid Apps Script global-namespace overwrites. This file
 *    keeps only manageMarketLeadersSymbols_().
 *
 * 5. Keeps the useful existing logic:
 *      - build / refresh / batch refresh wrappers
 *      - selected-row refresh wrapper
 *      - sort and rank by configured active field, Rank, Opportunity,
 *        or Overall Score
 *      - symbol normalization
 *      - header diagnostics
 *      - single-symbol blank-cell diagnostic
 *
 * 6. Adds debugMarketLeadersInvestmentGate_(symbol) for a direct
 *    row-level check of the current investment-gate fields.
 */

'use strict';

/* =========================================================
 * Top-level identifiers
 * ========================================================= */

var MARKET_LEADERS_PAGE_ = SHEET_NAMES.MARKET_LEADERS;
var MARKET_LEADERS_EXPECTED_COLUMNS_ = 115;

var MARKET_LEADERS_REQUIRED_BUSINESS_HEADERS_ = Object.freeze([
  'Symbol',
  'Name',
  'Asset Class',
  'Exchange',
  'Currency',
  'Country',
  'Sector',
  'Industry',
  'Current Price',
  'Opportunity Score',
  'Overall Score',
  'Rank (Overall)',
  'Recommendation',
  'Recommendation Detail',
  'Recommendation Reason',
  'Data Provider',
  'Last Updated (Riyadh)',
  'Warnings'
]);

var MARKET_LEADERS_INVESTMENT_GATE_HEADERS_ = Object.freeze([
  'Forecast Source',
  'Data Quality Score',
  'Forecast Reliability Score',
  'Provider/Engine Conflict',
  'Conflict Type',
  'Final Decision Basis',
  'Investability Status',
  'Final Action',
  'Block Reason'
]);

// Existing single-symbol blank-cell diagnostic fields. Kept for
// backward compatibility with diagnostics that still check the legacy
// view-field population signal.
var MARKET_LEADERS_VIEW_FIELDS_ = Object.freeze([
  'Fundamental View',
  'Technical View',
  'Risk View',
  'Value View',
  'Upside %'
]);

/* =========================================================
 * Public page module entry points
 * ========================================================= */

function buildMarketLeaders_() {
  return buildPage_(MARKET_LEADERS_PAGE_);
}

function refreshMarketLeaders_(options) {
  options = options || {};

  var symbols = marketLeadersNormalizeSymbols_(
    options.symbols && options.symbols.length
      ? options.symbols
      : getMarketLeadersSymbols_()
  );

  var payloadOverrides = {
    symbols: symbols,
    tickers: symbols,
    direct_symbols: symbols
  };

  if (options.limit) {
    payloadOverrides.limit = options.limit;
  }

  if (options.mode) {
    payloadOverrides.mode = options.mode;
  }

  var refreshOpts = {
    payloadOverrides: payloadOverrides,
    applyFormat: options.applyFormat !== false,
    applySort: options.applySort !== false,
    allowAutoBatch: options.allowAutoBatch !== false
  };

  if (options.forceHeaders === true) {
    refreshOpts.forceHeaders = true;
  }

  if (options.forceHeaders === false) {
    refreshOpts.forceHeaders = false;
  }

  if (options.legacyPath === true) {
    refreshOpts.legacyPath = true;
  }

  var result = refreshPage_(MARKET_LEADERS_PAGE_, refreshOpts);
  result = _marketLeadersAppendFallbackWarning_(result);

  if (!result || result.paused || result.partial) {
    return result;
  }

  result = _marketLeadersRunOptionalPostProcessors_(result, options);
  return result;
}

function refreshMarketLeadersSelectedRow_(options) {
  options = options || {};
  var result = refreshSelectedRow_(MARKET_LEADERS_PAGE_, options);
  result = _marketLeadersRunOptionalPostProcessors_(result, {
    skipValidation: false
  });
  return result;
}

function refreshMarketLeadersInBatches_(options) {
  options = options || {};

  var symbols = marketLeadersNormalizeSymbols_(
    options.symbols && options.symbols.length
      ? options.symbols
      : getMarketLeadersSymbols_()
  );

  var payloadOverrides = {
    symbols: symbols,
    tickers: symbols,
    direct_symbols: symbols,
    mode: options.mode || getDefaultMode()
  };

  if (options.limit) {
    payloadOverrides.limit = options.limit;
  }

  var batchOpts = {
    batchSize: options.batchSize || REFRESH_BATCH_SIZE_DEFAULT_,
    payloadOverrides: payloadOverrides,
    applyFormat: options.applyFormat !== false,
    applySort: options.applySort !== false
  };

  if (options.legacyPath === true) {
    batchOpts.legacyPath = true;
  }

  var result = refreshPageInBatches_(MARKET_LEADERS_PAGE_, batchOpts);
  result = _marketLeadersAppendFallbackWarning_(result);

  if (!result || result.paused || result.partial) {
    return result;
  }

  result = _marketLeadersRunOptionalPostProcessors_(result, options);
  return result;
}

function validateMarketLeaders_() {
  var page = getPageConfig(MARKET_LEADERS_PAGE_);
  var sheet = ensureSheetExists_(MARKET_LEADERS_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var rowCount = Math.max(0, sheet.getLastRow() - 1);
  var headerMap = getHeaderMapMarketLeaders_(headers);

  var expectedWidth = Number(page.expectedColumnCount || MARKET_LEADERS_EXPECTED_COLUMNS_);
  var missingBusinessHeaders = getMissingMarketLeadersBusinessHeaders_(headerMap);
  var missingGateHeaders = getMissingMarketLeadersInvestmentGateHeaders_(headerMap);
  var inferredLayout = inferMarketLeadersLayout_(headerMap);

  var strictResult = null;
  var mappingResult = null;
  var widthValidation = null;
  var postRefreshValidation = null;

  try {
    strictResult = validatePage_(MARKET_LEADERS_PAGE_);
  } catch (e) {
    strictResult = {
      ok: false,
      error: e && e.message ? e.message : String(e),
      missingHeaders: [],
      extraHeaders: [],
      missingRequiredKeys: [],
      unmappedHeaders: []
    };
  }

  if (strictResult && strictResult.mappingModeUsed) {
    mappingResult = {
      ok: (strictResult.unmappedHeaders || []).length === 0,
      pageName: strictResult.pageName || MARKET_LEADERS_PAGE_,
      unmapped: (strictResult.unmappedHeaders || []).slice(),
      backendNew: [],
      counts: {
        total: strictResult.actualHeadersCount || headers.length,
        mapped: Math.max(0, (strictResult.actualHeadersCount || headers.length) -
          (strictResult.unmappedHeaders || []).length),
        unmapped: (strictResult.unmappedHeaders || []).length,
        userOwned: 0,
        backendNew: 0
      }
    };
  }

  if (typeof validateExpectedRefreshWidth_ === 'function') {
    try {
      widthValidation = validateExpectedRefreshWidth_(MARKET_LEADERS_PAGE_, sheet);
    } catch (e1) {
      widthValidation = {
        ok: false,
        warnings: ['Width validation failed: ' + (e1 && e1.message ? e1.message : String(e1))]
      };
    }
  } else {
    widthValidation = {
      ok: expectedWidth <= 0 || headers.length === expectedWidth,
      expected: expectedWidth,
      actual: headers.length,
      warnings: headers.length === expectedWidth
        ? []
        : ['Width mismatch: expected ' + expectedWidth + ', actual ' + headers.length]
    };
  }

  if (typeof validatePostRefreshIntegrity_ === 'function') {
    try {
      postRefreshValidation = validatePostRefreshIntegrity_(MARKET_LEADERS_PAGE_, sheet, {
        expectedSymbolCount: getMarketLeadersSymbols_().length
      });
    } catch (e2) {
      postRefreshValidation = {
        ok: false,
        warnings: ['Post-refresh validation failed: ' + (e2 && e2.message ? e2.message : String(e2))]
      };
    }
  } else {
    postRefreshValidation = validateMarketLeadersGateLocally_(sheet, headers);
  }

  var strictOk = !strictResult || strictResult.ok !== false;
  var mappingOk = !mappingResult || mappingResult.ok !== false;
  var widthOk = !widthValidation || widthValidation.ok !== false;
  var postOk = !postRefreshValidation || postRefreshValidation.ok !== false;

  var ok = strictOk && mappingOk && widthOk && postOk &&
    headers.length === expectedWidth &&
    missingBusinessHeaders.length === 0 &&
    missingGateHeaders.length === 0;

  var warningsList = []
    .concat(missingBusinessHeaders.map(function (h) { return 'Missing business header: ' + h; }))
    .concat(missingGateHeaders.map(function (h) { return 'Missing gate header: ' + h; }))
    .concat(strictResult && strictResult.missingHeaders ? strictResult.missingHeaders : [])
    .concat(strictResult && strictResult.extraHeaders ? strictResult.extraHeaders : [])
    .concat(strictResult && strictResult.missingRequiredKeys ? strictResult.missingRequiredKeys : [])
    .concat(strictResult && strictResult.unmappedHeaders ? strictResult.unmappedHeaders : [])
    .concat(widthValidation && widthValidation.warnings ? widthValidation.warnings : [])
    .concat(postRefreshValidation && postRefreshValidation.warnings ? postRefreshValidation.warnings : []);

  warningsList = marketLeadersUniqueStrings_(warningsList);

  var enrichedResult = {
    ok: ok,
    pageName: MARKET_LEADERS_PAGE_,
    expectedColumnCount: expectedWidth,
    actualHeadersCount: headers.length,
    rowCount: rowCount,
    inferredLayout: inferredLayout,
    missingBusinessHeaders: missingBusinessHeaders,
    missingGateHeaders: missingGateHeaders,
    missingHeaders: strictResult.missingHeaders || [],
    extraHeaders: strictResult.extraHeaders || [],
    orderMatches: !!strictResult.orderMatches,
    missingRequiredKeys: strictResult.missingRequiredKeys || [],
    strictResult: strictResult,
    mappingResult: mappingResult,
    widthValidation: widthValidation,
    postRefreshValidation: postRefreshValidation,
    gateCoverage: getMarketLeadersInvestmentGateCoverage_(headerMap),
    strictApprovedRows: getMarketLeadersStrictApprovedRows_(),
    validationMode: mappingResult ? 'header-map' : 'strict-positional'
  };

  logRun_(
    enrichedResult.ok ? 'INFO' : 'WARN',
    'validateMarketLeaders',
    MARKET_LEADERS_PAGE_,
    enrichedResult.ok
      ? 'Market_Leaders validation passed as full 115-column investment page.'
      : 'Market_Leaders validation found issues.',
    {
      status: enrichedResult.ok ? 'PASS' : 'WARN',
      details: {
        ok: enrichedResult.ok,
        validationMode: enrichedResult.validationMode,
        expectedColumnCount: expectedWidth,
        actualHeadersCount: enrichedResult.actualHeadersCount,
        rowCount: enrichedResult.rowCount,
        inferredLayout: inferredLayout,
        missingBusinessHeaders: missingBusinessHeaders,
        missingGateHeaders: missingGateHeaders,
        widthValidation: widthValidation,
        postRefreshValidation: postRefreshValidation
      }
    }
  );

  writePageStatus_(MARKET_LEADERS_PAGE_, {
    status: enrichedResult.ok ? 'VALID' : 'WARN',
    message: enrichedResult.ok
      ? 'Market_Leaders validation passed - full 115-column investment page'
      : 'Market_Leaders validation issues found',
    endpoint: '',
    httpCode: '',
    rows: rowCount,
    columns: headers.length,
    durationMs: '',
    warnings: warningsList
  });

  return enrichedResult;
}

function sortMarketLeaders_() {
  return withScriptLock_(function () {
    var page = getPageConfig(MARKET_LEADERS_PAGE_);
    var sheet = ensureSheetExists_(MARKET_LEADERS_PAGE_);
    var headers = readCurrentSheetHeaders_(sheet);
    var lastRow = sheet.getLastRow();

    if (lastRow <= 2 || !headers.length) {
      return {
        ok: true,
        pageName: MARKET_LEADERS_PAGE_,
        skipped: true,
        message: 'No data rows to sort.'
      };
    }

    var headerMap = getHeaderMapMarketLeaders_(headers);
    var sortPlan = resolveMarketLeadersSortPlan_(page, headerMap);

    if (!sortPlan || !sortPlan.column) {
      return {
        ok: true,
        pageName: MARKET_LEADERS_PAGE_,
        skipped: true,
        message: 'No usable Market_Leaders sort field found.'
      };
    }

    var expectedWidth = Number(page.expectedColumnCount || MARKET_LEADERS_EXPECTED_COLUMNS_);
    var sortRangeCols = Math.min(
      Math.max(1, headers.length),
      Math.max(1, sheet.getLastColumn())
    );

    if (expectedWidth > 0) {
      sortRangeCols = Math.min(sortRangeCols, expectedWidth);
    }

    if (sortPlan.column > sortRangeCols) {
      sortRangeCols = Math.min(sortPlan.column, sheet.getLastColumn());
    }

    sheet.getRange(2, 1, lastRow - 1, sortRangeCols).sort({
      column: sortPlan.column,
      ascending: sortPlan.ascending
    });

    logRun_(
      'INFO',
      'sortMarketLeaders',
      MARKET_LEADERS_PAGE_,
      'Market_Leaders sorted successfully.',
      {
        status: 'OK',
        sortField: sortPlan.field,
        ascending: sortPlan.ascending,
        sortRangeCols: sortRangeCols
      }
    );

    writePageStatus_(MARKET_LEADERS_PAGE_, {
      status: 'SORT_OK',
      message: 'Sorted by ' + sortPlan.field,
      endpoint: '',
      httpCode: '',
      rows: lastRow - 1,
      columns: headers.length,
      durationMs: '',
      warnings: []
    });

    return {
      ok: true,
      pageName: MARKET_LEADERS_PAGE_,
      sortedBy: sortPlan.field,
      ascending: sortPlan.ascending,
      sortRangeCols: sortRangeCols
    };
  });
}

function manageMarketLeadersSymbols_() {
  var symbols = getMarketLeadersSymbols_();
  var ui = SpreadsheetApp.getUi();

  ui.alert(
    'Market_Leaders Symbols',
    symbols.length ? symbols.join(', ') : 'No symbols configured.',
    ui.ButtonSet.OK
  );

  return {
    ok: true,
    pageName: MARKET_LEADERS_PAGE_,
    symbols: symbols
  };
}

/* =========================================================
 * Optional post-processors
 * ========================================================= */

function _marketLeadersRunOptionalPostProcessors_(result, options) {
  options = options || {};
  result = result || {};

  if (options.skipValidation !== true) {
    try {
      result.marketLeadersValidation = validateMarketLeaders_();
    } catch (e) {
      result.warnings = (result.warnings || []).concat([
        'Market_Leaders validation failed: ' + (e && e.message ? e.message : String(e))
      ]);
    }
  }

  return result;
}

function _marketLeadersAppendFallbackWarning_(result) {
  if (typeof appendGenericFallbackWarning_ === 'function' && result) {
    return appendGenericFallbackWarning_(MARKET_LEADERS_PAGE_, result) || result;
  }
  return result;
}

/* =========================================================
 * Header diagnostic / non-destructive merge entry points
 * ========================================================= */

function mergeMarketLeadersHeaders_(options) {
  if (typeof mergeBackendHeadersIntoSheet_ !== 'function') {
    throw new Error(
      'mergeMarketLeadersHeaders_ requires 03_Schema.gs v2.5.0 or later. '
      + 'Update 03_Schema.gs first, then re-run this command.'
    );
  }

  options = options || {};

  if (options.force !== false) {
    options.force = true;
  }

  return mergeBackendHeadersIntoSheet_(MARKET_LEADERS_PAGE_, options);
}

function proposeMarketLeadersHeaderUpgrades_() {
  if (typeof proposeHeaderUpgrades_ !== 'function') {
    throw new Error(
      'proposeMarketLeadersHeaderUpgrades_ requires 03_Schema.gs v2.5.0 or later.'
    );
  }

  return proposeHeaderUpgrades_(MARKET_LEADERS_PAGE_);
}

function diagnoseMarketLeaders_() {
  var page = getPageConfig(MARKET_LEADERS_PAGE_);
  var sheet = ensureSheetExists_(MARKET_LEADERS_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var headerMap = getHeaderMapMarketLeaders_(headers);

  var expectedWidth = Number(page.expectedColumnCount || MARKET_LEADERS_EXPECTED_COLUMNS_);
  var missingBusinessHeaders = getMissingMarketLeadersBusinessHeaders_(headerMap);
  var missingGateHeaders = getMissingMarketLeadersInvestmentGateHeaders_(headerMap);
  var symbols = getMarketLeadersSymbols_();
  var sortPlan = resolveMarketLeadersSortPlan_(page, headerMap);

  var coverage = null;
  if (typeof getMappedHeaderCoverage_ === 'function') {
    try {
      coverage = getMappedHeaderCoverage_(MARKET_LEADERS_PAGE_);
    } catch (e) {
      coverage = null;
    }
  }

  var ok = headers.length === expectedWidth &&
    missingBusinessHeaders.length === 0 &&
    missingGateHeaders.length === 0 &&
    (!coverage || coverage.unmapped === 0);

  var report = {
    ok: ok,
    pageName: MARKET_LEADERS_PAGE_,
    expectedColumnCount: expectedWidth,
    headerCount: headers.length,
    rowCount: Math.max(0, sheet.getLastRow() - 1),
    inferredLayout: inferMarketLeadersLayout_(headerMap),
    missingBusinessHeaders: missingBusinessHeaders,
    missingGateHeaders: missingGateHeaders,
    gateCoverage: getMarketLeadersInvestmentGateCoverage_(headerMap),
    strictApprovedRows: getMarketLeadersStrictApprovedRows_(),
    symbolsCount: symbols.length,
    sortField: sortPlan ? sortPlan.field : '',
    sortAscending: sortPlan ? sortPlan.ascending : null,
    coverage: coverage
  };

  logRun_(
    ok ? 'INFO' : 'WARN',
    'diagnoseMarketLeaders',
    MARKET_LEADERS_PAGE_,
    ok ? 'Market_Leaders diagnostic passed.' : 'Market_Leaders diagnostic flagged issues.',
    {
      status: ok ? 'OK' : 'WARN',
      details: report
    }
  );

  return report;
}

/* =========================================================
 * Row-level diagnostics
 * ========================================================= */

function debugMarketLeadersBlankCells_(optSymbol) {
  var sheet = ensureSheetExists_(MARKET_LEADERS_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var headerMap = getHeaderMapMarketLeaders_(headers);
  var lastRow = sheet.getLastRow();

  if (lastRow < 2 || headers.length === 0) {
    var emptyMsg = 'Market_Leaders has no data — run buildMarketLeaders_() first.';
    Logger.log('[v1.6.0 BLANK-CELLS DIAG] ' + emptyMsg);
    return { ok: false, message: emptyMsg };
  }

  var symbolColIdx = marketLeadersResolveHeaderColumn_(headerMap, 'Symbol');

  if (!symbolColIdx) {
    var noSymMsg = 'Market_Leaders has no Symbol column.';
    Logger.log('[v1.6.0 BLANK-CELLS DIAG] ' + noSymMsg);
    return { ok: false, message: noSymMsg };
  }

  var target = resolveMarketLeadersTargetRow_(sheet, symbolColIdx, optSymbol);

  if (!target.ok) {
    Logger.log('[v1.6.0 BLANK-CELLS DIAG] ' + target.message);
    return target;
  }

  Logger.log('[v1.6.0 BLANK-CELLS DIAG] Starting diagnostic for symbol="' +
    target.symbol + '" at row=' + target.row);

  var refreshResult = null;
  var refreshError = null;

  try {
    refreshResult = refreshSelectedRow_(MARKET_LEADERS_PAGE_, { row: target.row });
  } catch (refreshErr) {
    refreshError = refreshErr && refreshErr.message ? refreshErr.message : String(refreshErr);
    Logger.log('[v1.6.0 BLANK-CELLS DIAG] refreshSelectedRow_ THREW: ' + refreshError);
  }

  var rowValues = sheet.getRange(target.row, 1, 1, headers.length).getValues()[0];
  var viewFields = {};
  var populatedCount = 0;
  var emptyCount = 0;

  for (var v = 0; v < MARKET_LEADERS_VIEW_FIELDS_.length; v++) {
    var fieldName = MARKET_LEADERS_VIEW_FIELDS_[v];
    var col = marketLeadersResolveHeaderColumn_(headerMap, fieldName);
    var value = col ? rowValues[col - 1] : null;
    var isEmpty = marketLeadersIsBlank_(value) || String(value).trim() === '0';

    viewFields[fieldName] = {
      present: !!col,
      value: col ? value : null,
      empty: isEmpty
    };

    if (col) {
      if (isEmpty) emptyCount++;
      else populatedCount++;
    }
  }

  var report = {
    ok: refreshError === null && populatedCount > 0,
    symbol: target.symbol,
    row: target.row,
    refreshError: refreshError,
    refreshPath: refreshResult ? refreshResult.path : null,
    viewFields: viewFields,
    populatedCount: populatedCount,
    emptyCount: emptyCount,
    totalViewFields: MARKET_LEADERS_VIEW_FIELDS_.length
  };

  var verdict;
  if (refreshError !== null) {
    verdict = 'REFRESH_THREW';
  } else if (populatedCount === 0) {
    verdict = 'ALL_VIEW_FIELDS_BLANK';
  } else if (emptyCount > 0) {
    verdict = 'PARTIAL_POPULATION';
  } else {
    verdict = 'OK_ALL_POPULATED';
  }

  Logger.log('[v1.6.0 BLANK-CELLS DIAG] ' + verdict + ' symbol=' + target.symbol +
    ' populated=' + populatedCount + '/' + MARKET_LEADERS_VIEW_FIELDS_.length +
    ' refreshPath=' + (report.refreshPath || 'n/a'));

  for (var f = 0; f < MARKET_LEADERS_VIEW_FIELDS_.length; f++) {
    var fn = MARKET_LEADERS_VIEW_FIELDS_[f];
    var fv = viewFields[fn];
    Logger.log('[v1.6.0 BLANK-CELLS DIAG]   ' + fn +
      ': present=' + fv.present +
      ' empty=' + fv.empty +
      ' value=' + JSON.stringify(fv.value));
  }

  return report;
}

function debugMarketLeadersInvestmentGate_(optSymbol) {
  var sheet = ensureSheetExists_(MARKET_LEADERS_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var headerMap = getHeaderMapMarketLeaders_(headers);
  var lastRow = sheet.getLastRow();

  if (lastRow < 2 || headers.length === 0) {
    var emptyMsg = 'Market_Leaders has no data — run buildMarketLeaders_() first.';
    Logger.log('[v1.6.0 GATE DIAG] ' + emptyMsg);
    return { ok: false, message: emptyMsg };
  }

  var symbolCol = marketLeadersResolveHeaderColumn_(headerMap, 'Symbol');
  if (!symbolCol) {
    var noSymMsg = 'Market_Leaders has no Symbol column.';
    Logger.log('[v1.6.0 GATE DIAG] ' + noSymMsg);
    return { ok: false, message: noSymMsg };
  }

  var target = resolveMarketLeadersTargetRow_(sheet, symbolCol, optSymbol);
  if (!target.ok) {
    Logger.log('[v1.6.0 GATE DIAG] ' + target.message);
    return target;
  }

  var rowValues = sheet.getRange(target.row, 1, 1, Math.max(headers.length, sheet.getLastColumn())).getValues()[0];

  var fields = MARKET_LEADERS_INVESTMENT_GATE_HEADERS_.concat([
    'Current Price',
    'Risk Bucket',
    'Expected ROI 3M',
    'Expected ROI 12M',
    'Recommendation',
    'Recommendation Detail',
    'Warnings'
  ]);

  var fieldReport = {};
  var missingColumns = [];
  var blankFields = [];

  fields.forEach(function (field) {
    var col = marketLeadersResolveHeaderColumn_(headerMap, field);

    if (!col) {
      missingColumns.push(field);
      fieldReport[field] = {
        present: false,
        blank: true,
        value: null
      };
      return;
    }

    var value = rowValues[col - 1];
    var blank = marketLeadersIsBlank_(value);

    if (blank) {
      blankFields.push(field);
    }

    fieldReport[field] = {
      present: true,
      blank: blank,
      value: value
    };
  });

  var ok = missingColumns.length === 0 &&
    blankFields.indexOf('Final Action') === -1 &&
    blankFields.indexOf('Investability Status') === -1;

  Logger.log('[v1.6.0 GATE DIAG] ' +
    (ok ? 'OK' : 'WARN') +
    ' symbol=' + target.symbol +
    ' row=' + target.row +
    ' missingColumns=' + missingColumns.length +
    ' blankFields=' + blankFields.length);

  return {
    ok: ok,
    symbol: target.symbol,
    row: target.row,
    missingColumns: missingColumns,
    blankFields: blankFields,
    fields: fieldReport
  };
}

/* =========================================================
 * Helpers — local gate audit
 * ========================================================= */

function validateMarketLeadersGateLocally_(sheet, headers) {
  var result = {
    ok: true,
    pageName: MARKET_LEADERS_PAGE_,
    checkedRows: 0,
    investRows: 0,
    warnings: [],
    investWithBlankPrice: [],
    investWithNegativeRoi3m: [],
    investWithNegativeRoi12m: [],
    investWithHighRisk: [],
    investWithConflict: [],
    investWithBlockReason: [],
    investWithLowDataQuality: [],
    investWithLowReliability: []
  };

  headers = headers || readCurrentSheetHeaders_(sheet);

  if (!headers.length || sheet.getLastRow() < 2) {
    result.ok = false;
    result.warnings.push('Market_Leaders has no data rows.');
    return result;
  }

  var headerMap = getHeaderMapMarketLeaders_(headers);

  var cols = {
    symbol: marketLeadersResolveHeaderColumn_(headerMap, 'Symbol'),
    price: marketLeadersResolveHeaderColumn_(headerMap, 'Current Price'),
    finalAction: marketLeadersResolveHeaderColumn_(headerMap, 'Final Action'),
    investability: marketLeadersResolveHeaderColumn_(headerMap, 'Investability Status'),
    risk: marketLeadersResolveHeaderColumn_(headerMap, 'Risk Bucket'),
    roi3m: marketLeadersResolveHeaderColumn_(headerMap, 'Expected ROI 3M'),
    roi12m: marketLeadersResolveHeaderColumn_(headerMap, 'Expected ROI 12M'),
    dq: marketLeadersResolveHeaderColumn_(headerMap, 'Data Quality Score'),
    rel: marketLeadersResolveHeaderColumn_(headerMap, 'Forecast Reliability Score'),
    conflict: marketLeadersResolveHeaderColumn_(headerMap, 'Provider/Engine Conflict'),
    blockReason: marketLeadersResolveHeaderColumn_(headerMap, 'Block Reason')
  };

  if (!cols.symbol || !cols.finalAction) {
    result.ok = false;
    result.warnings.push('Local gate validation cannot run because Symbol or Final Action column is missing.');
    return result;
  }

  var dqMin = getMarketLeadersStrictThreshold_('TFB_STRICT_INVEST_DQ_MIN', 80);
  var relMin = getMarketLeadersStrictThreshold_('TFB_STRICT_INVEST_RELIABILITY_MIN', 70);

  var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, Math.max(headers.length, sheet.getLastColumn())).getValues();

  for (var i = 0; i < values.length; i++) {
    var row = values[i];
    var rowNum = i + 2;
    var symbol = String(row[cols.symbol - 1] || '').trim();

    if (!symbol) continue;

    result.checkedRows++;

    var finalAction = String(row[cols.finalAction - 1] || '').trim().toUpperCase();
    if (finalAction !== 'INVEST') continue;

    result.investRows++;

    var tag = symbol + '@R' + rowNum;

    if (cols.price && marketLeadersIsBlank_(row[cols.price - 1])) {
      result.investWithBlankPrice.push(tag);
    }

    if (cols.roi3m) {
      var roi3m = marketLeadersNumber_(row[cols.roi3m - 1], null);
      if (roi3m !== null && roi3m <= 0) result.investWithNegativeRoi3m.push(tag + '=' + roi3m);
    }

    if (cols.roi12m) {
      var roi12m = marketLeadersNumber_(row[cols.roi12m - 1], null);
      if (roi12m !== null && roi12m <= 0) result.investWithNegativeRoi12m.push(tag + '=' + roi12m);
    }

    if (cols.risk) {
      var risk = String(row[cols.risk - 1] || '').trim().toUpperCase();
      if (risk === 'HIGH') result.investWithHighRisk.push(tag);
    }

    if (cols.conflict) {
      var conflict = marketLeadersBoolean_(row[cols.conflict - 1], false);
      if (conflict) result.investWithConflict.push(tag);
    }

    if (cols.blockReason) {
      var blockReason = String(row[cols.blockReason - 1] || '').trim();
      if (blockReason) result.investWithBlockReason.push(tag);
    }

    if (cols.dq) {
      var dq = marketLeadersNumber_(row[cols.dq - 1], null);
      if (dq !== null && dq < dqMin) result.investWithLowDataQuality.push(tag + '=' + dq);
    }

    if (cols.rel) {
      var rel = marketLeadersNumber_(row[cols.rel - 1], null);
      if (rel !== null && rel < relMin) result.investWithLowReliability.push(tag + '=' + rel);
    }
  }

  marketLeadersPushWarning_(result.warnings, 'INVEST with blank Current Price', result.investWithBlankPrice);
  marketLeadersPushWarning_(result.warnings, 'INVEST with Expected ROI 3M <= 0', result.investWithNegativeRoi3m);
  marketLeadersPushWarning_(result.warnings, 'INVEST with Expected ROI 12M <= 0', result.investWithNegativeRoi12m);
  marketLeadersPushWarning_(result.warnings, 'INVEST with HIGH risk', result.investWithHighRisk);
  marketLeadersPushWarning_(result.warnings, 'INVEST with Provider/Engine Conflict', result.investWithConflict);
  marketLeadersPushWarning_(result.warnings, 'INVEST with nonblank Block Reason', result.investWithBlockReason);
  marketLeadersPushWarning_(result.warnings, 'INVEST with Data Quality below ' + dqMin, result.investWithLowDataQuality);
  marketLeadersPushWarning_(result.warnings, 'INVEST with Forecast Reliability below ' + relMin, result.investWithLowReliability);

  result.ok = result.warnings.length === 0;
  return result;
}

function getMarketLeadersStrictApprovedRows_() {
  var sheet = ensureSheetExists_(MARKET_LEADERS_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);

  if (!headers.length || sheet.getLastRow() < 2) {
    return [];
  }

  var headerMap = getHeaderMapMarketLeaders_(headers);

  var cols = {
    symbol: marketLeadersResolveHeaderColumn_(headerMap, 'Symbol'),
    recommendation: marketLeadersResolveHeaderColumn_(headerMap, 'Recommendation'),
    finalAction: marketLeadersResolveHeaderColumn_(headerMap, 'Final Action'),
    investability: marketLeadersResolveHeaderColumn_(headerMap, 'Investability Status'),
    price: marketLeadersResolveHeaderColumn_(headerMap, 'Current Price'),
    dq: marketLeadersResolveHeaderColumn_(headerMap, 'Data Quality Score'),
    rel: marketLeadersResolveHeaderColumn_(headerMap, 'Forecast Reliability Score'),
    conflict: marketLeadersResolveHeaderColumn_(headerMap, 'Provider/Engine Conflict'),
    risk: marketLeadersResolveHeaderColumn_(headerMap, 'Risk Bucket'),
    roi3m: marketLeadersResolveHeaderColumn_(headerMap, 'Expected ROI 3M'),
    roi12m: marketLeadersResolveHeaderColumn_(headerMap, 'Expected ROI 12M'),
    blockReason: marketLeadersResolveHeaderColumn_(headerMap, 'Block Reason')
  };

  if (!cols.symbol || !cols.finalAction || !cols.investability) {
    return [];
  }

  var dqMin = getMarketLeadersStrictThreshold_('TFB_STRICT_INVEST_DQ_MIN', 80);
  var relMin = getMarketLeadersStrictThreshold_('TFB_STRICT_INVEST_RELIABILITY_MIN', 70);

  var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, Math.max(headers.length, sheet.getLastColumn())).getValues();
  var out = [];

  values.forEach(function (row, i) {
    var symbol = String(row[cols.symbol - 1] || '').trim();
    if (!symbol) return;

    var finalAction = String(row[cols.finalAction - 1] || '').trim().toUpperCase();
    var investability = String(row[cols.investability - 1] || '').trim().toUpperCase();
    var price = cols.price ? row[cols.price - 1] : '';
    var dq = cols.dq ? marketLeadersNumber_(row[cols.dq - 1], null) : null;
    var rel = cols.rel ? marketLeadersNumber_(row[cols.rel - 1], null) : null;
    var conflict = cols.conflict ? marketLeadersBoolean_(row[cols.conflict - 1], false) : false;
    var risk = cols.risk ? String(row[cols.risk - 1] || '').trim().toUpperCase() : '';
    var roi3m = cols.roi3m ? marketLeadersNumber_(row[cols.roi3m - 1], null) : null;
    var roi12m = cols.roi12m ? marketLeadersNumber_(row[cols.roi12m - 1], null) : null;
    var blockReason = cols.blockReason ? String(row[cols.blockReason - 1] || '').trim() : '';

    if (
      finalAction === 'INVEST' &&
      investability === 'INVESTABLE' &&
      !marketLeadersIsBlank_(price) &&
      dq !== null && dq >= dqMin &&
      rel !== null && rel >= relMin &&
      conflict === false &&
      risk !== 'HIGH' &&
      (roi3m === null || roi3m > 0) &&
      (roi12m === null || roi12m > 0) &&
      !blockReason
    ) {
      out.push({
        row: i + 2,
        symbol: symbol,
        recommendation: cols.recommendation ? String(row[cols.recommendation - 1] || '').trim() : '',
        finalAction: finalAction,
        investabilityStatus: investability,
        dataQualityScore: dq,
        forecastReliabilityScore: rel,
        expectedRoi3m: roi3m,
        expectedRoi12m: roi12m,
        riskBucket: risk
      });
    }
  });

  return out;
}

/* =========================================================
 * Helpers — symbols
 * ========================================================= */

function getMarketLeadersSymbols_() {
  var page = getPageConfig(MARKET_LEADERS_PAGE_);
  var symbols = getWorkingSymbolsForPage_(page.pageName);

  if (!symbols || !symbols.length) {
    symbols = getDefaultSymbols(page.pageName);
  }

  return marketLeadersNormalizeSymbols_(symbols);
}

function marketLeadersNormalizeSymbols_(symbols) {
  var safeInput = Array.isArray(symbols) ? symbols : [];

  return marketLeadersUniqueStrings_(
    safeInput
      .map(function (x) {
        return String(x || '').trim().toUpperCase();
      })
      .filter(function (x) {
        return !!x;
      })
  );
}

// Backward-compatible alias retained for any older ad-hoc call.
function normalizeSymbolsList_(symbols) {
  return marketLeadersNormalizeSymbols_(symbols);
}

/* =========================================================
 * Helpers — headers and layout
 * ========================================================= */

function getHeaderMapMarketLeaders_(headers) {
  var safeHeaders = Array.isArray(headers) ? headers : [];
  var map = {};

  for (var i = 0; i < safeHeaders.length; i++) {
    var raw = String(safeHeaders[i] == null ? '' : safeHeaders[i]);
    var trimmed = raw.trim();
    var normalized = marketLeadersNormalizeHeader_(raw);
    var col = i + 1;

    if (raw && map[raw] === undefined) {
      map[raw] = col;
    }

    if (trimmed && map[trimmed] === undefined) {
      map[trimmed] = col;
    }

    if (normalized && map[normalized] === undefined) {
      map[normalized] = col;
    }

    if (normalized && map[String(normalized).toLowerCase()] === undefined) {
      map[String(normalized).toLowerCase()] = col;
    }
  }

  return map;
}

function marketLeadersNormalizeHeader_(text) {
  if (text === null || text === undefined) return '';

  if (typeof _normalizeHeaderText_ === 'function') {
    return _normalizeHeaderText_(text);
  }

  var s = String(text);
  s = s.replace(/[\u200B\u200C\u200D\uFEFF]/g, '');
  s = s.replace(/[\s\u00A0\u1680\u2000-\u200A\u2028\u2029\u202F\u205F\u3000]+/g, ' ');
  return s.trim();
}

function marketLeadersResolveHeaderColumn_(headerMap, headerName) {
  if (!headerMap || !headerName) return null;

  var raw = String(headerName);
  var trimmed = raw.trim();
  var normalized = marketLeadersNormalizeHeader_(raw);

  return headerMap[raw] ||
         headerMap[trimmed] ||
         headerMap[normalized] ||
         headerMap[String(normalized).toLowerCase()] ||
         null;
}

function getMissingMarketLeadersBusinessHeaders_(headerMap) {
  var missing = [];

  MARKET_LEADERS_REQUIRED_BUSINESS_HEADERS_.forEach(function (h) {
    if (!marketLeadersResolveHeaderColumn_(headerMap, h)) {
      missing.push(h);
    }
  });

  return missing;
}

function getMissingMarketLeadersInvestmentGateHeaders_(headerMap) {
  var missing = [];

  MARKET_LEADERS_INVESTMENT_GATE_HEADERS_.forEach(function (h) {
    if (!marketLeadersResolveHeaderColumn_(headerMap, h)) {
      missing.push(h);
    }
  });

  return missing;
}

function getMarketLeadersInvestmentGateCoverage_(headerMap) {
  var present = [];
  var missing = [];

  MARKET_LEADERS_INVESTMENT_GATE_HEADERS_.forEach(function (h) {
    if (marketLeadersResolveHeaderColumn_(headerMap, h)) {
      present.push(h);
    } else {
      missing.push(h);
    }
  });

  return {
    total: MARKET_LEADERS_INVESTMENT_GATE_HEADERS_.length,
    presentCount: present.length,
    missingCount: missing.length,
    present: present,
    missing: missing,
    complete: missing.length === 0
  };
}

function inferMarketLeadersLayout_(headerMap) {
  var hasFinalAction = !!marketLeadersResolveHeaderColumn_(headerMap, 'Final Action');
  var hasBlockReason = !!marketLeadersResolveHeaderColumn_(headerMap, 'Block Reason');
  var hasDataQuality = !!marketLeadersResolveHeaderColumn_(headerMap, 'Data Quality Score');
  var hasForecastReliability = !!marketLeadersResolveHeaderColumn_(headerMap, 'Forecast Reliability Score');

  if (hasFinalAction && hasBlockReason && hasDataQuality && hasForecastReliability) {
    return 'full_115_investment_schema';
  }

  if (
    marketLeadersResolveHeaderColumn_(headerMap, 'Rank (Overall)') ||
    marketLeadersResolveHeaderColumn_(headerMap, 'Opportunity Score')
  ) {
    return 'partial_canonical_schema_missing_gate';
  }

  return 'unknown_or_minimal_schema';
}

function resolveMarketLeadersSortPlan_(page, headerMap) {
  var configured = page && page.sort ? page.sort.activeField : '';
  var configuredCol = configured ? marketLeadersResolveHeaderColumn_(headerMap, configured) : null;

  if (configured && configuredCol) {
    return {
      field: configured,
      column: configuredCol,
      ascending: !!(page.sort && page.sort.ascending)
    };
  }

  var rankCol = marketLeadersResolveHeaderColumn_(headerMap, 'Rank (Overall)');
  if (rankCol) {
    return {
      field: 'Rank (Overall)',
      column: rankCol,
      ascending: true
    };
  }

  var finalActionCol = marketLeadersResolveHeaderColumn_(headerMap, 'Final Action');
  if (finalActionCol) {
    return {
      field: 'Final Action',
      column: finalActionCol,
      ascending: true
    };
  }

  var opportunityCol = marketLeadersResolveHeaderColumn_(headerMap, 'Opportunity Score');
  if (opportunityCol) {
    return {
      field: 'Opportunity Score',
      column: opportunityCol,
      ascending: false
    };
  }

  var overallCol = marketLeadersResolveHeaderColumn_(headerMap, 'Overall Score');
  if (overallCol) {
    return {
      field: 'Overall Score',
      column: overallCol,
      ascending: false
    };
  }

  var roi12mCol = marketLeadersResolveHeaderColumn_(headerMap, 'Expected ROI 12M');
  if (roi12mCol) {
    return {
      field: 'Expected ROI 12M',
      column: roi12mCol,
      ascending: false
    };
  }

  return null;
}

function resolveMarketLeadersTargetRow_(sheet, symbolCol, optSymbol) {
  var lastRow = sheet.getLastRow();
  var targetSymbol = optSymbol ? String(optSymbol).trim().toUpperCase() : '';
  var targetRow = 0;
  var resolvedSymbol = '';

  if (targetSymbol) {
    var symbolValues = sheet.getRange(2, symbolCol, lastRow - 1, 1).getValues();

    for (var i = 0; i < symbolValues.length; i++) {
      if (String(symbolValues[i][0] || '').trim().toUpperCase() === targetSymbol) {
        targetRow = i + 2;
        resolvedSymbol = targetSymbol;
        break;
      }
    }

    if (!targetRow) {
      return {
        ok: false,
        message: 'Symbol "' + targetSymbol + '" not found in Market_Leaders.',
        requestedSymbol: targetSymbol
      };
    }
  } else {
    targetRow = 2;
    resolvedSymbol = String(sheet.getRange(2, symbolCol).getValue() || '').trim().toUpperCase();

    if (!resolvedSymbol) {
      return {
        ok: false,
        message: 'First data row has no Symbol value.'
      };
    }
  }

  return {
    ok: true,
    row: targetRow,
    symbol: resolvedSymbol
  };
}

/* =========================================================
 * Helpers — small utilities
 * ========================================================= */

function marketLeadersPushWarning_(warnings, label, items) {
  if (!items || !items.length) return;
  warnings.push(label + ': ' + items.slice(0, 15).join(', ') + (items.length > 15 ? ' ...(+' + (items.length - 15) + ')' : ''));
}

function marketLeadersIsBlank_(value) {
  return value === null || value === undefined || String(value).trim() === '';
}

function marketLeadersNumber_(value, fallback) {
  if (value === null || value === undefined || value === '') return fallback;

  if (typeof value === 'number') {
    if (!isFinite(value) || isNaN(value)) return fallback;
    return value;
  }

  var s = String(value).trim();
  if (!s) return fallback;

  var isPercent = s.indexOf('%') >= 0;
  s = s.replace(/,/g, '').replace(/%/g, '');

  var n = Number(s);
  if (!isFinite(n) || isNaN(n)) return fallback;

  if (isPercent) return n / 100;
  return n;
}

function marketLeadersBoolean_(value, fallback) {
  if (value === null || value === undefined || value === '') return !!fallback;
  if (typeof value === 'boolean') return value;

  var s = String(value).trim().toLowerCase();

  if (['1', 'true', 'yes', 'y', 'on', 't', 'enabled', 'enable'].indexOf(s) >= 0) return true;
  if (['0', 'false', 'no', 'n', 'off', 'f', 'disabled', 'disable'].indexOf(s) >= 0) return false;

  return !!fallback;
}

function marketLeadersUniqueStrings_(arr) {
  if (typeof uniqueStrings_ === 'function') {
    return uniqueStrings_(arr);
  }

  var seen = {};
  var out = [];
  var safe = Array.isArray(arr) ? arr : [];

  for (var i = 0; i < safe.length; i++) {
    var s = String(safe[i] || '').trim();
    if (!s) continue;
    if (seen[s]) continue;
    seen[s] = true;
    out.push(s);
  }

  return out;
}

function getMarketLeadersStrictThreshold_(propertyName, fallback) {
  try {
    var props = PropertiesService.getScriptProperties();
    var raw = props.getProperty(propertyName);
    if (raw === null || raw === undefined || raw === '') return fallback;
    var n = Number(raw);
    return isNaN(n) || !isFinite(n) ? fallback : n;
  } catch (e) {
    return fallback;
  }
}
