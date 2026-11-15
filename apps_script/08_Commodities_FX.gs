/**
 * 08_Commodities_FX.gs
 * Tadawul Tracker — Commodities & FX Page Module
 * Version: 1.3.0
 *
 * Aligned with — current 115/118 investment reliability contract
 * - 00_Config.gs        v1.12.1
 * - 01_Menu.gs          v2.9.0
 * - 03_Schema.gs        v2.5.0
 * - 04_Format.gs        v2.12.0
 * - 05_Refresh.gs       v1.11.2
 * - 06_Market_Leaders.gs v1.6.0
 * - 07_Global_Markets.gs v1.7.1
 * - 11_SpecialPages.gs  v1.6.0
 * - 99_Data_Audit.gs    v1.3.0
 * - 99_Diag.gs          v2.3.0
 *
 * v1.3.0 MAIN CHANGES
 * -------------------
 * 1. Updates Commodities_FX from the old v1.2.2 dependency chain
 *    to the current 115-column main-page contract.
 *
 * 2. Adds explicit investment-gate header tracking:
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
 * 3. Uses the current 05_Refresh validators when available:
 *      validateExpectedRefreshWidth_
 *      validatePostRefreshIntegrity_
 *    This keeps Commodities_FX aligned with the same validation layer
 *    used by Market_Leaders, Global_Markets, and Top_10_Investments.
 *
 * 4. Removes the duplicate shared dispatcher manageSymbolsForPage_().
 *    That dispatcher must live only in the central menu/special-pages
 *    layer to avoid Apps Script global-namespace overwrites. This file
 *    keeps only manageCommoditiesFxSymbols_().
 *
 * 5. Stops the fallback-warning helper from writing a visible panel
 *    into row 1. It delegates to appendGenericFallbackWarning_ when
 *    present, and otherwise writes only _Status/_Run_Log metadata.
 *
 * 6. Keeps the useful existing logic:
 *      - build / refresh / batch refresh wrappers
 *      - selected-row refresh wrapper
 *      - commodity/FX-specific sort fallback using Percent Change
 *      - symbol normalization
 *      - header diagnostics
 *
 * 7. Adds debugCommoditiesFxInvestmentGate_(symbol) for a direct
 *    row-level check of the current investment-gate fields.
 */

'use strict';

/* =========================================================
 * Top-level identifiers
 * ========================================================= */

var COMMODITIES_FX_PAGE_ = SHEET_NAMES.COMMODITIES_FX;
var COMMODITIES_FX_EXPECTED_COLUMNS_ = 115;

var COMMODITIES_FX_REQUIRED_BUSINESS_HEADERS_ = Object.freeze([
  'Symbol',
  'Name',
  'Asset Class',
  'Exchange',
  'Currency',
  'Country',
  'Current Price',
  'Price Change',
  'Percent Change',
  'Opportunity Score',
  'Overall Score',
  'Rank (Overall)',
  'Recommendation',
  'Recommendation Detail',
  'Data Provider',
  'Last Updated (Riyadh)',
  'Warnings'
]);

var COMMODITIES_FX_INVESTMENT_GATE_HEADERS_ = Object.freeze([
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

/* =========================================================
 * Public page module entry points
 * ========================================================= */

function buildCommoditiesFx_() {
  return buildPage_(COMMODITIES_FX_PAGE_);
}

function refreshCommoditiesFx_(options) {
  options = options || {};

  var symbols = normalizeCommoditiesFxSymbols_(
    options.symbols && options.symbols.length
      ? options.symbols
      : getCommoditiesFxSymbols_()
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

  var result = refreshPage_(COMMODITIES_FX_PAGE_, refreshOpts);
  result = appendCommoditiesFxFallbackWarning_(result);

  if (!result || result.paused || result.partial) {
    return result;
  }

  result = _commoditiesFxRunOptionalPostProcessors_(result, options);
  return result;
}

function refreshCommoditiesFxSelectedRow_(options) {
  options = options || {};
  var result = refreshSelectedRow_(COMMODITIES_FX_PAGE_, options);
  result = _commoditiesFxRunOptionalPostProcessors_(result, {
    skipValidation: false
  });
  return result;
}

function refreshCommoditiesFxInBatches_(options) {
  options = options || {};

  var symbols = normalizeCommoditiesFxSymbols_(
    options.symbols && options.symbols.length
      ? options.symbols
      : getCommoditiesFxSymbols_()
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

  if (options.forceHeaders === true) {
    batchOpts.forceHeaders = true;
  }

  if (options.forceHeaders === false) {
    batchOpts.forceHeaders = false;
  }

  if (options.legacyPath === true) {
    batchOpts.legacyPath = true;
  }

  var result = refreshPageInBatches_(COMMODITIES_FX_PAGE_, batchOpts);
  result = appendCommoditiesFxFallbackWarning_(result);

  if (!result || result.paused || result.partial) {
    return result;
  }

  result = _commoditiesFxRunOptionalPostProcessors_(result, options);
  return result;
}

function validateCommoditiesFx_() {
  var page = getPageConfig(COMMODITIES_FX_PAGE_);
  var sheet = ensureSheetExists_(COMMODITIES_FX_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var rowCount = Math.max(0, sheet.getLastRow() - 1);
  var headerMap = getHeaderMapCommoditiesFx_(headers);

  var expectedWidth = Number(page.expectedColumnCount || COMMODITIES_FX_EXPECTED_COLUMNS_);
  var missingBusinessHeaders = getMissingCommoditiesFxBusinessHeaders_(headerMap);
  var missingGateHeaders = getMissingCommoditiesFxInvestmentGateHeaders_(headerMap);
  var inferredLayout = inferCommoditiesFxLayout_(headerMap);

  var strictResult = null;
  var mappingResult = null;
  var widthValidation = null;
  var postRefreshValidation = null;

  try {
    strictResult = validatePage_(COMMODITIES_FX_PAGE_);
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
      pageName: strictResult.pageName || COMMODITIES_FX_PAGE_,
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
      widthValidation = validateExpectedRefreshWidth_(COMMODITIES_FX_PAGE_, sheet);
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
      postRefreshValidation = validatePostRefreshIntegrity_(COMMODITIES_FX_PAGE_, sheet, {
        expectedSymbolCount: getCommoditiesFxSymbols_().length
      });
    } catch (e2) {
      postRefreshValidation = {
        ok: false,
        warnings: ['Post-refresh validation failed: ' + (e2 && e2.message ? e2.message : String(e2))]
      };
    }
  } else {
    postRefreshValidation = validateCommoditiesFxGateLocally_(sheet, headers);
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

  warningsList = commoditiesFxUniqueStrings_(warningsList);

  var enrichedResult = {
    ok: ok,
    pageName: COMMODITIES_FX_PAGE_,
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
    gateCoverage: getCommoditiesFxInvestmentGateCoverage_(headerMap),
    strictApprovedRows: getCommoditiesFxStrictApprovedRows_(),
    validationMode: mappingResult ? 'header-map' : 'strict-positional'
  };

  logRun_(
    enrichedResult.ok ? 'INFO' : 'WARN',
    'validateCommoditiesFx',
    COMMODITIES_FX_PAGE_,
    enrichedResult.ok
      ? 'Commodities_FX validation passed as full 115-column investment page.'
      : 'Commodities_FX validation found issues.',
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

  writePageStatus_(COMMODITIES_FX_PAGE_, {
    status: enrichedResult.ok ? 'VALID' : 'WARN',
    message: enrichedResult.ok
      ? 'Commodities_FX validation passed - full 115-column investment page'
      : 'Commodities_FX validation issues found',
    endpoint: '',
    httpCode: '',
    rows: rowCount,
    columns: headers.length,
    durationMs: '',
    warnings: warningsList
  });

  return enrichedResult;
}

function sortCommoditiesFx_() {
  return withScriptLock_(function () {
    var page = getPageConfig(COMMODITIES_FX_PAGE_);
    var sheet = ensureSheetExists_(COMMODITIES_FX_PAGE_);
    var headers = readCurrentSheetHeaders_(sheet);
    var lastRow = sheet.getLastRow();

    if (lastRow <= 2 || !headers.length) {
      return {
        ok: true,
        pageName: COMMODITIES_FX_PAGE_,
        skipped: true,
        message: 'No data rows to sort.'
      };
    }

    var headerMap = getHeaderMapCommoditiesFx_(headers);
    var sortPlan = resolveCommoditiesFxSortPlan_(page, headerMap);

    if (!sortPlan || !sortPlan.column) {
      return {
        ok: true,
        pageName: COMMODITIES_FX_PAGE_,
        skipped: true,
        message: 'No usable Commodities_FX sort field found.'
      };
    }

    var expectedWidth = Number(page.expectedColumnCount || COMMODITIES_FX_EXPECTED_COLUMNS_);
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
      'sortCommoditiesFx',
      COMMODITIES_FX_PAGE_,
      'Commodities_FX sorted successfully.',
      {
        status: 'OK',
        sortField: sortPlan.field,
        ascending: sortPlan.ascending,
        sortRangeCols: sortRangeCols
      }
    );

    writePageStatus_(COMMODITIES_FX_PAGE_, {
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
      pageName: COMMODITIES_FX_PAGE_,
      sortedBy: sortPlan.field,
      ascending: sortPlan.ascending,
      sortRangeCols: sortRangeCols
    };
  });
}

function manageCommoditiesFxSymbols_() {
  var symbols = getCommoditiesFxSymbols_();
  var ui = SpreadsheetApp.getUi();

  ui.alert(
    'Commodities & FX Symbols',
    symbols.length ? symbols.join(', ') : 'No symbols configured.',
    ui.ButtonSet.OK
  );

  return {
    ok: true,
    pageName: COMMODITIES_FX_PAGE_,
    symbols: symbols
  };
}

/* =========================================================
 * Optional post-processors
 * ========================================================= */

function _commoditiesFxRunOptionalPostProcessors_(result, options) {
  options = options || {};
  result = result || {};

  if (options.skipValidation !== true) {
    try {
      result.commoditiesFxValidation = validateCommoditiesFx_();
    } catch (e) {
      result.warnings = (result.warnings || []).concat([
        'Commodities_FX validation failed: ' + (e && e.message ? e.message : String(e))
      ]);
    }
  }

  return result;
}

function appendCommoditiesFxFallbackWarning_(result) {
  if (typeof appendGenericFallbackWarning_ === 'function' && result) {
    return appendGenericFallbackWarning_(COMMODITIES_FX_PAGE_, result) || result;
  }

  if (!result || !result.endpointUsed) return result;

  var page = getPageConfig(COMMODITIES_FX_PAGE_);
  var primaryEndpoint = page.primaryEndpoint;

  if (result.endpointUsed === primaryEndpoint) return result;

  var sheet = ensureSheetExists_(COMMODITIES_FX_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var fallbackWarnings = commoditiesFxUniqueStrings_(
    (result.warnings || []).concat([
      'Fallback endpoint used: ' + result.endpointUsed
    ])
  );

  writePageStatus_(COMMODITIES_FX_PAGE_, {
    status: result.partial ? 'PARTIAL' : 'SUCCESS',
    message: 'Fallback endpoint used for Commodities_FX: ' + result.endpointUsed,
    endpoint: result.endpointUsed,
    httpCode: '',
    rows: result.rowCount || '',
    columns: headers.length || '',
    durationMs: '',
    warnings: fallbackWarnings
  });

  logRun_(
    'WARN',
    'refreshCommoditiesFxFallback',
    COMMODITIES_FX_PAGE_,
    'Fallback endpoint was used for Commodities_FX: ' + result.endpointUsed,
    {
      status: 'WARN',
      endpoint: result.endpointUsed,
      warnings: fallbackWarnings,
      primaryEndpoint: primaryEndpoint
    }
  );

  result.warnings = fallbackWarnings;
  result.usedFallback = true;
  result.fallbackEndpoint = result.endpointUsed;
  result.primaryEndpoint = primaryEndpoint;

  return result;
}

/* =========================================================
 * Header diagnostic / non-destructive merge entry points
 * ========================================================= */

function mergeCommoditiesFxHeaders_(options) {
  if (typeof mergeBackendHeadersIntoSheet_ !== 'function') {
    throw new Error(
      'mergeCommoditiesFxHeaders_ requires 03_Schema.gs v2.5.0 or later. '
      + 'Update 03_Schema.gs first, then re-run this command.'
    );
  }

  options = options || {};

  if (options.force !== false) {
    options.force = true;
  }

  return mergeBackendHeadersIntoSheet_(COMMODITIES_FX_PAGE_, options);
}

function proposeCommoditiesFxHeaderUpgrades_() {
  if (typeof proposeHeaderUpgrades_ !== 'function') {
    throw new Error(
      'proposeCommoditiesFxHeaderUpgrades_ requires 03_Schema.gs v2.5.0 or later.'
    );
  }

  return proposeHeaderUpgrades_(COMMODITIES_FX_PAGE_);
}

function diagnoseCommoditiesFx_() {
  var page = getPageConfig(COMMODITIES_FX_PAGE_);
  var sheet = ensureSheetExists_(COMMODITIES_FX_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var headerMap = getHeaderMapCommoditiesFx_(headers);

  var expectedWidth = Number(page.expectedColumnCount || COMMODITIES_FX_EXPECTED_COLUMNS_);
  var missingBusinessHeaders = getMissingCommoditiesFxBusinessHeaders_(headerMap);
  var missingGateHeaders = getMissingCommoditiesFxInvestmentGateHeaders_(headerMap);
  var symbols = getCommoditiesFxSymbols_();
  var sortPlan = resolveCommoditiesFxSortPlan_(page, headerMap);

  var coverage = null;
  if (typeof getMappedHeaderCoverage_ === 'function') {
    try {
      coverage = getMappedHeaderCoverage_(COMMODITIES_FX_PAGE_);
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
    pageName: COMMODITIES_FX_PAGE_,
    expectedColumnCount: expectedWidth,
    headerCount: headers.length,
    rowCount: Math.max(0, sheet.getLastRow() - 1),
    inferredLayout: inferCommoditiesFxLayout_(headerMap),
    missingBusinessHeaders: missingBusinessHeaders,
    missingGateHeaders: missingGateHeaders,
    gateCoverage: getCommoditiesFxInvestmentGateCoverage_(headerMap),
    strictApprovedRows: getCommoditiesFxStrictApprovedRows_(),
    symbolsCount: symbols.length,
    sortField: sortPlan ? sortPlan.field : '',
    sortAscending: sortPlan ? sortPlan.ascending : null,
    coverage: coverage
  };

  logRun_(
    ok ? 'INFO' : 'WARN',
    'diagnoseCommoditiesFx',
    COMMODITIES_FX_PAGE_,
    ok ? 'Commodities_FX diagnostic passed.' : 'Commodities_FX diagnostic flagged issues.',
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

function debugCommoditiesFxInvestmentGate_(optSymbol) {
  var sheet = ensureSheetExists_(COMMODITIES_FX_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var headerMap = getHeaderMapCommoditiesFx_(headers);
  var lastRow = sheet.getLastRow();

  if (lastRow < 2 || headers.length === 0) {
    var emptyMsg = 'Commodities_FX has no data — run buildCommoditiesFx_() first.';
    Logger.log('[v1.3.0 GATE DIAG] ' + emptyMsg);
    return { ok: false, message: emptyMsg };
  }

  var symbolCol = commoditiesFxResolveHeaderColumn_(headerMap, 'Symbol');
  if (!symbolCol) {
    var noSymMsg = 'Commodities_FX has no Symbol column.';
    Logger.log('[v1.3.0 GATE DIAG] ' + noSymMsg);
    return { ok: false, message: noSymMsg };
  }

  var target = resolveCommoditiesFxTargetRow_(sheet, symbolCol, optSymbol);
  if (!target.ok) {
    Logger.log('[v1.3.0 GATE DIAG] ' + target.message);
    return target;
  }

  var rowValues = sheet.getRange(target.row, 1, 1, Math.max(headers.length, sheet.getLastColumn())).getValues()[0];

  var fields = COMMODITIES_FX_INVESTMENT_GATE_HEADERS_.concat([
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
    var col = commoditiesFxResolveHeaderColumn_(headerMap, field);

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
    var blank = commoditiesFxIsBlank_(value);

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

  Logger.log('[v1.3.0 GATE DIAG] ' +
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

function validateCommoditiesFxGateLocally_(sheet, headers) {
  var result = {
    ok: true,
    pageName: COMMODITIES_FX_PAGE_,
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
    result.warnings.push('Commodities_FX has no data rows.');
    return result;
  }

  var headerMap = getHeaderMapCommoditiesFx_(headers);

  var cols = {
    symbol: commoditiesFxResolveHeaderColumn_(headerMap, 'Symbol'),
    price: commoditiesFxResolveHeaderColumn_(headerMap, 'Current Price'),
    finalAction: commoditiesFxResolveHeaderColumn_(headerMap, 'Final Action'),
    investability: commoditiesFxResolveHeaderColumn_(headerMap, 'Investability Status'),
    risk: commoditiesFxResolveHeaderColumn_(headerMap, 'Risk Bucket'),
    roi3m: commoditiesFxResolveHeaderColumn_(headerMap, 'Expected ROI 3M'),
    roi12m: commoditiesFxResolveHeaderColumn_(headerMap, 'Expected ROI 12M'),
    dq: commoditiesFxResolveHeaderColumn_(headerMap, 'Data Quality Score'),
    rel: commoditiesFxResolveHeaderColumn_(headerMap, 'Forecast Reliability Score'),
    conflict: commoditiesFxResolveHeaderColumn_(headerMap, 'Provider/Engine Conflict'),
    blockReason: commoditiesFxResolveHeaderColumn_(headerMap, 'Block Reason')
  };

  if (!cols.symbol || !cols.finalAction) {
    result.ok = false;
    result.warnings.push('Local gate validation cannot run because Symbol or Final Action column is missing.');
    return result;
  }

  var dqMin = getCommoditiesFxStrictThreshold_('TFB_STRICT_INVEST_DQ_MIN', 80);
  var relMin = getCommoditiesFxStrictThreshold_('TFB_STRICT_INVEST_RELIABILITY_MIN', 70);

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

    if (cols.price && commoditiesFxIsBlank_(row[cols.price - 1])) {
      result.investWithBlankPrice.push(tag);
    }

    if (cols.roi3m) {
      var roi3m = commoditiesFxNumber_(row[cols.roi3m - 1], null);
      if (roi3m !== null && roi3m <= 0) result.investWithNegativeRoi3m.push(tag + '=' + roi3m);
    }

    if (cols.roi12m) {
      var roi12m = commoditiesFxNumber_(row[cols.roi12m - 1], null);
      if (roi12m !== null && roi12m <= 0) result.investWithNegativeRoi12m.push(tag + '=' + roi12m);
    }

    if (cols.risk) {
      var risk = String(row[cols.risk - 1] || '').trim().toUpperCase();
      if (risk === 'HIGH') result.investWithHighRisk.push(tag);
    }

    if (cols.conflict) {
      var conflict = commoditiesFxBoolean_(row[cols.conflict - 1], false);
      if (conflict) result.investWithConflict.push(tag);
    }

    if (cols.blockReason) {
      var blockReason = String(row[cols.blockReason - 1] || '').trim();
      if (blockReason) result.investWithBlockReason.push(tag);
    }

    if (cols.dq) {
      var dq = commoditiesFxNumber_(row[cols.dq - 1], null);
      if (dq !== null && dq < dqMin) result.investWithLowDataQuality.push(tag + '=' + dq);
    }

    if (cols.rel) {
      var rel = commoditiesFxNumber_(row[cols.rel - 1], null);
      if (rel !== null && rel < relMin) result.investWithLowReliability.push(tag + '=' + rel);
    }
  }

  commoditiesFxPushWarning_(result.warnings, 'INVEST with blank Current Price', result.investWithBlankPrice);
  commoditiesFxPushWarning_(result.warnings, 'INVEST with Expected ROI 3M <= 0', result.investWithNegativeRoi3m);
  commoditiesFxPushWarning_(result.warnings, 'INVEST with Expected ROI 12M <= 0', result.investWithNegativeRoi12m);
  commoditiesFxPushWarning_(result.warnings, 'INVEST with HIGH risk', result.investWithHighRisk);
  commoditiesFxPushWarning_(result.warnings, 'INVEST with Provider/Engine Conflict', result.investWithConflict);
  commoditiesFxPushWarning_(result.warnings, 'INVEST with nonblank Block Reason', result.investWithBlockReason);
  commoditiesFxPushWarning_(result.warnings, 'INVEST with Data Quality below ' + dqMin, result.investWithLowDataQuality);
  commoditiesFxPushWarning_(result.warnings, 'INVEST with Forecast Reliability below ' + relMin, result.investWithLowReliability);

  result.ok = result.warnings.length === 0;
  return result;
}

function getCommoditiesFxStrictApprovedRows_() {
  var sheet = ensureSheetExists_(COMMODITIES_FX_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);

  if (!headers.length || sheet.getLastRow() < 2) {
    return [];
  }

  var headerMap = getHeaderMapCommoditiesFx_(headers);

  var cols = {
    symbol: commoditiesFxResolveHeaderColumn_(headerMap, 'Symbol'),
    recommendation: commoditiesFxResolveHeaderColumn_(headerMap, 'Recommendation'),
    finalAction: commoditiesFxResolveHeaderColumn_(headerMap, 'Final Action'),
    investability: commoditiesFxResolveHeaderColumn_(headerMap, 'Investability Status'),
    price: commoditiesFxResolveHeaderColumn_(headerMap, 'Current Price'),
    dq: commoditiesFxResolveHeaderColumn_(headerMap, 'Data Quality Score'),
    rel: commoditiesFxResolveHeaderColumn_(headerMap, 'Forecast Reliability Score'),
    conflict: commoditiesFxResolveHeaderColumn_(headerMap, 'Provider/Engine Conflict'),
    risk: commoditiesFxResolveHeaderColumn_(headerMap, 'Risk Bucket'),
    roi3m: commoditiesFxResolveHeaderColumn_(headerMap, 'Expected ROI 3M'),
    roi12m: commoditiesFxResolveHeaderColumn_(headerMap, 'Expected ROI 12M'),
    blockReason: commoditiesFxResolveHeaderColumn_(headerMap, 'Block Reason')
  };

  if (!cols.symbol || !cols.finalAction || !cols.investability) {
    return [];
  }

  var dqMin = getCommoditiesFxStrictThreshold_('TFB_STRICT_INVEST_DQ_MIN', 80);
  var relMin = getCommoditiesFxStrictThreshold_('TFB_STRICT_INVEST_RELIABILITY_MIN', 70);

  var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, Math.max(headers.length, sheet.getLastColumn())).getValues();
  var out = [];

  values.forEach(function (row, i) {
    var symbol = String(row[cols.symbol - 1] || '').trim();
    if (!symbol) return;

    var finalAction = String(row[cols.finalAction - 1] || '').trim().toUpperCase();
    var investability = String(row[cols.investability - 1] || '').trim().toUpperCase();
    var price = cols.price ? row[cols.price - 1] : '';
    var dq = cols.dq ? commoditiesFxNumber_(row[cols.dq - 1], null) : null;
    var rel = cols.rel ? commoditiesFxNumber_(row[cols.rel - 1], null) : null;
    var conflict = cols.conflict ? commoditiesFxBoolean_(row[cols.conflict - 1], false) : false;
    var risk = cols.risk ? String(row[cols.risk - 1] || '').trim().toUpperCase() : '';
    var roi3m = cols.roi3m ? commoditiesFxNumber_(row[cols.roi3m - 1], null) : null;
    var roi12m = cols.roi12m ? commoditiesFxNumber_(row[cols.roi12m - 1], null) : null;
    var blockReason = cols.blockReason ? String(row[cols.blockReason - 1] || '').trim() : '';

    if (
      finalAction === 'INVEST' &&
      investability === 'INVESTABLE' &&
      !commoditiesFxIsBlank_(price) &&
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

function getCommoditiesFxSymbols_() {
  var page = getPageConfig(COMMODITIES_FX_PAGE_);
  var symbols = getWorkingSymbolsForPage_(page.pageName);

  if (!symbols || !symbols.length) {
    symbols = getDefaultSymbols(page.pageName);
  }

  return normalizeCommoditiesFxSymbols_(symbols);
}

function normalizeCommoditiesFxSymbols_(symbols) {
  var safeInput = Array.isArray(symbols) ? symbols : [];

  return commoditiesFxUniqueStrings_(
    safeInput
      .map(function (x) {
        return String(x || '').trim().toUpperCase();
      })
      .filter(function (x) {
        return !!x;
      })
  );
}

/* =========================================================
 * Helpers — headers and layout
 * ========================================================= */

function getHeaderMapCommoditiesFx_(headers) {
  var safeHeaders = Array.isArray(headers) ? headers : [];
  var map = {};

  for (var i = 0; i < safeHeaders.length; i++) {
    var raw = String(safeHeaders[i] == null ? '' : safeHeaders[i]);
    var trimmed = raw.trim();
    var normalized = commoditiesFxNormalizeHeader_(raw);
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

function commoditiesFxNormalizeHeader_(text) {
  if (text === null || text === undefined) return '';

  if (typeof _normalizeHeaderText_ === 'function') {
    return _normalizeHeaderText_(text);
  }

  var s = String(text);
  s = s.replace(/[\u200B\u200C\u200D\uFEFF]/g, '');
  s = s.replace(/[\s\u00A0\u1680\u2000-\u200A\u2028\u2029\u202F\u205F\u3000]+/g, ' ');
  return s.trim();
}

function commoditiesFxResolveHeaderColumn_(headerMap, headerName) {
  if (!headerMap || !headerName) return null;

  var raw = String(headerName);
  var trimmed = raw.trim();
  var normalized = commoditiesFxNormalizeHeader_(raw);

  return headerMap[raw] ||
         headerMap[trimmed] ||
         headerMap[normalized] ||
         headerMap[String(normalized).toLowerCase()] ||
         null;
}

function getMissingCommoditiesFxBusinessHeaders_(headerMap) {
  var missing = [];

  COMMODITIES_FX_REQUIRED_BUSINESS_HEADERS_.forEach(function (h) {
    if (!commoditiesFxResolveHeaderColumn_(headerMap, h)) {
      missing.push(h);
    }
  });

  return missing;
}

function getMissingCommoditiesFxInvestmentGateHeaders_(headerMap) {
  var missing = [];

  COMMODITIES_FX_INVESTMENT_GATE_HEADERS_.forEach(function (h) {
    if (!commoditiesFxResolveHeaderColumn_(headerMap, h)) {
      missing.push(h);
    }
  });

  return missing;
}

function getCommoditiesFxInvestmentGateCoverage_(headerMap) {
  var present = [];
  var missing = [];

  COMMODITIES_FX_INVESTMENT_GATE_HEADERS_.forEach(function (h) {
    if (commoditiesFxResolveHeaderColumn_(headerMap, h)) {
      present.push(h);
    } else {
      missing.push(h);
    }
  });

  return {
    total: COMMODITIES_FX_INVESTMENT_GATE_HEADERS_.length,
    presentCount: present.length,
    missingCount: missing.length,
    present: present,
    missing: missing,
    complete: missing.length === 0
  };
}

function inferCommoditiesFxLayout_(headerMap) {
  var hasFinalAction = !!commoditiesFxResolveHeaderColumn_(headerMap, 'Final Action');
  var hasBlockReason = !!commoditiesFxResolveHeaderColumn_(headerMap, 'Block Reason');
  var hasDataQuality = !!commoditiesFxResolveHeaderColumn_(headerMap, 'Data Quality Score');
  var hasForecastReliability = !!commoditiesFxResolveHeaderColumn_(headerMap, 'Forecast Reliability Score');

  if (hasFinalAction && hasBlockReason && hasDataQuality && hasForecastReliability) {
    return 'full_115_investment_schema';
  }

  if (
    commoditiesFxResolveHeaderColumn_(headerMap, 'Rank (Overall)') ||
    commoditiesFxResolveHeaderColumn_(headerMap, 'Opportunity Score') ||
    commoditiesFxResolveHeaderColumn_(headerMap, 'Percent Change')
  ) {
    return 'partial_canonical_schema_missing_gate';
  }

  return 'unknown_or_minimal_schema';
}

function resolveCommoditiesFxSortPlan_(page, headerMap) {
  var configured = page && page.sort ? page.sort.activeField : '';
  var configuredCol = configured ? commoditiesFxResolveHeaderColumn_(headerMap, configured) : null;

  if (configured && configuredCol) {
    return {
      field: configured,
      column: configuredCol,
      ascending: !!(page.sort && page.sort.ascending)
    };
  }

  var rankCol = commoditiesFxResolveHeaderColumn_(headerMap, 'Rank (Overall)');
  if (rankCol) {
    return {
      field: 'Rank (Overall)',
      column: rankCol,
      ascending: true
    };
  }

  var opportunityCol = commoditiesFxResolveHeaderColumn_(headerMap, 'Opportunity Score');
  if (opportunityCol) {
    return {
      field: 'Opportunity Score',
      column: opportunityCol,
      ascending: false
    };
  }

  var overallCol = commoditiesFxResolveHeaderColumn_(headerMap, 'Overall Score');
  if (overallCol) {
    return {
      field: 'Overall Score',
      column: overallCol,
      ascending: false
    };
  }

  var roi12mCol = commoditiesFxResolveHeaderColumn_(headerMap, 'Expected ROI 12M');
  if (roi12mCol) {
    return {
      field: 'Expected ROI 12M',
      column: roi12mCol,
      ascending: false
    };
  }

  var pctChangeCol = commoditiesFxResolveHeaderColumn_(headerMap, 'Percent Change');
  if (pctChangeCol) {
    return {
      field: 'Percent Change',
      column: pctChangeCol,
      ascending: false
    };
  }

  return null;
}

function resolveCommoditiesFxTargetRow_(sheet, symbolCol, optSymbol) {
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
        message: 'Symbol "' + targetSymbol + '" not found in Commodities_FX.',
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

function commoditiesFxPushWarning_(warnings, label, items) {
  if (!items || !items.length) return;
  warnings.push(label + ': ' + items.slice(0, 15).join(', ') + (items.length > 15 ? ' ...(+' + (items.length - 15) + ')' : ''));
}

function commoditiesFxIsBlank_(value) {
  return value === null || value === undefined || String(value).trim() === '';
}

function commoditiesFxNumber_(value, fallback) {
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

function commoditiesFxBoolean_(value, fallback) {
  if (value === null || value === undefined || value === '') return !!fallback;
  if (typeof value === 'boolean') return value;

  var s = String(value).trim().toLowerCase();

  if (['1', 'true', 'yes', 'y', 'on', 't', 'enabled', 'enable'].indexOf(s) >= 0) return true;
  if (['0', 'false', 'no', 'n', 'off', 'f', 'disabled', 'disable'].indexOf(s) >= 0) return false;

  return !!fallback;
}

function commoditiesFxUniqueStrings_(arr) {
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

function getCommoditiesFxStrictThreshold_(propertyName, fallback) {
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
