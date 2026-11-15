/**
 * 07_Global_Markets.gs
 * Tadawul Tracker — Global Markets Page Module
 * Version: 1.7.1
 *
 * Aligned with:
 * - 00_Config.gs v1.12.1
 * - 03_Schema.gs v2.5.0
 * - 05_Refresh.gs v1.11.1
 * - Global_Markets = full 115-column investment-gate page
 *
 * v1.7.0 MAIN FIXES
 * -----------------
 * 1. Removes old production assumption that Global_Markets is a
 *    46-column Layout A page.
 *
 * 2. Treats Global_Markets as a full 115-column investment page.
 *
 * 3. Validates the new investment-gate fields:
 *      Data Quality Score
 *      Forecast Reliability Score
 *      Provider/Engine Conflict
 *      Conflict Type
 *      Final Decision Basis
 *      Investability Status
 *      Final Action
 *      Block Reason
 *
 * 4. Keeps the useful existing logic:
 *      - batch refresh wrapper
 *      - selected-row refresh wrapper
 *      - sheet-level Rank (Overall) recomputation
 *      - symbol normalization
 *      - diagnostics
 *      - fallback warning support
 *
 * 5. Keeps legacy view-token override only as optional compatibility.
 *    It will only run if the legacy view columns exist on the sheet.
 *
 * 6. Uses validatePostRefreshIntegrity_ from 05_Refresh.gs v1.11.1
 *    when available.
 *
 * v1.7.1 HOTFIX
 * --------------
 * - Removed duplicate global manageSymbolsForPage_ dispatcher from this file.
 *   The full shared dispatcher belongs in 11_SpecialPages.gs. Keeping a second
 *   definition here creates Apps Script global-namespace overwrite risk.
 * - getGlobalMarketsStrictApprovedRows_ now reads the configurable strict
 *   thresholds from Script Properties:
 *      TFB_STRICT_INVEST_DQ_MIN
 *      TFB_STRICT_INVEST_RELIABILITY_MIN
 *   with defaults of 80 / 70.
 */

'use strict';

/* =========================================================
 * Top-level identifiers
 * ========================================================= */

var GLOBAL_MARKETS_PAGE_ = SHEET_NAMES.GLOBAL_MARKETS;

var GLOBAL_MARKETS_EXPECTED_COLUMNS_ = 115;

var GLOBAL_MARKETS_REQUIRED_CORE_HEADERS_ = Object.freeze([
  'Symbol',
  'Name',
  'Asset Class',
  'Exchange',
  'Currency',
  'Country',
  'Sector',
  'Industry',
  'Current Price',
  '52W High',
  '52W Low',
  'Avg Volume 10D',
  'Avg Volume 30D',
  'Risk Score',
  'Risk Bucket',
  'Forecast Price 1M',
  'Forecast Price 3M',
  'Forecast Price 12M',
  'Expected ROI 1M',
  'Expected ROI 3M',
  'Expected ROI 12M',
  'Forecast Confidence',
  'Confidence Score',
  'Confidence Bucket',
  'Overall Score',
  'Opportunity Score',
  'Rank (Overall)',
  'Recommendation',
  'Recommendation Detail',
  'Recommendation Reason',
  'Position Size Hint',
  'Provider Rating',
  'Scoring Reco Source',
  'Scoring Schema Version',
  'Overall Score (Raw)',
  'Overall Penalty Factor',
  'Forecast Source',
  'Data Quality Score',
  'Forecast Reliability Score',
  'Provider/Engine Conflict',
  'Conflict Type',
  'Final Decision Basis',
  'Investability Status',
  'Final Action',
  'Block Reason',
  'Data Provider',
  'Last Updated (Riyadh)',
  'Warnings'
]);

var GLOBAL_MARKETS_INVESTMENT_GATE_HEADERS_ = Object.freeze([
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

var GLOBAL_MARKETS_LEGACY_VIEW_HEADERS_ = Object.freeze([
  'Fundamental View',
  'Technical View',
  'Risk View',
  'Value View'
]);

/* =========================================================
 * Public page module entry points
 * ========================================================= */

function buildGlobalMarkets_() {
  return buildPage_(GLOBAL_MARKETS_PAGE_);
}

function refreshGlobalMarkets_(options) {
  options = options || {};

  var symbols = normalizeGlobalMarketSymbols_(
    options.symbols && options.symbols.length
      ? options.symbols
      : getGlobalMarketsSymbols_()
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

  var result = refreshPage_(GLOBAL_MARKETS_PAGE_, refreshOpts);

  result = _globalMarketsAppendFallbackWarning_(result);

  if (!result || result.paused || result.partial) {
    return result;
  }

  result = _globalMarketsRunOptionalPostProcessors_(result, options);

  return result;
}

function refreshGlobalMarketsSelectedRow_(options) {
  options = options || {};
  var result = refreshSelectedRow_(GLOBAL_MARKETS_PAGE_, options);
  result = _globalMarketsRunOptionalPostProcessors_(result, {
    skipRankOverallPass: true,
    skipViewTokenOverride: true
  });
  return result;
}

function refreshGlobalMarketsInBatches_(options) {
  options = options || {};

  var symbols = normalizeGlobalMarketSymbols_(
    options.symbols && options.symbols.length
      ? options.symbols
      : getGlobalMarketsSymbols_()
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

  var result = refreshPageInBatches_(GLOBAL_MARKETS_PAGE_, batchOpts);

  result = _globalMarketsAppendFallbackWarning_(result);

  if (!result || result.paused || result.partial) {
    return result;
  }

  result = _globalMarketsRunOptionalPostProcessors_(result, options);

  return result;
}

function validateGlobalMarkets_() {
  var page = getPageConfig(GLOBAL_MARKETS_PAGE_);
  var sheet = ensureSheetExists_(GLOBAL_MARKETS_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var rowCount = Math.max(0, sheet.getLastRow() - 1);
  var headerMap = getHeaderMapGlobalMarkets_(headers);

  var expectedWidth = Number(page.expectedColumnCount || GLOBAL_MARKETS_EXPECTED_COLUMNS_);
  var missingCoreHeaders = getMissingGlobalMarketsCoreHeaders_(headerMap);
  var missingGateHeaders = getMissingGlobalMarketsInvestmentGateHeaders_(headerMap);
  var inferredLayout = inferGlobalMarketsLayout_(headerMap);

  var strictResult = null;
  var postRefreshValidation = null;
  var widthValidation = null;

  try {
    strictResult = validatePage_(GLOBAL_MARKETS_PAGE_);
  } catch (e) {
    strictResult = {
      ok: false,
      error: e && e.message ? e.message : String(e)
    };
  }

  if (typeof validateExpectedRefreshWidth_ === 'function') {
    try {
      widthValidation = validateExpectedRefreshWidth_(GLOBAL_MARKETS_PAGE_, sheet);
    } catch (e1) {
      widthValidation = {
        ok: false,
        warnings: ['Width validation failed: ' + (e1 && e1.message ? e1.message : String(e1))]
      };
    }
  } else {
    widthValidation = {
      ok: headers.length === expectedWidth,
      expected: expectedWidth,
      actual: headers.length,
      warnings: headers.length === expectedWidth
        ? []
        : ['Width mismatch: expected ' + expectedWidth + ', actual ' + headers.length]
    };
  }

  if (typeof validatePostRefreshIntegrity_ === 'function') {
    try {
      postRefreshValidation = validatePostRefreshIntegrity_(GLOBAL_MARKETS_PAGE_, sheet, {
        expectedSymbolCount: getGlobalMarketsSymbols_().length
      });
    } catch (e2) {
      postRefreshValidation = {
        ok: false,
        warnings: ['Post-refresh validation failed: ' + (e2 && e2.message ? e2.message : String(e2))]
      };
    }
  } else {
    postRefreshValidation = validateGlobalMarketsGateLocally_(sheet, headers);
  }

  var ok =
    headers.length === expectedWidth &&
    missingCoreHeaders.length === 0 &&
    missingGateHeaders.length === 0 &&
    (!strictResult || strictResult.ok !== false) &&
    (!widthValidation || widthValidation.ok !== false) &&
    (!postRefreshValidation || postRefreshValidation.ok !== false);

  var warnings = []
    .concat(missingCoreHeaders.map(function (h) { return 'Missing core header: ' + h; }))
    .concat(missingGateHeaders.map(function (h) { return 'Missing gate header: ' + h; }))
    .concat(widthValidation && widthValidation.warnings ? widthValidation.warnings : [])
    .concat(postRefreshValidation && postRefreshValidation.warnings ? postRefreshValidation.warnings : []);

  var report = {
    ok: ok,
    pageName: GLOBAL_MARKETS_PAGE_,
    expectedColumnCount: expectedWidth,
    actualHeadersCount: headers.length,
    rowCount: rowCount,
    inferredLayout: inferredLayout,
    missingCoreHeaders: missingCoreHeaders,
    missingGateHeaders: missingGateHeaders,
    strictResult: strictResult,
    widthValidation: widthValidation,
    postRefreshValidation: postRefreshValidation,
    gateCoverage: getGlobalMarketsInvestmentGateCoverage_(headerMap),
    strictApprovedRows: getGlobalMarketsStrictApprovedRows_()
  };

  logRun_(
    ok ? 'INFO' : 'WARN',
    'validateGlobalMarkets',
    GLOBAL_MARKETS_PAGE_,
    ok
      ? 'Global_Markets validation passed as full 115-column investment page.'
      : 'Global_Markets validation found issues.',
    {
      status: ok ? 'PASS' : 'WARN',
      details: report
    }
  );

  writePageStatus_(GLOBAL_MARKETS_PAGE_, {
    status: ok ? 'VALID' : 'WARN',
    message: ok
      ? 'Global_Markets validation passed - full 115-column investment page'
      : 'Global_Markets validation issues found',
    endpoint: '',
    httpCode: '',
    rows: rowCount,
    columns: headers.length,
    durationMs: '',
    warnings: uniqueStrings_(warnings)
  });

  return report;
}

function sortGlobalMarkets_() {
  return withScriptLock_(function () {
    var page = getPageConfig(GLOBAL_MARKETS_PAGE_);
    var sheet = ensureSheetExists_(GLOBAL_MARKETS_PAGE_);
    var headers = readCurrentSheetHeaders_(sheet);
    var lastRow = sheet.getLastRow();

    if (lastRow <= 2 || !headers.length) {
      return {
        ok: true,
        pageName: GLOBAL_MARKETS_PAGE_,
        skipped: true,
        message: 'No data rows to sort.'
      };
    }

    var headerMap = getHeaderMapGlobalMarkets_(headers);
    var sortPlan = resolveGlobalMarketsSortPlan_(page, headerMap);

    if (!sortPlan || !sortPlan.column) {
      return {
        ok: true,
        pageName: GLOBAL_MARKETS_PAGE_,
        skipped: true,
        message: 'No usable Global_Markets sort field found.'
      };
    }

    var expectedWidth = Number(page.expectedColumnCount || GLOBAL_MARKETS_EXPECTED_COLUMNS_);
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
      'sortGlobalMarkets',
      GLOBAL_MARKETS_PAGE_,
      'Global_Markets sorted successfully.',
      {
        status: 'OK',
        sortField: sortPlan.field,
        ascending: sortPlan.ascending,
        sortRangeCols: sortRangeCols
      }
    );

    writePageStatus_(GLOBAL_MARKETS_PAGE_, {
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
      pageName: GLOBAL_MARKETS_PAGE_,
      sortedBy: sortPlan.field,
      ascending: sortPlan.ascending,
      sortRangeCols: sortRangeCols
    };
  });
}

function manageGlobalMarketsSymbols_() {
  var symbols = getGlobalMarketsSymbols_();
  var ui = SpreadsheetApp.getUi();

  ui.alert(
    'Global Markets Symbols',
    symbols.length ? symbols.join(', ') : 'No symbols configured.',
    ui.ButtonSet.OK
  );

  return {
    ok: true,
    pageName: GLOBAL_MARKETS_PAGE_,
    symbols: symbols
  };
}

/* =========================================================
 * Optional compatibility post-processors
 * ========================================================= */

function _globalMarketsRunOptionalPostProcessors_(result, options) {
  options = options || {};
  result = result || {};

  if (options.skipViewTokenOverride !== true && _globalMarketsHasLegacyViewColumns_()) {
    result = _globalMarketsApplyLegacyViewTokens_(result);
  }

  if (options.skipRankOverallPass !== true) {
    result = _globalMarketsApplyRankOverall_(result);
  }

  try {
    result.globalMarketsValidation = validateGlobalMarkets_();
  } catch (e) {
    result.warnings = (result.warnings || []).concat([
      'Global_Markets validation failed: ' + (e && e.message ? e.message : String(e))
    ]);
  }

  return result;
}

function _globalMarketsAppendFallbackWarning_(result) {
  if (typeof appendGenericFallbackWarning_ === 'function' && result) {
    return appendGenericFallbackWarning_(GLOBAL_MARKETS_PAGE_, result) || result;
  }
  return result;
}

function _globalMarketsHasLegacyViewColumns_() {
  try {
    var sheet = ensureSheetExists_(GLOBAL_MARKETS_PAGE_);
    var headers = readCurrentSheetHeaders_(sheet);
    var headerMap = getHeaderMapGlobalMarkets_(headers);

    return GLOBAL_MARKETS_LEGACY_VIEW_HEADERS_.some(function (h) {
      return !!globalMarketsResolveHeaderColumn_(headerMap, h);
    });
  } catch (e) {
    return false;
  }
}

function _globalMarketsApplyLegacyViewTokens_(result) {
  if (typeof applyViewTokensAndRecommendationOverride_ !== 'function') {
    return result;
  }

  try {
    var viewResult = applyViewTokensAndRecommendationOverride_(GLOBAL_MARKETS_PAGE_);

    if (viewResult && !viewResult.skipped) {
      result.viewTokenOverride = {
        rowsParsed: viewResult.rowsParsed,
        viewsWritten: viewResult.viewsWritten,
        recommendationsOverridden: viewResult.recommendationsOverridden,
        breakdown: viewResult.breakdown,
        alignedFromDetailCount: viewResult.alignedFromDetailCount,
        alignedFromReasonCount: viewResult.alignedFromReasonCount,
        recDetailColumnPresent: viewResult.recDetailColumnPresent,
        sourceCounts: viewResult.sourceCounts
      };
    }
  } catch (e) {
    result.warnings = (result.warnings || []).concat([
      'Legacy view-token override failed: ' + (e && e.message ? e.message : 'unknown error')
    ]);

    logRun_(
      'WARN',
      'applyViewTokensAndRecommendationOverride',
      GLOBAL_MARKETS_PAGE_,
      'Legacy view-token helper threw — result preserved without override.',
      {
        status: 'WARN',
        details: {
          error: e && e.message ? e.message : String(e || '')
        }
      }
    );
  }

  return result;
}

function _globalMarketsApplyRankOverall_(result) {
  try {
    var rankResult = applyRankOverallSheet_(GLOBAL_MARKETS_PAGE_);

    if (rankResult && !rankResult.skipped) {
      result.rankOverallPass = {
        rowsRanked: rankResult.rowsRanked,
        rowsUnranked: rankResult.rowsUnranked,
        rowsChanged: rankResult.rowsChanged,
        rankSource: rankResult.rankSource
      };
    }
  } catch (e) {
    result.warnings = (result.warnings || []).concat([
      'Rank Overall pass failed: ' + (e && e.message ? e.message : 'unknown error')
    ]);

    logRun_(
      'WARN',
      'applyRankOverallSheet',
      GLOBAL_MARKETS_PAGE_,
      'Rank helper threw — result preserved without rank pass.',
      {
        status: 'WARN',
        details: {
          error: e && e.message ? e.message : String(e || '')
        }
      }
    );
  }

  return result;
}

/* =========================================================
 * Manual compatibility wrappers
 * ========================================================= */

function applyViewTokensGlobalMarkets_() {
  if (typeof applyViewTokensAndRecommendationOverride_ !== 'function') {
    throw new Error(
      'applyViewTokensGlobalMarkets_ requires 11_SpecialPages.gs with applyViewTokensAndRecommendationOverride_.'
    );
  }

  if (!_globalMarketsHasLegacyViewColumns_()) {
    return {
      ok: true,
      skipped: true,
      pageName: GLOBAL_MARKETS_PAGE_,
      message: 'Skipped. Global_Markets is now full 115-column schema and has no legacy view columns.'
    };
  }

  return applyViewTokensAndRecommendationOverride_(GLOBAL_MARKETS_PAGE_);
}

function applyRankOverallGlobalMarkets_() {
  return applyRankOverallSheet_(GLOBAL_MARKETS_PAGE_);
}

/* =========================================================
 * Sheet-level Rank (Overall) recomputation
 * ========================================================= */

function applyRankOverallSheet_(pageName) {
  return withScriptLock_(function () {
    var sheet = ensureSheetExists_(pageName);
    var headers = readCurrentSheetHeaders_(sheet);
    var lastRow = sheet.getLastRow();
    var headerMap = getHeaderMapGlobalMarkets_(headers);

    if (lastRow < 2 || !headers.length) {
      Logger.log('[v1.7.0 RANK DIAG] No data rows to rank. page=' + pageName);
      return {
        ok: true,
        skipped: true,
        pageName: pageName,
        message: 'No data rows to rank.',
        rowsRanked: 0,
        rowsUnranked: 0,
        rowsChanged: 0,
        rankSource: null
      };
    }

    var rankCol = globalMarketsResolveHeaderColumn_(headerMap, 'Rank (Overall)');
    var overallCol = globalMarketsResolveHeaderColumn_(headerMap, 'Overall Score');
    var oppCol = globalMarketsResolveHeaderColumn_(headerMap, 'Opportunity Score');

    if (!rankCol) {
      Logger.log('[v1.7.0 RANK DIAG] No Rank (Overall) column. page=' + pageName);
      return {
        ok: true,
        skipped: true,
        pageName: pageName,
        message: 'Sheet has no Rank (Overall) column.',
        rowsRanked: 0,
        rowsUnranked: 0,
        rowsChanged: 0,
        rankSource: null
      };
    }

    if (!overallCol && !oppCol) {
      Logger.log('[v1.7.0 RANK DIAG] No Overall Score or Opportunity Score. page=' + pageName);
      return {
        ok: true,
        skipped: true,
        pageName: pageName,
        message: 'Sheet has no Overall Score or Opportunity Score column.',
        rowsRanked: 0,
        rowsUnranked: 0,
        rowsChanged: 0,
        rankSource: null
      };
    }

    var dataRows = lastRow - 1;
    var primarySource = overallCol ? 'overall_score' : 'opportunity_score';

    var primaryValues = overallCol
      ? sheet.getRange(2, overallCol, dataRows, 1).getValues()
      : sheet.getRange(2, oppCol, dataRows, 1).getValues();

    var fallbackValues = overallCol && oppCol
      ? sheet.getRange(2, oppCol, dataRows, 1).getValues()
      : null;

    var existingRanks = sheet.getRange(2, rankCol, dataRows, 1).getValues();

    var scored = [];
    var unrankedRows = 0;

    for (var i = 0; i < dataRows; i++) {
      var rowNum = i + 2;
      var score = _coerceRankScoreGlobalMarkets_(primaryValues[i][0]);

      if (score === null && fallbackValues) {
        score = _coerceRankScoreGlobalMarkets_(fallbackValues[i][0]);
      }

      if (score !== null) {
        scored.push({
          row: rowNum,
          score: score
        });
      } else {
        unrankedRows++;
      }
    }

    scored.sort(function (a, b) {
      if (b.score !== a.score) return b.score - a.score;
      return a.row - b.row;
    });

    var rankByRow = {};
    for (var r = 0; r < scored.length; r++) {
      rankByRow[scored[r].row] = r + 1;
    }

    var newRankColumn = [];
    var changed = 0;

    for (var rr = 0; rr < dataRows; rr++) {
      var thisRowNum = rr + 2;
      var newRank = rankByRow[thisRowNum];
      var existingRank = existingRanks[rr][0];

      if (newRank !== undefined) {
        newRankColumn.push([newRank]);

        var existingNum = existingRank === '' || existingRank === null || existingRank === undefined
          ? null
          : Number(existingRank);

        if (existingNum !== newRank) {
          changed++;
        }
      } else {
        newRankColumn.push([
          existingRank === undefined || existingRank === null ? '' : existingRank
        ]);
      }
    }

    sheet.getRange(2, rankCol, dataRows, 1).setValues(newRankColumn);

    var verdict = scored.length === dataRows
      ? 'OK_ALL_RANKED'
      : (scored.length === 0 ? 'NONE_RANKABLE' : 'PARTIAL_RANKED');

    Logger.log(
      '[v1.7.0 RANK DIAG] ' + verdict +
      ' page=' + pageName +
      ' rows-ranked=' + scored.length +
      ' rows-unranked=' + unrankedRows +
      ' rows-changed=' + changed +
      ' rank-source=' + primarySource +
      (fallbackValues ? ' fallback=opportunity_score' : '')
    );

    logRun_(
      'INFO',
      'applyRankOverallSheet',
      pageName,
      'Rank (Overall) sheet-level pass: ' + verdict,
      {
        status: 'OK',
        details: {
          verdict: verdict,
          rowsRanked: scored.length,
          rowsUnranked: unrankedRows,
          rowsChanged: changed,
          rankSource: primarySource,
          fallbackUsed: !!fallbackValues
        }
      }
    );

    return {
      ok: true,
      skipped: false,
      pageName: pageName,
      verdict: verdict,
      rowsRanked: scored.length,
      rowsUnranked: unrankedRows,
      rowsChanged: changed,
      rankSource: primarySource
    };
  });
}

function _coerceRankScoreGlobalMarkets_(value) {
  if (value === null || value === undefined || value === '') return null;

  if (typeof value === 'number') {
    if (!isFinite(value) || isNaN(value)) return null;
    return value;
  }

  var s = String(value).trim();
  if (!s) return null;

  s = s.replace(/,/g, '').replace(/%/g, '');

  var n = Number(s);
  if (isNaN(n) || !isFinite(n)) return null;

  return n;
}

/* =========================================================
 * Header diagnostic / merge entry points
 * ========================================================= */

function mergeGlobalMarketsHeaders_(options) {
  if (typeof mergeBackendHeadersIntoSheet_ !== 'function') {
    throw new Error(
      'mergeGlobalMarketsHeaders_ requires 03_Schema.gs v2.5.0 or later.'
    );
  }

  options = options || {};

  if (options.force !== false) {
    options.force = true;
  }

  return mergeBackendHeadersIntoSheet_(GLOBAL_MARKETS_PAGE_, options);
}

function proposeGlobalMarketsHeaderUpgrades_() {
  if (typeof proposeHeaderUpgrades_ !== 'function') {
    throw new Error(
      'proposeGlobalMarketsHeaderUpgrades_ requires 03_Schema.gs v2.5.0 or later.'
    );
  }

  return proposeHeaderUpgrades_(GLOBAL_MARKETS_PAGE_);
}

function diagnoseGlobalMarkets_() {
  var page = getPageConfig(GLOBAL_MARKETS_PAGE_);
  var sheet = ensureSheetExists_(GLOBAL_MARKETS_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var headerMap = getHeaderMapGlobalMarkets_(headers);

  var coverage = null;
  if (typeof getMappedHeaderCoverage_ === 'function') {
    try {
      coverage = getMappedHeaderCoverage_(GLOBAL_MARKETS_PAGE_);
    } catch (e) {
      coverage = null;
    }
  }

  var report = {
    ok: true,
    pageName: GLOBAL_MARKETS_PAGE_,
    expectedColumnCount: Number(page.expectedColumnCount || GLOBAL_MARKETS_EXPECTED_COLUMNS_),
    headerCount: headers.length,
    rowCount: Math.max(0, sheet.getLastRow() - 1),
    inferredLayout: inferGlobalMarketsLayout_(headerMap),
    missingCoreHeaders: getMissingGlobalMarketsCoreHeaders_(headerMap),
    missingGateHeaders: getMissingGlobalMarketsInvestmentGateHeaders_(headerMap),
    gateCoverage: getGlobalMarketsInvestmentGateCoverage_(headerMap),
    strictApprovedRows: getGlobalMarketsStrictApprovedRows_(),
    coverage: coverage
  };

  report.ok =
    report.headerCount === report.expectedColumnCount &&
    report.missingCoreHeaders.length === 0 &&
    report.missingGateHeaders.length === 0 &&
    (!coverage || coverage.unmapped === 0);

  logRun_(
    report.ok ? 'INFO' : 'WARN',
    'diagnoseGlobalMarkets',
    GLOBAL_MARKETS_PAGE_,
    report.ok
      ? 'Global_Markets diagnostic passed.'
      : 'Global_Markets diagnostic flagged issues.',
    {
      status: report.ok ? 'OK' : 'WARN',
      details: report
    }
  );

  return report;
}

/**
 * Backward-compatible diagnostic name.
 * In v1.7.0 this no longer checks old Layout A view fields only.
 * It now checks investment-gate fields for one symbol.
 */
function debugGlobalMarketsBlankCells_(optSymbol) {
  return debugGlobalMarketsInvestmentGate_(optSymbol);
}

function debugGlobalMarketsInvestmentGate_(optSymbol) {
  var sheet = ensureSheetExists_(GLOBAL_MARKETS_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);
  var headerMap = getHeaderMapGlobalMarkets_(headers);
  var lastRow = sheet.getLastRow();

  if (lastRow < 2 || headers.length === 0) {
    var emptyMsg = 'Global_Markets has no data — run buildGlobalMarkets_() first.';
    Logger.log('[v1.7.0 GATE DIAG] ' + emptyMsg);
    return {
      ok: false,
      message: emptyMsg
    };
  }

  var symbolCol = globalMarketsResolveHeaderColumn_(headerMap, 'Symbol');

  if (!symbolCol) {
    var noSymMsg = 'Global_Markets has no Symbol column.';
    Logger.log('[v1.7.0 GATE DIAG] ' + noSymMsg);
    return {
      ok: false,
      message: noSymMsg
    };
  }

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
      var notFoundMsg = 'Symbol "' + targetSymbol + '" not found in Global_Markets.';
      Logger.log('[v1.7.0 GATE DIAG] ' + notFoundMsg);
      return {
        ok: false,
        message: notFoundMsg,
        requestedSymbol: targetSymbol
      };
    }
  } else {
    targetRow = 2;
    resolvedSymbol = String(sheet.getRange(2, symbolCol).getValue() || '').trim().toUpperCase();

    if (!resolvedSymbol) {
      var noFirstMsg = 'First data row has no Symbol value.';
      Logger.log('[v1.7.0 GATE DIAG] ' + noFirstMsg);
      return {
        ok: false,
        message: noFirstMsg
      };
    }
  }

  var rowValues = sheet.getRange(targetRow, 1, 1, Math.max(headers.length, sheet.getLastColumn())).getValues()[0];

  var fields = GLOBAL_MARKETS_INVESTMENT_GATE_HEADERS_.concat([
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
    var col = globalMarketsResolveHeaderColumn_(headerMap, field);

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
    var blank = value === null || value === undefined || String(value).trim() === '';

    if (blank) {
      blankFields.push(field);
    }

    fieldReport[field] = {
      present: true,
      blank: blank,
      value: value
    };
  });

  var ok = missingColumns.length === 0 && blankFields.indexOf('Final Action') === -1 && blankFields.indexOf('Investability Status') === -1;

  Logger.log(
    '[v1.7.0 GATE DIAG] ' +
    (ok ? 'OK' : 'WARN') +
    ' symbol=' + resolvedSymbol +
    ' row=' + targetRow +
    ' missingColumns=' + missingColumns.length +
    ' blankFields=' + blankFields.length
  );

  return {
    ok: ok,
    symbol: resolvedSymbol,
    row: targetRow,
    missingColumns: missingColumns,
    blankFields: blankFields,
    fields: fieldReport
  };
}

/* =========================================================
 * Shared symbol manager hook
 * =========================================================
 *
 * v1.7.1: Intentionally no manageSymbolsForPage_ definition here.
 * The complete global dispatcher is owned by 11_SpecialPages.gs to avoid
 * Apps Script global namespace overwrite risk.
 */

/* =========================================================
 * Helpers — symbols
 * ========================================================= */

function getGlobalMarketsSymbols_() {
  var page = getPageConfig(GLOBAL_MARKETS_PAGE_);
  var symbols = getWorkingSymbolsForPage_(page.pageName);

  if (!symbols || !symbols.length) {
    symbols = getDefaultSymbols(page.pageName);
  }

  return normalizeGlobalMarketSymbols_(symbols);
}

function normalizeGlobalMarketSymbols_(symbols) {
  var safeInput = Array.isArray(symbols) ? symbols : [];

  return uniqueStrings_(
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

function getHeaderMapGlobalMarkets_(headers) {
  var safeHeaders = Array.isArray(headers) ? headers : [];
  var map = {};

  for (var i = 0; i < safeHeaders.length; i++) {
    var raw = String(safeHeaders[i] == null ? '' : safeHeaders[i]);
    var trimmed = raw.trim();
    var normalized = globalMarketsNormalizeHeader_(raw);

    if (raw) {
      map[raw] = i + 1;
    }

    if (trimmed) {
      map[trimmed] = i + 1;
    }

    if (normalized) {
      map[normalized] = i + 1;
      map[normalized.toLowerCase()] = i + 1;
    }
  }

  return map;
}

function globalMarketsNormalizeHeader_(text) {
  if (text === null || text === undefined) return '';

  if (typeof _normalizeHeaderText_ === 'function') {
    return _normalizeHeaderText_(text);
  }

  var s = String(text);
  s = s.replace(/[\u200B\u200C\u200D\uFEFF]/g, '');
  s = s.replace(/[\s\u00A0\u1680\u2000-\u200A\u2028\u2029\u202F\u205F\u3000]+/g, ' ');
  return s.trim();
}

function globalMarketsResolveHeaderColumn_(headerMap, headerName) {
  if (!headerMap || !headerName) return null;

  var raw = String(headerName);
  var trimmed = raw.trim();
  var normalized = globalMarketsNormalizeHeader_(raw);

  return headerMap[raw] ||
    headerMap[trimmed] ||
    headerMap[normalized] ||
    headerMap[String(normalized).toLowerCase()] ||
    null;
}

function getMissingGlobalMarketsCoreHeaders_(headerMap) {
  var missing = [];

  GLOBAL_MARKETS_REQUIRED_CORE_HEADERS_.forEach(function (h) {
    if (!globalMarketsResolveHeaderColumn_(headerMap, h)) {
      missing.push(h);
    }
  });

  return missing;
}

function getMissingGlobalMarketsInvestmentGateHeaders_(headerMap) {
  var missing = [];

  GLOBAL_MARKETS_INVESTMENT_GATE_HEADERS_.forEach(function (h) {
    if (!globalMarketsResolveHeaderColumn_(headerMap, h)) {
      missing.push(h);
    }
  });

  return missing;
}

function getGlobalMarketsInvestmentGateCoverage_(headerMap) {
  var present = [];
  var missing = [];

  GLOBAL_MARKETS_INVESTMENT_GATE_HEADERS_.forEach(function (h) {
    if (globalMarketsResolveHeaderColumn_(headerMap, h)) {
      present.push(h);
    } else {
      missing.push(h);
    }
  });

  return {
    total: GLOBAL_MARKETS_INVESTMENT_GATE_HEADERS_.length,
    presentCount: present.length,
    missingCount: missing.length,
    present: present,
    missing: missing,
    complete: missing.length === 0
  };
}

function inferGlobalMarketsLayout_(headerMap) {
  var hasFinalAction = !!globalMarketsResolveHeaderColumn_(headerMap, 'Final Action');
  var hasBlockReason = !!globalMarketsResolveHeaderColumn_(headerMap, 'Block Reason');
  var hasDataQuality = !!globalMarketsResolveHeaderColumn_(headerMap, 'Data Quality Score');
  var hasForecastReliability = !!globalMarketsResolveHeaderColumn_(headerMap, 'Forecast Reliability Score');
  var hasLegacyViews = GLOBAL_MARKETS_LEGACY_VIEW_HEADERS_.some(function (h) {
    return !!globalMarketsResolveHeaderColumn_(headerMap, h);
  });

  if (hasFinalAction && hasBlockReason && hasDataQuality && hasForecastReliability) {
    return 'full_115_investment_schema';
  }

  if (hasLegacyViews) {
    return 'legacy_layout_a_or_mixed_schema';
  }

  if (
    globalMarketsResolveHeaderColumn_(headerMap, 'Rank (Overall)') ||
    globalMarketsResolveHeaderColumn_(headerMap, 'Opportunity Score')
  ) {
    return 'partial_canonical_schema_missing_gate';
  }

  return 'unknown_or_minimal_schema';
}

function resolveGlobalMarketsSortPlan_(page, headerMap) {
  var configured = page && page.sort ? page.sort.activeField : '';
  var configuredCol = configured ? globalMarketsResolveHeaderColumn_(headerMap, configured) : null;

  if (configured && configuredCol) {
    return {
      field: configured,
      column: configuredCol,
      ascending: !!(page.sort && page.sort.ascending)
    };
  }

  var rankCol = globalMarketsResolveHeaderColumn_(headerMap, 'Rank (Overall)');
  if (rankCol) {
    return {
      field: 'Rank (Overall)',
      column: rankCol,
      ascending: true
    };
  }

  var finalActionCol = globalMarketsResolveHeaderColumn_(headerMap, 'Final Action');
  if (finalActionCol) {
    return {
      field: 'Final Action',
      column: finalActionCol,
      ascending: true
    };
  }

  var overallCol = globalMarketsResolveHeaderColumn_(headerMap, 'Overall Score');
  if (overallCol) {
    return {
      field: 'Overall Score',
      column: overallCol,
      ascending: false
    };
  }

  var opportunityCol = globalMarketsResolveHeaderColumn_(headerMap, 'Opportunity Score');
  if (opportunityCol) {
    return {
      field: 'Opportunity Score',
      column: opportunityCol,
      ascending: false
    };
  }

  var roi12mCol = globalMarketsResolveHeaderColumn_(headerMap, 'Expected ROI 12M');
  if (roi12mCol) {
    return {
      field: 'Expected ROI 12M',
      column: roi12mCol,
      ascending: false
    };
  }

  return null;
}

/* =========================================================
 * Helpers — local gate audit
 * ========================================================= */

function validateGlobalMarketsGateLocally_(sheet, headers) {
  var result = {
    ok: true,
    pageName: GLOBAL_MARKETS_PAGE_,
    checkedRows: 0,
    investRows: 0,
    warnings: [],
    investWithBlankPrice: [],
    investWithNegativeRoi3m: [],
    investWithNegativeRoi12m: [],
    investWithHighRisk: []
  };

  headers = headers || readCurrentSheetHeaders_(sheet);

  if (!headers.length || sheet.getLastRow() < 2) {
    result.ok = false;
    result.warnings.push('Global_Markets has no data rows.');
    return result;
  }

  var headerMap = getHeaderMapGlobalMarkets_(headers);

  var cols = {
    symbol: globalMarketsResolveHeaderColumn_(headerMap, 'Symbol'),
    price: globalMarketsResolveHeaderColumn_(headerMap, 'Current Price'),
    finalAction: globalMarketsResolveHeaderColumn_(headerMap, 'Final Action'),
    risk: globalMarketsResolveHeaderColumn_(headerMap, 'Risk Bucket'),
    roi3m: globalMarketsResolveHeaderColumn_(headerMap, 'Expected ROI 3M'),
    roi12m: globalMarketsResolveHeaderColumn_(headerMap, 'Expected ROI 12M')
  };

  if (!cols.symbol || !cols.finalAction) {
    result.ok = false;
    result.warnings.push('Local gate validation cannot run because Symbol or Final Action column is missing.');
    return result;
  }

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

    if (cols.price && _globalMarketsBlank_(row[cols.price - 1])) {
      result.investWithBlankPrice.push(tag);
    }

    if (cols.roi3m) {
      var roi3m = _globalMarketsNumber_(row[cols.roi3m - 1], null);
      if (roi3m !== null && roi3m <= 0) result.investWithNegativeRoi3m.push(tag + '=' + roi3m);
    }

    if (cols.roi12m) {
      var roi12m = _globalMarketsNumber_(row[cols.roi12m - 1], null);
      if (roi12m !== null && roi12m <= 0) result.investWithNegativeRoi12m.push(tag + '=' + roi12m);
    }

    if (cols.risk) {
      var risk = String(row[cols.risk - 1] || '').trim().toUpperCase();
      if (risk === 'HIGH') result.investWithHighRisk.push(tag);
    }
  }

  _globalMarketsPushWarning_(result.warnings, 'INVEST with blank Current Price', result.investWithBlankPrice);
  _globalMarketsPushWarning_(result.warnings, 'INVEST with Expected ROI 3M <= 0', result.investWithNegativeRoi3m);
  _globalMarketsPushWarning_(result.warnings, 'INVEST with Expected ROI 12M <= 0', result.investWithNegativeRoi12m);
  _globalMarketsPushWarning_(result.warnings, 'INVEST with HIGH risk', result.investWithHighRisk);

  result.ok = result.warnings.length === 0;
  return result;
}

function getGlobalMarketsStrictApprovedRows_() {
  var sheet = ensureSheetExists_(GLOBAL_MARKETS_PAGE_);
  var headers = readCurrentSheetHeaders_(sheet);

  if (!headers.length || sheet.getLastRow() < 2) {
    return [];
  }

  var headerMap = getHeaderMapGlobalMarkets_(headers);

  var cols = {
    symbol: globalMarketsResolveHeaderColumn_(headerMap, 'Symbol'),
    recommendation: globalMarketsResolveHeaderColumn_(headerMap, 'Recommendation'),
    finalAction: globalMarketsResolveHeaderColumn_(headerMap, 'Final Action'),
    investability: globalMarketsResolveHeaderColumn_(headerMap, 'Investability Status'),
    price: globalMarketsResolveHeaderColumn_(headerMap, 'Current Price'),
    dq: globalMarketsResolveHeaderColumn_(headerMap, 'Data Quality Score'),
    rel: globalMarketsResolveHeaderColumn_(headerMap, 'Forecast Reliability Score'),
    conflict: globalMarketsResolveHeaderColumn_(headerMap, 'Provider/Engine Conflict'),
    risk: globalMarketsResolveHeaderColumn_(headerMap, 'Risk Bucket'),
    roi3m: globalMarketsResolveHeaderColumn_(headerMap, 'Expected ROI 3M'),
    roi12m: globalMarketsResolveHeaderColumn_(headerMap, 'Expected ROI 12M'),
    blockReason: globalMarketsResolveHeaderColumn_(headerMap, 'Block Reason')
  };

  if (!cols.symbol || !cols.finalAction || !cols.investability) {
    return [];
  }

  var strictSettings = _getGlobalMarketsStrictSettings_();
  var dqMin = strictSettings.dqMin;
  var reliabilityMin = strictSettings.reliabilityMin;

  var values = sheet.getRange(2, 1, sheet.getLastRow() - 1, Math.max(headers.length, sheet.getLastColumn())).getValues();
  var out = [];

  values.forEach(function (row, i) {
    var symbol = String(row[cols.symbol - 1] || '').trim();
    if (!symbol) return;

    var finalAction = String(row[cols.finalAction - 1] || '').trim().toUpperCase();
    var investability = String(row[cols.investability - 1] || '').trim().toUpperCase();
    var price = cols.price ? row[cols.price - 1] : '';
    var dq = cols.dq ? _globalMarketsNumber_(row[cols.dq - 1], null) : null;
    var rel = cols.rel ? _globalMarketsNumber_(row[cols.rel - 1], null) : null;
    var conflict = cols.conflict ? _globalMarketsBoolean_(row[cols.conflict - 1], false) : false;
    var risk = cols.risk ? String(row[cols.risk - 1] || '').trim().toUpperCase() : '';
    var roi3m = cols.roi3m ? _globalMarketsNumber_(row[cols.roi3m - 1], null) : null;
    var roi12m = cols.roi12m ? _globalMarketsNumber_(row[cols.roi12m - 1], null) : null;
    var blockReason = cols.blockReason ? String(row[cols.blockReason - 1] || '').trim() : '';

    if (
      finalAction === 'INVEST' &&
      investability === 'INVESTABLE' &&
      !_globalMarketsBlank_(price) &&
      dq !== null && dq >= dqMin &&
      rel !== null && rel >= reliabilityMin &&
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

function _getGlobalMarketsStrictSettings_() {
  var dqMin = 80;
  var reliabilityMin = 70;

  try {
    var props = PropertiesService.getScriptProperties();
    dqMin = _globalMarketsNumber_(props.getProperty('TFB_STRICT_INVEST_DQ_MIN'), 80);
    reliabilityMin = _globalMarketsNumber_(props.getProperty('TFB_STRICT_INVEST_RELIABILITY_MIN'), 70);
  } catch (e) {
    dqMin = 80;
    reliabilityMin = 70;
  }

  if (dqMin === null || dqMin === undefined || isNaN(dqMin) || !isFinite(dqMin)) {
    dqMin = 80;
  }

  if (reliabilityMin === null || reliabilityMin === undefined || isNaN(reliabilityMin) || !isFinite(reliabilityMin)) {
    reliabilityMin = 70;
  }

  return {
    dqMin: dqMin,
    reliabilityMin: reliabilityMin
  };
}

function _globalMarketsPushWarning_(warnings, label, items) {
  if (!items || !items.length) return;
  warnings.push(label + ': ' + items.slice(0, 15).join(', ') + (items.length > 15 ? ' ...(+' + (items.length - 15) + ')' : ''));
}

function _globalMarketsBlank_(value) {
  return value === null || value === undefined || String(value).trim() === '';
}

function _globalMarketsNumber_(value, fallback) {
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

function _globalMarketsBoolean_(value, fallback) {
  if (value === null || value === undefined || value === '') return !!fallback;
  if (typeof value === 'boolean') return value;

  var s = String(value).trim().toLowerCase();

  if (['1', 'true', 'yes', 'y', 'on', 't', 'enabled', 'enable'].indexOf(s) >= 0) return true;
  if (['0', 'false', 'no', 'n', 'off', 'f', 'disabled', 'disable'].indexOf(s) >= 0) return false;

  return !!fallback;
}
