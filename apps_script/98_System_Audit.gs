// TADAWUL FAST BRIDGE - GOOGLE SHEETS SYSTEM AUDIT v1.0.0
//
// Read-only by default. This file does not create triggers and does not change
// recommendations, portfolio positions, or market data. It provides a bounded
// persisted-sheet audit plus a post-write hash primitive for the existing
// writer to call after a completed page write.
//
// Optional write-back is blocked unless Script Property
// TFB_AUDIT_ALLOW_WRITE=1. When enabled, only the dedicated hidden tab
// _Full_System_Audit is replaced.

var TFB_SYSTEM_AUDIT_VERSION = '1.0.0';

var TFB_SYSTEM_AUDIT_ = Object.freeze({
  MAIN_PAGES: ['Global_Markets', 'Market_Leaders', 'Commodities_FX', 'Mutual_Funds', 'My_Portfolio'],
  AUX_PAGES: ['Top_10_Investments', 'Portfolio_Decision', '_Status', '_Run_Log', 'Dashboard_Audit'],
  AUDIT_SHEET: '_Full_System_Audit',
  MAX_HEADER_SCAN_ROWS: 20,
  CHUNK_ROWS: 200,
  MAX_SAMPLES_PER_RULE: 12,
  BUY_FAMILY: Object.freeze({STRONG_BUY: true, BUY: true, ACCUMULATE: true}),
  INVEST_CLASS: Object.freeze({INVEST: true, INVESTABLE: true, BUY: true, STRONG_BUY: true, ACCUMULATE: true}),
  IDENTITY_MARKERS: Object.freeze([
    'quote_current_price_missing',
    'quote_exchange_missing',
    'quote_currency_missing',
    'name_unresolved'
  ])
});

function TFB_AUDIT_runSystemAudit() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var result = {
    auditVersion: TFB_SYSTEM_AUDIT_VERSION,
    spreadsheetId: ss.getId(),
    spreadsheetName: ss.getName(),
    generatedAt: new Date().toISOString(),
    readOnly: true,
    pages: {},
    crossSurface: {},
    verdict: 'PASS',
    counts: {fail: 0, warn: 0, pass: 0, skip: 0}
  };

  var pages = TFB_SYSTEM_AUDIT_.MAIN_PAGES.concat(TFB_SYSTEM_AUDIT_.AUX_PAGES);
  for (var i = 0; i < pages.length; i++) {
    var pageName = pages[i];
    var sheet = ss.getSheetByName(pageName);
    if (!sheet) {
      result.pages[pageName] = {status: 'SKIP', reason: 'sheet_missing'};
      result.counts.skip++;
      continue;
    }
    if (TFB_SYSTEM_AUDIT_.MAIN_PAGES.indexOf(pageName) >= 0) {
      result.pages[pageName] = tfbAuditDecisionPage_(sheet);
    } else {
      result.pages[pageName] = tfbAuditAuxPage_(sheet);
    }
    tfbAuditAccumulateStatus_(result, result.pages[pageName].status);
  }

  result.crossSurface = tfbAuditCrossSurface_(ss);
  tfbAuditAccumulateStatus_(result, result.crossSurface.status);
  if (result.counts.fail > 0) {
    result.verdict = 'NO_GO';
  } else if (result.counts.warn > 0 || result.counts.skip > 0) {
    result.verdict = 'CONDITIONAL_NO_GO';
  }

  console.log('[TFB-SYSTEM-AUDIT] ' + JSON.stringify(result));
  return result;
}

function TFB_AUDIT_writeSystemAudit() {
  var allowed = String(
    PropertiesService.getScriptProperties().getProperty('TFB_AUDIT_ALLOW_WRITE') || ''
  ).toLowerCase();
  if (['1', 'true', 'yes', 'on'].indexOf(allowed) < 0) {
    throw new Error('Audit write-back is disabled. Set Script Property TFB_AUDIT_ALLOW_WRITE=1 explicitly.');
  }

  var result = TFB_AUDIT_runSystemAudit();
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(TFB_SYSTEM_AUDIT_.AUDIT_SHEET);
  if (!sheet) {
    sheet = ss.insertSheet(TFB_SYSTEM_AUDIT_.AUDIT_SHEET);
  }
  sheet.clearContents();

  var rows = [[
    'Generated At', 'Audit Version', 'Area', 'Status', 'Rows', 'Failures', 'Warnings', 'Details'
  ]];
  var names = Object.keys(result.pages).sort();
  for (var i = 0; i < names.length; i++) {
    var name = names[i];
    var page = result.pages[name] || {};
    rows.push([
      result.generatedAt,
      result.auditVersion,
      name,
      page.status || '',
      page.rows || 0,
      page.failures || 0,
      page.warnings || 0,
      JSON.stringify(page)
    ]);
  }
  rows.push([
    result.generatedAt,
    result.auditVersion,
    'Cross-Surface',
    result.crossSurface.status || '',
    0,
    result.crossSurface.failures || 0,
    result.crossSurface.warnings || 0,
    JSON.stringify(result.crossSurface)
  ]);

  sheet.getRange(1, 1, rows.length, rows[0].length).setValues(rows);
  sheet.setFrozenRows(1);
  sheet.hideSheet();
  return result;
}

/**
 * Return a deterministic critical-field hash for one persisted sheet.
 * The writer can call this after its write, compare it to the expected payload
 * hash, and refuse PAGE SUCCESS when row count or hash differs.
 */
function TFB_AUDIT_hashCriticalFields(sheetName) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(String(sheetName || ''));
  if (!sheet) {
    throw new Error('Sheet not found: ' + sheetName);
  }
  var header = tfbAuditFindHeader_(sheet);
  if (!header.found) {
    return {sheet: sheetName, status: 'FAIL', reason: 'header_not_found'};
  }
  var headerMap = tfbAuditHeaderMap_(header.values);
  var criticalNames = [
    'Symbol', 'Name', 'Currency', 'Current Price', 'Open', 'Day High', 'Day Low',
    'Recommendation', 'Recommendation Detail', 'Investability Status',
    'Final Action', 'Block Reason', 'Warnings', 'Reliability', 'Data Quality',
    'Forecast Price 12M', 'Expected ROI 12M'
  ];
  var indexes = [];
  for (var i = 0; i < criticalNames.length; i++) {
    var idx = tfbAuditFindIndex_(headerMap, [criticalNames[i]]);
    if (idx >= 0) {
      indexes.push({name: criticalNames[i], index: idx});
    }
  }

  var digestRows = [];
  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  for (var start = header.row + 1; start <= lastRow; start += TFB_SYSTEM_AUDIT_.CHUNK_ROWS) {
    var count = Math.min(TFB_SYSTEM_AUDIT_.CHUNK_ROWS, lastRow - start + 1);
    var values = sheet.getRange(start, 1, count, lastCol).getDisplayValues();
    for (var r = 0; r < values.length; r++) {
      var row = values[r];
      var selected = [];
      for (var j = 0; j < indexes.length; j++) {
        selected.push(String(row[indexes[j].index] || '').trim());
      }
      if (selected.join('').length > 0) {
        digestRows.push(selected.join('\u001f'));
      }
    }
  }
  var canonical = indexes.map(function(x) { return x.name; }).join('\u001f') + '\n' + digestRows.join('\n');
  var bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, canonical, Utilities.Charset.UTF_8);
  return {
    sheet: sheetName,
    status: 'PASS',
    headerRow: header.row,
    lastRow: lastRow,
    criticalColumns: indexes.map(function(x) { return x.name; }),
    dataRowsHashed: digestRows.length,
    sha256: tfbAuditHex_(bytes)
  };
}

function TFB_AUDIT_certifyPostWrite(sheetName, expected) {
  var actual = TFB_AUDIT_hashCriticalFields(sheetName);
  expected = expected || {};
  var expectedHash = String(expected.sha256 || '').toLowerCase();
  var expectedRows = Number(expected.dataRowsHashed || expected.rows || 0);
  var hashMatch = expectedHash && expectedHash === String(actual.sha256 || '').toLowerCase();
  var rowMatch = expectedRows > 0 && expectedRows === Number(actual.dataRowsHashed || 0);
  return {
    sheet: sheetName,
    certified: Boolean(hashMatch && rowMatch),
    hashMatch: Boolean(hashMatch),
    rowMatch: Boolean(rowMatch),
    expected: {sha256: expectedHash, dataRowsHashed: expectedRows},
    actual: actual,
    certifiedAt: new Date().toISOString()
  };
}

function tfbAuditDecisionPage_(sheet) {
  var header = tfbAuditFindHeader_(sheet);
  if (!header.found) {
    return {status: 'FAIL', failures: 1, warnings: 0, rows: 0, reason: 'header_not_found'};
  }
  var map = tfbAuditHeaderMap_(header.values);
  var idx = {
    symbol: tfbAuditFindIndex_(map, ['Symbol', 'Ticker']),
    price: tfbAuditFindIndex_(map, ['Current Price', 'Price']),
    open: tfbAuditFindIndex_(map, ['Open', 'Open Price']),
    high: tfbAuditFindIndex_(map, ['Day High', 'High']),
    low: tfbAuditFindIndex_(map, ['Day Low', 'Low']),
    reco: tfbAuditFindIndex_(map, ['Recommendation']),
    detail: tfbAuditFindIndex_(map, ['Recommendation Detail', 'Recommendation Detailed']),
    investability: tfbAuditFindIndex_(map, ['Investability Status', 'Investability']),
    action: tfbAuditFindIndex_(map, ['Final Action', 'Action']),
    block: tfbAuditFindIndex_(map, ['Block Reason']),
    warnings: tfbAuditFindIndex_(map, ['Warnings', 'Warning', 'Errors'])
  };

  var out = {
    status: 'PASS',
    headerRow: header.row,
    rows: 0,
    failures: 0,
    warnings: 0,
    rules: {
      blockedBuyActionable: {count: 0, samples: []},
      fetchFailedNotBlocked: {count: 0, samples: []},
      identityWarningInvestable: {count: 0, samples: []},
      hardRowSanityActionable: {count: 0, samples: []},
      ohlcOpenOutsideBand: {count: 0, samples: []}
    }
  };

  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  for (var start = header.row + 1; start <= lastRow; start += TFB_SYSTEM_AUDIT_.CHUNK_ROWS) {
    var count = Math.min(TFB_SYSTEM_AUDIT_.CHUNK_ROWS, lastRow - start + 1);
    var rows = sheet.getRange(start, 1, count, lastCol).getDisplayValues();
    for (var r = 0; r < rows.length; r++) {
      var row = rows[r];
      var symbol = tfbAuditCell_(row, idx.symbol);
      if (!symbol) {
        continue;
      }
      out.rows++;
      var reco = tfbAuditToken_(tfbAuditFirst_(row, [idx.reco, idx.detail]));
      var detail = tfbAuditToken_(tfbAuditCell_(row, idx.detail));
      var action = tfbAuditToken_(tfbAuditCell_(row, idx.action));
      var investability = tfbAuditToken_(tfbAuditCell_(row, idx.investability));
      var block = tfbAuditCell_(row, idx.block);
      var warningBlob = (
        tfbAuditCell_(row, idx.warnings) + ';' + block
      ).toLowerCase();
      var actionable = Boolean(
        TFB_SYSTEM_AUDIT_.INVEST_CLASS[action] ||
        TFB_SYSTEM_AUDIT_.INVEST_CLASS[investability] ||
        TFB_SYSTEM_AUDIT_.BUY_FAMILY[reco] ||
        TFB_SYSTEM_AUDIT_.BUY_FAMILY[detail]
      );
      var hardBlocked = investability === 'BLOCKED' || action === 'DO_NOT_INVEST';
      var buyFamily = Boolean(TFB_SYSTEM_AUDIT_.BUY_FAMILY[reco] || TFB_SYSTEM_AUDIT_.BUY_FAMILY[detail]);

      if (block && buyFamily && actionable) {
        tfbAuditHit_(out.rules.blockedBuyActionable, symbol);
      }
      if (warningBlob.indexOf('fetch_failed') >= 0 && !hardBlocked) {
        tfbAuditHit_(out.rules.fetchFailedNotBlocked, symbol);
      }
      if (tfbAuditHasIdentityWarning_(warningBlob) && (investability === 'INVESTABLE' || action === 'INVEST')) {
        tfbAuditHit_(out.rules.identityWarningInvestable, symbol);
      }

      var price = tfbAuditNumber_(tfbAuditCell_(row, idx.price));
      var high = tfbAuditNumber_(tfbAuditCell_(row, idx.high));
      var low = tfbAuditNumber_(tfbAuditCell_(row, idx.low));
      var open = tfbAuditNumber_(tfbAuditCell_(row, idx.open));
      var hardSanity = false;
      if (/\s/.test(symbol)) {
        hardSanity = true;
      }
      if (actionable && (!isFinite(price) || price <= 0)) {
        hardSanity = true;
      }
      if (isFinite(high) && isFinite(low) && high < low) {
        hardSanity = true;
      }
      if (hardSanity && actionable) {
        tfbAuditHit_(out.rules.hardRowSanityActionable, symbol);
      }
      if (isFinite(open) && isFinite(high) && isFinite(low) && (open > high || open < low)) {
        tfbAuditHit_(out.rules.ohlcOpenOutsideBand, symbol);
      }
    }
  }

  var hardRules = ['blockedBuyActionable', 'fetchFailedNotBlocked', 'identityWarningInvestable', 'hardRowSanityActionable'];
  for (var i = 0; i < hardRules.length; i++) {
    out.failures += Number(out.rules[hardRules[i]].count || 0);
  }
  out.warnings += Number(out.rules.ohlcOpenOutsideBand.count || 0);
  if (out.failures > 0) {
    out.status = 'FAIL';
  } else if (out.warnings > 0) {
    out.status = 'WARN';
  }
  return out;
}

function tfbAuditAuxPage_(sheet) {
  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  var result = {status: 'PASS', rows: lastRow, columns: lastCol, failures: 0, warnings: 0};
  if (sheet.getName() === '_Status' || sheet.getName() === '_Run_Log') {
    var sampleRows = Math.min(lastRow, 250);
    var sampleCols = Math.min(lastCol, 26);
    var values = sheet.getRange(Math.max(1, lastRow - sampleRows + 1), 1, sampleRows, sampleCols).getDisplayValues();
    var blob = values.map(function(row) { return row.join(' | '); }).join('\n').toUpperCase();
    if (blob.indexOf('PARTIAL') >= 0 || blob.indexOf('CHECKPOINT') >= 0) {
      result.status = 'WARN';
      result.warnings = 1;
      result.reason = 'recent_partial_or_checkpoint_state_detected';
    }
  }
  return result;
}

function tfbAuditCrossSurface_(ss) {
  var globalSheet = ss.getSheetByName('Global_Markets');
  var portfolioSheet = ss.getSheetByName('My_Portfolio');
  if (!globalSheet || !portfolioSheet) {
    return {status: 'SKIP', failures: 0, warnings: 0, reason: 'required_surface_missing'};
  }
  var globalRows = tfbAuditReadIdentityRows_(globalSheet, 7000);
  var portfolioRows = tfbAuditReadIdentityRows_(portfolioSheet, 1000);
  var byBase = {};
  for (var i = 0; i < globalRows.length; i++) {
    var g = globalRows[i];
    byBase[tfbAuditBaseSymbol_(g.symbol)] = g;
  }
  var conflicts = [];
  for (var j = 0; j < portfolioRows.length; j++) {
    var p = portfolioRows[j];
    var g2 = byBase[tfbAuditBaseSymbol_(p.symbol)];
    if (!g2) {
      continue;
    }
    var mismatch = [];
    if (p.name && g2.name && tfbAuditToken_(p.name) !== tfbAuditToken_(g2.name)) {
      mismatch.push('name');
    }
    if (p.currency && g2.currency && tfbAuditToken_(p.currency) !== tfbAuditToken_(g2.currency)) {
      mismatch.push('currency');
    }
    if (p.investability && g2.investability && tfbAuditToken_(p.investability) !== tfbAuditToken_(g2.investability)) {
      mismatch.push('investability');
    }
    if (p.action && g2.action && tfbAuditToken_(p.action) !== tfbAuditToken_(g2.action)) {
      mismatch.push('final_action');
    }
    if (mismatch.length) {
      conflicts.push({symbol: p.symbol, globalSymbol: g2.symbol, fields: mismatch});
    }
  }
  return {
    status: conflicts.length ? 'FAIL' : 'PASS',
    failures: conflicts.length,
    warnings: 0,
    comparedPortfolioRows: portfolioRows.length,
    conflictSamples: conflicts.slice(0, TFB_SYSTEM_AUDIT_.MAX_SAMPLES_PER_RULE)
  };
}

function tfbAuditReadIdentityRows_(sheet, maxRows) {
  var header = tfbAuditFindHeader_(sheet);
  if (!header.found) {
    return [];
  }
  var map = tfbAuditHeaderMap_(header.values);
  var indexes = {
    symbol: tfbAuditFindIndex_(map, ['Symbol', 'Ticker']),
    name: tfbAuditFindIndex_(map, ['Name', 'Company']),
    currency: tfbAuditFindIndex_(map, ['Currency', 'Ccy']),
    investability: tfbAuditFindIndex_(map, ['Investability Status', 'Investability']),
    action: tfbAuditFindIndex_(map, ['Final Action', 'Action'])
  };
  var lastRow = Math.min(sheet.getLastRow(), header.row + Number(maxRows || 1000));
  var lastCol = sheet.getLastColumn();
  var out = [];
  for (var start = header.row + 1; start <= lastRow; start += TFB_SYSTEM_AUDIT_.CHUNK_ROWS) {
    var count = Math.min(TFB_SYSTEM_AUDIT_.CHUNK_ROWS, lastRow - start + 1);
    var rows = sheet.getRange(start, 1, count, lastCol).getDisplayValues();
    for (var r = 0; r < rows.length; r++) {
      var symbol = tfbAuditCell_(rows[r], indexes.symbol);
      if (!symbol) {
        continue;
      }
      out.push({
        symbol: symbol,
        name: tfbAuditCell_(rows[r], indexes.name),
        currency: tfbAuditCell_(rows[r], indexes.currency),
        investability: tfbAuditCell_(rows[r], indexes.investability),
        action: tfbAuditCell_(rows[r], indexes.action)
      });
    }
  }
  return out;
}

function tfbAuditFindHeader_(sheet) {
  var rows = Math.min(TFB_SYSTEM_AUDIT_.MAX_HEADER_SCAN_ROWS, Math.max(1, sheet.getLastRow()));
  var cols = Math.max(1, sheet.getLastColumn());
  var values = sheet.getRange(1, 1, rows, cols).getDisplayValues();
  for (var r = 0; r < values.length; r++) {
    var tokens = values[r].map(tfbAuditToken_);
    var hasSymbol = tokens.indexOf('SYMBOL') >= 0 || tokens.indexOf('TICKER') >= 0;
    var hasDecision = tokens.indexOf('RECOMMENDATION') >= 0 || tokens.indexOf('FINAL_ACTION') >= 0 || tokens.indexOf('FINAL ACTION') >= 0;
    if (hasSymbol && hasDecision) {
      return {found: true, row: r + 1, values: values[r]};
    }
  }
  return {found: false, row: 0, values: []};
}

function tfbAuditHeaderMap_(headers) {
  var map = {};
  for (var i = 0; i < headers.length; i++) {
    var token = tfbAuditToken_(headers[i]);
    if (token && map[token] === undefined) {
      map[token] = i;
    }
  }
  return map;
}

function tfbAuditFindIndex_(map, aliases) {
  for (var i = 0; i < aliases.length; i++) {
    var token = tfbAuditToken_(aliases[i]);
    if (map[token] !== undefined) {
      return Number(map[token]);
    }
  }
  return -1;
}

function tfbAuditCell_(row, index) {
  if (index === undefined || index === null || index < 0 || index >= row.length) {
    return '';
  }
  return String(row[index] === null || row[index] === undefined ? '' : row[index]).trim();
}

function tfbAuditFirst_(row, indexes) {
  for (var i = 0; i < indexes.length; i++) {
    var value = tfbAuditCell_(row, indexes[i]);
    if (value) {
      return value;
    }
  }
  return '';
}

function tfbAuditToken_(value) {
  return String(value === null || value === undefined ? '' : value)
    .trim()
    .toUpperCase()
    .replace(/[\s\-\/]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function tfbAuditNumber_(value) {
  var text = String(value === null || value === undefined ? '' : value)
    .replace(/,/g, '')
    .replace(/%/g, '')
    .trim();
  if (!text) {
    return NaN;
  }
  var number = Number(text);
  return isFinite(number) ? number : NaN;
}

function tfbAuditHasIdentityWarning_(blob) {
  if (/xprovider_verified:[^;\s]*:0\.0+%/i.test(blob)) {
    return true;
  }
  for (var i = 0; i < TFB_SYSTEM_AUDIT_.IDENTITY_MARKERS.length; i++) {
    if (blob.indexOf(TFB_SYSTEM_AUDIT_.IDENTITY_MARKERS[i]) >= 0) {
      return true;
    }
  }
  return false;
}

function tfbAuditHit_(bucket, symbol) {
  bucket.count = Number(bucket.count || 0) + 1;
  if (bucket.samples.length < TFB_SYSTEM_AUDIT_.MAX_SAMPLES_PER_RULE) {
    bucket.samples.push(symbol);
  }
}

function tfbAuditAccumulateStatus_(result, status) {
  var token = String(status || 'SKIP').toUpperCase();
  if (token === 'FAIL') {
    result.counts.fail++;
  } else if (token === 'WARN') {
    result.counts.warn++;
  } else if (token === 'PASS') {
    result.counts.pass++;
  } else {
    result.counts.skip++;
  }
}

function tfbAuditBaseSymbol_(symbol) {
  var text = String(symbol || '').trim().toUpperCase();
  return text.replace(/\.(US|SR)$/i, '');
}

function tfbAuditHex_(bytes) {
  return bytes.map(function(value) {
    var normalized = (value + 256) % 256;
    return ('0' + normalized.toString(16)).slice(-2);
  }).join('');
}
