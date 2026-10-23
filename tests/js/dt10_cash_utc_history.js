/* Preserve the immutable v1.13.8 source without duplicating a 438 KB fixture.
 * The v1.13.13 integration/refresh guards, every v1.13.11 bulk edit,
 * then every v1.13.10 edit
 * (holding subunit FX, null value_sar for a missing rate, terminal FAILED
 * refresh state) and then every v1.13.9 edit is reversed exactly once and
 * the full historical digest
 * must match. Any unrelated edit or code hidden in an approved region fails.
 * Historical D1-D5 use this hash-proven source; current behavior tests use
 * the delivered source. This helper is a test fixture, never runtime code. */
'use strict';
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const HISTORY_SHA256 = '9d11832acae738be2299faa463ca6aa71b8e9a77ab31c05d4bf8d5e55bcd506c';
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
const INTEGRATION_WHY = " * v1.13.13 (2026-10-24) -- STACK INTEGRATION, REFRESH AND BULK RENDER\n * WHY: merge the pending failed-refresh/projection validation with main's\n * bulk formatting and terminal FAILED banner/receipt handling. Retain both\n * historical v1.13.11 WHY blocks and the raw subunit FX preservation. A\n * malformed response is still refused before stability or board mutation;\n * honest empty/WITHHELD results and the existing gate policies are unchanged.\n * Version 1.13.12 is reserved for the pending completion-receipt work.\n *\n";
const COMPLETION_EDITS = [
  [' * Version: 1.13.13 ', ' * Version: 1.13.11 '],
  [INTEGRATION_WHY, ''],
  [COMPLETION_WHY, ''],
  ["var DT10_VERSION = '1.13.13';", "var DT10_VERSION = '1.13.11';"],
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
/* v1.13.11 edits, reversed first (current -> v1.13.10 text). The result is
 * hash-pinned (V11310_SHA256 = the reviewed v1.13.10 file), so the bulk
 * formatting harness can run the previous per-cell code as its oracle. */
const V11310_SHA256 = '914592a3a103815e9d26ecc6b485233623c21cec5382aea68ef5c2b5b1cb32d8';
const EDITS_V11311 = [
  [" * Version: 1.13.11 (see DT10_VERSION; header kept in lockstep — restored\n",
    " * Version: 1.13.10 (see DT10_VERSION; header kept in lockstep — restored\n"],
  [" * v1.13.11 (2026-10-22) -- BULK RENDER FORMATTING (6-MINUTE LIMIT)\n * WHY (21 Oct 2026, Apps Script execution log): the automatic Top-10\n * refresh that followed the first decision-feed release logged 'upstream\n * verdict: EXECUTABLE; final board: HELD' at 23:48:42 Riyadh and was\n * killed at 23:50:59 ('Exceeded maximum execution time') before it wrote\n * its status line. B2 stayed 'running', the next scheduled release check\n * read T10_STATUS_NOT_OK and withdrew the release (decision_failed). The\n * 19:40 run was killed the same way; completed runs took 127-232 s. The\n * render painted formats cell by cell: one setBackground per zebra row in\n * dt10WriteTable_, and setBackground + setFontColor + setFontWeight per\n * coloured cell in dt10ColorVerdicts_, dt10ColorQualified_ and\n * dt10ColorStability_ -- about 1,750 range calls for the 500-row CANDIDATES\n * grid alone, each one slower while other scripts write the workbook.\n * WHAT: the same formats are written in bulk. dt10WriteTable_ reads the\n * body backgrounds once, sets the zebra rows and writes them back once.\n * The three colourers build one token per row and call the new\n * dt10BulkPaint_, which reads backgrounds, font colours and font weights\n * once, sets tok.bg / tok.fg / bold on the token rows and writes each\n * attribute once (nothing is read or written when no row has a token).\n * Rows the old loops left alone keep their current format (read back and\n * written unchanged), so the painted board is the same. Values, verdicts,\n * withholding, funding, stability, receipts and the status line are\n * unchanged. Harness: tests/js/dt10_bulk_format.test.js (cell-model\n * equivalence against the v1.13.10 code, bounded call counts).\n *\n",
    ""],
  ["var DT10_VERSION = '1.13.11';\n",
    "var DT10_VERSION = '1.13.10';\n"],
  ["function dt10WriteTable_(sheet, row, headers, dataRows, emptyText) {\n  // v1.1.0 reskin: column-header row is the brand navy band with white text.\n  sheet.getRange(row, 1, 1, headers.length).setValues([headers])\n      .setFontWeight('bold').setFontSize(9).setWrap(true)\n      .setBackground(DT10_NAVY).setFontColor('#FFFFFF');\n  row++;\n  if (!dataRows.length) {\n    sheet.getRange(row, 1, 1, DT10_LAST_COL).merge().setValue(emptyText)\n        .setFontStyle('italic').setFontColor(DT10_MUTED);\n    return { next: row + 1, firstDataRow: -1, count: 0 };\n  }\n  sheet.getRange(row, 1, dataRows.length, headers.length).setValues(dataRows);\n  // Body text in proposal ink; alternate rows in the proposal zebra tint.\n  var body = sheet.getRange(row, 1, dataRows.length, headers.length);\n  body.setFontColor(DT10_INK);\n  // v1.13.11: one read + one write instead of one call per zebra row; the\n  // other rows keep the background they already had.\n  var zebra = body.getBackgrounds();\n  for (var i = 0; i < dataRows.length; i += 2) {\n    for (var zc = 0; zc < headers.length; zc++) zebra[i][zc] = DT10_ZEBRA;\n  }\n  body.setBackgrounds(zebra);\n  sheet.getRange(row - 1, 1, dataRows.length + 1, headers.length)\n      .setBorder(true, true, true, true, true, true, DT10_BORDER,\n                 SpreadsheetApp.BorderStyle.SOLID);\n  return { next: row + dataRows.length, firstDataRow: row,\n           count: dataRows.length };\n}\n",
    "function dt10WriteTable_(sheet, row, headers, dataRows, emptyText) {\n  // v1.1.0 reskin: column-header row is the brand navy band with white text.\n  sheet.getRange(row, 1, 1, headers.length).setValues([headers])\n      .setFontWeight('bold').setFontSize(9).setWrap(true)\n      .setBackground(DT10_NAVY).setFontColor('#FFFFFF');\n  row++;\n  if (!dataRows.length) {\n    sheet.getRange(row, 1, 1, DT10_LAST_COL).merge().setValue(emptyText)\n        .setFontStyle('italic').setFontColor(DT10_MUTED);\n    return { next: row + 1, firstDataRow: -1, count: 0 };\n  }\n  sheet.getRange(row, 1, dataRows.length, headers.length).setValues(dataRows);\n  // Body text in proposal ink; alternate rows in the proposal zebra tint.\n  sheet.getRange(row, 1, dataRows.length, headers.length)\n      .setFontColor(DT10_INK);\n  for (var i = 0; i < dataRows.length; i += 2) {\n    sheet.getRange(row + i, 1, 1, headers.length).setBackground(DT10_ZEBRA);\n  }\n  sheet.getRange(row - 1, 1, dataRows.length + 1, headers.length)\n      .setBorder(true, true, true, true, true, true, DT10_BORDER,\n                 SpreadsheetApp.BorderStyle.SOLID);\n  return { next: row + dataRows.length, firstDataRow: row,\n           count: dataRows.length };\n}\n"],
  ["function dt10ColorStability_(sheet, firstDataRow, count, col, tokens) {\n  if (firstDataRow < 0 || !count) return;\n  var rng = sheet.getRange(firstDataRow, col, count, 1);\n  var vals = rng.getValues(), toks = [];\n  for (var i = 0; i < count; i++) {\n    var v = String(vals[i][0] || '');\n    var tok = null;\n    if (v.indexOf('ACTIVE') === 0 || v.indexOf('NEW') === 0) {\n      tok = tokens.VERDICT_POSITIVE;\n    } else if (v.indexOf('GRACE') === 0 || v.indexOf('FAST-TRACK') === 0) {\n      tok = tokens.VERDICT_WATCH;\n    }\n    toks.push(tok || null);\n  }\n  dt10BulkPaint_(rng, toks);   // v1.13.11\n}\n",
    "function dt10ColorStability_(sheet, firstDataRow, count, col, tokens) {\n  if (firstDataRow < 0 || !count) return;\n  var vals = sheet.getRange(firstDataRow, col, count, 1).getValues();\n  for (var i = 0; i < count; i++) {\n    var v = String(vals[i][0] || '');\n    var tok = null;\n    if (v.indexOf('ACTIVE') === 0 || v.indexOf('NEW') === 0) {\n      tok = tokens.VERDICT_POSITIVE;\n    } else if (v.indexOf('GRACE') === 0 || v.indexOf('FAST-TRACK') === 0) {\n      tok = tokens.VERDICT_WATCH;\n    }\n    if (tok) {\n      sheet.getRange(firstDataRow + i, col).setBackground(tok.bg)\n          .setFontColor(tok.fg).setFontWeight('bold');\n    }\n  }\n}\n"],
  ["function dt10ColorQualified_(sheet, firstDataRow, count, col, qualified,\n                             tokens) {\n  if (firstDataRow < 0 || !count) return;\n  var toks = [];\n  for (var i = 0; i < count; i++) {\n    var c = qualified[i] || {};\n    var tok;\n    if (c.selected === true) tok = tokens.VERDICT_POSITIVE;\n    else if (dt10HasValue_(c.deferral)) tok = tokens.VERDICT_WATCH;\n    else tok = tokens.VERDICT_BLOCK;\n    toks.push(tok || null);\n  }\n  dt10BulkPaint_(sheet.getRange(firstDataRow, col, count, 1), toks);   // v1.13.11\n}\n",
    "function dt10ColorQualified_(sheet, firstDataRow, count, col, qualified,\n                             tokens) {\n  if (firstDataRow < 0 || !count) return;\n  for (var i = 0; i < count; i++) {\n    var c = qualified[i] || {};\n    var tok;\n    if (c.selected === true) tok = tokens.VERDICT_POSITIVE;\n    else if (dt10HasValue_(c.deferral)) tok = tokens.VERDICT_WATCH;\n    else tok = tokens.VERDICT_BLOCK;\n    if (tok) {\n      sheet.getRange(firstDataRow + i, col).setBackground(tok.bg)\n          .setFontColor(tok.fg).setFontWeight('bold');\n    }\n  }\n}\n"],
  ["/** v1.13.11: paint a one-column range from per-row tokens in bulk. toks[i]\n * is a {bg, fg} token or null. A token row gets tok.bg, tok.fg and bold,\n * exactly what the per-cell setBackground/setFontColor/setFontWeight calls\n * wrote; a null row keeps its current background, font colour and weight\n * (read back and written unchanged). One read and one write per attribute;\n * nothing is read or written when no row has a token. */\nfunction dt10BulkPaint_(rng, toks) {\n  var any = false;\n  for (var a = 0; a < toks.length; a++) {\n    if (toks[a]) { any = true; break; }\n  }\n  if (!any) return;\n  var bg = rng.getBackgrounds(), fg = rng.getFontColors(),\n      fw = rng.getFontWeights();\n  for (var i = 0; i < toks.length; i++) {\n    var tok = toks[i];\n    if (!tok) continue;\n    bg[i][0] = tok.bg === undefined ? null : tok.bg;\n    fg[i][0] = tok.fg === undefined ? null : tok.fg;\n    fw[i][0] = 'bold';\n  }\n  rng.setBackgrounds(bg);\n  rng.setFontColors(fg);\n  rng.setFontWeights(fw);\n}\n/** Verdict/conf cell coloring via §3.4 tokens on a written table column. */\nfunction dt10ColorVerdicts_(sheet, firstDataRow, count, col, tokens) {\n  if (firstDataRow < 0 || !count) return;\n  var rng = sheet.getRange(firstDataRow, col, count, 1);\n  var vals = rng.getValues(), toks = [];\n  for (var i = 0; i < count; i++) {\n    var v = String(vals[i][0] || '');\n    var tok = null;\n    if (v === 'INVEST') tok = tokens.VERDICT_POSITIVE;\n    else if (v === 'WATCH') tok = tokens.VERDICT_WATCH;\n    else if (v === 'DO_NOT_INVEST') tok = tokens.VERDICT_NEGATIVE;\n    else if (v) tok = tokens.VERDICT_BLOCK;\n    toks.push(tok || null);\n  }\n  dt10BulkPaint_(rng, toks);   // v1.13.11\n}\n",
    "/** Verdict/conf cell coloring via §3.4 tokens on a written table column. */\nfunction dt10ColorVerdicts_(sheet, firstDataRow, count, col, tokens) {\n  if (firstDataRow < 0 || !count) return;\n  var rng = sheet.getRange(firstDataRow, col, count, 1);\n  var vals = rng.getValues();\n  for (var i = 0; i < count; i++) {\n    var v = String(vals[i][0] || '');\n    var tok = null;\n    if (v === 'INVEST') tok = tokens.VERDICT_POSITIVE;\n    else if (v === 'WATCH') tok = tokens.VERDICT_WATCH;\n    else if (v === 'DO_NOT_INVEST') tok = tokens.VERDICT_NEGATIVE;\n    else if (v) tok = tokens.VERDICT_BLOCK;\n    if (tok) {\n      sheet.getRange(firstDataRow + i, col).setBackground(tok.bg)\n          .setFontColor(tok.fg).setFontWeight('bold');\n    }\n  }\n}\n"]
];
/* v1.13.10 edits, reversed next (v1.13.10 -> v1.13.9 text). */
const EDITS_V11310 = [
  [" * Version: 1.13.10 (see DT10_VERSION; header kept in lockstep — restored\n",
    " * Version: 1.13.9 (see DT10_VERSION; header kept in lockstep — restored\n"],
  [" *\n * v1.13.10 (2026-10-20) -- HOLDING SUBUNIT FX, NO NATIVE-AS-SAR, FAILED STATE\n * (1) dt10HoldingFromRow_ upper-cased the currency before the FX lookup, so\n * a GBp (pence) holding converted at the GBP rate (100x too large). The new\n * dt10HoldingFxRate_ mirrors opportunity_builder._resolve_fx (USX gate off):\n * GBX/GBp, ZAC/ZAc, ILA/ILa are detected on the RAW token and use an exact\n * raw FX key, else parent / 100. USX keeps needing its own 'USX' FX row.\n * (2) A currency with no FX rate kept its NATIVE value labelled value_sar.\n * It now sends value_sar null and marks holdings_input_incomplete, so\n * funding stays withheld; the builder never sums a native amount as SAR.\n * (3) refreshDecisionTop10 wrote 'running...' and a throw left it there\n * (20 Oct: cockpit stuck RUNNING, no grid, validation failed). The body is\n * now dt10RefreshDecisionTop10Run_; on a throw dt10RefreshFailed_ writes\n * 'status: FAILED | failed_at_utc=<ISO Z> | <error>', upserts _Status ERROR,\n * clears the P-195 marker this run stored (a throw before the marker check\n * leaves a previous run's marker for ABORTED_PREV) and rethrows. The\n * freshness audit reads FAILED as T10_STATUS_NOT_OK. LIMIT: an Apps Script\n * hard timeout (6 min) kills the script without running catch; that case\n * still shows RUNNING and is reported by the next run's P-195 ABORTED_PREV\n * row.\n",
    ""],
  ["var DT10_VERSION = '1.13.10';\n",
    "var DT10_VERSION = '1.13.9';\n"],
  ["/** v1.13.10 PURE: SAR per unit of a holding's native currency cell, mirroring\n * opportunity_builder._resolve_fx with TFB_OPP_FX_USX_SUBUNIT off (default).\n * Subunits are detected on the RAW token BEFORE case folding ('GBp' would\n * otherwise fold into pounds): GBX/GBp, ZAC/ZAc, ILA/ILa (and gbx/zac/ila\n * spellings) are 1/100 of GBP/ZAR/ILS. An exact raw key in the map wins\n * (already per subunit); otherwise the parent rate / 100. USX is not folded\n * here (its gate is backend-only and DEFAULT OFF): it needs its own 'USX'\n * row, as before. Returns null for SAR/blank (no conversion), NaN when no\n * rate exists, else the positive rate. */\nvar DT10_FX_SUBUNIT_PARENT = { GBp: 'GBP', GBX: 'GBP', ZAC: 'ZAR', ZAc: 'ZAR',\n                               ILA: 'ILS', ILa: 'ILS' };\nfunction dt10HoldingFxRate_(cell, fx) {\n  var raw = String(cell === null || cell === undefined ? '' : cell).trim();\n  var up = raw.toUpperCase();\n  if (!raw || up === 'SAR') return null;\n  fx = fx || {};\n  var exact = Number(fx[raw]);\n  if (isFinite(exact) && exact > 0) return exact;\n  var parent = Object.prototype.hasOwnProperty.call(DT10_FX_SUBUNIT_PARENT, raw) ?\n      DT10_FX_SUBUNIT_PARENT[raw] :\n      (up === 'GBX' || up === 'ZAC' || up === 'ILA') ? DT10_FX_SUBUNIT_PARENT[up] : '';\n  var rate = Number(fx[parent || up]);\n  if (!(isFinite(rate) && rate > 0)) return NaN;\n  return parent ? rate / 100 : rate;\n}\n",
    ""],
  ["  // currency column and an FX map (SAR per unit). SAR rows and rows without a\n  // currency keep the native value. v1.13.10: subunits resolve on the RAW\n  // token and a currency missing from the map sends value_sar null and\n  // marks the row _input_incomplete instead of a native amount labelled SAR.\n",
    "  // currency column and an FX map (SAR per unit). SAR rows, rows without a\n  // currency and currencies missing from the map keep the native value.\n"],
  ["    var rate = dt10HoldingFxRate_(row[extra.iCcy], extra.fx);\n    if (rate === null) {\n      // SAR or blank currency: the value is already SAR (unchanged).\n    } else if (isFinite(rate) && rate > 0) {\n      val = val * rate;\n      extra._converted = (extra._converted || 0) + 1;\n    } else {\n      val = null;\n      extra._unconverted = (extra._unconverted || 0) + 1;\n",
    "    var ccy = String(row[extra.iCcy] === null || row[extra.iCcy] === undefined ?\n                     '' : row[extra.iCcy]).trim().toUpperCase();\n    if (ccy && ccy !== 'SAR') {\n      var rate = Number(extra.fx[ccy]);\n      if (isFinite(rate) && rate > 0) {\n        val = val * rate;\n        extra._converted = (extra._converted || 0) + 1;\n      } else {\n        extra._unconverted = (extra._unconverted || 0) + 1;\n      }\n"],
  ["      if (h.value_sar === null) out._input_incomplete = true;   // v1.13.10: no FX\n",
    ""],
  ["  dt10PropMemoReset_();   // v1.11.13 [P-202]: fresh property reads per run\n  var run = { statusCell: null, markerOwned: false };   // v1.13.10: a throw ends in FAILED\n  try {\n    dt10RefreshDecisionTop10Run_(run);\n  } catch (eRun) {\n    dt10RefreshFailed_(run, eRun);\n    throw eRun;\n  }\n}\n/** v1.13.10: a refresh that THROWS must not leave the banner at 'running...'\n * (live 20 Oct: cockpit stuck RUNNING with no grid). Writes a terminal\n * 'status: FAILED' line (the Riyadh 'Last run' stamp the audit parses, plus\n * failed_at_utc and a short error), upserts cockpit _Status ERROR and clears\n * the P-195 marker THIS run stored (a reported failure is not an abort); a\n * throw before the marker check leaves a previous run's stale marker for the\n * next check to report as ABORTED_PREV. Never throws; the caller rethrows. A\n * hard Apps Script timeout cannot be caught: that case still leaves RUNNING\n * and is reported by the next run's P-195 check. */\nfunction dt10RefreshFailed_(run, err) {\n  var msg = String(err && err.message ? err.message : err)\n      .replace(/\\s+/g, ' ').slice(0, 200);\n  var utc = '';\n  try { utc = new Date().toISOString().replace(/\\.\\d{3}Z$/, 'Z'); } catch (eIso) {}\n  try {\n    var cell = run && run.statusCell;\n    if (!cell) {\n      var sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(DT10_SHEET);\n      cell = sh ? sh.getRange(DT10_ROW_STATUS, 2) : null;\n    }\n    if (cell) {\n      cell.setValue(dt10StatusLine_('FAILED',\n          'failed_at_utc=' + utc + ' | ' + msg + dt10StaleNote_()));\n    }\n  } catch (eCell) {}\n  try {\n    dt10WritePageStatus_('ERROR', 'Decision cockpit: FAILED at ' + utc + ' \\u2014 ' + msg,\n        '', '', '', []);\n  } catch (eSt) {}\n  if (run && run.markerOwned) {   // only the marker this run stored\n    try { dt10RunMarkerClear_(); } catch (eMk) {}\n  }\n  try { SpreadsheetApp.flush(); } catch (eFl) {}\n}\nfunction dt10RefreshDecisionTop10Run_(run) {\n",
    ""],
  ["  var t0 = new Date().getTime();\n  var ss = SpreadsheetApp.getActiveSpreadsheet();\n",
    "  var t0 = new Date().getTime();\n  dt10PropMemoReset_();   // v1.11.13 [P-202]: fresh property reads per run\n  var ss = SpreadsheetApp.getActiveSpreadsheet();\n"],
  ["  run.statusCell = statusCell;                              // v1.13.10\n",
    ""],
  ["  dt10RunMarkerCheck_('refreshDecisionTop10');\n  run.markerOwned = true;   // v1.13.10: a later throw clears this run's marker\n",
    "  dt10RunMarkerCheck_('refreshDecisionTop10');\n"],
  ["  var hxE = dt10HoldingFromRow_(['VOD.L', 'Telecom', 1000, 'GBp'], 0, 1, 2,\n                                { iCcy: 3, fx: { GBP: 5 } });   // v1.13.10\n  report.push('holdings value_sar via FX (native USD 118.6 -> 445.08; GBp 1000 -> 50; ' +\n      'SAR/SAR-column untouched; unknown -> null): ' +\n",
    "  report.push('holdings value_sar via FX (native USD 118.6 -> 445.08; SAR/unknown/SAR-column untouched): ' +\n"],
  ["        hxC.value_sar === null && hxD.value_sar === 445.08 &&\n        Math.abs(hxE.value_sar - 50) < 1e-9) ? 'ok' :\n       'FAIL [' + hxA.value_sar + '/' + hxB.value_sar + '/' + hxC.value_sar + '/' +\n       hxD.value_sar + '/' + hxE.value_sar + ']'));\n",
    "        hxC.value_sar === 5852 && hxD.value_sar === 445.08) ? 'ok' :\n       'FAIL [' + hxA.value_sar + '/' + hxB.value_sar + '/' + hxC.value_sar + '/' + hxD.value_sar + ']'));\n"]
];
const EDITS = EDITS_V11311.concat(EDITS_V11310, [
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
]);
function feedHistorySource(source) {
  let historical = source;
  for (const [current, prior] of [...COMPLETION_EDITS, ...EDITS]) {
    assert.equal(historical.split(current).length - 1, 1,
      'exactly one approved v1.13.9/v1.13.10/v1.13.11/v1.13.13 edit: ' + current.split('\n')[0]);
    historical = historical.replace(current, prior);
  }
  assert.equal(crypto.createHash('sha256').update(historical).digest('hex'), HISTORY_SHA256,
    'v1.13.9/v1.13.10/v1.13.11/v1.13.13 may change only their enumerated edits');
  return historical;
}
/* v1.13.10 source (the bulk-format oracle): reverse the enumerated
 * integration/refresh guards and bulk edits exactly once, retaining the
 * original reviewed full-file digest. */
function v11310Source(source) {
  let prior = source;
  for (const [current, previous] of [...COMPLETION_EDITS, ...EDITS_V11311]) {
    assert.equal(prior.split(current).length - 1, 1,
      'exactly one approved v1.13.11 edit: ' + current.split('\n')[0]);
    prior = prior.replace(current, previous);
  }
  assert.equal(crypto.createHash('sha256').update(prior).digest('hex'), V11310_SHA256,
    'v1.13.11 may change only its enumerated edits');
  return prior;
}
module.exports = {feedHistorySource, HISTORY_SHA256, v11310Source, V11310_SHA256};
