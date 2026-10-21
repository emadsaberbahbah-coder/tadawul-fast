/* Preserve the immutable v1.13.8 source without duplicating a 438 KB fixture.
 * Every v1.13.10 edit (holding subunit FX, null value_sar for a missing rate,
 * terminal FAILED refresh state) and then every v1.13.9 edit is reversed
 * exactly once and the full historical digest
 * must match. Any unrelated edit or code hidden in an approved region fails.
 * Historical D1-D5 use this hash-proven source; current behavior tests use
 * the delivered source. This helper is a test fixture, never runtime code. */
'use strict';
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const HISTORY_SHA256 = '9d11832acae738be2299faa463ca6aa71b8e9a77ab31c05d4bf8d5e55bcd506c';
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
/* v1.13.10 edits, reversed first (current -> v1.13.9 text). */
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
const EDITS = EDITS_V11310.concat([
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
  for (const [current, prior] of EDITS) {
    assert.equal(historical.split(current).length - 1, 1,
      'exactly one approved v1.13.9/v1.13.10 edit: ' + current.split('\n')[0]);
    historical = historical.replace(current, prior);
  }
  assert.equal(crypto.createHash('sha256').update(historical).digest('hex'), HISTORY_SHA256,
    'v1.13.9/v1.13.10 may change only their enumerated edits');
  return historical;
}
module.exports = {feedHistorySource, HISTORY_SHA256};
