/* Preserve the immutable v1.13.8 source without duplicating a 438 KB fixture.
 * Every v1.13.9 and v1.13.11 edit is reversed exactly once; the historical digest
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
  for (const [current, prior] of [...COMPLETION_EDITS, ...EDITS]) {
    assert.equal(historical.split(current).length - 1, 1,
      'exactly one approved v1.13.9/v1.13.11 edit: ' + current.split('\n')[0]);
    historical = historical.replace(current, prior);
  }
  assert.equal(crypto.createHash('sha256').update(historical).digest('hex'), HISTORY_SHA256,
    'later versions may change only the enumerated cash/UTC and refresh-completion edits');
  return historical;
}
module.exports = {feedHistorySource, HISTORY_SHA256};
