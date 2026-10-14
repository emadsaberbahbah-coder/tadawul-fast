/* Preserve the immutable v1.13.8 source without duplicating a 438 KB fixture.
 * Every v1.13.9 edit is reversed exactly once and the full historical digest
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
  for (const [current, prior] of EDITS) {
    assert.equal(historical.split(current).length - 1, 1,
      'exactly one approved v1.13.9 edit: ' + current.split('\n')[0]);
    historical = historical.replace(current, prior);
  }
  assert.equal(crypto.createHash('sha256').update(historical).digest('hex'), HISTORY_SHA256,
    'v1.13.9 may change only the enumerated cash label and UTC holdings edits');
  return historical;
}
module.exports = {feedHistorySource, HISTORY_SHA256};
