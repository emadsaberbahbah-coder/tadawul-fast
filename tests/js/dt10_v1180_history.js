/* v1.18.0 (2026-10-17) reversal layer for the hash-proven history chain
 * (L4: send the scorecard cohorts to the backend evidence gate). Reverses
 * every v1.18.0 edit once to the v1.17.0 source; the v1170 -> v1160 -> v1150
 * -> v1140 -> cash_utc layers take it to the pinned v1.13.8 digest. Generated
 * with the edits; a test fixture, never runtime code. */
'use strict';
const assert = require('node:assert/strict');
const EDITS_1180 = [
  [
    " * Version: 1.18.0 (see DT10_VERSION; header kept in lockstep",
    " * Version: 1.17.0 (see DT10_VERSION; header kept in lockstep"
  ],
  [
    " *\n * v1.18.0 (2026-10-17) -- SEND THE SCORECARD COHORTS TO THE EVIDENCE GATE (L4)\n * WHY (Codex brief 2026-10-17 L4): opportunity_builder v1.30.0 gates (observe\n * first) on the Forecast Scorecard's Investability x Recommendation cohorts,\n * but the backend cannot read the workbook; the cockpit already reads the\n * _Forecast_Scorecard tab for the MODEL EVIDENCE strip (v1.17.0). WHAT: with\n * Script Property DT10_EVIDENCE_SCORECARD_SEND = 'on' the request body gains\n * evidence_scorecard = [{horizon, cohort, n, hit}] -- the Investability x\n * Recommendation rows of the tab (dt10EvidenceScorecardRows_, pure; capped at\n * 400 rows). Property absent/off (default): the request is byte-identical to\n * v1.17.0; the backend ignores the field unless TFB_T10_EVIDENCE_GATE is\n * armed. Status line gains ' | scorecard sent (N cohorts)'. Functions added\n * (0 removed): dt10EvidenceScorecardSendOn_, dt10EvidenceScorecardRows_.\n * dt10SelfTest reports the property state + 'scorecard rows core: ok'.\n * Harness: tests/js/dt10_v1180_evidence_send.test.js (+ dt10_v1180_history.js).\n *\n * v1.17.0 (2026-10-17) -- MODEL EVIDENCE STRIP FROM THE FORECAST SCORECARD (L3)\n",
    " *\n * v1.17.0 (2026-10-17) -- MODEL EVIDENCE STRIP FROM THE FORECAST SCORECARD (L3)\n"
  ],
  [
    "var DT10_VERSION = '1.18.0';",
    "var DT10_VERSION = '1.17.0';"
  ],
  [
    "// ---------------------------------------------------------------------------\n// v1.18.0 [L4] -- send the scorecard cohorts to the backend evidence gate\n// ---------------------------------------------------------------------------\n/** v1.18.0 [L4]: Script Property DT10_EVIDENCE_SCORECARD_SEND = 'on' -> the\n * request carries evidence_scorecard; anything else -> off (request\n * byte-identical to v1.17.0). Never throws. */\nfunction dt10EvidenceScorecardSendOn_() {\n  try {\n    return String(PropertiesService.getScriptProperties()\n        .getProperty('DT10_EVIDENCE_SCORECARD_SEND') || '')\n        .replace(/^\\s+|\\s+$/g, '').toLowerCase() === 'on';\n  } catch (eEss) {}\n  return false;\n}\n/** v1.18.0 [L4] PURE: compact rows [{horizon, cohort, n, hit}] of the\n * 'Investability x Recommendation' dimension from a dt10ScorecardIndex_\n * result; [] when absent; at most 400 rows (largest n first). */\nfunction dt10EvidenceScorecardRows_(idx) {\n  var out = [];\n  if (!idx || !idx.map) return out;\n  var prefix = 'Investability x Recommendation|';\n  for (var key in idx.map) {\n    if (!idx.map.hasOwnProperty(key) || key.indexOf(prefix) !== 0) continue;\n    var rest = key.slice(prefix.length);\n    var bar = rest.indexOf('|');\n    if (bar < 0) continue;\n    var e = idx.map[key] || {};\n    if (e.hit === null || e.hit === undefined) continue;\n    out.push({ horizon: rest.slice(0, bar), cohort: rest.slice(bar + 1),\n               n: Number(e.n) || 0, hit: Number(e.hit) });\n  }\n  out.sort(function (a, b) { return b.n - a.n; });\n  return out.slice(0, 400);\n}\n// ---------------------------------------------------------------------------\n// v1.17.0 [L3] -- model-evidence strip from the _Forecast_Scorecard tab\n",
    "// ---------------------------------------------------------------------------\n// v1.17.0 [L3] -- model-evidence strip from the _Forecast_Scorecard tab\n"
  ],
  [
    "  var body = {\n    criteria: dt10CriteriaFromPanel_(panel),\n    fx_rates: dt10FxRates_(ss),\n    portfolio: dt10PortfolioInputs_(ss, panel),\n    pool_limit: poolLimit\n  };\n  // v1.18.0 [L4]: the cockpit's copy of the Forecast Scorecard cohorts for the\n  // backend evidence gate (property-gated; off -> body unchanged).\n  var scorecardNote = '';\n  if (dt10EvidenceScorecardSendOn_()) {\n    var escRows = dt10EvidenceScorecardRows_(\n        dt10ScorecardIndex_(dt10ReadScorecard_(ss)));\n    if (escRows.length) {\n      body.evidence_scorecard = escRows;\n      scorecardNote = ' | scorecard sent (' + escRows.length + ' cohorts)';\n    } else {\n      scorecardNote = ' | scorecard: no cohorts to send';\n    }\n  }\n",
    "  var body = {\n    criteria: dt10CriteriaFromPanel_(panel),\n    fx_rates: dt10FxRates_(ss),\n    portfolio: dt10PortfolioInputs_(ss, panel),\n    pool_limit: poolLimit\n  };\n"
  ],
  [
    "      poolNote + heldNote + cashNote + scorecardNote + ' | ' + dt10MetaLine_(payload.meta) +   // v1.18.0 [L4]\n",
    "      poolNote + heldNote + cashNote + ' | ' + dt10MetaLine_(payload.meta) +\n"
  ],
  [
    "  // v1.18.0 [L4]: compact rows from a synthetic index (largest n first).\n  var escT = dt10EvidenceScorecardRows_({ asOf: 'x', rows: 3, map: {\n      'Investability x Recommendation|1M|INVESTABLE|BUY': { n: 671, hit: 63.3 },\n      'Investability x Recommendation|3M|WATCHLIST|HOLD': { n: 5000, hit: 47 },\n      'Investability|1M|INVESTABLE': { n: 963, hit: 47.1 } } });\n  report.push('scorecard rows core: ' +\n      ((escT.length === 2 && escT[0].cohort === 'WATCHLIST|HOLD' && escT[0].horizon === '3M' &&\n        escT[1].cohort === 'INVESTABLE|BUY' && escT[1].n === 671 && escT[1].hit === 63.3 &&\n        dt10EvidenceScorecardRows_(null).length === 0) ? 'ok' : 'FAIL') +\n      ' | send (DT10_EVIDENCE_SCORECARD_SEND): ' + (dt10EvidenceScorecardSendOn_() ? 'on' : 'off (default)'));\n  report.push('seat truth (P-149 / KPI cell): ' +\n",
    "  report.push('seat truth (P-149 / KPI cell): ' +\n"
  ]
];
function revertV1180(source) {
  let prior = source;
  for (const [current, previous] of EDITS_1180) {
    assert.equal(prior.split(current).length - 1, 1,
      'exactly one approved v1.18.0 edit: ' + current.split('\n')[0].slice(0, 80));
    prior = prior.replace(current, previous);
  }
  return prior;
}
function addedLines1180() {
  const out = [];
  for (const [current, previous] of EDITS_1180) {
    const prev = new Set(previous.split('\n'));
    for (const line of current.split('\n')) if (!prev.has(line)) out.push(line);
  }
  return out;
}
module.exports = {revertV1180, addedLines1180, EDITS_1180};
