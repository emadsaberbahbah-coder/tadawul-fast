/* v1.14.0 (2026-10-17) reversal layer for the hash-proven history chain.
 * Every v1.14.0 edit (T1 investability-reason table, T5 earnings hold,
 * T6 turnover token + SelLog column) is reversed exactly once; the result
 * is the v1.13.9 source, which dt10_cash_utc_history.js then reverses to
 * the pinned v1.13.8 digest. Generated with the edits (one source of truth);
 * a test fixture, never runtime code. */
'use strict';
const assert = require('node:assert/strict');
const EDITS_1140 = [
  [
    " * Version: 1.14.0 (see DT10_VERSION; header kept in lockstep",
    " * Version: 1.13.9 (see DT10_VERSION; header kept in lockstep"
  ],
  [
    " *\n * v1.14.0 (2026-10-17) -- INVESTABILITY REASONS, EARNINGS HOLD, TURNOVER TOKEN (T1/T5/T6)\n * WHY (Codex brief 2026-10-17): three cockpit halves whose builder halves\n * landed in opportunity_builder v1.27.0/v1.28.0 and run_shadow_scorer\n * v1.10.0. (T1) DATA GAPS said only 'Investability 466 (93.2%)'; the\n * builder now publishes meta.investability_reasons (top engine reasons,\n * counts summing to that row) and this page renders it as a second table\n * directly under DATA GAPS. (T5) A sized ticket could be issued into an\n * earnings print; under TFB_T10_EARNINGS_GATE=enforce the builder marks\n * in-window seats sizing_hold='earnings' and this page applies the\n * P-145 dash vocabulary to them (seat kept, clocks untouched) and prints\n * the status token 'earn-held n'. (T6) Seat turnover is the S-1 fresh-\n * floor lever (October: 4/4 exclusions fresh-floor); the status line\n * gains 'turnover=N/M new' (seats whose stability clock started today,\n * _stab_days === 1; stable across same-day reruns) and _Selection_Log\n * gains column 34 'New Today' (YES/NO/blank; header healed on existing\n * sheets; positional withhold indexes untouched). All three default ON\n * -- display/read-back only, no selection, funding or clock change; the\n * sizing hold only follows a builder marker that is itself dark. KILL\n * (Script Properties, '1' each): DT10_INVEST_REASONS_LEGACY,\n * DT10_EARN_HOLD_LEGACY, DT10_TURNOVER_TOKEN_LEGACY. Functions added (0\n * removed): dt10InvestReasonsLegacy_, dt10InvestReasonRows_,\n * dt10EarnHoldLegacy_, dt10EarningsSuspend_, dt10ApplyEarningsHold_,\n * dt10TurnoverLegacy_, dt10TurnoverNote_, dt10SelLogNewToday_; var added:\n * DT10_INVREASON_HEADERS. Harness: tests/js/dt10_v1140_reasons_earnings_\n * turnover.test.js (+ tests/js/dt10_v1140_history.js reverses these edits\n * so the v1.13.9 hash chain still proves the file). DT10_VERSION and the\n * header bumped in lockstep.\n *\n * v1.13.9 (2026-10-14) -- ENTERED CASH LABEL AND HOLDING UTC PROVENANCE\n",
    " *\n * v1.13.9 (2026-10-14) -- ENTERED CASH LABEL AND HOLDING UTC PROVENANCE\n"
  ],
  [
    "var DT10_VERSION = '1.14.0';",
    "var DT10_VERSION = '1.13.9';"
  ],
  [
    "  'Stability', 'Days',\n  // v1.14.0 [T6] appended (pre-existing rows keep blanks here):\n  'New Today'];\n",
    "  'Stability', 'Days'];\n"
  ],
  [
    "          dt10Cell_(t._stab_status), dt10Cell_(t._stab_days),\n          dt10SelLogNewToday_(t)];   // v1.14.0 [T6]\n",
    "          dt10Cell_(t._stab_status), dt10Cell_(t._stab_days)];\n"
  ],
  [
    "  if (String(sh.getRange(1, 32).getValue() || '') === '') {\n    sh.getRange(1, 32, 1, 2).setValues([['Stability', 'Days']])\n        .setFontWeight('bold');\n  }\n  // v1.14.0 [T6]: heal column 34 on existing sheets (same pattern).\n  if (String(sh.getRange(1, 34).getValue() || '') === '') {\n    sh.getRange(1, 34).setValue('New Today').setFontWeight('bold');\n  }\n",
    "  if (String(sh.getRange(1, 32).getValue() || '') === '') {\n    sh.getRange(1, 32, 1, 2).setValues([['Stability', 'Days']])\n        .setFontWeight('bold');\n  }\n"
  ],
  [
    "// ---------------------------------------------------------------------------\n// v1.14.0 [T1 / T5 / T6] -- pure cores + kill switches (Codex brief 2026-10-17)\n// ---------------------------------------------------------------------------\nvar DT10_INVREASON_HEADERS = ['Engine Reason', 'Candidates', 'Share %',\n  'Example Symbols'];\n/** v1.14.0 [T1]: Script Property kill switch. Absent/junk -> table ON;\n * DT10_INVEST_REASONS_LEGACY = '1' -> legacy (v1.13.9: no table). Never throws. */\nfunction dt10InvestReasonsLegacy_() {\n  try {\n    return String(PropertiesService.getScriptProperties()\n        .getProperty('DT10_INVEST_REASONS_LEGACY') || '') === '1';\n  } catch (eIrl) {}\n  return false;\n}\n/** v1.14.0 [T1] PURE: table rows from the builder's meta.investability_reasons\n * block ({total, rows: [{reason, count, share_pct, examples[]}]}) -- the rows\n * sum to the DATA GAPS Investability row by construction. Any missing or odd\n * shape -> [] (the section is then skipped). Never throws. */\nfunction dt10InvestReasonRows_(meta) {\n  var out = [];\n  try {\n    var blk = meta && meta.investability_reasons;\n    var rows = blk && blk.rows;\n    if (!Array.isArray(rows) || !rows.length) return out;\n    for (var i = 0; i < rows.length; i++) {\n      var r = rows[i] || {};\n      var ex = (r.examples && r.examples.length) ? r.examples.join(', ') : '';\n      out.push([dt10Cell_(r.reason), dt10Cell_(r.count),\n                (typeof r.share_pct === 'number') ? r.share_pct : 0, ex]);\n    }\n  } catch (eIrr) {\n    return [];\n  }\n  return out;\n}\n/** v1.14.0 [T5]: kill switch. DT10_EARN_HOLD_LEGACY = '1' -> the builder's\n * sizing_hold markers are ignored (v1.13.9 behaviour). Never throws. */\nfunction dt10EarnHoldLegacy_() {\n  try {\n    return String(PropertiesService.getScriptProperties()\n        .getProperty('DT10_EARN_HOLD_LEGACY') || '') === '1';\n  } catch (eEhl) {}\n  return false;\n}\n/** v1.14.0 [T5] PURE: suspend sizing on a seat the builder marked\n * sizing_hold === 'earnings' (TFB_T10_EARNINGS_GATE=enforce) -- the\n * dt10FastTrackSuspend_ / dt10GraceSuspend_ dash vocabulary (P-145 family).\n * The seat is KEPT (an earnings hold is a pause, not an exit; stability\n * clocks are untouched); _earn_hold = true is the countable read-back;\n * funds_from is blanked on a COPY (P-77 family). Mutates and returns tk. */\nfunction dt10EarningsSuspend_(tk) {\n  var days = (tk.earnings_hold_days === 0 || tk.earnings_hold_days) ?\n      String(tk.earnings_hold_days) : '?';\n  tk._earn_hold = true;\n  tk.entry_zone = '\\u2014';\n  tk.suggested_sar = '\\u2014';\n  tk.suggested_shares = '\\u2014';\n  tk.stop_sar = '\\u2014';\n  tk.tp1_sar = '\\u2014';\n  tk.tp2_sar = '\\u2014';\n  tk.exp_gain_12m_sar = '\\u2014';\n  if (tk.detail && typeof tk.detail === 'object') {\n    var dcp = {};\n    for (var dk in tk.detail) {\n      if (tk.detail.hasOwnProperty(dk)) dcp[dk] = tk.detail[dk];\n    }\n    dcp.funds_from = '\\u2014';\n    tk.detail = dcp;\n  }\n  tk.advisor_note = 'EARNINGS HOLD (' + days + 'd to print) \\u2014 sizing ' +\n      'suspended until the earnings window clears (seat kept; stability ' +\n      'clocks unaffected; not an executable ticket today).';\n  return tk;\n}\n/** v1.14.0 [T5]: apply the hold to every marked seat of the final board;\n * returns the status token 'earn-held n' ('' when none, or legacy). Never\n * throws; a failure leaves the board as it was. */\nfunction dt10ApplyEarningsHold_(payload) {\n  try {\n    if (dt10EarnHoldLegacy_()) return '';\n    var sel = (payload && payload.selected) || [];\n    var n = 0;\n    for (var i = 0; i < sel.length; i++) {\n      var tk = sel[i];\n      if (tk && tk.sizing_hold === 'earnings') {\n        dt10EarningsSuspend_(tk);\n        n++;\n      }\n    }\n    return n ? 'earn-held ' + n : '';\n  } catch (eEh) {\n    return '';\n  }\n}\n/** v1.14.0 [T6]: kill switch. DT10_TURNOVER_TOKEN_LEGACY = '1' -> no\n * 'turnover=' status token (the SelLog column is unaffected). Never throws. */\nfunction dt10TurnoverLegacy_() {\n  try {\n    return String(PropertiesService.getScriptProperties()\n        .getProperty('DT10_TURNOVER_TOKEN_LEGACY') || '') === '1';\n  } catch (eTol) {}\n  return false;\n}\n/** v1.14.0 [T6] PURE: 'turnover=N/M new' -- seats of today's final board\n * whose stability clock started today (_stab_days === 1: confirmed entrants,\n * fast-track fills and displacement entrants alike). Epoch-keyed, so the\n * value is stable across same-day reruns (unlike the stab note's 'N new',\n * which counts only confirmed entrants and empties on a rerun). '' when the\n * board is empty, stability is OFF/frozen (no _stab_* fields) or legacy. */\nfunction dt10TurnoverNote_(payload) {\n  try {\n    if (dt10TurnoverLegacy_()) return '';\n    var sel = (payload && payload.selected) || [];\n    if (!sel.length) return '';\n    var n = 0, known = 0;\n    for (var i = 0; i < sel.length; i++) {\n      var t = sel[i] || {};\n      if (t._stab_days === 0 || t._stab_days) {\n        known++;\n        if (Number(t._stab_days) === 1) n++;\n      }\n    }\n    if (!known) return '';\n    return 'turnover=' + n + '/' + sel.length + ' new';\n  } catch (eTo) {\n    return '';\n  }\n}\n/** v1.14.0 [T6] PURE: the _Selection_Log 'New Today' cell -- 'YES' when the\n * seat's stability clock started today, 'NO' when older, '' when unknown. */\nfunction dt10SelLogNewToday_(t) {\n  t = t || {};\n  if (!(t._stab_days === 0 || t._stab_days)) return '';\n  return Number(t._stab_days) === 1 ? 'YES' : 'NO';\n}\n/** v1.11.9 [P-145]: suspend sizing on a GRACE-labeled seat that is\n",
    "/** v1.11.9 [P-145]: suspend sizing on a GRACE-labeled seat that is\n"
  ],
  [
    "  if (resp.local !== true) dt10ReallocateBoard_(payload, body);\n  dt10FinalizeBoard_(payload);\n  var earnHoldNote = dt10ApplyEarningsHold_(payload);   // v1.14.0 [T5]\n  var turnoverNote = dt10TurnoverNote_(payload);        // v1.14.0 [T6]\n",
    "  if (resp.local !== true) dt10ReallocateBoard_(payload, body);\n  dt10FinalizeBoard_(payload);\n"
  ],
  [
    "      (stab && stab.note ? ' | ' + stab.note : '') +\n      (turnoverNote ? ' | ' + turnoverNote : '') +      // v1.14.0 [T6]\n      (earn && earn.note ? ' | ' + earn.note : '') +\n      (earnHoldNote ? ' | ' + earnHoldNote : '') +      // v1.14.0 [T5]\n",
    "      (stab && stab.note ? ' | ' + stab.note : '') +\n      (earn && earn.note ? ' | ' + earn.note : '') +\n"
  ],
  [
    "  row = gapT.next + 1;\n  // v1.14.0 [T1]: WHY the Investability gate fails -- the builder's\n  // meta.investability_reasons (top engine reasons; counts sum to the DATA\n  // GAPS Investability row). Absent block or kill switch -> nothing drawn.\n  var irRows = dt10InvestReasonRows_(payload.meta);\n  if (irRows.length && !dt10InvestReasonsLegacy_()) {\n    var irBlk = payload.meta.investability_reasons;\n    row = dt10WriteSection_(sheet, row,\n        'INVESTABILITY \\u2014 BY ENGINE REASON (' + irBlk.total +\n        ' candidates benched by the engine gate; builder ' +\n        'meta.investability_reasons)', tokens);\n    var irT = dt10WriteTable_(sheet, row, DT10_INVREASON_HEADERS, irRows, 'None');\n    dt10ApplyColFormats_(sheet, irT.firstDataRow, irT.count, {\n      2: DT10_FMT_INT, 3: DT10_FMT_PCT\n    });\n    if (irT.firstDataRow > 0) {\n      sheet.getRange(irT.firstDataRow, DT10_INVREASON_HEADERS.length, irT.count, 1)\n          .setWrap(true).setFontSize(9);\n    }\n    row = irT.next + 1;\n  }\n",
    "  row = gapT.next + 1;\n"
  ],
  [
    "  report.push('v1.14.0 switches: invest reasons ' +\n              (dt10InvestReasonsLegacy_() ? 'OFF (DT10_INVEST_REASONS_LEGACY)' : 'ON') +\n              ' | earn hold ' + (dt10EarnHoldLegacy_() ? 'OFF (DT10_EARN_HOLD_LEGACY)' : 'ON') +\n              ' | turnover token ' + (dt10TurnoverLegacy_() ? 'OFF (DT10_TURNOVER_TOKEN_LEGACY)' : 'ON'));\n  report.push('seat truth (P-149 / KPI cell): ' +\n",
    "  report.push('seat truth (P-149 / KPI cell): ' +\n"
  ]
];
function revertV1140(source) {
  let prior = source;
  for (const [current, previous] of EDITS_1140) {
    assert.equal(prior.split(current).length - 1, 1,
      'exactly one approved v1.14.0 edit: ' + current.split('\n')[0].slice(0, 80));
    prior = prior.replace(current, previous);
  }
  return prior;
}
function addedLines1140() {
  const out = [];
  for (const [current, previous] of EDITS_1140) {
    const prev = new Set(previous.split('\n'));
    for (const line of current.split('\n')) if (!prev.has(line)) out.push(line);
  }
  return out;
}
module.exports = {revertV1140, addedLines1140, EDITS_1140};
