/* v1.15.0 (2026-10-17) reversal layer for the hash-proven history chain.
 * Every v1.15.0 edit (T2 sector cap on carried seats + deferred-seat
 * table + ALL QUALIFIED Cluster column) is reversed exactly once; the
 * result is the v1.14.0 source, which dt10_v1140_history.js reverses to
 * v1.13.9 and dt10_cash_utc_history.js to the pinned v1.13.8 digest.
 * Generated with the edits (one source of truth); a test fixture, never
 * runtime code. */
'use strict';
const assert = require('node:assert/strict');
const EDITS_1150 = [
  [
    " * Version: 1.15.0 (see DT10_VERSION; header kept in lockstep",
    " * Version: 1.14.0 (see DT10_VERSION; header kept in lockstep"
  ],
  [
    " *\n * v1.15.0 (2026-10-17) -- SECTOR CAP ON CARRIED SEATS + CLUSTER COLUMN (T2)\n * WHY (Codex brief 2026-10-17 T2, cockpit half): on the 17-Oct board 5 of\n * 10 seats were Real Estate against panel 'T10: Max Per Sector' = 2. The\n * builder applies the cap at selection (its raw picks honour it), but a\n * carried seat (GRACE ghost held by stability memory, absent from today's\n * raw selection) re-enters from dt10StabCore_ with no cap at all. WHAT:\n * (a) dt10SeatSectorCapApply_ runs right after the core: raw seats count\n * first (never deferred -- the builder already capped them), then carried\n * seats fill what the cap leaves in rank order; the over-cap carried seats\n * are DEFERRED -- taken off payload.selected (so SelLog, KPIs and the\n * board show the capped set), listed in a 'SECTOR CAP -- CARRIED SEATS\n * DEFERRED' table directly under SELECTED with the note 'sector cap --\n * carried seat deferred (<sector> n/cap)', named in the stab token\n * 'seat-cap deferred n (syms)' and in meta.stability.audit\n * .sector_cap_deferred. Membership and clocks are untouched (the state\n * blob is saved before the pass); '' / 'Unknown' sectors never cap\n * (builder v1.0.13 doctrine); an unparseable panel value defers nothing\n * (fail-open). No backfill: the freed seat stays empty today (a fill\n * would be a selection change -- panel decision). (b) ALL QUALIFIED\n * gains a 'Cluster' column (before 'Why Not Selected') from the\n * builder's per-row cluster field ('Banks - Regional x19', v1.29.0);\n * the CANDIDATES grid is unchanged because its header list is the WP-8\n * hidden-tab checksum contract. DEFAULT ON -- the off state is the\n * defect (P-127/P-130 precedent). KILL: Script Property\n * DT10_SEAT_SECTOR_CAP_LEGACY = '1' (no deferral, no table, no token;\n * the Cluster column stays, blank when the builder is legacy).\n * Functions added (0 removed): dt10SeatSectorCapLegacy_,\n * dt10SeatCapSector_, dt10SeatCapDefer_, dt10SeatSectorCapCore_,\n * dt10SeatSectorCapApply_, dt10SeatCapRow_; var added:\n * DT10_SEATCAP_HEADERS. Harness: tests/js/dt10_v1150_cluster_seat_cap\n * .test.js (+ tests/js/dt10_v1150_history.js reverses these edits so the\n * v1.14.0 -> v1.13.9 hash chain still proves the file). dt10SelfTest\n * gains 'seat sector cap core: ok'. DT10_VERSION and the header bumped\n * in lockstep.\n *\n * v1.14.0 (2026-10-17) -- INVESTABILITY REASONS, EARNINGS HOLD, TURNOVER TOKEN (T1/T5/T6)\n",
    " *\n * v1.14.0 (2026-10-17) -- INVESTABILITY REASONS, EARNINGS HOLD, TURNOVER TOKEN (T1/T5/T6)\n"
  ],
  [
    "var DT10_VERSION = '1.15.0';",
    "var DT10_VERSION = '1.14.0';"
  ],
  [
    "  'ROI % (TP1)', 'Engine ROI % (12M)', 'Ann ROI %', 'R/R (TP2)', 'Rel', 'DQ', 'Risk', 'Score', 'Selected',\n  'Cluster',   // v1.15.0 [T2]: builder per-row cluster ('Banks - Regional x19')\n  'Why Not Selected'];\n",
    "  'ROI % (TP1)', 'Engine ROI % (12M)', 'Ann ROI %', 'R/R (TP2)', 'Rel', 'DQ', 'Risk', 'Score', 'Selected',\n  'Why Not Selected'];\n"
  ],
  [
    "          c.selected === true ? 'Yes' : 'No',\n          dt10Cell_(c.cluster),   // v1.15.0 [T2]\n          why];\n",
    "          c.selected === true ? 'Yes' : 'No', why];\n"
  ],
  [
    "// ---------------------------------------------------------------------------\n// v1.15.0 [T2] -- sector cap on CARRIED seats + deferred-seat table (pure cores)\n// ---------------------------------------------------------------------------\nvar DT10_SEATCAP_HEADERS = ['Rank', 'Symbol', 'Name', 'Sector', 'Stability',\n  'Note'];\n/** v1.15.0 [T2]: Script Property kill switch. Absent/junk -> cap ON;\n * DT10_SEAT_SECTOR_CAP_LEGACY = '1' -> v1.14.0 (carried seats bypass the\n * cap). Never throws. */\nfunction dt10SeatSectorCapLegacy_() {\n  try {\n    return String(PropertiesService.getScriptProperties()\n        .getProperty('DT10_SEAT_SECTOR_CAP_LEGACY') || '') === '1';\n  } catch (eScl) {}\n  return false;\n}\n/** v1.15.0 [T2] PURE: the cap bucket of a seat -- its trimmed sector; '' for\n * blank or 'Unknown' (a data gap is not a sector; builder v1.0.13). */\nfunction dt10SeatCapSector_(t) {\n  var sec = String((t && t.sector) || '').replace(/^\\s+|\\s+$/g, '');\n  if (!sec || /^unknown$/i.test(sec)) return '';\n  return sec;\n}\n/** v1.15.0 [T2] PURE: mark one carried seat deferred by the cap. The seat\n * keeps its identity, stability label (suffixed) and clock; the countable\n * marker is _seat_cap_deferred; the original rank is kept for the table.\n * Mutates and returns t. */\nfunction dt10SeatCapDefer_(t, sec, n, cap) {\n  t._seat_cap_deferred = true;\n  t._seat_cap_rank = t.rank;\n  t._seat_cap_note = 'sector cap — carried seat deferred (' + sec + ' ' +\n      n + '/' + cap + ')';\n  t._stab_status = String(t._stab_status || '') + ' - sector cap deferred';\n  t.advisor_note = 'SECTOR CAP — carried seat deferred: ' + sec +\n      ' already holds ' + cap + ' seat(s) (panel T10: Max Per Sector). ' +\n      'Listed, not seated, today; the stability clock is untouched.';\n  return t;\n}\n/** v1.15.0 [T2] PURE: apply the panel sector cap to the stability board.\n *   tickets  dt10StabCore_ output (rank order)\n *   rawSyms  { SYM: true } -- today's raw builder picks (already capped by\n *            the builder among themselves; NEVER deferred here)\n *   cap      panel 'T10: Max Per Sector' (int > 0; anything else -> no-op)\n * Raw seats count first; carried seats then fill what the cap leaves, in\n * rank order; the rest are deferred. Kept seats are re-ranked 1..N.\n * Returns { kept: [tickets], deferred: [tickets] }. */\nfunction dt10SeatSectorCapCore_(tickets, rawSyms, cap) {\n  var res = { kept: [], deferred: [] };\n  tickets = tickets || [];\n  rawSyms = rawSyms || {};\n  cap = parseInt(cap, 10);\n  if (!(cap > 0)) { res.kept = tickets.slice(); return res; }\n  var counts = {};\n  var i, t, sec, sym;\n  for (i = 0; i < tickets.length; i++) {\n    t = tickets[i] || {};\n    sym = dt10NormSym_(t.symbol);\n    sec = dt10SeatCapSector_(t);\n    if (sec && rawSyms[sym] === true) counts[sec] = (counts[sec] || 0) + 1;\n  }\n  for (i = 0; i < tickets.length; i++) {\n    t = tickets[i] || {};\n    sym = dt10NormSym_(t.symbol);\n    sec = dt10SeatCapSector_(t);\n    if (!sec || rawSyms[sym] === true) { res.kept.push(t); continue; }\n    var n = counts[sec] || 0;\n    if (n >= cap) {\n      res.deferred.push(dt10SeatCapDefer_(t, sec, n + 1, cap));\n    } else {\n      counts[sec] = n + 1;\n      res.kept.push(t);\n    }\n  }\n  if (res.deferred.length) {\n    for (i = 0; i < res.kept.length; i++) {\n      if (res.kept[i] && (res.kept[i].rank === 0 || res.kept[i].rank)) {\n        res.kept[i].rank = i + 1;\n      }\n    }\n  }\n  return res;\n}\n/** v1.15.0 [T2]: run the cap on the core output IN PLACE -- out.tickets\n * becomes the kept set, out.audit.sector_cap_deferred names the deferred\n * symbols, payload._dt10_seat_cap_deferred carries the deferred tickets for\n * the table under SELECTED. Returns the status token ('' when nothing was\n * deferred, under the kill switch, or on any error -- the board is then\n * exactly the core's). */\nfunction dt10SeatSectorCapApply_(out, rawTickets, panel, payload) {\n  try {\n    if (dt10SeatSectorCapLegacy_()) return '';\n    if (!out || !out.tickets) return '';\n    var rawSyms = {};\n    rawTickets = rawTickets || [];\n    for (var i = 0; i < rawTickets.length; i++) {\n      var rs = dt10NormSym_(rawTickets[i] && rawTickets[i].symbol);\n      if (rs) rawSyms[rs] = true;\n    }\n    var res = dt10SeatSectorCapCore_(out.tickets, rawSyms,\n        panel && panel['T10: Max Per Sector']);\n    if (!res.deferred.length) return '';\n    out.tickets = res.kept;\n    var syms = [];\n    for (var d = 0; d < res.deferred.length; d++) {\n      syms.push(String(res.deferred[d].symbol || ''));\n    }\n    if (out.audit && typeof out.audit === 'object') {\n      out.audit.sector_cap_deferred = syms;\n    }\n    if (payload && typeof payload === 'object') {\n      payload._dt10_seat_cap_deferred = res.deferred;\n    }\n    return 'seat-cap deferred ' + syms.length + ' (' + dt10JoinCap_(syms, 4) + ')';\n  } catch (eSca) {\n    return '';\n  }\n}\n/** v1.15.0 [T2] PURE: one DT10_SEATCAP_HEADERS row from a deferred seat. */\nfunction dt10SeatCapRow_(t) {\n  t = t || {};\n  return [dt10Cell_(t._seat_cap_rank), dt10Cell_(t.symbol), dt10Cell_(t.name),\n          dt10Cell_(t.sector), dt10Cell_(t._stab_status),\n          dt10Cell_(t._seat_cap_note)];\n}\n// ---------------------------------------------------------------------------\n// v1.14.0 [T1 / T5 / T6] -- pure cores + kill switches (Codex brief 2026-10-17)\n",
    "// ---------------------------------------------------------------------------\n// v1.14.0 [T1 / T5 / T6] -- pure cores + kill switches (Codex brief 2026-10-17)\n"
  ],
  [
    "                          state, knobs, limit, epoch.key, outageMap);\n  // v1.15.0 [T2]: the panel sector cap applies to CARRIED seats too (the\n  // builder capped only today's raw picks). Over-cap carried seats are\n  // listed under SELECTED, not seated; membership and clocks untouched.\n  var seatCapNote = dt10SeatSectorCapApply_(out, payload.selected || [],\n                                            panel, payload);\n  if (seatCapNote) out.note += ' | ' + seatCapNote;\n",
    "                          state, knobs, limit, epoch.key, outageMap);\n"
  ],
  [
    "  row = selT.next + 1;\n  // v1.15.0 [T2]: carried seats the panel sector cap deferred this run --\n  // listed (rank, symbol, sector, stability, note), never seated or sized.\n  var capDef = payload._dt10_seat_cap_deferred || [];\n  if (capDef.length && !dt10SeatSectorCapLegacy_()) {\n    row = dt10WriteSection_(sheet, row,\n        'SECTOR CAP \\u2014 CARRIED SEATS DEFERRED (' + capDef.length +\n        '; panel T10: Max Per Sector applied to carried seats)', tokens);\n    var capRows = [];\n    for (var cd = 0; cd < capDef.length; cd++) {\n      capRows.push(dt10SeatCapRow_(capDef[cd]));\n    }\n    var capT = dt10WriteTable_(sheet, row, DT10_SEATCAP_HEADERS, capRows, 'None');\n    if (capT.firstDataRow > 0) {\n      sheet.getRange(capT.firstDataRow, DT10_SEATCAP_HEADERS.length, capT.count, 1)\n          .setWrap(true).setFontSize(9);\n    }\n    row = capT.next + 1;\n  }\n  // ALL QUALIFIED — full INVEST opportunity set (v1.2.0). Every name that\n",
    "  row = selT.next + 1;\n  // ALL QUALIFIED — full INVEST opportunity set (v1.2.0). Every name that\n"
  ],
  [
    "  // v1.15.0 [T2]: raw A counts first; carried B fills the second slot;\n  // carried C, D deferred; Unknown E never caps; kept re-ranked 1..3.\n  var scT = dt10SeatSectorCapCore_([\n      { symbol: 'A', sector: 'Real Estate', rank: 1, _stab_status: 'ACTIVE (day 9)' },\n      { symbol: 'B', sector: 'Real Estate', rank: 2, _stab_status: 'GRACE (1/3 missed)' },\n      { symbol: 'C', sector: 'Real Estate', rank: 3, _stab_status: 'GRACE (2/3 missed)' },\n      { symbol: 'D', sector: 'Real Estate', rank: 4, _stab_status: 'GRACE (1/3 missed)' },\n      { symbol: 'E', sector: 'Unknown', rank: 5, _stab_status: 'GRACE (1/3 missed)' }],\n      { A: true }, 2);\n  var scNo = dt10SeatSectorCapCore_([{ symbol: 'A', sector: 'X', rank: 1 }], {}, 'junk');\n  report.push('seat sector cap core: ' +\n      ((scT.kept.length === 3 && scT.deferred.length === 2 &&\n        scT.deferred[0].symbol === 'C' && scT.deferred[1].symbol === 'D' &&\n        scT.kept[2].symbol === 'E' && scT.kept[2].rank === 3 &&\n        scT.deferred[0]._seat_cap_rank === 3 &&\n        scT.deferred[0]._stab_status === 'GRACE (2/3 missed) - sector cap deferred' &&\n        scNo.kept.length === 1 && scNo.deferred.length === 0) ? 'ok' : 'FAIL') +\n      ' | ' + (dt10SeatSectorCapLegacy_() ? 'OFF (DT10_SEAT_SECTOR_CAP_LEGACY)' : 'ON'));\n  report.push('seat truth (P-149 / KPI cell): ' +\n",
    "  report.push('seat truth (P-149 / KPI cell): ' +\n"
  ],
  [
    "        (!hsTruth || (hsQual[15].indexOf('Held research seat') === 0 &&   // v1.15.0 [T2]: Why Not Selected moved to col 16 (Cluster inserted)\n          hsQual[15].indexOf('1/3 confirmed') > 0 &&\n          dt10CandToRow_(hsPayload.candidates_rows[0])[24] === hsQual[15])))\n",
    "        (!hsTruth || (hsQual[14].indexOf('Held research seat') === 0 &&\n          hsQual[14].indexOf('1/3 confirmed') > 0 &&\n          dt10CandToRow_(hsPayload.candidates_rows[0])[24] === hsQual[14])))\n"
  ]
];
function revertV1150(source) {
  let prior = source;
  for (const [current, previous] of EDITS_1150) {
    assert.equal(prior.split(current).length - 1, 1,
      'exactly one approved v1.15.0 edit: ' + current.split('\n')[0].slice(0, 80));
    prior = prior.replace(current, previous);
  }
  return prior;
}
function addedLines1150() {
  const out = [];
  for (const [current, previous] of EDITS_1150) {
    const prev = new Set(previous.split('\n'));
    for (const line of current.split('\n')) if (!prev.has(line)) out.push(line);
  }
  return out;
}
module.exports = {revertV1150, addedLines1150, EDITS_1150};
