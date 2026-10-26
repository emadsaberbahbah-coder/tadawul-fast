/* CONFIRM-BASIS (16_Decision_Top10.gs v1.14.0) -- confirmation counts qualified
 * days inside the rank buffer (Script Property TFB_T10_CONFIRM_BASIS, default
 * 'qualified'; 'raw' = the v1.13.11 clocks byte for byte).
 *
 * Dual-tree node harness over the REAL file: loads the .gs source in a vm
 * context with minimal GAS shims (PropertiesService/Logger/SpreadsheetApp/
 * Utilities), replays a 2-seat board through the REAL dt10StabCore_ and the
 * REAL dt10ApplyStability_ (service read -> knob), and proves:
 *   T1 GOLDEN NEGATIVE on the v1.13.11 base: the challenger confirmed on days
 *      1-2 and displaced from the raw set on day 3 while still INVEST at rank
 *      12 resets to 0 and is only 'pending 1/3' on day 4 (not seated); a
 *      FAST-TRACK seat displaced the same way restarts at 1/3.
 *   T2 v1.14.0 (default 'qualified'): day 3 counts (ci 3; 'near 1 CHAL(3/3)'
 *      token), day 4 seats 'NEW (confirmed 3/3)' carrying the raw ticket's
 *      sizing; a GRACE member INVEST inside the buffer gets co 0 and ci
 *      advanced; a FAST-TRACK seat graduates on the qualified day and is
 *      'ACTIVE' with sizing when the raw ticket returns; WATCH, structural
 *      block, rank buffer + 1 and rank buffer 0 all fall back to the legacy
 *      branch; rank == buffer counts (boundary); seating still needs a raw
 *      ticket (a qualified-only challenger is never seated).
 *   T3 KILL 'raw': every day of every fixture deep-equals the base file's
 *      output (tickets, state, audit, note), through the pure core AND through
 *      dt10ApplyStability_ (saved blob, payload.selected, audit, note).
 *   T4 embedded dt10SelfTest replay: 'confirm basis core: ok', the basis line
 *      follows the property, every base 'core: ok' line unchanged, zero FAIL.
 *   T5 ES5/ASCII lint of the diff: added lines carry no let/const/=>/`/class,
 *      are ASCII (the version header line keeps only its pre-existing em dash),
 *      no non-ASCII code point is added or removed, lockstep version pins.
 *   T6 inventory: zero functions and zero vars removed vs the base; exactly
 *      the designed names added.
 *   T7 service read: ' RAW ' / 'raw' -> raw; '', 'Raw-ish', junk, absent and a
 *      PropertiesService fault -> qualified; the knob never reaches the
 *      criteria snapshot (dt10StabKnobs_ unchanged).
 *
 * The BASE is the v1.13.11 file rebuilt from the delivered source by reversing
 * exactly the enumerated v1.14.0 edits (tests/js/dt10_cash_utc_history.js,
 * SHA-256 pinned 268b85ad...), or a file given as the second argument, which
 * must carry the same SHA-256. The whole battery runs three times and the
 * three digests must match; the last line is 'PASS k/k'.
 *
 * Usage: node tests/test_dt10_confirm_basis.js [new.gs] [base.gs]
 */
'use strict';
var fs = require('fs'), vm = require('vm'), path = require('path'), crypto = require('crypto');
var ROOT = path.join(__dirname, '..');
var NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '16_Decision_Top10.gs');
var BASE_PATH = process.argv[3] || '';
var history = require(path.join(ROOT, 'tests', 'js', 'dt10_cash_utc_history.js'));
var PROP = 'TFB_T10_CONFIRM_BASIS';
var DASH = '—';

var newSource = fs.readFileSync(NEW_PATH, 'utf8');
function sha256(s) { return crypto.createHash('sha256').update(s).digest('hex'); }
/* The oracle: a supplied v1.13.11 file, else the delivered file with exactly the
 * enumerated v1.14.0 edits reversed (any unlisted change makes this throw). */
var baseSource;
try {
  baseSource = BASE_PATH ? fs.readFileSync(BASE_PATH, 'utf8') : history.v11311Source(newSource);
} catch (eBase) {
  console.log('FAIL T0 base oracle: ' + String(eBase && eBase.message || eBase).split('\n')[0]);
  console.log('FAIL 0/1');
  process.exit(1);
}

/* ------------------------------------------------------------ runner */
var passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n  ')); }
}
function assert(c, m) { if (!c) throw new Error(m || 'assertion failed'); }
function eq(a, b, m) { if (a !== b) throw new Error((m || 'not equal') + ': ' + JSON.stringify(a) + ' !== ' + JSON.stringify(b)); }
function deq(a, b, m) {
  var ja = JSON.stringify(a), jb = JSON.stringify(b);
  if (ja !== jb) {
    var i = 0; while (i < ja.length && ja[i] === jb[i]) i++;
    throw new Error((m || 'deep-equal') + ' differs at ' + i + ': ...' + ja.slice(Math.max(0, i - 60), i + 80) + ' | ...' + jb.slice(Math.max(0, i - 60), i + 80));
  }
}
function plain(v) { return JSON.parse(JSON.stringify(v)); }

/* ------------------------------------------------------------ GAS shims */
function ctx(props, options) {
  options = options || {};
  var store = {}; for (var k in props) if (props.hasOwnProperty(k)) store[k] = props[k];
  var logs = [], reads = {};
  var statusRows = [];
  for (var r = 0; r < 60; r++) statusRows.push(['', '']);
  statusRows[0] = ['TFB Feed Global_Markets', 'OK | cov=99.0 | run=40079965055 | 2026-10-24 06:00:00+03:00'];
  var statusSheet = { getRange: function () { return { getValues: function () { return plain(statusRows); } }; } };
  var ssStub = { getSheetByName: function (n) { return n === '_Status' ? statusSheet : null; },
                 getRangeByName: function () { return null; } };
  var context = {
    PropertiesService: { getScriptProperties: function () {
      if (options.propsThrow) throw new Error('synthetic property outage');
      return { getProperty: function (key) { reads[key] = (reads[key] || 0) + 1; return store.hasOwnProperty(key) ? store[key] : null; },
               setProperty: function (key, v) { store[key] = String(v); },
               deleteProperty: function (key) { delete store[key]; } }; } },
    Logger: { log: function (m) { logs.push(String(m)); } },
    SpreadsheetApp: { getActiveSpreadsheet: function () { return ssStub; },
                      getUi: function () { throw new Error('no ui'); } },
    Utilities: { formatDate: function (d, tz, pattern) { return pattern === 'yyyy-MM-dd' ? '2026-10-24' : '2026-10-24 00:00:00'; } },
    Session: { getScriptTimeZone: function () { return 'Asia/Riyadh'; } },
    UrlFetchApp: { fetch: function () { throw new Error('no net'); } }
  };
  vm.createContext(context);
  context.__store = store; context.__logs = logs; context.__reads = reads;
  return context;
}
function load(source, props, options) {
  var c = ctx(props || {}, options);
  vm.runInContext(source, c, { filename: '16_Decision_Top10.gs' });
  return c;
}

/* ------------------------------------------------------------ fixtures */
/* Board of 2 seats. Audited grid ranks (opportunity_score desc): NEWC 97 (1),
 * A 95 (2), F1..F12 94.5..83.5 (3..14 around CHAL), CHAL 86 (12), B 84 (15 =
 * the buffer boundary). Raw (sized top-2): d1 [A, CHAL], d2 [A, CHAL],
 * d3 [A, NEWC] (CHAL displaced), d4 [A, CHAL]. */
var DAYS = ['2026-10-21', '2026-10-22', '2026-10-23', '2026-10-24'];
function knobs(basis, buf) {
  var k = { enabled: true, confirm_days: 3, exit_days: 3, rank_buffer: (buf === undefined ? 15 : buf),
            smooth_days: 5, hard_strict: true };
  if (basis !== undefined) k.confirm_basis = basis;
  return k;
}
function cand(sym, sc, verdict, extra) {
  var c = { symbol: sym, name: sym, market: 'NYSE/NASDAQ', sector: 'Financial Services', currency: 'USD',
            opportunity_score: sc, verdict: (verdict === undefined ? 'INVEST' : verdict), price: 10, price_sar: 37.5,
            roi_pct: 9, engine_roi_pct: 18, ann_roi_pct: 12, reliability: 75, dq: 100, confidence_band: 'High' };
  if (extra) for (var k in extra) if (extra.hasOwnProperty(k)) c[k] = extra[k];
  return c;
}
function ticket(sym, sc) {
  return { symbol: sym, name: sym, market: 'NYSE/NASDAQ', sector: 'Financial Services', currency: 'USD', fx_to_sar: 3.75,
           price: 10, price_sar: 37.5, entry_zone: '36-39 SAR', suggested_sar: 5000, suggested_shares: 133,
           stop_sar: 33, tp1_sar: 44, tp2_sar: 48, roi_pct: 9, engine_roi_pct: 18, ann_roi_pct: 12,
           exp_gain_12m_sar: 900, reliability: 75, dq: 100, confidence_band: 'High', opportunity_score: sc,
           advisor_note: 'INVEST - 5,000 SAR (133 sh)', detail: { funds_from: 'Cash 5,000 SAR', review_date: '2026-11-24' } };
}
function grid(chalScore, chalVerdict, chalExtra) {
  var g = [cand('NEWC.US', 97), cand('A.US', 95)];
  for (var i = 1; i <= 12; i++) g.push(cand('F' + i + '.US', 95.5 - i));
  g.push(cand('B.US', 84));
  g.push(cand('CHAL.US', chalScore === undefined ? 86 : chalScore, chalVerdict, chalExtra));
  return g;
}
function seed() {
  return { v: 1, date: '2026-10-20', symbols: {
    'A.US': { ci: 3, co: 0, member: true, since: '2026-10-15', ls: '2026-10-20', hist: [95], ft: false },
    'B.US': { ci: 3, co: 1, member: true, since: '2026-10-15', ls: '2026-10-20', hist: [84], ft: false } } };
}
function raws() {
  return [[ticket('A.US', 95), ticket('CHAL.US', 86)],
          [ticket('A.US', 95), ticket('CHAL.US', 86)],
          [ticket('A.US', 95), ticket('NEWC.US', 97)],
          [ticket('A.US', 95), ticket('CHAL.US', 86)]];
}
/* Owner's live case: a FAST-TRACK seat (FTM, seated day 0 at 1/3) confirmed
 * on days 1-2, displaced from raw by NEWC on day 3 while INVEST at rank 3,
 * back in raw on day 4. Grid: NEWC 97, A 95, FTM 90, B.. not present. */
function ftSeed() {
  return { v: 1, date: '2026-10-20', symbols: {
    'A.US': { ci: 3, co: 0, member: true, since: '2026-10-15', ls: '2026-10-20', hist: [95], ft: false },
    'FTM.US': { ci: 0, co: 0, member: true, since: '2026-10-20', ls: '2026-10-20', hist: [90], ft: true } } };
}
function ftGrid() { return [cand('NEWC.US', 97), cand('A.US', 95), cand('FTM.US', 90)]; }
function ftRaws() {
  return [[ticket('A.US', 95), ticket('FTM.US', 90)],
          [ticket('A.US', 95), ticket('FTM.US', 90)],
          [ticket('A.US', 95), ticket('NEWC.US', 97)],
          [ticket('A.US', 95), ticket('FTM.US', 90)]];
}
/* Replay: returns one {tickets, state, audit, note} per day through the REAL
 * pure core of context g; grids[d] / rawsArr[d] per day; limit 2. */
function replay(g, k, seedObj, rawsArr, grids) {
  var st = g.dt10StabParseState_(plain(seedObj));
  var outs = [];
  for (var d = 0; d < DAYS.length; d++) {
    var o = g.dt10StabCore_(plain(rawsArr[d]), plain(grids[d]), st, plain(k), 2, DAYS[d]);
    outs.push(plain({ tickets: o.tickets, state: o.state, audit: o.audit, note: o.note }));
    st = g.dt10StabParseState_(plain(o.state));
  }
  return outs;
}
function grids4(day3) { var base = grid(); return [plain(base), plain(base), plain(day3 || base), plain(base)]; }
function ftGrids4() { var b = ftGrid(); return [b, b, b, b]; }
function bySym(out, s) { for (var i = 0; i < out.tickets.length; i++) if (out.tickets[i].symbol === s) return out.tickets[i]; return null; }
function sym(out, s) { return out.state.symbols[s]; }

/* All scenario replays for one context under one basis (undefined = knob absent). */
function scenarios(g, basis) {
  return {
    main: replay(g, knobs(basis), seed(), raws(), grids4()),
    watch: replay(g, knobs(basis), seed(), raws(), grids4(grid(86, 'WATCH'))),
    blocked: replay(g, knobs(basis), seed(), raws(), grids4(grid(86, 'INVEST', { structural_block: true }))),
    out16: replay(g, knobs(basis), seed(), raws(), grids4(grid(83))),
    edge15: replay(g, knobs(basis), seed(), raws(), grids4(grid(84.2))),
    buf0: replay(g, knobs(basis, 0), seed(), raws(), grids4()),
    absent: replay(g, knobs(basis), seed(), raws(), grids4([cand('NEWC.US', 97), cand('A.US', 95), cand('B.US', 84)])),
    ft: replay(g, knobs(basis), ftSeed(), ftRaws(), ftGrids4())
  };
}
/* End to end through the REAL dt10ApplyStability_ (service read -> knob,
 * state load/save, epoch from the _Status feed stamp = 2026-10-24). */
function applyE2E(source, props, options) {
  var g = load(source, props, options);
  var seedObj = seed(); seedObj.date = '2026-10-23';
  seedObj.symbols['CHAL.US'] = { ci: 2, co: 0, member: false, since: '', ls: '2026-10-23', hist: [86, 86], ft: false };
  g.__store.DT10_STAB_STATE_V1 = JSON.stringify(seedObj);
  var payload = { selected: [ticket('A.US', 95), ticket('NEWC.US', 97)], candidates_rows: grid(), meta: {} };
  var panel = { 'T10: Max Selected': 2, 'T10: Stability Enabled': 'Yes', 'T10: Stability Confirm Days': 3,
                'T10: Stability Exit Days': 3, 'T10: Stability Rank Buffer': 15, 'T10: Stability Smooth Days': 5 };
  var out = g.dt10ApplyStability_(payload, panel, null);
  var k = plain(payload.meta.stability.knobs); delete k.confirm_basis;
  return { g: g, note: out.note, selected: plain(payload.selected), audit: plain(payload.meta.stability.audit),
           knobs: k, confirm_basis: payload.meta.stability.knobs.confirm_basis,
           saved: JSON.parse(g.__store.DT10_STAB_STATE_V1), reads: plain(g.__reads) };
}

/* ------------------------------------------------------------ battery */
function battery() {
  var N = load(newSource, {});
  var B = load(baseSource, {});
  var nq = scenarios(N, 'qualified'), nr = scenarios(N, 'raw'), nn = scenarios(N, undefined), bb = scenarios(B, undefined);

  test('T0 base oracle is the reviewed v1.13.11 file (SHA-256 pinned); delivered file compiles', function () {
    eq(sha256(baseSource), history.V11311_SHA256, 'base SHA-256');
    eq(B.DT10_VERSION, '1.13.11', 'base version');
    // 2026-10-25/26: the delivered file is v1.16.0 (pending grace and funding evidence stacked on this change); pins moved
    eq(N.DT10_VERSION, '1.16.0', 'delivered version');
    assert(/^ \* Version: 1\.16\.0 /.test(newSource.split('\n')[3]), 'header line 4 lockstep');
    assert(newSource.indexOf("var DT10_VERSION = '1.16.0';") > 0, 'DT10_VERSION literal');
  });

  test('T1 GOLDEN NEGATIVE (v1.13.11): displaced challenger resets to 0 on day 3; pending 1/3, unseated on day 4; displaced FAST-TRACK seat restarts', function () {
    var m = bb.main;
    eq(sym(m[1], 'CHAL.US').ci, 2, 'day 2 ci');
    eq(sym(m[2], 'CHAL.US').ci, 0, 'day 3 ci (defect)');
    eq(sym(m[3], 'CHAL.US').ci, 1, 'day 4 ci');
    eq(bySym(m[3], 'CHAL.US'), null, 'not seated on day 4');
    assert(m[3].note.indexOf('pending CHAL.US(1/3)') > 0, 'day 4 note: ' + m[3].note);
    eq(m[2].note.indexOf('confirm='), -1, 'no basis token');
    eq(m[2].note.indexOf('near'), -1, 'no near token');
    // B: GRACE 1/3, INVEST at rank 15 (inside the buffer): legacy pauses (co stays 1) and ci resets
    eq(sym(m[0], 'B.US').co, 1, 'B co paused'); eq(sym(m[0], 'B.US').ci, 0, 'B ci reset');
    eq(bySym(m[0], 'B.US')._stab_status, 'GRACE (1/3 missed)', 'B label');
    // the owner's case: the fast-track seat restarts
    var f = bb.ft;
    eq(bySym(f[1], 'FTM.US')._stab_status, 'FAST-TRACK (day 3, 2/3 confirmed)', 'ft day 2');
    eq(sym(f[2], 'FTM.US').ci, 0, 'ft day 3 reset'); eq(sym(f[2], 'FTM.US').ft, true, 'ft not graduated');
    eq(bySym(f[2], 'FTM.US')._stab_status, 'GRACE (0/3 missed)', 'ft day 3 label');
    eq(bySym(f[3], 'FTM.US')._stab_status, 'FAST-TRACK (day 5, 1/3 confirmed)', 'ft day 4 restarts');
    eq(bySym(f[3], 'FTM.US').suggested_shares, DASH, 'ft day 4 unsized');
    // the base ignores an unknown knob: 'qualified' on the base = legacy
    deq(scenarios(B, 'qualified'), bb, 'base ignores confirm_basis');
  });

  test('T2 v1.14.0 qualified basis: day 3 counts (ci 3, near token), day 4 NEW 3/3 with raw sizing; GRACE member co 0; FAST-TRACK graduates; legacy fallbacks; seating needs raw', function () {
    var m = nq.main;
    eq(sym(m[0], 'CHAL.US').ci, 1, 'day 1'); eq(sym(m[1], 'CHAL.US').ci, 2, 'day 2');
    eq(sym(m[2], 'CHAL.US').ci, 3, 'day 3 counts'); eq(sym(m[2], 'CHAL.US').co, 0, 'day 3 co');
    eq(sym(m[2], 'CHAL.US').member, false, 'day 3 still a challenger');
    eq(m[2].tickets.length, 2, 'day 3 board size'); eq(bySym(m[2], 'CHAL.US'), null, 'day 3 not seated (no raw ticket)');
    deq(m[2].state.symbols['CHAL.US'].hist, [86, 86, 86], 'score point recorded on the qualified day');
    eq(m[2].note, 'stab: 1 grace, pending NEWC.US(1/3), confirm=qualified, near 1 CHAL.US(3/3)', 'day 3 note');
    var c4 = bySym(m[3], 'CHAL.US');
    assert(c4, 'seated day 4'); eq(c4._stab_status, 'NEW (confirmed 3/3)', 'day 4 label');
    eq(c4.suggested_sar, 5000, 'sizing SAR'); eq(c4.suggested_shares, 133, 'sizing shares'); eq(c4.entry_zone, '36-39 SAR', 'entry zone');
    eq(c4.detail.funds_from, 'Cash 5,000 SAR', 'funds from'); assert(c4._grace_hold !== true && c4._ft_suspended !== true, 'no suspension');
    eq(c4.rank, 2, 'rank re-stamped behind A (smoothed 95 > 86)');
    deq(m[3].audit.entered, ['CHAL.US'], 'entered'); deq(m[3].audit.exited_displaced, ['B.US'], 'B displaced');
    eq(m[3].note, 'stab: 1 new, displaced B.US, confirm=qualified, near 1 NEWC.US(2/3)', 'day 4 note');
    eq(sym(m[3], 'NEWC.US').ci, 2, 'NEWC counted its qualified day 4');
    // (f) GRACE member B (co 1) INVEST at rank 15 (boundary): co 0 and ci advanced on day 1
    eq(sym(m[0], 'B.US').co, 0, 'B co reset'); eq(sym(m[0], 'B.US').ci, 4, 'B ci advanced');
    eq(bySym(m[0], 'B.US')._stab_status, 'GRACE (0/3 missed)', 'B label unchanged vocabulary');
    eq(bySym(m[0], 'B.US').suggested_sar, DASH, 'B ghost unsized');
    // A in raw every day: identical to legacy
    deq([sym(m[0], 'A.US'), sym(m[3], 'A.US')], [sym(bb.main[0], 'A.US'), sym(bb.main[3], 'A.US')], 'A unchanged');
    // owner's case: the FAST-TRACK seat graduates on the qualified day
    var f = nq.ft;
    eq(sym(f[2], 'FTM.US').ci, 3, 'ft day 3 counts'); eq(sym(f[2], 'FTM.US').ft, false, 'graduated');
    eq(bySym(f[2], 'FTM.US')._stab_status, 'GRACE (0/3 missed)', 'ft day 3 label (ghost, no raw ticket)');
    eq(bySym(f[2], 'FTM.US').suggested_sar, DASH, 'ft day 3 unsized');
    eq(bySym(f[3], 'FTM.US')._stab_status, 'ACTIVE (day 5)', 'ft day 4 ACTIVE');
    eq(bySym(f[3], 'FTM.US').suggested_shares, 133, 'ft day 4 sized');
    // legacy fallbacks inside the qualified basis
    eq(sym(nq.watch[2], 'CHAL.US').ci, 0, 'WATCH -> legacy');
    eq(sym(nq.blocked[2], 'CHAL.US').ci, 0, 'structural_block -> legacy');
    eq(sym(nq.out16[2], 'CHAL.US').ci, 0, 'rank 16 (> buffer 15) -> legacy');
    eq(sym(nq.edge15[2], 'CHAL.US').ci, 3, 'rank 15 (== buffer) counts');
    eq(sym(nq.absent[2], 'CHAL.US').ci, 0, 'absent from the grid -> legacy');
    eq(nq.watch[2].note.indexOf('near'), -1, 'no near token when not qualified');
    assert(nq.watch[2].note.indexOf('confirm=qualified') > 0, 'basis token always present');
    // rank buffer 0: clocks, tickets, audit identical to raw; only the basis token differs
    for (var d = 0; d < 4; d++) {
      deq([nq.buf0[d].tickets, nq.buf0[d].state, nq.buf0[d].audit], [nr.buf0[d].tickets, nr.buf0[d].state, nr.buf0[d].audit], 'buffer 0 day ' + d);
      eq(nq.buf0[d].note.replace(', confirm=qualified', ''), nr.buf0[d].note, 'buffer 0 note day ' + d);
    }
    // the near token is capped at 3 entries and ordered by ci desc, rank asc
    var st3 = N.dt10StabParseState_({ v: 1, date: '2026-10-23', symbols: {
      'A.US': { ci: 3, co: 0, member: true, since: '2026-10-15', ls: '2026-10-23', hist: [95], ft: false },
      'B.US': { ci: 3, co: 0, member: true, since: '2026-10-15', ls: '2026-10-23', hist: [84], ft: false },
      'F1.US': { ci: 1, co: 0, member: false, since: '', ls: '2026-10-23', hist: [94.5], ft: false },
      'F2.US': { ci: 2, co: 0, member: false, since: '', ls: '2026-10-23', hist: [93.5], ft: false },
      'F3.US': { ci: 2, co: 0, member: false, since: '', ls: '2026-10-23', hist: [92.5], ft: false },
      'F4.US': { ci: 1, co: 0, member: false, since: '', ls: '2026-10-23', hist: [91.5], ft: false } } });
    var o3 = N.dt10StabCore_([ticket('A.US', 95), ticket('B.US', 84)], grid(), st3, knobs('qualified'), 2, '2026-10-24');
    eq(o3.note, 'stab: steady — no membership change, confirm=qualified, near 4 F2.US(3/3) F3.US(3/3) F1.US(2/3)',
       'near cap/order (the steady text is kept; tokens are appended)');
    // a pure helper check
    var cbs = { 'X.US': cand('X.US', 50) };
    assert(N.dt10StabQualifiedWithin_('X.US', cbs, { 'X.US': 15 }, knobs(), null) === true, 'within at 15');
    assert(N.dt10StabQualifiedWithin_('X.US', cbs, { 'X.US': 16 }, knobs(), null) === false, 'outside at 16');
    assert(N.dt10StabQualifiedWithin_('X.US', cbs, { 'X.US': 1 }, knobs('qualified', 0), null) === false, 'buffer 0');
    assert(N.dt10StabQualifiedWithin_('X.US', cbs, { 'X.US': 1 }, knobs(), function () { return true; }) === false, 'hardOut wins');
    assert(N.dt10StabQualifiedWithin_('X.US', { 'X.US': cand('X.US', 50, 'invest') }, { 'X.US': 1 }, knobs(), null) === true, 'verdict case-insensitive');
    assert(N.dt10StabQualifiedWithin_('Y.US', cbs, { 'Y.US': 1 }, knobs(), null) === false, 'absent symbol');
    assert(N.dt10StabQualifiedWithin_('X.US', null, null, null, null) === false, 'null inputs never throw');
  });

  test('T3 KILL raw: every fixture, every day deep-equals the v1.13.11 base (tickets, state, audit, note); knob absent == raw; dt10ApplyStability_ end to end', function () {
    deq(nr, bb, 'raw basis vs base (all scenarios)');
    deq(nn, bb, 'knob absent vs base (all scenarios)');
    var e2eN = applyE2E(newSource, { TFB_T10_CONFIRM_BASIS: 'raw' }), e2eB = applyE2E(baseSource, { TFB_T10_CONFIRM_BASIS: 'raw' });
    eq(e2eN.confirm_basis, 'raw', 'knob read');
    deq([e2eN.note, e2eN.selected, e2eN.audit, e2eN.saved, e2eN.knobs], [e2eB.note, e2eB.selected, e2eB.audit, e2eB.saved, e2eB.knobs], 'e2e raw vs base');
    eq(e2eN.saved.symbols['CHAL.US'].ci, 0, 'e2e raw: displaced challenger reset');
    eq(e2eN.reads[PROP], 1, 'one property read per apply');
    eq(e2eB.reads[PROP], undefined, 'base never reads the property');
    // the base's state blob shape is kept: no new field is written by the qualified basis either
    var e2eQ = applyE2E(newSource, {});
    deq(Object.keys(e2eQ.saved.symbols['CHAL.US']).sort(), Object.keys(e2eB.saved.symbols['CHAL.US']).sort(), 'state field set unchanged');
    eq(e2eQ.saved.symbols['CHAL.US'].ci, 3, 'e2e qualified: displaced challenger keeps counting');
    assert(e2eQ.note.indexOf('near 1 CHAL.US(3/3)') > 0 && e2eQ.note.indexOf('confirm=qualified') > 0 && e2eQ.note.indexOf(' | epoch=2026-10-24/feed') > 0, 'e2e note: ' + e2eQ.note);
    deq(e2eQ.selected.map(function (t) { return t.symbol; }), e2eB.selected.map(function (t) { return t.symbol; }), 'e2e board (no seating change on a qualified day)');
  });

  test('T4 embedded dt10SelfTest: confirm basis battery ok, basis line follows the property, every base core line unchanged, zero FAIL', function () {
    var rep = String(N.dt10SelfTest()), repB = String(B.dt10SelfTest());
    assert(rep.indexOf('confirm basis core: ok') > 0, 'battery line');
    assert(rep.indexOf('confirm basis (TFB_T10_CONFIRM_BASIS): qualified') > 0, 'basis line default');
    assert(rep.indexOf('[DT10 v1.16.0] self-test') === 0, 'version line');
    assert(rep.indexOf(': FAIL') < 0 && rep.indexOf('FAIL [') < 0, 'no FAIL');
    var baseLines = repB.split('\n'), newLines = rep.split('\n');
    var missing = baseLines.filter(function (l) { return l.indexOf('[DT10 v1.13.11]') !== 0 && newLines.indexOf(l) < 0; });
    deq(missing, [], 'every base self-test line present');
    var cores = baseLines.filter(function (l) { return / core: ok/.test(l); });
    assert(cores.length >= 27, 'base core lines ' + cores.length);
    var raw = String(load(newSource, { TFB_T10_CONFIRM_BASIS: ' RAW ' }).dt10SelfTest());
    assert(raw.indexOf('confirm basis (TFB_T10_CONFIRM_BASIS): raw') > 0 && raw.indexOf('confirm basis core: ok') > 0 && raw.indexOf('FAIL') < 0, 'raw property line');
  });

  test('T5 ES5/ASCII lint of the diff: added lines ES5 and ASCII, no non-ASCII code point added or removed, file compiles', function () {
    var baseLines = {}, bl = baseSource.split('\n'), nl = newSource.split('\n');
    for (var i = 0; i < bl.length; i++) baseLines[bl[i]] = true;
    var added = nl.filter(function (l) { return !baseLines[l]; });
    assert(added.length >= 200, 'added lines ' + added.length);
    added.forEach(function (l) {
      var code = l.replace(/\/\/.*$/, '');
      assert(!/\b(let|const|class)\s|=>|`/.test(code) || /^\s*(\*|\/\*)/.test(l), 'ES6 token: ' + l);
      if (!/^[\x09\x20-\x7e]*$/.test(l)) {
        // the only non-ASCII added line is the version header rewrite (pre-existing em dash)
        assert(baseLines[l.replace('1.16.0', '1.13.11')] === true, 'non-ASCII added line: ' + l);
      }
    });
    function nonAscii(s) { var m = {}; for (var j = 0; j < s.length; j++) { var c = s.charCodeAt(j); if (c > 126) m[c] = (m[c] || 0) + 1; } return m; }
    deq(nonAscii(newSource), nonAscii(baseSource), 'non-ASCII code point multiset');
    assert(newSource.indexOf('\r') < 0, 'LF only');
    assert(!/[\u2018\u2019\u201c\u201d]/.test(newSource.slice(0, newSource.indexOf(' * v1.13.11 (2026-10-22)'))), 'no smart quotes in the new WHY');
    var why = newSource.slice(newSource.indexOf(' * v1.14.0 (2026-10-24)'), newSource.indexOf(' * v1.13.11 (2026-10-22)'));
    assert(why.length > 1000 && /^[\x09\x0a\x20-\x7e]*$/.test(why), 'WHY block present and ASCII');
    for (var n2 = 0; n2 < 2; n2++) {
      var name = ['dt10StabQualifiedWithin_', 'dt10StabConfirmBasis_'][n2];
      var start = newSource.indexOf('function ' + name + '(');
      assert(start > 0, name);
      var body = newSource.slice(start, newSource.indexOf('\n}\n', start) + 3);
      assert(/^[\x00-\x7f]*$/.test(body), 'ASCII ' + name);
      new vm.Script('(function(){' + body + '})');
    }
    new vm.Script(newSource, { filename: '16_Decision_Top10.gs' });
  });

  test('T6 inventory: zero functions and zero vars removed vs the base; exactly the designed names added', function () {
    function names(src, re) { var out = {}, m; while ((m = re.exec(src))) out[m[1]] = true; return out; }
    var fb = names(baseSource, /function\s+([A-Za-z_$][\w$]*)\s*\(/g), fn = names(newSource, /function\s+([A-Za-z_$][\w$]*)\s*\(/g);
    var removed = Object.keys(fb).filter(function (k) { return !fn[k]; });
    deq(removed, [], 'functions removed');
    var tb = names(baseSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), tn = names(newSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
    // 2026-10-25: v1.15.0 (pending grace) adds dt10StabSoftMiss_ / dt10StabPendingGrace_ on top of this change
    // 2026-10-26: v1.16.0 (funding evidence) adds the dt10Funding* family on top of these changes
    deq(Object.keys(tn).filter(function (k) { return !tb[k]; }).sort(), ['dt10FundingAge_', 'dt10FundingApplyCore_', 'dt10FundingApply_', 'dt10FundingCashCents_', 'dt10FundingDecimal_', 'dt10FundingDiv_', 'dt10FundingExcludeList_', 'dt10FundingLinks_', 'dt10FundingMode_', 'dt10FundingNative_', 'dt10FundingNote_', 'dt10FundingRates_', 'dt10FundingSym_', 'dt10FundingUnits_', 'dt10FundingValueSar_', 'dt10StabConfirmBasis_', 'dt10StabPendingGrace_', 'dt10StabQualifiedWithin_', 'dt10StabSoftMiss_'], 'top-level functions added');
    eq(Object.keys(tb).length, 195, 'base top-level census'); eq(Object.keys(tn).length, 214, 'new top-level census');
    var vb = names(baseSource, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = names(newSource, /^var\s+([A-Za-z_$][\w$]*)/mg);
    deq(Object.keys(vb).filter(function (k) { return !vn[k]; }), [], 'vars removed');
    deq(Object.keys(vn).filter(function (k) { return !vb[k]; }).sort(), ['DT10_FUNDING_EXCLUDE_PROP', 'DT10_FUNDING_MAX_AGE_MS', 'DT10_FUNDING_MAX_ITEMS', 'DT10_FUNDING_PROP', 'DT10_FUNDING_SAFE', 'DT10_T10_CONFIRM_BASIS_PROP', 'DT10_T10_PENDING_GRACE_MAX', 'DT10_T10_PENDING_GRACE_PROP'], 'vars added');
    // every nested function of dt10StabCore_ is kept, and the new closure is present
    var core = newSource.slice(newSource.indexOf('function dt10StabCore_('), newSource.indexOf('\n// ----', newSource.indexOf('function dt10StabCore_(')));
    ['outageOf', 'hardOut', 'scoreOf', 'smoothed', 'boardSort', 'inFinal', 'inList', 'qualifiedWithin'].forEach(function (f) {
      assert(core.indexOf('function ' + f + '(') > 0, 'nested ' + f);
    });
    assert(typeof N.dt10StabQualifiedWithin_ === 'function' && typeof N.dt10StabConfirmBasis_ === 'function', 'callable');
    eq(N.DT10_T10_CONFIRM_BASIS_PROP, 'TFB_T10_CONFIRM_BASIS', 'property name');
  });

  test('T7 service read: raw spellings -> raw; blank, junk, absent, fault -> qualified; the criteria snapshot never carries the knob', function () {
    [[' RAW ', 'raw'], ['raw', 'raw'], ['Raw', 'raw'], ['', 'qualified'], ['rawish', 'qualified'], ['qualified', 'qualified'],
     ['legacy', 'qualified'], ['1', 'qualified'], ['0', 'qualified']].forEach(function (pair) {
      var p = {}; p[PROP] = pair[0];
      eq(load(newSource, p).dt10StabConfirmBasis_(), pair[1], 'property ' + JSON.stringify(pair[0]));
    });
    eq(load(newSource, {}).dt10StabConfirmBasis_(), 'qualified', 'absent');
    eq(load(newSource, {}, { propsThrow: true }).dt10StabConfirmBasis_(), 'qualified', 'service fault');
    var k = N.dt10StabKnobs_({ 'T10: Stability Rank Buffer': 15 });
    assert(!k.hasOwnProperty('confirm_basis'), 'dt10StabKnobs_ (criteria snapshot) unchanged');
    deq(k, B.dt10StabKnobs_({ 'T10: Stability Rank Buffer': 15 }), 'knob mapping identical to base');
    var e2e = applyE2E(newSource, {});
    eq(e2e.confirm_basis, 'qualified', 'default through dt10ApplyStability_');
    eq(e2e.reads[PROP], 1, 'exactly one read');
  });

  return sha256(JSON.stringify([nq, nr, nn, bb, applyE2E(newSource, {}).saved, applyE2E(newSource, { TFB_T10_CONFIRM_BASIS: 'raw' }).saved])).slice(0, 16);
}

var digests = [], total = 0;
for (var run = 1; run <= 3; run++) {
  passed = 0; failed = 0;
  var d = battery();
  digests.push(d);
  total = passed + failed;
  console.log('run ' + run + ': ' + passed + '/' + total + ' digest ' + d);
  if (failed) { console.log('FAIL ' + passed + '/' + total); process.exit(1); }
}
if (digests[0] !== digests[1] || digests[1] !== digests[2]) { console.log('FAIL x3 digest mismatch ' + digests.join(' ')); process.exit(1); }
console.log('CONFIRM-BASIS T0-T7 x3 identical digest ' + digests[0] + ' | new SHA256 ' + sha256(newSource) + ' | base SHA256 ' + sha256(baseSource));
console.log('PASS ' + passed + '/' + total);
