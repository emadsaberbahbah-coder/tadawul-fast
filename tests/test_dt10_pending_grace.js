/* PENDING-GRACE (16_Decision_Top10.gs v1.15.0) -- a SOFT criteria miss pauses a
 * pending challenger's confirmation count instead of resetting it (Script
 * Property TFB_T10_PENDING_GRACE_DAYS, default absent/0 = the v1.14.0 clocks
 * byte for byte; 1..5 arms).
 *
 * Dual-tree node harness over the REAL file: loads the .gs source in a vm
 * context with minimal GAS shims, replays a 2-seat board through the REAL
 * dt10StabCore_ and the REAL dt10ApplyStability_ (service read -> knob), and
 * proves:
 *   T1 GOLDEN NEGATIVE on the v1.14.0 base: the challenger confirmed on days
 *      1-2 that is absent from raw on day 3 with an audited row whose verdict is
 *      WATCH at rank 12 (no block) resets to 0 and is only 'pending 1/3' on
 *      day 4 (not seated). The base ignores an unknown pending_grace knob.
 *   T2 v1.15.0 armed (grace 1): day 3 pauses (ci 2, cp 1, 'pgrace=1' and
 *      'pheld 1 CHAL.US(2/3)' tokens, audit.pending_held), day 4 seats
 *      'NEW (confirmed 3/3)' with the raw ticket's sizing and cp cleared;
 *      two consecutive soft misses with grace 1 reset on the second; grace 2
 *      absorbs both; a structural block, a hard-out verdict, a row absent
 *      from the grid, rank buffer + 1 and rank buffer 0 are HARD misses
 *      (reset); a member's clocks are untouched; a challenger with ci 0 never
 *      pauses; a qualified (INVEST inside the buffer) day still counts and
 *      clears cp; the knob clamps at 5.
 *   T3 KILL absent/0: every day of every fixture deep-equals the base file's
 *      output (tickets, state, audit, note), through the pure core AND through
 *      dt10ApplyStability_ (saved blob, payload.selected, audit, note, knobs).
 *   T4 embedded dt10SelfTest replay: 'pending grace core: ok', the property
 *      line follows the property, every base 'core: ok' line unchanged, zero
 *      FAIL.
 *   T5 ES5/ASCII lint of the diff: added lines carry no let/const/=>/`/class,
 *      are ASCII (the version header line keeps only its pre-existing em dash),
 *      no non-ASCII code point is added or removed, lockstep version pins.
 *   T6 inventory: zero functions and zero vars removed vs the base; exactly
 *      the designed names added.
 *   T7 service read: '1'..'5' -> n, '9' -> 5 (clamp), '', '0', '-1', '1.5',
 *      'two', absent and a PropertiesService fault -> 0; the knob never reaches
 *      the criteria snapshot (dt10StabKnobs_ unchanged); the state sanitizer
 *      keeps cp only while > 0.
 *
 * The BASE is the v1.14.0 file rebuilt from the delivered source by reversing
 * exactly the enumerated v1.15.0 edits (tests/js/dt10_cash_utc_history.js,
 * SHA-256 pinned b888d7fd...), or a file given as the second argument, which
 * must carry the same SHA-256. The whole battery runs three times and the
 * three digests must match; the last line is 'PASS k/k'.
 *
 * Usage: node tests/test_dt10_pending_grace.js [new.gs] [base.gs]
 */
'use strict';
var fs = require('fs'), vm = require('vm'), path = require('path'), crypto = require('crypto');
var ROOT = path.join(__dirname, '..');
var NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '16_Decision_Top10.gs');
var BASE_PATH = process.argv[3] || '';
var history = require(path.join(ROOT, 'tests', 'js', 'dt10_cash_utc_history.js'));
var PROP = 'TFB_T10_PENDING_GRACE_DAYS';
var DASH = '—';

var newSource = fs.readFileSync(NEW_PATH, 'utf8');
function sha256(s) { return crypto.createHash('sha256').update(s).digest('hex'); }
/* The oracle: a supplied v1.14.0 file, else the delivered file with exactly the
 * enumerated v1.15.0 edits reversed (any unlisted change makes this throw). */
var baseSource;
try {
  baseSource = BASE_PATH ? fs.readFileSync(BASE_PATH, 'utf8') : history.v1140Source(newSource);
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
  statusRows[0] = ['TFB Feed Global_Markets', 'OK | cov=99.0 | run=40145487350 | 2026-10-25 04:09:22+03:00'];
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
    Utilities: { formatDate: function (d, tz, pattern) { return pattern === 'yyyy-MM-dd' ? '2026-10-25' : '2026-10-25 00:00:00'; } },
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
 * A 95 (2), F1..F12 94.5..83.5, CHAL 86 (12), B 84 (15 = buffer boundary).
 * Raw (sized top-2): d1 [A, CHAL], d2 [A, CHAL], d3 [A, NEWC] (CHAL out),
 * d4 [A, CHAL] unless a fixture says otherwise. */
var DAYS = ['2026-10-21', '2026-10-22', '2026-10-23', '2026-10-24'];
function knobs(pg, buf) {
  var k = { enabled: true, confirm_days: 3, exit_days: 3, rank_buffer: (buf === undefined ? 15 : buf),
            smooth_days: 5, hard_strict: true, confirm_basis: 'qualified' };
  if (pg !== undefined) k.pending_grace = pg;
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
function grid(chalScore, chalVerdict, chalExtra, withoutChal) {
  var g = [cand('NEWC.US', 97), cand('A.US', 95)];
  for (var i = 1; i <= 12; i++) g.push(cand('F' + i + '.US', 95.5 - i));
  g.push(cand('B.US', 84));
  if (!withoutChal) g.push(cand('CHAL.US', chalScore === undefined ? 86 : chalScore, chalVerdict, chalExtra));
  return g;
}
function seed() {
  return { v: 1, date: '2026-10-20', symbols: {
    'A.US': { ci: 3, co: 0, member: true, since: '2026-10-15', ls: '2026-10-20', hist: [95], ft: false },
    'B.US': { ci: 3, co: 1, member: true, since: '2026-10-15', ls: '2026-10-20', hist: [84], ft: false } } };
}
function raws(day4Chal) {
  return [[ticket('A.US', 95), ticket('CHAL.US', 86)],
          [ticket('A.US', 95), ticket('CHAL.US', 86)],
          [ticket('A.US', 95), ticket('NEWC.US', 97)],
          day4Chal === false ? [ticket('A.US', 95), ticket('NEWC.US', 97)] : [ticket('A.US', 95), ticket('CHAL.US', 86)]];
}
/* Replay through the REAL pure core of context g; grids[d] / rawsArr[d] per day; limit 2. */
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
var G = grid(), SOFT = grid(86, 'WATCH'), DNI = grid(86, 'DO_NOT_INVEST'), BLK = grid(86, 'WATCH', { structural_block: true }),
    HARD = grid(86, 'SELL'), OUT16 = grid(83, 'WATCH'), EDGE15 = grid(84.2, 'WATCH'), ABSENT = grid(undefined, undefined, undefined, true);
function grids(d3, d4) { return [plain(G), plain(G), plain(d3 || G), plain(d4 || G)]; }
function bySym(out, s) { for (var i = 0; i < out.tickets.length; i++) if (out.tickets[i].symbol === s) return out.tickets[i]; return null; }
function sym(out, s) { return out.state.symbols[s]; }

/* All scenario replays for one context under one knob value (undefined = absent). */
function scenarios(g, pg) {
  return {
    soft: replay(g, knobs(pg), seed(), raws(), grids(SOFT)),
    dni: replay(g, knobs(pg), seed(), raws(), grids(DNI)),
    soft2: replay(g, knobs(pg), seed(), raws(false), grids(SOFT, SOFT)),
    blocked: replay(g, knobs(pg), seed(), raws(), grids(BLK)),
    hard: replay(g, knobs(pg), seed(), raws(), grids(HARD)),
    out16: replay(g, knobs(pg), seed(), raws(), grids(OUT16)),
    edge15: replay(g, knobs(pg), seed(), raws(), grids(EDGE15)),
    buf0: replay(g, knobs(pg, 0), seed(), raws(), grids(SOFT)),
    absent: replay(g, knobs(pg), seed(), raws(), grids(ABSENT)),
    plain: replay(g, knobs(pg), seed(), raws(), grids())
  };
}
/* End to end through the REAL dt10ApplyStability_ (service read -> knob,
 * state load/save, epoch from the _Status feed stamp = 2026-10-25). */
function applyE2E(source, props, options) {
  var g = load(source, props, options);
  var seedObj = seed(); seedObj.date = '2026-10-24';
  seedObj.symbols['CHAL.US'] = { ci: 2, co: 0, member: false, since: '', ls: '2026-10-24', hist: [86, 86], ft: false };
  g.__store.DT10_STAB_STATE_V1 = JSON.stringify(seedObj);
  var payload = { selected: [ticket('A.US', 95), ticket('NEWC.US', 97)], candidates_rows: grid(86, 'WATCH'), meta: {} };
  var panel = { 'T10: Max Selected': 2, 'T10: Stability Enabled': 'Yes', 'T10: Stability Confirm Days': 3,
                'T10: Stability Exit Days': 3, 'T10: Stability Rank Buffer': 15, 'T10: Stability Smooth Days': 5 };
  var out = g.dt10ApplyStability_(payload, panel, null);
  return { g: g, note: out.note, selected: plain(payload.selected), audit: plain(payload.meta.stability.audit),
           knobs: plain(payload.meta.stability.knobs),
           saved: JSON.parse(g.__store.DT10_STAB_STATE_V1), reads: plain(g.__reads) };
}

/* ------------------------------------------------------------ battery */
function battery() {
  var N = load(newSource, {});
  var B = load(baseSource, {});
  var n1 = scenarios(N, 1), n2 = scenarios(N, 2), n0 = scenarios(N, 0), nn = scenarios(N, undefined), bb = scenarios(B, undefined);

  test('T0 base oracle is the reviewed v1.14.0 file (SHA-256 pinned); delivered file compiles', function () {
    eq(sha256(baseSource), history.V1140_SHA256, 'base SHA-256');
    eq(B.DT10_VERSION, '1.14.0', 'base version');
    // 2026-10-26/27: the delivered file is v1.17.0 (funding evidence and the shadow rank log stacked on this change); pins moved
    // 2026-10-27: v1.17.1 (shadow momentum) stacked as well; pins moved
    // 2026-10-27: v1.18.0 (post-earnings tag) stacked as well; pins moved
    eq(N.DT10_VERSION, '1.18.0', 'delivered version');
    assert(/^ \* Version: 1\.18\.0 /.test(newSource.split('\n')[3]), 'header line 4 lockstep');
    assert(newSource.indexOf("var DT10_VERSION = '1.18.0';") > 0, 'DT10_VERSION literal');
    eq(N.DT10_T10_PENDING_GRACE_MAX, 5, 'clamp constant');
  });

  test('T1 GOLDEN NEGATIVE (v1.14.0): a soft miss on day 3 resets the challenger to 0; pending 1/3 on day 4, not seated; base ignores the knob', function () {
    var m = bb.soft;
    eq(sym(m[1], 'CHAL.US').ci, 2, 'day 2 ci');
    eq(sym(m[2], 'CHAL.US').ci, 0, 'day 3 ci (defect)');
    eq(sym(m[2], 'CHAL.US').cp, undefined, 'base has no cp');
    eq(sym(m[3], 'CHAL.US').ci, 1, 'day 4 ci');
    eq(bySym(m[3], 'CHAL.US'), null, 'not seated on day 4');
    assert(m[3].note.indexOf('pending CHAL.US(1/3)') > 0, 'day 4 note: ' + m[3].note);
    eq(m[2].note.indexOf('pgrace'), -1, 'no pgrace token');
    eq(m[2].audit.pending_held, undefined, 'no pending_held key');
    deq(scenarios(B, 1), bb, 'base ignores pending_grace');
  });

  test('T2 v1.15.0 armed: soft miss pauses (ci kept, cp 1, tokens), next raw day seats NEW 3/3 sized; second soft miss resets at grace 1 and is absorbed at grace 2; hard misses reset; members untouched', function () {
    var s = n1.soft;
    eq(sym(s[2], 'CHAL.US').ci, 2, 'day 3 ci kept');
    eq(sym(s[2], 'CHAL.US').cp, 1, 'day 3 cp');
    eq(sym(s[2], 'CHAL.US').member, false, 'still a challenger');
    assert(s[2].note.indexOf(', pgrace=1') > 0, 'pgrace token: ' + s[2].note);
    assert(s[2].note.indexOf(', pheld 1 CHAL.US(2/3)') > 0, 'pheld token: ' + s[2].note);
    deq(s[2].audit.pending_held, ['CHAL.US'], 'audit pending_held');
    eq(s[2].tickets.length, 2, 'board full on day 3');
    eq(bySym(s[2], 'CHAL.US'), null, 'a paused challenger is not seated');
    assert(s[2].state.symbols['CHAL.US'].hist.length === 3, 'score point recorded on the paused day');
    eq(sym(s[3], 'CHAL.US').ci, 3, 'day 4 ci');
    eq(sym(s[3], 'CHAL.US').cp, undefined, 'day 4 cp cleared');
    var t4 = bySym(s[3], 'CHAL.US');
    assert(t4 !== null, 'seated on day 4');
    eq(t4._stab_status, 'NEW (confirmed 3/3)', 'day 4 label');
    eq(t4.suggested_sar, 5000, 'raw sizing'); eq(t4.suggested_shares, 133, 'raw shares');
    assert(s[3].note.indexOf('pheld') < 0 && s[3].note.indexOf(', pgrace=1') > 0, 'day 4 note: ' + s[3].note);
    deq(s[3].audit.pending_held, [], 'day 4 nothing held');
    // DO_NOT_INVEST: a hard-out under hard_strict (v1.8.0 BE-2 doctrine kept) -> reset;
    // a soft miss when hard_strict is off -> paused
    eq(sym(n1.dni[2], 'CHAL.US').ci, 0, 'DO_NOT_INVEST under hard_strict is a hard miss');
    var kLax = knobs(1); kLax.hard_strict = false;
    var dniLax = replay(N, kLax, seed(), raws(), grids(DNI));
    eq(sym(dniLax[2], 'CHAL.US').ci, 2, 'DO_NOT_INVEST without hard_strict pauses');
    eq(sym(dniLax[2], 'CHAL.US').cp, 1, 'DO_NOT_INVEST without hard_strict cp 1');
    // two consecutive soft misses: grace 1 resets on the second, grace 2 absorbs both
    eq(sym(n1.soft2[2], 'CHAL.US').ci, 2, 'soft2 day 3 paused');
    eq(sym(n1.soft2[3], 'CHAL.US').ci, 0, 'soft2 day 4 reset at grace 1');
    eq(sym(n1.soft2[3], 'CHAL.US').cp, undefined, 'soft2 cp cleared on reset');
    assert(n1.soft2[3].note.indexOf('pheld') < 0, 'no pheld after reset');
    eq(sym(n2.soft2[3], 'CHAL.US').ci, 2, 'grace 2 absorbs the second miss');
    eq(sym(n2.soft2[3], 'CHAL.US').cp, 2, 'grace 2 cp 2');
    // hard misses: structural block, hard-out verdict, absent row, rank 16, buffer 0 -> v1.14.0 reset
    eq(sym(n1.blocked[2], 'CHAL.US').ci, 0, 'structural block resets');
    eq(sym(n1.hard[2], 'CHAL.US').ci, 0, 'SELL verdict (hard out) resets');
    eq(sym(n1.out16[2], 'CHAL.US').ci, 0, 'rank 16 resets');
    eq(sym(n1.edge15[2], 'CHAL.US').ci, 2, 'rank 15 (boundary) pauses');
    eq(sym(n1.buf0[2], 'CHAL.US').ci, 0, 'buffer 0 resets');
    eq(sym(n1.absent[2], 'CHAL.US').ci, 0, 'absent from the grid resets');
    ['blocked', 'hard', 'out16', 'buf0', 'absent'].forEach(function (k) {
      assert(n1[k][2].note.indexOf('pheld') < 0, k + ' never pheld');
    });
    // a plain day (CHAL INVEST inside the buffer) still counts through the qualified branch, no cp
    eq(sym(n1.plain[2], 'CHAL.US').ci, 3, 'qualified day counts');
    eq(sym(n1.plain[2], 'CHAL.US').cp, undefined, 'qualified day: no cp');
    // members are untouched on the paused day and before it: B (GRACE) and A identical to
    // the base through day 3 in every scenario; on day 4 the seated challenger displaces B
    // exactly as a raw-confirmed challenger would (weakest incumbent, audit.exited_displaced)
    Object.keys(n1).forEach(function (k) {
      for (var d = 0; d < 3; d++) {
        deq(sym(n1[k][d], 'B.US'), sym(bb[k][d], 'B.US'), k + ' day ' + d + ' member B');
        deq(sym(n1[k][d], 'A.US'), sym(bb[k][d], 'A.US'), k + ' day ' + d + ' member A');
      }
      deq(sym(n1[k][3], 'A.US'), sym(bb[k][3], 'A.US'), k + ' day 3 member A');
    });
    deq(s[3].audit.exited_displaced, ['B.US'], 'day 4: CHAL seated, B displaced');
    deq(s[3].audit.entered, ['CHAL.US'], 'day 4: entered');
    ['blocked', 'hard', 'out16', 'buf0', 'absent'].forEach(function (k) {
      deq(sym(n1[k][3], 'B.US'), sym(bb[k][3], 'B.US'), k + ' day 4 member B unchanged (no seating)');
    });
    // a challenger with ci 0 never pauses: NEWC (raw on day 3 only) then absent day 4 -> reset path, no cp
    eq(sym(n1.soft2[3], 'NEWC.US').ci, 2, 'NEWC counted on days 3-4 (raw)');
    // clamp: knob 9 behaves like 5 (cp advances to 2 on soft2 like grace 2)
    var n9 = replay(N, knobs(9), seed(), raws(false), grids(SOFT, SOFT));
    deq(n9.map(function (o) { return o.note.replace('pgrace=5', 'pgrace=X'); }), n2.soft2.map(function (o) { return o.note.replace('pgrace=2', 'pgrace=X'); }), 'knob 9 clamps to 5 (notes)');
    eq(sym(n9[3], 'CHAL.US').cp, 2, 'knob 9 clamps: cp 2');
    assert(n9[3].note.indexOf('pgrace=5') > 0, 'clamped token');
    // the pheld token is capped at 3 entries and ordered by ci desc, rank asc
    var st3 = N.dt10StabParseState_({ v: 1, date: '2026-10-24', symbols: {
      'A.US': { ci: 3, co: 0, member: true, since: '2026-10-15', ls: '2026-10-24', hist: [95], ft: false },
      'B.US': { ci: 3, co: 0, member: true, since: '2026-10-15', ls: '2026-10-24', hist: [84], ft: false },
      'F1.US': { ci: 1, co: 0, member: false, since: '', ls: '2026-10-24', hist: [94.5], ft: false },
      'F2.US': { ci: 2, co: 0, member: false, since: '', ls: '2026-10-24', hist: [93.5], ft: false },
      'F3.US': { ci: 2, co: 0, member: false, since: '', ls: '2026-10-24', hist: [92.5], ft: false },
      'F4.US': { ci: 1, co: 0, member: false, since: '', ls: '2026-10-24', hist: [91.5], ft: false } } });
    var g3 = grid(); for (var gi = 0; gi < g3.length; gi++) if (/^F[1-4]\.US$/.test(g3[gi].symbol)) g3[gi].verdict = 'WATCH';
    var o3 = N.dt10StabCore_([ticket('A.US', 95), ticket('B.US', 84)], g3, st3, knobs(1), 2, '2026-10-25');
    eq(o3.note, 'stab: steady ' + DASH + ' no membership change, confirm=qualified, pgrace=1, pheld 4 F2.US(2/3) F3.US(2/3) F1.US(1/3)',
       'pheld cap/order');
    deq(o3.audit.pending_held, ['F2.US', 'F3.US', 'F1.US', 'F4.US'], 'audit holds all four');
    // pure helper checks
    var cbs = { 'X.US': cand('X.US', 50, 'WATCH') };
    assert(N.dt10StabSoftMiss_('X.US', cbs, { 'X.US': 15 }, knobs(1), null) === true, 'soft at 15');
    assert(N.dt10StabSoftMiss_('X.US', cbs, { 'X.US': 16 }, knobs(1), null) === false, 'outside at 16');
    assert(N.dt10StabSoftMiss_('X.US', cbs, { 'X.US': 1 }, knobs(1, 0), null) === false, 'buffer 0');
    assert(N.dt10StabSoftMiss_('X.US', cbs, { 'X.US': 1 }, knobs(1), function () { return true; }) === false, 'hardOut wins');
    assert(N.dt10StabSoftMiss_('X.US', { 'X.US': cand('X.US', 50, 'invest') }, { 'X.US': 1 }, knobs(1), null) === false, 'INVEST is not a miss');
    assert(N.dt10StabSoftMiss_('X.US', { 'X.US': cand('X.US', 50, 'WATCH', { structural_block: true }) }, { 'X.US': 1 }, knobs(1), null) === false, 'structural block');
    assert(N.dt10StabSoftMiss_('Y.US', cbs, { 'Y.US': 1 }, knobs(1), null) === false, 'absent symbol');
    assert(N.dt10StabSoftMiss_('X.US', null, null, null, null) === false, 'null inputs never throw');
  });

  test('T3 KILL absent/0: every fixture, every day deep-equals the v1.14.0 base; dt10ApplyStability_ end to end (knobs object, saved blob, note)', function () {
    deq(n0, bb, 'pending_grace 0 vs base (all scenarios)');
    deq(nn, bb, 'knob absent vs base (all scenarios)');
    var e2eN = applyE2E(newSource, {}), e2eB = applyE2E(baseSource, {});
    deq([e2eN.note, e2eN.selected, e2eN.audit, e2eN.saved, e2eN.knobs], [e2eB.note, e2eB.selected, e2eB.audit, e2eB.saved, e2eB.knobs], 'e2e unarmed vs base');
    eq(e2eN.saved.symbols['CHAL.US'].ci, 0, 'e2e unarmed: soft miss resets');
    assert(!e2eN.knobs.hasOwnProperty('pending_grace'), 'knobs object byte-identical when unarmed');
    eq(e2eN.reads[PROP], 1, 'one property read per apply');
    eq(e2eB.reads[PROP], undefined, 'base never reads the property');
    var e2e0 = applyE2E(newSource, { TFB_T10_PENDING_GRACE_DAYS: '0' });
    deq([e2e0.note, e2e0.selected, e2e0.audit, e2e0.saved, e2e0.knobs], [e2eB.note, e2eB.selected, e2eB.audit, e2eB.saved, e2eB.knobs], 'e2e property 0 vs base');
    // armed end to end: knob published, count paused, tokens present, cp saved
    var e2eA = applyE2E(newSource, { TFB_T10_PENDING_GRACE_DAYS: '1' });
    eq(e2eA.knobs.pending_grace, 1, 'knob published when armed');
    eq(e2eA.saved.symbols['CHAL.US'].ci, 2, 'e2e armed: count kept');
    eq(e2eA.saved.symbols['CHAL.US'].cp, 1, 'e2e armed: cp saved');
    assert(e2eA.note.indexOf('pgrace=1') > 0 && e2eA.note.indexOf('pheld 1 CHAL.US(2/3)') > 0 && e2eA.note.indexOf(' | epoch=2026-10-25/feed') > 0, 'e2e armed note: ' + e2eA.note);
    deq(e2eA.selected.map(function (t) { return t.symbol; }), e2eB.selected.map(function (t) { return t.symbol; }), 'e2e board unchanged on a paused day');
    deq(Object.keys(e2eA.saved.symbols['A.US']).sort(), Object.keys(e2eB.saved.symbols['A.US']).sort(), 'member blob fields unchanged');
  });

  test('T4 embedded dt10SelfTest: pending grace battery ok, property line follows the property, every base core line unchanged, zero FAIL', function () {
    var rep = String(N.dt10SelfTest()), repB = String(B.dt10SelfTest());
    assert(rep.indexOf('pending grace core: ok') > 0, 'battery line');
    assert(rep.indexOf('pending grace (TFB_T10_PENDING_GRACE_DAYS): 0 (') > 0, 'property line default');
    assert(rep.indexOf('[DT10 v1.18.0] self-test') === 0, 'version line');
    assert(rep.indexOf(': FAIL') < 0 && rep.indexOf('FAIL [') < 0, 'no FAIL');
    // 2026-10-27: v1.17.1 (shadow momentum) keeps the shadow log mode in the per-execution
    // property memo, so the memo line reports one more key (keys=3); pin moved
    var baseLines = repB.split('\n'), newLines = rep.split('\n').map(function (l) {
      return l.replace('(pool fields memoized; keys=3; reads=1/execution)', '(pool fields memoized; keys=2; reads=1/execution)');
    });
    var missing = baseLines.filter(function (l) { return l.indexOf('[DT10 v1.14.0]') !== 0 && newLines.indexOf(l) < 0; });
    deq(missing, [], 'every base self-test line present');
    var cores = baseLines.filter(function (l) { return / core: ok/.test(l); });
    assert(cores.length >= 28, 'base core lines ' + cores.length);
    var armed = String(load(newSource, { TFB_T10_PENDING_GRACE_DAYS: ' 2 ' }).dt10SelfTest());
    assert(armed.indexOf('pending grace (TFB_T10_PENDING_GRACE_DAYS): 2 (') > 0 && armed.indexOf('pending grace core: ok') > 0 && armed.indexOf('FAIL') < 0, 'armed property line');
  });

  test('T5 ES5/ASCII lint of the diff: added lines ES5 and ASCII, no non-ASCII code point added or removed, file compiles', function () {
    var baseLines = {}, bl = baseSource.split('\n'), nl = newSource.split('\n');
    for (var i = 0; i < bl.length; i++) baseLines[bl[i]] = true;
    var added = nl.filter(function (l) { return !baseLines[l]; });
    assert(added.length >= 150, 'added lines ' + added.length);
    added.forEach(function (l) {
      var code = l.replace(/\/\/.*$/, '');
      assert(!/\b(let|const|class)\s|=>|`/.test(code) || /^\s*(\*|\/\*)/.test(l), 'ES6 token: ' + l);
      if (!/^[\x09\x20-\x7e]*$/.test(l)) {
        // the only non-ASCII added line is the version header rewrite (pre-existing em dash)
        assert(baseLines[l.replace('1.18.0', '1.14.0')] === true, 'non-ASCII added line: ' + l);
      }
    });
    function nonAscii(s) { var m = {}; for (var j = 0; j < s.length; j++) { var c = s.charCodeAt(j); if (c > 126) m[c] = (m[c] || 0) + 1; } return m; }
    deq(nonAscii(newSource), nonAscii(baseSource), 'non-ASCII code point multiset');
    assert(newSource.indexOf('\r') < 0, 'LF only');
    var why = newSource.slice(newSource.indexOf(' * v1.15.0 (2026-10-25)'), newSource.indexOf(' * v1.14.0 (2026-10-24)'));
    assert(why.length > 1500 && /^[\x09\x0a\x20-\x7e]*$/.test(why), 'WHY block present and ASCII');
    assert(!/[‘’“”]/.test(why), 'no smart quotes in the new WHY');
    for (var n2 = 0; n2 < 2; n2++) {
      var name = ['dt10StabSoftMiss_', 'dt10StabPendingGrace_'][n2];
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
    deq(Object.keys(fb).filter(function (k) { return !fn[k]; }), [], 'functions removed');
    var tb = names(baseSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), tn = names(newSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
    // 2026-10-26: v1.16.0 (funding evidence) adds the dt10Funding* family on top of this change
    // 2026-10-27: v1.17.0 (shadow rank log) adds dt10ShadowLogMode_ / dt10ShadowLogRows_ / dt10AppendShadowRankLog_
    // 2026-10-27: v1.17.1 (shadow momentum) adds dt10PoolFieldsWithShadow_ / DT10_SHADOW_POOL_FIELDS
    // 2026-10-27: v1.18.0 (post-earnings tag) adds the dt10PostEarn* family, dt10ApplyPostEarnTags_,
    //   dt10WeekdaysBetween_ and DT10_POST_EARN_PROP / _SESSIONS / _SESSIONS_NEG / _MAX_DAYS
    deq(Object.keys(tn).filter(function (k) { return !tb[k]; }).sort(), ['dt10AppendShadowRankLog_', 'dt10ApplyPostEarnTags_', 'dt10FundingAge_', 'dt10FundingApplyCore_', 'dt10FundingApply_', 'dt10FundingCashCents_', 'dt10FundingDecimal_', 'dt10FundingDiv_', 'dt10FundingExcludeList_', 'dt10FundingLinks_', 'dt10FundingMode_', 'dt10FundingNative_', 'dt10FundingNote_', 'dt10FundingRates_', 'dt10FundingSym_', 'dt10FundingUnits_', 'dt10FundingValueSar_', 'dt10PoolFieldsWithShadow_', 'dt10PostEarnAnnotate_', 'dt10PostEarnMapFromValues_', 'dt10PostEarnMap_', 'dt10PostEarnMode_', 'dt10ShadowLogMode_', 'dt10ShadowLogRows_', 'dt10StabPendingGrace_', 'dt10StabSoftMiss_', 'dt10WeekdaysBetween_'], 'top-level functions added');
    eq(Object.keys(tb).length, 197, 'base top-level census'); eq(Object.keys(tn).length, 224, 'new top-level census');
    var vb = names(baseSource, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = names(newSource, /^var\s+([A-Za-z_$][\w$]*)/mg);
    deq(Object.keys(vb).filter(function (k) { return !vn[k]; }), [], 'vars removed');
    deq(Object.keys(vn).filter(function (k) { return !vb[k]; }).sort(), ['DT10_FUNDING_EXCLUDE_PROP', 'DT10_FUNDING_MAX_AGE_MS', 'DT10_FUNDING_MAX_ITEMS', 'DT10_FUNDING_PROP', 'DT10_FUNDING_SAFE', 'DT10_POST_EARN_MAX_DAYS', 'DT10_POST_EARN_PROP', 'DT10_POST_EARN_SESSIONS', 'DT10_POST_EARN_SESSIONS_NEG', 'DT10_SHADOW_LOG_HEADERS', 'DT10_SHADOW_LOG_PROP', 'DT10_SHADOW_LOG_SHEET', 'DT10_SHADOW_LOG_STATE_PROP', 'DT10_SHADOW_POOL_FIELDS', 'DT10_T10_PENDING_GRACE_MAX', 'DT10_T10_PENDING_GRACE_PROP'], 'vars added');
    var core = newSource.slice(newSource.indexOf('function dt10StabCore_('), newSource.indexOf('\n// ----', newSource.indexOf('function dt10StabCore_(')));
    ['outageOf', 'hardOut', 'scoreOf', 'smoothed', 'boardSort', 'inFinal', 'inList', 'qualifiedWithin', 'softMiss'].forEach(function (f) {
      assert(core.indexOf('function ' + f + '(') > 0, 'nested ' + f);
    });
    assert(typeof N.dt10StabSoftMiss_ === 'function' && typeof N.dt10StabPendingGrace_ === 'function', 'callable');
    eq(N.DT10_T10_PENDING_GRACE_PROP, 'TFB_T10_PENDING_GRACE_DAYS', 'property name');
  });

  test('T7 service read: 1..5 -> n, 9 -> 5, blank/0/-1/1.5/junk/absent/fault -> 0; criteria snapshot never carries the knob; sanitizer keeps cp only while > 0', function () {
    [['1', 1], [' 2 ', 2], ['3', 3], ['5', 5], ['9', 5], ['', 0], ['0', 0], ['-1', 0], ['1.5', 0], ['two', 0], ['01', 1]].forEach(function (pair) {
      var p = {}; p[PROP] = pair[0];
      eq(load(newSource, p).dt10StabPendingGrace_(), pair[1], 'property ' + JSON.stringify(pair[0]));
    });
    eq(load(newSource, {}).dt10StabPendingGrace_(), 0, 'absent');
    eq(load(newSource, {}, { propsThrow: true }).dt10StabPendingGrace_(), 0, 'service fault');
    var k = N.dt10StabKnobs_({ 'T10: Stability Rank Buffer': 15 });
    assert(!k.hasOwnProperty('pending_grace'), 'dt10StabKnobs_ (criteria snapshot) unchanged');
    deq(k, B.dt10StabKnobs_({ 'T10: Stability Rank Buffer': 15 }), 'knob mapping identical to base');
    var p1 = N.dt10StabParseState_({ v: 1, date: '2026-10-24', symbols: {
      'X.US': { ci: 2, co: 0, member: false, since: '', ls: '2026-10-24', hist: [], ft: false, cp: 1 },
      'Y.US': { ci: 2, co: 0, member: false, since: '', ls: '2026-10-24', hist: [], ft: false, cp: 0 },
      'Z.US': { ci: 2, co: 0, member: false, since: '', ls: '2026-10-24', hist: [], ft: false, cp: 'junk' } } });
    eq(p1.symbols['X.US'].cp, 1, 'cp kept');
    assert(!p1.symbols['Y.US'].hasOwnProperty('cp') && !p1.symbols['Z.US'].hasOwnProperty('cp'), 'cp 0/junk dropped');
    deq(Object.keys(p1.symbols['Y.US']).sort(), Object.keys(B.dt10StabParseState_({ v: 1, date: '2026-10-24', symbols: {
      'Y.US': { ci: 2, co: 0, member: false, since: '', ls: '2026-10-24', hist: [], ft: false } } }).symbols['Y.US']).sort(), 'blob field set unchanged without cp');
  });

  return sha256(JSON.stringify([n1, n2, n0, nn, bb, applyE2E(newSource, {}).saved, applyE2E(newSource, { TFB_T10_PENDING_GRACE_DAYS: '1' }).saved])).slice(0, 16);
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
console.log('PENDING-GRACE T0-T7 x3 identical digest ' + digests[0] + ' | new SHA256 ' + sha256(newSource) + ' | base SHA256 ' + sha256(baseSource));
console.log('PASS ' + passed + '/' + total);
