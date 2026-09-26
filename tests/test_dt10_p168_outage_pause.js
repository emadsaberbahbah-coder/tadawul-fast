#!/usr/bin/env node
/* tests/test_dt10_p168_outage_pause.js — dual-tree real-module harness for
 * 16_Decision_Top10.gs v1.11.10 [P-168 OUTAGE-EPOCH CLOCK PAUSE].
 * Loads the REAL .gs source (base v1.11.9 and delivered v1.11.10) into a vm
 * context with minimal Apps Script service stubs, then replays the
 * 2026-09-26 board (real pool rows + real audit rows from the browser
 * export) through dt10StabCore_. No stand-in objects: every call runs the
 * file's own functions. Usage: node test_dt10_p168_outage_pause.js
 *   [--base <path>] [--delivered <path>] [--export-dir <dir>] */
'use strict';
var fs = require('fs'), path = require('path'), vm = require('vm'), crypto = require('crypto');
var args = process.argv.slice(2);
function arg(k, d) { var i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; }
var BASE = arg('--base', '16_Decision_Top10_base.gs');
var DELIV = arg('--delivered', '16_Decision_Top10.gs');
var EXP = arg('--export-dir', '../exp');

function stubs() {
  var props = {};
  var sheetStub = {
    getDataRange: function () { return { getValues: function () { return [[]]; } }; },
    getLastRow: function () { return 0; }, getLastColumn: function () { return 0; },
    getRange: function () { return { getValues: function () { return [[]]; }, getValue: function () { return ''; },
                                     setValue: function () { return this; }, setValues: function () { return this; },
                                     getNumRows: function () { return 0; } }; },
    getName: function () { return 'stub'; }
  };
  var ss = { getSheetByName: function () { return null; }, getRangeByName: function () { return null; },
             getSheets: function () { return []; }, getId: function () { return 'stub'; } };
  return {
    console: console,
    Logger: { log: function () {} },
    Utilities: { formatDate: function (d, tz, fmt) { return d.toISOString().slice(0, 10); },
                 sleep: function () {}, computeDigest: function () { return []; }, base64Encode: function () { return ''; } },
    PropertiesService: { getScriptProperties: function () { return {
      getProperty: function (k) { return props.hasOwnProperty(k) ? props[k] : null; },
      setProperty: function (k, v) { props[k] = String(v); return this; },
      deleteProperty: function (k) { delete props[k]; return this; } }; },
      getUserProperties: function () { return this.getScriptProperties(); } },
    SpreadsheetApp: { getActiveSpreadsheet: function () { return ss; },
                      getUi: function () { throw new Error('no UI'); },
                      openById: function () { return ss; } },
    UrlFetchApp: { fetch: function () { throw new Error('no network in harness'); } },
    Session: { getScriptTimeZone: function () { return 'Etc/UTC'; } },
    CacheService: { getScriptCache: function () { return { get: function () { return null; }, put: function () {} }; } },
    _props: props, _sheetStub: sheetStub
  };
}
function loadTree(file) {
  var src = fs.readFileSync(file, 'utf8');
  var ctx = vm.createContext(stubs());
  vm.runInContext(src, ctx, { filename: file });
  return ctx;
}
function tsv(name) {
  var p = path.join(EXP, name);
  var lines = fs.readFileSync(p, 'utf8').split(/\r?\n/).filter(function (l) { return l.length; });
  var h = lines[0].split('\t');
  return lines.slice(1).map(function (l) { var c = l.split('\t'), o = {}; for (var i = 0; i < h.length; i++) o[h[i]] = c[i] || ''; return o; });
}
function poolRows() {   // real pool rows: the two keys the outage map reads
  var out = [];
  ['Market_Leaders.tsv', 'Global_Markets.tsv', 'Commodities_FX.tsv', 'Mutual_Funds.tsv'].forEach(function (f) {
    tsv(f).forEach(function (r) { if (r.Symbol) out.push({ 'Symbol': r.Symbol, 'Warnings': r.Warnings || '' }); });
  });
  return out;
}
function auditRows() {  // real CANDIDATES — FULL AUDIT block of Top_10_Investments.tsv
  var lines = fs.readFileSync(path.join(EXP, 'Top_10_Investments.tsv'), 'utf8').split(/\r?\n/);
  var i = 0; while (i < lines.length && !(lines[i].indexOf('Symbol\tName\tMarket') === 0 && lines[i].indexOf('First Fail') > 0)) i++;
  var h = lines[i].split('\t'), rows = [];
  for (var j = i + 1; j < lines.length; j++) {
    var c = lines[j].split('\t'); if (!c[0]) break;
    rows.push({ symbol: c[h.indexOf('Symbol')], name: c[h.indexOf('Name')], market: c[h.indexOf('Market')],
                sector: c[h.indexOf('Sector')], currency: c[h.indexOf('Ccy')], price: Number(c[h.indexOf('Price')]),
                opportunity_score: Number(c[h.indexOf('Score')]), verdict: c[h.indexOf('Verdict')],
                reliability: c[h.indexOf('Rel')], dq: c[h.indexOf('DQ')] });
  }
  return rows;
}
var KNOBS = { enabled: true, confirm_days: 3, exit_days: 3, rank_buffer: 15, smooth_days: 5, hard_strict: true };
// The 2026-09-25 evening board (SelLog 18:35:18): four members, co as logged
// at the 02:36 same-day run (ITRN 0/3, PINFRA 1/3, NVDA 0/3, 2222.SR 1/3).
function seedState() {
  return { v: 1, date: '2026-09-25', symbols: {
    'ITRN.US':   { ci: 3, co: 0, member: true, since: '2026-09-19', ls: '2026-09-25', hist: [79, 78.6], ft: false },
    'PINFRA.MX': { ci: 3, co: 1, member: true, since: '2026-09-15', ls: '2026-09-24', hist: [75, 74.8], ft: false },
    'NVDA.US':   { ci: 1, co: 0, member: true, since: '2026-09-25', ls: '2026-09-25', hist: [69.8], ft: true },
    '2222.SR':   { ci: 3, co: 1, member: true, since: '2026-09-12', ls: '2026-09-24', hist: [67, 66.7], ft: false } } };
}
function tk(res, sym) { for (var i = 0; i < res.tickets.length; i++) if (res.tickets[i].symbol === sym) return res.tickets[i]; return null; }
function stateOf(res, sym) { return res.state.symbols[sym]; }
function json(x) { return JSON.stringify(x); }

function runOnce() {
  var B = loadTree(BASE), D = loadTree(DELIV);
  var log = [], fails = 0;
  function check(name, ok, detail) { log.push((ok ? 'PASS ' : 'FAIL ') + name + (detail ? ' :: ' + detail : '')); if (!ok) fails++; }
  var pool = poolRows(), cands = auditRows();
  var map = D.dt10OutageMapFromPool_(pool);
  var mapN = Object.keys(map).length;
  check('T0 real pool: outage map from ' + pool.length + ' rows', mapN === 6713 + 47 + 35 || mapN > 6700,
        'outage symbols=' + mapN + ' ITRN=' + map['ITRN.US'] + ' PINFRA=' + map['PINFRA.MX'] + ' NVDA=' + map['NVDA.US'] + ' 2222=' + map['2222.SR']);
  check('T0 seats: three GM seats fetch-failed, 2222.SR (yahoo page) clean',
        map['ITRN.US'] === true && map['PINFRA.MX'] === true && map['NVDA.US'] === true && map['2222.SR'] !== true);
  check('T0 seats absent from the audited 500 (as on the live board)',
        !cands.some(function (c) { return /^(ITRN\.US|PINFRA\.MX|NVDA\.US|2222\.SR)$/.test(c.symbol); }), 'audit rows=' + cands.length);

  // T1 golden-negative on BASE: the live 06:41 outcome reproduces (every seat takes a miss)
  var rb = B.dt10StabCore_([], cands, B.dt10StabParseState_(seedState()), KNOBS, 10, '2026-09-26');
  check('T1 base reproduces the live defect: ITRN 0->1, PINFRA 1->2, NVDA 0->1, 2222 1->2',
        stateOf(rb, 'ITRN.US').co === 1 && stateOf(rb, 'PINFRA.MX').co === 2 && stateOf(rb, 'NVDA.US').co === 1 && stateOf(rb, '2222.SR').co === 2,
        rb.note);
  // T1 delivered with the real outage map: the three fetch-failed seats pause, 2222.SR counts on merit
  var rd = D.dt10StabCore_([], cands, D.dt10StabParseState_(seedState()), KNOBS, 10, '2026-09-26', map);
  check('T1 delivered: ITRN 0 (paused), PINFRA 1 (paused), NVDA 0 (paused), 2222.SR 1->2 (clean row, miss counts)',
        stateOf(rd, 'ITRN.US').co === 0 && stateOf(rd, 'PINFRA.MX').co === 1 && stateOf(rd, 'NVDA.US').co === 0 && stateOf(rd, '2222.SR').co === 2, rd.note);
  check('T1 delivered: op counters 1/1/1/absent; all four still members',
        stateOf(rd, 'ITRN.US').op === 1 && stateOf(rd, 'PINFRA.MX').op === 1 && stateOf(rd, 'NVDA.US').op === 1 && !stateOf(rd, '2222.SR').op &&
        rd.tickets.length === 4 && rd.audit.exited_soft.length === 0 && rd.audit.exited_hard.length === 0);
  check('T1 delivered labels: PINFRA "GRACE (1/3 missed) - outage pause 1/5"; 2222.SR "GRACE (2/3 missed)"',
        tk(rd, 'PINFRA.MX')._stab_status === 'GRACE (1/3 missed) - outage pause 1/5' && tk(rd, 'PINFRA.MX')._p168_paused === true &&
        tk(rd, '2222.SR')._stab_status === 'GRACE (2/3 missed)' && tk(rd, '2222.SR')._p168_paused !== true &&
        tk(rd, 'ITRN.US')._stab_status === 'GRACE (0/3 missed) - outage pause 1/5',
        [tk(rd, 'ITRN.US')._stab_status, tk(rd, 'PINFRA.MX')._stab_status, tk(rd, 'NVDA.US')._stab_status, tk(rd, '2222.SR')._stab_status].join(' | '));
  check('T1 delivered note + audit: "3 outage-paused" and audit.outage_paused = the three GM seats',
        rd.note.indexOf('3 outage-paused') >= 0 && rd.audit.outage_paused.length === 3 && rd.audit.outage_paused.indexOf('2222.SR') < 0, rd.note);
  check('T1 delivered: ghost note explains the pause; sizing stays suspended (dash vocabulary, _grace_hold)',
        tk(rd, 'ITRN.US').advisor_note.indexOf('Outage pause') > 0 && tk(rd, 'ITRN.US').suggested_sar === '—' && tk(rd, 'ITRN.US')._grace_hold === true);
  check('T1 delivered: hist not fed on the paused day (ITRN [79,78.6] unchanged); paint/marker prefix intact',
        json(stateOf(rd, 'ITRN.US').hist) === json([79, 78.6]) && tk(rd, 'ITRN.US')._stab_status.indexOf('GRACE') === 0);
  // T1b: the NEXT poisoned day-advance would soft-exit PINFRA/2222 on base; delivered keeps PINFRA, exits 2222.SR on merit
  var rb2 = B.dt10StabCore_([], cands, B.dt10StabParseState_(rb.state), KNOBS, 10, '2026-09-27');
  var rd2 = D.dt10StabCore_([], cands, D.dt10StabParseState_(rd.state), KNOBS, 10, '2026-09-27', map);
  check('T1b base day 2: PINFRA + 2222.SR soft-exit', rb2.audit.exited_soft.indexOf('PINFRA.MX') >= 0 && rb2.audit.exited_soft.indexOf('2222.SR') >= 0, rb2.note);
  check('T1b delivered day 2: PINFRA held (op 2), 2222.SR exits on merit (3/3), ITRN/NVDA op 2',
        rd2.audit.exited_soft.length === 1 && rd2.audit.exited_soft[0] === '2222.SR' && stateOf(rd2, 'PINFRA.MX').member === true &&
        stateOf(rd2, 'PINFRA.MX').op === 2 && stateOf(rd2, 'ITRN.US').op === 2, rd2.note);

  // T2 kill path (outage null) == base, deep-equal on tickets/state/note
  var rk = D.dt10StabCore_([], cands, D.dt10StabParseState_(seedState()), KNOBS, 10, '2026-09-26', null);
  var rk2 = D.dt10StabCore_([], cands, D.dt10StabParseState_(seedState()), KNOBS, 10, '2026-09-26');
  var auditB = JSON.parse(json(rb.audit)); var auditK = JSON.parse(json(rk.audit)); delete auditK.outage_paused;
  var hbB = auditB.held_by_grace.map(function (h) { return h.symbol + ':' + h.missed_days + '/' + h.exit_days; }).join(',');
  var hbK = auditK.held_by_grace.map(function (h) { return h.symbol + ':' + h.missed_days + '/' + h.exit_days + (h.outage_pause ? 'X' : ''); }).join(',');
  check('T2 kill path: tickets deep-equal to base', json(rk.tickets) === json(rb.tickets) && json(rk2.tickets) === json(rb.tickets));
  check('T2 kill path: state blob byte-identical to base (no op keys written)', json(rk.state) === json(rb.state) && json(rk2.state) === json(rb.state));
  check('T2 kill path: note identical; audit identical apart from the additive outage_paused=[] / outage_pause:0 keys',
        rk.note === rb.note && hbB === hbK && json(auditB.exited_soft) === json(auditK.exited_soft) && rk.audit.outage_paused.length === 0);

  // T3 bounded pause: op already 5 => the miss counts and op keeps counting
  var s5 = seedState(); s5.symbols['ITRN.US'].op = 5;
  var r5 = D.dt10StabCore_([], cands, D.dt10StabParseState_(s5), KNOBS, 10, '2026-09-26', map);
  check('T3 cap: ITRN with op=5 takes the miss (co 1, op 6, no marker); PINFRA/NVDA still paused',
        stateOf(r5, 'ITRN.US').co === 1 && stateOf(r5, 'ITRN.US').op === 6 && tk(r5, 'ITRN.US')._p168_paused !== true &&
        stateOf(r5, 'PINFRA.MX').co === 1 && r5.audit.outage_paused.length === 2, r5.note);
  // T3b: a clean day-advance resets op to 0 (key dropped) and counts normally
  var r5b = D.dt10StabCore_([], cands, D.dt10StabParseState_(rd.state), KNOBS, 10, '2026-09-27', {});
  check('T3b clean day resets the pause counter: op keys dropped, misses count (ITRN co 1, PINFRA co 2)',
        !stateOf(r5b, 'ITRN.US').op && stateOf(r5b, 'ITRN.US').co === 1 && stateOf(r5b, 'PINFRA.MX').co === 2 && r5b.audit.outage_paused.length === 0);

  // T4 same-day re-run stays frozen (no pause bookkeeping either)
  var rf = D.dt10StabCore_([], cands, D.dt10StabParseState_(rd.state), KNOBS, 10, '2026-09-26', map);
  check('T4 same-day re-run: clocks + op frozen, no outage-paused token, labels keep the suffix from state',
        json(rf.state.symbols) === json(rd.state.symbols) && rf.audit.outage_paused.length === 0 && rf.note.indexOf('outage-paused') < 0 &&
        tk(rf, 'PINFRA.MX')._stab_status === 'GRACE (1/3 missed)', rf.note);

  // T5 purity: inputs untouched
  var mapJ = json(map), candJ = json(cands);
  D.dt10StabCore_([], cands, D.dt10StabParseState_(seedState()), KNOBS, 10, '2026-09-26', map);
  check('T5 purity: outage map and audit rows not mutated by the core', json(map) === mapJ && json(cands) === candJ);
  var mapE = D.dt10OutageMapFromPool_(null), mapE2 = D.dt10OutageMapFromPool_([null, {}, { Symbol: 'X' }]);
  check('T5 helper never throws: null -> {}, junk rows -> {}', Object.keys(mapE).length === 0 && Object.keys(mapE2).length === 0);

  // T6 a challenger (ci>0, non-member) pauses too; a raw-present symbol never pauses
  var s6 = seedState(); s6.symbols['CHAL.US'] = { ci: 2, co: 0, member: false, since: '', ls: '2026-09-25', hist: [70], ft: false };
  var map6 = JSON.parse(mapJ); map6['CHAL.US'] = true; map6['RAWX.US'] = true;
  var raw6 = [{ symbol: 'RAWX.US', name: 'Raw X', opportunity_score: 80, suggested_sar: 1000, suggested_shares: 10 }];
  var r6 = D.dt10StabCore_(raw6, cands.concat([{ symbol: 'RAWX.US', opportunity_score: 80, verdict: 'INVEST' }]), D.dt10StabParseState_(s6), KNOBS, 10, '2026-09-26', map6);
  check('T6 challenger CHAL.US keeps ci=2 (paused, op 1); raw-present RAWX.US counts ci=1 despite an outage mark',
        stateOf(r6, 'CHAL.US').ci === 2 && stateOf(r6, 'CHAL.US').op === 1 && stateOf(r6, 'RAWX.US').ci === 1 && !stateOf(r6, 'RAWX.US').op, r6.note);

  // T7 embedded self-test replay on both trees (services stubbed; the pure blocks are what matter)
  function selftest(T) { try { return T.dt10SelfTest(); } catch (e) { return 'THREW ' + e; } }
  var stB = selftest(B), stD = selftest(D);
  var okLines = function (s) { return ['grace sizing core: ok', 'funding containment core: ok', 'cash source core: ok'].every(function (l) { return s.indexOf(l) >= 0; }); };
  check('T7 delivered dt10SelfTest prints "outage pause core: ok" plus the three prior ok lines',
        stD.indexOf('outage pause core: ok') >= 0 && okLines(stD), stD.split('\n').filter(function (l) { return /core: /.test(l); }).join(' | '));
  check('T7 base dt10SelfTest has no outage line and its three ok lines still pass', stB.indexOf('outage pause core') < 0 && okLines(stB));
  check('T7 versions: base DT10_VERSION 1.11.9, delivered 1.11.10', B.DT10_VERSION === '1.11.9' && D.DT10_VERSION === '1.11.10');

  var digestInput = json({ rd: rd.tickets.map(function (t) { return [t.symbol, t._stab_status, t._p168_paused === true]; }), st: rd.state, note: rd.note,
                           rd2: rd2.note, rk: rk.note, r5: r5.note, r6: r6.note, stD: stD.split('\n').filter(function (l) { return /core: /.test(l); }) });
  return { log: log, fails: fails, digest: crypto.createHash('sha256').update(digestInput).digest('hex').slice(0, 12) };
}
var runs = [];
for (var i = 0; i < 3; i++) runs.push(runOnce());
runs[0].log.forEach(function (l) { console.log(l); });
console.log('---');
console.log('runs x3 digests:', runs.map(function (r) { return r.digest; }).join(' '), 'identical=' + (runs[0].digest === runs[1].digest && runs[1].digest === runs[2].digest));
console.log('result:', runs.every(function (r) { return r.fails === 0; }) ? 'ALL PASS' : 'FAILURES ' + runs[0].fails);
process.exit(runs.every(function (r) { return r.fails === 0; }) ? 0 : 1);
