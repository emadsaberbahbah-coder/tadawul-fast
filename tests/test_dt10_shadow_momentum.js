#!/usr/bin/env node
/* tests/test_dt10_shadow_momentum.js
 * Harness for apps_script/16_Decision_Top10.gs v1.17.1 [SHADOW-MOMENTUM].
 * Loads the REAL delivered file -- and the v1.17.0 base rebuilt from it by
 * reversing tests/js/dt10_edits_v1171.json (SHA-256 pinned) -- into separate
 * node vm contexts with PropertiesService / Logger stubs (nothing else is
 * needed by the pool projection). Sections:
 *   M1 kill identity: property absent / off / blank / junk / service fault
 *      -> the very v1.17.0 list object; header maps, pool rows and the JSON
 *      body are byte-identical to the base (also under the W52 legacy flag)
 *   M2 armed (on / all / ' On '): Momentum Score appended last; mapped, sent
 *      as the sheet's own number; blank / placeholder / absent column -> no
 *      key; the JSON body differs from the base by exactly that key
 *   M3 golden negative: the v1.17.0 base, armed, never sends it (the defect
 *      the backend logged as momentum=0/26 on 27 Oct)
 *   M4 memo: one projection per execution, one read of each property
 *   M5 pure helper: identity when unarmed, no mutation, no duplicate send
 *   M6 the builder reads exactly this key: opportunity_builder.py alias
 *      'momentumscore' for momentum_score, attached only off legacy
 *   M7 embedded self-test lines (off / on) and the memo line, zero FAIL
 *   M8 version lockstep, WHY block, ES5/ASCII lint of the diff, inventory,
 *      nine hunks whose reversal is the reviewed v1.17.0 file
 *   M9 one mode per execution: the log written after the POST uses the mode
 *      the projection used, even when the property flips mid-run; with no
 *      projection in the execution it reads the property (v1.17.0)
 * Run: node tests/test_dt10_shadow_momentum.js   (exit 0 = all PASS)
 */
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const vm = require('vm');
const history = require('./js/dt10_cash_utc_history');

const SRC = process.argv[2] || path.join(__dirname, '..', 'apps_script', '16_Decision_Top10.gs');
const src = fs.readFileSync(SRC, 'utf8');
// merge note 2026-10-27 (#778): the delivered file is main's v1.17.1 plus #778's
// held-seat regions (v1.17.2; tests/js/dt10_audit_keep_regions.js). Behaviour
// runs on the delivered file; the base oracle, the lint and the inventory
// compare main's v1.17.1 layer: the delivered file with exactly those regions
// removed (stripDesigned throws on any other shape).
// merge note 2026-10-28 (#778): the delivered file is now main's v1.18.0 plus the
// v1.18.1 held-seat regions, so the layer is main's v1.18.0 byte for byte.
// merge note 2026-11-01 (#778): the delivered file is now main's v1.19.0 plus the
// v1.19.1 held-seat regions, so the layer is main's v1.19.0 byte for byte.
const layerSrc = require('./js/dt10_audit_keep_regions.js').stripDesigned(src);
const dt10Floor = (v, f) => { const x = String(v).split('.').map(Number); for (let k = 0; k < f.length; k++) { if (x[k] !== f[k]) return x[k] > f[k]; } return true; };
const deliveredVersion = (src.match(/^var DT10_VERSION = '(\d+\.\d+\.\d+)';$/m) || [])[1];
const OB = path.join(__dirname, '..', 'core', 'analysis', 'opportunity_builder.py');
const base = history.v1170Source(layerSrc);

let pass = 0;
function ok(cond, msg) {
  if (!cond) { console.log('FAIL ' + msg); process.exit(1); }
  pass++;
}
function deep(a, b) { return JSON.stringify(a) === JSON.stringify(b); }

// ------------------------------------------------------------------ loader
function load(source, props, opts) {
  opts = opts || {};
  const reads = [];
  const ctx = {
    Logger: { log() {} },
    Session: { getScriptTimeZone: () => 'Asia/Riyadh' },
    Utilities: { formatDate: (d, zone, pattern) => (pattern === 'yyyy-MM-dd' ? '2026-10-28' : '2026-10-28 04:02:00') },
    PropertiesService: {
      getScriptProperties() {
        if (opts.propsThrow) throw new Error('property service down');
        return {
          getProperty(k) {
            reads.push(k);
            return Object.prototype.hasOwnProperty.call(props || {}, k) ? props[k] : null;
          },
          setProperty() {}
        };
      }
    }
  };
  vm.createContext(ctx);
  vm.runInContext(source, ctx, { filename: '16_Decision_Top10.gs' });
  ctx.__reads = reads;
  return ctx;
}

// A Global_Markets-shaped header slice: the pool contract's own columns, the
// four timing-gate fields, the engine's Momentum Score (column BK on the
// pages) and two unrelated engine columns around it.
const HDR = ['Symbol', 'Name', 'Sector', 'Exchange', 'Currency', 'Current Price', 'Data Provider',
  'Target Price', 'Intrinsic Value', 'Expected ROI 12M', 'Forecast Reliability Score',
  'Data Quality Score', 'Risk Bucket', 'Value Score', 'Momentum Score', 'Growth Score',
  'Investability Status', 'Warnings', 'Last Updated (UTC)', 'Forecast Source',
  '52W High', '52W Low', '52W Position %', 'Percent Change'];
const ROWS = [
  ['OPBK.US', 'OP Bancorp', 'Financials', 'NASDAQ', 'USD', 12.41, 'eodhd', 15.0, 14.2, 18.3, 76.5, 100,
   'MODERATE', 61.2, 57.03, 48.0, 'INVESTABLE', '', '2026-10-27T05:47:00Z', 'provider', 13.9, 10.1, 61.4, 0.012],
  ['WPC.US', 'W. P. Carey', 'Real Estate', 'NYSE', 'USD', 66.2, 'eodhd', 74.0, 70.0, 18.8, 76.5, 100,
   'LOW', 55.0, 59.0, 40.0, 'INVESTABLE', '', '2026-10-27T05:47:00Z', 'provider', 70.1, 54.3, 75.0, -0.004],
  ['LIEN.US', 'Chicago Atlantic', 'Financials', 'NASDAQ', 'USD', 10.9, 'eodhd', 13.0, 12.1, 20.6, 76.5, 100,
   'MODERATE', 50.0, 21.58, 30.0, 'INVESTABLE', '', '2026-10-27T05:47:00Z', 'provider', 13.8, 10.2, 19.9, -0.02],
  ['BLNK.US', 'Blank Momentum', 'Industrials', 'NYSE', 'USD', 5.0, 'eodhd', 6.0, 6.0, 20.0, 70.0, 90,
   'HIGH', 40.0, '', 20.0, 'INVESTABLE', '', '2026-10-27T05:47:00Z', 'provider', 7.0, 4.0, 33.0, 0.0],
  ['DASH.US', 'Dash Momentum', 'Industrials', 'NYSE', 'USD', 5.0, 'eodhd', 6.0, 6.0, 20.0, 70.0, 90,
   'HIGH', 40.0, '—', 20.0, 'INVESTABLE', '', '2026-10-27T05:47:00Z', 'provider', 7.0, 4.0, 33.0, 0.0],
  ['NA.US', 'NA Momentum', 'Industrials', 'NYSE', 'USD', 5.0, 'eodhd', 6.0, 6.0, 20.0, 70.0, 90,
   'HIGH', 40.0, 'n/a', 20.0, 'INVESTABLE', '', '2026-10-27T05:47:00Z', 'provider', 7.0, 4.0, 33.0, 0.0],
  ['ZERO.US', 'Zero Momentum', 'Industrials', 'NYSE', 'USD', 5.0, 'eodhd', 6.0, 6.0, 20.0, 70.0, 90,
   'HIGH', 40.0, 0, 20.0, 'INVESTABLE', '', '2026-10-27T05:47:00Z', 'provider', 7.0, 4.0, 33.0, 0.0]
];
const MOM = HDR.indexOf('Momentum Score');
function project(ctx, header, rows, page) {
  const map = ctx.dt10MapHeaderCols_(header || HDR);
  const out = (rows || ROWS).map((r) => ctx.dt10PoolRowFromSheetRow_(r, map, page || 'Global_Markets'));
  return { map, rows: out, body: JSON.stringify(out) };
}
const baseOff = load(base, {});
const baseProj = project(baseOff, HDR, ROWS);
ok(baseProj.rows.every((r) => r && r['Momentum Score'] === undefined), 'base projection never carries Momentum Score');

// ------------------------------------------------------------ M1 kill identity
const OFF_PROPS = [{}, { DT10_SHADOW_RANK_LOG: 'off' }, { DT10_SHADOW_RANK_LOG: '' },
  { DT10_SHADOW_RANK_LOG: 'yes' }, { DT10_SHADOW_RANK_LOG: '1' }, { DT10_SHADOW_RANK_LOG: 'onn' }];
for (const props of OFF_PROPS) {
  const n = load(src, props);
  const list = n.dt10PoolFieldsActive_();
  ok(list === n.DT10_POOL_FIELDS, 'M1 unarmed ' + JSON.stringify(props) + ': the v1.17.0 list object itself');
  const p = project(n, HDR, ROWS);
  ok(deep(p.map, baseProj.map), 'M1 header map identical to the base ' + JSON.stringify(props));
  ok(p.body === baseProj.body, 'M1 POST rows byte-identical to the base ' + JSON.stringify(props));
}
const nFault = load(src, {}, { propsThrow: true });
ok(nFault.dt10PoolFieldsActive_() === nFault.DT10_POOL_FIELDS, 'M1 property service fault -> unarmed, never throws');
ok(project(nFault, HDR, ROWS).body === baseProj.body, 'M1 fault: rows byte-identical to the base');
const legBase = load(base, { DT10_P181B_W52_LEGACY: '1' });
const legNew = load(src, { DT10_P181B_W52_LEGACY: '1' });
ok(deep(legNew.dt10PoolFieldsActive_().map((f) => f.send), legBase.dt10PoolFieldsActive_().map((f) => f.send)),
   'M1 W52 legacy flag, unarmed: the base legacy list');
ok(project(legNew, HDR, ROWS).body === project(legBase, HDR, ROWS).body, 'M1 W52 legacy flag, unarmed: rows identical');
console.log('M1 PASS kill identity (absent / off / blank / junk / fault / legacy flag)');

// ------------------------------------------------------------ M2 armed
for (const mode of ['on', 'all', ' On ', 'ALL']) {
  const n = load(src, { DT10_SHADOW_RANK_LOG: mode });
  const list = n.dt10PoolFieldsActive_();
  ok(list.length === n.DT10_POOL_FIELDS.length + 1 && list[list.length - 1].send === 'Momentum Score' &&
     deep(list.slice(0, -1), n.DT10_POOL_FIELDS) && list[list.length - 1] === n.DT10_SHADOW_POOL_FIELDS[0],
     'M2 armed ' + JSON.stringify(mode) + ': the v1.17.0 list + Momentum Score, last');
  const p = project(n, HDR, ROWS);
  ok(p.map['Momentum Score'] === MOM, 'M2 Momentum Score mapped to its column');
  const want = { 'OPBK.US': 57.03, 'WPC.US': 59.0, 'LIEN.US': 21.58, 'ZERO.US': 0 };
  for (const r of p.rows) {
    const sym = r.Symbol;
    if (Object.prototype.hasOwnProperty.call(want, sym)) {
      ok(r['Momentum Score'] === want[sym], 'M2 ' + sym + ' sends the sheet number as is (' + r['Momentum Score'] + ')');
    } else {
      ok(r['Momentum Score'] === undefined, 'M2 ' + sym + ' blank / placeholder momentum -> no key');
    }
  }
  // every other key and value equals the base row; only Momentum Score is added
  p.rows.forEach((r, i) => {
    const b = baseProj.rows[i];
    const rk = Object.keys(r).filter((k) => k !== 'Momentum Score');
    ok(deep(rk, Object.keys(b)) && rk.every((k) => r[k] === b[k]), 'M2 row ' + i + ' differs from the base only by Momentum Score');
  });
  const noCol = HDR.filter((h) => h !== 'Momentum Score');
  const noColRows = ROWS.map((r) => r.filter((_v, i) => i !== MOM));
  const pn = project(n, noCol, noColRows);
  ok(pn.map['Momentum Score'] === null && pn.body === project(baseOff, noCol, noColRows).body,
     'M2 a page without the column: map null, rows byte-identical to the base');
}
const legOn = load(src, { DT10_P181B_W52_LEGACY: '1', DT10_SHADOW_RANK_LOG: 'on' });
const legList = legOn.dt10PoolFieldsActive_();
ok(legList.length === legOn.DT10_POOL_FIELDS.length - 4 + 1 && legList[legList.length - 1].send === 'Momentum Score' &&
   !legList.some((f) => legOn.DT10_P181B_SENDS[f.send]), 'M2 W52 legacy flag + armed: legacy list + Momentum Score');
console.log('M2 PASS armed projection');

// ------------------------------------------------------------ M3 golden negative
const baseOn = load(base, { DT10_SHADOW_RANK_LOG: 'on' });
ok(baseOn.DT10_VERSION === '1.17.0' && baseOn.DT10_SHADOW_POOL_FIELDS === undefined &&
   typeof baseOn.dt10PoolFieldsWithShadow_ === 'undefined', 'M3 the base is v1.17.0 without the new names');
const bp = project(baseOn, HDR, ROWS);
ok(bp.rows.every((r) => r['Momentum Score'] === undefined) && bp.body === baseProj.body,
   'M3 the base, armed, never sends Momentum Score (the 27 Oct momentum=0/26)');
ok(project(load(src, { DT10_SHADOW_RANK_LOG: 'on' }), HDR, ROWS).body !== bp.body, 'M3 the delivered file, armed, does');
console.log('M3 PASS golden negative on the v1.17.0 base');

// ------------------------------------------------------------ M4 memo
const memo = load(src, { DT10_SHADOW_RANK_LOG: 'on' });
memo.__reads.length = 0;
const a1 = memo.dt10PoolFieldsActive_(), a2 = memo.dt10PoolFieldsActive_(), a3 = memo.dt10PoolFieldsActive_();
ok(a1 === a2 && a2 === a3, 'M4 one projection object per execution');
ok(deep(memo.__reads.slice().sort(), ['DT10_P181B_W52_LEGACY', 'DT10_SHADOW_RANK_LOG']), 'M4 each property read once: ' + JSON.stringify(memo.__reads));
project(memo, HDR, ROWS);
ok(memo.__reads.length === 2, 'M4 projecting rows reads no property (memoized)');
memo.dt10PropMemoReset_();
memo.dt10PoolFieldsActive_();
ok(memo.__reads.length === 4, 'M4 a new execution (memo reset) reads both once again');
console.log('M4 PASS memo');

// ------------------------------------------------------------ M5 pure helper
const h = load(src, {});
const f0 = h.DT10_POOL_FIELDS;
const snap = JSON.stringify(f0);
ok(h.dt10PoolFieldsWithShadow_(f0, false) === f0, 'M5 unarmed -> the same array');
const on1 = h.dt10PoolFieldsWithShadow_(f0, true);
ok(on1 !== f0 && on1.length === f0.length + 1 && JSON.stringify(f0) === snap, 'M5 armed -> a new array; the input untouched');
ok(h.dt10PoolFieldsWithShadow_(on1, true).length === on1.length, 'M5 no duplicate send when already present');
ok(h.dt10PoolFieldsWithShadow_([], true).length === 1 && h.dt10PoolFieldsWithShadow_([], 0).length === 0, 'M5 empty list');
console.log('M5 PASS pure helper');

// ------------------------------------------------------------ M6 builder alias
const ob = fs.readFileSync(OB, 'utf8');
ok(/"momentum_score": \("momentumscore",\),/.test(ob), 'M6 opportunity_builder alias momentum_score <- momentumscore');
ok('Momentum Score'.toLowerCase().replace(/[^a-z0-9]/g, '') === 'momentumscore', 'M6 the send normalizes to the alias');
ok(/if\s+_env_rank_mode\(\)\s*!=\s*["']legacy["']\s*:\s*\n\s+cand\[["']momentum_score["']\]\s*=\s*_to_float\(_field\(view,\s*["']momentum_score["']\)\)/.test(ob),
   'M6 the builder attaches momentum_score only off legacy (champion never reads it)');
console.log('M6 PASS builder alias');

// ------------------------------------------------------------ M7 self-test lines
const s0 = src.indexOf('  /* v1.17.1 [SHADOW-MOMENTUM]: the challenger\'s momentum input travels on');
// 2026-10-27: v1.18.0 (post-earnings tag) inserts its self-test block between this one and
// SYNC-INFLIGHT; the slice ends where that block starts (pin moved).
const sPe = src.indexOf('  /* v1.18.0 [POST-EARNINGS]: pure core on the FRST case', s0);
const s1 = sPe > 0 ? sPe : src.indexOf('  /* v1.11.12 [SYNC-INFLIGHT]: pure core on the 2026-10-03 13:05 race', s0);
const m0 = src.indexOf('  /* v1.11.13 [P-202]: property memo -- the no-arg projection is ONE list');
ok(s0 > 0 && s1 > s0 && m0 > 0 && m0 < s0, 'M7 self-test blocks located');
function runBlock(ctx, code) {
  const report = [];
  vm.runInContext('(function (report) {\n' + code + '\n})', ctx)(report);
  return report;
}
for (const [props, sent] of [[{}, 'no'], [{ DT10_SHADOW_RANK_LOG: 'on' }, 'yes'], [{ DT10_SHADOW_RANK_LOG: 'all' }, 'yes']]) {
  const c = load(src, props);
  const before = c.dt10PoolFieldsActive_();
  const rep = runBlock(c, src.slice(m0, s1));
  ok(rep.length === 2 && /^property memo core: ok/.test(rep[0]), 'M7 memo line ok ' + JSON.stringify(props) + ': ' + rep[0]);
  ok(rep[1] === 'shadow pool fields core: ok (Momentum Score sent only while the shadow log is armed) | sent now: ' + sent,
     'M7 shadow pool fields line ' + JSON.stringify(props) + ': ' + rep[1]);
  ok(c.dt10PoolFieldsActive_().length === before.length, 'M7 the self-test leaves the projection as it found it');
}
console.log('M7 PASS embedded self-test lines');

// ------------------------------------------------------------ M8 static
// 2026-10-27: v1.18.0 (post-earnings tag) stacked on this change; pins moved. The delivered file
// is v1.18.0; the added-lines lint and the census compare the v1.17.1 file rebuilt from it
// (v1.18.0 edits reversed, SHA-256 pinned) with v1.17.0.
// 2026-11-01: v1.19.0 (qualified hold, seat live fill, owner rule) stacked as well; pins moved.
// merge note 2026-11-01 (#778): held seats are v1.19.1 over #887's v1.19.0: the rebuild starts from the layer (the delivered file with
// this bundle's regions stripped = main's v1.19.0); the delivered file sits above it.
const s1171 = history.v1171Source(layerSrc);
ok((layerSrc.match(/^var DT10_VERSION = '1\.19\.0';$/m) || []).length === 1 && /^ \* Version: 1\.19\.0 \(see DT10_VERSION;/.test(layerSrc.split('\n')[3]) &&
   (s1171.match(/^var DT10_VERSION = '1\.17\.1';$/m) || []).length === 1 && /^ \* Version: 1\.17\.1 \(see DT10_VERSION;/.test(s1171.split('\n')[3]),
   'M8 version lockstep 1.19.0 (layer) and 1.17.1 (rebuilt)');
ok(dt10Floor(deliveredVersion, [1, 19, 0]) && (src.match(/^var DT10_VERSION = '/mg) || []).length === 1 &&
   src.split('\n')[3].indexOf(' * Version: ' + deliveredVersion + ' (see DT10_VERSION;') === 0,
   'M8 delivered version floor 1.19.0 in lockstep: ' + deliveredVersion);
const why = src.slice(src.indexOf(' * v1.17.1 (2026-10-27) [SHADOW-MOMENTUM]'), src.indexOf(' * v1.17.0 (2026-10-27) [SHADOW-RANK-LOG]'));
ok(why.length > 1500 && /^[\x09\x0a\x20-\x7e]*$/.test(why), 'M8 WHY block present (newest first) and ASCII');
ok(src.indexOf(base.slice(base.indexOf(' * v1.17.0 (2026-10-27) [SHADOW-RANK-LOG]'), base.indexOf(' * v1.16.0 (2026-10-26) [FUNDING-EVIDENCE]'))) > 0,
   'M8 the v1.17.0 WHY block verbatim');
const baseLines = new Set(base.split('\n'));
const added = s1171.split('\n').filter((l) => !baseLines.has(l));
ok(added.length >= 80 && added.length <= 160, 'M8 added lines ' + added.length);
for (const l of added) {
  if (/^ \* Version: 1\.17\.1 /.test(l)) continue;   // the version line keeps its pre-existing em dash
  ok(/^[\x09\x20-\x7e]*$/.test(l), 'M8 ASCII added line: ' + l);
  const code = l.replace(/\/\/.*$/, '');
  ok(!/\b(let|const|class)\s|=>|`|\.\.\./.test(code) || /^\s*(\*|\/\*)/.test(l), 'M8 ES5 added line: ' + l);
}
function census(s, re) { const out = new Set(); let m; while ((m = re.exec(s))) out.add(m[1]); return out; }
const fb = census(base, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), fn = census(s1171, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
const vb = census(base, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = census(s1171, /^var\s+([A-Za-z_$][\w$]*)/mg);
ok([...fb].every((f) => fn.has(f)) && [...vb].every((v) => vn.has(v)), 'M8 zero functions / vars removed');
ok(deep([...fn].filter((f) => !fb.has(f)), ['dt10PoolFieldsWithShadow_']) && deep([...vn].filter((v) => !vb.has(v)), ['DT10_SHADOW_POOL_FIELDS']),
   'M8 exactly the designed names added');
ok(fb.size === 217 && fn.size === 218 && vb.size === 116 && vn.size === 117, 'M8 census 217 -> 218 functions, 116 -> 117 vars');
const edits = require('./js/dt10_edits_v1171.json');
ok(edits.length === 9, 'M8 nine hunks');
ok(crypto.createHash('sha256').update(base).digest('hex') === history.V1170_SHA256, 'M8 reversal = the reviewed v1.17.0 file (SHA-256 pinned)');
new vm.Script(src, { filename: '16_Decision_Top10.gs' });
console.log('M8 PASS lockstep, WHY, lint, inventory, hunks');

// ------------------------------------------------------------ M9 one mode per execution
const props9 = { DT10_SHADOW_RANK_LOG: 'on' };
const c9 = load(src, props9);
const noBlock = { meta: {}, selected: [] };
c9.dt10PropMemoReset_();
const sent9 = c9.dt10PoolFieldsActive_();
ok(sent9.length === c9.DT10_POOL_FIELDS.length + 1 && c9.DT10_PROP_MEMO_.shadow_log_mode === 'on',
   'M9 the projection keeps the mode it used (on)');
props9.DT10_SHADOW_RANK_LOG = 'off';                    // flipped after the rows were built
c9.__reads.length = 0;
ok(c9.dt10AppendShadowRankLog_(null, noBlock, 'c') === 'ShadowLog: none (no challenger block)' &&
   c9.__reads.indexOf('DT10_SHADOW_RANK_LOG') < 0,
   'M9 rows sent with momentum -> the log still runs as armed (no second read)');
c9.dt10PropMemoReset_();
c9.dt10PoolFieldsActive_();                             // a new execution: off now
ok(c9.DT10_PROP_MEMO_.shadow_log_mode === 'off', 'M9 a new execution reads the property again (off)');
props9.DT10_SHADOW_RANK_LOG = 'on';                     // flipped on after rows went without momentum
ok(c9.dt10AppendShadowRankLog_(null, noBlock, 'c') === '',
   'M9 rows sent without momentum -> no log event that day from this run');
c9.dt10PropMemoReset_();                                // no projection in this execution
c9.__reads.length = 0;
ok(c9.dt10AppendShadowRankLog_(null, noBlock, 'c') === 'ShadowLog: none (no challenger block)' &&
   c9.__reads.filter((k) => k === 'DT10_SHADOW_RANK_LOG').length === 1,
   'M9 no projection in the execution -> one live read, as in v1.17.0');
const base9 = load(base, { DT10_SHADOW_RANK_LOG: 'on' });
ok(base9.dt10AppendShadowRankLog_(null, noBlock, 'c') === 'ShadowLog: none (no challenger block)',
   'M9 the v1.17.0 base answers the same on the same call');
console.log('M9 PASS one mode per execution');

const digest = crypto.createHash('sha256').update(src).digest('hex').slice(0, 12);
console.log('ALL PASS (' + pass + ' checks) on ' + path.basename(SRC) + ' sha256 ' + digest);
