/* P-145 (16_Decision_Top10.gs v1.11.9) — grace sizing suspension.
 * Dual-tree node harness over the REAL file: loads the .gs source in a vm
 * context with minimal GAS shims (PropertiesService/Logger/SpreadsheetApp),
 * replays the 2026-09-16 TSM board through the REAL dt10StabCore_, and
 * proves: T1 base v1.11.8 reproduces the defect (golden-negative, when a
 * base file is supplied); T2 v1.11.9 suspends the src-present GRACE seat;
 * T3 kill switch restores base behavior deep-equal; T4 renderer rank dash;
 * T5 embedded dt10SelfTest replay (three 'core: ok' lines, zero FAIL).
 * Run: node tests/test_dt10_p145_grace_sizing.js [path-to-v1.11.9.gs] [path-to-base.gs]
 */
var fs = require('fs'), vm = require('vm');
var NEW_PATH = process.argv[2] || '16_Decision_Top10.gs';
var BASE_PATH = process.argv[3] || '';

function ctx(props) {
  var store = {}; for (var k in props) store[k] = props[k];
  var logs = [];
  var ssStub = { getSheetByName: function () { return null; },
                 getRangeByName: function () { return null; } };
  return {
    context: {
      PropertiesService: { getScriptProperties: function () {
        return { getProperty: function (k) { return store.hasOwnProperty(k) ? store[k] : null; },
                 setProperty: function (k, v) { store[k] = String(v); },
                 deleteProperty: function (k) { delete store[k]; } }; } },
      Logger: { log: function (m) { logs.push(String(m)); } },
      SpreadsheetApp: { getActiveSpreadsheet: function () { return ssStub; },
                        getUi: function () { throw new Error('no ui'); } },
      Utilities: { formatDate: function () { return '2026-09-16 00:00:00'; } },
      Session: { getScriptTimeZone: function () { return 'Asia/Riyadh'; } },
      UrlFetchApp: { fetch: function () { throw new Error('no net'); } }
    }, logs: logs };
}
function load(path, props) {
  var c = ctx(props || {});
  vm.createContext(c.context);
  vm.runInContext(fs.readFileSync(path, 'utf8'), c.context, { filename: path });
  c.context.__logs = c.logs;
  return c.context;
}
function fixtures() {
  var state = { v: 1, date: '2026-09-16', symbols: {
    'TSM.US':   { ci: 0, co: 1, member: true, since: '2026-09-15', ls: '2026-09-16', hist: [71.5, 71.5], ft: false },
    'PINFRA.MX':{ ci: 0, co: 1, member: true, since: '2026-09-15', ls: '2026-09-16', hist: [76.9], ft: false },
    'KRP.US':   { ci: 1, co: 0, member: true, since: '2026-09-16', ls: '2026-09-16', hist: [72.5], ft: true },
    '2286.SR':  { ci: 0, co: 0, member: true, since: '2026-09-14', ls: '2026-09-16', hist: [71.5], ft: false } } };
  var tsm = { symbol: 'TSM.US', name: 'Taiwan Semiconductor', market: 'NYSE/NASDAQ',
    sector: 'Information Technology', currency: 'USD', fx_to_sar: 3.7531,
    price: 413.75, price_sar: 1552.83, entry_zone: '1,506.25-1,568.36 SAR',
    suggested_sar: 13976, suggested_shares: 9, stop_sar: 1371.28,
    tp1_sar: 1804.25, tp2_sar: 2055.67, roi_pct: 16.2, engine_roi_pct: 32.4,
    ann_roi_pct: 82.2, exp_gain_12m_sar: 11488, reliability: 70.4, dq: 100,
    confidence_band: 'Medium', opportunity_score: 71.5,
    advisor_note: 'INVEST - 13,976 SAR (9 sh)',
    detail: { funds_from: 'Cash 13,976 SAR', review_date: '2026-10-16' } };
  var krp = { symbol: 'KRP.US', name: 'Kimbell Royalty', currency: 'USD',
    price: 14.94, price_sar: 56.07, suggested_sar: 5000, suggested_shares: 89,
    entry_zone: '54-58', stop_sar: 49, tp1_sar: 64, tp2_sar: 70,
    exp_gain_12m_sar: 900, opportunity_score: 72.5,
    advisor_note: 'INVEST', detail: { funds_from: 'Cash 5,000 SAR' } };
  function cand(sym, sc) { return { symbol: sym, name: sym, opportunity_score: sc,
    verdict: 'INVEST', price: 1, price_sar: 1, roi_pct: 1, engine_roi_pct: 1,
    ann_roi_pct: 1, reliability: 70, dq: 100, confidence_band: 'High' }; }
  return { state: state, raw: [krp, tsm],
    cands: [cand('TSM.US', 71.5), cand('KRP.US', 72.5),
            cand('PINFRA.MX', 76.9), cand('2286.SR', 71.5)],
    knobs: { enabled: true, confirm_days: 3, exit_days: 3, rank_buffer: 15,
             smooth_days: 5, hard_strict: true },
    tsm: tsm };
}
function run(g) {
  var f = fixtures();
  var st = g.dt10StabParseState_(JSON.parse(JSON.stringify(f.state)));
  return g.dt10StabCore_(JSON.parse(JSON.stringify(f.raw)),
                         JSON.parse(JSON.stringify(f.cands)),
                         st, f.knobs, 10, '2026-09-16');
}
function bySym(out, s) {
  for (var i = 0; i < out.tickets.length; i++) if (out.tickets[i].symbol === s) return out.tickets[i];
  return null;
}
function assert(c, m) { if (!c) { console.error('FAIL: ' + m); process.exit(1); } }
var DASH = '\u2014';

function battery() {
  var N = load(NEW_PATH, {});
  var K = load(NEW_PATH, { DT10_P145_GRACE_SIZING_LEGACY: '1' });
  var outN = run(N), outK = run(K);
  var tN = bySym(outN, 'TSM.US'), kN = bySym(outN, 'KRP.US');
  // T2 — v1.11.9 suspends the src-present GRACE seat
  assert(tN._stab_status === 'GRACE (1/3 missed)', 'TSM status ' + tN._stab_status);
  assert(tN._grace_hold === true && tN._p145_suspended === true, 'markers');
  ['entry_zone','suggested_sar','suggested_shares','stop_sar','tp1_sar','tp2_sar','exp_gain_12m_sar']
    .forEach(function (fld) { assert(tN[fld] === DASH, 'field ' + fld + '=' + tN[fld]); });
  assert(tN.detail.funds_from === DASH, 'funds_from');
  assert(tN.advisor_note.indexOf('GRACE (1/3 missed)') === 0 &&
         tN.advisor_note.indexOf('sizing suspended') > 0, 'note');
  assert(kN._stab_status.indexOf('FAST-TRACK (day') === 0 && kN._ft_suspended === true,
         'KRP FT suspend intact: ' + kN._stab_status);
  assert(bySym(outN, 'PINFRA.MX')._grace_hold === true &&
         bySym(outN, '2286.SR')._grace_hold === true, 'ghosts');
  // T4 — renderer rank dash + seat classifier
  var row = N.dt10TicketToRow_(tN);
  assert(row[0] === DASH, 'rank cell ' + row[0]);
  var cls = N.dt10TicketClasses_(outN.tickets);
  assert(cls.exec === 0 && cls.grace === 3, 'classes ' + JSON.stringify(cls));
  // T3 — kill switch restores legacy behavior (defect reproduced)
  var tK = bySym(outK, 'TSM.US');
  assert(tK._stab_status === 'GRACE (1/3 missed)' && tK.suggested_shares === 9 &&
         tK.suggested_sar === 13976 && tK._grace_hold !== true, 'kill=legacy defect');
  assert(N.dt10TicketToRow_(tK)[0] === 3 || N.dt10TicketToRow_(tK)[0] === tK.rank, 'kill rank numeric');
  // T1 — base golden-negative (when supplied)
  if (BASE_PATH) {
    var B = load(BASE_PATH, {});
    var outB = run(B), tB = bySym(outB, 'TSM.US');
    assert(tB._stab_status === 'GRACE (1/3 missed)' && tB.suggested_shares === 9 &&
           tB._grace_hold !== true, 'base reproduces defect');
    assert(JSON.stringify(outK) === JSON.stringify(outB), 'kill deep-equal base');
  }
  // T5 — embedded selftest replay
  var rep = String(N.dt10SelfTest());
  assert(rep.indexOf('grace sizing core: ok') > 0, 'selftest grace line');
  assert(rep.indexOf('funding containment core: ok') > 0, 'selftest p142 line');
  assert(rep.indexOf('cash source core: ok') > 0, 'selftest p134 line');
  assert(rep.indexOf(': FAIL') < 0, 'selftest has FAIL');
  return require('crypto').createHash('sha256')
    .update(JSON.stringify([outN, outK])).digest('hex').slice(0, 16);
}
var d1 = battery(), d2 = battery(), d3 = battery();
assert(d1 === d2 && d2 === d3, 'x3 digest mismatch');
console.log('P-145 battery T1-T5 PASS x3 | digest ' + d1);
