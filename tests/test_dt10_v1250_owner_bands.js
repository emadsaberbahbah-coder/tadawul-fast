/* OWNER-BANDS (16_Decision_Top10.gs v1.25.0) -- the owner rule can accept Medium.
 * DT10_OWNER_RULE_BANDS = 'HIGH,MEDIUM' (owner decision A, 8 Nov 2026): the
 * v1.19.0 Owner Rule cell and the v1.20.0 ticket guard accept every listed
 * confidence band; ROI 12M < 34, earnings, ex-dividend and the ticket cap are
 * unchanged; absent / blank / 'HIGH' / junk / a fault = the v1.24.0 rule.
 *
 * Dual-tree node harness over the REAL file; the base is the v1.24.0 file
 * rebuilt by reversing the enumerated v1.25.0 edits (SHA-256 pinned).
 *   O0 base oracle; versions in lockstep; both compile; census +4 functions
 *      +2 vars, 0 removed; 16 hunks reverse to the pinned base.
 *   O1 GOLDEN NEGATIVE (10:14 Riyadh, 8 Nov: 'tguard: held 1 (EFC.US owner
 *      rule)') on v1.24.0: an ACTIVE Medium ticket with ROI < 34 is held by
 *      the owner rule even when the cfg carries the band set. v1.25.0 armed:
 *      kept; Low held; Medium at ROI >= 34 held; earnings still first; the
 *      cap still applies after the rule.
 *   O2 parser and band test: HIGH always in; any unknown word, blank, 'HIGH'
 *      alone = off; separators , ; + / and spaces; any case.
 *   O3 KILL: the property absent / blank / HIGH / junk / partly junk, the
 *      owner rule off, or a PropertiesService fault -> dt10TicketGuardConfig_,
 *      the guard core, the cells, the note and the token deep-equal v1.24.0.
 *   O4 service reads: cfg.ownerBands only when DT10_GUARD_OWNER_RULE=on AND
 *      armed; the orchestrator's token gains ', rule bands HIGH/MEDIUM' and
 *      its Logger line 'owner rule on (HIGH/MEDIUM)'.
 *   O5 embedded dt10SelfTest: two new lines ok, zero FAIL, base + 2 lines; the
 *      base lacks them; armed line names the bands.
 *   O6 lint: ES5/ASCII diff, LF only, WHY block newest-first.
 *   O7 mutants: four one-line mutations are each caught.
 *
 * The whole battery runs three times and the three digests must match.
 * Usage: node tests/test_dt10_v1250_owner_bands.js [new.gs] [base.gs]
 */
'use strict';
var fs = require('fs'), vm = require('vm'), path = require('path'), crypto = require('crypto');
var ROOT = path.join(__dirname, '..');
var NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '16_Decision_Top10.gs');
var BASE_PATH = process.argv[3] || '';
var history = require(path.join(ROOT, 'tests', 'js', 'dt10_cash_utc_history.js'));
var EDITS = require(path.join(ROOT, 'tests', 'js', 'dt10_edits_v1250.json'));
var OB = 'DT10_OWNER_RULE_BANDS', TG = 'DT10_TICKET_GUARD', OR = 'DT10_GUARD_OWNER_RULE';

// 2026-11-08: the delivered file is v1.26.0 [FUNDABLE-SEATS]; this harness proves the v1.25.0 diff, so it reads
// the v1.25.0 file rebuilt from the delivered one (tests/js/dt10_edits_v1260.json reversed, SHA-256 pinned).
var newSource = history.v1250Source(fs.readFileSync(NEW_PATH, 'utf8'));
function sha256(s) { return crypto.createHash('sha256').update(s).digest('hex'); }
var baseSource;
try {
  baseSource = BASE_PATH ? fs.readFileSync(BASE_PATH, 'utf8') : history.v1240Source(newSource);
} catch (eBase) {
  console.log('FAIL O0 base oracle: ' + String(eBase && eBase.message || eBase).split('\n')[0]);
  console.log('FAIL 0/1');
  process.exit(1);
}

var passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n  ')); }
}
function assert(c, m) { if (!c) throw new Error(m || 'assertion failed'); }
function eq(a, b, m) { if (a !== b) throw new Error((m || 'not equal') + ': ' + JSON.stringify(a) + ' !== ' + JSON.stringify(b)); }
function deq(a, b, m) {
  var ja = JSON.stringify(a), jb = JSON.stringify(b);
  if (ja !== jb) throw new Error((m || 'deep-equal') + ': ' + ja.slice(0, 300) + ' | ' + jb.slice(0, 300));
}
function plain(v) { return JSON.parse(JSON.stringify(v)); }
function names(src, re) { var out = {}, m; while ((m = re.exec(src)) !== null) out[m[1]] = true; return out; }

/* ------------------------------------------------------------ GAS shims */
function load(source, props, options) {
  options = options || {};
  var store = {}; for (var k in props) if (props.hasOwnProperty(k)) store[k] = props[k];
  var logs = [];
  var statusRows = [];
  for (var r = 0; r < 60; r++) statusRows.push(['', '']);
  var statusSheet = { getRange: function () { return { getValues: function () { return plain(statusRows); } }; } };
  var ssStub = { getSheetByName: function (n) { return n === '_Status' ? statusSheet : null; }, getRangeByName: function () { return null; } };
  var c = {
    PropertiesService: { getScriptProperties: function () {
      if (options.propsThrow) throw new Error('synthetic property outage');
      return { getProperty: function (key) { return store.hasOwnProperty(key) ? store[key] : null; },
               setProperty: function (key, v) { store[key] = String(v); },
               deleteProperty: function (key) { delete store[key]; } }; } },
    Logger: { log: function (m) { logs.push(String(m)); } },
    SpreadsheetApp: { getActiveSpreadsheet: function () { return ssStub; }, getActive: function () { return ssStub; },
                      getUi: function () { throw new Error('no ui'); } },
    Utilities: { formatDate: function (d, tz, pattern) { return pattern === 'yyyy-MM-dd' ? '2026-10-25' : '2026-10-25 00:00:00'; } },
    Session: { getScriptTimeZone: function () { return 'Asia/Riyadh'; } },
    UrlFetchApp: { fetch: function () { throw new Error('no net'); } }
  };
  vm.createContext(c);
  vm.runInContext(source, c, { filename: '16_Decision_Top10.gs' });
  c.__logs = logs;
  return c;
}

/* ------------------------------------------------------------ fixtures */
function tk(sym, band, roi, extra) {
  var t = { symbol: sym, confidence_band: band, engine_roi_pct: roi, suggested_shares: 4, suggested_sar: 1200 };
  for (var x in (extra || {})) if (extra.hasOwnProperty(x)) t[x] = extra[x];
  return t;
}
/* the 10:14 board, executable part: EFC.US Medium 20.1, PFS.US High 25.3, a Low,
   a Medium at the ROI cap and a High in earnings */
function board() {
  return [tk('EFC.US', 'Medium', 20.1), tk('PFS.US', 'High', 25.3), tk('LOW.US', 'Low', 18),
          tk('CAP.US', 'Medium', 34.6), tk('ERN.US', 'High', 22)];
}
var EARN = { 'ERN.US': 1 };
function guard(M, cfg) { return plain(M.dt10TicketGuardCore_(board(), true, EARN, cfg, null)); }
function heldSyms(res) { return res.held.map(function (h) { return h.symbol + ':' + h.kind; }).join(','); }
var CELLS = [{ confidence_band: 'High', engine_roi_pct: 31.6 }, { confidence_band: 'Medium', engine_roi_pct: 34.8 },
             { confidence_band: 'Medium', engine_roi_pct: 20 }, { confidence_band: '', engine_roi_pct: '' },
             { confidence_band: 'low', engine_roi_pct: 10 }, { confidence_band: ' medium ', engine_roi_pct: '12.5' }];

function battery() {
  var N = load(newSource, {}), B = load(baseSource, {});
  var digestParts = [];
  var HM = N.dt10OwnerRuleBandsParse_('HIGH,MEDIUM');

  test('O0 base oracle is the reviewed v1.24.0 file (SHA-256 pinned); versions in lockstep; both compile; census +4 functions +2 vars, 0 removed; 16 hunks reverse to the base', function () {
    eq(sha256(baseSource), history.V1240_SHA256, 'base SHA-256');
    eq(B.DT10_VERSION, '1.24.0'); eq(N.DT10_VERSION, '1.25.0');
    assert(/^ \* Version: 1\.25\.0 /.test(newSource.split('\n')[3]), 'header line 4 lockstep');
    new vm.Script(newSource, { filename: 'new.gs' }); new vm.Script(baseSource, { filename: 'base.gs' });
    var fb = names(baseSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), fn = names(newSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
    deq(Object.keys(fb).filter(function (k) { return !fn[k]; }), [], 'functions removed');
    deq(Object.keys(fn).filter(function (k) { return !fb[k]; }).sort(),
        ['dt10OwnerRuleBandOk_', 'dt10OwnerRuleBandsParse_', 'dt10OwnerRuleBandsText_', 'dt10OwnerRuleBands_'], 'functions added');
    var vb = names(baseSource, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = names(newSource, /^var\s+([A-Za-z_$][\w$]*)/mg);
    deq(Object.keys(vb).filter(function (k) { return !vn[k]; }), [], 'vars removed');
    deq(Object.keys(vn).filter(function (k) { return !vb[k]; }).sort(), ['DT10_OWNER_RULE_BANDS_PROP', 'DT10_OWNER_RULE_BAND_ORDER'], 'vars added');
    eq(N.DT10_OWNER_RULE_BANDS_PROP, OB); deq(plain(N.DT10_OWNER_RULE_BAND_ORDER), ['HIGH', 'MEDIUM', 'LOW']);
    eq(EDITS.length, 16, 'hunk count');
    for (var e = 0; e < EDITS.length; e++) eq(newSource.split(EDITS[e][0]).length - 1, 1, 'each current is unique: ' + EDITS[e][0].split('\n')[0]);
    eq(sha256(history.v1240Source(newSource)), history.V1240_SHA256, 'reversal of the delivered file');
  });

  test('O1 GOLDEN NEGATIVE on v1.24.0: EFC.US (Medium, ROI 20.1) is held by the owner rule even with the band set in cfg; v1.25.0 armed keeps it, holds Low and the Medium at the ROI cap; earnings first; the cap applies after the rule', function () {
    var cfgArmed = { earnDays: 3, ownerRule: true, ownerBands: HM };
    var b = guard(B, cfgArmed), n = guard(N, cfgArmed);
    eq(heldSyms(b), 'EFC.US:rule,LOW.US:rule,CAP.US:rule,ERN.US:earn', 'base holds EFC.US (owner rule)');
    eq(heldSyms(n), 'LOW.US:rule,CAP.US:rule,ERN.US:earn', 'armed keeps EFC.US');
    eq(n.kept, 2, 'EFC.US and PFS.US kept');
    var c = guard(N, { earnDays: 3, ownerRule: true, ownerBands: HM, maxTickets: 1 });
    eq(heldSyms(c), 'PFS.US:cap,LOW.US:rule,CAP.US:rule,ERN.US:earn', 'cap 1 keeps the first survivor (EFC.US)');
    var all = guard(N, { earnDays: 3, ownerRule: true, ownerBands: N.dt10OwnerRuleBandsParse_('high medium low') });
    eq(heldSyms(all), 'CAP.US:rule,ERN.US:earn', 'Low too when listed; ROI >= 34 still held');
    digestParts.push(b, n, c, all);
  });

  test('O2 parser and band test: HIGH always in; unknown word / blank / HIGH alone = off; separators and case', function () {
    var P = N.dt10OwnerRuleBandsParse_;
    [null, undefined, '', '   ', 'HIGH', ' high ', 'HIGH,FOO', 'medium,x', 'MED', '1', 'HIGH;HIGH'].forEach(function (raw) {
      eq(P(raw), null, 'off: ' + JSON.stringify(raw));
    });
    deq(plain(P('medium')), { HIGH: true, MEDIUM: true }, 'HIGH added');
    deq(plain(P('High+Medium')), { HIGH: true, MEDIUM: true }, 'plus');
    deq(plain(P(' HIGH / medium ; Low ')), { HIGH: true, MEDIUM: true, LOW: true }, 'slash, semicolon, spaces');
    deq(plain(P('HIGH,,MEDIUM,')), { HIGH: true, MEDIUM: true }, 'empty tokens ignored');
    eq(N.dt10OwnerRuleBandsText_(P('low,medium')), 'HIGH/MEDIUM/LOW', 'fixed order');
    eq(N.dt10OwnerRuleBandsText_(null), '', 'no set');
    var cellsN = CELLS.map(function (t) { return N.dt10OwnerRuleCell_(t, HM); });
    deq(cellsN, ['rule: yes', 'rule: no (ROI 34.8)', 'rule: yes', 'rule: no (conf unknown; ROI unknown)',
                 'rule: no (low)', 'rule: yes'], 'armed cells');
    eq(N.dt10OwnerRuleBandOk_('Medium', { HIGH: true, toString: true }), false, 'own true flags only');
    eq(N.dt10OwnerRuleNote_(CELLS, HM), 'owner-rule 3/6 yes', 'armed note');
    digestParts.push(cellsN);
  });

  test('O3 KILL: absent / blank / HIGH / junk / partly junk, owner rule off, or a fault -> config, guard, cells, note and token deep-equal v1.24.0', function () {
    var cells0 = CELLS.map(function (t) { return B.dt10OwnerRuleCell_(t); });
    deq(CELLS.map(function (t) { return N.dt10OwnerRuleCell_(t); }), cells0, 'cells without a set');
    deq(CELLS.map(function (t) { return N.dt10OwnerRuleCell_(t, null); }), cells0, 'cells with null');
    eq(N.dt10OwnerRuleNote_(CELLS), B.dt10OwnerRuleNote_(CELLS), 'note');
    [undefined, '', ' ', 'HIGH', 'high', 'junk', 'HIGH,FOO', 'Medium,Xtra'].forEach(function (raw) {
      var props = {}; props[TG] = 'enforce'; props[OR] = 'on';
      if (raw !== undefined) props[OB] = raw;
      var n = load(newSource, props), b = load(baseSource, props);
      var cn = plain(n.dt10TicketGuardConfig_()), cb = plain(b.dt10TicketGuardConfig_());
      deq(cn, cb, 'config for ' + JSON.stringify(raw));
      deq(guard(n, cn), guard(b, cb), 'guard for ' + JSON.stringify(raw));
      eq(n.dt10TicketGuardToken_(guard(n, cn), 'enforce'), b.dt10TicketGuardToken_(guard(b, cb), 'enforce'), 'token');
    });
    var offRule = {}; offRule[TG] = 'enforce'; offRule[OB] = 'HIGH,MEDIUM';
    deq(plain(load(newSource, offRule).dt10TicketGuardConfig_()), plain(load(baseSource, offRule).dt10TicketGuardConfig_()), 'owner rule off: no ownerBands key');
    var faultN = load(newSource, {}, { propsThrow: true }), faultB = load(baseSource, {}, { propsThrow: true });
    eq(faultN.dt10OwnerRuleBands_(), null, 'fault reads as off');
    deq(plain(faultN.dt10TicketGuardConfig_()), plain(faultB.dt10TicketGuardConfig_()), 'fault config');
    digestParts.push(cells0);
  });

  test("O4 service reads: cfg.ownerBands only with the owner rule on AND armed; the orchestrator token gains ', rule bands HIGH/MEDIUM' and the Logger line names the bands", function () {
    var props = {}; props[TG] = 'observe'; props[OR] = 'on'; props[OB] = 'High, Medium';
    var n = load(newSource, props);
    var cfg = plain(n.dt10TicketGuardConfig_());
    deq(cfg.ownerBands, { HIGH: true, MEDIUM: true }, 'cfg.ownerBands');
    eq(Object.keys(cfg).join(','), 'mode,earnDays,ownerRule,maxTickets,ownerBands', 'key order: v1.24.0 keys then ownerBands');
    var payload = { selected: board(), _dt10_tguard: n.dt10TicketGuardConfig_(), _dt10_uv: { state: 'EXECUTABLE' }, kpis: {} };
    var tok = n.dt10TicketGuard_(payload, EARN, null);
    eq(tok, 'tguard: would hold LOW.US(owner rule) CAP.US(owner rule) ERN.US(earn 1d), rule bands HIGH/MEDIUM', 'observe token');
    var line = n.__logs.filter(function (l) { return /ticket guard \(observe\)/.test(l); })[0] || '';
    assert(line.indexOf('owner rule on (HIGH/MEDIUM), cap none') > 0, 'Logger line: ' + line);
    var b = load(baseSource, props);
    var payloadB = { selected: board(), _dt10_tguard: b.dt10TicketGuardConfig_(), _dt10_uv: { state: 'EXECUTABLE' }, kpis: {} };
    eq(b.dt10TicketGuard_(payloadB, EARN, null), 'tguard: would hold EFC.US(owner rule) LOW.US(owner rule) CAP.US(owner rule) ERN.US(earn 1d)', 'GOLDEN NEGATIVE token on v1.24.0');
    digestParts.push(cfg, tok);
  });

  test('O5 embedded dt10SelfTest: two new lines ok, zero FAIL, base + 2 lines; the base lacks them; armed line names the bands', function () {
    var rep = String(N.dt10SelfTest()), repB = String(B.dt10SelfTest());
    assert(rep.indexOf('[DT10 v1.25.0] self-test') === 0, 'version line');
    ['owner rule bands (DT10_OWNER_RULE_BANDS): off (High only)', 'owner rule bands core: ok'].forEach(function (l) {
      assert(rep.indexOf(l) > 0, 'line: ' + l); assert(repB.indexOf('owner rule bands') < 0, 'GOLDEN NEGATIVE: the base lacks ' + l);
    });
    assert(rep.indexOf(': FAIL') < 0 && rep.indexOf('FAIL [') < 0, 'no FAIL');
    eq(rep.split('\n').length, repB.split('\n').length + 2, 'two lines added');
    var p = {}; p[OB] = 'HIGH,MEDIUM';
    var armed = String(load(newSource, p).dt10SelfTest());
    assert(armed.indexOf('owner rule bands (DT10_OWNER_RULE_BANDS): HIGH/MEDIUM') > 0 && armed.indexOf('FAIL') < 0, 'armed line');
  });

  test('O6 lint: ES5/ASCII diff, LF only, WHY block newest-first', function () {
    var baseLines = {}, bl = baseSource.split('\n'), nl = newSource.split('\n');
    for (var i = 0; i < bl.length; i++) baseLines[bl[i]] = true;
    var added = nl.filter(function (l) { return !baseLines[l]; });
    assert(added.length >= 100 && added.length <= 160, 'added lines ' + added.length);
    added.forEach(function (l) {
      assert(!/\b(let|const|class)\b|=>|`/.test(l), 'ES5 violation: ' + l);
      if (!/^[\x09\x20-\x7e]*$/.test(l)) assert(baseLines[l.replace('1.25.0', '1.24.0')] === true, 'non-ASCII added line: ' + l);
    });
    assert(newSource.indexOf('\r') < 0, 'LF only');
    var why = newSource.indexOf(' * v1.25.0 (2026-11-08) [OWNER-BANDS]'), prev = newSource.indexOf(' * v1.24.0 (2026-11-08) [SEAT-GUARD]');
    assert(why > 0 && prev > why, 'WHY block newest-first');
  });

  test('O7 mutants: four one-line mutations are each caught', function () {
    var mutants = [
      ['junk tolerated', "    if (DT10_OWNER_RULE_BAND_ORDER.indexOf(toks[i]) < 0) return null;", "    if (DT10_OWNER_RULE_BAND_ORDER.indexOf(toks[i]) < 0) continue;"],
      ['guard ignores the set', "    } else if (cfg.ownerRule === true && dt10OwnerRuleCell_(t, cfg.ownerBands) !== 'rule: yes') {", "    } else if (cfg.ownerRule === true && dt10OwnerRuleCell_(t) !== 'rule: yes') {"],
      ['no-set default widened', "  if (!bands || typeof bands !== 'object') return b === 'HIGH';", "  if (!bands || typeof bands !== 'object') return b === 'HIGH' || b === 'MEDIUM';"],
      ['set rides with the rule off', "  if (cfg.ownerRule === true) {\n    var obSet = dt10OwnerRuleBands_();", "  if (true) {\n    var obSet = dt10OwnerRuleBands_();"]
    ];
    mutants.forEach(function (mu) {
      eq(newSource.split(mu[1]).length - 1, 1, 'mutant anchor unique: ' + mu[0]);
      var src = newSource.replace(mu[1], mu[2]);
      var caught = false;
      try {
        var M = load(src, {});
        var ok1 = M.dt10OwnerRuleBandsParse_('MEDIUM,FOO') === null;
        var ok2 = heldSyms(guard(M, { earnDays: 3, ownerRule: true, ownerBands: M.dt10OwnerRuleBandsParse_('HIGH,MEDIUM') })) === 'LOW.US:rule,CAP.US:rule,ERN.US:earn';
        var ok3 = M.dt10OwnerRuleCell_({ confidence_band: 'Medium', engine_roi_pct: 20 }) === 'rule: no (Medium)';
        var p = {}; p[TG] = 'enforce'; p[OB] = 'HIGH,MEDIUM';
        var ok4 = !('ownerBands' in plain(load(src, p).dt10TicketGuardConfig_()));
        caught = !(ok1 && ok2 && ok3 && ok4);
      } catch (eM) { caught = true; }
      assert(caught, 'mutant not caught: ' + mu[0]);
    });
  });

  return sha256(JSON.stringify(digestParts));
}

var digests = [];
for (var run = 0; run < 3; run++) {
  passed = 0; failed = 0;
  if (run > 0) console.log('--- run ' + (run + 1));
  digests.push(battery());
}
var same = digests[0] === digests[1] && digests[1] === digests[2];
if (!same) { failed++; console.log('FAIL digests differ across runs: ' + digests.join(' ')); }
console.log('digest ' + digests[0].slice(0, 16) + (same ? ' x3 identical' : ''));
console.log((failed ? 'FAIL ' : 'PASS ') + passed + '/' + (passed + failed));
process.exit(failed ? 1 : 0);
