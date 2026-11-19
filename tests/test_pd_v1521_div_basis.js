#!/usr/bin/env node
/* tests/test_pd_v1521_div_basis.js
 * Harness for apps_script/10_My_Portfolio.gs v1.5.21 [DIV-BASIS] (owner, 18 Nov 2026: Portfolio_Decision printed
 * "[dd-observe] drawdown -18.1% ... would EXIT" on a holding whose price had fallen by a special dividend he had been
 * paid). The dividends a holding has received (the ledger's 'Dividends Recv', ACTIVE lots) go to the backend with its
 * row, so portfolio_actions 1.19.0 can count them in its loss rules.
 * The REAL file runs in a vm; the v1.5.20 base is rebuilt by reversing the enumerated hunks (SHA-256 pinned).
 * Every symbol and figure below is synthetic.
 *   V0  compile both; version lockstep; 8 hunks whose reversal is the v1.5.20 file; ES5 / ASCII; no property read added
 *   V1  _myPortfolioLedgerMap_: divRecv = the sum over ACTIVE lots only; a net amount of zero or less leaves no key;
 *       without the column, or without a dividend, every entry equals the base's key for key
 *   V2  pdDivRecvApply_ (pure): only a finite amount above zero on a position with a quantity
 *   V3  pdCollectHoldings_ (REAL): the rows sent equal the base's, plus 'Dividends Recv' on the holdings that have one
 *   V4  mutants
 * The last line is 'PASS k/k' (exit 1 on any failure).
 */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const ROOT = path.join(__dirname, '..');
const hist = require(path.join(__dirname, 'js', 'pd_history.js'));
// 2026-11-19: v1.5.22 [TOTAL-RETURN] is delivered; this harness proves the v1.5.21 diff, so it reads the v1.5.21 file
// rebuilt from the delivered one (tests/js/pd_history.js: the v1.5.22 hunks reversed, SHA-256 pinned).
const NEW = hist.v1521Source(fs.readFileSync(process.argv[2] || path.join(ROOT, 'apps_script', '10_My_Portfolio.gs'), 'utf8'));
const BASE = hist.v1520Source(NEW);
const copy = (v) => JSON.parse(JSON.stringify(v));

const LEDGER_HEAD = ['Symbol', 'Name', 'Ccy', 'Status', 'Buy Date', 'Buy Price', 'Shares', 'Buy Fees', 'Sell Date', 'Sell Price', 'Sell Fees', 'Dividends Recv', 'Notes'];
const lot = (sym, status, price, shares, fees, div) => [sym, 'Name ' + sym, 'USD', status, new Date(Date.UTC(2026, 9, 1)), price, shares, fees, '', '', '', div, ''];
function gridSheet(grid) {
  const width = Math.max(...grid.map((r) => r.length));
  return {
    getLastRow() { return grid.length; },
    getLastColumn() { return width; },
    getRange(r, c, nr, nc) {
      nr = nr || 1; nc = nc || 1;
      const block = grid.slice(r - 1, r - 1 + nr).map((row) => { const out = []; for (let j = 0; j < nc; j++) out.push(row[c - 1 + j] === undefined ? '' : row[c - 1 + j]); return out; });
      return { getValue() { return block[0][0]; }, getValues() { return block; } };
    }
  };
}
function ledgerGrid(rows, head) { return [['Portfolio Ledger'], ['Status:'], ['legend'], head || LEDGER_HEAD].concat(rows); }
function makeCtx(source, ledger, portfolio) {
  const sheets = { _Portfolio_CostBasis: ledger ? gridSheet(ledger) : null, My_Portfolio: portfolio ? gridSheet(portfolio) : null };
  const propReads = [];
  const ctx = {
    SHEET_NAMES: { MY_PORTFOLIO: 'My_Portfolio' }, Logger: { log() {} }, propReads,
    SpreadsheetApp: { getActive() { return { getSheetByName(n) { return sheets[n] || null; } }; } },
    PropertiesService: { getScriptProperties() { return { getProperty(k) { propReads.push(k); return null; } }; }, getDocumentProperties() { return { getProperty(k) { propReads.push(k); return null; } }; } },
    ensureSheetExists_(n) { return sheets[n]; },
    readCurrentSheetHeaders_(sh) { return sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0]; }
  };
  vm.createContext(ctx);
  vm.runInContext(source, ctx, { filename: '10_My_Portfolio.gs' });
  return ctx;
}
const MP_HEAD = ['Symbol', 'Name', 'Sector', 'Currency', 'Current Price', 'Position Qty', 'Avg Cost', 'Last Updated (UTC)'];
const MP = [MP_HEAD, ['PAID.US', 'Paid Out', 'Health', 'USD', 28.7, '', '', '2026-11-18 05:00:00'], ['PLAIN.US', 'Plain', 'Fin', 'USD', 50, '', '', '2026-11-18 05:00:00'],
  ['TWO.US', 'Two Lots', 'Fin', 'USD', 20, '', '', '2026-11-18 05:00:00'], ['LOOSE.US', 'Not in the ledger', 'Fin', 'USD', 10, 7, 9.5, '2026-11-18 05:00:00']];
const LEDGER = ledgerGrid([
  lot('PAID.US', 'Active', 35, 100, 0, 560),
  lot('PLAIN.US', 'Active', 48, 40, 2, ''),
  lot('TWO.US', 'Active', 21, 10, 0, 12.5), lot('TWO.US', 'Active', 19, 30, 0, '7.25'), lot('TWO.US', 'Sold', 25, 50, 0, 400), lot('TWO.US', 'Inactive', 25, 5, 0, 90),
  lot('GONE.US', 'Closed', 10, 10, 0, 33)]);

function battery(source) {
  const results = [];
  let passed = 0, total = 0;
  const section = (label, fn) => { total++; try { fn(); passed++; results.push('PASS ' + label); } catch (e) { results.push('FAIL ' + label + ' ... ' + String(e && e.message || e).split('\n').slice(0, 3).join(' ')); } };

  section('V0 compile; version lockstep 1.5.21; 8 hunks whose reversal is the v1.5.20 file; ES5 / ASCII; no property read added', () => {
    new vm.Script(source, { filename: 'new.gs' }); new vm.Script(BASE, { filename: 'base.gs' });
    assert.ok(/^ \* Version: 1\.5\.21$/m.test(source) && /^var PD_VERSION = '1\.5\.21';$/m.test(source), 'lockstep');
    assert.ok(/^var PD_VERSION = '1\.5\.20';$/m.test(BASE) && /^ \* Version: 1\.5\.20$/m.test(BASE), 'base is 1.5.20');
    assert.equal(hist.EDITS_V1521.length, 8);
    for (const [cur] of hist.EDITS_V1521) {
      assert.ok(!/[^\x00-\x7F]/.test(cur), 'ASCII: ' + cur.slice(0, 40));
      const code = cur.split('\n').filter((l) => !/^\s*(\/?\*|\/\/)/.test(l)).join('\n').replace(/\/\*[\s\S]*?\*\//g, '');
      assert.ok(!/\b(let|const)\s|=>|`/.test(code), 'ES5: ' + cur.slice(0, 40));
      assert.ok(!/getProperty|PropertiesService/.test(code), 'no Script Property read: the switch is the backend gate');
    }
    assert.equal(BASE.indexOf('pdDivRecvApply_'), -1); assert.equal(BASE.indexOf('divRecv'), -1);
    const h0 = source.indexOf(' * v1.5.21 (2026-11-18)'), h1 = source.indexOf(' * v1.5.20 (2026-11-16)');
    assert.ok(h0 > 0 && h1 > h0, 'WHY block newest first');
    for (const l of BASE.slice(0, BASE.indexOf('*/')).split('\n')) if (!/^ \* Version: /.test(l)) assert.ok(source.indexOf(l) >= 0, 'prior WHY line kept: ' + l.slice(0, 60));
  });

  section('V1 the ledger map: ACTIVE lots only; zero or less leaves no key; no column or no dividend = the base, key for key', () => {
    const m = copy(makeCtx(source, LEDGER)._myPortfolioLedgerMap_());
    const b = copy(makeCtx(BASE, LEDGER)._myPortfolioLedgerMap_());
    assert.equal(m['PAID.US'].divRecv, 560);
    assert.equal(m['TWO.US'].divRecv, 19.75, 'the two ACTIVE lots (a text cell is read as its number); the Sold and Inactive lots are not counted');
    assert.equal(m['TWO.US'].qty, 40); assert.equal(m['TWO.US'].lots, 2);
    assert.ok(!('divRecv' in m['PLAIN.US']) && !('divSum' in m['PLAIN.US']) && !('divSum' in m['PAID.US']), 'no key without a dividend, no working key left');
    assert.equal(m['GONE.US'], undefined, 'a closed symbol is not held');
    for (const k of Object.keys(b)) { const x = Object.assign({}, m[k]); delete x.divRecv; assert.deepEqual(x, b[k], k + ': everything else is the base entry'); }
    assert.deepEqual(Object.keys(m), Object.keys(b));
    // a net amount of zero or less (a tax row larger than the dividend, a correction) is not a dividend received
    for (const [a1, a2] of [[10, -10], [-4, ''], [0, 0], ['', ''], ['n/a', '']]) {
      const g = ledgerGrid([lot('NET.US', 'Active', 10, 5, 0, a1), lot('NET.US', 'Active', 10, 5, 0, a2)]);
      assert.deepEqual(copy(makeCtx(source, g)._myPortfolioLedgerMap_()), copy(makeCtx(BASE, g)._myPortfolioLedgerMap_()), JSON.stringify([a1, a2]));
    }
    const net = ledgerGrid([lot('NET.US', 'Active', 10, 5, 0, 81.3), lot('NET.US', 'Active', 10, 5, 0, -24.39)]);
    assert.equal(Math.round(makeCtx(source, net)._myPortfolioLedgerMap_()['NET.US'].divRecv * 100) / 100, 56.91, 'a tax row counts as its negative amount');
    // a lot without shares carries no position: its dividend is not counted
    const noQty = ledgerGrid([lot('NQ.US', 'Active', 10, '', 0, 50)]);
    assert.deepEqual(copy(makeCtx(source, noQty)._myPortfolioLedgerMap_()), copy(makeCtx(BASE, noQty)._myPortfolioLedgerMap_()));
    // a ledger without the column: the base map exactly
    const head = LEDGER_HEAD.filter((h) => h !== 'Dividends Recv');
    const bare = ledgerGrid([['PAID.US', 'n', 'USD', 'Active', new Date(Date.UTC(2026, 9, 1)), 35, 100, 0, '', '', '', '']], head);
    assert.deepEqual(copy(makeCtx(source, bare)._myPortfolioLedgerMap_()), copy(makeCtx(BASE, bare)._myPortfolioLedgerMap_()));
    assert.equal(makeCtx(source, null)._myPortfolioLedgerMap_(), null, 'no ledger: null, as before');
  });

  section('V2 pdDivRecvApply_ (pure): only a finite amount above zero on a position with a quantity', () => {
    const f = makeCtx(source, LEDGER).pdDivRecvApply_;
    let o = { Symbol: 'A' };
    assert.equal(f(o, { qty: 100, buyPrice: 35, divRecv: 560 }), true); assert.deepEqual(o, { Symbol: 'A', 'Dividends Recv': 560 });
    o = {}; assert.equal(f(o, { qty: 3, divRecv: 1.234567 }), true); assert.equal(o['Dividends Recv'], 1.2346, '4 decimals');
    for (const info of [{ qty: 100 }, { qty: 100, divRecv: 0 }, { qty: 100, divRecv: -5 }, { qty: 100, divRecv: NaN }, { qty: 100, divRecv: Infinity }, { qty: 100, divRecv: '560' },
      { qty: 0, divRecv: 5 }, { qty: null, divRecv: 5 }, { divRecv: 5 }, null]) {
      o = { Symbol: 'A' }; assert.equal(f(o, info), false, JSON.stringify(info)); assert.deepEqual(o, { Symbol: 'A' });
    }
    assert.equal(f(null, { qty: 1, divRecv: 1 }), false);
  });

  section('V3 pdCollectHoldings_ (REAL): the rows sent are the base rows, plus Dividends Recv where the ledger has one', () => {
    const n = makeCtx(source, LEDGER, MP), b = makeCtx(BASE, LEDGER, MP);
    const got = copy(n.pdCollectHoldings_(null, true)), want = copy(b.pdCollectHoldings_(null, true));
    assert.equal(got.rows.length, 4); assert.equal(got.costBasisApplied, want.costBasisApplied); assert.deepEqual(got.stamps, want.stamps);
    const bySym = (rows) => { const o = {}; rows.forEach((r) => { o[r.Symbol] = r; }); return o; };
    const g = bySym(got.rows), w = bySym(want.rows);
    assert.equal(g['PAID.US']['Dividends Recv'], 560); assert.equal(g['TWO.US']['Dividends Recv'], 19.75);
    assert.equal(g['PAID.US'].Quantity, 100); assert.equal(g['PAID.US']['Buy Price'], 35, 'the same lots give the quantity, the cost and the dividends');
    assert.ok(!('Dividends Recv' in g['PLAIN.US']) && !('Dividends Recv' in g['LOOSE.US']), 'none without a dividend, none for a holding outside the ledger');
    for (const s of Object.keys(w)) { const x = Object.assign({}, g[s]); delete x['Dividends Recv']; assert.deepEqual(x, w[s], s + ': every other field is the base row'); }
    assert.deepEqual(got.rows.map((r) => r.Symbol), want.rows.map((r) => r.Symbol), 'the same rows in the same order');
    assert.deepEqual(n.propReads, b.propReads, 'no Script Property read was added');
    // no dividend anywhere: the body rows are the base's, byte for byte
    const plain = ledgerGrid([lot('PAID.US', 'Active', 35, 100, 0, ''), lot('PLAIN.US', 'Active', 48, 40, 2, 0), lot('TWO.US', 'Active', 21, 10, 0, '')]);
    assert.equal(JSON.stringify(makeCtx(source, plain, MP).pdCollectHoldings_(null, true)), JSON.stringify(makeCtx(BASE, plain, MP).pdCollectHoldings_(null, true)));
    // no ledger at all: unchanged
    assert.equal(JSON.stringify(makeCtx(source, null, MP).pdCollectHoldings_()), JSON.stringify(makeCtx(BASE, null, MP).pdCollectHoldings_()));
  });
  return { passed, total, results };
}

const first = battery(NEW);
first.results.forEach((l) => console.log(l));
let ok = first.passed === first.total, killed = 0;
const MUTANTS = [
  ['a dividend of zero or less is sent', "if (typeof d !== 'number' || !isFinite(d) || d <= 0) { return false; }", "if (typeof d !== 'number' || !isFinite(d)) { return false; }"],
  ['a position without a quantity is sent', "if (typeof q !== 'number' || !isFinite(q) || q <= 0) { return false; }", ''],
  ['the working key is left on the map', '        delete map[k].divSum;\n', ''],
  ['a negative net amount is kept', "if (typeof ds === 'number' && isFinite(ds) && ds > 0) {", "if (typeof ds === 'number' && isFinite(ds) && ds !== 0) {"],
  ['the row never gets the field', '      pdDivRecvApply_(obj, cbInfo);   // v1.5.21 [DIV-BASIS]\n', ''],
  ['another ledger column is read', "else if (hName === 'Dividends Recv' && !col.divRecv) {", "else if (hName === 'Notes' && !col.divRecv) {"],
  ['the amount is sent unrounded under another name', "obj['Dividends Recv'] = Math.round(d * 10000) / 10000;", "obj['Dividends'] = d;"]
];
for (const [label, from, to] of MUTANTS) {
  assert.equal(NEW.split(from).length - 1, 1, 'mutant anchor: ' + label);
  let res;
  try { res = battery(NEW.replace(from, () => to)); } catch (e) { res = { passed: 0, total: 1 }; }
  const dead = res.passed < res.total;
  if (dead) killed++; else { ok = false; console.log('SURVIVED mutant: ' + label); }
}
console.log('V4 mutants killed ' + killed + '/' + MUTANTS.length);
const k = first.passed + (killed === MUTANTS.length ? 1 : 0), t = first.total + 1;
console.log((ok && killed === MUTANTS.length ? 'PASS ' : 'FAIL ') + k + '/' + t);
process.exit(ok && killed === MUTANTS.length ? 0 : 1);
