#!/usr/bin/env node
/* tests/test_pd_v1522_total_return.js
 * Harness for apps_script/10_My_Portfolio.gs v1.5.22 [TOTAL-RETURN] (owner, 19 Nov 2026: a holding paid a special
 * dividend still showed its price-only loss on Portfolio_Decision). The P&L SAR / P&L % cells and the two P&L tiles
 * show portfolio_actions 1.20.0's total (price change plus dividends received) when the backend sends it; the row's
 * Advisor Note names the dividends and the price-only figure.
 * The REAL file runs in a vm; the v1.5.21 base is rebuilt by reversing the enumerated hunks (SHA-256 pinned).
 * Every symbol and figure below is synthetic.
 *   T0  compile both; version lockstep; 6 hunks whose reversal is the v1.5.21 file; ES5 / ASCII; no property read
 *   T1  pdTotalOr_ / pdTotalNote_ (pure)
 *   T2  pdActionToRow_ (REAL): a row with a total shows it in P&L SAR / P&L % and the note; a row without one, and
 *       every reply of an older backend, renders exactly as the base
 *   T3  pdKpiValues_ (REAL): the two P&L tiles take the totals when sent; otherwise the base values
 *   T4  the column names are unchanged (every reader of the page by header keeps working)
 *   T5  mutants
 * The last line is 'PASS k/k' (exit 1 on any failure).
 */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const ROOT = path.join(__dirname, '..');
const hist = require(path.join(__dirname, 'js', 'pd_history.js'));
const NEW = fs.readFileSync(process.argv[2] || path.join(ROOT, 'apps_script', '10_My_Portfolio.gs'), 'utf8');
const BASE = hist.v1521Source(NEW);
const copy = (v) => JSON.parse(JSON.stringify(v));

function makeCtx(source) {
  const propReads = [];
  const ctx = {
    SHEET_NAMES: { MY_PORTFOLIO: 'My_Portfolio' }, Logger: { log() {} }, propReads,
    SpreadsheetApp: { getActive() { return { getSheetByName() { return null; } }; } },
    PropertiesService: { getScriptProperties() { return { getProperty(k) { propReads.push(k); return null; } }; }, getDocumentProperties() { return { getProperty(k) { propReads.push(k); return null; } }; } }
  };
  vm.createContext(ctx);
  vm.runInContext(source, ctx, { filename: '10_My_Portfolio.gs' });
  return ctx;
}

// SAR figures of a holding bought for 13,125 SAR, now worth 10,763 SAR, paid 2,100 SAR of dividends after tax
const PRICE_ONLY = { action: 'HOLD', symbol: 'PAID.US', name: 'Paid Out', sector: 'Health', currency: 'USD', quantity: 100,
  avg_cost: 35, price: 28.7, price_sar: 107.63, market_value_sar: 10763, cost_sar: 13125, pnl_sar: -2362, pnl_pct: -18,
  weight_pct: 12.1, suggested_delta_sar: 0, suggested_delta_shares: 0, funds_from: null, stop_sar: 99, tp1_sar: 125, tp2_sar: 140,
  roi_pct: 8, engine_roi_pct: null, reliability: 70, dq: 90, confidence_band: 'MEDIUM', advisor_note: 'HOLD; within all caps' };
const WITH_TOTAL = Object.assign({}, PRICE_ONLY, { dividends_sar: 2100, total_pnl_sar: -262, total_pnl_pct: -2 });
const PLAIN = Object.assign({}, PRICE_ONLY, { symbol: 'PLAIN.US', pnl_sar: 300, pnl_pct: 4.2, advisor_note: 'HOLD; fine' });
const KPI = { portfolio_value_sar: 90000, holdings_value_sar: 80000, cash_sar: 10000, cash_pct: 11.1, pnl_sar: -2062, pnl_pct: -10.1,
  deployable_sar: 0, adds_funded_sar: 0, capital_unallocated_sar: 0, action_counts: { ADD: 0, HOLD: 2, TRIM: 0, EXIT: 0, BLOCK: 0 } };

function battery(source) {
  const results = [];
  let passed = 0, total = 0;
  const section = (label, fn) => { total++; try { fn(); passed++; results.push('PASS ' + label); } catch (e) { results.push('FAIL ' + label + ' ... ' + String(e && e.message || e).split('\n').slice(0, 3).join(' ')); } };

  section('T0 compile; version lockstep 1.5.22; 6 hunks whose reversal is the v1.5.21 file; ES5 / ASCII; no property read', () => {
    new vm.Script(source, { filename: 'new.gs' }); new vm.Script(BASE, { filename: 'base.gs' });
    assert.ok(/^ \* Version: 1\.5\.22$/m.test(source) && /^var PD_VERSION = '1\.5\.22';$/m.test(source), 'lockstep');
    assert.ok(/^var PD_VERSION = '1\.5\.21';$/m.test(BASE) && /^ \* Version: 1\.5\.21$/m.test(BASE), 'base is 1.5.21');
    assert.equal(hist.EDITS_V1522.length, 6);
    for (const [cur] of hist.EDITS_V1522) {
      assert.ok(!/[^\x00-\x7F]/.test(cur), 'ASCII: ' + cur.slice(0, 40));
      const code = cur.split('\n').filter((l) => !/^\s*(\/?\*|\/\/)/.test(l)).join('\n').replace(/\/\*[\s\S]*?\*\//g, '');
      assert.ok(!/\b(let|const)\s|=>|`/.test(code), 'ES5: ' + cur.slice(0, 40));
      assert.ok(!/getProperty|PropertiesService/.test(code), 'no Script Property read');
    }
    assert.equal(BASE.indexOf('pdTotalOr_'), -1);
    const h0 = source.indexOf(' * v1.5.22 (2026-11-19)'), h1 = source.indexOf(' * v1.5.21 (2026-11-18)');
    assert.ok(h0 > 0 && h1 > h0, 'WHY block newest first');
    for (const l of BASE.slice(0, BASE.indexOf('*/')).split('\n')) if (!/^ \* Version: /.test(l)) assert.ok(source.indexOf(l) >= 0, 'prior WHY line kept: ' + l.slice(0, 60));
  });

  section('T1 pdTotalOr_ / pdTotalNote_ (pure)', () => {
    const c = makeCtx(source);
    assert.equal(c.pdTotalOr_(-2, -18), -2); assert.equal(c.pdTotalOr_(0, -18), 0, 'a total of zero is a total');
    assert.equal(c.pdTotalOr_('-2.5', -18), -2.5, 'a numeric text is read as its number');
    for (const v of [null, undefined, '', 'n/a', NaN, Infinity, true, false, {}]) assert.equal(c.pdTotalOr_(v, -18), -18, JSON.stringify(v));
    assert.equal(c.pdTotalOr_(null, null), null);
    assert.equal(c.pdTotalNote_(WITH_TOTAL), 'HOLD; within all caps | P&L counts dividends received 2100 SAR (price only -18%)');
    assert.equal(c.pdTotalNote_(Object.assign({}, WITH_TOTAL, { advisor_note: '' })), 'P&L counts dividends received 2100 SAR (price only -18%)');
    assert.equal(c.pdTotalNote_(Object.assign({}, WITH_TOTAL, { dividends_sar: null, pnl_pct: null })), 'HOLD; within all caps | P&L counts dividends received');
    assert.equal(c.pdTotalNote_(PRICE_ONLY), 'HOLD; within all caps', 'no total: the note as it was');
    assert.equal(c.pdTotalNote_(Object.assign({}, PRICE_ONLY, { advisor_note: null })), '');
    assert.equal(c.pdTotalNote_(null), '');
  });

  section('T2 pdActionToRow_ (REAL): the total in P&L SAR / P&L % and the note; without one, the base row exactly', () => {
    const n = makeCtx(source), b = makeCtx(BASE);
    const r = copy(n.pdActionToRow_(WITH_TOTAL)), rb = copy(b.pdActionToRow_(WITH_TOTAL));
    const H = copy(n.PD_ACTION_HEADERS), iS = H.indexOf('P&L SAR'), iP = H.indexOf('P&L %'), iN = H.indexOf('Advisor Note');
    assert.ok(iS > 0 && iP === iS + 1 && iN === H.length - 1);
    assert.equal(r[iS], -262); assert.equal(r[iP], -2);
    assert.equal(rb[iS], -2362); assert.equal(rb[iP], -18, 'the base showed the price-only loss');
    assert.equal(r[iN], 'HOLD; within all caps | P&L counts dividends received 2100 SAR (price only -18%)');
    for (let i = 0; i < r.length; i++) if (i !== iS && i !== iP && i !== iN) assert.deepEqual(r[i], rb[i], 'cell ' + H[i] + ' unchanged');
    assert.equal(r.length, rb.length);
    for (const a of [PRICE_ONLY, PLAIN, Object.assign({}, PRICE_ONLY, { total_pnl_pct: 'n/a', total_pnl_sar: '' }), { action: 'BLOCK', symbol: 'X' }]) {
      assert.deepEqual(copy(n.pdActionToRow_(a)), copy(b.pdActionToRow_(a)), 'no total: the base row ' + a.symbol);
    }
  });

  section('T3 pdKpiValues_ (REAL): the two P&L tiles take the totals when sent', () => {
    const n = makeCtx(source), b = makeCtx(BASE);
    const L = copy(n.PD_KPI_LABELS), iS = L.indexOf('P&L (SAR)'), iP = L.indexOf('P&L %');
    const k = Object.assign({}, KPI, { total_pnl_sar: 38, total_pnl_pct: 0.2 });
    const v = copy(n.pdKpiValues_(k)), vb = copy(b.pdKpiValues_(k));
    assert.equal(v[iS], 38); assert.equal(v[iP], 0.2); assert.equal(vb[iS], -2062); assert.equal(vb[iP], -10.1);
    for (let i = 0; i < v.length; i++) if (i !== iS && i !== iP) assert.deepEqual(v[i], vb[i], L[i]);
    for (const kk of [KPI, {}, null, Object.assign({}, KPI, { total_pnl_sar: null, total_pnl_pct: 'x' })]) {
      assert.deepEqual(copy(n.pdKpiValues_(kk)), copy(b.pdKpiValues_(kk)), 'no totals: the base tiles');
    }
  });

  section('T4 the column and tile names are unchanged', () => {
    const n = makeCtx(source), b = makeCtx(BASE);
    assert.deepEqual(copy(n.PD_ACTION_HEADERS), copy(b.PD_ACTION_HEADERS));
    assert.deepEqual(copy(n.PD_KPI_LABELS), copy(b.PD_KPI_LABELS));
    assert.deepEqual(n.propReads, b.propReads);
  });
  return { passed, total, results };
}

const first = battery(NEW);
first.results.forEach((l) => console.log(l));
let ok = first.passed === first.total, killed = 0;
const MUTANTS = [
  ['the P&L SAR cell ignores the total', "pdCell_(pdTotalOr_(a.total_pnl_sar, a.pnl_sar)),", "pdCell_(a.pnl_sar),"],
  ['the P&L % cell ignores the total', "pdCell_(pdTotalOr_(a.total_pnl_pct, a.pnl_pct)), pdCell_(a.weight_pct),", "pdCell_(a.pnl_pct), pdCell_(a.weight_pct),"],
  ['the note does not say what the column counts', "          pdCell_(pdTotalNote_(a))];", "          pdCell_(a.advisor_note)];"],
  ['the P&L tile ignores the total', "pdCell_(pdTotalOr_(k.total_pnl_sar, k.pnl_sar)),", "pdCell_(k.pnl_sar),"],
  ['the P&L % tile ignores the total', "pdCell_(pdTotalOr_(k.total_pnl_pct, k.pnl_pct)), pdCell_(k.deployable_sar),", "pdCell_(k.pnl_pct), pdCell_(k.deployable_sar),"],
  ['a text total is taken as text', "  var n = Number(total);\n  return isFinite(n) ? n : price;", "  return total;"],
  ['the price-only figure is not named', "      (price !== null ? ' (price only ' + price + '%)' : '');", "      '';"],
  ['a note is written on a row without a total', "  if (!a || pdTotalOr_(a.total_pnl_pct, null) === null) return note;", "  if (!a) return note;"]
];
for (const [label, from, to] of MUTANTS) {
  assert.equal(NEW.split(from).length - 1, 1, 'mutant anchor: ' + label);
  let res;
  try { res = battery(NEW.replace(from, () => to)); } catch (e) { res = { passed: 0, total: 1 }; }
  const dead = res.passed < res.total;
  if (dead) killed++; else { ok = false; console.log('SURVIVED mutant: ' + label); }
}
console.log('T5 mutants killed ' + killed + '/' + MUTANTS.length);
const k = first.passed + (killed === MUTANTS.length ? 1 : 0), t = first.total + 1;
console.log((ok && k === t ? 'PASS ' : 'FAIL ') + k + '/' + t);
process.exit(ok && k === t ? 0 : 1);
