#!/usr/bin/env node
/* tests/test_dt10_funding_evidence.js
 * Harness for apps_script/16_Decision_Top10.gs v1.16.0 [FUNDING-EVIDENCE].
 * Extracts the REAL functions from the delivered file (never a copy) and runs
 * them in node with PropertiesService / Logger stubs. Every test calls the
 * real code. Sections:
 *   F1 mode reader            F2 integer ports (units, div half-even)
 *   F3 FX proofs              F4 certified cash
 *   F5 links                  F6 value_sar
 *   F7 core apply (live-shaped packet; untouched-body invariants)
 *   F8 note                   F9 kill identity (property absent)
 *   F10 ES5/ASCII lint of the added block   F11 golden negative (base file)
 * Run: node tests/test_dt10_funding_evidence.js   (exit 0 = all PASS)
 *      DT10_BASE=<path to the v1.15.0 file> adds the golden negative.
 */
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const SRC = process.argv[2] || path.join(__dirname, '..', 'apps_script', '16_Decision_Top10.gs');
const src = fs.readFileSync(SRC, 'utf8');

function extract(source, name) {
  const i = source.indexOf('function ' + name + '(');
  if (i < 0) throw new Error('missing ' + name);
  let d = 0;
  const j = source.indexOf('{', i);
  for (let k = j; k < source.length; k++) {
    if (source[k] === '{') d++;
    else if (source[k] === '}') { d--; if (!d) return source.slice(i, k + 1); }
  }
  throw new Error('unbalanced ' + name);
}
function extractVar(source, name) {
  const m = source.match(new RegExp('^var ' + name + ' = [^;]+;', 'm'));
  if (!m) throw new Error('missing var ' + name);
  return m[0];
}

let PROPS = {};
let LOGS = [];
global.PropertiesService = {
  getScriptProperties: () => ({ getProperty: (k) => (Object.prototype.hasOwnProperty.call(PROPS, k) ? PROPS[k] : null) }),
  getDocumentProperties: () => ({ getProperty: (k) => (Object.prototype.hasOwnProperty.call(PROPS, 'doc:' + k) ? PROPS['doc:' + k] : null) })
};
global.Logger = { log: (m) => LOGS.push(String(m)) };

const VARS = ['DT10_VERSION', 'DT10_FUNDING_PROP', 'DT10_FUNDING_EXCLUDE_PROP', 'DT10_FUNDING_MAX_AGE_MS',
  'DT10_FUNDING_MAX_ITEMS', 'DT10_FUNDING_SAFE'];
const FUNCS = ['dt10SourceStampMs_', 'dt10ReconciliationEvidence_', 'dt10FundingMode_', 'dt10FundingExcludeList_',
  'dt10FundingSym_', 'dt10FundingUnits_', 'dt10FundingDiv_', 'dt10FundingAge_', 'dt10FundingNative_',
  'dt10FundingRates_', 'dt10FundingCashCents_', 'dt10FundingLinks_', 'dt10FundingDecimal_', 'dt10FundingValueSar_',
  'dt10FundingApplyCore_', 'dt10FundingNote_', 'dt10FundingApply_'];
const geval = eval;   // indirect eval: global scope, so the real functions are callable here
for (const v of VARS) geval(extractVar(src, v));
for (const f of FUNCS) geval(extract(src, f));

let pass = 0;
function ok(cond, label) {
  if (!cond) { console.error('FAIL ' + label); process.exit(1); }
  pass++;
}
function deep(a, b) { return JSON.stringify(a) === JSON.stringify(b); }

// ---------------------------------------------------------------- F1 mode
PROPS = {}; ok(dt10FundingMode_() === 'off', 'F1 default off');
PROPS = { DT10_FUNDING_EVIDENCE: 'junk' }; ok(dt10FundingMode_() === 'off', 'F1 junk off');
PROPS = { DT10_FUNDING_EVIDENCE: ' ON ' }; ok(dt10FundingMode_() === 'on', 'F1 on (trim, case)');
PROPS = { DT10_FUNDING_EVIDENCE: 'observe' }; ok(dt10FundingMode_() === 'off', 'F1 observe is not a mode');
ok(deep(dt10FundingExcludeList_(' 5023.sr, abc; 5023.SR '), ['5023.SR', 'ABC']), 'F1 exclude list');
ok(deep(dt10FundingExcludeList_(null), []), 'F1 exclude null');
console.log('F1 PASS mode reader + exclude list');

// ------------------------------------------------------------- F2 integer ports
ok(dt10FundingUnits_('22085.12', 6) === 22085120000, 'F2 units text');
ok(dt10FundingUnits_(2644.8, 6) === 2644800000, 'F2 units number');
ok(dt10FundingUnits_('3.75', 2) === 375, 'F2 units scale 2');
ok(dt10FundingUnits_('3.7448', 2) === null, 'F2 finer than scale');
ok(dt10FundingUnits_('3.7500', 2) === 375, 'F2 trailing zeros ok');
ok(dt10FundingUnits_('abc', 2) === null && dt10FundingUnits_(true, 2) === null && dt10FundingUnits_(null, 2) === null, 'F2 junk');
ok(dt10FundingUnits_('-0', 2) === 0 && dt10FundingUnits_('-1.5', 2) === -150, 'F2 sign');
ok(dt10FundingDiv_(25, 10, 'even') === 2 && dt10FundingDiv_(35, 10, 'even') === 4, 'F2 half even');
ok(dt10FundingDiv_(26, 10, 'even') === 3 && dt10FundingDiv_(24, 10, 'even') === 2, 'F2 nearest');
ok(dt10FundingDiv_(21, 10, 'up') === 3 && dt10FundingDiv_(20, 10, 'up') === 2, 'F2 ceiling');
ok(dt10FundingAge_(0) === '0m' && dt10FundingAge_(4 * 60000) === '4m' && dt10FundingAge_(125 * 60000) === '2h5m', 'F2 age');
console.log('F2 PASS integer ports');

// ------------------------------------------------------------------ F3 FX
const ISO = '2026-10-26T05:40:00Z';
function packet(over) {
  const p = {
    schema_version: 1, source_ref: 'ibkr-capture:TFB_IBKR_Capture_20261026-084107.json', captured_at: ISO,
    accounts: [{ account_id: 'ibkr-primary', positions_asof: ISO, positions_complete: true, positions: [],
                 cash_asof: ISO, cash_complete: true,
                 cash: [{ currency: 'USD', settled_cash: '22085.12', reserved_cash: '2644.8', reservations_complete: true }] }],
    holding_links: [], funding_accounts: ['ibkr-primary'],
    fx_rates: [{ currency: 'USD', rate_to_sar: '3.75', asof: ISO, source_ref: 'policy://sama-usd-peg-3.75' }]
  };
  return Object.assign(p, over || {});
}
ok(deep(dt10FundingRates_(packet()), { SAR: 100, USD: 375 }), 'F3 USD proof');
ok(dt10FundingRates_(packet({ fx_rates: [{ currency: 'USD', rate_to_sar: '3.7448' }] })) === null, 'F3 finer proof rejected');
ok(dt10FundingRates_(packet({ fx_rates: [{ currency: 'USD', rate_to_sar: '3.75' }, { currency: 'usd', rate_to_sar: '3.75' }] })) === null, 'F3 duplicate proof');
ok(dt10FundingRates_(packet({ fx_rates: [{ currency: 'SAR', rate_to_sar: '1' }] })) === null, 'F3 SAR proof is a duplicate of the fixed 100');
ok(deep(dt10FundingRates_(packet({ fx_rates: [] })), { SAR: 100 }), 'F3 no proofs');
ok(dt10FundingRates_(packet({ fx_rates: [{ currency: 'US', rate_to_sar: '3.75' }] })) === null, 'F3 bad currency');
console.log('F3 PASS fx proofs');

// ---------------------------------------------------------------- F4 cash
const R = { SAR: 100, USD: 375 };
ok(dt10FundingCashCents_(packet(), R).cents === 7290120, 'F4 certified cash 72901.20');
ok(dt10FundingCashCents_(packet({ accounts: [Object.assign({}, packet().accounts[0], { cash: [{ currency: 'USD', settled_cash: '22085.12', reserved_cash: '0', reservations_complete: true }] })] }), R).cents === 8281920, 'F4 no reservation 82819.20');
ok(/over-reserved/.test(dt10FundingCashCents_(packet({ accounts: [Object.assign({}, packet().accounts[0], { cash: [{ currency: 'USD', settled_cash: '1', reserved_cash: '2', reservations_complete: true }] })] }), R).reason), 'F4 over-reserved');
ok(/reservations incomplete/.test(dt10FundingCashCents_(packet({ accounts: [Object.assign({}, packet().accounts[0], { cash: [{ currency: 'USD', settled_cash: '1', reserved_cash: '0', reservations_complete: false }] })] }), R).reason), 'F4 incomplete');
ok(/without FX proof/.test(dt10FundingCashCents_(packet({ accounts: [Object.assign({}, packet().accounts[0], { cash: [{ currency: 'EUR', settled_cash: '1', reserved_cash: '0', reservations_complete: true }] })] }), R).reason), 'F4 no proof');
ok(/unknown/.test(dt10FundingCashCents_(packet({ funding_accounts: ['other'] }), R).reason), 'F4 unknown account');
ok(/missing/.test(dt10FundingCashCents_(packet({ funding_accounts: [] }), R).reason), 'F4 no funding accounts');
ok(/unreadable/.test(dt10FundingCashCents_(packet({ accounts: [Object.assign({}, packet().accounts[0], { cash: [{ currency: 'USD', settled_cash: 'x', reserved_cash: '0', reservations_complete: true }] })] }), R).reason), 'F4 unreadable');
// half-even at the cent: 0.000005 USD x 3.75 = 0.00001875 SAR -> 0 cents; 1.234 USD x 3.75 = 4.6275 -> 4.63 (odd->up), 1.2347 x 3.75 = 4.630125 -> 4.63
const c1 = dt10FundingCashCents_(packet({ accounts: [Object.assign({}, packet().accounts[0], { cash: [{ currency: 'USD', settled_cash: '1.234', reserved_cash: '0', reservations_complete: true }] })] }), R).cents;
ok(c1 === 463, 'F4 half-even cent (4.6275 -> 4.63)');
const c2 = dt10FundingCashCents_(packet({ accounts: [Object.assign({}, packet().accounts[0], { cash: [{ currency: 'USD', settled_cash: '1.23', reserved_cash: '0', reservations_complete: true }] })] }), R).cents;
ok(c2 === 461, 'F4 1.23 x 3.75 = 4.6125 -> 4.61 (half even, 1 is odd? 461.25 -> 461)');
console.log('F4 PASS certified cash');

// --------------------------------------------------------------- F5 links
const pos = (id, sym, ccy, qty) => ({ instrument_id: id, symbol: sym, currency: ccy, quantity: qty });
const pk = packet({ accounts: [Object.assign({}, packet().accounts[0], { positions: [pos('479798353', 'FRST', 'USD', '174'), pos('1', 'BRK B', 'USD', '2'), pos('2', 'SOLD', 'USD', '0')] })] });
const L = dt10FundingLinks_([{ symbol: 'FRST.US', currency: 'USD' }, { symbol: 'BRK-B.US', currency: 'USD' }, { symbol: '5023.SR', currency: 'SAR' }], pk);
ok(L.links.length === 2 && L.links[0].row_index === 0 && L.links[0].instrument_id === '479798353' && L.links[0].position_symbol === 'FRST' && L.links[0].symbol === 'FRST.US' && L.links[0].currency === 'USD', 'F5 FRST link');
ok(L.links[1].row_index === 1 && L.links[1].position_symbol === 'BRK B', 'F5 BRK B -> BRK-B.US');
ok(deep(L.unlinked, ['5023.SR']) && deep(L.unrepresented, []), 'F5 unlinked / zero position not unrepresented');
const L2 = dt10FundingLinks_([], pk);
ok(deep(L2.unrepresented, ['FRST', 'BRK B']) && L2.links.length === 0, 'F5 unrepresented positives');
const dup = packet({ accounts: [Object.assign({}, packet().accounts[0], { positions: [pos('1', 'FRST', 'USD', '1'), pos('2', 'FRST', 'USD', '1')] })] });
ok(deep(dt10FundingLinks_([{ symbol: 'FRST.US', currency: 'USD' }], dup).unlinked, ['FRST.US']), 'F5 ambiguous -> unlinked');
ok(dt10FundingNative_('FRST', 'usd') === 'FRST.US' && dt10FundingNative_('FRST', 'EUR') === '' && dt10FundingNative_('bad symbol!', 'USD') === '', 'F5 native rule');
console.log('F5 PASS links');

// ------------------------------------------------------------ F6 value_sar
ok(dt10FundingValueSar_(15.2, 174, 375) === 9918, 'F6 15.20 x 174 x 3.75 = 9918.00');
ok(dt10FundingValueSar_(15.28, 170, 375) === 9741, 'F6 15.28 x 170 x 3.75 = 9741.00');
ok(dt10FundingValueSar_(1.111, 1, 100) === 1.11 && dt10FundingValueSar_(1.115, 1, 100) === 1.12 && dt10FundingValueSar_(1.125, 1, 100) === 1.12, 'F6 half-even cents');
ok(deep(dt10FundingDecimal_(15.2), { units: 152, scale: 1 }) && deep(dt10FundingDecimal_('174'), { units: 174, scale: 0 }) && deep(dt10FundingDecimal_('0.000001'), { units: 1, scale: 6 }) && dt10FundingDecimal_(-1) === null, 'F6 minimal scale');
ok(dt10FundingValueSar_(1234.5678, 9999999.999999, 375) === null, 'F6 beyond safe range -> null');
ok(dt10FundingValueSar_('x', 1, 375) === null && dt10FundingValueSar_(1, -1, 375) === null && dt10FundingValueSar_(1, 1, 0) === null, 'F6 junk');
console.log('F6 PASS value_sar');

// ------------------------------------------------------------- F7 core apply
function body() {
  return { criteria: { x: 1 }, fx_rates: { USD: 3.7448, USX: 0.037448, GBX: 0.0498 },
    portfolio: { cash_available_sar: 82819.2, pending_proceeds_sar: 0, holdings_input_incomplete: false,
      holdings: [{ symbol: '5023.SR', sector: 'Financials', value_sar: 9390, currency: 'SAR' }] }, pool_limit: 0 };
}
const NOW = Date.parse(ISO) + 5 * 60000;
let b = body();
const p0 = packet();
const p0json = JSON.stringify(p0);
let res = dt10FundingApplyCore_(b, p0, ['5023.SR'], NOW);
ok(res.applied === true && res.cashCents === 7290120 && res.ageMs === 300000, 'F7 applied');
ok(b.portfolio.cash_available_sar === 72901.2 && b.portfolio.pending_proceeds_sar === 0, 'F7 certified cash sent');
ok(b.fx_rates.USD === 3.75 && b.fx_rates.USX === 0.037448 && b.fx_rates.GBX === 0.0498, 'F7 fx USD peg, others kept');
ok(b.portfolio.holdings.length === 0 && deep(res.excluded, ['5023.SR']), 'F7 sukuk excluded');
ok(b.portfolio.reconciliation_evidence.schema_version === 1 && deep(b.portfolio.reconciliation_evidence.holding_links, []), 'F7 evidence forwarded');
ok(b.portfolio.reconciliation_evidence !== p0 && JSON.stringify(p0) === p0json, 'F7 packet not mutated (copy)');
ok(b.criteria.x === 1 && b.pool_limit === 0, 'F7 other body keys untouched');
// tomorrow's shape: FRST held
const bh = body();
bh.portfolio.holdings = [
  { symbol: 'FRST.US', sector: 'Financials', value_sar: 9904.9, currency: 'USD', quantity: 174, current_price: 15.2 },
  { symbol: '5023.SR', sector: 'Financials', value_sar: 9390, currency: 'SAR' }];
const ph = packet({ accounts: [Object.assign({}, packet().accounts[0], { positions: [pos('479798353', 'FRST', 'USD', '174')] })] });
res = dt10FundingApplyCore_(bh, ph, ['5023.SR'], NOW);
ok(res.applied && res.linked === 1 && res.rows === 1 && deep(res.unlinked, []) && deep(res.unrepresented, []), 'F7 FRST linked');
ok(bh.portfolio.holdings[0].value_sar === 9918 && bh.portfolio.holdings[0].symbol === 'FRST.US', 'F7 value_sar at the peg');
ok(bh.portfolio.reconciliation_evidence.holding_links[0].row_index === 0 && bh.portfolio.reconciliation_evidence.holding_links[0].instrument_id === '479798353', 'F7 link row_index is the kept index');
// untouched-body invariants
function untouched(label, pkt, bodyOver, exclude, now) {
  const bb = body(); Object.assign(bb.portfolio, bodyOver || {});
  const before = JSON.stringify(bb);
  const r = dt10FundingApplyCore_(bb, pkt, exclude || [], now === undefined ? NOW : now);
  ok(r.applied === false && JSON.stringify(bb) === before, 'F7 untouched: ' + label + ' (' + r.reason + ')');
  return r.reason;
}
ok(/too old/.test(untouched('stale 841 s', packet(), null, [], Date.parse(ISO) + 841000)), 'F7 stale reason');
ok(dt10FundingApplyCore_(body(), packet(), [], Date.parse(ISO) + 840000).applied === true, 'F7 840 s still fresh');
ok(/ahead/.test(untouched('future', packet(), null, [], Date.parse(ISO) - 1000)), 'F7 future reason');
ok(/unreadable/.test(untouched('naive captured_at', packet({ captured_at: '2026-10-26 08:40:00' }))), 'F7 naive stamp rejected');
ok(/no evidence/.test(untouched('schema 2', packet({ schema_version: 2 }))), 'F7 schema');
ok(/no evidence/.test(untouched('null packet', null)), 'F7 null');
ok(/not sent/.test(untouched('holdings absent', packet(), { holdings: undefined })), 'F7 holdings absent');
ok(/incomplete/.test(untouched('holdings incomplete', packet(), { holdings_input_incomplete: true })), 'F7 incomplete');
ok(/fx proofs/.test(untouched('bad proof', packet({ fx_rates: [{ currency: 'USD', rate_to_sar: '3.7448' }] }))), 'F7 proof');
ok(/over-reserved/.test(untouched('over-reserved', packet({ accounts: [Object.assign({}, packet().accounts[0], { cash: [{ currency: 'USD', settled_cash: '1', reserved_cash: '2', reservations_complete: true }] })] }))), 'F7 cash');
ok(/request body/.test(dt10FundingApplyCore_(null, packet(), [], NOW).reason), 'F7 no body');
console.log('F7 PASS core apply');

// ------------------------------------------------------------------ F8 note
const n1 = dt10FundingNote_({ applied: true, ageMs: 240000, cashCents: 7290120, rows: 1, linked: 1, unlinked: [], unrepresented: [], excluded: ['5023.SR'], rates: { SAR: 100, USD: 375 } });
ok(n1 === ' | funding evidence APPLIED (capture age 4m; cash 72901.20 SAR; linked 1/1; excluded 5023.SR; USD 3.75)', 'F8 applied note: ' + n1);
ok(dt10FundingNote_({ applied: false, reason: 'capture too old (20m)' }) === ' | funding evidence NOT APPLIED (capture too old (20m))', 'F8 not applied note');
ok(dt10FundingNote_(null) === '', 'F8 null');
ok(dt10FundingNote_({ applied: true, ageMs: 0, cashCents: 5, rows: 2, linked: 1, unlinked: ['X.US'], unrepresented: ['Y'], excluded: [], rates: { SAR: 100 } }).indexOf('cash 0.05 SAR; linked 1/2; unlinked X.US; unrepresented Y)') > 0, 'F8 cents and lists');
console.log('F8 PASS note');

// --------------------------------------------------------- F9 kill identity
PROPS = { 'doc:TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1': JSON.stringify(packet()), PD_EXECUTION_EXCLUDE: '5023.SR' };
let kb = body(); let kbefore = JSON.stringify(kb);
ok(dt10FundingApply_(kb, NOW) === '' && JSON.stringify(kb) === kbefore, 'F9 property absent: no note, body identical');
PROPS.DT10_FUNDING_EVIDENCE = 'on';
kb = body(); const note = dt10FundingApply_(kb, NOW);
ok(note.indexOf(' | funding evidence APPLIED (capture age 5m; cash 72901.20 SAR; linked 0/0; excluded 5023.SR; USD 3.75)') === 0 && kb.portfolio.cash_available_sar === 72901.2, 'F9 on: applied through the orchestrator: ' + note);
ok(LOGS.some((l) => /funding evidence applied/.test(l)) && !LOGS.some((l) => /settled_cash|22085/.test(l)), 'F9 log line, never the packet');
delete PROPS['doc:TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1'];
kb = body(); kbefore = JSON.stringify(kb);
ok(dt10FundingApply_(kb, NOW) === ' | funding evidence NOT APPLIED (no evidence packet)' && JSON.stringify(kb) === kbefore, 'F9 on without a packet');
console.log('F9 PASS kill identity + orchestrator');

// ------------------------------------------------------------- F10 lint
const i0 = src.indexOf('// v1.16.0 [FUNDING-EVIDENCE] -- apply');
const i1 = src.indexOf('// v1.3.0', i0);
const block = src.slice(i0, i1);
ok(i0 > 0 && i1 > i0, 'F10 block located');
ok(!/\b(let|const)\s|=>|`|\.\.\.|class\s/.test(block), 'F10 ES5 only');
ok(![...block].some((ch) => ch.charCodeAt(0) > 126), 'F10 ASCII only');
for (const f of FUNCS.slice(2)) ok(block.indexOf('function ' + f + '(') > 0, 'F10 inventory ' + f);
// 2026-10-27: v1.17.0 (shadow rank log) stacked on this change; pins moved
// 2026-10-27: v1.17.1 (shadow momentum) stacked as well; pins moved
ok((src.match(/var DT10_VERSION = '1\.17\.1';/) || []).length === 1 && /\* Version: 1\.17\.1 \(see DT10_VERSION;/.test(src), 'F10 version lockstep');
ok(src.indexOf("var fundingNote = dt10FundingApply_(body, Date.now());") > 0 && src.indexOf("cashNote + fundingNote + ' | '") > 0, 'F10 wired into the request + status line');
console.log('F10 PASS lint + inventory');

// -------------------------------------------------------- F11 golden negative
if (process.env.DT10_BASE) {
  const base = fs.readFileSync(process.env.DT10_BASE, 'utf8');
  const sha = crypto.createHash('sha256').update(base).digest('hex');
  let missing = 0;
  for (const f of FUNCS.slice(2)) { try { extract(base, f); } catch (e) { missing++; } }
  ok(missing === FUNCS.length - 2 && /var DT10_VERSION = '1\.15\.0';/.test(base) && base.indexOf('fundingNote') < 0, 'F11 base lacks every added function (sha256 ' + sha.slice(0, 12) + ')');
  console.log('F11 PASS golden negative on the v1.15.0 base ' + sha.slice(0, 12));
} else {
  console.log('F11 SKIP (set DT10_BASE=<v1.15.0 file> to run the golden negative)');
}
const digest = crypto.createHash('sha256').update(src).digest('hex').slice(0, 12);
console.log('ALL PASS (' + pass + ' checks) on ' + path.basename(SRC) + ' sha256 ' + digest);
