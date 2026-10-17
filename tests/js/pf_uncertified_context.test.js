/* 10_My_Portfolio.gs v1.5.14 (2026-10-17) -- C1 / C3 cockpit half: stale-capture
 * context + ledger cash snapshot for the backend's uncertified view (Script
 * Property PD_UNCERTIFIED_CONTEXT = on; default off = v1.5.13 request, render
 * and line byte-identical -- proven by the off-mode golden in
 * pf_ibkr_evidence.test.js). Pure functions in node:vm with service stubs.
 * Run: node tests/js/pf_uncertified_context.test.js */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { execFileSync } = require('node:child_process');

const ROOT = path.join(__dirname, '../..');
const GAS = path.join(ROOT, 'apps_script/10_My_Portfolio.gs');
const source = fs.readFileSync(GAS, 'utf8');
const plain = v => (v === undefined ? undefined : JSON.parse(JSON.stringify(v)));
let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + (e && e.stack || e)); }
}
function load(props, sheets) {
  const store = Object.assign({}, props || {});
  const propsService = {
    getProperty(k) { return Object.prototype.hasOwnProperty.call(store, k) ? store[k] : null; },
    setProperty(k, v) { store[k] = String(v); return propsService; },
    deleteProperty(k) { delete store[k]; return propsService; }
  };
  const ss = { getSheetByName(n) {
    if (!sheets || !Object.prototype.hasOwnProperty.call(sheets, n)) return null;
    const grid = sheets[n];
    if (grid === 'throw') throw new Error('sheet boom');
    return { getDataRange() { return { getValues() { return grid; } }; } };
  } };
  const ctx = {
    SHEET_NAMES: { MY_PORTFOLIO: 'My_Portfolio' },
    ensureSheetExists_(name) { return ss.getSheetByName(name); },
    readCurrentSheetHeaders_(sheet) { return (sheet && sheet.__headers) || []; },
    console, Logger: { log() {} }, Date,
    Utilities: { formatDate() { return ''; }, sleep() {}, getUuid() { return 'u'; }, computeDigest() { return []; },
      base64Encode() { return ''; }, DigestAlgorithm: { SHA_256: 'SHA_256' }, Charset: { UTF_8: 'UTF_8' } },
    PropertiesService: { getScriptProperties() { return propsService; }, getDocumentProperties() { return propsService; } },
    SpreadsheetApp: { getActiveSpreadsheet() { return ss; }, getActive() { return ss; }, flush() {} },
    UrlFetchApp: { fetch() { throw new Error('no network'); } }, DriveApp: {},
    Session: { getScriptTimeZone() { return 'Asia/Riyadh'; } },
    ScriptApp: { getProjectTriggers() { return []; } },
    LockService: { getScriptLock() { return { tryLock() { return true; }, releaseLock() {} }; } }
  };
  vm.createContext(ctx);
  vm.runInContext(source, ctx, { filename: '10_My_Portfolio.gs' });
  return ctx;
}

test('V1 version lockstep (header line 4, PD_VERSION), syntax, the five functions and two vars exist', () => {
  const ctx = load();
  assert.equal(ctx.PD_VERSION, '1.5.14');
  assert.match(source.split('\n')[3], /^ \* Version: 1\.5\.14$/);
  assert(source.includes(' * v1.5.14 MAIN CHANGES  (UNCERTIFIED-VIEW CONTEXT + LEDGER CASH SNAPSHOT --'));
  assert(source.includes(' * v1.5.13 MAIN CHANGES  (RECEIPT BOUND TO THE ROWS ACTUALLY READ'), 'prior block kept');
  for (const f of ['pdUncertifiedContextOn_', 'pdUncertifiedContext_', 'pdCashSnapshotParse_', 'pdCashSnapshotRead_', 'pdActionLabel_']) {
    assert.equal(typeof ctx[f], 'function', f);
  }
  assert.equal(ctx.PD_UNCERT_CTX_PROP, 'PD_UNCERTIFIED_CONTEXT'); assert.equal(ctx.PD_CASH_SNAPSHOT_SHEET, '_Cash_Snapshot');
});
test('V2 every line added since the previous commit is ES5 and free of smart quotes / CR', () => {
  const diff = execFileSync('git', ['diff', 'HEAD', '--', 'apps_script/10_My_Portfolio.gs'], { cwd: ROOT, encoding: 'utf8' });
  const added = diff.split('\n').filter(l => l.startsWith('+') && !l.startsWith('+++')).map(l => l.slice(1));
  const bad = /\b(let|const)\b|=>|`|\bclass\b|\.\.\.|\.includes\(/;
  for (const line of added) { assert(!bad.test(line), 'ES5: ' + line); assert(!/[‘’“”\r]/.test(line), 'quotes/CR: ' + line); }
  assert(added.length === 0 || added.length > 60, 'diff present when uncommitted (' + added.length + ')');
});
test('C1 gate reads PD_UNCERTIFIED_CONTEXT = on only', () => {
  assert.equal(load().pdUncertifiedContextOn_(), false);
  assert.equal(load({ PD_UNCERTIFIED_CONTEXT: 'on' }).pdUncertifiedContextOn_(), true);
  assert.equal(load({ PD_UNCERTIFIED_CONTEXT: ' ON ' }).pdUncertifiedContextOn_(), true);
  for (const v of ['off', '1', 'true', 'observe', '']) assert.equal(load({ PD_UNCERTIFIED_CONTEXT: v }).pdUncertifiedContextOn_(), false, v);
});
test('C2 context: null when fresh; age/limit/reason from the plan; no-capture and future-capture shapes', () => {
  const ctx = load();
  const capMs = Date.parse('2026-10-16T05:41:07Z'), nowMs = capMs + 1497 * 60000 + 20000;
  assert.equal(ctx.pdUncertifiedContext_({ ok: true, capturedMs: capMs, ageMs: nowMs - capMs }, true, nowMs), null);
  const c = plain(ctx.pdUncertifiedContext_({ ok: true, capturedMs: capMs, ageMs: nowMs - capMs }, false, nowMs));
  assert.equal(c.captured_at, '2026-10-16T05:41:07.000Z'); assert.equal(c.age_min, 1497); assert.equal(c.limit_min, 14);
  assert.match(c.reason, /^capture .* is 24h57m old \(limit 14m\)$/);
  const none = plain(ctx.pdUncertifiedContext_({ ok: false, reason: 'capture folder not set (X)' }, false, nowMs));
  assert.deepEqual(none, { captured_at: null, age_min: null, limit_min: 14, reason: 'capture folder not set (X)' });
  assert.deepEqual(plain(ctx.pdUncertifiedContext_(null, false, nowMs)), { captured_at: null, age_min: null, limit_min: 14, reason: 'no capture' });
  const fut = plain(ctx.pdUncertifiedContext_({ ok: true, capturedMs: nowMs + 60000, ageMs: -60000 }, false, nowMs));
  assert.equal(fut.age_min, null); assert.equal(fut.reason, 'capture time is ahead of this clock');
  const noAge = plain(ctx.pdUncertifiedContext_({ ok: true, capturedMs: capMs }, false, capMs + 120000));
  assert.equal(noAge.age_min, 2, 'age derived from the clock when the plan has none');
});
test('C3 _Cash_Snapshot parse: last non-empty record, Balance SAR header scan, date column, ambiguity and junk', () => {
  const ctx = load();
  const grid = [['Snapshot Date', 'Balance SAR', 'Note'], ['2026-08-01', '9,130.00', 'USD 9130'], ['2026-10-16', 53769.26, ''], ['', '', '']];
  assert.deepEqual(plain(ctx.pdCashSnapshotParse_(grid)), { balance_sar: 53769.26, as_of: '2026-10-16' });
  assert.deepEqual(plain(ctx.pdCashSnapshotParse_([['balance_sar'], ['1200']])), { balance_sar: 1200, as_of: '' });
  assert.deepEqual(plain(ctx.pdCashSnapshotParse_([['Date', 'Balance SAR'], [new Date(Date.UTC(2026, 9, 16)), '10']])), { balance_sar: 10, as_of: '2026-10-16' });
  assert.equal(ctx.pdCashSnapshotParse_([['Balance SAR', 'Balance SAR'], ['1', '2']]), null, 'ambiguous header');
  assert.equal(ctx.pdCashSnapshotParse_([['Cash'], ['1']]), null, 'no balance column');
  assert.equal(ctx.pdCashSnapshotParse_([['Balance SAR'], ['junk']]), null); assert.equal(ctx.pdCashSnapshotParse_([['Balance SAR'], ['-5']]), null);
  assert.equal(ctx.pdCashSnapshotParse_([['Balance SAR']]), null); assert.equal(ctx.pdCashSnapshotParse_([]), null); assert.equal(ctx.pdCashSnapshotParse_(null), null);
  // reader: sheet present / absent / throwing
  assert.deepEqual(plain(load({}, { _Cash_Snapshot: grid }).pdCashSnapshotRead_()), { balance_sar: 53769.26, as_of: '2026-10-16' });
  assert.equal(load({}, {}).pdCashSnapshotRead_(), null); assert.equal(load({}, { _Cash_Snapshot: 'throw' }).pdCashSnapshotRead_(), null);
});
test('C4 action label: "(uncertified)" only on detail.uncertified === true; the ACTIONS row otherwise unchanged', () => {
  const ctx = load();
  assert.equal(ctx.pdActionLabel_({ action: 'HOLD', detail: { uncertified: true } }), 'HOLD (uncertified)');
  assert.equal(ctx.pdActionLabel_({ action: 'EXIT', detail: { uncertified: true } }), 'EXIT (uncertified)');
  assert.equal(ctx.pdActionLabel_({ action: 'HOLD', detail: { uncertified: 'true' } }), 'HOLD');
  assert.equal(ctx.pdActionLabel_({ action: 'BLOCK' }), 'BLOCK'); assert.equal(ctx.pdActionLabel_({}), ''); assert.equal(ctx.pdActionLabel_(null), '');
  const a = { action: 'HOLD', symbol: 'DDI.US', name: 'n', sector: 's', currency: 'USD', quantity: 200, market_value_sar: 11858, weight_pct: 14.3,
    suggested_delta_shares: 0, detail: { uncertified: true } };
  const row = plain(ctx.pdActionToRow_(a));
  assert.equal(row[0], 'HOLD (uncertified)'); assert.equal(row[1], 'DDI.US'); assert.equal(row[9], 11858); assert.equal(row[13], 14.3); assert.equal(row[15], 0);
  assert.equal(row.length, ctx.PD_ACTION_HEADERS.length);
  assert.equal(plain(ctx.pdActionToRow_(Object.assign({}, a, { detail: {} })))[0], 'HOLD');
});
test('C5 hooks: context attached inside the enforce branch only when not fresh and gated; note after pdIbkrNote_; selftest line', () => {
  const sent = source.indexOf('      if (fresh) body.reconciliation_evidence = plan.packet;'), hook = source.indexOf('      if (!fresh && pdUncertifiedContextOn_()) {');
  const noteBase = source.indexOf('    res.note = pdIbkrNote_(mode, plan, fresh, split.excluded);'), note = source.indexOf("      res.note += ' | uncertified ctx sent (age ' +");
  assert(sent > 0 && hook === source.indexOf('\n', sent) + 1 + '      /* v1.5.14 [C1 / C3]: stale capture -> caller context + ledger cash\n       * snapshot for the backend\'s uncertified view (property-gated). */\n'.length, 'hook directly after the packet line');
  assert(noteBase > 0 && noteBase < note && note - noteBase < 200);
  assert(source.includes("        body.capture_context = pdUncertifiedContext_(plan, fresh, nowMs);"));
  assert(source.includes("        if (snap) body.cash_snapshot = snap;"));
  assert(source.includes("report.push('uncertified ctx (' + PD_UNCERT_CTX_PROP + '): ' +"));
});

console.log((failed ? 'FAIL ' : 'PASS ') + passed + '/' + (passed + failed));
process.exit(failed ? 1 : 0);
