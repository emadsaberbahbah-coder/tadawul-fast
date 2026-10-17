/* 16_Decision_Top10.gs v1.15.0 (2026-10-17) -- T2: panel sector cap applied to
 * CARRIED seats (deferred-seat table under SELECTED, stab token, audit list,
 * kill switch DT10_SEAT_SECTOR_CAP_LEGACY) + ALL QUALIFIED 'Cluster' column.
 * Full Apps Script source loaded into node:vm with minimal service stubs; pure
 * functions called directly; the stability core is driven end to end on the
 * 17-Oct shape (5 Real Estate seats of 10 against Max Per Sector 2). The
 * reversal layer (dt10_v1150_history.js) must take the delivered file back to
 * the v1.14.0 source that dt10_v1140_history.js then proves.
 * Run: node tests/js/dt10_v1150_cluster_seat_cap.test.js */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {revertV1150, addedLines1150, EDITS_1150} = require('./dt10_v1150_history');
const {revertV1140} = require('./dt10_v1140_history');
const {feedHistorySource} = require('./dt10_cash_utc_history');

const ROOT = path.join(__dirname, '../..');
const source = fs.readFileSync(path.join(ROOT, 'apps_script/16_Decision_Top10.gs'), 'utf8');
const plain = v => (v === undefined ? undefined : JSON.parse(JSON.stringify(v)));
let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + (e && e.stack || e)); }
}
function load(props) {
  const store = Object.assign({}, props || {});
  const propsService = {
    getProperty(k) { return Object.prototype.hasOwnProperty.call(store, k) ? store[k] : null; },
    setProperty(k, v) { store[k] = String(v); return propsService; },
    deleteProperty(k) { delete store[k]; return propsService; },
    getProperties() { return store; }
  };
  const ctx = {
    console, Logger: { log() {} }, Date,
    Utilities: { formatDate() { return ''; }, sleep() {}, getUuid() { return 'u'; }, computeDigest() { return []; },
      base64Encode() { return ''; }, DigestAlgorithm: { MD5: 'MD5', SHA_256: 'SHA_256' }, Charset: { UTF_8: 'UTF_8' } },
    PropertiesService: { getScriptProperties() { return propsService; }, getDocumentProperties() { return null; }, getUserProperties() { return propsService; } },
    SpreadsheetApp: { getActiveSpreadsheet() { throw new Error('no sheet in this harness'); }, getActive() { throw new Error('no sheet'); }, flush() {}, BorderStyle: { SOLID: 'SOLID' } },
    UrlFetchApp: { fetch() { throw new Error('no network'); } },
    Session: { getScriptTimeZone() { return 'Asia/Riyadh'; } },
    CacheService: { getScriptCache() { return { get() { return null; }, put() {} }; } },
    LockService: { getScriptLock() { return { tryLock() { return true; }, releaseLock() {} }; } }
  };
  vm.createContext(ctx);
  vm.runInContext(source, ctx, { filename: '16_Decision_Top10.gs' });
  return ctx;
}

/* ---------------------------------------------------------------- V */
test('V1 version lockstep, header line 4, syntax, census 205/105, the six functions and one var exist', () => {
  const ctx = load();
  assert.equal(ctx.DT10_VERSION, '1.15.0');
  assert.match(source.split('\n')[3], /^ \* Version: 1\.15\.0 /);
  assert(source.includes(' * v1.15.0 (2026-10-17) -- SECTOR CAP ON CARRIED SEATS + CLUSTER COLUMN (T2)'));
  assert(source.includes(' * v1.14.0 (2026-10-17) -- INVESTABILITY REASONS, EARNINGS HOLD, TURNOVER TOKEN (T1/T5/T6)'), 'prior WHY kept');
  assert.doesNotThrow(() => new vm.Script(source, { filename: '16_Decision_Top10.gs' }));
  const fns = new Set(); let m; const re = /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg; while ((m = re.exec(source))) fns.add(m[1]);
  const vars = new Set(); const rv = /^var\s+([A-Za-z_$][\w$]*)/mg; while ((m = rv.exec(source))) vars.add(m[1]);
  assert.equal(fns.size, 205); assert.equal(vars.size, 105);
  for (const f of ['dt10SeatSectorCapLegacy_', 'dt10SeatCapSector_', 'dt10SeatCapDefer_', 'dt10SeatSectorCapCore_',
    'dt10SeatSectorCapApply_', 'dt10SeatCapRow_']) {
    assert(fns.has(f), f); assert.equal(typeof ctx[f], 'function', f);
  }
  assert(vars.has('DT10_SEATCAP_HEADERS'));
  assert.deepEqual(plain(ctx.DT10_SEATCAP_HEADERS), ['Rank', 'Symbol', 'Name', 'Sector', 'Stability', 'Note']);
});
test('V2 reversal layer: the ten v1.15.0 edits occur exactly once and the chain reaches the pinned v1.13.8 digest via v1.14.0', () => {
  assert.equal(EDITS_1150.length, 10);
  const prior = revertV1150(source);
  assert(prior.includes("var DT10_VERSION = '1.14.0';") && !prior.includes('dt10SeatSectorCapCore_'));
  feedHistorySource(revertV1140(prior));   // throws on any digest mismatch
  assert.throws(() => revertV1150(source + "\nvar DT10_VERSION = '1.15.0';"), /exactly one approved v1\.15\.0 edit/);
});
test('V3 every added line is ES5 and free of smart quotes / CR', () => {
  const bad = /\b(let|const)\b|=>|`|\bclass\b|\.\.\.|\.includes\(/;
  for (const line of addedLines1150()) {
    assert(!bad.test(line), 'ES5: ' + line);
    assert(!/[‘’“”\r]/.test(line), 'quotes/CR: ' + line);
  }
  assert.equal((source.match(/[‘’“”]/g) || []).length, 0);
});

/* ---------------------------------------------------------------- core */
function seat(sym, sector, rank, status, extra) {
  return Object.assign({ symbol: sym, name: sym + ' Co', sector, rank, _stab_status: status,
    entry_zone: '—', suggested_sar: '—', suggested_shares: '—', advisor_note: 'x' }, extra || {});
}
test('C1 sector bucket: trimmed sector; blank and Unknown never cap', () => {
  const ctx = load();
  assert.equal(ctx.dt10SeatCapSector_({ sector: ' Real Estate ' }), 'Real Estate');
  assert.equal(ctx.dt10SeatCapSector_({ sector: 'Unknown' }), '');
  assert.equal(ctx.dt10SeatCapSector_({ sector: 'unknown' }), '');
  assert.equal(ctx.dt10SeatCapSector_({ sector: '' }), ''); assert.equal(ctx.dt10SeatCapSector_({}), ''); assert.equal(ctx.dt10SeatCapSector_(null), '');
});
test('C2 17-Oct shape: 5 Real Estate of 10 (2 raw + 3 carried) against cap 2 -> 2 kept, the lowest-ranked three carried deferred, SelLog set otherwise unchanged', () => {
  const ctx = load();
  const board = [
    seat('VICI.US', 'Real Estate', 1, 'ACTIVE (day 12)'), seat('AX.US', 'Financials', 2, 'ACTIVE (day 9)'),
    seat('GLPI.US', 'Real Estate', 3, 'GRACE (1/3 missed)'), seat('EPRT.US', 'Real Estate', 4, 'ACTIVE (day 4)'),
    seat('VLY.US', 'Financials', 5, 'NEW (confirmed 2/2)'), seat('NNN.US', 'Real Estate', 6, 'GRACE (2/3 missed)'),
    seat('O.US', 'Real Estate', 7, 'GRACE (1/3 missed)'), seat('CRC.US', 'Energy', 8, 'ACTIVE (day 3)'),
    seat('PGC.US', 'Financials', 9, 'FAST-TRACK (day 1)'), seat('ZZ.US', 'Unknown', 10, 'GRACE (1/3 missed)')];
  const raw = { 'VICI.US': true, 'EPRT.US': true, 'AX.US': true, 'VLY.US': true, 'CRC.US': true, 'PGC.US': true };
  const res = ctx.dt10SeatSectorCapCore_(board, raw, 2);
  assert.deepEqual(plain(res.deferred.map(t => t.symbol)), ['GLPI.US', 'NNN.US', 'O.US'], 'all three carried RE seats go: the raw pair already fills the cap');
  assert.deepEqual(plain(res.kept.map(t => t.symbol)), ['VICI.US', 'AX.US', 'EPRT.US', 'VLY.US', 'CRC.US', 'PGC.US', 'ZZ.US']);
  assert.equal(res.kept.filter(t => t.sector === 'Real Estate').length, 2, 'Real Estate seats 5 -> 2');
  assert.deepEqual(plain(res.kept.map(t => t.rank)), [1, 2, 3, 4, 5, 6, 7], 're-ranked 1..N');
  const d = res.deferred[0];
  assert.equal(d._seat_cap_deferred, true); assert.equal(d._seat_cap_rank, 3);
  assert.equal(d._seat_cap_note, 'sector cap — carried seat deferred (Real Estate 3/2)');
  assert.equal(d._stab_status, 'GRACE (1/3 missed) - sector cap deferred', 'prefix kept: SelLog signature / paint key on the first word');
  assert.match(d.advisor_note, /^SECTOR CAP — carried seat deferred: Real Estate already holds 2 seat\(s\)/);
  assert.equal(res.deferred[1]._seat_cap_note, 'sector cap — carried seat deferred (Real Estate 3/2)');
  assert.equal(res.kept[6].symbol, 'ZZ.US', 'Unknown never capped');
  for (const t of res.kept) assert.equal(t._seat_cap_deferred, undefined);
});
test('C3 carried seats fill what the cap leaves, in rank order; raw seats are never deferred even above the cap; junk cap is a no-op', () => {
  const ctx = load();
  const one = ctx.dt10SeatSectorCapCore_([seat('A', 'RE', 1, 'ACTIVE (day 2)'), seat('B', 'RE', 2, 'GRACE (1/3 missed)'),
    seat('C', 'RE', 3, 'GRACE (2/3 missed)')], { A: true }, 2);
  assert.deepEqual(plain(one.kept.map(t => t.symbol)), ['A', 'B']); assert.deepEqual(plain(one.deferred.map(t => t.symbol)), ['C']);
  const rawOver = ctx.dt10SeatSectorCapCore_([seat('A', 'RE', 1, 'ACTIVE (day 2)'), seat('B', 'RE', 2, 'ACTIVE (day 2)'),
    seat('C', 'RE', 3, 'ACTIVE (day 2)'), seat('D', 'RE', 4, 'GRACE (1/3 missed)')], { A: true, B: true, C: true }, 2);
  assert.deepEqual(plain(rawOver.kept.map(t => t.symbol)), ['A', 'B', 'C'], 'builder picks are its contract, not ours');
  assert.deepEqual(plain(rawOver.deferred.map(t => t.symbol)), ['D']);
  assert.equal(rawOver.deferred[0]._seat_cap_note, 'sector cap — carried seat deferred (RE 4/2)');
  for (const cap of ['junk', '', null, undefined, 0, -1]) {
    const r = ctx.dt10SeatSectorCapCore_([seat('A', 'RE', 1, 'GRACE (1/3 missed)'), seat('B', 'RE', 2, 'GRACE (1/3 missed)')], {}, cap);
    assert.equal(r.deferred.length, 0, 'cap ' + cap); assert.deepEqual(plain(r.kept.map(t => t.rank)), [1, 2], 'untouched ranks');
  }
  assert.deepEqual(plain(ctx.dt10SeatSectorCapCore_([], {}, 2)), { kept: [], deferred: [] });
  assert.deepEqual(plain(ctx.dt10SeatSectorCapCore_(null, null, 2)), { kept: [], deferred: [] });
});
test('C4 apply: trims out.tickets in place, names the deferred in audit + payload, returns the token; nothing when none deferred', () => {
  const ctx = load();
  const out = { tickets: [seat('A', 'RE', 1, 'ACTIVE (day 2)'), seat('B', 'RE', 2, 'GRACE (1/3 missed)'), seat('C', 'RE', 3, 'GRACE (1/3 missed)'),
    seat('D', 'RE', 4, 'GRACE (2/3 missed)'), seat('E', 'RE', 5, 'GRACE (1/3 missed)'), seat('F', 'RE', 6, 'GRACE (1/3 missed)')],
    audit: { entered: [] }, note: 'stab: 1 grace' };
  const payload = { selected: [{ symbol: 'A' }] };
  const tok = ctx.dt10SeatSectorCapApply_(out, payload.selected, { 'T10: Max Per Sector': '2' }, payload);
  assert.equal(tok, 'seat-cap deferred 4 (C/D/E/F)');
  assert.deepEqual(plain(out.tickets.map(t => t.symbol)), ['A', 'B']);
  assert.deepEqual(plain(out.audit.sector_cap_deferred), ['C', 'D', 'E', 'F']);
  assert.deepEqual(plain(payload._dt10_seat_cap_deferred.map(t => t.symbol)), ['C', 'D', 'E', 'F']);
  const seven = { tickets: [seat('A', 'RE', 1, 'x')].concat(['B', 'C', 'D', 'E', 'F', 'G'].map((s, i) => seat(s, 'RE', i + 2, 'GRACE (1/3 missed)'))), audit: {}, note: '' };
  assert.equal(ctx.dt10SeatSectorCapApply_(seven, [{ symbol: 'A' }], { 'T10: Max Per Sector': 2 }, {}), 'seat-cap deferred 5 (C/D/E/F+1)', 'dt10JoinCap_ at 4');
  const none = { tickets: [seat('A', 'RE', 1, 'x'), seat('B', 'FI', 2, 'y')], audit: {}, note: 'stab: steady' };
  const p2 = {};
  assert.equal(ctx.dt10SeatSectorCapApply_(none, [{ symbol: 'A' }], { 'T10: Max Per Sector': 2 }, p2), '');
  assert.equal(none.tickets.length, 2); assert.equal(none.audit.sector_cap_deferred, undefined); assert.equal(p2._dt10_seat_cap_deferred, undefined);
  assert.equal(ctx.dt10SeatSectorCapApply_(null, [], {}, {}), ''); assert.equal(ctx.dt10SeatSectorCapApply_({}, [], {}, {}), '');
});
test('C5 kill switch DT10_SEAT_SECTOR_CAP_LEGACY = "1": board exactly the core output, no token, no payload list', () => {
  const ctx = load({ DT10_SEAT_SECTOR_CAP_LEGACY: '1' });
  assert.equal(ctx.dt10SeatSectorCapLegacy_(), true);
  assert.equal(load({ DT10_SEAT_SECTOR_CAP_LEGACY: 'true' }).dt10SeatSectorCapLegacy_(), false);
  assert.equal(load().dt10SeatSectorCapLegacy_(), false);
  const out = { tickets: [seat('A', 'RE', 1, 'x'), seat('B', 'RE', 2, 'GRACE (1/3 missed)'), seat('C', 'RE', 3, 'GRACE (1/3 missed)')], audit: {}, note: '' };
  const payload = {};
  assert.equal(ctx.dt10SeatSectorCapApply_(out, [{ symbol: 'A' }], { 'T10: Max Per Sector': 2 }, payload), '');
  assert.equal(out.tickets.length, 3); assert.equal(payload._dt10_seat_cap_deferred, undefined);
});
test('C6 end to end through dt10StabCore_: carried GRACE ghosts of a capped sector are deferred; the state blob (membership, clocks) is untouched', () => {
  const ctx = load();
  const today = '2026-10-17';
  const state = { v: 1, date: '2026-10-16', symbols: {} };
  const mk = (member, co, ci) => ({ ci, co, member, since: '2026-10-01', ls: '2026-10-16', hist: [60, 61], ft: false,
    id: { n: 'n', mk: 'US', se: 'Real Estate', cy: 'USD', px: 10 } });
  for (const s of ['VICI.US', 'GLPI.US', 'NNN.US', 'O.US', 'EPRT.US']) state.symbols[s] = mk(true, 0, 3);
  state.symbols['AX.US'] = Object.assign(mk(true, 0, 3), { id: { n: 'n', mk: 'US', se: 'Financials', cy: 'USD', px: 10 } });
  const raw = [{ symbol: 'VICI.US', sector: 'Real Estate', suggested_shares: 10 }, { symbol: 'EPRT.US', sector: 'Real Estate', suggested_shares: 10 },
    { symbol: 'AX.US', sector: 'Financials', suggested_shares: 10 }];
  const cands = raw.map(t => ({ symbol: t.symbol, sector: t.sector, verdict: 'INVEST', opportunity_score: 70, name: 'n', market: 'US', currency: 'USD', price: 10 }));
  const knobs = { enabled: true, confirm_days: 2, exit_days: 3, rank_buffer: 0, smooth_days: 5, hard_strict: false };
  const out = ctx.dt10StabCore_(raw, cands, state, knobs, 10, today, null);
  assert.deepEqual(plain(out.tickets.map(t => t.symbol).sort()), ['AX.US', 'EPRT.US', 'GLPI.US', 'NNN.US', 'O.US', 'VICI.US']);
  assert.equal(out.tickets.filter(t => t.sector === 'Real Estate').length, 5, 'before the cap: 5 Real Estate of 6');
  const before = JSON.stringify(out.state);
  const payload = { selected: raw };
  const tok = ctx.dt10SeatSectorCapApply_(out, payload.selected, { 'T10: Max Per Sector': 2 }, payload);
  assert.match(tok, /^seat-cap deferred 3 \(/);
  assert.equal(out.tickets.filter(t => t.sector === 'Real Estate').length, 2, 'Real Estate seats 5 -> 2');
  assert.deepEqual(plain(out.tickets.filter(t => t.sector === 'Real Estate').map(t => t.symbol).sort()), ['EPRT.US', 'VICI.US'], 'the raw pair stays');
  assert.deepEqual(plain(out.audit.sector_cap_deferred.sort()), ['GLPI.US', 'NNN.US', 'O.US']);
  for (const t of payload._dt10_seat_cap_deferred) {
    assert.equal(t._grace_hold, true, 'ghost'); assert.match(t._stab_status, /^GRACE \(1\/3 missed\) - sector cap deferred$/);
  }
  assert.equal(JSON.stringify(out.state), before, 'membership and clocks untouched by the pass');
  assert.equal(out.state.symbols['GLPI.US'].member, true); assert.equal(out.state.symbols['GLPI.US'].co, 1);
  // SelLog: the kept board signs as before minus the deferred rows
  const sig = ctx.dt10SelLogSignature_(out.tickets);
  assert(!/GLPI|NNN|O\.US/.test(sig) && /VICI\.US~SF~ACTIVE/.test(sig), sig);
});
test('C7 deferred-seat table row, hook placement, render block, selftest line', () => {
  const ctx = load();
  const t = ctx.dt10SeatCapDefer_(seat('GLPI.US', 'Real Estate', 3, 'GRACE (1/3 missed)'), 'Real Estate', 3, 2);
  assert.deepEqual(plain(ctx.dt10SeatCapRow_(t)), [3, 'GLPI.US', 'GLPI.US Co', 'Real Estate', 'GRACE (1/3 missed) - sector cap deferred',
    'sector cap — carried seat deferred (Real Estate 3/2)']);
  assert.deepEqual(plain(ctx.dt10SeatCapRow_(null)), ['', '', '', '', '', '']);
  const core = source.indexOf('state, knobs, limit, epoch.key, outageMap);'), hook = source.indexOf('var seatCapNote = dt10SeatSectorCapApply_(out, payload.selected || [],'),
    assign = source.indexOf('payload.selected = out.tickets;'), save = source.indexOf('var saveNote = dt10StabSave_(out.state);');
  assert(core > 0 && core < hook && hook < save && save < assign, 'hook sits after the core, before the state save and the board assignment');
  const sel = source.indexOf('  row = selT.next + 1;'), tbl = source.indexOf("'SECTOR CAP \\u2014 CARRIED SEATS DEFERRED ('"), qual = source.indexOf('var qualified = dt10QualifiedFromCands_(cands);');
  assert(sel > 0 && sel < tbl && tbl < qual, 'table sits between SELECTED and ALL QUALIFIED');
  assert(source.includes("report.push('seat sector cap core: ' +"));
});
test('C8 ALL QUALIFIED gains Cluster before Why Not Selected; the row builder places c.cluster there; CANDIDATES headers unchanged (WP-8 checksum contract)', () => {
  const ctx = load();
  assert.equal(ctx.DT10_QUAL_HEADERS.length, 16);
  assert.equal(ctx.DT10_QUAL_HEADERS[13], 'Selected'); assert.equal(ctx.DT10_QUAL_HEADERS[14], 'Cluster'); assert.equal(ctx.DT10_QUAL_HEADERS[15], 'Why Not Selected');
  const row = ctx.dt10QualToRow_({ symbol: 'AX.US', selected: true, cluster: 'Banks - Regional ×19', opportunity_score: 70 }, 1, {}, 24, 10, {});
  assert.equal(row.length, 16); assert.equal(row[14], 'Banks - Regional ×19'); assert.equal(row[15], '— selected'); assert.equal(row[13], 'Yes');
  assert.equal(ctx.dt10QualToRow_({ symbol: 'X' }, 2, {}, 24, 10, {})[14], '', 'legacy builder: blank cell');
  assert.equal(ctx.DT10_CAND_HEADERS.length, 25); assert.equal(ctx.DT10_CAND_HEADERS[24], 'Deferral');
});

console.log((failed ? 'FAIL ' : 'PASS ') + passed + '/' + (passed + failed));
process.exit(failed ? 1 : 0);
