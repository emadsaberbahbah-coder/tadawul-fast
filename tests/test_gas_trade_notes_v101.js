#!/usr/bin/env node
/* tests/test_gas_trade_notes_v101.js
 * Harness for apps_script/25_Trade_Notes.gs v1.0.1 [review fixes, PR #721].
 * Loads the REAL .gs into a vm context with the same in-memory Sheets model
 * as test_gas_trade_notes_v100.js, plus a recording LockService.
 *
 *   N1-N2  Reason Class must be one of REASON_CLASSES and is stored canonical
 *   N3-N6  an existing log tab must start with the declared header
 *   N7-N9  the Note ID scan, allocation and append run under one lock
 *
 * Golden negative: TFB_TN_SRC=<path to the v1.0.0 file> runs the same cases
 * against the old source, where N1-N4 and N7-N9 must FAIL.
 * Run: node tests/test_gas_trade_notes_v101.js
 */
'use strict';
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const SRC = process.env.TFB_TN_SRC || path.join(__dirname, '..', 'apps_script', '25_Trade_Notes.gs');
const code = fs.readFileSync(SRC, 'utf8');

class Sheet {
  constructor(name, events) { this.name = name; this.rows = []; this.frozen = 0; this.validations = {}; this.appendLog = []; this.events = events || []; this.failAppend = false; }
  _ensure(r, c) { while (this.rows.length < r) { this.rows.push([]); } for (const row of this.rows) { while (row.length < c) { row.push(''); } } }
  getLastRow() { this.events.push('read:' + this.name); let last = 0; this.rows.forEach((row, i) => { if (row.some((v) => v !== '' && v !== null && v !== undefined)) { last = i + 1; } }); return last; }
  setFrozenRows(n) { this.frozen = n; }
  appendRow(row) {
    if (this.failAppend) { throw new Error('append failed'); }
    this.events.push('append:' + this.name);
    const last = this.rows.reduce((acc, r, i) => (r.some((v) => v !== '' && v !== null && v !== undefined) ? i + 1 : acc), 0);
    this._ensure(last + 1, row.length); this.rows[last] = row.slice(); this.appendLog.push(row.slice());
  }
  getRange(r, c, nr, nc) {
    nr = nr || 1; nc = nc || 1; const sh = this;
    return {
      getValues() { sh._ensure(r + nr - 1, c + nc - 1); const out = []; for (let i = 0; i < nr; i++) { out.push(sh.rows[r - 1 + i].slice(c - 1, c - 1 + nc)); } return out; },
      getValue() { return this.getValues()[0][0]; },
      setValues(vals) { sh._ensure(r + vals.length - 1, c + vals[0].length - 1); vals.forEach((row, i) => row.forEach((v, j) => { sh.rows[r - 1 + i][c - 1 + j] = v; })); },
      setValue(v) { this.setValues([[v]]); },
      clearContent() { this.setValues(Array.from({ length: nr }, () => Array(nc).fill(''))); },
      setDataValidation(rule) { sh.validations[r + ':' + c] = rule; }
    };
  }
}

// lockMode: 'ok' (tryLock succeeds) | 'busy' (tryLock times out)
function makeWorld(opts) {
  const o = opts || {};
  const events = [];
  const sheets = { _Run_Log: new Sheet('_Run_Log', []) };
  const ss = { getSheetByName: (n) => sheets[n] || null, insertSheet: (n) => { sheets[n] = new Sheet(n, events); return sheets[n]; } };
  const world = { sheets, events, props: Object.assign({}, o.props || {}) };
  const lock = {
    tryLock: (ms) => { events.push('lock'); return o.lockMode !== 'busy'; },
    waitLock: (ms) => { events.push('lock'); if (o.lockMode === 'busy') { throw new Error('busy'); } },
    releaseLock: () => { events.push('release'); }
  };
  const ctx = {
    Date, JSON, Math, Number, String, parseInt, isNaN, Object, Array, RegExp, Error,
    PropertiesService: { getScriptProperties: () => ({ getProperty: (k) => (k in world.props ? world.props[k] : null) }) },
    SpreadsheetApp: {
      getActiveSpreadsheet: () => ss,
      newDataValidation: () => { const b = { list: null, allow: null, requireValueInList: (l) => { b.list = l; return b; }, setAllowInvalid: (a) => { b.allow = a; return b; }, build: () => ({ list: b.list, allow: b.allow }) }; return b; }
    },
    LockService: { getScriptLock: () => lock, getDocumentLock: () => lock },
    Logger: { log: () => {} }
  };
  vm.createContext(ctx);
  vm.runInContext(code + '\n;', ctx, { filename: '25_Trade_Notes.gs' });
  world.ctx = ctx;
  world.addSheet = (name) => { sheets[name] = new Sheet(name, events); return sheets[name]; };
  return world;
}

function divergenceRec(reasonClass) {
  return {
    type: 'DIVERGENCE', trade_date: '2026-10-06', symbol: 'ADAM.US', side: 'NONE', origin: 'NO-TRADE',
    divergence: 'YES', reason_class: reasonClass, reason_ref: 'D-3', system_verdict: 'board INVEST 269 sh',
    thesis: 't', evidence: 'e', benchmark: 'b', exit_rule: 'n/a', size_rationale: 'n/a'
  };
}

const out = []; let fails = 0;
function T(name, cond, detail) { out.push((cond ? 'PASS ' : 'FAIL ') + name + (detail ? ' | ' + detail : '')); if (!cond) { fails++; } }
function safe(fn) { try { return fn(); } catch (e) { return 'THREW:' + e.message; } }

function run() {
  // N1 a Reason Class typo on a divergence is rejected and nothing is appended
  {
    const w = makeWorld();
    const r = safe(() => w.ctx.tbLogTradeNote(divergenceRec('JUDGEMENT')));
    const tn = w.sheets._Trade_Notes;
    T('N1 reason_class typo rejected', String(r).indexOf('FAILED:') === 0 && String(r).indexOf('reason_class not in') !== -1, r);
    T('N1 nothing appended', !tn || tn.getLastRow() <= 1);
  }
  // N2 a valid class in another case is accepted and stored in canonical spelling
  {
    const w = makeWorld();
    const r = safe(() => w.ctx.tbLogTradeNote(divergenceRec(' judgment ')));
    const row = w.sheets._Trade_Notes && w.sheets._Trade_Notes.rows[1];
    T('N2 lower-case class accepted, stored canonical', r === 'OK:TN-20261006-001' && row && row[13] === 'JUDGMENT', r + ' ' + (row && row[13]));
  }
  // N3 an existing tab with a reordered header: log, score and setup all refuse, header untouched
  {
    const w = makeWorld();
    const sh = w.addSheet('_Trade_Notes');
    const hdr = w.ctx.TFB_TN_HEADER.slice(); const t = hdr[4]; hdr[4] = hdr[5]; hdr[5] = t; // swap Symbol / Side
    sh.getRange(1, 1, 1, hdr.length).setValues([hdr]);
    sh.appendRow(['TN-20261001-001'].concat(Array(27).fill('x')));
    const before = JSON.stringify(sh.rows);
    const r = safe(() => w.ctx.tbLogTradeNote(divergenceRec('JUDGMENT')));
    T('N3 log refuses reordered header', String(r).indexOf('FAILED:') === 0 && String(r).indexOf('schema mismatch') !== -1, r);
    const s = safe(() => w.ctx.tbScoreTradeNote('TN-20261001-001', 'WIN', 1, 'x'));
    T('N3 score refuses reordered header', String(s).indexOf('FAILED:') === 0 && String(s).indexOf('schema mismatch') !== -1, s);
    const u = safe(() => w.ctx.tbTradeNotesSetup());
    T('N3 setup reports the mismatch', String(u).indexOf('FAILED:') === 0, u);
    T('N3 sheet unchanged', JSON.stringify(sh.rows) === before);
    const rl = w.sheets._Run_Log.appendLog.map((x) => x[2] + ':' + x[4]);
    T('N3 run_log records the refusals', rl.includes('tbLogTradeNote:FAILED') && rl.includes('tbScoreTradeNote:FAILED') && rl.includes('tbTradeNotesSetup:FAILED'), rl.join(','));
  }
  // N4 a truncated header (only A1) is refused
  {
    const w = makeWorld();
    const sh = w.addSheet('_Trade_Notes');
    sh.getRange(1, 1).setValue('Note ID');
    const r = safe(() => w.ctx.tbLogTradeNote(divergenceRec('JUDGMENT')));
    T('N4 truncated header refused', String(r).indexOf('FAILED:') === 0 && sh.getLastRow() === 1, r);
  }
  // N5 an extra trailing column is allowed (new columns go last)
  {
    const w = makeWorld();
    const sh = w.addSheet('_Trade_Notes');
    const hdr = w.ctx.TFB_TN_HEADER.concat(['Operator Comment']);
    sh.getRange(1, 1, 1, hdr.length).setValues([hdr]);
    const r = safe(() => w.ctx.tbLogTradeNote(divergenceRec('JUDGMENT')));
    T('N5 extra trailing column accepted', r === 'OK:TN-20261006-001', r);
  }
  // N6 an empty existing tab still gets the header, and setup stays idempotent
  {
    const w = makeWorld();
    w.addSheet('_Trade_Notes');
    const r1 = safe(() => w.ctx.tbTradeNotesSetup());
    const r2 = safe(() => w.ctx.tbTradeNotesSetup());
    T('N6 empty tab gets header, setup idempotent', r1 === 'OK:_Trade_Notes,_Trade_Notes_Input' && r2 === r1 && JSON.stringify(w.sheets._Trade_Notes.rows[0]) === JSON.stringify(w.ctx.TFB_TN_HEADER), r1 + ' / ' + r2);
  }
  // N7 the ID read and the append both happen inside one lock
  {
    const w = makeWorld();
    w.ctx.tbTradeNotesSetup();
    w.events.length = 0;
    const r = safe(() => w.ctx.tbLogTradeNote(divergenceRec('JUDGMENT')));
    const ev = w.events.filter((e) => e === 'lock' || e === 'release' || e === 'read:_Trade_Notes' || e === 'append:_Trade_Notes');
    const iL = ev.indexOf('lock'), iR = ev.indexOf('read:_Trade_Notes'), iA = ev.indexOf('append:_Trade_Notes'), iU = ev.lastIndexOf('release');
    T('N7 lock held across ID read and append', r === 'OK:TN-20261006-001' && iL !== -1 && iL < iR && iR < iA && iA < iU, ev.join(','));
  }
  // N8 a lock timeout appends nothing and returns FAILED
  {
    const w = makeWorld({ lockMode: 'busy' });
    w.addSheet('_Trade_Notes').getRange(1, 1, 1, 28).setValues([makeWorld().ctx.TFB_TN_HEADER]);
    const r = safe(() => w.ctx.tbLogTradeNote(divergenceRec('JUDGMENT')));
    T('N8 lock timeout -> FAILED, nothing appended', r === 'FAILED:lock timeout' && w.sheets._Trade_Notes.getLastRow() === 1, r);
    const rl = w.sheets._Run_Log.appendLog.map((x) => x[2] + ':' + x[4] + ':' + x[5]);
    T('N8 run_log records the timeout', rl.some((x) => x === 'tbLogTradeNote:FAILED:lock timeout'), rl.join(','));
  }
  // N9 the lock is released even when the append throws
  {
    const w = makeWorld();
    w.ctx.tbTradeNotesSetup();
    w.sheets._Trade_Notes.failAppend = true;
    w.events.length = 0;
    const r = safe(() => w.ctx.tbLogTradeNote(divergenceRec('JUDGMENT')));
    T('N9 lock released after a failed append', String(r).indexOf('THREW:') === 0 && w.events.includes('lock') && w.events[w.events.length - 1] === 'release', r + ' ' + w.events.join(','));
  }
  // N10 self-test and version floor
  {
    const w = makeWorld();
    const v = w.ctx.tbTradeNotesSelfTest();
    const ver = String(w.ctx.TFB_TRADE_NOTES_VERSION).split('.').map(Number);
    T('N10 selftest ok, version >= 1.0.1', v === 'trade notes core: ok' && (ver[0] > 1 || (ver[0] === 1 && (ver[1] > 0 || ver[2] >= 1))), v + ' v' + w.ctx.TFB_TRADE_NOTES_VERSION);
  }
}

run();
console.log(out.join('\n'));
console.log(`SUMMARY ${out.length - fails}/${out.length} PASS`);
process.exit(fails ? 1 : 0);
