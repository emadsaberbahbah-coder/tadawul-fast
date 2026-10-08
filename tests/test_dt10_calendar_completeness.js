#!/usr/bin/env node
/* Offline regression tests for the REAL native Calendar_Events reader.
 * Usage: node tests/test_dt10_calendar_completeness.js [--source path/to/16_Decision_Top10.gs]
 * Extracts production functions and their dt10 dependencies; no network,
 * spreadsheet writes, or copied implementation of date parsing is used.
 */
'use strict';

const assert = require('assert');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const args = process.argv.slice(2);
const sourceOption = args.indexOf('--source');
const sourcePath = sourceOption >= 0 ? args[sourceOption + 1] :
  path.join(__dirname, '..', 'apps_script', '16_Decision_Top10.gs');
if (!sourcePath) throw new Error('--source requires a file path');
const source = fs.readFileSync(sourcePath, 'utf8');

// This is the same real-function extraction approach as existing DT10
// harnesses. Calendar functions contain balanced regex quantifiers and no
// unmatched braces inside strings; dependencies are taken from production.
function extract(name) {
  const start = source.indexOf('function ' + name + '(');
  if (start < 0) throw new Error('Missing production function ' + name);
  let depth = 0;
  for (let i = source.indexOf('{', start); i < source.length; i++) {
    if (source[i] === '{') depth++;
    else if (source[i] === '}' && --depth === 0) return source.slice(start, i + 1);
  }
  throw new Error('Unbalanced production function ' + name);
}

const extracted = new Map();
function include(name) {
  if (extracted.has(name)) return;
  const text = extract(name);
  extracted.set(name, text);
  for (const match of text.matchAll(/\b(dt10[A-Za-z0-9]+_)\s*\(/g)) include(match[1]);
}
include('dt10EarningsMapFromValues_');
include('dt10EarningsMap_');
include('dt10ApplyEarningsTags_');

const capMatch = source.match(/^var\s+DT10_CALENDAR_MAX_ROWS\s*=\s*(\d+)\s*;/m);
if (!capMatch) throw new Error('Missing production DT10_CALENDAR_MAX_ROWS constant');
const bodyCap = Number(capMatch[1]);
const production = 'var DT10_CALENDAR_MAX_ROWS = ' + bodyCap + ';\n' +
  Array.from(extracted.values()).join('\n\n');
const NativeDate = Date;

function load(now = NativeDate.parse('2026-10-08T21:30:00Z')) {
  function FixedDate(...dateArgs) {
    if (!new.target) return new NativeDate(now).toString();
    return dateArgs.length ? new NativeDate(...dateArgs) : new NativeDate(now);
  }
  FixedDate.prototype = NativeDate.prototype;
  FixedDate.now = () => now;
  FixedDate.parse = NativeDate.parse;
  FixedDate.UTC = NativeDate.UTC;
  const context = vm.createContext({
    Date: FixedDate,
    Logger: { log() {} },
    Session: { getScriptTimeZone: () => 'Asia/Riyadh' },
    Utilities: {
      formatDate(date, timezone, pattern) {
        assert.strictEqual(timezone, 'Asia/Riyadh', 'Date conversion must use Riyadh');
        assert.strictEqual(pattern, 'yyyy-MM-dd');
        return new NativeDate(date.getTime() + 3 * 60 * 60 * 1000).toISOString().slice(0, 10);
      }
    }
  });
  vm.runInContext(production, context, { filename: sourcePath });
  return context;
}

const header = ['Symbol', 'Next Earnings Date', 'Days To Earnings',
  'Next Ex-Div Date', 'Days To Ex-Div', 'Last Updated (Riyadh)', 'Source'];
function row(symbol, date, days = 999) {
  return [symbol, date, days, '', '', '2026-10-07 14:46', 'provider; carried'];
}
function grid(rows, headings = header) { return [headings.slice(), ...rows]; }
function plain(value) { return JSON.parse(JSON.stringify(value)); }
function eq(actual, expected) { assert.deepStrictEqual(plain(actual), expected); }
function immutable(values) {
  for (const cells of values) Object.freeze(cells);
  return Object.freeze(values);
}

// The stub models a real seven-column sheet and rejects out-of-grid reads.
// It records service access and makes every mutation fail immediately.
function workbook(values, options = {}) {
  const calls = { ranges: [], reads: 0, writes: 0, sheets: [] };
  const lastRow = options.lastRow === undefined ? values.length : options.lastRow;
  const lastColumn = options.lastColumn === undefined ? 7 : options.lastColumn;
  const maxRows = options.maxRows === undefined ? Math.max(lastRow, 1000) : options.maxRows;
  const maxColumns = options.maxColumns === undefined ? lastColumn : options.maxColumns;
  const fail = stage => { if (options.fail === stage) throw new Error('Simulated ' + stage + ' failure'); };
  const mutation = () => { calls.writes++; throw new Error('Reader attempted a sheet mutation'); };
  const sheet = {
    getLastRow() { fail('lastRow'); return lastRow; },
    getLastColumn() { fail('lastColumn'); return lastColumn; },
    getMaxRows() { return maxRows; },
    getMaxColumns() { return maxColumns; },
    getRange(r, c, nr, nc) {
      fail('range');
      assert.ok(Number.isInteger(r) && Number.isInteger(c) &&
        Number.isInteger(nr) && Number.isInteger(nc), 'Use a numeric bounded range');
      assert.ok(r >= 1 && c >= 1 && nr >= 1 && nc >= 1, 'Range must have positive dimensions');
      assert.ok(r + nr - 1 <= maxRows && c + nc - 1 <= maxColumns, 'Range exceeds sheet grid');
      calls.ranges.push([r, c, nr, nc]);
      return {
        getValues() {
          fail('values');
          calls.reads++;
          return Array.from({ length: nr }, (_, i) =>
            Array.from({ length: nc }, (_, j) => (values[r + i - 1] || [])[c + j - 1] ?? ''));
        },
        setValue: mutation, setValues: mutation, clearContent: mutation
      };
    },
    clear: mutation, clearContents: mutation, appendRow: mutation,
    deleteRows: mutation, insertRows: mutation, insertColumns: mutation
  };
  const ss = {
    getSheetByName(name) {
      calls.sheets.push(name);
      assert.strictEqual(name, 'Calendar_Events', 'Reader must not depend on another status sheet');
      fail('sheet');
      return options.missing ? null : sheet;
    },
    insertSheet: mutation
  };
  return { ss, calls };
}

const results = [];
function test(name, body) {
  try { body(); results.push({ name, pass: true }); console.log('PASS ' + name); }
  catch (error) {
    results.push({ name, pass: false });
    console.error('FAIL ' + name + ': ' + error.message);
  }
}

test('Riyadh review day recomputes stale countdowns from original dates', () => {
  const values = immutable(grid([
    row('FRST.US', '2026-10-22', 14),
    row('PHP.L', '2026-10-15', 7),
    row('TODAY.US', '2026-10-09', 1)
  ]));
  const before = JSON.stringify(values);
  eq(load().dt10EarningsMapFromValues_(values, '2026-10-09'),
    { 'FRST.US': 13, 'PHP.L': 6, 'TODAY.US': 0 });
  assert.strictEqual(JSON.stringify(values), before, 'Original dates and observations must remain intact');
});

test('Countdown follows the requested date even when the sheet was not refreshed', () => {
  const context = load();
  const values = grid([row('FRST.US', '2026-10-22', 14)]);
  eq(context.dt10EarningsMapFromValues_(values, '2026-10-08'), { 'FRST.US': 14 });
  eq(context.dt10EarningsMapFromValues_(values, '2026-10-09'), { 'FRST.US': 13 });
});

test('Default clock rolls at Riyadh midnight, three hours before UTC midnight', () => {
  const values = grid([row('EVENT.US', '2026-10-09', 123)]);
  eq(load(NativeDate.parse('2026-10-08T20:59:59Z')).dt10EarningsMapFromValues_(values),
    { 'EVENT.US': 1 });
  eq(load(NativeDate.parse('2026-10-08T21:00:00Z')).dt10EarningsMapFromValues_(values),
    { 'EVENT.US': 0 });
});

test('An explicit Date asOf uses its Riyadh calendar day', () => {
  const values = grid([row('EVENT.US', '2026-10-09')]);
  const context = load();
  eq(context.dt10EarningsMapFromValues_(values, new NativeDate('2026-10-08T20:59:59Z')),
    { 'EVENT.US': 1 });
  eq(context.dt10EarningsMapFromValues_(values, new NativeDate('2026-10-08T21:00:00Z')),
    { 'EVENT.US': 0 });
});

test('Date cells preserve the event day in Riyadh across the UTC boundary', () => {
  const values = grid([
    row('BOUNDARY.US', new NativeDate('2026-10-08T21:00:00Z'), 55),
    row('MIDDAY.US', new NativeDate('2026-10-09T09:00:00Z'), 55),
    row('PAST.US', new NativeDate('2026-10-08T20:59:59Z'), 55)
  ]);
  const before = JSON.stringify(values);
  eq(load().dt10EarningsMapFromValues_(values, '2026-10-09'),
    { 'BOUNDARY.US': 0, 'MIDDAY.US': 0 });
  assert.strictEqual(JSON.stringify(values), before);
});

test('Known event dates work without a Days To Earnings column', () => {
  const values = [['Symbol', 'Next Earnings Date'], ['GRT-UN.TO', '2026-10-22']];
  eq(load().dt10EarningsMapFromValues_(values, '2026-10-09'), { 'GRT-UN.TO': 13 });
});

test('Header normalization supports earningsdate and normalizes symbols', () => {
  const values = [[' SYMBOL ', 'Earnings Date'], [' grt-un.to ', '2026-10-10']];
  eq(load().dt10EarningsMapFromValues_(values, '2026-10-09'), { 'GRT-UN.TO': 1 });
});

test('Preamble does not hide a complete header or later events', () => {
  const values = [['Calendar observations', ''], [], ...grid([row('EVENT.US', '2026-10-10')])];
  eq(load().dt10EarningsMapFromValues_(values, '2026-10-09'), { 'EVENT.US': 1 });
});

test('Header columns cannot be borrowed across different rows', () => {
  const values = [['Symbol', ''], ['', 'Next Earnings Date'], ['FALSE.US', '2026-10-10']];
  eq(load().dt10EarningsMapFromValues_(values, '2026-10-09'), {});
});

test('Unknown dates never fall back to finite static countdowns', () => {
  const values = grid([
    row('BLANK.US', '', 0), row('NULL.US', null, 2), row('MISSING.US', undefined, 5),
    row('UNKNOWN.US', 'Unknown', 0), row('INVALID.US', 'not-a-date', 999)
  ]);
  eq(load().dt10EarningsMapFromValues_(values, '2026-10-09'), {});
});

test('Past dates cannot create future tags from stale positive counters', () => {
  const values = grid([row('PAST.US', '2026-10-08', 7), row('OLD.US', '2026-01-01', 1)]);
  eq(load().dt10EarningsMapFromValues_(values, '2026-10-09'), {});
});

test('ISO dates must be complete calendar dates, not timestamps or numeric serials', () => {
  const invalid = ['10/09/2026', '2026-2-03', '2026-10-09T00:00:00Z',
    '2026-13-01', '2026-00-10', '2026-10-00', '2026-10-32',
    46304, new NativeDate(NaN), '2026-02-30'];
  const values = grid(invalid.map((date, i) => row('BAD' + i + '.US', date, 0)));
  eq(load().dt10EarningsMapFromValues_(values, '2026-01-01'), {});
});

test('Leap-day arithmetic accepts a leap year and rejects a false leap day', () => {
  const values = grid([
    row('LEAP.US', '2028-02-29', 999), row('MARCH.US', '2028-03-01', 999),
    row('INVALID.US', '2027-02-29', 0)
  ]);
  eq(load().dt10EarningsMapFromValues_(values, '2028-02-28'),
    { 'LEAP.US': 1, 'MARCH.US': 2 });
});

test('Gregorian century rules accept 2000 and reject 2100 as a leap year', () => {
  const context = load();
  eq(context.dt10EarningsMapFromValues_(grid([row('LEAP.US', '2000-02-29')]), '2000-02-28'),
    { 'LEAP.US': 1 });
  eq(context.dt10EarningsMapFromValues_(grid([row('INVALID.US', '2100-02-29', 0)]), '2100-02-28'), {});
});

test('Invalid explicit review dates fail safe rather than inventing a day', () => {
  const context = load();
  const values = grid([row('EVENT.US', '2026-10-10', 1)]);
  for (const asOf of [null, '', 'today', '2026-02-30', '2026-10-09T00:00:00Z',
    46304, new NativeDate(NaN)]) eq(context.dt10EarningsMapFromValues_(values, asOf), {});
});

test('Missing or incomplete inputs safely produce no annotations', () => {
  const context = load();
  for (const values of [null, undefined, [], [['Symbol', 'Days To Earnings'], ['EVENT.US', 0]],
    [['Next Earnings Date'], ['2026-10-10']]])
    eq(context.dt10EarningsMapFromValues_(values, '2026-10-09'), {});
});

test('Full used range includes row 256 and the last event on a seven-column sheet', () => {
  const values = [header.slice(), ...Array.from({ length: 384 }, () => row('', ''))];
  values[255] = row('GRT-UN.TO', '2026-10-22', 0);
  values[384] = row('LAST.US', '2026-10-23', 0);
  const before = JSON.stringify(values);
  const book = workbook(values, { maxRows: 1000, maxColumns: 7 });
  eq(load().dt10EarningsMap_(book.ss, '2026-10-09'), { 'GRT-UN.TO': 13, 'LAST.US': 14 });
  assert.deepStrictEqual(book.calls.ranges, [[1, 1, 385, 7]]);
  assert.strictEqual(book.calls.reads, 1);
  assert.strictEqual(book.calls.writes, 0);
  assert.deepStrictEqual(book.calls.sheets, ['Calendar_Events']);
  assert.strictEqual(JSON.stringify(values), before);
});

test('Reader uses actual used columns and never requests nonexistent H', () => {
  const values = [['Symbol', 'Next Earnings Date'], ['EVENT.US', '2026-10-10']];
  const book = workbook(values, { lastColumn: 2, maxColumns: 2 });
  eq(load().dt10EarningsMap_(book.ss, '2026-10-09'), { 'EVENT.US': 1 });
  assert.deepStrictEqual(book.calls.ranges, [[1, 1, 2, 2]]);
});

test('Extra used columns are bounded to the seven-column calendar contract', () => {
  const values = grid([row('EVENT.US', '2026-10-10')]);
  const book = workbook(values, { lastColumn: 12, maxColumns: 12 });
  eq(load().dt10EarningsMap_(book.ss, '2026-10-09'), { 'EVENT.US': 1 });
  assert.deepStrictEqual(book.calls.ranges, [[1, 1, 2, 7]]);
});

test('Body-row safety cap permits the complete final row at its exact limit', () => {
  assert.strictEqual(bodyCap, 5000);
  const values = [header.slice(), ...Array.from({ length: bodyCap }, () => row('', ''))];
  values[bodyCap] = row('BOUNDARY.US', '2026-10-10', 999);
  const book = workbook(values);
  eq(load().dt10EarningsMap_(book.ss, '2026-10-09'), { 'BOUNDARY.US': 1 });
  assert.deepStrictEqual(book.calls.ranges, [[1, 1, bodyCap + 1, 7]]);
});

test('Oversized calendar fails safe without silently reading a truncated prefix', () => {
  const values = grid([row('VISIBLE.US', '2026-10-10', 1)]);
  const book = workbook(values, { lastRow: bodyCap + 2 });
  eq(load().dt10EarningsMap_(book.ss, '2026-10-09'), {});
  assert.deepStrictEqual(book.calls.ranges, []);
  assert.strictEqual(book.calls.reads, 0);
});

test('Missing calendar sheet remains safe when no status sheet exists', () => {
  const book = workbook([], { missing: true });
  eq(load().dt10EarningsMap_(book.ss, '2026-10-09'), {});
  assert.deepStrictEqual(book.calls.sheets, ['Calendar_Events']);
  assert.strictEqual(book.calls.reads, 0);
  assert.strictEqual(book.calls.writes, 0);
});

test('Empty used dimensions produce no read and no tags', () => {
  for (const options of [{ lastRow: 0 }, { lastRow: 1 }, { lastColumn: 0 }]) {
    const book = workbook([], options);
    eq(load().dt10EarningsMap_(book.ss, '2026-10-09'), {});
    assert.strictEqual(book.calls.reads, 0);
  }
});

test('Read-service failures cannot break the native annotation reader', () => {
  for (const stage of ['sheet', 'lastRow', 'lastColumn', 'range', 'values']) {
    const book = workbook(grid([row('EVENT.US', '2026-10-10')]), { fail: stage });
    eq(load().dt10EarningsMap_(book.ss, '2026-10-09'), {});
    assert.strictEqual(book.calls.writes, 0);
  }
  eq(load().dt10EarningsMap_(null, '2026-10-09'), {});
});

test('Derived dates flow to annotation horizon without changing verdict or sizing', () => {
  const context = load();
  const values = grid([
    row('DUE.US', '2026-10-22', 99), row('FAR.US', '2026-10-24', 0),
    row('TODAY.US', '2026-10-09', 99), row('UNKNOWN.US', '', 0)
  ]);
  const map = context.dt10EarningsMapFromValues_(values, '2026-10-09');
  const tickets = ['DUE.US', 'FAR.US', 'TODAY.US', 'UNKNOWN.US'].map(symbol =>
    ({ symbol, advisor_note: 'Review', verdict: 'WATCH', suggested_shares: 0 }));
  assert.strictEqual(context.dt10ApplyEarningsTags_(tickets, map, 14), 2);
  assert.ok(tickets[0].advisor_note.startsWith('\u26a0 earnings \u226413d'));
  assert.strictEqual(tickets[1].advisor_note, 'Review');
  assert.ok(tickets[2].advisor_note.startsWith('\u26a0 earnings \u22640d'));
  assert.strictEqual(tickets[3].advisor_note, 'Review');
  assert.strictEqual(context.dt10ApplyEarningsTags_(tickets, map, 14), 0, 'Tags must be idempotent');
  for (const ticket of tickets) {
    assert.strictEqual(ticket.verdict, 'WATCH');
    assert.strictEqual(ticket.suggested_shares, 0);
  }
});

const passed = results.filter(result => result.pass).length;
console.log(passed + '/' + results.length + ' PASS');
console.log('SOURCE_SHA256 ' + crypto.createHash('sha256').update(source).digest('hex'));
console.log('CASES_SHA256 ' + crypto.createHash('sha256').update(JSON.stringify(results)).digest('hex'));
if (passed !== results.length) process.exitCode = 1;
