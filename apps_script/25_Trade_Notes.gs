// TADAWUL FAST BRIDGE - TRADE NOTES + DIVERGENCE LEDGER v1.0.0  [PROGRAM v2 / D-LEDGER]
//
// Purpose
// -------
// Give every fill its 5-line trade note (Program v2, discipline #2) and every
// human-vs-system divergence its register row (the divergence ledger the
// 2026-09-14 governance answer proposed), in ONE append-only tab, written
// from a small input tab so the operator never types into the log directly.
//
// -----------------------------------------------------------------------------
// v1.0.1 (2026-10-07, review fixes on landing the mirror, PR #721)
// WHY: an automated review of the landed v1.0.0 found three ways a row can be
//   wrong without any error, each reproduced in
//   tests/test_gas_trade_notes_v101.js against the v1.0.0 source:
//   (a) Reason Class was only checked for being non-empty. The form dropdown
//       is built with setAllowInvalid(true), so a typo such as JUDGEMENT was
//       logged outside the four declared classes and broke grouping. It is
//       now validated case-insensitively against REASON_CLASSES and written
//       in the canonical spelling.
//   (b) An existing log tab was accepted whenever A1 was non-empty. A tab with
//       a reordered, truncated or unrelated header (for example a wrong
//       TFB_TRADE_NOTES_TAB property) then received rows in the fixed
//       28-column layout, and scoring wrote into a computed Outcome column.
//       Row 1 must now start with TFB_TN_HEADER (extra columns after it are
//       allowed); otherwise setup, log and score return FAILED and write
//       nothing. A non-empty header is still never overwritten.
//   (c) The Note ID scan, allocation and append were not serialized, so two
//       writers on the same trade date could both get the same ID, and
//       scoring then updated only the first match. tbLogTradeNote now holds
//       the script lock across all three; a 30 s lock timeout returns
//       FAILED:lock timeout and appends nothing.
// -----------------------------------------------------------------------------
// v1.0.0 (2026-09-29, One-Pass Script - NEW FILE, no base)
// WHY: Program v2 says "every fill gets a 5-line trade note (thesis, factor
//   evidence, benchmark, exit rule, size rationale) - in the ledger Notes
//   column until the _Trade_Notes tab exists". On 2026-09-28 two fills
//   (AER.US 14 @ 148.18, KRP.US 130 @ 14.68) were made outside the weekly
//   board; on 09-29 both ledger Notes cells are empty and the divergence
//   events D-1/D-2 have no home. A divergence without written numbers is the
//   failure mode; the ledger is what lets authority transfer by evidence.
// WHAT (new file, no existing function/tab touched):
//   tbTradeNotesSetup()          creates _Trade_Notes (header, frozen row)
//                                and _Trade_Notes_Input (label/value form
//                                with dropdowns); idempotent
//   tbLogTradeNoteFromInput()    validates the input form, appends ONE row,
//                                clears the form, stamps _Run_Log, returns
//                                the Note ID (TN-YYYYMMDD-nnn)
//   tbScoreTradeNoteFromInput()  fills Outcome / P&L / Scored At / Scored By
//                                on an existing Note ID (from the form's
//                                SCORE block)
//   tbSeedD1_AER() / tbSeedD2_KRP()  pre-fill the form with the measured
//                                facts of the two 2026-09-28 divergence
//                                fills; the operator writes lines 1 and 5
//   tbTradeNotesSelfTest()       pure-logic self-test (no sheet write)
//   tbLogTradeNote(rec)          programmatic API (same validation)
// RULES: append-only (scoring edits only the four Outcome cells of a row);
//   the 5 note lines are the operator's words - the seeds fill only the
//   measured facts; Note IDs are per-day counters; every write stamps
//   _Run_Log (fail-open); Script Properties TFB_TRADE_NOTES_TAB /
//   TFB_TRADE_NOTES_INPUT_TAB rename the tabs (defaults below).
// ES5 only; no let/const/arrow.
// -----------------------------------------------------------------------------
var TFB_TRADE_NOTES_VERSION = '1.0.1';

var TFB_TRADE_NOTES_ = Object.freeze({
  PROP_TAB: 'TFB_TRADE_NOTES_TAB',
  PROP_INPUT_TAB: 'TFB_TRADE_NOTES_INPUT_TAB',
  DEFAULT_TAB: '_Trade_Notes',
  DEFAULT_INPUT_TAB: '_Trade_Notes_Input',
  TAB_RUN_LOG: '_Run_Log',
  ID_PREFIX: 'TN',
  TYPES: ['FILL', 'DIVERGENCE', 'FILL+DIVERGENCE'],
  SIDES: ['BUY', 'SELL', 'NONE'],
  ORIGINS: ['BOARD', 'OVERRIDE', 'RULE-EXIT', 'STOP', 'TIME-BOX', 'BOARD-EXIT', 'NO-TRADE'],
  REASON_CLASSES: ['DATA DEFECT (P-item)', 'POLICY GAP (F-item)', 'JUDGMENT', 'N/A (board-originated)'],
  OUTCOMES: ['', 'WIN', 'LOSS', 'FLAT', 'OPEN', 'VOID']
});

// Column order of _Trade_Notes (row 1). Keep append-only: new columns go last.
var TFB_TN_HEADER = [
  'Note ID', 'Logged At (Riyadh)', 'Type', 'Trade Date', 'Symbol', 'Side', 'Qty',
  'Price', 'Ccy', 'Venue', 'Board Ref', 'Origin', 'Divergence', 'Reason Class',
  'Reason Ref', 'System Verdict At Time', '1 Thesis', '2 Evidence', '3 Benchmark',
  '4 Exit Rule', '5 Size Rationale', 'Size % NAV', 'Outcome', 'Outcome P&L SAR',
  'Scored At (Riyadh)', 'Scored By', 'Ledger Ref', 'Writer Version'
];

// Input form: [key, label, kind] - kind: text | list:<name> | number | date
var TFB_TN_FORM = [
  ['type', 'Type', 'list:TYPES'],
  ['trade_date', 'Trade Date (YYYY-MM-DD)', 'date'],
  ['symbol', 'Symbol', 'text'],
  ['side', 'Side', 'list:SIDES'],
  ['qty', 'Qty', 'number'],
  ['price', 'Price', 'number'],
  ['ccy', 'Ccy', 'text'],
  ['venue', 'Venue', 'text'],
  ['board_ref', 'Board Ref (run date/time + rank, or none)', 'text'],
  ['origin', 'Origin', 'list:ORIGINS'],
  ['divergence', 'Divergence (YES/NO)', 'text'],
  ['reason_class', 'Reason Class', 'list:REASON_CLASSES'],
  ['reason_ref', 'Reason Ref (P-xxx / F-x / text)', 'text'],
  ['system_verdict', 'System Verdict At Time', 'text'],
  ['thesis', '1 Thesis (one falsifiable sentence)', 'text'],
  ['evidence', '2 Evidence (board ref + factor scores at ticket time)', 'text'],
  ['benchmark', '3 Benchmark (sleeve + blend line)', 'text'],
  ['exit_rule', '4 Exit Rule (stop / time-box / cap / board EXIT)', 'text'],
  ['size_rationale', '5 Size Rationale (% NAV, cash source, why)', 'text'],
  ['size_pct_nav', 'Size % NAV (number)', 'number'],
  ['ledger_ref', 'Ledger Ref (symbol + buy date)', 'text']
];
var TFB_TN_SCORE_FORM = [
  ['score_note_id', 'SCORE: Note ID', 'text'],
  ['score_outcome', 'SCORE: Outcome', 'list:OUTCOMES'],
  ['score_pnl_sar', 'SCORE: Outcome P&L SAR', 'number'],
  ['score_by', 'SCORE: Scored By (rule / board / human)', 'text']
];
var TFB_TN_REQUIRED = ['type', 'trade_date', 'symbol', 'origin', 'thesis', 'evidence', 'benchmark', 'exit_rule', 'size_rationale'];

// ---------------------------------------------------------------------------
// Pure helpers (self-tested)
// ---------------------------------------------------------------------------
function tbTrim_(v) {
  return v === null || v === undefined ? '' : String(v).replace(/^\s+|\s+$/g, '');
}

function tbPad3_(n) {
  var s = String(n);
  while (s.length < 3) { s = '0' + s; }
  return s;
}

function tbNowRiyadh_(d) {
  var t = d ? new Date(d.getTime()) : new Date();
  t = new Date(t.getTime() + 3 * 3600 * 1000);
  var y = t.getUTCFullYear(), m = tbPad2_(t.getUTCMonth() + 1), dd = tbPad2_(t.getUTCDate());
  return y + '-' + m + '-' + dd + ' ' + tbPad2_(t.getUTCHours()) + ':' + tbPad2_(t.getUTCMinutes()) + ':' + tbPad2_(t.getUTCSeconds());
}

function tbPad2_(n) { return (n < 10 ? '0' : '') + String(n); }

// YYYYMMDD of the trade date (falls back to the logged date)
function tbIdDatePart_(tradeDate, nowRiyadh) {
  var m = tbTrim_(tradeDate).match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (m) { return m[1] + m[2] + m[3]; }
  return tbTrim_(nowRiyadh).slice(0, 10).replace(/-/g, '');
}

// Next TN-YYYYMMDD-nnn given the existing IDs in column A
function tbNextNoteId_(existingIds, datePart) {
  var prefix = TFB_TRADE_NOTES_.ID_PREFIX + '-' + datePart + '-';
  var max = 0;
  for (var i = 0; i < existingIds.length; i++) {
    var s = tbTrim_(existingIds[i]);
    if (s.indexOf(prefix) === 0) {
      var n = parseInt(s.slice(prefix.length), 10);
      if (!isNaN(n) && n > max) { max = n; }
    }
  }
  return prefix + tbPad3_(max + 1);
}

function tbIsYes_(v) {
  var s = tbTrim_(v).toUpperCase();
  return s === 'YES' || s === 'Y' || s === 'TRUE' || s === '1';
}

// v1.0.1: the canonical REASON_CLASSES member matching v (case-insensitive), or ''
function tbReasonClass_(v) {
  var s = tbTrim_(v).toUpperCase();
  if (s === '') { return ''; }
  for (var i = 0; i < TFB_TRADE_NOTES_.REASON_CLASSES.length; i++) {
    if (TFB_TRADE_NOTES_.REASON_CLASSES[i].toUpperCase() === s) { return TFB_TRADE_NOTES_.REASON_CLASSES[i]; }
  }
  return '';
}

// v1.0.1: '' when headerRow starts with TFB_TN_HEADER (extra columns after it
// are allowed), else a description of the first mismatching column
function tbHeaderProblem_(headerRow) {
  var h = headerRow || [];
  for (var i = 0; i < TFB_TN_HEADER.length; i++) {
    if (tbTrim_(h[i]) !== TFB_TN_HEADER[i]) {
      return 'column ' + (i + 1) + ' is "' + tbTrim_(h[i]) + '", expected "' + TFB_TN_HEADER[i] + '"';
    }
  }
  return '';
}

// Validation: returns [] when ok, else the list of problems
function tbValidate_(rec) {
  var r = rec || {};
  var errs = [];
  for (var i = 0; i < TFB_TN_REQUIRED.length; i++) {
    if (tbTrim_(r[TFB_TN_REQUIRED[i]]) === '') { errs.push('missing ' + TFB_TN_REQUIRED[i]); }
  }
  if (tbTrim_(r.type) !== '' && TFB_TRADE_NOTES_.TYPES.indexOf(tbTrim_(r.type).toUpperCase()) === -1) { errs.push('type not in ' + TFB_TRADE_NOTES_.TYPES.join('/')); }
  if (tbTrim_(r.side) !== '' && TFB_TRADE_NOTES_.SIDES.indexOf(tbTrim_(r.side).toUpperCase()) === -1) { errs.push('side not in ' + TFB_TRADE_NOTES_.SIDES.join('/')); }
  if (tbTrim_(r.origin) !== '' && TFB_TRADE_NOTES_.ORIGINS.indexOf(tbTrim_(r.origin).toUpperCase()) === -1) { errs.push('origin not in ' + TFB_TRADE_NOTES_.ORIGINS.join('/')); }
  if (tbTrim_(r.reason_class) !== '' && tbReasonClass_(r.reason_class) === '') { errs.push('reason_class not in ' + TFB_TRADE_NOTES_.REASON_CLASSES.join('/')); }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(tbTrim_(r.trade_date))) { errs.push('trade_date must be YYYY-MM-DD'); }
  var t = tbTrim_(r.type).toUpperCase();
  if (t.indexOf('FILL') !== -1) {
    if (!(Number(r.qty) > 0)) { errs.push('qty must be > 0 for a FILL'); }
    if (!(Number(r.price) > 0)) { errs.push('price must be > 0 for a FILL'); }
    if (tbTrim_(r.side) === '' || tbTrim_(r.side).toUpperCase() === 'NONE') { errs.push('side required for a FILL'); }
  }
  var div = tbIsYes_(r.divergence) || t.indexOf('DIVERGENCE') !== -1;
  if (div && tbTrim_(r.reason_class) === '') { errs.push('reason_class required for a divergence'); }
  if (div && tbTrim_(r.system_verdict) === '') { errs.push('system_verdict required for a divergence'); }
  return errs;
}

function tbBuildRow_(rec, noteId, nowRiyadh) {
  var r = rec || {};
  var t = tbTrim_(r.type).toUpperCase();
  var div = (tbIsYes_(r.divergence) || t.indexOf('DIVERGENCE') !== -1) ? 'YES' : 'NO';
  return [
    noteId, nowRiyadh, t, tbTrim_(r.trade_date), tbTrim_(r.symbol).toUpperCase(),
    tbTrim_(r.side).toUpperCase(), r.qty === '' || r.qty === undefined || r.qty === null ? '' : Number(r.qty),
    r.price === '' || r.price === undefined || r.price === null ? '' : Number(r.price),
    tbTrim_(r.ccy).toUpperCase(), tbTrim_(r.venue), tbTrim_(r.board_ref) || 'none',
    tbTrim_(r.origin).toUpperCase(), div, tbReasonClass_(r.reason_class), tbTrim_(r.reason_ref),
    tbTrim_(r.system_verdict), tbTrim_(r.thesis), tbTrim_(r.evidence), tbTrim_(r.benchmark),
    tbTrim_(r.exit_rule), tbTrim_(r.size_rationale),
    r.size_pct_nav === '' || r.size_pct_nav === undefined || r.size_pct_nav === null ? '' : Number(r.size_pct_nav),
    '', '', '', '', tbTrim_(r.ledger_ref), TFB_TRADE_NOTES_VERSION
  ];
}

// ---------------------------------------------------------------------------
// GAS-facing helpers
// ---------------------------------------------------------------------------
function tbProp_(key, dflt) {
  try {
    var v = PropertiesService.getScriptProperties().getProperty(key);
    return v === null || v === undefined || tbTrim_(v) === '' ? dflt : tbTrim_(v);
  } catch (err) { return dflt; }
}

function tbTabName_() { return tbProp_(TFB_TRADE_NOTES_.PROP_TAB, TFB_TRADE_NOTES_.DEFAULT_TAB); }
function tbInputTabName_() { return tbProp_(TFB_TRADE_NOTES_.PROP_INPUT_TAB, TFB_TRADE_NOTES_.DEFAULT_INPUT_TAB); }

function tbSs_() { return SpreadsheetApp.getActiveSpreadsheet(); }

function tbLog_(action, status, message, details) {
  var det = details || {};
  det.version = TFB_TRADE_NOTES_VERSION;
  try {
    var sh = tbSs_().getSheetByName(TFB_TRADE_NOTES_.TAB_RUN_LOG);
    if (sh) {
      sh.appendRow([new Date(), status === 'FAILED' ? 'ERROR' : 'INFO', action, tbTabName_(), status, message, '', '', '', JSON.stringify(det)]);
    }
  } catch (err) { /* fail-open */ }
  try { Logger.log('[TRADE-NOTES v' + TFB_TRADE_NOTES_VERSION + '] ' + action + ' ' + status + ' | ' + message); } catch (e2) { /* noop */ }
}

// Ensure the log tab exists with the header; returns the sheet. Idempotent.
function tbEnsureTab_() {
  var ss = tbSs_();
  var name = tbTabName_();
  var sh = ss.getSheetByName(name);
  if (!sh) {
    sh = ss.insertSheet(name);
    sh.getRange(1, 1, 1, TFB_TN_HEADER.length).setValues([TFB_TN_HEADER]);
    try { sh.setFrozenRows(1); } catch (err) { /* noop */ }
    tbLog_('tbTradeNotesSetup', 'OK', 'created ' + name + ' with ' + TFB_TN_HEADER.length + ' columns', { tab: name });
    return sh;
  }
  // header guard: fill an empty header, never overwrite a non-empty one
  var first = tbTrim_(sh.getRange(1, 1).getValue());
  if (first === '') {
    sh.getRange(1, 1, 1, TFB_TN_HEADER.length).setValues([TFB_TN_HEADER]);
    try { sh.setFrozenRows(1); } catch (err2) { /* noop */ }
  }
  // v1.0.1: refuse a header that does not start with TFB_TN_HEADER; the
  // caller turns the error into FAILED:<reason> and writes nothing
  var problem = tbHeaderProblem_(sh.getRange(1, 1, 1, TFB_TN_HEADER.length).getValues()[0]);
  if (problem !== '') { throw new Error('schema mismatch on ' + name + ': ' + problem); }
  return sh;
}

// v1.0.1: run fn under the script lock and return its value, or
// 'FAILED:lock timeout' when another writer holds the lock for 30 s.
// LockService always exists in Apps Script; the unlocked path is only for an
// environment without it (the offline node harness).
function tbWithLock_(fn) {
  var lock = null;
  try { lock = LockService.getScriptLock(); } catch (err) { lock = null; }
  if (lock && !lock.tryLock(30000)) { return 'FAILED:lock timeout'; }
  try {
    return fn();
  } finally {
    if (lock) { try { lock.releaseLock(); } catch (err2) { /* noop */ } }
  }
}

function tbListFor_(kind) {
  var m = String(kind).match(/^list:(\w+)$/);
  if (!m) { return null; }
  var arr = TFB_TRADE_NOTES_[m[1]];
  return arr ? arr.slice() : null;
}

// Ensure the input form exists (labels in A, values in B, dropdowns). Idempotent.
function tbEnsureInputTab_() {
  var ss = tbSs_();
  var name = tbInputTabName_();
  var sh = ss.getSheetByName(name);
  var rows = TFB_TN_FORM.concat(TFB_TN_SCORE_FORM);
  if (!sh) {
    sh = ss.insertSheet(name);
    var labels = [['TRADE NOTE INPUT - fill column B, then run tbLogTradeNoteFromInput (or tbScoreTradeNoteFromInput for the SCORE block)', '']];
    for (var i = 0; i < rows.length; i++) { labels.push([rows[i][1], '']); }
    sh.getRange(1, 1, labels.length, 2).setValues(labels);
    try { sh.setFrozenRows(1); } catch (err) { /* noop */ }
    for (var j = 0; j < rows.length; j++) {
      var lst = tbListFor_(rows[j][2]);
      if (lst) {
        try {
          var rule = SpreadsheetApp.newDataValidation().requireValueInList(lst, true).setAllowInvalid(true).build();
          sh.getRange(j + 2, 2).setDataValidation(rule);
        } catch (err2) { /* noop */ }
      }
    }
    tbLog_('tbTradeNotesSetup', 'OK', 'created ' + name + ' (' + rows.length + ' fields)', { tab: name });
  }
  return sh;
}

// Read the form -> {key: value} by label order (row i+2)
function tbReadForm_(sh) {
  var rows = TFB_TN_FORM.concat(TFB_TN_SCORE_FORM);
  var vals = sh.getRange(2, 2, rows.length, 1).getValues();
  var out = {};
  for (var i = 0; i < rows.length; i++) {
    var v = vals[i][0];
    if (v instanceof Date) {
      // a date-only cell is midnight Asia/Riyadh = 21:00Z the day before;
      // tbNowRiyadh_ adds the +3h back, so the Riyadh date is returned
      v = tbNowRiyadh_(v).slice(0, 10);
    }
    out[rows[i][0]] = v === null || v === undefined ? '' : v;
  }
  return out;
}

function tbClearForm_(sh, keys) {
  var rows = TFB_TN_FORM.concat(TFB_TN_SCORE_FORM);
  for (var i = 0; i < rows.length; i++) {
    if (!keys || keys.indexOf(rows[i][0]) !== -1) { sh.getRange(i + 2, 2).clearContent(); }
  }
}

function tbWriteForm_(sh, rec) {
  var rows = TFB_TN_FORM.concat(TFB_TN_SCORE_FORM);
  for (var i = 0; i < rows.length; i++) {
    var k = rows[i][0];
    if (rec.hasOwnProperty(k)) { sh.getRange(i + 2, 2).setValue(rec[k]); }
  }
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------
function tbTradeNotesSetup() {
  try {
    tbEnsureTab_();
  } catch (err) {
    tbLog_('tbTradeNotesSetup', 'FAILED', err.message, { tab: tbTabName_() });
    return 'FAILED:' + err.message;
  }
  tbEnsureInputTab_();
  return 'OK:' + tbTabName_() + ',' + tbInputTabName_();
}

// Programmatic API: rec = {type, trade_date, symbol, side, qty, price, ccy,
// venue, board_ref, origin, divergence, reason_class, reason_ref,
// system_verdict, thesis, evidence, benchmark, exit_rule, size_rationale,
// size_pct_nav, ledger_ref}. Returns "OK:<Note ID>" or "FAILED:<reasons>".
function tbLogTradeNote(rec) {
  var errs = tbValidate_(rec);
  if (errs.length) {
    tbLog_('tbLogTradeNote', 'FAILED', 'validation: ' + errs.join('; '), { symbol: tbTrim_(rec && rec.symbol) });
    return 'FAILED:' + errs.join('; ');
  }
  // v1.0.1: ID scan, allocation and append happen under one lock
  var res = tbWithLock_(function () {
    var sh;
    try { sh = tbEnsureTab_(); } catch (err) { return 'FAILED:' + err.message; }
    var now = tbNowRiyadh_();
    var last = sh.getLastRow();
    var ids = last >= 2 ? sh.getRange(2, 1, last - 1, 1).getValues().map(function (r) { return r[0]; }) : [];
    var id = tbNextNoteId_(ids, tbIdDatePart_(rec.trade_date, now));
    var row = tbBuildRow_(rec, id, now);
    sh.appendRow(row);
    tbLog_('tbLogTradeNote', 'OK', id + ' ' + row[4] + ' ' + row[5] + ' ' + row[6] + ' @ ' + row[7] + ' type=' + row[2] + ' divergence=' + row[12] + ' origin=' + row[11],
      { note_id: id, symbol: row[4], type: row[2], divergence: row[12], origin: row[11], reason_class: row[13] });
    return 'OK:' + id;
  });
  if (res.indexOf('FAILED:') === 0) {
    tbLog_('tbLogTradeNote', 'FAILED', res.slice('FAILED:'.length), { symbol: tbTrim_(rec && rec.symbol) });
  }
  return res;
}

function tbLogTradeNoteFromInput() {
  var sh = tbEnsureInputTab_();
  var rec = tbReadForm_(sh);
  var res = tbLogTradeNote(rec);
  if (res.indexOf('OK:') === 0) {
    tbClearForm_(sh, TFB_TN_FORM.map(function (r) { return r[0]; }));
  }
  return res;
}

// Fill the four Outcome cells of one Note ID. Returns "OK:<id>" / "FAILED:...".
function tbScoreTradeNote(noteId, outcome, pnlSar, scoredBy) {
  var id = tbTrim_(noteId);
  var oc = tbTrim_(outcome).toUpperCase();
  if (id === '') { return 'FAILED:missing note id'; }
  if (TFB_TRADE_NOTES_.OUTCOMES.indexOf(oc) === -1 || oc === '') { return 'FAILED:outcome not in ' + TFB_TRADE_NOTES_.OUTCOMES.slice(1).join('/'); }
  var sh;
  try {
    sh = tbEnsureTab_();
  } catch (err) {
    tbLog_('tbScoreTradeNote', 'FAILED', err.message, { note_id: id });
    return 'FAILED:' + err.message;
  }
  var last = sh.getLastRow();
  if (last < 2) { return 'FAILED:no rows'; }
  var ids = sh.getRange(2, 1, last - 1, 1).getValues();
  for (var i = 0; i < ids.length; i++) {
    if (tbTrim_(ids[i][0]) === id) {
      var rowIdx = i + 2;
      var col = TFB_TN_HEADER.indexOf('Outcome') + 1;
      sh.getRange(rowIdx, col, 1, 4).setValues([[oc, pnlSar === '' || pnlSar === undefined || pnlSar === null ? '' : Number(pnlSar), tbNowRiyadh_(), tbTrim_(scoredBy)]]);
      tbLog_('tbScoreTradeNote', 'OK', id + ' outcome=' + oc + ' pnl=' + pnlSar + ' by=' + tbTrim_(scoredBy), { note_id: id, outcome: oc });
      return 'OK:' + id;
    }
  }
  tbLog_('tbScoreTradeNote', 'FAILED', 'note id not found: ' + id, { note_id: id });
  return 'FAILED:note id not found';
}

function tbScoreTradeNoteFromInput() {
  var sh = tbEnsureInputTab_();
  var rec = tbReadForm_(sh);
  var res = tbScoreTradeNote(rec.score_note_id, rec.score_outcome, rec.score_pnl_sar, rec.score_by);
  if (res.indexOf('OK:') === 0) {
    tbClearForm_(sh, TFB_TN_SCORE_FORM.map(function (r) { return r[0]; }));
  }
  return res;
}

// ---------------------------------------------------------------------------
// Seeds for the two 2026-09-28 divergence fills (measured facts only; the
// operator writes line 1 Thesis and line 5 Size Rationale before logging)
// ---------------------------------------------------------------------------
function tbSeedRecords_() {
  return {
    D1: {
      type: 'FILL+DIVERGENCE', trade_date: '2026-09-28', symbol: 'AER.US', side: 'BUY', qty: 14, price: 148.18,
      ccy: 'USD', venue: 'IBKR', board_ref: 'none on 2026-09-28 (board alumni: S-1 challenger 2026-09-23)',
      origin: 'OVERRIDE', divergence: 'YES', reason_class: 'JUDGMENT', reason_ref: 'D-1',
      system_verdict: 'GM 2026-09-29: engine BUY / INVESTABLE overall 78.2, rel 58.1 (both_present_fallback, below add gate 70), 12M +22.4%, intrinsic 205 (upside capped 40%); PF page HOLD (reliability 58.1 < 70); Program v2 weekday = monitoring only, no valid weekly board',
      thesis: '', evidence: 'Fill 13:30:01Z LIMIT DAY at the opening print = day high (prev close 149.21, low 145.21, close 146.51); P/E 7.3, RSI 56, momentum 67, 52W position 69%; commission 2.2885 USD',
      benchmark: 'Global sleeve; S-1 section-1 composite 0.7 SPUS + 0.3 ^TASI.SR (cost-adjusted)',
      exit_rule: 'Stop 141.40 (-3.5% vs 146.51) / TP 165.00 as one OCA (levels placed 2026-09-28 15:35 Riyadh, REPLACED without successor - must be re-placed); weekly board EXIT or cap breach',
      size_rationale: '', size_pct_nav: 8.3, ledger_ref: 'AER.US 2026-09-28'
    },
    D2: {
      type: 'FILL+DIVERGENCE', trade_date: '2026-09-28', symbol: 'KRP.US', side: 'BUY', qty: 130, price: 14.68,
      ccy: 'USD', venue: 'IBKR', board_ref: 'none on 2026-09-28 (board alumni: grace seat 2026-09-18/19)',
      origin: 'OVERRIDE', divergence: 'YES', reason_class: 'JUDGMENT', reason_ref: 'D-2',
      system_verdict: 'GM 2026-09-29: engine HOLD / WATCHLIST overall 63.5 (< 68 gate), rel 54.3, momentum 30, RSI 34, technical bearish; PF valuation leg EXIT (-21.1% to intrinsic 11.41) capped to HOLD by reliability; 12M provider target +32.5%; yield 11%',
      thesis: '', evidence: 'Fill 13:30:00Z LIMIT DAY at the opening print = day high, +0.75% gap above prev close 14.57 (low 14.43, close 14.46); three partial fills, commission 2.975 USD',
      benchmark: 'Global sleeve; S-1 section-1 composite 0.7 SPUS + 0.3 ^TASI.SR (cost-adjusted)',
      exit_rule: 'Stop 13.95 (-3.5% vs 14.46) / TP 16.40 as one OCA (levels placed 2026-09-28 15:37 Riyadh, REPLACED without successor - must be re-placed); weekly board EXIT or cap breach',
      size_rationale: '', size_pct_nav: 7.6, ledger_ref: 'KRP.US 2026-09-28'
    }
  };
}

function tbSeedD1_AER() {
  var sh = tbEnsureInputTab_();
  tbWriteForm_(sh, tbSeedRecords_().D1);
  tbLog_('tbSeedD1_AER', 'OK', 'input form pre-filled for D-1 AER.US; write line 1 Thesis and line 5 Size Rationale, then run tbLogTradeNoteFromInput', {});
  return 'OK:seeded D-1 AER.US (thesis + size rationale still blank)';
}

function tbSeedD2_KRP() {
  var sh = tbEnsureInputTab_();
  tbWriteForm_(sh, tbSeedRecords_().D2);
  tbLog_('tbSeedD2_KRP', 'OK', 'input form pre-filled for D-2 KRP.US; write line 1 Thesis and line 5 Size Rationale, then run tbLogTradeNoteFromInput', {});
  return 'OK:seeded D-2 KRP.US (thesis + size rationale still blank)';
}

// ---------------------------------------------------------------------------
// Self-test (pure; no sheet write)
// ---------------------------------------------------------------------------
function tbTradeNotesSelfTest() {
  var fails = [];
  function ok(c, name) { if (!c) { fails.push(name); } }
  ok(TFB_TN_HEADER.length === 28 && TFB_TN_HEADER[0] === 'Note ID' && TFB_TN_HEADER[27] === 'Writer Version', 'header shape');
  ok(tbNextNoteId_([], '20260929') === 'TN-20260929-001', 'first id');
  ok(tbNextNoteId_(['TN-20260929-001', 'TN-20260928-007', 'junk', 'TN-20260929-003'], '20260929') === 'TN-20260929-004', 'next id per day');
  ok(tbIdDatePart_('2026-09-28', '2026-09-29 10:00:00') === '20260928' && tbIdDatePart_('', '2026-09-29 10:00:00') === '20260929', 'id date part');
  var good = tbSeedRecords_().D1; good.thesis = 'AER re-rates to 9x earnings as lease rates hold'; good.size_rationale = '8.3% NAV from IBKR cash; under the 10% cap';
  ok(tbValidate_(good).length === 0, 'valid seed passes');
  var bad = tbSeedRecords_().D2; // thesis + size rationale blank
  var e1 = tbValidate_(bad);
  ok(e1.length === 2 && e1[0] === 'missing thesis' && e1[1] === 'missing size_rationale', 'seed without lines 1/5 fails exactly twice');
  ok(tbValidate_({}).length >= 9, 'empty record fails all required');
  ok(tbValidate_({ type: 'FILL', trade_date: '2026-09-28', symbol: 'X', origin: 'BOARD', thesis: 't', evidence: 'e', benchmark: 'b', exit_rule: 'x', size_rationale: 's', side: 'BUY', qty: 0, price: 1 }).indexOf('qty must be > 0 for a FILL') !== -1, 'fill needs qty');
  ok(tbValidate_({ type: 'DIVERGENCE', trade_date: '2026-09-28', symbol: 'X', origin: 'NO-TRADE', thesis: 't', evidence: 'e', benchmark: 'b', exit_rule: 'x', size_rationale: 's' }).join(';').indexOf('reason_class required') !== -1, 'divergence needs reason class');
  ok(tbValidate_({ type: 'FILL', trade_date: '28/09/2026', symbol: 'X', origin: 'BOARD', side: 'BUY', qty: 1, price: 1, thesis: 't', evidence: 'e', benchmark: 'b', exit_rule: 'x', size_rationale: 's' }).indexOf('trade_date must be YYYY-MM-DD') !== -1, 'date format');
  var row = tbBuildRow_(good, 'TN-20260928-001', '2026-09-29 13:00:00');
  ok(row.length === 28 && row[0] === 'TN-20260928-001' && row[4] === 'AER.US' && row[6] === 14 && row[7] === 148.18 && row[12] === 'YES' && row[11] === 'OVERRIDE' && row[22] === '' && row[27] === TFB_TRADE_NOTES_VERSION, 'row shape');
  ok(tbBuildRow_({ type: 'FILL', divergence: 'no' }, 'x', 'y')[12] === 'NO' && tbBuildRow_({ type: 'FILL', divergence: 'yes' }, 'x', 'y')[12] === 'YES', 'divergence flag');
  ok(tbIsYes_('Yes') && tbIsYes_('1') && !tbIsYes_('no') && !tbIsYes_(''), 'yes parser');
  ok(tbReasonClass_(' judgment ') === 'JUDGMENT' && tbReasonClass_('JUDGEMENT') === '' && tbReasonClass_('') === '', 'reason class canon');
  ok(tbValidate_({ type: 'DIVERGENCE', trade_date: '2026-09-28', symbol: 'X', origin: 'NO-TRADE', reason_class: 'JUDGEMENT', system_verdict: 'v', thesis: 't', evidence: 'e', benchmark: 'b', exit_rule: 'x', size_rationale: 's' }).join(';').indexOf('reason_class not in') !== -1, 'reason class enum');
  ok(tbHeaderProblem_(TFB_TN_HEADER.concat(['Extra'])) === '' && tbHeaderProblem_(['Note ID']).indexOf('column 2') === 0 && tbHeaderProblem_(['Type', 'Note ID']).indexOf('column 1') === 0, 'header check');
  ok(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(tbNowRiyadh_(new Date(Date.UTC(2026, 8, 29, 10, 0, 0)))) && tbNowRiyadh_(new Date(Date.UTC(2026, 8, 29, 10, 0, 0))) === '2026-09-29 13:00:00', 'riyadh clock');
  var verdict = fails.length === 0 ? 'trade notes core: ok' : 'trade notes core: FAIL ' + fails.length + ' [' + fails.join('; ') + ']';
  try { Logger.log('[TRADE-NOTES v' + TFB_TRADE_NOTES_VERSION + '] selftest -> ' + verdict); } catch (err) { /* noop */ }
  return verdict;
}
