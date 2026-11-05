#!/usr/bin/env node
/* tests/test_dt10_v1211_mf_scope.js
 * Harness for apps_script/16_Decision_Top10.gs v1.21.1 [MF-SCOPE-2422] (Thu 5
 * Nov 2026): the approved Mutual_Funds scope drops 52 dead fund rows (owner
 * approval 12:51 Riyadh). Policy data only: DT10_SOURCE_APPROVED_SCOPE
 * (revision 2026-11-05.mf-retire-52.v2, fund count 2422 and its symbol-set
 * digest) and DT10_SOURCE_MIN_ROWS.Mutual_Funds.
 *
 * Loads the delivered file and the v1.21.0 BASE -- a file given as the second
 * argument (it must carry the pinned SHA-256 of origin/main 7fb3f77f) or, by
 * default, the base rebuilt from the delivered file by reversing exactly the
 * four hunks of tests/js/dt10_edits_v1211.json -- into separate vm contexts.
 *   K0 base oracle: the hunks reverse to the pinned v1.21.0 file; both compile.
 *   K1 inventory: exactly six base lines replaced (the version line,
 *      DT10_VERSION, the fund floor, the revision line, the fund scope's two
 *      lines); the only other added lines are the WHY block; ASCII (the
 *      version line keeps its em dash); no function or var added or removed;
 *      every prior WHY header kept; LF only.
 *   K2 manifest binding: the native scope equals
 *      config/approved_universe_scope_v1.json; a scoped page's floor is its
 *      approved count; the Global_Markets / Commodities_FX floors unchanged.
 *   K3 the 52: unique, ticker-shaped, none in the new manifest; new + 52 = the
 *      v1.21.0 manifest (membership digest pinned); the commit sheet's
 *      _Retired_Symbols block lists exactly them, Page = Mutual_Funds.
 *   K4 REAL behaviour: policy floors; no property moves the fund floor; the
 *      scope core accepts an approved 2422 set and refuses the un-retired 2474
 *      page, a 2421 page, a duplicate and a non-approved membership; the
 *      readiness core names 2422.
 *   K5 GOLDEN NEGATIVE: the delivered file given the v1.21.0 scope data
 *      behaves exactly like the v1.21.0 file (policy, scope core, readiness
 *      core, dt10SelfTest); with its own data it differs only in the count.
 *   K6 mutants: twelve one-line mutations of the delivered file are each
 *      caught by K1 or K2.
 * The battery runs twice; the digests must be equal. A test fixture, never
 * runtime code. */
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const vm = require('vm');
const ROOT = path.join(__dirname, '..');
const history = require(path.join(ROOT, 'tests', 'js', 'dt10_cash_utc_history.js'));
const NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '16_Decision_Top10.gs');
const BASE_PATH = process.argv[3] || '';
const newSource = fs.readFileSync(NEW_PATH, 'utf8');
const sha256 = s => crypto.createHash('sha256').update(s, 'utf8').digest('hex');
let baseSource;
try {
  baseSource = BASE_PATH ? fs.readFileSync(BASE_PATH, 'utf8') : history.v1210Source(newSource);
} catch (eBase) {
  console.log('FAIL K0 base oracle: ' + String(eBase && eBase.message || eBase).split('\n')[0]);
  console.log('FAIL 0/1');
  process.exit(1);
}
const MANIFEST = JSON.parse(fs.readFileSync(path.join(ROOT, 'config', 'approved_universe_scope_v1.json'), 'utf8'));
const SHEET = fs.readFileSync(path.join(ROOT, 'docs', 'evidence',
  'TFB_Commit_Sheet_approved_scope_v1.0.1_mf_retire_52_2026-11-05.md'), 'utf8');
const EDITS = require(path.join(ROOT, 'tests', 'js', 'dt10_edits_v1211.json'));
/* The 52 rows, exact page spellings, page order (the 5 Nov evidence: 45 chart
 * API 404s, 5 with no bar in two years, SPLG.US renamed SPYM, bare SPLG). */
const RETIRED = ('FM.US USRD.US GXTG.US IBMM.US IBDD.US HEWG.US IBMN.US MIDF.US SPVU.US IBDQ.US VFLQ.US HEWW.US ' +
  'IBCE.US SPLG.US IBTE.US ICOL.US KLD.US IBMK.US IBHA.US IRBO.US DEFA.US HEWC.US IAUF.US KRMA.US ' +
  'IBTD.US HYXU.US IBTF.US IBHC.US JKJ.US CZAR.US IBMJ.US IBML.US GXG.US IBHB.US NUSI.US IECS.US ' +
  'IBDM.US SPLG IBTB.US EMBH.US HJPX.US IGN.US IBDN.US JKK.US JKH.US IBDP.US HEWU.US IBHD.US ' +
  'EWGS.US JKE.US FLFR.US HWAY.US').split(' ');
const OLD = {revision: '2026-10-12.current-workbook.v1', approved_on: '2026-10-12', count: 2474,
  membership: '10b7327f802d7c346708c347a4d40945bdc28db2daf385c5456c8dc28366e151',
  symbolSet: 'd9a3fcb8aefc9de60de28c1e4d2c89464be2dbaf0907e11edd904bd48214bc02'};
const NEW = {revision: '2026-11-05.mf-retire-52.v2', approved_on: '2026-11-05', count: 2422,
  membership: '74347d41ddce6b26afdbaa8241c8a72f9c722991b8996de6eab3307a8676850e',
  symbolSet: '1bb8747656e2a4c635863c933c41f51cae530892dd5c08c3d0a132a3bf6926ff'};
/* scripts/approved_universe_scope.py: symbol_fingerprint / membership_sha256 / symbol_set_sha256 */
const fingerprint = s => sha256('tfb.approved-symbol/v1\u0000' + s.trim().toUpperCase());
const membership = fps => sha256(fps.slice().sort().join('\n') + '\n');
const symbolSet = syms => sha256(syms.map(s => s.trim().toUpperCase()).sort().join('\n') + '\n');
const NOW = Date.parse('2026-10-12T09:30:00Z');
const STAMP = '2026-10-12 12:00:00+03:00';

/* ------------------------------------------------------------ runner */
let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n  ')); }
}
function ok(c, m) { if (!c) throw new Error(m || 'assertion failed'); }
function eq(a, b, m) { if (a !== b) throw new Error((m || 'not equal') + ': ' + JSON.stringify(a) + ' !== ' + JSON.stringify(b)); }
function deq(a, b, m) {
  const ja = JSON.stringify(a), jb = JSON.stringify(b);
  if (ja !== jb) {
    let i = 0; while (i < ja.length && ja[i] === jb[i]) i++;
    throw new Error((m || 'deep-equal') + ' differs at ' + i + ': ...' + ja.slice(Math.max(0, i - 80), i + 120) + ' | ...' + jb.slice(Math.max(0, i - 80), i + 120));
  }
}
const plain = v => v === undefined ? undefined : JSON.parse(JSON.stringify(v));
const vNorm = x => JSON.parse(JSON.stringify(x).replace(/1\.21\.1/g, '1.21.0'));

/* ------------------------------------------------------------ GAS sandbox (the E9 mini of the v1.21.0 harness) */
function load(src, props, opts) {
  opts = opts || {};
  const P = Object.assign({}, props || {}), reads = [];
  const statusRows = []; for (let r = 0; r < 60; r++) statusRows.push(['', '']);
  const ss = { getSheetByName(n) { return n === '_Status' ? { getRange() { return { getValues() { return plain(statusRows); } }; } } : null; },
    getRangeByName() { return null; } };
  const ctx = {
    Logger: { log() {} },
    SpreadsheetApp: { getActiveSpreadsheet() { return ss; }, getUi() { throw new Error('no ui'); } },
    PropertiesService: { getScriptProperties() {
      if (opts.propsThrow) throw new Error('down');
      return { getProperty(k) { reads.push(k); return Object.prototype.hasOwnProperty.call(P, k) ? P[k] : null; },
        setProperty(k, v) { P[k] = String(v); }, deleteProperty(k) { delete P[k]; } };
    } },
    Utilities: { DigestAlgorithm: { SHA_256: 'SHA_256' }, Charset: { UTF_8: 'UTF_8' },
      computeDigest(a, v) { return [...crypto.createHash('sha256').update(String(v), 'utf8').digest()].map(n => n > 127 ? n - 256 : n); },
      formatDate() { return '2026-11-05 00:00:00'; } },
    Session: { getScriptTimeZone() { return 'Asia/Riyadh'; } },
    UrlFetchApp: { fetch() { throw new Error('no net'); } }
  };
  vm.createContext(ctx); vm.runInContext(src, ctx, { filename: '16_Decision_Top10.gs' });
  ctx.__reads = reads;
  return ctx;
}
/* The v1.21.0 scope data written into a delivered context (primitives only). */
function inject(ctx) {
  const o = load(baseSource), S = ctx.DT10_SOURCE_APPROVED_SCOPE, O = plain(o.DT10_SOURCE_APPROVED_SCOPE), F = plain(o.DT10_SOURCE_MIN_ROWS);
  S.policy_revision = O.policy_revision; S.approved_on = O.approved_on;
  for (const page of Object.keys(O.pages)) for (const k of Object.keys(O.pages[page])) S.pages[page][k] = O.pages[page][k];
  for (const k of Object.keys(F)) ctx.DT10_SOURCE_MIN_ROWS[k] = F[k];
  return ctx;
}

/* ------------------------------------------------------------ K1 / K2 as pure checks (the K6 mutants reuse them) */
const REPLACED_BASE = [
  '* Version: 1.21.0 (see DT10_VERSION; header kept in lockstep — restored', "var DT10_VERSION = '1.21.0';",
  'Commodities_FX: 453, Mutual_Funds: 2474};', "policy_revision: '2026-10-12.current-workbook.v1', approved_on: '2026-10-12',",
  'Mutual_Funds: {approved_count: 2474, supersedes_min_rows: 4496,', "symbol_set_sha256: '" + OLD.symbolSet + "'}"].sort();
const REPLACED_NEW = [
  '* Version: 1.21.1 (see DT10_VERSION; header kept in lockstep — restored', "var DT10_VERSION = '1.21.1';",
  'Commodities_FX: 453, Mutual_Funds: 2422};', "policy_revision: '" + NEW.revision + "', approved_on: '" + NEW.approved_on + "',",
  'Mutual_Funds: {approved_count: 2422, supersedes_min_rows: 4496,', "symbol_set_sha256: '" + NEW.symbolSet + "'}"].sort();
const WHY_HEAD = ' * v1.21.1 (2026-11-05) [MF-SCOPE-2422]', WHY_NEXT = ' * v1.21.0 (2026-11-03) [EXDIV-GUARD]';
const names = (src, re) => { const out = {}; let m; while ((m = re.exec(src))) out[m[1]] = true; return Object.keys(out).sort(); };
function nonAscii(s) { const m = {}; for (let j = 0; j < s.length; j++) { const c = s.charCodeAt(j); if (c > 126) m[c] = (m[c] || 0) + 1; } return m; }
function inventory(src) {
  const errs = [];
  try { new vm.Script(src, { filename: 'new.gs' }); } catch (e) { errs.push('compile: ' + e.message); }
  if (src.indexOf('\r') >= 0) errs.push('CR');
  try { if (sha256(history.v1210Source(src)) !== history.V1210_SHA256) errs.push('reversal SHA'); }
  catch (e) { errs.push('reversal: ' + String(e.message).split('\n')[0]); }
  const bl = baseSource.split('\n'), nl = src.split('\n'), bset = new Set(bl), nset = new Set(nl);
  const removed = bl.filter(l => !nset.has(l)).map(l => l.trim()).sort();
  if (JSON.stringify(removed) !== JSON.stringify(REPLACED_BASE)) errs.push('replaced base lines ' + JSON.stringify(removed));
  const w0 = src.indexOf(WHY_HEAD), w1 = src.indexOf(WHY_NEXT);
  if (!(w0 > 0 && w1 > w0 && src.indexOf('\n * ====') < w0)) errs.push('WHY block position');
  const why = w0 > 0 && w1 > w0 ? src.slice(w0, w1) : '';
  if (!/^[\x0a\x20-\x7e]*$/.test(why)) errs.push('WHY not ASCII');
  for (const sect of [' * WHY (owner, Thu 5 Nov 2026 12:51 Riyadh', ' * WHAT: ', ' * GATES: ', ' * ROLLBACK: ',
    ' * Harness: tests/test_dt10_v1211_mf_scope.js']) if (why.indexOf(sect) < 0) errs.push('WHY section ' + sect);
  const whyLines = new Set(why.split('\n'));
  const added = nl.filter(l => !bset.has(l) && !whyLines.has(l)).map(l => l.trim()).sort();
  if (JSON.stringify(added) !== JSON.stringify(REPLACED_NEW)) errs.push('added code lines ' + JSON.stringify(added));
  for (const l of nl.filter(x => !bset.has(x))) {
    if (!/^[\x09\x20-\x7e]*$/.test(l) && !bset.has(l.replace('1.21.1', '1.21.0'))) errs.push('non-ASCII added line ' + l);
  }
  if (JSON.stringify(nonAscii(src)) !== JSON.stringify(nonAscii(baseSource))) errs.push('non-ASCII multiset');
  if (JSON.stringify(names(src, /function\s+([A-Za-z_$][\w$]*)\s*\(/g)) !== JSON.stringify(names(baseSource, /function\s+([A-Za-z_$][\w$]*)\s*\(/g))) errs.push('function census');
  if (JSON.stringify(names(src, /^var\s+([A-Za-z_$][\w$]*)/mg)) !== JSON.stringify(names(baseSource, /^var\s+([A-Za-z_$][\w$]*)/mg))) errs.push('var census');
  for (const l of bl.filter(x => /^ \* v\d+\.\d+\.\d+ \(/.test(x))) if (src.indexOf(l) < 0) errs.push('WHY header lost ' + l);
  return errs;
}
function binding(src) {
  const errs = [];
  let c; try { c = load(src); } catch (e) { return ['load: ' + e.message]; }
  const S = plain(c.DT10_SOURCE_APPROVED_SCOPE), F = plain(c.DT10_SOURCE_MIN_ROWS);
  if (S.policy_revision !== MANIFEST.policy_revision) errs.push('policy_revision ' + S.policy_revision);
  if (S.approved_on !== MANIFEST.approved_on) errs.push('approved_on ' + S.approved_on);
  if (JSON.stringify(Object.keys(S.pages).sort()) !== JSON.stringify(Object.keys(MANIFEST.pages).sort())) errs.push('scoped pages');
  for (const page of Object.keys(MANIFEST.pages)) {
    const d = S.pages[page] || {}, a = MANIFEST.pages[page];
    for (const f of ['approved_count', 'supersedes_min_rows', 'symbol_set_sha256']) if (d[f] !== a[f]) errs.push(page + ' ' + f + ' ' + d[f]);
    if (JSON.stringify(Object.keys(d).sort()) !== '["approved_count","supersedes_min_rows","symbol_set_sha256"]') errs.push(page + ' fields');
    if (F[page] !== a.approved_count) errs.push(page + ' floor ' + F[page] + ' is not its approved count');
  }
  if (JSON.stringify(Object.keys(F).sort()) !== '["Commodities_FX","Global_Markets","Market_Leaders","Mutual_Funds"]') errs.push('floor pages');
  if (F.Global_Markets !== 6512 || F.Commodities_FX !== 453) errs.push('unscoped floors ' + JSON.stringify(F));
  if (c.DT10_VERSION !== '1.21.1') errs.push('DT10_VERSION ' + c.DT10_VERSION);
  if (!/^ \* Version: 1\.21\.1 \(see DT10_VERSION;/.test(src.split('\n')[3])) errs.push('header line 4');
  if ((src.match(/^var DT10_VERSION = '1\.21\.1';$/mg) || []).length !== 1) errs.push('DT10_VERSION literal');
  return errs;
}

/* ------------------------------------------------------------ fixtures */
const SYN = Array.from({ length: 2422 }, (_, i) => 'SYNTHFUND' + String(i).padStart(4, '0') + '.US');
const HEADER = ['Page', 'Last Updated', 'Status', 'Message', 'Endpoint', 'HTTP Code', 'Rows', 'Columns', 'Duration ms', 'Warnings'];
function grid(mf) {
  const counts = { Market_Leaders: 255, Global_Markets: 6512, Commodities_FX: 453, Mutual_Funds: mf };
  const rows = [HEADER];
  for (const [page, n] of Object.entries(counts)) {
    if (n === null) continue;
    rows.push([page, STAMP, 'SUCCESS', 'acquired=' + n + '/' + n + ' acquisition=COMPLETE fetchfail_effective=enforce | data=COMPLETE run=90001',
      'synthetic-source', 200, n, 115, 1, 0]);
  }
  return rows;
}
function pasteRows(doc) {
  const a = doc.indexOf('<!-- retired-symbols-paste:begin -->'), b = doc.indexOf('<!-- retired-symbols-paste:end -->');
  ok(a > 0 && b > a, 'paste block markers');
  return doc.slice(a, b).split('\n').slice(1).filter(l => l && !/^```/.test(l)).map(l => l.split('\t'));
}
/* One context per call: dt10SourcePolicy_ memoizes per execution. */
function behaviour(mk) {
  const out = {};
  for (const props of [{}, { TFB_EXPECTED_MIN_ROWS_MUTUAL_FUNDS: '2474' }, { TFB_EXPECTED_MIN_ROWS_MUTUAL_FUNDS: '4496' },
    { TFB_EXPECTED_MIN_ROWS_MUTUAL_FUNDS: '1' }, { TFB_EXPECTED_MIN_ROWS_GLOBAL_MARKETS: '6600', TFB_EXPECTED_MIN_ROWS_COMMODITIES_FX: '1' }]) {
    out['policy ' + JSON.stringify(props)] = plain(mk(props).dt10SourcePolicy_());
  }
  const c = mk({}), approval = c.DT10_SOURCE_APPROVED_SCOPE, native = plain(approval.pages.Mutual_Funds);
  const lists = { approved: SYN, unretired: SYN.concat(RETIRED), short: SYN.slice(1), dup: SYN.slice(1).concat([SYN[1]]),
    v1Size: SYN.concat(RETIRED).slice(0, 2474) };
  for (const [name, list] of Object.entries(lists)) {
    for (const specName of ['bound', 'native']) {
      const spec = specName === 'native' ? native : Object.assign({}, native, { symbol_set_sha256: symbolSet(list.length === native.approved_count ? list : SYN) });
      out['scope ' + name + ' ' + specName] = plain(c.dt10SourceScopeCore_(list, spec, approval));
    }
  }
  const ML = Array.from({ length: 255 }, (_, i) => String(900001 + i) + '.SR');
  for (const mf of [2474, 2422, 2421, 0, null]) {
    const k = mk({}), ap = k.DT10_SOURCE_APPROVED_SCOPE;
    // synthetic rosters stand in for the approved memberships (the readiness harness does the same)
    ap.pages.Market_Leaders.symbol_set_sha256 = symbolSet(ML); ap.pages.Mutual_Funds.symbol_set_sha256 = symbolSet(SYN);
    const policy = k.dt10SourcePolicy_();
    const mfList = mf === 2474 ? SYN.concat(RETIRED) : mf === 2421 ? SYN.slice(1) : mf === 0 ? [] : SYN;
    const receipts = {
      Market_Leaders: plain(k.dt10SourceScopeCore_(ML, ap.pages.Market_Leaders, ap)),
      Mutual_Funds: plain(k.dt10SourceScopeCore_(mfList, ap.pages.Mutual_Funds, ap))
    };
    out['readiness MF ' + mf] = plain(k.dt10SourceReadinessCore_(grid(mf), NOW, null, policy, receipts));
  }
  out.selfTest = String(mk({}).dt10SelfTest());
  return out;
}

/* ------------------------------------------------------------ battery */
function battery() {
  test('K0 base oracle: the four hunks of dt10_edits_v1211.json reverse to the reviewed v1.21.0 file; both files compile', () => {
    eq(EDITS.length, 4, 'four hunks');
    for (const [cur] of EDITS) eq(newSource.split(cur).length - 1, 1, 'each current is unique: ' + cur.split('\n')[0]);
    eq(sha256(baseSource), history.V1210_SHA256, 'base SHA-256 (origin/main 7fb3f77f)');
    eq(sha256(history.v1210Source(newSource)), history.V1210_SHA256, 'reversal of the delivered file');
    new vm.Script(newSource, { filename: 'new.gs' }); new vm.Script(baseSource, { filename: 'base.gs' });
    ok(/^var DT10_VERSION = '1\.21\.0';$/m.test(baseSource), 'the base is v1.21.0');
  });

  test('K1 inventory: six base lines replaced, the WHY block added, nothing else; ASCII; no function / var added or removed; prior WHY headers kept; LF only', () => {
    deq(inventory(newSource), [], 'inventory');
    const why = newSource.slice(newSource.indexOf(WHY_HEAD), newSource.indexOf(WHY_NEXT));
    ok(why.length > 1500 && why.length < 4000, 'WHY block size ' + why.length);
    ok(why.indexOf('2026-11-05.mf-retire-52.v2') > 0 && why.indexOf('41944918372') > 0 && why.indexOf('_Retired_Symbols') > 0, 'WHY names the revision, the run and the seam');
  });

  test('K2 manifest binding: native scope = config/approved_universe_scope_v1.json (revision, date, counts, digests); a scoped page floor = its approved count; the other floors unchanged', () => {
    deq(binding(newSource), [], 'binding');
    eq(MANIFEST.policy_revision, NEW.revision, 'manifest revision'); eq(MANIFEST.approved_on, NEW.approved_on, 'manifest date');
    eq(MANIFEST.pages.Mutual_Funds.approved_count, NEW.count); eq(MANIFEST.pages.Mutual_Funds.symbol_set_sha256, NEW.symbolSet);
    ok(MANIFEST.approval_basis.indexOf('Approved to be removed') > 0 && MANIFEST.approval_basis.indexOf('TFB_Commit_Sheet_approved_scope_v1.0.1_mf_retire_52_2026-11-05.md') > 0,
      'the approval basis quotes the owner and names this commit sheet');
    const b = load(baseSource), n = load(newSource);
    deq(plain(n.DT10_SOURCE_APPROVED_SCOPE.pages.Market_Leaders), plain(b.DT10_SOURCE_APPROVED_SCOPE.pages.Market_Leaders), 'Market_Leaders untouched');
    deq(Object.assign(plain(b.DT10_SOURCE_MIN_ROWS), { Mutual_Funds: 2422 }), plain(n.DT10_SOURCE_MIN_ROWS), 'only the fund floor moves');
    eq(b.DT10_SOURCE_MIN_ROWS.Mutual_Funds, 2474, 'base fund floor');
  });

  test('K3 the 52: unique tickers, none in the new scope; the new scope + the 52 = the v1.21.0 membership; the commit sheet paste block = the 52 with Page Mutual_Funds', () => {
    eq(RETIRED.length, 52); eq(new Set(RETIRED).size, 52, 'unique spellings');
    for (const s of RETIRED) ok(/^[A-Z0-9.\-=^&/]{1,24}$/.test(s), 'ticker shape ' + s);
    ok(RETIRED.indexOf('SPLG') >= 0 && RETIRED.indexOf('SPLG.US') >= 0, 'both SPLG rows');
    const mf = MANIFEST.pages.Mutual_Funds, fps = mf.symbol_fingerprints_sha256;
    eq(fps.length, 2422, 'fingerprints'); eq(mf.scope, 'current_fund_tab'); eq(mf.supersedes_min_rows, 4496);
    ok(fps.every((f, i) => /^[0-9a-f]{64}$/.test(f) && (i === 0 || fps[i - 1] < f)), 'sorted, unique, lowercase SHA-256');
    eq(membership(fps), mf.membership_sha256, 'membership digest recomputes'); eq(mf.membership_sha256, NEW.membership, 'pinned v2 digest');
    const dead = RETIRED.map(fingerprint), have = new Set(fps);
    deq(dead.filter(f => have.has(f)), [], 'no retired name in the new scope');
    eq(new Set(dead).size, 52, 'distinct fingerprints');
    eq(membership(fps.concat(dead)), OLD.membership, 'the new scope + the 52 = the v1.21.0 manifest membership');
    ok(baseSource.indexOf("symbol_set_sha256: '" + OLD.symbolSet + "'") > 0, 'the v1.21.0 file pinned the v1 symbol-set digest');
    const rows = pasteRows(SHEET);
    eq(rows.length, 52, 'paste rows'); deq(rows.map(r => r[0]), RETIRED, 'paste block symbols, page order');
    for (const r of rows) {
      eq(r.length, 5, 'five columns ' + r[0]); eq(r[1], '2026-11-05', 'Retired On ' + r[0]); eq(r[4], 'Mutual_Funds', 'Page ' + r[0]);
      ok(r[2].length > 10 && /^[\x20-\x7e]+$/.test(r[2]), 'Reason ' + r[0]);
      eq(r[3], r[0] === 'SPLG' || r[0] === 'SPLG.US' ? 'SPYM.US' : '', 'Replacement ' + r[0]);
    }
    for (const s of RETIRED) ok(SHEET.indexOf('| `' + s + '` |') > 0, 'evidence row ' + s);
  });

  test('K4 REAL behaviour: floors 255/6512/453/2422; no property moves the fund floor; the scope core accepts 2422 and refuses the un-retired page, 2421, a duplicate and a non-approved set; the readiness core names 2422', () => {
    const b = behaviour(p => load(newSource, p));
    deq(b['policy {}'].floors, { Market_Leaders: 255, Global_Markets: 6512, Commodities_FX: 453, Mutual_Funds: 2422 }, 'floors');
    eq(b['policy {}'].approved_scope.policy_revision, NEW.revision);
    for (const v of ['2474', '4496', '1']) {
      const p = b['policy ' + JSON.stringify({ TFB_EXPECTED_MIN_ROWS_MUTUAL_FUNDS: v })];
      eq(p.floors.Mutual_Funds, 2422, 'fund floor with the property ' + v); eq(p.superseded_properties.Mutual_Funds, Number(v), 'recorded as superseded ' + v);
    }
    const t = b['policy ' + JSON.stringify({ TFB_EXPECTED_MIN_ROWS_GLOBAL_MARKETS: '6600', TFB_EXPECTED_MIN_ROWS_COMMODITIES_FX: '1' })];
    eq(t.floors.Global_Markets, 6600, 'an unscoped floor can still be tightened'); eq(t.floors.Commodities_FX, 453, 'never lowered');
    const a = b['scope approved bound'];
    ok(a.ready === true && a.actual_count === 2422 && a.actual_unique_count === 2422 && a.expected_count === 2422 && a.policy_revision === NEW.revision, 'approved set: ' + a.reason);
    eq(a.observed_symbol_set_sha256, symbolSet(SYN), 'observed digest');
    const u = b['scope unretired bound'];
    ok(u.ready === false && u.reason.indexOf('physical count 2474 differs from approved 2422') >= 0, 'un-retired page: ' + u.reason);
    ok(b['scope short bound'].reason.indexOf('physical count 2421 differs from approved 2422') >= 0, '2421: ' + b['scope short bound'].reason);
    ok(b['scope dup bound'].ready === false && b['scope dup bound'].reason.indexOf('duplicate Symbol rows 1') >= 0, 'duplicate: ' + b['scope dup bound'].reason);
    const nat = b['scope approved native'];
    ok(nat.ready === false && nat.reason.indexOf('exact approved membership mismatch') >= 0, 'a 2422 set that is not the approved membership: ' + nat.reason);
    eq(nat.expected_symbol_set_sha256, NEW.symbolSet, 'the native digest is enforced');
    const r74 = JSON.stringify(b['readiness MF 2474']), r22 = JSON.stringify(b['readiness MF 2422']);
    ok(r74.indexOf('Mutual_Funds status count differs from approved scope 2422') >= 0, 'a 2474 status row is refused: ' + b['readiness MF 2474'].reason);
    ok(r22.indexOf('Mutual_Funds status count differs') < 0 && r22.indexOf('Mutual_Funds approved scope unverified') < 0 &&
       r22.indexOf('Mutual_Funds rows') < 0, 'a 2422 status row with its receipt raises no fund issue: ' + b['readiness MF 2422'].reason);
    eq(b['readiness MF 2422'].ready, true, 'the retired 2422 page with its receipt is VERIFIED: ' + b['readiness MF 2422'].reason);
    ok(JSON.stringify(b['readiness MF 0']).indexOf('Mutual_Funds rows 0 below floor 2422') >= 0, 'empty page: below floor 2422');
    ok(JSON.stringify(b['readiness MF null']).indexOf('Mutual_Funds source status missing') >= 0, 'missing status row');
  });

  test('K5 GOLDEN NEGATIVE: given the v1.21.0 scope data the delivered file = the v1.21.0 file (policy, scope core, readiness core, dt10SelfTest); with its own data only the count / revision / digest differ', () => {
    const base = behaviour(p => load(baseSource, p)), injected = behaviour(p => inject(load(newSource, p)));
    for (const k of Object.keys(base)) deq(vNorm(injected[k]), base[k], 'identical ' + k);
    const own = behaviour(p => load(newSource, p));
    eq(vNorm(own.selfTest), base.selfTest, 'dt10SelfTest: the v1.21.0 report (version normalized)');
    ok(own.selfTest.indexOf('[DT10 v1.21.1] self-test') === 0 && own.selfTest.indexOf('FAIL') < 0, 'self-test version line, no FAIL');
    const swap = x => JSON.parse(JSON.stringify(x).split(NEW.revision).join(OLD.revision).split(NEW.approved_on).join(OLD.approved_on)
      .split(NEW.symbolSet).join(OLD.symbolSet).replace(/approved 2422\b/g, 'approved 2474').replace(/scope 2422\b/g, 'scope 2474')
      .replace(/floor 2422\b/g, 'floor 2474').replace(/"approved_count":2422/g, '"approved_count":2474').replace(/"Mutual_Funds":2422/g, '"Mutual_Funds":2474'));
    // the policy objects and the self-test differ only by the data; the scope / readiness receipts depend on the
    // list sizes against the count, so they are pinned explicitly (K4 and below)
    for (const k of Object.keys(base).filter(x => /^policy /.test(x) || x === 'selfTest' || x === 'readiness MF null')) {
      deq(swap(vNorm(own[k])), base[k], 'only count / revision / digest differ ' + k);
    }
    eq(base['scope v1Size bound'].ready, true, 'v1.21.0 accepted a 2474 page');
    ok(base['readiness MF 2422'].ready === false && base['readiness MF 2422'].reason.indexOf('Mutual_Funds rows 2422 below floor 2474') >= 0,
      'v1.21.0 withholds the retired 2422 page: ' + base['readiness MF 2422'].reason);
    eq(own['readiness MF 2422'].ready, true, 'v1.21.1 verifies it');
    eq(own['scope v1Size bound'].ready, false, 'v1.21.1 refuses the same 2474 page');
  });

  test('K6 mutants: each one-line mutation of the delivered file is caught by the inventory or the binding', () => {
    const MUT = [
      ['fund count 2421', 'Mutual_Funds: {approved_count: 2422,', 'Mutual_Funds: {approved_count: 2421,'],
      ['fund digest', "'" + NEW.symbolSet + "'", "'" + NEW.symbolSet.slice(0, -1) + "0'"],
      ['revision left at v1', "policy_revision: '" + NEW.revision + "'", "policy_revision: '" + OLD.revision + "'"],
      ['approval date', "approved_on: '2026-11-05',", "approved_on: '2026-10-12',"],
      ['fund floor not following', 'Commodities_FX: 453, Mutual_Funds: 2422};', 'Commodities_FX: 453, Mutual_Funds: 2474};'],
      ['Global_Markets floor', 'var DT10_SOURCE_MIN_ROWS = {Market_Leaders: 255, Global_Markets: 6512,', 'var DT10_SOURCE_MIN_ROWS = {Market_Leaders: 255, Global_Markets: 6500,'],
      ['Market_Leaders count', 'Market_Leaders: {approved_count: 255,', 'Market_Leaders: {approved_count: 256,'],
      ['fund supersedes floor', 'Mutual_Funds: {approved_count: 2422, supersedes_min_rows: 4496,', 'Mutual_Funds: {approved_count: 2422, supersedes_min_rows: 4497,'],
      ['header version left behind', ' * Version: 1.21.1 (see', ' * Version: 1.21.0 (see'],
      ['DT10_VERSION left behind', "var DT10_VERSION = '1.21.1';", "var DT10_VERSION = '1.21.0';"],
      ['an unrelated code edit', 'var policy = {floors: {}, freshMin: 95,', 'var policy = {floors: {}, freshMin: 94,'],
      ['the WHY block dropped', newSource.slice(newSource.indexOf(WHY_HEAD), newSource.indexOf(WHY_NEXT)), '']
    ];
    for (const [label, from, to] of MUT) {
      eq(newSource.split(from).length - 1, 1, 'mutant anchor unique: ' + label);
      const m = newSource.replace(from, () => to), errs = inventory(m).concat(binding(m));
      ok(errs.length > 0, 'mutant survived: ' + label);
    }
    console.log('  mutants killed: ' + MUT.length + '/' + MUT.length);
  });

  return sha256(JSON.stringify([inventory(newSource), binding(newSource), behaviour(p => load(newSource, p)),
    behaviour(p => inject(load(newSource, p)))])).slice(0, 16);
}

const digests = [];
let total = 0;
for (let run = 1; run <= 2; run++) {
  passed = 0; failed = 0;
  const d = battery();
  digests.push(d);
  total = passed + failed;
  console.log('run ' + run + ': ' + passed + '/' + total + ' digest ' + d);
  if (failed) { console.log('FAIL ' + passed + '/' + total); process.exit(1); }
}
if (digests[0] !== digests[1]) { console.log('FAIL digests differ ' + digests.join(' ')); process.exit(1); }
console.log('V1211 K0-K6 x2 identical digest ' + digests[0] + ' | new SHA256 ' + sha256(newSource) + ' | base SHA256 ' + sha256(baseSource));
console.log('PASS ' + total + '/' + total);
