#!/usr/bin/env node
/* tests/test_pd_evidence_publish.js
 * Harness for apps_script/10_My_Portfolio.gs v1.5.16 [EVIDENCE PUBLICATION].
 * Extracts the REAL functions from the delivered file and runs them in node
 * with PropertiesService / ScriptApp / SpreadsheetApp / Logger stubs.
 *   P1 publish mode          P2 pdIbkrPublish_ (document property, limits, marker)
 *   P3 follow-up core        P4 arm / remove the one-off trigger
 *   P5 pdIbkrEvidenceFollowUp end to end (skips, render, error, run-log row)
 *   P6 wiring (apply + watcher text)   P7 kill identity (property absent)
 *   P8 ES5/ASCII lint + inventory      P9 golden negative (PD_BASE=<v1.5.15 file>)
 * Run: node tests/test_pd_evidence_publish.js   (exit 0 = all PASS)
 */
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const SRC = process.argv[2] || path.join(__dirname, '..', 'apps_script', '10_My_Portfolio.gs');
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

// ------------------------------------------------------------------ world
const W = { script: {}, doc: {}, triggers: [], created: [], deleted: [], logs: [], runLog: [], hasRunLog: true };
global.PropertiesService = {
  getScriptProperties: () => ({ getProperty: (k) => (Object.prototype.hasOwnProperty.call(W.script, k) ? W.script[k] : null),
    setProperty: (k, v) => { W.script[k] = String(v); } }),
  getDocumentProperties: () => (W.noDoc ? null : ({ getProperty: (k) => (Object.prototype.hasOwnProperty.call(W.doc, k) ? W.doc[k] : null),
    setProperty: (k, v) => { if (W.docThrow) throw new Error('quota'); W.doc[k] = String(v); } }))
};
global.ScriptApp = {
  getProjectTriggers: () => W.triggers.slice(),
  deleteTrigger: (t) => { W.deleted.push(t._h); const i = W.triggers.indexOf(t); if (i >= 0) W.triggers.splice(i, 1); },
  newTrigger: (h) => {
    const spec = { handler: h };
    const b = { timeBased: () => b, after: (ms) => { spec.after = ms; return b; },
      create: () => { if (W.armThrow) throw new Error('trigger limit'); W.created.push(spec); W.triggers.push({ getHandlerFunction: () => h, _h: h }); return {}; } };
    return b;
  }
};
global.SpreadsheetApp = { getActiveSpreadsheet: () => ({ getSheetByName: (n) => (n === '_Run_Log' && W.hasRunLog ? { appendRow: (r) => W.runLog.push(r) } : null) }) };
global.Logger = { log: (m) => W.logs.push(String(m)) };

const VARS = ['PD_VERSION', 'PD_EVIDENCE_PROP', 'PD_EVIDENCE_MAX_CHARS', 'PD_IBKR_PUBLISH_PROP', 'PD_IBKR_FOLLOWUP_HANDLER',
  'PD_IBKR_FOLLOWUP_DELAY_MS', 'PD_IBKR_FOLLOWUP_MAX_AGE_MS', 'PD_IBKR_PUBLISHED_RUN_'];
const FUNCS = ['pdSrcStampMs_', 'pdIbkrStampMs_', 'pdIbkrAge_', 'pdIbkrProp_', 'pdEvidencePacket_', 'pdIbkrPublishMode_',
  'pdIbkrPublish_', 'pdIbkrRemoveFollowUp_', 'pdIbkrArmFollowUp_', 'pdIbkrFollowUpCore_', 'pdIbkrRunLog_',
  'pdIbkrEvidenceFollowUp', 'pdIbkrFollowUpState_'];
const geval = eval;
for (const v of VARS) geval(extractVar(src, v));
for (const f of FUNCS) geval(extract(src, f));

let pass = 0;
function ok(cond, label) { if (!cond) { console.error('FAIL ' + label); process.exit(1); } pass++; }
function deep(a, b) { return JSON.stringify(a) === JSON.stringify(b); }
function reset() { W.script = {}; W.doc = {}; W.triggers = []; W.created = []; W.deleted = []; W.logs = []; W.runLog = []; W.hasRunLog = true; W.noDoc = false; W.docThrow = false; W.armThrow = false; PD_IBKR_PUBLISHED_RUN_ = null; }
const ISO = '2026-10-26T05:41:07Z';
const packet = () => ({ schema_version: 1, source_ref: 'ibkr-capture:TFB_IBKR_Capture_20261026-084107.json', captured_at: ISO,
  accounts: [{ account_id: 'ibkr-primary', positions_asof: ISO, positions_complete: true, positions: [], cash_asof: ISO, cash_complete: true,
    cash: [{ currency: 'USD', settled_cash: '22085.12', reserved_cash: '2644.8', reservations_complete: true }] }],
  holding_links: [], funding_accounts: ['ibkr-primary'],
  fx_rates: [{ currency: 'USD', rate_to_sar: '3.75', asof: ISO, source_ref: 'policy://sama-usd-peg-3.75' }] });

// ---------------------------------------------------------------- P1
reset(); ok(pdIbkrPublishMode_() === 'off', 'P1 default off');
W.script.PD_IBKR_EVIDENCE_PUBLISH = ' On '; ok(pdIbkrPublishMode_() === 'on', 'P1 on');
W.script.PD_IBKR_EVIDENCE_PUBLISH = 'enforce'; ok(pdIbkrPublishMode_() === 'off', 'P1 other words are off');
console.log('P1 PASS publish mode');

// ---------------------------------------------------------------- P2
reset();
let r = pdIbkrPublish_(packet(), 1000);
ok(r.published === true && W.doc.TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1 === JSON.stringify(packet()), 'P2 property written verbatim');
ok(PD_IBKR_PUBLISHED_RUN_ && PD_IBKR_PUBLISHED_RUN_.at_ms === 1000 && PD_IBKR_PUBLISHED_RUN_.captured_at === ISO, 'P2 marker set');
ok(pdEvidencePacket_().schema_version === 1 && pdEvidencePacket_().captured_at === ISO, 'P2 the reader accepts what was written');
reset();
const big = packet(); big.source_ref = 'x'.repeat(9000);
r = pdIbkrPublish_(big, 1);
ok(r.published === false && r.reason === 'packet oversized' && !W.doc.TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1 && PD_IBKR_PUBLISHED_RUN_ === null, 'P2 oversized: nothing written');
reset(); W.noDoc = true; r = pdIbkrPublish_(packet(), 1);
ok(r.published === false && /unavailable/.test(r.reason), 'P2 no document properties');
reset(); W.docThrow = true; r = pdIbkrPublish_(packet(), 1);
ok(r.published === false && r.reason === 'publish failed' && PD_IBKR_PUBLISHED_RUN_ === null, 'P2 setProperty throws -> failed, no marker');
ok(!W.logs.some((l) => /22085/.test(l)), 'P2 never logs the packet');
console.log('P2 PASS publish');

// ---------------------------------------------------------------- P3
const guards = { automationAllowed: true, inflight: false, cockpitRunning: false, quiet: false };
ok(pdIbkrFollowUpCore_('on', 300000, true, guards).go === true, 'P3 go');
ok(pdIbkrFollowUpCore_('on', 300000, true, null).go === true, 'P3 go without the chain');
ok(pdIbkrFollowUpCore_('on', 840000, true, guards).go === true, 'P3 840 s still inside');
ok(/too old/.test(pdIbkrFollowUpCore_('on', 840001, true, guards).reason), 'P3 841 s too old');
ok(/ahead/.test(pdIbkrFollowUpCore_('on', -1, true, guards).reason), 'P3 ahead of the clock');
ok(/no published/.test(pdIbkrFollowUpCore_('on', null, true, guards).reason), 'P3 no packet');
ok(/publish off/.test(pdIbkrFollowUpCore_('off', 1, true, guards).reason), 'P3 off');
ok(/unavailable/.test(pdIbkrFollowUpCore_('on', 1, false, guards).reason), 'P3 no Top_10');
ok(/paused/.test(pdIbkrFollowUpCore_('on', 1, true, Object.assign({}, guards, { automationAllowed: false })).reason), 'P3 automation paused');
ok(/in flight/.test(pdIbkrFollowUpCore_('on', 1, true, Object.assign({}, guards, { inflight: true })).reason), 'P3 inflight');
ok(/cockpit/.test(pdIbkrFollowUpCore_('on', 1, true, Object.assign({}, guards, { cockpitRunning: true })).reason), 'P3 cockpit running');
ok(/quiet/.test(pdIbkrFollowUpCore_('on', 1, true, Object.assign({}, guards, { quiet: true })).reason), 'P3 quiet');
console.log('P3 PASS follow-up core');

// ---------------------------------------------------------------- P4
reset();
ok(pdIbkrArmFollowUp_() === 'follow-up armed' && W.created.length === 1 && W.created[0].handler === 'pdIbkrEvidenceFollowUp' && W.created[0].after === 60000, 'P4 armed once, +60 s');
ok(pdIbkrArmFollowUp_() === 'follow-up armed' && W.triggers.length === 1 && W.deleted.length === 1, 'P4 re-arm replaces, never stacks');
ok(pdIbkrFollowUpState_() === '1 armed', 'P4 state');
ok(pdIbkrRemoveFollowUp_() === 1 && W.triggers.length === 0 && pdIbkrFollowUpState_() === 'none armed', 'P4 remove');
reset(); W.armThrow = true;
ok(/not armed \(trigger limit\)/.test(pdIbkrArmFollowUp_()), 'P4 create throws -> reported');
console.log('P4 PASS arm/remove');

// ---------------------------------------------------------------- P5
function runFollowUp(opts) {
  reset();
  Object.assign(W, opts.world || {});
  W.script.PD_IBKR_EVIDENCE_PUBLISH = opts.mode || 'on';
  if (opts.packet !== null) W.doc.TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1 = JSON.stringify(opts.packet || packet());
  W.triggers.push({ getHandlerFunction: () => 'pdIbkrEvidenceFollowUp', _h: 'pdIbkrEvidenceFollowUp' });
  const realNow = Date.now;
  Date.now = () => opts.now;
  if (opts.top10 === false) delete global.refreshDecisionTop10; else global.refreshDecisionTop10 = opts.top10;
  if (opts.chain) global.tfbDcReadInputs_ = opts.chain; else delete global.tfbDcReadInputs_;
  let out;
  try { out = pdIbkrEvidenceFollowUp(); } finally { Date.now = realNow; }
  return out;
}
let rendered = 0;
let out = runFollowUp({ now: Date.parse(ISO) + 4 * 60000, top10: () => { rendered++; } });
ok(out === 'rendered' && rendered === 1 && W.triggers.length === 0 && W.deleted.length === 1, 'P5 renders, clears its trigger');
ok(W.runLog.length === 1 && W.runLog[0][1] === 'INFO' && W.runLog[0][2] === 'pdIbkrEvidenceFollowUp' && W.runLog[0][3] === 'Top_10_Investments' && W.runLog[0][4] === 'OK' && /packet age 4m; trigger cleared/.test(W.runLog[0][5]), 'P5 run-log OK row');
out = runFollowUp({ now: Date.parse(ISO) + 20 * 60000, top10: () => { throw new Error('should not render'); } });
ok(out === 'skipped: packet too old (20m)' && W.runLog[0][4] === 'SKIPPED' && W.runLog[0][1] === 'INFO', 'P5 stale -> skipped, no render');
out = runFollowUp({ now: Date.parse(ISO) + 1000, top10: () => {}, chain: () => ({ automationAllowed: true, inflight: true, cockpitRunning: false, quiet: false }) });
ok(out === 'skipped: sync in flight', 'P5 chain guard honoured');
out = runFollowUp({ now: Date.parse(ISO) + 1000, top10: () => {}, chain: () => { throw new Error('chain broken'); } });
ok(out === 'rendered', 'P5 a broken chain read does not block (guards null)');
out = runFollowUp({ now: Date.parse(ISO) + 1000, top10: () => { throw new Error('boom'); } });
ok(out === 'error: boom' && W.runLog[0][4] === 'FAILED' && W.runLog[0][1] === 'ERROR', 'P5 render error -> FAILED row');
out = runFollowUp({ now: Date.parse(ISO) + 1000, top10: false });
ok(out === 'skipped: refreshDecisionTop10 unavailable', 'P5 no cockpit function');
out = runFollowUp({ now: Date.parse(ISO) + 1000, top10: () => {}, packet: null });
ok(out === 'skipped: no published packet', 'P5 no packet');
out = runFollowUp({ now: Date.parse(ISO) + 1000, top10: () => {}, mode: 'off' });
ok(out === 'skipped: publish off', 'P5 off');
out = runFollowUp({ now: Date.parse(ISO) + 1000, top10: () => {}, world: { hasRunLog: false } });
ok(out === 'rendered' && W.runLog.length === 0 && W.logs.some((l) => /evidence follow-up -> rendered/.test(l)), 'P5 no _Run_Log sheet: still renders, Logger only');
ok(!W.logs.some((l) => /22085|settled_cash/.test(l)), 'P5 never the packet');
console.log('P5 PASS follow-up end to end');

// ---------------------------------------------------------------- P6 wiring
ok(src.indexOf("      if (fresh && pdIbkrPublishMode_() === 'on') {\n        var pub = pdIbkrPublish_(plan.packet, nowMs);") > 0, 'P6 apply publishes only fresh + on');
ok(src.indexOf("    res.note = pdIbkrNote_(mode, plan, fresh, split.excluded) + (res.publishNote || '');") > 0, 'P6 note suffix');
ok(src.indexOf("    PD_IBKR_PUBLISHED_RUN_ = null;   /* v1.5.16 */\n    var outcome = String(refreshPortfolioDecision() || '');") > 0, 'P6 watcher resets the marker before the refresh');
ok(src.indexOf("    var armed = (PD_IBKR_PUBLISHED_RUN_ && pdIbkrPublishMode_() === 'on')\n        ? pdIbkrArmFollowUp_() : '';") > 0, 'P6 watcher arms after a publishing refresh');
const applyText = extract(src, 'pdIbkrApply_');
ok(applyText.indexOf("if (mode === 'enforce')") > 0 && applyText.indexOf('pdIbkrPublish_') > applyText.indexOf("if (mode === 'enforce')"), 'P6 publication sits inside the enforce branch');
console.log('P6 PASS wiring');

// ---------------------------------------------------------------- P7 kill identity
reset();
// property absent: the watcher-side arming expression and the apply-side publication both stay inert
ok(pdIbkrPublishMode_() === 'off', 'P7 off by default');
PD_IBKR_PUBLISHED_RUN_ = { at_ms: 1 };
const armedExpr = (PD_IBKR_PUBLISHED_RUN_ && pdIbkrPublishMode_() === 'on') ? pdIbkrArmFollowUp_() : '';
ok(armedExpr === '' && W.created.length === 0, 'P7 nothing armed when off');
ok(Object.keys(W.doc).length === 0, 'P7 nothing written when off');
console.log('P7 PASS kill identity');

// ---------------------------------------------------------------- P8 lint + inventory
const i0 = src.indexOf('/* ----- v1.5.16 evidence publication');
const i1 = src.indexOf('/** PURE: one OUTSIDE EXECUTION SCOPE display row.', i0);
const block = src.slice(i0, i1);
ok(i0 > 0 && i1 > i0, 'P8 block located');
ok(!/\b(let|const)\s|=>|`|\.\.\.|class\s/.test(block), 'P8 ES5 only');
ok(![...block].some((ch) => ch.charCodeAt(0) > 126), 'P8 ASCII only');
for (const f of FUNCS.slice(5)) ok(block.indexOf('function ' + f + '(') > 0, 'P8 inventory ' + f);
// merge note 2026-10-27 (#780): panel preserve is v1.5.17 over #844's v1.5.16: the delivered file may sit above
// v1.5.16; the floor is v1.5.16 and the header line stays in lockstep with PD_VERSION.
const pdv = src.match(/^var PD_VERSION = '(\d+)\.(\d+)\.(\d+)';$/m);
const pdAtLeast = (v, f) => { for (let k = 0; k < f.length; k++) { if (v[k] !== f[k]) return v[k] > f[k]; } return true; };
ok(pdv && (src.match(/var PD_VERSION = '/g) || []).length === 1 && pdAtLeast(pdv.slice(1).map(Number), [1, 5, 16]) &&
   new RegExp('^ \\* Version: ' + pdv.slice(1).join('\\.') + '$', 'm').test(src), 'P8 version lockstep');
ok(src.indexOf(" * v1.5.16 (2026-10-26)  (EVIDENCE PUBLICATION") > 0 && src.indexOf(" * v1.5.15 (2026-10-24)  (PD-ALLCASH") > 0, 'P8 header blocks, prior WHY kept');
console.log('P8 PASS lint + inventory');

// ---------------------------------------------------------------- P9 golden negative
if (process.env.PD_BASE) {
  const base = fs.readFileSync(process.env.PD_BASE, 'utf8');
  const sha = crypto.createHash('sha256').update(base).digest('hex');
  let missing = 0;
  for (const f of FUNCS.slice(5)) { try { extract(base, f); } catch (e) { missing++; } }
  ok(missing === FUNCS.length - 5 && /var PD_VERSION = '1\.5\.15';/.test(base) && base.indexOf('PD_IBKR_EVIDENCE_PUBLISH') < 0, 'P9 base lacks every added function and the property (sha256 ' + sha.slice(0, 12) + ')');
  // every v1.5.15 top-level function is still present
  const names = (s) => (s.match(/^function\s+([A-Za-z_$][\w$]*)\s*\(/mg) || []).map((m) => m.replace(/^function\s+/, '').replace(/\s*\($/, ''));
  const nb = names(base), nn = new Set(names(src));
  // merge note 2026-10-27 (#780): panel preserve is v1.5.17 over #844's v1.5.16: the delivered file also carries
  // #780's three panel-preserve functions (tests/js/pf_panel_preserve.test.js).
  const PD780 = ['pdPanelPreserveMode_', 'pdPanelPreserve_', 'pdPanelSeedValue_'];
  ok(nb.every((n) => nn.has(n)) && PD780.every((n) => nn.has(n) && nb.indexOf(n) < 0) && nn.size === nb.length + 8 + PD780.length,
     'P9 zero functions removed; exactly 8 added + #780\'s 3 (' + nb.length + ' -> ' + nn.size + ')');
  console.log('P9 PASS golden negative on the v1.5.15 base ' + sha.slice(0, 12));
} else {
  console.log('P9 SKIP (set PD_BASE=<v1.5.15 file> to run the golden negative)');
}
console.log('ALL PASS (' + pass + ' checks) on ' + path.basename(SRC) + ' sha256 ' + crypto.createHash('sha256').update(src).digest('hex').slice(0, 12));
