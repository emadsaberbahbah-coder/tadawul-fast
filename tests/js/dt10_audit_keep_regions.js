/* tests/js/dt10_audit_keep_regions.js -- the designed regions of
 * 16_Decision_Top10.gs v1.18.1 (held seats kept in the written audit),
 * byte for byte. 2026-10-22: renumbered over #815's v1.13.11, so
 * stripDesigned now rebuilds main's v1.13.11 text (the start of the history
 * chain in tests/js/dt10_cash_utc_history.js, and the input of v11310Source).
 * 2026-10-25: renumbered v1.15.1 over #838's v1.15.0; stripDesigned now
 * rebuilds main's v1.15.0 text, the input of v1140Source / v11311Source in
 * tests/test_dt10_pending_grace.js and tests/test_dt10_confirm_basis.js.
 * 2026-10-27: renumbered v1.17.2 over #852's v1.17.1; stripDesigned now
 * rebuilds main's v1.17.1 text, the start of every history oracle in
 * tests/js/dt10_cash_utc_history.js (v1170Source, v1160Source, ...) and the
 * layer of tests/test_dt10_shadow_momentum.js, tests/test_dt10_shadow_rank_log.js
 * and tests/test_dt10_funding_evidence.js.
 * 2026-10-28: renumbered v1.18.1 over #855's v1.18.0; stripDesigned now
 * rebuilds main's v1.18.0 text, the input of history.v1171Source (the v1.18.0
 * edits reversed, tests/js/dt10_edits_v1180.json) and so of every older oracle.
 * Shared by tests/js/dt10_audit_keep.test.js (D2) and by
 * the harnesses that rebuild the v1.13.10 text before the history helper
 * (tests/js/dt10_cash_utc_history.js): dt10_feed_cohort_binding.test.js and
 * decision_input_truth.test.js. stripDesigned removes exactly these texts
 * and throws on any other shape, so any other edit fails the byte diffs.
 * Not a test file: it runs nothing on load. */
'use strict';
const assert = require('node:assert/strict');
const count = (text, needle) => String(text).split(needle).length - 1;
const DESIGNED = [
  [' * Version: 1.18.1 (see DT10_VERSION;', ' * Version: 1.18.0 (see DT10_VERSION;'],
  ["var DT10_VERSION = '1.18.1';", "var DT10_VERSION = '1.18.0';"],
];
const HEADER_REGION = " * v1.18.1 (2026-10-14) -- HELD SEATS KEPT IN THE WRITTEN AUDIT\n * (Built 2026-10-13 as an unreleased v1.13.9 draft in PR #770; renumbered\n * after #774 shipped v1.13.9, again on 2026-10-21 after #810 shipped a\n * different v1.13.10, on 2026-10-22 after #815 shipped a different\n * v1.13.11 (bulk render formatting), on 2026-10-25 after #833 and\n * #838 shipped v1.14.0 (confirm basis) and v1.15.0 (pending grace), and\n * on 2026-10-27 after #844, #849 and #852 shipped v1.16.0 (funding\n * evidence), v1.17.0 (shadow rank log) and v1.17.1 (shadow momentum),\n * and on 2026-10-28 after #855 shipped v1.18.0 (post-earnings).\n * ALL v1.18.0 logic carried verbatim. No behaviour changed in the\n * renumbering; the base named below is now v1.18.0, the one it sits on.)\n * WHY (Top_10 export 2026-10-13 20:37 Riyadh): six stability grace-held\n * seats (AED.BR, BBOX.L, INDV.US, CRC.US, MGY.US, JMSB.US) rendered blank\n * Rel / DQ / Conf / Engine ROI. None was in the backend's written audit\n * (TFB_OPP_AUDIT_ROWS_MAX keeps selected, unfunded, near-miss and INVEST\n * rows only), so dt10StabIdFallback_ built an identity-only shell: the\n * backend never learns which incumbents this cockpit holds.\n * WHAT: Script Property DT10_AUDIT_KEEP. Exactly 'on' (trimmed, any case)\n * arms it; absent or any other word is off. OFF IS BYTE-IDENTICAL to\n * v1.18.0 in every request and render (one memoized property read is\n * added). Armed, body.criteria gains audit_keep_symbols = the sorted\n * stability members (seated and grace-held, fast-track included; at most\n * 60) read from DT10_STAB_STATE_V1, the blob dt10StabCore_ already uses.\n * The allocation replay deep-copies body, so it carries the same list.\n * opportunity_builder 1.27.0 writes those rows into the capped audit only\n * when its own TFB_OPP_AUDIT_KEEP=1 is set: both switches are needed and\n * either alone is inert. The list is display context, never a decision\n * input: it moves no gate, score, seat or ticket. A property or state read\n * failure sends nothing (the v1.18.0 body).\n * Functions added (0 removed): dt10AuditKeepOn_, dt10AuditKeepSymbols_;\n * vars added: DT10_AUDIT_KEEP_PROP, DT10_AUDIT_KEEP_MAX. refreshDecisionTop10\n * makes one guarded call before the POST. Harness:\n * tests/js/dt10_audit_keep.test.js.\n * Rollback: delete the property (the next run is v1.18.0 behaviour) or\n * re-paste v1.18.0.\n *\n";
const FN_REGION = "/** v1.18.1: Script Property DT10_AUDIT_KEEP. Exactly 'on' (trimmed, any\n * case) arms the held-seat hand-off; absent or any other word is off. A\n * property-service failure reads as OFF: the list is display context only,\n * so failing open keeps the unarmed request. Memoized (P-202). */\nvar DT10_AUDIT_KEEP_PROP = 'DT10_AUDIT_KEEP';\nvar DT10_AUDIT_KEEP_MAX = 60;   // mirrors opportunity_builder _AUDIT_KEEP_MAX\nfunction dt10AuditKeepOn_() {\n  return dt10PropMemoGet_('audit_keep', function () {\n    try {\n      return String(PropertiesService.getScriptProperties()\n          .getProperty(DT10_AUDIT_KEEP_PROP) || '')\n          .replace(/^\\s+|\\s+$/g, '').toLowerCase() === 'on';\n    } catch (eProp) { return false; }\n  });\n}\n/** v1.18.1 PURE: the stability members (member === true: seated, grace-held\n * and fast-track seats) of a parsed DT10_STAB_STATE_V1 state, sorted, at\n * most DT10_AUDIT_KEEP_MAX. [] for a missing or malformed state. */\nfunction dt10AuditKeepSymbols_(state) {\n  var out = [];\n  var syms = state && state.symbols;\n  if (!syms || typeof syms !== 'object') return out;\n  for (var k in syms) {\n    if (syms.hasOwnProperty(k) && k && syms[k] && syms[k].member === true) {\n      out.push(String(k));\n    }\n  }\n  out.sort();\n  return out.slice(0, DT10_AUDIT_KEEP_MAX);\n}\n";
const CALL_REGION = "  // v1.18.1: held seats kept in the backend's written audit (display only).\n  if (dt10AuditKeepOn_()) {\n    try {\n      var akSyms = dt10AuditKeepSymbols_(dt10StabLoad_());\n      if (akSyms.length) body.criteria.audit_keep_symbols = akSyms;\n    } catch (eAk) { /* send nothing: the unarmed body */ }\n  }\n";
function stripDesigned(src) {
  let out = src;
  for (const [delivered, base] of DESIGNED.concat([[HEADER_REGION, ''], [FN_REGION, ''], [CALL_REGION, '']])) {
    assert.equal(count(out, delivered), 1, 'designed v1.18.1 region once: ' + delivered.slice(0, 60));
    out = out.replace(delivered, base);
  }
  return out;
}
module.exports = {DESIGNED, HEADER_REGION, FN_REGION, CALL_REGION, stripDesigned};
