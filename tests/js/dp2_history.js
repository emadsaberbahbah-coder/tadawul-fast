'use strict';
/* tests/js/dp2_history.js - the reviewed 27_Decision_Pages_v2.gs files rebuilt from the delivered one.
 * 2026-11-11: v1.3.1 [EXEC-TILE-FEED] (DP2_EXEC_TILE_FEED, default off; first 1.2.37 on 1.2.36) is delivered on top of
 * v1.3.0; the v1.3.0 harness proves its own diff, so it reads the v1.3.0 file rebuilt by reversing the enumerated v1.3.1
 * hunks (tests/js/dp2_edits_v1301.json), SHA-256 pinned; v1236Source and the older ones chain through it.
 * 2026-11-11: v1.3.0 [REFRESH-SLOTS] (the slot trigger, installed from the menu) is delivered; the v1.2.36 harness proves
 * its own diff, so it reads the v1.2.36 file rebuilt by reversing the enumerated v1.3.0 hunks
 * (tests/js/dp2_edits_v1300.json), SHA-256 pinned; the older harnesses chain through it.
 * 2026-11-09: v1.2.36 [EVIDENCE-SINCE] (DP2_EVIDENCE_SINCE, default off) is delivered; the v1.2.35 harness proves its own
 * diff, so it reads the v1.2.35 file rebuilt by reversing the enumerated v1.2.36 hunks (tests/js/dp2_edits_v1236.json),
 * SHA-256 pinned.
 * 2026-11-08: v1.2.35 [STOP-RULE] (DP2_STOP_RULE, default off) is delivered; the v1.2.31-v1.2.34 harnesses prove their
 * own diffs, so they read the v1.2.34 file rebuilt by reversing the enumerated v1.2.35 hunks
 * (tests/js/dp2_edits_v1235.json) from the v1.2.35 file, SHA-256 pinned. A source that already is the older file is
 * returned unchanged.
 */
const crypto = require('crypto');
const assert = require('assert');
const V1300_SHA256 = 'f670ad1cd2ccadfbd4a4b60028ceaeacf0939f51a6ea4407d0bbe4a03330c4c9';   // v1.3.0 (origin/main 53f2abd)
const V1236_SHA256 = '487c0778e1f509eb9ee60e91d4603ed884367719af1b065e6231b723bb1f10f3';   // v1.2.36 (origin/main 58910af)
const V1235_SHA256 = 'dc88cfab7cc706b391f6288dcc44d8e9e0cf6479015b22a725f134c6fd694abf';   // v1.2.35 (origin/main 772a497)
const V1234_SHA256 = '992fb7f77e0d4c4c59f1eb6fb42b7b45be41e1ffaa47957797222662246811c0';   // v1.2.34 (origin/main bdf2da2)
const EDITS_V1301 = require('./dp2_edits_v1301.json');
const EDITS_V1300 = require('./dp2_edits_v1300.json');
const EDITS_V1236 = require('./dp2_edits_v1236.json');
const EDITS_V1235 = require('./dp2_edits_v1235.json');
const sha = s => crypto.createHash('sha256').update(s).digest('hex');
function reverse(source, edits, label, pin) {
  let prior = source;
  for (const [current, previous] of edits) {
    assert.equal(prior.split(current).length - 1, 1, 'exactly one approved ' + label + ' hunk: ' + current.split('\n')[0].slice(0, 90));
    prior = prior.replace(current, () => previous);
  }
  assert.equal(sha(prior), pin, label + ' may change only its enumerated hunks');
  return prior;
}
function v1300Source(source) {
  const h = sha(source);
  if (h === V1300_SHA256 || h === V1236_SHA256 || h === V1235_SHA256 || h === V1234_SHA256) return source;
  return reverse(source, EDITS_V1301, 'v1.3.1', V1300_SHA256);
}
function v1236Source(source) {
  const h = sha(source);
  if (h === V1236_SHA256 || h === V1235_SHA256 || h === V1234_SHA256) return source;
  return reverse(v1300Source(source), EDITS_V1300, 'v1.3.0', V1236_SHA256);
}
function v1235Source(source) {
  if (sha(source) === V1235_SHA256 || sha(source) === V1234_SHA256) return source;
  return reverse(v1236Source(source), EDITS_V1236, 'v1.2.36', V1235_SHA256);
}
function v1234Source(source) {
  if (sha(source) === V1234_SHA256) return source;
  return reverse(v1235Source(source), EDITS_V1235, 'v1.2.35', V1234_SHA256);
}
module.exports = { v1300Source, v1236Source, v1235Source, v1234Source, V1300_SHA256, V1236_SHA256, V1235_SHA256, V1234_SHA256,
  EDITS_V1301, EDITS_V1300, EDITS_V1236, EDITS_V1235 };
