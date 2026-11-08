'use strict';
/* tests/js/dp2_history.js - the reviewed 27_Decision_Pages_v2.gs files rebuilt from the delivered one.
 * 2026-11-08: v1.2.35 [STOP-RULE] (DP2_STOP_RULE, default off) is delivered; the v1.2.31-v1.2.34 harnesses prove their
 * own diffs, so they read the v1.2.34 file rebuilt by reversing the enumerated v1.2.35 hunks
 * (tests/js/dp2_edits_v1235.json), SHA-256 pinned. A source that already is the v1.2.34 file is returned unchanged.
 */
const crypto = require('crypto');
const assert = require('assert');
const V1234_SHA256 = '992fb7f77e0d4c4c59f1eb6fb42b7b45be41e1ffaa47957797222662246811c0';   // v1.2.34 (origin/main bdf2da2)
const EDITS_V1235 = require('./dp2_edits_v1235.json');
const sha = s => crypto.createHash('sha256').update(s).digest('hex');
function v1234Source(source) {
  if (sha(source) === V1234_SHA256) return source;
  let prior = source;
  for (const [current, previous] of EDITS_V1235) {
    assert.equal(prior.split(current).length - 1, 1, 'exactly one approved v1.2.35 hunk: ' + current.split('\n')[0].slice(0, 90));
    prior = prior.replace(current, () => previous);
  }
  assert.equal(sha(prior), V1234_SHA256, 'v1.2.35 may change only its enumerated hunks');
  return prior;
}
module.exports = { v1234Source, V1234_SHA256, EDITS_V1235 };
