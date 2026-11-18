/* Rebuild earlier reviewed versions of apps_script/10_My_Portfolio.gs from the delivered file, without fixture copies.
 * v1520Source: the v1.5.21 [DIV-BASIS] hunks (tests/js/pd_edits_v1521.json, pairs [current, previous]) are each
 * reversed exactly once and the result must be the reviewed v1.5.20 file (SHA-256 pinned, origin/main 79f61e5).
 * v1519Source: from that file, the v1.5.20 [EXIT-PLAN-STOP] hunks (tests/js/pd_edits_v1520.json) are each reversed
 * exactly once and the result must be the reviewed v1.5.19 file (SHA-256 pinned, origin/main 2b66b51). A file that
 * already is the version asked for is returned as it is. Test fixture only, never runtime code. */
'use strict';
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const sha = (s) => crypto.createHash('sha256').update(s).digest('hex');
const V1519_SHA256 = '4f246be194a7ce5ade7a53e56381c19b44c20055608e33979584e47aeccfe584';
const V1520_SHA256 = '199989e9b8aa4bba39f1a8651eead0c85711f6955956723053a29940b007a6cc';
const EDITS_V1520 = require('./pd_edits_v1520.json');
const EDITS_V1521 = require('./pd_edits_v1521.json');
function reverse(source, edits, wantSha, label) {
  let prior = source;
  for (const [current, previous] of edits) {
    assert.equal(prior.split(current).length - 1, 1, 'exactly one approved ' + label + ' hunk: ' + current.split('\n')[0].slice(0, 80));
    prior = prior.replace(current, () => previous);
  }
  assert.equal(sha(prior), wantSha, label + ' may change only its enumerated hunks');
  return prior;
}
function v1520Source(source) {
  if (sha(source) === V1520_SHA256 || sha(source) === V1519_SHA256) return source;
  return reverse(source, EDITS_V1521, V1520_SHA256, 'v1.5.21');
}
function v1519Source(source) {
  if (sha(source) === V1519_SHA256) return source;
  return reverse(v1520Source(source), EDITS_V1520, V1519_SHA256, 'v1.5.20');
}
module.exports = { v1519Source, v1520Source, V1519_SHA256, V1520_SHA256, EDITS_V1520, EDITS_V1521 };
