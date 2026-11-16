/* Rebuild earlier reviewed versions of apps_script/10_My_Portfolio.gs from the delivered file, without fixture copies.
 * v1519Source: the v1.5.20 [EXIT-PLAN-STOP] hunks (tests/js/pd_edits_v1520.json, pairs [current, previous]) are each
 * reversed exactly once and the result must be the reviewed v1.5.19 file (SHA-256 pinned, origin/main 2b66b51). A file
 * that already is v1.5.19 is returned as it is. Test fixture only, never runtime code. */
'use strict';
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const sha = (s) => crypto.createHash('sha256').update(s).digest('hex');
const V1519_SHA256 = '4f246be194a7ce5ade7a53e56381c19b44c20055608e33979584e47aeccfe584';
const EDITS_V1520 = require('./pd_edits_v1520.json');
function v1519Source(source) {
  if (sha(source) === V1519_SHA256) return source;
  let prior = source;
  for (const [current, previous] of EDITS_V1520) {
    assert.equal(prior.split(current).length - 1, 1, 'exactly one approved v1.5.20 hunk: ' + current.split('\n')[0].slice(0, 80));
    prior = prior.replace(current, () => previous);
  }
  assert.equal(sha(prior), V1519_SHA256, 'v1.5.20 may change only its enumerated hunks');
  return prior;
}
module.exports = { v1519Source, V1519_SHA256, EDITS_V1520 };
