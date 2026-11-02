'use strict';
const assert = require('node:assert/strict');
const {collectWorkflowRuns} = require('../../scripts/github_workflow_run_window.js');

const since = '2026-10-18T00:00:00.000Z';
const until = '2026-10-24T00:00:00.000Z';
const start = Date.parse(since), finish = Date.parse(until);
const makeRun = (id, stamp) => ({id, created_at: new Date(stamp).toISOString(),
  head_branch: id % 2 ? 'main' : 'feature/test', conclusion: id % 3 ? 'success' : 'failure'});
function api(rows, mutate) {
  const calls = [];
  const github = {rest: {actions: {async listWorkflowRunsForRepo(params) {
    calls.push(params);
    assert.equal(params.per_page, 100);
    assert.equal(params.branch, undefined, 'all-branch scope must be preserved');
    const [left, right] = params.created.split('..').map(Date.parse);
    const selected = rows.filter(r => Date.parse(r.created_at) >= left && Date.parse(r.created_at) <= right)
      .sort((a, b) => b.created_at.localeCompare(a.created_at) || b.id - a.id);
    const data = {total_count: selected.length,
      workflow_runs: selected.slice((params.page - 1) * 100, params.page * 100)};
    return {data: mutate ? mutate(data, params) : data};
  }}}};
  return {github, calls};
}
const options = extra => ({owner:'test', repo:'test', since, until, ...extra});
async function rejectsIncomplete(operation, reason) {
  await assert.rejects(operation, error => {
    assert.equal(error.coverage.complete, false);
    assert.match(error.message, reason);
    return true;
  });
}
async function main() {
  let cases = 0;
  async function check(name, operation) {
    await operation();
    cases++;
    process.stdout.write('PASS ' + name + '\n');
  }
  await check('complete pagination below API cap', async () => {
    const rows = Array.from({length:205}, (_, i) => makeRun(i + 1, start + i * 1000));
    const {github, calls} = api(rows);
    const result = await collectWorkflowRuns(github, options());
    assert.deepEqual(result.runs.map(r => r.id).sort((a,b)=>a-b), rows.map(r=>r.id));
    assert.equal(calls.length, 3);
    assert.equal(result.coverage.complete, true);
    assert.equal(result.coverage.scope, 'all_branches');
  });
  await check('1678-run weekly window splits without truncation', async () => {
    const rows = Array.from({length:1678}, (_, i) => makeRun(i + 1, start + i * 300000));
    const {github} = api(rows);
    const result = await collectWorkflowRuns(github, options());
    assert.equal(result.runs.length, rows.length);
    assert.equal(result.coverage.root_reported_count, rows.length);
    assert.equal(result.coverage.query_unique_runs, rows.length);
    assert.equal(new Set(result.runs.map(r=>r.id)).size, rows.length);
    assert.ok(result.coverage.leaf_windows > 1);
    assert.ok(result.runs.some(r=>r.head_branch !== 'main'));
    assert.ok(result.runs.some(r=>r.conclusion === 'failure'));
  });
  await check('inclusive split-boundary duplicate is removed by immutable ID', async () => {
    const midpoint = (start + finish) / 2;
    const rows = Array.from({length:1000}, (_, i) => makeRun(i + 1,
      i < 500 ? start + i * 1000 : midpoint + 1000 + i * 1000));
    rows.push(makeRun(1001, midpoint));
    const {github} = api(rows);
    const result = await collectWorkflowRuns(github, options());
    assert.equal(result.runs.length, 1001);
    assert.equal(result.coverage.duplicates_removed, 1);
  });
  await check('individually complete children cannot conceal a changed root census', async () => {
    const rows = Array.from({length:1300}, (_, i) => makeRun(i + 1, start + i * 300000));
    for (const delta of [-1, 1]) {
      let first = true;
      const {github} = api(rows, data => {
        if (first) {data.total_count += delta; first = false;}
        return data;
      });
      await rejectsIncomplete(() => collectWorkflowRuns(github, options()), /root\/child unique run census changed/);
    }
  });
  await check('root census includes records excluded only by exact fractional boundaries', async () => {
    const rows = Array.from({length:1001}, (_, i) => makeRun(i + 1, start + i * 300000));
    const {github} = api(rows);
    const result = await collectWorkflowRuns(github, options({since:new Date(start+500).toISOString()}));
    assert.equal(result.coverage.complete, true);
    assert.equal(result.coverage.root_reported_count, 1001);
    assert.equal(result.coverage.query_unique_runs, 1001);
    assert.equal(result.runs.length, 1000);
    assert.ok(result.runs.every(run=>Date.parse(run.created_at)>=start+500));
  });
  await check('query census budget includes fractional-boundary exclusions', async () => {
    const {github} = api([makeRun(1,start), makeRun(2,start+1000)]);
    await rejectsIncomplete(() => collectWorkflowRuns(github,
      options({since:new Date(start+500).toISOString(),maxRuns:1})), /run budget/);
  });
  await check('empty window is verified after one authoritative read', async () => {
    const {github, calls} = api([]);
    const result = await collectWorkflowRuns(github, options());
    assert.equal(result.runs.length, 0);
    assert.equal(calls.length, 1);
    assert.equal(result.coverage.complete, true);
  });
  await check('request bound fails with incomplete receipt', async () => {
    const {github} = api(Array.from({length:205}, (_, i) => makeRun(i + 1, start + i * 1000)));
    await rejectsIncomplete(() => collectWorkflowRuns(github, options({maxRequests:1})), /request budget/);
  });
  await check('run bound fails without a partial success', async () => {
    const {github} = api([makeRun(1,start), makeRun(2,start+1000)]);
    await rejectsIncomplete(() => collectWorkflowRuns(github, options({maxRuns:1})), /run budget/);
  });
  await check('unsplittable dense second is unknown, never truncated', async () => {
    const stamp = start + 1000;
    const {github} = api(Array.from({length:1000}, (_, i) => makeRun(i + 1, stamp)));
    await rejectsIncomplete(() => collectWorkflowRuns(github,
      options({since:new Date(stamp).toISOString(), until:new Date(stamp).toISOString()})), /dense interval/);
  });
  await check('split depth limit is explicit incomplete coverage', async () => {
    const {github} = api(Array.from({length:1001}, (_, i) => makeRun(i+1,start+i*1000)));
    await rejectsIncomplete(() => collectWorkflowRuns(github, options({maxDepth:0})), /split bounds/);
  });
  await check('transport error cannot masquerade as an empty window', async () => {
    const {github, calls} = api([], () => {throw new Error('Unavailable');});
    await rejectsIncomplete(() => collectWorkflowRuns(github, options()), /read failed/);
    assert.equal(calls.length, 1);
  });
  await check('short or changing pages fail census verification', async () => {
    const rows = Array.from({length:150}, (_, i) => makeRun(i + 1, start + i * 1000));
    for (const change of ['short','count']) {
      const {github} = api(rows, (data, p) => {
        if (p.page === 2) {
          if (change === 'short') data.workflow_runs.pop();
          else data.total_count++;
        }
        return data;
      });
      await rejectsIncomplete(() => collectWorkflowRuns(github, options()), /page|count changed/);
    }
  });
  await check('duplicate pagination identity is incomplete', async () => {
    const rows = Array.from({length:150}, (_, i) => makeRun(i + 1, start + i * 1000));
    const {github} = api(rows, (data, p) => {
      if (p.page === 2) data.workflow_runs[0] = rows.at(-1);
      return data;
    });
    await rejectsIncomplete(() => collectWorkflowRuns(github, options()), /duplicate run ID/);
  });
  await check('malformed response and out-of-window run fail', async () => {
    for (const change of ['invalid_count','bad_id','bad_time']) {
      const {github} = api([makeRun(1,start)], data => {
        if (change === 'invalid_count') data.total_count = '1';
        if (change === 'bad_id') data.workflow_runs[0].id = null;
        if (change === 'bad_time') data.workflow_runs[0].created_at = '2020-01-01';
        return data;
      });
      await rejectsIncomplete(() => collectWorkflowRuns(github, options()), /invalid|out-of-window/);
    }
  });
  await check('invalid windows and budgets are rejected before reads', async () => {
    for (const values of [{since:'bad'}, {since:until,until:since},
      {since:'2020-01-01'}, {maxRequests:0}, {maxRuns:0}, {maxDepth:-1}]) {
      const {github,calls} = api([]);
      await assert.rejects(() => collectWorkflowRuns(github, options(values)), /Invalid bounded/);
      assert.equal(calls.length, 0);
    }
  });
  console.log(JSON.stringify({cases, passed:cases}));
}
main().catch(error => {console.error(error);process.exitCode=1;});
