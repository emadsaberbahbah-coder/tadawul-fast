'use strict';

// GitHub caps searches using `created` at 1,000 results. Split one fixed
// all-branch interval instead of certifying the truncated first search.
// https://docs.github.com/en/rest/actions/workflow-runs#list-workflow-runs-for-a-repository
async function collectWorkflowRuns(github, options) {
  const {owner, repo, since, until} = options;
  const start = Date.parse(since), end = Date.parse(until);
  const maxRequests = options.maxRequests ?? 200;
  const maxRuns = options.maxRuns ?? 20000;
  const maxDepth = options.maxDepth ?? 32;
  if (!Number.isFinite(start) || !Number.isFinite(end) || start > end ||
      end - start > 31 * 86400000 || !Number.isSafeInteger(maxRequests) || maxRequests < 1 ||
      !Number.isSafeInteger(maxRuns) || maxRuns < 1 ||
      !Number.isSafeInteger(maxDepth) || maxDepth < 0) {
    throw new Error('Invalid bounded workflow inventory window or limits');
  }
  const coverage = {complete: false, scope: 'all_branches',
    since_utc: new Date(start).toISOString(), until_utc: new Date(end).toISOString(),
    api_requests: 0, leaf_windows: 0, duplicates_removed: 0};
  const collected = new Map();
  let rootCount;
  const iso = second => new Date(second * 1000).toISOString();
  const fail = reason => {
    const error = new Error('Workflow inventory incomplete: ' + reason);
    error.coverage = {...coverage, reason, unique_runs: collected.size};
    throw error;
  };
  async function request(left, right, page) {
    if (coverage.api_requests >= maxRequests) fail('request budget reached');
    coverage.api_requests++;
    let response;
    try {
      response = await github.rest.actions.listWorkflowRunsForRepo({
        owner, repo, created: `${iso(left)}..${iso(right)}`, per_page: 100, page,
      });
    } catch (_) {
      fail('workflow-run read failed');
    }
    const data = response && response.data;
    if (!data || !Number.isSafeInteger(data.total_count) || data.total_count < 0 ||
        !Array.isArray(data.workflow_runs)) fail('invalid workflow-run response');
    return data;
  }
  async function visit(left, right, depth) {
    const first = await request(left, right, 1);
    if (depth === 0) {
      rootCount = first.total_count;
      coverage.root_reported_count = rootCount;
    }
    if (first.total_count >= 1000) {
      if (depth >= maxDepth || left >= right) fail('dense interval cannot be verified within split bounds');
      const midpoint = Math.floor((left + right) / 2);
      await visit(left, midpoint, depth + 1);
      // Inclusive windows deliberately overlap the boundary second. Dedup by
      // immutable run ID below; use disjoint seconds when only two remain.
      await visit(midpoint === left ? midpoint + 1 : midpoint, right, depth + 1);
      return;
    }
    const expected = first.total_count;
    const leaf = new Map();
    const pages = Math.max(1, Math.ceil(expected / 100));
    for (let page = 1; page <= pages; page++) {
      const data = page === 1 ? first : await request(left, right, page);
      if (data.total_count !== expected) fail('run count changed during bounded read');
      const expectedPage = Math.min(100, Math.max(0, expected - (page - 1) * 100));
      if (data.workflow_runs.length !== expectedPage) fail('short or oversized workflow-run page');
      for (const run of data.workflow_runs) {
        const created = run && Date.parse(run.created_at);
        if (!run || !Number.isSafeInteger(run.id) || run.id < 1 || !Number.isFinite(created) ||
            created < left * 1000 || created >= (right + 1) * 1000) fail('invalid or out-of-window run identity');
        if (leaf.has(run.id)) fail('duplicate run ID within one paginated interval');
        leaf.set(run.id, run);
      }
    }
    if (leaf.size !== expected) fail('unique run census does not match API count');
    coverage.leaf_windows++;
    for (const [id, run] of leaf) {
      if (collected.has(id)) {
        if (collected.get(id).created_at !== run.created_at) fail('conflicting immutable run identity');
        coverage.duplicates_removed++;
      } else {
        if (collected.size >= maxRuns) fail('unique run budget reached');
        collected.set(id, run);
      }
    }
  }
  await visit(Math.floor(start / 1000), Math.floor(end / 1000), 0);
  // Verify the complete API query census before filtering fractional requested
  // boundaries. Individually complete leaves cannot establish a complete root
  // interval when runs disappear/appear between its initial count and children.
  if (collected.size !== rootCount) fail('root/child unique run census changed');
  const runs = [...collected.values()].filter(run =>
    Date.parse(run.created_at) >= start && Date.parse(run.created_at) <= end).sort((a, b) =>
    b.created_at.localeCompare(a.created_at) || b.id - a.id);
  return {runs, coverage: {...coverage, complete: true,
    query_unique_runs: collected.size, unique_runs: runs.length}};
}

module.exports = {collectWorkflowRuns};
