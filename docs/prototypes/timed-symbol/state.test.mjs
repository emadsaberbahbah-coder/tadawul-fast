import test from "node:test";
import assert from "node:assert/strict";
import {
  SymbolSession, SessionTicker, OVERRIDE_TTL_MS, PAGE_IDS,
  STORAGE_KEY, DEFAULT_SYMBOL, normalizeSymbol,
} from "./state.mjs";
import { INTEGRATION_CONFIG, buildResearchUrl } from "./integration.mjs";

class MemoryStorage {
  constructor() { this.values = new Map(); }
  getItem(key) { return this.values.get(key) ?? null; }
  setItem(key, value) { this.values.set(key, String(value)); }
}

class FakeClock {
  time = 1_800_000_000_000;
  nextId = 1;
  timers = new Map();
  now = () => this.time;
  setTimeout = (callback, delay) => {
    const id = this.nextId++;
    this.timers.set(id, { callback, at: this.time + delay });
    return id;
  };
  clearTimeout = id => this.timers.delete(id);
  // Deliberately does not run callbacks: models background-tab throttling.
  elapseWithoutCallbacks(milliseconds) { this.time += milliseconds; }
  fireDue() {
    const due = [...this.timers.entries()].filter(([, timer]) => timer.at <= this.time);
    for (const [id, timer] of due) {
      if (!this.timers.delete(id)) continue;
      timer.callback();
    }
  }
}

function setup(storage = new MemoryStorage()) {
  const clock = new FakeClock();
  const session = new SymbolSession({ storage, now: clock.now });
  return { clock, session, storage };
}

test("master selection follows every research page; overrides stay page-local", () => {
  const { session } = setup();
  session.setMaster("  itrn.us  ");
  for (const page of PAGE_IDS) {
    assert.equal(session.setPage(page).effectiveSymbol, "ITRN.US");
  }
  session.setOverride("risk", "2222.SR");
  session.setOverride("news", "AAPL.US");
  assert.equal(session.setPage("risk").effectiveSymbol, "2222.SR");
  assert.equal(session.setPage("news").effectiveSymbol, "AAPL.US");
  assert.equal(session.setPage("fundamentals").effectiveSymbol, "ITRN.US");
  assert.equal(session.setPage("overview").effectiveSymbol, "ITRN.US");
});

test("five-minute boundary uses the original absolute deadline", () => {
  const { clock, session } = setup();
  session.setPage("price");
  const createdAt = clock.time;
  session.setOverride("price", "ITRN.US");
  assert.equal(session.snapshot().expiresAt, createdAt + OVERRIDE_TTL_MS);
  clock.elapseWithoutCallbacks(299_999);
  assert.equal(session.snapshot().effectiveSymbol, "ITRN.US");
  assert.equal(session.snapshot().remainingMs, 1);
  clock.elapseWithoutCallbacks(1);
  assert.deepEqual(session.refresh().expiredPages, ["price"]);
  assert.equal(session.snapshot().effectiveSymbol, DEFAULT_SYMBOL);
  assert.equal(session.snapshot().expiresAt, null);
});

test("leaving and revisiting a page never restarts its deadline", () => {
  const { clock, session } = setup();
  session.setPage("risk");
  const deadline = session.setOverride("risk", "ITRN.US").expiresAt;
  clock.elapseWithoutCallbacks(120_000);
  session.setPage("news");
  clock.elapseWithoutCallbacks(179_999);
  assert.equal(session.setPage("risk").expiresAt, deadline);
  assert.equal(session.snapshot().remainingMs, 1);
  session.setPage("overview");
  clock.elapseWithoutCallbacks(1);
  assert.equal(session.setPage("risk").effectiveSymbol, DEFAULT_SYMBOL);
});

test("new master clears all overrides, including pages not currently visible", () => {
  const { session } = setup();
  session.setOverride("news", "ITRN.US");
  session.setOverride("risk", "AAPL.US");
  session.setPage("overview");
  assert.deepEqual(session.setMaster("3333.SR").overrides, {});
  for (const page of PAGE_IDS) {
    assert.equal(session.setPage(page).effectiveSymbol, "3333.SR");
  }
  session.setOverride("news", "ITRN.US");
  // Explicitly applying the current master also resets comparisons.
  assert.deepEqual(session.setMaster("3333.SR").overrides, {});
});

test("manual reset or matching the master clears only the requested override", () => {
  const { session } = setup();
  session.setOverride("risk", "ITRN.US");
  session.setOverride("news", "AAPL.US");
  session.setOverride("risk", DEFAULT_SYMBOL);
  assert.equal(session.setPage("risk").isOverride, false);
  assert.equal(session.setPage("news").effectiveSymbol, "AAPL.US");
  session.clearOverride();
  assert.equal(session.snapshot().effectiveSymbol, DEFAULT_SYMBOL);
});

test("refreshing the interface preserves a valid deadline but never extends it", () => {
  const { clock, session, storage } = setup();
  session.setPage("fundamentals");
  session.setMaster("AAPL.US");
  const deadline = session.setOverride("fundamentals", "ITRN.US").expiresAt;
  clock.elapseWithoutCallbacks(50_000);
  const reloaded = new SymbolSession({ storage, now: clock.now });
  assert.equal(reloaded.snapshot().effectiveSymbol, "ITRN.US");
  assert.equal(reloaded.snapshot().expiresAt, deadline);
  clock.elapseWithoutCallbacks(250_000);
  const afterExpiry = new SymbolSession({ storage, now: clock.now });
  assert.equal(afterExpiry.snapshot().effectiveSymbol, "AAPL.US");
  assert.deepEqual(afterExpiry.snapshot().overrides, {});
});

test("focus after throttled timers immediately restores master on all expired pages", () => {
  const { clock, session } = setup();
  session.setPage("price");
  session.setOverride("price", "ITRN.US");
  session.setOverride("news", "AAPL.US");
  const frames = [];
  const ticker = new SessionTicker(session, (snapshot, expired) => frames.push({ snapshot, expired }), {
    setTimeoutFn: clock.setTimeout, clearTimeoutFn: clock.clearTimeout,
  });
  ticker.start();
  assert.equal(frames.at(-1).snapshot.effectiveSymbol, "ITRN.US");
  clock.elapseWithoutCallbacks(OVERRIDE_TTL_MS + 30_000);
  ticker.focus();
  assert.equal(frames.at(-1).snapshot.effectiveSymbol, DEFAULT_SYMBOL);
  assert.deepEqual(frames.at(-1).expired.sort(), ["news", "price"]);
  assert.equal(clock.timers.size, 1);
  ticker.stop();
  assert.equal(clock.timers.size, 0);
});

test("a late timer callback expires against wall time rather than callback count", () => {
  const { clock, session } = setup();
  session.setPage("news");
  session.setOverride("news", "ITRN.US");
  let rendered;
  const ticker = new SessionTicker(session, snapshot => { rendered = snapshot; }, {
    setTimeoutFn: clock.setTimeout, clearTimeoutFn: clock.clearTimeout,
  });
  ticker.start();
  clock.elapseWithoutCallbacks(OVERRIDE_TTL_MS);
  clock.fireDue();
  assert.equal(rendered.effectiveSymbol, DEFAULT_SYMBOL);
  ticker.stop();
});

test("a navigation or state read cannot consume expiry before the UI announcement", () => {
  const { clock, session } = setup();
  session.setOverride("risk", "ITRN.US");
  const frames = [];
  const ticker = new SessionTicker(session, (snapshot, expired) => frames.push({ snapshot, expired }), {
    setTimeoutFn: clock.setTimeout, clearTimeoutFn: clock.clearTimeout,
  });
  ticker.start();
  clock.elapseWithoutCallbacks(OVERRIDE_TTL_MS);
  session.setPage("risk");
  session.snapshot();
  ticker.focus();
  assert.equal(frames.at(-1).snapshot.effectiveSymbol, DEFAULT_SYMBOL);
  assert.deepEqual(frames.at(-1).expired, ["risk"]);
  ticker.focus();
  assert.deepEqual(frames.at(-1).expired, []);
  ticker.stop();
});

test("page lifecycle stop/start checks expiry before its first recovered render", () => {
  const { clock, session } = setup();
  session.setPage("risk");
  session.setOverride("risk", "ITRN.US");
  const frames = [];
  const ticker = new SessionTicker(session, snapshot => frames.push(snapshot), {
    setTimeoutFn: clock.setTimeout, clearTimeoutFn: clock.clearTimeout,
  });
  ticker.start();
  ticker.stop();
  clock.elapseWithoutCallbacks(OVERRIDE_TTL_MS + 1);
  ticker.start();
  assert.equal(frames.at(-1).effectiveSymbol, DEFAULT_SYMBOL);
  assert.equal(clock.timers.size, 1);
  ticker.stop();
});

test("observed backwards clock change cancels overrides rather than extending them", () => {
  const { clock, session } = setup();
  session.setOverride("risk", "ITRN.US");
  clock.elapseWithoutCallbacks(20_000);
  session.refresh();
  clock.elapseWithoutCallbacks(-10_000);
  assert.deepEqual(session.refresh().expiredPages, ["risk"]);
});

test("session storage is per-viewer context and snapshots cannot mutate internal state", () => {
  const one = setup();
  const two = setup();
  one.session.setMaster("ITRN.US");
  one.session.setOverride("risk", "AAPL.US");
  const snapshot = one.session.snapshot();
  snapshot.overrides.risk.symbol = "MUTATED.US";
  assert.equal(one.session.setPage("risk").effectiveSymbol, "AAPL.US");
  assert.equal(two.session.snapshot().masterSymbol, DEFAULT_SYMBOL);
  assert.deepEqual(two.session.snapshot().overrides, {});
});

test("corrupt or unrecognized persisted state falls back safely", () => {
  for (const value of ["{broken", "null", "[]", JSON.stringify({ version: 9, masterSymbol: "ITRN.US" })]) {
    const storage = new MemoryStorage();
    storage.setItem(STORAGE_KEY, value);
    assert.equal(setup(storage).session.snapshot().masterSymbol, DEFAULT_SYMBOL);
  }
});

test("restore drops tampered deadlines, unknown pages, master-page overrides and expired values", () => {
  const storage = new MemoryStorage();
  const clock = new FakeClock();
  const good = { symbol: "ITRN.US", createdAt: clock.time - 1, expiresAt: clock.time + 299_999 };
  storage.setItem(STORAGE_KEY, JSON.stringify({
    version: 1, masterSymbol: "2222.SR", activePage: "unknown", overrides: {
      overview: good,
      unknown: good,
      risk: { ...good, expiresAt: clock.time + 300_000 },
      price: { ...good, createdAt: clock.time + 1 },
      ratings: { symbol: "AAPL.US", createdAt: clock.time - OVERRIDE_TTL_MS, expiresAt: clock.time },
      news: good,
    },
  }));
  const session = new SymbolSession({ storage, now: clock.now });
  assert.equal(session.snapshot().activePage, "overview");
  assert.deepEqual(Object.keys(session.snapshot().overrides), ["news"]);
});

test("denied browser storage still supports an isolated in-memory session", () => {
  const storage = { getItem() { throw new Error("denied"); }, setItem() { throw new Error("denied"); } };
  const { session } = setup(storage);
  assert.equal(session.setMaster("ITRN.US").masterSymbol, "ITRN.US");
  assert.equal(session.setPage("news").effectiveSymbol, "ITRN.US");
});

test("invalid page and symbol inputs are rejected before changing valid selection", () => {
  const { session } = setup();
  for (const value of ["", "AAPL US", "<script>", "../evil", "AAPL&params=evil", null]) {
    assert.throws(() => normalizeSymbol(value));
    assert.throws(() => session.setMaster(value));
  }
  assert.throws(() => session.setPage("not-a-page"));
  assert.throws(() => session.setOverride("overview", "AAPL.US"));
  assert.equal(session.snapshot().masterSymbol, DEFAULT_SYMBOL);
});

// Fictional fixture keys and embed page IDs, never production bindings.
function configuredFixture() {
  const reportId = "11111111-2222-3333-4444-555555555555";
  return {
    enabled: true, reportId,
    embedPageUrls: Object.fromEntries(PAGE_IDS.map(page => [page,
      `https://lookerstudio.google.com/embed/reporting/${reportId}/page/fixture_${page}`])),
    parameterBindings: Object.fromEntries(PAGE_IDS.map(page => [page, ["fixture.parameter_one", "fixture.parameter_two"]])),
    verification: {
      verifiedAt: "2026-11-03T00:00:00Z", nativeSymbolControlsDisabled: true,
      nativePageNavigationDisabled: true, allPagesTested: true,
    },
  };
}

test("production prototype stays disconnected by default for all six pages", () => {
  for (const page of PAGE_IDS) {
    assert.deepEqual(buildResearchUrl(INTEGRATION_CONFIG, page, "ITRN.US"), {
      connected: false, reason: "unverified_integration",
    });
  }
});

test("adapter sends only exact reviewed keys and normalized symbol, on each configured page", () => {
  const fixture = configuredFixture();
  for (const page of PAGE_IDS) {
    const result = buildResearchUrl(fixture, page, " itrn.us ");
    assert.equal(result.connected, true);
    const url = new URL(result.url);
    assert.equal(url.pathname.endsWith(`fixture_${page}`), true);
    assert.deepEqual(JSON.parse(url.searchParams.get("params")), {
      "fixture.parameter_one": "ITRN.US", "fixture.parameter_two": "ITRN.US",
    });
  }
});

test("unverified or incomplete integration cannot fall back to another page or unfiltered URL", () => {
  for (const key of ["verifiedAt", "nativeSymbolControlsDisabled", "nativePageNavigationDisabled", "allPagesTested"]) {
    const fixture = configuredFixture();
    fixture.verification[key] = key === "verifiedAt" ? "invalid" : false;
    assert.equal(buildResearchUrl(fixture, "news", "ITRN.US").connected, false);
  }
  const fixture = configuredFixture();
  delete fixture.embedPageUrls.news;
  assert.equal(buildResearchUrl(fixture, "news", "ITRN.US").connected, false);
  assert.equal(buildResearchUrl(fixture, "unknown", "ITRN.US").connected, false);
});

test("adapter rejects unsafe destinations, report mismatch and unreviewed parameter keys", () => {
  const fixture = configuredFixture();
  const valid = fixture.embedPageUrls.news;
  const unsafeUrls = [
    valid.replace("https:", "http:"),
    valid.replace("lookerstudio.google.com", "lookerstudio.google.com.evil.test"),
    valid.replace("https://", "https://name:password@"),
    valid.replace(".com/", ".com:8443/"),
    `${valid}#fragment`,
    valid.replace("11111111-2222-3333-4444-555555555555", "66666666-2222-3333-4444-555555555555"),
    valid.replace("/embed/", "/"),
    valid.replace("fixture_news", ""),
    "javascript:alert(1)",
  ];
  for (const url of unsafeUrls) {
    fixture.embedPageUrls.news = url;
    assert.equal(buildResearchUrl(fixture, "news", "ITRN.US").connected, false, url);
  }
  fixture.embedPageUrls.news = valid;
  for (const keys of [undefined, [], ["same", "same"], ["bad key"], ["<script>"], [42]]) {
    fixture.parameterBindings.news = keys;
    assert.equal(buildResearchUrl(fixture, "news", "ITRN.US").connected, false);
  }
});
