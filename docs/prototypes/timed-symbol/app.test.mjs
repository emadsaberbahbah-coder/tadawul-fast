import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

// Minimal event/DOM harness for the real app module, not a browser-layout test.
class Element {
  constructor(tagName = "div") {
    this.tagName = tagName; this.children = []; this.listeners = new Map();
    this.dataset = {}; this.attributes = new Map(); this.value = "";
    this.textContent = ""; this.hidden = false; this.disabled = false;
    const classes = new Set();
    this.classList = { toggle(name, on) { if (on) classes.add(name); else classes.delete(name); } };
  }
  append(...items) { this.children.push(...items); }
  replaceChildren(...items) { this.children = [...items]; }
  setAttribute(key, value) { this.attributes.set(key, value); }
  removeAttribute(key) { this.attributes.delete(key); }
  querySelectorAll(tag) { return this.children.filter(item => item.tagName === tag); }
  addEventListener(type, callback) {
    if (!this.listeners.has(type)) this.listeners.set(type, []);
    this.listeners.get(type).push(callback);
  }
  dispatch(type) { for (const callback of this.listeners.get(type) ?? []) callback({ preventDefault() {} }); }
  showModal() { this.open = true; }
  close() { this.open = false; }
}

test("real UI preserves drafts, expires comparisons, and never embeds unverified data", async () => {
  const html = await readFile(new URL("./index.html", import.meta.url), "utf8");
  const elements = new Map([...html.matchAll(/id="([^"]+)"/g)].map(([, id]) => [id, new Element()]));
  const document = new Element();
  document.getElementById = id => {
    assert.ok(elements.has(id), `app references an HTML element: ${id}`);
    return elements.get(id);
  };
  document.createElement = tag => new Element(tag);
  document.visibilityState = "visible";
  const window = new Element();
  const stored = new Map();
  window.sessionStorage = { getItem: key => stored.get(key) ?? null, setItem: (key, value) => stored.set(key, value) };
  let now = 1_800_000_000_000;
  let nextTimer = 0;
  const timers = new Map();
  const globals = { document: globalThis.document, window: globalThis.window,
    setTimeout: globalThis.setTimeout, clearTimeout: globalThis.clearTimeout, now: Date.now };
  const byId = id => elements.get(id);
  const tickAfter = elapsed => {
    now += elapsed;
    for (const [id, timer] of [...timers]) {
      if (timer.at <= now && timers.delete(id)) timer.callback();
    }
  };
  try {
    globalThis.document = document;
    globalThis.window = window;
    globalThis.setTimeout = (callback, delay) => {
      const id = ++nextTimer; timers.set(id, { callback, at: now + delay }); return id;
    };
    globalThis.clearTimeout = id => timers.delete(id);
    Date.now = () => now;
    await import(`./app.mjs?dom-test=${now}`);

    assert.equal(byId("dataPlaceholder").hidden, false);
    assert.equal(byId("embedSection").hidden, true);
    assert.deepEqual(byId("embedHost").children, []);
    assert.equal(byId("navigation").children.length, 6);
    byId("openHelp").dispatch("click");
    assert.equal(byId("helpDialog").open, true);
    assert.ok(byId("helpBody").children.length > 0);
    byId("closeHelp").dispatch("click");

    // A keyboard viewer tabs away from the field, pauses, then presses Apply.
    byId("masterInput").value = "ITRN.US";
    document.activeElement = new Element("button");
    tickAfter(1100);
    assert.equal(byId("masterInput").value, "ITRN.US");
    byId("masterForm").dispatch("submit");
    assert.equal(byId("masterBadge").textContent, "ITRN.US");

    const go = page => byId("navigation").children.find(button => button.dataset.page === page).dispatch("click");
    go("news");
    assert.equal(byId("effectiveSymbol").textContent, "ITRN.US");
    byId("overrideInput").value = "AAPL.US";
    byId("overrideForm").dispatch("submit");
    assert.equal(byId("effectiveSymbol").textContent, "AAPL.US");
    assert.equal(byId("countdown").textContent, "05:00");
    go("risk");
    assert.equal(byId("effectiveSymbol").textContent, "ITRN.US");
    go("news");
    assert.equal(byId("effectiveSymbol").textContent, "AAPL.US");

    // Expiry must restore displayed selection without deleting a new draft.
    byId("overrideInput").value = "3333.SR";
    document.activeElement = byId("overrideInput");
    now += 300_000; // No timer callbacks while tab is backgrounded.
    document.dispatch("visibilitychange");
    assert.equal(byId("effectiveSymbol").textContent, "ITRN.US");
    assert.equal(byId("overrideInput").value, "3333.SR");
    assert.equal(byId("countdown").hidden, true);
    assert.match(byId("announcements").textContent, /انتهت المقارنة المؤقتة/);
    assert.deepEqual(byId("embedHost").children, []);

    byId("overrideForm").dispatch("submit");
    now += 300_001; // Apply again before a queued timer can render the expiry.
    byId("overrideForm").dispatch("submit");
    assert.equal(byId("effectiveSymbol").textContent, "3333.SR");
    assert.equal(byId("overrideInput").value, "3333.SR");
    assert.match(byId("announcements").textContent, /استخدم 3333.SR/);
    window.dispatch("pagehide");
    assert.equal(timers.size, 0);
    now += 300_001;
    window.dispatch("pageshow");
    assert.equal(byId("effectiveSymbol").textContent, "ITRN.US");
    assert.equal(timers.size, 1);
  } finally {
    globalThis.document = globals.document;
    globalThis.window = globals.window;
    globalThis.setTimeout = globals.setTimeout;
    globalThis.clearTimeout = globals.clearTimeout;
    Date.now = globals.now;
  }
});
