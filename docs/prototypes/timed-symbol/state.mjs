// Reader-interface state only. This module never reads or changes investment data.
export const OVERRIDE_TTL_MS = 300_000;
export const DEFAULT_SYMBOL = "2222.SR";
export const PAGE_IDS = Object.freeze([
  "overview", "fundamentals", "risk", "price", "ratings", "news",
]);
export const STORAGE_KEY = "tfb.research.symbol-session.v1";

export function normalizeSymbol(value) {
  const symbol = typeof value === "string" ? value.trim().toUpperCase() : "";
  // Includes the source's exchange-qualified examples. Does not claim membership
  // of the universe: that must be checked by the connected symbol directory.
  if (!/^[A-Z0-9^][A-Z0-9^._=-]{0,47}$/.test(symbol)) {
    throw new Error("أدخل رمزًا صحيحًا بلا مسافات، مثل 2222.SR أو ITRN.US.");
  }
  return symbol;
}

function validPage(page) {
  if (!PAGE_IDS.includes(page)) throw new Error("صفحة البحث غير معروفة.");
  return page;
}

function validOverride(value, now) {
  if (!value || typeof value !== "object") return false;
  try { normalizeSymbol(value.symbol); } catch { return false; }
  return Number.isSafeInteger(value.createdAt) &&
    Number.isSafeInteger(value.expiresAt) &&
    value.createdAt <= now &&
    value.expiresAt - value.createdAt === OVERRIDE_TTL_MS &&
    value.expiresAt > now;
}

/** A tab-local session with absolute deadlines, never a shared user default. */
export class SymbolSession {
  constructor({ storage, now = Date.now } = {}) {
    this.storage = storage;
    this.now = now;
    this.lastNow = this.time();
    this.expiryEvents = new Set();
    this.state = {
      version: 1,
      masterSymbol: DEFAULT_SYMBOL,
      activePage: "overview",
      overrides: {},
    };
    this.restore();
  }

  time() {
    const value = this.now();
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new Error("تعذر تحديد وقت انتهاء المقارنة المؤقتة.");
    }
    return value;
  }

  restore() {
    let stored;
    try { stored = JSON.parse(this.storage?.getItem(STORAGE_KEY) ?? "null"); }
    catch { stored = null; }
    if (stored?.version === 1) {
      try { this.state.masterSymbol = normalizeSymbol(stored.masterSymbol); }
      catch { /* Use the explicit prototype default. */ }
      if (PAGE_IDS.includes(stored.activePage)) this.state.activePage = stored.activePage;
      if (stored.overrides && typeof stored.overrides === "object") {
        for (const page of PAGE_IDS) {
          const value = stored.overrides[page];
          if (page !== "overview" && validOverride(value, this.lastNow)) {
            const symbol = normalizeSymbol(value.symbol);
            if (symbol !== this.state.masterSymbol) {
              this.state.overrides[page] = {
                symbol, createdAt: value.createdAt, expiresAt: value.expiresAt,
              };
            }
          }
        }
      }
    }
    this.persist();
  }

  persist() {
    try { this.storage?.setItem(STORAGE_KEY, JSON.stringify(this.state)); }
    catch { /* Storage denial leaves an in-memory session, never a global fallback. */ }
  }

  refresh() {
    const now = this.time();
    const expiredPages = [];
    // A backwards system-clock change must not silently extend an override.
    const clockWentBack = now < this.lastNow;
    this.lastNow = now;
    for (const [page, value] of Object.entries(this.state.overrides)) {
      if (clockWentBack || !validOverride(value, now)) {
        delete this.state.overrides[page];
        expiredPages.push(page);
        this.expiryEvents.add(page);
      }
    }
    if (expiredPages.length) this.persist();
    return { now, expiredPages };
  }

  consumeExpiryEvents() {
    const pages = [...this.expiryEvents];
    this.expiryEvents.clear();
    return pages;
  }

  setMaster(value) {
    const symbol = normalizeSymbol(value);
    this.refresh();
    this.state.masterSymbol = symbol;
    this.state.overrides = {};
    this.persist();
    return this.snapshot();
  }

  setPage(page) {
    validPage(page);
    this.refresh();
    this.state.activePage = page;
    this.persist();
    return this.snapshot();
  }

  setOverride(page, value) {
    validPage(page);
    if (page === "overview") throw new Error("غيّر الرمز الأساسي من نظرة عامة.");
    const symbol = normalizeSymbol(value);
    const { now } = this.refresh();
    if (symbol === this.state.masterSymbol) {
      delete this.state.overrides[page];
    } else {
      this.state.overrides[page] = {
        symbol, createdAt: now, expiresAt: now + OVERRIDE_TTL_MS,
      };
    }
    this.persist();
    return this.snapshot();
  }

  clearOverride(page = this.state.activePage) {
    validPage(page);
    this.refresh();
    delete this.state.overrides[page];
    this.persist();
    return this.snapshot();
  }

  snapshot() {
    const { now } = this.refresh();
    const page = this.state.activePage;
    const override = this.state.overrides[page];
    return {
      version: 1,
      activePage: page,
      observedAt: now,
      masterSymbol: this.state.masterSymbol,
      effectiveSymbol: override?.symbol ?? this.state.masterSymbol,
      isOverride: Boolean(override),
      expiresAt: override?.expiresAt ?? null,
      remainingMs: override ? Math.max(0, override.expiresAt - now) : 0,
      overrides: structuredClone(this.state.overrides),
    };
  }
}

/** Re-check on every timer callback and focus; callbacks do not define expiry. */
export class SessionTicker {
  constructor(session, render, {
    setTimeoutFn = setTimeout, clearTimeoutFn = clearTimeout,
  } = {}) {
    this.session = session;
    this.render = render;
    this.setTimeoutFn = setTimeoutFn;
    this.clearTimeoutFn = clearTimeoutFn;
    this.timer = null;
    this.running = false;
  }

  tick() {
    if (!this.running) return;
    if (this.timer !== null) this.clearTimeoutFn(this.timer);
    const snapshot = this.session.snapshot();
    this.render(snapshot, this.session.consumeExpiryEvents());
    const deadlines = Object.values(snapshot.overrides).map(item => item.expiresAt);
    const untilNext = deadlines.length ? Math.min(...deadlines) - snapshot.observedAt : 1000;
    this.timer = this.setTimeoutFn(() => this.tick(), Math.max(1, Math.min(1000, untilNext)));
  }

  start() { this.running = true; this.tick(); }
  focus() { if (this.running) this.tick(); }
  stop() {
    this.running = false;
    if (this.timer !== null) this.clearTimeoutFn(this.timer);
    this.timer = null;
  }
}
