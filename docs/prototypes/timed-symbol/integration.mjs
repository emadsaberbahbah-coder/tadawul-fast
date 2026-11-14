import { PAGE_IDS, normalizeSymbol } from "./state.mjs";

// Unconnected by design. Copy actual embed URLs and permitted parameter keys
// from the reviewed research report; never infer them from source field names.
export const INTEGRATION_CONFIG = Object.freeze({
  enabled: false,
  reportId: "dcc2db01-69ea-4561-81e1-73622b4d0bea",
  embedPageUrls: {},
  parameterBindings: {},
  verification: {
    verifiedAt: null,
    nativeSymbolControlsDisabled: false,
    nativePageNavigationDisabled: false,
    allPagesTested: false,
  },
});

// Verified existing viewer routes are reference links only. Opening one does
// not promise to apply this prototype's selection to the existing report.
export const CURRENT_REPORT_URLS = Object.freeze({
  overview: "https://datastudio.google.com/u/0/reporting/dcc2db01-69ea-4561-81e1-73622b4d0bea/page/p_htefzclv8d",
  fundamentals: "https://datastudio.google.com/u/0/reporting/dcc2db01-69ea-4561-81e1-73622b4d0bea/page/p_w28uy3vx8d",
  risk: "https://datastudio.google.com/u/0/reporting/dcc2db01-69ea-4561-81e1-73622b4d0bea/page/p_8ulh6zwx8d",
  price: "https://datastudio.google.com/u/0/reporting/dcc2db01-69ea-4561-81e1-73622b4d0bea/page/p_y3owhfex8d",
  ratings: "https://datastudio.google.com/u/0/reporting/dcc2db01-69ea-4561-81e1-73622b4d0bea/page/p_fn6t5fex8d",
  news: "https://datastudio.google.com/u/0/reporting/dcc2db01-69ea-4561-81e1-73622b4d0bea/page/p_miszsgex8d",
});

export function buildResearchUrl(config, page, rawSymbol) {
  if (!PAGE_IDS.includes(page)) return { connected: false, reason: "unknown_page" };
  const symbol = normalizeSymbol(rawSymbol);
  const verification = config?.verification;
  if (config?.enabled !== true || !verification?.verifiedAt ||
      verification.nativeSymbolControlsDisabled !== true ||
      verification.nativePageNavigationDisabled !== true ||
      verification.allPagesTested !== true) {
    return { connected: false, reason: "unverified_integration" };
  }
  const verifiedTime = Date.parse(verification.verifiedAt);
  if (!Number.isFinite(verifiedTime)) {
    return { connected: false, reason: "unverified_integration" };
  }
  const reportId = config.reportId;
  if (typeof reportId !== "string" || !/^[A-Za-z0-9_-]{16,128}$/.test(reportId)) {
    return { connected: false, reason: "invalid_report" };
  }
  let url;
  try { url = new URL(config.embedPageUrls?.[page]); }
  catch { return { connected: false, reason: "missing_embed_url" }; }
  if (url.protocol !== "https:" ||
      !["lookerstudio.google.com", "datastudio.google.com"].includes(url.hostname) ||
      url.username || url.password || url.port || url.hash ||
      !url.pathname.startsWith(`/embed/reporting/${reportId}/page/`) ||
      !url.pathname.slice(`/embed/reporting/${reportId}/page/`.length)) {
    return { connected: false, reason: "invalid_embed_url" };
  }
  const keys = config.parameterBindings?.[page];
  if (!Array.isArray(keys) || !keys.length || new Set(keys).size !== keys.length ||
      keys.some(key => typeof key !== "string" || !/^[A-Za-z0-9_.-]{1,128}$/.test(key))) {
    return { connected: false, reason: "unverified_parameter_keys" };
  }
  const params = Object.create(null);
  for (const key of keys) params[key] = symbol;
  url.searchParams.set("params", JSON.stringify(params));
  return { connected: true, url: url.href, page, symbol };
}
