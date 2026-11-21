# Timed symbol research interface — private prototype

This reviewable companion interface implements the requested symbol behavior outside the native report. It is **not published, not connected to report data, and not a production release**. Its default state shows an Arabic connection notice instead of an iframe. It contains no financial values, generated news, or trade execution controls.

Baseline: `90b9384108771f9fa366492971bf0c3cc4384018` (`main`, 2026-11-03). All changes are under this directory; model scores, sources, decision gates, broker connections, report content, and production services are untouched.

## Run and verify

From the repository root, serve only this prototype directory on localhost:

```sh
python3 -m http.server 8765 --bind 127.0.0.1 --directory docs/prototypes/timed-symbol
```

Open `http://127.0.0.1:8765/`. A local HTTP server is necessary for ES modules; opening the HTML as a `file:` URL is insufficient. The server does not publish the interface to the internet.

Run the dependency-free tests using Node 20 or later:

```sh
node --test docs/prototypes/timed-symbol/*.test.mjs
```

## Selection contract

| Action | Result |
| --- | --- |
| Apply a symbol in Symbol Overview | Set the master for all six research pages; clear every temporary override. |
| Apply another symbol on a research detail page | Override only that page until `createdAt + 300000` milliseconds. |
| Navigate away and return | Retain the original deadline; navigation never grants extra time. |
| Reach the deadline | Restore the current master, including overrides on pages that are not visible. |
| Refresh the companion interface | Restore valid tab-session state with the same deadline; discard expired state. |
| Return after a suspended/background tab or browser back/forward cache | Re-check time before the recovered render; do not depend on callbacks having run. |
| Manually return to master, or apply the master as the temporary symbol | Clear that page's override immediately. |

An interval only updates the display. Absolute wall-clock deadlines determine expiry. A browser can suspend JavaScript completely; the interface reconciles immediately when execution resumes. An observed backwards clock change cancels temporary selections rather than extending them. This is a navigation convenience, not a security timer.

State is held in `sessionStorage` for this origin and tab, never in shared server/account settings. If browser storage is denied, the interface continues in memory and cannot preserve state across reload. Browser-duplicated tabs can inherit an initial copy of session state, then evolve independently. The symbol input checks syntax only; a connected source's symbol directory must still validate coverage. `2222.SR` is an editable prototype default, not a recommendation.

## Pages and Arabic explanations

| Page | English research/chart terminology retained |
| --- | --- |
| نظرة عامة على الرمز | Symbol Overview; Engine Decision & Evidence; Earnings & Dividend Calendar |
| البيانات المالية والتقييم | Fundamentals & Valuation; Business Fundamentals; Valuation & Income |
| المخاطر والمؤشرات الفنية | Risk & Technical Indicators; Volatility; Drawdown; VaR; Beta; RSI; Sharpe |
| الرسوم السعرية | Relative Candlestick Chart (OHLC); Close & SMA 20 / 50 / 200; Volume & 20-Session Average |
| التقييم والأدلة | TFB Evidence Score & Components; Analyst Consensus |
| الأخبار والسجل التاريخي | News & Events; Signal History; Board Episode History |

Each page includes its purpose, reading guidance, limitations, and an Arabic explanation dialog. Source-specific explanations require revalidation if chart formulas, source fields, units, or periods change. Neutral navy/teal identifies the master state; amber plus explicit text and countdown identifies a temporary comparison. No unavailable financial number is assigned a success/failure color.

## Connection is deliberately disabled

`integration.mjs` contains the verified ordinary viewer routes for the six existing report pages. The explicit “فتح التقرير الحالي” link opens the existing report independently; it does **not** apply the companion's symbol, timeout, or page state.

`INTEGRATION_CONFIG.enabled` is `false`. No embed URLs or parameter keys have been invented. Tests use clearly fictional report/page IDs and parameter names solely to verify encoding and rejection behavior. They are not deployment examples or production bindings.

The adapter refuses to produce an iframe URL without all of the following:

- Enabled configuration and a recorded verification date.
- A generated, reviewed HTTPS embed URL on Google's Data Studio/Looker Studio host for the configured report.
- Exact permitted URL parameter keys for every relevant source on the requested page.
- Recorded verification that native symbol controls and internal page navigation cannot contradict the companion's selection and heading.
- Recorded verification of all six pages.

The flags record a completed human integration review; setting them does not prove that Google accepted the parameters. A cross-origin iframe cannot be inspected by this interface. The connected label therefore says **symbol sent to the report**, not “data verified for this symbol.” No iframe DOM scripting, credentials, permission grants, public sharing, or fallback to an unfiltered report is implemented.

## Native report hookup requirements — not performed

Use an isolated research-report copy for connection work. Keep the existing report and existing viewers' access unchanged while reviewing the route. Do not enable public sharing, add viewers, or create new credentials to make a prototype load.

1. Inventory the real symbol field, source alias, blends, and existing symbol filters for every chart on the six pages. Agree on one canonical source ticker format. Display names must not substitute for canonical tickers.
2. Create or select real text parameters and explicitly filter each source/chart to its parameter value. Account for parameter precedence at report, page, group, and component level. Remove or disable native symbol controls and symbol-changing chart interactions in the embedded copy so that they cannot conflict with the companion. A report-level dimension filter combined with a page filter is not a substitute for a temporary override.
3. In **Resource → Manage variables (parameters)**, permit URL changes only for the required symbol parameters. Copy each exact **URL parameter** identifier shown there into the page's binding array; do not infer it from a field/display name. The adapter emits URL-encoded JSON in the documented `params` query parameter.
4. Generate each page's embed URL from the report's **Embed report** dialog and copy it verbatim into `embedPageUrls`. An ordinary viewer/share link is not an embed link. Keep existing private access. Merely adding `/embed/` to known viewer routes is not accepted evidence.
5. Disable internal report navigation in the embedded copy and verify the result, including keyboard shortcuts, clickable navigation elements, and page links. **Theme and layout → Layout → Navigation type → Hidden** is the documented visual setting, but must not be assumed to block every navigation path. If effective page isolation cannot be achieved, keep the adapter disabled and use verified single-page reports with an explicitly revised adapter instead. Do not let the iframe silently show another page below the companion's heading.
6. Preserve useful native date/range controls on each research page, with units, ranges, currency, source age, and missing coverage stated correctly. The companion currently controls only symbol selection; it has no invented date/range binding. Native source cache/freshness settings remain authoritative.
7. Manually compare two supported symbols on **all six pages**, including different source types and missing-data cases. Confirm every chart, title, current-symbol display, news table, and source coverage indicator actually changes to the requested entity. Check there is no stale row or conflicting filter. Verify master changes, independent overrides, cross-page travel, exact expiry, background return, reload, browser history, narrow layouts, keyboard use, and Arabic dialog focus. Record screenshots and source timestamps.
8. Only after that evidence exists, populate the reviewed bindings, mark the verification flags, and separately review any proposed publishing change. No publishing or access change is authorized or performed by this prototype.

Embedded reports cannot be manually refreshed from inside the embed; the documented data freshness threshold governs automatic refresh. Reloading the companion or iframe does not prove that upstream data/cache was refreshed. The prototype adds no cache-bypass parameter and makes no freshness guarantee.

Primary documentation checked on 2026-11-03:

- [Parameters and URL-parameter permissions](https://docs.cloud.google.com/data-studio/parameters)
- [Embed a report, private access, generated links, and refresh limits](https://docs.cloud.google.com/data-studio/embed-a-report)
- [Report layout and navigation settings](https://docs.cloud.google.com/data-studio/report-and-page-layout)
- [Report navigation, including keyboard shortcuts](https://docs.cloud.google.com/data-studio/add-pages-and-report-navigation)

## Validation scope

The automated suite covers master propagation, independent overrides, the 299999/300000 ms boundary, hidden-page expiry, refresh persistence, focus after suspension, late callbacks, lifecycle stop/start, clock rollback, corrupt/tampered storage, denied storage, isolated sessions, unverified/unsafe URL rejection, and exact configured parameter encoding. A minimal DOM/event harness runs the actual app module to check draft preservation, page travel, help opening, visible expiry, and absence of an iframe in the default state.

These tests do not prove browser layout, live Google parameter application, source freshness, completeness of news, or financial correctness. No shared browser or live report was controlled for this prototype. Browser visual review and live integration remain pending.

## Files

- `index.html`: responsive Arabic presentation and help dialog.
- `app.mjs`: page help, forms, lifecycle events, and fail-visible embed presentation.
- `state.mjs`: selection state, storage sanitation, absolute expiry, retained expiry events, and timer controller.
- `integration.mjs`: disconnected config, existing independent viewer links, and validated adapter.
- `state.test.mjs`, `app.test.mjs`: fake-clock logic, adapter, and actual app-event tests.
