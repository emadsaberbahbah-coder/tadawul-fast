/**
 * TFB_Settings_Wizard.gs
 * ------------------------------------------------------------
 * REVIEWED + ENHANCED (Phase-1 safe)
 *
 * Improvements:
 *  - Input validation (URL, row numbers, MAX_VALIDATE_ROWS)
 *  - Optional "Skip unchanged" behavior
 *  - Better error formatting
 *  - Tests BOTH /health (no token) and /api/v1/pages (token)
 *  - Never breaks if backend is down (shows readable message)
 *
 * Sets / updates Script Properties:
 *  - BACKEND_BASE_URL
 *  - APP_TOKEN
 *  - HEADER_ROW
 *  - FIRST_DATA_ROW
 *  - MAX_VALIDATE_ROWS
 */

function tfbSettingsWizard_Run() {
  const ui = SpreadsheetApp.getUi();
  const props = PropertiesService.getScriptProperties();

  const current = {
    BACKEND_BASE_URL: String(props.getProperty("BACKEND_BASE_URL") || ""),
    APP_TOKEN: String(props.getProperty("APP_TOKEN") || ""),
    HEADER_ROW: String(props.getProperty("HEADER_ROW") || "1"),
    FIRST_DATA_ROW: String(props.getProperty("FIRST_DATA_ROW") || "2"),
    MAX_VALIDATE_ROWS: String(props.getProperty("MAX_VALIDATE_ROWS") || "1000"),
  };

  // 1) BACKEND_BASE_URL
  const newBase = tfbWiz_prompt_(
    "TFB Settings Wizard",
    `BACKEND_BASE_URL\n\nCurrent:\n${current.BACKEND_BASE_URL || "(empty)"}\n\nExample:\nhttps://tadawul-fast-bridge.onrender.com`,
    current.BACKEND_BASE_URL || "https://tadawul-fast-bridge.onrender.com"
  );
  if (newBase === null) return;

  const baseClean = tfbWiz_normalizeBaseUrl_(newBase);
  if (!baseClean) {
    ui.alert("Invalid BACKEND_BASE_URL", "Please enter a valid https URL.", ui.ButtonSet.OK);
    return;
  }

  // 2) APP_TOKEN
  const newToken = tfbWiz_prompt_(
    "TFB Settings Wizard",
    `APP_TOKEN\n\nCurrent:\n${current.APP_TOKEN ? "(set)" : "(empty)"}\n\nEnter your APP_TOKEN (same as Render env var).`,
    current.APP_TOKEN || ""
  );
  if (newToken === null) return;

  const tokenClean = String(newToken || "").trim();

  // 3) HEADER_ROW
  const newHeader = tfbWiz_prompt_(
    "TFB Settings Wizard",
    `HEADER_ROW\n\nCurrent: ${current.HEADER_ROW}\n\nEnter header row number (default 1)`,
    current.HEADER_ROW || "1"
  );
  if (newHeader === null) return;

  const headerRow = tfbWiz_parsePosInt_(newHeader, 1);
  if (!headerRow) {
    ui.alert("Invalid HEADER_ROW", "Must be a positive integer (>= 1).", ui.ButtonSet.OK);
    return;
  }

  // 4) FIRST_DATA_ROW
  const newFirst = tfbWiz_prompt_(
    "TFB Settings Wizard",
    `FIRST_DATA_ROW\n\nCurrent: ${current.FIRST_DATA_ROW}\n\nEnter first data row number (default 2)`,
    current.FIRST_DATA_ROW || "2"
  );
  if (newFirst === null) return;

  const firstDataRow = tfbWiz_parsePosInt_(newFirst, 2);
  if (!firstDataRow) {
    ui.alert("Invalid FIRST_DATA_ROW", "Must be a positive integer (>= 1).", ui.ButtonSet.OK);
    return;
  }
  if (firstDataRow <= headerRow) {
    ui.alert("Invalid row config", "FIRST_DATA_ROW must be greater than HEADER_ROW.", ui.ButtonSet.OK);
    return;
  }

  // 5) MAX_VALIDATE_ROWS
  const newMax = tfbWiz_prompt_(
    "TFB Settings Wizard",
    `MAX_VALIDATE_ROWS\n\nCurrent: ${current.MAX_VALIDATE_ROWS}\n\nEnter max rows to apply validation (default 1000).`,
    current.MAX_VALIDATE_ROWS || "1000"
  );
  if (newMax === null) return;

  const maxValidateRows = tfbWiz_parsePosInt_(newMax, 1000);
  if (!maxValidateRows) {
    ui.alert("Invalid MAX_VALIDATE_ROWS", "Must be a positive integer (>= 1).", ui.ButtonSet.OK);
    return;
  }

  // Prepare changes
  const changes = {
    BACKEND_BASE_URL: baseClean,
    APP_TOKEN: tokenClean,
    HEADER_ROW: String(headerRow),
    FIRST_DATA_ROW: String(firstDataRow),
    MAX_VALIDATE_ROWS: String(maxValidateRows),
  };

  // Detect overwrite / unchanged
  const diffs = [];
  Object.keys(changes).forEach(k => {
    const oldV = String(current[k] || "");
    const newV = String(changes[k] || "");
    if (oldV !== newV) diffs.push(k);
  });

  if (diffs.length === 0) {
    ui.alert("No changes", "All settings are already up to date.", ui.ButtonSet.OK);
    // Still allow a quick test
    tfbSettingsWizard_TestConnection_();
    return;
  }

  // Confirm overwrite if needed
  const confirm = ui.alert(
    "Confirm Update",
    "These settings will be updated:\n\n- " + diffs.join("\n- ") + "\n\nProceed?",
    ui.ButtonSet.YES_NO
  );
  if (confirm !== ui.Button.YES) return;

  props.setProperties(changes, true);

  ui.alert(
    "Saved ✅",
    JSON.stringify(
      {
        BACKEND_BASE_URL: baseClean,
        APP_TOKEN: tokenClean ? "(set)" : "(empty)",
        HEADER_ROW: String(headerRow),
        FIRST_DATA_ROW: String(firstDataRow),
        MAX_VALIDATE_ROWS: String(maxValidateRows),
      },
      null,
      2
    ),
    ui.ButtonSet.OK
  );

  // Connection test
  tfbSettingsWizard_TestConnection_();
}

/**
 * Show current Script Properties (safe display)
 */
function tfbSettingsWizard_Show() {
  const ui = SpreadsheetApp.getUi();
  const props = PropertiesService.getScriptProperties();

  const base = String(props.getProperty("BACKEND_BASE_URL") || "");
  const tokenSet = !!props.getProperty("APP_TOKEN");
  const header = String(props.getProperty("HEADER_ROW") || "1");
  const first = String(props.getProperty("FIRST_DATA_ROW") || "2");
  const max = String(props.getProperty("MAX_VALIDATE_ROWS") || "1000");

  ui.alert(
    "TFB Script Properties",
    JSON.stringify(
      {
        BACKEND_BASE_URL: base,
        APP_TOKEN: tokenSet ? "(set)" : "(empty)",
        HEADER_ROW: header,
        FIRST_DATA_ROW: first,
        MAX_VALIDATE_ROWS: max,
      },
      null,
      2
    ),
    ui.ButtonSet.OK
  );
}

/** -------------------------
 * Internal: connection tests
 * ------------------------- */

function tfbSettingsWizard_TestConnection_() {
  const ui = SpreadsheetApp.getUi();
  try {
    // /health does not require token (backend side)
    const health = tfbWiz_tryHealth_();
    // /pages requires token
    const pages = tfbListPages();

    ui.alert(
      "Connection Test ✅",
      JSON.stringify({ health, pages }, null, 2),
      ui.ButtonSet.OK
    );
  } catch (e) {
    ui.alert("Connection Test Failed", tfbWiz_err_(e), ui.ButtonSet.OK);
  }
}

function tfbWiz_tryHealth_() {
  // Uses the same Script Properties as the bridge.
  const props = PropertiesService.getScriptProperties();
  const base = tfbWiz_normalizeBaseUrl_(String(props.getProperty("BACKEND_BASE_URL") || ""));
  if (!base) throw new Error("BACKEND_BASE_URL is missing/invalid.");

  const url = base + "/health";
  const res = UrlFetchApp.fetch(url, {
    method: "get",
    muteHttpExceptions: true,
    followRedirects: true,
  });

  const code = res.getResponseCode();
  const text = res.getContentText();
  let json = null;
  try { json = JSON.parse(text); } catch (_) {}

  if (code >= 200 && code < 300) {
    return json || { ok: true, raw: text };
  }
  return { ok: false, status_code: code, body: json || text };
}

/** -------------------------
 * Helpers
 * ------------------------- */

function tfbWiz_prompt_(title, message, defaultValue) {
  const ui = SpreadsheetApp.getUi();
  const resp = ui.prompt(title, message, ui.ButtonSet.OK_CANCEL);
  if (resp.getSelectedButton() !== ui.Button.OK) return null;
  const v = String(resp.getResponseText() || "").trim();
  // If user leaves empty, we fall back to defaultValue
  return v !== "" ? v : String(defaultValue || "").trim();
}

function tfbWiz_parsePosInt_(s, fallback) {
  const v = String(s || "").trim();
  if (!v) return Number(fallback || 1);
  const n = Number(v);
  if (!Number.isFinite(n)) return 0;
  const i = Math.floor(n);
  if (i < 1) return 0;
  return i;
}

function tfbWiz_normalizeBaseUrl_(u) {
  let v = String(u || "").trim();
  if (!v) return "";
  // remove trailing slashes
  while (v.endsWith("/")) v = v.slice(0, -1);
  // basic validation
  if (!/^https:\/\/[a-z0-9.-]+/i.test(v)) return "";
  return v;
}

function tfbWiz_err_(e) {
  try {
    if (!e) return "Unknown error";
    if (typeof e === "string") return e;
    if (e.message) return String(e.message);
    return JSON.stringify(e, null, 2);
  } catch (_) {
    return String(e);
  }
}
