/**
 * TFB_Settings_Wizard.gs
 * ------------------------------------------------------------
 * One-click setup wizard for Script Properties used by Phase-1.
 *
 * Sets / updates:
 *  - BACKEND_BASE_URL
 *  - APP_TOKEN
 *  - HEADER_ROW
 *  - FIRST_DATA_ROW
 *  - MAX_VALIDATE_ROWS
 *
 * Safe: does NOT overwrite existing values unless user confirms.
 */

function tfbSettingsWizard_Run() {
  const ui = SpreadsheetApp.getUi();
  const props = PropertiesService.getScriptProperties();

  const currentBase = String(props.getProperty("BACKEND_BASE_URL") || "");
  const currentToken = String(props.getProperty("APP_TOKEN") || "");
  const currentHeader = String(props.getProperty("HEADER_ROW") || "1");
  const currentFirst = String(props.getProperty("FIRST_DATA_ROW") || "2");
  const currentMax = String(props.getProperty("MAX_VALIDATE_ROWS") || "1000");

  // 1) BASE URL
  const baseResp = ui.prompt(
    "TFB Settings Wizard",
    `BACKEND_BASE_URL\n\nCurrent:\n${currentBase || "(empty)"}\n\nEnter new value (example: https://tadawul-fast-bridge.onrender.com)`,
    ui.ButtonSet.OK_CANCEL
  );
  if (baseResp.getSelectedButton() !== ui.Button.OK) return;
  const newBase = String(baseResp.getResponseText() || "").trim();

  // 2) TOKEN
  const tokenResp = ui.prompt(
    "TFB Settings Wizard",
    `APP_TOKEN\n\nCurrent:\n${currentToken ? "(set)" : "(empty)"}\n\nEnter your APP_TOKEN (same as Render env var)`,
    ui.ButtonSet.OK_CANCEL
  );
  if (tokenResp.getSelectedButton() !== ui.Button.OK) return;
  const newToken = String(tokenResp.getResponseText() || "").trim();

  // 3) HEADER ROW
  const headerResp = ui.prompt(
    "TFB Settings Wizard",
    `HEADER_ROW\n\nCurrent: ${currentHeader}\n\nEnter header row number (default 1)`,
    ui.ButtonSet.OK_CANCEL
  );
  if (headerResp.getSelectedButton() !== ui.Button.OK) return;
  const newHeader = String(headerResp.getResponseText() || "").trim() || "1";

  // 4) FIRST DATA ROW
  const firstResp = ui.prompt(
    "TFB Settings Wizard",
    `FIRST_DATA_ROW\n\nCurrent: ${currentFirst}\n\nEnter first data row number (default 2)`,
    ui.ButtonSet.OK_CANCEL
  );
  if (firstResp.getSelectedButton() !== ui.Button.OK) return;
  const newFirst = String(firstResp.getResponseText() || "").trim() || "2";

  // 5) MAX VALIDATE ROWS
  const maxResp = ui.prompt(
    "TFB Settings Wizard",
    `MAX_VALIDATE_ROWS\n\nCurrent: ${currentMax}\n\nEnter max rows to apply validation (default 1000)`,
    ui.ButtonSet.OK_CANCEL
  );
  if (maxResp.getSelectedButton() !== ui.Button.OK) return;
  const newMax = String(maxResp.getResponseText() || "").trim() || "1000";

  // Prepare changes
  const changes = {
    BACKEND_BASE_URL: newBase,
    APP_TOKEN: newToken,
    HEADER_ROW: newHeader,
    FIRST_DATA_ROW: newFirst,
    MAX_VALIDATE_ROWS: newMax,
  };

  // Confirm overwrites if needed
  const willOverwrite = (
    (currentBase && currentBase !== newBase) ||
    (currentToken && currentToken !== newToken) ||
    (currentHeader && currentHeader !== newHeader) ||
    (currentFirst && currentFirst !== newFirst) ||
    (currentMax && currentMax !== newMax)
  );

  if (willOverwrite) {
    const confirm = ui.alert(
      "Confirm Update",
      "Some settings already exist and will be updated.\n\nProceed?",
      ui.ButtonSet.YES_NO
    );
    if (confirm !== ui.Button.YES) return;
  }

  props.setProperties(changes, true);

  ui.alert(
    "Saved ✅",
    "Script Properties updated:\n\n" + JSON.stringify(
      { BACKEND_BASE_URL: newBase, APP_TOKEN: newToken ? "(set)" : "(empty)", HEADER_ROW: newHeader, FIRST_DATA_ROW: newFirst, MAX_VALIDATE_ROWS: newMax },
      null,
      2
    ),
    ui.ButtonSet.OK
  );

  // Quick connection test
  try {
    const pages = tfbListPages();
    ui.alert("Connection Test ✅", JSON.stringify(pages, null, 2), ui.ButtonSet.OK);
  } catch (e) {
    ui.alert("Connection Test Failed", String(e && e.message ? e.message : e), ui.ButtonSet.OK);
  }
}

/**
 * Convenience: show current Script Properties (safe display)
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
      { BACKEND_BASE_URL: base, APP_TOKEN: tokenSet ? "(set)" : "(empty)", HEADER_ROW: header, FIRST_DATA_ROW: first, MAX_VALIDATE_ROWS: max },
      null,
      2
    ),
    ui.ButtonSet.OK
  );
}
