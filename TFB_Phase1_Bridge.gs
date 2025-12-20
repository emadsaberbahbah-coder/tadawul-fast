/**
 * TFB_Phase1_Bridge.gs
 * ------------------------------------------------------------
 * Phase-1 Bridge (Backend-first, DB-free)
 *
 * ✅ Reads YAML-driven config from backend:
 *    GET  /api/v1/config/{page_id}
 *
 * ✅ Builds sheet headers + data validation (basic, fast)
 *
 * ✅ Sends rows to backend for validation:
 *    POST /api/v1/{ksa|global}/ingest/{page_id}
 *    Body MUST be JSON ARRAY: [ {row}, {row} ]
 *
 * ✅ Appends validated_rows into History sheet (append-only):
 *    History_KSA / History_Global
 *
 * ------------------------------------------------------------
 * REQUIRED Script Properties:
 *   BACKEND_BASE_URL = https://tadawul-fast-bridge.onrender.com
 *   APP_TOKEN        = <same as Render APP_TOKEN>
 *
 * OPTIONAL Script Properties:
 *   HEADER_ROW       = "1"   (default 1)
 *   FIRST_DATA_ROW   = "2"   (default 2)
 *   MAX_VALIDATE_ROWS= "1000"(default 1000)
 *
 * ------------------------------------------------------------
 * IMPORTANT:
 * - This is Phase-1: we do NOT store DB history. We store in Google Sheets history.
 * - We standardize POST body as array of rows.
 */

const TFB = {
  PROP_BASE_URL: "BACKEND_BASE_URL",
  PROP_TOKEN: "APP_TOKEN",
  PROP_HEADER_ROW: "HEADER_ROW",
  PROP_FIRST_DATA_ROW: "FIRST_DATA_ROW",
  PROP_MAX_VALIDATE_ROWS: "MAX_VALIDATE_ROWS",

  HISTORY_KSA: "History_KSA",
  HISTORY_GLOBAL: "History_Global",
};

/** =========================
 *  PUBLIC FUNCTIONS (Run these)
 *  ========================= */

/**
 * 1) Build Headers + Validation for active sheet using backend YAML config
 * @param {string} pageId e.g. "page_01_market_summary_ksa"
 */
function tfbBuildFromConfig(pageId) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getActiveSheet();

  const cfg = tfbFetchConfig_(pageId);
  const cols = cfg.columns || [];
  if (!cols.length) throw new Error("No columns returned for: " + pageId);

  const headerRow = tfbNumProp_(TFB.PROP_HEADER_ROW, 1);
  const firstDataRow = tfbNumProp_(TFB.PROP_FIRST_DATA_ROW, 2);
  const maxRows = tfbNumProp_(TFB.PROP_MAX_VALIDATE_ROWS, 1000);

  // Set headers
  const headerValues = cols.map(c => (c.label || c.name || ""));
  sheet.getRange(headerRow, 1, 1, headerValues.length).setValues([headerValues]);
  sheet.setFrozenRows(headerRow);

  // Apply validations (limited rows for performance)
  for (let i = 0; i < cols.length; i++) {
    const col = cols[i];
    const colIndex = i + 1;
    const colLetter = tfbColToLetter_(colIndex);
    const cellRef = `${colLetter}${firstDataRow}`;

    const rule = tfbBuildValidationRule_(col, cellRef);
    const range = sheet.getRange(firstDataRow, colIndex, maxRows, 1);
    range.setDataValidation(rule);

    // Header note (helps debugging)
    const note = [
      `name: ${col.name}`,
      `type: ${col.type || "any"}`,
      `required: ${!!col.required}`,
      col.enum ? `enum: ${col.enum.join(", ")}` : null,
      col.ge !== undefined ? `ge: ${col.ge}` : null,
      col.le !== undefined ? `le: ${col.le}` : null,
      col.max_length !== undefined ? `max_length: ${col.max_length}` : null,
    ].filter(Boolean).join("\n");
    sheet.getRange(headerRow, colIndex).setNote(note);
  }

  // Basic UX
  sheet.getRange(headerRow, 1, 1, cols.length).setFontWeight("bold");
  sheet.getRange(headerRow, 1, 1, cols.length).setWrap(true);

  return { ok: true, page_id: cfg.page_id, schema_hash: cfg.schema_hash, columns: cols.length };
}

/**
 * 2) Ingest rows from active sheet -> backend validation -> append to History sheet
 * @param {"ksa"|"global"} region
 * @param {string} pageId
 */
function tfbIngestActiveSheet(region, pageId) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getActiveSheet();

  const cfg = tfbFetchConfig_(pageId);
  const cols = cfg.columns || [];
  if (!cols.length) throw new Error("No columns returned for: " + pageId);

  const headerRow = tfbNumProp_(TFB.PROP_HEADER_ROW, 1);
  const firstDataRow = tfbNumProp_(TFB.PROP_FIRST_DATA_ROW, 2);

  const lastRow = sheet.getLastRow();
  if (lastRow < firstDataRow) return { ok: true, message: "No data rows found." };

  const width = cols.length;
  const height = lastRow - firstDataRow + 1;

  // Read data block
  const values = sheet.getRange(firstDataRow, 1, height, width).getValues();

  // Convert to array of row objects (ONLY non-empty rows)
  const payloadRows = [];
  for (let r = 0; r < values.length; r++) {
    const row = values[r];
    if (tfbIsRowEmpty_(row)) continue;

    const obj = {};
    for (let c = 0; c < cols.length; c++) {
      const key = cols[c].name;
      const v = row[c];

      if (v === "" || v === null || v === undefined) continue;

      // date/datetime -> ISO strings
      const t = String(cols[c].type || "any").toLowerCase();
      if (v instanceof Date) {
        if (t === "date") obj[key] = Utilities.formatDate(v, "UTC", "yyyy-MM-dd");
        else if (t === "datetime") obj[key] = Utilities.formatDate(v, "UTC", "yyyy-MM-dd'T'HH:mm:ss'Z'");
        else obj[key] = v;
      } else {
        obj[key] = v;
      }
    }

    payloadRows.push(obj);
  }

  if (!payloadRows.length) return { ok: true, message: "No non-empty rows to ingest." };

  // POST array (Phase-1 contract)
  const endpoint = (region === "ksa")
    ? `/api/v1/ksa/ingest/${encodeURIComponent(pageId)}`
    : `/api/v1/global/ingest/${encodeURIComponent(pageId)}`;

  const res = tfbRequestJson_("post", endpoint, payloadRows);

  if (!res || res.ok !== true) {
    throw new Error("Backend ingest failed: " + JSON.stringify(res, null, 2));
  }

  // Append validated_rows to history
  const histName = (region === "ksa") ? TFB.HISTORY_KSA : TFB.HISTORY_GLOBAL;
  const hist = tfbGetOrCreateSheet_(ss, histName);
  tfbEnsureHistoryHeader_(hist, cols);

  const now = new Date();
  const schemaHash = res.schema_hash || cfg.schema_hash || "";

  const out = (res.validated_rows || []).map(vr => {
    const rowOut = [now, region, pageId, schemaHash];
    for (let i = 0; i < cols.length; i++) {
      const key = cols[i].name;
      rowOut.push((vr && Object.prototype.hasOwnProperty.call(vr, key)) ? vr[key] : "");
    }
    return rowOut;
  });

  if (out.length) {
    hist.getRange(hist.getLastRow() + 1, 1, out.length, out[0].length).setValues(out);
  }

  return {
    ok: true,
    region,
    page_id: pageId,
    schema_hash: schemaHash,
    sent_rows: payloadRows.length,
    accepted_count: res.accepted_count || 0,
    rejected_count: res.rejected_count || 0,
    history_appended: out.length,
  };
}

/**
 * Optional helper: list all page IDs from backend
 */
function tfbListPages() {
  return tfbRequestJson_("get", "/api/v1/pages");
}

/** =========================
 *  BACKEND CALLS
 *  ========================= */

function tfbFetchConfig_(pageId) {
  return tfbRequestJson_("get", `/api/v1/config/${encodeURIComponent(pageId)}`);
}

function tfbRequestJson_(method, path, payload) {
  const props = PropertiesService.getScriptProperties();
  const baseUrl = String(props.getProperty(TFB.PROP_BASE_URL) || "").replace(/\/+$/, "");
  const token = String(props.getProperty(TFB.PROP_TOKEN) || "");

  if (!baseUrl) throw new Error(`Missing Script Property: ${TFB.PROP_BASE_URL}`);
  if (!token) throw new Error(`Missing Script Property: ${TFB.PROP_TOKEN}`);

  const url = baseUrl + path;

  const options = {
    method: String(method || "get").toUpperCase(),
    muteHttpExceptions: true,
    headers: {
      "Accept": "application/json",
      "X-APP-TOKEN": token,
    },
  };

  if (payload !== undefined) {
    options.contentType = "application/json";
    options.payload = JSON.stringify(payload); // NOTE: sends array of rows
  }

  const resp = UrlFetchApp.fetch(url, options);
  const code = resp.getResponseCode();
  const text = resp.getContentText();

  let data;
  try { data = JSON.parse(text); } catch (e) { data = { raw: text }; }

  if (code >= 200 && code < 300) return data;

  return { ok: false, status: code, error: data, url };
}

/** =========================
 *  VALIDATION RULES (fast + simple)
 *  ========================= */

function tfbBuildValidationRule_(col, cellRefA1) {
  const required = !!col.required;
  const t = String(col.type || "any").toLowerCase();

  // Type base condition
  let cond = "TRUE";
  if (["number", "float", "decimal", "int", "integer", "date", "datetime"].includes(t)) {
    cond = `ISNUMBER(${cellRefA1})`;
  } else if (["bool", "boolean"].includes(t)) {
    cond = `ISLOGICAL(${cellRefA1})`;
  } else if (["string", "str"].includes(t)) {
    cond = `ISTEXT(${cellRefA1})`;
  }

  // Enum restriction
  if (Array.isArray(col.enum) && col.enum.length) {
    const arr = col.enum.map(v => `"${String(v).replace(/"/g, '""')}"`).join(",");
    cond = `ISNUMBER(MATCH(${cellRefA1}, {${arr}}, 0))`;
  }

  // Numeric bounds
  const extra = [];
  if (["number", "float", "decimal", "int", "integer"].includes(t)) {
    if (col.ge !== undefined) extra.push(`${cellRefA1}>=${Number(col.ge)}`);
    if (col.le !== undefined) extra.push(`${cellRefA1}<=${Number(col.le)}`);
  }

  // String length bounds
  if (["string", "str"].includes(t)) {
    if (col.max_length !== undefined) extra.push(`LEN(${cellRefA1})<=${Number(col.max_length)}`);
    if (col.min_length !== undefined) extra.push(`LEN(${cellRefA1})>=${Number(col.min_length)}`);
  }

  let allCond = cond;
  if (extra.length) allCond = `AND(${[cond].concat(extra).join(",")})`;

  // Required vs optional
  const finalFormula = required
    ? `AND(${cellRefA1}<>"", ${allCond})`
    : `OR(${cellRefA1}="", ${allCond})`;

  return SpreadsheetApp.newDataValidation()
    .requireFormulaSatisfied(finalFormula)
    .setAllowInvalid(false)
    .build();
}

/** =========================
 *  HISTORY HELPERS
 *  ========================= */

function tfbEnsureHistoryHeader_(sheet, cols) {
  const headers = ["ingested_at", "region", "page_id", "schema_hash"].concat(cols.map(c => c.name));
  const lastCol = headers.length;

  if (sheet.getLastRow() < 1) {
    sheet.getRange(1, 1, 1, lastCol).setValues([headers]);
    sheet.setFrozenRows(1);
    return;
  }

  const current = sheet.getRange(1, 1, 1, lastCol).getValues()[0];
  const mismatch = headers.some((h, i) => String(current[i] || "") !== h);
  if (mismatch) {
    sheet.clear();
    sheet.getRange(1, 1, 1, lastCol).setValues([headers]);
    sheet.setFrozenRows(1);
  }
}

function tfbGetOrCreateSheet_(ss, name) {
  let sh = ss.getSheetByName(name);
  if (!sh) sh = ss.insertSheet(name);
  return sh;
}

/** =========================
 *  UTILS
 *  ========================= */

function tfbNumProp_(name, def) {
  const v = PropertiesService.getScriptProperties().getProperty(name);
  const n = Number(v);
  return isFinite(n) && n > 0 ? n : def;
}

function tfbIsRowEmpty_(row) {
  for (let i = 0; i < row.length; i++) {
    const v = row[i];
    if (v !== "" && v !== null && v !== undefined) return false;
  }
  return true;
}

function tfbColToLetter_(col) {
  let n = col, s = "";
  while (n > 0) {
    const m = (n - 1) % 26;
    s = String.fromCharCode(65 + m) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}
