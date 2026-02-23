// sheets.js
const { google } = require("googleapis");
const cfg = require("./config");

// --------------------
// Cache (IMPORTANT)
// --------------------
let _auth = null;
let _sheets = null;

function getAuth() {
  if (_auth) return _auth;

  const json = Buffer.from(cfg.GOOGLE_SERVICE_ACCOUNT_JSON_B64, "base64").toString("utf-8");
  const key = JSON.parse(json);

  _auth = new google.auth.JWT({
    email: key.client_email,
    key: key.private_key,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  return _auth;
}

async function getSheetsClient() {
  if (_sheets) return _sheets;

  const auth = getAuth();
  // authorize() solo una vez
  await auth.authorize();

  _sheets = google.sheets({ version: "v4", auth });
  return _sheets;
}

// --------------------
// Read
// --------------------
async function readRange(rangeA1) {
  const sheets = await getSheetsClient();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: cfg.SHEETS_SPREADSHEET_ID,
    range: rangeA1,
  });
  return res.data.values || [];
}

// --------------------
// Append
// --------------------
async function appendRow(sheetName, rowValues) {
  const sheets = await getSheetsClient();
  await sheets.spreadsheets.values.append({
    spreadsheetId: cfg.SHEETS_SPREADSHEET_ID,
    range: `${sheetName}!A:Z`,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values: [rowValues] },
  });
}

async function appendRows(sheetName, rows) {
  if (!rows || rows.length === 0) return;

  const sheets = await getSheetsClient();
  await sheets.spreadsheets.values.append({
    spreadsheetId: cfg.SHEETS_SPREADSHEET_ID,
    range: `${sheetName}!A:Z`,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values: rows },
  });
}

// --------------------
// Update (single row/cell) - keep for compatibility
// --------------------
async function updateRow(sheetName, rowNumber1Based, rowValues) {
  const sheets = await getSheetsClient();
  await sheets.spreadsheets.values.update({
    spreadsheetId: cfg.SHEETS_SPREADSHEET_ID,
    range: `${sheetName}!A${rowNumber1Based}:Z${rowNumber1Based}`,
    valueInputOption: "RAW",
    requestBody: { values: [rowValues] },
  });
}

// col helper
function colToLetter(colNumber1Based) {
  let n = colNumber1Based;
  let s = "";
  while (n > 0) {
    const mod = (n - 1) % 26;
    s = String.fromCharCode(65 + mod) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

async function updateCell(sheetName, rowNumber1Based, colNumber1Based, value) {
  const sheets = await getSheetsClient();
  const colLetter = colToLetter(colNumber1Based);
  const range = `${sheetName}!${colLetter}${rowNumber1Based}:${colLetter}${rowNumber1Based}`;

  await sheets.spreadsheets.values.update({
    spreadsheetId: cfg.SHEETS_SPREADSHEET_ID,
    range,
    valueInputOption: "RAW",
    requestBody: { values: [[value]] },
  });
}

// --------------------
// ✅ NEW: Batch update MANY ranges in ONE request
// --------------------
// updates = [
//   { range: "Leads!A2:Z2", values: [rowArray] },
//   { range: "Leads!O55:O55", values: [[email]] },
// ]
async function batchUpdateValues(updates) {
  if (!updates || updates.length === 0) return;

  const sheets = await getSheetsClient();
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: cfg.SHEETS_SPREADSHEET_ID,
    requestBody: {
      valueInputOption: "RAW",
      data: updates,
    },
  });
}

// ✅ helper: batch update rows (A:Z) by row number
async function batchUpdateRows(sheetName, rowUpdates) {
  // rowUpdates = [{ rowNumber: 2, values: [...] }, ...]
  if (!rowUpdates || rowUpdates.length === 0) return;

  const data = rowUpdates.map(u => ({
    range: `${sheetName}!A${u.rowNumber}:Z${u.rowNumber}`,
    values: [u.values],
  }));

  return batchUpdateValues(data);
}

// ✅ helper: batch update single cells
async function batchUpdateCells(sheetName, cellUpdates) {
  // cellUpdates = [{ row: 2, col: 15, value: "a@b.com" }, ...]
  if (!cellUpdates || cellUpdates.length === 0) return;

  const data = cellUpdates.map(u => {
    const colLetter = colToLetter(u.col);
    const range = `${sheetName}!${colLetter}${u.row}:${colLetter}${u.row}`;
    return { range, values: [[u.value]] };
  });

  return batchUpdateValues(data);
}

module.exports = {
  readRange,
  appendRow,
  appendRows,
  updateRow,
  updateCell,
  batchUpdateValues,
  batchUpdateRows,
  batchUpdateCells,
};