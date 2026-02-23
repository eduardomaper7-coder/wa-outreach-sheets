// sheets.js
const { google } = require("googleapis");
const cfg = require("./config");

function getAuth() {
  const json = Buffer.from(cfg.GOOGLE_SERVICE_ACCOUNT_JSON_B64, "base64").toString("utf-8");
  const key = JSON.parse(json);

  return new google.auth.JWT({
    email: key.client_email,
    key: key.private_key,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
}

async function getSheetsClient() {
  const auth = getAuth();
  await auth.authorize();
  return google.sheets({ version: "v4", auth });
}

async function readRange(rangeA1) {
  const sheets = await getSheetsClient();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: cfg.SHEETS_SPREADSHEET_ID,
    range: rangeA1,
  });
  return res.data.values || [];
}

async function appendRow(sheetName, rowValues) {
  const sheets = await getSheetsClient();
  await sheets.spreadsheets.values.append({
    spreadsheetId: cfg.SHEETS_SPREADSHEET_ID,
    range: `${sheetName}!A:Z`,
    valueInputOption: "RAW",
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

async function updateRow(sheetName, rowNumber1Based, rowValues) {
  const sheets = await getSheetsClient();
  await sheets.spreadsheets.values.update({
    spreadsheetId: cfg.SHEETS_SPREADSHEET_ID,
    range: `${sheetName}!A${rowNumber1Based}:Z${rowNumber1Based}`,
    valueInputOption: "RAW",
    requestBody: { values: [rowValues] },
  });
}

// ✅ updateCell REAL
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

module.exports = { readRange, updateRow, appendRow, appendRows, updateCell };