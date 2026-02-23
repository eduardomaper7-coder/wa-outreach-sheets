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

module.exports = { readRange, updateRow, appendRow, appendRows };