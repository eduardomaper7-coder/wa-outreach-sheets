require("dotenv").config();

function must(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

module.exports = {
  PORT: parseInt(process.env.PORT || "3000", 10),

  PUBLIC_BASE_URL: must("PUBLIC_BASE_URL"),

  TWILIO_ACCOUNT_SID: must("TWILIO_ACCOUNT_SID"),
  TWILIO_AUTH_TOKEN: must("TWILIO_AUTH_TOKEN"),
  TWILIO_WHATSAPP_FROM: must("TWILIO_WHATSAPP_FROM"),

  TPL_MSG1_SID: must("TPL_MSG1_SID"),
  TPL_MSG2_SID: must("TPL_MSG2_SID"),
  TPL_MSG3_SID: must("TPL_MSG3_SID"),

  SHEETS_SPREADSHEET_ID: must("SHEETS_SPREADSHEET_ID"),
  GOOGLE_SERVICE_ACCOUNT_JSON_B64: must("GOOGLE_SERVICE_ACCOUNT_JSON_B64"),

  APIFY_TOKEN: must("APIFY_TOKEN"),
  APIFY_ACTOR_ID: must("APIFY_ACTOR_ID"),
  APIFY_ZONES: (process.env.APIFY_ZONES || "").split(",").map(s => s.trim()).filter(Boolean),
  APIFY_RUN_HOUR: parseInt(process.env.APIFY_RUN_HOUR || "6", 10),

  DAILY_NEW_LIMIT: parseInt(process.env.DAILY_NEW_LIMIT || "100", 10),
  SEND_WINDOW_START: parseInt(process.env.SEND_WINDOW_START || "10", 10),
  SEND_WINDOW_END: parseInt(process.env.SEND_WINDOW_END || "19", 10),
  JITTER_MIN_HOURS: parseInt(process.env.JITTER_MIN_HOURS || "-6", 10),
  JITTER_MAX_HOURS: parseInt(process.env.JITTER_MAX_HOURS || "10", 10),
};