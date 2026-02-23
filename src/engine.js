const cron = require("node-cron");
const cfg = require("./config");
const { readRange, appendRow, updateRow } = require("./sheets");
const { scrapeZone } = require("./apify");
const { computeNextSendFrom, expectedNewSendsByNow } = require("./utils");
const { sendTemplate } = require("./twilio");

function isoNow() { return new Date().toISOString(); }

function statusCallbackUrl() {
  return `${cfg.PUBLIC_BASE_URL}/webhooks/status`;
}

// --- Helpers de Sheets (leer Leads y mapear filas)
async function getLeadsTable() {
  const values = await readRange("Leads!A:M");
  if (values.length < 1) return { header: [], rows: [] };

  const header = values[0];
  const rows = values.slice(1).map((row, idx) => {
    const obj = {};
    header.forEach((h, i) => obj[h] = row[i] ?? "");
    // rowNumber en sheet (1-based). +2 porque: header=1, primera fila de datos=2
    obj.__rowNumber = idx + 2;
    return obj;
  });

  return { header, rows };
}

function rowFromLeadObj(lead) {
  // Mantén el orden A..M
  return [
    lead.lead_id || "",
    lead.business_name || "",
    lead.zone || "",
    lead.whatsapp_e164 || "",
    lead.google_reviews || "",
    lead.google_rating || "",
    lead.source || "",
    lead.status || "",
    lead.last_outbound_at || "",
    lead.next_send_at || "",
    lead.msg1_sid || "",
    lead.msg2_sid || "",
    lead.msg3_sid || "",
  ];
}

async function upsertLeadByPhone(newLead) {
  const { rows } = await getLeadsTable();
  const existing = rows.find(r => r.whatsapp_e164 === newLead.whatsapp_e164);

  if (!existing) {
    // lead_id simple: timestamp+rand
    const lead_id = `L${Date.now()}${Math.floor(Math.random()*1000)}`;
    const lead = {
      lead_id,
      ...newLead,
      status: "NEW",
      last_outbound_at: "",
      next_send_at: "",
      msg1_sid: "",
      msg2_sid: "",
      msg3_sid: "",
    };
    await appendRow("Leads", rowFromLeadObj(lead));
    return { action: "insert" };
  }

  // si ya existe, actualiza datos “enriquecibles” sin pisar estado si ya está en conversación
  const keepStatus = existing.status && existing.status !== "NEW";
  const merged = {
    ...existing,
    business_name: newLead.business_name || existing.business_name,
    zone: newLead.zone || existing.zone,
    google_reviews: newLead.google_reviews ?? existing.google_reviews,
    google_rating: newLead.google_rating ?? existing.google_rating,
    source: newLead.source || existing.source,
    status: keepStatus ? existing.status : "NEW",
  };

  await updateRow("Leads", existing.__rowNumber, rowFromLeadObj(merged));
  return { action: "update" };
}

// --- 1) SCRAPING diario por zonas (Apify → Sheets)
async function dailyScrape() {
  for (const zone of cfg.APIFY_ZONES) {
    const leads = await scrapeZone(zone);
    for (const l of leads) {
      await upsertLeadByPhone(l);
    }
  }
}

// --- 2) ENVÍO de nuevos repartido
async function sendMsg1(lead) {
  const msg = await sendTemplate({
    toE164: lead.whatsapp_e164,
    contentSid: cfg.TPL_MSG1_SID,
    variables: { "1": String(lead.google_reviews || "") },
    statusCallbackUrl: statusCallbackUrl(),
  });

  const sentAt = isoNow();
  const next = computeNextSendFrom(sentAt, 48, cfg);

  lead.status = "MSG1_SENT";
  lead.last_outbound_at = sentAt;
  lead.next_send_at = next;
  lead.msg1_sid = msg.sid;

  await updateRow("Leads", lead.__rowNumber, rowFromLeadObj(lead));
}

function isTodayIso(iso) {
  if (!iso) return false;
  const d = new Date(iso);
  const now = new Date();
  return d.getFullYear() === now.getFullYear()
    && d.getMonth() === now.getMonth()
    && d.getDate() === now.getDate();
}

async function processNewLeadsPaced() {
  const now = new Date();
  const { rows } = await getLeadsTable();

  // cuenta cuántos MSG1 enviados hoy
  const msg1Today = rows.filter(r => r.msg1_sid && isTodayIso(r.last_outbound_at)).length;

  const shouldHaveSentByNow = expectedNewSendsByNow(now, cfg);
  const allowedToSendNow = Math.max(0, shouldHaveSentByNow - msg1Today);

  if (allowedToSendNow <= 0) return;

  const newLeads = rows
    .filter(r => r.status === "NEW")
    .slice(0, Math.min(allowedToSendNow, 3)); // cap por tick (suave). Ajusta si quieres.

  for (const lead of newLeads) {
    try {
      await sendMsg1(lead);
    } catch (e) {
      lead.status = "ERROR";
      await updateRow("Leads", lead.__rowNumber, rowFromLeadObj(lead));
    }
  }
}

// --- 3) FOLLOWUPS (MSG2 / MSG3) cuando toque
async function sendMsg2(lead) {
  const current = parseInt(lead.google_reviews || "0", 10);
  const target3m = Math.max(current + 30, Math.round(current * 1.5));

  const msg = await sendTemplate({
    toE164: lead.whatsapp_e164,
    contentSid: cfg.TPL_MSG2_SID,
    variables: { "1": lead.business_name, "2": String(current), "3": String(target3m) },
    statusCallbackUrl: statusCallbackUrl(),
  });

  const sentAt = isoNow();
  lead.status = "MSG2_SENT";
  lead.last_outbound_at = sentAt;
  lead.next_send_at = computeNextSendFrom(sentAt, 48, cfg);
  lead.msg2_sid = msg.sid;

  await updateRow("Leads", lead.__rowNumber, rowFromLeadObj(lead));
}

async function sendMsg3(lead) {
  const msg = await sendTemplate({
    toE164: lead.whatsapp_e164,
    contentSid: cfg.TPL_MSG3_SID,
    variables: {},
    statusCallbackUrl: statusCallbackUrl(),
  });

  const sentAt = isoNow();
  lead.status = "MSG3_SENT";
  lead.last_outbound_at = sentAt;
  lead.next_send_at = "";
  lead.msg3_sid = msg.sid;

  await updateRow("Leads", lead.__rowNumber, rowFromLeadObj(lead));
}

async function processDueFollowups() {
  const nowIso = isoNow();
  const { rows } = await getLeadsTable();

  const due = rows
    .filter(r => (r.status === "MSG1_SENT" || r.status === "MSG2_SENT"))
    .filter(r => r.next_send_at && r.next_send_at <= nowIso)
    .slice(0, 10); // cap por tick para no meter picos

  for (const lead of due) {
    // si respondió o lo paraste manualmente, no tocar
    if (lead.status === "REPLIED" || lead.status === "STOPPED") continue;

    try {
      if (lead.status === "MSG1_SENT") await sendMsg2(lead);
      else if (lead.status === "MSG2_SENT") await sendMsg3(lead);
    } catch (e) {
      lead.status = "ERROR";
      await updateRow("Leads", lead.__rowNumber, rowFromLeadObj(lead));
    }
  }
}

// --- Scheduler
function startEngine() {
  // Followups + pacing cada 5 min
  cron.schedule("*/5 * * * *", () => {
    processNewLeadsPaced().catch(() => {});
    processDueFollowups().catch(() => {});
  });

  // Scrape diario a la hora que elijas (ej. 06:00)
  cron.schedule(`0 ${cfg.APIFY_RUN_HOUR} * * *`, () => {
    dailyScrape().catch(() => {});
  });
}

module.exports = { startEngine, dailyScrape, upsertLeadByPhone };


