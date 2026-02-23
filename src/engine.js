// engine.js (COMPLETO con cambios: stop_all + email1/2/3 + followups email)
// Nota: asegúrate de que sheets.updateRow actualiza A:Z y que Leads! tiene columnas hasta Z.

const cron = require("node-cron");
const cfg = require("./config");
const { readRange, appendRow, updateRow } = require("./sheets");
const { scrapeZone } = require("./apify");
const { computeNextSendFrom, expectedNewSendsByNow, addHoursIso } = require("./utils");
const { sendTemplate } = require("./twilio");
const { sendEmail } = require("./sendgrid");

function isoNow() { return new Date().toISOString(); }

function statusCallbackUrl() {
  return `${cfg.PUBLIC_BASE_URL}/webhooks/status`;
}

// --- Helpers de Sheets (leer Leads y mapear filas)
async function getLeadsTable() {
  const values = await readRange("Leads!A:Z");
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
    // --- NUEVO (N..W)
    lead.email || "",
    lead.stop_all || "",
    lead.stop_reason || "",
    lead.email_status || "",
    lead.email_last_outbound_at || "",
    lead.email_next_send_at || "",
    lead.email1_id || "",
    lead.email2_id || "",
    lead.email3_id || "",
    lead.email_reply_at || "",
  ];
}

async function upsertLeadByPhone(newLead) {
  const { rows } = await getLeadsTable();
  const existing = rows.find(r => r.whatsapp_e164 === newLead.whatsapp_e164);

  if (!existing) {
    // lead_id simple: timestamp+rand
    const lead_id = `L${Date.now()}${Math.floor(Math.random() * 1000)}`;
    const lead = {
      lead_id,
      ...newLead,

      // --- WhatsApp (existente)
      status: "NEW",
      last_outbound_at: "",
      next_send_at: "",
      msg1_sid: "",
      msg2_sid: "",
      msg3_sid: "",

      // --- Email + stop global (N..W)
      email: newLead.email || "",
      stop_all: "",
      stop_reason: "",
      email_status: "EMAIL_NEW",
      email_last_outbound_at: "",
      email_next_send_at: "",
      email1_id: "",
      email2_id: "",
      email3_id: "",
      email_reply_at: "",
    };

    await appendRow("Leads", rowFromLeadObj(lead));
    return { action: "insert" };
  }

  // si ya existe, actualiza datos “enriquecibles” sin pisar estado si ya está en conversación
  const keepStatus = existing.status && existing.status !== "NEW";

  // Si ya lo paraste o ya respondió, NO reactivarlo jamás al re-scrapear
  const alreadyStopped =
    String(existing.stop_all || "").toUpperCase() === "TRUE" ||
    existing.status === "REPLIED" ||
    existing.status === "STOPPED";

  const merged = {
    ...existing,

    // --- enrichment "seguro"
    business_name: newLead.business_name || existing.business_name,
    zone: newLead.zone || existing.zone,
    google_reviews: newLead.google_reviews ?? existing.google_reviews,
    google_rating: newLead.google_rating ?? existing.google_rating,
    source: newLead.source || existing.source,

    // --- email: si llega uno nuevo y el existente está vacío, lo rellenamos
    email: newLead.email || existing.email,

    // --- estados
    status: alreadyStopped ? existing.status : (keepStatus ? existing.status : "NEW"),

    // --- mantener stop/email state si ya existe
    stop_all: existing.stop_all || "",
    stop_reason: existing.stop_reason || "",
    email_status: existing.email_status || "EMAIL_NEW",
    email_last_outbound_at: existing.email_last_outbound_at || "",
    email_next_send_at: existing.email_next_send_at || "",
    email1_id: existing.email1_id || "",
    email2_id: existing.email2_id || "",
    email3_id: existing.email3_id || "",
    email_reply_at: existing.email_reply_at || "",
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

// --- 2) ENVÍO de nuevos repartido (WhatsApp MSG1)
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

// --- EMAILS (D0, +24h, +72h)
async function sendEmail1(lead) {
  if (!lead.email) return;

  const subject = `Ayuda rápida para subir reseñas en ${lead.business_name || "tu clínica"}`;
  const text = `Hola ${lead.business_name || ""}, ...`;
  const html = `<p>Hola ${lead.business_name || ""}, ...</p>`;

  const { messageId } = await sendEmail({
    to: lead.email,
    subject,
    text,
    html,
    customArgs: { lead_id: lead.lead_id, step: "email1" },
  });

  const sentAt = isoNow();
  lead.email_status = "EMAIL1_SENT";
  lead.email_last_outbound_at = sentAt;
  lead.email_next_send_at = addHoursIso(sentAt, 24);
  lead.email1_id = messageId || "sent";

  await updateRow("Leads", lead.__rowNumber, rowFromLeadObj(lead));
}

async function sendEmail2(lead) {
  if (!lead.email) return;

  const subject = `¿Lo revisaste, ${lead.business_name || ""}?`;
  const text = `Hola, solo hago seguimiento...`;
  const html = `<p>Hola, solo hago seguimiento...</p>`;

  const { messageId } = await sendEmail({
    to: lead.email,
    subject,
    text,
    html,
    customArgs: { lead_id: lead.lead_id, step: "email2" },
  });

  const sentAt = isoNow();
  lead.email_status = "EMAIL2_SENT";
  lead.email_last_outbound_at = sentAt;
  lead.email_next_send_at = addHoursIso(sentAt, 48); // para llegar a +72h total
  lead.email2_id = messageId || "sent";

  await updateRow("Leads", lead.__rowNumber, rowFromLeadObj(lead));
}

async function sendEmail3(lead) {
  if (!lead.email) return;

  const subject = `Último mensaje (prometo) 🙂`;
  const text = `Si quieres lo dejamos aquí...`;
  const html = `<p>Si quieres lo dejamos aquí...</p>`;

  const { messageId } = await sendEmail({
    to: lead.email,
    subject,
    text,
    html,
    customArgs: { lead_id: lead.lead_id, step: "email3" },
  });

  const sentAt = isoNow();
  lead.email_status = "EMAIL3_SENT";
  lead.email_last_outbound_at = sentAt;
  lead.email_next_send_at = "";
  lead.email3_id = messageId || "sent";

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

  const msg1Today = rows.filter(r => r.msg1_sid && isTodayIso(r.last_outbound_at)).length;

  const shouldHaveSentByNow = expectedNewSendsByNow(now, cfg);
  const allowedToSendNow = Math.max(0, shouldHaveSentByNow - msg1Today);

  const newCount = rows.filter(r =>
    r.status === "NEW" &&
    String(r.whatsapp_e164 || "").trim() &&
    String(r.stop_all || "").toUpperCase() !== "TRUE"
  ).length;

  console.log("[paced]",
    "now=", now.toISOString(),
    "msg1Today=", msg1Today,
    "shouldHaveSentByNow=", shouldHaveSentByNow,
    "allowedToSendNow=", allowedToSendNow,
    "newLeads=", newCount
  );

  if (allowedToSendNow <= 0) return;

  const newLeads = rows
    .filter(r =>
      r.status === "NEW" &&
      String(r.stop_all || "").toUpperCase() !== "TRUE"
    )
    .slice(0, Math.min(allowedToSendNow, 3));

  console.log("[paced] sending", newLeads.length, "leads");

  for (const lead of newLeads) {
    try {
      // WhatsApp D0
      await sendMsg1(lead);

      // Email D0 (si tiene email y no se ha enviado aún)
      if (
        lead.email &&
        String(lead.stop_all || "").toUpperCase() !== "TRUE" &&
        (!lead.email_status || lead.email_status === "EMAIL_NEW")
      ) {
        await sendEmail1(lead);
      }
    } catch (e) {
      console.error("[paced] send error", lead.whatsapp_e164, e?.message || e);
      lead.status = "ERROR";
      await updateRow("Leads", lead.__rowNumber, rowFromLeadObj(lead));
    }
  }
}

// --- 3) FOLLOWUPS WhatsApp (MSG2 / MSG3) cuando toque
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

    // stop global
    if (String(lead.stop_all || "").toUpperCase() === "TRUE") continue;

    try {
      if (lead.status === "MSG1_SENT") await sendMsg2(lead);
      else if (lead.status === "MSG2_SENT") await sendMsg3(lead);
    } catch (e) {
      lead.status = "ERROR";
      await updateRow("Leads", lead.__rowNumber, rowFromLeadObj(lead));
    }
  }
}

// --- 4) FOLLOWUPS Email (Email2 / Email3) cuando toque
async function processDueEmailFollowups() {
  const nowIso = isoNow();
  const { rows } = await getLeadsTable();

  const due = rows
    .filter(r => String(r.stop_all || "").toUpperCase() !== "TRUE")
    .filter(r => r.email && r.email_next_send_at && r.email_next_send_at <= nowIso)
    .filter(r => r.email_status === "EMAIL1_SENT" || r.email_status === "EMAIL2_SENT")
    .slice(0, 10);

  for (const lead of due) {
    // si respondió o lo paraste manualmente, no tocar
    if (lead.status === "REPLIED" || lead.status === "STOPPED") continue;
    if (String(lead.stop_all || "").toUpperCase() === "TRUE") continue;

    try {
      if (lead.email_status === "EMAIL1_SENT") await sendEmail2(lead);
      else if (lead.email_status === "EMAIL2_SENT") await sendEmail3(lead);
    } catch (e) {
      // no marques status global ERROR por fallo de email
      lead.email_status = "EMAIL_ERROR";
      await updateRow("Leads", lead.__rowNumber, rowFromLeadObj(lead));
    }
  }
}
// --- ENRICH: extraer emails para leads existentes sin email
const { scrapeEmailFromWebsite } = require("./emailExtractor");

async function enrichExistingLeadsEmails() {
  const { rows } = await getLeadsTable();

  for (const lead of rows) {
    if (
      !lead.email &&
      lead.website &&
      String(lead.stop_all || "").toUpperCase() !== "TRUE"
    ) {
      console.log("[enrich] buscando email en", lead.website);

      try {
        const email = await scrapeEmailFromWebsite(lead.website);

        if (email) {
          lead.email = email;
          await updateRow("Leads", lead.__rowNumber, rowFromLeadObj(lead));
          console.log("[enrich] encontrado:", email);
        }
      } catch (e) {
        console.log("[enrich] error:", e.message);
      }
    }
  }

  console.log("[enrich] terminado");
}
// --- Scheduler
function startEngine() {
  // Followups + pacing cada 5 min
  cron.schedule("*/5 * * * *", () => {
    console.log("[cron] tick", new Date().toISOString());

    processNewLeadsPaced().catch(console.error);
    processDueFollowups().catch(console.error);
    processDueEmailFollowups().catch(console.error);

  }, { timezone: "Europe/Madrid" });

  // Scrape diario
  cron.schedule(`0 ${cfg.APIFY_RUN_HOUR} * * *`, () => {
    console.log("[cron] daily scrape", new Date().toISOString());

    dailyScrape().catch(console.error);

  }, { timezone: "Europe/Madrid" });
}

module.exports = {
  startEngine,
  dailyScrape,
  upsertLeadByPhone,
  processNewLeadsPaced,
  processDueFollowups,
  processDueEmailFollowups,
  enrichExistingLeadsEmails,
};