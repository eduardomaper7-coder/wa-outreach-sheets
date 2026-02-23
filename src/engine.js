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

  // Normalizamos los encabezados: quitamos espacios y pasamos a minúsculas
  // Esto evita que "Email " o "EMAIL" rompan la lógica de lead.email
  const rawHeader = values[0];
  const header = rawHeader.map(h => String(h).trim().toLowerCase());

  const rows = values.slice(1).map((row, idx) => {
    const obj = {};
    header.forEach((h, i) => {
      // Mapeamos el valor de la celda a la llave normalizada
      obj[h] = row[i] ?? "";
    });
    
    // Guardamos el número de fila real para las actualizaciones (updateRow)
    obj.__rowNumber = idx + 2;
    return obj;
  });

  // Log de diagnóstico para consola
  console.log(`[sheets] Tabla cargada: ${rows.length} leads. Columnas detectadas: ${header.join(', ')}`);

  return { header, rows };
}

function rowFromLeadObj(lead) {
  return [
    lead.lead_id || "",               // A
    lead.business_name || "",         // B
    lead.zone || "",                  // C
    lead.whatsapp_e164 || "",         // D
    lead.google_reviews || "",        // E
    lead.google_rating || "",         // F
    lead.source || "",                // G (Según tu log, source está antes)
    lead.website || "",               // H (Según tu log, website está aquí)
    lead.status || "",                // I
    lead.last_outbound_at || "",      // J
    lead.next_send_at || "",          // K
    lead.msg1_sid || "",              // L
    lead.msg2_sid || "",              // M
    lead.email || "",                 // N
    lead.stop_all || "",              // O
    lead.stop_reason || "",           // P
    lead.email_status || "",          // Q
    lead.email_last_outbound_at || "",// R
    lead.email_next_send_at || "",    // S
    lead.email1_id || "",             // T
    lead.email2_id || "",             // U
    lead.email3_id || "",             // V
    lead.email_reply_at || "",        // W
  ];
}

async function upsertLeadByPhone(newLead) {
  const { rows } = await getLeadsTable();
  const hoy = isoNow();

  // Limpiamos el teléfono que viene de Apify para la comparación
  const newPhoneClean = String(newLead.whatsapp_e164 || "").replace(/\D/g, "");

  // Buscamos si ya existe comparando solo los números
  const existing = rows.find(r => {
    const sheetPhoneClean = String(r.whatsapp_e164 || "").replace(/\D/g, "");
    return sheetPhoneClean === newPhoneClean && sheetPhoneClean !== "";
  });

  // --- CASO 1: EL LEAD NO EXISTE (NUEVO) ---
  if (!existing) {
    const lead_id = `L${Date.now()}${Math.floor(Math.random() * 1000)}`;
    const lead = {
      lead_id,
      ...newLead,
      // WhatsApp: Empieza Hoy (Día 1)
      status: "NEW",
      last_outbound_at: "",
      next_send_at: hoy, 
      msg1_sid: "",
      msg2_sid: "",
      msg3_sid: "",
      // Email: Empieza Hoy (Día 1)
      stop_all: "FALSE",
      stop_reason: "",
      email_status: "EMAIL_NEW",
      email_last_outbound_at: "",
      email_next_send_at: hoy,
      email1_id: "",
      email2_id: "",
      email3_id: "",
      email_reply_at: "",
    };

    console.log(`[engine] Insertando nuevo lead: ${newLead.business_name}`);
    await appendRow("Leads", rowFromLeadObj(lead));
    return { action: "insert" };
  }

  // --- CASO 2: EL LEAD YA EXISTE (MATCH POR TELÉFONO) ---
  
  // Verificamos si podemos añadir un email que antes no estaba
  const sheetEmail = String(existing.email || "").trim();
  const newEmail = String(newLead.email || "").trim();
  
  // Se activa si el de la hoja está vacío y el de Apify trae un email válido
  const shouldUpdateEmail = sheetEmail.length < 5 && newEmail.includes("@");

  const merged = {
    ...existing,
    // Actualizamos datos informativos
    business_name: newLead.business_name || existing.business_name,
    zone: newLead.zone || existing.zone,
    google_reviews: newLead.google_reviews ?? existing.google_reviews,
    google_rating: newLead.google_rating ?? existing.google_rating,
    website: newLead.website || existing.website,
    source: newLead.source || existing.source,
    
    // Si hay email nuevo, lo ponemos. Si no, dejamos el que había.
    email: shouldUpdateEmail ? newEmail : existing.email,

    // Si actualizamos email, ponemos estado EMAIL_NEW y fecha de HOY
    email_status: shouldUpdateEmail ? "EMAIL_NEW" : (existing.email_status || "EMAIL_NEW"),
    email_next_send_at: shouldUpdateEmail ? hoy : (existing.email_next_send_at || ""),
    
    // Mantenemos estado de WhatsApp
    status: existing.status || "NEW",
    stop_all: existing.stop_all || "FALSE"
  };

  // Solo hacemos el update en la hoja si realmente cambió algo (email o datos)
  console.log(`[engine] Actualizando lead: ${merged.business_name} ${shouldUpdateEmail ? "(CON EMAIL NUEVO)" : ""}`);
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
  // 1. Enviamos el template de WhatsApp (Día 1)
  const msg = await sendTemplate({
    toE164: lead.whatsapp_e164,
    contentSid: cfg.TPL_MSG1_SID,
    variables: { 
      "1": String(lead.google_reviews || "0") 
    },
    statusCallbackUrl: statusCallbackUrl(),
  });

  const sentAt = isoNow();
  
  // 2. Calculamos el próximo envío: Hoy + 48 horas = Día 3
  // Esto hace que el sistema ignore este lead el Día 2 y lo retome el Día 3.
  const nextDate = addHoursIso(sentAt, 48);

  // 3. Actualizamos el objeto lead con los nuevos datos
  lead.status = "MSG1_SENT";
  lead.last_outbound_at = sentAt;
  lead.next_send_at = nextDate; // Programado para el Día 3
  lead.msg1_sid = msg.sid;

  // 4. Guardamos los cambios en la Google Sheet
  console.log(`[engine] WhatsApp MSG1 enviado a ${lead.business_name}. Próximo WA: ${nextDate}`);
  await updateRow("Leads", lead.__rowNumber, rowFromLeadObj(lead));
}

// --- EMAILS (D0, +24h, +72h)
async function sendEmail1(lead) {
  if (!lead.email) {
    console.log(`[engine] Saltando Email1 para ${lead.business_name}: Sin dirección de email.`);
    return;
  }

  // 1. Definimos el contenido del correo (Día 1)
  const subject = `Ayuda para las reseñas de ${lead.business_name}`;
  const text = `Hola ${lead.business_name}, te escribía por aquí también para...`;
  const html = `<p>Hola ${lead.business_name},</p><p>Te escribía por aquí también para...</p>`;

  // 2. Enviamos el correo vía SendGrid
  const { messageId } = await sendEmail({
    to: lead.email,
    subject,
    text,
    html,
    customArgs: { lead_id: lead.lead_id, step: "email1" },
  });

  const sentAt = isoNow();

  // 3. Programamos el SIGUIENTE Email para el Día 2 (Hoy + 24 horas)
  const nextDate = addHoursIso(sentAt, 24);

  // 4. Actualizamos el objeto lead
  lead.email_status = "EMAIL1_SENT";
  lead.email_last_outbound_at = sentAt;
  lead.email_next_send_at = nextDate; // Esto dispara el Email 2 mañana
  lead.email1_id = messageId || "sent";

  // 5. Guardamos en la Google Sheet
  console.log(`[engine] Email1 enviado a ${lead.email}. Próximo Email (Día 2): ${nextDate}`);
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
  
  // CAMBIO AQUÍ: Usamos addHoursIso para asegurar el salto de 2 días exactos (Día 3 -> Día 5)
  lead.next_send_at = addHoursIso(sentAt, 48); 
  
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
  console.log(`[enrich] Analizando ${rows.length} filas...`);

  for (const lead of rows) {
    // Usamos la columna website que ahora existe
    const url = lead.website;

    if (!lead.email && url && url.includes("http") && !url.includes("google.com")) {
      console.log(`[enrich] Buscando email en web real: ${url}`);

      try {
        const email = await scrapeEmailFromWebsite(url);

        if (email) {
          lead.email = email;
          await updateRow("Leads", lead.__rowNumber, rowFromLeadObj(lead));
          console.log(`[enrich] ✅ EMAIL ENCONTRADO: ${email}`);
        } else {
          console.log(`[enrich] ❌ No se encontró email en la web.`);
        }
      } catch (e) {
        console.log(`[enrich] Error visitando ${url}: ${e.message}`);
      }
    }
  }

  console.log("[enrich] terminado.");
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