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
    lead.source || "",                // G
    lead.website || "",               // H
    lead.status || "",                // I
    lead.last_outbound_at || "",      // J
    lead.next_send_at || "",          // K
    lead.msg1_sid || "",              // L
    lead.msg2_sid || "",              // M
    lead.msg3_sid || "",              // N  <-- TE FALTABA ESTA
    lead.email || "",                 // O  <-- AHORA EL EMAIL CAE EN LA "O"
    lead.stop_all || "",              // P
    lead.stop_reason || "",           // Q
    lead.email_status || "",          // R
    lead.email_last_outbound_at || "",// S
    lead.email_next_send_at || "",    // T
    lead.email1_id || "",             // U
    lead.email2_id || "",             // V
    lead.email3_id || "",             // W
    lead.email_reply_at || "",        // X
  ];
}

async function upsertLeadByPhone(newLead) {
  // ✅ Ahora hace UPSERT por TELÉFONO y, si no hay match, por EMAIL.
  // Mantengo el nombre de la función para que no tengas que cambiar llamadas.

  const { rows } = await getLeadsTable();
  const hoy = isoNow();

  const normEmail = (e) => String(e || "").trim().toLowerCase();
  const cleanPhone = (p) => String(p || "").replace(/\D/g, "");

  const newPhoneClean = cleanPhone(newLead.whatsapp_e164);
  const newEmailNorm = normEmail(newLead.email);

  console.log(
    `[DEBUG] Lead: ${newLead.business_name} | Phone=${newLead.whatsapp_e164 || ""} | Email=${newLead.email || ""}`
  );

  // 1) Match por teléfono
  let existing = null;
  if (newPhoneClean) {
    existing = rows.find((r) => {
      const sheetPhoneClean = cleanPhone(r.whatsapp_e164);
      return sheetPhoneClean && sheetPhoneClean === newPhoneClean;
    });
  }

  // 2) Si no hay match por teléfono, match por email
  if (!existing && newEmailNorm && newEmailNorm.includes("@")) {
    existing = rows.find((r) => normEmail(r.email) === newEmailNorm);
  }

  // --- CASO 1: NO EXISTE (NUEVO) ---
  if (!existing) {
    const lead_id = `L${Date.now()}${Math.floor(Math.random() * 1000)}`;

    const hasEmail = newEmailNorm.includes("@");

    const lead = {
      lead_id,
      ...newLead,

      // WhatsApp flow
      status: "NEW",
      last_outbound_at: "",
      next_send_at: hoy,
      msg1_sid: "",
      msg2_sid: "",
      msg3_sid: "",

      // Global stop
      stop_all: "FALSE",
      stop_reason: "",

      // Email flow (solo si hay email)
      email: hasEmail ? newEmailNorm : "",
      email_status: hasEmail ? "EMAIL_NEW" : "",
      email_last_outbound_at: "",
      email_next_send_at: hasEmail ? hoy : "",
      email1_id: "",
      email2_id: "",
      email3_id: "",
      email_reply_at: "",
    };

    console.log(`[engine] Insertando nuevo lead: ${lead.business_name} | email=${lead.email || ""}`);
    await appendRow("Leads", rowFromLeadObj(lead));
    return { action: "insert" };
  }

  // --- CASO 2: YA EXISTE (MATCH POR TELÉFONO O EMAIL) ---
  const sheetEmail = normEmail(existing.email);
  const sheetPhoneClean = cleanPhone(existing.whatsapp_e164);

  const shouldUpdateEmail =
    (!sheetEmail || sheetEmail.length < 5) && newEmailNorm.includes("@");

  const shouldUpdatePhone =
    (!sheetPhoneClean || sheetPhoneClean.length < 6) && newPhoneClean.length >= 9;

  const merged = {
    ...existing,

    // info
    business_name: newLead.business_name || existing.business_name,
    zone: newLead.zone || existing.zone,
    google_reviews: newLead.google_reviews ?? existing.google_reviews,
    google_rating: newLead.google_rating ?? existing.google_rating,
    website: newLead.website || existing.website,
    source: newLead.source || existing.source,

    // identifiers
    whatsapp_e164: shouldUpdatePhone ? newLead.whatsapp_e164 : existing.whatsapp_e164,
    email: shouldUpdateEmail ? newEmailNorm : existing.email,

    // si entra email nuevo, rearmamos el flujo email
    email_status: shouldUpdateEmail ? "EMAIL_NEW" : (existing.email_status || ""),
    email_next_send_at: shouldUpdateEmail ? hoy : (existing.email_next_send_at || ""),

    // mantener estados WA + stops
    status: existing.status || "NEW",
    stop_all: existing.stop_all || "FALSE",
  };

  console.log(
    `[engine] Actualizando lead: ${merged.business_name}` +
      `${shouldUpdateEmail ? " (EMAIL NUEVO)" : ""}` +
      `${shouldUpdatePhone ? " (PHONE NUEVO)" : ""}`
  );

  await updateRow("Leads", existing.__rowNumber, rowFromLeadObj(merged));
  return { action: "update" };
}

// --- EMAILS (D0, +24h, +72h)
async function sendEmail1(lead) {
  if (!lead.email) return;

  const business = lead.business_name || "";
  const reviews = lead.google_reviews || "0";

  const subject = `Más reseñas para ${business} (sin esfuerzo)`;

  const text = `
Hola,

He visto que tenéis ${reviews} reseñas en Google y creo que podemos ayudaros a conseguir muchas más, mejorando también vuestra valoración.

Hemos desarrollado un sistema que automatiza la recogida de reseñas por WhatsApp y analiza la opinión de vuestros pacientes para mejorar vuestra reputación online.

¿Te vendría bien mañana o pasado para una llamada rápida (10 min) y te explico cómo funciona?

Un saludo,
Eduardo
`;

  const html = `
<p>Hola,</p>

<p>He visto que tenéis <strong>${reviews}</strong> reseñas en Google y creo que podemos ayudaros a conseguir muchas más, mejorando también vuestra valoración.</p>

<p>Hemos desarrollado un sistema que automatiza la recogida de reseñas por WhatsApp y analiza la opinión de vuestros pacientes para mejorar vuestra reputación online.</p>

<p>¿Te vendría bien mañana o pasado para una llamada rápida (10 min) y te explico cómo funciona?</p>

<p>Un saludo,<br>Eduardo</p>
`;

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

  const business = lead.business_name || "";

  const subject = `¿Lo vemos esta semana, ${business}?`;

  const text = `
Hola,

Te escribo por si no viste mi mensaje anterior.

Creo que lo que estamos haciendo puede encajaros bien para aumentar reseñas sin más trabajo por vuestra parte.

¿Te va bien esta semana para comentarlo?

Un saludo,
Eduardo
`;

  const html = `
<p>Hola,</p>

<p>Te escribo por si no viste mi mensaje anterior.</p>

<p>Creo que lo que estamos haciendo puede encajaros bien para aumentar reseñas sin más trabajo por vuestra parte.</p>

<p>¿Te va bien esta semana para comentarlo?</p>

<p>Un saludo,<br>Eduardo</p>
`;

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
  lead.email_next_send_at = addHoursIso(sentAt, 48);
  lead.email2_id = messageId || "sent";

  await updateRow("Leads", lead.__rowNumber, rowFromLeadObj(lead));
}

async function sendEmail3(lead) {
  if (!lead.email) return;

  const subject = `¿Lo dejamos aquí?`;

  const text = `
No quiero ser insistente, así que este es mi último mensaje.

Si ahora no es buen momento, lo dejamos aquí sin problema.

Y si más adelante quieres mejorar vuestras reseñas, estaré encantado de ayudarte.

Un saludo,
Eduardo
`;

  const html = `
<p>No quiero ser insistente, así que este es mi último mensaje.</p>

<p>Si ahora no es buen momento, lo dejamos aquí sin problema.</p>

<p>Y si más adelante quieres mejorar vuestras reseñas, estaré encantado de ayudarte.</p>

<p>Un saludo,<br>Eduardo</p>
`;

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

function email1SentTodayCount(rows) {
  return rows.filter(r =>
    (r.email_status === "EMAIL1_SENT" || String(r.email1_id || "").trim()) &&
    isTodayIso(r.email_last_outbound_at)
  ).length;
}

async function sendMsg1(lead) {
  const msg = await sendTemplate({
    toE164: lead.whatsapp_e164,
    contentSid: cfg.TPL_MSG1_SID,
    variables: { "1": String(lead.google_reviews || "0") },
    statusCallbackUrl: statusCallbackUrl(),
  });

  const sentAt = isoNow();
  const nextDate = addHoursIso(sentAt, 48);

  lead.status = "MSG1_SENT";
  lead.last_outbound_at = sentAt;
  lead.next_send_at = nextDate;
  lead.msg1_sid = msg.sid;

  console.log(`[engine] WhatsApp MSG1 enviado a ${lead.business_name}. Próximo WA: ${nextDate}`);
  await updateRow("Leads", lead.__rowNumber, rowFromLeadObj(lead));
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
      String(r.whatsapp_e164 || "").trim() && 
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
async function processNewEmailsPaced() {
  const now = new Date();
  const nowIso = isoNow();
  const { rows } = await getLeadsTable();

  // Respetar la misma ventana horaria que WhatsApp
  const h = now.getHours();
  if (h < cfg.SEND_WINDOW_START || h >= cfg.SEND_WINDOW_END) return;

  // WhatsApp enviados hoy (MSG1)
  const msg1Today = rows.filter(r => r.msg1_sid && isTodayIso(r.last_outbound_at)).length;

  // Email1 enviados hoy
  const email1Today = email1SentTodayCount(rows);

  // ✅ Cupo de Email1 hoy = no superar los MSG1 de hoy
  const remainingToday = Math.max(0, msg1Today - email1Today);

  console.log("[email-sync]",
    "msg1Today=", msg1Today,
    "email1Today=", email1Today,
    "remainingEmail1Today=", remainingToday
  );

  if (remainingToday <= 0) return;

  // ✅ SOLO “catch-up” de leads que YA recibieron WhatsApp Día 1
  // y aún no tienen Email1 enviado.
  const due = rows
    .filter(r => String(r.stop_all || "").toUpperCase() !== "TRUE")
    .filter(r => r.email)
    .filter(r => r.status === "MSG1_SENT" || String(r.msg1_sid || "").trim()) // ya tuvo WhatsApp D1
    .filter(r => !r.email_status || r.email_status === "EMAIL_NEW") // no mandado Email1 aún
    .filter(r => !r.email_next_send_at || r.email_next_send_at <= nowIso)
    .slice(0, Math.min(remainingToday, 3)); // cap por tick

  for (const lead of due) {
    try {
      await sendEmail1(lead);
    } catch (e) {
      console.error("[email-paced] send error", lead.email, e?.message || e);
      lead.email_status = "EMAIL_ERROR";
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

async function dailyScrape() {
  for (const zone of cfg.APIFY_ZONES) {
    const leads = await scrapeZone(zone);
    for (const l of leads) {
      await upsertLeadByPhone(l); // (con tu upsert nuevo, este nombre ya vale)
    }
  }
}
// --- Scheduler
function startEngine() {
  // Followups + pacing cada 5 min
  cron.schedule("*/5 * * * *", () => {
    console.log("[cron] tick", new Date().toISOString());

    processNewLeadsPaced().catch(console.error);
    processDueFollowups().catch(console.error);
    processDueEmailFollowups().catch(console.error);
    processNewEmailsPaced().catch(console.error);

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