// server.js (COMPLETO con cambios: Leads!A:Z + stop_all on replies + inbound email webhook)

const express = require("express");
const twilio = require("twilio");
const multer = require("multer");
const cfg = require("./config");
const { startEngine } = require("./engine");
const { enrichExistingLeadsEmails } = require("./engine"); // 👈 AQUI
const { readRange, updateRow, appendRow, appendRows } = require("./sheets");
const { toE164Spain } = require("./utils");

const app = express();
const upload = multer(); // para SendGrid Inbound Parse (multipart/form-data)

app.use(express.urlencoded({ extended: false }));
app.use(express.json());


app.get("/test-enrich", async (req, res) => {
  try {
    await enrichExistingLeadsEmails();
    res.send("Enrich terminado");
  } catch (err) {
    console.error(err);
    res.status(500).send("Error en enrich");
  }
});
function validateTwilioRequest(req) {
  // Si lo quieres activar 100%: usa validateRequest con tu PUBLIC_BASE_URL
  // (si falla por proxy/railway headers, lo desactivas o lo ajustas)
  const signature = req.headers["x-twilio-signature"];
  const url = `${cfg.PUBLIC_BASE_URL}${req.originalUrl}`;
  return twilio.validateRequest(cfg.TWILIO_AUTH_TOKEN, signature, url, req.body);
}

async function findLeadRowByPhone(e164) {
  const values = await readRange("Leads!A:Z");
  const header = values[0] || [];
  const rows = values.slice(1);

  const idxPhone = header.indexOf("whatsapp_e164");
  if (idxPhone === -1) return null;

  for (let i = 0; i < rows.length; i++) {
    if ((rows[i][idxPhone] || "") === e164) {
      return { header, row: rows[i], rowNumber: i + 2 };
    }
  }
  return null;
}

async function findLeadRowByEmail(email) {
  const values = await readRange("Leads!A:Z");
  const header = values[0] || [];
  const rows = values.slice(1);

  const idxEmail = header.indexOf("email");
  if (idxEmail === -1) return null;

  const target = String(email || "").trim().toLowerCase();
  if (!target) return null;

  for (let i = 0; i < rows.length; i++) {
    const v = String(rows[i][idxEmail] || "").trim().toLowerCase();
    if (v && v === target) {
      return { header, row: rows[i], rowNumber: i + 2 };
    }
  }
  return null;
}

function updateLeadRowFromHeader(header, row, patch) {
  const out = [...row];
  for (const [k, v] of Object.entries(patch)) {
    const idx = header.indexOf(k);
    if (idx >= 0) out[idx] = v;
  }
  return out;
}

// INBOUND WhatsApp: si responde → status=REPLIED, stop_all=TRUE, next_send_at vacío
app.post("/webhooks/inbound", async (req, res) => {
  // opcional: si quieres validar firma Twilio
  // if (!validateTwilioRequest(req)) return res.status(403).send("Invalid signature");

  const from = String(req.body.From || ""); // whatsapp:+34...
  const body = String(req.body.Body || "");
  const e164 = from.replace(/^whatsapp:/, "");

  // registra inbound
  await appendRow("Inbound", [new Date().toISOString(), e164, body, JSON.stringify(req.body)]);

  const found = await findLeadRowByPhone(e164);
  if (found) {
    const { header, row, rowNumber } = found;
    const updated = updateLeadRowFromHeader(header, row, {
      status: "REPLIED",
      next_send_at: "",
      stop_all: "TRUE",
      stop_reason: "WA_REPLY",
      email_next_send_at: "", // para parar email también
    });
    await updateRow("Leads", rowNumber, updated);
  }

  res.status(200).type("text/xml").send("<Response></Response>");
});

// INBOUND Email (SendGrid Inbound Parse): si responde → parar email + whatsapp
// Configura en SendGrid Inbound Parse la URL: https://TU_PUBLIC_BASE_URL/webhooks/sendgrid/inbound
app.post("/webhooks/sendgrid/inbound", upload.none(), async (req, res) => {
  const fromRaw = String(req.body.from || "");
  const subject = String(req.body.subject || "");
  const text = String(req.body.text || "");
  const html = String(req.body.html || "");

  // extrae email del "from" (puede venir "Nombre <email@dominio.com>")
  const match = fromRaw.match(/<([^>]+)>/);
  const fromEmail = (match ? match[1] : fromRaw).trim().toLowerCase();

  // registra inbound email (crea sheet "InboundEmail" si no existe)
  await appendRow("InboundEmail", [
    new Date().toISOString(),
    fromEmail,
    subject,
    (text || "").slice(0, 500),
    (html || "").slice(0, 500),
  ]);

  const found = await findLeadRowByEmail(fromEmail);
  if (found) {
    const { header, row, rowNumber } = found;
    const updated = updateLeadRowFromHeader(header, row, {
      status: "REPLIED",
      next_send_at: "",
      stop_all: "TRUE",
      stop_reason: "EMAIL_REPLY",
      email_status: "REPLIED",
      email_next_send_at: "",
      email_reply_at: new Date().toISOString(),
    });
    await updateRow("Leads", rowNumber, updated);
  }

  res.status(200).send("ok");
});

// STATUS callback: si falla → marca ERROR (simple)
app.post("/webhooks/status", async (req, res) => {
  // if (!validateTwilioRequest(req)) return res.status(403).send("Invalid signature");

  const status = String(req.body.MessageStatus || "");
  const messageSid = String(req.body.MessageSid || "");

  if (status === "failed" || status === "undelivered") {
    // busca por SID (msg1/msg2/msg3) leyendo sheet (simple, vale para pocos miles)
    const values = await readRange("Leads!A:Z");
    const header = values[0] || [];
    const rows = values.slice(1);

    const idx1 = header.indexOf("msg1_sid");
    const idx2 = header.indexOf("msg2_sid");
    const idx3 = header.indexOf("msg3_sid");

    for (let i = 0; i < rows.length; i++) {
      if ((rows[i][idx1] === messageSid) || (rows[i][idx2] === messageSid) || (rows[i][idx3] === messageSid)) {
        const rowNumber = i + 2;
        const updated = updateLeadRowFromHeader(header, rows[i], { status: "ERROR" });
        await updateRow("Leads", rowNumber, updated);
        break;
      }
    }
  }

  res.status(200).type("text/xml").send("<Response></Response>");
});

app.get("/health", (req, res) => res.json({ ok: true }));

startEngine();
app.listen(cfg.PORT, () => console.log(`Listening on ${cfg.PORT}`));

app.get("/admin/scrape", async (req, res) => {
  const { dailyScrape } = require("./engine");
  await dailyScrape();
  res.send("scrape ok");
});

app.get("/admin/import-apify/:datasetId", async (req, res) => {
  const datasetId = req.params.datasetId;
  console.log(`[import] Intentando bajar dataset: ${datasetId}`);

  try {
    // 1) Cargar datos existentes para comparar
    const values = await readRange("Leads!A:Z");
    const header = values[0] || [];
    const rows = values.slice(1);
    const idxPhone = header.indexOf("whatsapp_e164");
    const idxEmail = header.indexOf("email");
    const idxWebsite = header.indexOf("website");
    
    // Mapeo de teléfonos a su índice de fila para actualizaciones rápidas
    const phoneMap = new Map(); 
    if (idxPhone >= 0) {
      rows.forEach((r, index) => {
        if (r[idxPhone]) phoneMap.set(String(r[idxPhone]).trim(), index);
      });
    }

    // 2) Fetch a Apify
    const url = `https://api.apify.com/v2/datasets/${datasetId}/items?format=json&token=${cfg.APIFY_TOKEN}`;
    const resp = await fetch(url);
    
    if (!resp.ok) {
      const errorText = await resp.text();
      return res.status(resp.status).send(`Error de Apify: ${errorText}`);
    }

    const items = await resp.json();
    if (!Array.isArray(items)) return res.status(500).send("Apify no devolvió una lista.");

    // 3) Procesar Items
    const rowsToInsert = [];
    let updatedCount = 0;
    const hoy = new Date().toISOString().split('T')[0]; // Fecha para programar Día 1

    for (const x of items) {
      const e164 = toE164Spain(x.phone || "");
      if (!e164) continue;

      // Extraer email (formato directo o enriquecido)
      const foundEmail = x.email || (x.contactInfo && x.contactInfo.emails && x.contactInfo.emails[0]) || "";
      const foundWeb = x.website || "";

      // ¿YA EXISTE EL TELÉFONO?
      if (phoneMap.has(e164)) {
        const rowIndex = phoneMap.get(e164);
        const currentRow = rows[rowIndex];
        const rowNumber = rowIndex + 2; // +2 por encabezado y base 1

        // Si existe pero NO tiene email, lo actualizamos (Enriquecimiento)
        if (!currentRow[idxEmail] && foundEmail) {
          console.log(`[import] Actualizando datos para lead existente: ${e164}`);
          
          // Actualizamos Web (Columna H = 8) y Email (Columna N = 14)
          if (foundWeb) await updateCell("Leads", rowNumber, 8, foundWeb);
          await updateCell("Leads", rowNumber, 14, foundEmail);
          
          // También reseteamos el estado de email para que entre en la campaña
          await updateCell("Leads", rowNumber, 17, "EMAIL_NEW"); // Columna Q
          await updateCell("Leads", rowNumber, 19, hoy);         // Columna S (email_next_send)
          
          updatedCount++;
        }
        continue; // No lo insertamos como nuevo
      }

      // SI ES NUEVO: Creamos la fila completa con el plan del Día 1
      const lead_id = `L${Date.now()}${Math.floor(Math.random() * 1000)}`;
      rowsToInsert.push([
        lead_id,               // A: lead_id
        x.title || "",         // B: business_name
        x.city || "Getafe",    // C: zone
        e164,                  // D: whatsapp_e164
        x.reviewsCount ?? "",  // E: google_reviews
        x.totalScore ?? "",    // F: google_rating
        x.url || "apify",      // G: source
        foundWeb,              // H: website
        "NEW",                 // I: status (WA)
        "",                    // J: last_outbound_at
        hoy,                   // K: next_send_at (WA DIA 1)
        "", "", "",            // L-N: SIDs
        foundEmail,            // O: email (Columna N en la hoja si A es 1)
        "FALSE",               // P: stop_all
        "",                    // Q: stop_reason
        "EMAIL_NEW",           // R: email_status
        "",                    // S: email_last_outbound_at
        hoy,                   // T: email_next_send_at (EMAIL DIA 1)
        "", "", "", ""         // U-X: email IDs / replies
      ]);
      phoneMap.set(e164, -1); // Evitar duplicados en el mismo dataset
    }

    // 4) Insertar Nuevos
    let imported = 0;
    if (rowsToInsert.length > 0) {
      const CHUNK = 40;
      for (let i = 0; i < rowsToInsert.length; i += CHUNK) {
        const chunk = rowsToInsert.slice(i, i + CHUNK);
        await appendRows("Leads", chunk);
        imported += chunk.length;
      }
    }

    res.send(`✅ Proceso completado: ${imported} nuevos leads y ${updatedCount} leads existentes actualizados con email.`);

  } catch (err) {
    console.error("[import] Error crítico:", err);
    res.status(500).send(`Error: ${err.message}`);
  }
});

app.get("/admin/force-send", async (req, res) => {
  try {
    console.log("[force-send] start");

    const values = await readRange("Leads!A:Z");
    const header = values[0] || [];
    const rows = values.slice(1);

    const idxStatus = header.indexOf("status");
    const idxPhone = header.indexOf("whatsapp_e164");
    const idxReviews = header.indexOf("google_reviews");
    const idxLast = header.indexOf("last_outbound_at");
    const idxNext = header.indexOf("next_send_at");
    const idxSid1 = header.indexOf("msg1_sid");
    const idxStopAll = header.indexOf("stop_all");

    const i = rows.findIndex(r =>
      String(r[idxStatus] || "") === "NEW" &&
      String(r[idxPhone] || "").trim() &&
      String(r[idxStopAll] || "").toUpperCase() !== "TRUE"
    );
    if (i === -1) return res.status(404).send("No NEW leads found (or all are stopped)");

    const rowNumber = i + 2;
    const toE164 = String(rows[i][idxPhone]).trim();
    const reviews = String(rows[i][idxReviews] || "");

    console.log("[force-send] sending to", toE164, "row", rowNumber);
    console.log("[force-send] TPL_MSG1_SID", cfg.TPL_MSG1_SID);

    const client = twilio(cfg.TWILIO_ACCOUNT_SID, cfg.TWILIO_AUTH_TOKEN);
    const msg = await client.messages.create({
      from: cfg.TWILIO_WHATSAPP_FROM,
      to: `whatsapp:${toE164}`,
      contentSid: cfg.TPL_MSG1_SID,
      contentVariables: JSON.stringify({ "1": reviews }),
      statusCallback: `${cfg.PUBLIC_BASE_URL}/webhooks/status`,
    });

    const sentAt = new Date().toISOString();
    const next = require("./utils").computeNextSendFrom(sentAt, 48, cfg);

    rows[i][idxStatus] = "MSG1_SENT";
    rows[i][idxLast] = sentAt;
    rows[i][idxNext] = next;
    rows[i][idxSid1] = msg.sid;

    await updateRow("Leads", rowNumber, rows[i]);

    console.log("[force-send] ok sid", msg.sid);
    res.send(`OK sent to ${toE164} sid=${msg.sid}`);
  } catch (e) {
    console.error("[force-send] ERROR", e);
    res.status(500).send(String(e?.message || e));
  }
});

// ENDPOINT PARA RESPONDER MANUALMENTE DESDE EL NAVEGADOR
app.get("/admin/reply", async (req, res) => {
  const { to, msg } = req.query;
  
  if (!to || !msg) {
    return res.send("Error: Faltan parámetros. Uso: /admin/reply?to=346XXXXXX&msg=Tu mensaje");
  }

  try {
    const client = require("twilio")(cfg.TWILIO_ACCOUNT_SID, cfg.TWILIO_AUTH_TOKEN);
    
    await client.messages.create({
      from: `whatsapp:${cfg.TWILIO_WHATSAPP_NUMBER}`,
      to: `whatsapp:${to}`,
      body: msg
    });

    res.send(`✅ Mensaje enviado con éxito a ${to}.<br><br><b>Mensaje:</b> ${msg}`);
  } catch (e) {
    console.error("Error al enviar respuesta manual:", e);
    res.status(500).send("Error de Twilio: " + e.message);
  }
});