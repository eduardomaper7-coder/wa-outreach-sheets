// server.js (COMPLETO con cambios: Leads!A:Z + stop_all on replies + inbound email webhook)

const express = require("express");
const twilio = require("twilio");
const multer = require("multer");
const cfg = require("./config");
const { startEngine } = require("./engine");
const { enrichExistingLeadsEmails } = require("./engine"); // 👈 AQUI
const { readRange, updateRow, appendRow, appendRows, updateCell } = require("./sheets");
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
  const rawHeader = values[0] || [];
  const header = rawHeader.map(h => String(h || "").trim().toLowerCase());
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

app.get("/admin/import-dataset", async (req, res) => {
  // Acepta el ID tanto de la URL como de la query (?datasetId=...)
  const datasetId = req.query.datasetId || req.params.datasetId;
  console.log(`[import] Intentando bajar dataset: ${datasetId}`);

  if (!datasetId) return res.status(400).send("Falta el ID del dataset");

  try {
    // 1) Cargar datos existentes para comparar (NORMALIZANDO HEADERS)
    const values = await readRange("Leads!A:Z");
    const rawHeader = values[0] || [];
    const header = rawHeader.map(h => String(h || "").trim().toLowerCase());
    const rows = values.slice(1);

    const idxPhone = header.indexOf("whatsapp_e164");        // D
    const idxEmail = header.indexOf("email");                // O
    const idxWebsite = header.indexOf("website");            // H
    const idxEmailStatus = header.indexOf("email_status");   // R
    const idxEmailNext = header.indexOf("email_next_send_at");// T

    if (idxPhone < 0 && idxEmail < 0) {
      return res.status(500).send("No se encontraron columnas whatsapp_e164 ni email en Leads.");
    }

    // Mapas para buscar rápido
    const phoneMap = new Map(); // e164 -> rowIndex
    const emailMap = new Map(); // email -> rowIndex

    rows.forEach((r, index) => {
      const p = idxPhone >= 0 ? String(r[idxPhone] || "").trim() : "";
      const e = idxEmail >= 0 ? String(r[idxEmail] || "").trim().toLowerCase() : "";
      if (p) phoneMap.set(p, index);
      if (e) emailMap.set(e, index);
    });

    // 2) Fetch a Apify
    const url = `https://api.apify.com/v2/datasets/${datasetId}/items?format=json&token=${cfg.APIFY_TOKEN}`;
    const resp = await fetch(url);

    if (!resp.ok) {
      const errorText = await resp.text();
      return res.status(resp.status).send(`Error de Apify: ${errorText}`);
    }

    const items = await resp.json();
    console.log("[import] items:", Array.isArray(items) ? items.length : typeof items);
    console.log("[import] sample item keys:", items?.[0] ? Object.keys(items[0]) : "NO ITEMS");
    console.log("[import] sample item:", items?.[0] || "NO ITEMS");
    if (!Array.isArray(items)) return res.status(500).send("Apify no devolvió una lista.");

    // 3) Procesar Items
    const rowsToInsert = [];
    let updatedCount = 0;
    const hoy = new Date().toISOString();

    for (const x of items) {
      const e164 = toE164Spain(x.phone || x.phoneUnformatted || (Array.isArray(x.phones) ? x.phones[0] : ""));
      // ✅ emails viene como array "emails"
      const foundEmailRaw = (Array.isArray(x.emails) && x.emails[0]) || x.email || "";
      const foundEmail = String(foundEmailRaw || "").trim().toLowerCase();
      const foundWeb = x.website || "";

      const hasEmail = foundEmail.includes("@");
      const hasPhone = !!e164;

      // si no hay ni phone ni email, skip
      if (!hasPhone && !hasEmail) continue;

      // ✅ 1) MATCH POR EMAIL (PRIORIDAD)
      if (hasEmail && idxEmail >= 0 && emailMap.has(foundEmail)) {
        const rowIndex = emailMap.get(foundEmail);
        const currentRow = rows[rowIndex];
        const rowNumber = rowIndex + 2;

        // Si el lead por email NO tiene phone y ahora lo traemos, lo metemos
        if (hasPhone && idxPhone >= 0 && !String(currentRow[idxPhone] || "").trim()) {
          console.log(`[import] Match por EMAIL. Añadiendo teléfono ${e164} a ${foundEmail}`);
          await updateCell("Leads", rowNumber, idxPhone + 1, e164);
          phoneMap.set(e164, rowIndex); // para futuras iteraciones
        }

        // Si no tenía website, lo completamos
        if (idxWebsite >= 0 && foundWeb && !String(currentRow[idxWebsite] || "").trim()) {
          await updateCell("Leads", rowNumber, idxWebsite + 1, foundWeb);
        }

        // Ya existe: no insertamos
        continue;
      }

      // ✅ 2) MATCH POR TELÉFONO
      if (hasPhone && idxPhone >= 0 && phoneMap.has(e164)) {
        const rowIndex = phoneMap.get(e164);
        const currentRow = rows[rowIndex];
        const rowNumber = rowIndex + 2;

        // Si existe pero NO tiene email, lo actualizamos
        if (hasEmail && idxEmail >= 0 && !String(currentRow[idxEmail] || "").trim()) {
          console.log(`[import] Match por PHONE. Enriqueciendo ${e164} con email ${foundEmail}`);

          // Website si viene y falta
          if (idxWebsite >= 0 && foundWeb && !String(currentRow[idxWebsite] || "").trim()) {
            await updateCell("Leads", rowNumber, idxWebsite + 1, foundWeb);
          }

          // Email
          await updateCell("Leads", rowNumber, idxEmail + 1, foundEmail);

          // Estados email
          if (idxEmailStatus >= 0) await updateCell("Leads", rowNumber, idxEmailStatus + 1, "EMAIL_NEW");
          if (idxEmailNext >= 0) await updateCell("Leads", rowNumber, idxEmailNext + 1, hoy);

          // para futuras iteraciones
          emailMap.set(foundEmail, rowIndex);
          updatedCount++;
        } else {
          // aunque ya tenga email, si falta website, lo completamos
          if (idxWebsite >= 0 && foundWeb && !String(currentRow[idxWebsite] || "").trim()) {
            await updateCell("Leads", rowNumber, idxWebsite + 1, foundWeb);
          }
        }

        continue;
      }

      // ✅ 3) NO EXISTE -> INSERT NUEVO
      // Mantengo tu comportamiento: solo insertamos si hay teléfono.
      if (!hasPhone) continue;

      const lead_id = `L${Date.now()}${Math.floor(Math.random() * 1000)}`;

      rowsToInsert.push([
        lead_id,                    // A: lead_id
        x.title || "",              // B: business_name
        x.city || "Getafe",         // C: zone
        e164,                       // D: whatsapp_e164
        x.reviewsCount ?? "",       // E: google_reviews
        x.totalScore ?? "",         // F: google_rating
        x.url || "apify",           // G: source
        foundWeb,                   // H: website
        "NEW",                      // I: status (WA)
        "",                         // J: last_outbound_at
        hoy,                        // K: next_send_at (WA D0)
        "",                         // L: msg1_sid
        "",                         // M: msg2_sid
        "",                         // N: msg3_sid
        hasEmail ? foundEmail : "", // O: email
        "FALSE",                    // P: stop_all
        "",                         // Q: stop_reason
        hasEmail ? "EMAIL_NEW" : "",// R: email_status
        "",                         // S: email_last_outbound_at
        hasEmail ? hoy : "",        // T: email_next_send_at
        "", "", "", ""              // U-X: Ids y Replies
      ]);

      phoneMap.set(e164, -1);
      if (hasEmail) emailMap.set(foundEmail, -1);
    }

    // 4) Insertar los nuevos leads en bloques
    let imported = 0;
    if (rowsToInsert.length > 0) {
      const CHUNK = 40;
      for (let i = 0; i < rowsToInsert.length; i += CHUNK) {
        const chunk = rowsToInsert.slice(i, i + CHUNK);
        await appendRows("Leads", chunk);
        imported += chunk.length;
      }
    }

    res.send(`✅ Importación terminada: ${imported} nuevos y ${updatedCount} enriquecidos/mergeados.`);

  } catch (err) {
    console.error("[import] Error:", err);
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
  
  if (!to || !msg) return res.send("Faltan parámetros: to y msg");

  try {
    const client = require("twilio")(cfg.TWILIO_ACCOUNT_SID, cfg.TWILIO_AUTH_TOKEN);
    
    await client.messages.create({
      // Pon aquí tu número de Twilio directamente para evitar el 'undefined'
      from: `whatsapp:+34691830446`, // <--- CAMBIA ESTO POR TU NÚMERO DE TWILIO
      to: `whatsapp:${to}`,
      body: msg
    });

    res.send(`✅ Mensaje enviado con éxito a ${to}`);
  } catch (e) {
    console.error("Error:", e.message);
    res.status(500).send("Error de Twilio: " + e.message);
  }
});