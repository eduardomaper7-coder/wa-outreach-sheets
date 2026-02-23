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
  const rawHeader = values[0] || [];
  const header = rawHeader.map(h => String(h || "").trim().toLowerCase());
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
    const rawHeader = values[0] || [];
    const header = rawHeader.map(h => String(h || "").trim().toLowerCase());
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
  const datasetId = req.query.datasetId || req.params.datasetId;
  console.log(`[import] Intentando bajar dataset: ${datasetId}`);

  if (!datasetId) return res.status(400).send("Falta el ID del dataset");

  try {
    // 1) Cargar Leads existentes
    const values = await readRange("Leads!A:Z");
    const rawHeader = values[0] || [];
    const header = rawHeader.map(h => String(h || "").trim().toLowerCase());
    const rows = values.slice(1);

    const idxPhone = header.indexOf("whatsapp_e164");          // D
    const idxEmail = header.indexOf("email");                  // O
    const idxWebsite = header.indexOf("website");              // H
    const idxEmailStatus = header.indexOf("email_status");     // R
    const idxEmailNext = header.indexOf("email_next_send_at"); // T

    if (idxPhone < 0 && idxEmail < 0) {
      return res.status(500).send("No se encontraron columnas whatsapp_e164 ni email en Leads.");
    }

    // Mapas para buscar rápido en filas existentes del sheet
    const phoneMap = new Map(); // e164 -> rowIndex (0-based sobre rows)
    const emailMap = new Map(); // email -> rowIndex (0-based sobre rows)

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
    if (!Array.isArray(items)) return res.status(500).send("Apify no devolvió una lista.");

    // 3) Helpers
    const hoy = new Date().toISOString();
    const rowsToInsert = [];
    let updatedCount = 0;

    // ✅ Evitar duplicados dentro del dataset
    const seenPhones = new Set();
    const seenEmails = new Set();

    // ✅ Acumular updates por fila (para 1 batch write)
    // key = rowNumber (1-based), value = rowValues completo A:Z
    const pendingRowUpdates = new Map();

    function ensureRowWidth(row, width) {
      const out = Array.isArray(row) ? [...row] : [];
      while (out.length < width) out.push("");
      return out;
    }

    function setIfEmpty(row, idx, value) {
      if (idx < 0) return false;
      const cur = String(row[idx] || "").trim();
      if (!cur && value) {
        row[idx] = value;
        return true;
      }
      return false;
    }

    function setAlways(row, idx, value) {
      if (idx < 0) return false;
      row[idx] = value;
      return true;
    }

    function queueRowUpdate(rowIndex0Based, updatedRow) {
      const rowNumber = rowIndex0Based + 2; // + header row
      pendingRowUpdates.set(rowNumber, updatedRow);
    }

    // 4) Procesar items
    for (const x of items) {
      const e164 = toE164Spain(
        x.phone || x.phoneUnformatted || (Array.isArray(x.phones) ? x.phones[0] : "")
      );

      const foundEmailRaw = (Array.isArray(x.emails) && x.emails[0]) || x.email || "";
      const foundEmail = String(foundEmailRaw || "").trim().toLowerCase();

      const foundWeb = String(x.website || "").trim();

      const hasEmail = foundEmail.includes("@");
      const hasPhone = !!e164;

      if (!hasPhone && !hasEmail) continue;

      // Dedupe dentro del dataset
      if (hasPhone) {
        if (seenPhones.has(e164)) continue;
        seenPhones.add(e164);
      }
      if (hasEmail) {
        if (seenEmails.has(foundEmail)) continue;
        seenEmails.add(foundEmail);
      }

      // ✅ 1) MATCH POR EMAIL
      if (hasEmail && idxEmail >= 0 && emailMap.has(foundEmail)) {
        const rowIndex = emailMap.get(foundEmail);
        const current = ensureRowWidth(rows[rowIndex], 26);

        let changed = false;

        // añadir phone si falta
        if (hasPhone && idxPhone >= 0) {
          changed = setIfEmpty(current, idxPhone, e164) || changed;
          if (changed) phoneMap.set(e164, rowIndex);
        }

        // completar website si falta
        if (idxWebsite >= 0) {
          changed = setIfEmpty(current, idxWebsite, foundWeb) || changed;
        }

        if (changed) {
          rows[rowIndex] = current;
          queueRowUpdate(rowIndex, current);
          updatedCount++;
        }

        continue;
      }

      // ✅ 2) MATCH POR PHONE
      if (hasPhone && idxPhone >= 0 && phoneMap.has(e164)) {
        const rowIndex = phoneMap.get(e164);
        const current = ensureRowWidth(rows[rowIndex], 26);

        let changed = false;

        // completar website si falta
        if (idxWebsite >= 0) {
          changed = setIfEmpty(current, idxWebsite, foundWeb) || changed;
        }

        // completar email si falta
        if (hasEmail && idxEmail >= 0) {
          const before = String(current[idxEmail] || "").trim();
          if (!before) {
            changed = setAlways(current, idxEmail, foundEmail) || changed;

            // set email_status / next_send_at
            if (idxEmailStatus >= 0) changed = setAlways(current, idxEmailStatus, "EMAIL_NEW") || changed;
            if (idxEmailNext >= 0) changed = setAlways(current, idxEmailNext, hoy) || changed;

            emailMap.set(foundEmail, rowIndex);
          }
        }

        if (changed) {
          rows[rowIndex] = current;
          queueRowUpdate(rowIndex, current);
          updatedCount++;
        }

        continue;
      }

      // ✅ 3) NO EXISTE -> INSERT NUEVO (mantienes: solo insert si hay teléfono)
      if (!hasPhone) continue;

      const lead_id = `L${Date.now()}${Math.floor(Math.random() * 1000)}`;

      rowsToInsert.push([
        lead_id,                    // A
        x.title || "",              // B
        x.city || "Getafe",         // C
        e164,                       // D
        x.reviewsCount ?? "",       // E
        x.totalScore ?? "",         // F
        x.url || "apify",           // G
        foundWeb,                   // H
        "NEW",                      // I
        "",                         // J
        hoy,                        // K
        "",                         // L
        "",                         // M
        "",                         // N
        hasEmail ? foundEmail : "", // O
        "FALSE",                    // P
        "",                         // Q
        hasEmail ? "EMAIL_NEW" : "",// R
        "",                         // S
        hasEmail ? hoy : "",        // T
        "", "", "", ""              // U-X
      ]);
    }

    // 5) ✅ Hacer batch update de filas existentes (MUY pocas requests)
    // Necesitas batchUpdateRows en sheets.js
    const { batchUpdateRows } = require("./sheets");

    if (pendingRowUpdates.size > 0) {
      const all = Array.from(pendingRowUpdates.entries()).map(([rowNumber, values]) => ({
        rowNumber,
        values,
      }));

      // Enviar en chunks para no hacer un request gigante
      const CHUNK_UPDATES = 200; // 200 filas por batch (1 request)
      for (let i = 0; i < all.length; i += CHUNK_UPDATES) {
        const chunk = all.slice(i, i + CHUNK_UPDATES);
        await batchUpdateRows("Leads", chunk);
      }
    }

    // 6) Insertar nuevos (append en bloques)
    let imported = 0;
    if (rowsToInsert.length > 0) {
      const CHUNK_INSERT = 200; // sube a 200 para menos requests
      for (let i = 0; i < rowsToInsert.length; i += CHUNK_INSERT) {
        const chunk = rowsToInsert.slice(i, i + CHUNK_INSERT);
        await appendRows("Leads", chunk);
        imported += chunk.length;
      }
    }

    res.send(`✅ Importación terminada: ${imported} nuevos y ${updatedCount} actualizados (batch).`);
  } catch (err) {
    console.error("[import] Error:", err);
    res.status(500).send(`Error: ${err.message}`);
  }
});

app.get("/admin/force-send", async (req, res) => {
  try {
    console.log("[force-send] start");

    const values = await readRange("Leads!A:Z");
    const rawHeader = values[0] || [];
    const header = rawHeader.map(h => String(h || "").trim().toLowerCase());
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