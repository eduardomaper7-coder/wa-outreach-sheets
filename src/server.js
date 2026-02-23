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

  // 1) cargar teléfonos ya existentes
  const values = await readRange("Leads!A:Z");
  const header = values[0] || [];
  const rows = values.slice(1);
  const idxPhone = header.indexOf("whatsapp_e164");

  const existing = new Set();
  if (idxPhone >= 0) {
    for (const r of rows) {
      const p = String(r[idxPhone] || "").trim();
      if (p) existing.add(p);
    }
  }

  // 2) bajar dataset
  const url = `https://api.apify.com/v2/datasets/${datasetId}/items?format=json&token=${cfg.APIFY_TOKEN}`;
  const r = await fetch(url);
  const items = await r.json();

  // 3) preparar filas SOLO si no existe el teléfono
  const rowsToInsert = [];
  for (const x of items) {
    const e164 = toE164Spain(x.phone || "");
    if (!e164) continue;
    if (existing.has(e164)) continue; // <-- evita duplicados

    existing.add(e164);

    const lead_id = `L${Date.now()}${Math.floor(Math.random() * 1000)}`;
    rowsToInsert.push([
      lead_id,
      x.title || "",
      x.city || "",
      e164,
      x.reviewsCount ?? "",
      x.totalScore ?? "",
      x.url || "apify",
      "NEW",
      "",
      "",
      "",
      "",
      "",
      // --- NUEVO (N..W) - placeholders para que cuadre con A:Z (si tu sheet tiene estas columnas)
      x.email || "",   // N: email (si viene)
      "",              // O: stop_all
      "",              // P: stop_reason
      "EMAIL_NEW",     // Q: email_status
      "",              // R: email_last_outbound_at
      "",              // S: email_next_send_at
      "",              // T: email1_id
      "",              // U: email2_id
      "",              // V: email3_id
      "",              // W: email_reply_at
    ]);
  }

  // 4) batch + pausas
  let imported = 0;
  const CHUNK = 50;
  for (let i = 0; i < rowsToInsert.length; i += CHUNK) {
    const chunk = rowsToInsert.slice(i, i + CHUNK);
    await appendRows("Leads", chunk);
    imported += chunk.length;
    await new Promise(r => setTimeout(r, 800));
  }

  res.send(`Imported ${imported} new leads (skipped duplicates)`);
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