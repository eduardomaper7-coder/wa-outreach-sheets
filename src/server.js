const express = require("express");
const twilio = require("twilio");
const cfg = require("./config");
const { startEngine } = require("./engine");
const { readRange, updateRow, appendRow } = require("./sheets");

const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

function validateTwilioRequest(req) {
  // Si lo quieres activar 100%: usa validateRequest con tu PUBLIC_BASE_URL
  // (si falla por proxy/railway headers, lo desactivas o lo ajustas)
  const signature = req.headers["x-twilio-signature"];
  const url = `${cfg.PUBLIC_BASE_URL}${req.originalUrl}`;
  return twilio.validateRequest(cfg.TWILIO_AUTH_TOKEN, signature, url, req.body);
}

async function findLeadRowByPhone(e164) {
  const values = await readRange("Leads!A:M");
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

function updateLeadRowFromHeader(header, row, patch) {
  const out = [...row];
  for (const [k, v] of Object.entries(patch)) {
    const idx = header.indexOf(k);
    if (idx >= 0) out[idx] = v;
  }
  return out;
}

// INBOUND: si responde → status=REPLIED, next_send_at vacío
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
    const values = await readRange("Leads!A:M");
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

  res.status(200).send("ok");
});

app.get("/health", (req, res) => res.json({ ok: true }));

startEngine();

app.listen(cfg.PORT, () => console.log(`Listening on ${cfg.PORT}`));

app.get("/admin/scrape", async (req, res) => {
  const { dailyScrape } = require("./engine");
  await dailyScrape();
  res.send("scrape ok");
});