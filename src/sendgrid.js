const sgMail = require("@sendgrid/mail");
const cfg = require("./config");

sgMail.setApiKey(cfg.SENDGRID_API_KEY);

async function sendEmail({ to, subject, text, html, customArgs }) {
  const msg = {
    to,
    from: cfg.EMAIL_FROM,
    replyTo: cfg.EMAIL_REPLY_TO || cfg.EMAIL_FROM,
    subject,
    text,
    html,
    customArgs: customArgs || {}, // para correlación en event webhook
  };

  const [res] = await sgMail.send(msg);
  // SendGrid devuelve cabeceras, a veces viene x-message-id
  const messageId = res?.headers?.["x-message-id"] || res?.headers?.["X-Message-Id"] || "";
  return { messageId };
}

module.exports = { sendEmail };