const twilio = require("twilio");
const cfg = require("./config");

const client = twilio(cfg.TWILIO_ACCOUNT_SID, cfg.TWILIO_AUTH_TOKEN);

async function sendTemplate({ toE164, contentSid, variables, statusCallbackUrl }) {
  // Twilio: ContentSid + ContentVariables para templates ([twilio.com](https://www.twilio.com/docs/whatsapp/tutorial/send-whatsapp-notification-messages-templates?utm_source=chatgpt.com))
  const msg = await client.messages.create({
    from: cfg.TWILIO_WHATSAPP_FROM,
    to: `whatsapp:${toE164}`,
    contentSid,
    contentVariables: JSON.stringify(variables || {}),
    statusCallback: statusCallbackUrl,
  });
  return msg;
}

module.exports = { sendTemplate };