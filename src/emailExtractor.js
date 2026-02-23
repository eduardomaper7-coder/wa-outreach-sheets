const fetch = require("node-fetch");

function extractEmailsFromText(text) {
  const regex = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi;
  const matches = text.match(regex) || [];
  return [...new Set(matches)];
}

async function scrapeEmailFromWebsite(url) {
  if (!url) return null;

  try {
    const res = await fetch(url, { timeout: 10000 });
    const html = await res.text();

    let emails = extractEmailsFromText(html);

    if (emails.length > 0) return emails[0];

    // intentar páginas comunes
    const commonPaths = ["/contacto", "/contact", "/aviso-legal"];
    for (const path of commonPaths) {
      try {
        const res2 = await fetch(new URL(path, url).href);
        const html2 = await res2.text();
        emails = extractEmailsFromText(html2);
        if (emails.length > 0) return emails[0];
      } catch {}
    }

    return null;
  } catch {
    return null;
  }
}

module.exports = { scrapeEmailFromWebsite };