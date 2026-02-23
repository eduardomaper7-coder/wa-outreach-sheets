const cfg = require("./config");
const { toE164Spain } = require("./utils");
const { scrapeEmailFromWebsite } = require("./emailExtractor");

// Ejecuta actor y devuelve items (sync)
async function runActorAndGetItems(input) {
  const url = new URL(
    `https://api.apify.com/v2/acts/${encodeURIComponent(
      cfg.APIFY_ACTOR_ID
    )}/run-sync-get-dataset-items`
  );

  url.searchParams.set("token", cfg.APIFY_TOKEN);
  url.searchParams.set("format", "json");

  const res = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(input),
  });

  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`Apify error ${res.status}: ${txt}`);
  }

  return await res.json();
}
async function scrapeZone(zone) {
  const input = {
    searchStringsArray: [
      `clínica dental ${zone} España`,
      `dentista ${zone} España`,
      `clinica estética ${zone} España`,
    ],
    maxItems: 200,
    language: "es",
    countryCode: "es",
    // --- CAMBIOS CLAVE PARA EMAILS ---
    scrapePlaceDetailPage: true,
    scrapeContacts: true,
    scrapeSocialMediaProfiles: {
      facebooks: true,
      instagrams: true
    },
    maximumLeadsEnrichmentRecords: 10
    // ---------------------------------
  };

  const items = await runActorAndGetItems(input);

  const leads = await Promise.all(
    items.map(async (x) => {
      // Ahora x.email debería venir lleno desde Apify
      return {
        business_name: x.title || "",
        zone,
        whatsapp_e164: toE164Spain(x.phone || ""),
        google_reviews: x.reviewsCount ?? null,
        google_rating: x.totalScore ?? null,
        website: x.website || "", 
        email: x.email || "", // <--- El email ya estará aquí
        source: x.url || "apify",
      };
    })
  );

  return leads.filter(r => r.business_name && r.whatsapp_e164);
}

module.exports = { scrapeZone };