const cfg = require("./config");
const { toE164Spain } = require("./utils");

// Ejecuta actor y devuelve items (sync)
async function runActorAndGetItems(input) {
  const url = new URL(`https://api.apify.com/v2/acts/${encodeURIComponent(cfg.APIFY_ACTOR_ID)}/run-sync-get-dataset-items`);
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

/**
 * Ajusta este input al Actor exacto que uses.
 * La idea: pasar "zone" / "search" / "location" y que el actor devuelva datos.
 */
async function scrapeZone(zone) {
  const input = {
    searchStringsArray: [
      `clínica dental ${zone} España`,
      `dentista ${zone} España`,
      `clinica estética ${zone} España`
    ],
    maxItems: 200,
    language: "es",
    countryCode: "es", // obligatorio en minúsculas
  };

  const items = await runActorAndGetItems(input);

  return items
    .map((x) => ({
      business_name: x.title || "",
      zone,
      whatsapp_e164: toE164Spain(x.phone || ""),
      google_reviews: x.reviewsCount ?? null,
      google_rating: x.totalScore ?? null,
      source: x.url || "apify",
    }))
    .filter((r) => r.business_name && r.whatsapp_e164);
}

module.exports = { scrapeZone };