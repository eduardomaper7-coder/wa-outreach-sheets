const cfg = require("./config");
const { toE164Spain } = require("./utils");

// Ejecuta actor y devuelve items (sync)
async function runActorAndGetItems(input) {
  // Usamos run-sync-get-dataset-items para obtener resultados directamente
  const url = new URL(
    `https://api.apify.com/v2/acts/${encodeURIComponent(
      cfg.APIFY_ACTOR_ID || "apify/google-maps-scraper"
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
    
    // --- CONFIGURACIÓN DE AHORRO CRÍTICA (Pay-per-result) ---
    // Desactivamos detalles de Google para bajar el coste a la tarifa base
    scrapePlaceDetailPage: false, 
    
    // Activamos búsqueda de emails en la web (Add-on: business leads enrichment)
    scrapeContacts: true, 
    
    // Desactivamos redes sociales (Es el Add-on más caro, $8/1000)
    scrapeSocialMediaProfiles: {
      facebooks: false,
      instagrams: false,
      youtubes: false,
      tiktoks: false,
      twitters: false
    },

    // Limitamos a 1 búsqueda por web para no pagar páginas adicionales
    maximumLeadsEnrichmentRecords: 1,
    
    // Saltamos sitios cerrados para no pagar por leads inactivos
    skipClosedPlaces: true,
    // -------------------------------------------------------
  };

  console.log(`[apify] Iniciando búsqueda económica en ${zone}...`);
  const items = await runActorAndGetItems(input);

  const leads = await Promise.all(
    items.map(async (x) => {
      // Intentamos capturar el email de donde sea que Apify lo haya guardado
      const extractedEmail = x.email || 
                             (x.contactInfo && x.contactInfo.emails && x.contactInfo.emails[0]) || 
                             "";

      return {
        business_name: x.title || "",
        zone,
        whatsapp_e164: toE164Spain(x.phone || ""),
        google_reviews: x.reviewsCount ?? null,
        google_rating: x.totalScore ?? null,
        website: x.website || "", 
        email: extractedEmail, // Ya viene procesado por Apify
        source: x.url || "apify",
      };
    })
  );

  // Solo devolvemos los que tienen nombre y teléfono válido
  const filtered = leads.filter(r => r.business_name && r.whatsapp_e164);
  console.log(`[apify] Procesados ${filtered.length} leads para ${zone}.`);
  
  return filtered;
}

module.exports = { scrapeZone };