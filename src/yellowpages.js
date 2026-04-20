const turf = require("@turf/turf");
const cheerio = require("cheerio");
const { ProxyAgent } = require("undici");
const {
  parseBoundingBox,
  bboxCenter,
  pointInsideBBox,
  pointInsideGeometry,
} = require("./geo");

let _proxyAgent = null;
function getProxyAgent(proxyUrl) {
  if (!proxyUrl) return null;
  if (!_proxyAgent) _proxyAgent = new ProxyAgent(proxyUrl);
  return _proxyAgent;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

let ypThrottle = Promise.resolve();

function queueYPRequest(task, delayMs) {
  const run = ypThrottle.then(task);
  ypThrottle = run.catch(() => undefined).then(() => sleep(delayMs));
  return run;
}

// Nominatim allows 1 req/sec — use the same serial-queue pattern.
let nominatimThrottle = Promise.resolve();

function queueNominatimRequest(task) {
  const run = nominatimThrottle.then(task);
  nominatimThrottle = run.catch(() => undefined).then(() => sleep(1100));
  return run;
}

// Coarse-resolution reverse geocode cache (~10 km cells).
const reverseGeocodeCache = new Map();

async function resolveCountry(country, config) {
  const params = new URLSearchParams({
    country,
    format: "jsonv2",
    limit: "1",
    featuretype: "country",
    polygon_geojson: "1",
  });

  const response = await queueNominatimRequest(() =>
    fetch(`${config.nominatimUrl}?${params.toString()}`, {
      headers: { "User-Agent": config.userAgent, Accept: "application/json" },
    })
  );

  if (!response.ok) {
    throw new Error(`Nominatim returned ${response.status} while resolving "${country}".`);
  }

  const payload = await response.json();
  const first = payload[0];

  if (!first) {
    const error = new Error(`Country "${country}" could not be resolved.`);
    error.statusCode = 404;
    throw error;
  }

  const geometry = first.geojson ? turf.feature(first.geojson) : null;

  return {
    displayName: first.display_name,
    countryCode: first.address?.country_code || null,
    bbox: parseBoundingBox(first.boundingbox),
    geometry,
    raw: first,
  };
}

async function reverseGeocode(lat, lon, config) {
  const key = `${lat.toFixed(1)},${lon.toFixed(1)}`;
  if (reverseGeocodeCache.has(key)) return reverseGeocodeCache.get(key);

  const reverseUrl = config.nominatimUrl.replace(/\/search(\?.*)?$/, "/reverse");
  const params = new URLSearchParams({
    lat: lat.toFixed(6),
    lon: lon.toFixed(6),
    format: "json",
  });

  const response = await queueNominatimRequest(() =>
    fetch(`${reverseUrl}?${params.toString()}`, {
      headers: { "User-Agent": config.userAgent, Accept: "application/json" },
    })
  );

  if (!response.ok) {
    reverseGeocodeCache.set(key, null);
    return null;
  }

  const data = await response.json();
  const addr = data.address || {};
  const countryCode = (addr.country_code || "").toUpperCase();
  const city =
    addr.city || addr.town || addr.village || addr.suburb || addr.county || "";
  const state = addr.state_code || addr.state || "";
  const postcode = addr.postcode || "";

  let locationTerm;
  if (countryCode === "US" && postcode) {
    locationTerm = postcode;
  } else if (city && state) {
    locationTerm = `${city}, ${state}`;
  } else if (city) {
    locationTerm = city;
  } else if (data.display_name) {
    locationTerm = data.display_name.split(",").slice(0, 2).join(",").trim();
  } else {
    locationTerm = null;
  }

  reverseGeocodeCache.set(key, locationTerm);
  return locationTerm;
}

const YP_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  Accept:
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
  "Accept-Encoding": "gzip, deflate, br",
  Connection: "keep-alive",
  "Upgrade-Insecure-Requests": "1",
  "Cache-Control": "max-age=0",
};

async function fetchYPPage(locationTerm, keyword, page, config) {
  return queueYPRequest(async () => {
    const ypUrl = new URL("https://www.yellowpages.com/search");
    ypUrl.searchParams.set("search_terms", keyword);
    ypUrl.searchParams.set("geo_location_terms", locationTerm);
    if (page > 1) ypUrl.searchParams.set("page", String(page));

    const requestUrl = ypUrl.toString();
    const dispatcher = getProxyAgent(config.ypProxyUrl);

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), config.ypTimeoutMs);

    try {
      const fetchOpts = {
        headers: YP_HEADERS,
        signal: controller.signal,
      };
      if (dispatcher) fetchOpts.dispatcher = dispatcher;

      const response = await fetch(requestUrl, fetchOpts);

      if (!response.ok) {
        const error = new Error(`YellowPages returned ${response.status} for "${locationTerm}"`);
        error.statusCode = response.status;
        throw error;
      }

      const html = await response.text();
      return parseYPResults(html);
    } finally {
      clearTimeout(timeout);
    }
  }, config.ypDelayMs);
}

function parseYPResults(html) {
  const $ = cheerio.load(html);

  let total = null;
  // YP shows "Results 1 - 30 of 245" or "Showing 1-30 of 245"
  const countText = $(".showing-count, .count-text").first().text() ||
    $("[class*='showing']").first().text() ||
    $("p:contains('of')").filter((_, el) => /\d+ of \d+/.test($(el).text())).first().text();
  const countMatch = countText.match(/of\s+([\d,]+)/i);
  if (countMatch) total = parseInt(countMatch[1].replace(/,/g, ""), 10);

  const results = [];

  $(".v-card").each((_, el) => {
    const $el = $(el);

    const nameEl = $el.find("a.business-name");
    const name = nameEl.text().trim() || null;
    if (!name) return;

    const ypHref = nameEl.attr("href") || null;
    // ypHref is like "/new-york-ny/mip/acme-plumbing-12345678" — extract the last segment as ID
    const ypId = ypHref
      ? ypHref.split("/").filter(Boolean).pop()?.split("?")[0] || null
      : null;
    const ypUrl = ypHref
      ? ypHref.startsWith("http")
        ? ypHref
        : `https://www.yellowpages.com${ypHref}`
      : null;

    const phone =
      $el.find(".phones.phone.primary").first().text().trim() ||
      $el.find(".phone").first().text().trim() ||
      null;

    const website =
      $el.find("a.website").attr("href") ||
      $el.find("a[class*='website']").attr("href") ||
      null;

    const street = $el.find(".street-address").first().text().trim() || null;
    const localityRaw = $el.find(".locality").first().text().trim() || null;

    let city = null;
    let stateRegion = null;
    let postcode = null;

    if (localityRaw) {
      // Format: "City, ST 12345" or "City, State"
      const commaIdx = localityRaw.indexOf(",");
      if (commaIdx !== -1) {
        city = localityRaw.slice(0, commaIdx).trim();
        const rest = localityRaw.slice(commaIdx + 1).trim();
        const parts = rest.split(/\s+/);
        if (parts.length >= 2) {
          stateRegion = parts[0] || null;
          postcode = parts[1] || null;
        } else {
          stateRegion = rest || null;
        }
      } else {
        city = localityRaw;
      }
    }

    const categories = [];
    $el.find(".categories a, .categories span").each((_, catEl) => {
      const cat = $(catEl).text().trim();
      if (cat) categories.push(cat);
    });

    // Rating: YP uses star classes or an <em> with numeric rating
    let rating = null;
    const ratingEm = $el.find(".result-rating em, .rating em").first().text().trim();
    if (ratingEm) rating = parseFloat(ratingEm);
    if (!Number.isFinite(rating)) {
      // Try star class: "rating-stars s45" means 4.5
      const starClass = $el.find("[class*='rating-stars']").attr("class") || "";
      const starMatch = starClass.match(/s(\d+)/);
      if (starMatch) rating = parseInt(starMatch[1], 10) / 10;
    }

    const reviewCountText = $el
      .find(".count, .review-count")
      .first()
      .text()
      .replace(/[()]/g, "")
      .trim();
    const reviewCount = reviewCountText ? parseInt(reviewCountText, 10) : 0;

    results.push({
      name,
      ypId,
      ypUrl,
      phone: phone || null,
      website: website || null,
      street: street || null,
      city,
      stateRegion,
      postcode,
      country: "US",
      categories,
      category: categories[0] || null,
      rating: Number.isFinite(rating) ? rating : null,
      reviewCount: Number.isFinite(reviewCount) ? reviewCount : 0,
    });
  });

  return { total, results };
}

async function queryYellowPages({ job, shard, geometry, config, exhaustive = false }) {
  const center = bboxCenter(shard.bbox);
  const keyword = job.searchParams?.query || job.keyword;

  const locationTerm = await reverseGeocode(center.lat, center.lon, config);
  if (!locationTerm) {
    return { rawCount: 0, leads: [] };
  }

  const firstPage = await fetchYPPage(locationTerm, keyword, 1, config);
  const firstResults = firstPage.results || [];
  const total = firstPage.total ?? firstResults.length;

  if (!exhaustive) {
    return { rawCount: total, leads: normalizeResults(firstResults, shard.bbox, geometry) };
  }

  // Exhaustive: paginate up to ypMaxPages
  const allResults = [...firstResults];
  let page = 2;
  let lastPageSize = firstResults.length;

  while (lastPageSize >= 30 && page <= config.ypMaxPages) {
    const nextPage = await fetchYPPage(locationTerm, keyword, page, config);
    const pageResults = nextPage.results || [];
    allResults.push(...pageResults);
    lastPageSize = pageResults.length;
    page++;
  }

  return { rawCount: total, leads: normalizeResults(allResults, shard.bbox, geometry) };
}

function normalizeResults(results, bbox, geometry) {
  return results.map((r) => normalizeEntry(r, bbox)).filter(Boolean);
}

function normalizeEntry(item, bbox) {
  if (!item || !item.name) return null;

  const dedupeKey =
    item.ypId ||
    `${String(item.name)}:${String(item.phone || "")}:${String(item.street || "")}`;

  const address = [item.street, item.city, item.stateRegion, item.postcode]
    .filter(Boolean)
    .join(", ");

  return {
    dedupeKey,
    placeId: item.ypId || null,
    cid: null,
    dataId: null,
    link: item.ypUrl || null,
    name: item.name,
    category: item.category || "",
    subcategory: null,
    allSubcategories: null,
    categories: item.categories || [],
    website: item.website || null,
    phone: item.phone || null,
    email: null,
    address,
    completeAddress: {
      street: item.street,
      city: item.city,
      state: item.stateRegion,
      postcode: item.postcode,
      country: item.country,
    },
    city: item.city || null,
    area: null,
    stateRegion: item.stateRegion || null,
    postcode: item.postcode || null,
    country: item.country || null,
    lat: null,
    lon: null,
    reviewCount: item.reviewCount || 0,
    reviewRating: item.rating || null,
    status: null,
    priceRange: null,
    bbox,
    raw: item,
  };
}

function extractLocationParts(entry) {
  return {
    city: entry.city || null,
    area: null,
    stateRegion: entry.stateRegion || entry.state_region || null,
    postcode: entry.postcode || null,
    country: entry.country || null,
  };
}

module.exports = {
  resolveCountry,
  queryYellowPages,
  normalizeEntry,
  extractLocationParts,
};
