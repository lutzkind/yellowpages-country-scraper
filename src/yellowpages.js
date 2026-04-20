const turf = require("@turf/turf");
const cheerio = require("cheerio");
const { chromium } = require("playwright-core");
const {
  parseBoundingBox,
  bboxCenter,
  pointInsideBBox,
  pointInsideGeometry,
} = require("./geo");

// ---------------------------------------------------------------------------
// Per-country YellowPages site configurations
// ---------------------------------------------------------------------------

const YP_SITE_CONFIGS = {
  us: {
    domain: "www.yellowpages.com",
    countryName: "US",
    resultsPerPage: 30,
    buildSearchUrl(keyword, location, page) {
      const url = new URL("https://www.yellowpages.com/search");
      url.searchParams.set("search_terms", keyword);
      url.searchParams.set("geo_location_terms", location);
      if (page > 1) url.searchParams.set("page", String(page));
      return url.toString();
    },
    waitSelector: ".v-card, .no-results, #no-listings",
    parseResults: parseYPComResults,
  },
  au: {
    domain: "www.yellowpages.com.au",
    countryName: "AU",
    resultsPerPage: 25,
    buildSearchUrl(keyword, location, page) {
      const url = new URL("https://www.yellowpages.com.au/search/listings");
      url.searchParams.set("clue", keyword);
      url.searchParams.set("locationClue", location);
      if (page > 1) url.searchParams.set("pageNumber", String(page));
      return url.toString();
    },
    waitSelector: ".search-contact-card, .no-results, .empty-state",
    parseResults: parseYPAuResults,
  },
  ca: {
    domain: "www.yellowpages.ca",
    countryName: "CA",
    resultsPerPage: 20,
    buildSearchUrl(keyword, location, page) {
      const safePage = page > 1 ? page : 1;
      const safeKeyword = encodeURIComponent(keyword);
      const safeLocation = encodeURIComponent(location).replace(/%20/g, "+");
      return `https://www.yellowpages.ca/search/si/${safePage}/${safeKeyword}/${safeLocation}`;
    },
    waitSelector: ".listing__item, .no-results, .noresults",
    parseResults: parseYPCaResults,
  },
  nz: {
    domain: "www.yellowpages.co.nz",
    countryName: "NZ",
    resultsPerPage: 20,
    buildSearchUrl(keyword, location, page) {
      const url = new URL("https://www.yellowpages.co.nz/search");
      url.searchParams.set("q", keyword);
      url.searchParams.set("l", location);
      if (page > 1) url.searchParams.set("page", String(page));
      return url.toString();
    },
    waitSelector: ".listing-item, .no-results",
    parseResults: parseYPNzResults,
  },
};

function getYPSiteConfig(countryCode) {
  const code = (countryCode || "us").toLowerCase();
  return YP_SITE_CONFIGS[code] || YP_SITE_CONFIGS.us;
}

// ---------------------------------------------------------------------------
// Browser management
// ---------------------------------------------------------------------------

let _browser = null;

async function getBrowser(config) {
  if (_browser) return _browser;
  _browser = await chromium.launch({
    executablePath: config.chromiumPath,
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-dev-shm-usage",
      "--disable-setuid-sandbox",
      "--disable-gpu",
      "--disable-blink-features=AutomationControlled",
    ],
  });
  return _browser;
}

async function closeBrowser() {
  if (_browser) {
    await _browser.close().catch(() => {});
    _browser = null;
  }
}

// ---------------------------------------------------------------------------
// Throttle helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Nominatim: country resolution + reverse geocoding
// ---------------------------------------------------------------------------

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
  // Prefer short state code (NSW, VIC, CA, NY…) over full name
  const state = addr.state_code || addr["ISO3166-2-lvl4"]?.split("-")[1] || addr.state || "";
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

// ---------------------------------------------------------------------------
// YP page fetch (Playwright, works for all country domains)
// ---------------------------------------------------------------------------

async function fetchYPPage(locationTerm, keyword, page, config, siteConfig) {
  return queueYPRequest(async () => {
    const url = siteConfig.buildSearchUrl(keyword, locationTerm, page);

    const browser = await getBrowser(config);
    const contextOptions = {
      locale: "en-US",
      userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    };
    if (config.ypProxyUrl) {
      const u = new URL(config.ypProxyUrl);
      contextOptions.proxy = {
        server: `${u.protocol}//${u.hostname}:${u.port}`,
        username: u.username || undefined,
        password: u.password || undefined,
      };
    }
    const context = await browser.newContext(contextOptions);
    await context.addInitScript(() => {
      Object.defineProperty(navigator, "webdriver", { get: () => undefined });
    });
    const browserPage = await context.newPage();

    try {
      const response = await browserPage.goto(url, {
        waitUntil: "domcontentloaded",
        timeout: config.ypTimeoutMs,
      });

      const status = response?.status() ?? 0;
      if (status === 404) {
        // Location not in YP's index — treat as empty, not an error.
        return { total: 0, results: [] };
      }
      if (status >= 400) {
        const error = new Error(`YellowPages returned ${status} for "${locationTerm}"`);
        error.statusCode = status;
        throw error;
      }

      await browserPage.waitForSelector(siteConfig.waitSelector, {
        timeout: 15000,
      }).catch(() => {});

      const html = await browserPage.content();
      return siteConfig.parseResults(html);
    } finally {
      await browserPage.close().catch(() => {});
      await context.close().catch(() => {});
    }
  }, config.ypDelayMs);
}

// ---------------------------------------------------------------------------
// HTML parsers — one per country site
// ---------------------------------------------------------------------------

function parseYPComResults(html) {
  const $ = cheerio.load(html);

  let total = null;
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
    const ypId = ypHref
      ? ypHref.split("/").filter(Boolean).pop()?.split("?")[0] || null
      : null;
    const ypUrl = ypHref
      ? ypHref.startsWith("http") ? ypHref : `https://www.yellowpages.com${ypHref}`
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
    const { city, stateRegion, postcode } = parseLocality(localityRaw);

    const categories = [];
    $el.find(".categories a, .categories span").each((_, catEl) => {
      const cat = $(catEl).text().trim();
      if (cat) categories.push(cat);
    });

    let rating = null;
    const ratingEm = $el.find(".result-rating em, .rating em").first().text().trim();
    if (ratingEm) rating = parseFloat(ratingEm);
    if (!Number.isFinite(rating)) {
      const starClass = $el.find("[class*='rating-stars']").attr("class") || "";
      const starMatch = starClass.match(/s(\d+)/);
      if (starMatch) rating = parseInt(starMatch[1], 10) / 10;
    }

    const reviewCountText = $el.find(".count, .review-count").first()
      .text().replace(/[()]/g, "").trim();
    const reviewCount = reviewCountText ? parseInt(reviewCountText, 10) : 0;

    results.push({
      name, ypId, ypUrl,
      phone: phone || null,
      website: website || null,
      street: street || null,
      city, stateRegion, postcode,
      country: "US",
      categories,
      category: categories[0] || null,
      rating: Number.isFinite(rating) ? rating : null,
      reviewCount: Number.isFinite(reviewCount) ? reviewCount : 0,
    });
  });

  return { total, results };
}

function parseYPAuResults(html) {
  const $ = cheerio.load(html);

  // AU shows "1 - 25 of 182 results" or similar
  let total = null;
  const countText = $("[class*='results-count'], [class*='result-count'], [class*='showing']").first().text() ||
    $("p, span, div").filter((_, el) => /\d+\s*-\s*\d+\s+of\s+\d+/.test($(el).text())).first().text() ||
    $("p, span, div").filter((_, el) => /\d+\s+results?/i.test($(el).text()) && !/no results/i.test($(el).text())).first().text();
  const countMatch = countText.match(/of\s+([\d,]+)/i) || countText.match(/([\d,]+)\s+results?/i);
  if (countMatch) total = parseInt(countMatch[1].replace(/,/g, ""), 10);

  const results = [];

  // AU uses .search-contact-card as the primary container
  $(".search-contact-card").each((_, el) => {
    const $el = $(el);

    // Business name: in an <a> or <h3> element
    const nameEl = $el.find("h3 a, .listing-name a, a[class*='name'], h3").first();
    const name = nameEl.text().trim() || null;
    if (!name) return;

    const ypHref = nameEl.attr("href") || $el.find("a[href*='/business/']").first().attr("href") || null;
    const ypId = ypHref ? ypHref.split("/").filter(Boolean).pop()?.split("?")[0] || null : null;
    const ypUrl = ypHref
      ? ypHref.startsWith("http") ? ypHref : `https://www.yellowpages.com.au${ypHref}`
      : null;

    // Phone: span.contact-text or similar
    const phone =
      $el.find(".contact-text, [class*='phone'], [href^='tel:']").first().text().trim() ||
      $el.find("[href^='tel:']").first().attr("href")?.replace("tel:", "") ||
      null;

    // Website: external link with rel=nofollow
    const website =
      $el.find("a[href*='website'], a.website-link, a[class*='website']").attr("href") ||
      $el.find("a[rel='nofollow'][href^='http']:not([href*='yellowpages'])").first().attr("href") ||
      null;

    // Address
    const street = $el.find("[class*='address-street'], [class*='street']").first().text().trim() || null;
    const localityRaw = $el.find("[class*='address-suburb'], [class*='suburb'], [class*='locality']").first().text().trim() ||
      $el.find("[class*='address']").first().text().trim() || null;
    const { city, stateRegion, postcode } = parseLocality(localityRaw);

    // Category
    const categories = [];
    $el.find("[class*='category'] a, [class*='category'] span").each((_, catEl) => {
      const cat = $(catEl).text().trim();
      if (cat) categories.push(cat);
    });

    results.push({
      name, ypId, ypUrl,
      phone: phone || null,
      website: website || null,
      street: street || null,
      city, stateRegion, postcode,
      country: "AU",
      categories,
      category: categories[0] || null,
      rating: null,
      reviewCount: 0,
    });
  });

  return { total, results };
}

function parseYPCaResults(html) {
  const $ = cheerio.load(html);

  let total = null;
  const countText = $("[class*='count'], [class*='results-number']").first().text();
  const countMatch = countText.match(/([\d,]+)/);
  if (countMatch) total = parseInt(countMatch[1].replace(/,/g, ""), 10);

  const results = [];

  // CA: .listing__item or .listing__content__wrap--flexed
  $(".listing__item, .listing-block").each((_, el) => {
    const $el = $(el);

    const nameEl = $el.find(".listing__title--wrap a, h3 a, .listing-name a").first();
    const name = nameEl.text().trim() || null;
    if (!name) return;

    const ypHref = nameEl.attr("href") || null;
    const ypId = ypHref ? ypHref.split("/").filter(Boolean).pop()?.split("?")[0] || null : null;
    const ypUrl = ypHref
      ? ypHref.startsWith("http") ? ypHref : `https://www.yellowpages.ca${ypHref}`
      : null;

    const phone =
      $el.find("ul.mlr__submenu li h4, .listing-phone, [href^='tel:']").first().text().trim() ||
      $el.find("[href^='tel:']").first().attr("href")?.replace("tel:", "") ||
      null;

    const website =
      $el.find("li.mlr__item--website a, a.website-link").attr("href") ||
      null;

    const addressText = $el.find(".listing__address--full, [class*='address']").first().text().trim() || null;
    const street = null; // CA YP shows full address in one field
    const { city, stateRegion, postcode } = parseLocality(addressText);

    const categories = [];
    $el.find("[class*='category'] a, [class*='category'] span").each((_, catEl) => {
      const cat = $(catEl).text().trim();
      if (cat) categories.push(cat);
    });

    results.push({
      name, ypId, ypUrl,
      phone: phone || null,
      website: website || null,
      street: street || null,
      city, stateRegion, postcode,
      country: "CA",
      categories,
      category: categories[0] || null,
      rating: null,
      reviewCount: 0,
    });
  });

  return { total, results };
}

function parseYPNzResults(html) {
  const $ = cheerio.load(html);

  let total = null;
  const countText = $("[class*='count'], [class*='results']").first().text();
  const countMatch = countText.match(/([\d,]+)/);
  if (countMatch) total = parseInt(countMatch[1].replace(/,/g, ""), 10);

  const results = [];

  $(".listing-item, .business-listing, [class*='listing-item']").each((_, el) => {
    const $el = $(el);

    const nameEl = $el.find("h3 a, .listing-name a, a[class*='name']").first();
    const name = nameEl.text().trim() || null;
    if (!name) return;

    const ypHref = nameEl.attr("href") || null;
    const ypId = ypHref ? ypHref.split("/").filter(Boolean).pop()?.split("?")[0] || null : null;
    const ypUrl = ypHref
      ? ypHref.startsWith("http") ? ypHref : `https://www.yellowpages.co.nz${ypHref}`
      : null;

    const phone =
      $el.find("[class*='phone'], [href^='tel:']").first().text().trim() ||
      $el.find("[href^='tel:']").first().attr("href")?.replace("tel:", "") ||
      null;

    const website =
      $el.find("a[class*='website'], a[rel='nofollow'][href^='http']").first().attr("href") ||
      null;

    const localityRaw = $el.find("[class*='address'], [class*='location']").first().text().trim() || null;
    const { city, stateRegion, postcode } = parseLocality(localityRaw);

    results.push({
      name, ypId, ypUrl,
      phone: phone || null,
      website: website || null,
      street: null,
      city, stateRegion, postcode,
      country: "NZ",
      categories: [],
      category: null,
      rating: null,
      reviewCount: 0,
    });
  });

  return { total, results };
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

function parseLocality(localityRaw) {
  if (!localityRaw) return { city: null, stateRegion: null, postcode: null };

  const commaIdx = localityRaw.indexOf(",");
  if (commaIdx !== -1) {
    const city = localityRaw.slice(0, commaIdx).trim();
    const rest = localityRaw.slice(commaIdx + 1).trim();
    const parts = rest.split(/\s+/);
    if (parts.length >= 2) {
      return { city, stateRegion: parts[0] || null, postcode: parts[1] || null };
    }
    return { city, stateRegion: rest || null, postcode: null };
  }
  return { city: localityRaw, stateRegion: null, postcode: null };
}

// ---------------------------------------------------------------------------
// Main query entry point
// ---------------------------------------------------------------------------

async function queryYellowPages({ job, shard, geometry, config, exhaustive = false }) {
  const center = bboxCenter(shard.bbox);
  const keyword = job.searchParams?.query || job.keyword;
  const siteConfig = getYPSiteConfig(job.countryCode);

  const locationTerm = await reverseGeocode(center.lat, center.lon, config);
  if (!locationTerm) {
    return { rawCount: 0, leads: [] };
  }

  const firstPage = await fetchYPPage(locationTerm, keyword, 1, config, siteConfig);
  const firstResults = firstPage.results || [];
  const total = firstPage.total ?? firstResults.length;

  if (!exhaustive) {
    return { rawCount: total, leads: normalizeResults(firstResults, shard.bbox, geometry) };
  }

  // Exhaustive: paginate up to ypMaxPages
  const allResults = [...firstResults];
  let page = 2;
  let lastPageSize = firstResults.length;

  while (lastPageSize >= siteConfig.resultsPerPage && page <= config.ypMaxPages) {
    const nextPage = await fetchYPPage(locationTerm, keyword, page, config, siteConfig);
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
  closeBrowser,
  getYPSiteConfig,
};
