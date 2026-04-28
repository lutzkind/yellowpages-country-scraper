const turf = require("@turf/turf");
const cheerio = require("cheerio");
const { chromium } = require("playwright-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
chromium.use(StealthPlugin());
const {
  parseBoundingBox,
  bboxCenter,
  pointInsideBBox,
  pointInsideGeometry,
  deriveSeedBBoxes,
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
      const keywordSlug = slugAuPathSegment(keyword);
      const locationSlug = slugAuPathSegment(location);
      const url = new URL(`https://www.yellowpages.com.au/${locationSlug}/${keywordSlug}`);
      if (page > 1) url.searchParams.set("page", String(page));
      return url.toString();
    },
    waitSelector: ".v-card, .no-results, .empty-state",
    parseResults: parseYPAuResults,
  },
  ca: {
    domain: "www.yellowpages.ca",
    countryName: "CA",
    resultsPerPage: 20,
    usePlaywright: false, // No Cloudflare — plain fetch is sufficient
    buildSearchUrl(keyword, location, page) {
      const safePage = page > 1 ? page : 1;
      const safeKeyword = encodeURIComponent(keyword);
      const safeLocation = encodeURIComponent(location).replace(/%20/g, "+");
      return `https://www.yellowpages.ca/search/si/${safePage}/${safeKeyword}/${safeLocation}`;
    },
    parseResults: parseYPCaResults,
  },
};

const SUPPORTED_COUNTRY_CODES = Object.freeze(Object.keys(YP_SITE_CONFIGS));

function getYPSiteConfig(countryCode) {
  const code = (countryCode || "").toLowerCase();
  return YP_SITE_CONFIGS[code] || null;
}

function getSupportedCountryCodes() {
  return SUPPORTED_COUNTRY_CODES;
}

// ---------------------------------------------------------------------------
// Browser management
// ---------------------------------------------------------------------------

let _browser = null;
let _browserPromise = null;

async function getBrowser(config) {
  if (_browser) return _browser;
  if (_browserPromise) return _browserPromise;
  _browserPromise = chromium.launch({
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
  _browser = await _browserPromise;
  _browserPromise = null;
  return _browser;
}

async function closeBrowser() {
  if (_browserPromise) {
    await _browserPromise.catch(() => {});
    _browserPromise = null;
  }
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

const ypThrottleByKey = new Map();

function queueYPRequest(key, task, delayMs) {
  const queueKey = key || "default";
  const previous = ypThrottleByKey.get(queueKey) || Promise.resolve();
  const run = previous.then(task);
  ypThrottleByKey.set(queueKey, run.catch(() => undefined).then(() => sleep(delayMs)));
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
    countryCode: first.address?.country_code || country || null,
    bbox: parseBoundingBox(first.boundingbox),
    seedBBoxes: deriveSeedBBoxes(geometry, parseBoundingBox(first.boundingbox)),
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
  // City/state terms are more stable than raw postcodes for YellowPages US queries.
  if (city && state) {
    locationTerm = `${city}, ${state}`;
  } else if (countryCode === "US" && postcode) {
    locationTerm = postcode;
  } else if (city) {
    locationTerm = city;
  } else if (data.display_name) {
    locationTerm = data.display_name.split(",").slice(0, 2).join(",").trim();
  } else {
    locationTerm = null;
  }

  const payload = {
    locationTerm,
    city: city || null,
    state: state || null,
    postcode: postcode || null,
    countryCode: countryCode || null,
    displayName: data.display_name || null,
  };

  reverseGeocodeCache.set(key, payload);
  return payload;
}

// ---------------------------------------------------------------------------
// YP page fetch (Playwright, works for all country domains)
// ---------------------------------------------------------------------------

// Resource types that are safe to block — not needed for Cloudflare or HTML parsing.
// Blocking these cuts per-page bandwidth by ~50-60% for Playwright requests.
const BLOCKED_RESOURCE_TYPES = new Set(["image", "media", "font"]);

function buildProxyConfig(proxyUrl, countryCode) {
  if (!proxyUrl) return null;

  const u = new URL(proxyUrl);
  let username = u.username || undefined;

  if (username && u.hostname === "p.webshare.io" && countryCode) {
    // Webshare rotating residential uses username suffixes for country targeting.
    // Strip sticky-session / prior country / rotate suffixes and rebuild per request.
    const baseUsername = username
      .replace(/-\d+$/, "")
      .replace(/-(us|ca|au|nz)(-rotate)?$/i, "")
      .replace(/-rotate$/i, "");
    username = `${baseUsername}-${countryCode.toLowerCase()}-rotate`;
  } else if (username) {
    username = username.replace(/-\d+$/, "");
  }

  return {
    server: `${u.protocol}//${u.hostname}:${u.port}`,
    username: username || undefined,
    password: u.password || undefined,
  };
}

function getProxyUrlForCountry(config, countryCode) {
  const code = String(countryCode || "").toLowerCase();
  return config.ypProxyUrls?.[code] || config.ypProxyUrl || null;
}

async function fetchYPPage(locationTerm, keyword, page, config, siteConfig) {
  return queueYPRequest(siteConfig.countryName, async () => {
    const url = siteConfig.buildSearchUrl(keyword, locationTerm, page);

    // Sites without Cloudflare (e.g. yellowpages.ca) use plain fetch — much cheaper.
    if (siteConfig.usePlaywright === false) {
      return fetchYPPagePlain(url, locationTerm, config, siteConfig);
    }

    const browser = await getBrowser(config);
    const contextOptions = {
      locale: "en-US",
      userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    };
    const proxyConfig = buildProxyConfig(
      getProxyUrlForCountry(config, siteConfig.countryName),
      siteConfig.countryName
    );
    if (proxyConfig) contextOptions.proxy = proxyConfig;
    const context = await browser.newContext(contextOptions);
    const browserPage = await context.newPage();

    // Block images, fonts, and media — not needed for Cloudflare challenge or HTML parsing.
    await browserPage.route("**/*", (route) => {
      if (BLOCKED_RESOURCE_TYPES.has(route.request().resourceType())) {
        route.abort();
      } else {
        route.continue();
      }
    });

    try {
      const response = await browserPage.goto(url, {
        waitUntil: "load",
        timeout: config.ypTimeoutMs,
      });

      const status = response?.status() ?? 0;
      if (status === 404) {
        return { total: 0, results: [] };
      }

      const pageTitle = (await browserPage.title().catch(() => "")).toLowerCase();

      // "Attention Required!" = Cloudflare hard block (CAPTCHA) — cannot be solved programmatically.
      // Throw 403 so the shard retries/splits, giving a different IP a chance.
      if (pageTitle.includes("attention required") || pageTitle.includes("access denied")) {
        const error = new Error(`YellowPages returned 403 (Cloudflare hard block) for "${locationTerm}"`);
        error.statusCode = 403;
        throw error;
      }

      // "Just a moment..." = Cloudflare JS challenge — Playwright can solve it automatically.
      // Wait for the post-challenge navigation and then the content to settle.
      const isCfJsChallenge = status === 403 || status === 503 || pageTitle.includes("just a moment");
      if (isCfJsChallenge) {
        await browserPage.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 20000 }).catch(() => {});
      }

      if (!isCfJsChallenge && status >= 400) {
        const error = new Error(`YellowPages returned ${status} for "${locationTerm}"`);
        error.statusCode = status;
        throw error;
      }

      // If the CF challenge was not solved, treat it as a hard block so the shard retries
      // with a fresh proxy IP rather than silently returning 0 results.
      const titleAfterWait = (await browserPage.title().catch(() => "")).toLowerCase();
      if (titleAfterWait.includes("just a moment") || titleAfterWait.includes("attention required") || titleAfterWait.includes("access denied")) {
        const error = new Error(`YellowPages returned 403 (Cloudflare hard block) for "${locationTerm}"`);
        error.statusCode = 403;
        throw error;
      }

      // Wait for actual result cards to appear (up to 15s after challenge clears).
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

async function fetchYPPagePlain(url, locationTerm, config, siteConfig) {
  const fetchOptions = {
    headers: {
      "User-Agent": config.userAgent,
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.5",
    },
    signal: AbortSignal.timeout(config.ypTimeoutMs),
  };

  const response = await fetch(url, fetchOptions);

  if (response.status === 404) {
    return { total: 0, results: [] };
  }
  if (!response.ok) {
    const error = new Error(`YellowPages returned ${response.status} for "${locationTerm}"`);
    error.statusCode = response.status;
    throw error;
  }

  const html = await response.text();
  return siteConfig.parseResults(html);
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

  let total = null;
  const countText = $(".showing-count, .count-text").first().text() ||
    $("[class*='showing']").first().text() ||
    $("p:contains('of')").filter((_, el) => /\d+ of \d+/.test($(el).text())).first().text();
  const countMatch = countText.match(/of\s+([\d,]+)/i);
  if (countMatch) total = parseInt(countMatch[1].replace(/,/g, ""), 10);

  const results = [];

  // yellowpages.com.au uses the same HTML structure as yellowpages.com
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
      ? ypHref.startsWith("http") ? ypHref : `https://www.yellowpages.com.au${ypHref}`
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

    const reviewCountText = $el.find(".count, .review-count").first()
      .text().replace(/[()]/g, "").trim();
    const reviewCount = reviewCountText ? parseInt(reviewCountText, 10) : 0;

    results.push({
      name, ypId, ypUrl,
      phone: phone || null,
      website: website || null,
      street: street || null,
      city, stateRegion, postcode,
      country: "AU",
      categories,
      category: categories[0] || null,
      rating: Number.isFinite(rating) ? rating : null,
      reviewCount: Number.isFinite(reviewCount) ? reviewCount : 0,
    });
  });

  return { total, results };
}

function parseYPCaResults(html) {
  const $ = cheerio.load(html);

  // Result count: "(11450 Result(s))"
  let total = null;
  const countText = $(".resultCount").first().text();
  const countMatch = countText.match(/([\d,]+)/);
  if (countMatch) total = parseInt(countMatch[1].replace(/,/g, ""), 10);

  const results = [];

  // Each listing is a .listing__content div with a [data-merchanturl] child
  $(".listing__content").each((_, el) => {
    const $el = $(el);

    const name = $el.find(".listing__name--link").text().trim() || null;
    if (!name) return;

    // ID and URL from data-merchanturl="/bus/Province/City/Name/12345.html"
    const merchantPath = ($el.find("[data-merchanturl]").attr("data-merchanturl") || "").split("?")[0];
    const ypId = merchantPath ? merchantPath.replace(/\.html$/, "").split("/").filter(Boolean).pop() || null : null;
    const ypUrl = merchantPath ? `https://www.yellowpages.ca${merchantPath}` : null;

    // Phone: prefer tel: href (clean), fallback to text with digits only
    const telHref = $el.find("[href^='tel:']").first().attr("href");
    const phone = telHref
      ? telHref.replace("tel:", "").trim()
      : ($el.find("[class*='phone']").first().text().replace(/[^0-9+()\-\s]/g, "").trim() || null);

    const website = $el.find("[class*='website'] a, a[class*='website']").first().attr("href") ||
      $el.find("a[href^='http']:not([href*='yellowpages'])").first().attr("href") ||
      null;

    // Full address "123 Main St, Toronto, ON M5V 2H1" — split at first comma
    const fullAddress = $el.find(".listing__address--full").text().trim() || null;
    const commaIdx = fullAddress ? fullAddress.indexOf(",") : -1;
    const street = commaIdx > 0 ? fullAddress.slice(0, commaIdx).trim() : null;
    const localityRaw = commaIdx > 0 ? fullAddress.slice(commaIdx + 1).trim() : fullAddress;
    const { city, stateRegion, postcode } = parseLocality(localityRaw);

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

function slugAuPathSegment(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/['’]/g, "")
    .replace(/,/g, " ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

// ---------------------------------------------------------------------------
// Main query entry point
// ---------------------------------------------------------------------------

async function queryYellowPages({ job, shard, geometry, config, exhaustive = false }) {
  const center = bboxCenter(shard.bbox);
  const keyword = job.searchParams?.query || job.keyword;
  const siteConfig = getYPSiteConfig(job.countryCode);
  if (!siteConfig) {
    const error = new Error(`YellowPages scraping is not supported for country "${job.countryCode || job.country}".`);
    error.statusCode = 400;
    throw error;
  }

  const location = await reverseGeocode(center.lat, center.lon, config);
  if (!location?.locationTerm) {
    return { rawCount: 0, leads: [] };
  }

  const locationTerm = location.locationTerm;
  const requiresSpecificLocality = siteConfig.usePlaywright !== false;
  const isCoarsePlaywrightLocation =
    requiresSpecificLocality &&
    !location.city &&
    !location.postcode;

  if (isCoarsePlaywrightLocation) {
    const error = new Error(`Reverse geocode too coarse for "${locationTerm}"`);
    error.code = "COARSE_LOCATION";
    throw error;
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
  getSupportedCountryCodes,
};
