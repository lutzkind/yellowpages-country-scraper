# yellowpages-country-scraper

Country-scale YellowPages lead scraper. Give it a country and a keyword, and it recursively splits the country into geographic shards, reverse-geocodes each shard center to a city/state location term, queries the appropriate YellowPages site for each shard, deduplicates results, and stores leads in a local SQLite database. Includes a web dashboard, REST API, CSV/JSON exports, and optional NocoDB sync.

Built to match the architecture of [gmaps-country-scraper](https://github.com/lutzkind/gmaps-country-scraper), [yelp-country-scraper](https://github.com/lutzkind/yelp-country-scraper), and [osm-country-scraper](https://github.com/lutzkind/osm-country-scraper).

## Supported countries

| Country | Site | Results/page | Status |
|---------|------|-------------|--------|
| United States (`us`) | yellowpages.com | 30 | Working |
| Australia (`au`) | yellowpages.com.au | 25 | Working |
| Canada (`ca`) | yellowpages.ca | 20 | Working |
| New Zealand (`nz`) | yellowpages.co.nz | 20 | **Geo-restricted** (see below) |

When creating a job, set the **Country** field to the two-letter code (`us`, `au`, `ca`, `nz`). The scraper automatically routes requests to the correct YellowPages domain, uses the right URL format, and parses the site-specific HTML.

### New Zealand geo-restriction

`yellowpages.co.nz` is served behind AWS CloudFront with a geo-restriction that blocks all non-NZ IPs. This means:

- NZ jobs will fail unless the proxy exit IP is located in New Zealand.
- Webshare's rotating residential proxy pool has no New Zealand exit IPs.
- Alternative NZ directories (`finda.co.nz`, `nzpages.co.nz`) were also tested and are unreachable via non-NZ IPs.

To enable NZ scraping you need a proxy provider with New Zealand residential IPs. Set `YP_PROXY_URL` to that provider's endpoint when running NZ jobs.

## How it works

1. A job is created for a country + keyword pair.
2. The country bounding box (resolved via Nominatim) is seeded as a single root shard.
3. A worker loop claims shards and probes each one by reverse-geocoding the shard center (Nominatim) to get a `city, STATE` or suburb string, then querying the appropriate YellowPages site.
4. If results hit the per-site page cap and the shard can be split further (radius > `YP_TARGET_SHARD_RADIUS_METERS`), the shard is split into 4 child shards.
5. If the shard is at minimum size, it is exhaustively paginated (up to `YP_MAX_PAGES`).
6. Leads are deduplicated by YellowPages business ID and upserted into SQLite.
7. When all shards are terminal the job is finalized and artifacts (CSV + JSON) are written.

## Cloudflare bypass and bandwidth

| Source | Bot protection | Fetch method | Why |
|--------|---------------|--------------|-----|
| yellowpages.com | Cloudflare | Playwright | Real Chrome fingerprint needed |
| yellowpages.com.au | Cloudflare | Playwright | Real Chrome fingerprint needed |
| yellowpages.co.nz | Cloudflare | Playwright | Real Chrome fingerprint needed |
| yellowpages.ca | None | Plain `fetch` | No bot protection, ~5× cheaper |

Playwright blocks images, fonts, and media so only HTML + JS (needed for Cloudflare) is loaded. This cuts bandwidth by ~50–60% vs a full page load. Canada jobs skip Playwright entirely, reducing bandwidth by ~5×.

### Webshare rotating residential proxy

Set `YP_PROXY_URL` to your Webshare rotating endpoint. Webshare rotates the exit IP on every new connection automatically:

```env
YP_PROXY_URL=http://<username>:<password>@p.webshare.io:80
```

Webshare also offers a port-per-country option (port `80` = global rotation). Each new Playwright browser context opens a new connection, so every YP request gets a fresh IP.

A static IP burns out after ~200–500 YP requests. A rotating pool avoids this completely.

> **Note:** Webshare's rotating pool does not include New Zealand exit IPs. NZ jobs require a different proxy provider with NZ residential IPs.

### Stealth / Cloudflare bypass

US and AU jobs use [`playwright-extra`](https://github.com/berstend/puppeteer-extra/tree/master/packages/playwright-extra) with the `puppeteer-extra-plugin-stealth` plugin. This patches the Chromium fingerprint so Cloudflare Turnstile does not present a CAPTCHA challenge when browsing through residential proxy IPs.

### Bandwidth estimates

| Country | Avg shards | Avg pages/shard | Per-page (Playwright) | Total |
|---------|-----------|----------------|----------------------|-------|
| US | ~5,000 | 1.5 | ~250 KB | ~2 GB |
| AU | ~2,000 | 1.5 | ~250 KB | ~750 MB |
| CA | ~1,500 | 1.5 | ~50 KB (plain fetch) | ~110 MB |
| NZ | ~500 | 1.2 | ~250 KB | ~150 MB |

Residential proxy bandwidth at Webshare (~$3.50/GB): US ≈ $7, AU ≈ $2.60, CA ≈ $0.40, NZ ≈ $0.53.

## Requirements

- Node.js 22+
- Chromium installed at `CHROMIUM_PATH` (default: `/usr/bin/chromium`)
- No API key required — YellowPages is scraped via Playwright.

## Setup

```bash
npm install
```

Create a `.env` file (or set environment variables):

```env
# Optional auth (leave blank to disable)
ADMIN_USERNAME=admin
ADMIN_PASSWORD=secret

# Chromium path (default works for Debian/Ubuntu)
CHROMIUM_PATH=/usr/bin/chromium

# Webshare rotating residential proxy (strongly recommended for US/AU/NZ jobs)
YP_PROXY_URL=http://<username>:<password>@p.webshare.io:80

# Optional NocoDB sync
NOCODB_BASE_URL=https://nocodb.example.com
NOCODB_API_TOKEN=your_token
NOCODB_BASE_ID=your_base_id
NOCODB_TABLE_ID=your_table_id

# Optional tuning
YP_DELAY_MS=1500
YP_TARGET_SHARD_RADIUS_METERS=25000
```

## Running

```bash
node index.js
```

Open `http://localhost:3000/dashboard` to create jobs and monitor progress.

## Docker

```bash
docker build -t yellowpages-country-scraper .
docker run -d \
  -p 3000:3000 \
  -e ADMIN_USERNAME=admin \
  -e ADMIN_PASSWORD=secret \
  -v $(pwd)/data:/app/data \
  yellowpages-country-scraper
```
