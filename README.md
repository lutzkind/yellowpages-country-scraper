# yellowpages-country-scraper

Country-scale YellowPages lead scraper. Give it a country and a keyword, and it recursively splits the country into geographic shards, reverse-geocodes each shard center to a city/state location term, queries YellowPages.com for each shard, deduplicates results, and stores leads in a local SQLite database. Includes a web dashboard, REST API, CSV/JSON exports, and optional NocoDB sync.

Built to match the architecture of [gmaps-country-scraper](https://github.com/lutzkind/gmaps-country-scraper), [yelp-country-scraper](https://github.com/lutzkind/yelp-country-scraper), and [osm-country-scraper](https://github.com/lutzkind/osm-country-scraper).

## How it works

1. A job is created for a country + keyword pair.
2. The country bounding box (resolved via Nominatim) is seeded as a single root shard.
3. A worker loop claims shards and probes each one by reverse-geocoding the shard center (Nominatim) to get a `city, STATE` or ZIP code string, then querying YellowPages.com.
4. If results hit the 30-result page cap and the shard can be split further (radius > `YP_TARGET_SHARD_RADIUS_METERS`), the shard is split into 4 child shards.
5. If the shard is at minimum size, it is exhaustively paginated (up to `YP_MAX_PAGES`).
6. Leads are deduplicated by YellowPages business ID and upserted into SQLite.
7. When all shards are terminal the job is finalized and artifacts (CSV + JSON) are written.

> **Note:** YellowPages.com is primarily a US directory. Best results when scraping US locations.

## Requirements

- Node.js 22+
- No API key required — YellowPages is scraped via HTTP.

## Setup

```bash
npm install
```

Create a `.env` file (or set environment variables):

```env
# Optional auth (leave blank to disable)
ADMIN_USERNAME=admin
ADMIN_PASSWORD=secret

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
