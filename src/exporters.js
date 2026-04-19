const fs = require("fs");
const path = require("path");

function escapeCsv(value) {
  const text = value == null ? "" : String(value);
  if (/[",\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
  return text;
}

function writeArtifacts(store, config, jobId) {
  const job = store.getJob(jobId);
  const leads = store.getJobLeads(jobId, { limit: 1000000000, offset: 0 });
  const targetDir = path.join(config.exportsDir, jobId);
  fs.mkdirSync(targetDir, { recursive: true });

  const csvPath = path.join(targetDir, "leads.csv");
  const jsonPath = path.join(targetDir, "leads.json");

  const headers = [
    "query_name", "source", "country", "city", "area", "state_region",
    "postcode", "lead_country", "name", "category", "categories",
    "phone", "website", "address", "review_count", "review_rating",
    "yp_id", "yp_url",
  ];

  const rows = leads.map((lead) => ({
    queryName: job?.keyword || "",
    source: lead.source || "yellowpages",
    country: job?.country || "",
    city: lead.city,
    area: lead.area,
    stateRegion: lead.stateRegion,
    postcode: lead.postcode,
    leadCountry: lead.country,
    name: lead.name,
    category: lead.category,
    categories: Array.isArray(lead.categories) ? lead.categories.join(" | ") : "",
    phone: lead.phone,
    website: lead.website,
    address: lead.address,
    reviewCount: lead.reviewCount,
    reviewRating: lead.reviewRating,
    ypId: lead.placeId,
    ypUrl: lead.link,
  }));

  const csvLines = [
    headers.join(","),
    ...rows.map((row) =>
      [
        row.queryName, row.source, row.country, row.city, row.area,
        row.stateRegion, row.postcode, row.leadCountry, row.name,
        row.category, row.categories, row.phone, row.website, row.address,
        row.reviewCount, row.reviewRating, row.ypId, row.ypUrl,
      ].map(escapeCsv).join(",")
    ),
  ];

  fs.writeFileSync(csvPath, `${csvLines.join("\n")}\n`, "utf8");
  fs.writeFileSync(jsonPath, JSON.stringify(rows, null, 2), "utf8");

  return { csvPath, jsonPath };
}

module.exports = { writeArtifacts };
