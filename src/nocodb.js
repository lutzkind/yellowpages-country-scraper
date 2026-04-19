const STANDARD_FIELDS = [
  { name: "query_name", type: "SingleLineText" },
  { name: "source", type: "SingleLineText" },
  { name: "job_id", type: "SingleLineText" },
  { name: "country", type: "SingleLineText" },
  { name: "country_name", type: "SingleLineText" },
  { name: "country_code", type: "SingleLineText" },
  { name: "job_status", type: "SingleLineText" },
  { name: "yp_id", type: "SingleLineText" },
  { name: "yp_alias", type: "SingleLineText" },
  { name: "yp_url", type: "URL" },
  { name: "name", type: "SingleLineText" },
  { name: "category", type: "SingleLineText" },
  { name: "subcategory", type: "SingleLineText" },
  { name: "all_subcategories", type: "LongText" },
  { name: "categories_json", type: "LongText" },
  { name: "website", type: "URL" },
  { name: "phone", type: "PhoneNumber" },
  { name: "address", type: "LongText" },
  { name: "city", type: "SingleLineText" },
  { name: "area", type: "SingleLineText" },
  { name: "state_region", type: "SingleLineText" },
  { name: "postcode", type: "SingleLineText" },
  { name: "lead_country", type: "SingleLineText" },
  { name: "complete_address_json", type: "LongText" },
  { name: "review_count", type: "Number" },
  { name: "review_rating", type: "Decimal" },
  { name: "business_status", type: "SingleLineText" },
  { name: "price_range", type: "SingleLineText" },
  { name: "raw_json", type: "LongText" },
  { name: "source_bbox_json", type: "LongText" },
  { name: "scraped_at", type: "DateTime" },
  { name: "lead_created_at", type: "DateTime" },
  { name: "lead_updated_at", type: "DateTime" },
];

function createNocoDbService({ store, config }) {
  return {
    getConfig() {
      return toPublicConfig(store.getNocoDbConfig());
    },

    saveConfig(input) {
      const saved = store.saveNocoDbConfig(input);
      return toPublicConfig(saved);
    },

    async testConnection(input = null) {
      const settings = resolveSettings(store, input);
      validateSettings(settings);

      const columns = await listColumns(settings);
      return {
        ok: true,
        tableId: settings.tableId,
        columnCount: columns.length,
        autoSyncOnCompletion: settings.autoSyncOnCompletion,
        autoSyncIntervalMinutes: settings.autoSyncIntervalMinutes || 0,
        autoCreateColumns: settings.autoCreateColumns,
      };
    },

    getJobSyncStatus(jobId) {
      const settings = store.getNocoDbConfig();
      const sync = store.getNocoDbSyncState(jobId);
      return {
        enabled: hasEnoughSettings(settings),
        config: toPublicConfig(settings),
        sync,
        telemetry: buildSyncTelemetry(store, settings, jobId, sync),
      };
    },

    async syncJob(jobId, options = {}) {
      const settings = resolveSettings(store, options.config);
      validateSettings(settings);

      const job = store.getJob(jobId);
      if (!job) {
        throw createHttpError(404, "Job not found.");
      }

      store.markNocoDbSyncStarted(jobId);

      try {
        const desiredFields = buildDesiredFields();
        let columns = await listColumns(settings);
        let availableFields = collectColumnNames(columns);

        if (settings.autoCreateColumns) {
          const missingFields = desiredFields.filter(
            (field) => !availableFields.has(field.name)
          );

          for (const field of missingFields) {
            await createColumn(settings, field);
          }

          if (missingFields.length > 0) {
            columns = await listColumns(settings);
            availableFields = collectColumnNames(columns);
          }
        }

        const syncState = options.force
          ? defaultSyncState(jobId)
          : store.getNocoDbSyncState(jobId);

        let lastSyncedLeadId = options.force ? 0 : syncState.lastSyncedLeadId;
        let syncedRecordCount = 0;

        while (true) {
          const leads = store.getJobLeadsAfterId(jobId, lastSyncedLeadId, {
            limit: 100,
          });

          if (leads.length === 0) {
            break;
          }

          const records = leads.map((lead) => buildRecord(job, lead, availableFields));

          await createRecords(settings, records);
          syncedRecordCount += records.length;
          lastSyncedLeadId = leads[leads.length - 1].id;
        }

        const message = syncedRecordCount
          ? `Synced ${syncedRecordCount} lead records to NocoDB.`
          : "No new leads to sync.";

        store.markNocoDbSyncSuccess(jobId, {
          lastSyncedLeadId,
          syncedRecordCount,
          message,
        });

        return {
          ok: true,
          jobId,
          syncedRecordCount,
          config: toPublicConfig(settings),
          sync: store.getNocoDbSyncState(jobId),
        };
      } catch (error) {
        store.markNocoDbSyncFailure(jobId, error.message);
        throw error;
      }
    },

    async syncCompletedJobIfEnabled(jobId) {
      const settings = store.getNocoDbConfig();
      if (!settings.autoSyncOnCompletion || !hasEnoughSettings(settings)) {
        return null;
      }

      try {
        return await this.syncJob(jobId);
      } catch (error) {
        console.error(`NocoDB sync failed for job ${jobId}:`, error.message);
        return null;
      }
    },

    getRunningJobSyncIdsDue() {
      const settings = store.getNocoDbConfig();
      if (!hasEnoughSettings(settings)) {
        return [];
      }

      const intervalMinutes = Number(settings.autoSyncIntervalMinutes || 0);
      if (!intervalMinutes) {
        return [];
      }

      return store
        .listJobs()
        .filter((job) => job.status === "running")
        .filter((job) => {
          const syncState = store.getNocoDbSyncState(job.id);
          if (syncState.lastStatus === "running") {
            return false;
          }

          if (!store.countJobLeadsAfterId(job.id, syncState.lastSyncedLeadId || 0)) {
            return false;
          }

          const dueAt = getNextIncrementalSyncAt(job, syncState, settings);
          if (!dueAt) {
            return true;
          }

          return Date.now() >= Date.parse(dueAt);
        })
        .map((job) => job.id);
    },
  };
}

function resolveSettings(store, input) {
  if (!input) {
    return store.getNocoDbConfig();
  }

  const current = store.getNocoDbConfig();
  return sanitizeSettings({
    ...current,
    ...input,
    apiToken:
      input.apiToken == null || input.apiToken === ""
        ? current.apiToken
        : input.apiToken,
  });
}

function sanitizeSettings(input) {
  const autoSyncIntervalMinutes = Number.parseInt(
    String(input.autoSyncIntervalMinutes ?? "0"),
    10
  );

  return {
    baseUrl: cleanString(input.baseUrl),
    apiToken: cleanString(input.apiToken),
    baseId: cleanString(input.baseId),
    tableId: cleanString(input.tableId),
    autoSyncOnCompletion: Boolean(input.autoSyncOnCompletion),
    autoSyncIntervalMinutes:
      Number.isFinite(autoSyncIntervalMinutes) && autoSyncIntervalMinutes > 0
        ? autoSyncIntervalMinutes
        : 0,
    autoCreateColumns:
      input.autoCreateColumns == null ? true : Boolean(input.autoCreateColumns),
  };
}

function validateSettings(settings) {
  if (!hasEnoughSettings(settings)) {
    throw createHttpError(
      400,
      "NocoDB base URL, API token, base ID, and table ID are required."
    );
  }
}

function hasEnoughSettings(settings) {
  return Boolean(
    settings.baseUrl &&
      settings.apiToken &&
      settings.baseId &&
      settings.tableId
  );
}

function toPublicConfig(settings) {
  return {
    baseUrl: settings.baseUrl,
    baseId: settings.baseId,
    tableId: settings.tableId,
    autoSyncOnCompletion: Boolean(settings.autoSyncOnCompletion),
    autoSyncIntervalMinutes: settings.autoSyncIntervalMinutes || 0,
    autoCreateColumns: settings.autoCreateColumns !== false,
    hasApiToken: Boolean(settings.apiToken),
  };
}

function buildSyncTelemetry(store, settings, jobId, sync = null) {
  const syncState = sync || store.getNocoDbSyncState(jobId);
  const job = store.getJob(jobId);
  const unsyncedLeadCount = store.countJobLeadsAfterId(
    jobId,
    syncState.lastSyncedLeadId || 0
  );

  return {
    unsyncedLeadCount,
    nextDueAt: getNextIncrementalSyncAt(job, syncState, settings, unsyncedLeadCount),
  };
}

function getNextIncrementalSyncAt(job, syncState, settings, unsyncedLeadCount = null) {
  if (!job || job.status !== "running") {
    return null;
  }

  const intervalMinutes = Number(settings.autoSyncIntervalMinutes || 0);
  if (!intervalMinutes || syncState.lastStatus === "running") {
    return null;
  }

  const pendingCount =
    unsyncedLeadCount == null ? 0 : Number.parseInt(String(unsyncedLeadCount), 10);
  if (Number.isFinite(pendingCount) && pendingCount <= 0) {
    return null;
  }

  const referenceTime = syncState.lastSyncedAt || job.startedAt || job.updatedAt;
  if (!referenceTime) {
    return new Date().toISOString();
  }

  const dueAt = new Date(referenceTime).getTime() + intervalMinutes * 60_000;
  return Number.isNaN(dueAt) ? new Date().toISOString() : new Date(dueAt).toISOString();
}

function buildDesiredFields() {
  const byName = new Map();
  for (const field of STANDARD_FIELDS) {
    byName.set(field.name, field);
  }

  return [...byName.values()];
}

function buildRecord(job, lead, availableFields) {
  const record = {
    query_name: job.keyword,
    source: lead.source || "yellowpages",
    job_id: job.id,
    country: job.country,
    country_name: job.countryName || job.country,
    country_code: job.countryCode || "",
    job_status: job.status,
    yp_id: lead.placeId || "",
    yp_alias: lead.cid || "",
    yp_url: lead.link || "",
    name: lead.name || "",
    category: lead.category || "",
    subcategory: lead.subcategory || "",
    all_subcategories: Array.isArray(lead.allSubcategories)
      ? lead.allSubcategories.join(" | ")
      : "",
    categories_json: JSON.stringify(lead.categories || []),
    website: lead.website || "",
    phone: lead.phone || "",
    address: lead.address || "",
    city: lead.city || "",
    area: lead.area || "",
    state_region: lead.stateRegion || "",
    postcode: lead.postcode || "",
    lead_country: lead.country || "",
    complete_address_json: JSON.stringify(lead.completeAddress || null),
    review_count: lead.reviewCount || 0,
    review_rating: lead.reviewRating ?? "",
    business_status: lead.status || "",
    price_range: lead.priceRange || "",
    raw_json: JSON.stringify(lead.raw || {}),
    source_bbox_json: JSON.stringify(lead.sourceBBox || null),
    scraped_at: lead.updatedAt || lead.createdAt || new Date().toISOString(),
    lead_created_at: lead.createdAt || null,
    lead_updated_at: lead.updatedAt || null,
  };

  return Object.fromEntries(
    Object.entries(record).filter(([fieldName]) => availableFields.has(fieldName))
  );
}

async function listColumns(settings) {
  const payload = await apiRequestFallback(settings, [
    {
      pathname: `/api/v2/meta/tables/${encodeURIComponent(settings.tableId)}/columns`,
    },
    {
      pathname: `/api/v1/db/meta/tables/${encodeURIComponent(settings.tableId)}`,
      transform: (result) => result?.columns || [],
    },
  ]);

  if (Array.isArray(payload)) {
    return payload;
  }

  if (Array.isArray(payload?.list)) {
    return payload.list;
  }

  return [];
}

async function createColumn(settings, field) {
  const payload = {
    title: field.name,
    column_name: field.name,
    name: field.name,
    uidt: field.type,
    type: field.type,
  };

  return apiRequestFallback(settings, [
    {
      pathname: `/api/v2/base/${encodeURIComponent(settings.baseId)}/table/${encodeURIComponent(
        settings.tableId
      )}/column`,
      method: "POST",
      body: payload,
    },
    {
      pathname: `/api/v1/db/meta/tables/${encodeURIComponent(settings.tableId)}/columns`,
      method: "POST",
      body: payload,
    },
  ]);
}

async function createRecords(settings, records) {
  if (!records.length) {
    return null;
  }

  return apiRequestFallback(settings, [
    {
      pathname: `/api/v2/tables/${encodeURIComponent(settings.tableId)}/records`,
      method: "POST",
      body: records,
    },
    {
      pathname: `/api/v1/db/data/noco/${encodeURIComponent(settings.baseId)}/${encodeURIComponent(
        settings.tableId
      )}`,
      method: "POST",
      body: records,
    },
    {
      pathname: `/api/v1/db/data/noco/${encodeURIComponent(settings.baseId)}/${encodeURIComponent(
        settings.tableId
      )}`,
      method: "POST",
      body: { list: records },
    },
  ]);
}

async function apiRequest(settings, pathname, options = {}) {
  const response = await fetch(joinUrl(settings.baseUrl, pathname), {
    method: options.method || "GET",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "xc-auth": settings.apiToken,
      "xc-token": settings.apiToken,
    },
    body: options.body ? JSON.stringify(options.body) : undefined,
  });

  const text = await response.text();
  const payload = text ? safeJsonParse(text) : null;

  if (!response.ok) {
    const message =
      payload?.msg ||
      payload?.message ||
      payload?.error ||
      `NocoDB request failed with status ${response.status}.`;
    throw createHttpError(response.status, message);
  }

  return payload;
}

async function apiRequestFallback(settings, attempts) {
  let lastError = null;

  for (const attempt of attempts) {
    try {
      const result = await apiRequest(settings, attempt.pathname, attempt);
      return typeof attempt.transform === "function"
        ? attempt.transform(result)
        : result;
    } catch (error) {
      lastError = error;
      if (![400, 404].includes(error.statusCode)) {
        throw error;
      }
    }
  }

  throw lastError || createHttpError(500, "NocoDB request failed.");
}

function joinUrl(baseUrl, pathname) {
  return `${String(baseUrl || "").replace(/\/+$/, "")}${pathname}`;
}

function collectColumnNames(columns) {
  const names = new Set();
  for (const column of columns) {
    const candidates = [
      column.column_name,
      column.name,
      column.title,
      column.displayName,
    ];
    for (const value of candidates) {
      if (value) {
        names.add(String(value));
      }
    }
  }
  return names;
}

function defaultSyncState(jobId) {
  return {
    jobId,
    lastSyncedLeadId: 0,
    lastSyncedAt: null,
    lastStatus: "idle",
    lastMessage: null,
    lastStartedAt: null,
    lastFinishedAt: null,
    syncedRecordCount: 0,
  };
}

function cleanString(value) {
  const normalized = String(value || "").trim();
  return normalized || null;
}

function safeJsonParse(value) {
  try {
    return JSON.parse(value);
  } catch {
    return { message: value };
  }
}

function createHttpError(statusCode, message) {
  const error = new Error(message);
  error.statusCode = statusCode;
  return error;
}

module.exports = {
  createNocoDbService,
};
