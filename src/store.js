const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const Database = require("better-sqlite3");
const { extractLocationParts } = require("./yellowpages");

function nowIso() {
  return new Date().toISOString();
}

function createStore(config) {
  fs.mkdirSync(path.dirname(config.dbPath), { recursive: true });
  fs.mkdirSync(config.exportsDir, { recursive: true });

  const db = new Database(config.dbPath);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");

  db.exec(`
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      country TEXT NOT NULL,
      keyword TEXT NOT NULL,
      search_params_json TEXT NOT NULL,
      status TEXT NOT NULL,
      message TEXT,
      country_name TEXT,
      country_code TEXT,
      country_bbox_json TEXT,
      country_geometry_json TEXT,
      total_shards INTEGER NOT NULL DEFAULT 0,
      completed_shards INTEGER NOT NULL DEFAULT 0,
      failed_shards INTEGER NOT NULL DEFAULT 0,
      lead_count INTEGER NOT NULL DEFAULT 0,
      artifact_csv_path TEXT,
      artifact_json_path TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      last_claimed_at TEXT,
      started_at TEXT,
      finished_at TEXT
    );

    CREATE TABLE IF NOT EXISTS shards (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id TEXT NOT NULL,
      bbox_json TEXT NOT NULL,
      depth INTEGER NOT NULL,
      status TEXT NOT NULL,
      result_count INTEGER NOT NULL DEFAULT 0,
      attempt_count INTEGER NOT NULL DEFAULT 0,
      run_token TEXT,
      next_run_at TEXT NOT NULL,
      last_error TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_shards_status_next_run
      ON shards(status, next_run_at);

    CREATE TABLE IF NOT EXISTS leads (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id TEXT NOT NULL,
      dedupe_key TEXT NOT NULL,
      place_id TEXT,
      cid TEXT,
      data_id TEXT,
      link TEXT,
      name TEXT,
      category TEXT,
      categories_json TEXT NOT NULL,
      website TEXT,
      phone TEXT,
      email TEXT,
      address TEXT,
      complete_address_json TEXT NOT NULL,
      city TEXT,
      area TEXT,
      state_region TEXT,
      postcode TEXT,
      country TEXT,
      lat REAL,
      lon REAL,
      review_count INTEGER NOT NULL DEFAULT 0,
      review_rating REAL,
      business_status TEXT,
      price_range TEXT,
      source_bbox_json TEXT NOT NULL,
      raw_json TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(job_id, dedupe_key),
      FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS sessions (
      id TEXT PRIMARY KEY,
      username TEXT NOT NULL,
      expires_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      last_seen_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_sessions_expires_at
      ON sessions(expires_at);

    CREATE TABLE IF NOT EXISTS app_settings (
      key TEXT PRIMARY KEY,
      value_json TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS nocodb_sync_state (
      job_id TEXT PRIMARY KEY,
      last_synced_lead_id INTEGER NOT NULL DEFAULT 0,
      last_synced_at TEXT,
      last_status TEXT NOT NULL DEFAULT 'idle',
      last_message TEXT,
      last_started_at TEXT,
      last_finished_at TEXT,
      synced_record_count INTEGER NOT NULL DEFAULT 0,
      FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
    );
  `);

  ensureLeadColumns(db, "leads", [
    ["city", "TEXT"],
    ["area", "TEXT"],
    ["state_region", "TEXT"],
    ["postcode", "TEXT"],
    ["country", "TEXT"],
  ]);
  ensureLeadColumns(db, "shards", [["run_token", "TEXT"]]);
  ensureLeadColumns(db, "jobs", [["last_claimed_at", "TEXT"]]);
  migrateDropLatLonNotNull(db);
  backfillLeadLocations(db);

  resetRunningShards(db);
  cleanupExpiredSessions(db);

  function upsertJobLeads(jobId, leads, timestamp) {
    const insert = db.prepare(
      `
        INSERT INTO leads (
          job_id, dedupe_key, place_id, cid, data_id, link, name, category,
          categories_json, website, phone, email, address, complete_address_json,
          city, area, state_region, postcode, country, lat, lon, review_count,
          review_rating, business_status, price_range, source_bbox_json, raw_json,
          created_at, updated_at
        ) VALUES (
          @jobId, @dedupeKey, @placeId, @cid, @dataId, @link, @name, @category,
          @categoriesJson, @website, @phone, @email, @address, @completeAddressJson,
          @city, @area, @stateRegion, @postcode, @country, @lat, @lon,
          @reviewCount, @reviewRating, @businessStatus, @priceRange,
          @sourceBBoxJson, @rawJson, @timestamp, @timestamp
        )
        ON CONFLICT(job_id, dedupe_key) DO UPDATE SET
          place_id = COALESCE(excluded.place_id, leads.place_id),
          cid = COALESCE(excluded.cid, leads.cid),
          data_id = COALESCE(excluded.data_id, leads.data_id),
          link = COALESCE(excluded.link, leads.link),
          name = excluded.name,
          category = excluded.category,
          categories_json = excluded.categories_json,
          website = CASE
            WHEN COALESCE(leads.website, '') = '' THEN excluded.website
            ELSE leads.website
          END,
          phone = CASE
            WHEN COALESCE(leads.phone, '') = '' THEN excluded.phone
            ELSE leads.phone
          END,
          address = CASE
            WHEN COALESCE(leads.address, '') = '' THEN excluded.address
            ELSE leads.address
          END,
          complete_address_json = excluded.complete_address_json,
          city = CASE
            WHEN COALESCE(leads.city, '') = '' THEN excluded.city
            ELSE leads.city
          END,
          area = CASE
            WHEN COALESCE(leads.area, '') = '' THEN excluded.area
            ELSE leads.area
          END,
          state_region = CASE
            WHEN COALESCE(leads.state_region, '') = '' THEN excluded.state_region
            ELSE leads.state_region
          END,
          postcode = CASE
            WHEN COALESCE(leads.postcode, '') = '' THEN excluded.postcode
            ELSE leads.postcode
          END,
          country = CASE
            WHEN COALESCE(leads.country, '') = '' THEN excluded.country
            ELSE leads.country
          END,
          review_count = CASE
            WHEN excluded.review_count > COALESCE(leads.review_count, 0) THEN excluded.review_count
            ELSE leads.review_count
          END,
          review_rating = COALESCE(excluded.review_rating, leads.review_rating),
          business_status = COALESCE(excluded.business_status, leads.business_status),
          price_range = COALESCE(excluded.price_range, leads.price_range),
          raw_json = excluded.raw_json,
          updated_at = excluded.updated_at
      `
    );

    for (const lead of leads) {
      insert.run({
        jobId,
        dedupeKey: lead.dedupeKey,
        placeId: lead.placeId,
        cid: lead.cid,
        dataId: lead.dataId || null,
        link: lead.link,
        name: lead.name,
        category: lead.category,
        categoriesJson: JSON.stringify(lead.categories || []),
        website: lead.website || null,
        phone: lead.phone,
        email: lead.email || null,
        address: lead.address,
        completeAddressJson: JSON.stringify(lead.completeAddress || null),
        city: lead.city,
        area: lead.area,
        stateRegion: lead.stateRegion,
        postcode: lead.postcode,
        country: lead.country,
        lat: lead.lat,
        lon: lead.lon,
        reviewCount: lead.reviewCount || 0,
        reviewRating: lead.reviewRating,
        businessStatus: lead.status,
        priceRange: lead.priceRange,
        sourceBBoxJson: JSON.stringify(lead.bbox),
        rawJson: JSON.stringify(lead.raw || {}),
        timestamp,
      });
    }
  }

  return {
    db,
    createJob(input) {
      const timestamp = nowIso();
      db.prepare(
        `
          INSERT INTO jobs (
            id, country, keyword, search_params_json, status, message,
            country_name, country_code, country_bbox_json, country_geometry_json,
            created_at, updated_at
          ) VALUES (
            @id, @country, @keyword, @searchParamsJson, 'pending', 'Queued',
            NULL, NULL, NULL, NULL, @timestamp, @timestamp
          )
        `
      ).run({
        id: input.id,
        country: input.country,
        keyword: input.keyword,
        searchParamsJson: JSON.stringify(input.searchParams || {}),
        timestamp,
      });
    },

    seedJob(jobId, countryData) {
      const timestamp = nowIso();
      const seedBBoxes = Array.isArray(countryData.seedBBoxes) && countryData.seedBBoxes.length > 0
        ? countryData.seedBBoxes
        : [countryData.bbox];
      db.transaction(() => {
        db.prepare(
          `
            UPDATE jobs
            SET status = 'running',
                message = 'Running',
                country_name = @countryName,
                country_code = @countryCode,
                country_bbox_json = @bboxJson,
                country_geometry_json = @geometryJson,
                started_at = COALESCE(started_at, @timestamp),
                updated_at = @timestamp
            WHERE id = @jobId
          `
        ).run({
          jobId,
          countryName: countryData.displayName,
          countryCode: countryData.countryCode,
          bboxJson: JSON.stringify(countryData.bbox),
          geometryJson: JSON.stringify(countryData.geometry?.geometry || null),
          timestamp,
        });
        for (const bbox of seedBBoxes) {
          db.prepare(
            `
              INSERT INTO shards (
                job_id, bbox_json, depth, status, next_run_at,
                created_at, updated_at
              ) VALUES (
                @jobId, @bboxJson, 0, 'pending', @timestamp, @timestamp, @timestamp
              )
            `
          ).run({
            jobId,
            bboxJson: JSON.stringify(bbox),
            timestamp,
          });
        }
      })();

      this.refreshJobStats(jobId);
    },

    failJob(jobId, errorMessage) {
      db.prepare(
        `
          UPDATE jobs
          SET status = 'failed',
              message = @errorMessage,
              finished_at = @timestamp,
              updated_at = @timestamp
          WHERE id = @jobId
        `
      ).run({ jobId, errorMessage, timestamp: nowIso() });
    },

    listJobs() {
      return db
        .prepare(
          `
            SELECT *
            FROM jobs
            ORDER BY created_at DESC
          `
        )
        .all()
        .map(deserializeJobRow);
    },

    getJob(jobId) {
      const row = db
        .prepare(
          `
            SELECT *
            FROM jobs
            WHERE id = ?
          `
        )
        .get(jobId);

      return row ? deserializeJobRow(row) : null;
    },

    getJobLeads(jobId, { limit = 100, offset = 0 } = {}) {
      return db
        .prepare(
          `
            SELECT *
            FROM leads
            WHERE job_id = ?
            ORDER BY id ASC
            LIMIT ?
            OFFSET ?
          `
        )
        .all(jobId, limit, offset)
        .map(deserializeLeadRow);
    },

    countJobShards(jobId, status = null) {
      const row = status
        ? db
            .prepare(
              `
                SELECT COUNT(*) AS total
                FROM shards
                WHERE job_id = ?
                  AND status = ?
              `
            )
            .get(jobId, status)
        : db
            .prepare(
              `
                SELECT COUNT(*) AS total
                FROM shards
                WHERE job_id = ?
              `
            )
            .get(jobId);

      return row?.total || 0;
    },

    listJobShards(jobId, { status = null, limit = 100, offset = 0 } = {}) {
      const rows = status
        ? db
            .prepare(
              `
                SELECT *
                FROM shards
                WHERE job_id = ?
                  AND status = ?
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
                OFFSET ?
              `
            )
            .all(jobId, status, limit, offset)
        : db
            .prepare(
              `
                SELECT *
                FROM shards
                WHERE job_id = ?
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
                OFFSET ?
              `
            )
            .all(jobId, limit, offset);

      return rows.map(deserializeShardRow);
    },

    getJobErrors(jobId, { limit = 25 } = {}) {
      return db
        .prepare(
          `
            SELECT *
            FROM shards
            WHERE job_id = ?
              AND COALESCE(last_error, '') != ''
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
          `
        )
        .all(jobId, limit)
        .map(deserializeShardRow);
    },

    getJobStats(jobId) {
      const job = this.getJob(jobId);
      if (!job) {
        return null;
      }

      const shardStats = db
        .prepare(
          `
            SELECT
              COUNT(*) AS total_shards,
              SUM(CASE WHEN s.status = 'pending' THEN 1 ELSE 0 END) AS pending_shards,
              SUM(CASE WHEN s.status = 'retry' THEN 1 ELSE 0 END) AS retry_shards,
              SUM(CASE WHEN s.status = 'running' THEN 1 ELSE 0 END) AS running_shards,
              SUM(CASE WHEN j.status = 'paused' AND s.status IN ('pending', 'retry') THEN 1 ELSE 0 END) AS paused_shards,
              SUM(CASE WHEN s.status = 'done' THEN 1 ELSE 0 END) AS done_shards,
              SUM(CASE WHEN s.status = 'failed' THEN 1 ELSE 0 END) AS failed_shards,
              SUM(CASE WHEN s.status = 'split' THEN 1 ELSE 0 END) AS split_shards,
              SUM(CASE WHEN s.status = 'skipped' THEN 1 ELSE 0 END) AS skipped_shards,
              SUM(CASE WHEN s.status = 'canceled' THEN 1 ELSE 0 END) AS canceled_shards,
              SUM(CASE WHEN s.status IN ('done', 'failed', 'split', 'skipped', 'canceled') THEN 1 ELSE 0 END) AS terminal_shards,
              SUM(s.result_count) AS shard_result_count,
              SUM(s.attempt_count) AS total_attempts,
              MAX(s.depth) AS max_depth,
              MIN(CASE WHEN s.status IN ('pending', 'retry') THEN s.next_run_at END) AS next_run_at,
              MAX(s.updated_at) AS last_activity_at
            FROM shards s
            JOIN jobs j ON j.id = s.job_id
            WHERE s.job_id = ?
          `
        )
        .get(jobId);

      const websiteStats = db
        .prepare(
          `
            SELECT
              SUM(CASE WHEN COALESCE(website, '') != '' THEN 1 ELSE 0 END) AS leads_with_website,
              SUM(CASE WHEN COALESCE(phone, '') != '' THEN 1 ELSE 0 END) AS leads_with_phone
            FROM leads
            WHERE job_id = ?
          `
        )
        .get(jobId);

      const recentLeadStats = db
        .prepare(
          `
            SELECT
              SUM(CASE WHEN created_at >= @oneHourAgo THEN 1 ELSE 0 END) AS leads_last_hour,
              SUM(CASE WHEN created_at >= @oneDayAgo THEN 1 ELSE 0 END) AS leads_last_day
            FROM leads
            WHERE job_id = @jobId
          `
        )
        .get({
          jobId,
          oneHourAgo: new Date(Date.now() - 60 * 60 * 1000).toISOString(),
          oneDayAgo: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
        });

      const referenceStart = job.startedAt || job.createdAt;
      const referenceEnd =
        job.finishedAt && ["completed", "partial", "failed", "canceled"].includes(job.status)
          ? job.finishedAt
          : job.status === "paused"
            ? job.updatedAt
          : nowIso();
      const elapsedMs = Math.max(
        0,
        new Date(referenceEnd).getTime() - new Date(referenceStart).getTime()
      );
      const elapsedHours = elapsedMs / (60 * 60 * 1000);
      const safeElapsedHours = elapsedHours > 0 ? elapsedHours : null;

      return {
        statusCounts: {
          pending: shardStats.pending_shards || 0,
          retry: shardStats.retry_shards || 0,
          running: shardStats.running_shards || 0,
          paused: shardStats.paused_shards || 0,
          done: shardStats.done_shards || 0,
          failed: shardStats.failed_shards || 0,
          split: shardStats.split_shards || 0,
          skipped: shardStats.skipped_shards || 0,
          canceled: shardStats.canceled_shards || 0,
          terminal: shardStats.terminal_shards || 0,
          total: shardStats.total_shards || 0,
        },
        leadCoverage: {
          leadsWithWebsite: websiteStats.leads_with_website || 0,
          leadsWithPhone: websiteStats.leads_with_phone || 0,
        },
        recentActivity: {
          leadsLastHour: recentLeadStats.leads_last_hour || 0,
          leadsLastDay: recentLeadStats.leads_last_day || 0,
          nextRunAt: shardStats.next_run_at || null,
          lastActivityAt: shardStats.last_activity_at || job.updatedAt,
        },
        throughput: {
          leadsPerHour: safeElapsedHours
            ? Number((job.leadCount / safeElapsedHours).toFixed(2))
            : null,
          completedShardsPerHour: safeElapsedHours
            ? Number((job.completedShards / safeElapsedHours).toFixed(2))
            : null,
        },
        progress: {
          knownShardCompletionRatio:
            (shardStats.total_shards || 0) > 0
              ? Number(
                  (
                    ((shardStats.terminal_shards || 0) / shardStats.total_shards) *
                    100
                  ).toFixed(2)
                )
              : 0,
        },
        depth: {
          maxDepth: shardStats.max_depth || 0,
        },
        attempts: {
          totalAttempts: shardStats.total_attempts || 0,
        },
        elapsed: {
          startedAt: referenceStart,
          finishedAt: job.finishedAt,
          elapsedMs,
          elapsedHours: Number(elapsedHours.toFixed(2)),
        },
      };
    },

    createSession({ id, username, expiresAt }) {
      const timestamp = nowIso();
      db.prepare(
        `
          INSERT INTO sessions (
            id, username, expires_at, created_at, last_seen_at
          ) VALUES (
            @id, @username, @expiresAt, @timestamp, @timestamp
          )
        `
      ).run({
        id,
        username,
        expiresAt,
        timestamp,
      });
    },

    getSession(sessionId) {
      const row = db
        .prepare(
          `
            SELECT *
            FROM sessions
            WHERE id = ?
          `
        )
        .get(sessionId);

      if (!row) {
        return null;
      }

      if (new Date(row.expires_at).getTime() <= Date.now()) {
        db.prepare(`DELETE FROM sessions WHERE id = ?`).run(sessionId);
        return null;
      }

      return {
        id: row.id,
        username: row.username,
        expiresAt: row.expires_at,
        createdAt: row.created_at,
        lastSeenAt: row.last_seen_at,
      };
    },

    touchSession(sessionId, expiresAt) {
      const timestamp = nowIso();
      db.prepare(
        `
          UPDATE sessions
          SET expires_at = @expiresAt,
              last_seen_at = @timestamp
          WHERE id = @id
        `
      ).run({
        id: sessionId,
        expiresAt,
        timestamp,
      });
    },

    deleteSession(sessionId) {
      db.prepare(`DELETE FROM sessions WHERE id = ?`).run(sessionId);
    },

    cleanupExpiredSessions() {
      cleanupExpiredSessions(db);
    },

    getAppSetting(key, fallback = null) {
      const row = db
        .prepare(
          `
            SELECT value_json
            FROM app_settings
            WHERE key = ?
          `
        )
        .get(key);

      if (!row) {
        return fallback;
      }

      return parseJsonOrFallback(row.value_json, fallback);
    },

    setAppSettings(settings) {
      const timestamp = nowIso();
      const upsert = db.prepare(
        `
          INSERT INTO app_settings (key, value_json, updated_at)
          VALUES (@key, @valueJson, @timestamp)
          ON CONFLICT(key) DO UPDATE SET
            value_json = excluded.value_json,
            updated_at = excluded.updated_at
        `
      );

      db.transaction(() => {
        for (const [key, value] of Object.entries(settings)) {
          upsert.run({
            key,
            valueJson: JSON.stringify(value),
            timestamp,
          });
        }
      })();
    },

    getNocoDbConfig() {
      return sanitizeNocoDbConfig({
        baseUrl: this.getAppSetting("nocodb.baseUrl", config.nocoDb.baseUrl),
        apiToken: this.getAppSetting("nocodb.apiToken", config.nocoDb.apiToken),
        baseId: this.getAppSetting("nocodb.baseId", config.nocoDb.baseId),
        tableId: this.getAppSetting("nocodb.tableId", config.nocoDb.tableId),
        autoSyncOnCompletion: this.getAppSetting(
          "nocodb.autoSyncOnCompletion",
          config.nocoDb.autoSyncOnCompletion
        ),
        autoSyncIntervalMinutes: this.getAppSetting(
          "nocodb.autoSyncIntervalMinutes",
          config.nocoDb.autoSyncIntervalMinutes
        ),
        autoCreateColumns: this.getAppSetting(
          "nocodb.autoCreateColumns",
          config.nocoDb.autoCreateColumns
        ),
      });
    },

    saveNocoDbConfig(input) {
      const current = this.getNocoDbConfig();
      const next = sanitizeNocoDbConfig({
        ...current,
        ...input,
        apiToken:
          input.apiToken == null || input.apiToken === ""
            ? current.apiToken
            : input.apiToken,
      });

      this.setAppSettings({
        "nocodb.baseUrl": next.baseUrl,
        "nocodb.apiToken": next.apiToken,
        "nocodb.baseId": next.baseId,
        "nocodb.tableId": next.tableId,
        "nocodb.autoSyncOnCompletion": next.autoSyncOnCompletion,
        "nocodb.autoSyncIntervalMinutes": next.autoSyncIntervalMinutes,
        "nocodb.autoCreateColumns": next.autoCreateColumns,
      });

      return next;
    },

    getNocoDbSyncState(jobId) {
      const row = db
        .prepare(
          `
            SELECT *
            FROM nocodb_sync_state
            WHERE job_id = ?
          `
        )
        .get(jobId);

      return row ? deserializeSyncStateRow(row) : defaultSyncState(jobId);
    },

    markNocoDbSyncStarted(jobId) {
      const timestamp = nowIso();
      db.prepare(
        `
          INSERT INTO nocodb_sync_state (
            job_id, last_status, last_message, last_started_at, last_finished_at
          ) VALUES (
            @jobId, 'running', 'Sync in progress.', @timestamp, NULL
          )
          ON CONFLICT(job_id) DO UPDATE SET
            last_status = 'running',
            last_message = 'Sync in progress.',
            last_started_at = excluded.last_started_at,
            last_finished_at = NULL
        `
      ).run({ jobId, timestamp });
    },

    markNocoDbSyncSuccess(jobId, input) {
      const timestamp = nowIso();
      db.prepare(
        `
          INSERT INTO nocodb_sync_state (
            job_id, last_synced_lead_id, last_synced_at, last_status,
            last_message, last_started_at, last_finished_at, synced_record_count
          ) VALUES (
            @jobId, @lastSyncedLeadId, @timestamp, 'success',
            @message, COALESCE(@startedAt, @timestamp), @timestamp, @syncedRecordCount
          )
          ON CONFLICT(job_id) DO UPDATE SET
            last_synced_lead_id = @lastSyncedLeadId,
            last_synced_at = @timestamp,
            last_status = 'success',
            last_message = @message,
            last_finished_at = @timestamp,
            synced_record_count = COALESCE(nocodb_sync_state.synced_record_count, 0) + @syncedRecordCount
        `
      ).run({
        jobId,
        lastSyncedLeadId: input.lastSyncedLeadId || 0,
        syncedRecordCount: input.syncedRecordCount || 0,
        message: input.message || "Sync completed.",
        startedAt: input.startedAt || null,
        timestamp,
      });
    },

    markNocoDbSyncFailure(jobId, message) {
      const timestamp = nowIso();
      db.prepare(
        `
          INSERT INTO nocodb_sync_state (
            job_id, last_status, last_message, last_started_at, last_finished_at
          ) VALUES (
            @jobId, 'failed', @message, @timestamp, @timestamp
          )
          ON CONFLICT(job_id) DO UPDATE SET
            last_status = 'failed',
            last_message = @message,
            last_finished_at = @timestamp
        `
      ).run({ jobId, message, timestamp });
    },

    cancelJob(jobId) {
      const timestamp = nowIso();
      db.transaction(() => {
        db.prepare(
          `
            UPDATE jobs
            SET status = 'canceled',
                message = 'Canceled',
                finished_at = @timestamp,
                updated_at = @timestamp
            WHERE id = @jobId
          `
        ).run({ jobId, timestamp });

        db.prepare(
          `
            UPDATE shards
            SET status = 'canceled',
                updated_at = @timestamp
            WHERE job_id = @jobId
              AND status IN ('pending', 'retry', 'running')
          `
        ).run({ jobId, timestamp });
      })();

      this.refreshJobStats(jobId);
      return this.getJob(jobId);
    },

    pauseJob(jobId) {
      db.prepare(
        `
          UPDATE jobs
          SET status = 'paused',
              message = 'Paused by operator.',
              updated_at = @timestamp
          WHERE id = @jobId
        `
      ).run({ jobId, timestamp: nowIso() });

      return this.getJob(jobId);
    },

    resumeJob(jobId) {
      const job = this.getJob(jobId);
      if (!job) {
        return null;
      }

      const resumedStatus =
        job.totalShards > 0 || job.startedAt ? "running" : "pending";
      const message = resumedStatus === "running" ? "Running" : "Queued";

      db.prepare(
        `
          UPDATE jobs
          SET status = @status,
              message = @message,
              updated_at = @timestamp
          WHERE id = @jobId
        `
      ).run({
        jobId,
        status: resumedStatus,
        message,
        timestamp: nowIso(),
      });

      return this.getJob(jobId);
    },

    deleteJob(jobId) {
      const job = this.getJob(jobId);
      if (!job) {
        return null;
      }

      if (!["completed", "partial", "failed", "canceled"].includes(job.status)) {
        const error = new Error(
          "Only completed, partial, failed, or canceled jobs can be deleted."
        );
        error.statusCode = 409;
        throw error;
      }

      const artifactPaths = [job.artifactCsvPath, job.artifactJsonPath].filter(Boolean);

      db.transaction(() => {
        db.prepare(`DELETE FROM jobs WHERE id = ?`).run(jobId);
      })();

      for (const artifactPath of artifactPaths) {
        try {
          fs.unlinkSync(artifactPath);
        } catch (error) {
          if (error.code !== "ENOENT") {
            throw error;
          }
        }
      }

      return job;
    },

    getJobLeadsAfterId(jobId, leadId = 0, { limit = 100 } = {}) {
      return db
        .prepare(
          `
            SELECT *
            FROM leads
            WHERE job_id = ?
              AND id > ?
            ORDER BY id ASC
            LIMIT ?
          `
        )
        .all(jobId, leadId, limit)
        .map(deserializeLeadRow);
    },

    countJobLeadsAfterId(jobId, leadId = 0) {
      const row = db
        .prepare(
          `
            SELECT COUNT(*) AS total
            FROM leads
            WHERE job_id = ?
              AND id > ?
          `
        )
        .get(jobId, leadId);

      return row?.total || 0;
    },

    claimNextShard() {
      const timestamp = nowIso();
      const runToken = crypto.randomUUID();
      const row = db
        .prepare(
          `
            SELECT s.*
            FROM shards s
            JOIN jobs j ON j.id = s.job_id
            WHERE s.status IN ('pending', 'retry')
              AND s.next_run_at <= @timestamp
              AND j.status = 'running'
              AND s.id = (
                SELECT s2.id
                FROM shards s2
                WHERE s2.job_id = s.job_id
                  AND s2.status IN ('pending', 'retry')
                  AND s2.next_run_at <= @timestamp
                ORDER BY CASE s2.status WHEN 'pending' THEN 0 ELSE 1 END ASC,
                         s2.updated_at ASC,
                         s2.depth DESC,
                         s2.id ASC
                LIMIT 1
              )
            ORDER BY COALESCE(j.last_claimed_at, j.created_at) ASC,
                     CASE WHEN j.completed_shards = 0 THEN s.depth ELSE NULL END ASC,
                     CASE WHEN j.completed_shards > 0 THEN s.depth ELSE NULL END DESC,
                     s.updated_at ASC,
                     s.id ASC
            LIMIT 1
          `
        )
        .get({ timestamp });

      if (!row) {
        return null;
      }

      db.prepare(
        `
          UPDATE shards
          SET status = 'running',
              attempt_count = attempt_count + 1,
              updated_at = @timestamp,
              run_token = @runToken
          WHERE id = @id
        `
      ).run({ id: row.id, timestamp, runToken });

      db.prepare(
        `
          UPDATE jobs
          SET updated_at = @timestamp
             ,last_claimed_at = @timestamp
          WHERE id = @jobId
        `
      ).run({ jobId: row.job_id, timestamp });

      return this.getShard(row.id);
    },

    getShard(shardId) {
      const row = db
        .prepare(
          `
            SELECT *
            FROM shards
            WHERE id = ?
          `
        )
        .get(shardId);

      return row ? deserializeShardRow(row) : null;
    },

    splitShard(shardId, childBBoxes, runToken = null) {
      const shard = this.getShard(shardId);
      if (!shard) {
        return null;
      }

      const timestamp = nowIso();

      const splitJobId = db.transaction(() => {
        const updated = db.prepare(
          `
            UPDATE shards
            SET status = 'split',
                run_token = NULL,
                updated_at = @timestamp
            WHERE id = @id
              ${buildOwnedRunningShardClause(runToken)}
          `
        ).run(buildOwnedRunningShardParams({ id: shardId, timestamp, runToken }));

        if (updated.changes === 0) {
          return null;
        }

        const insert = db.prepare(
          `
            INSERT INTO shards (
              job_id, bbox_json, depth, status, next_run_at, created_at, updated_at
            ) VALUES (
              @jobId, @bboxJson, @depth, 'pending', @timestamp, @timestamp, @timestamp
            )
          `
        );

        for (const bbox of childBBoxes) {
          insert.run({
            jobId: shard.jobId,
            bboxJson: JSON.stringify(bbox),
            depth: shard.depth + 1,
            timestamp,
          });
        }
        return shard.jobId;
      })();

      if (!splitJobId) {
        return null;
      }

      this.refreshJobStats(splitJobId);
      return splitJobId;
    },

    skipShard(shardId, message, runToken = null) {
      const shard = this.getShard(shardId);
      if (!shard) {
        return null;
      }

      const result = db.prepare(
        `
          UPDATE shards
          SET status = 'skipped',
              run_token = NULL,
              last_error = @message,
              updated_at = @timestamp
          WHERE id = @id
            ${buildOwnedRunningShardClause(runToken)}
        `
      ).run(buildOwnedRunningShardParams({
        id: shardId,
        message,
        timestamp: nowIso(),
        runToken,
      }));

      if (result.changes === 0) {
        return null;
      }

      this.refreshJobStats(shard.jobId);
      return shard.jobId;
    },

    completeShard(shardId, leads, runToken = null) {
      const shard = this.getShard(shardId);
      if (!shard) {
        return null;
      }

      const timestamp = nowIso();

      const completedJobId = db.transaction(() => {
        const ownedShard = db
          .prepare(
            `
              SELECT id
              FROM shards
              WHERE id = @id
                ${buildOwnedRunningShardClause(runToken)}
            `
          )
          .get(buildOwnedRunningShardParams({ id: shardId, runToken }));

        if (!ownedShard) {
          return null;
        }

        upsertJobLeads(shard.jobId, leads, timestamp);

        db.prepare(
          `
            UPDATE shards
            SET status = 'done',
                result_count = @resultCount,
                run_token = NULL,
                last_error = NULL,
                updated_at = @timestamp
            WHERE id = @id
              ${buildOwnedRunningShardClause(runToken)}
          `
        ).run(buildOwnedRunningShardParams({
          id: shardId,
          resultCount: leads.length,
          timestamp,
          runToken,
        }));

        return shard.jobId;
      })();

      if (!completedJobId) {
        return null;
      }

      this.refreshJobStats(completedJobId);
      return completedJobId;
    },

    upsertLeads(jobId, leads) {
      if (!Array.isArray(leads) || leads.length === 0) {
        return 0;
      }

      const timestamp = nowIso();
      db.transaction(() => {
        upsertJobLeads(jobId, leads, timestamp);
      })();
      this.refreshJobStats(jobId);
      return leads.length;
    },

    retryShard(shardId, errorMessage, delayMs, runToken = null) {
      const shard = this.getShard(shardId);
      if (!shard) {
        return null;
      }

      const nextRunAt = new Date(Date.now() + delayMs).toISOString();
      const result = db.prepare(
        `
          UPDATE shards
          SET status = 'retry',
              run_token = NULL,
              last_error = @errorMessage,
              next_run_at = @nextRunAt,
              updated_at = @timestamp
          WHERE id = @id
            ${buildOwnedRunningShardClause(runToken)}
        `
      ).run(buildOwnedRunningShardParams({
        id: shardId,
        errorMessage,
        nextRunAt,
        timestamp: nowIso(),
        runToken,
      }));

      if (result.changes === 0) {
        return null;
      }

      this.refreshJobStats(shard.jobId);
      return shard.jobId;
    },

    failShard(shardId, errorMessage, runToken = null) {
      const shard = this.getShard(shardId);
      if (!shard) {
        return null;
      }

      const result = db.prepare(
        `
          UPDATE shards
          SET status = 'failed',
              run_token = NULL,
              last_error = @errorMessage,
              updated_at = @timestamp
          WHERE id = @id
            ${buildOwnedRunningShardClause(runToken)}
        `
      ).run(buildOwnedRunningShardParams({
        id: shardId,
        errorMessage,
        timestamp: nowIso(),
        runToken,
      }));

      if (result.changes === 0) {
        return null;
      }

      this.refreshJobStats(shard.jobId);
      return shard.jobId;
    },

    reclaimStaleRunningShards(staleMs) {
      const staleBefore = new Date(Date.now() - staleMs).toISOString();
      const timestamp = nowIso();
      const staleJobs = db
        .prepare(
          `
            SELECT DISTINCT job_id AS jobId
            FROM shards
            WHERE status = 'running'
              AND updated_at <= @staleBefore
          `
        )
        .all({ staleBefore })
        .map((row) => row.jobId);

      if (staleJobs.length === 0) {
        return [];
      }

      db.prepare(
        `
          UPDATE shards
          SET status = 'retry',
              run_token = NULL,
              next_run_at = @timestamp,
              updated_at = @timestamp,
              last_error = CASE
                WHEN COALESCE(last_error, '') = '' THEN @message
                ELSE last_error
              END
          WHERE status = 'running'
            AND updated_at <= @staleBefore
        `
      ).run({
        staleBefore,
        timestamp,
        message: "Recovered after running shard exceeded the stale timeout.",
      });

      for (const jobId of staleJobs) {
        this.refreshJobStats(jobId);
      }

      return staleJobs;
    },

    refreshJobStats(jobId) {
      const shardStats = db
        .prepare(
          `
            SELECT
              COUNT(*) AS total_shards,
              SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) AS completed_shards,
              SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_shards,
              SUM(CASE WHEN status IN ('pending', 'retry', 'running') THEN 1 ELSE 0 END) AS unfinished_shards
            FROM shards
            WHERE job_id = ?
          `
        )
        .get(jobId);

      const leadStats = db
        .prepare(
          `
            SELECT COUNT(*) AS lead_count
            FROM leads
            WHERE job_id = ?
          `
        )
        .get(jobId);

      db.prepare(
        `
          UPDATE jobs
          SET total_shards = @totalShards,
              completed_shards = @completedShards,
              failed_shards = @failedShards,
              lead_count = @leadCount,
              updated_at = @timestamp
          WHERE id = @jobId
        `
      ).run({
        jobId,
        totalShards: shardStats.total_shards || 0,
        completedShards: shardStats.completed_shards || 0,
        failedShards: shardStats.failed_shards || 0,
        leadCount: leadStats.lead_count || 0,
        timestamp: nowIso(),
      });

      return shardStats.unfinished_shards || 0;
    },

    finalizeJob(jobId, status, message, artifacts = {}) {
      db.prepare(
        `
          UPDATE jobs
          SET status = @status,
              message = @message,
              artifact_csv_path = COALESCE(@csvPath, artifact_csv_path),
              artifact_json_path = COALESCE(@jsonPath, artifact_json_path),
              finished_at = @timestamp,
              updated_at = @timestamp
          WHERE id = @jobId
        `
      ).run({
        jobId,
        status,
        message,
        csvPath: artifacts.csvPath || null,
        jsonPath: artifacts.jsonPath || null,
        timestamp: nowIso(),
      });
    },
  };
}

function resetRunningShards(db) {
  const timestamp = nowIso();

  db.prepare(
    `
      UPDATE shards
      SET status = 'retry',
          run_token = NULL,
          next_run_at = @timestamp,
          updated_at = @timestamp,
          last_error = COALESCE(last_error, 'Recovered after process restart.')
      WHERE status = 'running'
    `
  ).run({ timestamp });

  db.prepare(
    `
      UPDATE jobs
      SET status = CASE
        WHEN status = 'running' THEN 'pending'
        ELSE status
      END,
      message = CASE
        WHEN status = 'running' THEN 'Recovered after process restart.'
        ELSE message
      END,
      updated_at = @timestamp
      WHERE status = 'running'
    `
  ).run({ timestamp });
}

function cleanupExpiredSessions(db) {
  db.prepare(
    `
      DELETE FROM sessions
      WHERE expires_at <= @timestamp
    `
  ).run({ timestamp: nowIso() });
}

function buildOwnedRunningShardClause(runToken) {
  return runToken
    ? "AND status = 'running' AND run_token = @runToken"
    : "";
}

function buildOwnedRunningShardParams(params) {
  return params.runToken ? params : omitRunToken(params);
}

function omitRunToken(params) {
  const { runToken, ...rest } = params;
  return rest;
}

function migrateDropLatLonNotNull(db) {
  const cols = db.prepare("PRAGMA table_info(leads)").all();
  const latCol = cols.find((c) => c.name === "lat");
  if (!latCol || !latCol.notnull) return;
  db.pragma("foreign_keys = OFF");
  db.transaction(() => {
    db.exec(`
      CREATE TABLE leads_migration (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT NOT NULL,
        dedupe_key TEXT NOT NULL,
        place_id TEXT, cid TEXT, data_id TEXT, link TEXT, name TEXT, category TEXT,
        categories_json TEXT NOT NULL,
        website TEXT, phone TEXT, email TEXT, address TEXT,
        complete_address_json TEXT NOT NULL,
        city TEXT, area TEXT, state_region TEXT, postcode TEXT, country TEXT,
        lat REAL, lon REAL,
        review_count INTEGER NOT NULL DEFAULT 0,
        review_rating REAL, business_status TEXT, price_range TEXT,
        source_bbox_json TEXT NOT NULL,
        raw_json TEXT NOT NULL,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        UNIQUE(job_id, dedupe_key),
        FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
      );
      INSERT INTO leads_migration SELECT * FROM leads;
      DROP TABLE leads;
      ALTER TABLE leads_migration RENAME TO leads;
    `);
  })();
  db.pragma("foreign_keys = ON");
}

function ensureLeadColumns(db, tableName, columns) {
  const existing = new Set(
    db
      .prepare(`PRAGMA table_info(${tableName})`)
      .all()
      .map((column) => column.name)
  );

  for (const [name, type] of columns) {
    if (!existing.has(name)) {
      db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${name} ${type}`);
    }
  }
}

function backfillLeadLocations(db) {
  const rows = db
    .prepare(
      `
        SELECT id, raw_json, complete_address_json, city, area, state_region, postcode, country
        FROM leads
        WHERE COALESCE(city, '') = ''
           OR COALESCE(area, '') = ''
           OR COALESCE(state_region, '') = ''
           OR COALESCE(postcode, '') = ''
           OR COALESCE(country, '') = ''
      `
    )
    .all();

  if (!rows.length) {
    return;
  }

  const update = db.prepare(
    `
      UPDATE leads
      SET city = CASE WHEN COALESCE(city, '') = '' THEN @city ELSE city END,
          area = CASE WHEN COALESCE(area, '') = '' THEN @area ELSE area END,
          state_region = CASE WHEN COALESCE(state_region, '') = '' THEN @stateRegion ELSE state_region END,
          postcode = CASE WHEN COALESCE(postcode, '') = '' THEN @postcode ELSE postcode END,
          country = CASE WHEN COALESCE(country, '') = '' THEN @country ELSE country END
      WHERE id = @id
    `
  );

  db.transaction(() => {
    for (const row of rows) {
      const raw = safeJsonParse(row.raw_json);
      const completeAddress = safeJsonParse(row.complete_address_json);
      const location = extractLocationParts({
        ...raw,
        complete_address: completeAddress,
      });
      update.run({
        id: row.id,
        city: location.city,
        area: location.area,
        stateRegion: location.stateRegion,
        postcode: location.postcode,
        country: location.country,
      });
    }
  })();
}

function safeJsonParse(value) {
  try {
    return value ? JSON.parse(value) : null;
  } catch {
    return null;
  }
}

function deserializeJobRow(row) {
  return {
    id: row.id,
    country: row.country,
    keyword: row.keyword,
    searchParams: JSON.parse(row.search_params_json),
    status: row.status,
    message: row.message,
    countryName: row.country_name,
    countryCode: row.country_code,
    countryBBox: row.country_bbox_json ? JSON.parse(row.country_bbox_json) : null,
    countryGeometry: row.country_geometry_json
      ? JSON.parse(row.country_geometry_json)
      : null,
    totalShards: row.total_shards,
    completedShards: row.completed_shards,
    failedShards: row.failed_shards,
    leadCount: row.lead_count,
    artifactCsvPath: row.artifact_csv_path,
    artifactJsonPath: row.artifact_json_path,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    startedAt: row.started_at,
    finishedAt: row.finished_at,
  };
}

function deserializeShardRow(row) {
  return {
    id: row.id,
    jobId: row.job_id,
    bbox: JSON.parse(row.bbox_json),
    depth: row.depth,
    status: row.status,
    resultCount: row.result_count,
    attemptCount: row.attempt_count,
    runToken: row.run_token || null,
    nextRunAt: row.next_run_at,
    lastError: row.last_error,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function deserializeLeadRow(row) {
  const categories = JSON.parse(row.categories_json);
  const allSubcategories = extractYelpSubcategories(row.category, categories);
  return {
    id: row.id,
    jobId: row.job_id,
    dedupeKey: row.dedupe_key,
    placeId: row.place_id,
    cid: row.cid,
    link: row.link,
    name: row.name,
    category: row.category,
    subcategory: allSubcategories[0] || "",
    allSubcategories,
    categories,
    website: row.website,
    phone: row.phone,
    address: row.address,
    completeAddress: JSON.parse(row.complete_address_json),
    city: row.city,
    area: row.area,
    stateRegion: row.state_region,
    postcode: row.postcode,
    country: row.country,
    lat: row.lat,
    lon: row.lon,
    reviewCount: row.review_count,
    reviewRating: row.review_rating,
    status: row.business_status,
    priceRange: row.price_range,
    sourceBBox: JSON.parse(row.source_bbox_json),
    raw: JSON.parse(row.raw_json),
    source: "yelp",
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function extractYelpSubcategories(primaryCategory, categories) {
  const primary = String(primaryCategory || "").trim().toLowerCase();
  return [
    ...new Set(
      (Array.isArray(categories) ? categories : [])
        .map((value) => String(value || "").trim())
        .filter(Boolean)
        .filter((value) => value.toLowerCase() !== primary)
    ),
  ];
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

function deserializeSyncStateRow(row) {
  return {
    jobId: row.job_id,
    lastSyncedLeadId: row.last_synced_lead_id || 0,
    lastSyncedAt: row.last_synced_at,
    lastStatus: row.last_status,
    lastMessage: row.last_message,
    lastStartedAt: row.last_started_at,
    lastFinishedAt: row.last_finished_at,
    syncedRecordCount: row.synced_record_count || 0,
  };
}

function sanitizeNocoDbConfig(input) {
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

function cleanString(value) {
  const normalized = String(value || "").trim();
  return normalized || null;
}

function parseJsonOrFallback(value, fallback) {
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

module.exports = {
  createStore,
};
