const crypto = require("crypto");
const { bboxIntersectsGeometry, bboxRadiusMeters, splitBBox, canSplitBBox } = require("./geo");
const { resolveCountry, queryYellowPages } = require("./yellowpages");
const { writeArtifacts } = require("./exporters");

function createWorker({ store, config, nocoDb = null }) {
  let timer = null;
  let inFlight = 0;
  let pumping = false;
  let stopped = false;

  const api = {
    async start() {
      stopped = false;
      recoverStaleRunningShards();
      await bootstrapPendingJobs();
      await this.pump();
      timer = setInterval(() => {
        this.pump().catch((error) => console.error("Worker pump failed:", error));
      }, config.workerPollMs);
    },
    stop() {
      stopped = true;
      if (timer) clearInterval(timer);
    },
    async pump() {
      if (pumping || stopped) return;
      pumping = true;
      try {
        recoverStaleRunningShards();
        await bootstrapPendingJobs();

        while (!stopped && inFlight < config.workerConcurrency) {
          const shard = store.claimNextShard();
          if (!shard) break;

          const job = store.getJob(shard.jobId);
          if (!job || job.status !== "running") continue;

          const geometry = job.countryGeometry
            ? { type: "Feature", geometry: job.countryGeometry }
            : null;

          inFlight += 1;
          processShard(job, shard, geometry)
            .then(async () => {
              await maybeSyncRunningJobs();
              await maybeFinalizeJob(job.id);
            })
            .catch((error) => {
              console.error("Worker shard failed:", error);
            })
            .finally(() => {
              inFlight -= 1;
            });
        }

        if (inFlight === 0) {
          await maybeSyncRunningJobs();
        }
      } finally {
        pumping = false;
      }
    },
  };

  return api;

  async function bootstrapPendingJobs() {
    const jobs = store.listJobs().filter((j) => j.status === "pending");
    for (const job of jobs) {
      try {
        if (job.totalShards > 0 || job.startedAt) { store.resumeJob(job.id); continue; }
        const countryData = await resolveCountry(job.country, config);
        store.seedJob(job.id, countryData);
      } catch (error) {
        store.failJob(job.id, error.message);
      }
    }
  }

  async function processShard(job, shard, geometry) {
    if (geometry?.geometry && !bboxIntersectsGeometry(shard.bbox, geometry)) {
      store.skipShard(shard.id, "Shard does not intersect the country geometry.", shard.runToken);
      return;
    }

    const canSplit = shard.depth < config.maxShardDepth && canSplitBBox(shard.bbox, config);
    const shardRadiusMeters = bboxRadiusMeters(shard.bbox);

    try {
      const response = await queryYellowPages({ job, shard, geometry, config });

      if (store.getJob(job.id)?.status === "canceled") return;

      if (canSplit && shardRadiusMeters > config.ypTargetShardRadiusMeters) {
        if (response.leads.length > 0) {
          store.upsertLeads(job.id, response.leads);
        }
        store.splitShard(shard.id, splitBBox(shard.bbox), shard.runToken);
        return;
      }

      if (response.rawCount >= config.resultSplitThreshold && canSplit) {
        if (response.leads.length > 0) {
          store.upsertLeads(job.id, response.leads);
        }
        store.splitShard(shard.id, splitBBox(shard.bbox), shard.runToken);
        return;
      }

      // Dense leaf: re-query exhaustively with pagination.
      if (!canSplit && response.rawCount >= config.resultSplitThreshold) {
        const exhaustiveResponse = await queryYellowPages({ job, shard, geometry, config, exhaustive: true });
        if (store.getJob(job.id)?.status === "canceled") return;
        store.completeShard(shard.id, exhaustiveResponse.leads, shard.runToken);
        return;
      }

      store.completeShard(shard.id, response.leads, shard.runToken);
    } catch (error) {
      const isRateOrBlocked =
        error.name === "AbortError" ||
        error.statusCode === 429 ||
        error.statusCode === 403 ||
        error.statusCode === 503 ||
        error.statusCode === 504 ||
        error.statusCode === 530 || // Cloudflare origin DNS error
        /timeout|rate.limit|blocked/i.test(error.message);

      if (store.getJob(job.id)?.status === "canceled") return;

      if (isRateOrBlocked && canSplit && shard.attemptCount >= 2) {
        store.splitShard(shard.id, splitBBox(shard.bbox), shard.runToken);
        return;
      }
      // Leaf shards that can't split: cap 403/blocked retries at 2, then treat as empty.
      if (isRateOrBlocked && !canSplit && shard.attemptCount >= 2) {
        store.completeShard(shard.id, [], shard.runToken);
        return;
      }
      if (shard.attemptCount < config.retryLimit) {
        const delay = config.retryBaseDelayMs * 2 ** (shard.attemptCount - 1);
        store.retryShard(shard.id, error.message, delay, shard.runToken);
        return;
      }
      if (canSplit) {
        store.splitShard(shard.id, splitBBox(shard.bbox), shard.runToken);
        return;
      }
      store.failShard(shard.id, error.message, shard.runToken);
    }
  }

  async function maybeFinalizeJob(jobId) {
    const unfinished = store.refreshJobStats(jobId);
    if (unfinished > 0) return;
    const job = store.getJob(jobId);
    if (!job || ["completed", "partial", "failed", "canceled"].includes(job.status)) return;
    if (job.leadCount === 0 && job.failedShards === job.totalShards) {
      store.finalizeJob(jobId, "failed", "All shards failed.");
      return;
    }
    const artifacts = writeArtifacts(store, config, jobId);
    const status = job.failedShards > 0 ? "partial" : "completed";
    const message = status === "completed" ? "Completed successfully." : "Completed with failed shards.";
    store.finalizeJob(jobId, status, message, artifacts);
    if (nocoDb) await nocoDb.syncCompletedJobIfEnabled(jobId);
  }

  async function maybeSyncRunningJobs() {
    if (!nocoDb?.getRunningJobSyncIdsDue) return;
    for (const jobId of nocoDb.getRunningJobSyncIdsDue()) {
      try { await nocoDb.syncJob(jobId); }
      catch (error) { console.error(`Incremental NocoDB sync failed for job ${jobId}:`, error.message); }
    }
  }

  function recoverStaleRunningShards() {
    for (const jobId of store.reclaimStaleRunningShards(config.runningShardStaleMs)) {
      console.warn(`Recovered stale running shard(s) for job ${jobId}.`);
    }
  }
}

function createJobId() { return crypto.randomUUID(); }

module.exports = { createWorker, createJobId };
