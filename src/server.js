const express = require("express");
const path = require("path");
const { resolveSearchParams } = require("./keywords");
const { createJobId } = require("./worker");
const { createAuth } = require("./auth");

function createApp({ store, config, nocoDb }) {
  const app = express();
  const auth = createAuth({ store, config });

  app.use(express.json({ limit: "1mb" }));
  app.use("/assets", express.static(path.join(__dirname, "..", "public")));

  app.get("/health", (_req, res) => res.json({ ok: true }));

  app.get("/", (req, res) => {
    if (!auth.isConfigured()) return res.redirect("/login");
    return res.redirect(auth.currentSession(req) ? "/dashboard" : "/login");
  });

  app.get("/login", (req, res) => {
    if (auth.isConfigured() && auth.currentSession(req)) return res.redirect("/dashboard");
    res.sendFile(path.join(__dirname, "..", "public", "login.html"));
  });

  app.post("/api/auth/login", (req, res) => auth.handleLogin(req, res));
  app.post("/api/auth/logout", withAuth(auth), (req, res) => auth.handleLogout(req, res));
  app.get("/api/auth/session", withAuth(auth), (req, res) => {
    res.json({ authenticated: true, username: req.authSession.username, expiresAt: req.authSession.expiresAt });
  });

  app.get("/dashboard", withAuth(auth), (_req, res) => {
    res.sendFile(path.join(__dirname, "..", "public", "dashboard.html"));
  });

  app.get("/integrations/nocodb", withAuth(auth), (_req, res) => res.json(nocoDb.getConfig()));
  app.put("/integrations/nocodb", withAuth(auth), (req, res, next) => {
    try { res.json(nocoDb.saveConfig(req.body || {})); } catch (e) { next(e); }
  });
  app.post("/integrations/nocodb/test", withAuth(auth), async (req, res, next) => {
    try { res.json(await nocoDb.testConnection(req.body || null)); } catch (e) { next(e); }
  });

  app.use("/jobs", withAuth(auth));

  app.get("/jobs", (_req, res) => res.json({ jobs: store.listJobs() }));

  app.post("/jobs", async (req, res, next) => {
    try {
      const country = String(req.body.country || "").trim();
      const keyword = String(req.body.keyword || "").trim();
      if (!country || !keyword) {
        return res.status(400).json({ error: "country and keyword are required." });
      }
      const id = createJobId();
      store.createJob({ id, country, keyword, searchParams: resolveSearchParams(keyword) });
      return res.status(202).json({ job: store.getJob(id), links: buildLinks(req, config, id) });
    } catch (error) { return next(error); }
  });

  app.get("/jobs/:jobId", (req, res) => {
    const job = store.getJob(req.params.jobId);
    if (!job) return res.status(404).json({ error: "Job not found." });
    return res.json({ job, stats: store.getJobStats(job.id), links: buildLinks(req, config, job.id) });
  });

  app.get("/jobs/:jobId/stats", (req, res) => {
    const job = store.getJob(req.params.jobId);
    if (!job) return res.status(404).json({ error: "Job not found." });
    return res.json({ job, stats: store.getJobStats(job.id), links: buildLinks(req, config, job.id) });
  });

  app.get("/jobs/:jobId/shards", (req, res) => {
    const job = store.getJob(req.params.jobId);
    if (!job) return res.status(404).json({ error: "Job not found." });
    const limit = Math.min(Math.max(Number.parseInt(req.query.limit, 10) || 100, 1), 1000);
    const offset = Math.max(Number.parseInt(req.query.offset, 10) || 0, 0);
    const status = req.query.status ? String(req.query.status).trim() : null;
    return res.json({ jobId: job.id, status, limit, offset, total: store.countJobShards(job.id, status), shards: store.listJobShards(job.id, { status, limit, offset }) });
  });

  app.get("/jobs/:jobId/errors", (req, res) => {
    const job = store.getJob(req.params.jobId);
    if (!job) return res.status(404).json({ error: "Job not found." });
    const limit = Math.min(Math.max(Number.parseInt(req.query.limit, 10) || 25, 1), 250);
    return res.json({ jobId: job.id, limit, errors: store.getJobErrors(job.id, { limit }) });
  });

  app.get("/jobs/:jobId/leads", (req, res) => {
    const job = store.getJob(req.params.jobId);
    if (!job) return res.status(404).json({ error: "Job not found." });
    const limit = Math.min(Number.parseInt(req.query.limit, 10) || 100, 1000);
    const offset = Math.max(Number.parseInt(req.query.offset, 10) || 0, 0);
    return res.json({ jobId: job.id, limit, offset, leads: store.getJobLeads(job.id, { limit, offset }) });
  });

  app.post("/jobs/:jobId/cancel", (req, res) => {
    const job = store.getJob(req.params.jobId);
    if (!job) return res.status(404).json({ error: "Job not found." });
    if (["completed", "partial", "failed", "canceled"].includes(job.status))
      return res.status(409).json({ error: `Job is already ${job.status}.` });
    return res.json({ job: store.cancelJob(job.id), stats: store.getJobStats(job.id), links: buildLinks(req, config, job.id) });
  });

  app.post("/jobs/:jobId/pause", (req, res) => {
    const job = store.getJob(req.params.jobId);
    if (!job) return res.status(404).json({ error: "Job not found." });
    if (job.status === "paused") return res.status(409).json({ error: "Job is already paused." });
    if (["completed", "partial", "failed", "canceled"].includes(job.status))
      return res.status(409).json({ error: `Job is already ${job.status}.` });
    return res.json({ job: store.pauseJob(job.id), stats: store.getJobStats(job.id), links: buildLinks(req, config, job.id) });
  });

  app.post("/jobs/:jobId/resume", (req, res) => {
    const job = store.getJob(req.params.jobId);
    if (!job) return res.status(404).json({ error: "Job not found." });
    if (job.status !== "paused") return res.status(409).json({ error: "Only paused jobs can be resumed." });
    return res.json({ job: store.resumeJob(job.id), stats: store.getJobStats(job.id), links: buildLinks(req, config, job.id) });
  });

  app.delete("/jobs/:jobId", (req, res, next) => {
    try {
      const job = store.getJob(req.params.jobId);
      if (!job) return res.status(404).json({ error: "Job not found." });
      return res.json({ ok: true, deletedJob: store.deleteJob(job.id) });
    } catch (e) { return next(e); }
  });

  app.get("/jobs/:jobId/sync/nocodb", (req, res) => {
    const job = store.getJob(req.params.jobId);
    if (!job) return res.status(404).json({ error: "Job not found." });
    return res.json({ jobId: job.id, ...nocoDb.getJobSyncStatus(job.id) });
  });

  app.post("/jobs/:jobId/sync/nocodb", async (req, res, next) => {
    const job = store.getJob(req.params.jobId);
    if (!job) return res.status(404).json({ error: "Job not found." });
    try { return res.json(await nocoDb.syncJob(job.id, { force: Boolean(req.body?.force) })); }
    catch (e) { return next(e); }
  });

  app.get("/jobs/:jobId/download", (req, res) => {
    const job = store.getJob(req.params.jobId);
    if (!job) return res.status(404).json({ error: "Job not found." });
    const format = (req.query.format || "csv").toString().toLowerCase();
    const filePath = format === "json" ? job.artifactJsonPath : job.artifactCsvPath;
    if (!filePath) return res.status(409).json({ error: "Artifacts are not ready yet.", jobStatus: job.status });
    return res.download(filePath);
  });

  app.use((error, _req, res, _next) => {
    res.status(error.statusCode || 500).json({ error: error.message || "Unexpected error." });
  });

  return app;
}

function withAuth(auth) {
  return (req, res, next) => auth.requireAuth(req, res, next);
}

function buildLinks(req, config, jobId) {
  const base = config.publicBaseUrl || `${req.protocol}://${req.get("host")}`;
  return {
    self: `${base}/jobs/${jobId}`,
    dashboard: `${base}/dashboard?jobId=${jobId}`,
    stats: `${base}/jobs/${jobId}/stats`,
    shards: `${base}/jobs/${jobId}/shards`,
    errors: `${base}/jobs/${jobId}/errors`,
    leads: `${base}/jobs/${jobId}/leads`,
    csv: `${base}/jobs/${jobId}/download?format=csv`,
    json: `${base}/jobs/${jobId}/download?format=json`,
    cancel: `${base}/jobs/${jobId}/cancel`,
    pause: `${base}/jobs/${jobId}/pause`,
    resume: `${base}/jobs/${jobId}/resume`,
    delete: `${base}/jobs/${jobId}`,
    nocodbSync: `${base}/jobs/${jobId}/sync/nocodb`,
  };
}

module.exports = { createApp };
