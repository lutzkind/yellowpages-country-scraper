const crypto = require("crypto");

function createAuth({ store, config }) {
  return {
    isConfigured() {
      return Boolean(config.adminUsername && config.adminPassword);
    },
    currentSession(req) {
      const sessionId = getSessionIdFromRequest(req, config.sessionCookieName);
      if (!sessionId) return null;
      const session = store.getSession(sessionId);
      if (!session) return null;
      const expiresAt = createExpiryTimestamp(config.sessionTtlHours);
      store.touchSession(session.id, expiresAt);
      return { ...session, expiresAt };
    },
    requireAuth(req, res, next) {
      res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, private");
      if (!this.isConfigured()) return respondAuthNotConfigured(req, res);
      const session = this.currentSession(req);
      if (!session) return respondUnauthorized(req, res);
      req.authSession = session;
      setSessionCookie(res, config.sessionCookieName, session.id, session.expiresAt);
      return next();
    },
    handleLogin(req, res) {
      if (!this.isConfigured()) {
        return res.status(503).json({ error: "Dashboard login is not configured on the server." });
      }
      const username = String(req.body.username || "").trim();
      const password = String(req.body.password || "");
      if (
        !timingSafeTextEqual(username, config.adminUsername) ||
        !timingSafeTextEqual(password, config.adminPassword)
      ) {
        return res.status(401).json({ error: "Invalid username or password." });
      }
      store.cleanupExpiredSessions();
      const sessionId = crypto.randomUUID();
      const expiresAt = createExpiryTimestamp(config.sessionTtlHours);
      store.createSession({ id: sessionId, username: config.adminUsername, expiresAt });
      setSessionCookie(res, config.sessionCookieName, sessionId, expiresAt);
      return res.json({ ok: true, username: config.adminUsername, expiresAt });
    },
    handleLogout(req, res) {
      const sessionId = getSessionIdFromRequest(req, config.sessionCookieName);
      if (sessionId) store.deleteSession(sessionId);
      clearSessionCookie(res, config.sessionCookieName);
      res.json({ ok: true });
    },
  };
}

function getSessionIdFromRequest(req, cookieName) {
  const cookies = parseCookies(req.headers.cookie);
  return cookies[cookieName] || null;
}

function parseCookies(headerValue) {
  const cookies = {};
  const raw = String(headerValue || "");
  if (!raw) return cookies;
  for (const chunk of raw.split(";")) {
    const [name, ...rest] = chunk.trim().split("=");
    if (!name) continue;
    cookies[name] = decodeURIComponent(rest.join("="));
  }
  return cookies;
}

function respondUnauthorized(req, res) {
  if (wantsHtml(req)) {
    return res.redirect(`/login?next=${encodeURIComponent(req.originalUrl || "/dashboard")}`);
  }
  return res.status(401).json({ error: "Authentication required." });
}

function respondAuthNotConfigured(req, res) {
  if (wantsHtml(req)) return res.status(503).send("Dashboard login is not configured on the server.");
  return res.status(503).json({ error: "Dashboard login is not configured on the server." });
}

function wantsHtml(req) {
  return req.method === "GET" && String(req.headers.accept || "").includes("text/html");
}

function setSessionCookie(res, cookieName, value, expiresAt) {
  const parts = [`${cookieName}=${encodeURIComponent(value)}`, "Path=/", "HttpOnly", "SameSite=Lax", `Expires=${new Date(expiresAt).toUTCString()}`];
  if (process.env.NODE_ENV === "production") parts.push("Secure");
  res.setHeader("Set-Cookie", parts.join("; "));
}

function clearSessionCookie(res, cookieName) {
  const parts = [`${cookieName}=`, "Path=/", "HttpOnly", "SameSite=Lax", "Expires=Thu, 01 Jan 1970 00:00:00 GMT"];
  if (process.env.NODE_ENV === "production") parts.push("Secure");
  res.setHeader("Set-Cookie", parts.join("; "));
}

function createExpiryTimestamp(sessionTtlHours) {
  return new Date(Date.now() + sessionTtlHours * 60 * 60 * 1000).toISOString();
}

function timingSafeTextEqual(left, right) {
  const l = Buffer.from(String(left));
  const r = Buffer.from(String(right));
  if (l.length !== r.length) return false;
  return crypto.timingSafeEqual(l, r);
}

module.exports = { createAuth };
