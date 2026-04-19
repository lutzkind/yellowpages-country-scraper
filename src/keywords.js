function normalizeKeyword(value) {
  const keyword = String(value || "").trim();
  if (!keyword) {
    const error = new Error("keyword is required.");
    error.statusCode = 400;
    throw error;
  }
  return keyword;
}

function resolveSearchParams(keyword) {
  return { query: normalizeKeyword(keyword) };
}

module.exports = { normalizeKeyword, resolveSearchParams };
