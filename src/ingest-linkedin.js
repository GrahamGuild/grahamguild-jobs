require("dotenv").config();
const axios = require("axios");
const fs = require("fs");
const path = require("path");

// ---- Config ----
// Your config appears to live at: src/config/linkedin.query.json
// (because your current code uses "./config/..." from within src/)
const queryConfig = require("./config/linkedin.query.json");

const RAPIDAPI_KEY = process.env.RAPIDAPI_KEY;
const RAPIDAPI_HOST = process.env.RAPIDAPI_HOST;

if (!RAPIDAPI_KEY || !RAPIDAPI_HOST) {
  console.error("Missing RAPIDAPI_KEY or RAPIDAPI_HOST in .env");
  process.exit(1);
}

// Endpoint for Fantastic Jobs LinkedIn API (per your earlier curl and testing)
const BASE_URL = `https://${RAPIDAPI_HOST}/active-jb-7d`;

// ---- Helpers ----
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function toNumber(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function jobKey(job) {
  // Prefer stable unique ids; fall back to url if needed
  return (
    (job.linkedin_id != null ? `li:${job.linkedin_id}` : null) ||
    (job.id != null ? `id:${job.id}` : null) ||
    (job.url ? `url:${job.url}` : null)
  );
}

async function fetchPage({ limit, offset }) {
  const headers = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": RAPIDAPI_HOST,
  };

  const params = {
    ...queryConfig.params,
    limit,
    offset,
  };

  try {
    const response = await axios.get(BASE_URL, { headers, params });
    return response.data;
  } catch (err) {
    const status = err?.response?.status;
    const data = err?.response?.data;

    // Basic backoff on rate limiting
    if (status === 429) {
      console.warn("Rate limited (429). Backing off 10s and retrying once...");
      await sleep(10_000);
      const retry = await axios.get(BASE_URL, { headers, params });
      return retry.data;
    }

    console.error("API Error:", data || err.message);
    throw err;
  }
}

async function run() {
  // Force numeric values so offset math never becomes NaN
  const limit = toNumber(queryConfig?.paging?.per_request_limit, 100);
  const maxRequests = toNumber(queryConfig?.paging?.max_requests_per_run, 10);
  const stopWhenShort = Boolean(
    queryConfig?.paging?.stop_when_results_less_than_limit,
  );

  if (limit < 10 || limit > 100) {
    console.warn(
      `per_request_limit=${limit} is outside allowed [10..100]. Forcing to 100.`,
    );
  }
  const effectiveLimit = Math.min(100, Math.max(10, limit));

  let offset = 0;
  let fetchedCount = 0;

  const seen = new Set();
  const uniqueJobs = [];

  // Ensure output folder exists
  const rawDir = path.join(__dirname, "../data/raw");
  ensureDir(rawDir);

  for (let i = 0; i < maxRequests; i++) {
    console.log(`Fetching offset ${offset} (limit ${effectiveLimit})...`);

    const results = await fetchPage({ limit: effectiveLimit, offset });
    const pageCount = Array.isArray(results) ? results.length : 0;

    fetchedCount += pageCount;

    if (!results || pageCount === 0) {
      console.log("No more results returned. Stopping.");
      break;
    }

    let addedThisPage = 0;
    let dupesThisPage = 0;

    for (const job of results) {
      const key = jobKey(job);

      // If a job has no usable key, keep it but tag it (rare)
      if (!key) {
        uniqueJobs.push(job);
        addedThisPage++;
        continue;
      }

      if (seen.has(key)) {
        dupesThisPage++;
        continue;
      }

      seen.add(key);
      uniqueJobs.push(job);
      addedThisPage++;
    }

    console.log(
      `Page results: ${pageCount} | added unique: ${addedThisPage} | dupes skipped: ${dupesThisPage} | total unique: ${uniqueJobs.length}`,
    );

    // Stop if we hit the end of the result set
    if (stopWhenShort && pageCount < effectiveLimit) {
      console.log("Reached end of result set (results < limit). Stopping.");
      break;
    }

    // Move the window forward
    offset += effectiveLimit;

    // Gentle pacing to reduce rate-limit pain (tweak or remove if you want)
    await sleep(350);
  }

  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outputPath = path.join(rawDir, `linkedin_${timestamp}.json`);

  fs.writeFileSync(outputPath, JSON.stringify(uniqueJobs, null, 2));

  console.log(
    `Done. Fetched total: ${fetchedCount} | Unique saved: ${uniqueJobs.length} | File: ${outputPath}`,
  );
}

run().catch((e) => {
  console.error("Fatal error:", e?.message || e);
  process.exit(1);
});
