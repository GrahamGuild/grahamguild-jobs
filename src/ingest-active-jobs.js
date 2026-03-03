// src/ingest-active-jobs.js
// Fetches Active Jobs DB "active-ats-7d" results from RapidAPI using a query JSON file.
// Saves a de-duped raw snapshot to data/raw/.

require("dotenv").config();

const fs = require("fs");
const path = require("path");
const axios = require("axios");

function assertEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

function safeIsoForFilename(d = new Date()) {
  // 2026-03-02T00-17-17-288Z style (colon replaced)
  return d.toISOString().replace(/:/g, "-").replace(/\./g, "-");
}

function loadQueryFile(filePath) {
  const abs = path.isAbsolute(filePath)
    ? filePath
    : path.join(process.cwd(), filePath);
  if (!fs.existsSync(abs)) throw new Error(`Query file not found: ${abs}`);
  const raw = fs.readFileSync(abs, "utf8");
  return JSON.parse(raw);
}

function buildDedupKey(item) {
  // Active Jobs DB usually has id; keep fallbacks just in case.
  const parts = [
    item?.id,
    item?.url,
    item?.external_apply_url,
    item?.job_url,
    item?.apply_url,
  ].filter(Boolean);

  // If we somehow have nothing, stringify a small stable subset
  if (parts.length === 0) {
    return JSON.stringify({
      title: item?.title,
      organization: item?.organization,
      date_posted: item?.date_posted,
    });
  }
  return parts.join("|");
}

async function fetchPage({ baseUrl, endpointPath, host, apiKey, params }) {
  const url = `${baseUrl.replace(/\/$/, "")}/${endpointPath.replace(/^\//, "")}`;

  const resp = await axios.get(url, {
    params,
    headers: {
      "x-rapidapi-key": apiKey,
      "x-rapidapi-host": host,
    },
    timeout: 60000,
    validateStatus: () => true,
  });

  if (resp.status >= 400) {
    const detail =
      resp.data && typeof resp.data === "object"
        ? resp.data
        : { message: String(resp.data) };
    const err = new Error(`Request failed with status ${resp.status}`);
    err.detail = detail;
    err.status = resp.status;
    throw err;
  }

  return resp.data;
}

(async function main() {
  try {
    const apiKey = assertEnv("RAPIDAPI_KEY");
    const host = assertEnv("ACTIVE_JOBS_HOST");
    const baseUrl = assertEnv("ACTIVE_JOBS_BASE_URL");
    const endpointPath =
      process.env.ACTIVE_JOBS_ENDPOINT_PATH || "active-ats-7d";

    const limit = Number(process.env.ACTIVE_JOBS_LIMIT || "100");
    const maxRequests = Number(process.env.ACTIVE_JOBS_MAX_REQUESTS || "10");

    if (!Number.isFinite(limit) || limit < 10 || limit > 100) {
      throw new Error(
        `ACTIVE_JOBS_LIMIT must be a number between 10 and 100. Got: ${process.env.ACTIVE_JOBS_LIMIT}`,
      );
    }
    if (!Number.isFinite(maxRequests) || maxRequests < 1) {
      throw new Error(
        `ACTIVE_JOBS_MAX_REQUESTS must be a positive number. Got: ${process.env.ACTIVE_JOBS_MAX_REQUESTS}`,
      );
    }

    const queryFile =
      process.env.ACTIVE_JOBS_QUERY_FILE || "queries/active_jobs_greg_v1.json";
    const query = loadQueryFile(queryFile);

    const paramsFromFile = query?.params || {};
    const pagingFromFile = query?.paging || {};

    // Priority: env overrides file paging if present, but both are supported.
    const effectiveLimit = limit || Number(pagingFromFile.limit) || 100;
    const effectiveMaxRequests =
      maxRequests || Number(pagingFromFile.max_requests_per_run) || 10;
    const stopWhenLess =
      pagingFromFile.stop_when_results_less_than_limit !== false;

    const allUnique = [];
    const seen = new Set();

    let offset = 0;
    let totalFetched = 0;

    for (let req = 0; req < effectiveMaxRequests; req++) {
      console.log(`Fetching offset ${offset} (limit ${effectiveLimit})...`);

      const pageParams = {
        ...paramsFromFile,
        limit: effectiveLimit,
        offset,
      };

      const data = await fetchPage({
        baseUrl,
        endpointPath,
        host,
        apiKey,
        params: pageParams,
      });

      // Provider returns an array for this endpoint (like LinkedIn one).
      const items = Array.isArray(data)
        ? data
        : data?.items || data?.data || [];
      const pageCount = Array.isArray(items) ? items.length : 0;

      totalFetched += pageCount;

      let added = 0;
      let dupes = 0;

      for (const item of items) {
        const key = buildDedupKey(item);
        if (seen.has(key)) {
          dupes++;
          continue;
        }
        seen.add(key);
        allUnique.push(item);
        added++;
      }

      console.log(
        `Page results: ${pageCount} | added unique: ${added} | dupes skipped: ${dupes} | total unique: ${allUnique.length}`,
      );

      // Stop condition
      if (stopWhenLess && pageCount < effectiveLimit) {
        console.log("Reached end of result set (results < limit). Stopping.");
        break;
      }

      offset += effectiveLimit;
    }

    const outDir = path.join(process.cwd(), "data", "raw");
    fs.mkdirSync(outDir, { recursive: true });

    const filename = `active_jobs_${safeIsoForFilename(new Date())}.json`;
    const outPath = path.join(outDir, filename);

    fs.writeFileSync(outPath, JSON.stringify(allUnique, null, 2), "utf8");

    console.log(
      `Done. Fetched total: ${totalFetched} | Unique saved: ${allUnique.length} | File: ${outPath}`,
    );
  } catch (err) {
    if (err && err.detail) {
      console.error("API Error:", err.detail);
    }
    console.error("Fatal error:", err.message || err);
    process.exit(1);
  }
})();
