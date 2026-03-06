// src/ingest-active-jobs.js
// Fetches Active Jobs DB "active-ats-7d" results from RapidAPI using a query JSON file.
// Saves a de-duped raw snapshot to data/raw/, but ONLY includes jobs we have NOT seen before,
// based on Supabase table: public.job_seen (job_key PK).
//
// Usage:
//   node src/ingest-active-jobs.js

require("dotenv").config();

const fs = require("fs");
const path = require("path");
const axios = require("axios");
const crypto = require("crypto");
const { createClient } = require("@supabase/supabase-js");

function assertEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function safeIsoForFilename(d = new Date()) {
  return d.toISOString().replace(/:/g, "-").replace(/\./g, "-");
}

function loadQueryFile(filePath) {
  const abs = path.isAbsolute(filePath)
    ? filePath
    : path.join(process.cwd(), filePath);
  if (!fs.existsSync(abs)) throw new Error(`Query file not found: ${abs}`);
  return JSON.parse(fs.readFileSync(abs, "utf8"));
}

function cleanUrl(u) {
  if (!u || typeof u !== "string") return "";
  try {
    const x = new URL(u);
    x.search = "";
    x.hash = "";
    const scheme = (x.protocol || "https:").toLowerCase();
    const host = (x.host || "").toLowerCase();
    let pathname = (x.pathname || "").replace(/\/{2,}/g, "/");
    if (pathname.length > 1 && pathname.endsWith("/"))
      pathname = pathname.slice(0, -1);
    return `${scheme}//${host}${pathname}`;
  } catch {
    return u.trim().toLowerCase();
  }
}

function sha1(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex");
}

/**
 * Determine stable source_id for Active Jobs item.
 */
function getSourceId(item) {
  const id =
    item?.id ?? item?.job_id ?? item?.req_id ?? item?.requisition_id ?? null;
  return id != null && String(id).trim() !== "" ? String(id).trim() : "";
}

/**
 * Determine stable job_key for job_seen + downstream.
 * Prefer id. Fallback to url hash. Final fallback is a hash of a small stable subset.
 */
function getJobKey(item) {
  const sourceId = getSourceId(item);
  if (sourceId) return `active_jobs:${sourceId}`;

  const urlCandidate =
    item?.url ??
    item?.job_url ??
    item?.apply_url ??
    item?.external_apply_url ??
    "";

  const cu = cleanUrl(urlCandidate);
  if (cu) return `active_jobs:urlhash:${sha1(cu)}`;

  return `active_jobs:fallback:${sha1(
    JSON.stringify({
      title: item?.title || "",
      organization:
        item?.organization || item?.company || item?.company_name || "",
      date_posted: item?.date_posted || item?.datePosted || "",
    }),
  )}`;
}

/**
 * Within-run de-dupe (still useful).
 */
function buildDedupKey(item) {
  const parts = [
    item?.id,
    item?.url,
    item?.external_apply_url,
    item?.job_url,
    item?.apply_url,
  ].filter(Boolean);

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

/**
 * Fetch existing job_seen rows for a set of job_keys.
 * Returns a Set of keys that already exist.
 */
async function fetchSeenKeys(supabase, jobKeys) {
  if (!jobKeys.length) return new Set();

  // Supabase "in" has practical limits; chunk it.
  const CHUNK = 200;
  const seen = new Set();

  for (let i = 0; i < jobKeys.length; i += CHUNK) {
    const slice = jobKeys.slice(i, i + CHUNK);

    const { data, error } = await supabase
      .from("job_seen")
      .select("job_key")
      .in("job_key", slice);

    if (error)
      throw new Error(`Supabase select job_seen failed: ${error.message}`);

    for (const row of data || []) {
      if (row?.job_key) seen.add(String(row.job_key));
    }
  }

  return seen;
}

/**
 * Upsert job_seen (updates last_seen_at; first_seen_at stays on existing rows).
 */
async function upsertSeen(supabase, rows) {
  if (!rows.length) return;

  // Chunk to avoid payload limits
  const CHUNK = 500;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const slice = rows.slice(i, i + CHUNK);
    const { error } = await supabase
      .from("job_seen")
      .upsert(slice, { onConflict: "job_key" });

    if (error)
      throw new Error(`Supabase upsert job_seen failed: ${error.message}`);
  }
}

(async function main() {
  try {
    // RapidAPI env
    const apiKey = assertEnv("RAPIDAPI_KEY");
    const host = assertEnv("ACTIVE_JOBS_HOST");
    const baseUrl = assertEnv("ACTIVE_JOBS_BASE_URL");
    const endpointPath =
      process.env.ACTIVE_JOBS_ENDPOINT_PATH || "active-ats-7d";

    // Supabase env
    const supabaseUrl = assertEnv("SUPABASE_URL");
    const serviceKey = assertEnv("SUPABASE_SERVICE_ROLE_KEY");
    const supabase = createClient(supabaseUrl, serviceKey, {
      auth: { persistSession: false },
    });

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

    const effectiveLimit = limit || Number(pagingFromFile.limit) || 100;
    const effectiveMaxRequests =
      maxRequests || Number(pagingFromFile.max_requests_per_run) || 10;
    const stopWhenLess =
      pagingFromFile.stop_when_results_less_than_limit !== false;

    const outDir = path.join(process.cwd(), "data", "raw");
    ensureDir(outDir);

    let offset = 0;
    let totalFetched = 0;
    let skippedSeen = 0;
    let dupesThisRun = 0;
    let keptNew = 0;

    const allNew = [];
    const dedupWithinRun = new Set();

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

      const items = Array.isArray(data)
        ? data
        : data?.items || data?.data || [];
      const pageCount = Array.isArray(items) ? items.length : 0;

      totalFetched += pageCount;

      if (!items || pageCount === 0) {
        console.log("No more results returned. Stopping.");
        break;
      }

      // within-run dedupe first
      const pageUnique = [];
      for (const item of items) {
        const dKey = buildDedupKey(item);
        if (dedupWithinRun.has(dKey)) {
          dupesThisRun++;
          continue;
        }
        dedupWithinRun.add(dKey);
        pageUnique.push(item);
      }

      // derive keys, check seen in Supabase
      const pageKeys = pageUnique.map(getJobKey);
      const alreadySeen = await fetchSeenKeys(supabase, pageKeys);

      const nowIso = new Date().toISOString();

      // Upsert "seen" for ALL pageUnique so we don't re-process next run
      const seenRows = pageUnique.map((item) => {
        const sourceId = getSourceId(item) || null;
        return {
          job_key: getJobKey(item),
          source: "active_jobs",
          source_id: sourceId || "", // table has NOT NULL; keep empty string if fallback key
          last_seen_at: nowIso,
          // don't send first_seen_at; let default populate on insert
        };
      });
      await upsertSeen(supabase, seenRows);

      // keep only new
      let addedThisPage = 0;
      for (let i = 0; i < pageUnique.length; i++) {
        const item = pageUnique[i];
        const key = pageKeys[i];
        if (alreadySeen.has(key)) {
          skippedSeen++;
          continue;
        }
        allNew.push(item);
        addedThisPage++;
      }

      keptNew += addedThisPage;

      console.log(
        `Page results: ${pageCount} | pageUnique: ${pageUnique.length} | new kept: ${addedThisPage} | dupes(run): ${dupesThisRun} | skipped(seen): ${skippedSeen} | total new kept: ${keptNew}`,
      );

      if (stopWhenLess && pageCount < effectiveLimit) {
        console.log("Reached end of result set (results < limit). Stopping.");
        break;
      }

      offset += effectiveLimit;
    }

    const filename = `active_jobs_${safeIsoForFilename(new Date())}.json`;
    const outPath = path.join(outDir, filename);

    fs.writeFileSync(outPath, JSON.stringify(allNew, null, 2), "utf8");

    console.log(
      `Done. Fetched total: ${totalFetched} | New saved: ${allNew.length} | Skipped already-seen: ${skippedSeen} | Dupes within run: ${dupesThisRun} | File: ${outPath}`,
    );
  } catch (err) {
    if (err && err.detail) console.error("API Error:", err.detail);
    console.error("Fatal error:", err.message || err);
    process.exit(1);
  }
})();
