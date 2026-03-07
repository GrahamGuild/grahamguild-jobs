// src/ingest-yc-jobs.js
// Fetches Y Combinator Jobs "active-jb-7d" results from RapidAPI using a query JSON file.
// Saves a de-duped raw snapshot to data/raw/.
// Uses Supabase table job_seen to skip jobs already pulled before.
//
// Usage:
//   node src/ingest-yc-jobs.js

require("dotenv").config();

const fs = require("fs");
const path = require("path");
const axios = require("axios");
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

function toNumber(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function extractSourceId(item) {
  return item?.id ?? null;
}

function dedupWithinRunKey(item) {
  const id = extractSourceId(item);
  if (id != null) return `id:${String(id)}`;
  if (item.url) return `url:${String(item.url)}`;
  return JSON.stringify({
    title: item.title || "",
    organization: item.organization || "",
    date_posted: item.date_posted || "",
  });
}

async function fetchPage({ baseUrl, endpointPath, host, apiKey, params }) {
  const url = `${baseUrl.replace(/\/$/, "")}/${endpointPath.replace(/^\//, "")}`;

  const response = await axios.get(url, {
    params,
    headers: {
      "x-rapidapi-key": apiKey,
      "x-rapidapi-host": host,
    },
    validateStatus: () => true,
    timeout: 60000,
  });

  if (response.status === 429) {
    console.warn("Rate limited (429). Backing off 10s and retrying once...");
    await sleep(10_000);
    const retry = await axios.get(url, {
      params,
      headers: {
        "x-rapidapi-key": apiKey,
        "x-rapidapi-host": host,
      },
      timeout: 60000,
    });
    return retry.data;
  }

  if (response.status >= 400) {
    const err = new Error(`YC Jobs API failed (${response.status})`);
    err.detail = response.data;
    throw err;
  }

  return response.data;
}

async function loadSeenYcIds(supabase) {
  const seen = new Set();
  const pageSize = 1000;
  let from = 0;

  while (true) {
    const to = from + pageSize - 1;
    const { data, error } = await supabase
      .from("job_seen")
      .select("source_id")
      .eq("source", "yc_jobs")
      .range(from, to);

    if (error)
      throw new Error(`Failed loading job_seen (yc_jobs): ${error.message}`);

    for (const row of data ?? []) {
      if (row?.source_id) seen.add(String(row.source_id));
    }

    if (!data || data.length < pageSize) break;
    from += pageSize;
  }

  return seen;
}

async function upsertSeenYcIds(supabase, ids) {
  if (!ids.length) return;

  const now = new Date().toISOString();
  const rows = ids.map((id) => ({
    job_key: `yc_jobs:${id}`,
    source: "yc_jobs",
    source_id: String(id),
    last_seen_at: now,
  }));

  const chunkSize = 500;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const slice = rows.slice(i, i + chunkSize);
    const { error } = await supabase.from("job_seen").upsert(slice, {
      onConflict: "job_key",
      ignoreDuplicates: false,
    });
    if (error)
      throw new Error(`Supabase upsert job_seen failed: ${error.message}`);
  }
}

async function run() {
  const apiKey = assertEnv("RAPIDAPI_KEY");
  const host = assertEnv("YC_JOBS_HOST");
  const baseUrl = assertEnv("YC_JOBS_BASE_URL");
  const endpointPath = process.env.YC_JOBS_ENDPOINT_PATH || "active-jb-7d";

  const supabaseUrl =
    process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL;
  const serviceKey =
    process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SECRET_KEY;

  if (!supabaseUrl || !serviceKey) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }

  const supabase = createClient(supabaseUrl, serviceKey, {
    auth: { persistSession: false },
  });

  const queryFile =
    process.env.YC_JOBS_QUERY_FILE || "queries/yc_jobs_greg_v1.json";
  const query = loadQueryFile(queryFile);

  const paramsFromFile = query?.params || {};
  const pagingFromFile = query?.paging || {};

  const limit = toNumber(
    process.env.YC_JOBS_LIMIT || pagingFromFile.per_request_limit,
    100,
  );
  const maxRequests = toNumber(
    process.env.YC_JOBS_MAX_REQUESTS || pagingFromFile.max_requests_per_run,
    10,
  );
  const stopWhenShort =
    pagingFromFile.stop_when_results_less_than_limit !== false;

  const effectiveLimit = Math.min(100, Math.max(10, limit));

  console.log("Loading seen YC source_ids from Supabase...");
  const seenIds = await loadSeenYcIds(supabase);
  console.log(`Seen YC source_ids: ${seenIds.size}`);

  let offset = 0;
  let fetchedCount = 0;

  const dedupWithinRun = new Set();
  const newJobs = [];
  const newlySeenIds = new Set();

  let skippedSeen = 0;
  let dupesThisRun = 0;

  const rawDir = path.join(process.cwd(), "data", "raw");
  ensureDir(rawDir);

  for (let i = 0; i < maxRequests; i++) {
    console.log(`Fetching offset ${offset} (limit ${effectiveLimit})...`);

    const params = {
      ...paramsFromFile,
      limit: effectiveLimit,
      offset,
    };

    const results = await fetchPage({
      baseUrl,
      endpointPath,
      host,
      apiKey,
      params,
    });

    const items = Array.isArray(results)
      ? results
      : results?.items || results?.data || [];
    const pageCount = Array.isArray(items) ? items.length : 0;

    fetchedCount += pageCount;

    if (!items || pageCount === 0) {
      console.log("No more results returned. Stopping.");
      break;
    }

    let addedThisPage = 0;

    for (const item of items) {
      const dk = dedupWithinRunKey(item);
      if (dedupWithinRun.has(dk)) {
        dupesThisRun++;
        continue;
      }
      dedupWithinRun.add(dk);

      const id = extractSourceId(item);

      if (id != null) {
        const sid = String(id);
        if (seenIds.has(sid) || newlySeenIds.has(sid)) {
          skippedSeen++;
          continue;
        }
        newlySeenIds.add(sid);
      }

      newJobs.push(item);
      addedThisPage++;
    }

    console.log(
      `Page results: ${pageCount} | new kept: ${addedThisPage} | dupes(run): ${dupesThisRun} | skipped(seen): ${skippedSeen} | total new kept: ${newJobs.length}`,
    );

    if (stopWhenShort && pageCount < effectiveLimit) {
      console.log("Reached end of result set (results < limit). Stopping.");
      break;
    }

    offset += effectiveLimit;
    await sleep(350);
  }

  await upsertSeenYcIds(supabase, Array.from(newlySeenIds));

  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outputPath = path.join(rawDir, `yc_jobs_${timestamp}.json`);
  fs.writeFileSync(outputPath, JSON.stringify(newJobs, null, 2));

  console.log(
    `Done. Fetched total: ${fetchedCount} | New saved: ${newJobs.length} | Skipped already-seen: ${skippedSeen} | File: ${outputPath}`,
  );
  console.log(`Upserted newly seen YC ids into job_seen: ${newlySeenIds.size}`);
}

run().catch((e) => {
  console.error("Fatal error:", e?.message || e);
  if (e?.detail) console.error("Detail:", e.detail);
  process.exit(1);
});
