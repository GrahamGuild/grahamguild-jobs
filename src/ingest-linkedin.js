require("dotenv").config();
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { createClient } = require("@supabase/supabase-js");

// ---- Config ----
const queryConfig = require("../queries/linkedin_greg_v1.json");

const RAPIDAPI_KEY = process.env.RAPIDAPI_KEY;
const RAPIDAPI_HOST = process.env.RAPIDAPI_HOST;

const SUPABASE_URL =
  process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SECRET_KEY;

if (!RAPIDAPI_KEY || !RAPIDAPI_HOST) {
  console.error("Missing RAPIDAPI_KEY or RAPIDAPI_HOST in .env");
  process.exit(1);
}
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// Endpoint for Fantastic Jobs LinkedIn API
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

function extractSourceId(job) {
  // Match what build-inbox normalizes as sourceId
  return job.linkedin_id ?? job.linkedinId ?? job.id ?? null;
}

function seenKeyFromSourceId(sourceId) {
  return `linkedin:${String(sourceId)}`;
}

async function fetchAllSeenSourceIdsForLinkedIn() {
  // Pull ALL rows for source='linkedin' in pages (avoid 1000-row caps)
  const pageSize = 1000;
  let from = 0;

  const seen = new Set();

  while (true) {
    const to = from + pageSize - 1;

    const { data, error } = await supabase
      .from("job_seen")
      .select("source_id")
      .eq("source", "linkedin")
      .range(from, to);

    if (error) throw new Error(`Failed to read job_seen: ${error.message}`);

    for (const row of data ?? []) {
      if (row?.source_id) seen.add(String(row.source_id));
    }

    if (!data || data.length < pageSize) break;
    from += pageSize;
  }

  return seen;
}

async function upsertSeenRows(rows) {
  if (!rows.length) return;

  // Only columns that actually exist in your job_seen table:
  // (job_key, source, source_id, first_seen_at, last_seen_at)
  const res = await fetch(
    `${SUPABASE_URL}/rest/v1/job_seen?on_conflict=job_key`,
    {
      method: "POST",
      headers: {
        apikey: SUPABASE_SERVICE_ROLE_KEY,
        Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        "Content-Type": "application/json",
        Prefer: "resolution=merge-duplicates,return=minimal",
      },
      body: JSON.stringify(rows),
    },
  );

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Supabase upsert job_seen failed (${res.status}): ${text}`);
  }
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
  const limit = toNumber(queryConfig?.paging?.per_request_limit, 100);
  const maxRequests = toNumber(queryConfig?.paging?.max_requests_per_run, 10);
  const stopWhenShort = Boolean(
    queryConfig?.paging?.stop_when_results_less_than_limit,
  );

  const effectiveLimit = Math.min(100, Math.max(10, limit));

  console.log("Loading seen LinkedIn source_ids from Supabase...");
  const seenSourceIds = await fetchAllSeenSourceIdsForLinkedIn();
  console.log(`Seen LinkedIn source_ids: ${seenSourceIds.size}`);

  let offset = 0;
  let fetchedCount = 0;

  const seenThisRun = new Set(); // de-dupe within this run
  const uniqueNewJobs = [];
  const newlySeenRows = [];

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
    let skippedSeenThisPage = 0;
    let dupesThisPage = 0;

    for (const job of results) {
      const sourceId = extractSourceId(job);

      // If we have a stable id, use job_seen to skip
      if (sourceId != null) {
        const sid = String(sourceId);

        if (seenSourceIds.has(sid)) {
          skippedSeenThisPage++;
          continue;
        }

        // Dedup inside the run
        if (seenThisRun.has(sid)) {
          dupesThisPage++;
          continue;
        }

        seenThisRun.add(sid);
        uniqueNewJobs.push(job);
        addedThisPage++;

        newlySeenRows.push({
          job_key: seenKeyFromSourceId(sid),
          source: "linkedin",
          source_id: sid,
          // first_seen_at default will be applied on insert;
          // last_seen_at we set each time we see it
          last_seen_at: new Date().toISOString(),
        });

        continue;
      }

      // If no usable id (rare), keep it but still de-dupe by url-ish fields
      // so we don’t explode the file.
      const fallbackKey = job?.url ? `url:${job.url}` : JSON.stringify(job);
      if (seenThisRun.has(fallbackKey)) {
        dupesThisPage++;
        continue;
      }
      seenThisRun.add(fallbackKey);
      uniqueNewJobs.push(job);
      addedThisPage++;
    }

    console.log(
      `Page results: ${pageCount} | added NEW: ${addedThisPage} | skipped already seen: ${skippedSeenThisPage} | dupes skipped: ${dupesThisPage} | total NEW: ${uniqueNewJobs.length}`,
    );

    if (stopWhenShort && pageCount < effectiveLimit) {
      console.log("Reached end of result set (results < limit). Stopping.");
      break;
    }

    offset += effectiveLimit;
    await sleep(350);
  }

  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outputPath = path.join(rawDir, `linkedin_${timestamp}.json`);
  fs.writeFileSync(outputPath, JSON.stringify(uniqueNewJobs, null, 2));

  console.log(
    `Wrote raw snapshot with NEW jobs only: ${uniqueNewJobs.length} jobs -> ${outputPath}`,
  );

  // Upsert newly seen rows (in batches)
  const batchSize = 500;
  for (let i = 0; i < newlySeenRows.length; i += batchSize) {
    const batch = newlySeenRows.slice(i, i + batchSize);
    await upsertSeenRows(batch);
  }

  console.log(
    `Done. Fetched total: ${fetchedCount} | NEW kept: ${uniqueNewJobs.length} | Newly marked seen: ${newlySeenRows.length}`,
  );
}

run().catch((e) => {
  console.error("Fatal error:", e?.message || e);
  process.exit(1);
});
