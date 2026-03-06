/**
 * backfill-seen-from-inbox.js
 *
 * Seeds/updates public.job_seen from web/public/inbox_latest.json
 *
 * - Inserts new rows with job_key/source/source_id (first_seen_at + last_seen_at default to now()).
 * - For existing rows, only updates last_seen_at (does NOT overwrite first_seen_at).
 *
 * Usage:
 *   node src/backfill-seen-from-inbox.js
 */

require("dotenv").config();

const fs = require("fs");
const path = require("path");

const INBOX_PATH = path.join(
  process.cwd(),
  "web",
  "public",
  "inbox_latest.json",
);

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL) throw new Error("Missing SUPABASE_URL");
if (!SUPABASE_SERVICE_ROLE_KEY)
  throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY");

function readInbox() {
  const raw = fs.readFileSync(INBOX_PATH, "utf-8");
  return JSON.parse(raw);
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function upsertSeen(rows) {
  // NOTE: We intentionally do NOT send first_seen_at so we don't overwrite it on updates.
  // We DO send last_seen_at so we can refresh "last seen".
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

async function main() {
  const inbox = readInbox();
  const jobs = Array.isArray(inbox.jobs) ? inbox.jobs : [];

  console.log(`Loaded ${jobs.length} jobs from ${INBOX_PATH}`);

  const nowIso = new Date().toISOString();

  // Build rows for job_seen schema
  const rows = jobs
    .map((j) => {
      const source = String(j.source ?? "").trim();
      const sourceId = String(j.sourceId ?? "").trim();
      if (!source || !sourceId) return null;

      return {
        job_key: `${source}:${sourceId}`,
        source,
        source_id: sourceId,
        last_seen_at: nowIso,
      };
    })
    .filter(Boolean);

  const batches = chunk(rows, 500);

  for (let i = 0; i < batches.length; i++) {
    const b = batches[i];
    console.log(`Upserting batch ${i + 1}/${batches.length} (${b.length})...`);
    await upsertSeen(b);
  }

  console.log(`Done. Upserted/updated ${rows.length} rows into job_seen.`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
