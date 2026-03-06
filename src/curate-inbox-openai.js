/**
 * curate-inbox-openai.js
 *
 * Reads web/public/inbox_latest.json, asks OpenAI to classify each job as:
 *   keep | ignore_nonfit | ignore_non_us_remote
 * For ignored items, writes an "ignored" row to Supabase (job_decisions),
 * so they never show in the Inbox again.
 *
 * ALSO writes local reports:
 *   data/inbox/curation_report_latest.json
 *   data/inbox/curation_report_<timestamp>.json
 *
 * Usage:
 *   node src/curate-inbox-openai.js
 */

require("dotenv").config();

const fs = require("fs");
const path = require("path");
const OpenAI = require("openai");

// ---------- paths ----------
const INBOX_PATH = path.join(
  process.cwd(),
  "web",
  "public",
  "inbox_latest.json",
);
const REPORT_DIR = path.join(process.cwd(), "data", "inbox");
const REPORT_LATEST_PATH = path.join(REPORT_DIR, "curation_report_latest.json");

// ---------- env ----------
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!OPENAI_API_KEY) throw new Error("Missing OPENAI_API_KEY");
if (!SUPABASE_URL) throw new Error("Missing SUPABASE_URL");
if (!SUPABASE_SERVICE_ROLE_KEY)
  throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY");

const MODEL = process.env.OPENAI_MODEL || "gpt-4.1";
const BATCH_SIZE = Number(process.env.CURATION_BATCH_SIZE || 25);
const MAX_RETRIES = Number(process.env.CURATION_MAX_RETRIES || 2);

const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

// ---------- utils ----------
function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

function readInbox() {
  const raw = fs.readFileSync(INBOX_PATH, "utf-8");
  return JSON.parse(raw);
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Strip common markdown fences and try to extract JSON object
 */
function coerceJsonObject(text) {
  if (!text) return null;
  let t = String(text).trim();

  // Strip ```json ... ``` or ``` ... ```
  if (t.startsWith("```")) {
    // remove starting fence line
    t = t.replace(/^```[a-zA-Z]*\s*/m, "");
    // remove ending fence
    t = t.replace(/```$/m, "").trim();
  }

  // If it still has leading/trailing junk, attempt to extract first {...} block
  const firstBrace = t.indexOf("{");
  const lastBrace = t.lastIndexOf("}");
  if (firstBrace !== -1 && lastBrace !== -1 && lastBrace > firstBrace) {
    t = t.slice(firstBrace, lastBrace + 1);
  }

  try {
    return JSON.parse(t);
  } catch {
    return null;
  }
}

async function supabaseUpsertIgnored(rows) {
  const res = await fetch(
    `${SUPABASE_URL}/rest/v1/job_decisions?on_conflict=job_key`,
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
    throw new Error(`Supabase upsert failed (${res.status}): ${text}`);
  }
}

/**
 * Ask OpenAI to classify a batch.
 * Returns array aligned to inputJobs: { job_key, verdict, reason }
 */
async function classifyBatch(jobs) {
  const inputJobs = jobs.map((j) => ({
    job_key: `${j.source}:${String(j.sourceId)}`,
    source: j.source ?? null,
    sourceId: j.sourceId != null ? String(j.sourceId) : null,
    title: j.title ?? null,
    organization: j.organization ?? null,
    location: j.location ?? null,
    remote: typeof j.remote === "boolean" ? j.remote : null,
    url: j.url ?? null,
  }));

  const instructions = `
You are helping me curate executive tech/product/transformation job listings for a US-based candidate.

Classify each job as exactly one of:
- "keep"
- "ignore_non_us_remote"
- "ignore_nonfit"

Candidate profile:
- senior executive technology/product/transformation leader
- interested in roles like CTO, CIO, CPO, Chief Transformation Officer
- also interested in EVP/SVP/VP Technology or Engineering leadership roles
- also interested in Operating Partner, Technology Advisor, Transformation leadership roles

IGNORE_NONFIT if the role is clearly not a fit, including:
- founder / co-founder roles
- security-only leadership roles like CISO / Chief Information Security Officer
- clearly non-executive roles
- product manager / program manager / project manager roles
- recruiting / sales / marketing / consultant roles
- clearly unrelated domain-specific roles

Geography rules:
- If location explicitly indicates a non-US country/city and remote is false, classify as "ignore_non_us_remote".
- If location explicitly indicates a non-US country/city and the role appears onsite or hybrid, classify as "ignore_non_us_remote".
- If location explicitly indicates a non-US country/city and remote is null, lean toward "ignore_non_us_remote" unless the title/location clearly suggests US-eligible remote work.
- If location says "Remote" with no country, you may keep it.
- If location explicitly says US / United States / USA, you may keep it unless clearly nonfit.
- If location is ambiguous, you may keep it.

Be stricter than before about excluding international onsite or hybrid roles.
When in doubt between "keep" and "ignore_non_us_remote" for a clearly international role that is not obviously US-remote, choose "ignore_non_us_remote".

Return ONLY valid JSON (no markdown, no code fences) with this exact shape:
{
  "results": [
    { "job_key": "source:sourceId", "verdict": "keep" | "ignore_non_us_remote" | "ignore_nonfit", "reason": "short" }
  ]
}
`.trim();

  // Retry loop in case the model wraps output or returns partial JSON
  let lastText = "";
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const resp = await openai.responses.create({
      model: MODEL,
      input: [
        { role: "system", content: instructions },
        { role: "user", content: JSON.stringify({ jobs: inputJobs }) },
      ],
    });

    const text = resp.output_text?.trim() || "";
    lastText = text;

    const parsed = coerceJsonObject(text);
    if (parsed && Array.isArray(parsed.results)) {
      const map = new Map(parsed.results.map((r) => [r.job_key, r]));
      return inputJobs.map((j) => {
        const r = map.get(j.job_key);
        if (!r) {
          return {
            job_key: j.job_key,
            verdict: "keep",
            reason: "missing_result_default_keep",
          };
        }

        const verdict = String(r.verdict || "").trim();
        const reason = String(r.reason || "")
          .trim()
          .slice(0, 220);

        if (
          verdict === "keep" ||
          verdict === "ignore_nonfit" ||
          verdict === "ignore_non_us_remote"
        ) {
          return {
            job_key: j.job_key,
            verdict,
            reason: reason || "no_reason",
          };
        }

        return {
          job_key: j.job_key,
          verdict: "keep",
          reason: "invalid_verdict_default_keep",
        };
      });
    }

    if (attempt < MAX_RETRIES) {
      // brief backoff then try again
      await sleep(300 * (attempt + 1));
      continue;
    }
  }

  // If we got here, parsing failed every time
  throw new Error(
    `Failed to parse OpenAI JSON after retries. First 800 chars:\n${lastText.slice(0, 800)}`,
  );
}

function nowStamp() {
  // filesystem friendly timestamp
  return new Date().toISOString().replace(/[:.]/g, "-");
}

// ---------- main ----------
async function main() {
  ensureDir(REPORT_DIR);

  const inbox = readInbox();
  const jobs = Array.isArray(inbox.jobs) ? inbox.jobs : [];

  console.log(`Loaded ${jobs.length} jobs from ${INBOX_PATH}`);

  const batches = chunk(jobs, BATCH_SIZE);

  const report = {
    builtAt: new Date().toISOString(),
    model: MODEL,
    batchSize: BATCH_SIZE,
    totals: {
      jobsLoaded: jobs.length,
      keep: 0,
      ignore_nonfit: 0,
      ignore_non_us_remote: 0,
      ignoredWrittenToSupabase: 0,
    },
    // keep this report useful but not massive
    samples: {
      ignored: [], // up to 50 samples
    },
    // optional: store per-batch stats
    batches: [],
  };

  let ignoredCount = 0;

  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];

    console.log(
      `Classifying batch ${i + 1}/${batches.length} (${batch.length} jobs)...`,
    );

    const verdicts = await classifyBatch(batch);

    const ignoredRows = [];
    const batchStats = {
      batch: i + 1,
      keep: 0,
      ignore_nonfit: 0,
      ignore_non_us_remote: 0,
      ignoredWritten: 0,
    };

    for (const v of verdicts) {
      if (v.verdict === "keep") {
        report.totals.keep += 1;
        batchStats.keep += 1;
        continue;
      }
      if (v.verdict === "ignore_nonfit") {
        report.totals.ignore_nonfit += 1;
        batchStats.ignore_nonfit += 1;
      }
      if (v.verdict === "ignore_non_us_remote") {
        report.totals.ignore_non_us_remote += 1;
        batchStats.ignore_non_us_remote += 1;
      }

      const original = batch.find(
        (j) => `${j.source}:${String(j.sourceId)}` === v.job_key,
      );
      if (!original) continue;

      const row = {
        job_key: v.job_key,
        source: original.source ?? null,
        source_id: original.sourceId != null ? String(original.sourceId) : null,
        decision: "ignored",
        note: `ai:${v.verdict}:${String(v.reason || "").slice(0, 180)}`,
        title: original.title ?? null,
        organization: original.organization ?? null,
        location: original.location ?? null,
        url: original.url ?? null,
        updated_at: new Date().toISOString(),
      };

      ignoredRows.push(row);

      if (report.samples.ignored.length < 50) {
        report.samples.ignored.push({
          job_key: v.job_key,
          verdict: v.verdict,
          reason: v.reason,
          title: original.title ?? null,
          organization: original.organization ?? null,
          location: original.location ?? null,
          remote: typeof original.remote === "boolean" ? original.remote : null,
          url: original.url ?? null,
        });
      }
    }

    if (ignoredRows.length) {
      await supabaseUpsertIgnored(ignoredRows);
      ignoredCount += ignoredRows.length;

      report.totals.ignoredWrittenToSupabase += ignoredRows.length;
      batchStats.ignoredWritten = ignoredRows.length;

      console.log(
        `→ wrote ${ignoredRows.length} ignored decisions to Supabase`,
      );
    } else {
      console.log("→ nothing ignored in this batch");
    }

    report.batches.push(batchStats);

    // write a rolling "latest" report as we go (handy if it crashes mid-way)
    fs.writeFileSync(
      REPORT_LATEST_PATH,
      JSON.stringify(report, null, 2),
      "utf-8",
    );
  }

  const stampedPath = path.join(
    REPORT_DIR,
    `curation_report_${nowStamp()}.json`,
  );
  fs.writeFileSync(stampedPath, JSON.stringify(report, null, 2), "utf-8");

  console.log(`Done. Total ignored written: ${ignoredCount}`);
  console.log(`Report: ${REPORT_LATEST_PATH}`);
  console.log(`Report (timestamped): ${stampedPath}`);
  console.log(
    "Next: run your normal build-inbox pipeline; ignored items will no longer show in Inbox.",
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
