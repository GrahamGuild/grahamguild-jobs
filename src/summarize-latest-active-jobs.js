/**
 * Summarize the latest Active Jobs raw JSON file in data/raw.
 * - No API calls
 * - Helps validate duplicates, title/org distribution, dates, remote, source, etc.
 *
 * Expected raw filename pattern:
 *   data/raw/active_jobs_YYYY-MM-DDTHH-mm-ss-SSSZ.json
 */

const fs = require("fs");
const path = require("path");

const RAW_DIR = path.join(process.cwd(), "data", "raw");
const PREFIX = "active_jobs_";

function safeParseDate(value) {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
}

function pickLatestFile(dir, prefix) {
  const files = fs
    .readdirSync(dir)
    .filter((f) => f.startsWith(prefix) && f.endsWith(".json"))
    .sort(); // lexical works because your filenames are ISO-ish
  return files.length ? files[files.length - 1] : null;
}

function inc(map, key) {
  const k = (key ?? "").toString().trim() || "(blank)";
  map.set(k, (map.get(k) || 0) + 1);
}

function topN(map, n = 15) {
  return [...map.entries()].sort((a, b) => b[1] - a[1]).slice(0, n);
}

function normalizeString(s) {
  return (s ?? "").toString().trim().toLowerCase();
}

// Try common field names across feeds
function getTitle(job) {
  return job.title ?? job.job_title ?? job.position ?? job.role ?? null;
}

function getOrg(job) {
  return (
    job.organization ??
    job.company ??
    job.company_name ??
    job.employer_name ??
    job.hiring_organization ??
    null
  );
}

function getUrl(job) {
  return (
    job.url ?? job.job_url ?? job.apply_url ?? job.external_apply_url ?? null
  );
}

function getSource(job) {
  return (
    job.source ??
    job.source_domain ??
    job.source_type ??
    job.ats ??
    job.platform ??
    null
  );
}

function getDatePosted(job) {
  return (
    job.date_posted ??
    job.posted_at ??
    job.posted_date ??
    job.datePublished ??
    job.date_published ??
    job.created_at ??
    job.date_created ??
    null
  );
}

function getRemoteFlag(job) {
  // multiple possible representations
  if (typeof job.remote === "boolean") return job.remote;
  if (typeof job.remote_derived === "boolean") return job.remote_derived;
  if (typeof job.is_remote === "boolean") return job.is_remote;
  if (typeof job.remote_ok === "boolean") return job.remote_ok;

  // sometimes strings like "remote", "hybrid", "onsite"
  const wt = normalizeString(
    job.workplace_type ?? job.work_type ?? job.location_type,
  );
  if (wt.includes("remote")) return true;
  if (wt.includes("on-site") || wt.includes("onsite")) return false;

  return null; // unknown
}

function getEmploymentType(job) {
  // could be "employment_type": ["FULL_TIME"] or string
  const t = job.employment_type ?? job.employmentType ?? job.type ?? null;
  if (Array.isArray(t)) return t.join(",");
  return t;
}

function getLocation(job) {
  // sometimes already derived
  if (Array.isArray(job.locations_derived) && job.locations_derived.length)
    return job.locations_derived[0];
  if (Array.isArray(job.locations) && job.locations.length)
    return job.locations[0];
  if (job.location) return job.location;
  if (Array.isArray(job.locations_raw) && job.locations_raw.length) {
    // try to pull locality/region/country
    const addr = job.locations_raw?.[0]?.address;
    if (addr) {
      const parts = [
        addr.addressLocality,
        addr.addressRegion,
        addr.addressCountry,
      ].filter(Boolean);
      if (parts.length) return parts.join(", ");
    }
  }
  return null;
}

function makeDedupeKey(job) {
  // Prefer stable IDs if present, otherwise URL, otherwise (title+org+date)
  const id =
    job.id ??
    job.job_id ??
    job.ats_id ??
    job.ats_job_id ??
    job.external_id ??
    null;

  const url = getUrl(job);

  if (id) return `id:${id}`;
  if (url) return `url:${url}`;

  const title = normalizeString(getTitle(job));
  const org = normalizeString(getOrg(job));
  const posted = normalizeString(getDatePosted(job));
  return `fuzzy:${title}|${org}|${posted}`;
}

function main() {
  if (!fs.existsSync(RAW_DIR)) {
    console.error(`Missing folder: ${RAW_DIR}`);
    process.exit(1);
  }

  const latest = pickLatestFile(RAW_DIR, PREFIX);
  if (!latest) {
    console.error(`No files found in ${RAW_DIR} with prefix "${PREFIX}"`);
    process.exit(1);
  }

  const fullPath = path.join(RAW_DIR, latest);
  const raw = fs.readFileSync(fullPath, "utf8");
  const rows = JSON.parse(raw);

  if (!Array.isArray(rows)) {
    console.error("Expected the JSON file to contain an array of jobs.");
    process.exit(1);
  }

  const titles = new Map();
  const orgs = new Map();
  const sources = new Map();
  const remoteMap = new Map();
  const employment = new Map();

  const seen = new Set();
  let dupes = 0;

  let minDate = null;
  let maxDate = null;

  for (const job of rows) {
    const key = makeDedupeKey(job);
    if (seen.has(key)) dupes++;
    else seen.add(key);

    inc(titles, getTitle(job));
    inc(orgs, getOrg(job));
    inc(sources, getSource(job));
    inc(employment, getEmploymentType(job));

    const rf = getRemoteFlag(job);
    inc(remoteMap, rf === null ? "(unknown)" : rf ? "true" : "false");

    const d = safeParseDate(getDatePosted(job));
    if (d) {
      if (!minDate || d < minDate) minDate = d;
      if (!maxDate || d > maxDate) maxDate = d;
    }
  }

  const line = "=".repeat(50);
  console.log(line);
  console.log(`Latest file: ${latest}`);
  console.log(`Path: ${fullPath}`);
  console.log(`Rows: ${rows.length}`);
  console.log(`Unique (by dedupe key): ${seen.size}`);
  console.log(`Dupes detected: ${dupes}`);
  console.log(
    `Date range: ${minDate ? minDate.toISOString() : "(unknown)"}  →  ${maxDate ? maxDate.toISOString() : "(unknown)"}`,
  );
  console.log("-".repeat(50));

  console.log("\nTop Titles:");
  for (const [k, v] of topN(titles, 15)) console.log(String(v).padStart(5), k);

  console.log("\nTop Organizations:");
  for (const [k, v] of topN(orgs, 15)) console.log(String(v).padStart(5), k);

  console.log("\nBy Source / ATS:");
  for (const [k, v] of topN(sources, 15)) console.log(String(v).padStart(5), k);

  console.log("\nBy Employment Type:");
  for (const [k, v] of topN(employment, 15))
    console.log(String(v).padStart(5), k);

  console.log("\nRemote flag (best-effort):");
  for (const [k, v] of topN(remoteMap, 10))
    console.log(String(v).padStart(5), k);

  console.log(line);
}

main();
