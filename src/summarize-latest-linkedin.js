// src/summarize-latest-linkedin.js
/* eslint-disable no-console */
const fs = require("fs");
const path = require("path");

const RAW_DIR = path.join(process.cwd(), "data", "raw");
const PREFIX = "linkedin_";
const SUFFIX = ".json";

function formatPct(n, d) {
  if (!d) return "0%";
  return `${((n / d) * 100).toFixed(1)}%`;
}

function safeReadJson(filePath) {
  const txt = fs.readFileSync(filePath, "utf8");
  return JSON.parse(txt);
}

function listLatestLinkedInFile() {
  if (!fs.existsSync(RAW_DIR)) {
    throw new Error(`Raw dir not found: ${RAW_DIR}`);
  }

  const files = fs
    .readdirSync(RAW_DIR)
    .filter((f) => f.startsWith(PREFIX) && f.endsWith(SUFFIX))
    .map((f) => ({
      name: f,
      fullPath: path.join(RAW_DIR, f),
      mtimeMs: fs.statSync(path.join(RAW_DIR, f)).mtimeMs,
    }))
    .sort((a, b) => b.mtimeMs - a.mtimeMs);

  if (files.length === 0) {
    throw new Error(`No ${PREFIX}*.json files found in ${RAW_DIR}`);
  }

  return files[0];
}

function topCounts(items, keyFn, n = 10) {
  const m = new Map();
  for (const it of items) {
    const k = keyFn(it);
    if (!k) continue;
    m.set(k, (m.get(k) || 0) + 1);
  }
  return [...m.entries()].sort((a, b) => b[1] - a[1]).slice(0, n);
}

function normalizeStr(s) {
  if (!s) return "";
  return String(s).trim().toLowerCase();
}

function main() {
  const latest = listLatestLinkedInFile();
  const data = safeReadJson(latest.fullPath);

  if (!Array.isArray(data)) {
    throw new Error("Expected the JSON file to contain an array of jobs.");
  }

  const total = data.length;

  // Unique by "id" if present, else fall back to linkedin_id, else url
  const seen = new Set();
  let dupes = 0;

  for (const j of data) {
    const key =
      (j && j.id != null ? `id:${j.id}` : "") ||
      (j && j.linkedin_id != null ? `li:${j.linkedin_id}` : "") ||
      (j && j.url ? `url:${j.url}` : "");

    if (!key) continue;

    if (seen.has(key)) dupes += 1;
    else seen.add(key);
  }

  const unique = seen.size;

  // Useful summaries
  const topTitles = topCounts(data, (j) => j.title, 15);
  const topOrgs = topCounts(data, (j) => j.organization, 15);
  const bySeniority = topCounts(data, (j) => j.seniority, 12);
  const byRemoteDerived = topCounts(data, (j) => String(j.remote_derived), 10);
  const byLocationType = topCounts(data, (j) => j.location_type, 10);

  // “Suspicious” / quick QA buckets
  const agencyJobs = data.filter(
    (j) => j.linkedin_org_recruitment_agency_derived === true,
  );
  const tinyCompanyJobs = data.filter(
    (j) =>
      typeof j.linkedin_org_employees === "number" &&
      j.linkedin_org_employees > 0 &&
      j.linkedin_org_employees < 10,
  );

  // Also: confirm date range roughness
  const dates = data
    .map((j) => j.date_posted)
    .filter(Boolean)
    .sort(); // ISO sorts lexicographically
  const minDate = dates[0] || "n/a";
  const maxDate = dates[dates.length - 1] || "n/a";

  console.log("==================================================");
  console.log(`Latest file: ${latest.name}`);
  console.log(`Path: ${latest.fullPath}`);
  console.log(`Rows: ${total}`);
  console.log(`Unique (by id/linkedin_id/url): ${unique}`);
  console.log(`Dupes detected: ${dupes}`);
  console.log(`Date posted range: ${minDate}  →  ${maxDate}`);
  console.log("--------------------------------------------------");

  console.log("\nTop Titles:");
  for (const [k, v] of topTitles)
    console.log(`  ${v.toString().padStart(3)}  ${k}`);

  console.log("\nTop Organizations:");
  for (const [k, v] of topOrgs)
    console.log(`  ${v.toString().padStart(3)}  ${k}`);

  console.log("\nBy Seniority:");
  for (const [k, v] of bySeniority)
    console.log(`  ${v.toString().padStart(3)}  ${k}`);

  console.log("\nRemote (remote_derived):");
  for (const [k, v] of byRemoteDerived)
    console.log(`  ${v.toString().padStart(3)}  ${k}`);

  console.log("\nBy Location Type (LinkedIn):");
  for (const [k, v] of byLocationType)
    console.log(`  ${v.toString().padStart(3)}  ${k}`);

  console.log("\nQA Buckets:");
  console.log(
    `  Agency/recruiter flagged: ${agencyJobs.length} (${formatPct(agencyJobs.length, total)})`,
  );
  console.log(
    `  Tiny companies (<10 employees): ${tinyCompanyJobs.length} (${formatPct(
      tinyCompanyJobs.length,
      total,
    )})`,
  );

  // Quick scan for Field CTO leakage (since you exclude it, it should be ~0)
  const fieldCtoLeak = data.filter((j) =>
    normalizeStr(j.title).includes("field cto"),
  );
  console.log(
    `  “Field CTO” in title (should be 0): ${fieldCtoLeak.length} (${formatPct(
      fieldCtoLeak.length,
      total,
    )})`,
  );

  console.log("==================================================");
}

try {
  main();
} catch (e) {
  console.error("Summarize failed:", e.message);
  process.exit(1);
}
