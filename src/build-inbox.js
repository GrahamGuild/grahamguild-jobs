/**
 * build-inbox.js
 *
 * Reads newest non-empty raw LinkedIn + Active Jobs + YC Jobs JSON files from /data/raw,
 * normalizes them into a shared schema, dedupes cross-source (with transitive merging),
 * carries forward prior inbox jobs (so inbox doesn't wipe to empty when no new raws),
 * filters out jobs that already have a decision, and writes:
 *   /data/inbox/inbox_latest.json
 *   /data/inbox/dedupe_report_latest.json
 */

require("dotenv").config();

const fs = require("fs");
const path = require("path");
const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL =
  process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL;

const SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY ||
  process.env.SUPABASE_SECRET_KEY ||
  process.env.SUPABASE_SERVICE_KEY ||
  process.env.SUPABASE_SECRET;

const RAW_DIR = path.join(process.cwd(), "data", "raw");
const INBOX_DIR = path.join(process.cwd(), "data", "inbox");
const STATE_DIR = path.join(process.cwd(), "data", "state");

const OUT_PATH = path.join(INBOX_DIR, "inbox_latest.json");
const DEDUPE_REPORT_PATH = path.join(INBOX_DIR, "dedupe_report_latest.json");
const WEB_PUBLIC_INBOX_PATH = path.join(
  process.cwd(),
  "web",
  "public",
  "inbox_latest.json",
);

const PREV_INBOX_PATH = fs.existsSync(OUT_PATH)
  ? OUT_PATH
  : WEB_PUBLIC_INBOX_PATH;

function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf-8"));
}

function tryReadJson(p, fallback) {
  try {
    if (!fs.existsSync(p)) return fallback;
    return readJson(p);
  } catch {
    return fallback;
  }
}

function listFiles(dir) {
  if (!fs.existsSync(dir)) return [];
  return fs.readdirSync(dir).map((f) => path.join(dir, f));
}

function fileHasAtLeastOneRecord(p) {
  try {
    if (!fs.existsSync(p)) return false;
    const raw = fs.readFileSync(p, "utf-8");
    if (!raw || !raw.trim()) return false;

    const parsed = JSON.parse(raw);

    if (Array.isArray(parsed)) return parsed.length > 0;
    if (parsed && Array.isArray(parsed.data)) return parsed.data.length > 0;
    if (parsed && Array.isArray(parsed.items)) return parsed.items.length > 0;
    if (parsed && typeof parsed === "object")
      return Object.keys(parsed).length > 0;

    return false;
  } catch {
    return false;
  }
}

function newestFileMatching(prefix) {
  const files = listFiles(RAW_DIR)
    .filter((p) => path.basename(p).startsWith(prefix))
    .sort();

  for (let i = files.length - 1; i >= 0; i--) {
    if (fileHasAtLeastOneRecord(files[i])) return files[i];
  }

  return null;
}

function normText(s) {
  if (!s || typeof s !== "string") return "";
  return s
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function safeIsoDate(dateStr) {
  if (!dateStr) return null;
  const d = new Date(dateStr);
  return isNaN(d.getTime()) ? null : d.toISOString();
}

function dateBucket(dateStr) {
  const iso = safeIsoDate(dateStr);
  if (!iso) return "";
  return iso.slice(0, 10);
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
    return u.toLowerCase().trim();
  }
}

function urlDomain(rawUrl) {
  if (!rawUrl || typeof rawUrl !== "string") return "";
  try {
    const x = new URL(rawUrl);
    return (x.host || "").toLowerCase();
  } catch {
    const cu = cleanUrl(rawUrl);
    if (!cu) return "";
    try {
      const x2 = new URL(cu);
      return (x2.host || "").toLowerCase();
    } catch {
      return "";
    }
  }
}

function isLinkedInJobUrl(u) {
  const cu = cleanUrl(u);
  return cu.includes("linkedin.com/jobs/view");
}

function safeParseUrl(rawUrl) {
  if (!rawUrl || typeof rawUrl !== "string") return null;
  try {
    return new URL(rawUrl);
  } catch {
    try {
      return new URL(`https://${rawUrl}`);
    } catch {
      return null;
    }
  }
}

function extractAtsSignature(rawUrl) {
  const urlObj = safeParseUrl(rawUrl);
  if (!urlObj) return "";

  const host = (urlObj.host || "").toLowerCase();
  const pathn = urlObj.pathname || "";
  const pathLower = pathn.toLowerCase();
  const sp = urlObj.searchParams;

  const lastPathToken = () => {
    const toks = (urlObj.pathname || "").split("/").filter(Boolean);
    return toks.length ? toks[toks.length - 1] : "";
  };

  const findQueryAny = (keys) => {
    for (const k of keys) {
      const v = sp.get(k);
      if (v) return v;
    }
    return "";
  };

  if (host.includes("myworkdayjobs.com") || host.includes("workday")) {
    const q =
      findQueryAny([
        "jobreqid",
        "jobReqId",
        "reqid",
        "ReqId",
        "requisitionid",
        "RequisitionId",
        "jobid",
        "jobId",
      ]) || "";

    if (q) return `workday:${host}:${q}`;

    const m1 = pathLower.match(/\/job\/[^/]+\/([^/?#]+)/i);
    if (m1 && m1[1]) return `workday:${host}:${m1[1]}`;

    const tok = lastPathToken();
    if (tok) return `workday:${host}:${tok}`;
  }

  if (host.includes("greenhouse.io")) {
    const q = findQueryAny(["gh_jid", "jid", "job_id", "jobid"]);
    if (q && /^\d+$/.test(q)) return `greenhouse:${q}`;

    const m = pathLower.match(/\/jobs\/(\d+)/i);
    if (m && m[1]) return `greenhouse:${m[1]}`;
  }

  if (host.includes("lever.co")) {
    const toks = (urlObj.pathname || "").split("/").filter(Boolean);
    if (toks.length >= 2) return `lever:${toks[1]}`;
    const tok = lastPathToken();
    if (tok) return `lever:${tok}`;
  }

  if (host.includes("workable.com")) {
    const m = pathLower.match(/\/view\/([^/?#]+)/i);
    if (m && m[1]) return `workable:${m[1]}`;
  }

  if (host.includes("oraclecloud.com") || host.includes("oracle")) {
    const q = findQueryAny(["jobid", "jobId", "job_id", "id"]);
    if (q && /^\d{4,}$/.test(q)) return `oracle:${q}`;

    const m = pathLower.match(/\/job\/(\d+)/i);
    if (m && m[1]) return `oracle:${m[1]}`;

    const tok = lastPathToken();
    if (tok && /^\d{4,}$/.test(tok)) return `oracle:${tok}`;
  }

  if (host.includes("icims.com")) {
    const m = pathLower.match(/\/jobs\/(\d+)/i);
    if (m && m[1]) return `icims:${m[1]}`;
    const q = findQueryAny(["jobid", "jobId"]);
    if (q && /^\d+$/.test(q)) return `icims:${q}`;
  }

  if (host.includes("smartrecruiters.com")) {
    const m = pathLower.match(/\/job\/([^/?#]+)/i);
    if (m && m[1]) return `smartrecruiters:${m[1]}`;
    const tok = lastPathToken();
    if (tok) return `smartrecruiters:${tok}`;
  }

  return "";
}

function normalizeLinkedIn(item) {
  const url = item.url || "";
  const applyUrl = item.external_apply_url || item.externalApplyUrl || "";
  const title = item.title || "";
  const organization = item.organization || item.company || "";
  const location =
    (Array.isArray(item.locations_derived) && item.locations_derived[0]) ||
    (Array.isArray(item.locations_raw) &&
      item.locations_raw[0] &&
      item.locations_raw[0].address &&
      [
        item.locations_raw[0].address.addressLocality,
        item.locations_raw[0].address.addressRegion,
      ]
        .filter(Boolean)
        .join(", ")) ||
    "";

  const datePosted =
    item.date_posted || item.datePosted || item.date_created || "";

  const remote =
    typeof item.remote_derived === "boolean"
      ? item.remote_derived
      : (item.location_type || "").toUpperCase() === "TELECOMMUTE";

  const urlClean = cleanUrl(url);
  const applyClean = cleanUrl(applyUrl);
  const atsSig = extractAtsSignature(applyUrl || url);

  return {
    source: "linkedin",
    sourceId: item.linkedin_id || item.linkedinId || item.id || null,
    title,
    organization,
    location,
    datePosted: safeIsoDate(datePosted),
    remote: !!remote,
    url: url || null,
    applyUrl: applyUrl || null,
    employmentType: Array.isArray(item.employment_type)
      ? item.employment_type[0]
      : item.employment_type || null,
    seniority: item.seniority || null,
    employees: item.linkedin_org_employees || null,
    atsSource: item.source || null,
    _orgNorm: normText(organization),
    _titleNorm: normText(title),
    _urlClean: urlClean,
    _applyClean: applyClean,
    _domain: urlDomain(applyUrl || url),
    _dateBucket: dateBucket(datePosted),
    _atsSig: atsSig,
  };
}

function normalizeActiveJobs(item) {
  const title = item.title || "";
  const organization =
    item.organization || item.company || item.company_name || "";
  const url =
    item.url || item.job_url || item.apply_url || item.external_apply_url || "";
  const applyUrl =
    item.apply_url || item.external_apply_url || item.applyUrl || "";

  const location =
    item.location ||
    item.locations ||
    item.location_raw ||
    (Array.isArray(item.locations_derived) && item.locations_derived[0]) ||
    "";

  const datePosted =
    item.date_posted ||
    item.datePosted ||
    item.date_created ||
    item.created_at ||
    item.updated_at ||
    "";

  const remote =
    typeof item.remote === "boolean"
      ? item.remote
      : typeof item.remote_derived === "boolean"
        ? item.remote_derived
        : /remote/i.test(String(location || "")) ||
          /remote/i.test(String(title || ""));

  const urlClean = cleanUrl(url);
  const applyClean = cleanUrl(applyUrl);
  const atsSig = extractAtsSignature(applyUrl || url);

  return {
    source: "active_jobs",
    sourceId:
      item.id || item.job_id || item.req_id || item.requisition_id || null,
    title,
    organization,
    location:
      typeof location === "string" ? location : JSON.stringify(location),
    datePosted: safeIsoDate(datePosted),
    remote: !!remote,
    url: url || null,
    applyUrl: applyUrl || null,
    employmentType: item.employment_type || item.employmentType || null,
    atsSource: item.source || item.ats || item.platform || null,
    seniority: item.seniority || null,
    employees: item.linkedin_org_employees || null,
    _orgNorm: normText(organization),
    _titleNorm: normText(title),
    _urlClean: urlClean,
    _applyClean: applyClean,
    _domain: urlDomain(applyUrl || url),
    _dateBucket: dateBucket(datePosted),
    _atsSig: atsSig,
  };
}

function normalizeYcJobs(item) {
  const title = item.title || "";
  const organization = item.organization || "";
  const url = item.url || "";
  const applyUrl = item.external_apply_url || item.externalApplyUrl || "";

  const location =
    (Array.isArray(item.locations_derived) && item.locations_derived[0]) ||
    (Array.isArray(item.locations_raw) &&
      item.locations_raw[0] &&
      item.locations_raw[0].address &&
      [
        item.locations_raw[0].address.addressLocality,
        item.locations_raw[0].address.addressRegion,
        item.locations_raw[0].address.addressCountry,
      ]
        .filter(Boolean)
        .join(", ")) ||
    "";

  const datePosted =
    item.date_posted || item.datePosted || item.date_created || "";

  const remote =
    typeof item.remote_derived === "boolean"
      ? item.remote_derived
      : (item.location_type || "").toUpperCase() === "TELECOMMUTE";

  const urlClean = cleanUrl(url);
  const applyClean = cleanUrl(applyUrl);
  const atsSig = extractAtsSignature(applyUrl || url);

  return {
    source: "yc_jobs",
    sourceId: item.id || null,
    title,
    organization,
    location,
    datePosted: safeIsoDate(datePosted),
    remote: !!remote,
    url: url || null,
    applyUrl: applyUrl || null,
    employmentType: Array.isArray(item.employment_type)
      ? item.employment_type[0]
      : item.employment_type || null,
    seniority: item.seniority || null,
    employees: item.linkedin_org_employees || null,
    atsSource: item.source || "ycombinator",
    _orgNorm: normText(organization),
    _titleNorm: normText(title),
    _urlClean: urlClean,
    _applyClean: applyClean,
    _domain: urlDomain(applyUrl || url),
    _dateBucket: dateBucket(datePosted),
    _atsSig: atsSig,
  };
}

function normalizeExistingInboxJob(j) {
  const source = j.source || "";
  const sourceId = j.sourceId != null ? j.sourceId : null;

  const title = j.title || "";
  const organization = j.organization || "";
  const location = j.location || "";

  const datePosted = safeIsoDate(j.datePosted || j.date_posted || "");
  const remote = !!j.remote;

  const url = j.url || null;
  const applyUrl = j.applyUrl || j.apply_url || null;

  const urlClean = cleanUrl(url || "");
  const applyClean = cleanUrl(applyUrl || "");
  const atsSig = extractAtsSignature(applyUrl || url || "");

  return {
    source,
    sourceId,
    title,
    organization,
    location,
    datePosted,
    remote,
    url,
    applyUrl,
    employmentType: j.employmentType || j.employment_type || null,
    seniority: j.seniority || null,
    employees: j.employees || null,
    atsSource: j.atsSource || j.ats_source || null,
    _orgNorm: normText(organization),
    _titleNorm: normText(title),
    _urlClean: urlClean,
    _applyClean: applyClean,
    _domain: urlDomain(applyUrl || url || ""),
    _dateBucket: dateBucket(datePosted),
    _atsSig: atsSig,
  };
}

function fallbackSignature(job) {
  return `sig:${job._orgNorm}|${job._titleNorm}|${job._domain}|${job._dateBucket}`;
}

function jobKeys(job) {
  const keys = [];

  if (job._atsSig) keys.push(`ats:${job._atsSig}`);
  if (job._applyClean) keys.push(`apply:${job._applyClean}`);

  if (job._urlClean && !isLinkedInJobUrl(job._urlClean)) {
    keys.push(`url:${job._urlClean}`);
  }
  if (job._urlClean && isLinkedInJobUrl(job._urlClean)) {
    keys.push(`liurl:${job._urlClean}`);
  }

  keys.push(fallbackSignature(job));
  return Array.from(new Set(keys));
}

function chooseBetter(a, b) {
  const score = (j) => {
    let s = 0;
    if (j.applyUrl) s += 3;
    if (j.url) s += 2;
    if (j.location) s += 1;
    if (j.datePosted) s += 1;
    if (j.employmentType) s += 1;
    if (j.seniority) s += 1;
    if (j.employees) s += 1;
    if (j.atsSource) s += 1;
    if (j._atsSig) s += 2;
    return s;
  };
  return score(b) > score(a) ? b : a;
}

function makeUF(n) {
  const parent = Array.from({ length: n }, (_, i) => i);
  const rank = Array.from({ length: n }, () => 0);

  function find(x) {
    while (parent[x] !== x) {
      parent[x] = parent[parent[x]];
      x = parent[x];
    }
    return x;
  }

  function union(a, b) {
    let ra = find(a);
    let rb = find(b);
    if (ra === rb) return;

    if (rank[ra] < rank[rb]) parent[ra] = rb;
    else if (rank[ra] > rank[rb]) parent[rb] = ra;
    else {
      parent[rb] = ra;
      rank[ra] += 1;
    }
  }

  return { find, union };
}

async function main() {
  ensureDir(INBOX_DIR);
  ensureDir(STATE_DIR);

  const liPath = newestFileMatching("linkedin_");
  const ajPath = newestFileMatching("active_jobs_");
  const ycPath = newestFileMatching("yc_jobs_");

  if (!liPath && !ajPath && !ycPath && !fs.existsSync(PREV_INBOX_PATH)) {
    console.error(
      "No non-empty raw files found in data/raw and no existing inbox to carry forward.",
    );
    process.exit(1);
  }

  const prevInbox = tryReadJson(PREV_INBOX_PATH, null);
  const prevJobsArr = Array.isArray(prevInbox?.jobs) ? prevInbox.jobs : [];
  const prevNormalized = prevJobsArr.map(normalizeExistingInboxJob);

  const sources = {
    previous_inbox: fs.existsSync(PREV_INBOX_PATH)
      ? {
          latestFile: path.basename(PREV_INBOX_PATH),
          rows: prevJobsArr.length,
        }
      : null,
    linkedin: liPath ? { latestFile: path.basename(liPath), rows: 0 } : null,
    active_jobs: ajPath ? { latestFile: path.basename(ajPath), rows: 0 } : null,
    yc_jobs: ycPath ? { latestFile: path.basename(ycPath), rows: 0 } : null,
  };

  const normalizedNew = [];

  if (liPath) {
    const liRaw = readJson(liPath);
    const arr = Array.isArray(liRaw)
      ? liRaw
      : liRaw && Array.isArray(liRaw.data)
        ? liRaw.data
        : null;
    if (!arr) {
      console.warn(
        "LinkedIn raw file shape not recognized. Expected an array.",
      );
    } else {
      sources.linkedin.rows = arr.length;
      arr.forEach((x) => normalizedNew.push(normalizeLinkedIn(x)));
    }
  }

  if (ajPath) {
    const ajRaw = readJson(ajPath);
    const arr = Array.isArray(ajRaw)
      ? ajRaw
      : ajRaw && Array.isArray(ajRaw.data)
        ? ajRaw.data
        : null;
    if (!arr) {
      console.warn(
        "Active Jobs raw file shape not recognized. Expected an array.",
      );
    } else {
      sources.active_jobs.rows = arr.length;
      arr.forEach((x) => normalizedNew.push(normalizeActiveJobs(x)));
    }
  }

  if (ycPath) {
    const ycRaw = readJson(ycPath);
    const arr = Array.isArray(ycRaw)
      ? ycRaw
      : ycRaw && Array.isArray(ycRaw.data)
        ? ycRaw.data
        : null;
    if (!arr) {
      console.warn("YC Jobs raw file shape not recognized. Expected an array.");
    } else {
      sources.yc_jobs.rows = arr.length;
      arr.forEach((x) => normalizedNew.push(normalizeYcJobs(x)));
    }
  }

  const normalized = [...prevNormalized, ...normalizedNew];

  if (normalized.length === 0) {
    const out = {
      meta: {
        builtAt: new Date().toISOString(),
        inputs: {
          previous_inbox: sources.previous_inbox?.latestFile || null,
          linkedin: liPath ? path.basename(liPath) : null,
          active_jobs: ajPath ? path.basename(ajPath) : null,
          yc_jobs: ycPath ? path.basename(ycPath) : null,
        },
      },
      sources,
      counts: {
        normalizedTotal: 0,
        uniqueBeforeDecisions: 0,
        dupesDiscarded: 0,
        filteredOutByDecision: 0,
        unique: 0,
      },
      jobs: [],
    };

    fs.writeFileSync(OUT_PATH, JSON.stringify(out, null, 2), "utf-8");
    fs.writeFileSync(
      DEDUPE_REPORT_PATH,
      JSON.stringify(
        {
          builtAt: out.meta.builtAt,
          inputs: out.meta.inputs,
          counts: out.counts,
          groupsWithMerges: 0,
          groups: [],
        },
        null,
        2,
      ),
      "utf-8",
    );

    console.log("Counts:", out.counts);
    console.log("Wrote:", OUT_PATH);
    console.log("Dedupe report:", DEDUPE_REPORT_PATH);
    return;
  }

  const uf = makeUF(normalized.length);
  const keyToIndex = new Map();

  for (let i = 0; i < normalized.length; i++) {
    const keys = jobKeys(normalized[i]);
    for (const k of keys) {
      if (!keyToIndex.has(k)) keyToIndex.set(k, i);
      else uf.union(i, keyToIndex.get(k));
    }
  }

  const groups = new Map();
  for (let i = 0; i < normalized.length; i++) {
    const r = uf.find(i);
    if (!groups.has(r)) groups.set(r, []);
    groups.get(r).push(i);
  }

  const dedupeGroups = [];
  const uniqueJobs = [];

  for (const idxs of groups.values()) {
    let kept = normalized[idxs[0]];
    for (let j = 1; j < idxs.length; j++) {
      kept = chooseBetter(kept, normalized[idxs[j]]);
    }
    uniqueJobs.push(kept);

    if (idxs.length > 1) {
      const members = idxs.map((ix) => {
        const jj = normalized[ix];
        return {
          source: jj.source,
          sourceId: jj.sourceId,
          title: jj.title,
          organization: jj.organization,
          url: jj.url,
          applyUrl: jj.applyUrl,
          atsSig: jj._atsSig || null,
          datePosted: jj.datePosted,
        };
      });

      const keys = Array.from(
        new Set(idxs.flatMap((ix) => jobKeys(normalized[ix]))),
      );

      dedupeGroups.push({
        size: idxs.length,
        keys,
        kept: {
          source: kept.source,
          sourceId: kept.sourceId,
          title: kept.title,
          organization: kept.organization,
          url: kept.url,
          applyUrl: kept.applyUrl,
          atsSig: kept._atsSig || null,
          datePosted: kept.datePosted,
        },
        members,
      });
    }
  }

  let jobs = uniqueJobs
    .sort((a, b) => {
      const da = a.datePosted ? new Date(a.datePosted).getTime() : 0;
      const db = b.datePosted ? new Date(b.datePosted).getTime() : 0;
      return db - da;
    })
    .map((j) => {
      const {
        _orgNorm,
        _titleNorm,
        _urlClean,
        _applyClean,
        _domain,
        _dateBucket,
        _atsSig,
        ...clean
      } = j;
      return clean;
    });

  const uniqueBeforeDecisions = jobs.length;

  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error(
      "Missing SUPABASE_URL and/or SUPABASE_SERVICE_ROLE_KEY in environment for build-inbox.js",
    );
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

  const decidedSet = new Set();
  const pageSize = 1000;
  let from = 0;

  while (true) {
    const to = from + pageSize - 1;
    const { data: decidedRows, error: decidedErr } = await supabase
      .from("job_decisions")
      .select("job_key")
      .range(from, to);

    if (decidedErr) {
      throw new Error(
        `Failed to load job_decisions from Supabase: ${decidedErr.message}`,
      );
    }

    for (const r of decidedRows || []) {
      if (r?.job_key) decidedSet.add(String(r.job_key));
    }

    if (!decidedRows || decidedRows.length < pageSize) break;
    from += pageSize;
  }

  const filtered = [];
  let filteredOutByDecision = 0;

  for (const job of jobs) {
    const jobKey = `${job.source}:${job.sourceId}`;
    if (decidedSet.has(jobKey)) {
      filteredOutByDecision += 1;
      continue;
    }
    filtered.push(job);
  }

  const dupesDiscarded = normalized.length - uniqueBeforeDecisions;

  const out = {
    meta: {
      builtAt: new Date().toISOString(),
      inputs: {
        previous_inbox: sources.previous_inbox?.latestFile || null,
        linkedin: liPath ? path.basename(liPath) : null,
        active_jobs: ajPath ? path.basename(ajPath) : null,
        yc_jobs: ycPath ? path.basename(ycPath) : null,
      },
    },
    sources,
    counts: {
      normalizedTotal: normalized.length,
      uniqueBeforeDecisions,
      dupesDiscarded,
      filteredOutByDecision,
      unique: filtered.length,
    },
    jobs: filtered,
  };

  fs.writeFileSync(OUT_PATH, JSON.stringify(out, null, 2), "utf-8");

  const report = {
    builtAt: out.meta.builtAt,
    inputs: out.meta.inputs,
    counts: out.counts,
    groupsWithMerges: dedupeGroups.length,
    groups: dedupeGroups.sort((a, b) => b.size - a.size),
  };

  fs.writeFileSync(
    DEDUPE_REPORT_PATH,
    JSON.stringify(report, null, 2),
    "utf-8",
  );

  console.log("Counts:", out.counts);
  console.log("Wrote:", OUT_PATH);
  console.log("Dedupe report:", DEDUPE_REPORT_PATH);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
