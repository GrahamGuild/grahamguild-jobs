/**
 * build-inbox.js
 *
 * Reads newest raw LinkedIn + Active Jobs JSON files from /data/raw,
 * normalizes them into a shared schema, dedupes cross-source (with transitive merging),
 * filters out jobs that already have a decision, and writes:
 *   /data/inbox/inbox_latest.json
 *   /data/inbox/dedupe_report_latest.json
 *
 * Dedupe strategy (KEYS, in order of strength):
 *  1) ATS signature (extracted from RAW applyUrl/url, query intact)  -> strongest cross-source signal
 *  2) applyUrl (cleaned: strip query/fragment)
 *  3) non-LinkedIn url (cleaned)
 *  4) LinkedIn job url (cleaned)
 *  5) fallback signature: org_norm + title_norm + domain + date_bucket
 *
 * IMPORTANT: We generate MULTIPLE keys per job and union groups across keys.
 * This preserves transitive merges (A matches B on apply, B matches C on ATS sig, etc.).
 */

const fs = require("fs");
const path = require("path");

const RAW_DIR = path.join(process.cwd(), "data", "raw");
const INBOX_DIR = path.join(process.cwd(), "data", "inbox");
const OUT_PATH = path.join(INBOX_DIR, "inbox_latest.json");
const DEDUPE_REPORT_PATH = path.join(INBOX_DIR, "dedupe_report_latest.json");

const STATE_DIR = path.join(process.cwd(), "data", "state");
const DECISIONS_PATH = path.join(STATE_DIR, "decisions.json");

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

function newestFileMatching(prefix) {
  const files = listFiles(RAW_DIR)
    .filter((p) => path.basename(p).startsWith(prefix))
    .sort(); // filenames include ISO-ish timestamps, lexicographic sort works
  return files.length ? files[files.length - 1] : null;
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
    // strip query + fragment
    x.search = "";
    x.hash = "";
    // normalize
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
    // try after cleaning
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

/** -------------------------
 * ATS signature extraction (USES RAW URL - query intact)
 * ------------------------*/

function safeParseUrl(rawUrl) {
  if (!rawUrl || typeof rawUrl !== "string") return null;
  try {
    return new URL(rawUrl);
  } catch {
    // try adding scheme if missing
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
  const pathname = (urlObj.pathname || "").toLowerCase();
  const path = urlObj.pathname || "";
  const sp = urlObj.searchParams;

  // helpers
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

  // Workday (tons of variants)
  // Try query params first because many Workday links put req/job ids there.
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

    // Path-based: .../job/.../<something>
    const m1 = path.match(/\/job\/[^/]+\/([^/?#]+)/i);
    if (m1 && m1[1]) return `workday:${host}:${m1[1]}`;

    // Sometimes req id is the last token and looks like <slug>_<id>
    const tok = lastPathToken();
    if (tok) return `workday:${host}:${tok}`;
  }

  // Greenhouse
  // https://boards.greenhouse.io/<company>/jobs/<id>
  if (host.includes("greenhouse.io")) {
    const q = findQueryAny(["gh_jid", "jid", "job_id", "jobid"]);
    if (q && /^\d+$/.test(q)) return `greenhouse:${q}`;

    const m = path.match(/\/jobs\/(\d+)/i);
    if (m && m[1]) return `greenhouse:${m[1]}`;
  }

  // Lever
  // https://jobs.lever.co/<company>/<postingId>
  // https://apply.lever.co/<company>/<postingId>
  if (host.includes("lever.co")) {
    const toks = (urlObj.pathname || "").split("/").filter(Boolean);
    // toks: [company, postingId] or [company, postingId, ...]
    if (toks.length >= 2) return `lever:${toks[1]}`;
    const tok = lastPathToken();
    if (tok) return `lever:${tok}`;
  }

  // Workable
  // https://jobs.workable.com/view/<id>/...
  if (host.includes("workable.com")) {
    const m = path.match(/\/view\/([^/?#]+)/i);
    if (m && m[1]) return `workable:${m[1]}`;
  }

  // Oracle Cloud / Oracle recruiting variants
  // Many have jobId in query OR a numeric tail token.
  if (host.includes("oraclecloud.com") || host.includes("oracle")) {
    const q = findQueryAny(["jobid", "jobId", "job_id", "id"]);
    if (q && /^\d{4,}$/.test(q)) return `oracle:${q}`;

    const m = path.match(/\/job\/(\d+)/i);
    if (m && m[1]) return `oracle:${m[1]}`;

    const tok = lastPathToken();
    if (tok && /^\d{4,}$/.test(tok)) return `oracle:${tok}`;
  }

  // iCIMS
  if (host.includes("icims.com")) {
    const m = path.match(/\/jobs\/(\d+)/i);
    if (m && m[1]) return `icims:${m[1]}`;
    const q = findQueryAny(["jobid", "jobId"]);
    if (q && /^\d+$/.test(q)) return `icims:${q}`;
  }

  // SmartRecruiters sometimes encodes id in path
  if (host.includes("smartrecruiters.com")) {
    // /Company/<id>-<slug> or /job/<id>
    const m = path.match(/\/job\/([^/?#]+)/i);
    if (m && m[1]) return `smartrecruiters:${m[1]}`;
    const tok = lastPathToken();
    if (tok) return `smartrecruiters:${tok}`;
  }

  // If nothing matched, return empty (no ATS signature)
  return "";
}

/** -------------------------
 * Normalizers
 * ------------------------*/

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

  // IMPORTANT: ATS signature from RAW URLs (query intact)
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

  // IMPORTANT: ATS signature from RAW URLs (query intact)
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

    _orgNorm: normText(organization),
    _titleNorm: normText(title),
    _urlClean: urlClean,
    _applyClean: applyClean,
    _domain: urlDomain(applyUrl || url),
    _dateBucket: dateBucket(datePosted),
    _atsSig: atsSig,
  };
}

/** -------------------------
 * Dedupe key generation (multiple)
 * ------------------------*/

function fallbackSignature(job) {
  return `sig:${job._orgNorm}|${job._titleNorm}|${job._domain}|${job._dateBucket}`;
}

function jobKeys(job) {
  const keys = [];

  if (job._atsSig) keys.push(`ats:${job._atsSig}`);
  if (job._applyClean) keys.push(`apply:${job._applyClean}`);

  if (job._urlClean && !isLinkedInJobUrl(job._urlClean))
    keys.push(`url:${job._urlClean}`);
  if (job._urlClean && isLinkedInJobUrl(job._urlClean))
    keys.push(`liurl:${job._urlClean}`);

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

/** -------------------------
 * Union-Find for transitive dedupe
 * ------------------------*/

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

/** -------------------------
 * Main
 * ------------------------*/

function main() {
  ensureDir(INBOX_DIR);
  ensureDir(STATE_DIR);

  const liPath = newestFileMatching("linkedin_");
  const ajPath = newestFileMatching("active_jobs_");

  if (!liPath && !ajPath) {
    console.error(
      "No raw files found in data/raw (expected linkedin_* and/or active_jobs_*).",
    );
    process.exit(1);
  }

  const normalized = [];
  const sources = {
    linkedin: liPath ? { latestFile: path.basename(liPath), rows: 0 } : null,
    active_jobs: ajPath ? { latestFile: path.basename(ajPath), rows: 0 } : null,
  };

  if (liPath) {
    const liRaw = readJson(liPath);
    const arr = Array.isArray(liRaw)
      ? liRaw
      : liRaw && Array.isArray(liRaw.data)
        ? liRaw.data
        : null;
    if (!arr)
      console.warn(
        "LinkedIn raw file shape not recognized. Expected an array.",
      );
    else {
      sources.linkedin.rows = arr.length;
      arr.forEach((x) => normalized.push(normalizeLinkedIn(x)));
    }
  }

  if (ajPath) {
    const ajRaw = readJson(ajPath);
    const arr = Array.isArray(ajRaw)
      ? ajRaw
      : ajRaw && Array.isArray(ajRaw.data)
        ? ajRaw.data
        : null;
    if (!arr)
      console.warn(
        "Active Jobs raw file shape not recognized. Expected an array.",
      );
    else {
      sources.active_jobs.rows = arr.length;
      arr.forEach((x) => normalized.push(normalizeActiveJobs(x)));
    }
  }

  // Union by keys
  const uf = makeUF(normalized.length);
  const keyToIndex = new Map();

  for (let i = 0; i < normalized.length; i++) {
    const keys = jobKeys(normalized[i]);
    for (const k of keys) {
      if (!keyToIndex.has(k)) keyToIndex.set(k, i);
      else uf.union(i, keyToIndex.get(k));
    }
  }

  // Group indices by root
  const groups = new Map();
  for (let i = 0; i < normalized.length; i++) {
    const r = uf.find(i);
    if (!groups.has(r)) groups.set(r, []);
    groups.get(r).push(i);
  }

  // Choose best per group + build report groups
  const dedupeGroups = [];
  const uniqueJobs = [];

  for (const idxs of groups.values()) {
    let kept = normalized[idxs[0]];
    for (let j = 1; j < idxs.length; j++)
      kept = chooseBetter(kept, normalized[idxs[j]]);
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

  // Sort + strip private fields
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

  // Respect decisions
  const decisionsDoc = tryReadJson(DECISIONS_PATH, {
    version: 1,
    updatedAt: null,
    decisions: {},
  });
  const decisionsMap = (decisionsDoc && decisionsDoc.decisions) || {};

  const filtered = [];
  let filteredOutByDecision = 0;
  for (const job of jobs) {
    const key = `${job.source}:${job.sourceId}`;
    if (decisionsMap[key]) {
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
        linkedin: liPath ? path.basename(liPath) : null,
        active_jobs: ajPath ? path.basename(ajPath) : null,
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

main();
