import fs from "fs";
import path from "path";

export type Job = {
  source: string;
  sourceId: string;
  title?: string;
  organization?: string;
  location?: string;
  url?: string;
  applyUrl?: string;
  datePosted?: string | null;
  remote?: boolean;
};

type InboxFile = {
  jobs: Job[];
};

export function loadInbox(): Job[] {
  // You said you moved inbox_latest.json to web/public/
  const p = path.join(process.cwd(), "public", "inbox_latest.json");
  const raw = fs.readFileSync(p, "utf-8");
  const parsed = JSON.parse(raw) as InboxFile;

  const jobs = Array.isArray(parsed?.jobs) ? parsed.jobs : [];
  // Ensure required fields exist (so we can key reliably)
  return jobs.filter((j) => j?.source && j?.sourceId);
}
