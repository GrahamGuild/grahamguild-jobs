import Link from "next/link";
import { createSupabaseServerClient } from "@/lib/supabase/server";
import path from "node:path";
import fs from "node:fs/promises";

type InboxJob = { source?: string; sourceId?: string | number };
type InboxFile = { jobs?: InboxJob[] };

export default async function NavLinksServer() {
  const supabase = await createSupabaseServerClient();

  const { data: decisionRows, error } = await supabase
    .from("job_decisions")
    .select("decision, job_key, source, source_id");

  const counts = { inbox: 0, saved: 0, applied: 0 };
  const decidedKeys = new Set<string>();

  if (!error) {
    for (const row of decisionRows ?? []) {
      if (row.decision === "saved") counts.saved += 1;
      if (row.decision === "applied") counts.applied += 1;

      const jobKey =
        (row.job_key as string | null) ??
        `${row.source ?? ""}:${row.source_id ?? ""}`;

      if (jobKey && jobKey.includes(":")) decidedKeys.add(jobKey);
    }
  }

  // Compute inbox by reading inbox_latest.json and excluding decided keys
  try {
    const inboxPath = path.join(process.cwd(), "public", "inbox_latest.json");
    const raw = await fs.readFile(inboxPath, "utf-8");
    const parsed = JSON.parse(raw) as InboxFile;

    const jobs = Array.isArray(parsed.jobs) ? parsed.jobs : [];
    let inboxTotal = 0;
    for (const j of jobs) {
      const key = `${j.source ?? ""}:${String(j.sourceId ?? "")}`;
      if (!decidedKeys.has(key)) inboxTotal += 1;
    }
    counts.inbox = inboxTotal;
  } catch {
    counts.inbox = 0;
  }

  return (
    <nav style={{ display: "flex", gap: 14, flexWrap: "wrap" }}>
      <Link href="/jobs">Inbox ({counts.inbox})</Link>
      <Link href="/saved">Saved ({counts.saved})</Link>
      <Link href="/applied">Applied ({counts.applied})</Link>
      <Link href="/">Home</Link>
    </nav>
  );
}
