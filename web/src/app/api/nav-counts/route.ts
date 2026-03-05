import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";
import path from "node:path";
import fs from "node:fs/promises";

export const dynamic = "force-dynamic";

type InboxJob = { source?: string; sourceId?: string | number };
type InboxFile = { jobs?: InboxJob[] };

export async function GET() {
  const supabase = await createSupabaseServerClient();

  const { data: claimsData, error: claimsError } =
    await supabase.auth.getClaims();
  if (claimsError || !claimsData?.claims) {
    return NextResponse.json({ error: "unauthorized" }, { status: 401 });
  }

  // 1) Load decisions
  const { data: decisionRows, error: decisionErr } = await supabase
    .from("job_decisions")
    .select("decision, job_key, source, source_id");

  if (decisionErr) {
    return NextResponse.json({ error: decisionErr.message }, { status: 500 });
  }

  let saved = 0;
  let applied = 0;

  // Any decision row means "not in inbox"
  const decidedKeys = new Set<string>();

  for (const row of decisionRows ?? []) {
    if (row.decision === "saved") saved += 1;
    if (row.decision === "applied") applied += 1;

    const jobKey =
      (row.job_key as string | null) ??
      `${row.source ?? ""}:${row.source_id ?? ""}`;

    if (jobKey && jobKey.includes(":")) decidedKeys.add(jobKey);
  }

  // 2) Load inbox_latest.json and compute inbox count as "jobs not decided"
  let inboxTotal = 0;
  try {
    const inboxPath = path.join(process.cwd(), "public", "inbox_latest.json");
    const raw = await fs.readFile(inboxPath, "utf-8");
    const parsed = JSON.parse(raw) as InboxFile;

    const jobs = Array.isArray(parsed.jobs) ? parsed.jobs : [];
    for (const j of jobs) {
      const key = `${j.source ?? ""}:${String(j.sourceId ?? "")}`;
      if (!decidedKeys.has(key)) inboxTotal += 1;
    }
  } catch {
    // If file missing, we still return saved/applied; inbox will be 0.
    inboxTotal = 0;
  }

  return NextResponse.json({ inbox: inboxTotal, saved, applied });
}
