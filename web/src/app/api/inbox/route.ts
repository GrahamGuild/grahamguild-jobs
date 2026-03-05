import { NextResponse } from "next/server";
import path from "path";
import fs from "fs/promises";
import { createSupabaseServerClient } from "@/lib/supabase/server";

type Job = {
  source?: string;
  sourceId?: string;
  [k: string]: unknown;
};

type InboxFile = {
  jobs: Job[];
};

export async function GET() {
  // Must be logged in (we’ll scope decisions to your account)
  const supabase = await createSupabaseServerClient();
  const { data: userData, error: userErr } = await supabase.auth.getUser();
  if (userErr || !userData?.user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  // Load the static inbox snapshot from /public
  const inboxPath = path.join(process.cwd(), "public", "inbox_latest.json");
  const raw = await fs.readFile(inboxPath, "utf-8");
  const inbox: InboxFile = JSON.parse(raw);

  // Pull decisions (job_key is UNIQUE, so this list is clean)
  const { data: decisions, error: decErr } = await supabase
    .from("job_decisions")
    .select("job_key");

  if (decErr) {
    return NextResponse.json({ error: decErr.message }, { status: 500 });
  }

  const decided = new Set((decisions ?? []).map((d) => d.job_key));

  const filtered = (inbox.jobs ?? []).filter((job) => {
    const key = `${job.source}:${job.sourceId}`;
    return job.source && job.sourceId && !decided.has(key);
  });

  return NextResponse.json({ jobs: filtered });
}
