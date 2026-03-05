import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";

type Decision = "ignored" | "saved" | "applied";

const normalizeDecision = (raw: unknown): Decision | null => {
  const v = String(raw || "")
    .toLowerCase()
    .trim();
  if (v === "ignored") return "ignored";
  if (v === "saved") return "saved";
  if (v === "applied") return "applied";
  return null;
};

export async function GET(req: Request) {
  try {
    const supabase = await createSupabaseServerClient();

    // Require auth (so nobody can hit this publicly)
    const { data: userData } = await supabase.auth.getUser();
    if (!userData?.user) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const url = new URL(req.url);
    const decision = normalizeDecision(url.searchParams.get("decision"));

    let q = supabase
      .from("job_decisions")
      .select(
        "id, job_key, decision, note, decided_at, updated_at, source, source_id, title, organization, location, url",
      )
      .order("updated_at", { ascending: false })
      .limit(500);

    if (decision) {
      q = q.eq("decision", decision);
    }

    const { data, error } = await q;

    if (error) {
      return NextResponse.json(
        { error: error.message, details: error },
        { status: 500 },
      );
    }

    return NextResponse.json({ rows: data ?? [] });
  } catch (err) {
    console.error("GET /api/decisions error:", err);
    return NextResponse.json({ error: "Server error" }, { status: 500 });
  }
}
