// web/src/app/api/decision/route.ts
import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";

type NormalizedDecision = "ignored" | "saved" | "applied" | "inbox";

const normalizeDecision = (raw: unknown): NormalizedDecision | null => {
  const v = String(raw ?? "")
    .toLowerCase()
    .trim();

  if (v === "inbox" || v === "back" || v === "back_to_inbox") return "inbox";
  if (v === "ignored" || v === "ignore") return "ignored";
  if (v === "saved" || v === "save" || v === "save_for_later") return "saved";
  if (v === "applied" || v === "apply") return "applied";

  return null;
};

export async function POST(req: Request) {
  try {
    const body = await req.json().catch(() => ({}));

    const source = String(body?.source || "").trim();
    const sourceId = String(body?.sourceId || "").trim();
    const decision = normalizeDecision(body?.decision);
    const note = body?.note != null ? String(body.note) : null;

    // Optional metadata
    const title = body?.title != null ? String(body.title) : null;
    const organization =
      body?.organization != null ? String(body.organization) : null;
    const location = body?.location != null ? String(body.location) : null;
    const url = body?.url != null ? String(body.url) : null;

    if (!source || !sourceId) {
      return NextResponse.json(
        { error: "Missing source or sourceId" },
        { status: 400 },
      );
    }

    if (!decision) {
      return NextResponse.json(
        {
          error:
            "Invalid decision. Allowed: ignored | saved | applied | inbox (or aliases ignore/save/apply).",
          received: body?.decision,
        },
        { status: 400 },
      );
    }

    const supabase = await createSupabaseServerClient();
    const job_key = `${source}:${sourceId}`;

    // INBOX = remove decision row
    if (decision === "inbox") {
      // Delete by BOTH strategies:
      //  - job_key match
      //  - (source, source_id) match
      // and return what got deleted so we can verify it's actually happening.
      const { data, error, count } = await supabase
        .from("job_decisions")
        .delete({ count: "exact" })
        .or(
          `job_key.eq.${job_key},and(source.eq.${source},source_id.eq.${sourceId})`,
        )
        .select("job_key, source, source_id, decision");

      if (error) {
        console.error("Decision delete failed:", {
          job_key,
          source,
          sourceId,
          error,
        });
        return NextResponse.json(
          { error: error.message, details: error },
          { status: 500 },
        );
      }

      // If nothing deleted, surface it explicitly (this is the “facts not guessing” part).
      if (!count || count === 0) {
        console.warn("Decision delete matched 0 rows:", {
          job_key,
          source,
          sourceId,
        });
        return NextResponse.json(
          {
            error: "Move-to-inbox deleted 0 rows (no match).",
            attempted: { job_key, source, sourceId },
          },
          { status: 409 },
        );
      }

      return NextResponse.json({
        ok: true,
        decision: "inbox",
        deletedCount: count,
        deletedRows: data ?? [],
      });
    }

    // UPSERT decision
    const { error } = await supabase.from("job_decisions").upsert(
      {
        job_key,

        source,
        source_id: sourceId,
        title,
        organization,
        location,
        url,

        decision, // ignored|saved|applied
        note,
        updated_at: new Date().toISOString(),
      },
      { onConflict: "job_key" },
    );

    if (error) {
      console.error("Decision upsert failed:", { job_key, decision, error });
      return NextResponse.json(
        { error: error.message, details: error },
        { status: 500 },
      );
    }

    return NextResponse.json({ ok: true, job_key, decision });
  } catch (err) {
    console.error("Decision route error:", err);
    return NextResponse.json({ error: "Server error" }, { status: 500 });
  }
}
