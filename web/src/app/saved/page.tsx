// web/src/app/saved/page.tsx
export const dynamic = "force-dynamic";
import Link from "next/link";
import { redirect } from "next/navigation";
import { createSupabaseServerClient } from "@/lib/supabase/server";
import JobActions from "@/components/JobActions";

type Row = {
  job_key: string;
  source: string | null;
  source_id: string | null;
  title: string | null;
  organization: string | null;
  location: string | null;
  url: string | null;
  note: string | null;
  updated_at: string | null;
};

export default async function SavedPage() {
  const supabase = await createSupabaseServerClient();

  // Protect the page
  const { data: claimsData, error: claimsError } =
    await supabase.auth.getClaims();
  if (claimsError || !claimsData?.claims) redirect("/login");

  const { data, error } = await supabase
    .from("job_decisions")
    .select(
      "job_key, source, source_id, title, organization, location, url, note, updated_at",
    )
    .eq("decision", "saved")
    .order("updated_at", { ascending: false });

  if (error) {
    return (
      <main style={{ maxWidth: 900, margin: "40px auto", padding: 16 }}>
        <h1 style={{ fontSize: 28, marginBottom: 12 }}>Saved for Later</h1>
        <p style={{ color: "crimson" }}>Error: {error.message}</p>
        <p>
          <Link href="/">Back</Link>
        </p>
      </main>
    );
  }

  const rows = (data ?? []) as Row[];

  return (
    <main style={{ maxWidth: 900, margin: "40px auto", padding: 16 }}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          gap: 12,
          alignItems: "baseline",
        }}
      >
        <h1 style={{ fontSize: 28, marginBottom: 12 }}>Saved for Later</h1>

        <nav style={{ display: "flex", gap: 14 }}>
          <Link href="/">Home</Link>
          <Link href="/jobs">Inbox</Link>
          <Link href="/saved">Saved</Link>
          <Link href="/applied">Applied</Link>
        </nav>
      </div>

      <p style={{ marginBottom: 20, opacity: 0.8 }}>
        Saved: <strong>{rows.length}</strong>
      </p>

      {!rows.length ? (
        <p>No saved jobs yet.</p>
      ) : (
        <div style={{ display: "grid", gap: 14 }}>
          {rows.map((r) => {
            const jobKey = r.job_key || `${r.source}:${r.source_id}`;

            return (
              <div
                key={jobKey}
                style={{
                  border: "1px solid #e5e5e5",
                  background: "#fafafa",
                  borderRadius: 10,
                  padding: 18,
                }}
              >
                <div
                  style={{
                    display: "flex",
                    justifyContent: "space-between",
                    gap: 12,
                    alignItems: "flex-start",
                  }}
                >
                  <div>
                    <div style={{ fontSize: 18, fontWeight: 600 }}>
                      {r.title ?? "(no title)"}
                    </div>
                    <div style={{ opacity: 0.85 }}>
                      {r.organization ?? "(no org)"}{" "}
                      {r.location ? `• ${r.location}` : ""}
                    </div>

                    {r.updated_at ? (
                      <div style={{ fontSize: 12, opacity: 0.7, marginTop: 6 }}>
                        Saved: {new Date(r.updated_at).toLocaleString()}
                      </div>
                    ) : null}

                    {r.note ? (
                      <div style={{ fontSize: 12, opacity: 0.8, marginTop: 6 }}>
                        Note: {r.note}
                      </div>
                    ) : null}
                  </div>

                  <div
                    style={{
                      display: "flex",
                      gap: 10,
                      alignItems: "center",
                      justifyContent: "flex-end",
                      flexWrap: "wrap",
                    }}
                  >
                    {r.url ? (
                      <a href={r.url} target="_blank" rel="noreferrer">
                        View job
                      </a>
                    ) : null}

                    {/* No "Apply" link — only decisions */}
                    {(() => {
                      const jobKey = String(r.job_key ?? "");
                      const colon = jobKey.indexOf(":");
                      const fallbackSource =
                        colon > 0 ? jobKey.slice(0, colon) : "";
                      const fallbackSourceId =
                        colon > 0 ? jobKey.slice(colon + 1) : "";

                      const src =
                        String(r.source ?? "").trim() || fallbackSource;
                      const sid =
                        String(r.source_id ?? "").trim() || fallbackSourceId;

                      return (
                        <JobActions
                          source={src}
                          sourceId={sid}
                          title={r.title ?? null}
                          organization={r.organization ?? null}
                          location={r.location ?? null}
                          url={r.url ?? null}
                          showSave={false}
                          showApplied
                          showIgnore
                          showBackToInbox
                        />
                      );
                    })()}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </main>
  );
}
