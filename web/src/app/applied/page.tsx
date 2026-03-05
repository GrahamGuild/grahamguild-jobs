// web/src/app/applied/page.tsx
export const dynamic = "force-dynamic";
import Link from "next/link";
import { redirect } from "next/navigation";
import { createSupabaseServerClient } from "@/lib/supabase/server";
import JobActions from "@/components/JobActions";

export default async function AppliedPage() {
  const supabase = await createSupabaseServerClient();

  // Protect the page
  const { data: claimsData, error: claimsError } =
    await supabase.auth.getClaims();
  if (claimsError || !claimsData?.claims) redirect("/login");

  const { data: rows, error } = await supabase
    .from("job_decisions")
    .select(
      "job_key, source, source_id, title, organization, location, url, note, updated_at",
    )
    .eq("decision", "applied")
    .order("updated_at", { ascending: false });

  if (error) {
    return (
      <main style={{ maxWidth: 900, margin: "40px auto", padding: 16 }}>
        <h1 style={{ fontSize: 28, marginBottom: 12 }}>Applied</h1>
        <p style={{ color: "crimson" }}>Error: {error.message}</p>
        <p>
          <Link href="/">Back</Link>
        </p>
      </main>
    );
  }

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
        <h1 style={{ fontSize: 28, marginBottom: 12 }}>Applied</h1>

        <nav style={{ display: "flex", gap: 14 }}>
          <Link href="/">Home</Link>
          <Link href="/jobs">Inbox</Link>
          <Link href="/saved">Saved</Link>
          <Link href="/applied">Applied</Link>
        </nav>
      </div>

      <p style={{ marginBottom: 20, opacity: 0.8 }}>
        Applied: <strong>{rows?.length ?? 0}</strong>
      </p>

      {!rows?.length ? (
        <p>No applied jobs yet.</p>
      ) : (
        <div style={{ display: "grid", gap: 14 }}>
          {rows.map((r) => {
            const jobKey = r.job_key ?? `${r.source}:${r.source_id}`;

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
                        Applied: {new Date(r.updated_at).toLocaleString()}
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
                        View
                      </a>
                    ) : null}

                    {/* Allow moving back to inbox / saving again */}
                    {(() => {
                      const jobKey = String(r.job_key ?? "");
                      let source = String(r.source ?? "");
                      let sourceId = String(r.source_id ?? "");

                      // Backfill source/sourceId from job_key for older rows
                      if ((!source || !sourceId) && jobKey.includes(":")) {
                        const i = jobKey.indexOf(":");
                        source = jobKey.slice(0, i);
                        sourceId = jobKey.slice(i + 1);
                      }

                      return (
                        <JobActions
                          source={source}
                          sourceId={sourceId}
                          title={r.title ?? null}
                          organization={r.organization ?? null}
                          location={r.location ?? null}
                          url={r.url ?? null}
                          showSave={true}
                          showApplied={false}
                          showIgnore={true}
                          showBackToInbox={true}
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
