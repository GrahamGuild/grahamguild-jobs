import { redirect } from "next/navigation";
import { createSupabaseServerClient } from "@/lib/supabase/server";
import NavLinksServer from "@/components/NavLinksServer";

export default async function Home() {
  const supabase = await createSupabaseServerClient();

  const { data: claimsData, error: claimsError } =
    await supabase.auth.getClaims();
  if (claimsError || !claimsData?.claims) redirect("/login");

  return (
    <main style={{ maxWidth: 900, margin: "60px auto", padding: 20 }}>
      <h1 style={{ fontSize: 34, marginBottom: 20 }}>GrahamGuild</h1>

      {/* Feature card */}
      <div
        style={{
          background: "#f6f7f8",
          border: "1px solid #e5e5e5",
          borderRadius: 10,
          padding: 22,
        }}
      >
        <h2
          style={{
            fontSize: 22,
            fontWeight: 600,
            marginTop: 0,
            marginBottom: 10,
          }}
        >
          Greg&apos;s Job Search
        </h2>

        <div style={{ marginTop: 10 }}>
          <NavLinksServer />
        </div>

        <p style={{ marginTop: 16, opacity: 0.75 }}>
          Use Inbox to triage new roles, then review Saved and Applied as you
          go.
        </p>
      </div>
    </main>
  );
}
