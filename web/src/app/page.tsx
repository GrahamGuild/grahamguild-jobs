import Link from "next/link";
import { redirect } from "next/navigation";
import { createSupabaseServerClient } from "@/lib/supabase/server";

export default async function Home() {
  const supabase = await createSupabaseServerClient();

  const {
    data: { user },
  } = await supabase.auth.getUser();

  if (!user) {
    redirect("/login");
  }

  return (
    <main style={{ maxWidth: 760, margin: "40px auto", padding: 16 }}>
      <h1 style={{ fontSize: 28, marginBottom: 12 }}>GrahamGuild</h1>
      <p style={{ marginBottom: 24 }}>Private dashboard</p>

      <div style={{ display: "flex", gap: 12 }}>
        <Link href="/jobs">Greg’s Job Listings</Link>
        <Link href="/saved">Saved for Later</Link>
      </div>
    </main>
  );
}
