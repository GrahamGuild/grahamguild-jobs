import { cookies } from "next/headers";
import { createServerClient } from "@supabase/ssr";

export async function createSupabaseServerClient() {
  const cookieStore = await cookies(); // Next 16: cookies() is async

  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const key = process.env.NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY;

  if (!url || !key) {
    throw new Error(
      "Missing NEXT_PUBLIC_SUPABASE_URL or NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY in web/.env.local",
    );
  }

  return createServerClient(url, key, {
    cookies: {
      getAll() {
        return cookieStore.getAll();
      },
      // Server Components are read-only; middleware handles syncing.
      setAll() {
        /* no-op */
      },
    },
  });
}

// Back-compat aliases (so older imports keep working)
export const createClient = createSupabaseServerClient;
export const createSupabaseClient = createSupabaseServerClient;
