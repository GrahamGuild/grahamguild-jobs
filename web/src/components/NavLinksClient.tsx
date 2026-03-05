"use client";

import Link from "next/link";
import { useEffect, useState } from "react";

type Counts = { saved: number; applied: number };

export default function NavLinksClient() {
  const [counts, setCounts] = useState<Counts>({ saved: 0, applied: 0 });

  useEffect(() => {
    let alive = true;
    (async () => {
      const res = await fetch("/api/nav-counts", { cache: "no-store" });
      if (!res.ok) return;
      const json = (await res.json()) as Counts;
      if (alive) setCounts(json);
    })();
    return () => {
      alive = false;
    };
  }, []);

  return (
    <nav style={{ display: "flex", gap: 14, flexWrap: "wrap" }}>
      <Link href="/jobs">Inbox</Link>
      <Link href="/saved">Saved ({counts.saved})</Link>
      <Link href="/applied">Applied ({counts.applied})</Link>
      <Link href="/">Home</Link>
    </nav>
  );
}
