"use client";

import { useTransition } from "react";
import { useRouter } from "next/navigation";

type Props = {
  source: string;
  sourceId: string;

  // Optional job metadata (so DB rows stay complete no matter which page triggers the action)
  title?: string | null;
  organization?: string | null;
  location?: string | null;
  url?: string | null;

  // which buttons to show:
  showSave?: boolean;
  showIgnore?: boolean;
  showApplied?: boolean;
  showBackToInbox?: boolean;
};

type Decision = "saved" | "ignored" | "applied" | "inbox";

export default function JobActions({
  source,
  sourceId,
  title = null,
  organization = null,
  location = null,
  url = null,
  showSave,
  showIgnore,
  showApplied,
  showBackToInbox,
}: Props) {
  const router = useRouter();
  const [pending, startTransition] = useTransition();

  async function send(decision: Decision) {
    const res = await fetch("/api/decision", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        source,
        sourceId,
        decision,
        title,
        organization,
        location,
        url,
      }),
    });

    if (!res.ok) {
      const text = await res.text();
      alert(`Decision failed (${decision}). Status: ${res.status}\n${text}`);
      return;
    }

    startTransition(() => {
      // Re-render server components (Saved/Applied are server pages)
      router.refresh();

      // Do NOT navigate away. Keep user on the current page.
      // (Removed router.push("/jobs"))
    });
  }

  return (
    <div style={{ display: "flex", gap: 10, marginTop: 10, flexWrap: "wrap" }}>
      {showSave ? (
        <button disabled={pending} onClick={() => send("saved")}>
          Save for later
        </button>
      ) : null}

      {showApplied ? (
        <button disabled={pending} onClick={() => send("applied")}>
          Mark as applied
        </button>
      ) : null}

      {showIgnore ? (
        <button disabled={pending} onClick={() => send("ignored")}>
          Ignore
        </button>
      ) : null}

      {showBackToInbox ? (
        <button disabled={pending} onClick={() => send("inbox")}>
          Move back to inbox
        </button>
      ) : null}
    </div>
  );
}
