"use client";

import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";

type Job = {
  source?: string;
  sourceId?: string | number;
  title?: string;
  organization?: string;
  location?: string;
  url?: string;
};

type InboxFile = {
  jobs: Job[];
};

type DecisionPayload = {
  source: string;
  sourceId: string; // always string when sent
  decision: string;

  // optional metadata
  title?: string | null;
  organization?: string | null;
  location?: string | null;
  url?: string | null;
};

type Counts = {
  inbox?: number;
  saved?: number;
  applied?: number;
};

type ToastState =
  | {
      open: true;
      message: string;
      undoPayload: DecisionPayload; // decision: "inbox"
      restoreJob: Job;
      restoreIndex: number;
    }
  | { open: false };

export default function JobsPage() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [busyKey, setBusyKey] = useState<string | null>(null);
  const [counts, setCounts] = useState<Counts>({});
  const [toast, setToast] = useState<ToastState>({ open: false });
  const toastTimer = useRef<number | null>(null);

  const inboxCount = jobs.length;

  function jobKey(job: Job) {
    return `${job.source ?? ""}:${String(job.sourceId ?? "")}`;
  }

  const navCountsLabel = useMemo(() => {
    const saved = counts.saved ?? 0;
    const applied = counts.applied ?? 0;
    return { saved, applied };
  }, [counts]);

  function clearToastTimer() {
    if (toastTimer.current) {
      window.clearTimeout(toastTimer.current);
      toastTimer.current = null;
    }
  }

  function showUndoToast(args: {
    message: string;
    undoPayload: DecisionPayload;
    restoreJob: Job;
    restoreIndex: number;
  }) {
    clearToastTimer();
    setToast({ open: true, ...args });
    toastTimer.current = window.setTimeout(() => {
      setToast({ open: false });
      toastTimer.current = null;
    }, 7000);
  }

  async function postDecision(payload: DecisionPayload) {
    const res = await fetch("/api/decision", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    let bodyText = "";
    try {
      bodyText = await res.text();
    } catch {
      /* ignore */
    }

    return { ok: res.ok, status: res.status, bodyText };
  }

  async function refreshCounts() {
    // ✅ FIX: correct endpoint is /api/nav-counts
    try {
      const res = await fetch("/api/nav-counts", { cache: "no-store" });
      if (!res.ok) return;
      const data = (await res.json()) as Counts;
      setCounts({
        inbox: data.inbox,
        saved: data.saved,
        applied: data.applied,
      });
    } catch {
      /* ignore */
    }
  }

  async function loadJobs() {
    const res = await fetch("/api/inbox", { cache: "no-store" });
    if (!res.ok) {
      console.error("Failed to load /api/inbox:", res.status);
      return;
    }
    const data: InboxFile = await res.json();
    const list = Array.isArray(data.jobs) ? data.jobs : [];
    setJobs(list);
  }

  useEffect(() => {
    loadJobs();
    refreshCounts();
    return () => clearToastTimer();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function decide(job: Job, decision: "saved" | "ignored" | "applied") {
    const source = job.source ?? "";
    const sourceId = String(job.sourceId ?? "");
    if (!source || !sourceId) return;

    const key = `${source}:${sourceId}`;
    setBusyKey(key);

    const restoreIndex = jobs.findIndex(
      (j) =>
        (j.source ?? "") === source && String(j.sourceId ?? "") === sourceId,
    );
    const restoreJob: Job = job;

    try {
      let attempt = await postDecision({
        source,
        sourceId,
        decision,
        title: job.title ?? null,
        organization: job.organization ?? null,
        location: job.location ?? null,
        url: job.url ?? null,
      });

      if (!attempt.ok && decision === "ignored") {
        attempt = await postDecision({ source, sourceId, decision: "ignore" });
      }
      if (!attempt.ok && decision === "saved") {
        attempt = await postDecision({ source, sourceId, decision: "save" });
      }

      if (!attempt.ok) {
        console.error(
          "Decision API failed:",
          { source, sourceId, decision },
          "status:",
          attempt.status,
          "body:",
          attempt.bodyText,
        );
        alert(
          `Failed to save decision (${decision}).\nStatus: ${attempt.status}\n${attempt.bodyText || ""}`,
        );
        return;
      }

      setJobs((prev) =>
        prev.filter(
          (j) => !(j.source === source && String(j.sourceId) === sourceId),
        ),
      );

      setCounts((c) => ({
        ...c,
        saved:
          decision === "saved" ? (c.saved ?? 0) + 1 : (c.saved ?? undefined),
        applied:
          decision === "applied"
            ? (c.applied ?? 0) + 1
            : (c.applied ?? undefined),
      }));

      showUndoToast({
        message:
          decision === "saved"
            ? "Saved."
            : decision === "applied"
              ? "Marked as applied."
              : "Ignored.",
        undoPayload: {
          source,
          sourceId,
          decision: "inbox",
          title: job.title ?? null,
          organization: job.organization ?? null,
          location: job.location ?? null,
          url: job.url ?? null,
        },
        restoreJob,
        restoreIndex: restoreIndex >= 0 ? restoreIndex : 0,
      });

      refreshCounts();
    } finally {
      setBusyKey(null);
    }
  }

  async function undoLast() {
    if (!toast.open) return;

    const { undoPayload, restoreJob, restoreIndex } = toast;

    setToast({ open: false });
    clearToastTimer();

    const attempt = await postDecision(undoPayload);

    if (!attempt.ok) {
      alert(
        `Undo failed.\nStatus: ${attempt.status}\n${attempt.bodyText || ""}`,
      );
      await loadJobs();
      await refreshCounts();
      return;
    }

    setJobs((prev) => {
      const key = jobKey(restoreJob);
      if (prev.some((j) => jobKey(j) === key)) return prev;

      const next = [...prev];
      const idx = Math.max(0, Math.min(restoreIndex, next.length));
      next.splice(idx, 0, restoreJob);
      return next;
    });

    await refreshCounts();
  }

  return (
    <main style={{ maxWidth: 900, margin: "40px auto", padding: 20 }}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          gap: 12,
          alignItems: "baseline",
          marginBottom: 10,
        }}
      >
        <div>
          <h1 style={{ fontSize: 28, margin: 0 }}>Inbox</h1>
          <div style={{ opacity: 0.75, marginTop: 6 }}>
            Jobs: <strong>{inboxCount}</strong>
          </div>
        </div>

        <nav style={{ display: "flex", gap: 14, alignItems: "center" }}>
          <Link href="/">Home</Link>
          <Link href="/jobs">Inbox ({inboxCount})</Link>
          <Link href="/saved">Saved ({navCountsLabel.saved})</Link>
          <Link href="/applied">Applied ({navCountsLabel.applied})</Link>
        </nav>
      </div>

      {jobs.length === 0 ? (
        <p style={{ marginTop: 20, opacity: 0.8 }}>No inbox jobs right now.</p>
      ) : (
        <div style={{ display: "grid", gap: 14, marginTop: 18 }}>
          {jobs.map((job, idx) => {
            const key = jobKey(job);
            const disabled = busyKey === key;

            return (
              <div
                key={key || idx}
                style={{
                  border: "1px solid #ddd",
                  padding: 16,
                  borderRadius: 8,
                  opacity: disabled ? 0.6 : 1,
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
                  <div style={{ minWidth: 0 }}>
                    <div style={{ fontSize: 18, fontWeight: 600 }}>
                      {job.title ?? "(no title)"}
                    </div>
                    <div style={{ opacity: 0.85, marginTop: 4 }}>
                      {job.organization ?? "(no org)"}{" "}
                      {job.location ? `• ${job.location}` : ""}
                    </div>

                    {job.url ? (
                      <div style={{ marginTop: 10 }}>
                        <a href={job.url} target="_blank" rel="noreferrer">
                          View job
                        </a>
                      </div>
                    ) : null}
                  </div>

                  <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                    <button
                      disabled={disabled}
                      onClick={() => decide(job, "saved")}
                    >
                      Save
                    </button>

                    <button
                      disabled={disabled}
                      onClick={() => decide(job, "ignored")}
                    >
                      Ignore
                    </button>

                    <button
                      disabled={disabled}
                      onClick={() => decide(job, "applied")}
                    >
                      Mark as applied
                    </button>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {toast.open ? (
        <div
          role="status"
          aria-live="polite"
          style={{
            position: "fixed",
            left: 16,
            right: 16,
            bottom: 16,
            maxWidth: 900,
            margin: "0 auto",
            border: "1px solid #ddd",
            background: "white",
            borderRadius: 10,
            padding: 12,
            boxShadow: "0 10px 25px rgba(0,0,0,0.08)",
            display: "flex",
            justifyContent: "space-between",
            gap: 12,
            alignItems: "center",
          }}
        >
          <div style={{ display: "flex", gap: 10, alignItems: "baseline" }}>
            <strong>{toast.message}</strong>
            <span style={{ opacity: 0.75 }}>Undo?</span>
          </div>

          <div style={{ display: "flex", gap: 10 }}>
            <button onClick={undoLast}>Undo</button>
            <button onClick={() => setToast({ open: false })}>Dismiss</button>
          </div>
        </div>
      ) : null}
    </main>
  );
}
