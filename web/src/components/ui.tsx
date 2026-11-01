import { useEffect, useState } from "react";
import type { JobStatus } from "../types";

/** Plain names for statuses, in the order a job moves through them. */
export const STATUS_ORDER: JobStatus[] = [
  "blocked",
  "scheduled",
  "queued",
  "running",
  "succeeded",
  "dead",
  "upstream_failed",
  "cancelled",
];

export const STATUS_LABEL: Record<JobStatus, string> = {
  blocked: "Blocked",
  scheduled: "Scheduled",
  queued: "Waiting",
  running: "Running",
  succeeded: "Succeeded",
  dead: "Dead",
  upstream_failed: "Upstream failed",
  cancelled: "Cancelled",
};

export const STATUS_HELP: Record<JobStatus, string> = {
  blocked: "Workflow jobs waiting for their dependencies",
  scheduled: "Waiting for a retry or a scheduled start time",
  queued: "Ready, waiting for a free worker",
  running: "Being run by a worker right now",
  succeeded: "Finished and recorded exactly once",
  dead: "Out of attempts or failed permanently",
  upstream_failed: "Never ran: a dependency died or was cancelled",
  cancelled: "Stopped before it ran",
};

export function Lamp({
  status,
  live = false,
  size,
  off = false,
}: {
  status: JobStatus;
  live?: boolean;
  size?: "lg";
  off?: boolean;
}) {
  return (
    <span
      aria-hidden
      className={["lamp", size, live && "live", off && "off"].filter(Boolean).join(" ")}
      style={{ ["--c" as string]: `var(--s-${status})` }}
    />
  );
}

export function Status({ status }: { status: JobStatus }) {
  return (
    <span className="status">
      <Lamp status={status} live={status === "running"} />
      {STATUS_LABEL[status]}
    </span>
  );
}

/** Re-renders every `ms` so durations on screen keep counting between polls. */
export function useNow(ms = 1000): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), ms);
    return () => clearInterval(id);
  }, [ms]);
  return now;
}

export function ErrorNotice({ error }: { error: unknown }) {
  if (!error) return null;
  const message = error instanceof Error ? error.message : String(error);
  return (
    <p className="notice error" role="alert">
      {message}
    </p>
  );
}

export function Json({ value, error = false }: { value: unknown; error?: boolean }) {
  return <pre className={error ? "code error" : "code"}>{typeof value === "string" ? value : JSON.stringify(value, null, 2)}</pre>;
}
