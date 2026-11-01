/** "4s", "2m 05s", "1h 12m": compact durations for a board that updates every second. */
export function duration(ms: number): string {
  const s = Math.max(0, Math.floor(ms / 1000));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${String(s % 60).padStart(2, "0")}s`;
  const h = Math.floor(m / 60);
  if (h < 48) return `${h}h ${String(m % 60).padStart(2, "0")}m`;
  return `${Math.floor(h / 24)}d`;
}

/** "just now", "12s ago", "3m ago", or a date for anything older than a day. */
export function ago(iso: string | null, now: number = Date.now()): string {
  if (!iso) return "—";
  const ms = now - new Date(iso).getTime();
  // Within 2s either way is "now": clocks and poll timing jitter by a few milliseconds.
  if (Math.abs(ms) < 2000) return "just now";
  if (ms < 0) return `in ${duration(-ms)}`;
  if (ms < 86_400_000) return `${duration(ms).split(" ")[0]} ago`;
  return new Date(iso).toLocaleDateString(undefined, { day: "numeric", month: "short" });
}

/** The first 8 characters of an id: enough to tell jobs apart on screen. */
export const shortId = (id: string) => id.slice(0, 8);
