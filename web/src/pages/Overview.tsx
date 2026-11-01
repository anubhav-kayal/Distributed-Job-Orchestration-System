import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api } from "../api";
import { ErrorNotice, Lamp, STATUS_HELP, STATUS_LABEL, STATUS_ORDER, useNow } from "../components/ui";
import { ago, duration, shortId } from "../lib/time";
import type { Priority, RunningJob, Stats } from "../types";

export function Overview() {
  const { data, error, dataUpdatedAt } = useQuery({
    queryKey: ["stats"],
    queryFn: api.stats,
    refetchInterval: 2000,
  });
  const now = useNow();

  return (
    <>
      <header className="page-head">
        <div>
          <h1>Dispatch board</h1>
          <p>Every job for this tenant, by where it is right now. Updates every two seconds.</p>
        </div>
        {data && <span className="faint">Updated {ago(new Date(dataUpdatedAt).toISOString(), now)}</span>}
      </header>
      <ErrorNotice error={error} />
      {!data && !error && <p className="muted" role="status">Connecting to the API…</p>}
      {data && (
        <>
          <Gantry stats={data} />
          <div className="board-layout">
            <section aria-labelledby="running-head">
              <div className="section-head">
                <h2 id="running-head">Running now</h2>
                <span className="note">Each strip shows its worker, attempt and fencing token</span>
              </div>
              <Bay jobs={data.running} now={now} />
            </section>
            <div className="side">
              <Waiting queued={data.queued_by_priority} />
              <Throughput stats={data} />
              <Failures stats={data} now={now} />
            </div>
          </div>
        </>
      )}
    </>
  );
}

function Gantry({ stats }: { stats: Stats }) {
  return (
    <nav className="gantry" aria-label="Jobs by status">
      {STATUS_ORDER.map((s) => {
        const n = stats.counts[s];
        return (
          <Link key={s} to={`/jobs?status=${s}`} className="signal" title={STATUS_HELP[s]}>
            <span className={n ? "figure" : "figure zero"} title={n.toLocaleString()}>
              {compact(n)}
            </span>
            <span className="label">
              <Lamp status={s} off={!n} live={s === "running" && n > 0} />
              {STATUS_LABEL[s]}
            </span>
          </Link>
        );
      })}
    </nav>
  );
}

/** 9,999 stays exact; beyond that, 42.2k and 1.3M keep the gantry from overflowing. */
const compact = (n: number) =>
  n < 10_000 ? n.toLocaleString() : new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 }).format(n);

const PRIORITY_MARK: Record<Priority, string> = { high: "H", normal: "N", low: "L" };

function Bay({ jobs, now }: { jobs: RunningJob[]; now: number }) {
  if (!jobs.length) {
    return (
      <div className="bay">
        <div className="bay-empty">
          <h3>No jobs running</h3>
          <p>Workers are idle. Submit a job and it appears here within a couple of seconds.</p>
          <Link className="btn" to="/jobs?new=1">
            Submit a job
          </Link>
        </div>
      </div>
    );
  }
  return (
    <div className="bay" role="list">
      {jobs.map((j) => (
        <Link key={j.id} to={`/jobs/${j.id}`} className="strip" role="listitem">
          <span className={`holder ${j.priority}`} title={`${j.priority} priority`}>
            {PRIORITY_MARK[j.priority]}
          </span>
          <span className="what">
            {j.workflow_key ? `${j.workflow_key}` : j.type}
            <small>
              {j.workflow_key ? `${j.type}, in a workflow ` : ""}
              <span className="mono">{shortId(j.id)}</span>
            </small>
          </span>
          <span className="who">
            <span>{j.worker ?? "unknown worker"}</span>
            <small>worker</small>
          </span>
          <span className="token">
            <span className="num">
              {j.attempt}
              <span className="faint"> / </span>
              {j.lease_token}
            </span>
            <small>attempt / token</small>
          </span>
          <span className="clock">
            <span className="num">{j.started_at ? duration(now - Date.parse(j.started_at)) : "—"}</span>
            <small>running</small>
          </span>
        </Link>
      ))}
    </div>
  );
}

function Waiting({ queued }: { queued: Record<Priority, number> }) {
  const max = Math.max(10, ...Object.values(queued));
  return (
    <section className="panel" aria-labelledby="waiting-head">
      <div className="section-head">
        <h3 id="waiting-head">Waiting for a worker</h3>
      </div>
      {(["high", "normal", "low"] as Priority[]).map((p) => (
        <div className="meter" key={p}>
          <span className="muted">{p[0].toUpperCase() + p.slice(1)}</span>
          <span className="track" aria-hidden>
            <span className="fill" style={{ width: `${(queued[p] / max) * 100}%`, display: "block" }} />
          </span>
          <span className="value">{queued[p]}</span>
        </div>
      ))}
    </section>
  );
}

function Throughput({ stats }: { stats: Stats }) {
  const buckets = stats.throughput;
  const peak = Math.max(1, ...buckets.map((b) => b.succeeded + b.dead));
  const total = buckets.reduce((a, b) => a + b.succeeded, 0);
  const dead = buckets.reduce((a, b) => a + b.dead, 0);
  return (
    <section className="panel" aria-labelledby="tp-head">
      <div className="section-head">
        <h3 id="tp-head">Finished in the last 5 minutes</h3>
        <span className="note">
          {total.toLocaleString()} succeeded{dead ? `, ${dead} dead` : ""}
        </span>
      </div>
      <div className="bars" role="img" aria-label={`${total} jobs succeeded and ${dead} died in the last 5 minutes`}>
        {buckets.map((b) => (
          <div className="col" key={b.at} title={`${new Date(b.at).toLocaleTimeString()}: ${b.succeeded} succeeded, ${b.dead} dead`}>
            <span className="ok" style={{ height: `${(b.succeeded / peak) * 100}%` }} />
            <span className="bad" style={{ height: `${(b.dead / peak) * 100}%` }} />
          </div>
        ))}
      </div>
      <div className="axis">
        <span>5 min ago</span>
        <span>now</span>
      </div>
    </section>
  );
}

function Failures({ stats, now }: { stats: Stats; now: number }) {
  return (
    <section className="panel" aria-labelledby="fail-head">
      <div className="section-head">
        <h3 id="fail-head">Latest dead jobs</h3>
        {stats.recent_failures.length > 0 && (
          <Link to="/dead" className="note">
            Open dead letters
          </Link>
        )}
      </div>
      {stats.recent_failures.length === 0 ? (
        <p className="muted">Nothing has died. Jobs that run out of attempts land here.</p>
      ) : (
        <ul className="failures">
          {stats.recent_failures.map((f) => (
            <li key={f.id}>
              <Link to={`/jobs/${f.id}`}>
                <span>
                  <strong>{f.type}</strong> <span className="faint">{ago(f.finished_at, now)}</span>
                </span>
                <span className="err">{f.last_error ?? "No error recorded"}</span>
              </Link>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
