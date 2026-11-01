import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams, useSearchParams } from "react-router-dom";
import { api } from "../api";
import { ErrorNotice, Json, Lamp, STATUS_LABEL, STATUS_ORDER, Status, useNow } from "../components/ui";
import { ago, duration, shortId } from "../lib/time";
import type { Job, JobStatus, JobType, Priority } from "../types";

export function Jobs() {
  const { id } = useParams();
  const [params, setParams] = useSearchParams();
  const status = (params.get("status") ?? "") as JobStatus | "";
  const [offset, setOffset] = useState(0);
  const [showForm, setShowForm] = useState(params.get("new") === "1");
  const navigate = useNavigate();
  const stats = useQuery({ queryKey: ["stats"], queryFn: api.stats, refetchInterval: 2000 });
  const jobs = useQuery({
    queryKey: ["jobs", status, offset],
    queryFn: () => api.jobs(status, offset),
    refetchInterval: 2000,
  });
  const now = useNow();

  useEffect(() => setOffset(0), [status]);

  const filter = (s: JobStatus | "") => {
    const next = new URLSearchParams(params);
    if (s) next.set("status", s);
    else next.delete("status");
    setParams(next);
  };

  return (
    <>
      <header className="page-head">
        <div>
          <h1>Jobs</h1>
          <p>Newest first. Select a job to see its attempts, result or error.</p>
        </div>
        <button className="btn" onClick={() => setShowForm((v) => !v)} aria-expanded={showForm}>
          {showForm ? "Close form" : "New job"}
        </button>
      </header>

      {showForm && (
        <NewJob
          onSubmitted={(job) => {
            setShowForm(false);
            navigate(`/jobs/${job.id}`);
          }}
        />
      )}

      <div className="segmented" role="group" aria-label="Filter by status">
        <button aria-pressed={!status} onClick={() => filter("")}>
          All
        </button>
        {STATUS_ORDER.map((s) => (
          <button key={s} aria-pressed={status === s} onClick={() => filter(s)}>
            <Lamp status={s} off={!stats.data?.counts[s]} />
            {STATUS_LABEL[s]}
            {stats.data && <span className="n">{stats.data.counts[s]}</span>}
          </button>
        ))}
      </div>

      <ErrorNotice error={jobs.error} />
      <div className={id ? "split" : "split closed"}>
        <div className="table-wrap">
          {jobs.data && jobs.data.items.length === 0 ? (
            <div className="empty">
              <h3>{status ? `No ${STATUS_LABEL[status].toLowerCase()} jobs` : "No jobs yet"}</h3>
              <p>{status ? "Pick another status, or show all jobs." : "Submit one to see it move across the board."}</p>
              {!status && (
                <button className="btn" onClick={() => setShowForm(true)}>
                  New job
                </button>
              )}
            </div>
          ) : (
            <table>
              <thead>
                <tr>
                  <th>Status</th>
                  <th>Type</th>
                  <th>Job</th>
                  <th>Priority</th>
                  <th>Attempts</th>
                  <th>Worker</th>
                  <th>Created</th>
                </tr>
              </thead>
              <tbody>
                {jobs.data?.items.map((j) => (
                  <tr
                    key={j.id}
                    aria-selected={j.id === id}
                    onClick={() => navigate(`/jobs/${j.id}${params.size ? `?${params}` : ""}`)}
                  >
                    <td>
                      <Status status={j.status} />
                    </td>
                    <td>
                      <strong>{j.type}</strong>
                    </td>
                    <td>
                      <Link to={`/jobs/${j.id}`} className="mono" onClick={(e) => e.stopPropagation()}>
                        {shortId(j.id)}
                      </Link>
                    </td>
                    <td>{j.priority}</td>
                    <td className="num">
                      {j.attempts} of {j.max_attempts}
                    </td>
                    <td className="muted">{j.locked_by ?? "—"}</td>
                    <td className="muted">{ago(j.created_at, now)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          {jobs.data && (offset > 0 || jobs.data.items.length === 50) && (
            <div className="pager">
              <span>
                Showing {offset + 1}–{offset + jobs.data.items.length}
              </span>
              <span style={{ display: "flex", gap: "0.4rem" }}>
                <button className="btn small quiet" disabled={offset === 0} onClick={() => setOffset(offset - 50)}>
                  Newer
                </button>
                <button
                  className="btn small quiet"
                  disabled={jobs.data.items.length < 50}
                  onClick={() => setOffset(offset + 50)}
                >
                  Older
                </button>
              </span>
            </div>
          )}
        </div>
        {id && <JobDetail id={id} onClose={() => navigate(`/jobs${params.size ? `?${params}` : ""}`)} />}
      </div>
    </>
  );
}

export function JobDetail({ id, onClose }: { id: string; onClose: () => void }) {
  const qc = useQueryClient();
  const job = useQuery({ queryKey: ["job", id], queryFn: () => api.job(id), refetchInterval: 1500 });
  const now = useNow();
  const refresh = () => qc.invalidateQueries();
  const cancel = useMutation({ mutationFn: () => api.cancelJob(id), onSuccess: refresh });
  const retry = useMutation({ mutationFn: () => api.retryJob(id), onSuccess: refresh });

  if (job.error) {
    return (
      <aside className="detail">
        <ErrorNotice error={job.error} />
        <button className="btn quiet" onClick={onClose}>
          Close
        </button>
      </aside>
    );
  }
  const j = job.data;
  if (!j) return <aside className="detail muted">Loading job…</aside>;

  return (
    <aside className="detail" aria-labelledby="job-head">
      <header>
        <div>
          <h2 id="job-head">{j.type}</h2>
          <span className="mono faint">{j.id}</span>
        </div>
        <button className="btn small quiet" onClick={onClose} aria-label="Close job details">
          Close
        </button>
      </header>
      <Status status={j.status} />

      {(j.status === "queued" || j.status === "scheduled" || j.status === "dead") && (
        <div style={{ display: "flex", gap: "0.5rem" }}>
          {j.status === "dead" ? (
            <button className="btn" onClick={() => retry.mutate()} disabled={retry.isPending}>
              Retry with fresh attempts
            </button>
          ) : (
            <button className="btn danger" onClick={() => cancel.mutate()} disabled={cancel.isPending}>
              Cancel job
            </button>
          )}
        </div>
      )}
      <ErrorNotice error={cancel.error ?? retry.error} />

      <dl className="facts">
        <dt>Priority</dt>
        <dd>{j.priority}</dd>
        <dt>Attempts</dt>
        <dd>
          {j.attempts} of {j.max_attempts}
        </dd>
        <dt>Fencing token</dt>
        <dd title="Increases on every claim; a worker holding an older token can't record a result">
          {j.lease_token}
        </dd>
        <dt>Last worker</dt>
        <dd>{j.locked_by ?? "—"}</dd>
        {j.idempotency_key && (
          <>
            <dt>Idempotency key</dt>
            <dd className="mono">{j.idempotency_key}</dd>
          </>
        )}
      </dl>

      <section>
        <h3 style={{ marginBottom: "0.6rem" }}>Timeline</h3>
        <ol className="timeline">
          <li>
            <Lamp status="queued" />
            Submitted <span className="faint">{ago(j.created_at, now)}</span>
          </li>
          {j.status === "scheduled" && j.run_at && (
            <li>
              <Lamp status="scheduled" />
              {j.attempts ? "Retry due" : "Starts"} <span className="faint">{ago(j.run_at, now)}</span>
            </li>
          )}
          {j.started_at && (
            <li>
              <Lamp status="running" live={j.status === "running"} />
              {j.attempts > 1 ? `Attempt ${j.attempts} started` : "Started"}
              <span className="faint">{ago(j.started_at, now)}</span>
            </li>
          )}
          {j.finished_at && (
            <li>
              <Lamp status={j.status} />
              {STATUS_LABEL[j.status]}
              <span className="faint">
                {j.started_at ? `after ${duration(Date.parse(j.finished_at) - Date.parse(j.started_at))}` : ago(j.finished_at, now)}
              </span>
            </li>
          )}
        </ol>
      </section>

      {j.last_error && (
        <section>
          <h3 style={{ marginBottom: "0.5rem" }}>{j.status === "succeeded" ? "Last error before it succeeded" : "Error"}</h3>
          <Json value={j.last_error} error={j.status !== "succeeded"} />
        </section>
      )}
      {j.result && (
        <section>
          <h3 style={{ marginBottom: "0.5rem" }}>Result</h3>
          <Json value={j.result} />
        </section>
      )}
      <section>
        <h3 style={{ marginBottom: "0.5rem" }}>Payload</h3>
        <Json value={j.payload} />
      </section>
    </aside>
  );
}

function defaults(t: JobType | undefined): string {
  if (!t) return "{}";
  const props = t.payload_schema.properties ?? {};
  const example: Record<string, unknown> = {};
  for (const [name, spec] of Object.entries(props)) {
    example[name] = spec.default ?? (spec.type === "integer" || spec.type === "number" ? 1 : spec.type === "boolean" ? false : "");
  }
  return JSON.stringify(example, null, 2);
}

function NewJob({ onSubmitted }: { onSubmitted: (job: Job) => void }) {
  const qc = useQueryClient();
  const types = useQuery({ queryKey: ["job-types"], queryFn: api.jobTypes, staleTime: Infinity });
  const [type, setType] = useState("sleep");
  const selected = useMemo(() => types.data?.find((t) => t.type === type), [types.data, type]);
  const [payload, setPayload] = useState("{}");
  const [priority, setPriority] = useState<Priority>("normal");
  const [maxAttempts, setMaxAttempts] = useState(3);
  const [delay, setDelay] = useState("");
  const [key, setKey] = useState("");
  const [parseError, setParseError] = useState("");

  useEffect(() => setPayload(defaults(selected)), [selected]);

  const submit = useMutation({
    mutationFn: () => {
      const body: Record<string, unknown> = {
        type,
        payload: JSON.parse(payload),
        priority,
        max_attempts: maxAttempts,
      };
      if (delay) body.delay_seconds = Number(delay);
      return api.submitJob(body, key.trim() || undefined);
    },
    onSuccess: (job) => {
      qc.invalidateQueries();
      onSubmitted(job);
    },
  });

  return (
    <form
      className="panel"
      style={{ marginBottom: "1.5rem" }}
      onSubmit={(e) => {
        e.preventDefault();
        try {
          JSON.parse(payload);
          setParseError("");
          submit.mutate();
        } catch {
          setParseError("The payload isn't valid JSON.");
        }
      }}
    >
      <h2 style={{ marginBottom: "0.4rem" }}>New job</h2>
      <p className="muted" style={{ marginBottom: "1rem" }}>
        {selected?.description || "Choose what to run."}
      </p>
      <div className="form-grid">
        <label className="field">
          Type
          <select value={type} onChange={(e) => setType(e.target.value)}>
            {types.data?.map((t) => (
              <option key={t.type} value={t.type}>
                {t.type}
              </option>
            ))}
          </select>
        </label>
        <label className="field">
          Priority
          <select value={priority} onChange={(e) => setPriority(e.target.value as Priority)}>
            <option value="high">High</option>
            <option value="normal">Normal</option>
            <option value="low">Low</option>
          </select>
        </label>
        <label className="field">
          Max attempts
          <input type="number" min={1} max={25} value={maxAttempts} onChange={(e) => setMaxAttempts(Number(e.target.value))} />
        </label>
        <label className="field">
          Start after (seconds)
          <input type="number" min={0} placeholder="Right away" value={delay} onChange={(e) => setDelay(e.target.value)} />
        </label>
        <label className="field">
          Idempotency key
          <input placeholder="Optional" value={key} onChange={(e) => setKey(e.target.value)} />
          <span className="hint">Resubmitting with the same key returns the same job.</span>
        </label>
      </div>
      <label className="field" style={{ marginTop: "0.9rem" }}>
        Payload
        <textarea value={payload} onChange={(e) => setPayload(e.target.value)} spellCheck={false} rows={6} />
      </label>
      <div className="form-actions">
        <button className="btn" type="submit" disabled={submit.isPending}>
          Submit job
        </button>
        {parseError && <span className="notice error">{parseError}</span>}
      </div>
      <div style={{ marginTop: "0.75rem" }}>
        <ErrorNotice error={submit.error} />
      </div>
    </form>
  );
}
