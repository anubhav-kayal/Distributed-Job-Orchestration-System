import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { api } from "../api";
import { ErrorNotice, Lamp, STATUS_LABEL, Status, useNow } from "../components/ui";
import { layout } from "../lib/dag";
import { ago, duration } from "../lib/time";
import type { JobStatus, Workflow, WorkflowNode, WorkflowStatus } from "../types";

const WORKFLOW_LAMP: Record<WorkflowStatus, JobStatus> = {
  running: "running",
  succeeded: "succeeded",
  failed: "dead",
  cancelled: "cancelled",
};

const EXAMPLE = JSON.stringify(
  {
    name: "nightly-etl",
    jobs: [
      { key: "extract", type: "sleep", payload: { seconds: 2 } },
      { key: "clean", type: "sleep", payload: { seconds: 3 }, depends_on: ["extract"] },
      { key: "enrich", type: "hash", payload: { data: "rows", iterations: 200000 }, depends_on: ["extract"] },
      { key: "load", type: "sleep", payload: { seconds: 1 }, depends_on: ["clean", "enrich"] },
      {
        key: "notify",
        type: "fake_email",
        payload: { to: "data-team@example.com", subject: "ETL finished" },
        depends_on: ["load"],
      },
    ],
  },
  null,
  2,
);

export function Workflows() {
  const list = useQuery({ queryKey: ["workflows"], queryFn: api.workflows, refetchInterval: 2000 });
  const [showForm, setShowForm] = useState(false);
  const navigate = useNavigate();
  const now = useNow();

  return (
    <>
      <header className="page-head">
        <div>
          <h1>Workflows</h1>
          <p>Jobs that depend on each other. A job runs once everything it depends on has succeeded.</p>
        </div>
        <button className="btn" onClick={() => setShowForm((v) => !v)} aria-expanded={showForm}>
          {showForm ? "Close form" : "New workflow"}
        </button>
      </header>
      {showForm && <NewWorkflow onSubmitted={(wf) => navigate(`/workflows/${wf.id}`)} />}
      <ErrorNotice error={list.error} />
      <div className="table-wrap">
        {list.data?.items.length === 0 ? (
          <div className="empty">
            <h3>No workflows yet</h3>
            <p>Submit the example to watch a five-step pipeline run in dependency order.</p>
            <button className="btn" onClick={() => setShowForm(true)}>
              New workflow
            </button>
          </div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Status</th>
                <th>Name</th>
                <th>Progress</th>
                <th>Created</th>
              </tr>
            </thead>
            <tbody>
              {list.data?.items.map((w) => {
                const total = Object.values(w.counts).reduce((a, b) => a + (b ?? 0), 0);
                return (
                  <tr key={w.id} onClick={() => navigate(`/workflows/${w.id}`)}>
                    <td>
                      <span className="status">
                        <Lamp status={WORKFLOW_LAMP[w.status]} live={w.status === "running"} />
                        {w.status[0].toUpperCase() + w.status.slice(1)}
                      </span>
                    </td>
                    <td>
                      <Link to={`/workflows/${w.id}`} onClick={(e) => e.stopPropagation()}>
                        <strong>{w.name ?? "Untitled workflow"}</strong>
                      </Link>
                    </td>
                    <td className="num">
                      {w.counts.succeeded ?? 0} of {total} succeeded
                      {w.counts.dead ? <span style={{ color: "var(--s-dead)" }}>, {w.counts.dead} dead</span> : null}
                    </td>
                    <td className="muted">{ago(w.created_at, now)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </>
  );
}

function NewWorkflow({ onSubmitted }: { onSubmitted: (wf: Workflow) => void }) {
  const qc = useQueryClient();
  const [body, setBody] = useState(EXAMPLE);
  const [parseError, setParseError] = useState("");
  const submit = useMutation({
    mutationFn: () => api.submitWorkflow(JSON.parse(body)),
    onSuccess: (wf) => {
      qc.invalidateQueries();
      onSubmitted(wf);
    },
  });
  return (
    <form
      className="panel"
      style={{ marginBottom: "1.5rem" }}
      onSubmit={(e) => {
        e.preventDefault();
        try {
          JSON.parse(body);
          setParseError("");
          submit.mutate();
        } catch {
          setParseError("The workflow isn't valid JSON.");
        }
      }}
    >
      <h2 style={{ marginBottom: "0.4rem" }}>New workflow</h2>
      <p className="muted" style={{ marginBottom: "0.9rem" }}>
        Each job has a key and lists the keys it depends on. Cycles and unknown keys are rejected.
      </p>
      <label className="field">
        Workflow
        <textarea value={body} onChange={(e) => setBody(e.target.value)} spellCheck={false} rows={18} />
      </label>
      <div className="form-actions">
        <button className="btn" type="submit" disabled={submit.isPending}>
          Submit workflow
        </button>
        {parseError && <span className="notice error">{parseError}</span>}
      </div>
      <div style={{ marginTop: "0.75rem" }}>
        <ErrorNotice error={submit.error} />
      </div>
    </form>
  );
}

const NODE_W = 168;
const NODE_H = 56;
const GAP_X = 72;
const GAP_Y = 22;

export function WorkflowDetail() {
  const { id = "" } = useParams();
  const qc = useQueryClient();
  const wf = useQuery({ queryKey: ["workflow", id], queryFn: () => api.workflow(id), refetchInterval: 1500 });
  const cancel = useMutation({ mutationFn: () => api.cancelWorkflow(id), onSuccess: () => qc.invalidateQueries() });
  const [selected, setSelected] = useState<string | null>(null);
  const now = useNow();

  if (wf.error) return <ErrorNotice error={wf.error} />;
  if (!wf.data) return <p className="muted">Loading workflow…</p>;
  const w = wf.data;
  const node = w.jobs.find((j) => j.key === selected);

  return (
    <>
      <header className="page-head">
        <div>
          <Link to="/workflows" className="faint">
            Workflows
          </Link>
          <h1>{w.name ?? "Untitled workflow"}</h1>
          <p className="status" style={{ marginTop: "0.5rem" }}>
            <Lamp status={WORKFLOW_LAMP[w.status]} live={w.status === "running"} />
            {w.status[0].toUpperCase() + w.status.slice(1)}, created {ago(w.created_at, now)}
          </p>
        </div>
        {w.status === "running" && (
          <button className="btn danger" onClick={() => cancel.mutate()} disabled={cancel.isPending}>
            Cancel workflow
          </button>
        )}
      </header>
      <ErrorNotice error={cancel.error} />
      <div className={node ? "split" : "split closed"}>
        <Diagram nodes={w.jobs} selected={selected} onSelect={setSelected} now={now} />
        {node && <NodeDetail node={node} onClose={() => setSelected(null)} now={now} />}
      </div>
    </>
  );
}

function Diagram({
  nodes,
  selected,
  onSelect,
  now,
}: {
  nodes: WorkflowNode[];
  selected: string | null;
  onSelect: (key: string) => void;
  now: number;
}) {
  const { placed, columns, rows } = layout(nodes);
  const pos = new Map(placed.map((p) => [p.key, p]));
  const byKey = new Map(nodes.map((n) => [n.key, n]));
  const x = (c: number) => c * (NODE_W + GAP_X);
  const y = (r: number) => r * (NODE_H + GAP_Y);
  const width = Math.max(1, columns) * (NODE_W + GAP_X) - GAP_X;
  const height = Math.max(1, rows) * (NODE_H + GAP_Y) - GAP_Y;

  return (
    <div className="diagram">
      <svg width={width + 8} height={height + 8} viewBox={`-4 -4 ${width + 8} ${height + 8}`} role="img" aria-label="Workflow dependency diagram">
        {nodes.flatMap((n) =>
          n.depends_on.map((dep) => {
            const from = pos.get(dep)!;
            const to = pos.get(n.key)!;
            const parent = byKey.get(dep)!;
            const x1 = x(from.column) + NODE_W;
            const y1 = y(from.row) + NODE_H / 2;
            const x2 = x(to.column);
            const y2 = y(to.row) + NODE_H / 2;
            const mid = (x1 + x2) / 2;
            const cls =
              parent.status === "succeeded" ? "edge done" : ["dead", "cancelled", "upstream_failed"].includes(parent.status) ? "edge broken" : "edge";
            return <path key={`${dep}-${n.key}`} className={cls} d={`M${x1},${y1} H${mid} V${y2} H${x2}`} />;
          }),
        )}
        {nodes.map((n) => {
          const p = pos.get(n.key)!;
          const elapsed =
            n.started_at && (n.finished_at ? Date.parse(n.finished_at) : now) - Date.parse(n.started_at);
          return (
            <g
              key={n.key}
              className={selected === n.key ? "node selected" : "node"}
              transform={`translate(${x(p.column)},${y(p.row)})`}
              onClick={() => onSelect(n.key)}
              onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && onSelect(n.key)}
              tabIndex={0}
              role="button"
              aria-label={`${n.key}: ${STATUS_LABEL[n.status]}`}
            >
              <rect width={NODE_W} height={NODE_H} rx={3} />
              <rect width={6} height={NODE_H} rx={1} style={{ fill: `var(--s-${n.status})`, stroke: "none" }} />
              <circle
                cx={NODE_W - 16}
                cy={18}
                r={6}
                style={{ fill: `var(--s-${n.status})`, stroke: "none" }}
                className={n.status === "running" ? "lamp-svg live" : "lamp-svg"}
              />
              <text x={18} y={24}>
                {n.key}
              </text>
              <text x={18} y={42} className="sub">
                {STATUS_LABEL[n.status]}
                {elapsed ? `${n.status === "running" ? " for" : " in"} ${duration(elapsed)}` : ""}
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}

function NodeDetail({ node, onClose, now }: { node: WorkflowNode; onClose: () => void; now: number }) {
  return (
    <aside className="detail" aria-labelledby="node-head">
      <header>
        <div>
          <h2 id="node-head">{node.key}</h2>
          <span className="faint">{node.type}</span>
        </div>
        <button className="btn small quiet" onClick={onClose}>
          Close
        </button>
      </header>
      <Status status={node.status} />
      <dl className="facts">
        <dt>Depends on</dt>
        <dd>{node.depends_on.length ? node.depends_on.join(", ") : "Nothing: it runs first"}</dd>
        {node.status === "blocked" && (
          <>
            <dt>Waiting for</dt>
            <dd>
              {node.pending_deps} {node.pending_deps === 1 ? "dependency" : "dependencies"}
            </dd>
          </>
        )}
        <dt>Attempts</dt>
        <dd>{node.attempts}</dd>
        <dt>Started</dt>
        <dd>{ago(node.started_at, now)}</dd>
        <dt>Finished</dt>
        <dd>{ago(node.finished_at, now)}</dd>
      </dl>
      {node.last_error && <pre className="code error">{node.last_error}</pre>}
      {node.result && <pre className="code">{JSON.stringify(node.result, null, 2)}</pre>}
      <Link className="btn quiet" to={`/jobs/${node.job_id}`}>
        Open job
      </Link>
    </aside>
  );
}
