import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "../api";
import { ErrorNotice, useNow } from "../components/ui";
import { ago, shortId } from "../lib/time";

export function DeadLetters() {
  const qc = useQueryClient();
  const navigate = useNavigate();
  const [type, setType] = useState("");
  const types = useQuery({ queryKey: ["job-types"], queryFn: api.jobTypes, staleTime: Infinity });
  const dead = useQuery({ queryKey: ["dead", type], queryFn: () => api.deadJobs(type), refetchInterval: 3000 });
  const retry = useMutation({ mutationFn: api.retryJob, onSuccess: () => qc.invalidateQueries() });
  const now = useNow();

  return (
    <>
      <header className="page-head">
        <div>
          <h1>Dead letters</h1>
          <p>
            Jobs that ran out of attempts or failed permanently. Fix the cause, then retry: a retried job gets a fresh
            set of attempts, and any workflow jobs waiting on it resume.
          </p>
        </div>
        <label className="field" style={{ minWidth: "12rem" }}>
          Job type
          <select value={type} onChange={(e) => setType(e.target.value)}>
            <option value="">All types</option>
            {types.data?.map((t) => (
              <option key={t.type}>{t.type}</option>
            ))}
          </select>
        </label>
      </header>
      <ErrorNotice error={dead.error ?? retry.error} />
      <div className="table-wrap">
        {dead.data?.items.length === 0 ? (
          <div className="empty">
            <h3>Nothing is dead</h3>
            <p>Jobs that exhaust their retries or raise a permanent error will wait here for you.</p>
          </div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Type</th>
                <th>Job</th>
                <th>Error</th>
                <th>Attempts</th>
                <th>Died</th>
                <th>
                  <span className="visually-hidden">Action</span>
                </th>
              </tr>
            </thead>
            <tbody>
              {dead.data?.items.map((j) => (
                <tr key={j.id} onClick={() => navigate(`/jobs/${j.id}`)}>
                  <td>
                    <strong>{j.type}</strong>
                  </td>
                  <td className="mono">{shortId(j.id)}</td>
                  <td className="error" title={j.last_error ?? ""}>
                    {j.last_error}
                  </td>
                  <td className="num">
                    {j.attempts} of {j.max_attempts}
                  </td>
                  <td className="muted">{ago(j.finished_at, now)}</td>
                  <td>
                    <button
                      className="btn small"
                      disabled={retry.isPending}
                      onClick={(e) => {
                        e.stopPropagation();
                        retry.mutate(j.id);
                      }}
                    >
                      Retry
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </>
  );
}
