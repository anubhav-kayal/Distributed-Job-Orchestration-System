import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { api } from "../api";
import { ErrorNotice, Lamp, useNow } from "../components/ui";
import { ago } from "../lib/time";

export function Schedules() {
  const qc = useQueryClient();
  const schedules = useQuery({ queryKey: ["schedules"], queryFn: api.schedules, refetchInterval: 5000 });
  const toggle = useMutation({
    mutationFn: ({ id, enabled }: { id: string; enabled: boolean }) => api.setScheduleEnabled(id, enabled),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["schedules"] }),
  });
  const remove = useMutation({
    mutationFn: api.deleteSchedule,
    onSuccess: () => qc.invalidateQueries({ queryKey: ["schedules"] }),
  });
  const [showForm, setShowForm] = useState(false);
  const now = useNow();

  return (
    <>
      <header className="page-head">
        <div>
          <h1>Schedules</h1>
          <p>Recurring jobs on a cron timetable, evaluated in UTC. Each fire creates exactly one job.</p>
        </div>
        <button className="btn" onClick={() => setShowForm((v) => !v)} aria-expanded={showForm}>
          {showForm ? "Close form" : "New schedule"}
        </button>
      </header>
      {showForm && <NewSchedule onCreated={() => setShowForm(false)} />}
      <ErrorNotice error={schedules.error ?? toggle.error ?? remove.error} />
      <div className="table-wrap">
        {schedules.data?.items.length === 0 ? (
          <div className="empty">
            <h3>No schedules</h3>
            <p>Create one to run a job every minute, every night, or on any cron timetable.</p>
          </div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>When</th>
                <th>Runs</th>
                <th>Next</th>
                <th>Last</th>
                <th>
                  <span className="visually-hidden">Actions</span>
                </th>
              </tr>
            </thead>
            <tbody>
              {schedules.data?.items.map((s) => (
                <tr key={s.id} style={{ cursor: "default" }}>
                  <td>
                    <span className="status">
                      <Lamp status={s.enabled ? "succeeded" : "cancelled"} off={!s.enabled} />
                      <strong>{s.name}</strong>
                    </span>
                  </td>
                  <td className="mono">{s.cron}</td>
                  <td>{s.job.type}</td>
                  <td className="muted">{s.enabled ? ago(s.next_fire_at, now) : "Paused"}</td>
                  <td className="muted">{ago(s.last_fired_at, now)}</td>
                  <td style={{ display: "flex", gap: "0.4rem", justifyContent: "flex-end" }}>
                    <button className="btn small quiet" onClick={() => toggle.mutate({ id: s.id, enabled: !s.enabled })}>
                      {s.enabled ? "Pause" : "Resume"}
                    </button>
                    <button
                      className="btn small danger"
                      onClick={() => window.confirm(`Delete the schedule “${s.name}”? Jobs it already created stay.`) && remove.mutate(s.id)}
                    >
                      Delete
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

function NewSchedule({ onCreated }: { onCreated: () => void }) {
  const qc = useQueryClient();
  const types = useQuery({ queryKey: ["job-types"], queryFn: api.jobTypes, staleTime: Infinity });
  const [name, setName] = useState("");
  const [cron, setCron] = useState("*/5 * * * *");
  const [type, setType] = useState("sleep");
  const [payload, setPayload] = useState('{\n  "seconds": 1\n}');
  const [parseError, setParseError] = useState("");
  const create = useMutation({
    mutationFn: () => api.createSchedule({ name, cron, job: { type, payload: JSON.parse(payload) } }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["schedules"] });
      onCreated();
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
          create.mutate();
        } catch {
          setParseError("The payload isn't valid JSON.");
        }
      }}
    >
      <h2 style={{ marginBottom: "1rem" }}>New schedule</h2>
      <div className="form-grid">
        <label className="field">
          Name
          <input required placeholder="nightly-digest" value={name} onChange={(e) => setName(e.target.value)} />
          <span className="hint">Lowercase letters, digits, dots, dashes.</span>
        </label>
        <label className="field">
          Cron
          <input required className="mono" value={cron} onChange={(e) => setCron(e.target.value)} />
          <span className="hint">Minute, hour, day, month, weekday (UTC).</span>
        </label>
        <label className="field">
          Job type
          <select value={type} onChange={(e) => setType(e.target.value)}>
            {types.data?.map((t) => (
              <option key={t.type}>{t.type}</option>
            ))}
          </select>
        </label>
      </div>
      <label className="field" style={{ marginTop: "0.9rem" }}>
        Payload
        <textarea value={payload} onChange={(e) => setPayload(e.target.value)} spellCheck={false} rows={4} />
      </label>
      <div className="form-actions">
        <button className="btn" type="submit" disabled={create.isPending}>
          Create schedule
        </button>
        {parseError && <span className="notice error">{parseError}</span>}
      </div>
      <div style={{ marginTop: "0.75rem" }}>
        <ErrorNotice error={create.error} />
      </div>
    </form>
  );
}
