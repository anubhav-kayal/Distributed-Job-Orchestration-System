import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { NavLink, Outlet } from "react-router-dom";
import { api, credentials } from "../api";
import { Lamp } from "./ui";

export function Shell() {
  const stats = useQuery({ queryKey: ["stats"], queryFn: api.stats, refetchInterval: 2000 });
  const c = stats.data?.counts;
  const active = c ? c.queued + c.running + c.scheduled + c.blocked : undefined;

  return (
    <div className="shell">
      <aside className="rail">
        <div className="wordmark">
          <Lamp
            status={stats.isError ? "dead" : "succeeded"}
            off={!stats.data && !stats.isError}
            live={Boolean(stats.data) && !stats.isError}
          />
          <div>
            Dispatch
            <small>
              {stats.isError ? "Can't reach the API" : stats.data ? "Job orchestration console" : "Connecting…"}
            </small>
          </div>
        </div>
        <nav className="nav" aria-label="Sections">
          <NavLink to="/" end>
            Board
          </NavLink>
          <NavLink to="/jobs">
            Jobs {active !== undefined && <span className="count">{active}</span>}
          </NavLink>
          <NavLink to="/workflows">Workflows</NavLink>
          <NavLink to="/dead">
            Dead letters{" "}
            {c && <span className={c.dead ? "count alarm" : "count"}>{c.dead}</span>}
          </NavLink>
          <NavLink to="/schedules">Schedules</NavLink>
          <NavLink to="/tenants">Tenants</NavLink>
        </nav>
        <ActingAs />
      </aside>
      <main className="main">
        <Outlet />
      </main>
    </div>
  );
}

function ActingAs() {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState("");
  const key = credentials.apiKey();

  const save = (value: string) => {
    credentials.setApiKey(value.trim());
    setEditing(false);
    window.location.reload(); // every view belongs to the tenant: start clean
  };

  return (
    <div className="acting-as">
      Acting as
      <strong>{key ? `${key.slice(0, 12)}…` : "Default tenant"}</strong>
      {editing ? (
        <form
          onSubmit={(e) => {
            e.preventDefault();
            save(draft);
          }}
          style={{ display: "grid", gap: "0.5rem", marginTop: "0.6rem" }}
        >
          <label className="visually-hidden" htmlFor="api-key">
            API key
          </label>
          <input
            id="api-key"
            placeholder="djos_…"
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            autoFocus
          />
          <div style={{ display: "flex", gap: "0.4rem" }}>
            <button className="btn small" type="submit" disabled={!draft.trim()}>
              Use key
            </button>
            <button className="btn small quiet" type="button" onClick={() => setEditing(false)}>
              Cancel
            </button>
          </div>
        </form>
      ) : (
        <div style={{ display: "flex", gap: "0.4rem", marginTop: "0.5rem" }}>
          <button className="btn small quiet" onClick={() => setEditing(true)}>
            {key ? "Switch key" : "Use an API key"}
          </button>
          {key && (
            <button className="btn small quiet" onClick={() => save("")}>
              Clear
            </button>
          )}
        </div>
      )}
    </div>
  );
}
