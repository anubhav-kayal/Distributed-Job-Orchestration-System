import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { api, credentials } from "../api";
import { ErrorNotice, useNow } from "../components/ui";
import { ago } from "../lib/time";
import type { Tenant } from "../types";

export function Tenants() {
  const [token, setToken] = useState(credentials.adminToken());
  const [draft, setDraft] = useState("");

  if (!token) {
    return (
      <>
        <header className="page-head">
          <div>
            <h1>Tenants</h1>
            <p>Managing tenants needs the admin token the API was started with (DJOS_ADMIN_TOKEN).</p>
          </div>
        </header>
        <form
          className="panel"
          style={{ maxWidth: "32rem" }}
          onSubmit={(e) => {
            e.preventDefault();
            credentials.setAdminToken(draft.trim());
            setToken(draft.trim());
          }}
        >
          <label className="field">
            Admin token
            <input type="password" value={draft} onChange={(e) => setDraft(e.target.value)} autoComplete="off" />
            <span className="hint">Kept for this browser tab only.</span>
          </label>
          <div className="form-actions">
            <button className="btn" type="submit" disabled={!draft.trim()}>
              Unlock
            </button>
          </div>
        </form>
      </>
    );
  }
  return (
    <TenantAdmin
      onLock={() => {
        credentials.setAdminToken("");
        setToken("");
      }}
    />
  );
}

function TenantAdmin({ onLock }: { onLock: () => void }) {
  const qc = useQueryClient();
  const tenants = useQuery({ queryKey: ["tenants"], queryFn: api.tenants, retry: false });
  const [created, setCreated] = useState<Tenant | null>(null);
  const rotate = useMutation({
    mutationFn: api.rotateKey,
    onSuccess: (t) => {
      setCreated(t);
      qc.invalidateQueries({ queryKey: ["tenants"] });
    },
  });
  const now = useNow();

  return (
    <>
      <header className="page-head">
        <div>
          <h1>Tenants</h1>
          <p>Each tenant has its own API key, sees only its own jobs, and can be given limits.</p>
        </div>
        <button className="btn quiet" onClick={onLock}>
          Lock admin
        </button>
      </header>
      <ErrorNotice error={tenants.error ?? rotate.error} />
      {created?.api_key && <KeyReveal tenant={created} onDone={() => setCreated(null)} />}
      <NewTenant onCreated={setCreated} />
      <div className="table-wrap" style={{ marginTop: "1.5rem" }}>
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Key</th>
              <th>Rate limit</th>
              <th>Max running</th>
              <th>Created</th>
              <th>
                <span className="visually-hidden">Actions</span>
              </th>
            </tr>
          </thead>
          <tbody>
            {tenants.data?.items.map((t) => (
              <tr key={t.id} style={{ cursor: "default" }}>
                <td>
                  <strong>{t.name}</strong>
                </td>
                <td className="mono muted">{t.api_key_prefix ? `${t.api_key_prefix}…` : "No key (default tenant)"}</td>
                <td>{t.rate_limit_per_second ? `${t.rate_limit_per_second}/s, bursts of ${t.burst}` : "Unlimited"}</td>
                <td>{t.max_concurrency ?? "Unlimited"}</td>
                <td className="muted">{ago(t.created_at, now)}</td>
                <td>
                  {t.api_key_prefix && (
                    <button
                      className="btn small quiet"
                      onClick={() =>
                        window.confirm(`Issue a new key for “${t.name}”? The current key stops working at once.`) &&
                        rotate.mutate(t.id)
                      }
                    >
                      Rotate key
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

function KeyReveal({ tenant, onDone }: { tenant: Tenant; onDone: () => void }) {
  const [copied, setCopied] = useState(false);
  return (
    <section className="notice ok" style={{ marginBottom: "1.5rem", display: "grid", gap: "0.6rem" }}>
      <strong>API key for {tenant.name}. Copy it now: it won't be shown again.</strong>
      <div className="key-reveal">
        <code>{tenant.api_key}</code>
        <button
          className="btn small"
          onClick={() => {
            navigator.clipboard.writeText(tenant.api_key ?? "");
            setCopied(true);
          }}
        >
          {copied ? "Copied" : "Copy"}
        </button>
        <button
          className="btn small quiet"
          onClick={() => {
            credentials.setApiKey(tenant.api_key ?? "");
            window.location.assign("/");
          }}
        >
          Act as {tenant.name}
        </button>
      </div>
      <button className="btn small quiet" style={{ justifySelf: "start" }} onClick={onDone}>
        Done
      </button>
    </section>
  );
}

function NewTenant({ onCreated }: { onCreated: (t: Tenant) => void }) {
  const qc = useQueryClient();
  const [name, setName] = useState("");
  const [rate, setRate] = useState("");
  const [burst, setBurst] = useState("");
  const [cap, setCap] = useState("");
  const create = useMutation({
    mutationFn: () =>
      api.createTenant({
        name,
        rate_limit_per_second: rate ? Number(rate) : null,
        burst: burst ? Number(burst) : null,
        max_concurrency: cap ? Number(cap) : null,
      }),
    onSuccess: (t) => {
      setName("");
      qc.invalidateQueries({ queryKey: ["tenants"] });
      onCreated(t);
    },
  });
  return (
    <form
      className="panel"
      onSubmit={(e) => {
        e.preventDefault();
        create.mutate();
      }}
    >
      <h2 style={{ marginBottom: "1rem" }}>New tenant</h2>
      <div className="form-grid">
        <label className="field">
          Name
          <input required placeholder="acme" value={name} onChange={(e) => setName(e.target.value)} />
        </label>
        <label className="field">
          Submissions per second
          <input type="number" min={0} step="any" placeholder="Unlimited" value={rate} onChange={(e) => setRate(e.target.value)} />
        </label>
        <label className="field">
          Burst
          <input type="number" min={1} placeholder="Unlimited" value={burst} onChange={(e) => setBurst(e.target.value)} />
          <span className="hint">Set together with the rate.</span>
        </label>
        <label className="field">
          Max running jobs
          <input type="number" min={1} placeholder="Unlimited" value={cap} onChange={(e) => setCap(e.target.value)} />
        </label>
      </div>
      <div className="form-actions">
        <button className="btn" type="submit" disabled={create.isPending}>
          Create tenant
        </button>
      </div>
      <div style={{ marginTop: "0.75rem" }}>
        <ErrorNotice error={create.error} />
      </div>
    </form>
  );
}
