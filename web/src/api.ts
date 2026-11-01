import type { Job, JobStatus, JobType, Page, Schedule, Stats, Tenant, Workflow, WorkflowSummary } from "./types";

const KEY = "djos.apiKey";
const ADMIN = "djos.adminToken";

export const credentials = {
  apiKey: () => localStorage.getItem(KEY) ?? "",
  setApiKey: (key: string) => (key ? localStorage.setItem(KEY, key) : localStorage.removeItem(KEY)),
  // The admin token is more powerful, so it only lives for this browser tab.
  adminToken: () => sessionStorage.getItem(ADMIN) ?? "",
  setAdminToken: (token: string) =>
    token ? sessionStorage.setItem(ADMIN, token) : sessionStorage.removeItem(ADMIN),
};

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

/** FastAPI errors are `{detail: string}` or a list of validation problems. */
function describe(status: number, body: unknown): string {
  const detail = (body as { detail?: unknown })?.detail;
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) {
    return detail
      .map((d: { loc?: (string | number)[]; msg?: string }) => {
        const where = (d.loc ?? []).filter((p) => p !== "body").join(".");
        return where ? `${where}: ${d.msg}` : d.msg;
      })
      .join("; ");
  }
  if (status === 401) return "The API key was rejected. Check it under “Acting as”.";
  if (status === 429) return "Rate limit reached for this tenant. Try again in a moment.";
  return `The API returned ${status}.`;
}

async function request<T>(path: string, init: RequestInit & { admin?: boolean } = {}): Promise<T> {
  const token = init.admin ? credentials.adminToken() : credentials.apiKey();
  const headers = new Headers(init.headers);
  if (token) headers.set("Authorization", `Bearer ${token}`);
  if (init.body) headers.set("Content-Type", "application/json");
  const res = await fetch(`/api${path}`, { ...init, headers });
  if (res.status === 204) return undefined as T;
  const body = await res.json().catch(() => null);
  if (!res.ok) throw new ApiError(res.status, describe(res.status, body));
  return body as T;
}

const post = <T>(path: string, body?: unknown, extra: RequestInit & { admin?: boolean } = {}) =>
  request<T>(path, { method: "POST", body: body === undefined ? undefined : JSON.stringify(body), ...extra });

export const api = {
  stats: () => request<Stats>("/stats"),
  jobTypes: () => request<JobType[]>("/job-types"),

  jobs: (status: JobStatus | "", offset = 0) =>
    request<Page<Job>>(`/jobs?limit=50&offset=${offset}${status ? `&status=${status}` : ""}`),
  job: (id: string) => request<Job>(`/jobs/${id}`),
  submitJob: (body: unknown, idempotencyKey?: string) =>
    post<Job>("/jobs", body, idempotencyKey ? { headers: { "Idempotency-Key": idempotencyKey } } : {}),
  cancelJob: (id: string) => post<Job>(`/jobs/${id}/cancel`),

  deadJobs: (type = "") => request<Page<Job>>(`/dlq?limit=100${type ? `&type=${type}` : ""}`),
  retryJob: (id: string) => post<Job>(`/dlq/${id}/retry`),

  workflows: () => request<Page<WorkflowSummary>>("/workflows?limit=100"),
  workflow: (id: string) => request<Workflow>(`/workflows/${id}`),
  submitWorkflow: (body: unknown) => post<Workflow>("/workflows", body),
  cancelWorkflow: (id: string) => post<Workflow>(`/workflows/${id}/cancel`),

  schedules: () => request<Page<Schedule>>("/schedules"),
  createSchedule: (body: unknown) => post<Schedule>("/schedules", body),
  setScheduleEnabled: (id: string, enabled: boolean) =>
    request<Schedule>(`/schedules/${id}`, { method: "PATCH", body: JSON.stringify({ enabled }) }),
  deleteSchedule: (id: string) => request<void>(`/schedules/${id}`, { method: "DELETE" }),

  tenants: () => request<Page<Tenant>>("/admin/tenants", { admin: true }),
  createTenant: (body: unknown) => post<Tenant>("/admin/tenants", body, { admin: true }),
  rotateKey: (id: string) => post<Tenant>(`/admin/tenants/${id}/rotate-key`, undefined, { admin: true }),
};
