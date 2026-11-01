export type JobStatus =
  | "blocked"
  | "upstream_failed"
  | "scheduled"
  | "queued"
  | "running"
  | "succeeded"
  | "dead"
  | "cancelled";

export type Priority = "high" | "normal" | "low";

export interface Job {
  id: string;
  type: string;
  payload: Record<string, unknown>;
  priority: Priority;
  status: JobStatus;
  attempts: number;
  max_attempts: number;
  idempotency_key: string | null;
  locked_by: string | null;
  lease_token: number;
  run_at: string | null;
  result: Record<string, unknown> | null;
  last_error: string | null;
  created_at: string;
  updated_at: string;
  started_at: string | null;
  finished_at: string | null;
}

export interface Page<T> {
  items: T[];
  limit?: number;
  offset?: number;
}

export interface RunningJob {
  id: string;
  type: string;
  priority: Priority;
  worker: string | null;
  attempt: number;
  lease_token: number;
  started_at: string | null;
  workflow_id: string | null;
  workflow_key: string | null;
}

export interface Stats {
  now: string;
  counts: Record<JobStatus, number>;
  queued_by_priority: Record<Priority, number>;
  running: RunningJob[];
  throughput: { at: string; succeeded: number; dead: number }[];
  recent_failures: { id: string; type: string; last_error: string | null; finished_at: string | null }[];
}

export interface JobType {
  type: string;
  description: string;
  payload_schema: {
    properties?: Record<string, { default?: unknown; type?: string }>;
    required?: string[];
  };
}

export type WorkflowStatus = "running" | "succeeded" | "failed" | "cancelled";

export interface WorkflowNode {
  key: string;
  job_id: string;
  type: string;
  status: JobStatus;
  depends_on: string[];
  pending_deps: number;
  attempts: number;
  result: Record<string, unknown> | null;
  last_error: string | null;
  started_at: string | null;
  finished_at: string | null;
}

export interface WorkflowSummary {
  id: string;
  name: string | null;
  status: WorkflowStatus;
  counts: Partial<Record<JobStatus, number>>;
  created_at: string;
  cancelled_at: string | null;
}

export interface Workflow extends WorkflowSummary {
  jobs: WorkflowNode[];
}

export interface Schedule {
  id: string;
  name: string;
  cron: string;
  job: { type: string; payload: Record<string, unknown>; priority: Priority; max_attempts: number };
  enabled: boolean;
  next_fire_at: string;
  last_fired_at: string | null;
  created_at: string;
}

export interface Tenant {
  id: string;
  name: string;
  api_key_prefix: string | null;
  rate_limit_per_second: number | null;
  burst: number | null;
  max_concurrency: number | null;
  created_at: string;
  api_key?: string;
}
