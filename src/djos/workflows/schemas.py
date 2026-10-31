import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from djos.jobs.schemas import JobSpec
from djos.models import JobStatus
from djos.workflows.dag import InvalidWorkflow, topological_order

WorkflowStatus = Literal["running", "succeeded", "failed", "cancelled"]


class WorkflowJob(JobSpec):
    key: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{0,99}$", examples=["extract"])
    depends_on: list[str] = Field(default_factory=list, max_length=100)


class WorkflowCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, max_length=200)
    jobs: list[WorkflowJob] = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def check_graph(self) -> "WorkflowCreate":
        keys = [j.key for j in self.jobs]
        duplicates = sorted({k for k in keys if keys.count(k) > 1})
        if duplicates:
            raise ValueError(f"duplicate job keys {duplicates}")
        try:
            topological_order({j.key: j.depends_on for j in self.jobs})
        except InvalidWorkflow as exc:
            raise ValueError(str(exc)) from None
        return self


class WorkflowNode(BaseModel):
    key: str
    job_id: uuid.UUID
    type: str
    status: JobStatus
    depends_on: list[str]
    pending_deps: int
    attempts: int
    result: dict[str, Any] | None
    last_error: str | None
    started_at: datetime | None
    finished_at: datetime | None


class WorkflowSummary(BaseModel):
    id: uuid.UUID
    name: str | None
    status: WorkflowStatus
    counts: dict[str, int]
    created_at: datetime
    cancelled_at: datetime | None


class WorkflowOut(WorkflowSummary):
    jobs: list[WorkflowNode]


class WorkflowList(BaseModel):
    items: list[WorkflowSummary]
    limit: int
    offset: int
