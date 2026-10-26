"""Maps a job `type` to the code that runs it and the payload schema it accepts.

The API validates payloads against the same schema at submit time, so a malformed job is
rejected with a 422 instead of failing later on a worker.
"""

import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel


@dataclass(frozen=True)
class JobContext:
    job_id: uuid.UUID
    attempt: int  # 1 on the first run, incremented on every claim
    worker_id: str
    # Fencing token of this run. A handler calling an external system should send job_id
    # as its idempotency key (so a re-run doesn't apply twice) and, if the system
    # supports it, lease_token as a fencing token (so it can reject a stale run).
    lease_token: int = 0


HandlerFn = Callable[[Any, JobContext], Awaitable[dict[str, Any] | None]]


@dataclass(frozen=True)
class Handler:
    name: str
    payload_model: type[BaseModel]
    fn: HandlerFn


class UnknownJobType(LookupError):
    pass


class PermanentError(Exception):
    """Raise from a handler when retrying can't help (bad input, a 4xx from an API, a
    record that no longer exists). The job fails immediately instead of retrying."""


_REGISTRY: dict[str, Handler] = {}


def handler(name: str, payload_model: type[BaseModel]) -> Callable[[HandlerFn], HandlerFn]:
    """Register `fn(payload, ctx) -> result` as the handler for job type `name`."""

    def decorator(fn: HandlerFn) -> HandlerFn:
        if name in _REGISTRY:
            raise ValueError(f"handler for {name!r} is already registered")
        _REGISTRY[name] = Handler(name, payload_model, fn)
        return fn

    return decorator


def get_handler(name: str) -> Handler:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise UnknownJobType(name) from None


def registered_types() -> list[str]:
    return sorted(_REGISTRY)
