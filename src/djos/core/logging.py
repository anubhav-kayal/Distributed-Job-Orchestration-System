"""Logging setup: one JSON object per line, with job context attached automatically.

`bind_job(...)` (used by the worker around each job) puts job_id, tenant_id, worker_id
and lease_token into a context variable, and every log record emitted while it is
active carries them, including records from library code that knows nothing about jobs.
In a log search, `job_id:<id>` then shows that job's whole story across workers,
including which run (lease_token) each line came from.
"""

import contextlib
import contextvars
import json
import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

_context: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "log_context", default=None
)


def current_context() -> dict[str, Any]:
    return _context.get() or {}


@contextlib.contextmanager
def bind(**fields: Any) -> Iterator[None]:
    """Attach fields to every log record emitted inside the block (nests, asyncio-safe)."""
    token = _context.set(
        {**current_context(), **{k: v for k, v in fields.items() if v is not None}}
    )
    try:
        yield
    finally:
        _context.reset(token)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "msg": record.getMessage(),
            **{k: str(v) for k, v in current_context().items()},
        }
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry)


class TextFormatter(logging.Formatter):
    def __init__(self) -> None:
        super().__init__("%(asctime)s %(levelname)s %(name)s: %(message)s")

    def format(self, record: logging.LogRecord) -> str:
        line = super().format(record)
        context = current_context()
        return f"{line} [{' '.join(f'{k}={v}' for k, v in context.items())}]" if context else line


def setup_logging(level: str = "INFO", fmt: str = "json") -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter() if fmt == "json" else TextFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
