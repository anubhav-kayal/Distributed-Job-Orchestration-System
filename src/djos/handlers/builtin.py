"""Demo job types. They stand in for real work: I/O waits, CPU-bound work, external calls
and failures."""

import asyncio
import hashlib
import random
import uuid

from pydantic import BaseModel, ConfigDict, Field

from djos.handlers.registry import JobContext, PermanentError, handler


class _Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SleepPayload(_Payload):
    seconds: float = Field(default=1.0, ge=0, le=300)


@handler("sleep", SleepPayload)
async def sleep(payload: SleepPayload, ctx: JobContext) -> dict:
    """I/O-bound work: waiting on something external."""
    await asyncio.sleep(payload.seconds)
    return {"slept": payload.seconds}


class HashPayload(_Payload):
    data: str = Field(max_length=1_000_000)
    iterations: int = Field(default=100_000, ge=1, le=5_000_000)


def _iterated_sha256(data: str, iterations: int) -> str:
    digest = data.encode()
    for _ in range(iterations):
        digest = hashlib.sha256(digest).digest()
    return digest.hex()


@handler("hash", HashPayload)
async def hash_(payload: HashPayload, ctx: JobContext) -> dict:
    """CPU-bound work. Runs in a thread so it doesn't block the worker's event loop
    (which will also be sending lease heartbeats)."""
    digest = await asyncio.to_thread(_iterated_sha256, payload.data, payload.iterations)
    return {"sha256": digest, "iterations": payload.iterations}


class FakeEmailPayload(_Payload):
    to: str = Field(pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    subject: str = Field(max_length=200)
    body: str = Field(default="", max_length=100_000)


@handler("fake_email", FakeEmailPayload)
async def fake_email(payload: FakeEmailPayload, ctx: JobContext) -> dict:
    """Simulates calling an external provider with variable latency."""
    await asyncio.sleep(random.uniform(0.05, 0.2))
    return {"message_id": str(uuid.uuid4()), "to": payload.to}


class FlakyPayload(_Payload):
    fail_times: int = Field(default=1, ge=0, le=10)


@handler("flaky", FlakyPayload)
async def flaky(payload: FlakyPayload, ctx: JobContext) -> dict:
    """Fails on its first `fail_times` attempts, then succeeds. Used to test retries."""
    if ctx.attempt <= payload.fail_times:
        raise RuntimeError(f"flaky failure on attempt {ctx.attempt} of {payload.fail_times}")
    return {"succeeded_on_attempt": ctx.attempt}


class FailPayload(_Payload):
    message: str = Field(default="always fails", max_length=1000)
    permanent: bool = False


@handler("fail", FailPayload)
async def fail(payload: FailPayload, ctx: JobContext) -> dict:
    """Always fails: retryably (uses up its attempts), or permanently (no retries)."""
    if payload.permanent:
        raise PermanentError(payload.message)
    raise RuntimeError(payload.message)
