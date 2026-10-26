from functools import lru_cache

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All configuration comes from environment variables (prefixed DJOS_)."""

    model_config = SettingsConfigDict(env_prefix="DJOS_", env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://djos:djos@localhost:55432/djos"
    redis_url: str = "redis://localhost:56379/0"
    # Max wait for any single Redis reply. Must exceed worker_block_seconds (BLMPOP).
    redis_socket_timeout_seconds: float = 5.0
    log_level: str = "INFO"
    # How long a readiness probe waits on a dependency before calling it down.
    dependency_timeout_seconds: float = 2.0
    # How long a worker blocks on an empty queue before re-checking for shutdown.
    worker_block_seconds: float = 1.0
    # A worker's lease on a running job expires after this long unless renewed.
    lease_ttl_seconds: float = 30.0
    # How often a worker renews its lease while running a job (at most half the TTL).
    heartbeat_interval_seconds: float = 10.0
    # Retry backoff after a failed attempt: random in [0, min(cap, base * 2^(attempt-1))].
    retry_base_seconds: float = 1.0
    retry_cap_seconds: float = 300.0
    # Monitor: how often it sweeps, how long a job may sit `queued` before we check that
    # its ID is really in Redis, and how many jobs each sweep looks at.
    monitor_interval_seconds: float = 5.0
    stranded_after_seconds: float = 30.0
    monitor_batch_size: int = 100

    @model_validator(mode="after")
    def check_timings(self) -> "Settings":
        if self.worker_block_seconds >= self.redis_socket_timeout_seconds:
            raise ValueError("worker_block_seconds must be less than redis_socket_timeout_seconds")
        if self.heartbeat_interval_seconds * 2 > self.lease_ttl_seconds:
            raise ValueError("heartbeat_interval_seconds must be at most half lease_ttl_seconds")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
