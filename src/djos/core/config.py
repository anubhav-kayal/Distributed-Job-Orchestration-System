from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All configuration comes from environment variables (prefixed DJOS_)."""

    model_config = SettingsConfigDict(env_prefix="DJOS_", env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://djos:djos@localhost:55432/djos"
    redis_url: str = "redis://localhost:56379/0"
    log_level: str = "INFO"
    # How long a readiness probe waits on a dependency before calling it down.
    dependency_timeout_seconds: float = 2.0
    # How long a worker blocks on an empty queue before re-checking for shutdown.
    worker_block_seconds: float = 1.0
    # A worker's lease on a running job expires after this long unless renewed.
    lease_ttl_seconds: float = 30.0
    # How often a worker renews its lease while running a job (at most half the TTL).
    heartbeat_interval_seconds: float = 10.0
    # Monitor: how often it sweeps, how long a job may sit `queued` before we check that
    # its ID is really in Redis, and how many jobs each sweep looks at.
    monitor_interval_seconds: float = 5.0
    stranded_after_seconds: float = 30.0
    monitor_batch_size: int = 100


@lru_cache
def get_settings() -> Settings:
    return Settings()
