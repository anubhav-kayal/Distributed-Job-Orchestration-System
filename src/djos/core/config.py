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


@lru_cache
def get_settings() -> Settings:
    return Settings()
