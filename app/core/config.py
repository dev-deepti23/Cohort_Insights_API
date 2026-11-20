from functools import lru_cache
from pydantic import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    app_name: str = "cohort-insights-api"
    app_env: str = "development"
    log_level: str = "INFO"

    # MongoDB
    mongodb_uri: str = "mongodb://localhost:27017"
    mongodb_db_name: str = "cohort_insights"

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    content_cache_ttl_seconds: int = 86400
    rate_limit_max_active_docs: int = 3
    rate_limit_key_ttl_seconds: int = 3600

    # Two-Stage Pipeline Simulation
    stage1_min_delay_seconds: float = 10.0
    stage1_max_delay_seconds: float = 20.0
    stage2_min_delay_seconds: float = 5.0
    stage2_max_delay_seconds: float = 15.0
    stage_failure_probability: float = 0.10
    worker_concurrency: int = 4
    stage_max_retries: int = 2
    retry_backoff_seconds: float = 1.0

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Return cached Settings instance."""
    return Settings()