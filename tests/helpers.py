from typing import Tuple

from tests.fakes import FakeDatabase, FakeRedis, install_driver_stubs_if_missing

install_driver_stubs_if_missing()

from fastapi import FastAPI  # noqa: E402
from app.main import create_app  # noqa: E402
from app.repositories.document_repo import DocumentRepository  # noqa: E402
from app.services.cache_service import ContentCacheService  # noqa: E402
from app.services.document_service import DocumentService  # noqa: E402
from app.services.pipeline_worker import PipelineWorker  # noqa: E402
from app.services.rate_limiter import RateLimiterService  # noqa: E402


class _FakePingManager:
    def __init__(self, ok: bool = True) -> None:
        self.ok = ok

    async def ping(self) -> bool:
        return self.ok


def build_test_app(
    failure_probability: float = 0.0,
    max_retries: int = 0,
    max_active_docs: int = 3,
    redis_fail_mode: bool = False,
) -> Tuple[FastAPI, FakeDatabase, FakeRedis, PipelineWorker]:
    """Build a FastAPI app wired with in-memory MongoDB/Redis fakes."""
    app = create_app()
    db = FakeDatabase()
    redis = FakeRedis(fail_mode=redis_fail_mode)
    repo = DocumentRepository(db)
    cache = ContentCacheService(redis_client=redis, ttl_seconds=3600)
    rate_limiter = RateLimiterService(
        redis_client=redis, max_active_docs=max_active_docs, key_ttl_seconds=3600
    )
    worker = PipelineWorker(
        repo=repo,
        cache=cache,
        rate_limiter=rate_limiter,
        stage1_delay_range=(0.0, 0.0),
        stage2_delay_range=(0.0, 0.0),
        failure_probability=failure_probability,
        concurrency=1,
        max_retries=max_retries,
        retry_backoff_seconds=0.0,
    )
    service = DocumentService(
        repo=repo,
        cache=cache,
        rate_limiter=rate_limiter,
        enqueue_fn=worker.enqueue,
        max_active_docs=max_active_docs,
    )

    app.state.mongo_manager = _FakePingManager(ok=True)
    app.state.redis_manager = _FakePingManager(ok=not redis_fail_mode)
    app.state.worker = worker
    app.state.document_service = service
    app.state.initialized = True
    return app, db, redis, worker