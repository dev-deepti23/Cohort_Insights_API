import logging
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.api.routes import documents, health
from app.core.config import Settings, get_settings
from app.core.exceptions import DomainError
from app.core.logging import setup_logging
from app.db.mongodb import MongoDBManager
from app.db.redis_client import RedisManager
from app.repositories.document_repo import DocumentRepository
from app.services.cache_service import ContentCacheService
from app.services.document_service import DocumentService
from app.services.pipeline_worker import PipelineWorker
from app.services.rate_limiter import RateLimiterService

logger = logging.getLogger(__name__)


def create_app(settings: Optional[Settings] = None) -> FastAPI:
    """Create and configure the FastAPI application."""
    cfg = settings or get_settings()
    setup_logging(cfg.log_level)

    app = FastAPI(
        title="Cohort Insights API",
        version="1.0.0",
        description="Two-stage async document processing service with staleness guarantees.",
    )

    app.include_router(health.router)
    app.include_router(documents.router)

    @app.exception_handler(DomainError)
    async def domain_error_handler(_request: Request, exc: DomainError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.message},
        )

    @app.on_event("startup")
    async def on_startup() -> None:
        if getattr(app.state, "initialized", False):
            return

        mongo_manager = MongoDBManager(cfg.mongodb_uri, cfg.mongodb_db_name)
        db = mongo_manager.connect()
        await mongo_manager.ensure_indexes()

        redis_manager = RedisManager(cfg.redis_url)
        redis_client = redis_manager.connect()

        repo = DocumentRepository(db)
        cache_service = ContentCacheService(
            redis_client=redis_client,
            ttl_seconds=cfg.content_cache_ttl_seconds,
        )
        rate_limiter = RateLimiterService(
            redis_client=redis_client,
            max_active_docs=cfg.rate_limit_max_active_docs,
            key_ttl_seconds=cfg.rate_limit_key_ttl_seconds,
        )
        worker = PipelineWorker(
            repo=repo,
            cache=cache_service,
            rate_limiter=rate_limiter,
            stage1_delay_range=(cfg.stage1_min_delay_seconds, cfg.stage1_max_delay_seconds),
            stage2_delay_range=(cfg.stage2_min_delay_seconds, cfg.stage2_max_delay_seconds),
            failure_probability=cfg.stage_failure_probability,
            concurrency=cfg.worker_concurrency,
            max_retries=cfg.stage_max_retries,
            retry_backoff_seconds=cfg.retry_backoff_seconds,
        )
        await worker.start()

        app.state.mongo_manager = mongo_manager
        app.state.redis_manager = redis_manager
        app.state.worker = worker
        app.state.document_service = DocumentService(
            repo=repo,
            cache=cache_service,
            rate_limiter=rate_limiter,
            enqueue_fn=worker.enqueue,
            max_active_docs=cfg.rate_limit_max_active_docs,
        )
        app.state.initialized = True
        logger.info("Cohort Insights API started")

    @app.on_event("shutdown")
    async def on_shutdown() -> None:
        worker = getattr(app.state, "worker", None)
        if worker is not None:
            await worker.stop()
        redis_manager = getattr(app.state, "redis_manager", None)
        if redis_manager is not None:
            await redis_manager.close()
        mongo_manager = getattr(app.state, "mongo_manager", None)
        if mongo_manager is not None:
            mongo_manager.close()
        logger.info("Cohort Insights API stopped")

    return app


app = create_app()