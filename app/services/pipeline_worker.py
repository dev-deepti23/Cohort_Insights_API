import asyncio
from datetime import datetime, timezone
import logging
import random
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.repositories.document_repo import DocumentRepository
from app.services.cache_service import ContentCacheService
from app.services.rate_limiter import RateLimiterService

logger = logging.getLogger(__name__)


class SimulatedStageError(RuntimeError):
    """Raised when a simulated pipeline stage experiences a transient failure."""


class PipelineWorker:
    """Two-stage async background pipeline with version-guarded writes and retry backoff."""

    def __init__(
        self,
        repo: DocumentRepository,
        cache: ContentCacheService,
        rate_limiter: RateLimiterService,
        stage1_delay_range: Tuple[float, float] = (10.0, 20.0),
        stage2_delay_range: Tuple[float, float] = (5.0, 15.0),
        failure_probability: float = 0.10,
        concurrency: int = 4,
        max_retries: int = 2,
        retry_backoff_seconds: float = 1.0,
        rng: Optional[Callable[[], float]] = None,
    ) -> None:
        self._repo = repo
        self._cache = cache
        self._rate_limiter = rate_limiter
        self._stage1_delay_range = stage1_delay_range
        self._stage2_delay_range = stage2_delay_range
        self._failure_probability = failure_probability
        self._concurrency = concurrency
        self._max_retries = max_retries
        self._retry_backoff_seconds = retry_backoff_seconds
        self._rng = rng or random.random

        self._queue: asyncio.Queue[Tuple[str, int]] = asyncio.Queue()
        self._workers: List[asyncio.Task[Any]] = []
        self._running = False

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._workers = [
            asyncio.create_task(self._worker_loop(i), name=f"pipeline-worker-{i}")
            for i in range(self._concurrency)
        ]
        logger.info("Started pipeline workers", extra={"extra_data": {"count": self._concurrency}})

    async def stop(self) -> None:
        self._running = False
        for task in self._workers:
            task.cancel()
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        logger.info("Stopped pipeline workers")

    async def enqueue(self, document_id: str, version: int) -> None:
        await self._queue.put((document_id, version))
        logger.info(
            "Enqueued document for processing",
            extra={"extra_data": {"document_id": document_id, "version": version}},
        )

    async def _worker_loop(self, worker_idx: int) -> None:
        while self._running:
            try:
                document_id, job_version = await self._queue.get()
                try:
                    await self.process_job(document_id, job_version)
                except Exception as exc:
                    logger.exception(
                        "Unexpected error in pipeline worker",
                        extra={
                            "extra_data": {
                                "worker": worker_idx,
                                "document_id": document_id,
                                "version": job_version,
                                "error": str(exc),
                            }
                        },
                    )
                finally:
                    self._queue.task_done()
            except asyncio.CancelledError:
                break

    @staticmethod
    def generate_mock_summary(title: str, content: str) -> str:
        snippet = " ".join(content.split())[:160]
        word_count = len(content.split())
        return f"[{title}] ({word_count} words): {snippet}"

    @staticmethod
    def generate_mock_tags(summary: str) -> List[str]:
        words = re.findall(r"[a-zA-Z]{4,}", summary.lower())
        stop_words = {"this", "that", "with", "from", "have", "were", "words", "into", "about"}
        seen: List[str] = []
        for word in words:
            if word not in stop_words and word not in seen:
                seen.append(word)
            if len(seen) >= 5:
                break
        return seen or ["general", "insight"]

    async def _run_with_retries(
        self,
        stage_name: str,
        delay_range: Tuple[float, float],
        document_id: str,
        job_version: int,
    ) -> None:
        attempts = self._max_retries + 1
        for attempt in range(1, attempts + 1):
            delay = random.uniform(*delay_range)
            if delay > 0:
                await asyncio.sleep(delay)

            if self._rng() >= self._failure_probability:
                return

            logger.warning(
                "Simulated stage failure",
                extra={
                    "extra_data": {
                        "stage": stage_name,
                        "document_id": document_id,
                        "version": job_version,
                        "attempt": attempt,
                        "max_attempts": attempts,
                    }
                },
            )
            if attempt < attempts and self._retry_backoff_seconds > 0:
                await asyncio.sleep(self._retry_backoff_seconds * (2 ** (attempt - 1)))

        raise SimulatedStageError(
            f"Stage '{stage_name}' failed after {attempts} attempt(s)."
        )

    async def process_job(self, document_id: str, job_version: int) -> None:
        """Execute Stage 1 (processing) and Stage 2 (enriching) with version guards."""
        doc = await self._repo.claim_stage_1(document_id, job_version)
        if doc is None:
            return

        user_id = doc["user_id"]
        content_hash = doc["content_hash"]

        try:
            await self._run_with_retries(
                "processing", self._stage1_delay_range, document_id, job_version
            )
        except SimulatedStageError as exc:
            await self._repo.fail_stage(document_id, job_version, "processing", str(exc))
            await self._rate_limiter.release_slot(user_id, document_id)
            return

        summary_text = self.generate_mock_summary(doc["title"], doc["content"])
        summary_payload = {
            "text": summary_text,
            "version": job_version,
            "content_hash": content_hash,
            "produced_at": datetime.now(timezone.utc),
        }

        doc_after_s1 = await self._repo.complete_stage_1(
            document_id, job_version, summary_payload
        )
        if doc_after_s1 is None:
            logger.info(
                "Aborted stale Stage 1 write due to version change",
                extra={"extra_data": {"document_id": document_id, "job_version": job_version}},
            )
            return

        await self._execute_stage_2(doc_after_s1, job_version)

    async def retry_stage_2(self, document_id: str, job_version: int) -> None:
        """Resume a failed Stage 2 without redoing completed Stage 1."""
        doc = await self._repo.claim_stage_2_retry(document_id, job_version)
        if doc is None:
            return
        await self._execute_stage_2(doc, job_version)

    async def _execute_stage_2(self, doc: Dict[str, Any], job_version: int) -> None:
        document_id = str(doc["_id"])
        user_id = doc["user_id"]
        content_hash = doc["content_hash"]
        summary_text = doc["summary"]["text"]

        try:
            await self._run_with_retries(
                "enriching", self._stage2_delay_range, document_id, job_version
            )
        except SimulatedStageError as exc:
            await self._repo.fail_stage(document_id, job_version, "enriching", str(exc))
            await self._rate_limiter.release_slot(user_id, document_id)
            return

        tags = self.generate_mock_tags(summary_text)
        tags_payload = {
            "items": tags,
            "version": job_version,
            "content_hash": content_hash,
            "produced_at": datetime.now(timezone.utc),
        }

        completed_doc = await self._repo.complete_stage_2(
            document_id, job_version, tags_payload
        )
        if completed_doc is None:
            logger.info(
                "Aborted stale Stage 2 write due to version change",
                extra={"extra_data": {"document_id": document_id, "job_version": job_version}},
            )
            return

        await self._cache.set(content_hash, summary_text, tags)
        await self._rate_limiter.release_slot(user_id, document_id)
        logger.info(
            "Completed two-stage pipeline",
            extra={"extra_data": {"document_id": document_id, "version": job_version}},
        )