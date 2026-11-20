import json
import logging
from dataclasses import dataclass
from typing import Any, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class CachedContentResult:
    summary_text: str
    tags: List[str]


class ContentCacheService:
    """Redis-backed content-addressed cache keyed by SHA-256 content hash."""

    KEY_PREFIX = "cohort:cache:content:"

    def __init__(self, redis_client: Optional[Any], ttl_seconds: int = 86400) -> None:
        self._redis = redis_client
        self._ttl_seconds = ttl_seconds

    def _key(self, content_hash: str) -> str:
        return f"{self.KEY_PREFIX}{content_hash}"

    async def get(self, content_hash: str) -> Optional[CachedContentResult]:
        if self._redis is None:
            return None
        try:
            raw = await self._redis.get(self._key(content_hash))
            if not raw:
                return None
            payload = json.loads(raw)
            return CachedContentResult(
                summary_text=payload["summary_text"],
                tags=payload["tags"],
            )
        except Exception as exc:
            logger.warning(
                "Redis cache read failed; degrading gracefully",
                extra={"extra_data": {"content_hash": content_hash, "error": str(exc)}},
            )
            return None

    async def set(self, content_hash: str, summary_text: str, tags: List[str]) -> None:
        if self._redis is None:
            return
        try:
            payload = json.dumps({"summary_text": summary_text, "tags": tags})
            await self._redis.set(self._key(content_hash), payload, ex=self._ttl_seconds)
        except Exception as exc:
            logger.warning(
                "Redis cache write failed; degrading gracefully",
                extra={"extra_data": {"content_hash": content_hash, "error": str(exc)}},
            )