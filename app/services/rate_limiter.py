import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

_ACQUIRE_SLOT_LUA = """
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
    return 1
end
if redis.call('SCARD', KEYS[1]) >= tonumber(ARGV[2]) then
    return 0
end
redis.call('SADD', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
return 1
"""


class RateLimiterService:
    """Tracks concurrent active pipeline documents per user using a Redis Set."""

    KEY_PREFIX = "cohort:active_jobs:"

    def __init__(
        self,
        redis_client: Optional[Any],
        max_active_docs: int = 3,
        key_ttl_seconds: int = 3600,
    ) -> None:
        self._redis = redis_client
        self._max_active_docs = max_active_docs
        self._key_ttl_seconds = key_ttl_seconds

    def _key(self, user_id: str) -> str:
        return f"{self.KEY_PREFIX}{user_id}"

    async def acquire_slot(self, user_id: str, document_id: str) -> bool:
        """Attempt to reserve an active-pipeline slot for (user_id, document_id)."""
        if self._redis is None:
            return True
        try:
            result = await self._redis.eval(
                _ACQUIRE_SLOT_LUA,
                1,
                self._key(user_id),
                document_id,
                str(self._max_active_docs),
                str(self._key_ttl_seconds),
            )
            return int(result) == 1
        except Exception as exc:
            logger.warning(
                "Redis rate limiter unavailable; failing open",
                extra={"extra_data": {"user_id": user_id, "error": str(exc)}},
            )
            return True

    async def release_slot(self, user_id: str, document_id: str) -> None:
        """Idempotently release an active-pipeline slot when processing finishes or fails."""
        if self._redis is None:
            return
        try:
            await self._redis.srem(self._key(user_id), document_id)
        except Exception as exc:
            logger.warning(
                "Redis rate limiter release failed",
                extra={
                    "extra_data": {
                        "user_id": user_id,
                        "document_id": document_id,
                        "error": str(exc),
                    }
                },
            )

    async def active_count(self, user_id: str) -> int:
        if self._redis is None:
            return 0
        try:
            return int(await self._redis.scard(self._key(user_id)))
        except Exception as exc:
            logger.warning(
                "Redis active_count check failed",
                extra={"extra_data": {"user_id": user_id, "error": str(exc)}},
            )
            return 0