import logging
from typing import Optional

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class RedisManager:
    """Manages async Redis client lifecycle and health checks."""

    def __init__(self, url: str) -> None:
        self._url = url
        self._client: Optional[aioredis.Redis] = None

    def connect(self) -> aioredis.Redis:
        """Initialize the async Redis client."""
        if self._client is None:
            self._client = aioredis.from_url(
                self._url,
                encoding="utf-8",
                decode_responses=True,
            )
            logger.info("Connected to Redis")
        return self._client

    @property
    def client(self) -> aioredis.Redis:
        if self._client is None:
            return self.connect()
        return self._client

    async def ping(self) -> bool:
        """Verify Redis connectivity."""
        try:
            return bool(await self.client.ping())
        except Exception as exc:
            logger.error("Redis health check failed", extra={"extra_data": {"error": str(exc)}})
            return False

    async def close(self) -> None:
        """Close the Redis client connection."""
        if self._client is not None:
            await self._client.close()
            self._client = None
            logger.info("Closed Redis connection")