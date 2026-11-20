import logging
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import ASCENDING, DESCENDING

logger = logging.getLogger(__name__)


class MongoDBManager:
    """Manages AsyncIOMotorClient lifecycle and database index initialization."""

    def __init__(self, uri: str, db_name: str) -> None:
        self._uri = uri
        self._db_name = db_name
        self._client: Optional[AsyncIOMotorClient] = None

    def connect(self) -> AsyncIOMotorDatabase:
        """Initialize the Motor client and return the configured database."""
        if self._client is None:
            self._client = AsyncIOMotorClient(self._uri)
            logger.info("Connected to MongoDB", extra={"extra_data": {"db_name": self._db_name}})
        return self._client[self._db_name]

    @property
    def db(self) -> AsyncIOMotorDatabase:
        if self._client is None:
            return self.connect()
        return self._client[self._db_name]

    async def ensure_indexes(self) -> None:
        """Create required indexes on the documents collection."""
        collection = self.db["documents"]
        await collection.create_index(
            [("user_id", ASCENDING), ("created_at", DESCENDING), ("_id", DESCENDING)],
            name="idx_user_created",
        )
        await collection.create_index(
            [("user_id", ASCENDING), ("status", ASCENDING), ("created_at", DESCENDING)],
            name="idx_user_status_created",
        )
        await collection.create_index(
            [("client_doc_ref", ASCENDING)],
            name="idx_client_doc_ref_unique",
            unique=True,
            partialFilterExpression={"client_doc_ref": {"$type": "string"}},
        )
        await collection.create_index(
            [("content_hash", ASCENDING)],
            name="idx_content_hash",
        )
        logger.info("MongoDB indexes ensured on documents collection")

    async def ping(self) -> bool:
        """Verify MongoDB connectivity."""
        try:
            await self.db.command("ping")
            return True
        except Exception as exc:
            logger.error("MongoDB health check failed", extra={"extra_data": {"error": str(exc)}})
            return False

    def close(self) -> None:
        """Close the Motor client connection."""
        if self._client is not None:
            self._client.close()
            self._client = None
            logger.info("Closed MongoDB connection")