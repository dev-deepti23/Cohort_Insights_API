from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.core.exceptions import DocumentNotFoundError
from app.models.document import DocumentStatus, PipelineStages, StageStatus
from app.utils.ids import InvalidId, ObjectId, parse_object_id


class DuplicateRefError(Exception):
    """Raised when a unique client_doc_ref constraint is violated."""


class DocumentRepository:
    """MongoDB persistence layer for documents with version-guarded atomic updates."""

    def __init__(self, database: Any) -> None:
        self._collection = database["documents"]

    @staticmethod
    def _to_oid(document_id: str) -> ObjectId:
        try:
            return parse_object_id(document_id)
        except InvalidId as exc:
            raise DocumentNotFoundError() from exc

    async def create(
        self,
        user_id: str,
        title: str,
        content: str,
        content_hash: str,
        client_doc_ref: Optional[str] = None,
        cached_summary: Optional[Dict[str, Any]] = None,
        cached_tags: Optional[Dict[str, Any]] = None,
        document_id: Optional[ObjectId] = None,
    ) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        is_cached = cached_summary is not None and cached_tags is not None

        doc: Dict[str, Any] = {
            "_id": document_id if document_id is not None else ObjectId(),
            "user_id": user_id,
            "title": title,
            "content": content,
            "content_hash": content_hash,
            "version": 1,
            "status": DocumentStatus.COMPLETED.value if is_cached else DocumentStatus.QUEUED.value,
            "stages": (
                PipelineStages(
                    processing=StageStatus.COMPLETED,
                    enriching=StageStatus.COMPLETED,
                ).dict()
                if is_cached
                else PipelineStages().dict()
            ),
            "summary": cached_summary,
            "tags": cached_tags,
            "created_at": now,
            "updated_at": now,
        }
        if client_doc_ref is not None:
            doc["client_doc_ref"] = client_doc_ref

        try:
            await self._collection.insert_one(doc)
        except Exception as exc:
            if "DuplicateKeyError" in type(exc).__name__ or "E11000" in str(exc):
                raise DuplicateRefError(f"Duplicate client_doc_ref: {client_doc_ref}") from exc
            raise
        return doc

    async def get_by_id_and_owner(self, document_id: str, user_id: str) -> Optional[Dict[str, Any]]:
        oid = self._to_oid(document_id)
        return await self._collection.find_one({"_id": oid, "user_id": user_id})

    async def get_by_id(self, document_id: str) -> Optional[Dict[str, Any]]:
        oid = self._to_oid(document_id)
        return await self._collection.find_one({"_id": oid})

    async def get_by_ref(self, client_doc_ref: str) -> Optional[Dict[str, Any]]:
        return await self._collection.find_one({"client_doc_ref": client_doc_ref})

    async def get_by_ref_and_owner(
        self, client_doc_ref: str, user_id: str
    ) -> Optional[Dict[str, Any]]:
        return await self._collection.find_one(
            {"client_doc_ref": client_doc_ref, "user_id": user_id}
        )

    async def list_by_user(
        self,
        user_id: str,
        page: int,
        page_size: int,
        status: Optional[DocumentStatus] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        query: Dict[str, Any] = {"user_id": user_id}
        if status is not None:
            query["status"] = status.value

        total = await self._collection.count_documents(query)
        skip = (page - 1) * page_size
        cursor = (
            self._collection.find(query)
            .sort([("created_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(page_size)
        )
        items = await cursor.to_list(length=page_size)
        return items, total

    async def update_content_in_place(
        self,
        document_id: str,
        user_id: str,
        new_content: str,
        new_content_hash: str,
        new_title: Optional[str] = None,
        expected_version: Optional[int] = None,
        cached_summary: Optional[Dict[str, Any]] = None,
        cached_tags: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Atomically bump version, replace content, and reset or populate derived fields."""
        oid = self._to_oid(document_id)
        now = datetime.now(timezone.utc)
        is_cached = cached_summary is not None and cached_tags is not None

        query: Dict[str, Any] = {"_id": oid, "user_id": user_id}
        if expected_version is not None:
            query["version"] = expected_version

        set_fields: Dict[str, Any] = {
            "content": new_content,
            "content_hash": new_content_hash,
            "status": DocumentStatus.COMPLETED.value if is_cached else DocumentStatus.QUEUED.value,
            "stages": (
                PipelineStages(
                    processing=StageStatus.COMPLETED,
                    enriching=StageStatus.COMPLETED,
                ).dict()
                if is_cached
                else PipelineStages().dict()
            ),
            "summary": cached_summary,
            "tags": cached_tags,
            "updated_at": now,
        }
        if new_title is not None:
            set_fields["title"] = new_title

        return await self._collection.find_one_and_update(
            query,
            {"$set": set_fields, "$inc": {"version": 1}},
            return_document=True,
        )

    async def claim_stage_1(self, document_id: str, expected_version: int) -> Optional[Dict[str, Any]]:
        oid = self._to_oid(document_id)
        return await self._collection.find_one_and_update(
            {
                "_id": oid,
                "version": expected_version,
                "status": DocumentStatus.QUEUED.value,
                "stages.processing": {"$in": [StageStatus.PENDING.value, StageStatus.FAILED.value]},
            },
            {
                "$set": {
                    "status": DocumentStatus.PROCESSING.value,
                    "stages.processing": StageStatus.RUNNING.value,
                    "stages.failed_stage": None,
                    "stages.failure_reason": None,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
            return_document=True,
        )

    async def complete_stage_1(
        self,
        document_id: str,
        expected_version: int,
        summary_payload: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        oid = self._to_oid(document_id)
        return await self._collection.find_one_and_update(
            {
                "_id": oid,
                "version": expected_version,
                "status": DocumentStatus.PROCESSING.value,
            },
            {
                "$set": {
                    "status": DocumentStatus.ENRICHING.value,
                    "stages.processing": StageStatus.COMPLETED.value,
                    "stages.enriching": StageStatus.RUNNING.value,
                    "summary": summary_payload,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
            return_document=True,
        )

    async def claim_stage_2_retry(
        self, document_id: str, expected_version: int
    ) -> Optional[Dict[str, Any]]:
        """Resume at Stage 2 without redoing Stage 1 when Stage 1 already succeeded."""
        oid = self._to_oid(document_id)
        return await self._collection.find_one_and_update(
            {
                "_id": oid,
                "version": expected_version,
                "status": DocumentStatus.FAILED.value,
                "stages.processing": StageStatus.COMPLETED.value,
                "stages.enriching": StageStatus.FAILED.value,
            },
            {
                "$set": {
                    "status": DocumentStatus.ENRICHING.value,
                    "stages.enriching": StageStatus.RUNNING.value,
                    "stages.failed_stage": None,
                    "stages.failure_reason": None,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
            return_document=True,
        )

    async def complete_stage_2(
        self,
        document_id: str,
        expected_version: int,
        tags_payload: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        oid = self._to_oid(document_id)
        return await self._collection.find_one_and_update(
            {
                "_id": oid,
                "version": expected_version,
                "status": DocumentStatus.ENRICHING.value,
            },
            {
                "$set": {
                    "status": DocumentStatus.COMPLETED.value,
                    "stages.enriching": StageStatus.COMPLETED.value,
                    "tags": tags_payload,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
            return_document=True,
        )

    async def fail_stage(
        self,
        document_id: str,
        expected_version: int,
        stage_name: str,
        reason: str,
    ) -> Optional[Dict[str, Any]]:
        oid = self._to_oid(document_id)
        return await self._collection.find_one_and_update(
            {
                "_id": oid,
                "version": expected_version,
            },
            {
                "$set": {
                    "status": DocumentStatus.FAILED.value,
                    f"stages.{stage_name}": StageStatus.FAILED.value,
                    "stages.failed_stage": stage_name,
                    "stages.failure_reason": reason,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
            return_document=True,
        )

    async def delete_by_id_and_owner(self, document_id: str, user_id: str) -> bool:
        """Delete a document only if owned by user_id."""
        oid = self._to_oid(document_id)
        result = await self._collection.delete_one({"_id": oid, "user_id": user_id})
        return result.deleted_count > 0
