from datetime import datetime, timezone
import logging
import math
from typing import Any, Callable, Coroutine, Dict, Optional, Tuple

from app.core.exceptions import (
    CrosswalkConflictError,
    DocumentNotFoundError,
    OptimisticConcurrencyError,
    RateLimitExceededError,
)
from app.models.document import (
    DocumentCreateRequest,
    DocumentDetailResponse,
    DocumentStatus,
    DocumentSubmitResponse,
    DocumentUpdateRequest,
    PaginatedDocumentsResponse,
    PipelineStages,
)
from app.repositories.document_repo import DocumentRepository, DuplicateRefError
from app.services.cache_service import ContentCacheService
from app.services.rate_limiter import RateLimiterService
from app.utils.hashing import compute_content_hash
from app.utils.ids import ObjectId

logger = logging.getLogger(__name__)

EnqueueFn = Callable[[str, int], Coroutine[Any, Any, None]]


class DocumentService:
    """Core business logic for document submission, updates, crosswalk, and staleness-safe reads."""

    def __init__(
        self,
        repo: DocumentRepository,
        cache: ContentCacheService,
        rate_limiter: RateLimiterService,
        enqueue_fn: EnqueueFn,
        max_active_docs: int = 3,
    ) -> None:
        self._repo = repo
        self._cache = cache
        self._rate_limiter = rate_limiter
        self._enqueue = enqueue_fn
        self._max_active_docs = max_active_docs

    @staticmethod
    def _build_versioned_outputs(
        summary_text: str,
        tags: list,
        version: int,
        content_hash: str,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        summary_payload = {
            "text": summary_text,
            "version": version,
            "content_hash": content_hash,
            "produced_at": now,
        }
        tags_payload = {
            "items": tags,
            "version": version,
            "content_hash": content_hash,
            "produced_at": now,
        }
        return summary_payload, tags_payload

    @staticmethod
    def to_detail_response(doc: Dict[str, Any]) -> DocumentDetailResponse:
        """Project a MongoDB document into a staleness-safe API response."""
        current_version = doc["version"]
        summary_obj = doc.get("summary")
        tags_obj = doc.get("tags")

        summary_is_current = (
            summary_obj is not None
            and summary_obj.get("version") == current_version
            and summary_obj.get("content_hash") == doc["content_hash"]
        )
        tags_is_current = (
            tags_obj is not None
            and tags_obj.get("version") == current_version
            and tags_obj.get("content_hash") == doc["content_hash"]
        )

        exposed_summary = summary_obj["text"] if summary_is_current else None
        exposed_tags = tags_obj["items"] if tags_is_current else None

        result_version: Optional[int] = None
        if summary_is_current and tags_is_current:
            result_version = current_version
        elif summary_is_current:
            result_version = summary_obj.get("version")

        is_current = (
            doc["status"] == DocumentStatus.COMPLETED.value
            and summary_is_current
            and tags_is_current
        )

        return DocumentDetailResponse(
            document_id=str(doc["_id"]),
            user_id=doc["user_id"],
            title=doc["title"],
            client_doc_ref=doc.get("client_doc_ref"),
            status=DocumentStatus(doc["status"]),
            stages=PipelineStages(**doc["stages"]),
            content_version=current_version,
            content_hash=doc["content_hash"],
            result_version=result_version,
            is_current=is_current,
            summary=exposed_summary,
            tags=exposed_tags,
            created_at=doc["created_at"],
            updated_at=doc["updated_at"],
        )

    async def submit_document(
        self, request: DocumentCreateRequest
    ) -> Tuple[DocumentSubmitResponse, bool]:
        """Submit a new document, handling crosswalk idempotency/conflict, cache hits, and rate limits."""
        content_hash = compute_content_hash(request.content)

        if request.client_doc_ref is not None:
            existing = await self._repo.get_by_ref(request.client_doc_ref)
            if existing is not None:
                if existing["user_id"] != request.user_id:
                    raise CrosswalkConflictError(request.client_doc_ref)
                if existing["content_hash"] == content_hash:
                    return (
                        DocumentSubmitResponse(
                            document_id=str(existing["_id"]),
                            status=DocumentStatus(existing["status"]),
                            version=existing["version"],
                            content_hash=existing["content_hash"],
                            client_doc_ref=existing.get("client_doc_ref"),
                            cache_hit=False,
                        ),
                        True,
                    )
                raise CrosswalkConflictError(
                    request.client_doc_ref, document_id=str(existing["_id"])
                )

        cached = await self._cache.get(content_hash)
        if cached is not None:
            summary_payload, tags_payload = self._build_versioned_outputs(
                cached.summary_text, cached.tags, version=1, content_hash=content_hash
            )
            try:
                doc = await self._repo.create(
                    user_id=request.user_id,
                    title=request.title,
                    content=request.content,
                    content_hash=content_hash,
                    client_doc_ref=request.client_doc_ref,
                    cached_summary=summary_payload,
                    cached_tags=tags_payload,
                )
            except DuplicateRefError as exc:
                return await self._handle_race_duplicate_ref(request, content_hash, exc)

            return (
                DocumentSubmitResponse(
                    document_id=str(doc["_id"]),
                    status=DocumentStatus.COMPLETED,
                    version=1,
                    content_hash=content_hash,
                    client_doc_ref=doc.get("client_doc_ref"),
                    cache_hit=True,
                ),
                False,
            )

        new_oid = ObjectId()
        doc_id_str = str(new_oid)
        acquired = await self._rate_limiter.acquire_slot(request.user_id, doc_id_str)
        if not acquired:
            raise RateLimitExceededError(self._max_active_docs)

        try:
            doc = await self._repo.create(
                user_id=request.user_id,
                title=request.title,
                content=request.content,
                content_hash=content_hash,
                client_doc_ref=request.client_doc_ref,
                document_id=new_oid,
            )
        except DuplicateRefError as exc:
            await self._rate_limiter.release_slot(request.user_id, doc_id_str)
            return await self._handle_race_duplicate_ref(request, content_hash, exc)
        except Exception:
            await self._rate_limiter.release_slot(request.user_id, doc_id_str)
            raise

        await self._enqueue(doc_id_str, 1)
        return (
            DocumentSubmitResponse(
                document_id=doc_id_str,
                status=DocumentStatus.QUEUED,
                version=1,
                content_hash=content_hash,
                client_doc_ref=doc.get("client_doc_ref"),
                cache_hit=False,
            ),
            False,
        )

    async def _handle_race_duplicate_ref(
        self,
        request: DocumentCreateRequest,
        content_hash: str,
        exc: DuplicateRefError,
    ) -> Tuple[DocumentSubmitResponse, bool]:
        if request.client_doc_ref is None:
            raise exc
        existing = await self._repo.get_by_ref(request.client_doc_ref)
        if existing is not None and existing["user_id"] == request.user_id:
            if existing["content_hash"] == content_hash:
                return (
                    DocumentSubmitResponse(
                        document_id=str(existing["_id"]),
                        status=DocumentStatus(existing["status"]),
                        version=existing["version"],
                        content_hash=existing["content_hash"],
                        client_doc_ref=existing.get("client_doc_ref"),
                        cache_hit=False,
                    ),
                    True,
                )
            raise CrosswalkConflictError(
                request.client_doc_ref, document_id=str(existing["_id"])
            ) from exc
        raise CrosswalkConflictError(request.client_doc_ref) from exc

    async def update_document(
        self,
        document_id: str,
        user_id: str,
        request: DocumentUpdateRequest,
    ) -> DocumentSubmitResponse:
        """Replace content in-place, bump version atomically, and reprocess or serve from cache."""
        existing = await self._repo.get_by_id_and_owner(document_id, user_id)
        if existing is None:
            raise DocumentNotFoundError()

        if (
            request.expected_version is not None
            and existing["version"] != request.expected_version
        ):
            raise OptimisticConcurrencyError(request.expected_version, existing["version"])

        current_version = existing["version"]
        new_version = current_version + 1
        new_hash = compute_content_hash(request.content)

        cached = await self._cache.get(new_hash)
        if cached is not None:
            summary_payload, tags_payload = self._build_versioned_outputs(
                cached.summary_text, cached.tags, version=new_version, content_hash=new_hash
            )
            updated = await self._repo.update_content_in_place(
                document_id=document_id,
                user_id=user_id,
                new_content=request.content,
                new_content_hash=new_hash,
                new_title=request.title,
                expected_version=current_version,
                cached_summary=summary_payload,
                cached_tags=tags_payload,
            )
            if updated is None:
                latest = await self._repo.get_by_id_and_owner(document_id, user_id)
                actual = latest["version"] if latest else current_version
                raise OptimisticConcurrencyError(current_version, actual)

            await self._rate_limiter.release_slot(user_id, document_id)
            return DocumentSubmitResponse(
                document_id=document_id,
                status=DocumentStatus.COMPLETED,
                version=updated["version"],
                content_hash=new_hash,
                client_doc_ref=updated.get("client_doc_ref"),
                cache_hit=True,
            )

        acquired = await self._rate_limiter.acquire_slot(user_id, document_id)
        if not acquired:
            raise RateLimitExceededError(self._max_active_docs)

        updated = await self._repo.update_content_in_place(
            document_id=document_id,
            user_id=user_id,
            new_content=request.content,
            new_content_hash=new_hash,
            new_title=request.title,
            expected_version=current_version,
        )
        if updated is None:
            latest = await self._repo.get_by_id_and_owner(document_id, user_id)
            actual = latest["version"] if latest else current_version
            raise OptimisticConcurrencyError(current_version, actual)

        await self._enqueue(document_id, updated["version"])
        return DocumentSubmitResponse(
            document_id=document_id,
            status=DocumentStatus.QUEUED,
            version=updated["version"],
            content_hash=new_hash,
            client_doc_ref=updated.get("client_doc_ref"),
            cache_hit=False,
        )

    async def get_document(self, document_id: str, user_id: str) -> DocumentDetailResponse:
        doc = await self._repo.get_by_id_and_owner(document_id, user_id)
        if doc is None:
            raise DocumentNotFoundError()
        return self.to_detail_response(doc)

    async def get_document_by_ref(
        self, client_doc_ref: str, user_id: str
    ) -> DocumentDetailResponse:
        doc = await self._repo.get_by_ref_and_owner(client_doc_ref, user_id)
        if doc is None:
            raise DocumentNotFoundError()
        return self.to_detail_response(doc)

    async def list_user_documents(
        self,
        user_id: str,
        page: int,
        page_size: int,
        status: Optional[DocumentStatus] = None,
    ) -> PaginatedDocumentsResponse:
        docs, total = await self._repo.list_by_user(
            user_id=user_id, page=page, page_size=page_size, status=status
        )
        total_pages = math.ceil(total / page_size) if total > 0 else 0
        return PaginatedDocumentsResponse(
            items=[self.to_detail_response(d) for d in docs],
            page=page,
            page_size=page_size,
            total=total,
            total_pages=total_pages,
        )

    async def delete_document(self, document_id: str, user_id: str) -> None:
        """Delete a document owned by user_id and release any active rate-limit slot."""
        deleted = await self._repo.delete_by_id_and_owner(document_id, user_id)
        if not deleted:
            raise DocumentNotFoundError()
        await self._rate_limiter.release_slot(user_id, document_id)
