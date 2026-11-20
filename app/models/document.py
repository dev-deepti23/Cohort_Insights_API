from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, validator


class DocumentStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    ENRICHING = "enriching"
    COMPLETED = "completed"
    FAILED = "failed"


class StageStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


ACTIVE_PIPELINE_STATUSES = {
    DocumentStatus.QUEUED,
    DocumentStatus.PROCESSING,
    DocumentStatus.ENRICHING,
}


class PipelineStages(BaseModel):
    """Tracks independent status of each stage so partial progress is preserved."""

    processing: StageStatus = StageStatus.PENDING
    enriching: StageStatus = StageStatus.PENDING
    failed_stage: Optional[str] = None
    failure_reason: Optional[str] = None


class VersionedSummary(BaseModel):
    """Stage 1 output stamped with the exact content version & hash that produced it."""

    text: str
    version: int
    content_hash: str
    produced_at: datetime


class VersionedTags(BaseModel):
    """Stage 2 output stamped with the exact content version & hash that produced it."""

    items: List[str]
    version: int
    content_hash: str
    produced_at: datetime


class DocumentCreateRequest(BaseModel):
    user_id: str = Field(..., min_length=1, max_length=128)
    title: str = Field(..., min_length=1, max_length=256)
    content: str = Field(..., min_length=1, max_length=200_000)
    client_doc_ref: Optional[str] = Field(default=None, min_length=1, max_length=128)

    @validator("user_id", "title", "content")
    def strip_non_empty(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped:
            raise ValueError("Field cannot be blank or whitespace only.")
        return stripped

    @validator("client_doc_ref")
    def strip_optional_ref(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        stripped = v.strip()
        if not stripped:
            raise ValueError("client_doc_ref cannot be blank when provided.")
        return stripped


class DocumentUpdateRequest(BaseModel):
    content: str = Field(..., min_length=1, max_length=200_000)
    title: Optional[str] = Field(default=None, min_length=1, max_length=256)
    expected_version: Optional[int] = Field(default=None, ge=1)

    @validator("content")
    def strip_content(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped:
            raise ValueError("content cannot be blank or whitespace only.")
        return stripped

    @validator("title")
    def strip_title(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        stripped = v.strip()
        if not stripped:
            raise ValueError("title cannot be blank when provided.")
        return stripped


class DocumentSubmitResponse(BaseModel):
    document_id: str
    status: DocumentStatus
    version: int
    content_hash: str
    client_doc_ref: Optional[str] = None
    cache_hit: bool = False


class DocumentDetailResponse(BaseModel):
    document_id: str
    user_id: str
    title: str
    client_doc_ref: Optional[str] = None
    status: DocumentStatus
    stages: PipelineStages
    content_version: int
    content_hash: str
    result_version: Optional[int] = None
    is_current: bool
    summary: Optional[str] = None
    tags: Optional[List[str]] = None
    created_at: datetime
    updated_at: datetime

class PaginatedDocumentsResponse(BaseModel):
    items: List[DocumentDetailResponse]
    page_size: int
    next_cursor: Optional[str] = None
    has_more: bool = False
    page: Optional[int] = None
    total: int
    total_pages: int