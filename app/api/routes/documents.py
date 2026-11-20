from typing import Optional

from fastapi import APIRouter, Depends, Header, Query, Response, status

from app.api.dependencies import get_current_user_id, get_document_service
from app.core.exceptions import DocumentNotFoundError
from app.models.document import (
    DocumentCreateRequest,
    DocumentDetailResponse,
    DocumentStatus,
    DocumentSubmitResponse,
    DocumentUpdateRequest,
    PaginatedDocumentsResponse,
)
from app.services.document_service import DocumentService

router = APIRouter(tags=["documents"])


@router.post(
    "/documents",
    response_model=DocumentSubmitResponse,
    status_code=status.HTTP_201_CREATED,
)
async def submit_document(
    payload: DocumentCreateRequest,
    response: Response,
    x_user_id: Optional[str] = Header(default=None, alias="X-User-Id"),
    service: DocumentService = Depends(get_document_service),
) -> DocumentSubmitResponse:
    """Submit a document for two-stage processing."""
    if x_user_id is not None and x_user_id.strip() != payload.user_id:
        raise DocumentNotFoundError()

    result, is_replay = await service.submit_document(payload)
    if is_replay:
        response.status_code = status.HTTP_200_OK
    return result

@router.get(
    "/documents",
    response_model=PaginatedDocumentsResponse,
)
async def list_my_documents(
    cursor: Optional[str] = Query(default=None, description="Opaque keyset pagination cursor"),
    page: Optional[int] = Query(default=None, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    status_filter: Optional[DocumentStatus] = Query(default=None, alias="status"),
    current_user_id: str = Depends(get_current_user_id),
    service: DocumentService = Depends(get_document_service),
) -> PaginatedDocumentsResponse:
    """List all documents owned by the caller (X-User-Id), newest first."""
    return await service.list_user_documents(
        user_id=current_user_id,
        page_size=page_size,
        cursor=cursor,
        page=page,
        status=status_filter,
    )


@router.get(
    "/documents/by-ref/{client_doc_ref}",
    response_model=DocumentDetailResponse,
)
async def get_document_by_ref(
    client_doc_ref: str,
    current_user_id: str = Depends(get_current_user_id),
    service: DocumentService = Depends(get_document_service),
) -> DocumentDetailResponse:
    """Lookup a document by its external partner reference ID (indexed)."""
    return await service.get_document_by_ref(client_doc_ref, current_user_id)


@router.get(
    "/documents/{document_id}",
    response_model=DocumentDetailResponse,
)
async def get_document(
    document_id: str,
    current_user_id: str = Depends(get_current_user_id),
    service: DocumentService = Depends(get_document_service),
) -> DocumentDetailResponse:
    """Poll document status and retrieve current summary/tags if up to date."""
    return await service.get_document(document_id, current_user_id)


@router.patch(
    "/documents/{document_id}",
    response_model=DocumentSubmitResponse,
)
async def update_document(
    document_id: str,
    payload: DocumentUpdateRequest,
    current_user_id: str = Depends(get_current_user_id),
    service: DocumentService = Depends(get_document_service),
) -> DocumentSubmitResponse:
    """Replace document content in place and trigger fresh two-stage reprocessing."""
    return await service.update_document(document_id, current_user_id, payload)


@router.delete(
    "/documents/{document_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_document(
    document_id: str,
    current_user_id: str = Depends(get_current_user_id),
    service: DocumentService = Depends(get_document_service),
) -> Response:
    """Delete a document owned by the caller (X-User-Id) and release its quota slot."""
    await service.delete_document(document_id, current_user_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    "/users/{user_id}/documents",
    response_model=PaginatedDocumentsResponse,
)
async def list_user_documents(
    user_id: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    status_filter: Optional[DocumentStatus] = Query(default=None, alias="status"),
    current_user_id: str = Depends(get_current_user_id),
    service: DocumentService = Depends(get_document_service),
) -> PaginatedDocumentsResponse:
    """List paginated documents for a user, newest first, with optional status filter."""
    if current_user_id != user_id:
        raise DocumentNotFoundError()
    return await service.list_user_documents(
        user_id=user_id,
        page=page,
        page_size=page_size,
        status=status_filter,
    )

