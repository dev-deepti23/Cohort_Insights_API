from typing import Optional

from fastapi import Header, HTTPException, Request, status

from app.services.document_service import DocumentService


def get_current_user_id(
    x_user_id: Optional[str] = Header(default=None, alias="X-User-Id"),
) -> str:
    """Extract and validate the caller's user_id from the X-User-Id header."""
    if not x_user_id or not x_user_id.strip():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or empty X-User-Id header.",
        )
    return x_user_id.strip()


def get_document_service(request: Request) -> DocumentService:
    """Retrieve the DocumentService instance from application state."""
    return request.app.state.document_service