from typing import Optional


class DomainError(Exception):
    """Base exception for domain/business logic errors."""

    def __init__(self, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class DocumentNotFoundError(DomainError):
    """Raised when a document does not exist or is not owned by the caller."""

    def __init__(self, message: str = "Document not found.") -> None:
        super().__init__(message=message, status_code=404)


class RateLimitExceededError(DomainError):
    """Raised when a user exceeds the concurrent active-pipeline limit."""

    def __init__(self, limit: int) -> None:
        super().__init__(
            message=f"Rate limit exceeded: maximum {limit} active documents allowed concurrently.",
            status_code=429,
        )


class CrosswalkConflictError(DomainError):
    """Raised when a repeat client_doc_ref arrives with different content."""

    def __init__(self, client_doc_ref: str, document_id: Optional[str] = None) -> None:
        detail = (
            f"client_doc_ref '{client_doc_ref}' is already registered with different content."
        )
        if document_id:
            detail += f" Use PATCH /documents/{document_id} to update it."
        super().__init__(message=detail, status_code=409)


class OptimisticConcurrencyError(DomainError):
    """Raised when a PATCH expected_version does not match the current document version."""

    def __init__(self, expected: int, actual: int) -> None:
        super().__init__(
            message=(
                f"Version conflict: expected version {expected}, "
                f"but current document version is {actual}."
            ),
            status_code=409,
        )