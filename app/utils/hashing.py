import hashlib


def compute_content_hash(content: str) -> str:
    """Compute a deterministic SHA-256 hex digest for document content."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()