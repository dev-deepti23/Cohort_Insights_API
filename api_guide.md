# Cohort Insights API — Endpoint Reference

Base URL: `http://localhost:8000` (or `http://localhost:8001` if port-mapped)

All document read, update, delete, listing, and `by-ref` endpoints require the `X-User-Id` header for ownership enforcement. Accessing another user's document returns `404 Not Found`.

---

## 1. Health Check

### `GET /health`
- **Purpose**: Verifies connectivity to MongoDB and Redis.
- **Headers**: None
- **Responses**:
  - `200 OK`:
    ```json
    {
      "status": "ok",
      "dependencies": {
        "mongodb": "up",
        "redis": "up"
      }
    }
    ```
  - `503 Service Unavailable`:
    ```json
    {
      "status": "degraded",
      "dependencies": {
        "mongodb": "up",
        "redis": "down"
      }
    }
    ```

---

## 2. Submit a Document

### `POST /documents`
- **Purpose**: Validates input, checks `client_doc_ref` crosswalk and SHA-256 content cache, enforces the per-user active-job rate limit (max 3), stores the document in MongoDB, and enqueues it for the two-stage pipeline.
- **Headers**: `Content-Type: application/json` (optional `X-User-Id`, must match `user_id` if provided)
- **Request Payload**:
  ```json
  {
    "user_id": "alice",
    "title": "Q3 Retention Report",
    "content": "Cohort retention grew 15% in Q3 across enterprise accounts.",
    "client_doc_ref": "cms-101"
  }
  ```
  *(Note: `client_doc_ref` is optional.)*
- **Responses**:
  - `201 Created` (new document queued, or immediate `completed` on cache hit):
    ```json
    {
      "document_id": "673b9f12c4e1a2b3c4d5e6f7",
      "status": "queued",
      "version": 1,
      "content_hash": "a1b2c3...",
      "client_doc_ref": "cms-101",
      "cache_hit": false
    }
    ```
  - `200 OK`: Idempotent retry (same `client_doc_ref`, same `user_id`, identical `content_hash`).
  - `409 Conflict`: Same `client_doc_ref` already exists with different content (or belongs to another user).
  - `429 Too Many Requests`: User already has 3 documents in `{queued, processing, enriching}`.

---

## 3. Poll Document Status

### `GET /documents/{document_id}`
- **Purpose**: Returns the document's progress across both stages (`processing`, `enriching`), version/staleness metadata, and `summary`/`tags` only when they match the current `content_version`.
- **Headers**: `X-User-Id: alice`
- **Responses**:
  - `200 OK` (while reprocessing / not yet current):
    ```json
    {
      "document_id": "673b9f12c4e1a2b3c4d5e6f7",
      "user_id": "alice",
      "title": "Q3 Retention Report",
      "client_doc_ref": "cms-101",
      "status": "enriching",
      "stages": {
        "processing": "completed",
        "enriching": "running",
        "failed_stage": null,
        "failure_reason": null
      },
      "content_version": 2,
      "content_hash": "d4e5f6...",
      "result_version": null,
      "is_current": false,
      "summary": null,
      "tags": null,
      "created_at": "2026-11-20T12:00:00Z",
      "updated_at": "2026-11-20T12:00:15Z"
    }
    ```
  - `200 OK` (once completed): `status: "completed"`, `result_version: 2`, `is_current: true`, and `summary` + `tags` populated.
  - `404 Not Found`: Document does not exist, ID is invalid, or document belongs to another user.

---

## 4. Update a Document

### `PATCH /documents/{document_id}`
- **Purpose**: Replaces `content` (and optionally `title`) in place under the same `document_id`, atomically increments `version`, clears stale derived outputs, and triggers fresh two-stage reprocessing (or serves immediately from cache).
- **Headers**: `X-User-Id: alice`, `Content-Type: application/json`
- **Request Payload**:
  ```json
  {
    "content": "Updated Q3 numbers: churn dropped 4%.",
    "title": "Q3 Retention Report (Revised)",
    "expected_version": 1
  }
  ```
  *(Note: `title` and `expected_version` are optional; `expected_version` enables optimistic concurrency.)*
- **Responses**:
  - `200 OK`:
    ```json
    {
      "document_id": "673b9f12c4e1a2b3c4d5e6f7",
      "status": "queued",
      "version": 2,
      "content_hash": "d4e5f6...",
      "client_doc_ref": "cms-101",
      "cache_hit": false
    }
    ```
  - `404 Not Found`: Document missing or not owned by caller.
  - `409 Conflict`: `expected_version` does not match the current version in MongoDB.
  - `429 Too Many Requests`: User already has 3 other active documents.

---

## 5. Lookup by External Reference

### `GET /documents/by-ref/{client_doc_ref}`
- **Purpose**: Looks up a document by the external partner CMS reference (`client_doc_ref`) using the unique partial index `idx_client_doc_ref_unique`.
- **Headers**: `X-User-Id: alice`
- **Responses**:
  - `200 OK`: Same schema as `GET /documents/{document_id}`.
  - `404 Not Found`: Reference does not exist or belongs to another user.

---

## 6. List User Documents

### `GET /users/{user_id}/documents`
- **Purpose**: Lists a user's documents newest-first using keyset cursor pagination by default (with optional `page` fallback and `status` filter).
- **Headers**: `X-User-Id: alice` (must match `{user_id}`)
- **Query Parameters**:
  - `cursor` (optional `string`): Opaque token from `next_cursor`
  - `page_size` (optional `int`, `1–100`, default `20`)
  - `page` (optional `int`, `>= 1`)
  - `status` (optional): `queued | processing | enriching | completed | failed`
- **Responses**:
  - `200 OK`:
    ```json
    {
      "items": [
        {
          "document_id": "673b9f12c4e1a2b3c4d5e6f7",
          "user_id": "alice",
          "title": "Q3 Retention Report",
          "client_doc_ref": "cms-101",
          "status": "completed",
          "stages": {
            "processing": "completed",
            "enriching": "completed",
            "failed_stage": null,
            "failure_reason": null
          },
          "content_version": 1,
          "content_hash": "a1b2c3...",
          "result_version": 1,
          "is_current": true,
          "summary": "[Q3 Retention Report] (9 words): Cohort retention grew...",
          "tags": ["cohort", "retention", "grew"],
          "created_at": "2026-11-20T12:00:00Z",
          "updated_at": "2026-11-20T12:00:25Z"
        }
      ],
      "page_size": 20,
      "next_cursor": "eyJ0cyI6ICIyMDI2...",
      "has_more": true,
      "page": null,
      "total": 100,
      "total_pages": 5
    }
    ```
  - `400 Bad Request`: Malformed `cursor`.
  - `404 Not Found`: `X-User-Id` does not match `{user_id}`.

---

## 7. List Caller Documents (Convenience Endpoint)

### `GET /documents`
- **Purpose**: Lists all documents owned by the authenticated caller (`X-User-Id`), newest-first, using the same cursor pagination and `status` filter as `GET /users/{user_id}/documents`.
- **Headers**: `X-User-Id: alice`
- **Query Parameters**: `cursor`, `page_size`, `page`, `status`
- **Responses**: Same `200 OK` / `400 Bad Request` schema as `GET /users/{user_id}/documents`.

---

## 8. Delete a Document

### `DELETE /documents/{document_id}`
- **Purpose**: Deletes a document owned by the caller and releases its Redis active-job rate-limit slot if it was still processing.
- **Headers**: `X-User-Id: alice`
- **Responses**:
  - `204 No Content`: Document deleted.
  - `404 Not Found`: Document does not exist or belongs to another user.