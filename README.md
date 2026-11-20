# Cohort Insights API

Backend service for ingesting documents, running them through a two-stage async processing pipeline (`processing` → `enriching`), and serving results to both submitting users and an external partner system (`client_doc_ref`). Built with Python 3.11, FastAPI, MongoDB, and Redis.

## Running Locally

Start the API, MongoDB, and Redis with one command:

```bash
docker compose up --build
```

The API runs on `http://localhost:8000` (or `8001` if you remapped the port in `docker-compose.yml`), with Swagger docs at `/docs`.

Check dependency health:

```bash
curl http://localhost:8000/health
```

Run the test suite:

```bash
docker compose exec api pytest -v
# or locally:
python3 -m unittest discover -s tests -v
```

## Project Layout

```text
app/
├── main.py                  # FastAPI app, startup/shutdown lifecycle, error handlers
├── core/                    # Env settings (.env.example), JSON logging, domain exceptions
├── db/                      # Motor (MongoDB) + index setup, async Redis client
├── models/document.py       # Pydantic request/response schemas & versioned sub-models
├── repositories/            # MongoDB queries & version-guarded atomic updates
├── services/                # Document service, 2-stage worker, Redis rate limiter & cache
├── api/                     # X-User-Id ownership dependency, /health, /documents routes
└── utils/                   # SHA-256 hashing & ObjectId validation
tests/                       # Unit & integration tests (httpx.AsyncClient + in-memory fakes)
```

## Design Decisions & Assumptions

**Ownership (`X-User-Id`)**
All reads, updates, deletes, listings, and `by-ref` lookups require an `X-User-Id` header. Queries always filter by both the identifier and `user_id`. If a document doesn't exist or belongs to someone else, the API returns `404` rather than `403` so a caller can't probe whether another user's document ID exists.

**Crosswalk (`client_doc_ref`) & Repeat Submissions**
- **Index**: `idx_client_doc_ref_unique` on `documents(client_doc_ref)` with `unique=True` and `partialFilterExpression: {"client_doc_ref": {"$type": "string"}}`. Using a partial index keeps `GET /documents/by-ref/{ref}` fast without causing unique-key collisions when `client_doc_ref` is omitted.
- **Duplicate `client_doc_ref` handling**: If the same user retries with the exact same content (matching `content_hash`), the API treats it as an idempotent retry and returns `200 OK` with the existing document. If the same `client_doc_ref` arrives with different content (or from a different user), it's rejected with `409 Conflict` and points the caller to `PATCH /documents/{document_id}` so out-of-order retries don't silently overwrite newer edits.

**Two-Stage Pipeline & Partial Progress**
- Jobs move through `queued` → `processing` (10–20s) → `enriching` (5–15s) → `completed` | `failed`, with a ~10% simulated failure rate per stage and exponential backoff retries.
- Each stage has its own status inside `stages` (`processing`, `enriching`, `failed_stage`, `failure_reason`). If `enriching` fails, Stage 1's `summary` stays saved in MongoDB, and `PipelineWorker.retry_stage_2` can resume from Stage 2 without redoing Stage 1.

**Redis Usage & Graceful Degradation**
Redis handles two separate jobs:
1. **Rate limiting (`cohort:active_jobs:{user_id}`)**: A Redis Set holding active `document_id`s, updated via a small Lua script (`SISMEMBER` / `SCARD` / `SADD` / `EXPIRE`). Using a Set instead of `INCR`/`DECR` makes `SREM` idempotent on retries and avoids double-counting if a user `PATCH`es a document that's already in flight. More than 3 active documents returns `429`.
2. **Content cache (`cohort:cache:content:{sha256}`)**: Keyed by the SHA-256 hash of `content` (with a 24h TTL), never by `document_id`. When a `PATCH` changes the text, the new hash misses the old cache entry or hits an entry for the new text.
If Redis goes down, both services catch the connection error, log a warning, and degrade cleanly (cache acts as a miss; rate limiter fails open).

**Pagination & Extras**
`GET /users/{user_id}/documents` and `GET /documents` use keyset cursor pagination on `(created_at, _id)` by default (`next_cursor`, `has_more`), while still accepting `page` for backwards compatibility. I also added `GET /documents` (caller-scoped list) and `DELETE /documents/{document_id}` (ownership-checked, releases the Redis quota slot).

## Schema & Staleness Design

Each document in MongoDB has an integer `version` (starting at `1` and incremented atomically on every `PATCH`) and a `content_hash` (SHA-256 of `content`). Derived results aren't stored as plain strings—they're stored as sub-documents stamped with the exact `version` and `content_hash` that produced them:

```json
{
  "_id": "ObjectId(...)",
  "version": 2,
  "content_hash": "sha256_of_v2",
  "status": "processing",
  "stages": { "processing": "running", "enriching": "pending" },
  "summary": { "text": "...", "version": 1, "content_hash": "sha256_of_v1" },
  "tags":    { "items": [...], "version": 1, "content_hash": "sha256_of_v1" }
}
```

Three rules make mismatched or mixed-version reads impossible:

1. **Atomic reset on `PATCH`**: `update_content_in_place` runs a single `find_one_and_update` that increments `version`, swaps `content` and `content_hash`, clears `summary` and `tags`, and resets the pipeline to `queued` (or `completed` on a cache hit, stamped with `new_version`). Callers can also pass `expected_version` for optimistic concurrency (`409` on a lost race).
2. **Version-guarded worker writes**: Every queued job carries `(document_id, job_version)`. Every worker update (`claim_stage_1`, `complete_stage_1`, `complete_stage_2`, `fail_stage`) includes `"version": job_version` in the MongoDB filter. If a `PATCH` bumps `version` from `1` to `2` while a `v1` worker is mid-sleep, the worker's write matches 0 documents and aborts without writing derived fields, updating the cache, or releasing the `v2` rate-limit slot.
3. **Read-time verification**: On `GET`, `to_detail_response` checks that `summary.version == doc.version` and `tags.version == doc.version` (plus `content_hash` equality). If either doesn't match, `summary` and `tags` are returned as `null` and `is_current` is `false`.

## At 100×

At 100× scale, the bottlenecks move from simulated compute to index skew, hot partitions, Redis throughput, and deep pagination.

**1. A single user with 500K documents**
The compound index `(user_id, created_at, _id)` becomes heavily skewed around that one `user_id` prefix. Every new insert or status update for that user contends on the same B-tree leaf pages, slowing down writes and putting pressure on MongoDB's WiredTiger cache. Listing or filtering for that user also touches a much larger slice of the index, which can evict hot index pages belonging to other users.

**2. Sharding / partition key**
Sharding on `user_id` alone would route all 500K documents for that power user onto a single shard, creating a hot shard and a jumbo chunk that MongoDB can't split. Instead, I would use either a hashed shard key on `_id` (`{_id: "hashed"}`) or a compound shard key `{user_id: 1, _id: 1}`. Hashing `_id` spreads writes evenly across all shards regardless of tenant size, while `{user_id: 1, _id: 1}` keeps a user's recent-page queries mostly targeted while still letting MongoDB split a 500K-doc user across multiple chunks by `_id`.

**3. Redis rate limiter at 100× submit QPS**
Using an atomic Lua script on a Redis Set already avoids the `SCARD` + `SADD` race condition under concurrency. At 100× QPS, though, a single Redis node will hit CPU or network saturation. Moving to Redis Cluster shards `cohort:active_jobs:{user_id}` across nodes by `user_id` (with `{user_id}` hash tags so Lua scripts run on a single slot). I'd also add a periodic background sweep that reconciles Redis Sets against MongoDB's active documents so crashed workers can't leave a user's quota stuck.

**4. Skip/limit pagination at 500K documents**
`skip(page * page_size)` degrades linearly because MongoDB still has to walk and discard `skip` index entries before returning the page—jumping to offset 400,000 gets slow fast. Keyset cursor pagination on `(created_at, _id)` avoids this completely by jumping straight to the next index entry in $O(\log N + \text{page\_size})$ time, which is why the listing endpoints already default to cursor pagination.

## What I Would Do Differently With More Time

- **External durable job queue**: Move from the in-process `asyncio.Queue` to Redis Streams (`XREADGROUP` / `XACK`) or `arq` so queued jobs survive API container restarts and workers can scale as separate containers.
- **Retry endpoint for failed Stage 2 jobs**: Expose `POST /documents/{id}/retry` wired to `PipelineWorker.retry_stage_2` so callers can trigger Stage 2 recovery directly via HTTP.
- **Real MongoDB/Redis integration tests in CI**: Run the test suite against live MongoDB and Redis containers in GitHub Actions alongside the fast in-memory fake suite.