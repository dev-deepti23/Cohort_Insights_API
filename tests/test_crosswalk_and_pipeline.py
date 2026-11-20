import unittest

import httpx

from tests.helpers import build_test_app


class TestCrosswalkAndPipeline(unittest.IsolatedAsyncioTestCase):
    async def test_crosswalk_lookup_idempotency_and_conflict(self) -> None:
        app, _, _, worker = build_test_app()
        async with httpx.AsyncClient(app=app, base_url="http://test") as client:
            payload = {
                "user_id": "alice",
                "title": "CMS Sync",
                "content": "Partner article body",
                "client_doc_ref": "cms-1001",
            }
            r1 = await client.post("/documents", json=payload)
            self.assertEqual(r1.status_code, 201)
            doc_id = r1.json()["document_id"]

            r_retry = await client.post("/documents", json=payload)
            self.assertEqual(r_retry.status_code, 200)
            self.assertEqual(r_retry.json()["document_id"], doc_id)

            r_conflict = await client.post(
                "/documents",
                json={
                    "user_id": "alice",
                    "title": "CMS Sync",
                    "content": "Modified partner article body",
                    "client_doc_ref": "cms-1001",
                },
            )
            self.assertEqual(r_conflict.status_code, 409)

            by_ref = await client.get(
                "/documents/by-ref/cms-1001", headers={"X-User-Id": "alice"}
            )
            self.assertEqual(by_ref.status_code, 200)
            self.assertEqual(by_ref.json()["document_id"], doc_id)

            by_ref_other = await client.get(
                "/documents/by-ref/cms-1001", headers={"X-User-Id": "bob"}
            )
            self.assertEqual(by_ref_other.status_code, 404)

    async def test_stage2_failure_preserves_stage1_and_retries_without_redoing_stage1(self) -> None:
        app, db, _, worker = build_test_app(failure_probability=0.0, max_retries=0)
        outcomes = iter([0.9, 0.1])
        worker._failure_probability = 0.5
        worker._rng = lambda: next(outcomes)

        async with httpx.AsyncClient(app=app, base_url="http://test") as client:
            r = await client.post(
                "/documents",
                json={"user_id": "alice", "title": "Partial", "content": "Cohort retention study"},
            )
            doc_id = r.json()["document_id"]
            await worker.process_job(doc_id, 1)

            detail = (await client.get(f"/documents/{doc_id}", headers={"X-User-Id": "alice"})).json()
            self.assertEqual(detail["status"], "failed")
            self.assertEqual(detail["stages"]["processing"], "completed")
            self.assertEqual(detail["stages"]["enriching"], "failed")
            self.assertEqual(detail["stages"]["failed_stage"], "enriching")
            self.assertFalse(detail["is_current"])

            raw_doc = await db["documents"].find_one({"user_id": "alice"})
            self.assertIsNotNone(raw_doc["summary"])
            saved_summary_time = raw_doc["summary"]["produced_at"]

            worker._rng = lambda: 0.9
            await worker.retry_stage_2(doc_id, 1)

            completed = (
                await client.get(f"/documents/{doc_id}", headers={"X-User-Id": "alice"})
            ).json()
            self.assertEqual(completed["status"], "completed")
            self.assertTrue(completed["is_current"])
            raw_after = await db["documents"].find_one({"user_id": "alice"})
            self.assertEqual(raw_after["summary"]["produced_at"], saved_summary_time)

    async def test_graceful_degradation_when_redis_is_unavailable(self) -> None:
        app, _, _, worker = build_test_app(redis_fail_mode=True)
        async with httpx.AsyncClient(app=app, base_url="http://test") as client:
            r = await client.post(
                "/documents",
                json={"user_id": "alice", "title": "Resilient", "content": "Works without Redis"},
            )
            self.assertEqual(r.status_code, 201)
            doc_id = r.json()["document_id"]
            await worker.process_job(doc_id, 1)

            detail = (await client.get(f"/documents/{doc_id}", headers={"X-User-Id": "alice"})).json()
            self.assertEqual(detail["status"], "completed")
            self.assertTrue(detail["is_current"])