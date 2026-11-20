import unittest

import httpx

from tests.helpers import build_test_app


class TestStalenessAndCache(unittest.IsolatedAsyncioTestCase):
    async def test_patch_mid_reprocessing_never_exposes_stale_or_mixed_results(self) -> None:
        app, _, _, worker = build_test_app()
        async with httpx.AsyncClient(app=app, base_url="http://test") as client:
            r = await client.post(
                "/documents",
                json={"user_id": "alice", "title": "Report", "content": "Original alpha revenue"},
            )
            doc_id = r.json()["document_id"]
            await worker.process_job(doc_id, 1)

            v1 = (await client.get(f"/documents/{doc_id}", headers={"X-User-Id": "alice"})).json()
            self.assertEqual(v1["content_version"], 1)
            self.assertEqual(v1["result_version"], 1)
            self.assertTrue(v1["is_current"])
            self.assertIn("Original alpha revenue", v1["summary"])

            patch_res = await client.patch(
                f"/documents/{doc_id}",
                headers={"X-User-Id": "alice"},
                json={"content": "Updated beta retention"},
            )
            self.assertEqual(patch_res.status_code, 200)
            self.assertEqual(patch_res.json()["version"], 2)
            self.assertEqual(patch_res.json()["status"], "queued")

            mid = (await client.get(f"/documents/{doc_id}", headers={"X-User-Id": "alice"})).json()
            self.assertEqual(mid["content_version"], 2)
            self.assertIsNone(mid["result_version"])
            self.assertFalse(mid["is_current"])
            self.assertIsNone(mid["summary"])
            self.assertIsNone(mid["tags"])

            # Stale v1 worker write must be discarded
            await worker.process_job(doc_id, 1)
            after_stale_worker = (
                await client.get(f"/documents/{doc_id}", headers={"X-User-Id": "alice"})
            ).json()
            self.assertEqual(after_stale_worker["status"], "queued")
            self.assertEqual(after_stale_worker["content_version"], 2)
            self.assertIsNone(after_stale_worker["summary"])

            # v2 worker completes cleanly
            await worker.process_job(doc_id, 2)
            v2 = (await client.get(f"/documents/{doc_id}", headers={"X-User-Id": "alice"})).json()
            self.assertEqual(v2["status"], "completed")
            self.assertEqual(v2["content_version"], 2)
            self.assertEqual(v2["result_version"], 2)
            self.assertTrue(v2["is_current"])
            self.assertIn("Updated beta retention", v2["summary"])

    async def test_optimistic_concurrency_on_racing_patches(self) -> None:
        app, _, _, _ = build_test_app()
        async with httpx.AsyncClient(app=app, base_url="http://test") as client:
            r = await client.post(
                "/documents",
                json={"user_id": "alice", "title": "Race", "content": "v1 content"},
            )
            doc_id = r.json()["document_id"]

            p1 = await client.patch(
                f"/documents/{doc_id}",
                headers={"X-User-Id": "alice"},
                json={"content": "v2 content", "expected_version": 1},
            )
            self.assertEqual(p1.status_code, 200)

            p2 = await client.patch(
                f"/documents/{doc_id}",
                headers={"X-User-Id": "alice"},
                json={"content": "conflicting content", "expected_version": 1},
            )
            self.assertEqual(p2.status_code, 409)

    async def test_content_cache_keyed_by_hash_survives_patch(self) -> None:
        app, _, _, worker = build_test_app()
        async with httpx.AsyncClient(app=app, base_url="http://test") as client:
            r1 = await client.post(
                "/documents",
                json={"user_id": "alice", "title": "Doc1", "content": "Shared content A"},
            )
            doc1_id = r1.json()["document_id"]
            await worker.process_job(doc1_id, 1)

            r2 = await client.post(
                "/documents",
                json={"user_id": "alice", "title": "Doc2", "content": "Different content B"},
            )
            doc2_id = r2.json()["document_id"]
            await worker.process_job(doc2_id, 1)

            p = await client.patch(
                f"/documents/{doc2_id}",
                headers={"X-User-Id": "alice"},
                json={"content": "Shared content A"},
            )
            self.assertEqual(p.status_code, 200)
            self.assertTrue(p.json()["cache_hit"])
            self.assertEqual(p.json()["status"], "completed")
            self.assertEqual(p.json()["version"], 2)

            d2 = (await client.get(f"/documents/{doc2_id}", headers={"X-User-Id": "alice"})).json()
            self.assertTrue(d2["is_current"])
            self.assertEqual(d2["result_version"], 2)
            self.assertIn("Shared content A", d2["summary"])