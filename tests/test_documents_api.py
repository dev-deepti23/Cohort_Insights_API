import asyncio
import unittest

import httpx

from tests.helpers import build_test_app


class TestDocumentsAPI(unittest.IsolatedAsyncioTestCase):
    async def test_health_endpoint_ok_and_degraded(self) -> None:
        app, _, _, _ = build_test_app()
        async with httpx.AsyncClient(app=app, base_url="http://test") as client:
            resp = await client.get("/health")
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json()["status"], "ok")

            app.state.redis_manager.ok = False
            degraded = await client.get("/health")
            self.assertEqual(degraded.status_code, 503)
            self.assertEqual(degraded.json()["dependencies"]["redis"], "down")

    async def test_submit_and_ownership_isolation(self) -> None:
        app, _, _, worker = build_test_app()
        async with httpx.AsyncClient(app=app, base_url="http://test") as client:
            res = await client.post(
                "/documents",
                json={"user_id": "alice", "title": "Doc A", "content": "Cohort metrics grew 18%"},
            )
            self.assertEqual(res.status_code, 201)
            doc_id = res.json()["document_id"]
            self.assertEqual(res.json()["status"], "queued")

            await worker.process_job(doc_id, 1)

            alice_get = await client.get(f"/documents/{doc_id}", headers={"X-User-Id": "alice"})
            self.assertEqual(alice_get.status_code, 200)
            body = alice_get.json()
            self.assertEqual(body["status"], "completed")
            self.assertTrue(body["is_current"])
            self.assertIsNotNone(body["summary"])
            self.assertTrue(len(body["tags"]) > 0)

            bob_get = await client.get(f"/documents/{doc_id}", headers={"X-User-Id": "bob"})
            self.assertEqual(bob_get.status_code, 404)

            missing = await client.get(
                "/documents/000000000000000000000000", headers={"X-User-Id": "alice"}
            )
            self.assertEqual(missing.status_code, 404)

    async def test_per_user_rate_limit_429_and_release_on_completion(self) -> None:
        app, _, _, worker = build_test_app(max_active_docs=3)
        async with httpx.AsyncClient(app=app, base_url="http://test") as client:
            doc_ids = []
            for i in range(3):
                r = await client.post(
                    "/documents",
                    json={"user_id": "alice", "title": f"T{i}", "content": f"Unique body {i}"},
                )
                self.assertEqual(r.status_code, 201)
                doc_ids.append(r.json()["document_id"])

            r4 = await client.post(
                "/documents",
                json={"user_id": "alice", "title": "T4", "content": "Unique body 4"},
            )
            self.assertEqual(r4.status_code, 429)

            bob_r = await client.post(
                "/documents",
                json={"user_id": "bob", "title": "Bob1", "content": "Bob unique body"},
            )
            self.assertEqual(bob_r.status_code, 201)

            await worker.process_job(doc_ids[0], 1)
            r5 = await client.post(
                "/documents",
                json={"user_id": "alice", "title": "T5", "content": "Unique body 5"},
            )
            self.assertEqual(r5.status_code, 201)

    async def test_list_user_documents_pagination_and_filter(self) -> None:
        app, _, _, worker = build_test_app()
        async with httpx.AsyncClient(app=app, base_url="http://test") as client:
            ids = []
            for i in range(3):
                r = await client.post(
                    "/documents",
                    json={"user_id": "alice", "title": f"Doc {i}", "content": f"Content {i}"},
                )
                ids.append(r.json()["document_id"])
                await asyncio.sleep(0.002)

            await worker.process_job(ids[0], 1)

            page1 = await client.get(
                "/users/alice/documents?page=1&page_size=2",
                headers={"X-User-Id": "alice"},
            )
            self.assertEqual(page1.status_code, 200)
            p1_data = page1.json()
            self.assertEqual(p1_data["total"], 3)
            self.assertEqual(p1_data["total_pages"], 2)
            self.assertEqual([d["title"] for d in p1_data["items"]], ["Doc 2", "Doc 1"])

            filtered = await client.get(
                "/users/alice/documents?status=completed",
                headers={"X-User-Id": "alice"},
            )
            self.assertEqual(filtered.status_code, 200)
            self.assertEqual(filtered.json()["total"], 1)
            self.assertEqual(filtered.json()["items"][0]["title"], "Doc 0")

            forbidden_list = await client.get(
                "/users/alice/documents",
                headers={"X-User-Id": "bob"},
            )
            self.assertEqual(forbidden_list.status_code, 404)


    async def test_delete_document_enforces_ownership_and_releases_rate_limit(self) -> None:
        app, _, _, _ = build_test_app(max_active_docs=1)
        async with httpx.AsyncClient(app=app, base_url="http://test") as client:
            r = await client.post(
                "/documents",
                json={"user_id": "alice", "title": "Temp", "content": "Delete me"},
            )
            doc_id = r.json()["document_id"]

            bob_del = await client.delete(f"/documents/{doc_id}", headers={"X-User-Id": "bob"})
            self.assertEqual(bob_del.status_code, 404)

            alice_del = await client.delete(f"/documents/{doc_id}", headers={"X-User-Id": "alice"})
            self.assertEqual(alice_del.status_code, 204)

            after_get = await client.get(f"/documents/{doc_id}", headers={"X-User-Id": "alice"})
            self.assertEqual(after_get.status_code, 404)

            r2 = await client.post(
                "/documents",
                json={"user_id": "alice", "title": "New", "content": "Fresh content"},
            )
            self.assertEqual(r2.status_code, 201)