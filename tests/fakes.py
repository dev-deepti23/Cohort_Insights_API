import copy
import sys
import types
from typing import Any, Dict, List, Optional, Tuple


def install_driver_stubs_if_missing() -> None:
    """Provide lightweight module stubs when motor/redis are not installed locally."""
    if "motor" not in sys.modules:
        motor_mod = types.ModuleType("motor")
        motor_asyncio = types.ModuleType("motor.motor_asyncio")

        class _StubClient:
            def __init__(self, *_args: Any, **_kwargs: Any) -> None:
                pass

            def __getitem__(self, _name: str) -> Any:
                return None

            def close(self) -> None:
                pass

        motor_asyncio.AsyncIOMotorClient = _StubClient  # type: ignore[attr-defined]
        motor_asyncio.AsyncIOMotorDatabase = Any  # type: ignore[attr-defined]
        motor_mod.motor_asyncio = motor_asyncio  # type: ignore[attr-defined]
        sys.modules["motor"] = motor_mod
        sys.modules["motor.motor_asyncio"] = motor_asyncio

    if "pymongo" not in sys.modules:
        pymongo_mod = types.ModuleType("pymongo")
        pymongo_mod.ASCENDING = 1  # type: ignore[attr-defined]
        pymongo_mod.DESCENDING = -1  # type: ignore[attr-defined]
        sys.modules["pymongo"] = pymongo_mod

    if "redis" not in sys.modules:
        redis_mod = types.ModuleType("redis")
        redis_asyncio = types.ModuleType("redis.asyncio")

        class _StubRedis:
            @classmethod
            def from_url(cls, *_args: Any, **_kwargs: Any) -> "_StubRedis":
                return cls()

        redis_asyncio.Redis = _StubRedis  # type: ignore[attr-defined]
        redis_asyncio.from_url = _StubRedis.from_url  # type: ignore[attr-defined]
        redis_mod.asyncio = redis_asyncio  # type: ignore[attr-defined]
        sys.modules["redis"] = redis_mod
        sys.modules["redis.asyncio"] = redis_asyncio


class DuplicateKeyError(Exception):
    pass


def _get_nested(doc: Dict[str, Any], dotted_key: str) -> Any:
    cur: Any = doc
    for part in dotted_key.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def _set_nested(doc: Dict[str, Any], dotted_key: str, value: Any) -> None:
    parts = dotted_key.split(".")
    cur = doc
    for part in parts[:-1]:
        if part not in cur or not isinstance(cur[part], dict):
            cur[part] = {}
        cur = cur[part]
    cur[parts[-1]] = value


def _matches(doc: Dict[str, Any], query: Dict[str, Any]) -> bool:
    for key, expected in query.items():
        actual = _get_nested(doc, key)
        if isinstance(expected, dict) and "$in" in expected:
            if actual not in expected["$in"]:
                return False
        else:
            if actual != expected:
                return False
    return True


class FakeCursor:
    def __init__(self, docs: List[Dict[str, Any]]) -> None:
        self._docs = docs

    def sort(self, spec: List[Tuple[str, int]]) -> "FakeCursor":
        for key, direction in reversed(spec):
            self._docs.sort(key=lambda d: d.get(key), reverse=(direction == -1))
        return self

    def skip(self, count: int) -> "FakeCursor":
        self._docs = self._docs[count:]
        return self

    def limit(self, count: int) -> "FakeCursor":
        self._docs = self._docs[:count]
        return self

    async def to_list(self, length: int) -> List[Dict[str, Any]]:
        return [copy.deepcopy(d) for d in self._docs[:length]]

class FakeDeleteResult:
    def __init__(self, deleted_count: int) -> None:
        self.deleted_count = deleted_count

class FakeCollection:
    def __init__(self) -> None:
        self._docs: List[Dict[str, Any]] = []

    async def create_index(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    async def insert_one(self, doc: Dict[str, Any]) -> None:
        ref = doc.get("client_doc_ref")
        if isinstance(ref, str):
            for existing in self._docs:
                if existing.get("client_doc_ref") == ref:
                    raise DuplicateKeyError(f"E11000 duplicate key error: {ref}")
        self._docs.append(copy.deepcopy(doc))

    async def find_one(self, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        for doc in self._docs:
            if _matches(doc, query):
                return copy.deepcopy(doc)
        return None

    async def count_documents(self, query: Dict[str, Any]) -> int:
        return sum(1 for doc in self._docs if _matches(doc, query))

    def find(self, query: Dict[str, Any]) -> FakeCursor:
        matched = [copy.deepcopy(d) for d in self._docs if _matches(d, query)]
        return FakeCursor(matched)

    async def find_one_and_update(
        self,
        query: Dict[str, Any],
        update: Dict[str, Any],
        return_document: bool = True,
    ) -> Optional[Dict[str, Any]]:
        for idx, doc in enumerate(self._docs):
            if _matches(doc, query):
                updated = copy.deepcopy(doc)
                for k, v in update.get("$set", {}).items():
                    _set_nested(updated, k, copy.deepcopy(v))
                for k, v in update.get("$inc", {}).items():
                    _set_nested(updated, k, (_get_nested(updated, k) or 0) + v)
                self._docs[idx] = updated
                return copy.deepcopy(updated if return_document else doc)
        return None

    async def delete_one(self, query: Dict[str, Any]) -> FakeDeleteResult:
        for idx, doc in enumerate(self._docs):
            if _matches(doc, query):
                self._docs.pop(idx)
                return FakeDeleteResult(1)
        return FakeDeleteResult(0)


class FakeDatabase:
    def __init__(self) -> None:
        self._collections: Dict[str, FakeCollection] = {}

    def __getitem__(self, name: str) -> FakeCollection:
        if name not in self._collections:
            self._collections[name] = FakeCollection()
        return self._collections[name]


class FakeRedis:
    def __init__(self, fail_mode: bool = False) -> None:
        self.kv: Dict[str, str] = {}
        self.sets: Dict[str, set] = {}
        self.fail_mode = fail_mode

    def _check(self) -> None:
        if self.fail_mode:
            raise ConnectionError("Simulated Redis outage")

    async def ping(self) -> bool:
        self._check()
        return True

    async def get(self, key: str) -> Optional[str]:
        self._check()
        return self.kv.get(key)

    async def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        self._check()
        self.kv[key] = value
        return True

    async def srem(self, key: str, member: str) -> int:
        self._check()
        s = self.sets.get(key, set())
        if member in s:
            s.remove(member)
            return 1
        return 0

    async def scard(self, key: str) -> int:
        self._check()
        return len(self.sets.get(key, set()))

    async def eval(
        self, _script: str, _numkeys: int, key: str, doc_id: str, max_active: str, _ttl: str
    ) -> int:
        self._check()
        s = self.sets.setdefault(key, set())
        if doc_id in s:
            return 1
        if len(s) >= int(max_active):
            return 0
        s.add(doc_id)
        return 1    
