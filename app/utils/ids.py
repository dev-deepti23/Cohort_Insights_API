import re
import uuid
from typing import Any

try:
    from bson import ObjectId
    from bson.errors import InvalidId
except ImportError:
    class InvalidId(ValueError):
        pass

    class ObjectId(str):  # type: ignore[no-redef]
        _HEX_RE = re.compile(r"^[0-9a-fA-F]{24}$")

        def __new__(cls, value: Any = None) -> "ObjectId":
            if value is None:
                value = uuid.uuid4().hex[:24]
            s = str(value)
            if not cls._HEX_RE.match(s):
                raise InvalidId(f"'{s}' is not a valid ObjectId")
            return super().__new__(cls, s.lower())


def parse_object_id(raw_id: str) -> ObjectId:
    """Parse a 24-char hex string into an ObjectId, raising InvalidId if malformed."""
    return ObjectId(raw_id)