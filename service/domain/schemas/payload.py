import json
from functools import cached_property
from hashlib import md5
from typing import Optional, Dict

from pydantic import BaseModel


class PayloadPK(BaseModel):
    id: int

    def __eq__(self, other):
        return isinstance(other, PayloadPK) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class PayloadBody(BaseModel):
    data: Optional[Dict] = None

    @cached_property
    def checksum(self) -> str:
        return md5(json.dumps(self.data, default=str).encode('utf-8')).hexdigest()


class Payload(PayloadPK, PayloadBody):
    pass
