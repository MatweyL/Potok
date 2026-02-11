import json
from functools import cached_property
from hashlib import md5
from typing import Optional, Dict
from uuid import UUID

from pydantic import BaseModel, model_validator


class PayloadPK(BaseModel):
    id: int = None

    def __eq__(self, other):
        return isinstance(other, PayloadPK) and self.id == other.id

    def __hash__(self):
        return hash(self.id)


class PayloadBody(BaseModel):
    data: Optional[Dict] = None
    checksum: UUID = None

    @model_validator(mode='after')
    def set_checksum(self):
        if not self.checksum:
            self.checksum = UUID(md5((json.dumps(self.data) if self.data is not None else "").encode()).hexdigest())
        return self


class Payload(PayloadPK, PayloadBody):
    pass
