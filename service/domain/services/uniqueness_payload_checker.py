from typing import List

from pydantic import BaseModel, Field

from service.domain.schemas.payload import Payload, PayloadPK, PayloadBody
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF, ConditionOperation


class CheckUniquenessPayloadResponse(BaseModel):
    uniqueness: List[PayloadBody] = Field(default_factory=list)
    exists: List[Payload] = Field(default_factory=list)


class UniquenessPayloadChecker:

    def __init__(self, payload_repo: Repo[Payload, Payload, PayloadPK]):
        self._payload_repo = payload_repo

    async def check(self, payload_bodies: List[PayloadBody]) -> CheckUniquenessPayloadResponse:
        payload_body_by_hashsum = {payload_body.checksum: payload_body for payload_body in payload_bodies}
        all_hashsum_list = list(payload_body_by_hashsum.keys())
        existing_payloads = await self._payload_repo.filter(FilterFieldsDNF.single("checksum",
                                                                                   all_hashsum_list,
                                                                                   ConditionOperation.IN))
        for existing_payload in existing_payloads:
            payload_body_by_hashsum.pop(existing_payload.checksum)
        response = CheckUniquenessPayloadResponse(exists=existing_payloads,
                                                  uniqueness=list(payload_body_by_hashsum.values()))
        return response
