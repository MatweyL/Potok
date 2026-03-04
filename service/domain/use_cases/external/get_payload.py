from typing import Optional

from service.domain.schemas.payload import Payload, PayloadPK
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo


class GetPayloadUCRq(UCRequest):
    payload_id: int


class GetPayloadUCRs(UCResponse):
    request: GetPayloadUCRq
    payload: Optional[Payload] = None


class GetPayloadUC(UseCase):

    def __init__(self, payload_repo: Repo[Payload, Payload, PayloadPK]):
        self._payload_repo = payload_repo
    async def apply(self, request: GetPayloadUCRq) -> GetPayloadUCRs:
        payload = await self._payload_repo.get(PayloadPK(id=request.payload_id))
        if not payload:
            return GetPayloadUCRs(success=False, error="Not found", request=request)
        return GetPayloadUCRs(success=True, request=request, payload=payload)
