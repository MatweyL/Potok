from typing import Optional, Dict

from service.domain.schemas.payload import Payload, PayloadPK, PayloadBody
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import UpdateFields


class UpdatePayloadUCRq(UCRequest):
    payload_id: int
    payload_data: Optional[Dict]


class UpdatePayloadUCRs(UCResponse):
    request: UpdatePayloadUCRq
    payload: Optional[Payload] = None


class UpdatePayloadUC(UseCase):
    def __init__(self, payload_repo: Repo[Payload, Payload, PayloadPK]):
        self._payload_repo = payload_repo

    async def apply(self, request: UpdatePayloadUCRq) -> UpdatePayloadUCRs:
        payload_body = PayloadBody(data=request.payload_data)
        updated_payload = await self._payload_repo.update(PayloadPK(id=request.payload_id),
                                                          UpdateFields.multiple({
                                                              'data': payload_body.data,
                                                              'checksum': payload_body.checksum,
                                                          }
                                                          ))
        if not updated_payload:
            return UpdatePayloadUCRs(success=False, error="Not found", request=request)
        return UpdatePayloadUCRs(success=True, request=request, payload=updated_payload)
