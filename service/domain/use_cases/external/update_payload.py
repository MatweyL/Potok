from typing import Optional, Dict

from service.domain.schemas.payload import Payload, PayloadPK, PayloadBody
from service.domain.services.uniqueness_payload_checker import UniquenessPayloadChecker
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
    def __init__(self, payload_repo: Repo[Payload, Payload, PayloadPK],
                 uniqueness_payload_checker: UniquenessPayloadChecker,
                 ):
        self._payload_repo = payload_repo
        self._uniqueness_payload_checker = uniqueness_payload_checker
    async def apply(self, request: UpdatePayloadUCRq) -> UpdatePayloadUCRs:
        payload_body = PayloadBody(data=request.payload_data)
        check_response = await self._uniqueness_payload_checker.check([payload_body])
        if check_response.exists:
            existing_payload = check_response.exists[0]
            if existing_payload.id != request.payload_id:
                return UpdatePayloadUCRs(success=False, request=request, error=f"Payload with equal data exists; "
                                                                               f"input payload id={request.payload_id}; "
                                                                           f"the same payload id={existing_payload.id}")
            return UpdatePayloadUCRs(success=True, request=request, payload=existing_payload)
        updated_payload = await self._payload_repo.update(PayloadPK(id=request.payload_id),
                                                          UpdateFields.multiple({
                                                              'data': payload_body.data,
                                                              'checksum': payload_body.checksum,
                                                          }
                                                          ))
        if not updated_payload:
            return UpdatePayloadUCRs(success=False, error="Not found", request=request)
        return UpdatePayloadUCRs(success=True, request=request, payload=updated_payload)
