from typing import Dict, Any

from service.domain.schemas.payload import Payload, PayloadPK, PayloadBody
from service.domain.services.uniqueness_payload_checker import UniquenessPayloadChecker
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo


class CreatePayloadUCRq(UCRequest):
    data: Dict[str, Any]

class CreatePayloadUCRs(UCResponse):
    request: CreatePayloadUCRq
    payload: Payload | None = None



class CreatePayloadUC(UseCase):
    def __init__(self,
                 uniqueness_payload_checker: UniquenessPayloadChecker,
                 payload_repo: Repo[Payload, Payload, PayloadPK],):
        self._uniqueness_payload_checker = uniqueness_payload_checker
        self._payload_repo = payload_repo

    async def apply(self, request: CreatePayloadUCRq) -> CreatePayloadUCRs:
        input_payload = Payload(data=request.data)
        check_response = await self._uniqueness_payload_checker.check([input_payload])
        if check_response.uniqueness:
            created_payload = await self._payload_repo.create(input_payload)
            return CreatePayloadUCRs(success=True, request=request, payload=created_payload)
        else:
            payload = check_response.exists[0]
            return CreatePayloadUCRs(success=False, error="Payload already exists", request=request, payload=payload)
