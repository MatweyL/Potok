from typing import List

from service.domain.schemas.payload import PayloadPK, Payload
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import PaginationQuery, FilterFieldsDNF


class GetPayloadsUCRq(UCRequest):
    pagination: PaginationQuery


class GetPayloadsUCRs(UCResponse):
    request: GetPayloadsUCRq
    payloads: List[Payload]
    total: int = 0


class GetPayloadsUC(UseCase):

    def __init__(self, payload_repo: Repo[Payload, Payload, PayloadPK]):
        self._payload_repo = payload_repo

    async def apply(self, request: GetPayloadsUCRq) -> GetPayloadsUCRs:
        payloads = await self._payload_repo.paginated(request.pagination)
        total = await self._payload_repo.count_by_fields(FilterFieldsDNF.empty())
        return GetPayloadsUCRs(success=True, request=request, payloads=payloads, total=total)
