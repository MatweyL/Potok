from typing import List

from service.domain.schemas.payload import PayloadPK, Payload
from service.domain.schemas.task import Task, TaskPK
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import PaginationQuery, FilterFieldsDNF, FilterField, ConditionOperation, \
    FilterFieldsConjunct


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
        total = await self._payload_repo.count_by_fields(request.pagination.filter_fields_dnf)
        return GetPayloadsUCRs(success=True, request=request, payloads=payloads, total=total)

class GetPayloadsByGroupUCRq(UCRequest):
    group_id: int
    pagination: PaginationQuery
    search: str | None = None


class GetPayloadsByGroupUCRs(UCResponse):
    request: GetPayloadsByGroupUCRq
    payloads: List[Payload]
    total: int = 0

class GetPayloadsByGroupUC(UseCase):

    def __init__(self, payload_repo: Repo[Payload, Payload, PayloadPK],
                 task_repo: Repo[Task, Task, TaskPK],):
        self._payload_repo = payload_repo
        self._task_repo = task_repo

    async def apply(self, request: GetPayloadsByGroupUCRq) -> GetPayloadsByGroupUCRs:
        # 1. Все задачи группы — берём только payload_id, поэтому без пагинации
        tasks: List[Task] = await self._task_repo.filter(
            FilterFieldsDNF.single("group_id", request.group_id)
        )

        if not tasks:
            return GetPayloadsByGroupUCRs(
                success=True,
                request=request,
                payloads=[],
                total=0,
            )

        payload_ids = list({t.payload_id for t in tasks if t.payload_id})

        # 2. Строим фильтр по payload_id + опциональный search по data
        conjunct_fields = [
            FilterField.new("id", payload_ids, ConditionOperation.IN),
        ]
        if request.search:
            conjunct_fields.append(FilterField.new('data', request.search, ConditionOperation.CONTAINS))

        pagination = PaginationQuery(
            offset_page=request.pagination.offset_page,
            limit_per_page=request.pagination.limit_per_page,
            order_by=request.pagination.order_by,
            asc_sort=request.pagination.asc_sort,
            filter_fields_dnf=FilterFieldsDNF.single_conjunct(conjunct_fields),
        )

        # 3. Грузим payload-ы с пагинацией и фильтром
        payloads = await self._payload_repo.paginated(pagination)

        # total — сколько всего подходит под фильтр (без пагинации)
        total = await self._payload_repo.count_by_fields(
            FilterFieldsDNF.single_conjunct(conjunct_fields)
        )

        return GetPayloadsByGroupUCRs(
            success=True,
            request=request,
            payloads=payloads,
            total=total,
        )
