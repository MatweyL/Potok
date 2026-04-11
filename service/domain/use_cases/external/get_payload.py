from typing import Optional, List

from service.domain.schemas.payload import Payload, PayloadPK
from service.domain.schemas.task import TaskPK, Task
from service.domain.schemas.task_detailed import TaskDetailed
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.domain.use_cases.external.get_tasks_detailed import GetTasksDetailedUC, GetTasksDetailedUCRq
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF, PaginationQuery


class GetPayloadUCRq(UCRequest):
    payload_id: int


class GetPayloadUCRs(UCResponse):
    request: GetPayloadUCRq
    payload: Optional[Payload] = None
    tasks_detailed_linked: Optional[List[TaskDetailed]] = None


class GetPayloadUC(UseCase):

    def __init__(self, payload_repo: Repo[Payload, Payload, PayloadPK],
                 task_repo: Repo[Task, Task, TaskPK],
                 get_tasks_detailed_uc: GetTasksDetailedUC, ):
        self._payload_repo = payload_repo
        self._task_repo = task_repo
        self._get_tasks_detailed_uc = get_tasks_detailed_uc

    async def apply(self, request: GetPayloadUCRq) -> GetPayloadUCRs:
        payload = await self._payload_repo.get(PayloadPK(id=request.payload_id))
        if not payload:
            return GetPayloadUCRs(success=False, error="Not found", request=request)
        tasks = await self._task_repo.filter(FilterFieldsDNF.single('payload_id', payload.id))
        tasks_detailed_rs = await self._get_tasks_detailed_uc.apply(
            GetTasksDetailedUCRq(tasks_ids=[task.id for task in tasks],
                                 pagination=PaginationQuery()))
        return GetPayloadUCRs(success=True, request=request, payload=payload,
                              tasks_detailed_linked=tasks_detailed_rs.tasks)
