from typing import Optional

from service.domain.schemas.task import Task, TaskPK
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo


class GetTaskUCRq(UCRequest):
    task_id: int


class GetTaskUCRs(UCResponse):
    request: GetTaskUCRq
    task: Optional[Task] = None


class GetTaskUC(UseCase):
    def __init__(self, task_repo: Repo[Task, Task, TaskPK]):
        self._task_repo = task_repo

    async def apply(self, request: GetTaskUCRq) -> GetTaskUCRs:
        task = await self._task_repo.get(TaskPK(id=request.task_id))
        if not task:
            return GetTaskUCRs(success=False, error='Not found', request=request)
        return GetTaskUCRs(success=True, request=request, task=task)