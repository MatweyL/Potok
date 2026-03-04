from typing import List

from service.domain.schemas.task import TaskPK, Task
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import PaginationQuery


class GetTasksUCRq(UCRequest):
    pagination: PaginationQuery


class GetTasksUCRs(UCResponse):
    request: GetTasksUCRq
    tasks: List[Task]


class GetTasksUC(UseCase):
    def __init__(self, task_repo: Repo[Task, Task, TaskPK]):
        self._task_repo = task_repo

    async def apply(self, request: GetTasksUCRq) -> GetTasksUCRs:
        tasks = await self._task_repo.paginated(request.pagination)
        return GetTasksUCRs(success=True, request=request, tasks=tasks)
