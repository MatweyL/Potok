from typing import Optional, List

from pydantic import Field

from service.domain.schemas.task import Task, TaskPK
from service.domain.schemas.task_run import TaskRunPK, TaskRun
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF


class GetTaskRunsUCRq(UCRequest):
    task_id: int


class GetTaskRunsUCRs(UCResponse):
    request: GetTaskRunsUCRq
    task: Optional[Task] = None
    task_runs: List[TaskRun] = Field(default_factory=list)


class GetTaskRunsUC(UseCase):

    def __init__(self,
                 task_repo: Repo[Task, Task, TaskPK],
                 task_runs_repo: Repo[TaskRun, TaskRun, TaskRunPK]):
        self._task_repo = task_repo
        self._task_runs_repo = task_runs_repo

    async def apply(self, request: GetTaskRunsUCRq) -> GetTaskRunsUCRs:
        task = await self._task_repo.get(TaskPK(id=request.task_id))
        if not task:
            return GetTaskRunsUCRs(success=False, error="Not found", request=request)
        task_runs = await self._task_runs_repo.filter(FilterFieldsDNF.single('task_id', request.task_id))
        return GetTaskRunsUCRs(success=True, request=request, task=task, task_runs=task_runs)
