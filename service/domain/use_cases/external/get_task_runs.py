from typing import Optional, List, Dict

from pydantic import Field

from service.domain.schemas.task import Task, TaskPK
from service.domain.schemas.task_run import TaskRunPK, TaskRun
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF, PaginationQuery, ConditionOperation


class GetTaskRunsUCRq(UCRequest):
    task_id: int
    pagination: Optional[PaginationQuery] = None


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
        filter_fields_dnf = FilterFieldsDNF.single('task_id', request.task_id)
        if request.pagination:
            pagination = request.pagination
            pagination.filter_fields_dnf = filter_fields_dnf
            task_runs = await self._task_runs_repo.paginated(pagination)
        else:
            task_runs = await self._task_runs_repo.filter(filter_fields_dnf)
        return GetTaskRunsUCRs(success=True, request=request, task=task, task_runs=task_runs)


class GetTasksRunsUCRq(UCRequest):
    tasks_ids: List[int]
    pagination: Optional[PaginationQuery] = None


class GetTasksRunsUCRs(UCResponse):
    request: GetTasksRunsUCRq
    task_runs_by_task_id: Dict[int, List[TaskRun]]


class GetTasksRunsUC(UseCase):

    def __init__(self,
                 task_runs_repo: Repo[TaskRun, TaskRun, TaskRunPK]):
        self._task_runs_repo = task_runs_repo

    async def apply(self, request: GetTasksRunsUCRq) -> GetTasksRunsUCRs:
        filter_fields_dnf = FilterFieldsDNF.single('task_id', request.tasks_ids, ConditionOperation.IN)
        if request.pagination:
            pagination = request.pagination
            pagination.filter_fields_dnf = filter_fields_dnf
            task_runs = await self._task_runs_repo.paginated(pagination)
        else:
            task_runs = await self._task_runs_repo.filter(filter_fields_dnf)
        task_runs_by_task_id = {}
        for task_run in task_runs:
            task_id = task_run.task_id
            try:
                task_runs_by_task_id[task_id].append(task_run)
            except KeyError:
                task_runs_by_task_id[task_id] = [task_run]
        return GetTasksRunsUCRs(success=True, request=request, task_runs_by_task_id=task_runs_by_task_id)
