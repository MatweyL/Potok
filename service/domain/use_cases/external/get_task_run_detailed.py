from typing import Optional

from service.domain.schemas.enums import TaskType
from service.domain.schemas.task_run import TaskRun, TaskRunPK, TaskRunTimeIntervalProgress, \
    TaskRunTimeIntervalProgressPK
from service.domain.schemas.task_run_detailed import TaskRunDetailed
from service.domain.use_cases.abstract import UseCase, UCRequest, UCResponse
from service.ports.outbound.repo.abstract import Repo
from service.ports.outbound.repo.fields import FilterFieldsDNF


class GetTaskRunDetailedUCRq(UCRequest):
    task_run_id: int


class GetTaskRunDetailedUCRs(UCResponse):
    request: GetTaskRunDetailedUCRq
    task_run_detailed: Optional[TaskRunDetailed] = None


class GetTaskRunDetailedUC(UseCase):
    def __init__(self, task_run_repo: Repo[TaskRun, TaskRun, TaskRunPK],
                 task_run_time_interval_progress_repo: Repo[TaskRunTimeIntervalProgress,
                                                            TaskRunTimeIntervalProgress,
                                                            TaskRunTimeIntervalProgressPK], ):
        self._task_run_repo = task_run_repo
        self._task_run_time_interval_progress_repo = task_run_time_interval_progress_repo

    async def apply(self, request: GetTaskRunDetailedUCRq) -> GetTaskRunDetailedUCRs:
        task_run = await self._task_run_repo.get(TaskRunPK(id=request.task_run_id))
        if not task_run:
            return GetTaskRunDetailedUCRs(success=False, request=request, error="Not found")
        if task_run.type == TaskType.TIME_INTERVAL:
            task_progress = await self._task_run_time_interval_progress_repo.filter(
                FilterFieldsDNF.single('task_run_id', request.task_run_id)
            )
        else:
            task_progress = None
        task_run_detailed = TaskRunDetailed(task_run=task_run, progress=task_progress)
        return GetTaskRunDetailedUCRs(success=True, request=request, task_run_detailed=task_run_detailed, )
